// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! CREATE INDEX and DROP INDEX — the online backfill protocol, for
//! both kinds of index (postings and unique entries), and the
//! removal both statements share. Tables and databases are ddl.zig.
//!
//! Free functions over `*Handler` — see the note in ddl.zig.

const std = @import("std");
const fiber = @import("zfiber");
const pgwire = @import("pgwire");
const zdb = @import("zdb");
const zen = zdb.buffers;

const _catalog = @import("catalog.zig");
const _ddl = @import("ddl.zig");
const _parse = @import("parse.zig");
const _key = @import("key.zig");
const _table = @import("table.zig");
const _txn = @import("txn.zig");
const _database = @import("database.zig");
const _exec = @import("exec.zig");
const _value = @import("value.zig");
const _server = @import("server.zig");

const Bitmap = zdb.zroar.Bitmap;
const Handler = _server.Handler;
const String = _value.String;
const log = std.log.scoped(.sql);
const replyError = _server.replyError;
const replyErrorCode = _server.replyErrorCode;
const replyLockErr = _server.replyLockErr;
const writeCatalogRows = _ddl.writeCatalogRows;

/// CREATE [UNIQUE] INDEX — the online-backfill protocol. Blocks the
/// issuing session until the index is built and valid (the reply
/// is a truthful completion signal, as in Postgres), but OTHER
/// sessions' writes flow during the scan — CONCURRENTLY's semantics
/// — and wait only for the short merge at the end. Until the index
/// is marked valid, its tree is written by this build alone: DML
/// skips an index that is being built, and the build never reads
/// the tree.
/// In order:
///  1. create the index tree (raft, applied);
///  2. reserve the tree against compactions for the whole build —
///     keeps its bottom level empty, the install condition;
///  3. take the exclusive table lock as a BARRIER only: drains
///     statements still holding a pre-index Table (openTable runs
///     after dmlTxn, so post-barrier statements load fresh);
///  4. under it: catalog row (valid_ts = building) + forgetTable —
///     every later statement loads the catalog afresh, sees the
///     index and leaves it alone;
///     read_ts = that row's own commit HLC, read back from the row.
///     Durable and self-describing: any node could re-run this
///     backfill from the catalog row alone;
///  5. release the barrier — writes flow for the whole scan, every
///     row they write landing at commit HLC > read_ts;
///  6. scan the base table at read_ts: for each row, the keys its
///     cell contributes (v1 in `RowIndexKeys`);
///  7. install v1 as BOTTOM-level SSTs at ts = read_ts: per key a
///     bitmap of egids, or for a unique index the one egid (two →
///     23505) — built locally, uploaded to the shared store, and
///     proposed through raft so every node installs the same files
///     in log order;
///  8. retake the barrier: every statement started after step 3 has
///     finished and its writes are applied. Scan the base table for
///     the rows written after read_ts (skipping tables and versions
///     at or below it) into v2, and write the difference between v2
///     and v1 as one transaction through raft: removes and adds for
///     postings; for a unique index a put or tombstone per touched
///     key, two rows on one key being 23505. valid_ts = the applied
///     ts after that commit;
///  9. set valid_ts + forgetTable — a statement whose own read_ts
///     is at or above valid_ts may now use the index; an older
///     snapshot scans instead. Forgetting destroys the cached Table,
///     which is only safe while no statement holds it (DML holds
///     the shared table lock from dmlTxn to txn end); the barrier
///     drains them and parks new ones.
/// A failure after step 4 removes the index again under the barrier
/// (catalog tombstone + tree drop, `removeIndex`), so the name is
/// free to retry. A crash before 4 leaves an invisible orphan tree;
/// after 4 an INVALID index, never read and never written (DROP
/// INDEX cleans it up). Two rows inserted with one value during a
/// unique build are both accepted, since neither probes the index;
/// the build then fails at step 8 and the rows stay for the user to
/// resolve.
pub fn createIndex(
    self: *Handler,
    state: *pgwire.ConnState,
    ci: _parse.CreateIndex,
) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    const ns = try self.sessionNamespace(state) orelse return;
    const base_tid = eng.forest.manifest.lookupTable(ns, ci.table) orelse
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_table,
            "table does not exist",
        );
    const base_key: zdb.kv.TableKey = .{
        .namespace = ns,
        .table_id = base_tid,
    };
    const t = self.srv.openTable(base_key) catch |err| {
        logCreateIndexErr(ci.table, "load schema", err);
        return replyError(state, "failed to load table schema");
    };
    const col = t.column(ci.column) orelse
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_column,
            "column does not exist",
        );
    if (col.ty == .interval)
        return replyErrorCode(
            state,
            pgwire.sqlstate.feature_not_supported,
            "indexes on interval columns are not supported",
        );
    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const arloc = arena.allocator();

    // Same NAME first — that's this exact index existing, the
    // IF NOT EXISTS idempotence case — before the one-index-per-
    // column limit, which is about a SECOND index on the column.
    const name = ci.name orelse
        try _table.indexName(arloc, ci.table, ci.column, .plain);
    if (eng.forest.manifest.lookupTable(ns, name) != null) {
        if (ci.if_not_exists)
            return state.sendCommandComplete("CREATE INDEX");
        return replyErrorCode(
            state,
            pgwire.sqlstate.duplicate_table,
            "index already exists",
        );
    }
    // One index per column, of either kind — dml's `Set.index`
    // invariant.
    for (t.indexes) |idx| {
        if (idx.column == col)
            return replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "column is already indexed",
            );
    }
    // `t` (and `col` into it) dangle past forgetTable below; keep
    // what's needed.
    const column_id = col.column_id;

    var changes = [_]zdb.manifest.TreeChange{zdb.manifest.newTableChange(
        name,
        if (ci.unique) .unique_index else .posting_index,
    )};
    const created = eng.forest.buildTables(
        arloc,
        ns,
        &changes,
        .create_tree,
    ) catch |err| switch (err) {
        error.NameExists => return replyErrorCode(
            state,
            pgwire.sqlstate.duplicate_table,
            "index already exists",
        ),
        else => {
            logCreateIndexErr(name, "create tree", err);
            return replyError(state, "create index failed");
        },
    };
    eng.proposeMC(created.change, .apply) catch |err| {
        logCreateIndexErr(name, "propose tree", err);
        return replyError(state, "create index failed");
    };
    const index_tid = created.table_ids[0];

    const tree_key = zdb.kv.TreeKey{
        .namespace = ns,
        .tree_id = zdb.kv.makeTreeId(index_tid, 0),
    };
    try eng.reserveTree(tree_key);
    defer eng.releaseTree(tree_key);

    const db = try self.srv.database(ns);

    // The barrier: the base table's exclusive lock, taken at steps
    // 3 and 7 and dropped for the build in between. `fail` retakes it
    // when needed, and one deferred dropBarrier covers every exit,
    // held or not.
    const def = _catalog.IndexDef{
        .base_table_id = base_tid,
        .index_table_id = index_tid,
        .column_id = column_id,
        .unique = ci.unique,
        .valid_ts = _catalog.IndexDef.building,
    };
    var build = IndexBuild{
        .handler = self,
        .state = state,
        .a = arloc,
        .ns = ns,
        .def = def,
        .name = name,
        .barrier = db.tableLock(base_tid) catch
            return replyError(state, "internal error: lock state"),
    };
    build.takeBarrier();
    defer build.dropBarrier();

    // TODO: This can return the timestamp the catalog was written at, so we
    // don't need to run build.readTs.
    writeCatalogRows(self, arloc, base_key, &.{}, &.{def}) catch |err| {
        logCreateIndexErr(name, "catalog row", err);
        return replyError(state, "create index failed");
    };
    self.srv.forgetTable(base_key) catch |err|
        return build.fail("forget table", err);
    build.read_ts = build.readTs() catch |err|
        return build.fail("read read_ts", err);
    // The backfill reads at read_ts until the index is valid: counted
    // among the open reads, it holds the read floor below itself, so
    // no compaction meanwhile discards a version the scan must see.
    const read_marks = &self.srv.engine.forest.read_marks;
    read_marks.begin(build.read_ts);
    defer read_marks.end(build.read_ts);

    // Drop the barrier before the backfill, so other txns can
    // proceed.
    build.dropBarrier();
    log.info(
        "create index '{s}': backfill (ns={d} base={d} index={d} read_ts={d})",
        .{ name, ns, base_tid, index_tid, build.read_ts },
    );

    // The backfill's reads and table writes queue behind users' I/O.
    // The fiber's own context: helpers copy the class at spawn.
    const ctx = fiber.ctx();
    const prev_class = ctx.class;
    ctx.class = .bg;
    defer ctx.class = prev_class;

    var rows = build.scan() catch |err| return build.fail("backfill", err);
    build.install(&rows) catch |err| return build.fail("install", err);

    build.takeBarrier();
    build.merge(&rows) catch |err| return build.fail("merge", err);
    build.markValid() catch |err| return build.fail("mark valid", err);

    log.info(
        "created index '{s}' (ns={d} base={d} index={d} rows={d} valid_ts={d})",
        .{ name, ns, base_tid, index_tid, rows.count(), build.valid_ts },
    );
    try state.sendCommandComplete("CREATE INDEX");
}

/// The index under construction, row by row: for each egid, the
/// keys its cell contributes (`Index.keys` — one for an exact index,
/// many for a later kind).
///
/// `v1` holds the first scan: every row as of read_ts. `items` turns
/// it into the index tables. `v2` holds the second scan: only the rows
/// written after read_ts, with the keys they have now. `writeDelta`
/// compares each v2 row with its v1 keys and writes the difference.
/// Nothing is read from the index tree. All of it is in memory for
/// now; moving it to disk later changes this struct only.
///
/// TODO(disk): for a table too big for memory, put the scan's
/// (key, egid) pairs into skiplists instead — a memtable's worth at
/// a time — and flush each full one to a scratch SST with the flush
/// code zdb already has. A MergeIterator over those SSTs then yields
/// the pairs in key order, which is exactly what the bottom-level
/// install consumes: a run of equal keys is one posting bitmap or a
/// unique duplicate. v1's own job, a changed row's old keys, can then
/// be answered by a point read of the base table at read_ts, and the
/// unique question "which v1 row has this key" by a point read of
/// the installed index tree — both bloom-filtered, both files that
/// already exist. The pieces are all in zdb; they need repurposing,
/// not writing.
pub const RowIndexKeys = struct {
    a: std.mem.Allocator,
    unique: bool,
    v1: std.AutoHashMapUnmanaged(u64, []const String) = .empty,
    v2: std.AutoHashMapUnmanaged(u64, []const String) = .empty,
    /// v1 as (key, egid) pairs sorted by key, kept by `items` so
    /// `holder` can find a key's v1 row by binary search.
    v1Pairs: []const Pair = &.{},

    const Pair = struct {
        key: String,
        egid: u64,

        fn lessThan(_: void, x: Pair, y: Pair) bool {
            return switch (std.mem.order(u8, x.key, y.key)) {
                .lt => true,
                .gt => false,
                .eq => x.egid < y.egid,
            };
        }
    };

    pub fn count(self: *const RowIndexKeys) usize {
        return self.v1.count();
    }

    /// First scan: the row's keys as of read_ts. No keys (a NULL
    /// under a unique index) means no entry.
    pub fn setV1(self: *RowIndexKeys, egid: u64, keys: []const String) !void {
        if (keys.len == 0) return;
        try self.v1.put(self.a, egid, keys);
    }

    /// Second scan: the row was written after read_ts; these are its
    /// keys now. No keys means the row was deleted.
    pub fn setV2(self: *RowIndexKeys, egid: u64, keys: []const String) !void {
        try self.v2.put(self.a, egid, keys);
    }

    /// v1 as bottom-level items at `ts` for the index table `table_key`,
    /// in key order: per key a roaring state of its egids, or the one
    /// egid.
    /// A unique key with two rows is error.DuplicateKey.
    pub fn items(
        self: *RowIndexKeys,
        table_key: zdb.kv.TableKey,
        ts: u64,
    ) ![]zdb.kv.Item {
        var pairs: std.ArrayList(Pair) = .empty;
        var it = self.v1.iterator();
        while (it.next()) |e| {
            for (e.value_ptr.*) |key| {
                try pairs.append(self.a, .{ .key = key, .egid = e.key_ptr.* });
            }
        }
        std.mem.sort(Pair, pairs.items, {}, Pair.lessThan);
        self.v1Pairs = pairs.items;

        var out: std.ArrayList(zdb.kv.Item) = .empty;
        var i: usize = 0;
        while (i < pairs.items.len) {
            const key = pairs.items[i].key;
            var end = i + 1;
            while (end < pairs.items.len and
                std.mem.eql(u8, pairs.items[end].key, key)) end += 1;
            const run = pairs.items[i..end];
            defer i = end;

            const item_key = zdb.kv.Key{
                .namespace = table_key.namespace,
                .table_id = table_key.table_id,
                .primary = key,
            };
            if (self.unique) {
                if (run.len > 1) return error.DuplicateKey;
                const ekey = try self.a.dupe(u8, &_key.encodeEgid(run[0].egid));
                try out.append(self.a, .{
                    .key = item_key,
                    .versions = .{ .one = .{
                        .ts = ts,
                        .meta = .{},
                        .value = ekey,
                    } },
                });
                continue;
            }
            var bm = try Bitmap.init(self.a);
            for (run) |p| _ = try bm.set(p.egid);
            try bm.compact(); // canonical minimal buffer
            try out.append(self.a, .{
                .key = item_key,
                .versions = .{ .one = .{
                    .ts = ts,
                    .meta = .{ .kind = .state, .encoding = .roaring },
                    .value = bm.toBuffer(),
                } },
            });
        }
        return out.items;
    }

    /// The v1 row that has `key` (unique index), if any: a binary
    /// search over the sorted pairs.
    fn v1Holder(self: *const RowIndexKeys, key: String) ?u64 {
        var lo: usize = 0;
        var hi: usize = self.v1Pairs.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            switch (std.mem.order(u8, self.v1Pairs[mid].key, key)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return self.v1Pairs[mid].egid,
            }
        }
        return null;
    }

    /// Write the v2 changes into `tx` for index `idx`.
    ///
    /// Postings: for each v2 row, compare its v1 keys with its v2
    /// keys. A key only in v1 gets a remove, a key only in v2 an add.
    ///
    /// Unique: a key has one entry, so each key is decided as a
    /// whole. For every key that a v2 row had in v1 or has in v2,
    /// find who holds it now. Two holders is error.DuplicateKey.
    /// One holder is a put. No holder is a tombstone.
    pub fn writeDelta(
        self: *const RowIndexKeys,
        tx: *_txn.Txn,
        idx: *const _table.Index,
    ) !void {
        const tid = idx.index_table_id;
        const empty: []const String = &.{};

        if (!self.unique) {
            var it = self.v2.iterator();
            while (it.next()) |e| {
                const egid = e.key_ptr.*;
                const old = self.v1.get(egid) orelse empty;
                const new = e.value_ptr.*;
                for (old) |key| {
                    if (contains(new, key)) continue;
                    try tx.putPosting(tid, key, egid, .remove);
                }
                for (new) |key| {
                    if (contains(old, key)) continue;
                    try tx.putPosting(tid, key, egid, .add);
                }
            }
            return;
        }

        // Step 1: collect the keys. `holder_v2[key]` is the v2 row
        // that has `key` now, or null if no v2 row has it. A key is
        // entered with null when a v2 row HAD it in v1 (the row may
        // have moved away), and with the row when a v2 row HAS it
        // now. Two v2 rows having the same key now is a duplicate:
        // for example, two rows inserted with the same value while
        // the index was being built.
        var holder_v2: std.StringArrayHashMapUnmanaged(?u64) = .empty;
        var it2 = self.v2.iterator();
        while (it2.next()) |e2| {
            const egid2 = e2.key_ptr.*;
            for (e2.value_ptr.*) |key2| {
                const gop = try holder_v2.getOrPut(self.a, key2);
                if (gop.found_existing and gop.value_ptr.* != null) {
                    // No two egids are allowed to hold the same key2.
                    return error.DuplicateKey;
                }
                gop.value_ptr.* = egid2;
            }
            // A row inserted after read_ts has no v1 keys to leave.
            const keys1 = self.v1.get(egid2) orelse continue;
            for (keys1) |key1| {
                // Only set key1 to null if not present already.
                _ = try holder_v2.getOrPutValue(self.a, key1, null);
            }
        }

        // Step 2: decide each key. `holder_v2` now maps every key a
        // v2 row touched to the v2 row that has it, or null. What is
        // left to ask is whether the v1 row of the key still has it.
        //
        // Example: row 1 had 7, then moved to 9 while row 3 was
        // inserted with 7. Key 7's v1 row is 1 and its v2 row is 3.
        // Not a duplicate: row 1 changed, and its v2 keys are [9].
        // Had row 1 not changed, it would still have 7, and row 3
        // would be a duplicate: a row inserted during the build with
        // a value an old row already had.
        for (holder_v2.keys(), holder_v2.values()) |key, v2_egid| {
            var owner = v2_egid;
            if (self.v1Holder(key)) |v1_egid| {
                // Does v1_egid still hold the key? If v1_egid changed,
                // its v2 keys were entered in step 1: it is `owner`
                // if it kept the key, and not involved if it left. So
                // only an unchanged v1 row needs a decision here: it
                // still holds the key, and a v2 row on the same key
                // is a second holder.
                const unchanged = !self.v2.contains(v1_egid);
                if (unchanged) {
                    if (owner != null) return error.DuplicateKey;
                    owner = v1_egid;
                }
            }
            const egid = owner orelse {
                try tx.put(tid, key, "", .{ .kind = .deleted });
                continue;
            };
            const ekey = _key.encodeEgid(egid);
            try tx.put(tid, key, &ekey, .{});
        }
    }
};

fn contains(keys: []const String, key: String) bool {
    for (keys) |k| if (std.mem.eql(u8, k, key)) return true;
    return false;
}

/// One CREATE INDEX past its catalog row: the steps of the protocol
/// as methods, and `fail`, which removes the index again.
const IndexBuild = struct {
    handler: *Handler,
    state: *pgwire.ConnState,
    a: std.mem.Allocator,
    ns: u64,
    def: _catalog.IndexDef,
    name: String,
    /// The barrier: the base table's exclusive lock, and whether
    /// this build holds it now.
    barrier: *_database.RWMutex,
    barrier_held: bool = false,
    /// The catalog row's commit HLC: the first scan's snapshot, and
    /// the bound the second scan starts from.
    read_ts: u64 = 0,
    /// The applied ts under barrier-2, which the merge brings the
    /// rows up to: the install's ts and the index's `valid_ts`.
    valid_ts: u64 = 0,

    /// Take the barrier unless held.
    fn takeBarrier(self: *IndexBuild) void {
        if (self.barrier_held) return;
        self.barrier.lock(@src());
        self.barrier_held = true;
    }

    /// Drop the barrier if held.
    fn dropBarrier(self: *IndexBuild) void {
        if (!self.barrier_held) return;
        self.barrier.unlock();
        self.barrier_held = false;
    }

    /// Log `err`, remove the index under the barrier (retaken if
    /// released — a no-op if held), and reply: 23505 for a duplicate,
    /// else a plain failure. The name is free to retry either way.
    fn fail(self: *IndexBuild, what: String, err: anyerror) !void {
        logCreateIndexErr(self.name, what, err);
        self.takeBarrier();
        removeIndex(
            self.handler,
            self.a,
            self.ns,
            self.def.base_table_id,
            self.def.index_table_id,
        ) catch |rerr| logCreateIndexErr(self.name, "remove", rerr);
        if (err == error.DuplicateKey)
            return replyErrorCode(
                self.state,
                pgwire.sqlstate.unique_violation,
                "could not create unique index: " ++
                    "duplicate key value violates unique constraint",
            );
        return replyError(self.state, "create index failed");
    }

    /// The commit HLC of the index's catalog row, read back from it.
    fn readTs(self: *IndexBuild) !u64 {
        const eng = self.handler.srv.engine;
        const ck = _catalog.encodeIndexKey(
            self.def.base_table_id,
            self.def.index_table_id,
        );
        const found = try eng.forest.get(eng.allocator, .{ .key = .{
            .namespace = self.ns,
            .table_id = _catalog.index_table_id,
            .primary = &ck,
        } }) orelse return error.RowMissing;
        defer found.free(eng.allocator);
        return found.newest().ts;
    }

    /// The reloaded Table and this index within it.
    fn index(self: *IndexBuild) !struct {
        tbl: *const _table.Table,
        idx: *const _table.Index,
    } {
        const tbl = try self.handler.srv.openTable(
            .{ .namespace = self.ns, .table_id = self.def.base_table_id },
        );
        for (tbl.indexes) |*idx| {
            if (idx.index_table_id == self.def.index_table_id)
                return .{ .tbl = tbl, .idx = idx };
        }
        return error.IndexMissing;
    }

    /// The keys of one base row for this index, from the row's cell.
    fn rowKeys(
        self: *IndexBuild,
        at: anytype,
        item: zdb.kv.Item,
    ) ![]const String {
        const cell = at.tbl.parseCol(item, at.idx.column);
        return at.idx.keys(self.a, cell);
    }

    /// Step 6: scan the base table at `read_ts` into v1. Memory ∝
    /// table, accepted for now.
    fn scan(self: *IndexBuild) !RowIndexKeys {
        const eng = self.handler.srv.engine;
        const at = try self.index();
        var rows = RowIndexKeys{ .a = self.a, .unique = self.def.unique };
        var cur = try eng.forest.openCursor(
            self.a,
            self.baseKey(),
            .all,
            .{ .read_ts = self.read_ts },
        );
        defer cur.deinit();
        var seen: usize = 0;
        while (try cur.next()) |item| : (seen += 1) {
            // The backfill is the longest statement there is: a yield
            // point every batch of rows, as a DML scan has.
            if (seen % _exec.row_cap == 0) try fiber.ctx().maybeYield();
            const egid = _key.decodeEgid(item.key.primary) orelse
                continue; // not row-shaped: nothing to index
            try rows.setV1(egid, try self.rowKeys(at, item));
        }
        log.info(
            "backfill index={d}: scanned rows={d}",
            .{ self.def.index_table_id, rows.count() },
        );
        return rows;
    }

    /// Step 8, under barrier-2: every statement started after
    /// barrier-1 has finished and its writes are applied. Scan the
    /// base table for the rows written after `read_ts` — tables and
    /// versions at or below it are skipped, so the pass costs what
    /// changed — into v2, a deleted row with no keys. Then write the
    /// difference between v2 and v1 as one transaction through raft,
    /// the way a DML statement writes its index entries; its commit
    /// lands above the installed tables. `valid_ts` is the applied ts
    /// after that: a snapshot older than it would see the tables
    /// without the difference.
    fn merge(self: *IndexBuild, rows: *RowIndexKeys) !void {
        const eng = self.handler.srv.engine;
        try eng.catchUp();
        const at = try self.index();
        var cur = try eng.forest.openCursor(self.a, self.baseKey(), .all, .{
            .read_ts = eng.node.appliedTs(),
            .min_ts = self.read_ts,
            .include_deleted = true,
        });
        defer cur.deinit();
        var changed: usize = 0;
        var seen: usize = 0;
        while (try cur.next()) |item| : (seen += 1) {
            if (seen % _exec.row_cap == 0) try fiber.ctx().maybeYield();
            const egid = _key.decodeEgid(item.key.primary) orelse continue;
            changed += 1;
            const keys: []const String = if (item.newest().meta.isDeleted())
                &.{}
            else
                try self.rowKeys(at, item);
            try rows.setV2(egid, keys);
        }

        const tx = try self.handler.newTxn(self.ns);
        defer self.handler.destroyTxn(tx);
        try rows.writeDelta(tx, at.idx);
        try self.handler.commitTxn(self.a, tx);
        self.valid_ts = eng.node.appliedTs();
        log.info(
            "backfill index={d}: merged changed rows={d} valid_ts={d}",
            .{ self.def.index_table_id, changed, self.valid_ts },
        );
    }

    /// The base table whose rows the index is built from.
    fn baseKey(self: *const IndexBuild) zdb.kv.TableKey {
        return .{ .namespace = self.ns, .table_id = self.def.base_table_id };
    }

    /// Step 7, outside the barrier: v1 as BOTTOM-level SSTs at ts =
    /// `read_ts` — built locally, uploaded and announced like every
    /// other SST creator's files (followers download from the shared
    /// store), then proposed through raft so every node applies the
    /// same manifest change. Roaring state values for postings; the
    /// 8-byte egid for unique entries; a unique key with two rows is
    /// error.DuplicateKey. The tree is empty and the merge writes at
    /// newer timestamps, so level order holds. An empty v1 installs
    /// nothing.
    fn install(self: *IndexBuild, rows: *RowIndexKeys) !void {
        const eng = self.handler.srv.engine;
        if (rows.count() == 0) return;
        const index_tid = self.def.index_table_id;
        const index_key: zdb.kv.TableKey = .{
            .namespace = self.ns,
            .table_id = index_tid,
        };

        const items = try rows.items(index_key, self.read_ts);
        var changes = try eng.forest.buildBottomTable(
            self.a,
            index_key,
            items,
        );
        defer changes.deinit(self.a);

        try eng.announceNewFiles(changes.items);
        try eng.proposeMC(.{
            .cmd = .index_backfill,
            .sstable_changes = changes.items,
            .tree_changes = &.{},
            .memtable_id = 0,
        }, .apply);
        // applyBackfill skips (deterministically, on every node) when
        // the bottom level was not empty; surface that as a failure
        // instead of marking valid an index that lacks its scanned
        // entries. The proposed files themselves are looked for: a
        // table count is non-zero after a skip too.
        const landed = try eng.forest.bottomLevelHas(
            index_key,
            changes.items,
        );
        if (!landed) return error.BackfillSkipped;
        log.info(
            "backfill index={d}: installed tables={d}",
            .{ index_tid, changes.items.len },
        );
    }

    /// Step 9: complete from `valid_ts` — the installed entries sit
    /// at that ts, and every later commit maintains the index itself.
    fn markValid(self: *IndexBuild) !void {
        var vdef = self.def;
        vdef.valid_ts = self.valid_ts;
        const base_key: zdb.kv.TableKey = .{
            .namespace = self.ns,
            .table_id = self.def.base_table_id,
        };
        try writeCatalogRows(
            self.handler,
            self.a,
            base_key,
            &.{},
            &.{vdef},
        );
        try self.handler.srv.forgetTable(base_key);
    }
};

/// DROP INDEX. The index's column must not be the PRIMARY KEY's —
/// that index is structural and lives and dies with its table; any
/// other index, unique or not, drops. Under the base table's
/// exclusive lock (the reload swaps its cached schema),
/// `removeIndex` does the work.
pub fn dropIndex(
    self: *Handler,
    state: *pgwire.ConnState,
    di: _parse.DropIndex,
) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    const ns = try self.sessionNamespace(state) orelse return;
    const index_tid = eng.forest.manifest.lookupTable(ns, di.name) orelse {
        if (di.missing_ok) return state.sendCommandComplete("DROP INDEX");
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_object,
            "index does not exist",
        );
    };

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var scan = _table.scanIndexKeys(
        a,
        eng.forest,
        .{ .namespace = ns, .table_id = index_tid },
    ) catch |err| {
        logDropIndexErr(di.name, "scan catalog", err);
        return replyError(state, "drop index failed");
    };
    defer scan.table_ids.deinit(a);
    if (!scan.is_index)
        return replyErrorCode(
            state,
            pgwire.sqlstate.wrong_object_type,
            "not an index; use DROP TABLE",
        );
    const base_tid = scan.base_table_id.?;

    const tbl = self.srv.openTable(.{
        .namespace = ns,
        .table_id = base_tid,
    }) catch |err| {
        logDropIndexErr(di.name, "load schema", err);
        return replyError(state, "drop index failed");
    };
    const idx = for (tbl.indexes) |*idx| {
        if (idx.index_table_id == index_tid) break idx;
    } else {
        logDropIndexErr(di.name, "find index", error.RowMissing);
        return replyError(state, "drop index failed");
    };
    if (idx.column.pkey_position != null)
        return replyErrorCode(
            state,
            pgwire.sqlstate.feature_not_supported,
            "cannot drop a PRIMARY KEY index",
        );

    // Locks: same shape as dropTable, on the BASE table — the
    // reload below swaps its cached schema.
    const db = try self.srv.database(ns);
    const table_lock = db.tableLock(base_tid) catch
        return replyError(state, "internal error: lock state");
    table_lock.lock(@src());
    defer table_lock.unlock();

    removeIndex(self, a, ns, base_tid, index_tid) catch |err| {
        logDropIndexErr(di.name, "remove", err);
        return replyError(state, "drop index failed");
    };

    log.info(
        "dropped index '{s}' (ns={d} base={d} index={d})",
        .{ di.name, ns, base_tid, index_tid },
    );
    try state.sendCommandComplete("DROP INDEX");
}

/// Remove an index: tombstone its catalog row, drop its tree, reload
/// the base table's schema. Order matters: the catalog row FIRST —
/// the reverse would leave a live IndexDef pointing at a dead tree
/// (reads would see silently-empty results), and unlike dropTable's
/// tolerated catalog garbage, a live IndexDef row would resurrect
/// the index on the next schema load. The caller holds the base
/// table's exclusive lock; this must not take it.
fn removeIndex(
    self: *Handler,
    a: std.mem.Allocator,
    ns: u64,
    base_tid: u32,
    index_tid: u32,
) !void {
    const eng = self.srv.engine;
    const ck = _catalog.encodeIndexKey(base_tid, index_tid);
    const wire_key = try (zdb.kv.Key{
        .namespace = ns,
        .table_id = _catalog.index_table_id,
        .primary = &ck,
    }).marshal(a);
    var kvs = [_]zen.KeyValueStruct{.{
        .key = wire_key,
        .value = "",
        .meta = (zdb.kv.Meta{ .kind = .deleted }).toByte(),
    }};
    try eng.proposeWrites(ns, &kvs, .apply);

    const mc = try eng.forest.buildTableDrop(a, ns, &.{index_tid});
    try eng.proposeMC(mc, .apply);
    try self.srv.forgetTable(.{ .namespace = ns, .table_id = base_tid });
}

fn logCreateIndexErr(name: String, what: String, err: anyerror) void {
    log.err(
        "create index '{s}': {s}: {s}",
        .{ name, what, @errorName(err) },
    );
}

fn logDropIndexErr(name: String, what: String, err: anyerror) void {
    log.err(
        "drop index '{s}': {s}: {s}",
        .{ name, what, @errorName(err) },
    );
}
