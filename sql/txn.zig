// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Transaction context for DML. Every statement runs inside a Txn —
//! implicit (one statement, auto-commit) today; interactive
//! (BEGIN..COMMIT) changes only its lifetime. Pending writes live in a
//! private skiplist — the memtable structure — holding full marshaled
//! keys at ts = read_ts. Point read-your-own-writes is a skiplist get;
//! scans can merge the skiplist in as one more iterator source; commit
//! walks it in key order into the flat wire kv list. Nothing touches
//! shared storage before commit; rollback just drops the skiplist.

const std = @import("std");
const utils = @import("utils");
const fiber = @import("zfiber");
const zdb = @import("zdb");

const zen = zdb.buffers;
const Item = zdb.kv.Item;
const Key = zdb.kv.Key;
const Forest = zdb.forest.Forest;
const Meta = zdb.kv.Meta;
const Oracle = @import("oracle.zig").Oracle;
const Database = @import("database.zig").Database;
const RWMutex = @import("database.zig").RWMutex;
const String = @import("value.zig").String;

/// One statement-generation's read tracking: the point-read identity
/// hashes it accumulated and whether it ran a scan. A Txn holds two —
/// `prev` (settled prior statements) and `cur` (the one in flight) — so
/// a restart just clears `cur`, and finishing a statement merges `cur`
/// into `prev`.
const ReadSet = struct {
    /// Identity hashes of point reads (Key.identityHash).
    reads: std.ArrayList(u64) = .empty,
    /// A scan ran: its result set can't be revalidated by key hashes (a
    /// fresh matching row would be a phantom), so a later bump must abort
    /// rather than revalidate.
    ran_scan: bool = false,

    fn deinit(self: *ReadSet, alloc: std.mem.Allocator) void {
        self.reads.deinit(alloc);
    }

    /// Discard everything — on restart, or after merging into `prev`.
    fn clear(self: *ReadSet) void {
        utils.clearRetainCap(&self.reads);
        self.ran_scan = false;
    }

    /// Fold `other` in: its reads join ours and its scan flag ORs into
    /// ours. Promotes a finished statement's `cur` into `prev`.
    fn merge(self: *ReadSet, alloc: std.mem.Allocator, other: ReadSet) !void {
        try self.reads.appendSlice(alloc, other.reads.items);
        self.ran_scan = self.ran_scan or other.ran_scan;
    }

    /// Would advancing to `read_ts` invalidate any read here? A scan
    /// can't be revalidated by hash (phantoms) so it always conflicts;
    /// a point read conflicts iff its key committed above the snapshot
    /// (oracle floor > read_ts). Conservative: a commit even above the
    /// new snapshot still aborts.
    fn aboveReadTs(self: ReadSet, oracle: *Oracle, read_ts: u64) bool {
        if (self.ran_scan) return true;
        return oracle.maxCommitTs(self.reads.items) > read_ts;
    }
};

/// A table lock this txn holds, released at txn end.
const HeldTable = struct {
    mutex: *RWMutex,
    mode: fiber.SemaphoreMap.Mode,
};

pub const Txn = struct {
    allocator: std.mem.Allocator,
    /// Owner token for the lock table; Server-issued, process-local.
    id: u64,
    ns: u64,
    /// Implicit (auto-commit) txn — created for and committed by a single
    /// statement — vs an interactive BEGIN..COMMIT block. The txn
    /// machinery ignores it; the SQL handler sets it at creation and
    /// reads it to decide whether the statement commits at its end.
    implicit: bool = false,
    /// Raft term at begin. Checked before commit: a demote/re-elect
    /// between our reads and the propose means another leader may have
    /// committed writes our locked reads never saw — abort instead.
    term: u64,
    /// The database this txn runs in: its row lock table (leader-local
    /// keyed semaphores with deadlock detection), table locks, and
    /// commit oracle (newest commit_ts per key identity, consulted
    /// after every fresh row lock — the bump check — and when a bump
    /// revalidates prior reads).
    db: *Database,
    /// The table locks this txn holds, in acquisition order.
    held_tables: std.ArrayList(HeldTable) = .empty,
    /// Committed storage, the fallback half of `lookup`.
    forest: *zdb.forest.Forest,
    /// Lock keys (row + table identity hashes) this txn newly acquired
    /// (the map handles re-entrant acquires); released in deinit — lock
    /// lifetime IS txn lifetime.
    held_rows: std.ArrayList(u64) = .empty,
    /// A statement in this txn errored: locks are already released
    /// (Postgres aborts and releases at error time), pending writes are
    /// doomed, and only COMMIT/ROLLBACK — both of which roll back — are
    /// accepted until the client ends the block.
    failed: bool = false,
    /// The snapshot every read in this txn sees. Starts at the initial
    /// read_ts and only moves FORWARD, via the bump in `lockRows`: locking
    /// a key another txn committed above our snapshot advances read_ts to
    /// that commit — after revalidating every prior read (see bump
    /// notes there) and forcing the current statement to restart at the
    /// new snapshot. Pending entries are inserted at the read_ts in
    /// effect at their put: at-or-above every committed version the txn
    /// can see, and visible to its own scans. The commit timestamp is
    /// raft's, not ours — apply rewrites every wire key's ts to the
    /// commit entry's HLC.
    ///
    /// The forest counts this txn among its open reads at `read_ts`
    /// (`Forest.read_marks`), which holds the read floor at or below it
    /// for as long as the txn lives: no build discards a version it can
    /// see. The count is found again from the value, so nothing assigns
    /// it but `init` and `bumpTo`, which move the count with it.
    read_ts: u64,
    /// Wall-clock µs since the Unix epoch when the txn began: the value
    /// of CURRENT_TIMESTAMP for every statement in it (transaction
    /// start, as in Postgres). Set by the server at creation; 0 in
    /// unit tests, which never evaluate it.
    start_us: i64 = 0,
    /// Reads settled by prior statements. Revalidated on a bump (see
    /// lockRows). INVARIANT: all are valid as of the current read_ts —
    /// bump revalidates `prev`, restart re-runs `cur` — so COMMIT never
    /// validates anything.
    prev: ReadSet = .{},
    /// The in-flight statement's reads: discarded and re-run on a
    /// restart, merged into `prev` when the statement finishes.
    cur: ReadSet = .{},
    /// Pending puts and tombstones, plus secondary-index posting ops
    /// (`putPosting`), created by the first write — a read-only
    /// statement never allocates it. Same-key re-put replaces
    /// value+meta in place (putItem's exact-match path) — last write
    /// wins within the txn. Owns its arena; released in deinit.
    writes: ?*zdb.skl.SkipList = null,

    pub fn init(
        allocator: std.mem.Allocator,
        id: u64,
        read_ts: u64,
        term: u64,
        db: *Database,
        forest: *zdb.forest.Forest,
    ) Txn {
        forest.read_marks.begin(read_ts);
        return .{
            .allocator = allocator,
            .id = id,
            .ns = db.ns,
            .read_ts = read_ts,
            .term = term,
            .db = db,
            .forest = forest,
        };
    }

    pub fn deinit(self: *Txn) void {
        self.forest.read_marks.end(self.read_ts);
        self.releaseLocks();
        self.held_rows.deinit(self.allocator);
        self.held_tables.deinit(self.allocator);
        self.prev.deinit(self.allocator);
        self.cur.deinit(self.allocator);
        if (self.writes) |w| w.release();
    }

    /// A new statement begins: merge the finished statement's reads and
    /// scan flag into `prev` (they become "prior" — revalidated on a
    /// later bump) and clear `cur` for the new one. The statement driver
    /// calls this once per DML statement of an interactive txn; an
    /// implicit txn is born at its only statement's start, so its empty
    /// defaults already say the same.
    pub fn beginStatement(self: *Txn) !void {
        try self.prev.merge(self.allocator, self.cur);
        self.cur.clear();
    }

    /// Rewind the current statement's tracking before re-running it at
    /// the bumped read_ts: its point reads and scan are about to be
    /// re-executed against the new snapshot. `prev` is untouched.
    pub fn restartStatement(self: *Txn) void {
        self.cur.clear();
    }

    /// The current statement ran (or is about to run) a scan at the
    /// current read_ts.
    pub fn recordScan(self: *Txn) void {
        self.cur.ran_scan = true;
    }

    /// The current statement read `key` outside `lookup` — the
    /// posting-bitmap path records the INDEX key here: any commit that
    /// changes the key's membership writes a posting delta under this
    /// exact key (bumping its oracle floor), so one identity hash
    /// revalidates the whole result set on a later bump. Phantom-free,
    /// unlike a scan.
    pub fn recordRead(self: *Txn, key: Key) !void {
        try self.cur.reads.append(self.allocator, key.identityHash());
    }

    /// Release every held row lock NOW — the abort path. Idempotent;
    /// deinit calls it too. Failing txns release immediately (a deadlock
    /// victim holding its locks until the client's ROLLBACK would keep
    /// the cycle alive); the txn object itself stays for the failed-state
    /// bookkeeping until the block ends.
    pub fn releaseLocks(self: *Txn) void {
        self.db.row_locks.releaseMany(self.held_rows.items, self.id);
        utils.clearRetainCap(&self.held_rows);
        // Tables after the rows, in reverse acquisition order.
        var i = self.held_tables.items.len;
        while (i > 0) {
            i -= 1;
            const h = self.held_tables.items[i];
            switch (h.mode) {
                .shared => h.mutex.unlockShared(),
                .exclusive => h.mutex.unlock(),
            }
        }
        utils.clearRetainCap(&self.held_tables);
    }

    pub const LockError = error{ Deadlock, Upgrade, OutOfMemory };
    /// What the bump adds: `Conflict` = a prior read is invalid at the
    /// snapshot the bump needs (40001, the txn aborts); `Restart` = the
    /// bump succeeded and the CURRENT statement must re-run at the new
    /// read_ts (the statement driver retries; never client-visible).
    pub const RowLockError = LockError || error{ Conflict, Restart };

    /// The row-lock and oracle key of a row: `Key.identityHash` over
    /// this txn's namespace.
    pub fn rowHash(self: *const Txn, table_id: u32, primary: String) u64 {
        return (Key{
            .namespace = self.ns,
            .table_id = table_id,
            .primary = primary,
        }).identityHash();
    }

    /// Block until this txn holds the row lock for every row in
    /// `hashes` (their `rowHash`es) — FIFO, re-entrant, released at
    /// txn end. The lock key is the row's identity hash (ts excluded):
    /// one identity per row, every version, no allocation. One round
    /// on the lock map for the whole set.
    ///
    /// Then the bump check, one oracle read for the highest commit
    /// among the keys: if any row committed above our snapshot, advance
    /// read_ts to that commit so the statement can see it — legal only
    /// if everything already read stays valid at the new snapshot:
    ///   - prior statements' scans: unverifiable by hash → Conflict;
    ///   - prior statements' point reads: one batch oracle probe; any
    ///     of them re-committed above the old snapshot → Conflict
    ///     (conservative: a commit even above the new floor aborts);
    ///   - the current statement's reads/scans: discarded and re-run —
    ///     Restart.
    /// Every key stays held through `Restart`/`Conflict`, so the retry
    /// re-locks them re-entrantly; a key held since before cannot have
    /// had its floor move, so the check is only ever run once per key.
    pub fn lockRows(self: *Txn, hashes: []const u64) RowLockError!void {
        try self.held_rows.ensureUnusedCapacity(self.allocator, hashes.len);
        try self.db.row_locks.acquireMany(
            hashes,
            self.id,
            .exclusive,
            &self.held_rows,
        );
        fiber.ctx().record("{d} row locks taken", .{hashes.len});
        const newest = self.db.oracle.maxCommitTs(hashes);
        fiber.ctx().record("oracle newest ts: {d}", .{newest});
        return self.bumpTo(newest);
    }

    /// Advance read_ts to `ts` so the statement can see state applied
    /// at that ts (a row's newest version under a fresh lock, an index
    /// completed at `Index.valid_ts`). No-op when already at or above
    /// it. Otherwise legal only if everything already read stays valid
    /// at the new snapshot — prior statements' reads are checked as
    /// lockRows describes, Conflict if any moved — and then the current
    /// statement is re-run from nothing: Restart. Needs no lock: `ts`
    /// names locally applied state, which cannot regress.
    pub fn bumpTo(self: *Txn, ts: u64) error{ Conflict, Restart }!void {
        if (ts <= self.read_ts) return;
        if (self.prev.aboveReadTs(&self.db.oracle, self.read_ts))
            return error.Conflict;
        // Counted at the new timestamp before the old count is given
        // up, so the floor never sees this txn as gone.
        self.forest.read_marks.begin(ts);
        self.forest.read_marks.end(self.read_ts);
        self.read_ts = ts;
        return error.Restart;
    }

    /// Table lock (Database.tableLock): shared for DML statements (they
    /// only need the table to keep existing), exclusive for DDL.
    /// Re-entrant per txn like row locks; released at txn end. The
    /// global lock order is table → row.
    pub fn lockTable(
        self: *Txn,
        table_id: u32,
        mode: fiber.SemaphoreMap.Mode,
    ) LockError!void {
        try self.lockTableMutex(try self.db.tableLock(table_id), mode);
    }

    /// Take a table lock unless this txn already holds it. A held
    /// exclusive covers a shared request; shared then exclusive is
    /// `Upgrade`: the mutex cannot grant that to its own reader, and
    /// SQL never asks (DDL runs outside blocks). Recorded for release.
    fn lockTableMutex(
        self: *Txn,
        mutex: *RWMutex,
        mode: fiber.SemaphoreMap.Mode,
    ) LockError!void {
        for (self.held_tables.items) |h| {
            if (h.mutex != mutex) continue;
            if (h.mode == .exclusive or mode == .shared) return;
            return error.Upgrade;
        }
        // Allocate memory before acquiring locks.
        try self.held_tables.ensureUnusedCapacity(self.allocator, 1);

        // Note that all DML txns would acquire locks in shared mode.
        switch (mode) {
            .shared => mutex.lockShared(@src()),
            .exclusive => mutex.lock(@src()),
        }
        self.held_tables.appendAssumeCapacity(.{
            .mutex = mutex,
            .mode = mode,
        });
    }

    /// Buffer a put — or a tombstone, via `meta` — of `primary` in
    /// `table_id`. Key and value bytes are copied into the txn's arena.
    pub fn put(
        self: *Txn,
        table_id: u32,
        primary: String,
        val: String,
        meta: Meta,
    ) !void {
        const w = try self.pendingWrites();
        try w.putItem(.{
            .key = .{
                .namespace = self.ns,
                .table_id = table_id,
                .primary = primary,
            },
            .versions = .{ .one = .{
                .ts = self.read_ts,
                .meta = meta,
                .value = val,
            } },
        });
    }

    fn pendingWrites(self: *Txn) !*zdb.skl.SkipList {
        if (self.writes) |w| return w;
        // Capacity 0: unlike the memtable, a txn's write set is
        // usually tiny — let the arena grow on demand.
        const w = try zdb.skl.SkipList.initCapacity(self.allocator, 0);
        self.writes = w;
        return w;
    }

    /// A pending secondary-index posting op: add/remove this row's
    /// egid under an index key. Rides the writes skiplist under
    /// key = {ns, index table, primary = index_key} with the version's
    /// TIMESTAMP holding the EGID — the one place a pending version's
    /// ts is not a timestamp — so one index key's ops form a
    /// contiguous run (egids descending, versions sort ts DESC) and the
    /// same (index_key, egid) always lands on the same node. The op is
    /// the 1-byte value; `noop` marks an annihilated pair and never
    /// reaches the wire.
    pub const PostingOp = enum(u8) { add, remove, noop };

    /// Meta for pending posting entries: the discriminator wireKvs
    /// uses to tell posting runs from row versions (row writes are
    /// never roaring).
    const posting_meta: Meta = .{ .kind = .delta, .encoding = .roaring };

    /// Record `op` for (index key, egid) — no locks: posting deltas
    /// commute, and the ROW's lock already serializes add/remove of
    /// one egid (docs/transactions.md, lock classes).
    ///
    /// Annihilation, not last-write-wins: ops for one (index_key,
    /// egid) strictly alternate within a txn — a remove is only
    /// emitted when the pending-aware row currently maps to this
    /// index key, an add only when it doesn't, and egids are never
    /// reused after a delete. An opposite pending op therefore
    /// cancels to `noop` (the pair nets to "no change vs committed
    /// state"), and a later op re-opens the slot. This keeps the wire
    /// delta the EXACT diff: UPDATE 3→5→3 emits nothing at all — no
    /// spurious record, and no spurious oracle bump for concurrent
    /// readers of an index key this txn net-changed nothing on.
    pub fn putPosting(
        self: *Txn,
        table_id: u32,
        index_key: String,
        egid: u64,
        op: PostingOp,
    ) !void {
        // egid 0 never exists (allocation starts at 1).
        std.debug.assert(egid != 0);
        std.debug.assert(op != .noop);
        const key = Key{
            .namespace = self.ns,
            .table_id = table_id,
            .primary = index_key,
        };
        const w = try self.pendingWrites();
        var next = op;
        // getVersion is MVCC-style (newest version <= ts), so require
        // the exact egid before treating it as this egid's slot.
        if (w.getVersion(key, egid)) |ver| {
            if (ver.ts == egid) {
                const cur: PostingOp = @enumFromInt(ver.value[0]);
                std.debug.assert(cur != op); // alternation, see above
                // Same op stays itself (idempotent — must not
                // annihilate even though the assert says it can't
                // happen); noop re-opens; opposite ops cancel.
                next = if (cur == op or cur == .noop) op else .noop;
            }
        }
        try w.putItem(.{ .key = key, .versions = .{ .one = .{
            .ts = egid,
            .meta = posting_meta,
            .value = &.{@intFromEnum(next)},
        } } });
    }

    /// Read-your-own-writes for index reads: apply this txn's pending
    /// posting ops for one index key onto a bitmap of committed egids
    /// (which the caller owns — getBitmap results are).
    pub fn overlayPostings(
        self: *Txn,
        table_id: u32,
        index_key: String,
        bm: *zdb.zroar.Bitmap,
    ) !void {
        const w = self.writes orelse return;
        var it = try w.iterator(self.allocator);
        defer it.deinit();
        const probe = Key{
            .namespace = self.ns,
            .table_id = table_id,
            .primary = index_key,
        };
        // The seek lands on the key's item, or on the next key when it
        // has none. A version's ts is the egid here.
        try it.seekRange(probe, Key.inf);
        const item = it.itemRef() orelse return;
        if (item.key.order(probe) != .eq) return;
        for (item.versions.slice()) |ver| {
            switch (@as(PostingOp, @enumFromInt(ver.value[0]))) {
                .add => _ = try bm.set(ver.ts),
                .remove => _ = bm.remove(ver.ts),
                .noop => {},
            }
        }
    }

    /// Read-your-own-writes for an index range: this txn's pending
    /// posting ops on every key in [lo, hi) of `table_id`, applied to
    /// `bm`, the union of the committed postings there. Removes before
    /// adds: a row this txn moved between two keys of the range is
    /// removed under its committed key and added under its new one,
    /// and only that order leaves it present. The ops are the exact
    /// diff against committed state (putPosting annihilates pairs), so
    /// the range needs no per-key fold.
    pub fn overlayPostingsInRange(
        self: *Txn,
        table_id: u32,
        lo: String,
        hi: String,
        bm: *zdb.zroar.Bitmap,
    ) !void {
        const w = self.writes orelse return;
        var it = try w.iterator(self.allocator);
        defer it.deinit();
        try it.seekRange(
            .{ .namespace = self.ns, .table_id = table_id, .primary = lo },
            .{ .namespace = self.ns, .table_id = table_id, .primary = hi },
        );
        var adds = try zdb.zroar.Bitmap.init(self.allocator);
        defer adds.deinit();
        while (it.itemRef()) |item| : (try it.next()) {
            // A version's ts is the egid here.
            for (item.versions.slice()) |ver| {
                switch (@as(PostingOp, @enumFromInt(ver.value[0]))) {
                    .add => _ = try adds.set(ver.ts),
                    .remove => _ = bm.remove(ver.ts),
                    .noop => {},
                }
            }
        }
        try bm.orInPlace(&adds);
    }

    /// The rows `keys` name in `table_id` as this txn sees them, in one
    /// batch (Forest.multiGet): the txn's own pending writes first — a
    /// pending tombstone means absent with NO forest fallback (that
    /// would resurrect the shadowed version) — else the forest at
    /// read_ts. Callers read at read_ts, never latest: after a lock's
    /// bump check, read_ts already covers the newest committed version
    /// of any key this txn writes. Pending entries sit at-or-below
    /// read_ts, so the batch sees them. Scans get the same overlay by
    /// merging `writes` in as a source. The result borrows `keys` and
    /// holds its pins until `deinit`; `item(key)` is null for an
    /// absent row.
    ///
    /// Every key is recorded in `reads` — absent keys included (their
    /// absence shaped results, and a later commit of one is a conflict
    /// like any other). Reads of keys this txn write-locks are
    /// recorded too: their floor is frozen at-or-below read_ts by our
    /// own lock, so revalidation can never falsely abort on them.
    pub fn multiGet(
        self: *Txn,
        table_id: u32,
        keys: []const String,
    ) !Forest.MultiGet {
        for (keys) |key| try self.recordRead(.{
            .namespace = self.ns,
            .table_id = table_id,
            .primary = key,
        });
        return self.forest.multiGet(self.allocator, .{
            .namespace = self.ns,
            .table_id = table_id,
        }, keys, .{
            .read_ts = self.read_ts,
            .pending = self.writes,
        });
    }

    /// The pending write set as wire kvs in key order — the commit
    /// payload. Row keys/values point into the txn arena (valid until
    /// deinit); posting deltas and the array itself come from `a`. A
    /// key re-put across a read_ts bump leaves versions at both
    /// stamps in the skiplist; an item's versions are newest first, so
    /// its newest is exactly the txn's final state. Posting items
    /// (meta roaring) are the exception: their versions are one op
    /// per egid, folded into a single RoaringDelta kv.
    pub fn wireKvs(self: *Txn, a: std.mem.Allocator) ![]zen.KeyValueStruct {
        var kvs: std.ArrayList(zen.KeyValueStruct) = .empty;
        const w = self.writes orelse return kvs.toOwnedSlice(a);
        var it = try w.iterator(self.allocator);
        defer it.deinit();
        while (it.itemRef()) |item| : (try it.next()) {
            const ver = item.newest();
            if (ver.meta.isRoaring()) {
                try flattenPostings(a, &kvs, item);
                continue;
            }
            try kvs.append(a, .{
                .key = try item.key.marshal(a),
                .value = ver.value,
                .meta = ver.meta.toByte(),
            });
        }
        return kvs.toOwnedSlice(a);
    }

    /// Fold one index key's pending posting ops (one per egid, egids
    /// descending) into a single RoaringDelta wire kv — or nothing,
    /// when every op annihilated to noop.
    fn flattenPostings(
        a: std.mem.Allocator,
        kvs: *std.ArrayList(zen.KeyValueStruct),
        item: Item,
    ) !void {
        var adds: std.ArrayList(u64) = .empty;
        var removes: std.ArrayList(u64) = .empty;
        for (item.versions.slice()) |ver| {
            // A version's ts is being used as a proxy for egid.
            switch (@as(PostingOp, @enumFromInt(ver.value[0]))) {
                .add => try adds.append(a, ver.ts),
                .remove => try removes.append(a, ver.ts),
                .noop => {},
            }
        }
        const delta = zen.RoaringDeltaStruct{
            .adds = adds.items,
            .removes = removes.items,
        };
        if (delta.adds.len == 0 and delta.removes.len == 0) return;

        // The run yields egids descending; the delta wants ascending.
        std.mem.reverse(u64, delta.adds);
        std.mem.reverse(u64, delta.removes);

        var buf: std.ArrayList(u8) = .empty;
        _ = try delta.marshalTo(a, &buf);
        // The write-side gate. One op slot per (key, egid) makes the
        // lists disjoint by construction; a failure here is a bug,
        // and erroring the commit beats proposing a corrupt posting.
        try zdb.validate.validateDelta(buf.items);

        try kvs.append(a, .{
            .key = try item.key.marshal(a),
            .value = buf.items,
            .meta = posting_meta.toByte(),
        });
    }
};
