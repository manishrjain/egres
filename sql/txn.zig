// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Transaction context for DML. Every statement runs inside a Txn —
//! implicit (one statement, auto-commit) today; interactive
//! (BEGIN..COMMIT) changes only its lifetime. Pending writes live in a
//! private skiplist — the memtable structure — holding full marshaled
//! keys at kv.pending_ts, above every committed version. Reads merge
//! the skiplist in as the first source, so a pending row shadows the
//! committed one and a pending posting delta folds over the committed
//! chain; commit walks it in key order into the flat wire kv list.
//! Nothing touches shared storage before commit; rollback just drops
//! the skiplist.

const std = @import("std");
const utils = @import("utils");
const fiber = @import("zfiber");
const zdb = @import("zdb");

const zen = zdb.buffers;
const zroar = zdb.zroar;
const Item = zdb.kv.Item;
const Key = zdb.kv.Key;
const Forest = zdb.forest.Forest;
const Meta = zdb.kv.Meta;
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
    fn aboveReadTs(self: ReadSet, db: *Database, read_ts: u64) bool {
        if (self.ran_scan) return true;
        return db.oracle.maxCommitTs(db.ns, self.reads.items) > read_ts;
    }
};

/// A table lock this txn holds, released at txn end.
const HeldTable = struct {
    mutex: *RWMutex,
    mode: fiber.SemaphoreMap.Mode,
};

/// One SAVEPOINT: its name, the write set as it stood when it was set,
/// and how many row locks were held then.
pub const Savepoint = struct {
    name: String, // owned by the txn allocator
    writes: ?Writes,
    held_rows_idx: usize,

    /// A write set as items in one arena, the form the skiplist's
    /// iterator yields and `putItem` takes back. A copy of the bytes
    /// only; the skiplist is rebuilt if ROLLBACK TO reaches for it.
    pub const Writes = struct {
        arena: std.heap.ArenaAllocator,
        items: []const Item,
    };
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
    /// A statement in this txn errored: only COMMIT/ROLLBACK — both of
    /// which roll back — and ROLLBACK TO a savepoint are accepted until
    /// the client ends the block or rewinds it. Without a savepoint to
    /// rewind to the locks were released at the error (Postgres aborts
    /// and releases at error time); with one they are held, since the
    /// pending writes they guard may yet commit.
    failed: bool = false,
    /// The failure left nothing a savepoint could return to: a lock
    /// error (a deadlock victim must drop its locks; a conflict read
    /// past the snapshot), a stale snapshot, a term change, an internal
    /// failure. Locks are released at once and ROLLBACK TO is refused.
    doomed: bool = false,
    /// The savepoints in effect, oldest first. ROLLBACK TO swaps the
    /// saved write set back in and releases the row locks taken since:
    /// a lock is taken before the write it guards, so those guarded
    /// only writes now discarded. Read tracking and allocated egids are
    /// not rewound, which is safe and merely revalidates more than
    /// Postgres would.
    savepoints: std.ArrayList(Savepoint) = .empty,
    /// The snapshot every read in this txn sees. Starts at the initial
    /// read_ts and only moves FORWARD, via the bump in `lockRows`: locking
    /// a key another txn committed above our snapshot advances read_ts to
    /// that commit — after revalidating every prior read (see bump
    /// notes there) and forcing the current statement to restart at the
    /// new snapshot. Pending entries carry kv.pending_ts, above every
    /// committed version and visible to the txn's own reads at any
    /// snapshot. The commit timestamp is raft's, not ours — apply
    /// rewrites every wire key's ts to the commit entry's HLC.
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
    /// Pending secondary-index posting ops, per index key: the egids
    /// this txn adds to and removes from the committed posting, as
    /// bitmaps, so an op is O(1) and an opposite op on one egid
    /// cancels. Keyed by the marshaled index Key. A dirty key is
    /// written to `writes` as one RoaringDelta before any read merges
    /// the write set (materializePostings).
    postings: std.StringHashMapUnmanaged(Posting) = .empty,

    pub const Posting = struct {
        adds: zroar.Bitmap,
        removes: zroar.Bitmap,
        dirty: bool = true,

        fn deinit(self: *Posting) void {
            self.adds.deinit();
            self.removes.deinit();
        }
    };

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
        self.dropPostings();
        self.dropSavepointsFrom(0);
        self.savepoints.deinit(self.allocator);
    }

    fn dropPostings(self: *Txn) void {
        var it = self.postings.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            e.value_ptr.deinit();
        }
        self.postings.deinit(self.allocator);
        self.postings = .empty;
    }

    /// SAVEPOINT `name`: remember the write set as it stands. A name
    /// may repeat; RELEASE and ROLLBACK TO address the newest.
    pub fn savepoint(self: *Txn, name: String) !void {
        // The write set holds every posting, so the snapshot does too.
        try self.materializePostings();
        const copy = if (self.writes) |w|
            try snapshotWrites(self.allocator, w)
        else
            null;
        errdefer if (copy) |c| c.arena.deinit();

        const owned = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned);

        try self.savepoints.append(self.allocator, .{
            .name = owned,
            .writes = copy,
            .held_rows_idx = self.held_rows.items.len,
        });
    }

    /// RELEASE SAVEPOINT `name`: forget it and every savepoint set after
    /// it; the writes since stay. false = no such savepoint.
    pub fn releaseSavepoint(self: *Txn, name: String) bool {
        const at = self.findSavepoint(name) orelse return false;
        self.dropSavepointsFrom(at);
        return true;
    }

    /// ROLLBACK TO SAVEPOINT `name`: the write set is again what it was
    /// at the savepoint, the row locks taken since are released (they
    /// are a suffix of `held_rows`, which is in acquisition order), the
    /// savepoints set after it are gone, `name` itself stays (as in
    /// Postgres), and a failed block is live again. false = no such
    /// savepoint.
    pub fn rollbackToSavepoint(self: *Txn, name: String) !bool {
        const at = self.findSavepoint(name) orelse return false;
        self.dropSavepointsFrom(at + 1);
        const sp = self.savepoints.items[at];

        const restored = if (sp.writes) |w|
            try restoreWrites(self.allocator, w)
        else
            null;

        if (self.writes) |w| w.release();
        self.writes = restored;
        // The cache is ahead of the restored set: a key reloads from it
        // on its next op (postingFor).
        self.dropPostings();

        const since = self.held_rows.items[sp.held_rows_idx..];
        self.db.row_locks.releaseMany(since, self.id);
        self.held_rows.shrinkRetainingCapacity(sp.held_rows_idx);
        self.failed = false;
        return true;
    }

    fn findSavepoint(self: *const Txn, name: String) ?usize {
        var i = self.savepoints.items.len;
        while (i > 0) {
            i -= 1;
            if (std.mem.eql(u8, self.savepoints.items[i].name, name)) return i;
        }
        return null;
    }

    fn dropSavepointsFrom(self: *Txn, at: usize) void {
        for (self.savepoints.items[at..]) |sp| {
            self.allocator.free(sp.name);
            if (sp.writes) |w| w.arena.deinit();
        }
        self.savepoints.shrinkRetainingCapacity(at);
    }

    /// Every item of `w` copied into one arena: an iterator's item
    /// borrows the skiplist's memory, which later writes replace.
    fn snapshotWrites(
        allocator: std.mem.Allocator,
        w: *zdb.skl.SkipList,
    ) !Savepoint.Writes {
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const arloc = arena.allocator();

        var items: std.ArrayList(Item) = .empty;
        var it = try w.iterator(allocator);
        defer it.deinit();

        while (it.itemRef()) |item| : (try it.next())
            try items.append(arloc, try item.dupe(arloc));
        return .{ .arena = arena, .items = items.items };
    }

    /// A skiplist holding `writes` again; `putItem` copies each item
    /// into it, so the savepoint keeps its own.
    fn restoreWrites(
        allocator: std.mem.Allocator,
        writes: Savepoint.Writes,
    ) !*zdb.skl.SkipList {
        const list = try zdb.skl.SkipList.initCapacity(allocator, 0);
        errdefer list.release();

        for (writes.items) |item| try list.putItem(item);
        return list;
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
        const newest = self.db.oracle.maxCommitTs(self.db.ns, hashes);
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
        if (self.prev.aboveReadTs(self.db, self.read_ts))
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
                .ts = zdb.kv.pending_ts,
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
    /// egid under an index key.
    pub const PostingOp = enum(u8) { add, remove };

    /// Meta for pending posting entries: a RoaringDelta, the shape a
    /// committed posting delta has, so a read folds it over the
    /// committed chain like any other, and wireKvs tells posting items
    /// from row versions by it (row writes are never roaring).
    const posting_meta: Meta = .{ .kind = .delta, .encoding = .roaring };

    /// Record `op` for (index key, egid) — no locks: posting deltas
    /// commute, and the ROW's lock already serializes add/remove of
    /// one egid (docs/design/transactions.md, lock classes).
    ///
    /// Cancellation, not last-write-wins: ops for one (index_key,
    /// egid) strictly alternate within a txn — a remove is only
    /// emitted when the pending-aware row currently maps to this
    /// index key, an add only when it doesn't, and egids are never
    /// reused after a delete. An opposite op therefore takes the
    /// pending one out (the pair nets to "no change vs committed
    /// state"), and a later op puts it back. This keeps the delta the
    /// EXACT diff: UPDATE 3→5→3 emits nothing at all — no spurious
    /// record, and no spurious oracle bump for concurrent readers of
    /// an index key this txn net-changed nothing on.
    pub fn putPosting(
        self: *Txn,
        table_id: u32,
        index_key: String,
        egid: u64,
        op: PostingOp,
    ) !void {
        // egid 0 never exists (allocation starts at 1).
        std.debug.assert(egid != 0);
        const posting = try self.postingFor(table_id, index_key);
        switch (op) {
            .add => if (!posting.removes.remove(egid)) {
                _ = try posting.adds.set(egid);
            },
            .remove => if (!posting.adds.remove(egid)) {
                _ = try posting.removes.set(egid);
            },
        }
        posting.dirty = true;
    }

    /// The pending ops under one index key, created on first touch:
    /// from the delta `writes` already holds for it when there is one
    /// (a ROLLBACK TO rebuilt the write set and dropped the cache),
    /// else empty.
    fn postingFor(self: *Txn, table_id: u32, index_key: String) !*Posting {
        const key = Key{
            .namespace = self.ns,
            .table_id = table_id,
            .primary = index_key,
        };
        const marshaled = try key.marshal(self.allocator);
        const gop = try self.postings.getOrPut(self.allocator, marshaled);
        if (gop.found_existing) {
            self.allocator.free(marshaled);
            return gop.value_ptr;
        }
        errdefer {
            self.postings.removeByPtr(gop.key_ptr);
            self.allocator.free(marshaled);
        }

        gop.value_ptr.* = .{
            .adds = try zroar.Bitmap.init(self.allocator),
            .removes = try zroar.Bitmap.init(self.allocator),
            .dirty = false,
        };
        errdefer gop.value_ptr.deinit();

        // Build it from what's already there in self.writes.
        const w = self.writes orelse return gop.value_ptr;
        const ver = w.getVersion(key, zdb.kv.pending_ts) orelse
            return gop.value_ptr;
        const r = try zen.RoaringDeltaReader.init(ver.value, 0);
        for (0..r.addsLen()) |i| {
            _ = try gop.value_ptr.adds.set(try r.addsAt(@intCast(i)));
        }
        for (0..r.removesLen()) |i| {
            _ = try gop.value_ptr.removes.set(try r.removesAt(@intCast(i)));
        }
        return gop.value_ptr;
    }

    /// Every dirty posting written to `writes` as its RoaringDelta,
    /// one version per key at pending_ts, replaced in place: the write
    /// set then holds everything a read merges and the wire carries.
    /// A key whose ops all cancelled is an empty delta, a no-op to a
    /// read; wireKvs leaves it out.
    ///
    /// It's OK to do this without a lock, because a call to this func won't be
    /// run concurrently.
    fn materializePostings(self: *Txn) !void {
        var it = self.postings.iterator();
        while (it.next()) |e| {
            const posting = e.value_ptr;
            if (!posting.dirty) continue;

            var buf: std.ArrayList(u8) = .empty;
            defer buf.deinit(self.allocator);
            try encodeDelta(self.allocator, posting, &buf);

            const w = try self.pendingWrites();
            try w.putItem(.{
                .key = Key.init(e.key_ptr.*),
                .versions = .{ .one = .{
                    .ts = zdb.kv.pending_ts,
                    .meta = posting_meta,
                    .value = buf.items,
                } },
            });
            posting.dirty = false;
        }
    }

    /// `posting` as RoaringDelta bytes, validated: one op slot per
    /// (key, egid) makes the lists disjoint by construction, so a
    /// failure here is a bug, and erroring beats a corrupt posting.
    fn encodeDelta(
        a: std.mem.Allocator,
        posting: *const Posting,
        out: *std.ArrayList(u8),
    ) !void {
        const adds = try posting.adds.toArray(a);
        defer a.free(adds);
        const removes = try posting.removes.toArray(a);
        defer a.free(removes);
        const delta = zen.RoaringDeltaStruct{
            .adds = adds,
            .removes = removes,
        };
        _ = try delta.marshalTo(a, out);
        try zdb.validate.validateDelta(out.items);
    }

    /// The write set as a read merges it (ScanOptions.pending), every
    /// pending posting materialized first. null = nothing written.
    pub fn materializedWrites(self: *Txn) !?*zdb.skl.SkipList {
        try self.materializePostings();
        return self.writes;
    }

    /// The rows `keys` name in `table_id` as this txn sees them, in one
    /// batch (Forest.multiGet): the txn's own pending writes first — a
    /// pending tombstone means absent with NO forest fallback (that
    /// would resurrect the shadowed version) — else the forest at
    /// read_ts. Callers read at read_ts, never latest: after a lock's
    /// bump check, read_ts already covers the newest committed version
    /// of any key this txn writes. Scans get the same overlay by
    /// merging the write set in as a source. The result borrows `keys`
    /// and holds its pins until `deinit`; `item(key)` is null for an
    /// absent row, and `bitmap(key)` folds a posting, pending delta
    /// included.
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
            .pending = try self.materializedWrites(),
        });
    }

    /// The pending write set as wire kvs in key order — the commit
    /// payload. Keys and values point into the txn arena (valid until
    /// deinit); the array itself comes from `a`. A key holds one
    /// pending version (same-stamp re-puts replace), the txn's final
    /// state of it. A posting whose ops all cancelled is an empty
    /// delta and is left out: nothing to record, and no oracle bump
    /// for readers of a key this txn net-changed nothing on.
    pub fn wireKvs(self: *Txn, a: std.mem.Allocator) ![]zen.KeyValueStruct {
        var kvs: std.ArrayList(zen.KeyValueStruct) = .empty;
        const w = (try self.materializedWrites()) orelse
            return kvs.toOwnedSlice(a);

        var it = try w.iterator(self.allocator);
        defer it.deinit();
        while (it.itemRef()) |item| : (try it.next()) {
            const ver = item.newest();
            if (ver.meta.isRoaring() and try emptyDelta(ver.value)) continue;
            try kvs.append(a, .{
                .key = try item.key.marshal(a),
                .value = ver.value,
                .meta = ver.meta.toByte(),
            });
        }
        return kvs.toOwnedSlice(a);
    }

    fn emptyDelta(value: String) !bool {
        const r = try zen.RoaringDeltaReader.init(value, 0);
        return r.addsLen() == 0 and r.removesLen() == 0;
    }
};
