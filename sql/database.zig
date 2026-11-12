// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! One SQL database's per-namespace state: the row lock table, the
//! table locks and the compiled-schema cache, plus the server's
//! commit oracle, which every database shares. A Txn holds a pointer
//! to its Database and takes what it needs from it; the Server owns
//! the map of Databases.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const zdb = @import("zdb");

const _key = @import("key.zig");
const _table = @import("table.zig");
const Oracle = @import("oracle.zig").Oracle;
const log = std.log.scoped(.sql);

/// A table lock: shared by every DML statement for its
/// duration (the table must keep existing), exclusive by DDL (DROP
/// TABLE and CREATE/DROP INDEX on their table; DROP DATABASE on every
/// table in turn, one at a time). Writer preference, so a pending DDL
/// parks new DML behind it. There is no namespace-level lock: DROP
/// DATABASE sets `Database.dropped` and then cycles through the table
/// mutexes, and a statement checks the flag after taking its table
/// (see Handler.dmlTxn).
///
/// Not in the SemaphoreMap: the map's per-key holder list made every
/// statement's table lock an O(holders) scan under one spinlock, a third
/// of all CPU at 64 clients. The price is that a cycle through an
/// table lock is not detected: txn A holds t1 and row r and wants t2, DDL
/// holds t2 exclusive and waits for txn B's shared hold, and B wants
/// row r. A parks here, B parks in the SemaphoreMap, the DDL parks on
/// B. Known and open; the fix is a timed exclusive take for DDL,
/// which holds nothing else and can safely withdraw.
pub const RWMutex = fiber.RWMutex(.tracked);

/// One database's transaction machinery: the lock table, and its view
/// of the commit oracle. A SQL txn is connection-bound to one
/// database, so wait-for graphs are naturally namespace-local —
/// per-ns lock tables keep tenants off each other's mutexes and give
/// DROP DATABASE one object to tear down (teardown itself is
/// deferred). The oracle is the server's: its maps are too large to
/// have one per database (see oracle.zig).
pub const Database = struct {
    /// This database's namespace id (implied by every key it owns).
    ns: u64,
    allocator: std.mem.Allocator,
    /// DROP DATABASE is landing or landed. Set BEFORE the drop takes
    /// any table lock, read by every statement AFTER it takes its
    /// own (dmlTxn). The table lock's spinlock orders the two, not
    /// the flag's memory order (release store, acquire load): either
    /// the statement's shared take precedes the drop's check of that
    /// table and the drop waits for the txn, or the drop's check
    /// precedes it and the statement's acquire of the spinlock makes
    /// the flag store visible, so it answers 3D000 instead of
    /// touching dropped trees. A table first touched after the
    /// drop's snapshot of `table_locks` gets the same order through
    /// that map's lock. (The manifest is the authority for NEW
    /// statements — sessionNamespace; this flag covers the ones
    /// already past that gate.)
    dropped: std.atomic.Value(bool) = .init(false),
    /// Row locks, with deadlock detection.
    row_locks: fiber.SemaphoreMap,
    /// Table locks, one RWMutex per table id, created on first use
    /// by `tableLock` and never freed (table ids are
    /// never reused; a dropped table's entry is a few bytes).
    table_locks: fiber.SafeHashMap(u32, *RWMutex),
    /// The server's, shared by every database; asked with `ns`.
    oracle: *Oracle,
    /// Per-table cache: compiled schema + egid counter, keyed by table_id
    /// (the namespace is this database's). Guards the map; each table's
    /// `next_egid` is atomic and needs only a shared hold to reach it.
    /// Heap entries so held pointers survive map growth. Readers share:
    /// every statement looks its table up here, egid hand-out and the
    /// apply fiber included (as an exclusive hand-off mutex this ran at
    /// ~94k hand-offs/s under a 64-client pgbench); first load and the
    /// DDL swap take it exclusively, only around the egid seed and the
    /// publish (see updateEgidAndPut) — the schema load itself runs
    /// with no lock held.
    tables_mu: fiber.RWMutex(.tracked) = .{},
    tables: std.AutoHashMap(u32, *_table.Table),

    pub fn init(
        allocator: std.mem.Allocator,
        ns: u64,
        oracle: *Oracle,
    ) !Database {
        return .{
            .ns = ns,
            .allocator = allocator,
            .row_locks = try fiber.SemaphoreMap.init(allocator),
            .table_locks = fiber.SafeHashMap(u32, *RWMutex).init(allocator),
            .oracle = oracle,
            .tables = std.AutoHashMap(u32, *_table.Table).init(allocator),
        };
    }

    pub fn deinit(self: *Database) void {
        var it = self.tables.valueIterator();
        while (it.next()) |t| self.destroyTable(t.*);
        self.tables.deinit();
        self.tables_mu.deinit();
        self.row_locks.deinit();
        var im = self.table_locks.valueIterator();
        while (im.next()) |m| {
            m.*.deinit();
            self.allocator.destroy(m.*);
        }
        self.table_locks.deinit();
    }

    /// The lock of `table_id`, created on first use. Two
    /// first-touchers race to publish; the loser destroys its copy.
    pub fn tableLock(self: *Database, table_id: u32) !*RWMutex {
        if (self.table_locks.get(table_id)) |m| return m;
        const fresh = try self.allocator.create(RWMutex);
        fresh.* = .{};
        const stored = self.table_locks.getOrPut(table_id, fresh) catch |err| {
            self.allocator.destroy(fresh);
            return err;
        };
        if (stored != fresh) {
            fresh.deinit();
            self.allocator.destroy(fresh);
        }
        return stored;
    }

    /// Every table's lock, for DROP DATABASE. A table first
    /// touched after this snapshot is not in it; the `dropped` flag,
    /// set before the snapshot and checked by every statement after
    /// its take, covers those.
    pub fn allTableLocks(self: *Database, arloc: std.mem.Allocator) ![]*RWMutex {
        var out: std.ArrayList(*RWMutex) = .empty;
        var it = self.table_locks.iterator();
        defer it.deinit();
        while (it.next()) |e| try out.append(arloc, e.value_ptr.*);
        return out.items;
    }

    /// A cached Table borrowed under the cache's shared lock: the
    /// entry cannot be destroyed while the hold lasts (forgetTable
    /// takes the lock exclusive), so the borrower needs no table lock
    /// and no txn. Keep it short, copy out what you need, and never
    /// write to a socket or take `tables_mu` while holding it.
    pub const TableHold = struct {
        db: *Database,
        tbl: *_table.Table,

        pub fn release(self: TableHold) void {
            self.db.tables_mu.unlockShared();
        }
    };

    /// Borrow `table_id`'s cached Table, or null on a miss (the
    /// caller loads it the way a statement does).
    pub fn holdTable(self: *Database, table_id: u32) ?TableHold {
        self.tables_mu.lockShared(@src());
        const tbl = self.tables.get(table_id) orelse {
            self.tables_mu.unlockShared();
            return null;
        };
        return .{ .db = self, .tbl = tbl };
    }

    /// The cached Table for `table_id`, or null on a miss. The pointer
    /// outlives the lookup only under the caller's table lock: DML
    /// holds it shared from dmlTxn to txn end, and forgetTable runs
    /// under the exclusive one.
    pub fn cachedTable(self: *Database, table_id: u32) ?*_table.Table {
        self.tables_mu.lockShared(@src());
        defer self.tables_mu.unlockShared();
        return self.tables.get(table_id);
    }

    /// Publish a freshly loaded Table: seed `next_egid` past the
    /// table's max applied row id and put it in the cache, under one
    /// exclusive hold. Returns the cached entry — `fresh` if this call
    /// published it, or the one another session published first (the
    /// caller then destroys `fresh`, the wasted copy).
    ///
    /// The seed and the publish are one step on purpose: the apply
    /// fiber skips uncached tables, so a row applied between the two
    /// would be missing from the counter and its egid handed out
    /// again. maxKey takes no lock and the memtable keeps taking
    /// writes under it, so its answer is a lower bound, not a
    /// snapshot. That is enough because of the apply order: one fiber
    /// does forest.put and then the applyCommitted observer call for
    /// the same rows, and the observer takes `tables_mu` shared. A
    /// row applied before this hold began was seen by the walk; a
    /// row applied during it blocks at the observer until the
    /// publish, then raises the counter itself. Either way the
    /// counter ends past every committed row.
    pub fn openAndCacheTable(
        self: *Database,
        forest: *zdb.forest.Forest,
        table_id: u32,
        fresh: *_table.Table,
    ) !*_table.Table {
        self.tables_mu.lock(@src());
        defer self.tables_mu.unlock();
        if (self.tables.get(table_id)) |t| return t;

        // Covers snapshot SSTs the apply observer never saw. Empty
        // table → start at 1.
        if (try forest.maxKey(self.allocator, .{
            .namespace = self.ns,
            .table_id = table_id,
        })) |mk| {
            defer mk.free(self.allocator);
            fresh.raiseNextEgid((_key.decodeEgid(mk.primary) orelse 0) + 1);
        } else {
            fresh.raiseNextEgid(1);
        }
        try self.tables.put(table_id, fresh);
        return fresh;
    }

    /// Drop a table's cache entry: DROP TABLE, and every DDL that
    /// changes the schema (CREATE/DROP INDEX). The next openTable
    /// loads fresh from the catalog and reseeds `next_egid` from
    /// maxKey. The caller holds the table lock EXCLUSIVELY, which is
    /// what makes destroying the entry safe: DML holds its Table only
    /// under the shared table lock (taken before openTable), and
    /// applyCommitted and holdTable touch cached tables only under
    /// tables_mu. The counter needs no carry-over: under the
    /// exclusive table lock no INSERT is in flight, and a committed
    /// row is applied before its txn releases the shared table lock,
    /// so the reseed already covers every handed-out id that became
    /// a row.
    pub fn forgetTable(self: *Database, table_id: u32) void {
        self.tables_mu.lock(@src());
        defer self.tables_mu.unlock();
        if (self.tables.fetchRemove(table_id)) |old| self.destroyTable(old.value);
    }

    pub fn destroyTable(self: *Database, t: *_table.Table) void {
        t.deinit();
        self.allocator.destroy(t);
    }

    /// Apply-fiber update for a run of committed items sharing this
    /// database's namespace: advance each cached table's egid counter
    /// past every committed row id, and record every key in the oracle at
    /// its commit ts (apply already rewrote it to the entry's HLC).
    /// Uncached tables are skipped — first cache seeds egid from
    /// forest.maxKey, covering rows applied before caching.
    pub fn applyCommitted(self: *Database, items: []const zdb.kv.Item) void {
        {
            self.tables_mu.lockShared(@src());
            defer self.tables_mu.unlockShared();
            // Items in a run share this namespace; only table_id varies,
            // and consecutive items share a table in practice.
            var tbl: ?*_table.Table = null;
            var table_id: u32 = 0;
            for (items) |item| {
                if (tbl == null or item.key.table_id != table_id) {
                    table_id = item.key.table_id;
                    tbl = self.tables.get(table_id);
                }
                const cached = tbl orelse continue;
                if (_key.decodeEgid(item.key.primary)) |egid|
                    cached.raiseNextEgid(egid + 1);
            }
        }
        self.oracle.updateItems(self.ns, items) catch |err|
            log.err("oracle ns={d}: {s}", .{ self.ns, @errorName(err) });
    }
};
