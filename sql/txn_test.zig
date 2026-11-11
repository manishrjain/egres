// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const fiber = @import("zfiber");
const sql = @import("sql");
const String = sql.value.String;
const utils = @import("utils");
const zdb = @import("zdb");
const createTree = @import("../zdb/testing.zig").createTree;
const flags = @import("flags");

const Txn = sql.txn.Txn;
const Forest = zdb.forest.Forest;
const Config = zdb.config.Config;
const Key = zdb.kv.Key;
const Meta = zdb.kv.Meta;

const ns: u64 = 16;
const tid: u32 = 16;

/// One row through `lockRows`, the way a statement locks its set.
fn lockOne(tx: *Txn, primary: String) Txn.RowLockError!void {
    return tx.lockRows(&.{tx.rowHash(tid, primary)});
}
const read_ts: u64 = 500;

/// A Forest in a tmpdir with one tree for (ns, tid) and the given
/// committed rows, plus the lock map — everything a Txn needs.
const Fixture = struct {
    tmp: zdb.vfs.TmpDir,
    forest: *Forest,
    /// The forest reads its options through this; freed after it.
    live: *flags.Live(Config),
    /// The forest's SSTs; heap-held so its address survives the move out
    /// of setup. Freed after the forest.
    store: *zdb.vfs.Store,
    db: sql.database.Database,

    fn setup(rows: []const zdb.kv.Item) !Fixture {
        var tmp = try zdb.vfs.TmpDir.init(testing.allocator);
        errdefer tmp.deinit();
        const store = try testing.allocator.create(zdb.vfs.Store);
        errdefer testing.allocator.destroy(store);
        store.* = try zdb.vfs.Store.initFile(
            testing.allocator,
            tmp.path,
            zdb.vfs.g(),
        );
        errdefer store.deinit();

        const f_live = try flags.Live(Config).create(
            testing.allocator,
            .{},
        );
        errdefer f_live.destroy();
        const f = try Forest.open(testing.allocator, f_live, store);
        errdefer f.close();
        _ = try createTree(f, .{
            .namespace = ns,
            .table_id = tid,
        }, .{}, .posting_index);
        try f.put(rows);

        return .{
            .tmp = tmp,
            .forest = f,
            .live = f_live,
            .store = store,
            .db = try sql.database.Database.init(testing.allocator, ns),
        };
    }

    fn teardown(self: *Fixture) void {
        self.db.deinit();
        self.forest.close();
        self.live.destroy();
        self.store.deinit();
        testing.allocator.destroy(self.store);
        self.tmp.deinit();
    }

    fn txn(self: *Fixture) Txn {
        return self.txnAs(1);
    }

    fn txnAs(self: *Fixture, id: u64) Txn {
        return Txn.init(
            testing.allocator,
            id,
            read_ts,
            1,
            &self.db,
            self.forest,
        );
    }

    /// Record a commit of `primary` at `ts` in the oracle, the way apply
    /// does — a one-item run through updateItems.
    fn commit(self: *Fixture, primary: String, ts: u64) !void {
        try self.db.oracle.updateItems(&.{.{
            .key = rowKey(primary),
            .versions = .{ .one = .{ .ts = ts, .meta = .{}, .value = "" } },
        }});
    }
};

fn rowKey(primary: String) Key {
    return .{ .namespace = ns, .table_id = tid, .primary = primary };
}

/// A one-key multiGet of `tid`: the row, duped with the test
/// allocator (free with `Item.free`), or null when absent.
fn lookupOne(tx: *Txn, primary: String) !?zdb.kv.Item {
    var res = try tx.multiGet(tid, &.{primary});
    defer res.deinit();
    const found = res.item(primary) orelse return null;
    return try found.dupe(testing.allocator);
}

test "txn: multiGet reads pending first, then the forest at read_ts" {
    var fx = try Fixture.setup(&.{.{
        .key = rowKey("k1"),
        .versions = .{ .one = .{
            .ts = 100,
            .meta = .{},
            .value = "old",
        } },
    }});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // Nothing pending: the committed row.
    {
        const found = (try lookupOne(&tx, "k1")) orelse
            return error.TestExpectedRow;
        defer found.free(testing.allocator);
        try testing.expectEqualSlices(u8, "old", found.newest().value);
        try testing.expectEqual(100, found.newest().ts);
    }
    // Absent key: null.
    try testing.expectEqual(null, try lookupOne(&tx, "nope"));

    // A pending write shadows the committed row; it carries the
    // pending stamp, above every committed version.
    try tx.put(tid, "k1", "new", .{});
    const found = (try lookupOne(&tx, "k1")) orelse
        return error.TestExpectedRow;
    defer found.free(testing.allocator);
    try testing.expectEqualSlices(u8, "new", found.newest().value);
    try testing.expectEqual(zdb.kv.pending_ts, found.newest().ts);
}

test "txn: pending tombstone hides the committed row, no resurrection" {
    var fx = try Fixture.setup(&.{.{
        .key = rowKey("k1"),
        .versions = .{ .one = .{
            .ts = 100,
            .meta = .{},
            .value = "old",
        } },
    }});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    try tx.put(tid, "k1", "", .{ .kind = .deleted });
    // The pending tombstone answers "absent" — the forest fallback must
    // NOT run, or the committed "old" would resurrect.
    try testing.expectEqual(null, try lookupOne(&tx, "k1"));
}

test "txn: same-key re-put is last-write-wins" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    try tx.put(tid, "k1", "v1", .{});
    try tx.put(tid, "k1", "v2", .{});

    // One version, not two: the commit payload carries the final state.
    const kvs = try tx.wireKvs(testing.allocator);
    defer {
        for (kvs) |kv| testing.allocator.free(kv.key);
        testing.allocator.free(kvs);
    }
    try testing.expectEqual(1, kvs.len);
    try testing.expectEqualSlices(u8, "v2", kvs[0].value);
    try testing.expect(!Meta.fromByte(kvs[0].meta).isDeleted());
}

test "txn: lockRows is re-entrant and deinit releases every held lock" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();

    try lockOne(&tx, "r1");
    try lockOne(&tx, "r2");
    // Re-lock of a held row: the map answers "already yours", no second
    // hold is recorded.
    try lockOne(&tx, "r1");
    try testing.expectEqual(2, tx.held_rows.items.len);
    try testing.expectEqual(2, fx.db.row_locks.entries.count());

    // Txn end releases everything — the table drains.
    tx.deinit();
    try testing.expectEqual(0, fx.db.row_locks.entries.count());
}

test "txn: fresh lock on a newer-committed row bumps and restarts" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // The row committed above our snapshot (read_ts 500).
    try fx.commit("r1", 600);
    try testing.expectError(error.Restart, lockOne(&tx, "r1"));
    // The bump happened AND the lock is held — a restart re-runs the
    // statement with both intact; the re-lock is a re-entrant no-op
    // with no second bump check.
    try testing.expectEqual(600, tx.read_ts);
    try lockOne(&tx, "r1");
    try testing.expectEqual(1, tx.held_rows.items.len);
}

test "txn: bumpTo — no-op at or below read_ts, else Restart or Conflict" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // Already covered: nothing happens.
    try tx.bumpTo(read_ts - 1);
    try tx.bumpTo(read_ts);
    try testing.expectEqual(read_ts, tx.read_ts);

    // Above the snapshot with nothing read before: Restart at the ts.
    try testing.expectError(error.Restart, tx.bumpTo(600));
    try testing.expectEqual(600, tx.read_ts);

    // A prior statement's scan makes a further bump a Conflict, and
    // read_ts stays.
    tx.recordScan();
    try tx.beginStatement();
    try testing.expectError(error.Conflict, tx.bumpTo(700));
    try testing.expectEqual(600, tx.read_ts);
}

test "txn: bump after a prior statement's scan is a conflict" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    tx.recordScan();
    try tx.beginStatement(); // the scan is now a PRIOR statement's
    try fx.commit("r1", 600);
    try testing.expectError(error.Conflict, lockOne(&tx, "r1"));
    try testing.expectEqual(read_ts, tx.read_ts); // no bump happened
}

test "txn: bump revalidates prior point reads" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // Statement 1 point-reads k1 (absent — still recorded).
    try testing.expectEqual(null, try lookupOne(&tx, "k1"));
    try tx.beginStatement();

    // k1 then commits above our snapshot; a bump for r1 must notice
    // the prior read of k1 went stale → conflict, not a silent bump.
    try fx.commit("k1", 550);
    try fx.commit("r1", 600);
    try testing.expectError(error.Conflict, lockOne(&tx, "r1"));
}

test "txn: bump with clean prior reads succeeds" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // Statement 1 point-reads k2; nothing ever commits to it.
    try testing.expectEqual(null, try lookupOne(&tx, "k2"));
    try tx.beginStatement();

    try fx.commit("r1", 600);
    try testing.expectError(error.Restart, lockOne(&tx, "r1"));
    try testing.expectEqual(600, tx.read_ts);
}

test "txn: bump revalidates a recorded posting read by its index key" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    const blue = Key{
        .namespace = ns,
        .table_id = itid,
        .primary = "blue",
    };

    // Statement 1 read the "blue" posting — the index-bitmap path
    // records the INDEX key, not a scan. An unrelated row committing
    // above the snapshot then bumps cleanly (a scan would Conflict).
    {
        var tx = fx.txn();
        defer tx.deinit();
        try tx.recordRead(blue);
        try tx.beginStatement();

        try fx.commit("r1", 600);
        try testing.expectError(error.Restart, lockOne(&tx, "r1"));
        try testing.expectEqual(600, tx.read_ts);
    }
    // Same shape, but a posting delta commits under "blue" itself —
    // the membership statement 1 read is stale at any higher snapshot,
    // so the bump must Conflict.
    {
        var tx = fx.txn();
        defer tx.deinit();
        try tx.recordRead(blue);
        try tx.beginStatement();

        try fx.db.oracle.updateItems(&.{.{
            .key = blue,
            .versions = .{ .one = .{ .ts = 550, .meta = .{}, .value = "" } },
        }});
        try fx.commit("r1", 600);
        try testing.expectError(error.Conflict, lockOne(&tx, "r1"));
    }
}

test "txn: restartStatement rewinds the statement's read tracking" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    try testing.expectEqual(null, try lookupOne(&tx, "a"));
    try tx.beginStatement();
    try testing.expectEqual(null, try lookupOne(&tx, "b"));
    try testing.expectEqual(null, try lookupOne(&tx, "c"));
    tx.recordScan();
    // 'a' settled into prev at beginStatement; 'b'/'c' are cur.
    try testing.expectEqual(1, tx.prev.reads.items.len);
    try testing.expectEqual(2, tx.cur.reads.items.len);

    tx.restartStatement();
    // Statement-local reads and scan flag are gone; the prior
    // statement's read survives.
    try testing.expectEqual(1, tx.prev.reads.items.len);
    try testing.expectEqual(0, tx.cur.reads.items.len);
    try testing.expect(!tx.cur.ran_scan);
    try testing.expect(!tx.prev.ran_scan);
}

test "txn: wireKvs dedups a key re-put across a bump" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    try tx.put(tid, "k1", "v1", .{});
    tx.read_ts = 600; // as a lockRows bump would
    try tx.put(tid, "k1", "v2", .{});

    // Two pending versions (ts 500 and 600) — the wire payload carries
    // only the txn's final state, under one timestamp-free key.
    const kvs = try tx.wireKvs(testing.allocator);
    defer {
        for (kvs) |kv| testing.allocator.free(kv.key);
        testing.allocator.free(kvs);
    }
    try testing.expectEqual(1, kvs.len);
    try testing.expectEqualSlices(u8, "v2", kvs[0].value);
    try testing.expectEqualSlices(u8, "k1", Key.init(kvs[0].key).primary);
}

test "txn: lockTable coexists with row locks and is re-entrant" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();

    // A shared table lock and a row lock in the same table live
    // apart: the table lock on its RWMutex, the row in the map.
    try tx.lockTable(tid, .shared);
    try lockOne(&tx, "r1");
    try testing.expectEqual(1, tx.held_tables.items.len);
    try testing.expectEqual(1, tx.held_rows.items.len);
    try testing.expectEqual(1, fx.db.row_locks.entries.count());
    const table_lock = try fx.db.tableLock(tid);
    try testing.expect(table_lock.isHeld(.shared));

    // Per-statement re-acquire inside one txn is a no-op; asking for
    // exclusive over a held shared is refused, never self-blocked.
    try tx.lockTable(tid, .shared);
    try testing.expectEqual(1, tx.held_tables.items.len);
    try testing.expectError(error.Upgrade, tx.lockTable(tid, .exclusive));

    // A second txn shares the table lock; an exclusive taker (DDL) would
    // park until both release, which is the mutex's own test surface.
    var tx2 = fx.txnAs(2);
    try tx2.lockTable(tid, .shared);
    tx2.deinit();
    try testing.expect(table_lock.isHeld(.shared));

    // Txn end releases the row lock and the table lock.
    tx.deinit();
    try testing.expectEqual(0, fx.db.row_locks.entries.count());
    try testing.expect(!table_lock.isHeld(.shared));
}

test "table locks: one mutex per table id, created on first use, allTableLocks lists them" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();

    // Same id, same mutex; different ids, different mutexes.
    const a = try fx.db.tableLock(tid);
    try testing.expectEqual(a, try fx.db.tableLock(tid));
    const b = try fx.db.tableLock(tid + 1);
    try testing.expect(a != b);

    // DROP DATABASE's set is every mutex created so far.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const every = try fx.db.allTableLocks(arena.allocator());
    try testing.expectEqual(2, every.len);
    var seen_a = false;
    var seen_b = false;
    for (every) |m| {
        if (m == a) seen_a = true;
        if (m == b) seen_b = true;
    }
    try testing.expect(seen_a and seen_b);

    // The pass DROP DATABASE makes: take and release each exclusive.
    // With nothing held it goes straight through, and leaves the
    // mutexes free for the next statement.
    for (every) |m| {
        m.lock(@src());
        m.unlock();
    }
    var tx = fx.txn();
    try tx.lockTable(tid, .shared);
    try testing.expect(a.isHeld(.shared));
    tx.deinit();
    try testing.expect(!a.isHeld(.shared));
}

const itid: u32 = 17; // a secondary index's table id in these tests

/// wireKvs into an arena: posting deltas allocate from `a`, so tests
/// route through one arena instead of freeing piecemeal.
fn wireKvsArena(
    arena: *std.heap.ArenaAllocator,
    tx: *Txn,
) ![]zdb.buffers.KeyValueStruct {
    return tx.wireKvs(arena.allocator());
}

test "txn: posting ops flatten to one ascending delta per index key" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    // Two index keys, ops arriving out of egid order, plus a row put
    // — the row is untouched by the posting machinery.
    try tx.put(tid, "row", "v", .{});
    try tx.putPosting(itid, "blue", 9, .add);
    try tx.putPosting(itid, "blue", 3, .add);
    try tx.putPosting(itid, "blue", 5, .remove);
    try tx.putPosting(itid, "red", 7, .add);

    const kvs = try wireKvsArena(&arena, &tx);
    try testing.expectEqual(3, kvs.len);

    // Key order: (tid "row") < (itid "blue") < (itid "red").
    try testing.expectEqualSlices(u8, "v", kvs[0].value);

    const blue = Key.init(kvs[1].key);
    try testing.expectEqualSlices(u8, "blue", blue.primary);
    try testing.expectEqual(itid, blue.table_id);
    const bm = Meta.fromByte(kvs[1].meta);
    try testing.expect(bm.isRoaring() and bm.isDelta());
    try zdb.validate.validateDelta(kvs[1].value);
    const br = try zdb.buffers.RoaringDeltaReader.init(kvs[1].value, 0);
    try testing.expectEqual(2, br.addsLen());
    try testing.expectEqual(3, try br.addsAt(0));
    try testing.expectEqual(9, try br.addsAt(1));
    try testing.expectEqual(1, br.removesLen());
    try testing.expectEqual(5, try br.removesAt(0));

    const rr = try zdb.buffers.RoaringDeltaReader.init(kvs[2].value, 0);
    try testing.expectEqual(1, rr.addsLen());
    try testing.expectEqual(0, rr.removesLen());
}

test "txn: opposite posting ops annihilate; a later op re-opens" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    // add→remove cancels (insert-then-delete of a fresh row);
    // remove→add cancels (UPDATE 3→5→3's home key); a key whose ops
    // all cancelled emits NOTHING on the wire — no spurious record,
    // no oracle touch for concurrent readers of that index key.
    try tx.putPosting(itid, "k", 1, .add);
    try tx.putPosting(itid, "k", 1, .remove);
    try tx.putPosting(itid, "j", 2, .remove);
    try tx.putPosting(itid, "j", 2, .add);
    try testing.expectEqual(0, (try wireKvsArena(&arena, &tx)).len);

    // add→remove→add nets a single add: the slot re-opens.
    try tx.putPosting(itid, "k", 1, .add);
    const kvs = try wireKvsArena(&arena, &tx);
    try testing.expectEqual(1, kvs.len);
    const r = try zdb.buffers.RoaringDeltaReader.init(kvs[0].value, 0);
    try testing.expectEqual(1, r.addsLen());
    try testing.expectEqual(1, try r.addsAt(0));
    try testing.expectEqual(0, r.removesLen());
}

test "txn: flattened postings fold through the forest into a bitmap" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    _ = try createTree(fx.forest, .{
        .namespace = ns,
        .table_id = itid,
    }, .{}, .posting_index);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    // Commit txn 1: adds {2, 4, 6} under "blue".
    {
        var tx = fx.txn();
        defer tx.deinit();
        for ([_]u64{ 2, 4, 6 }) |egid|
            try tx.putPosting(itid, "blue", egid, .add);
        try putWire(&arena, &fx, &tx, 600);
    }
    // Commit txn 2: removes 4, adds 8.
    {
        var tx = fx.txn();
        defer tx.deinit();
        try tx.putPosting(itid, "blue", 4, .remove);
        try tx.putPosting(itid, "blue", 8, .add);
        try putWire(&arena, &fx, &tx, 700);
    }

    var bm = try fx.forest.getBitmap(testing.allocator, .{
        .key = .{ .namespace = ns, .table_id = itid, .primary = "blue" },
        .read_ts = 800,
    });
    defer bm.deinit();
    try testing.expectEqual(3, bm.getCardinality());
    for ([_]u64{ 2, 6, 8 }) |egid| try testing.expect(bm.contains(egid));
    try testing.expect(!bm.contains(4));
}

/// Commit a txn's wire kvs into the forest the way apply does: stamp
/// every write with the commit HLC, then forest.put.
fn putWire(
    arena: *std.heap.ArenaAllocator,
    fx: *Fixture,
    tx: *Txn,
    commit_ts: u64,
) !void {
    const a = arena.allocator();
    const kvs = try tx.wireKvs(a);
    var items: std.ArrayList(zdb.kv.Item) = .empty;
    for (kvs) |kv| {
        try items.append(a, .{
            .key = Key.init(kv.key),
            .versions = .{ .one = .{
                .ts = commit_ts,
                .meta = Meta.fromByte(kv.meta),
                .value = kv.value,
            } },
        });
    }
    try fx.forest.put(items.items);
}

test "txn: pending posting ops fold over the committed bitmap on read" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    _ = try createTree(fx.forest, .{
        .namespace = ns,
        .table_id = itid,
    }, .{}, .posting_index);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    // Committed posting under "blue": {2, 4}; and a delta above the
    // snapshot (read_ts 500) that no read of this txn may see.
    {
        var tx = fx.txn();
        defer tx.deinit();
        try tx.putPosting(itid, "blue", 2, .add);
        try tx.putPosting(itid, "blue", 4, .add);
        try putWire(&arena, &fx, &tx, 400);
    }
    {
        var tx = fx.txn();
        defer tx.deinit();
        try tx.putPosting(itid, "blue", 6, .add);
        try putWire(&arena, &fx, &tx, 600);
    }

    var tx = fx.txn();
    defer tx.deinit();
    // Pending: add 9, remove 4, and a cancelled pair on 7 (must stay
    // invisible). "red" ops must not bleed into "blue".
    try tx.putPosting(itid, "blue", 9, .add);
    try tx.putPosting(itid, "blue", 4, .remove);
    try tx.putPosting(itid, "blue", 7, .add);
    try tx.putPosting(itid, "blue", 7, .remove);
    try tx.putPosting(itid, "red", 5, .add);

    var res = try tx.multiGet(itid, &.{"blue"});
    defer res.deinit();
    var bm = try res.bitmap(testing.allocator, "blue");
    defer bm.deinit();
    try testing.expectEqual(2, bm.getCardinality());
    try testing.expect(bm.contains(2));
    try testing.expect(bm.contains(9));
    try testing.expect(!bm.contains(4));
    try testing.expect(!bm.contains(6));
    try testing.expect(!bm.contains(7));

    // A key only the pending set knows reads as its adds.
    var red_res = try tx.multiGet(itid, &.{"red"});
    defer red_res.deinit();
    var red = try red_res.bitmap(testing.allocator, "red");
    defer red.deinit();
    try testing.expectEqual(1, red.getCardinality());
    try testing.expect(red.contains(5));
}

test "txn: posting ops survive a savepoint and rewind on ROLLBACK TO" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    _ = try createTree(fx.forest, .{
        .namespace = ns,
        .table_id = itid,
    }, .{}, .posting_index);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var tx = fx.txn();
    defer tx.deinit();
    try tx.putPosting(itid, "blue", 1, .add);
    try tx.savepoint("s");
    try tx.putPosting(itid, "blue", 2, .add);
    try tx.putPosting(itid, "blue", 1, .remove);
    try testing.expect(try tx.rollbackToSavepoint("s"));

    // Back to {add 1}; a later op on the key builds on that, not on
    // the ops rolled back.
    try tx.putPosting(itid, "blue", 3, .add);
    var res = try tx.multiGet(itid, &.{"blue"});
    defer res.deinit();
    var bm = try res.bitmap(testing.allocator, "blue");
    defer bm.deinit();
    try testing.expectEqual(2, bm.getCardinality());
    try testing.expect(bm.contains(1));
    try testing.expect(bm.contains(3));

    const kvs = try wireKvsArena(&arena, &tx);
    try testing.expectEqual(1, kvs.len);
    const r = try zdb.buffers.RoaringDeltaReader.init(kvs[0].value, 0);
    try testing.expectEqual(2, r.addsLen());
    try testing.expectEqual(1, try r.addsAt(0));
    try testing.expectEqual(3, try r.addsAt(1));
    try testing.expectEqual(0, r.removesLen());
}

test "txn: wireKvs emits key order regardless of insert order" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // Out of order, across two tables; empty txn emits nothing first.
    try testing.expectEqual(0, (try tx.wireKvs(testing.allocator)).len);
    try tx.put(tid + 1, "a", "t17-a", .{});
    try tx.put(tid, "c", "vc", .{});
    try tx.put(tid, "a", "va", .{});
    try tx.put(tid, "b", "vb", .{});

    const kvs = try tx.wireKvs(testing.allocator);
    defer {
        for (kvs) |kv| testing.allocator.free(kv.key);
        testing.allocator.free(kvs);
    }
    try testing.expectEqual(4, kvs.len);
    // (tid, "a") < (tid, "b") < (tid, "c") < (tid+1, "a"): table_id
    // orders before primary.
    const expect_primary = [_]String{ "a", "b", "c", "a" };
    const expect_tid = [_]u32{ tid, tid, tid, tid + 1 };
    for (kvs, expect_primary, expect_tid) |kv, p, t| {
        const k = Key.init(kv.key);
        try testing.expectEqual(ns, k.namespace);
        try testing.expectEqual(t, k.table_id);
        try testing.expectEqualSlices(u8, p, k.primary);
    }
}

test "txn: ROLLBACK TO restores the write set at the savepoint, RELEASE keeps it" {
    var fx = try Fixture.setup(&.{.{
        .key = rowKey("k0"),
        .versions = .{ .one = .{ .ts = 100, .meta = .{}, .value = "committed" } },
    }});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    // Before any write a savepoint remembers an empty set: ROLLBACK TO
    // it drops the pending tombstone and the committed row is back.
    try tx.savepoint("empty");
    try tx.put(tid, "k0", "", .{ .kind = .deleted });
    try testing.expectEqual(null, try lookupOne(&tx, "k0"));
    try testing.expect(try tx.rollbackToSavepoint("empty"));
    const k0 = (try lookupOne(&tx, "k0")) orelse return error.TestExpectedRow;
    defer k0.free(testing.allocator);
    try testing.expectEqualSlices(u8, "committed", k0.newest().value);

    try tx.put(tid, "k1", "a", .{});
    try tx.savepoint("s1");
    try tx.put(tid, "k1", "b", .{});
    try tx.put(tid, "k2", "c", .{});
    try tx.savepoint("s2");
    try tx.put(tid, "k3", "d", .{});
    tx.failed = true;

    // Back to s1: k1 is "a" again, k2 and k3 are gone, s2 with them,
    // s1 itself stays, and the block is live.
    try testing.expect(try tx.rollbackToSavepoint("s1"));
    try testing.expect(!tx.failed);
    try testing.expectEqual(@as(usize, 2), tx.savepoints.items.len);
    const k1 = (try lookupOne(&tx, "k1")) orelse return error.TestExpectedRow;
    defer k1.free(testing.allocator);
    try testing.expectEqualSlices(u8, "a", k1.newest().value);
    try testing.expectEqual(null, try lookupOne(&tx, "k2"));
    try testing.expectEqual(null, try lookupOne(&tx, "k3"));
    try testing.expect(!try tx.rollbackToSavepoint("s2"));

    // Writes after the rewind land on the restored set; RELEASE keeps
    // them and forgets the savepoint and the ones above it.
    try tx.put(tid, "k2", "e", .{});
    try testing.expect(try tx.rollbackToSavepoint("s1"));
    try testing.expectEqual(null, try lookupOne(&tx, "k2"));
    try tx.put(tid, "k2", "f", .{});
    try testing.expect(tx.releaseSavepoint("empty"));
    try testing.expectEqual(@as(usize, 0), tx.savepoints.items.len);
    try testing.expect(!tx.releaseSavepoint("s1"));
    const k2 = (try lookupOne(&tx, "k2")) orelse return error.TestExpectedRow;
    defer k2.free(testing.allocator);
    try testing.expectEqualSlices(u8, "f", k2.newest().value);
    // The commit payload is the restored set plus the later write.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const kvs = try tx.wireKvs(arena.allocator());
    try testing.expectEqual(@as(usize, 2), kvs.len);
}

test "txn: ROLLBACK TO releases the row locks taken since the savepoint" {
    var fx = try Fixture.setup(&.{});
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();

    try lockOne(&tx, "r1");
    try tx.savepoint("s");
    try lockOne(&tx, "r2");
    try lockOne(&tx, "r1"); // re-entrant: held since before, stays
    try lockOne(&tx, "r3");
    try testing.expectEqual(3, tx.held_rows.items.len);

    try testing.expect(try tx.rollbackToSavepoint("s"));
    try testing.expectEqual(1, tx.held_rows.items.len);
    try testing.expectEqual(1, fx.db.row_locks.entries.count());
    try testing.expect(fx.db.row_locks.entries.contains(tx.rowHash(tid, "r1")));
    try testing.expect(!fx.db.row_locks.entries.contains(tx.rowHash(tid, "r2")));
    // r2 is free for another txn right away.
    var other = fx.txnAs(2);
    defer other.deinit();
    try lockOne(&other, "r2");
    try testing.expectEqual(2, fx.db.row_locks.entries.count());
}
