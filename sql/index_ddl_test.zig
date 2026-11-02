// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const fiber = @import("zfiber");
const sql = @import("sql");
const utils = @import("utils");
const zdb = @import("zdb");
const flags = @import("flags");

const Forest = zdb.forest.Forest;
const Config = zdb.config.Config;
const Key = zdb.kv.Key;
const Meta = zdb.kv.Meta;
const RowIndexKeys = sql.index_ddl.RowIndexKeys;
const String = sql.value.String;
const Txn = sql.txn.Txn;

const ns: u64 = 16;
const base_tid: u32 = 16;
const index_tid: u32 = 17;
const index_key: zdb.kv.TableKey = .{ .namespace = ns, .table_id = index_tid };
const read_ts: u64 = 100;

/// A Table with one index on `a` (unique or not), the lock map,
/// oracle and Forest a Txn needs — the Forest is never read here —
/// and an arena everything else lives in.
const Fixture = struct {
    tmp: std.testing.TmpDir,
    dir_path: [:0]u8,
    forest: *Forest,
    /// The forest reads its options through this; freed after it.
    live: *flags.Live(Config),
    /// The forest's SSTs; heap-held so its address survives the move out
    /// of setup. Freed after the forest.
    store: *zdb.vfs.RemoteStore,
    db: sql.database.Database,
    arena: std.heap.ArenaAllocator,
    tbl: sql.table.Table,

    fn setup(unique: bool) !Fixture {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        errdefer arena.deinit();
        const arloc = arena.allocator();

        const logical = [_]sql.schema.LogicalColumn{
            .{ .column_id = 1, .name = "a", .ty = .int4 },
        };
        const ps = try sql.schema.compile(
            arloc,
            .{ .columns = &logical, .current_version = 1 },
            1,
        );
        const columns = try arloc.alloc(sql.table.Column, 1);
        columns[0] = .{
            .column_id = 1,
            .pos = 0,
            .name = "a",
            .ty = .int4,
            .pkey_position = null,
        };
        const indexes = try arloc.alloc(sql.table.Index, 1);
        indexes[0] = .{
            .index_table_id = index_tid,
            .unique = unique,
            .valid_ts = sql.catalog.IndexDef.building,
            .column = &columns[0],
        };

        var tmp = std.testing.tmpDir(.{});
        errdefer tmp.cleanup();
        const dir_path = try tmp.dir.realPathFileAlloc(
            utils.context.g().io,
            ".",
            testing.allocator,
        );
        errdefer testing.allocator.free(dir_path);
        const store = try testing.allocator.create(zdb.vfs.RemoteStore);
        errdefer testing.allocator.destroy(store);
        store.* = try zdb.vfs.RemoteStore.initFile(
            testing.allocator,
            dir_path,
            zdb.vfs.g(),
        );
        errdefer store.deinit();
        const f_live = try flags.Live(Config).create(testing.allocator, .{});
        errdefer f_live.destroy();
        const f = try Forest.open(testing.allocator, f_live, store);
        errdefer f.close();

        return .{
            .tmp = tmp,
            .dir_path = dir_path,
            .forest = f,
            .live = f_live,
            .store = store,
            .db = try sql.database.Database.init(testing.allocator, ns),
            .arena = arena,
            .tbl = .{
                .columns = columns,
                .indexes = indexes,
                .ps = ps,
                .version = 1,
                .arena = std.heap.ArenaAllocator.init(testing.allocator),
            },
        };
    }

    fn teardown(self: *Fixture) void {
        self.tbl.deinit();
        self.db.deinit();
        self.forest.close();
        self.live.destroy();
        self.store.deinit();
        testing.allocator.destroy(self.store);
        testing.allocator.free(self.dir_path);
        self.tmp.cleanup();
        self.arena.deinit();
    }

    fn rows(self: *Fixture) RowIndexKeys {
        return .{
            .a = self.arena.allocator(),
            .unique = self.tbl.indexes[0].unique,
        };
    }

    fn txn(self: *Fixture) Txn {
        return Txn.init(
            testing.allocator,
            1,
            read_ts,
            1,
            &self.db,
            self.forest,
        );
    }

    /// The index key of `a = v`, as one row's key slice.
    fn keys(self: *Fixture, v: i32) ![]const String {
        const arloc = self.arena.allocator();
        const one = try arloc.alloc(String, 1);
        one[0] = try sql.key.encodeIndexKey(arloc, .{ .int4 = v }, null);
        return one;
    }

    /// The pending writes of `tx`, in key order.
    fn kvs(self: *Fixture, tx: *Txn) ![]zdb.buffers.KeyValueStruct {
        return tx.wireKvs(self.arena.allocator());
    }
};

test "sql.index_ddl: v1 inverts into sorted items, one per key" {
    var fx = try Fixture.setup(false);
    defer fx.teardown();
    var rows = fx.rows();

    // Rows 1 and 3 share a = 5; row 2 has a = 4. Inserted out of key
    // order; the items come back in key order.
    try rows.setV1(1, try fx.keys(5));
    try rows.setV1(2, try fx.keys(4));
    try rows.setV1(3, try fx.keys(5));
    try rows.setV1(4, &.{}); // no key: not an entry
    try testing.expectEqual(@as(usize, 3), rows.count());

    const items = try rows.items(index_key, read_ts);
    try testing.expectEqual(@as(usize, 2), items.len);
    const key4 = (try fx.keys(4))[0];
    try testing.expectEqualSlices(u8, key4, items[0].key.primary);
    try testing.expectEqual(read_ts, items[0].newest().ts);
    try testing.expect(items[0].newest().meta.isRoaring());
    var bm = try zdb.zroar.Bitmap.fromBuffer(
        testing.allocator,
        @alignCast(items[1].newest().value),
        .borrow,
    );
    defer bm.deinit();
    try testing.expectEqual(@as(u64, 2), bm.getCardinality());
    try testing.expect(bm.contains(1) and bm.contains(3));
}

test "sql.index_ddl: a unique v1 with two rows on one key is a duplicate" {
    var fx = try Fixture.setup(true);
    defer fx.teardown();
    var rows = fx.rows();

    try rows.setV1(1, try fx.keys(5));
    try rows.setV1(2, try fx.keys(4));
    const items = try rows.items(index_key, read_ts);
    try testing.expectEqual(@as(usize, 2), items.len);
    try testing.expect(!items[0].newest().meta.isRoaring());
    const egid = sql.key.decodeEgid(items[0].newest().value).?;
    try testing.expectEqual(@as(u64, 2), egid);

    try rows.setV1(3, try fx.keys(5));
    try testing.expectError(
        error.DuplicateKey,
        rows.items(index_key, read_ts),
    );
}

test "sql.index_ddl: posting delta is a remove and an add per moved key" {
    var fx = try Fixture.setup(false);
    defer fx.teardown();
    var rows = fx.rows();
    var tx = fx.txn();
    defer tx.deinit();

    // v1: rows 1 and 2 on 5, row 3 on 4. v2: row 1 moved to 4, row 2
    // was deleted, row 3 rewrote another column (its key stays), and
    // row 4 was inserted on 5.
    try rows.setV1(1, try fx.keys(5));
    try rows.setV1(2, try fx.keys(5));
    try rows.setV1(3, try fx.keys(4));
    _ = try rows.items(index_key, read_ts);
    try rows.setV2(1, try fx.keys(4));
    try rows.setV2(2, &.{});
    try rows.setV2(3, try fx.keys(4));
    try rows.setV2(4, try fx.keys(5));
    try rows.writeDelta(&tx, &fx.tbl.indexes[0]);

    // One delta per index key, in key order: 4 gains row 1; 5 loses
    // rows 1 and 2 and gains row 4. Row 3 is untouched.
    const kvs = try fx.kvs(&tx);
    try testing.expectEqual(@as(usize, 2), kvs.len);
    const four = try zdb.buffers.RoaringDeltaReader.init(kvs[0].value, 0);
    try testing.expectEqual(@as(u32, 1), four.addsLen());
    try testing.expectEqual(@as(u64, 1), try four.addsAt(0));
    try testing.expectEqual(@as(u32, 0), four.removesLen());
    const five = try zdb.buffers.RoaringDeltaReader.init(kvs[1].value, 0);
    try testing.expectEqual(@as(u32, 1), five.addsLen());
    try testing.expectEqual(@as(u64, 4), try five.addsAt(0));
    try testing.expectEqual(@as(u32, 2), five.removesLen());
    try testing.expectEqual(@as(u64, 1), try five.removesAt(0));
    try testing.expectEqual(@as(u64, 2), try five.removesAt(1));
}

/// One pending unique-index write: the key, and the egid or null for
/// a tombstone.
const UniqueWrite = struct { key: String, egid: ?u64 };

/// The unique delta's pending writes, in key order.
fn uniqueWrites(fx: *Fixture, tx: *Txn) ![]const UniqueWrite {
    const arloc = fx.arena.allocator();
    const kvs = try fx.kvs(tx);
    const out = try arloc.alloc(UniqueWrite, kvs.len);
    for (kvs, out) |kv, *w| {
        const key = Key.init(kv.key);
        w.* = .{
            .key = key.primary,
            .egid = if (Meta.fromByte(kv.meta).isDeleted())
                null
            else
                sql.key.decodeEgid(kv.value),
        };
    }
    return out;
}

test "sql.index_ddl: unique delta moves, deletes, and takes over keys" {
    var fx = try Fixture.setup(true);
    defer fx.teardown();
    var rows = fx.rows();
    var tx = fx.txn();
    defer tx.deinit();

    // v1: row 1 on 7, row 2 on 8, row 5 on 6. v2: row 1 moved to 9;
    // row 2 was deleted; row 3 was inserted on 7, which row 1 left;
    // row 5 rewrote another column and keeps 6.
    try rows.setV1(1, try fx.keys(7));
    try rows.setV1(2, try fx.keys(8));
    try rows.setV1(5, try fx.keys(6));
    _ = try rows.items(index_key, read_ts);
    try rows.setV2(1, try fx.keys(9));
    try rows.setV2(2, &.{});
    try rows.setV2(3, try fx.keys(7));
    try rows.setV2(5, try fx.keys(6));
    try rows.writeDelta(&tx, &fx.tbl.indexes[0]);

    // 6 → row 5 (rewritten as is), 7 → row 3, 8 tombstoned, 9 → row 1.
    const got = try uniqueWrites(&fx, &tx);
    try testing.expectEqual(@as(usize, 4), got.len);
    try testing.expectEqualSlices(u8, (try fx.keys(6))[0], got[0].key);
    try testing.expectEqual(@as(?u64, 5), got[0].egid);
    try testing.expectEqualSlices(u8, (try fx.keys(7))[0], got[1].key);
    try testing.expectEqual(@as(?u64, 3), got[1].egid);
    try testing.expectEqualSlices(u8, (try fx.keys(8))[0], got[2].key);
    try testing.expectEqual(@as(?u64, null), got[2].egid);
    try testing.expectEqualSlices(u8, (try fx.keys(9))[0], got[3].key);
    try testing.expectEqual(@as(?u64, 1), got[3].egid);
}

test "sql.index_ddl: unique delta finds both kinds of duplicate" {
    var fx = try Fixture.setup(true);
    defer fx.teardown();

    // A v2 row on a key an unchanged v1 row still has.
    {
        var rows = fx.rows();
        var tx = fx.txn();
        defer tx.deinit();
        try rows.setV1(1, try fx.keys(7));
        _ = try rows.items(index_key, read_ts);
        try rows.setV2(3, try fx.keys(7));
        try testing.expectError(
            error.DuplicateKey,
            rows.writeDelta(&tx, &fx.tbl.indexes[0]),
        );
    }
    // Two v2 rows on one key.
    {
        var rows = fx.rows();
        var tx = fx.txn();
        defer tx.deinit();
        _ = try rows.items(index_key, read_ts);
        try rows.setV2(3, try fx.keys(7));
        try rows.setV2(4, try fx.keys(7));
        try testing.expectError(
            error.DuplicateKey,
            rows.writeDelta(&tx, &fx.tbl.indexes[0]),
        );
    }
    // A v1 row that kept its key, plus a v2 row on that key.
    {
        var rows = fx.rows();
        var tx = fx.txn();
        defer tx.deinit();
        try rows.setV1(1, try fx.keys(7));
        _ = try rows.items(index_key, read_ts);
        try rows.setV2(1, try fx.keys(7));
        try rows.setV2(3, try fx.keys(7));
        try testing.expectError(
            error.DuplicateKey,
            rows.writeDelta(&tx, &fx.tbl.indexes[0]),
        );
    }
}
