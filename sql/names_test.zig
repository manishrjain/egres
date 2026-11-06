// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const utils = @import("utils");
const zdb = @import("zdb");
const flags = @import("flags");

const names = sql.names;
const zen = zdb.buffers;
const Forest = zdb.forest.Forest;
const Key = zdb.kv.Key;
const Meta = zdb.kv.Meta;
const String = []const u8;

const ns: u64 = 16;
const sys_ns = zdb.manifest.system_namespace;

/// A bare forest holding database `ns` and the system namespace, the
/// trees a name row lands in.
const Fixture = struct {
    tmp: std.testing.TmpDir,
    dir_path: [:0]u8,
    live: *flags.Live(zdb.config.Config),
    store: *zdb.vfs.RemoteStore,
    forest: *Forest,
    arena: std.heap.ArenaAllocator,

    fn setup(self: *Fixture) !void {
        const alloc = testing.allocator;
        self.tmp = std.testing.tmpDir(.{});
        errdefer self.tmp.cleanup();
        self.dir_path = try self.tmp.dir.realPathFileAlloc(
            utils.context.g().io,
            ".",
            alloc,
        );
        errdefer alloc.free(self.dir_path);
        self.store = try alloc.create(zdb.vfs.RemoteStore);
        errdefer alloc.destroy(self.store);
        self.store.* = try zdb.vfs.RemoteStore.initFile(
            alloc,
            self.dir_path,
            zdb.vfs.g(),
        );
        errdefer self.store.deinit();
        self.live = try flags.Live(zdb.config.Config).create(alloc, .{});
        errdefer self.live.destroy();
        self.forest = try Forest.open(alloc, self.live, self.store);
        errdefer self.forest.close();
        self.arena = std.heap.ArenaAllocator.init(alloc);
        const db = try self.forest.buildNsCreate(
            self.arena.allocator(),
            .{ .given = ns },
        );
        try self.forest.applyTrees(1, db.change);
        const sys = zdb.manifest.system_namespace_trees;
        var sys_trees: [sys.len]zdb.manifest.TreeChange = undefined;
        for (sys, &sys_trees) |tid, *tc|
            tc.* = zdb.manifest.createTreeChange(
                zdb.kv.makeTreeId(tid, 0),
                sys_ns,
                "",
                "",
                .posting_index,
            );
        try self.forest.applyTrees(2, .{
            .tree_changes = &sys_trees,
            .sstable_changes = &.{},
            .cmd = .create_ns,
            .memtable_id = 0,
        });
    }

    fn teardown(self: *Fixture) void {
        const alloc = testing.allocator;
        self.arena.deinit();
        self.forest.close();
        self.live.destroy();
        self.store.deinit();
        alloc.destroy(self.store);
        alloc.free(self.dir_path);
        self.tmp.cleanup();
    }

    /// Apply `kv` at `ts`, as a committed entry's row would be.
    fn put(self: *Fixture, kv: zen.KeyValueStruct, ts: u64) !void {
        try self.forest.put(&.{.{
            .key = Key.init(kv.key),
            .versions = .{ .one = .{
                .ts = ts,
                .meta = Meta.fromByte(kv.meta),
                .value = kv.value,
            } },
        }});
    }

    fn arloc(self: *Fixture) std.mem.Allocator {
        return self.arena.allocator();
    }
};

test "names: a table name row is the name under table 3, id big-endian" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const kv = try names.tableNameKv(arena.allocator(), ns, "users", 17);
    const key = Key.init(kv.key);
    try testing.expectEqual(ns, key.namespace);
    try testing.expectEqual(zdb.manifest.names_table_id, key.table_id);
    try testing.expectEqualStrings("users", key.primary);
    try testing.expectEqualSlices(u8, &.{ 0, 0, 0, 17 }, kv.value);

    const db = try names.dbNameKv(arena.allocator(), "shop", 42);
    try testing.expectEqual(sys_ns, Key.init(db.key).namespace);
    try testing.expectEqual(@as(usize, 8), db.value.len);
}

test "names: a written name is found from its commit timestamp on" {
    var fx: Fixture = undefined;
    try fx.setup();
    defer fx.teardown();

    try fx.put(try names.tableNameKv(fx.arloc(), ns, "users", 17), 10);
    try testing.expectEqual(
        @as(?u32, 17),
        try names.tableId(fx.forest, 10, ns, "users"),
    );
    try testing.expectEqual(
        @as(?u32, null),
        try names.tableId(fx.forest, 9, ns, "users"),
    );
    try testing.expectEqual(
        @as(?u32, null),
        try names.tableId(fx.forest, 10, ns, "orders"),
    );
}

test "names: a tombstone frees a name, and a re-create gets the new id" {
    var fx: Fixture = undefined;
    try fx.setup();
    defer fx.teardown();

    try fx.put(try names.tableNameKv(fx.arloc(), ns, "users", 17), 10);
    try fx.put(try names.tombstone(fx.arloc(), ns, "users"), 20);
    try testing.expectEqual(
        @as(?u32, null),
        try names.tableId(fx.forest, 20, ns, "users"),
    );
    try fx.put(try names.tableNameKv(fx.arloc(), ns, "users", 18), 30);
    try testing.expectEqual(
        @as(?u32, 18),
        try names.tableId(fx.forest, 30, ns, "users"),
    );
    try testing.expectEqual(
        @as(?u32, 17),
        try names.tableId(fx.forest, 15, ns, "users"),
    );
}

test "names: a database name resolves to its namespace" {
    var fx: Fixture = undefined;
    try fx.setup();
    defer fx.teardown();

    try fx.put(try names.dbNameKv(fx.arloc(), "egres", ns), 3);
    try testing.expectEqual(
        @as(?u64, ns),
        try names.namespaceId(fx.forest, 3, "egres"),
    );
    try testing.expectEqual(
        @as(?u64, null),
        try names.namespaceId(fx.forest, 3, "shop"),
    );
    // Database names and table names live in different namespaces.
    try testing.expectEqual(
        @as(?u32, null),
        try names.tableId(fx.forest, 3, ns, "egres"),
    );
}

test "names: checkNew refuses a taken or repeated name" {
    var fx: Fixture = undefined;
    try fx.setup();
    defer fx.teardown();

    try fx.put(try names.tableNameKv(fx.arloc(), ns, "users", 17), 10);

    try names.checkNew(fx.forest, 10, ns, &.{ "orders", "orders_pkey" });
    try testing.expectError(
        error.NameExists,
        names.checkNew(fx.forest, 10, ns, &.{ "orders", "users" }),
    );
    try testing.expectError(
        error.NameExists,
        names.checkNew(fx.forest, 10, ns, &.{ "t_pkey", "t_pkey" }),
    );
    // A generated name longer than 63 bytes is fine, as on main.
    try names.checkNew(fx.forest, 10, ns, &.{"x" ** 68});
}

test "names: tombstonesFor removes exactly the names of the given ids" {
    var fx: Fixture = undefined;
    try fx.setup();
    defer fx.teardown();

    try fx.put(try names.tableNameKv(fx.arloc(), ns, "users", 16), 10);
    try fx.put(try names.tableNameKv(fx.arloc(), ns, "users_pkey", 17), 10);
    try fx.put(try names.tableNameKv(fx.arloc(), ns, "orders", 18), 10);

    const rows = try names.tombstonesFor(
        fx.arloc(),
        fx.forest,
        10,
        ns,
        &.{ 16, 18 },
    );
    // Name order: the scan walks table 3 by key.
    try testing.expectEqual(@as(usize, 2), rows.len);
    for (rows, [_]String{ "orders", "users" }) |row, want| {
        const want_row = try names.tombstone(fx.arloc(), ns, want);
        try testing.expectEqualStrings(want_row.key, row.key);
        try testing.expect(Meta.fromByte(row.meta).isDeleted());
        try testing.expectEqual(@as(usize, 0), row.value.len);
    }
}
