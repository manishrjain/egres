const std = @import("std");
const zio = @import("zio");
const zdb = @import("zdb");
const vfs = zdb.vfs;

const Key = zdb.key.Key;
const table_mod = zdb.table;
const Builder = table_mod.builder.Builder;
const Table = table_mod.Table;

const manifest = zdb.manifest;
const KeyRange = zdb.level.KeyRange;

const forest_mod = zdb.forest;
const Forest = forest_mod.Forest;
const RouteTable = forest_mod.RouteTable;

const options_mod = zdb.options;

const utils = @import("utils");
test "all" {
    try utils.runTestsInZioRuntime(@This());
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.realpathAlloc(alloc, ".");
}

// ---------------------------------------------------------------------------
// 1. RouteTable lookup
// ---------------------------------------------------------------------------

pub fn testRouteTableSingleEntry() !void {
    const alloc = std.testing.allocator;
    var rt = RouteTable.init(alloc, 1);
    defer rt.deinit();

    // Single tree with infinite range covers everything.
    try rt.addRoute(10, .{ .inf = true });
    try std.testing.expectEqual(@as(u64, 10), rt.lookup(.{ .namespace = 1, .primary = "anything" }).?);
}

pub fn testRouteTableMultiEntry() !void {
    const alloc = std.testing.allocator;
    var rt = RouteTable.init(alloc, 1);
    defer rt.deinit();

    // Two trees: [a, m) and [m, z].
    const ak = try (Key{ .namespace = 1, .primary = "a" }).marshal(alloc);
    defer alloc.free(ak);
    const mk = try (Key{ .namespace = 1, .primary = "m" }).marshal(alloc);
    defer alloc.free(mk);
    const lk = try (Key{ .namespace = 1, .primary = "lzzz" }).marshal(alloc);
    defer alloc.free(lk);
    const zk = try (Key{ .namespace = 1, .primary = "z" }).marshal(alloc);
    defer alloc.free(zk);

    try rt.addRoute(1, .{ .left = Key.init(ak), .right = Key.init(lk) });
    try rt.addRoute(2, .{ .left = Key.init(mk), .right = Key.init(zk) });

    // "c" falls in tree 1.
    try std.testing.expectEqual(@as(u64, 1), rt.lookup(.{ .namespace = 1, .primary = "c" }).?);
    // "n" falls in tree 2.
    try std.testing.expectEqual(@as(u64, 2), rt.lookup(.{ .namespace = 1, .primary = "n" }).?);
}

// ---------------------------------------------------------------------------
// 2. Forest open/close
// ---------------------------------------------------------------------------

pub fn testForestOpenCloseEmpty() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    f.close();
}

// ---------------------------------------------------------------------------
// 3. Forest create tree + get
// ---------------------------------------------------------------------------

pub fn testForestCreateTreeAndGet() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    // Create a tree for namespace 1 covering all keys.
    const tree_id = try f.createTree(1, .{ .inf = true });
    try std.testing.expect(tree_id > 0);

    // Verify the tree exists.
    const tree = f.getTree(tree_id);
    try std.testing.expect(tree != null);
    try std.testing.expectEqual(tree_id, tree.?.tree_id);
}

// ---------------------------------------------------------------------------
// 4. Forest create tree survives reopen
// ---------------------------------------------------------------------------

pub fn testForestTreePersistsAcrossReopen() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Create forest and add a tree.
    var tree_id: u64 = undefined;
    {
        const f = try Forest.open(alloc, .{ .dir = dir_path });
        tree_id = try f.createTree(42, .{ .inf = true });
        f.close();
    }

    // Reopen and verify tree is still there.
    {
        const f = try Forest.open(alloc, .{ .dir = dir_path });
        defer f.close();

        const tree = f.getTree(tree_id);
        try std.testing.expect(tree != null);
        try std.testing.expectEqual(tree_id, tree.?.tree_id);
    }
}

// ---------------------------------------------------------------------------
// 5. Forest route table lookup
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// 6. Tree splitting
// ---------------------------------------------------------------------------

// Helper: build a minimal SST table containing one key and write it to disk.
fn buildAndWriteTable(alloc: std.mem.Allocator, path: []const u8, primary: []const u8) !void {
    var bld = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer bld.deinit();
    try bld.add(.{ .primary = primary }, "v");
    var tb = try bld.finish();
    defer tb.deinit();
    const f = try std.fs.createFileAbsolute(path, .{});
    defer f.close();
    try f.writeAll(tb.bytes);
}

pub fn testForestSplitTree() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Phase 1: set up manifest + 3 SST files at L1 of tree 1 (namespace 42).
    // Keys: "aaa", "mmm", "zzz" — sorted, non-overlapping, one key per table.
    {
        var ms = try manifest.openOrCreate(alloc, dir_path);
        defer ms.close();

        try ms.commit(&.{}, &.{manifest.createTreeChange(1, 42, &.{}, &.{})});

        const table_keys = [_][]const u8{ "aaa", "mmm", "zzz" };
        for (table_keys, 1..) |key, tbl_id| {
            const path = try options_mod.sstPath(alloc, dir_path, tbl_id);
            defer alloc.free(path);
            try buildAndWriteTable(alloc, path, key);
            try ms.commit(&.{manifest.createTableChange(tbl_id, 1, 1)}, &.{});
        }
    }

    // Phase 2: open forest, verify 3 tables loaded, then split.
    {
        const f = try Forest.open(alloc, .{ .dir = dir_path });
        defer f.close();

        const tree = f.getTree(1).?;
        try std.testing.expectEqual(@as(usize, 3), tree.levels[1].numTables());

        // splitTree requires the write lock.
        f.mu.lock();
        defer f.mu.unlock();
        try f.splitTree(1);

        // Now 2 trees should exist.
        try std.testing.expectEqual(@as(usize, 2), f.trees.count());

        // The split key is "mmm" (middle table, index 1 of 3).
        // Old tree (id=1): tables ["aaa"] — left side.
        // New tree (id=2): tables ["mmm","zzz"] — right side.
        const old_tree = f.trees.get(1).?;
        const new_tree = f.trees.get(2).?;
        try std.testing.expectEqual(@as(usize, 1), old_tree.levels[1].numTables());
        try std.testing.expectEqual(@as(usize, 2), new_tree.levels[1].numTables());

        // Key ranges: old right = "mmm", new left = "mmm".
        try std.testing.expectEqualStrings("mmm", old_tree.key_range.right.primary);
        try std.testing.expectEqualStrings("mmm", new_tree.key_range.left.primary);

        // Route table for namespace 42 should have 2 entries.
        const rt = f.ns_routes.get(42).?;
        try std.testing.expectEqual(@as(usize, 2), rt.entries.items.len);

        // Routing: "aaa" → old tree (1), "zzz" → new tree (2).
        try std.testing.expectEqual(@as(u64, 1), rt.lookup(.{ .primary = "aaa" }).?);
        try std.testing.expectEqual(@as(u64, 2), rt.lookup(.{ .primary = "zzz" }).?);
    }
}

pub fn testForestRoutesToCorrectTree() !void {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    // Create two trees in different namespaces.
    _ = try f.createTree(1, .{ .inf = true });
    _ = try f.createTree(2, .{ .inf = true });

    // Both namespaces should have a route table.
    {
        f.mu.lockShared();
        defer f.mu.unlockShared();
        try std.testing.expect(f.ns_routes.contains(1));
        try std.testing.expect(f.ns_routes.contains(2));
    }
}
