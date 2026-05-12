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

const config = zdb.config;


// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.realpathAlloc(alloc, ".");
}

// ---------------------------------------------------------------------------
// 1. RouteTable lookup
// ---------------------------------------------------------------------------

test "forest: route table single entry" {
    const alloc = std.testing.allocator;
    var rt = RouteTable.init(alloc, 1);
    defer rt.deinit();

    // Single tree with infinite range covers everything.
    try rt.addRoute(10, .{ .inf = true });
    try std.testing.expectEqual(@as(u64, 10), rt.lookup(.{ .namespace = 1, .primary = "anything" }).?);
}

test "forest: route table multi-entry" {
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

test "forest: open close empty" {
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

test "forest: create tree and get" {
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

// Note: There used to be a `testForestTreePersistsAcrossReopen` test here.
// With the snapshot-resident manifest design, the on-disk manifest is gone:
// persistence flows through the Raft log + periodic snapshots. The
// equivalent end-to-end recovery test will live in raft/snapshot_test.zig
// once the snapshot apply path is wired up.

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

test "forest: split tree" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // findSplitKey now requires Lmax to have ≥ 10 tables and pulls
    // candidates from there. Install 10 single-key SSTs ("k01"…"k10")
    // directly into L3 (default max_levels=4 → lmax_idx=3) so the
    // splitter has a clean, evenly-sized Lmax to work with.
    const f = try Forest.open(alloc, .{ .dir = dir_path });
    defer f.close();

    const tree_id = try f.createTree(42, .{ .inf = true });
    try std.testing.expectEqual(@as(u64, 1), tree_id);

    const table_keys = [_][]const u8{ "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10" };
    for (table_keys, 1..) |key, tbl_id| {
        const path = try config.sstPath(alloc, dir_path, tbl_id);
        defer alloc.free(path);
        try buildAndWriteTable(alloc, path, key);
    }

    {
        const tree_ptr = f.getTree(tree_id).?;
        var changes: [10]manifest.TableChange = undefined;
        for (&changes, 1..) |*c, tbl_id| c.* = manifest.createTableChange(@intCast(tbl_id), tree_id, 3); // L3
        try tree_ptr.applyCompaction(&changes);
        try std.testing.expectEqual(@as(usize, 10), tree_ptr.levels[3].numTables());
    }

    // splitTreeForTests requires the write lock.
    {
        f.mu.lock();
        defer f.mu.unlock();
        try f.splitTreeForTests(1);
    }

    // Now 2 trees should exist.
    try std.testing.expectEqual(@as(usize, 2), f.trees.count());

    // Best balance with 10 equal-size L3 tables: split at items[5].smallest
    // = "k06" (left=5 tables, right=5 tables, |L−R| = 0). Old tree keeps
    // [k01..k05]; new tree gets [k06..k10].
    const old_tree = f.trees.get(1).?;
    const new_tree = f.trees.get(2).?;
    try std.testing.expectEqual(@as(usize, 5), old_tree.levels[3].numTables());
    try std.testing.expectEqual(@as(usize, 5), new_tree.levels[3].numTables());

    // Key ranges: old right = "k06", new left = "k06".
    try std.testing.expectEqualStrings("k06", old_tree.key_range.right.primary);
    try std.testing.expectEqualStrings("k06", new_tree.key_range.left.primary);

    // Route table for namespace 42 should have 2 entries.
    const rt = f.ns_routes.get(42).?;
    try std.testing.expectEqual(@as(usize, 2), rt.entries.items.len);

    // Routing: "k01" → old tree (1), "k10" → new tree (2).
    try std.testing.expectEqual(@as(u64, 1), rt.lookup(.{ .primary = "k01" }).?);
    try std.testing.expectEqual(@as(u64, 2), rt.lookup(.{ .primary = "k10" }).?);
}

// ---------------------------------------------------------------------------
// 7. Snapshot manifest round-trip — recovery path
// ---------------------------------------------------------------------------
//
// Build a forest with two trees (ns=42 with key range, ns=43 covering all)
// plus an SST file at L1 of the first tree. Serialize the manifest, install
// it into a fresh forest pointing at the same on-disk dir, and assert: tree
// set, namespace routes, level membership, and ID counters are all restored.

test "forest: snapshot manifest round-trip" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Phase 1: build source forest with content + serialize manifest.
    const ak = try (Key{ .namespace = 42, .primary = "a" }).marshal(alloc);
    defer alloc.free(ak);
    const mk = try (Key{ .namespace = 42, .primary = "m" }).marshal(alloc);
    defer alloc.free(mk);

    const bytes = blk: {
        const src = try Forest.open(alloc, .{ .dir = dir_path });
        defer src.close();

        // Tree 1: ns=42, range [a, m). Tree 2: ns=43, infinite.
        _ = try src.createTree(42, .{ .left = Key.init(ak), .right = Key.init(mk) });
        _ = try src.createTree(43, .{ .inf = true });

        // Drop one SST into tree 1's L1, both on disk and in the manifest.
        const sst_path = try config.sstPath(alloc, dir_path, 1);
        defer alloc.free(sst_path);
        try buildAndWriteTable(alloc, sst_path, "ccc");

        const tree1 = src.getTree(1).?;
        const changes = [_]manifest.TableChange{manifest.createTableChange(1, 1, 1)};
        try tree1.applyCompaction(&changes);

        break :blk try src.ms.serialize(alloc);
    };
    defer alloc.free(bytes);

    // Phase 2: fresh forest, install snapshot, verify.
    const dst = try Forest.open(alloc, .{ .dir = dir_path });
    defer dst.close();

    try dst.ms.restoreFrom(bytes);
    try dst.openTablesFromManifest();

    // Both trees present.
    try std.testing.expectEqual(@as(u32, 2), dst.trees.count());
    try std.testing.expect(dst.trees.contains(1));
    try std.testing.expect(dst.trees.contains(2));

    // Namespace routes wired.
    try std.testing.expect(dst.ns_routes.contains(42));
    try std.testing.expect(dst.ns_routes.contains(43));
    try std.testing.expectEqual(@as(u64, 1), dst.ns_routes.get(42).?.lookup(.{ .namespace = 42, .primary = "ccc" }).?);
    try std.testing.expectEqual(@as(u64, 2), dst.ns_routes.get(43).?.lookup(.{ .namespace = 43, .primary = "any" }).?);

    // Tree 1's L1 contains the SST.
    try std.testing.expectEqual(@as(usize, 1), dst.trees.get(1).?.levels[1].numTables());

    // Counters advanced past the existing IDs (trees 1,2 → next 3; tables 1 → next 2).
    try std.testing.expect(dst.next_tree_id.load(.monotonic) > 2);
    try std.testing.expect(dst.next_file_id.load(.monotonic) > 1);
}

// ---------------------------------------------------------------------------
// 8. Memtables.popOldestIfMatches — recovery-time pop semantics
// ---------------------------------------------------------------------------
//
// Helper that builds a Memtables with a chain of immutables of the given ids.
// The current memtable is the placeholder (id=0); each `swapCurrentOut(id_i)`
// freezes the prior current and makes a new current with id_i. After N swaps,
// immutables = [id_1, id_2, ..., id_{N-1}], current = id_N.

fn buildMemtables(alloc: std.mem.Allocator, ids: []const u64) !forest_mod.Memtables {
    const initial = try zdb.skl.SkipList.init(alloc);
    var mts = forest_mod.Memtables.init(alloc, initial);
    errdefer mts.deinit();
    for (ids) |id| try mts.swapCurrentOut(id, 0);
    return mts;
}

test "forest: popOldestIfMatches empty" {
    const alloc = std.testing.allocator;
    var mts = try buildMemtables(alloc, &.{}); // current=placeholder, immutables=[]
    defer mts.deinit();
    mts.popOldestIfMatches(7); // no-op
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
}

test "forest: popOldestIfMatches exact" {
    const alloc = std.testing.allocator;
    // immutables=[5], current=8.
    var mts = try buildMemtables(alloc, &.{ 5, 8 });
    defer mts.deinit();
    mts.popOldestIfMatches(5);
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
    try std.testing.expectEqual(@as(u64, 8), mts.current.id);
}

test "forest: popOldestIfMatches greater" {
    const alloc = std.testing.allocator;
    // immutables=[7], current=10. Flush proposal expects 5 — older than oldest.
    var mts = try buildMemtables(alloc, &.{ 7, 10 });
    defer mts.deinit();
    mts.popOldestIfMatches(5); // skip — oldest > expected
    try std.testing.expectEqual(@as(usize, 1), mts.immutables.items.len);
    try std.testing.expectEqual(@as(u64, 7), mts.immutables.items[0].id);
}

test "forest: popOldestIfMatches phantom" {
    const alloc = std.testing.allocator;
    // immutables=[3], current=10. Flush proposal expects 7. M_3 is a phantom.
    var mts = try buildMemtables(alloc, &.{ 3, 10 });
    defer mts.deinit();
    mts.popOldestIfMatches(7); // drops phantom M_3, no match for 7 → done
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
}

test "forest: popOldestIfMatches multi phantom then match" {
    const alloc = std.testing.allocator;
    // immutables=[2, 4, 7], current=10. Flush proposal expects 7.
    // Loop drops M_2 and M_4 as phantoms, then pops M_7.
    var mts = try buildMemtables(alloc, &.{ 2, 4, 7, 10 });
    defer mts.deinit();
    mts.popOldestIfMatches(7);
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
    try std.testing.expectEqual(@as(u64, 10), mts.current.id);
}

test "forest: popOldestIfMatches multi phantom no match" {
    const alloc = std.testing.allocator;
    // immutables=[2, 4], current=10. Flush proposal expects 7. Both phantoms;
    // no match — immutables drained, current untouched.
    var mts = try buildMemtables(alloc, &.{ 2, 4, 10 });
    defer mts.deinit();
    mts.popOldestIfMatches(7);
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);
    try std.testing.expectEqual(@as(u64, 10), mts.current.id);
}

test "forest: getIterators outlives a concurrent flush+pop" {
    // The race we're guarding against: a scan calls
    // `Memtables.getIterators` and then drops the lock; before the
    // scan is done iterating, a flush completes and `popOldestIfMatches`
    // calls `release()` on the immutable memtable. If the iterator
    // hadn't already bumped the refcount, the SkipList's arena would
    // be freed mid-iteration and the next .next() would UAF.
    const alloc = std.testing.allocator;

    // Build immutables=[5], current=10, then put a key into M_5 so we
    // can read it after the pop.
    var mts = try buildMemtables(alloc, &.{ 5, 10 });
    defer mts.deinit();
    try std.testing.expectEqual(@as(usize, 1), mts.immutables.items.len);
    const m5 = mts.immutables.items[0];
    try m5.put(.{ .namespace = 1, .primary = "alpha", .timestamp = 100 }, "v1");

    // Snapshot under the lock — each skl.Iterator.init bumps
    // refcount before getIterators returns.
    const iters = try mts.getIterators(alloc);

    // Simulate flush completion: drop M_5 from immutables (release fires
    // inside popOldestIfMatches). M_5's refcount goes from 2 → 1
    // because the snapshot iterator still holds a ref.
    mts.popOldestIfMatches(5);
    try std.testing.expectEqual(@as(usize, 0), mts.immutables.items.len);

    // Iterate AFTER the pop; the snapshotted iterator for M_5 should
    // still see the key. Use ts=maxInt because same-primary keys sort
    // newest-first (timestamp is reverse-ordered) — seeking with ts=0
    // would land on the *next* primary, missing alpha.
    var found: ?[]const u8 = null;
    for (iters) |*it| {
        var copy = it.*;
        try copy.seekRange(.{
            .namespace = 1,
            .primary = "alpha",
            .timestamp = std.math.maxInt(u64),
        }, .{});
        if (copy.itemRef()) |item| {
            if (std.mem.eql(u8, item.key.primary, "alpha")) {
                found = item.value;
            }
        }
    }
    try std.testing.expect(found != null);
    try std.testing.expectEqualStrings("v1", found.?);

    // Final release: deinit each iterator (mirroring what
    // MergeIterator.deinit would do), then free the slice. Without
    // this, M_5's last ref leaks and the arena is never freed.
    for (iters) |*it| it.deinit();
    alloc.free(iters);
}

test "forest: routes to correct tree" {
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
