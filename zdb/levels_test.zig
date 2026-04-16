const std = @import("std");
const zio = @import("zio");
const zdb = @import("zdb");
const vfs = zdb.vfs;

const Key = zdb.key.Key;
const table_mod = zdb.table;
const Builder = table_mod.builder.Builder;
const Table = table_mod.Table;
const TableIterator = table_mod.TableIterator;

const manifest = zdb.manifest;
const createTableChange = manifest.createTableChange;
const createTreeChange = manifest.createTreeChange;

const level_mod = zdb.level;
const Level = level_mod.Level;
const TableEntry = level_mod.TableEntry;
const KeyRange = level_mod.KeyRange;

const Tree = zdb.tree.Tree;
const TableCache = zdb.tree.TableCache;

const options_mod = zdb.options;

const utils = @import("utils");
test { try utils.runTestsInZioRuntime(@This()); }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns the absolute path of a TmpDir (caller frees).
fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return tmp.dir.realpathAlloc(alloc, ".");
}

/// Builds a TableBuffer containing the given key/value pairs.
/// Keys must be in sorted order (caller's responsibility).
/// Each entry is (key_no_ts, ts, value).
fn buildTable(
    alloc: std.mem.Allocator,
    entries: []const struct { key: []const u8, ts: u64, val: []const u8 },
) !table_mod.builder.TableBuffer {
    var b = Builder.init(alloc, .{ .target_block_size = 4 * 1024 });
    defer b.deinit();
    for (entries) |e| {
        try b.add(.{ .primary = e.key, .timestamp = e.ts }, e.val);
    }
    return b.finish();
}

/// Test helper: create an in-memory FileHandle from a buffer (dupes the data).
fn fromBuf(allocator: std.mem.Allocator, buf: []const u8) !vfs.FileHandle {
    const owned = try allocator.dupe(u8, buf);
    return .{
        .file_size = @intCast(owned.len),
        .owned_buf = owned,
        .buf_allocator = allocator,
        .vfs = vfs.g(),
    };
}

/// Opens a heap-allocated Table from a TableBuffer via VFS FileHandle.
/// Duplicates tb.bytes internally; caller retains ownership of tb.
fn openTableOwned(alloc: std.mem.Allocator, tb: *table_mod.builder.TableBuffer) !*Table {
    return Table.openHandle(alloc, try fromBuf(alloc, tb.bytes), .p0);
}

fn destroyTable(t: *Table) void {
    const allocator = t.allocator;
    t.decrRef();
    allocator.destroy(t);
}

// ---------------------------------------------------------------------------
// 1. level_handler L0 sort
// ---------------------------------------------------------------------------

pub fn testLevelHandlerL0Sort() !void {
    const alloc = std.testing.allocator;

    var tb3 = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "v" }});
    defer tb3.deinit();
    var tb1 = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "v" }});
    defer tb1.deinit();
    var tb2 = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "v" }});
    defer tb2.deinit();

    const t3 = try openTableOwned(alloc, &tb3);
    defer destroyTable(t3);
    const t1 = try openTableOwned(alloc, &tb1);
    defer destroyTable(t1);
    const t2 = try openTableOwned(alloc, &tb2);
    defer destroyTable(t2);

    var level = Level.init(alloc, 0);
    defer level.deinit();

    try level.addEntry(3, t3);
    try level.addEntry(1, t1);
    try level.addEntry(2, t2);

    // L0 sorts ascending by ID.
    try std.testing.expectEqual(@as(u64, 1), level.tables.items[0].id);
    try std.testing.expectEqual(@as(u64, 2), level.tables.items[1].id);
    try std.testing.expectEqual(@as(u64, 3), level.tables.items[2].id);
}

// ---------------------------------------------------------------------------
// 2. level_handler L1 sort
// ---------------------------------------------------------------------------

pub fn testLevelHandlerL1Sort() !void {
    const alloc = std.testing.allocator;

    // Three tables at L1 with non-overlapping key ranges (added out of order).
    var tbc = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "vc" }});
    defer tbc.deinit();
    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "va" }});
    defer tba.deinit();
    var tbb = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "vb" }});
    defer tbb.deinit();

    const tc = try openTableOwned(alloc, &tbc);
    defer destroyTable(tc);
    const ta = try openTableOwned(alloc, &tba);
    defer destroyTable(ta);
    const tb = try openTableOwned(alloc, &tbb);
    defer destroyTable(tb);

    var level = Level.init(alloc, 1);
    defer level.deinit();

    try level.addEntry(3, tc); // "c"
    try level.addEntry(1, ta); // "a"
    try level.addEntry(2, tb); // "b"

    // L1 sorts by smallest key ascending.
    const sk0 = level.tables.items[0].table.smallest.primary;
    const sk1 = level.tables.items[1].table.smallest.primary;
    const sk2 = level.tables.items[2].table.smallest.primary;
    try std.testing.expectEqualStrings("a", sk0);
    try std.testing.expectEqualStrings("b", sk1);
    try std.testing.expectEqualStrings("c", sk2);
}

// ---------------------------------------------------------------------------
// 3. overlapping tables L1
// ---------------------------------------------------------------------------

pub fn testOverlappingTablesL1() !void {
    const alloc = std.testing.allocator;

    // 5 tables with ranges: a-b, c-d, e-f, g-h, i-j
    const ranges = [_][2][]const u8{
        .{ "a", "b" },
        .{ "c", "d" },
        .{ "e", "f" },
        .{ "g", "h" },
        .{ "i", "j" },
    };

    var tables: [5]*Table = undefined;
    var tbs: [5]table_mod.builder.TableBuffer = undefined;
    for (ranges, 0..) |r, i| {
        tbs[i] = try buildTable(alloc, &.{
            .{ .key = r[0], .ts = 0, .val = "v" },
            .{ .key = r[1], .ts = 0, .val = "v" },
        });
        tables[i] = try openTableOwned(alloc, &tbs[i]);
    }
    defer for (&tbs) |*tb| tb.deinit();
    defer for (tables, 0..) |t, i| {
        destroyTable(t);
        _ = i;
    };

    var level = Level.init(alloc, 1);
    defer level.deinit();
    for (tables, 0..) |t, i| {
        try level.addEntry(@intCast(i + 1), t);
    }

    // Query range c-g should overlap tables c-d, e-f, g-h → indices 1..4.
    const ck = try (Key{ .primary = "c", .timestamp = 0 }).marshal(alloc);
    defer alloc.free(ck);
    const gk = try (Key{ .primary = "g", .timestamp = 0 }).marshal(alloc);
    defer alloc.free(gk);

    const result = try level.overlappingTables(alloc, .{ .left = Key.init(ck), .right = Key.init(gk) });
    defer alloc.free(result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
}

// ---------------------------------------------------------------------------
// 4. overlapping tables L0
// ---------------------------------------------------------------------------

pub fn testOverlappingTablesL0() !void {
    const alloc = std.testing.allocator;

    var tb1 = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "v" }});
    defer tb1.deinit();
    var tb2 = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "v" }});
    defer tb2.deinit();
    var tb3 = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "v" }});
    defer tb3.deinit();

    const t1 = try openTableOwned(alloc, &tb1);
    defer destroyTable(t1);
    const t2 = try openTableOwned(alloc, &tb2);
    defer destroyTable(t2);
    const t3 = try openTableOwned(alloc, &tb3);
    defer destroyTable(t3);

    var level = Level.init(alloc, 0);
    defer level.deinit();
    try level.addEntry(1, t1);
    try level.addEntry(2, t2);
    try level.addEntry(3, t3);

    const ck = try (Key{ .primary = "x", .timestamp = 0 }).marshal(alloc);
    defer alloc.free(ck);

    // L0 always returns all tables regardless of the query.
    const result = try level.overlappingTables(alloc, .{ .left = Key.init(ck), .right = Key.init(ck) });
    defer alloc.free(result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
}

// ---------------------------------------------------------------------------
// 5. get L0 newest wins
// ---------------------------------------------------------------------------

pub fn testGetL0NewestWins() !void {
    const alloc = std.testing.allocator;

    // Table ID=1: key "k" at ts=3 → "old"
    // Table ID=2: key "k" at ts=5 → "new"
    var tb1 = try buildTable(alloc, &.{.{ .key = "k", .ts = 3, .val = "old" }});
    defer tb1.deinit();
    var tb2 = try buildTable(alloc, &.{.{ .key = "k", .ts = 5, .val = "new" }});
    defer tb2.deinit();

    const t1 = try openTableOwned(alloc, &tb1);
    defer destroyTable(t1);
    const t2 = try openTableOwned(alloc, &tb2);
    defer destroyTable(t2);

    var level = Level.init(alloc, 0);
    defer level.deinit();
    try level.addEntry(1, t1);
    try level.addEntry(2, t2);

    // Search with max ts to find the newest version.
    const item = try level.get(.{ .primary = "k", .timestamp = std.math.maxInt(u64) }, .p0);
    try std.testing.expect(item != null);
    defer {
        alloc.free(item.?.key.primary);
        alloc.free(item.?.value);
    }
    try std.testing.expectEqualStrings("new", item.?.value);
}

// ---------------------------------------------------------------------------
// 6. get L1 hit
// ---------------------------------------------------------------------------

pub fn testGetL1Hit() !void {
    const alloc = std.testing.allocator;

    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "va" }});
    defer tba.deinit();
    var tbb = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "vb" }});
    defer tbb.deinit();
    var tbc = try buildTable(alloc, &.{.{ .key = "c", .ts = 0, .val = "vc" }});
    defer tbc.deinit();

    const ta = try openTableOwned(alloc, &tba);
    defer destroyTable(ta);
    const tb = try openTableOwned(alloc, &tbb);
    defer destroyTable(tb);
    const tc = try openTableOwned(alloc, &tbc);
    defer destroyTable(tc);

    var level = Level.init(alloc, 1);
    defer level.deinit();
    try level.addEntry(1, ta);
    try level.addEntry(2, tb);
    try level.addEntry(3, tc);

    const item = try level.get(.{ .primary = "b", .timestamp = 0 }, .p0);
    try std.testing.expect(item != null);
    defer {
        alloc.free(item.?.key.primary);
        alloc.free(item.?.value);
    }
    try std.testing.expectEqualStrings("vb", item.?.value);
}

// ---------------------------------------------------------------------------
// 7. get miss
// ---------------------------------------------------------------------------

pub fn testGetMiss() !void {
    const alloc = std.testing.allocator;

    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "va" }});
    defer tba.deinit();
    const ta = try openTableOwned(alloc, &tba);
    defer destroyTable(ta);

    var level = Level.init(alloc, 1);
    defer level.deinit();
    try level.addEntry(1, ta);

    const item = try level.get(.{ .primary = "z", .timestamp = 0 }, .p0);
    try std.testing.expect(item == null);
}

// ---------------------------------------------------------------------------
// 8. levels open
// ---------------------------------------------------------------------------

pub fn testLevelsOpen() !void {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Write SST 1 at L0, SST 2 at L1.
    {
        var tb1 = try buildTable(alloc, &.{.{ .key = "a", .ts = 0, .val = "va" }});
        defer tb1.deinit();
        var tb2 = try buildTable(alloc, &.{.{ .key = "b", .ts = 0, .val = "vb" }});
        defer tb2.deinit();

        const p1 = try std.fmt.allocPrint(alloc, "{s}/000001.sst", .{dir_path});
        defer alloc.free(p1);
        const p2 = try std.fmt.allocPrint(alloc, "{s}/000002.sst", .{dir_path});
        defer alloc.free(p2);

        const f1 = try std.fs.createFileAbsolute(p1, .{});
        defer f1.close();
        try f1.writeAll(tb1.bytes);

        const f2 = try std.fs.createFileAbsolute(p2, .{});
        defer f2.close();
        try f2.writeAll(tb2.bytes);
    }

    // Create manifest with table 1 at L0, table 2 at L1.
    const test_tree_id: u64 = 1;
    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();
    try ms.commit(&.{}, &.{createTreeChange(test_tree_id, 0, &.{}, &.{})});
    try ms.commit(&.{
        createTableChange(1, test_tree_id, 0),
        createTableChange(2, test_tree_id, 1),
    }, &.{});

    const opts = options_mod.Options{ .dir = dir_path };
    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    var tree = try Tree.open(alloc, opts, test_tree_id, &ms, &cache);
    defer tree.close();

    try std.testing.expectEqual(@as(usize, 1), tree.levels[0].numTables());
    try std.testing.expectEqual(@as(usize, 1), tree.levels[1].numTables());
    // All other levels empty.
    for (tree.levels[2..]) |*h| {
        try std.testing.expectEqual(@as(usize, 0), h.numTables());
    }
}

// ---------------------------------------------------------------------------
// 9. pick compact level
// ---------------------------------------------------------------------------

pub fn testPickCompactLevel() !void {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Create an empty manifest (no SST files needed for this test).
    const test_tree_id: u64 = 1;
    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();

    // Use a very small base_level_size so one table in L1 exceeds the target.
    const opts = options_mod.Options{
        .dir = dir_path,
        .base_level_size = 1, // 1 byte → any data exceeds it
        .num_level_zero_tables = 5,
    };

    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    var tree = try Tree.open(alloc, opts, test_tree_id, &ms, &cache);
    defer tree.close();

    // Manually plant a large table in L1.
    var tb = try buildTable(alloc, &.{
        .{ .key = "a", .ts = 0, .val = "va" },
        .{ .key = "b", .ts = 0, .val = "vb" },
    });
    defer tb.deinit();
    const t = try openTableOwned(alloc, &tb);
    try cache.own(999, t);
    try tree.levels[1].addEntry(999, t);
    t.decrRef(); // now levels owns the table.

    try tree.updateCompactCandidate();
    try std.testing.expect(tree.compact_candidate != null);
    try std.testing.expectEqual(@as(u8, 1), tree.compact_candidate.?.level);
}

// ---------------------------------------------------------------------------
// 10. do compact L0→L1
// ---------------------------------------------------------------------------

pub fn testDoCompactL0L1() !void {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // Write three L0 SST files with non-overlapping keys.
    const l0_data = [_]struct { id: u64, key: []const u8, val: []const u8 }{
        .{ .id = 1, .key = "aa", .val = "vaa" },
        .{ .id = 2, .key = "bb", .val = "vbb" },
        .{ .id = 3, .key = "cc", .val = "vcc" },
    };

    const test_tree_id: u64 = 1;
    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();

    for (l0_data) |d| {
        var tb = try buildTable(alloc, &.{.{ .key = d.key, .ts = 0, .val = d.val }});
        defer tb.deinit();
        const path = try options_mod.sstPath(alloc, dir_path, d.id);
        defer alloc.free(path);
        const f = try std.fs.createFileAbsolute(path, .{});
        defer f.close();
        try f.writeAll(tb.bytes);
    }
    try ms.commit(&.{}, &.{createTreeChange(test_tree_id, 0, &.{}, &.{})});
    try ms.commit(&.{
        createTableChange(1, test_tree_id, 0),
        createTableChange(2, test_tree_id, 0),
        createTableChange(3, test_tree_id, 0),
    }, &.{});

    // Use max_levels=2 so base_level=1 and doCompact(0) goes L0→L1.
    const opts = options_mod.Options{ .dir = dir_path, .max_levels = 2 };
    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    var tree = try Tree.open(alloc, opts, test_tree_id, &ms, &cache);
    defer tree.close();

    // All three tables should be in L0 before compaction.
    try std.testing.expectEqual(@as(usize, 3), tree.levels[0].numTables());
    try std.testing.expectEqual(@as(usize, 0), tree.levels[1].numTables());

    var next_file_id = std.atomic.Value(u64).init(100);
    try tree.doCompact(0, &next_file_id);

    // After compaction L0 must be empty and keys must live in L1.
    try std.testing.expectEqual(@as(usize, 0), tree.levels[0].numTables());
    try std.testing.expect(tree.levels[1].numTables() > 0);

    // All three keys must be retrievable via get.
    for (l0_data) |d| {
        const item = try tree.get(.{ .primary = d.key, .timestamp = std.math.maxInt(u64) }, .p0);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key.primary);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings(d.val, item.?.value);
    }
}

// ---------------------------------------------------------------------------
// 11. do compact L0→L1 with dest overlap
// ---------------------------------------------------------------------------

pub fn testDoCompactL0L1WithDestOverlap() !void {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const test_tree_id: u64 = 1;
    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();

    // L0 table 1: {aa→vaa}
    {
        var tb = try buildTable(alloc, &.{.{ .key = "aa", .ts = 0, .val = "vaa" }});
        defer tb.deinit();
        const p = try std.fmt.allocPrint(alloc, "{s}/000001.sst", .{dir_path});
        defer alloc.free(p);
        const f = try std.fs.createFileAbsolute(p, .{});
        defer f.close();
        try f.writeAll(tb.bytes);
    }
    // L0 table 2: {cc→vcc}
    {
        var tb = try buildTable(alloc, &.{.{ .key = "cc", .ts = 0, .val = "vcc" }});
        defer tb.deinit();
        const p = try std.fmt.allocPrint(alloc, "{s}/000002.sst", .{dir_path});
        defer alloc.free(p);
        const f = try std.fs.createFileAbsolute(p, .{});
        defer f.close();
        try f.writeAll(tb.bytes);
    }
    // L1 table 3: {bb→vbb, dd→vdd} — key range [bb..dd] overlaps [aa..cc].
    {
        var tb = try buildTable(alloc, &.{
            .{ .key = "bb", .ts = 0, .val = "vbb" },
            .{ .key = "dd", .ts = 0, .val = "vdd" },
        });
        defer tb.deinit();
        const p = try std.fmt.allocPrint(alloc, "{s}/000003.sst", .{dir_path});
        defer alloc.free(p);
        const f = try std.fs.createFileAbsolute(p, .{});
        defer f.close();
        try f.writeAll(tb.bytes);
    }
    try ms.commit(&.{}, &.{createTreeChange(test_tree_id, 0, &.{}, &.{})});
    try ms.commit(&.{
        createTableChange(1, test_tree_id, 0),
        createTableChange(2, test_tree_id, 0),
        createTableChange(3, test_tree_id, 1),
    }, &.{});

    // max_levels=2: L0 compacts directly to L1 (base_level=1).
    const opts = options_mod.Options{ .dir = dir_path, .max_levels = 2 };
    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    var tree = try Tree.open(alloc, opts, test_tree_id, &ms, &cache);
    defer tree.close();

    try std.testing.expectEqual(@as(usize, 2), tree.levels[0].numTables());
    try std.testing.expectEqual(@as(usize, 1), tree.levels[1].numTables());

    var next_file_id = std.atomic.Value(u64).init(100);
    try tree.doCompact(0, &next_file_id);

    // L0 must be empty; L1 holds the merged result in a single table.
    try std.testing.expectEqual(@as(usize, 0), tree.levels[0].numTables());
    try std.testing.expectEqual(@as(usize, 1), tree.levels[1].numTables());

    // All four keys from both levels must be present after compaction.
    const expected = [_]struct { key: []const u8, val: []const u8 }{
        .{ .key = "aa", .val = "vaa" },
        .{ .key = "bb", .val = "vbb" },
        .{ .key = "cc", .val = "vcc" },
        .{ .key = "dd", .val = "vdd" },
    };
    for (expected) |e| {
        const item = try tree.get(.{ .primary = e.key, .timestamp = std.math.maxInt(u64) }, .p0);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key.primary);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings(e.val, item.?.value);
    }
}

// ---------------------------------------------------------------------------
// 12. do compact L1→L2
// ---------------------------------------------------------------------------

pub fn testDoCompactL1L2() !void {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    // L1: {aa, bb}; L2: {cc} — non-overlapping, so compacting L1 doesn't
    // touch the L2 table; both survive in L2 after compaction.
    const test_tree_id: u64 = 1;
    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();

    {
        var tb1 = try buildTable(alloc, &.{
            .{ .key = "aa", .ts = 0, .val = "vaa" },
            .{ .key = "bb", .ts = 0, .val = "vbb" },
        });
        defer tb1.deinit();
        const p1 = try std.fmt.allocPrint(alloc, "{s}/000001.sst", .{dir_path});
        defer alloc.free(p1);
        const f1 = try std.fs.createFileAbsolute(p1, .{});
        defer f1.close();
        try f1.writeAll(tb1.bytes);

        var tb2 = try buildTable(alloc, &.{.{ .key = "cc", .ts = 0, .val = "vcc" }});
        defer tb2.deinit();
        const p2 = try std.fmt.allocPrint(alloc, "{s}/000002.sst", .{dir_path});
        defer alloc.free(p2);
        const f2 = try std.fs.createFileAbsolute(p2, .{});
        defer f2.close();
        try f2.writeAll(tb2.bytes);
    }
    try ms.commit(&.{}, &.{createTreeChange(test_tree_id, 0, &.{}, &.{})});
    try ms.commit(&.{
        createTableChange(1, test_tree_id, 1),
        createTableChange(2, test_tree_id, 2),
    }, &.{});

    // max_levels=3: doCompact(1) goes L1→L2 (dest = src + 1 = 2).
    const opts = options_mod.Options{ .dir = dir_path, .max_levels = 3 };
    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    var tree = try Tree.open(alloc, opts, test_tree_id, &ms, &cache);
    defer tree.close();

    try std.testing.expectEqual(@as(usize, 1), tree.levels[1].numTables());
    try std.testing.expectEqual(@as(usize, 1), tree.levels[2].numTables());

    var next_file_id = std.atomic.Value(u64).init(100);
    try tree.doCompact(1, &next_file_id);

    // L1 must be empty; L2 holds the original {cc} table plus new {aa, bb}.
    try std.testing.expectEqual(@as(usize, 0), tree.levels[1].numTables());
    try std.testing.expectEqual(@as(usize, 2), tree.levels[2].numTables());

    const expected = [_]struct { key: []const u8, val: []const u8 }{
        .{ .key = "aa", .val = "vaa" },
        .{ .key = "bb", .val = "vbb" },
        .{ .key = "cc", .val = "vcc" },
    };
    for (expected) |e| {
        const item = try tree.get(.{ .primary = e.key, .timestamp = std.math.maxInt(u64) }, .p2);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key.primary);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings(e.val, item.?.value);
    }
}

// ---------------------------------------------------------------------------
// 13. levels get: L0 shadows L1
// ---------------------------------------------------------------------------

pub fn testLevelsGetL0ShadowsL1() !void {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const test_tree_id: u64 = 1;
    var ms = try manifest.openOrCreate(alloc, dir_path);
    defer ms.close();

    const opts = options_mod.Options{ .dir = dir_path };
    var cache = TableCache.init(alloc, opts.dir);
    defer cache.deinit();
    var tree = try Tree.open(alloc, opts, test_tree_id, &ms, &cache);
    defer tree.close();

    // Plant in-memory tables directly into the level handlers.
    // L1: foo at ts=1 (old version).
    var tb_l1 = try buildTable(alloc, &.{.{ .key = "foo", .ts = 1, .val = "l1_val" }});
    defer tb_l1.deinit();
    const t_l1 = try openTableOwned(alloc, &tb_l1);
    try cache.own(1, t_l1);
    try tree.levels[1].addEntry(1, t_l1);
    t_l1.decrRef();

    // L0: foo at ts=5 (newer version; higher ts sorts earlier in byte order).
    var tb_l0 = try buildTable(alloc, &.{.{ .key = "foo", .ts = 5, .val = "l0_val" }});
    defer tb_l0.deinit();
    const t_l0 = try openTableOwned(alloc, &tb_l0);
    try cache.own(2, t_l0);
    try tree.levels[0].addEntry(2, t_l0);
    t_l0.decrRef();

    // Searching with maxInt ts seeks to the very beginning of "foo" entries;
    // L0 is checked first → the ts=5 version wins.
    {
        const item = try tree.get(.{ .primary = "foo", .timestamp = std.math.maxInt(u64) }, .p1);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key.primary);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings("l0_val", item.?.value);
    }

    // Searching with ts=3: L0's ts=5 entry sorts *before* ts=3 in byte order
    // (higher ts ↔ smaller suffix), so the L0 seek lands past it → L0 miss.
    // L1's ts=1 entry sorts *after* ts=3, so the L1 seek lands on it → hit.
    {
        const item = try tree.get(.{ .primary = "foo", .timestamp = 3 }, .p2);
        try std.testing.expect(item != null);
        defer {
            alloc.free(item.?.key.primary);
            alloc.free(item.?.value);
        }
        try std.testing.expectEqualStrings("l1_val", item.?.value);
    }
}
