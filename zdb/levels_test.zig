// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const utils = @import("utils");
const flags = @import("flags");
const String = utils.String;
const zdb = @import("zdb");

const vfs = zdb.vfs;

const Key = zdb.kv.Key;
const _table = zdb.table;
const Builder = _table.builder.Builder;
const Table = _table.Table;
const TableIterator = _table.TableIterator;

const _manifest = zdb.manifest;
const createSSTableChange = _manifest.createSSTableChange;
const createTreeChange = _manifest.createTreeChange;

const _level = zdb.level;
const Level = _level.Level;
const TableEntry = _level.TableEntry;
const KeyRange = zdb.kv.KeyRange;

const Tree = zdb.tree.Tree;
const TableCache = zdb.tree.TableCache;

const config = zdb.config;
const buildCompaction = @import("testing.zig").buildCompaction;
const openBuffer = @import("testing.zig").openBuffer;
const probeExtent = @import("testing.zig").probeExtent;

/// A bottom target of one byte makes a push L0's job at any size
/// (`pickJob`), so compacting L0 here pushes into L1 as the tests
/// expect, instead of L0 asking for nothing or merging within itself.
const push_lmax: i64 = 1;

/// Build the compaction the tree wants (`compact_candidate`) and apply
/// it, listed under log index `index`: what the engine does across a
/// proposal.
fn compact(tree: *Tree, index: u64, snapshot_ts: u64) !void {
    var changes = try buildCompaction(tree, snapshot_ts);
    defer changes.deinit(tree.allocator);
    try tree.applyCompaction(index, changes.items);
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Builds a TableBuffer containing the given key/value pairs.
/// Keys must be in sorted order (caller's responsibility).
/// Each entry is (key_no_ts, ts, value).
fn buildTable(
    alloc: std.mem.Allocator,
    entries: []const struct { key: String, ts: u64, val: String },
) !_table.builder.TableBuffer {
    var metas: std.ArrayList(MetaEntry) = .empty;
    defer metas.deinit(alloc);
    for (entries) |e| {
        try metas.append(alloc, .{ .key = e.key, .ts = e.ts, .val = e.val });
    }
    return buildTableMeta(alloc, metas.items);
}

/// One row for buildTableMeta. Named rather than anonymous so helpers can
/// pass the same slice along — Zig gives each anonymous struct literal its
/// own distinct type.
const MetaEntry = struct {
    key: String,
    ts: u64,
    val: String,
    meta: zdb.kv.Meta = .{},
};

/// buildTable, but each entry carries a Meta. Entries must already be in
/// sort order: primary ascending, then timestamp DESCENDING. A run of
/// entries with one key is written as one item, versions newest first.
fn buildTableMeta(
    alloc: std.mem.Allocator,
    entries: []const MetaEntry,
) !_table.builder.TableBuffer {
    var b = Builder.init(alloc, .{ .block_bytes = 4 * 1024 });
    defer b.deinit();
    var vers: std.ArrayList(zdb.kv.Version) = .empty;
    defer vers.deinit(alloc);
    var i: usize = 0;
    while (i < entries.len) {
        const key = entries[i].key;
        vers.clearRetainingCapacity();
        while (i < entries.len) : (i += 1) {
            const e = entries[i];
            if (!std.mem.eql(u8, e.key, key)) break;
            try vers.append(alloc, .{
                .ts = e.ts,
                .meta = e.meta,
                .value = e.val,
            });
        }
        try b.addItem(.{
            .key = .{ .primary = key },
            .versions = .{ .many = vers.items },
        });
    }
    return b.finish();
}

/// Install `tb` as SST `id` at `level` for `tree_id`, in the store + in
/// the manifest.
fn installTable(
    alloc: std.mem.Allocator,
    manifest: *_manifest.Manifest,
    store: *vfs.Store,
    tb: *_table.builder.TableBuffer,
    id: u64,
    tree_id: u64,
    level: u32,
) !void {
    const name = try zdb.sstName(alloc, id);
    defer alloc.free(name);
    try store.put(name, tb.bytes);
    // No raft here: the id doubles as the log index, so the fixtures
    // install in id order and L0 sorts the way each test expects.
    try manifest.commit(
        id,
        &.{createSSTableChange(id, 0, tree_id, level, tb.extent())},
        &.{},
    );
}

/// The extent of SST `id` already put in `store`, for a CREATE whose
/// TableBuffer is gone: an open checks the recorded size.
fn storedExtent(store: *vfs.Store, id: u64) !_manifest.Extent {
    const alloc = std.testing.allocator;
    const name = try zdb.sstName(alloc, id);
    defer alloc.free(name);
    return probeExtent(alloc, store, name);
}

/// Sum of stale_data_size across a level's tables. buildCompaction records it
/// per output SST; nothing in production reads it back yet, so the test reaches
/// into the index directly rather than through a helper on Level.
fn levelStaleBytes(level: *Level) u64 {
    var total: u64 = 0;
    for (level.tables.items) |e| total += e.table.index.stale_data_size;
    return total;
}

/// Opens a heap-allocated Table from a TableBuffer, memory-backed.
/// Duplicates tb.bytes internally; caller retains ownership of tb.
fn openTableOwned(alloc: std.mem.Allocator, tb: *_table.builder.TableBuffer) !*Table {
    return openBuffer(alloc, tb);
}

fn destroyTable(t: *Table) void {
    t.decrRef(); // the test's ref is the last one; this frees the table
}

// ---------------------------------------------------------------------------
// 1. level_handler L0 sort
// ---------------------------------------------------------------------------

test "levels: handler L0 sort" {
    const alloc = std.testing.allocator;

    var tb3 = try buildTable(alloc, &.{.{ .key = "c", .ts = 1, .val = "v" }});
    defer tb3.deinit();
    var tb1 = try buildTable(alloc, &.{.{ .key = "a", .ts = 1, .val = "v" }});
    defer tb1.deinit();
    var tb2 = try buildTable(alloc, &.{.{ .key = "b", .ts = 1, .val = "v" }});
    defer tb2.deinit();

    const t3 = try openTableOwned(alloc, &tb3);
    defer destroyTable(t3);
    const t1 = try openTableOwned(alloc, &tb1);
    defer destroyTable(t1);
    const t2 = try openTableOwned(alloc, &tb2);
    defer destroyTable(t2);

    var lock: zdb.level.Lock = .{};
    defer lock.deinit();

    var level = Level.init(alloc, 0, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();

    try level.addEntry(held, 3, 3, t3);
    try level.addEntry(held, 1, 1, t1);
    try level.addEntry(held, 2, 2, t2);

    // L0 sorts ascending by raft_index (here equal to the id).
    try std.testing.expectEqual(@as(u64, 1), level.tables.items[0].id);
    try std.testing.expectEqual(@as(u64, 2), level.tables.items[1].id);
    try std.testing.expectEqual(@as(u64, 3), level.tables.items[2].id);
}

test "levels: handler L0 sorts by raft_index, not by id" {
    const alloc = std.testing.allocator;

    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 1, .val = "v" }});
    defer tba.deinit();
    var tbb = try buildTable(alloc, &.{.{ .key = "b", .ts = 1, .val = "v" }});
    defer tbb.deinit();
    var tbc = try buildTable(alloc, &.{.{ .key = "c", .ts = 1, .val = "v" }});
    defer tbc.deinit();

    const ta = try openTableOwned(alloc, &tba);
    defer destroyTable(ta);
    const tb = try openTableOwned(alloc, &tbb);
    defer destroyTable(tb);
    const tc = try openTableOwned(alloc, &tbc);
    defer destroyTable(tc);

    var lock: zdb.level.Lock = .{};
    defer lock.deinit();
    var level = Level.init(alloc, 0, &lock);
    const held = lock.hold(.exclusive, @src());
    defer held.release();
    defer level.deinit();

    // Ids run opposite to the order the tables were introduced in, which
    // is what random ids do. Only raft_index says which one is newest.
    try level.addEntry(held, 900, 10, ta);
    try level.addEntry(held, 700, 20, tb);
    try level.addEntry(held, 800, 30, tc);

    try std.testing.expectEqual(@as(u64, 900), level.tables.items[0].id);
    try std.testing.expectEqual(@as(u64, 700), level.tables.items[1].id);
    try std.testing.expectEqual(@as(u64, 800), level.tables.items[2].id);
}

// ---------------------------------------------------------------------------
// 2. level_handler L1 sort
// ---------------------------------------------------------------------------

test "levels: handler L1 sort" {
    const alloc = std.testing.allocator;

    // Three tables at L1 with non-overlapping key ranges (added out of order).
    var tbc = try buildTable(alloc, &.{.{ .key = "c", .ts = 1, .val = "vc" }});
    defer tbc.deinit();
    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 1, .val = "va" }});
    defer tba.deinit();
    var tbb = try buildTable(alloc, &.{.{ .key = "b", .ts = 1, .val = "vb" }});
    defer tbb.deinit();

    const tc = try openTableOwned(alloc, &tbc);
    defer destroyTable(tc);
    const ta = try openTableOwned(alloc, &tba);
    defer destroyTable(ta);
    const tb = try openTableOwned(alloc, &tbb);
    defer destroyTable(tb);

    var lock: zdb.level.Lock = .{};
    defer lock.deinit();

    var level = Level.init(alloc, 1, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();

    try level.addEntry(held, 3, 3, tc); // "c"
    try level.addEntry(held, 1, 1, ta); // "a"
    try level.addEntry(held, 2, 2, tb); // "b"

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

test "levels: overlapping tables L1" {
    const alloc = std.testing.allocator;

    // 5 tables with ranges: a-b, c-d, e-f, g-h, i-j
    const ranges = [_][2]String{
        .{ "a", "b" },
        .{ "c", "d" },
        .{ "e", "f" },
        .{ "g", "h" },
        .{ "i", "j" },
    };

    var tables: [5]*Table = undefined;
    var tbs: [5]_table.builder.TableBuffer = undefined;
    for (ranges, 0..) |r, i| {
        tbs[i] = try buildTable(alloc, &.{
            .{ .key = r[0], .ts = 1, .val = "v" },
            .{ .key = r[1], .ts = 1, .val = "v" },
        });
        tables[i] = try openTableOwned(alloc, &tbs[i]);
    }
    defer for (&tbs) |*tb| tb.deinit();
    defer for (tables, 0..) |t, i| {
        destroyTable(t);
        _ = i;
    };

    var lock: zdb.level.Lock = .{};
    defer lock.deinit();

    var level = Level.init(alloc, 1, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    for (tables, 0..) |t, i| {
        try level.addEntry(held, @intCast(i + 1), @intCast(i + 1), t);
    }

    // Query range c-g should overlap tables c-d, e-f, g-h → indices 1..4.
    const ck = try (Key{ .primary = "c" }).marshal(alloc);
    defer alloc.free(ck);
    const gk = try (Key{ .primary = "g" }).marshal(alloc);
    defer alloc.free(gk);

    const result = try level.overlappingTables(
        held.shared(),
        alloc,
        .{ .left = Key.init(ck), .right = Key.init(gk) },
    );
    defer Level.releaseTables(alloc, result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
}

// ---------------------------------------------------------------------------
// 4. overlapping tables L0
// ---------------------------------------------------------------------------

test "levels: overlapping tables L0" {
    const alloc = std.testing.allocator;

    var tb1 = try buildTable(alloc, &.{.{ .key = "a", .ts = 1, .val = "v" }});
    defer tb1.deinit();
    var tb2 = try buildTable(alloc, &.{.{ .key = "b", .ts = 1, .val = "v" }});
    defer tb2.deinit();
    var tb3 = try buildTable(alloc, &.{.{ .key = "c", .ts = 1, .val = "v" }});
    defer tb3.deinit();

    const t1 = try openTableOwned(alloc, &tb1);
    defer destroyTable(t1);
    const t2 = try openTableOwned(alloc, &tb2);
    defer destroyTable(t2);
    const t3 = try openTableOwned(alloc, &tb3);
    defer destroyTable(t3);

    var lock: zdb.level.Lock = .{};
    defer lock.deinit();

    var level = Level.init(alloc, 0, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    try level.addEntry(held, 1, 1, t1);
    try level.addEntry(held, 2, 2, t2);
    try level.addEntry(held, 3, 3, t3);

    const ck = try (Key{ .primary = "x" }).marshal(alloc);
    defer alloc.free(ck);

    // L0 always returns all tables regardless of the query.
    const result = try level.overlappingTables(
        held.shared(),
        alloc,
        .{ .left = Key.init(ck), .right = Key.init(ck) },
    );
    defer Level.releaseTables(alloc, result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
}

// ---------------------------------------------------------------------------
// 5. get L0 newest wins
// ---------------------------------------------------------------------------

test "levels: get L0 newest wins" {
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

    var lock: zdb.level.Lock = .{};
    defer lock.deinit();

    var level = Level.init(alloc, 0, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    try level.addEntry(held, 1, 1, t1);
    try level.addEntry(held, 2, 2, t2);

    // Search with max ts to find the newest version.
    const item = try level.get(held.shared(), .{ .key = .{ .primary = "k" } });
    try std.testing.expect(item != null);
    defer item.?.free(alloc);
    try std.testing.expectEqualStrings("new", item.?.newest().value);
}

// ---------------------------------------------------------------------------
// 6. get L1 hit
// ---------------------------------------------------------------------------

test "levels: get L1 hit" {
    const alloc = std.testing.allocator;

    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 1, .val = "va" }});
    defer tba.deinit();
    var tbb = try buildTable(alloc, &.{.{ .key = "b", .ts = 1, .val = "vb" }});
    defer tbb.deinit();
    var tbc = try buildTable(alloc, &.{.{ .key = "c", .ts = 1, .val = "vc" }});
    defer tbc.deinit();

    const ta = try openTableOwned(alloc, &tba);
    defer destroyTable(ta);
    const tb = try openTableOwned(alloc, &tbb);
    defer destroyTable(tb);
    const tc = try openTableOwned(alloc, &tbc);
    defer destroyTable(tc);

    var lock: zdb.level.Lock = .{};
    defer lock.deinit();

    var level = Level.init(alloc, 1, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    try level.addEntry(held, 1, 1, ta);
    try level.addEntry(held, 2, 2, tb);
    try level.addEntry(held, 3, 3, tc);

    const item = try level.get(held.shared(), .{ .key = .{ .primary = "b" } });
    try std.testing.expect(item != null);
    defer item.?.free(alloc);
    try std.testing.expectEqualStrings("vb", item.?.newest().value);
}

// ---------------------------------------------------------------------------
// 7. get miss
// ---------------------------------------------------------------------------

test "levels: get miss" {
    const alloc = std.testing.allocator;

    var tba = try buildTable(alloc, &.{.{ .key = "a", .ts = 1, .val = "va" }});
    defer tba.deinit();
    const ta = try openTableOwned(alloc, &tba);
    defer destroyTable(ta);

    var lock: zdb.level.Lock = .{};
    defer lock.deinit();

    var level = Level.init(alloc, 1, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    try level.addEntry(held, 1, 1, ta);

    const item = try level.get(held.shared(), .{ .key = .{ .primary = "z" } });
    try std.testing.expect(item == null);
}

// ---------------------------------------------------------------------------
// 8. levels open
// ---------------------------------------------------------------------------

test "levels: open" {
    const alloc = std.testing.allocator;

    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;
    var store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    defer store.deinit();

    // Write SST 1 at L0, SST 2 at L1.
    {
        var tb1 = try buildTable(alloc, &.{
            .{ .key = "a", .ts = 1, .val = "va" },
        });
        defer tb1.deinit();
        var tb2 = try buildTable(alloc, &.{
            .{ .key = "b", .ts = 1, .val = "vb" },
        });
        defer tb2.deinit();

        const name1 = try zdb.sstName(alloc, 1);
        defer alloc.free(name1);
        const name2 = try zdb.sstName(alloc, 2);
        defer alloc.free(name2);

        try store.put(name1, tb1.bytes);
        try store.put(name2, tb2.bytes);
    }

    // Create manifest with table 1 at L0, table 2 at L1.
    const test_tree_id: u64 = 1;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();
    try manifest.commit(1, &.{}, &.{
        createTreeChange(test_tree_id, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(2, &.{
        createSSTableChange(1, 0, test_tree_id, 0, try storedExtent(&store, 1)),
        createSSTableChange(2, 0, test_tree_id, 1, try storedExtent(&store, 2)),
    }, &.{});

    const opts = config.Config{};
    var cache = TableCache.init(alloc, &store);
    defer cache.deinit();
    var live = flags.Live(config.Config).initFrom(alloc, opts);
    defer live.deinit();
    const tree = try Tree.open(
        alloc,
        &live,
        0,
        test_tree_id,
        &manifest,
        &cache,
    );
    defer tree.release();

    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(0));
    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(1));
    // All other levels empty.
    for (2..tree.levels.len) |i| {
        try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(i));
    }
}

// ---------------------------------------------------------------------------
// 9. pick compact level
// ---------------------------------------------------------------------------

test "levels: pick compact level" {
    const alloc = std.testing.allocator;

    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;
    var store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    defer store.deinit();

    // Create an empty manifest (no SST files needed for this test).
    const test_tree_id: u64 = 1;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    // Targets cascade from `lmax_target_bytes`; setting it to a single
    // byte makes every level's target tiny so one table in L1 exceeds it
    // and becomes the compaction candidate.
    const opts = config.Config{
        .lmax_target_bytes = 1,
    };

    var cache = TableCache.init(alloc, &store);
    defer cache.deinit();
    var live = flags.Live(config.Config).initFrom(alloc, opts);
    defer live.deinit();
    const tree = try Tree.open(
        alloc,
        &live,
        0,
        test_tree_id,
        &manifest,
        &cache,
    );
    defer tree.release();

    // Manually plant a large table in L1.
    var tb = try buildTable(alloc, &.{
        .{ .key = "a", .ts = 1, .val = "va" },
        .{ .key = "b", .ts = 1, .val = "vb" },
    });
    defer tb.deinit();
    const t = try openTableOwned(alloc, &tb);
    _ = try cache.own(999, t);
    {
        const h = tree.mu.hold(.exclusive, @src());
        defer h.release();
        try tree.levels[1].addEntry(h, 999, 999, t);
    }
    t.decrRef(); // now levels owns the table.

    try tree.updateCompactCandidate();
    try std.testing.expect(tree.compact_candidate != null);
    try std.testing.expectEqual(@as(u8, 1), tree.compact_candidate.?.level);
}

// ---------------------------------------------------------------------------
// 10. compact L0→L1
// ---------------------------------------------------------------------------

test "levels: compact L0 L1" {
    const alloc = std.testing.allocator;

    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;
    var store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    defer store.deinit();

    // Write three L0 SST files with non-overlapping keys.
    const l0_data = [_]struct { id: u64, key: String, val: String }{
        .{ .id = 1, .key = "aa", .val = "vaa" },
        .{ .id = 2, .key = "bb", .val = "vbb" },
        .{ .id = 3, .key = "cc", .val = "vcc" },
    };

    const test_tree_id: u64 = 1;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    for (l0_data) |d| {
        var tb = try buildTable(alloc, &.{
            .{ .key = d.key, .ts = 1, .val = d.val },
        });
        defer tb.deinit();
        const name = try zdb.sstName(alloc, d.id);
        defer alloc.free(name);
        try store.put(name, tb.bytes);
    }
    try manifest.commit(1, &.{}, &.{
        createTreeChange(test_tree_id, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(2, &.{
        createSSTableChange(1, 0, test_tree_id, 0, try storedExtent(&store, 1)),
        createSSTableChange(2, 0, test_tree_id, 0, try storedExtent(&store, 2)),
        createSSTableChange(3, 0, test_tree_id, 0, try storedExtent(&store, 3)),
    }, &.{});

    // Use max_levels=2 so base_level=1 and the compaction goes L0→L1.
    const opts = config.Config{
        .max_levels = 2,
        .lmax_target_bytes = push_lmax,
    };
    var cache = TableCache.init(alloc, &store);
    defer cache.deinit();
    var live = flags.Live(config.Config).initFrom(alloc, opts);
    defer live.deinit();
    const tree = try Tree.open(
        alloc,
        &live,
        0,
        test_tree_id,
        &manifest,
        &cache,
    );
    defer tree.release();

    // All three tables should be in L0 before compaction.
    try std.testing.expectEqual(@as(usize, 3), tree.numTablesAt(0));
    try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(1));

    try compact(tree, 1, 0);

    // After compaction L0 must be empty and keys must live in L1.
    try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(0));
    try std.testing.expect(tree.numTablesAt(1) > 0);

    // All three keys must be retrievable via get.
    for (l0_data) |d| {
        const item = try tree.get(.{ .key = .{ .primary = d.key } });
        try std.testing.expect(item != null);
        defer item.?.free(alloc);
        try std.testing.expectEqualStrings(d.val, item.?.newest().value);
    }
}

// ---------------------------------------------------------------------------
// 11. do compact L0→L1 with dest overlap
// ---------------------------------------------------------------------------

test "levels: compact L0 L1 with dest overlap" {
    const alloc = std.testing.allocator;

    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;
    var store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    defer store.deinit();

    const test_tree_id: u64 = 1;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    // L0 table 1: {aa→vaa}
    {
        var tb = try buildTable(alloc, &.{
            .{ .key = "aa", .ts = 1, .val = "vaa" },
        });
        defer tb.deinit();
        const name = try zdb.sstName(alloc, 1);
        defer alloc.free(name);
        try store.put(name, tb.bytes);
    }
    // L0 table 2: {cc→vcc}
    {
        var tb = try buildTable(alloc, &.{
            .{ .key = "cc", .ts = 1, .val = "vcc" },
        });
        defer tb.deinit();
        const name = try zdb.sstName(alloc, 2);
        defer alloc.free(name);
        try store.put(name, tb.bytes);
    }
    // L1 table 3: {bb→vbb, dd→vdd} — key range [bb..dd] overlaps [aa..cc].
    {
        var tb = try buildTable(alloc, &.{
            .{ .key = "bb", .ts = 1, .val = "vbb" },
            .{ .key = "dd", .ts = 1, .val = "vdd" },
        });
        defer tb.deinit();
        const name = try zdb.sstName(alloc, 3);
        defer alloc.free(name);
        try store.put(name, tb.bytes);
    }
    try manifest.commit(1, &.{}, &.{
        createTreeChange(test_tree_id, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(2, &.{
        createSSTableChange(1, 0, test_tree_id, 0, try storedExtent(&store, 1)),
        createSSTableChange(2, 0, test_tree_id, 0, try storedExtent(&store, 2)),
        createSSTableChange(3, 0, test_tree_id, 1, try storedExtent(&store, 3)),
    }, &.{});

    // max_levels=2: L0 compacts directly to L1 (base_level=1).
    const opts = config.Config{
        .max_levels = 2,
        .lmax_target_bytes = push_lmax,
    };
    var cache = TableCache.init(alloc, &store);
    defer cache.deinit();
    var live = flags.Live(config.Config).initFrom(alloc, opts);
    defer live.deinit();
    const tree = try Tree.open(
        alloc,
        &live,
        0,
        test_tree_id,
        &manifest,
        &cache,
    );
    defer tree.release();

    try std.testing.expectEqual(@as(usize, 2), tree.numTablesAt(0));
    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(1));

    try compact(tree, 1, 0);

    // L0 must be empty; L1 holds the merged result in a single table.
    try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(0));
    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(1));

    // All four keys from both levels must be present after compaction.
    const expected = [_]struct { key: String, val: String }{
        .{ .key = "aa", .val = "vaa" },
        .{ .key = "bb", .val = "vbb" },
        .{ .key = "cc", .val = "vcc" },
        .{ .key = "dd", .val = "vdd" },
    };
    for (expected) |e| {
        const item = try tree.get(.{ .key = .{ .primary = e.key } });
        try std.testing.expect(item != null);
        defer item.?.free(alloc);
        try std.testing.expectEqualStrings(e.val, item.?.newest().value);
    }
}

test "levels: an L0 push splits into a fiber per L1 range" {
    const alloc = std.testing.allocator;

    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;
    var store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    defer store.deinit();

    // L0 holds aa, cc, ee, gg in two tables; L1 holds bb, dd, ff, one
    // table each. Three fibers get [-inf, dd), [dd, ff) and [ff, inf):
    // one output each, every key present afterwards.
    const test_tree_id: u64 = 1;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();
    const l0 = [_][]const MetaEntry{
        &.{ .{ .key = "aa", .ts = 1, .val = "vaa" }, .{ .key = "ee", .ts = 1, .val = "vee" } },
        &.{ .{ .key = "cc", .ts = 1, .val = "vcc" }, .{ .key = "gg", .ts = 1, .val = "vgg" } },
    };
    const l1 = [_][]const MetaEntry{
        &.{.{ .key = "bb", .ts = 1, .val = "vbb" }},
        &.{.{ .key = "dd", .ts = 1, .val = "vdd" }},
        &.{.{ .key = "ff", .ts = 1, .val = "vff" }},
    };
    var id: u64 = 1;
    var changes: std.ArrayList(_manifest.SSTableChange) = .empty;
    defer changes.deinit(alloc);
    for ([_]struct { tables: []const []const MetaEntry, level: u8 }{
        .{ .tables = &l0, .level = 0 },
        .{ .tables = &l1, .level = 1 },
    }) |lvl| {
        for (lvl.tables) |entries| {
            var tb = try buildTableMeta(alloc, entries);
            defer tb.deinit();
            const name = try zdb.sstName(alloc, id);
            defer alloc.free(name);
            try store.put(name, tb.bytes);
            try changes.append(alloc, createSSTableChange(
                id,
                0,
                test_tree_id,
                lvl.level,
                tb.extent(),
            ));
            id += 1;
        }
    }
    try manifest.commit(1, &.{}, &.{
        createTreeChange(test_tree_id, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(2, changes.items, &.{});

    const opts = config.Config{
        .max_levels = 2,
        .lmax_target_bytes = push_lmax,
        .compaction_fibers = 3,
    };
    var cache = TableCache.init(alloc, &store);
    defer cache.deinit();
    var live = flags.Live(config.Config).initFrom(alloc, opts);
    defer live.deinit();
    const tree = try Tree.open(alloc, &live, 0, test_tree_id, &manifest, &cache);
    defer tree.release();

    try std.testing.expectEqual(@as(usize, 2), tree.numTablesAt(0));
    try std.testing.expectEqual(@as(usize, 3), tree.numTablesAt(1));

    try compact(tree, 1, 0);

    try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(0));
    try std.testing.expectEqual(@as(usize, 3), tree.numTablesAt(1));
    for ([_][]const u8{ "aa", "bb", "cc", "dd", "ee", "ff", "gg" }) |k| {
        const item = try tree.get(.{ .key = .{ .primary = k } });
        try std.testing.expect(item != null);
        defer item.?.free(alloc);
        try std.testing.expectEqualStrings(k, item.?.newest().value[1..]);
    }
}

test "levels: an L1 merge splits into a fiber per L2 range" {
    const alloc = std.testing.allocator;

    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;
    var store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    defer store.deinit();

    // L1 holds aa, cc, ee, gg in one table; L2 holds bb, dd, ff, one
    // table each. The L1 table overlaps all three, so it merges down,
    // and three fibers get [-inf, dd), [dd, ff) and [ff, inf): one
    // output each, every key present afterwards. The tree belongs to
    // SQL table 16: the partition bounds are cut from L2 tables' keys,
    // which a Table holds as bare primaries, and the iterators assert
    // that a seek key carries the tree's own table id (a tree id of 1
    // has table id 0, which the bare keys matched by accident).
    const test_tree_id: u64 = 16 << 32;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();
    const l1 = [_][]const MetaEntry{
        &.{
            .{ .key = "aa", .ts = 1, .val = "vaa" },
            .{ .key = "cc", .ts = 1, .val = "vcc" },
            .{ .key = "ee", .ts = 1, .val = "vee" },
            .{ .key = "gg", .ts = 1, .val = "vgg" },
        },
    };
    const l2 = [_][]const MetaEntry{
        &.{.{ .key = "bb", .ts = 1, .val = "vbb" }},
        &.{.{ .key = "dd", .ts = 1, .val = "vdd" }},
        &.{.{ .key = "ff", .ts = 1, .val = "vff" }},
    };
    var id: u64 = 1;
    var changes: std.ArrayList(_manifest.SSTableChange) = .empty;
    defer changes.deinit(alloc);
    for ([_]struct { tables: []const []const MetaEntry, level: u8 }{
        .{ .tables = &l1, .level = 1 },
        .{ .tables = &l2, .level = 2 },
    }) |lvl| {
        for (lvl.tables) |entries| {
            var tb = try buildTableMeta(alloc, entries);
            defer tb.deinit();
            const name = try zdb.sstName(alloc, id);
            defer alloc.free(name);
            try store.put(name, tb.bytes);
            try changes.append(
                alloc,
                createSSTableChange(
                    id,
                    0,
                    test_tree_id,
                    lvl.level,
                    tb.extent(),
                ),
            );
            id += 1;
        }
    }
    try manifest.commit(1, &.{}, &.{
        createTreeChange(test_tree_id, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(2, changes.items, &.{});

    // A one-byte bottom target puts L1 over its target; L0 is empty.
    const opts = config.Config{
        .max_levels = 3,
        .lmax_target_bytes = push_lmax,
        .compaction_fibers = 3,
    };
    var cache = TableCache.init(alloc, &store);
    defer cache.deinit();
    var live = flags.Live(config.Config).initFrom(alloc, opts);
    defer live.deinit();
    const tree = try Tree.open(
        alloc,
        &live,
        0,
        test_tree_id,
        &manifest,
        &cache,
    );
    defer tree.release();

    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(1));
    try std.testing.expectEqual(@as(usize, 3), tree.numTablesAt(2));

    try compact(tree, 1, 0);

    try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(1));
    try std.testing.expectEqual(@as(usize, 3), tree.numTablesAt(2));
    for ([_]String{ "aa", "bb", "cc", "dd", "ee", "ff", "gg" }) |k| {
        const item = try tree.get(.{
            .key = .{ .table_id = 16, .primary = k },
        });
        try std.testing.expect(item != null);
        defer item.?.free(alloc);
        try std.testing.expectEqualStrings(k, item.?.newest().value[1..]);
    }
}

// ---------------------------------------------------------------------------
// 12. do compact L1→L2
// ---------------------------------------------------------------------------

test "levels: compact L1 L2" {
    const alloc = std.testing.allocator;

    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;
    var store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    defer store.deinit();

    // L1: {aa, bb}; L2: {cc} — non-overlapping, so compacting L1 doesn't
    // touch the L2 table; both survive in L2 after compaction. L1's
    // values are padded so its file outweighs L2's: a level's score is
    // dampened by the fullness of the level below, and under the
    // one-byte target that is the ratio of their sizes. Files are 4 KiB
    // multiples, so the padding has to carry L1 past one.
    const vaa = "vaa" ++ "." ** 3072;
    const vbb = "vbb" ++ "." ** 3072;
    const test_tree_id: u64 = 1;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    {
        var tb1 = try buildTable(alloc, &.{
            .{ .key = "aa", .ts = 1, .val = vaa },
            .{ .key = "bb", .ts = 1, .val = vbb },
        });
        defer tb1.deinit();
        const name1 = try zdb.sstName(alloc, 1);
        defer alloc.free(name1);
        try store.put(name1, tb1.bytes);

        var tb2 = try buildTable(alloc, &.{
            .{ .key = "cc", .ts = 1, .val = "vcc" },
        });
        defer tb2.deinit();
        const name2 = try zdb.sstName(alloc, 2);
        defer alloc.free(name2);
        try store.put(name2, tb2.bytes);
    }
    try manifest.commit(1, &.{}, &.{
        createTreeChange(test_tree_id, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(2, &.{
        createSSTableChange(1, 0, test_tree_id, 1, try storedExtent(&store, 1)),
        createSSTableChange(2, 0, test_tree_id, 2, try storedExtent(&store, 2)),
    }, &.{});

    // max_levels=3, and a one-byte bottom target puts L1 over its
    // target: L1 is the candidate, so it goes L1→L2.
    const opts = config.Config{
        .max_levels = 3,
        .lmax_target_bytes = push_lmax,
    };
    var cache = TableCache.init(alloc, &store);
    defer cache.deinit();
    var live = flags.Live(config.Config).initFrom(alloc, opts);
    defer live.deinit();
    const tree = try Tree.open(
        alloc,
        &live,
        0,
        test_tree_id,
        &manifest,
        &cache,
    );
    defer tree.release();

    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(1));
    try std.testing.expectEqual(@as(usize, 1), tree.numTablesAt(2));

    try compact(tree, 1, 0);

    // L1 must be empty; L2 holds the original {cc} table plus new {aa, bb}.
    try std.testing.expectEqual(@as(usize, 0), tree.numTablesAt(1));
    try std.testing.expectEqual(@as(usize, 2), tree.numTablesAt(2));

    const expected = [_]struct { key: String, val: String }{
        .{ .key = "aa", .val = vaa },
        .{ .key = "bb", .val = vbb },
        .{ .key = "cc", .val = "vcc" },
    };
    for (expected) |e| {
        const item = try tree.get(.{
            .key = .{ .primary = e.key },
        });
        try std.testing.expect(item != null);
        defer item.?.free(alloc);
        try std.testing.expectEqualStrings(e.val, item.?.newest().value);
    }
}

// ---------------------------------------------------------------------------
// 13. levels get: L0 shadows L1
// ---------------------------------------------------------------------------

test "levels: get L0 shadows L1" {
    const alloc = std.testing.allocator;

    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;
    var store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    defer store.deinit();

    const test_tree_id: u64 = 1;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    const opts = config.Config{};
    var cache = TableCache.init(alloc, &store);
    defer cache.deinit();
    var live = flags.Live(config.Config).initFrom(alloc, opts);
    defer live.deinit();
    const tree = try Tree.open(
        alloc,
        &live,
        0,
        test_tree_id,
        &manifest,
        &cache,
    );
    defer tree.release();

    // Plant in-memory tables directly into the level handlers.
    // L1: foo at ts=1 (old version).
    var tb_l1 = try buildTable(
        alloc,
        &.{.{ .key = "foo", .ts = 1, .val = "l1_val" }},
    );
    defer tb_l1.deinit();
    const t_l1 = try openTableOwned(alloc, &tb_l1);
    _ = try cache.own(1, t_l1);
    {
        const h = tree.mu.hold(.exclusive, @src());
        defer h.release();
        try tree.levels[1].addEntry(h, 1, 1, t_l1);
    }
    t_l1.decrRef();

    // L0: foo at ts=5 (newer version; higher ts sorts earlier in byte order).
    var tb_l0 = try buildTable(
        alloc,
        &.{.{ .key = "foo", .ts = 5, .val = "l0_val" }},
    );
    defer tb_l0.deinit();
    const t_l0 = try openTableOwned(alloc, &tb_l0);
    _ = try cache.own(2, t_l0);
    {
        const h = tree.mu.hold(.exclusive, @src());
        defer h.release();
        try tree.levels[0].addEntry(h, 2, 2, t_l0);
    }
    t_l0.decrRef();

    // A seek lands on "foo"'s newest version; L0 is checked first → the
    // ts=5 version wins.
    {
        const item = try tree.get(.{
            .key = .{ .primary = "foo" },
        });
        try std.testing.expect(item != null);
        defer item.?.free(alloc);
        try std.testing.expectEqualStrings("l0_val", item.?.newest().value);
    }

    // Reading at ts=3: L0 holds only the ts=5 version, which readKey
    // steps over as too new, so L0 misses. L1's ts=1 version is visible
    // → hit.
    {
        const item = try tree.get(.{
            .key = .{ .primary = "foo" },
            .read_ts = 3,
        });
        try std.testing.expect(item != null);
        defer item.?.free(alloc);
        try std.testing.expectEqualStrings("l1_val", item.?.newest().value);
    }
}

test "levels: overlappingTables finds a dest table sharing the boundary primary" {
    const alloc = std.testing.allocator;

    // Dest table holding an OLDER version of "k", plus a later primary.
    // Sort order is primary asc, then ts DESC, so this is ascending.
    var tb = try buildTable(alloc, &.{
        .{ .key = "k", .ts = 50, .val = "old" },
        .{ .key = "m", .ts = 20, .val = "v" },
    });
    defer tb.deinit();
    const t = try openTableOwned(alloc, &tb);
    defer destroyTable(t);

    var lock: zdb.level.Lock = .{};
    defer lock.deinit();

    var level = Level.init(alloc, 1, &lock);

    const held = lock.hold(.exclusive, @src());

    defer held.release();
    defer level.deinit();
    try level.addEntry(held, 1, 1, t);

    // A src table {(a,10), (k,100), (k,90)} has smallest="a" and
    // biggest="k": the bounds name primaries, whatever versions sit
    // under them. That's the range a compaction probes dest with.
    const left = try (Key{ .primary = "a" }).marshal(alloc);
    defer alloc.free(left);
    const right = try (Key{ .primary = "k" }).marshal(alloc);
    defer alloc.free(right);

    const result = try level.overlappingTables(held.shared(), alloc, .{
        .left = Key.init(left),
        .right = Key.init(right),
    });
    defer Level.releaseTables(alloc, result);

    // The dest table holds "k" and MUST participate: otherwise the compaction
    // writes k@100/k@90 into dest while k@50 stays in a separate dest table,
    // putting two tables at the same level in charge of one primary.
    try std.testing.expectEqual(@as(usize, 1), result.len);
}

// ---------------------------------------------------------------------------
// Tombstone dropping in compaction
// ---------------------------------------------------------------------------

/// Open a one-tree forest-less Tree over `dir_path` with `max_levels`, holding
/// a single L0 SST built from `entries`. Returns the tree; caller closes it.
const TombstoneFixture = struct {
    manifest: _manifest.Manifest,
    store: vfs.Store,
    cache: TableCache,
    live: flags.Live(config.Config),
    tree: *Tree,

    fn deinit(self: *TombstoneFixture) void {
        self.tree.release();
        self.cache.deinit();
        self.store.deinit();
        self.live.deinit();
        self.manifest.deinit();
    }
};

fn setupTombstoneTree(
    alloc: std.mem.Allocator,
    fx: *TombstoneFixture,
    dir_path: String,
    max_levels: u8,
    entries: []const MetaEntry,
) !void {
    const tree_id: u64 = 1;
    fx.store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    errdefer fx.store.deinit();
    fx.manifest = _manifest.Manifest.init(alloc);
    errdefer fx.manifest.deinit();
    try fx.manifest.commit(1, &.{}, &.{
        createTreeChange(tree_id, 0, &.{}, &.{}, .posting_index),
    });

    var tb = try buildTableMeta(alloc, entries);
    defer tb.deinit();
    try installTable(alloc, &fx.manifest, &fx.store, &tb, 1, tree_id, 0);

    const opts = config.Config{
        .max_levels = max_levels,
        .lmax_target_bytes = push_lmax,
    };
    fx.cache = TableCache.init(alloc, &fx.store);
    errdefer fx.cache.deinit();
    fx.live = flags.Live(config.Config).initFrom(alloc, opts);
    errdefer fx.live.deinit();
    fx.tree = try Tree.open(
        alloc,
        &fx.live,
        0,
        tree_id,
        &fx.manifest,
        &fx.cache,
    );
}

/// One SST for `setupPlacedTree`: its id (also the log index it is listed
/// at, so L0 sorts by id), its level, and its egids, each at ts 1 with
/// the egid's decimal text as the value.
const PlacedTable = struct { id: u64, level: u32, egids: []const u64 };

fn egidKey(egid: u64) [8]u8 {
    var k: [8]u8 = undefined;
    std.mem.writeInt(u64, &k, egid, .big);
    return k;
}

/// A Tree of `kind` with `max_levels`, holding `tables` where they
/// say.
fn setupPlacedTree(
    alloc: std.mem.Allocator,
    fx: *TombstoneFixture,
    dir_path: String,
    max_levels: u8,
    kind: _manifest.TreeKind,
    tables: []const PlacedTable,
) !void {
    return setupPlacedTreeCfg(alloc, fx, dir_path, .{
        .max_levels = max_levels,
        .lmax_target_bytes = push_lmax,
    }, kind, tables);
}

fn setupPlacedTreeCfg(
    alloc: std.mem.Allocator,
    fx: *TombstoneFixture,
    dir_path: String,
    opts: config.Config,
    kind: _manifest.TreeKind,
    tables: []const PlacedTable,
) !void {
    const tree_id: u64 = 1;
    fx.store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    errdefer fx.store.deinit();
    fx.manifest = _manifest.Manifest.init(alloc);
    errdefer fx.manifest.deinit();
    const tc = createTreeChange(tree_id, 0, &.{}, &.{}, kind);
    try fx.manifest.commit(1, &.{}, &.{tc});
    for (tables) |pt| {
        var b = Builder.init(alloc, .{
            .block_bytes = 4 * 1024,
            .kind = kind,
        });
        defer b.deinit();
        for (pt.egids) |egid| {
            const k = egidKey(egid);
            var vbuf: [20]u8 = undefined;
            const v = try std.fmt.bufPrint(&vbuf, "{d}", .{egid});
            try b.add(.{ .primary = &k }, 1, v);
        }
        var tb = try b.finish();
        defer tb.deinit();
        try installTable(
            alloc,
            &fx.manifest,
            &fx.store,
            &tb,
            pt.id,
            tree_id,
            pt.level,
        );
    }
    fx.cache = TableCache.init(alloc, &fx.store);
    errdefer fx.cache.deinit();
    fx.live = flags.Live(config.Config).initFrom(alloc, opts);
    errdefer fx.live.deinit();
    fx.tree = try Tree.open(
        alloc,
        &fx.live,
        0,
        tree_id,
        &fx.manifest,
        &fx.cache,
    );
}

/// The SST ids a level lists, ascending.
fn levelIds(alloc: std.mem.Allocator, tree: *Tree, level: usize) ![]u64 {
    const items = tree.levels[level].tables.items;
    const ids = try alloc.alloc(u64, items.len);
    for (items, ids) |e, *id| id.* = e.id;
    std.mem.sortUnstable(u64, ids, {}, std.sort.asc(u64));
    return ids;
}

fn expectEgids(alloc: std.mem.Allocator, tree: *Tree, egids: []const u64) !void {
    for (egids) |egid| {
        const k = egidKey(egid);
        const item = try tree.get(.{ .key = .{ .primary = &k } });
        try std.testing.expect(item != null);
        defer item.?.free(alloc);
        var vbuf: [20]u8 = undefined;
        const want = try std.fmt.bufPrint(&vbuf, "{d}", .{egid});
        try std.testing.expectEqualStrings(want, item.?.newest().value);
    }
}

test "compaction: a push that overlaps nothing below moves its oldest table" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // Disjoint L0 tables of rows over an empty L1 that is not the
    // bottom: the same files end up in L1, none written, none retired.
    // One per compaction, oldest first, so L1 never holds a version
    // younger than one still in L0.
    var fx: TombstoneFixture = undefined;
    try setupPlacedTree(alloc, &fx, dir_path, 3, .rows, &.{
        .{ .id = 2, .level = 0, .egids = &.{1} },
        .{ .id = 3, .level = 0, .egids = &.{ 2, 3 } },
        .{ .id = 4, .level = 0, .egids = &.{4} },
    });
    defer fx.deinit();

    try compact(fx.tree, 10, 0);
    {
        const l0 = try levelIds(alloc, fx.tree, 0);
        defer alloc.free(l0);
        try std.testing.expectEqualSlices(u64, &.{ 3, 4 }, l0);
        const l1 = try levelIds(alloc, fx.tree, 1);
        defer alloc.free(l1);
        try std.testing.expectEqualSlices(u64, &.{2}, l1);
    }
    try compact(fx.tree, 11, 0);
    try compact(fx.tree, 12, 0);

    try std.testing.expectEqual(@as(usize, 0), fx.tree.numTablesAt(0));
    const ids = try levelIds(alloc, fx.tree, 1);
    defer alloc.free(ids);
    try std.testing.expectEqualSlices(u64, &.{ 2, 3, 4 }, ids);
    try std.testing.expectEqual(@as(u32, 0), fx.manifest.to_delete.count());
    try expectEgids(alloc, fx.tree, &.{ 1, 2, 3, 4 });
}

test "compaction: a unique index tree moves like a rows tree" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // One row key per entry, no roaring chains: nothing a rewrite above
    // the bottom would do, so the file moves as it is.
    var fx: TombstoneFixture = undefined;
    try setupPlacedTree(alloc, &fx, dir_path, 3, .unique_index, &.{
        .{ .id = 2, .level = 0, .egids = &.{1} },
    });
    defer fx.deinit();

    try compact(fx.tree, 10, 0);

    const ids = try levelIds(alloc, fx.tree, 1);
    defer alloc.free(ids);
    try std.testing.expectEqualSlices(u64, &.{2}, ids);
    try std.testing.expectEqual(@as(u32, 0), fx.manifest.to_delete.count());
    try expectEgids(alloc, fx.tree, &.{1});
}

test "compaction: overlapping L0 tables are merged, not moved" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // The oldest meets nothing in an empty L1 and moves; the next one
    // then meets it there and is merged into it. L1's target holds one
    // table, so L1 never outscores L0, whose one table left scores
    // 2.0 by its count.
    const cfg = config.Config{
        .max_levels = 3,
        .lmax_target_bytes = 5 * 4096, // L1 target: one 4 KiB table
        .l0_backpressure_start_count = 1,
        .l0_merge_min_tables = 0,
    };
    var fx: TombstoneFixture = undefined;
    try setupPlacedTreeCfg(alloc, &fx, dir_path, cfg, .rows, &.{
        .{ .id = 2, .level = 0, .egids = &.{ 1, 3 } },
        .{ .id = 3, .level = 0, .egids = &.{ 2, 4 } },
    });
    defer fx.deinit();

    try compact(fx.tree, 10, 0);
    {
        const l1 = try levelIds(alloc, fx.tree, 1);
        defer alloc.free(l1);
        try std.testing.expectEqualSlices(u64, &.{2}, l1);
    }

    try compact(fx.tree, 11, 0);

    try std.testing.expectEqual(@as(usize, 0), fx.tree.numTablesAt(0));
    const ids = try levelIds(alloc, fx.tree, 1);
    defer alloc.free(ids);
    try std.testing.expectEqual(@as(usize, 1), ids.len);
    try std.testing.expect(ids[0] != 2 and ids[0] != 3);
    try expectEgids(alloc, fx.tree, &.{ 1, 2, 3, 4 });
}

test "compaction: a table overlapping nothing below moves down a middle level" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    var fx: TombstoneFixture = undefined;
    try setupPlacedTree(alloc, &fx, dir_path, 4, .rows, &.{
        .{ .id = 2, .level = 1, .egids = &.{ 1, 2 } },
        .{ .id = 3, .level = 2, .egids = &.{5} },
    });
    defer fx.deinit();

    try compact(fx.tree, 10, 0);

    try std.testing.expectEqual(@as(usize, 0), fx.tree.numTablesAt(1));
    const ids = try levelIds(alloc, fx.tree, 2);
    defer alloc.free(ids);
    try std.testing.expectEqualSlices(u64, &.{ 2, 3 }, ids);
    try std.testing.expectEqual(@as(u32, 0), fx.manifest.to_delete.count());
    try expectEgids(alloc, fx.tree, &.{ 1, 2, 5 });
}

test "compaction: a push into the bottom level is rewritten, not moved" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // The bottom rewrite is where the floor's discards happen, so it is
    // kept even when nothing below overlaps.
    var fx: TombstoneFixture = undefined;
    try setupPlacedTree(alloc, &fx, dir_path, 2, .rows, &.{
        .{ .id = 2, .level = 0, .egids = &.{1} },
    });
    defer fx.deinit();

    try compact(fx.tree, 10, 0);

    const ids = try levelIds(alloc, fx.tree, 1);
    defer alloc.free(ids);
    try std.testing.expectEqual(@as(usize, 1), ids.len);
    try std.testing.expect(ids[0] != 2);
    try expectEgids(alloc, fx.tree, &.{1});
}

/// Scoring fixture: L1 and L2 both over target, L2 more so in bytes
/// but less so in score, so L1's dampened score is about 1.67 and L0's
/// (four tables, start count 2) is 4. Dampened by L1, L0 would score
/// 0.04 and L1 would win; exempt, L0 wins.
const score_cfg = config.Config{
    .max_levels = 3,
    .lmax_target_bytes = 1000, // L1 target 200 B
    .l0_backpressure_start_count = 2,
    .l0_merge_min_tables = 0,
};

fn egidRun(comptime first: u64, comptime n: usize) [n]u64 {
    var out: [n]u64 = undefined;
    for (&out, 0..) |*e, i| e.* = first + i;
    return out;
}

test "compaction: an L0 push that meets nothing in L1 is not held back by a full L1" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const cfg = score_cfg;
    var fx: TombstoneFixture = undefined;
    try setupPlacedTreeCfg(alloc, &fx, dir_path, cfg, .rows, &.{
        .{ .id = 2, .level = 0, .egids = &.{1} },
        .{ .id = 3, .level = 0, .egids = &.{2} },
        .{ .id = 4, .level = 0, .egids = &.{3} },
        .{ .id = 5, .level = 0, .egids = &.{4} },
        .{ .id = 6, .level = 1, .egids = &egidRun(100, 50) },
        .{ .id = 7, .level = 2, .egids = &egidRun(200, 50) },
        .{ .id = 8, .level = 2, .egids = &egidRun(300, 50) },
        .{ .id = 9, .level = 2, .egids = &egidRun(400, 50) },
    });
    defer fx.deinit();

    // L0's push is a move: it meets nothing in L1, so L1 being twenty
    // times its target is no reason to wait.
    try fx.tree.updateCompactCandidate();
    const cand = fx.tree.compact_candidate orelse return error.TestExpectedCandidate;
    try std.testing.expectEqual(@as(u8, 0), cand.level);
}

test "compaction: an L0 push that overlaps L1 waits for room below" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // L1's first table spans L0's keys, so the push would rewrite it.
    // L1 holds eight tables to L0's four smaller ones: it is the more
    // over-full by bytes as well as by count, and compacts first.
    const cfg = score_cfg;
    var fx: TombstoneFixture = undefined;
    try setupPlacedTreeCfg(alloc, &fx, dir_path, cfg, .rows, &.{
        .{ .id = 2, .level = 0, .egids = &.{101} },
        .{ .id = 3, .level = 0, .egids = &.{102} },
        .{ .id = 4, .level = 0, .egids = &.{103} },
        .{ .id = 5, .level = 0, .egids = &.{104} },
        .{ .id = 6, .level = 1, .egids = &egidRun(100, 50) },
        .{ .id = 7, .level = 1, .egids = &egidRun(1000, 50) },
        .{ .id = 8, .level = 1, .egids = &egidRun(1100, 50) },
        .{ .id = 9, .level = 1, .egids = &egidRun(1200, 50) },
        .{ .id = 10, .level = 1, .egids = &egidRun(1300, 50) },
        .{ .id = 11, .level = 1, .egids = &egidRun(1400, 50) },
        .{ .id = 12, .level = 1, .egids = &egidRun(1500, 50) },
        .{ .id = 13, .level = 1, .egids = &egidRun(1600, 50) },
        .{ .id = 14, .level = 2, .egids = &egidRun(200, 50) },
        .{ .id = 15, .level = 2, .egids = &egidRun(300, 50) },
        .{ .id = 16, .level = 2, .egids = &egidRun(400, 50) },
    });
    defer fx.deinit();

    try fx.tree.updateCompactCandidate();
    const cand = fx.tree.compact_candidate orelse return error.TestExpectedCandidate;
    try std.testing.expectEqual(@as(u8, 1), cand.level);
}

test "compaction: an L0 merge with tables to spare runs ahead of L1" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // Four small L0 tables where a merge takes three: the merge scores
    // above 1.0. With three it would wait for the tree to be idle.
    const cfg = config.Config{ .max_levels = 3, .l0_merge_min_tables = 3 };
    var fx: TombstoneFixture = undefined;
    try setupPlacedTreeCfg(alloc, &fx, dir_path, cfg, .rows, &.{
        .{ .id = 2, .level = 0, .egids = &.{101} },
        .{ .id = 3, .level = 0, .egids = &.{102} },
        .{ .id = 4, .level = 0, .egids = &.{103} },
        .{ .id = 9, .level = 0, .egids = &.{104} },
        .{ .id = 5, .level = 1, .egids = &egidRun(100, 450) },
        .{ .id = 6, .level = 1, .egids = &egidRun(1000, 450) },
        .{ .id = 7, .level = 1, .egids = &egidRun(2000, 450) },
        .{ .id = 8, .level = 1, .egids = &egidRun(3000, 450) },
    });
    defer fx.deinit();

    var l0_bytes: u64 = 0;
    for (fx.tree.levels[0].tables.items) |e| l0_bytes += e.table.fileSize();
    var l1_bytes: u64 = 0;
    for (fx.tree.levels[1].tables.items) |e| l1_bytes += e.table.fileSize();

    // An L1 target just over L0's bytes: no push is due, and L1 is
    // over its target, so it scores above 1.0 and would win the pick.
    const l1_target = l0_bytes + 1;
    try std.testing.expect(l1_bytes > l1_target);
    var buf: [64]u8 = undefined;
    try fx.live.update(try std.fmt.bufPrint(
        &buf,
        "lmax_target_bytes={d}",
        .{5 * l1_target},
    ));

    try fx.tree.updateCompactCandidate();
    const cand = fx.tree.compact_candidate orelse
        return error.TestExpectedCandidate;
    try std.testing.expectEqual(@as(u8, 0), cand.level);
    try std.testing.expect(cand.job == .merge_l0);
}

test "compaction: a tree runs an L0 merge beside its L1 job" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // As above: L0 holds a merge run, L1 is over its target.
    const cfg = config.Config{ .max_levels = 3, .l0_merge_min_tables = 3 };
    var fx: TombstoneFixture = undefined;
    try setupPlacedTreeCfg(alloc, &fx, dir_path, cfg, .rows, &.{
        .{ .id = 2, .level = 0, .egids = &.{101} },
        .{ .id = 3, .level = 0, .egids = &.{102} },
        .{ .id = 4, .level = 0, .egids = &.{103} },
        .{ .id = 9, .level = 0, .egids = &.{104} },
        .{ .id = 5, .level = 1, .egids = &egidRun(100, 450) },
        .{ .id = 6, .level = 1, .egids = &egidRun(1000, 450) },
        .{ .id = 7, .level = 1, .egids = &egidRun(2000, 450) },
        .{ .id = 8, .level = 1, .egids = &egidRun(3000, 450) },
    });
    defer fx.deinit();

    var l0_bytes: u64 = 0;
    for (fx.tree.levels[0].tables.items) |e| l0_bytes += e.table.fileSize();
    var buf: [64]u8 = undefined;
    try fx.live.update(try std.fmt.bufPrint(
        &buf,
        "lmax_target_bytes={d}",
        .{5 * (l0_bytes + 1)},
    ));
    try fx.tree.updateCompactCandidate();

    // The merge is taken first; L0 is then busy, and the tree offers
    // its L1 job in its place.
    const merge = try fx.tree.reserveJob() orelse
        return error.TestExpectedJob;
    try std.testing.expect(merge.job == .merge_l0);
    try std.testing.expect(fx.tree.busy_levels.isSet(0));
    const below = try fx.tree.reserveJob() orelse
        return error.TestExpectedJob;
    try std.testing.expectEqual(@as(u8, 1), below.level);
    try std.testing.expect(below.job == .merge_down);

    // Every level is now busy or writes to a busy one: nothing more.
    try std.testing.expect(fx.tree.compact_candidate == null);
    try std.testing.expect((try fx.tree.reserveJob()) == null);

    // Both build and install, in either order.
    {
        var changes = try fx.tree.buildCompaction(below, 0);
        defer changes.deinit(alloc);
        try fx.tree.applyCompaction(10, changes.items);
    }
    {
        var changes = try fx.tree.buildCompaction(merge, 0);
        defer changes.deinit(alloc);
        try fx.tree.applyCompaction(11, changes.items);
    }
    try std.testing.expectEqual(@as(usize, 1), fx.tree.numTablesAt(0));
    try std.testing.expect(fx.tree.numTablesAt(2) > 0);

    fx.tree.releaseJob(merge);
    fx.tree.releaseJob(below);
    try std.testing.expectEqual(@as(usize, 0), fx.tree.busy_levels.count());
    try expectEgids(alloc, fx.tree, &.{ 101, 102, 103, 104, 100, 549, 3449 });
}

test "compaction: a push with few tables scores by L0's bytes" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // Two L0 tables against a start count of 32: 0.125 by count. They
    // span L1's one smaller table, so the push is a rewrite.
    const cfg = config.Config{ .max_levels = 3 };
    var fx: TombstoneFixture = undefined;
    try setupPlacedTreeCfg(alloc, &fx, dir_path, cfg, .rows, &.{
        .{ .id = 2, .level = 0, .egids = &egidRun(100, 450) },
        .{ .id = 3, .level = 0, .egids = &egidRun(1000, 450) },
        .{ .id = 4, .level = 1, .egids = &egidRun(200, 50) },
    });
    defer fx.deinit();

    // Under the default target L0 is left to fill: no push is due, two
    // tables are too few to merge, and the tree has no candidate.
    try fx.tree.updateCompactCandidate();
    try std.testing.expect(fx.tree.compact_candidate == null);

    var l0_bytes: u64 = 0;
    for (fx.tree.levels[0].tables.items) |e| l0_bytes += e.table.fileSize();
    const l1_bytes = fx.tree.levels[1].tables.items[0].table.fileSize();
    try std.testing.expect(l0_bytes > l1_bytes);

    // An L1 target just under L1's bytes: L1 scores a little over 1.0
    // and would win over L0's count. L0 holds more bytes than L1, so
    // by bytes it is the more over-full, and its push is picked.
    var buf: [64]u8 = undefined;
    try fx.live.update(try std.fmt.bufPrint(
        &buf,
        "lmax_target_bytes={d}",
        .{5 * (l1_bytes - 1)},
    ));

    try fx.tree.updateCompactCandidate();
    const cand = fx.tree.compact_candidate orelse
        return error.TestExpectedCandidate;
    try std.testing.expectEqual(@as(u8, 0), cand.level);
    try std.testing.expect(cand.job == .merge_down);
}

test "compaction: L0 merges its small tables before a push that is due" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // A 1 byte target: L0 is due a push whatever it holds. L1 is the
    // bottom, so the push is a rewrite and L1 never competes.
    const cfg = config.Config{
        .max_levels = 2,
        .lmax_target_bytes = 1,
        .l0_merge_min_tables = 3,
    };
    var fx: TombstoneFixture = undefined;
    try setupPlacedTreeCfg(alloc, &fx, dir_path, cfg, .rows, &.{
        .{ .id = 2, .level = 0, .egids = &.{101} },
        .{ .id = 3, .level = 0, .egids = &.{102} },
        .{ .id = 4, .level = 0, .egids = &.{103} },
        .{ .id = 5, .level = 0, .egids = &.{104} },
        .{ .id = 6, .level = 1, .egids = &egidRun(100, 50) },
    });
    defer fx.deinit();

    // Both jobs are ready. Four tables where a merge takes three: the
    // merge goes first.
    try fx.tree.updateCompactCandidate();
    {
        const cand = fx.tree.compact_candidate orelse
            return error.TestExpectedCandidate;
        try std.testing.expectEqual(@as(u8, 0), cand.level);
        try std.testing.expect(cand.job == .merge_l0);
        try std.testing.expectEqual(@as(usize, 4), cand.job.merge_l0.len);
    }
    try compact(fx.tree, 10, 0);
    try std.testing.expectEqual(@as(usize, 1), fx.tree.numTablesAt(0));

    // One table left, nothing to merge: the push runs.
    {
        const cand = fx.tree.compact_candidate orelse
            return error.TestExpectedCandidate;
        try std.testing.expectEqual(@as(u8, 0), cand.level);
        try std.testing.expect(cand.job == .merge_down);
    }
    try compact(fx.tree, 11, 0);
    try std.testing.expectEqual(@as(usize, 0), fx.tree.numTablesAt(0));
    try expectEgids(alloc, fx.tree, &.{ 100, 101, 104, 149 });
}

fn setJobMaxBytes(fx: *TombstoneFixture, bytes: u64) !void {
    var buf: [64]u8 = undefined;
    const input = try std.fmt.bufPrint(&buf, "job_max_bytes={d}", .{bytes});
    try fx.live.update(input);
    try fx.tree.updateCompactCandidate();
}

test "compaction: a level below L0 rewrites a run of adjacent tables" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // Six adjacent L1 tables over one L2 table that spans them all. L2
    // is the bottom, so nothing moves: L1's job is a rewrite.
    const cfg = config.Config{ .max_levels = 3 };
    var fx: TombstoneFixture = undefined;
    try setupPlacedTreeCfg(alloc, &fx, dir_path, cfg, .rows, &.{
        .{ .id = 2, .level = 1, .egids = &egidRun(100, 50) },
        .{ .id = 3, .level = 1, .egids = &egidRun(200, 50) },
        .{ .id = 4, .level = 1, .egids = &egidRun(300, 50) },
        .{ .id = 5, .level = 1, .egids = &egidRun(400, 50) },
        .{ .id = 6, .level = 1, .egids = &egidRun(500, 50) },
        .{ .id = 7, .level = 1, .egids = &egidRun(600, 50) },
        .{ .id = 8, .level = 2, .egids = &.{ 50, 700 } },
    });
    defer fx.deinit();

    var l1_bytes: u64 = 0;
    for (fx.tree.levels[1].tables.items) |e| l1_bytes += e.size;
    const one = l1_bytes / 6;

    // A target of one byte and a budget of three and a half tables.
    // Every run overlaps the one L2 table, so a neighbour adds no
    // overlap: the job stops once its own tables reach the budget, at
    // four, whichever it starts at, and they are neighbours.
    try fx.live.update("lmax_target_bytes=5");
    try setJobMaxBytes(&fx, one * 7 / 2);
    {
        const cand = fx.tree.compact_candidate orelse
            return error.TestExpectedCandidate;
        try std.testing.expectEqual(@as(u8, 1), cand.level);
        const run = cand.job.merge_down;
        try std.testing.expectEqual(@as(usize, 4), run.len);
        for (run[1..], run[0 .. run.len - 1]) |e, prev|
            try std.testing.expectEqual(prev.id + 1, e.id);
    }

    // The run compacts as one job: its tables leave L1 together.
    try compact(fx.tree, 10, 0);
    try std.testing.expectEqual(@as(usize, 2), fx.tree.numTablesAt(1));
    try expectEgids(alloc, fx.tree, &.{ 50, 100, 349, 649, 700 });
}

test "compaction: a push takes L0 tables that add no L1 overlap" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // Four L0 tables, each spanning the one L1 table: any push rewrites
    // all of L1. L1 is the bottom of two levels with a 1 byte target,
    // so L0 pushes whenever it holds a table.
    const cfg = config.Config{
        .max_levels = 2,
        .lmax_target_bytes = 1,
        .l0_merge_min_tables = 0,
    };
    var fx: TombstoneFixture = undefined;
    try setupPlacedTreeCfg(alloc, &fx, dir_path, cfg, .rows, &.{
        .{ .id = 2, .level = 0, .egids = &.{ 50, 700 } },
        .{ .id = 3, .level = 0, .egids = &.{ 51, 701 } },
        .{ .id = 4, .level = 0, .egids = &.{ 52, 702 } },
        .{ .id = 5, .level = 0, .egids = &.{ 53, 703 } },
        .{ .id = 6, .level = 1, .egids = &egidRun(100, 500) },
    });
    defer fx.deinit();

    const l0 = fx.tree.levels[0].tables.items;
    const two_l0 = l0[0].table.fileSize() + l0[1].table.fileSize();

    // A budget of two L0 tables: L1 puts the first table over it, the
    // second adds no L1 bytes and is taken, and L0's own bytes stop
    // the third.
    try setJobMaxBytes(&fx, two_l0);
    try compact(fx.tree, 10, 0);
    {
        const ids = try levelIds(alloc, fx.tree, 0);
        defer alloc.free(ids);
        try std.testing.expectEqualSlices(u64, &.{ 4, 5 }, ids);
    }

    // A budget L1 alone reaches, with room for the rest of L0: one
    // push takes both tables left.
    var l1_bytes: u64 = 0;
    for (fx.tree.levels[1].tables.items) |e| l1_bytes += e.table.fileSize();
    try std.testing.expect(l1_bytes > two_l0);
    try setJobMaxBytes(&fx, l1_bytes);
    try compact(fx.tree, 11, 0);

    try std.testing.expectEqual(@as(usize, 0), fx.tree.numTablesAt(0));
    try expectEgids(alloc, fx.tree, &.{ 50, 53, 100, 599, 700, 703 });
}

test "compaction: a posting index's push is rewritten, not moved" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // Its tables may hold roaring chains only a rewrite rolls up.
    var fx: TombstoneFixture = undefined;
    try setupPlacedTree(alloc, &fx, dir_path, 3, .posting_index, &.{
        .{ .id = 2, .level = 0, .egids = &.{1} },
    });
    defer fx.deinit();

    try compact(fx.tree, 10, 0);

    const ids = try levelIds(alloc, fx.tree, 1);
    defer alloc.free(ids);
    try std.testing.expectEqual(@as(usize, 1), ids.len);
    try std.testing.expect(ids[0] != 2);
    try expectEgids(alloc, fx.tree, &.{1});
}

test "compaction: bottom level drops a tombstone below the horizon and what it shadows" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // max_levels=2 → L0's dest is L1, which IS the bottom level.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "k", .ts = 100, .val = "", .meta = .{ .kind = .deleted } },
        .{ .key = "k", .ts = 50, .val = "old" },
    });
    defer fx.deinit();

    // Horizon above the tombstone: nothing can read at or below ts=100 any
    // more, so the tombstone and the version it shadows are both unreachable.
    try compact(fx.tree, 1, 200);

    try std.testing.expect((try fx.tree.get(.{
        .key = .{ .primary = "k" },
    })) == null);
}

test "compaction: a tombstone above the horizon is retained with the state it shadows" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "k", .ts = 100, .val = "", .meta = .{ .kind = .deleted } },
        .{ .key = "k", .ts = 50, .val = "old" },
    });
    defer fx.deinit();

    // Horizon below the tombstone: the deletion is not settled. The tombstone
    // is the newest version and stays — and so does the state it shadows,
    // because the state@50 is the first terminal at or below the horizon (50),
    // i.e. the oldest version an allowed read can still resolve to.
    try compact(fx.tree, 1, 50);

    const got = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer got.free(alloc);
    try std.testing.expect(got.newest().meta.isDeleted());
    try std.testing.expectEqual(@as(u64, 100), got.newest().ts);

    // A read at 60 (>= horizon, so allowed) sees the pre-delete value: the
    // tombstone at 100 hasn't happened yet from there, so the shadowed state
    // is exactly what a correct MVCC read returns — and thus must survive GC.
    const mid = try fx.tree.get(.{
        .key = .{ .primary = "k" },
        .read_ts = 60,
    }) orelse return error.TestExpectedItem;
    defer mid.free(alloc);
    try std.testing.expectEqualStrings("old", mid.newest().value);
}

test "compaction: a non-bottom level never drops tombstones" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // max_levels=4 → L0's dest is L1, and L2/L3 sit below it. An older
    // version of "k" could be down there, so dropping here would resurrect it.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 4, &.{
        .{ .key = "k", .ts = 100, .val = "", .meta = .{ .kind = .deleted } },
        .{ .key = "k", .ts = 50, .val = "old" },
    });
    defer fx.deinit();

    // Horizon is way above the tombstone — eligible on every count EXCEPT
    // that this compaction isn't writing into the bottom level.
    try compact(fx.tree, 1, 200);

    const got = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer got.free(alloc);
    try std.testing.expect(got.newest().meta.isDeleted());
}

test "compaction: a put above the horizon survives a lower tombstone's drop" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // The key is re-created after being deleted. Sort order is ts descending.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "k", .ts = 300, .val = "new" },
        .{ .key = "k", .ts = 100, .val = "", .meta = .{ .kind = .deleted } },
        .{ .key = "k", .ts = 50, .val = "old" },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 200);

    // k@300 is above the horizon: still readable, untouched.
    const got = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer got.free(alloc);
    try std.testing.expectEqualStrings("new", got.newest().value);
    try std.testing.expect(!got.newest().meta.isDeleted());

    // k@100 and k@50 are gone. A read at 250 used to find the tombstone and
    // report "deleted"; now it finds nothing — the same answer, which is
    // exactly why the drop was legal.
    try std.testing.expect((try fx.tree.get(.{
        .key = .{ .primary = "k" },
        .read_ts = 250,
    })) == null);
}

test "compaction: dropping one primary does not disturb its neighbours" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // "j" and "m" bracket the dropped primary: the skip must end when the
    // primary changes, not run on and eat them.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "j", .ts = 10, .val = "jv" },
        .{ .key = "k", .ts = 100, .val = "", .meta = .{ .kind = .deleted } },
        .{ .key = "k", .ts = 50, .val = "old" },
        .{ .key = "m", .ts = 10, .val = "mv" },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 200);

    const j = try fx.tree.get(.{ .key = .{ .primary = "j" } }) orelse
        return error.TestExpectedItem;
    defer j.free(alloc);
    try std.testing.expectEqualStrings("jv", j.newest().value);
    const m = try fx.tree.get(.{ .key = .{ .primary = "m" } }) orelse
        return error.TestExpectedItem;
    defer m.free(alloc);
    try std.testing.expectEqualStrings("mv", m.newest().value);
    try std.testing.expect((try fx.tree.get(.{
        .key = .{ .primary = "k" },
    })) == null);
}

// ---------------------------------------------------------------------------
// Version GC: shadowed updates + stale accounting
// ---------------------------------------------------------------------------

test "compaction: bottom level drops shadowed versions once they reach the horizon" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // Three plain (no tombstone) versions of "k".
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "k", .ts = 500, .val = "v500" },
        .{ .key = "k", .ts = 300, .val = "v300" },
        .{ .key = "k", .ts = 100, .val = "v100" },
    });
    defer fx.deinit();

    // Horizon 600: all three are at or below it, so the two older versions are
    // collectable. The newest value (k@500) is the current value and stays.
    try compact(fx.tree, 1, 600);

    const got = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer got.free(alloc);
    try std.testing.expectEqualStrings("v500", got.newest().value);
    // A read that would have landed on k@300 finds nothing — it's collected.
    const below = try fx.tree.get(.{
        .key = .{ .primary = "k" },
        .read_ts = 400,
    });
    try std.testing.expect(below == null);
    // Only the newest survived → nothing shadowed remains → no stale data.
    try std.testing.expectEqual(
        @as(u64, 0),
        levelStaleBytes(&fx.tree.levels[1]),
    );
}

test "compaction: bottom level keeps a shadowed version above the horizon and counts it stale" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 2, &.{
        .{ .key = "k", .ts = 500, .val = "v500" },
        .{ .key = "k", .ts = 300, .val = "v300" },
    });
    defer fx.deinit();

    // Horizon 200: both versions are above it, so nothing is collected yet — a
    // read at 400 legitimately sees k@300, and it stays until the horizon
    // rises past 300.
    try compact(fx.tree, 1, 200);

    const cur = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer cur.free(alloc);
    try std.testing.expectEqualStrings("v500", cur.newest().value);
    const mid = try fx.tree.get(.{
        .key = .{ .primary = "k" },
        .read_ts = 350,
    }) orelse return error.TestExpectedItem;
    defer mid.free(alloc);
    try std.testing.expectEqualStrings("v300", mid.newest().value);
    // k@300 is shadowed by the newer full row k@500 → counted stale: primary
    // "k" (1) + value "v300" (4). Reclaimed once the horizon rises past 500.
    try std.testing.expectEqual(
        @as(u64, 5),
        levelStaleBytes(&fx.tree.levels[1]),
    );
}

test "compaction: a non-bottom level drops what a state below the horizon shadows" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // max_levels=4 → L0's dest is L1, well above the bottom (L3). k@500
    // is a state at or below the horizon (600): every read settles on it
    // or newer, so k@300 is unreachable at any level and goes here.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 4, &.{
        .{ .key = "k", .ts = 500, .val = "v500" },
        .{ .key = "k", .ts = 300, .val = "v300" },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 600);

    const cur = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer cur.free(alloc);
    try std.testing.expectEqualStrings("v500", cur.newest().value);
    // Below the horizon: nothing left (production refuses such reads).
    const mid = try fx.tree.get(.{ .key = .{ .primary = "k" }, .read_ts = 350 });
    try std.testing.expect(mid == null);
    try std.testing.expectEqual(@as(u64, 0), levelStaleBytes(&fx.tree.levels[1]));
}

test "compaction: a non-bottom level keeps a shadowed version above the horizon and counts it stale" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // The shadowing state k@500 is above the horizon (400), so a read at
    // 350..499 still needs k@300: kept, and counted stale.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 4, &.{
        .{ .key = "k", .ts = 500, .val = "v500" },
        .{ .key = "k", .ts = 300, .val = "v300" },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 400);

    const mid = try fx.tree.get(.{
        .key = .{ .primary = "k" },
        .read_ts = 350,
    }) orelse return error.TestExpectedItem;
    defer mid.free(alloc);
    try std.testing.expectEqualStrings("v300", mid.newest().value);
    try std.testing.expectEqual(
        @as(u64, 5),
        levelStaleBytes(&fx.tree.levels[1]),
    );
}

test "compaction: a delta above the newest state is live, not stale" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    // Newest-first: a delta on top of a full state, with an older state beneath
    // it. Only the version BELOW the newest terminal (state@200) is stale — the
    // delta and the state it rebuilds on are both live. This is the case the old
    // "every non-newest version is stale" rule got wrong. Non-bottom keeps the
    // drop path out of it, isolating the stale rule.
    var fx: TombstoneFixture = undefined;
    try setupTombstoneTree(alloc, &fx, dir_path, 4, &.{
        .{ .key = "k", .ts = 300, .val = "d300", .meta = .{ .kind = .delta } },
        .{ .key = "k", .ts = 200, .val = "s200", .meta = .{ .kind = .state } },
        .{ .key = "k", .ts = 100, .val = "s100", .meta = .{ .kind = .state } },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 0);

    // All three survive (a non-bottom compaction drops nothing).
    const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.newest().meta.isDelta());
    const base = try fx.tree.get(.{
        .key = .{ .primary = "k" },
        .read_ts = 250,
    }) orelse return error.TestExpectedItem;
    defer base.free(alloc);
    try std.testing.expectEqualStrings("s200", base.newest().value);

    // Only k@100 sits below the newest terminal (state@200) → stale: "k" (1) +
    // "s100" (4). The delta and its base state are NOT counted.
    try std.testing.expectEqual(
        @as(u64, 5),
        levelStaleBytes(&fx.tree.levels[1]),
    );
}

// ---------------------------------------------------------------------------
// Roaring rollup in compaction
// ---------------------------------------------------------------------------

const zroar = zdb.zroar;

const roaring_delta: zdb.kv.Meta = .{ .kind = .delta, .encoding = .roaring };
const roaring_state: zdb.kv.Meta = .{ .kind = .state, .encoding = .roaring };

/// setupTombstoneTree with a caller-supplied Config (dir is filled in
/// here). Rollup tests need to vary the rollup knobs.
fn setupTreeWithCfg(
    alloc: std.mem.Allocator,
    fx: *TombstoneFixture,
    dir_path: String,
    cfg: config.Config,
    entries: []const MetaEntry,
) !void {
    return setupTreeWithTables(alloc, fx, dir_path, cfg, &.{entries});
}

/// setupTreeWithCfg with one L0 table per entry list, oldest first.
fn setupTreeWithTables(
    alloc: std.mem.Allocator,
    fx: *TombstoneFixture,
    dir_path: String,
    cfg: config.Config,
    tables: []const []const MetaEntry,
) !void {
    const tree_id: u64 = 1;
    fx.store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    errdefer fx.store.deinit();
    fx.manifest = _manifest.Manifest.init(alloc);
    errdefer fx.manifest.deinit();
    try fx.manifest.commit(1, &.{}, &.{
        createTreeChange(tree_id, 0, &.{}, &.{}, .posting_index),
    });

    for (tables, 1..) |entries, sst_id| {
        var tb = try buildTableMeta(alloc, entries);
        defer tb.deinit();
        try installTable(
            alloc,
            &fx.manifest,
            &fx.store,
            &tb,
            sst_id,
            tree_id,
            0,
        );
    }

    fx.cache = TableCache.init(alloc, &fx.store);
    errdefer fx.cache.deinit();
    fx.live = flags.Live(config.Config).initFrom(alloc, cfg);
    errdefer fx.live.deinit();
    fx.tree = try Tree.open(
        alloc,
        &fx.live,
        0,
        tree_id,
        &fx.manifest,
        &fx.cache,
    );
}

fn encodeDelta(
    alloc: std.mem.Allocator,
    adds: []const u64,
    rems: []const u64,
) ![]u8 {
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(alloc);
    var w = try zdb.buffers.RoaringDeltaWriter.initWithBuf(alloc, &list);
    if (adds.len > 0) try w.setAdds(adds);
    if (rems.len > 0) try w.setRemoves(rems);
    _ = try w.finish();
    return list.toOwnedSlice(alloc);
}

// The aligned return type matters: toBufferCopy allocates align(8),
// and freeing through a plain []u8 trips the DebugAllocator's
// alignment bookkeeping.
fn encodeState(
    alloc: std.mem.Allocator,
    vals: []const u64,
) ![]align(8) u8 {
    var bm = try zroar.Bitmap.fromSortedList(alloc, vals);
    defer bm.deinit();
    return bm.toBufferCopy(alloc);
}

fn expectBitmapBytes(
    alloc: std.mem.Allocator,
    bytes: String,
    expected: []const u64,
) !void {
    var bm = try zroar.Bitmap.fromBufferCopy(alloc, bytes);
    defer bm.deinit();
    try std.testing.expectEqual(expected.len, bm.getCardinality());
    for (expected) |v| try std.testing.expect(bm.contains(v));
}

/// The posting visible at `read_ts`, via the real read walk
/// (Tree.rollupBitmap): after a rollup, an old-snapshot reader must
/// land on the retained chain below the too-new synthetic state and
/// fold to the same answer it got before the compaction.
fn foldAt(
    alloc: std.mem.Allocator,
    tree: *Tree,
    primary: String,
    read_ts: u64,
) !zroar.Bitmap {
    const bm = try tree.rollupBitmap(alloc, .{
        .key = .{ .primary = primary },
        .read_ts = read_ts,
    }, 0);
    return bm orelse zroar.Bitmap.init(alloc);
}

test "compaction: rollup folds a delta chain onto its in-inputs state" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const d300 = try encodeDelta(alloc, &.{40}, &.{2});
    defer alloc.free(d300);
    const d200 = try encodeDelta(alloc, &.{ 2, 3 }, &.{});
    defer alloc.free(d200);
    const s100 = try encodeState(alloc, &.{ 1, 2 });
    defer alloc.free(s100);

    // Non-bottom (4 levels): the retained chain must survive.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{
        .max_levels = 4,
        .lmax_target_bytes = push_lmax,
    }, &.{
        .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        .{ .key = "k", .ts = 200, .val = d200, .meta = roaring_delta },
        .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 0);

    // Newest version is now a synthetic state at the newest delta's
    // timestamp: {1,2} +{2,3} → {1,2,3}, then +{40}-{2} → {1,3,40}.
    const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.newest().meta.isState());
    try std.testing.expect(top.newest().meta.isRoaring());
    try std.testing.expectEqual(@as(u64, 300), top.newest().ts);
    try expectBitmapBytes(alloc, top.newest().value, &.{ 1, 3, 40 });

    // The replaced delta is gone; the older chain is retained verbatim
    // beneath the synthetic state.
    const mid = try fx.tree.get(.{
        .key = .{ .primary = "k" },
        .read_ts = 250,
    }) orelse return error.TestExpectedItem;
    defer mid.free(alloc);
    try std.testing.expect(mid.newest().meta.isDelta());
    try std.testing.expectEqual(@as(u64, 200), mid.newest().ts);

    // MVCC: an old-snapshot reader folding the retained chain gets the
    // historical bitmaps, unchanged by the rollup.
    var at250 = try foldAt(alloc, fx.tree, "k", 250);
    defer at250.deinit();
    try std.testing.expectEqual(@as(u64, 3), at250.getCardinality());
    for ([_]u64{ 1, 2, 3 }) |v| try std.testing.expect(at250.contains(v));

    var at150 = try foldAt(alloc, fx.tree, "k", 150);
    defer at150.deinit();
    try std.testing.expectEqual(@as(u64, 2), at150.getCardinality());
}

test "compaction: an L0 to L0 merge does not roll up" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const d300 = try encodeDelta(alloc, &.{40}, &.{2});
    defer alloc.free(d300);
    const d200 = try encodeDelta(alloc, &.{ 2, 3 }, &.{});
    defer alloc.free(d200);
    const s100 = try encodeState(alloc, &.{ 1, 2 });
    defer alloc.free(s100);

    // Three small L0 tables, far under L1's target, where a merge
    // takes three: the job is an L0 to L0 merge. The chain has its
    // state among the inputs, which a push would fold.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithTables(alloc, &fx, dir_path, .{
        .max_levels = 4,
        .l0_merge_min_tables = 3,
    }, &.{
        &.{.{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state }},
        &.{.{ .key = "k", .ts = 200, .val = d200, .meta = roaring_delta }},
        &.{.{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta }},
    });
    defer fx.deinit();

    try compact(fx.tree, 10, 0);
    try std.testing.expectEqual(@as(usize, 1), fx.tree.numTablesAt(0));
    try std.testing.expectEqual(@as(usize, 0), fx.tree.numTablesAt(1));

    // The newest version is still the delta, and the chain folds to
    // the posting it folded to before.
    const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.newest().meta.isDelta());
    try std.testing.expectEqual(@as(u64, 300), top.newest().ts);

    var bm = try foldAt(alloc, fx.tree, "k", std.math.maxInt(u64));
    defer bm.deinit();
    try std.testing.expectEqual(@as(u64, 3), bm.getCardinality());
    for ([_]u64{ 1, 3, 40 }) |v| try std.testing.expect(bm.contains(v));
}

test "compaction: rollup leaves lone states and baseless chains alone" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const s100 = try encodeState(alloc, &.{ 7, 8 });
    defer alloc.free(s100);
    const d200 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d200);

    // "a": already rolled (lone state). "b": delta with no base among
    // the inputs — read-below isn't wired yet, so it stays a delta.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{
        .max_levels = 4,
        .lmax_target_bytes = push_lmax,
    }, &.{
        .{ .key = "a", .ts = 100, .val = s100, .meta = roaring_state },
        .{ .key = "b", .ts = 200, .val = d200, .meta = roaring_delta },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 0);

    const a = try fx.tree.get(.{ .key = .{ .primary = "a" } }) orelse
        return error.TestExpectedItem;
    defer a.free(alloc);
    try std.testing.expect(a.newest().meta.isState());
    try std.testing.expectEqualSlices(u8, s100, a.newest().value);

    const b = try fx.tree.get(.{ .key = .{ .primary = "b" } }) orelse
        return error.TestExpectedItem;
    defer b.free(alloc);
    try std.testing.expect(b.newest().meta.isDelta());
    try std.testing.expectEqualSlices(u8, d200, b.newest().value);
}

test "compaction: a mixed chain aborts the rollup and stays verbatim" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const d300 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d300);

    // A raw state under a roaring delta: not ours to fold. Both
    // versions must come out byte-identical.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{
        .max_levels = 4,
        .lmax_target_bytes = push_lmax,
    }, &.{
        .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        .{ .key = "k", .ts = 200, .val = "raw-state", .meta = .{} },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 0);

    const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.newest().meta.isDelta());
    try std.testing.expectEqualSlices(u8, d300, top.newest().value);

    const under = try fx.tree.get(.{
        .key = .{ .primary = "k" },
        .read_ts = 250,
    }) orelse return error.TestExpectedItem;
    defer under.free(alloc);
    try std.testing.expectEqualStrings("raw-state", under.newest().value);
}

test "compaction: an empty fold emits a roaring tombstone" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const d200 = try encodeDelta(alloc, &.{}, &.{1});
    defer alloc.free(d200);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);

    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{
        .max_levels = 4,
        .lmax_target_bytes = push_lmax,
    }, &.{
        .{ .key = "k", .ts = 200, .val = d200, .meta = roaring_delta },
        .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 0);

    const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.newest().meta.isDeleted());
    try std.testing.expectEqual(@as(usize, 0), top.newest().value.len);
    try std.testing.expectEqual(@as(u64, 200), top.newest().ts);

    // Retained history below: the old state is still readable.
    const old = try fx.tree.get(.{
        .key = .{ .primary = "k" },
        .read_ts = 150,
    }) orelse return error.TestExpectedItem;
    defer old.free(alloc);
    try std.testing.expect(old.newest().meta.isState());
}

test "compaction: bottom level collapses a rolled chain below the horizon" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const d200 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d200);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);

    // max_levels=2 → dest L1 IS the bottom; horizon above the whole
    // chain lets the synthetic state's discard boundary drop the rest.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{
        .max_levels = 2,
        .lmax_target_bytes = push_lmax,
    }, &.{
        .{ .key = "k", .ts = 200, .val = d200, .meta = roaring_delta },
        .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 300);

    const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.newest().meta.isState());
    try expectBitmapBytes(alloc, top.newest().value, &.{ 1, 5 });

    // The chain beneath the synthetic state settled below the horizon
    // and is gone: nothing older than the state remains.
    try std.testing.expect(
        (try fx.tree.get(.{
            .key = .{ .primary = "k" },
            .read_ts = 150,
        })) == null,
    );
}

test "compaction: rollup gates — min_deltas and rollup_enabled" {
    const alloc = std.testing.allocator;

    const d200 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d200);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);
    const entries = [_]MetaEntry{
        .{ .key = "k", .ts = 200, .val = d200, .meta = roaring_delta },
        .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
    };

    // min_deltas=2 with a 1-delta chain: not worth the rewrite.
    {
        var tmp = try vfs.TmpDir.init(alloc);
        defer tmp.deinit();
        const dir_path = tmp.path;
        var fx: TombstoneFixture = undefined;
        try setupTreeWithCfg(
            alloc,
            &fx,
            dir_path,
            .{
                .max_levels = 4,
                .rollup_min_deltas = 2,
                .lmax_target_bytes = push_lmax,
            },
            &entries,
        );
        defer fx.deinit();
        try compact(fx.tree, 1, 0);
        const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
            return error.TestExpectedItem;
        defer top.free(alloc);
        try std.testing.expect(top.newest().meta.isDelta());
    }

    // rollup_enabled=false: today's behavior, everything verbatim.
    {
        var tmp = try vfs.TmpDir.init(alloc);
        defer tmp.deinit();
        const dir_path = tmp.path;
        var fx: TombstoneFixture = undefined;
        try setupTreeWithCfg(
            alloc,
            &fx,
            dir_path,
            .{
                .max_levels = 4,
                .rollup_enabled = false,
                .lmax_target_bytes = push_lmax,
            },
            &entries,
        );
        defer fx.deinit();
        try compact(fx.tree, 1, 0);
        const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
            return error.TestExpectedItem;
        defer top.free(alloc);
        try std.testing.expect(top.newest().meta.isDelta());
        try std.testing.expectEqualSlices(u8, d200, top.newest().value);
    }
}

/// One SST per (level, entries) pair, ids assigned 1..n. For the
/// read-below tests, which need history spread across levels.
const LevelTable = struct { level: u32, entries: []const MetaEntry };

fn setupTreeMulti(
    alloc: std.mem.Allocator,
    fx: *TombstoneFixture,
    dir_path: String,
    cfg: config.Config,
    tables: []const LevelTable,
) !void {
    const tree_id: u64 = 1;
    fx.store = try vfs.Store.initFile(alloc, dir_path, vfs.g());
    errdefer fx.store.deinit();
    fx.manifest = _manifest.Manifest.init(alloc);
    errdefer fx.manifest.deinit();
    try fx.manifest.commit(1, &.{}, &.{
        createTreeChange(tree_id, 0, &.{}, &.{}, .posting_index),
    });

    for (tables, 1..) |t, id| {
        var tb = try buildTableMeta(alloc, t.entries);
        defer tb.deinit();
        try installTable(
            alloc,
            &fx.manifest,
            &fx.store,
            &tb,
            id,
            tree_id,
            t.level,
        );
    }

    fx.cache = TableCache.init(alloc, &fx.store);
    errdefer fx.cache.deinit();
    fx.live = flags.Live(config.Config).initFrom(alloc, cfg);
    errdefer fx.live.deinit();
    fx.tree = try Tree.open(
        alloc,
        &fx.live,
        0,
        tree_id,
        &fx.manifest,
        &fx.cache,
    );
}

test "compaction: shadowed delta chains are never re-rolled" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const s400 = try encodeState(alloc, &.{ 1, 5 });
    defer alloc.free(s400);
    const d300 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d300);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);

    // The shape a chain has right after a rollup: synthetic state on
    // top, retained delta + old base beneath it. A later compaction
    // must NOT regenerate a synthetic state for the shadowed delta —
    // that would repeat the fold + write amp on every compaction
    // until GC collapses the chain.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{
        .max_levels = 4,
        .lmax_target_bytes = push_lmax,
    }, &.{
        .{ .key = "k", .ts = 400, .val = s400, .meta = roaring_state },
        .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 0);

    // The shadowed delta came through verbatim, not as a state.
    const mid = try fx.tree.get(.{
        .key = .{ .primary = "k" },
        .read_ts = 350,
    }) orelse return error.TestExpectedItem;
    defer mid.free(alloc);
    try std.testing.expect(mid.newest().meta.isDelta());
    try std.testing.expectEqualSlices(u8, d300, mid.newest().value);
}

test "compaction: read-below rolls a baseless chain onto a deep state" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const d300 = try encodeDelta(alloc, &.{5}, &.{1});
    defer alloc.free(d300);
    const s100 = try encodeState(alloc, &.{ 1, 2 });
    defer alloc.free(s100);

    // Delta at L1, its base two levels down at L3. Compacting L1→L2
    // (dest=2 >= rollup_at_level=2) peeks below and regenerates.
    var fx: TombstoneFixture = undefined;
    try setupTreeMulti(alloc, &fx, dir_path, .{
        .max_levels = 4,
        .lmax_target_bytes = push_lmax,
    }, &.{
        .{ .level = 1, .entries = &.{
            .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        } },
        .{ .level = 3, .entries = &.{
            .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
        } },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 0);

    const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.newest().meta.isState());
    try std.testing.expectEqual(@as(u64, 300), top.newest().ts);
    try expectBitmapBytes(alloc, top.newest().value, &.{ 2, 5 });
}

test "compaction: max_deltas forces read-below even at L1" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const d300 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d300);
    const d200 = try encodeDelta(alloc, &.{3}, &.{});
    defer alloc.free(d200);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);

    // Level trigger disabled (rollup_at_level way above max_levels);
    // the 2-delta chain hits rollup_max_deltas=2 and forces the peek
    // from an L0→L1 compaction. The state sits at the bottom, which
    // never scores, so L0 is the candidate.
    var fx: TombstoneFixture = undefined;
    try setupTreeMulti(alloc, &fx, dir_path, .{
        .max_levels = 4,
        .rollup_at_level = 200,
        .rollup_max_deltas = 2,
        .lmax_target_bytes = push_lmax,
    }, &.{
        .{ .level = 0, .entries = &.{
            .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
            .{ .key = "k", .ts = 200, .val = d200, .meta = roaring_delta },
        } },
        .{ .level = 3, .entries = &.{
            .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
        } },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 0);

    const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.newest().meta.isState());
    try std.testing.expectEqual(@as(u64, 300), top.newest().ts);
    try expectBitmapBytes(alloc, top.newest().value, &.{ 1, 3, 5 });
}

test "compaction: read-below with no state anywhere folds from empty" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const d300 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d300);
    const d100 = try encodeDelta(alloc, &.{3}, &.{});
    defer alloc.free(d100);

    var fx: TombstoneFixture = undefined;
    try setupTreeMulti(alloc, &fx, dir_path, .{
        .max_levels = 4,
        .lmax_target_bytes = push_lmax,
    }, &.{
        .{ .level = 1, .entries = &.{
            .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        } },
        .{ .level = 3, .entries = &.{
            .{ .key = "k", .ts = 100, .val = d100, .meta = roaring_delta },
        } },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 0);

    const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.newest().meta.isState());
    try expectBitmapBytes(alloc, top.newest().value, &.{ 3, 5 });
}

test "compaction: read-below declined by triggers or mixed chains" {
    const alloc = std.testing.allocator;

    const d300 = try encodeDelta(alloc, &.{5}, &.{});
    defer alloc.free(d300);
    const s100 = try encodeState(alloc, &.{1});
    defer alloc.free(s100);

    // Neither trigger fires: the chain stays a delta.
    {
        var tmp = try vfs.TmpDir.init(alloc);
        defer tmp.deinit();
        const dir_path = tmp.path;
        var fx: TombstoneFixture = undefined;
        try setupTreeMulti(alloc, &fx, dir_path, .{
            .max_levels = 4,
            .rollup_at_level = 200,
            .rollup_max_deltas = 200,
            .lmax_target_bytes = push_lmax,
        }, &.{
            .{ .level = 1, .entries = &.{
                .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
            } },
            .{ .level = 3, .entries = &.{
                .{ .key = "k", .ts = 100, .val = s100, .meta = roaring_state },
            } },
        });
        defer fx.deinit();
        try compact(fx.tree, 1, 0);
        const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
            return error.TestExpectedItem;
        defer top.free(alloc);
        try std.testing.expect(top.newest().meta.isDelta());
        try std.testing.expectEqualSlices(u8, d300, top.newest().value);
    }

    // A raw (non-roaring) version below: the peek reports the chain
    // unfoldable and the run stays verbatim.
    {
        var tmp = try vfs.TmpDir.init(alloc);
        defer tmp.deinit();
        const dir_path = tmp.path;
        var fx: TombstoneFixture = undefined;
        try setupTreeMulti(alloc, &fx, dir_path, .{
            .max_levels = 4,
            .lmax_target_bytes = push_lmax,
        }, &.{
            .{ .level = 1, .entries = &.{
                .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
            } },
            .{ .level = 3, .entries = &.{
                .{ .key = "k", .ts = 100, .val = "raw", .meta = .{} },
            } },
        });
        defer fx.deinit();
        try compact(fx.tree, 1, 0);
        const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
            return error.TestExpectedItem;
        defer top.free(alloc);
        try std.testing.expect(top.newest().meta.isDelta());
        try std.testing.expectEqualSlices(u8, d300, top.newest().value);
    }
}

test "compaction: rollup folds a chain terminated by a tombstone" {
    const alloc = std.testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const dir_path = tmp.path;

    const d300 = try encodeDelta(alloc, &.{9}, &.{});
    defer alloc.free(d300);

    // A raw tombstone terminates a roaring chain (the index key was
    // cleared, then a new delta arrived): fold = {} + {9}.
    var fx: TombstoneFixture = undefined;
    try setupTreeWithCfg(alloc, &fx, dir_path, .{
        .max_levels = 4,
        .lmax_target_bytes = push_lmax,
    }, &.{
        .{ .key = "k", .ts = 300, .val = d300, .meta = roaring_delta },
        .{ .key = "k", .ts = 200, .val = "", .meta = .{ .kind = .deleted } },
    });
    defer fx.deinit();

    try compact(fx.tree, 1, 0);

    const top = try fx.tree.get(.{ .key = .{ .primary = "k" } }) orelse
        return error.TestExpectedItem;
    defer top.free(alloc);
    try std.testing.expect(top.newest().meta.isState());
    try expectBitmapBytes(alloc, top.newest().value, &.{9});
}
