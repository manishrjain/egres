// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Tests for the batch read path (`Forest.multiGet`, read.zig).

const std = @import("std");
const utils = @import("utils");
const String = utils.String;
const zdb = @import("zdb");
const createTree = @import("testing.zig").createTree;
const buildCompaction = @import("testing.zig").buildCompaction;
const flags = @import("flags");
const fiber = @import("zfiber");
const vfs = zdb.vfs;
const zroar = zdb.zroar;

const Key = zdb.kv.Key;
const Item = zdb.kv.Item;
const Meta = zdb.kv.Meta;
const Forest = zdb.forest.Forest;
const Config = zdb.config.Config;
const Builder = zdb.table.builder.Builder;
const _manifest = zdb.manifest;

const ns: u64 = 1;
const tid: u32 = 0;
const table_key: zdb.kv.TableKey = .{ .namespace = ns, .table_id = tid };

fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![:0]u8 {
    return tmp.dir.realPathFileAlloc(utils.context.g().io, ".", alloc);
}

/// A forest with one table (`ns`, `tid`) and a live memtable, plus the
/// moves that put rows where a test wants them: frozen into an
/// immutable memtable, flushed to L0, compacted to L1.
const Fixture = struct {
    alloc: std.mem.Allocator,
    tmp: std.testing.TmpDir,
    dir: [:0]u8,
    /// The forest reads its options through this; freed after it.
    live: *flags.Live(Config),
    /// The forest's SSTs; freed after it.
    store: vfs.Store,
    f: *Forest,
    tree_id: u64,
    /// Stands in for the raft log: every fixture step that would be its
    /// own log entry takes the next index. Memtable ids and L0 order both
    /// come from it, exactly as they do on the apply path.
    next_index: u64,

    fn setup(self: *Fixture, alloc: std.mem.Allocator) !void {
        self.* = .{
            .alloc = alloc,
            .tmp = std.testing.tmpDir(.{}),
            .dir = undefined,
            .live = undefined,
            .store = undefined,
            .f = undefined,
            .tree_id = undefined,
            .next_index = 0,
        };
        self.dir = try tmpPath(alloc, &self.tmp);
        // A bottom target of one byte makes a push L0's job at any
        // size (`pickJob`), so `compactL0` below pushes into L1.
        self.live = try flags.Live(Config).create(alloc, .{
            .lmax_target_bytes = 1,
        });
        self.store = try vfs.Store.initFile(alloc, self.dir, vfs.g());
        self.f = try Forest.open(alloc, self.live, &self.store);
        self.tree_id = try createTree(self.f, table_key, .{}, .posting_index);
        // Forest.open leaves an id-0 placeholder memtable that a swap
        // discards rather than freezes; start on a real one.
        try self.f.newMemtable(self.nextIndex());
    }

    fn nextIndex(self: *Fixture) u64 {
        self.next_index += 1;
        return self.next_index;
    }

    fn teardown(self: *Fixture) void {
        self.f.close();
        self.store.deinit();
        self.live.destroy();
        self.alloc.free(self.dir);
        self.tmp.cleanup();
    }

    fn key(primary: String) Key {
        return .{
            .namespace = ns,
            .table_id = tid,
            .primary = primary,
        };
    }

    fn put(
        self: *Fixture,
        primary: String,
        ts: u64,
        value: String,
    ) !void {
        try self.putMeta(primary, ts, value, .{});
    }

    fn putMeta(
        self: *Fixture,
        primary: String,
        ts: u64,
        value: String,
        meta: Meta,
    ) !void {
        const items = [_]Item{.{
            .key = key(primary),
            .versions = .{ .one = .{ .ts = ts, .meta = meta, .value = value } },
        }};
        try self.f.put(&items);
    }

    /// Freeze the current memtable into an immutable one; nothing is
    /// flushed.
    fn freeze(self: *Fixture) !void {
        try self.f.newMemtable(self.nextIndex());
    }

    /// Freeze and flush to an L0 SST.
    fn flush(self: *Fixture) !void {
        try self.freeze();
        var fl = (try self.f.buildFlush()) orelse
            return error.TestExpectedFlush;
        defer fl.changes.deinit(self.alloc);
        try self.f.applyFlush(
            self.nextIndex(),
            fl.changes.items,
            fl.newest_memtable_id,
        );
    }

    /// Compact every L0 table into L1.
    fn compactL0(self: *Fixture) !void {
        const tree = self.f.getTree(ns, self.tree_id) orelse
            return error.TestExpectedTree;
        defer tree.release();
        var comp = try buildCompaction(tree, 0);
        defer comp.deinit(self.alloc);
        try std.testing.expect(comp.items.len > 0);
        try tree.applyCompaction(self.nextIndex(), comp.items);
    }
};

/// `mg.item(key)` against `Forest.get` of the same key at `read_ts`,
/// and against the expected value and timestamp (null = absent).
fn expectRow(
    fx: *Fixture,
    mg: *const Forest.MultiGet,
    read_ts: u64,
    primary: String,
    want: ?struct { value: String, ts: u64 },
) !void {
    const got = mg.item(primary);
    const single = try fx.f.get(fx.alloc, .{
        .key = Fixture.key(primary),
        .read_ts = read_ts,
    });
    defer if (single) |s| s.free(fx.alloc);
    if (want) |w| {
        const it = got orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings(primary, it.key.primary);
        try std.testing.expectEqualStrings(w.value, it.newest().value);
        try std.testing.expectEqual(w.ts, it.newest().ts);
        const s = single orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings(w.value, s.newest().value);
        try std.testing.expectEqual(w.ts, s.newest().ts);
    } else {
        try std.testing.expect(got == null);
        try std.testing.expect(single == null);
    }
}

test "multiget: rows resolve across pending writes, memtables, L0 and L1" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    // k01..k10 at ts 10, down in L1.
    const base = [_]String{
        "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10",
    };
    for (base) |k| try fx.put(k, 10, "v1");
    try fx.flush();
    try fx.compactL0();

    // Two overlapping L0 tables: k03 and k04 at ts 20, then k04 again
    // at ts 30 in the newer one.
    try fx.put("k03", 20, "v2");
    try fx.put("k04", 20, "v2");
    try fx.flush();
    try fx.put("k04", 30, "v3");
    try fx.flush();

    // An immutable memtable (k05), then the current one (k06 and a
    // committed tombstone for k02).
    try fx.put("k05", 40, "v4");
    try fx.freeze();
    try fx.put("k06", 50, "v5");
    try fx.putMeta("k02", 60, "", .{ .kind = .deleted });

    // The txn's own writes, at the pending stamp: a row and a
    // tombstone over a committed row.
    const read_ts: u64 = 100;
    const pending = try zdb.skl.SkipList.initCapacity(alloc, 0);
    defer pending.release();
    try pending.putItem(.{
        .key = Fixture.key("k07"),
        .versions = .{ .one = .{
            .ts = zdb.kv.pending_ts,
            .meta = .{},
            .value = "pend",
        } },
    });
    try pending.putItem(.{
        .key = Fixture.key("k08"),
        .versions = .{ .one = .{
            .ts = zdb.kv.pending_ts,
            .meta = .{ .kind = .deleted },
            .value = "",
        } },
    });

    // Unsorted, with a duplicate and an absent key.
    const keys = [_]String{
        "k09", "k03", "k01", "k04", "k05", "k06", "k07", "k08", "k02", "k99",
        "k03",
    };
    var mg = try fx.f.multiGet(alloc, table_key, &keys, .{
        .read_ts = read_ts,
        .pending = pending,
    });
    defer mg.deinit();
    try std.testing.expectEqual(@as(usize, 10), mg.nodes.len);
    try std.testing.expectEqual(@as(usize, 1), mg.pending.items.len);

    try expectRow(&fx, &mg, read_ts, "k01", .{ .value = "v1", .ts = 10 });
    try expectRow(&fx, &mg, read_ts, "k02", null);
    try expectRow(&fx, &mg, read_ts, "k03", .{ .value = "v2", .ts = 20 });
    try expectRow(&fx, &mg, read_ts, "k04", .{ .value = "v3", .ts = 30 });
    try expectRow(&fx, &mg, read_ts, "k05", .{ .value = "v4", .ts = 40 });
    try expectRow(&fx, &mg, read_ts, "k06", .{ .value = "v5", .ts = 50 });
    const pend = mg.item("k07").?.newest().value;
    try std.testing.expectEqualStrings("pend", pend);
    try std.testing.expect(mg.item("k08") == null);
    try expectRow(&fx, &mg, read_ts, "k09", .{ .value = "v1", .ts = 10 });
    try expectRow(&fx, &mg, read_ts, "k99", null);

    // include_deleted hands the tombstone back.
    var raw = try fx.f.multiGet(alloc, table_key, &.{"k02"}, .{
        .read_ts = read_ts,
        .include_deleted = true,
    });
    defer raw.deinit();
    const tomb = raw.item("k02") orelse return error.TestExpectedItem;
    try std.testing.expect(tomb.newest().meta.isDeleted());
    try std.testing.expectEqual(@as(u64, 60), tomb.newest().ts);
}

/// A key of table 7, the neighbour of `tid` in the namespace-wide
/// memtable.
fn otherKey(primary: String) Key {
    return .{
        .namespace = ns,
        .table_id = 7,
        .primary = primary,
    };
}

test "multiget: other tables in the memtables and pending writes are skipped" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    // The memtable spans the namespace: the same primary in table 7
    // sits next to table 0's rows.
    _ = try createTree(
        fx.f,
        .{ .namespace = ns, .table_id = 7 },
        .{},
        .posting_index,
    );
    try fx.put("a", 10, "a-t0");
    try fx.put("z", 1, "z-ts1");
    const other = [_]Item{
        .{ .key = otherKey("a"), .versions = .{
            .one = .{ .ts = 20, .meta = .{}, .value = "a-t7" },
        } },
        .{ .key = otherKey("b"), .versions = .{
            .one = .{ .ts = 20, .meta = .{}, .value = "b-t7" },
        } },
    };
    try fx.f.put(&other);

    // A txn's pending writes to table 7 while table 0 is probed: the
    // seek window must not let the merge land on them.
    const pending = try zdb.skl.SkipList.initCapacity(alloc, 0);
    defer pending.release();
    try pending.putItem(.{
        .key = otherKey("b"),
        .versions = .{ .one = .{
            .ts = 100,
            .meta = .{},
            .value = "pend-t7",
        } },
    });

    var mg = try fx.f.multiGet(alloc, table_key, &.{ "a", "b", "z" }, .{
        .read_ts = 100,
        .pending = pending,
    });
    defer mg.deinit();
    try std.testing.expectEqualStrings("a-t0", mg.item("a").?.newest().value);
    try std.testing.expect(mg.item("b") == null);
    try std.testing.expectEqualStrings("z-ts1", mg.item("z").?.newest().value);
}

test "multiget: read_ts hides the versions above it" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    try fx.put("a", 10, "a10");
    try fx.put("b", 10, "b10");
    try fx.flush();
    try fx.put("a", 30, "a30");
    try fx.putMeta("b", 30, "", .{ .kind = .deleted });
    try fx.put("c", 30, "c30");

    var mg = try fx.f.multiGet(alloc, table_key, &.{ "a", "b", "c" }, .{
        .read_ts = 20,
    });
    defer mg.deinit();
    try expectRow(&fx, &mg, 20, "a", .{ .value = "a10", .ts = 10 });
    try expectRow(&fx, &mg, 20, "b", .{ .value = "b10", .ts = 10 });
    try expectRow(&fx, &mg, 20, "c", null);

    var now = try fx.f.multiGet(alloc, table_key, &.{ "a", "b", "c" }, .{});
    defer now.deinit();
    const latest = std.math.maxInt(u64);
    try expectRow(&fx, &now, latest, "a", .{ .value = "a30", .ts = 30 });
    try expectRow(&fx, &now, latest, "b", null);
    try expectRow(&fx, &now, latest, "c", .{ .value = "c30", .ts = 30 });
}

test "multiget: rejects a read_ts below snapshotTs" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    _ = fx.f.setSnapshotTs(50);
    try std.testing.expectError(
        error.ReadBelowMinRead,
        fx.f.multiGet(alloc, table_key, &.{"a"}, .{ .read_ts = 10 }),
    );
}

fn writeOneKeyTable(
    alloc: std.mem.Allocator,
    store: *vfs.Store,
    file_id: u64,
    k: Key,
    ts: u64,
    value: String,
) !_manifest.Extent {
    const name = try zdb.sstName(alloc, file_id);
    defer alloc.free(name);
    var bld = Builder.init(alloc, .{ .block_bytes = 4 * 1024 });
    defer bld.deinit();
    try bld.add(k, ts, value);
    var tb = try bld.finish();
    defer tb.deinit();
    try store.put(name, tb.bytes);
    return tb.extent();
}

test "multiget: keys across two trees and several bottom tables" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    // Ten one-key L3 SSTs, value = key, then a split into two trees.
    const keys = [_]String{
        "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10",
    };
    var extents: [keys.len]_manifest.Extent = undefined;
    for (keys, &extents, 1..) |k, *ext, file_id| {
        ext.* = try writeOneKeyTable(
            alloc,
            &fx.store,
            file_id,
            Fixture.key(k),
            1,
            k,
        );
    }
    {
        const tree = fx.f.getTree(ns, fx.tree_id) orelse
            return error.TestExpectedTree;
        defer tree.release();
        var changes: [keys.len]_manifest.SSTableChange = undefined;
        for (&changes, extents, 1..) |*c, ext, file_id| {
            c.* = _manifest.createSSTableChange(
                @intCast(file_id),
                ns,
                fx.tree_id,
                3,
                ext,
            );
        }
        try tree.applyCompaction(1, &changes);
    }
    try fx.f.splitTreeForTests(ns, fx.tree_id);

    const asked = [_]String{
        "k10", "k01", "k05", "k06", "k00", "k11", "k03",
    };
    var mg = try fx.f.multiGet(alloc, table_key, &asked, .{});
    defer mg.deinit();
    for (asked) |k| {
        if (std.mem.eql(u8, k, "k00") or std.mem.eql(u8, k, "k11")) {
            try std.testing.expect(mg.item(k) == null);
            continue;
        }
        const it = mg.item(k) orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings(k, it.newest().value);
    }
}

test "multiget: prefetch reads the blocks the seeks land in, in one batch" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    // 300 rows of 1 KiB: one L0 table of several 64 KiB blocks.
    const wide = try alloc.alloc(u8, 1024);
    defer alloc.free(wide);
    @memset(wide, 'w');
    var name: [4]u8 = undefined;
    for (0..300) |i| {
        _ = try std.fmt.bufPrint(&name, "r{d:0>3}", .{i});
        try fx.put(&name, 10, wide);
    }
    try fx.flush();

    const tree = fx.f.getTree(ns, fx.tree_id) orelse
        return error.TestExpectedTree;
    defer tree.release();
    const tbl = blk: {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        const ents = tree.levels[0].entries(held);
        try std.testing.expectEqual(@as(usize, 1), ents.len);
        ents[0].table.incrRef();
        break :blk ents[0].table;
    };
    defer tbl.decrRef();
    const nblocks = tbl.blockCount();
    try std.testing.expect(nblocks >= 3);

    // blockFor names the block a seek opens.
    for ([_]String{ "r000", "r150", "r299" }) |k| {
        var it = zdb.table.TableIterator.init(alloc, tbl, table_key, .forward);
        defer it.deinit();
        try it.seekRange(Fixture.key(k), Key.inf);
        try std.testing.expect(it.itemRef() != null);
        const want = tbl.blockFor(Fixture.key(k)).?;
        try std.testing.expectEqual(want, it.bpos);
    }

    // Every block in one batch into a private cache; a second call
    // finds them all cached.
    var cache = try zdb.cache.ShardedCache(u128).init(alloc, 64 << 20, 2);
    defer cache.deinit();
    const idxs = try alloc.alloc(usize, nblocks);
    defer alloc.free(idxs);
    for (idxs, 0..) |*idx, i| idx.* = i;
    const read = try tbl.prefetchBlocks(&cache, alloc, idxs);
    try std.testing.expectEqual(nblocks, read);
    try std.testing.expectEqual(nblocks, cache.count());
    const misses = cache.misses.load(.monotonic);
    const again = try tbl.prefetchBlocks(&cache, alloc, idxs);
    try std.testing.expectEqual(@as(usize, 0), again);
    try std.testing.expectEqual(misses, cache.misses.load(.monotonic));
    try std.testing.expectEqual(@as(u64, nblocks), cache.hits.load(.monotonic));

    // A cache too small to admit the whole batch takes a prefix of it
    // and evicts nothing: every block read is still there.
    var small = try zdb.cache.ShardedCache(u128).init(alloc, 4 << 20, 2);
    defer small.deinit();
    const part = try tbl.prefetchBlocks(&small, alloc, idxs);
    try std.testing.expect(part >= 1 and part < nblocks);
    try std.testing.expectEqual(part, small.count());

    // A cached block decodes: block 0 starts at the first row.
    const key0: u128 = @as(u128, tbl.file_id) << 64;
    const val = cache.get(key0, .reuse) orelse return error.TestExpectedBlock;
    defer val.decrRef();
    var bi = try zdb.table.BlockIterator.init(
        alloc,
        val.data,
        try tbl.blockBaseKey(0),
        .{ .namespace = 0, .table_id = 0 },
    );
    defer bi.deinit();
    try bi.seekToFirst();
    const first = bi.itemRef() orelse return error.TestExpectedItem;
    try std.testing.expectEqualStrings("r000", first.key.primary);
}

test "readBlock: a bypass context neither fills nor marks the cache" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    const wide = try alloc.alloc(u8, 1024);
    defer alloc.free(wide);
    @memset(wide, 'w');
    var name: [4]u8 = undefined;
    for (0..300) |i| {
        _ = try std.fmt.bufPrint(&name, "r{d:0>3}", .{i});
        try fx.put(&name, 10, wide);
    }
    try fx.flush();

    const tree = fx.f.getTree(ns, fx.tree_id) orelse
        return error.TestExpectedTree;
    defer tree.release();
    const tbl = blk: {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        const ents = tree.levels[0].entries(held);
        ents[0].table.incrRef();
        break :blk ents[0].table;
    };
    defer tbl.decrRef();

    // A private cache: the process's is shared with the tests running
    // alongside.
    var cache = try zdb.cache.ShardedCache(u128).init(alloc, 64 << 20, 2);
    defer cache.deinit();

    const ctx = fiber.ctx();
    defer ctx.cache = .use;

    // Under bypass: the block is read but not kept.
    ctx.cache = .bypass;
    {
        const val = try tbl.readBlock_(&cache, 0);
        defer val.decrRef();
    }
    ctx.cache = .use;
    try std.testing.expectEqual(@as(usize, 0), cache.count());
    try std.testing.expectEqual(@as(u64, 0), cache.hits.load(.monotonic));

    // A normal read keeps it; a bypass read then takes it from the
    // cache and leaves it as it was.
    {
        const val = try tbl.readBlock_(&cache, 0);
        defer val.decrRef();
    }
    try std.testing.expectEqual(@as(usize, 1), cache.count());
    ctx.cache = .bypass;
    {
        const val = try tbl.readBlock_(&cache, 0);
        defer val.decrRef();
    }
    try std.testing.expectEqual(@as(usize, 1), cache.count());
}

const roar = struct {
    const delta_meta: Meta = .{ .kind = .delta, .encoding = .roaring };
    const state_meta: Meta = .{ .kind = .state, .encoding = .roaring };
    const tomb_meta: Meta = .{ .kind = .deleted, .encoding = .roaring };

    fn putDelta(
        fx: *Fixture,
        primary: String,
        ts: u64,
        adds: []const u64,
        rems: []const u64,
    ) !void {
        var list: std.ArrayList(u8) = .empty;
        defer list.deinit(fx.alloc);
        var w = try zdb.buffers.RoaringDeltaWriter.initWithBuf(fx.alloc, &list);
        if (adds.len > 0) try w.setAdds(adds);
        if (rems.len > 0) try w.setRemoves(rems);
        _ = try w.finish();
        try fx.putMeta(primary, ts, list.items, delta_meta);
    }

    fn putState(
        fx: *Fixture,
        primary: String,
        ts: u64,
        vals: []const u64,
    ) !void {
        var bm = try zroar.Bitmap.fromSortedList(fx.alloc, vals);
        defer bm.deinit();
        try fx.putMeta(primary, ts, bm.toBuffer(), state_meta);
    }

    /// `mg.bitmap(key)` against `Forest.getBitmap` and the expected set.
    fn expectBitmap(
        fx: *Fixture,
        mg: *const Forest.MultiGet,
        read_ts: u64,
        primary: String,
        expected: []const u64,
    ) !void {
        var bm = try mg.bitmap(fx.alloc, primary);
        defer bm.deinit();
        var single = try fx.f.getBitmap(fx.alloc, .{
            .key = Fixture.key(primary),
            .read_ts = read_ts,
        });
        defer single.deinit();
        try std.testing.expectEqual(expected.len, bm.getCardinality());
        try std.testing.expectEqual(expected.len, single.getCardinality());
        for (expected) |v| {
            try std.testing.expect(bm.contains(v));
            try std.testing.expect(single.contains(v));
        }
    }
};

test "multiget: postings fold across memtable, L0, tombstones and history" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    // p: a state in L0, deltas above it in the memtable.
    // q: a state, then a tombstone.
    // r: deltas only, no terminal anywhere.
    // s: absent.
    try roar.putState(&fx, "p", 10, &.{ 1, 2, 3 });
    try roar.putState(&fx, "q", 10, &.{ 7, 8 });
    try roar.putDelta(&fx, "r", 10, &.{ 1, 2 }, &.{});
    try fx.flush();
    try roar.putDelta(&fx, "p", 20, &.{ 40, 50 }, &.{2});
    try roar.putDelta(&fx, "p", 30, &.{2}, &.{ 1, 40 });
    try fx.putMeta("q", 15, "", roar.tomb_meta);
    try roar.putDelta(&fx, "r", 20, &.{3}, &.{1});

    const latest = std.math.maxInt(u64);
    var mg = try fx.f.multiGet(alloc, table_key, &.{ "s", "r", "q", "p" }, .{});
    defer mg.deinit();
    try roar.expectBitmap(&fx, &mg, latest, "p", &.{ 2, 3, 50 });
    try roar.expectBitmap(&fx, &mg, latest, "q", &.{});
    try roar.expectBitmap(&fx, &mg, latest, "r", &.{ 2, 3 });
    try roar.expectBitmap(&fx, &mg, latest, "s", &.{});

    // History: p before its second delta, q before its tombstone.
    var old = try fx.f.multiGet(alloc, table_key, &.{ "p", "q" }, .{
        .read_ts = 12,
    });
    defer old.deinit();
    try roar.expectBitmap(&fx, &old, 12, "p", &.{ 1, 2, 3 });
    try roar.expectBitmap(&fx, &old, 12, "q", &.{ 7, 8 });

    // A row is read like any key; only its bitmap is refused.
    try fx.put("t", 10, "row");
    var row = try fx.f.multiGet(alloc, table_key, &.{"t"}, .{});
    defer row.deinit();
    const row_val = row.item("t").?.newest().value;
    try std.testing.expectEqualStrings("row", row_val);
    try std.testing.expectError(error.NotRoaring, row.bitmap(alloc, "t"));
}

const WantRow = struct { key: String, value: String };

/// A reverse cursor over `range` at read_ts 100 with `pending` merged
/// in hands out exactly `want`, in that order.
fn expectReverse(
    fx: *Fixture,
    pending: *zdb.skl.SkipList,
    range: zdb.kv.KeyRange,
    max_rows: u64,
    want: []const WantRow,
) !void {
    var cur = try fx.f.openCursor(fx.alloc, table_key, range, .{
        .read_ts = 100,
        .pending = pending,
        .max_rows = max_rows,
        .direction = .reverse,
    });
    defer cur.deinit();
    for (want) |w| {
        const item = (try cur.next()) orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings(w.key, item.key.primary);
        try std.testing.expectEqualStrings(w.value, item.newest().value);
    }
    try std.testing.expect(try cur.next() == null);
}

test "cursor: reverse walks pending, memtables, L0 and L1 descending" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();

    // The layering of the multiget test: k01..k10 in L1, k03 and k04
    // rewritten in L0, k05 and k06 in memtables, k02 deleted, and the
    // txn's own k07 row and k08 tombstone.
    const base = [_]String{
        "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09", "k10",
    };
    for (base) |k| try fx.put(k, 10, "v1");
    try fx.flush();
    try fx.compactL0();
    try fx.put("k03", 20, "v2");
    try fx.put("k04", 20, "v2");
    try fx.flush();
    try fx.put("k04", 30, "v3");
    try fx.flush();
    try fx.put("k05", 40, "v4");
    try fx.freeze();
    try fx.put("k06", 50, "v5");
    try fx.putMeta("k02", 60, "", .{ .kind = .deleted });

    const pending = try zdb.skl.SkipList.initCapacity(alloc, 0);
    defer pending.release();
    try pending.putItem(.{
        .key = Fixture.key("k07"),
        .versions = .{ .one = .{
            .ts = zdb.kv.pending_ts,
            .meta = .{},
            .value = "pend",
        } },
    });
    try pending.putItem(.{
        .key = Fixture.key("k08"),
        .versions = .{ .one = .{
            .ts = zdb.kv.pending_ts,
            .meta = .{ .kind = .deleted },
            .value = "",
        } },
    });

    const all = [_]WantRow{
        .{ .key = "k10", .value = "v1" },
        .{ .key = "k09", .value = "v1" },
        .{ .key = "k07", .value = "pend" },
        .{ .key = "k06", .value = "v5" },
        .{ .key = "k05", .value = "v4" },
        .{ .key = "k04", .value = "v3" },
        .{ .key = "k03", .value = "v2" },
        .{ .key = "k01", .value = "v1" },
    };
    const unbounded = std.math.maxInt(u64);
    try expectReverse(&fx, pending, .all, unbounded, &all);
    // A range cuts both ends; a cap stops the walk early.
    try expectReverse(&fx, pending, .{
        .left = Fixture.key("k03"),
        .right = Fixture.key("k08"),
    }, unbounded, all[2..7]);
    try expectReverse(&fx, pending, .all, 3, all[0..3]);
}

test "multiget: allocation failure releases pending writes" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();
    const pending = try zdb.skl.SkipList.initCapacity(alloc, 0);
    defer {
        // Release leaked refs after measuring, so the failing test is safe
        // to repeat. The remaining owner ref stands in for the transaction.
        while (pending.ref_count.load(.acquire) > 1) pending.release();
        pending.release();
    }
    const keys = [_]String{ "a", "b", "c", "d" };
    for (keys) |key| {
        try pending.putItem(.{
            .key = Fixture.key(key),
            .versions = .{ .one = .{
                .ts = 10,
                .meta = .{},
                .value = "pending row",
            } },
        });
    }

    // With these four keys, the fifth allocation grows the pin array.
    var failing = std.testing.FailingAllocator.init(alloc, .{
        .fail_index = 4,
        .resize_fail_index = 0,
    });
    if (fx.f.multiGet(failing.allocator(), table_key, &keys, .{
        .read_ts = 10,
        .pending = pending,
    })) |result| {
        var mg = result;
        mg.deinit();
        return error.TestExpectedOutOfMemory;
    } else |err| {
        try std.testing.expectEqual(error.OutOfMemory, err);
    }
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(@as(u32, 1), pending.ref_count.load(.acquire));
}

test "cursor: allocation failure releases a pending value pin" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();
    const pending = try zdb.skl.SkipList.initCapacity(alloc, 0);
    defer {
        // Recover leaked refs after the assertion for repeatable runs.
        while (pending.ref_count.load(.acquire) > 1) pending.release();
        pending.release();
    }
    try pending.putItem(.{
        .key = Fixture.key("a"),
        .versions = .{ .one = .{
            .ts = 10,
            .meta = .{},
            .value = "pending row",
        } },
    });

    var failing = std.testing.FailingAllocator.init(alloc, .{
        .fail_index = 0,
    });
    {
        var cursor = try fx.f.openCursor(alloc, table_key, .{}, .{
            .read_ts = 10,
            .pending = pending,
            .pin_values = true,
        });
        defer cursor.deinit();
        // Keep the arena's backing allocator; fail only pin-list growth.
        cursor.allocator = failing.allocator();
        try std.testing.expectError(error.OutOfMemory, cursor.next());
    }
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(@as(u32, 1), pending.ref_count.load(.acquire));
}

test "cursor: allocation failure releases sources during merge handoff" {
    const alloc = std.testing.allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc);
    defer fx.teardown();
    const pending = try zdb.skl.SkipList.initCapacity(alloc, 0);
    const sources = [_]*zdb.skl.SkipList{ pending, fx.f.memtables.current };
    defer {
        // Recover leaked refs after the assertion for repeatable runs.
        for (sources) |source| {
            while (source.ref_count.load(.acquire) > 1) source.release();
        }
        pending.release();
    }
    try pending.putItem(.{
        .key = Fixture.key("a"),
        .versions = .{ .one = .{
            .ts = 10,
            .meta = .{},
            .value = "pending row",
        } },
    });

    // The second backing allocation grows openMerge's iterator list.
    var failing = std.testing.FailingAllocator.init(alloc, .{
        .fail_index = 1,
        .resize_fail_index = 0,
    });
    {
        var cursor = try fx.f.openCursor(
            failing.allocator(),
            table_key,
            .{},
            .{ .read_ts = 10, .pending = pending, .pin_values = false },
        );
        defer cursor.deinit();
        try std.testing.expectError(error.OutOfMemory, cursor.next());
    }
    try std.testing.expect(failing.has_induced_failure);
    for (sources) |source| {
        try std.testing.expectEqual(@as(u32, 1), source.ref_count.load(.acquire));
    }
}
