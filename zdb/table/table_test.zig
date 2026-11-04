// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const utils = @import("utils");
const compress = utils.compression;
const String = utils.String;
const zdb = @import("zdb");
const openBuffer = @import("../testing.zig").openBuffer;
const zbuf = @import("zbuf").zbuf;
const Key = zdb.kv.Key;
const TableKey = zdb.kv.TableKey;
/// The ids stamped on keys by tests that read a table on its own: an
/// SST stores primaries only, so any table key works.
const zero_table_key: TableKey = .{ .namespace = 0, .table_id = 0 };
const Item = zdb.kv.Item;
const Meta = zdb.kv.Meta;
const Version = zdb.kv.Version;
const vfs = zdb.vfs;

const Builder = zdb.table.builder.Builder;
const Table = zdb.table.Table;
const TableIterator = zdb.table.TableIterator;

/// Test helper: release the test's ref on a Table.
fn destroyTable(t: *Table) void {
    t.decrRef(); // the test's ref is the last one; this frees the table
}

/// Test helper: create an in-memory FileHandle from a buffer (dupes the data).

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a TableBuffer with entries keyed as "{prefix}{i:0>4}" for i in [start, end),
/// at ts=0, with value "{i}". block_size controls how many entries land per block.
fn buildRangeTable(
    allocator: std.mem.Allocator,
    prefix: String,
    start: usize,
    end: usize,
    block_size: u32,
) !zdb.table.builder.TableBuffer {
    var b = Builder.init(allocator, .{ .block_bytes = block_size });
    defer b.deinit();

    var i: usize = start;
    while (i < end) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key_no_ts = try std.fmt.bufPrint(&key_buf, "{s}{d:0>4}", .{ prefix, i });
        var val_buf: [20]u8 = undefined;
        const val = try std.fmt.bufPrint(&val_buf, "{d}", .{i});

        try b.add(.{ .primary = key_no_ts }, 0, val);
    }
    return b.finish();
}

/// Build a TableBuffer with n entries keyed as "{prefix}{i:0>4}" at ts=0,
/// with value "{i}". block_size controls how many entries land per block.
fn buildTestTable(
    allocator: std.mem.Allocator,
    prefix: String,
    n: usize,
    block_size: u32,
) !zdb.table.builder.TableBuffer {
    var b = Builder.init(allocator, .{ .block_bytes = block_size });
    defer b.deinit();

    var i: usize = 0;
    while (i < n) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key_no_ts = try std.fmt.bufPrint(&key_buf, "{s}{d:0>4}", .{ prefix, i });
        var val_buf: [20]u8 = undefined;
        const val = try std.fmt.bufPrint(&val_buf, "{d}", .{i});

        try b.add(.{ .primary = key_no_ts }, 0, val);
    }
    return b.finish();
}

test "builder: a key past max_primary_len is dropped, the rest built" {
    const alloc = std.testing.allocator;
    const long = try alloc.alloc(u8, Key.max_primary_len + 1);
    defer alloc.free(long);
    @memset(long, 'b');

    var b = Builder.init(alloc, .{ .block_bytes = 4 * 1024 });
    defer b.deinit();
    try b.add(.{ .primary = "a" }, 1, "1");
    try b.add(.{ .primary = long }, 1, "2");
    try b.add(.{ .primary = "c" }, 1, "3");
    var tb = try b.finish();
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
    defer it.deinit();
    try it.seekRange(Key.inf, Key.inf);
    try std.testing.expectEqualStrings("a", it.itemRef().?.key.primary);
    try it.next();
    try std.testing.expectEqualStrings("c", it.itemRef().?.key.primary);
    try it.next();
    try std.testing.expect(it.itemRef() == null);
}

// ---------------------------------------------------------------------------
// Existing tests (updated for itemRef / blocksAt)
// ---------------------------------------------------------------------------

// Builder.finish() pads the bytes so the total file size is a multiple of
// vfs.page_size. Verifies the math across several payload sizes that span
// page-boundary wraparound.
test "builder.finish output is page-aligned for any payload size" {
    const allocator = std.testing.allocator;
    const sizes = [_]usize{ 1, 32, 100, 1000, 4095, 4096, 4097, 9000 };
    for (sizes) |n| {
        var b = Builder.init(allocator, .{ .block_bytes = 4 * 1024 });
        defer b.deinit();
        var i: usize = 0;
        while (i < n) : (i += 1) {
            var key_buf: [32]u8 = undefined;
            const k = try std.fmt.bufPrint(&key_buf, "k{d:0>10}", .{i});
            try b.add(.{ .primary = k }, 0, "v");
        }
        var tb = try b.finish();
        defer tb.deinit();
        const remainder = tb.bytes.len % zdb.vfs.page_size;
        std.testing.expectEqual(@as(usize, 0), remainder) catch |err| {
            std.log.warn("payload n={d}: bytes.len={d}, page_size={d}, remainder={d}", .{
                n, tb.bytes.len, zdb.vfs.page_size, remainder,
            });
            return err;
        };
    }
}

test "table builder/reader roundtrip and seek" {
    var b = Builder.init(std.testing.allocator, .{ .block_bytes = 256 });
    defer b.deinit();

    try b.addItem(.{ .key = .{ .primary = "a" }, .versions = .{ .many = &.{
        .{ .ts = 9, .meta = .{}, .value = "va9" },
        .{ .ts = 7, .meta = .{}, .value = "va7" },
    } } });
    try b.add(.{ .primary = "b" }, 3, "vb3");
    try b.add(.{ .primary = "c" }, 1, "vc1");

    var tb = try b.finish();
    defer tb.deinit();

    const r = try openBuffer(std.testing.allocator, &tb);
    defer destroyTable(r);

    try std.testing.expectEqual(@as(u32, 4), r.index.key_count);
    try std.testing.expectEqual(@as(u64, 9), r.index.max_version);
    try std.testing.expect(r.blockCount() >= 1);

    var it = TableIterator.init(
        std.testing.allocator,
        r,
        zero_table_key,
        .forward,
    );
    defer it.deinit();
    try it.seekRange(Key.inf, Key.inf);
    try std.testing.expectEqualStrings("va9", it.itemRef().?.newest().value);

    try it.seekRange(.{ .primary = "b" }, Key.inf);
    try std.testing.expectEqualStrings("vb3", it.itemRef().?.newest().value);
}

test "table elects no codec when its sample does not compress" {
    // A value of random bytes: lz4 finds nothing to shrink, so the
    // table must come out uncompressed and still read back; a value of
    // one repeated byte shrinks, so the codec stays. One key per table:
    // a second copy of the same value would compress as a repeat. One
    // block, under the sample, so its codec byte alone says.
    var prng = std.Random.DefaultPrng.init(7);
    var random: [4096]u8 = undefined;
    prng.random().bytes(&random);
    const same = [_]u8{'x'} ** 4096;

    for ([_]struct { val: []const u8, want: compress.Codec }{
        .{ .val = &random, .want = .none },
        .{ .val = &same, .want = .lz4 },
    }) |case| {
        var b = Builder.init(std.testing.allocator, .{ .codec = .lz4 });
        defer b.deinit();
        try b.add(.{ .primary = "b" }, 1, case.val);
        var tb = try b.finish();
        defer tb.deinit();

        const r = try openBuffer(std.testing.allocator, &tb);
        defer destroyTable(r);
        try std.testing.expectEqual(case.want, blockCodec(tb.bytes, r, 0));

        var it = TableIterator.init(
            std.testing.allocator,
            r,
            zero_table_key,
            .forward,
        );
        defer it.deinit();
        try it.seekRange(.{ .primary = "b" }, Key.inf);
        const got = it.itemRef().?.newest().value;
        try std.testing.expectEqualSlices(u8, case.val, got);
    }
}

test "table election over many blocks, blocks stored per their own codec" {
    // Enough distinct random values to fill the sample and go past it,
    // in small blocks, one value each: the election drops the codec
    // mid-table, and every block reads back whatever it was stored
    // as. A table smaller than the sample never elects. The
    // compressible twin keeps its codec, and a mixed table keeps it
    // too, with its random blocks stored raw and its repeated ones
    // compressed: each block's codec byte says which it was.
    const alloc = std.testing.allocator;
    const val_len = 4096;
    const block = 8 * 1024;
    const elect_bytes = zdb.table.builder.Compression.elect_bytes;
    const past_sample = elect_bytes / val_len + 64;
    const under_sample = 16;

    const Fill = enum { random, same, mixed };
    for ([_]struct { n: usize, fill: Fill, want: ?compress.Codec }{
        .{ .n = past_sample, .fill = .random, .want = .none },
        .{ .n = under_sample, .fill = .random, .want = null },
        .{ .n = past_sample, .fill = .same, .want = .lz4 },
        .{ .n = past_sample, .fill = .mixed, .want = .lz4 },
    }) |case| {
        var prng = std.Random.DefaultPrng.init(11);
        var b = Builder.init(alloc, .{
            .codec = .lz4,
            .block_bytes = block,
        });
        defer b.deinit();
        var val: [val_len]u8 = undefined;
        var i: usize = 0;
        while (i < case.n) : (i += 1) {
            fillValue(&prng, case.fill, i, &val);
            var key: [8]u8 = undefined;
            std.mem.writeInt(u64, &key, i, .big);
            try b.add(.{ .primary = &key }, 1, &val);
        }
        var tb = try b.finish();
        defer tb.deinit();

        const r = try openBuffer(alloc, &tb);
        defer destroyTable(r);
        // `want` is what the election chose, null when the table was
        // too small to hold one; a random block is stored raw either way.
        try std.testing.expectEqual(case.want, b.compression.elected);
        try std.testing.expectEqual(case.n, r.blockCount());
        for (0..r.blockCount()) |bi| {
            const key = r.index.blocks[bi].key;
            const ki = std.mem.readInt(u64, key[0..8], .big);
            const is_random = case.fill == .random or
                (case.fill == .mixed and ki % 2 == 0);
            const want_block: compress.Codec = if (is_random) .none else .lz4;
            const got_block = blockCodec(tb.bytes, r, bi);
            try std.testing.expectEqual(want_block, got_block);
        }

        // Replay the values and check every key, first to last.
        prng = std.Random.DefaultPrng.init(11);
        var it = TableIterator.init(alloc, r, zero_table_key, .forward);
        defer it.deinit();
        try it.seekRange(Key.inf, Key.inf);
        i = 0;
        while (i < case.n) : (i += 1) {
            fillValue(&prng, case.fill, i, &val);
            const item = it.itemRef() orelse return error.TestUnexpectedResult;
            var key: [8]u8 = undefined;
            std.mem.writeInt(u64, &key, i, .big);
            try std.testing.expectEqualSlices(u8, &key, item.key.primary);
            try std.testing.expectEqualSlices(u8, &val, item.newest().value);
            try it.next();
        }
        try std.testing.expect(it.itemRef() == null);
    }
}

/// Block `i`'s codec byte, read from its envelope in the table bytes.
fn blockCodec(bytes: []const u8, r: *Table, i: usize) compress.Codec {
    const off: usize = r.index.blocks[i].offset;
    return @enumFromInt(bytes[off + 8]);
}

/// The i-th value of an election test table: random bytes, one
/// repeated byte, or the two alternating by key.
fn fillValue(
    prng: *std.Random.DefaultPrng,
    fill: anytype,
    i: usize,
    val: []u8,
) void {
    switch (fill) {
        .random => prng.random().bytes(val),
        .same => @memset(val, @as(u8, @truncate(i))),
        .mixed => if (i % 2 == 0)
            prng.random().bytes(val)
        else
            @memset(val, @as(u8, @truncate(i))),
    }
}

// seekGe compares stored keys in place, against the base key plus a diff,
// never materializing them. Two rules are easy to get wrong that way and
// unexercised by the fixed-width keys the other tests use: a primary that
// is a strict PREFIX of another sorts first (a < ab < abc < b), and among
// versions of one primary the NEWEST sorts first.
test "seekGe: prefix-length and version ordering" {
    const alloc = std.testing.allocator;

    var b = Builder.init(alloc, .{ .block_bytes = 4 * 1024 });
    defer b.deinit();
    try b.add(.{ .primary = "a" }, 5, "a5");
    try b.addItem(.{ .key = .{ .primary = "ab" }, .versions = .{ .many = &.{
        .{ .ts = 9, .meta = .{}, .value = "ab9" },
        .{ .ts = 3, .meta = .{}, .value = "ab3" },
    } } });
    try b.add(.{ .primary = "abc" }, 1, "abc1");
    try b.add(.{ .primary = "b" }, 7, "b7");

    var tb = try b.finish();
    defer tb.deinit();
    const t = try openBuffer(alloc, &tb);
    defer destroyTable(t);

    // One block, so every seek exercises the in-block search.
    try std.testing.expectEqual(@as(usize, 1), t.blockCount());

    var it = TableIterator.init(alloc, t, zero_table_key, .forward);
    defer it.deinit();

    // Stored order is the seek order, one item per key.
    const want = [_]String{ "a5", "ab9", "abc1", "b7" };
    try it.seekRange(Key.inf, Key.inf);
    for (want) |v| {
        try std.testing.expectEqualStrings(v, it.itemRef().?.newest().value);
        try it.next();
    }
    try std.testing.expect(it.itemRef() == null);

    const cases = [_]struct { k: Key, want: String }{
        // A seek names a primary and lands on its newest version.
        .{ .k = .{ .primary = "ab" }, .want = "ab9" },
        // Shorter primary sorts before its own extensions.
        .{ .k = .{ .primary = "a" }, .want = "a5" },
        .{ .k = .{ .primary = "abc" }, .want = "abc1" },
        // Absent keys land on the next one that exists.
        .{ .k = .{ .primary = "aa" }, .want = "ab9" },
        .{ .k = .{ .primary = "az" }, .want = "b7" },
    };
    for (cases) |c| {
        try it.seekRange(c.k, Key.inf);
        const got = it.itemRef().?.newest().value;
        try std.testing.expectEqualStrings(c.want, got);
    }

    // A key's item carries its older versions after the newest one.
    try it.seekRange(.{ .primary = "ab" }, Key.inf);
    const ab = it.itemRef().?;
    try std.testing.expectEqualStrings("ab3", ab.versions.slice()[1].value);
    try it.next();
    try std.testing.expectEqualStrings("abc1", it.itemRef().?.newest().value);

    // Past every key: nothing left.
    try it.seekRange(.{ .primary = "c" }, Key.inf);
    try std.testing.expect(it.itemRef() == null);
}

test "iterator window reads blocks in spans and yields the same items" {
    const alloc = std.testing.allocator;
    var tb = try buildRangeTable(alloc, "w", 0, 2000, 4 * 1024);
    defer tb.deinit();
    const t = try openBuffer(alloc, &tb);
    defer destroyTable(t);
    try std.testing.expect(t.blockCount() > 4);

    // Block by block, and through a window of a few blocks.
    var plain = TableIterator.init(alloc, t, zero_table_key, .forward);
    defer plain.deinit();
    var win = TableIterator.init(alloc, t, zero_table_key, .forward);
    defer win.deinit();
    win.window_bytes = 10 * 1024;

    try plain.seekRange(Key.inf, Key.inf);
    try win.seekRange(Key.inf, Key.inf);
    // The first window spans more than its block.
    const first = t.index.blocks[0];
    try std.testing.expect(win.window.?.data.len > first.len);
    var n: usize = 0;
    while (plain.itemRef()) |want| : (n += 1) {
        const got = win.itemRef() orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings(want.key.primary, got.key.primary);
        try std.testing.expectEqualStrings(
            want.newest().value,
            got.newest().value,
        );
        try plain.next();
        try win.next();
    }
    try std.testing.expect(win.itemRef() == null);
    try std.testing.expectEqual(@as(usize, 2000), n);

    // A seek into the middle lands on the same key, bounded the same.
    const start: Key = .{ .primary = "w1234" };
    const end: Key = .{ .primary = "w1300" };
    try plain.seekRange(start, end);
    try win.seekRange(start, end);
    n = 0;
    while (plain.itemRef()) |want| : (n += 1) {
        const got = win.itemRef() orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings(want.key.primary, got.key.primary);
        try plain.next();
        try win.next();
    }
    try std.testing.expect(win.itemRef() == null);
    try std.testing.expectEqual(@as(usize, 66), n);
}

test "table stores primaries only and the iterator stamps the tree's ids" {
    const alloc = std.testing.allocator;

    // Keys written with one namespace and table_id...
    var b = Builder.init(alloc, .{ .block_bytes = 4 * 1024 });
    defer b.deinit();
    for (0..300) |i| {
        var key_buf: [16]u8 = undefined;
        const primary = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{i});
        try b.add(.{ .namespace = 7, .table_id = 3, .primary = primary }, @intCast(i + 1), "v");
    }
    var tb = try b.finish();
    defer tb.deinit();

    const t = try openBuffer(alloc, &tb);
    defer destroyTable(t);

    // ...leave no trace of them in the file: the bounds and the block
    // base keys are primaries.
    try std.testing.expectEqual(@as(u64, 0), t.smallest.namespace);
    try std.testing.expectEqual(@as(u32, 0), t.smallest.table_id);
    try std.testing.expectEqualStrings("k000000", t.smallest.primary);
    try std.testing.expectEqualStrings("k000299", t.biggest.primary);
    try std.testing.expectEqualStrings("k000000", try t.blockBaseKey(0));
    try std.testing.expect(t.blockCount() > 1);
    try std.testing.expect(std.mem.startsWith(u8, try t.blockBaseKey(1), "k"));

    // A tree that lists the file opens it with its own ids, and every
    // key comes back with them, whatever the writer's were. Seeks use
    // the primary, so a full key of that tree lands.
    for ([_]TableKey{
        .{ .namespace = 7, .table_id = 3 },
        .{ .namespace = 9, .table_id = 40 },
    }) |owner| {
        var it = TableIterator.init(alloc, t, owner, .forward);
        defer it.deinit();
        try it.seekRange(.{
            .namespace = owner.namespace,
            .table_id = owner.table_id,
            .primary = "k000150",
        }, .{
            .namespace = owner.namespace,
            .table_id = owner.table_id,
            .primary = "k000153",
        });
        var n: usize = 0;
        while (it.itemRef()) |item| : (try it.next()) {
            try std.testing.expectEqual(owner.namespace, item.key.namespace);
            try std.testing.expectEqual(owner.table_id, item.key.table_id);
            var want_buf: [16]u8 = undefined;
            const want = try std.fmt.bufPrint(&want_buf, "k{d:0>6}", .{150 + n});
            try std.testing.expectEqualStrings(want, item.key.primary);
            n += 1;
        }
        try std.testing.expectEqual(@as(usize, 3), n);
    }
}

test "table iterator reads the table's infinity as no bound" {
    const alloc = std.testing.allocator;

    var b = Builder.init(alloc, .{ .block_bytes = 4 * 1024 });
    defer b.deinit();
    for (0..50) |i| {
        var key_buf: [16]u8 = undefined;
        const primary = try std.fmt.bufPrint(&key_buf, "k{d:0>3}", .{i});
        try b.add(.{ .primary = primary }, @intCast(i + 1), "v");
    }
    var tb = try b.finish();
    defer tb.deinit();
    const t = try openBuffer(alloc, &tb);
    defer destroyTable(t);

    const count = struct {
        fn f(a: std.mem.Allocator, tbl: *Table, start: Key, end: Key) !usize {
            var it = TableIterator.init(
                a,
                tbl,
                .{ .namespace = 5, .table_id = 20 },
                .forward,
            );
            defer it.deinit();
            try it.seekRange(start, end);
            var n: usize = 0;
            while (it.itemRef() != null) : (try it.next()) n += 1;
            return n;
        }
    }.f;
    const own = struct {
        fn key(primary: String) Key {
            return .{ .namespace = 5, .table_id = 20, .primary = primary };
        }
    };

    // A whole-table scan, as `KeyRange.limitToTable` builds it: between
    // the table's edges. An edge's primary is empty, but it is a bound
    // for a memtable, and no bound here.
    const left = Key.tableLeft(.{ .namespace = 5, .table_id = 20 });
    const end = Key.tableRight(.{ .namespace = 5, .table_id = 20 });
    try std.testing.expect(!end.isInf());
    try std.testing.expectEqual(
        @as(usize, 50),
        try count(alloc, t, left, end),
    );
    try std.testing.expectEqual(
        @as(usize, 50),
        try count(alloc, t, own.key("\x00"), end),
    );
    try std.testing.expectEqual(
        @as(usize, 50),
        try count(alloc, t, Key.inf, Key.inf),
    );
    // Within the table, primaries decide.
    try std.testing.expectEqual(@as(usize, 10), try count(alloc, t, own.key("k010"), own.key("k020")));
    try std.testing.expectEqual(
        @as(usize, 40),
        try count(alloc, t, own.key("k010"), end),
    );
    // The edges sort outside every key of their table and inside the
    // neighbouring tables' keys.
    try std.testing.expect(left.order(own.key("\x00")) == .lt);
    try std.testing.expect(own.key("k049").order(end) == .lt);
    const next_table = Key{ .namespace = 5, .table_id = 21, .primary = "\x00" };
    try std.testing.expect(end.order(next_table) == .lt);
    try std.testing.expect(end.orderPrimary(own.key("zzz")) == .gt);
}

test "table binary fuse contains keys" {
    const alloc = std.testing.allocator;

    var b = Builder.init(alloc, .{ .block_bytes = 4 * 1024 });
    defer b.deinit();

    const n: usize = 1024;
    var key_buf: [16]u8 = undefined;
    for (0..n) |i| {
        const key = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{i});
        try b.add(.{ .primary = key }, @intCast(i + 1), "v");
    }

    var tb = try b.finish();
    defer tb.deinit();

    const t = try openBuffer(alloc, &tb);
    defer destroyTable(t);

    // Sample present keys.
    try std.testing.expect(t.containsPrimary("k000000"));
    try std.testing.expect(t.containsPrimary("k000500"));
    try std.testing.expect(t.containsPrimary("k001023"));

    // Absent keys — BF should reject (containsPrimary returns false).
    try std.testing.expect(!t.containsPrimary("missing-1"));
    try std.testing.expect(!t.containsPrimary("missing-2"));
    try std.testing.expect(!t.containsPrimary("missing-3"));

    // What the open table pins: its index and the fuse filter, no
    // egid bitmap (`Table.resident` sums these over open tables).
    try std.testing.expect(t.resident.index > 0);
    try std.testing.expectEqual(t.binary_fuse.?.sizeInBytes(), t.resident.filter);
    try std.testing.expectEqual(@as(u64, 0), t.resident.egids);
}

test "table egid filter is exact and replaces the fuse filter" {
    const alloc = std.testing.allocator;

    var b = Builder.init(alloc, .{ .block_bytes = 4 * 1024, .kind = .rows });
    defer b.deinit();

    // Every third egid from 3 to 3000, ascending as a base table's rows
    // come.
    var key_buf: [8]u8 = undefined;
    var egid: u64 = 3;
    while (egid <= 3000) : (egid += 3) {
        std.mem.writeInt(u64, &key_buf, egid, .big);
        try b.add(.{ .primary = &key_buf }, egid, "v");
    }

    var tb = try b.finish();
    defer tb.deinit();

    const t = try openBuffer(alloc, &tb);
    defer destroyTable(t);

    // An egid table pins its bitmap in place of a fuse filter.
    try std.testing.expectEqual(@as(u64, 0), t.resident.filter);
    try std.testing.expect(t.resident.egids > 0);
    try std.testing.expect(t.egid_bm != null);
    try std.testing.expect(t.binary_fuse == null);

    // Exact: every present id hits, every absent one misses. A fuse
    // filter could only promise the first.
    egid = 1;
    while (egid <= 3003) : (egid += 1) {
        std.mem.writeInt(u64, &key_buf, egid, .big);
        const want = egid % 3 == 0 and egid <= 3000;
        try std.testing.expectEqual(want, t.containsPrimary(&key_buf));
        try std.testing.expectEqual(
            want,
            t.containsPrimaryHash(&key_buf, zdb.kv.bloomHash(&key_buf)),
        );
    }
    // Not an egid at all: certainly not in an egid table.
    try std.testing.expect(!t.containsPrimary("k000003"));
}

test "table egid filter of dense ids is far smaller than a fuse filter" {
    const alloc = std.testing.allocator;

    // 70,000 contiguous egids, a flush table's worth of rows: two 64k
    // chunks, each one 8 KiB bitmap container. A fuse filter is about
    // two bytes per key, 140 KiB here.
    var b = Builder.init(alloc, .{ .block_bytes = 64 * 1024, .kind = .rows });
    defer b.deinit();
    var key_buf: [8]u8 = undefined;
    var egid: u64 = 1;
    while (egid <= 70_000) : (egid += 1) {
        std.mem.writeInt(u64, &key_buf, egid, .big);
        try b.add(.{ .primary = &key_buf }, egid, "v");
    }
    var tb = try b.finish();
    defer tb.deinit();

    const t = try openBuffer(alloc, &tb);
    defer destroyTable(t);
    // The bitmap's buffer is the filter as stored; the index struct's
    // copy of it is released on open.
    try std.testing.expect(t.egid_bm.?.toBuffer().len < 20 * 1024);
    try std.testing.expectEqual(0, t.index.egid_bitmap.len);
    std.mem.writeInt(u64, &key_buf, 70_000, .big);
    try std.testing.expect(t.containsPrimary(&key_buf));
    std.mem.writeInt(u64, &key_buf, 70_001, .big);
    try std.testing.expect(!t.containsPrimary(&key_buf));
}

test "table egid builder rejects a key that is not an egid" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, .{ .kind = .rows });
    defer b.deinit();
    try std.testing.expectError(error.KeyNotEgid, b.add(.{ .primary = "k000001" }, 1, "v"));
}

test "table binary fuse large keyset contains sampled keys" {
    const alloc = std.testing.allocator;
    const n: usize = 1_000_000;
    const sample: usize = 10_000;

    var b = Builder.init(alloc, .{ .block_bytes = 32 * 1024 });
    defer b.deinit();

    var i: usize = 0;
    while (i < n) : (i += 1) {
        var key_buf: [16]u8 = undefined;
        const primary = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{i});
        try b.add(.{ .primary = primary }, 0, "v");
    }

    var tb = try b.finish();
    defer tb.deinit();

    const t = try openBuffer(alloc, &tb);
    defer destroyTable(t);

    const seed: u64 = @intCast(std.Io.Timestamp.now(utils.context.g().io, .real).nanoseconds);
    var prng = std.Random.DefaultPrng.init(seed);
    const rnd = prng.random();

    i = 0;
    while (i < sample) : (i += 1) {
        const idx = rnd.uintAtMost(u64, n - 1);
        var key_buf: [16]u8 = undefined;
        const key_no_ts = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{idx});
        try std.testing.expect(t.containsPrimary(key_no_ts));
    }

    var non_added_matches: usize = 0;
    i = 0;
    while (i < sample) : (i += 1) {
        const idx = @as(u64, n) + rnd.uintAtMost(u64, n - 1);
        var key_buf: [16]u8 = undefined;
        const key_no_ts = try std.fmt.bufPrint(&key_buf, "k{d:0>6}", .{idx});
        if (t.containsPrimary(key_no_ts)) {
            non_added_matches += 1;
        }
    }
    // Some false positives are possible.
    try std.testing.expect(non_added_matches < 10);
}

test "table checksum mismatch is detected" {
    var b = Builder.init(std.testing.allocator, .{ .block_bytes = 256 });
    defer b.deinit();

    try b.add(.{ .primary = "k1" }, 1, "v1");
    try b.add(.{ .primary = "k2" }, 1, "v2");

    var tb = try b.finish();
    defer tb.deinit();

    const r = try openBuffer(std.testing.allocator, &tb);
    defer destroyTable(r);

    const first_meta = r.index.blocks[0];
    const block_start: usize = first_meta.offset;
    const block_end = block_start + first_meta.len;
    r.obj.file.owned_buf.?[block_end - 5] ^= 0x01;

    try std.testing.expectError(error.ChecksumMismatch, r.readBlock(0));
}

test "merge iterator merges sorted streams" {
    var b1 = Builder.init(std.testing.allocator, .{ .block_bytes = 256 });
    defer b1.deinit();
    try b1.add(.{ .primary = "a" }, 5, "a5");
    try b1.add(.{ .primary = "c" }, 1, "c1");
    var t1 = try b1.finish();
    defer t1.deinit();

    var b2 = Builder.init(std.testing.allocator, .{ .block_bytes = 256 });
    defer b2.deinit();
    try b2.add(.{ .primary = "b" }, 4, "b4");
    try b2.add(.{ .primary = "d" }, 1, "d1");
    var t2 = try b2.finish();
    defer t2.deinit();

    const r1 = try openBuffer(std.testing.allocator, &t1);
    defer destroyTable(r1);
    const r2 = try openBuffer(std.testing.allocator, &t2);
    defer destroyTable(r2);

    var iters = [_]zdb.table.merge_iterator.Iterator{
        .{ .table = TableIterator.init(
            std.testing.allocator,
            r1,
            zero_table_key,
            .forward,
        ) },
        .{ .table = TableIterator.init(
            std.testing.allocator,
            r2,
            zero_table_key,
            .forward,
        ) },
    };
    const MergeIterator = zdb.table.merge_iterator.MergeIterator;
    var m = try MergeIterator.init(
        std.testing.allocator,
        iters[0..],
        .forward,
    );
    defer m.deinit();

    try m.seekRange(Key.inf, Key.inf);
    try std.testing.expectEqualStrings("a5", m.itemRef().?.newest().value);
    try m.next();
    try std.testing.expectEqualStrings("b4", m.itemRef().?.newest().value);
    try m.next();
    try std.testing.expectEqualStrings("c1", m.itemRef().?.newest().value);
    try m.next();
    try std.testing.expectEqualStrings("d1", m.itemRef().?.newest().value);
    try m.next();
    try std.testing.expect(m.itemRef() == null);
}

test "merge iterator gathers one primary from every source" {
    const alloc = std.testing.allocator;
    // Three tables holding versions of "k" with interleaved timestamps;
    // t1 and t2 both hold a version at 10.
    var b1 = Builder.init(alloc, .{ .block_bytes = 256 });
    defer b1.deinit();
    try b1.addItem(.{ .key = .{ .primary = "k" }, .versions = .{ .many = &.{
        .{ .ts = 30, .meta = .{}, .value = "k30" },
        .{ .ts = 10, .meta = .{}, .value = "k10" },
    } } });
    try b1.add(.{ .primary = "z" }, 5, "z5");
    var t1 = try b1.finish();
    defer t1.deinit();

    var b2 = Builder.init(alloc, .{ .block_bytes = 256 });
    defer b2.deinit();
    try b2.addItem(.{ .key = .{ .primary = "k" }, .versions = .{ .many = &.{
        .{ .ts = 20, .meta = .{}, .value = "k20" },
        .{ .ts = 10, .meta = .{}, .value = "k10-dup" },
    } } });
    var t2 = try b2.finish();
    defer t2.deinit();

    var b3 = Builder.init(alloc, .{ .block_bytes = 256 });
    defer b3.deinit();
    try b3.add(.{ .primary = "a" }, 1, "a1");
    try b3.add(.{ .primary = "k" }, 25, "k25");
    var t3 = try b3.finish();
    defer t3.deinit();

    const r1 = try openBuffer(alloc, &t1);
    defer destroyTable(r1);
    const r2 = try openBuffer(alloc, &t2);
    defer destroyTable(r2);
    const r3 = try openBuffer(alloc, &t3);
    defer destroyTable(r3);

    var iters = [_]zdb.table.merge_iterator.Iterator{
        .{ .table = TableIterator.init(alloc, r1, zero_table_key, .forward) },
        .{ .table = TableIterator.init(alloc, r2, zero_table_key, .forward) },
        .{ .table = TableIterator.init(alloc, r3, zero_table_key, .forward) },
    };
    var m = try zdb.table.merge_iterator.MergeIterator.init(
        alloc,
        iters[0..],
        .forward,
    );
    defer m.deinit();

    try m.seekRange(Key.inf, Key.inf);
    const a = m.itemRef().?;
    try std.testing.expectEqualStrings("a", a.key.primary);
    try std.testing.expectEqual(@as(usize, 1), a.versions.len());
    try std.testing.expectEqual(@as(usize, 1), m.currentPins().len);

    // k: every version of it, timestamp descending, drawn from all
    // three sources; the duplicate at 10 comes from the lower source.
    try m.next();
    const k = m.itemRef().?;
    try std.testing.expectEqualStrings("k", k.key.primary);
    const want_ts = [_]u64{ 30, 25, 20, 10 };
    const want_val = [_][]const u8{ "k30", "k25", "k20", "k10" };
    const k_vers = k.versions.slice();
    try std.testing.expectEqual(want_ts.len, k_vers.len);
    for (k_vers, want_ts, want_val) |ver, ts, val| {
        try std.testing.expectEqual(ts, ver.ts);
        try std.testing.expectEqualStrings(val, ver.value);
    }
    // One pin per contributing block; the item stays valid while the
    // merge is parked on it.
    try std.testing.expectEqual(@as(usize, 3), m.currentPins().len);

    try m.next();
    const z = m.itemRef().?;
    try std.testing.expectEqualStrings("z5", z.newest().value);
    try std.testing.expectEqual(@as(usize, 1), m.currentPins().len);
    try m.next();
    try std.testing.expect(m.itemRef() == null);
    try std.testing.expectEqual(@as(usize, 0), m.currentPins().len);

    // A seek at k lands on the whole of k; an end bound at z excludes z.
    try m.seekRange(.{ .primary = "k" }, .{ .primary = "z" });
    try std.testing.expectEqual(@as(usize, 4), m.itemRef().?.versions.len());
    try m.next();
    try std.testing.expect(m.itemRef() == null);
}

/// A table with every item of `items`, in one 4 KiB-block builder.
fn buildItems(alloc: std.mem.Allocator, items: []const Item) !*Table {
    var b = Builder.init(alloc, .{ .block_bytes = 4 * 1024 });
    defer b.deinit();
    for (items) |item| try b.addItem(item);
    var tb = try b.finish();
    defer tb.deinit();
    return openBuffer(alloc, &tb);
}

test "packed: one entry holds every version of a key" {
    const alloc = std.testing.allocator;
    var many: [40]Version = undefined;
    for (&many, 0..) |*ver, i| {
        ver.* = .{ .ts = 1000 - i, .meta = .{}, .value = "m" };
    }
    const tombstone: Meta = .{ .kind = .deleted };
    const items = [_]Item{
        .{ .key = .{ .primary = "one" }, .versions = .{
            .one = .{ .ts = 7, .meta = .{}, .value = "v7" },
        } },
        .{ .key = .{ .primary = "three" }, .versions = .{ .many = &.{
            .{ .ts = 30, .meta = .{ .kind = .delta }, .value = "d30" },
            .{ .ts = 20, .meta = tombstone, .value = "" },
            .{ .ts = 10, .meta = .{}, .value = "s10" },
        } } },
        .{ .key = .{ .primary = "two" }, .versions = .{ .many = &.{
            .{ .ts = 5, .meta = .{}, .value = "v5" },
            .{ .ts = 1, .meta = tombstone, .value = "" },
        } } },
        .{ .key = .{ .primary = "zmany" }, .versions = .{ .many = &many } },
    };
    const t = try buildItems(alloc, &items);
    defer destroyTable(t);
    try std.testing.expectEqual(@as(u32, 46), t.index.key_count);
    try std.testing.expectEqual(@as(u64, 1000), t.index.max_version);

    var it = TableIterator.init(alloc, t, zero_table_key, .forward);
    defer it.deinit();
    try it.seekRange(Key.inf, Key.inf);
    for (items) |want| {
        const got = it.itemRef().?;
        try std.testing.expectEqualStrings(want.key.primary, got.key.primary);
        try std.testing.expectEqual(want.versions.len(), got.versions.len());
        for (want.versions.slice(), got.versions.slice()) |w, g| {
            try std.testing.expectEqual(w.ts, g.ts);
            try std.testing.expectEqual(w.meta, g.meta);
            try std.testing.expectEqualStrings(w.value, g.value);
        }
        try it.next();
    }
    try std.testing.expect(it.itemRef() == null);

    // A seek between keys lands on the next one; an end bound at a key
    // excludes it.
    try it.seekRange(.{ .primary = "th" }, .{ .primary = "two" });
    try std.testing.expectEqualStrings("three", it.itemRef().?.key.primary);
    try it.next();
    try std.testing.expect(it.itemRef() == null);
}

test "packed: a chain past the block target is still one entry" {
    const alloc = std.testing.allocator;
    var vers: [64]Version = undefined;
    const wide = try alloc.alloc(u8, 256);
    defer alloc.free(wide);
    @memset(wide, 'w');
    for (&vers, 0..) |*ver, i| {
        ver.* = .{ .ts = 100 - i, .meta = .{}, .value = wide };
    }
    var b = Builder.init(alloc, .{ .block_bytes = 1024 });
    defer b.deinit();
    try b.add(.{ .primary = "a" }, 1, "a");
    try b.addItem(.{
        .key = .{ .primary = "chain" },
        .versions = .{ .many = &vers },
    });
    try b.add(.{ .primary = "z" }, 1, "z");
    var tb = try b.finish();
    defer tb.deinit();
    const t = try openBuffer(alloc, &tb);
    defer destroyTable(t);

    var it = TableIterator.init(alloc, t, zero_table_key, .forward);
    defer it.deinit();
    try it.seekRange(.{ .primary = "chain" }, Key.inf);
    const got = it.itemRef().?;
    try std.testing.expectEqual(@as(usize, 64), got.versions.len());
    try std.testing.expectEqualStrings(wide, got.versions.slice()[63].value);
    try it.next();
    try std.testing.expectEqualStrings("z", it.itemRef().?.key.primary);
}

test "packed: version order and key order are enforced" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, .{});
    defer b.deinit();
    // Equal timestamps within an item.
    try std.testing.expectError(error.KeyOrderViolation, b.addItem(.{
        .key = .{ .primary = "a" },
        .versions = .{ .many = &.{
            .{ .ts = 5, .meta = .{}, .value = "x" },
            .{ .ts = 5, .meta = .{}, .value = "y" },
        } },
    }));
    // A rising timestamp within an item.
    try std.testing.expectError(error.KeyOrderViolation, b.addItem(.{
        .key = .{ .primary = "a" },
        .versions = .{ .many = &.{
            .{ .ts = 5, .meta = .{}, .value = "x" },
            .{ .ts = 6, .meta = .{}, .value = "y" },
        } },
    }));
    try b.add(.{ .primary = "a" }, 5, "x");
    // The same key twice, and a key below the last.
    try std.testing.expectError(
        error.KeyOrderViolation,
        b.add(.{ .primary = "a" }, 4, "x"),
    );
    try std.testing.expectError(
        error.KeyOrderViolation,
        b.add(.{ .primary = "0" }, 9, "x"),
    );
}

test "packed: n versions cost at most 4 + 13n bytes over one" {
    const alloc = std.testing.allocator;
    // Two tables with the same key set; in the second the key holds n
    // versions of the same bytes as the single one, so the difference
    // is the packed header alone.
    const n: usize = 10;
    var vers: [n]Version = undefined;
    for (&vers, 0..) |*ver, i| {
        ver.* = .{ .ts = 100 - i, .meta = .{}, .value = "" };
    }
    const one = [_]Item{
        .{ .key = .{ .primary = "k" }, .versions = .{ .one = vers[0] } },
    };
    const all = [_]Item{
        .{ .key = .{ .primary = "k" }, .versions = .{ .many = &vers } },
    };
    const t1 = try buildItems(alloc, &one);
    defer destroyTable(t1);
    const tn = try buildItems(alloc, &all);
    defer destroyTable(tn);
    const grew = tn.index.uncompressed_size - t1.index.uncompressed_size;
    const ver_hdr = zdb.buffers.BlockVersionReader.SIZE;
    try std.testing.expectEqual(ver_hdr * (n - 1), grew);
}

// ---------------------------------------------------------------------------
// TestTableIterator: iterate forward over n entries, verify each key+value
// ---------------------------------------------------------------------------

test "table iterator: forward scan" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 99, 100, 101 }) |n| {
        var tb = try buildTestTable(alloc, "key", n, 4096);
        defer tb.deinit();
        const tbl = try openBuffer(alloc, &tb);
        defer destroyTable(tbl);

        var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
        defer it.deinit();

        var count: usize = 0;
        try it.seekRange(Key.inf, Key.inf);
        while (it.itemRef()) |entry| : (count += 1) {
            var key_buf: [32]u8 = undefined;
            const expected_key_no_ts = try std.fmt.bufPrint(&key_buf, "key{d:0>4}", .{count});
            try std.testing.expectEqualStrings(expected_key_no_ts, entry.key.primary);

            var val_buf: [20]u8 = undefined;
            const expected_val = try std.fmt.bufPrint(&val_buf, "{d}", .{count});
            const got = entry.newest().value;
            try std.testing.expectEqualStrings(expected_val, got);

            try it.next();
        }
        try std.testing.expectEqual(n, count);
    }
}

// ---------------------------------------------------------------------------
// TestSeekToFirst: seekToFirst lands on the first entry
// ---------------------------------------------------------------------------

test "seek to first" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 99, 100, 101, 199, 200, 250, 9999, 10000 }) |n| {
        var tb = try buildTestTable(alloc, "key", n, 4096);
        defer tb.deinit();
        const tbl = try openBuffer(alloc, &tb);
        defer destroyTable(tbl);

        var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
        defer it.deinit();

        try it.seekRange(Key.inf, Key.inf);
        const entry = it.itemRef() orelse return error.UnexpectedNull;
        try std.testing.expectEqualStrings("0", entry.newest().value);
        try std.testing.expectEqualStrings("key0000", entry.key.primary);
    }
}

// ---------------------------------------------------------------------------
// TestSeek: various seek targets on a 10 000-entry table
// ---------------------------------------------------------------------------

test "seek: various targets" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "k", 10000, 4096);
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
    defer it.deinit();

    const Case = struct { in: String, valid: bool, out: String };
    const cases = [_]Case{
        .{ .in = "abc", .valid = true, .out = "k0000" }, // before all keys
        .{ .in = "k0100", .valid = true, .out = "k0100" }, // exact hit
        .{ .in = "k0100b", .valid = true, .out = "k0101" }, // falls between blocks
        .{ .in = "k1234", .valid = true, .out = "k1234" },
        .{ .in = "k1234b", .valid = true, .out = "k1235" },
        .{ .in = "k9999", .valid = true, .out = "k9999" },
        .{ .in = "z", .valid = false, .out = "" }, // past end
    };

    for (cases) |c| {
        try it.seekRange(.{ .primary = c.in }, Key.inf);
        if (!c.valid) {
            try std.testing.expect(it.itemRef() == null);
            continue;
        }
        const entry = it.itemRef() orelse return error.UnexpectedNull;
        try std.testing.expectEqualStrings(c.out, entry.key.primary);
    }
}

// ---------------------------------------------------------------------------
// TestIterateFromStart: full forward scan for various n
// ---------------------------------------------------------------------------

test "iterate from start: full scan" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 99, 100, 101, 199, 200, 250, 9999, 10000 }) |n| {
        var tb = try buildTestTable(alloc, "key", n, 4096);
        defer tb.deinit();
        const tbl = try openBuffer(alloc, &tb);
        defer destroyTable(tbl);

        var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
        defer it.deinit();

        var count: usize = 0;
        try it.seekRange(Key.inf, Key.inf);
        while (it.itemRef() != null) : (count += 1) {
            try it.next();
        }
        try std.testing.expectEqual(n, count);
    }
}

// ---------------------------------------------------------------------------
// TestTable: seek to a mid-point, iterate to end; seek past end; seek before start
// ---------------------------------------------------------------------------

test "table: seek and iterate to end" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "key", 10000, 4096);
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
    defer it.deinit();

    // Seek to key1010, iterate to end — should visit keys 1010..9999.
    try it.seekRange(.{ .primary = "key1010" }, Key.inf);
    var kid: usize = 1010;
    while (it.itemRef()) |entry| : (kid += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "key{d:0>4}", .{kid});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 10000), kid);

    // Seek past end → invalid.
    try it.seekRange(.{ .primary = "key99999" }, Key.inf);
    try std.testing.expect(it.itemRef() == null);

    // Seek before start → first key.
    try it.seekRange(.{ .primary = "aaa" }, Key.inf);
    const first = it.itemRef() orelse return error.UnexpectedNull;
    try std.testing.expectEqualStrings("key0000", first.key.primary);
}

// ---------------------------------------------------------------------------
// TestMaxVersion: max_version is the highest timestamp seen
// ---------------------------------------------------------------------------

test "table: max_version" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, .{ .block_bytes = 4096 });
    defer b.deinit();

    const n = 1000;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var buf: [32]u8 = undefined;
        const key_str = try std.fmt.bufPrint(&buf, "foo:{d:0>4}", .{i});
        try b.add(.{ .primary = key_str }, i + 1, "");
    }

    var tb = try b.finish();
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    try std.testing.expectEqual(@as(u64, n), tbl.index.max_version);
}

// ---------------------------------------------------------------------------
// Smallest/biggest: Table open fills these from the index
// ---------------------------------------------------------------------------

test "table: smallest and biggest keys" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "key", 200, 256);
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    try std.testing.expectEqualStrings("key0000", tbl.smallest.primary);
    try std.testing.expectEqualStrings("key0199", tbl.biggest.primary);
}

// ---------------------------------------------------------------------------
// TestTableBigValues: values larger than the block size
// ---------------------------------------------------------------------------

test "table: big values" {
    const alloc = std.testing.allocator;
    const n = 20;
    const value_size = 64 * 1024; // 64 KB

    var b = Builder.init(alloc, .{ .block_bytes = 4096 });
    defer b.deinit();

    const big_val = try alloc.alloc(u8, value_size);
    defer alloc.free(big_val);

    var i: usize = 0;
    while (i < n) : (i += 1) {
        var buf: [16]u8 = undefined;
        const key_str = try std.fmt.bufPrint(&buf, "{d:0>4}", .{i});
        @memset(big_val, @intCast(i % 256));
        try b.add(
            .{ .primary = key_str },
            @as(u64, @intCast(i + 1)),
            big_val,
        );
    }

    var tb = try b.finish();
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    try std.testing.expectEqual(@as(u32, n), tbl.index.key_count);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
    defer it.deinit();
    try it.seekRange(Key.inf, Key.inf);

    var count: usize = 0;
    while (it.itemRef()) |entry| : (count += 1) {
        try std.testing.expectEqual(value_size, entry.newest().value.len);
        // Verify fill byte matches the key index.
        const fill = entry.newest().value[0];
        try std.testing.expectEqual(@as(u8, @intCast(count % 256)), fill);
        try it.next();
    }
    try std.testing.expectEqual(n, count);
}

// ---------------------------------------------------------------------------
// endBefore tests
// ---------------------------------------------------------------------------

test "seekRange: end truncates iteration" {
    const alloc = std.testing.allocator;
    // 100 keys: k0000..k0099
    var tb = try buildTestTable(alloc, "k", 100, 4096);
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
    defer it.deinit();

    // Stop before k0050.
    try it.seekRange(Key.inf, .{ .primary = "k0050" });

    var count: usize = 0;
    while (it.itemRef()) |_| : (count += 1) {
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 50), count);
}

test "seekRange: end after all keys returns everything" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "k", 100, 4096);
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
    defer it.deinit();

    try it.seekRange(Key.inf, .{ .primary = "z" });

    var count: usize = 0;
    while (it.itemRef()) |_| : (count += 1) {
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 100), count);
}

test "seekRange: end before all keys returns nothing" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "k", 100, 4096);
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
    defer it.deinit();

    try it.seekRange(Key.inf, .{ .primary = "a" });

    try std.testing.expect(it.itemRef() == null);
}

test "seekRange: no end restores full iteration" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "k", 100, 4096);
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
    defer it.deinit();

    // Set a bound, then clear it with a new seekRange.
    try it.seekRange(Key.inf, .{ .primary = "k0010" });
    try it.seekRange(Key.inf, Key.inf);

    var count: usize = 0;
    while (it.itemRef()) |_| : (count += 1) {
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 100), count);
}

test "seekRange: start + end combined" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "k", 100, 4096);
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
    defer it.deinit();

    // Iterate only k0020..k0029.
    try it.seekRange(.{ .primary = "k0020" }, .{ .primary = "k0030" });

    var count: usize = 0;
    while (it.itemRef()) |entry| : (count += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "k{d:0>4}", .{count + 20});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 10), count);
}

// ---------------------------------------------------------------------------
// seekRange across multiple blocks and multiple tables (ConcatIterator)
// ---------------------------------------------------------------------------

test "seekRange: multiple blocks within a single table" {
    const alloc = std.testing.allocator;
    // 200 keys with tiny block size → many blocks.
    var tb = try buildTestTable(alloc, "k", 200, 64);
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    // Sanity: we have more than one block.
    try std.testing.expect(tbl.blockCount() > 1);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
    defer it.deinit();

    // Range: k0050..k0150 (exclusive), spanning multiple blocks.
    try it.seekRange(.{ .primary = "k0050" }, .{ .primary = "k0150" });

    var count: usize = 0;
    while (it.itemRef()) |entry| : (count += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "k{d:0>4}", .{count + 50});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, 100), count);
}

test "seekRange: ConcatIterator across multiple tables" {
    const alloc = std.testing.allocator;
    const merge_iterator = zdb.table.merge_iterator;

    // Build 3 non-overlapping tables with small blocks:
    //   t0: k0000..k0099
    //   t1: k0100..k0199
    //   t2: k0200..k0299
    var tb0 = try buildTestTable(alloc, "k", 100, 64);
    defer tb0.deinit();
    var tb1 = try buildRangeTable(alloc, "k", 100, 200, 64);
    defer tb1.deinit();
    var tb2 = try buildRangeTable(alloc, "k", 200, 300, 64);
    defer tb2.deinit();

    const tbl0 = try openBuffer(alloc, &tb0);
    defer destroyTable(tbl0);
    const tbl1 = try openBuffer(alloc, &tb1);
    defer destroyTable(tbl1);
    const tbl2 = try openBuffer(alloc, &tb2);
    defer destroyTable(tbl2);

    var table_iters = [_]TableIterator{
        TableIterator.init(alloc, tbl0, zero_table_key, .forward),
        TableIterator.init(alloc, tbl1, zero_table_key, .forward),
        TableIterator.init(alloc, tbl2, zero_table_key, .forward),
    };

    var concat = merge_iterator.ConcatIterator.init(&table_iters, .forward);
    defer concat.deinit();

    // Range spanning all 3 tables: k0050..k0250 (exclusive).
    try concat.seekRange(.{ .primary = "k0050" }, .{ .primary = "k0250" });

    var count: usize = 0;
    while (concat.itemRef()) |entry| : (count += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "k{d:0>4}", .{count + 50});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try concat.next();
    }
    try std.testing.expectEqual(@as(usize, 200), count);

    // Range within a single middle table: k0120..k0180.
    try concat.seekRange(.{ .primary = "k0120" }, .{ .primary = "k0180" });

    count = 0;
    while (concat.itemRef()) |entry| : (count += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "k{d:0>4}", .{count + 120});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try concat.next();
    }
    try std.testing.expectEqual(@as(usize, 60), count);

    // Range past all tables.
    try concat.seekRange(.{ .primary = "z" }, Key.inf);
    try std.testing.expect(concat.itemRef() == null);

    // Range before all tables with end inside first table.
    try concat.seekRange(Key.inf, .{ .primary = "k0010" });
    count = 0;
    while (concat.itemRef()) |_| : (count += 1) {
        try concat.next();
    }
    try std.testing.expectEqual(@as(usize, 10), count);

    // Regression: empty start AND empty end must walk every key across
    // every sub-iter. The previous implementation only seeked iters[0]
    // up-front; once `next()` exhausted iters[0] and advanced to
    // iters[1], `itemRef()` returned null (never seeked) and the
    // ConcatIterator declared itself done after just the first table.
    try concat.seekRange(Key.inf, Key.inf);
    count = 0;
    while (concat.itemRef()) |entry| : (count += 1) {
        var buf: [32]u8 = undefined;
        const expected = try std.fmt.bufPrint(&buf, "k{d:0>4}", .{count});
        try std.testing.expectEqualStrings(expected, entry.key.primary);
        try concat.next();
    }
    try std.testing.expectEqual(@as(usize, 300), count);
}

// ---------------------------------------------------------------------------
// Meta byte: survives the SST entry encoding
// ---------------------------------------------------------------------------

/// Distinct-but-deterministic meta for entry i, mixing both nibbles so a
/// mix-up between kind and encoding shows up as a mismatch.
fn metaFor(i: usize) zdb.kv.Meta {
    return .{
        .kind = switch (i % 3) {
            0 => .deleted,
            1 => .delta,
            else => .state,
        },
        .encoding = if ((i % 2) == 1) .roaring else .raw,
    };
}

test "table: meta round-trips through builder and iterator" {
    const alloc = std.testing.allocator;
    // Small blocks so the table spans many blocks: meta has to survive each
    // block re-anchoring on a new base key.
    var b = Builder.init(alloc, .{ .block_bytes = 4096 });
    defer b.deinit();

    const n = 500;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key_str = try std.fmt.bufPrint(&key_buf, "k:{d:0>4}", .{i});
        var val_buf: [20]u8 = undefined;
        // Every third entry has an EMPTY value with a non-zero meta. This is
        // the case that would break if meta were a value prefix instead of a
        // header field: the value region's length is implicit, so an empty
        // value must stay a zero-byte region.
        const val = if (i % 3 == 0)
            ""
        else
            try std.fmt.bufPrint(&val_buf, "v{d}", .{i});

        // addItem copies during the call, so a temporary slice is fine.
        try b.addItem(.{
            .key = .{ .primary = key_str },
            .versions = .{ .one = .{
                .ts = 1,
                .meta = metaFor(i),
                .value = val,
            } },
        });
    }

    var tb = try b.finish();
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
    defer it.deinit();
    try it.seekRange(Key.inf, Key.inf);

    var count: usize = 0;
    while (it.itemRef()) |item| {
        const ver = item.newest();
        try std.testing.expectEqual(metaFor(count), ver.meta);
        if (count % 3 == 0) {
            try std.testing.expectEqual(@as(usize, 0), ver.value.len);
        }
        count += 1;
        try it.next();
    }
    try std.testing.expectEqual(@as(usize, n), count);
}

test "table: meta survives random access via seekGe" {
    const alloc = std.testing.allocator;
    var b = Builder.init(alloc, .{ .block_bytes = 4096 });
    defer b.deinit();

    const n = 500;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        const key_str = try std.fmt.bufPrint(&key_buf, "k:{d:0>4}", .{i});
        try b.addItem(.{
            .key = .{ .primary = key_str },
            .versions = .{ .one = .{
                .ts = 1,
                .meta = metaFor(i),
                .value = "x",
            } },
        });
    }

    var tb = try b.finish();
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);

    // setIdx is reached by binary search too, not just forward scans, so seek
    // straight into the middle of blocks rather than walking there.
    for ([_]usize{ 0, 1, 7, 128, 199, 256, 499 }) |target| {
        var key_buf: [32]u8 = undefined;
        const key_str = try std.fmt.bufPrint(&key_buf, "k:{d:0>4}", .{target});

        var it = TableIterator.init(alloc, tbl, zero_table_key, .forward);
        defer it.deinit();
        try it.seekRange(.{ .primary = key_str }, Key.inf);

        const item = it.itemRef() orelse return error.TestExpectedItem;
        try std.testing.expectEqualStrings(key_str, item.key.primary);
        try std.testing.expectEqual(metaFor(target), item.newest().meta);
    }
}

test "table: SST written before format_version is rejected" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "foo", 10, 4096);
    defer tb.deinit();

    // Make the index look like one written under an older entry
    // encoding: format_version is field id 1, a u32 in the slot right
    // after the 2-byte header size, so zero that slot and formatVersion()
    // reads 0. Then repair the index checksum, because the whole point is
    // that checksums do NOT catch a stale entry format; only this gate
    // does.
    const bytes = tb.bytes;
    const trailer_at = bytes.len - 12;
    const index_len = std.mem.readInt(u32, bytes[trailer_at..][0..4], .little);
    const index_at = trailer_at - index_len;
    const index_bytes = bytes[index_at..trailer_at];

    const version_slot = index_bytes[2..6];
    try std.testing.expectEqual(
        zdb.table.index.format_version,
        std.mem.readInt(u32, version_slot, .little),
    );
    std.mem.writeInt(u32, version_slot, 0, .little);
    std.mem.writeInt(u64, bytes[trailer_at + 4 ..][0..8], zbuf.hash(index_bytes), .little);

    // The patch is in place in `tb`, so opening it reads the zeroed slot.
    try std.testing.expectError(
        error.UnsupportedTableFormat,
        openBuffer(alloc, &tb),
    );
}

// ---------------------------------------------------------------------------
// Reverse walks
// ---------------------------------------------------------------------------

test "table iterator: reverse scan" {
    const alloc = std.testing.allocator;
    for ([_]usize{ 99, 100, 101 }) |n| {
        var tb = try buildTestTable(alloc, "key", n, 64);
        defer tb.deinit();
        const tbl = try openBuffer(alloc, &tb);
        defer destroyTable(tbl);

        var it = TableIterator.init(alloc, tbl, zero_table_key, .reverse);
        defer it.deinit();

        var count: usize = 0;
        try it.seekRange(Key.inf, Key.inf);
        while (it.itemRef()) |entry| : (count += 1) {
            const i = n - 1 - count;
            var key_buf: [32]u8 = undefined;
            const want_key = try std.fmt.bufPrint(&key_buf, "key{d:0>4}", .{i});
            try std.testing.expectEqualStrings(want_key, entry.key.primary);
            var val_buf: [20]u8 = undefined;
            const want_val = try std.fmt.bufPrint(&val_buf, "{d}", .{i});
            try std.testing.expectEqualStrings(want_val, entry.newest().value);
            try it.next();
        }
        try std.testing.expectEqual(n, count);
    }
}

/// A key of a table built with prefix "k".
fn kAt(primary: String) Key {
    return .{ .primary = primary };
}

/// Walk `it` in reverse over `[start, end)` and check it yields
/// "k{hi-1}" down to "k{lo}"; `lo == hi` means nothing at all.
fn expectReverseRun(
    it: anytype,
    start: Key,
    end: Key,
    lo: usize,
    hi: usize,
) !void {
    try it.seekRange(start, end);
    var i = hi;
    while (it.itemRef()) |entry| {
        if (i == lo) return error.TestUnexpectedItem;
        i -= 1;
        var buf: [32]u8 = undefined;
        const want = try std.fmt.bufPrint(&buf, "k{d:0>4}", .{i});
        try std.testing.expectEqualStrings(want, entry.key.primary);
        try it.next();
    }
    try std.testing.expectEqual(lo, i);
}

test "seekRange reverse: bounds inside, at and past the table" {
    const alloc = std.testing.allocator;
    var tb = try buildTestTable(alloc, "k", 100, 64);
    defer tb.deinit();
    const tbl = try openBuffer(alloc, &tb);
    defer destroyTable(tbl);
    try std.testing.expect(tbl.blockCount() > 3);

    var it = TableIterator.init(alloc, tbl, zero_table_key, .reverse);
    defer it.deinit();

    try expectReverseRun(&it, Key.inf, Key.inf, 0, 100);
    try expectReverseRun(&it, kAt("k0050"), kAt("k0080"), 50, 80);
    try expectReverseRun(&it, Key.inf, kAt("k0030"), 0, 30);
    try expectReverseRun(&it, kAt("k0090"), Key.inf, 90, 100);
    try expectReverseRun(&it, kAt("k0090"), kAt("z"), 90, 100);
    // Bounds that keep nothing: at the first key, past the last, beyond.
    try expectReverseRun(&it, Key.inf, kAt("k0000"), 0, 0);
    try expectReverseRun(&it, kAt("k0100"), Key.inf, 0, 0);
    try expectReverseRun(&it, kAt("k0120"), kAt("k0130"), 0, 0);

    // `end` on a block's base key: the walk starts in the block before;
    // `start` on one: the walk ends at that key.
    const base = try tbl.blockBaseKey(2);
    const base_idx = try std.fmt.parseInt(usize, base[1..], 10);
    try expectReverseRun(&it, Key.inf, kAt(base), 0, base_idx);
    try expectReverseRun(&it, kAt(base), Key.inf, base_idx, 100);

    // Table edges are no bound on their side.
    try expectReverseRun(
        &it,
        Key.tableLeft(zero_table_key),
        Key.tableRight(zero_table_key),
        0,
        100,
    );
}

test "seekRange reverse: ConcatIterator across multiple tables" {
    const alloc = std.testing.allocator;
    const merge_iterator = zdb.table.merge_iterator;

    var tb0 = try buildTestTable(alloc, "k", 100, 64);
    defer tb0.deinit();
    var tb1 = try buildRangeTable(alloc, "k", 100, 200, 64);
    defer tb1.deinit();
    var tb2 = try buildRangeTable(alloc, "k", 200, 300, 64);
    defer tb2.deinit();

    const tbl0 = try openBuffer(alloc, &tb0);
    defer destroyTable(tbl0);
    const tbl1 = try openBuffer(alloc, &tb1);
    defer destroyTable(tbl1);
    const tbl2 = try openBuffer(alloc, &tb2);
    defer destroyTable(tbl2);

    var table_iters = [_]TableIterator{
        TableIterator.init(alloc, tbl0, zero_table_key, .reverse),
        TableIterator.init(alloc, tbl1, zero_table_key, .reverse),
        TableIterator.init(alloc, tbl2, zero_table_key, .reverse),
    };
    var concat = merge_iterator.ConcatIterator.init(&table_iters, .reverse);
    defer concat.deinit();

    try expectReverseRun(&concat, kAt("k0050"), kAt("k0250"), 50, 250);
    try expectReverseRun(&concat, kAt("k0120"), kAt("k0180"), 120, 180);
    try expectReverseRun(&concat, Key.inf, Key.inf, 0, 300);
    try expectReverseRun(&concat, Key.inf, kAt("k0010"), 0, 10);
    try expectReverseRun(&concat, kAt("z"), Key.inf, 0, 0);
    // Bounds on a table boundary: `end` there starts in the table
    // below it, `start` there ends with that table's first key.
    try expectReverseRun(&concat, Key.inf, kAt("k0100"), 0, 100);
    try expectReverseRun(&concat, kAt("k0200"), Key.inf, 200, 300);
}

test "merge iterator reverse merges sorted streams, chains newest first" {
    const alloc = std.testing.allocator;
    var b1 = Builder.init(alloc, .{ .block_bytes = 256 });
    defer b1.deinit();
    try b1.add(.{ .primary = "a" }, 5, "a5");
    try b1.add(.{ .primary = "c" }, 1, "c1");
    var t1 = try b1.finish();
    defer t1.deinit();

    var b2 = Builder.init(alloc, .{ .block_bytes = 256 });
    defer b2.deinit();
    try b2.add(.{ .primary = "b" }, 4, "b4");
    try b2.add(.{ .primary = "c" }, 3, "c3");
    try b2.add(.{ .primary = "d" }, 1, "d1");
    var t2 = try b2.finish();
    defer t2.deinit();

    const r1 = try openBuffer(alloc, &t1);
    defer destroyTable(r1);
    const r2 = try openBuffer(alloc, &t2);
    defer destroyTable(r2);

    var iters = [_]zdb.table.merge_iterator.Iterator{
        .{ .table = TableIterator.init(alloc, r1, zero_table_key, .reverse) },
        .{ .table = TableIterator.init(alloc, r2, zero_table_key, .reverse) },
    };
    const MergeIterator = zdb.table.merge_iterator.MergeIterator;
    var m = try MergeIterator.init(alloc, iters[0..], .reverse);
    defer m.deinit();

    try m.seekRange(Key.inf, Key.inf);
    try std.testing.expectEqualStrings("d1", m.itemRef().?.newest().value);
    try m.next();
    // A key both sources hold: one item, its chain newest first.
    const c = m.itemRef().?;
    try std.testing.expectEqualStrings("c", c.key.primary);
    try std.testing.expectEqual(@as(usize, 2), c.versions.slice().len);
    try std.testing.expectEqualStrings("c3", c.versions.slice()[0].value);
    try std.testing.expectEqualStrings("c1", c.versions.slice()[1].value);
    try m.next();
    try std.testing.expectEqualStrings("b4", m.itemRef().?.newest().value);
    try m.next();
    try std.testing.expectEqualStrings("a5", m.itemRef().?.newest().value);
    try m.next();
    try std.testing.expect(m.itemRef() == null);

    try m.seekRange(.{ .primary = "b" }, .{ .primary = "d" });
    try std.testing.expectEqualStrings("c", m.itemRef().?.key.primary);
    try m.next();
    try std.testing.expectEqualStrings("b", m.itemRef().?.key.primary);
    try m.next();
    try std.testing.expect(m.itemRef() == null);
}
