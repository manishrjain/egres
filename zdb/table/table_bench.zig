// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Micro-benchmarks for the SST read path.
//!
//! Four angles, each isolating one cost so an optimization's effect is
//! attributable:
//!
//!   merge scan  — MergeIterator arbitration across 8 interleaved tables.
//!                 Keys are striped so the winner changes every step, the
//!                 worst case for the heap.
//!   point seeks — TableIterator.seekRange to a random key. Includes the
//!                 per-seek readBlock (openBuffer tables have file_id 0,
//!                 the block-cache skip sentinel), so it moves less than
//!                 `block seekGe` does.
//!   block seekGe— the in-block binary search alone: one cached block,
//!                 repeated seeks to keys inside it. The purest signal
//!                 for the key-compare path.
//!   full scan   — sequential setIdx/next over every entry of one table.
//!   key walks    — keys only, never a value byte, at two value sizes:
//!                 what version skipping and compaction's discard
//!                 decisions do.
//!
//! Run with `zig build bench` (ReleaseFast).

const std = @import("std");
const utils = @import("utils");
const fiber = @import("zfiber");
const String = utils.String;
const zdb = @import("zdb");
const openBuffer = @import("../testing.zig").openBuffer;
const bench = fiber.bench;
const Bench = bench.Bench;
const Key = zdb.kv.Key;
/// The ids stamped on keys: an SST stores primaries only, so a bench
/// reading a table on its own can use any table key.
const zero_table_key: zdb.kv.TableKey = .{ .namespace = 0, .table_id = 0 };
const vfs = zdb.vfs;

const Builder = zdb.table.builder.Builder;
const Table = zdb.table.Table;
const TableIterator = zdb.table.TableIterator;
const BlockIterator = zdb.table.BlockIterator;
const merge = zdb.table.merge_iterator;

// ── Helpers (mirrors of the table_test.zig ones; test files aren't
// importable from each other) ────────────────────────────────────────────

fn destroyTable(t: *Table) void {
    t.decrRef(); // the bench's ref is the last one; this frees the table
}

fn keyAt(buf: []u8, i: usize) !String {
    return std.fmt.bufPrint(buf, "k{d:0>7}", .{i});
}

/// Build a table over the arithmetic progression `stripe, stripe+stride,
/// …` of length `n`. `stride` tables built with distinct stripes hold
/// disjoint, perfectly interleaved key streams.
fn buildStriped(
    alloc: std.mem.Allocator,
    stripe: usize,
    stride: usize,
    n: usize,
    block_size: u32,
    val_len: usize,
) !zdb.table.builder.TableBuffer {
    var b = Builder.init(alloc, .{ .block_bytes = block_size });
    defer b.deinit();

    const val = try alloc.alloc(u8, val_len);
    defer alloc.free(val);
    @memset(val, 'v');

    var i: usize = 0;
    while (i < n) : (i += 1) {
        var kb: [16]u8 = undefined;
        const p = try keyAt(&kb, stripe + i * stride);
        try b.add(.{ .primary = p }, 1, val);
    }
    return b.finish();
}

/// Build + open one striped table. Caller destroys via destroyTable.
fn openStriped(
    alloc: std.mem.Allocator,
    stripe: usize,
    stride: usize,
    n: usize,
    block_size: u32,
    val_len: usize,
) !*Table {
    var tb = try buildStriped(alloc, stripe, stride, n, block_size, val_len);
    defer tb.deinit();
    return openBuffer(alloc, &tb);
}

const value_len: usize = 16;
const num_tables: usize = 8;
const keys_per_table: usize = 8 * 1024;
const single_keys: usize = 64 * 1024;

/// Not `b.alloc` (page_allocator): readBlock allocates per block, and an
/// mmap/munmap pair per 4 KiB block buries the iterator work we're here
/// to measure. smp_allocator is what production-shaped allocation costs.
const gpa = std.heap.smp_allocator;

// ── Benchmarks ───────────────────────────────────────────────────────────

fn benchMergeScan(b: *Bench) !void {
    const a = gpa;
    var tables: [num_tables]*Table = undefined;
    var opened: usize = 0;
    defer for (tables[0..opened]) |t| destroyTable(t);
    while (opened < num_tables) : (opened += 1) {
        tables[opened] = try openStriped(
            a,
            opened,
            num_tables,
            keys_per_table,
            4096,
            value_len,
        );
    }

    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        var iters: [num_tables]merge.Iterator = undefined;
        for (tables, 0..) |t, j| {
            iters[j] = .{
                .table = TableIterator.init(a, t, zero_table_key, .forward),
            };
        }
        var m = try merge.MergeIterator.init(a, iters[0..], .forward);
        defer m.deinit();

        try m.seekRange(Key.inf, Key.inf);
        var count: usize = 0;
        while (m.itemRef()) |item| : (count += 1) {
            b.addBytes(item.key.primary.len + item.newest().value.len);
            try m.next();
        }
        if (count != num_tables * keys_per_table) return error.ShortScan;
    }
}

/// Build a table holding EVERY key `0..n` with `nv` versions each, the
/// newest at `ts_top` and each next one lower. Tables with disjoint
/// timestamp bands give the merge a key that every source holds.
fn buildShared(
    alloc: std.mem.Allocator,
    n: usize,
    nv: usize,
    ts_top: usize,
    block_size: u32,
    val_len: usize,
) !zdb.table.builder.TableBuffer {
    var b = Builder.init(alloc, .{ .block_bytes = block_size });
    defer b.deinit();

    const val = try alloc.alloc(u8, val_len);
    defer alloc.free(val);
    @memset(val, 'v');

    var vers: [8]zdb.kv.Version = undefined;
    for (vers[0..nv], 0..) |*v, k| v.* = .{ .ts = ts_top - k, .meta = .{}, .value = val };

    var i: usize = 0;
    while (i < n) : (i += 1) {
        var kb: [16]u8 = undefined;
        const p = try keyAt(&kb, i);
        try b.addItem(.{ .key = .{ .primary = p }, .versions = .{ .many = vers[0..nv] } });
    }
    return b.finish();
}

const shared_keys: usize = 16 * 1024;
const shared_sources: usize = 3;
const shared_vers: usize = 2;

/// The version path of the merge: every key sits in all three sources
/// with two versions each, so each step appends six versions, sorts
/// them by timestamp and dedups — what `merge scan 8 tables`, with its
/// disjoint keys, never touches.
fn benchMergeScanShared(b: *Bench) !void {
    const a = gpa;
    var tables: [shared_sources]*Table = undefined;
    var opened: usize = 0;
    defer for (tables[0..opened]) |t| destroyTable(t);
    while (opened < shared_sources) : (opened += 1) {
        var tb = try buildShared(a, shared_keys, shared_vers, 100 - 10 * opened, 4096, value_len);
        defer tb.deinit();
        tables[opened] = try openBuffer(a, &tb);
    }

    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        var iters: [shared_sources]merge.Iterator = undefined;
        for (tables, 0..) |t, j| {
            iters[j] = .{
                .table = TableIterator.init(a, t, zero_table_key, .forward),
            };
        }
        var m = try merge.MergeIterator.init(a, iters[0..], .forward);
        defer m.deinit();

        try m.seekRange(Key.inf, Key.inf);
        var count: usize = 0;
        while (m.itemRef()) |item| : (count += 1) {
            if (item.versions.slice().len != shared_sources * shared_vers) return error.LostVersion;
            b.addBytes(item.key.primary.len + item.newest().value.len);
            try m.next();
        }
        if (count != shared_keys) return error.ShortScan;
    }
}

fn benchPointSeeks(b: *Bench) !void {
    const a = gpa;
    const t = try openStriped(a, 0, 1, single_keys, 4096, value_len);
    defer destroyTable(t);

    var it = TableIterator.init(a, t, zero_table_key, .forward);
    defer it.deinit();

    b.resetTimer();
    var i: u64 = 0;
    var j: usize = 0;
    var hits: u64 = 0;
    while (i < b.n) : (i += 1) {
        // Coprime stride: visits every key, defeats block locality.
        j = (j + 40503) % single_keys;
        var kb: [16]u8 = undefined;
        const p = try keyAt(&kb, j);
        try it.seekRange(.{ .primary = p }, Key.inf);
        if (it.itemRef() != null) hits += 1;
    }
    std.mem.doNotOptimizeAway(hits);
}

fn benchBlockSeekGe(b: *Bench) !void {
    const a = gpa;
    // 16 KiB blocks hold ~300 entries here, so the binary search runs
    // ~8-9 probes — deep enough for the per-probe cost to dominate.
    const t = try openStriped(a, 0, 1, single_keys, 16 * 1024, value_len);
    defer destroyTable(t);

    const bpos = t.blockCount() / 2;
    const block = try t.readBlock(bpos);
    defer block.decrRef();
    const base_raw = try t.blockBaseKey(bpos);

    var bi = try BlockIterator.init(a, block.data, base_raw, zero_table_key);
    defer bi.deinit();

    // Spread targets over the block: entry 0's key is the base key (a
    // primary, "k" then digits), and keys here are dense, so index =
    // first + offset.
    const first = try std.fmt.parseInt(usize, base_raw[1..], 10);
    const num_targets: usize = 64;
    const step = @max(1, bi.entryCount() / num_targets);

    // seekGe takes a primary; pre-build the targets so the loop
    // measures the search alone.
    var targets: [num_targets]String = undefined;
    for (&targets, 0..) |*raw, k| {
        var kb: [16]u8 = undefined;
        raw.* = try a.dupe(u8, try keyAt(&kb, first + k * step));
    }
    defer for (targets) |raw| a.free(raw);

    b.resetTimer();
    var i: u64 = 0;
    var hits: u64 = 0;
    while (i < b.n) : (i += 1) {
        try bi.seekGe(targets[i % num_targets]);
        if (bi.itemRef() != null) hits += 1;
    }
    std.mem.doNotOptimizeAway(hits);
}

/// Walk the keys of ONE already-resident block, never touching a value.
/// Keys and values are interleaved, so the stride per key grows with the
/// value size — this pair of numbers is what a keys/values split would
/// have to beat. It was tried and reverted (see builder.zig): the split
/// cost 4 bytes per entry and won nothing, because loading a block
/// already streams every byte through cache (decompress + checksum +
/// copy), leaving values resident whatever the layout.
fn benchBlockKeyWalk(b: *Bench, val_len: usize) !void {
    const a = gpa;
    const t = try openStriped(a, 0, 1, single_keys, 64 * 1024, val_len);
    defer destroyTable(t);

    const bpos = t.blockCount() / 2;
    const block = try t.readBlock(bpos);
    defer block.decrRef();
    const base_raw = try t.blockBaseKey(bpos);

    var bi = try BlockIterator.init(a, block.data, base_raw, zero_table_key);
    defer bi.deinit();
    const n = bi.entryCount();

    b.resetTimer();
    var i: u64 = 0;
    var bytes: u64 = 0;
    while (i < b.n) : (i += 1) {
        try bi.seekToFirst();
        var count: usize = 0;
        while (bi.itemRef()) |item| : (count += 1) {
            bytes += item.key.primary.len;
            try bi.next();
        }
        if (count != n) return error.ShortScan;
    }
    // Per-key cost is what matters here; report keys walked, not bytes.
    std.mem.doNotOptimizeAway(bytes);
}

/// Walk every entry of ONE resident block, materializing each key and
/// its versions, the way a block-level decode does.
fn benchBlockEntryWalk(b: *Bench, val_len: usize) !void {
    const a = gpa;
    const t = try openStriped(a, 0, 1, single_keys, 64 * 1024, val_len);
    defer destroyTable(t);

    const bpos = t.blockCount() / 2;
    const block = try t.readBlock(bpos);
    defer block.decrRef();
    const base_raw = try t.blockBaseKey(bpos);

    var bi = try BlockIterator.init(a, block.data, base_raw, zero_table_key);
    defer bi.deinit();
    const n = bi.entryCount();

    b.resetTimer();
    var i: u64 = 0;
    var bytes: u64 = 0;
    while (i < b.n) : (i += 1) {
        try bi.seekToFirst();
        var count: usize = 0;
        while (bi.itemRef()) |item| : (count += 1) {
            bytes += item.key.primary.len + item.newest().value.len;
            try bi.next();
        }
        if (count != n) return error.ShortScan;
    }
    std.mem.doNotOptimizeAway(bytes);
}

/// Versions per key drawn from a Zipf law: P(k) proportional to 1/k^s
/// over 1..max, so most keys hold one version and a few hold many, the
/// shape of a table with hot rows or posting keys before compaction
/// folds them. At s = 2 about 61% of keys hold one version, 15% two,
/// and the mean is just under three.
const zipf_max_versions: usize = 64;
const zipf_s: f64 = 2.0;

/// The cumulative weights of the law, for inverse sampling.
fn zipfCdf(alloc: std.mem.Allocator) ![]f64 {
    const cdf = try alloc.alloc(f64, zipf_max_versions);
    var total: f64 = 0;
    for (cdf, 1..) |*c, k| {
        total += 1.0 / std.math.pow(f64, @floatFromInt(k), zipf_s);
        c.* = total;
    }
    for (cdf) |*c| c.* /= total;
    return cdf;
}

fn zipfDraw(cdf: []const f64, prng: *std.Random.DefaultPrng) usize {
    const u = prng.random().float(f64);
    for (cdf, 1..) |c, k| {
        if (u < c) return k;
    }
    return cdf.len;
}

/// A table of `n` keys whose version counts follow the Zipf law, every
/// version `val_len` bytes, timestamps descending from the count.
fn buildZipfVersions(
    alloc: std.mem.Allocator,
    n: usize,
    block_size: u32,
    val_len: usize,
) !zdb.table.builder.TableBuffer {
    var b = Builder.init(alloc, .{ .block_bytes = block_size });
    defer b.deinit();

    const val = try alloc.alloc(u8, val_len);
    defer alloc.free(val);
    @memset(val, 'v');
    const cdf = try zipfCdf(alloc);
    defer alloc.free(cdf);
    var prng = std.Random.DefaultPrng.init(5);
    var vers: [zipf_max_versions]zdb.kv.Version = undefined;

    var i: usize = 0;
    while (i < n) : (i += 1) {
        var kb: [16]u8 = undefined;
        const p = try keyAt(&kb, i);
        const count = zipfDraw(cdf, &prng);
        for (vers[0..count], 0..) |*v, j| {
            v.* = .{ .ts = count - j, .meta = .{}, .value = val };
        }
        try b.addItem(.{
            .key = .{ .primary = p },
            .versions = .{ .many = vers[0..count] },
        });
    }
    return b.finish();
}

/// `benchBlockEntryWalk` over a block whose keys hold Zipf-distributed
/// version counts: every version of every entry is decoded, so this is
/// the multi-version decode cost the version layout decides.
fn benchBlockEntryWalkZipf(b: *Bench, val_len: usize) !void {
    const a = gpa;
    var tb = try buildZipfVersions(a, single_keys, 64 * 1024, val_len);
    defer tb.deinit();
    const t = try openBuffer(a, &tb);
    defer destroyTable(t);

    const bpos = t.blockCount() / 2;
    const block = try t.readBlock(bpos);
    defer block.decrRef();
    const base_raw = try t.blockBaseKey(bpos);

    var bi = try BlockIterator.init(a, block.data, base_raw, zero_table_key);
    defer bi.deinit();

    b.resetTimer();
    var i: u64 = 0;
    var bytes: u64 = 0;
    var versions: u64 = 0;
    while (i < b.n) : (i += 1) {
        try bi.seekToFirst();
        var count: usize = 0;
        while (bi.itemRef()) |item| : (count += 1) {
            const vs = item.versions.slice();
            versions += vs.len;
            bytes += item.key.primary.len + vs[vs.len - 1].value.len;
            try bi.next();
        }
        if (count == 0) return error.ShortScan;
    }
    std.mem.doNotOptimizeAway(bytes);
    std.mem.doNotOptimizeAway(versions);
}

/// Full-table key-only scan. Dominated by per-block load cost, so it
/// mostly measures blocks-per-scan — which is why it grows with value
/// size even though it reads no value bytes.
fn benchKeyScan(b: *Bench, val_len: usize) !void {
    const a = gpa;
    const t = try openStriped(a, 0, 1, single_keys, 64 * 1024, val_len);
    defer destroyTable(t);

    var it = TableIterator.init(a, t, zero_table_key, .forward);
    defer it.deinit();

    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        try it.seekRange(Key.inf, Key.inf);
        var count: usize = 0;
        while (it.itemRef()) |item| : (count += 1) {
            b.addBytes(item.key.primary.len);
            try it.next();
        }
        if (count != single_keys) return error.ShortScan;
    }
}

fn benchFullScan(b: *Bench) !void {
    const a = gpa;
    const t = try openStriped(a, 0, 1, single_keys, 4096, value_len);
    defer destroyTable(t);

    var it = TableIterator.init(a, t, zero_table_key, .forward);
    defer it.deinit();

    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        try it.seekRange(Key.inf, Key.inf);
        var count: usize = 0;
        while (it.itemRef()) |item| : (count += 1) {
            b.addBytes(item.key.primary.len + item.newest().value.len);
            try it.next();
        }
        if (count != single_keys) return error.ShortScan;
    }
}

test "BENCH: merge scan 8 tables" {
    try benchMergeScan(bench.current());
}
test "BENCH: merge scan 3 sources sharing every key" {
    try benchMergeScanShared(bench.current());
}
test "BENCH: table point seeks" {
    try benchPointSeeks(bench.current());
}
test "BENCH: block seekGe" {
    try benchBlockSeekGe(bench.current());
}
test "BENCH: table full scan" {
    try benchFullScan(bench.current());
}
test "BENCH: key-only scan 16B values" {
    try benchKeyScan(bench.current(), 16);
}
test "BENCH: key-only scan 256B values" {
    try benchKeyScan(bench.current(), 256);
}
test "BENCH: in-block key walk 16B values" {
    try benchBlockKeyWalk(bench.current(), 16);
}
test "BENCH: in-block entry walk 16B values" {
    try benchBlockEntryWalk(bench.current(), 16);
}
test "BENCH: in-block entry walk 256B values" {
    try benchBlockEntryWalk(bench.current(), 256);
}
test "BENCH: in-block key walk 256B values" {
    try benchBlockKeyWalk(bench.current(), 256);
}
test "BENCH: in-block entry walk, zipf versions, 16B values" {
    try benchBlockEntryWalkZipf(bench.current(), 16);
}
test "BENCH: in-block entry walk, zipf versions, 256B values" {
    try benchBlockEntryWalkZipf(bench.current(), 256);
}
