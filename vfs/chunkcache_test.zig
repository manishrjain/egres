// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const utils = @import("utils");
const vfs = @import("vfs");
const ChunkCache = vfs.ChunkCache;
const Chunk = vfs.Chunk;
const String = utils.String;
const page = vfs.page_size;

const chunk: u64 = 128 << 10;

/// A cache of `slots` chunks: the file header page, one page of slot
/// records (128 fit), and the slots.
fn config(path: String, slots: u64) ChunkCache.Config {
    return .{
        .path = path,
        .capacity_bytes = page * (1 + slots) + slots * chunk,
        .chunk_bytes = chunk,
    };
}

/// `len` bytes filled from `seed`, page-aligned as the fetch paths
/// allocate them, padded to whole pages.
fn fill(
    alloc: std.mem.Allocator,
    len: usize,
    seed: u64,
) ![]align(page) u8 {
    const full = std.mem.alignForward(usize, len, page);
    const buf = try alloc.alignedAlloc(u8, vfs.page_alignment, full);
    var prng = std.Random.DefaultPrng.init(seed);
    prng.fill(buf[0..len]);
    @memset(buf[len..], 0);
    return buf;
}

fn expectRead(
    c: *ChunkCache,
    name: String,
    idx: u64,
    off: u64,
    want: String,
) !void {
    const got = try testing.allocator.alloc(u8, want.len);
    defer testing.allocator.free(got);
    try testing.expect(try c.read(name, &span(idx, off, got)));
    try testing.expectEqualSlices(u8, want, got);
}

fn expectMiss(c: *ChunkCache, name: String, idx: u64, off: u64) !void {
    var buf: [16]u8 = undefined;
    try testing.expect(!try c.read(name, &span(idx, off, &buf)));
}

/// A read of `buf.len` bytes at `off` of chunk `idx`; `read` looks at
/// the span only, so `chunk_len` is just its end.
fn span(idx: u64, off: u64, buf: []u8) Chunk {
    return .{
        .idx = idx,
        .chunk_len = off + buf.len,
        .req_lo = off,
        .req_hi = off + buf.len,
        .req_buf = buf,
    };
}

test "chunkcache: put then read, any offset, tail chunk, miss" {
    const alloc = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try vfs.tmpDirPath(alloc, &tmp);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/slots", .{root});
    defer alloc.free(path);

    var c = try ChunkCache.init(alloc, config(path, 4));
    defer c.deinit();
    try testing.expect(c.enabled());
    try testing.expectEqual(chunk, c.chunkBytes());

    const a = try fill(alloc, chunk, 1);
    defer alloc.free(a);
    try c.write("x.sst", 0, a[0..chunk]);
    try expectRead(&c, "x.sst", 0, 0, a[0..chunk]);
    try expectRead(&c, "x.sst", 0, 1234, a[1234..9876]);
    try expectRead(&c, "x.sst", 0, chunk - 7, a[chunk - 7 .. chunk]);

    // A tail chunk, shorter than the grid: a read past its end misses.
    const tail_len: usize = 100_000;
    const t = try fill(alloc, tail_len, 2);
    defer alloc.free(t);
    try c.write("x.sst", 1, t[0..tail_len]);
    try expectRead(&c, "x.sst", 1, 50, t[50..tail_len]);
    try expectMiss(&c, "x.sst", 1, tail_len - 8);

    // Unknown chunk and object miss.
    try expectMiss(&c, "x.sst", 2, 0);
    try expectMiss(&c, "y.sst", 0, 0);

    const st = c.stats();
    try testing.expectEqual(@as(u64, 2), st.puts);
    try testing.expectEqual(@as(u64, 4), st.hits);
    try testing.expectEqual(@as(u64, 2), st.used);
    try testing.expectEqual(@as(u64, 0), st.corrupt);
}

test "chunkcache: restart restores slots; a bad page drops its chunk" {
    const alloc = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try vfs.tmpDirPath(alloc, &tmp);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/slots", .{root});
    defer alloc.free(path);

    const a = try fill(alloc, chunk, 11);
    defer alloc.free(a);
    const b = try fill(alloc, chunk, 12);
    defer alloc.free(b);
    const d = try fill(alloc, chunk, 13);
    defer alloc.free(d);
    {
        var c = try ChunkCache.init(alloc, config(path, 4));
        defer c.deinit();
        // S holds one chunk at this size; each put moves the previous
        // one to M, which has room, so all three stay resident.
        try c.write("a.sst", 0, a[0..chunk]);
        try c.write("a.sst", 1, b[0..chunk]);
        try c.write("d.sst", 0, d[0..chunk]);
        c.forget("d.sst");
        try expectMiss(&c, "d.sst", 0, 0);
        // d.sst's chunk was fetched and never read: all 32 pages wasted.
        try testing.expectEqual(@as(u64, 32), c.stats().evicted_pages);
        try testing.expectEqual(@as(u64, 32), c.stats().unread_pages);
        try testing.expectEqual(@as(u64, 1), c.stats().unread_chunks);
    }

    // Corrupt one data page of a.sst's chunk 1 behind the cache's back.
    // Slot ids come off the top of the free list: the puts took slots
    // 3, 2, 1 in order, so that chunk is slot 2; its page 17. Four
    // records fit one header page.
    {
        const fh = try vfs.g().openFile(alloc, path, .{
            .flags = .{ .writable = true },
        });
        defer fh.close();
        const data_off = page * 2 + 2 * chunk;
        var junk: [page]u8 = undefined;
        @memset(&junk, 0xee);
        try fh.write(&junk, data_off + 17 * page);
    }

    // Reopen with no chunk size set: the file's wins.
    var c = try ChunkCache.init(alloc, .{
        .path = path,
        .capacity_bytes = config(path, 4).capacity_bytes,
    });
    defer c.deinit();
    try testing.expectEqual(chunk, c.chunkBytes());
    try testing.expectEqual(@as(u64, 2), c.stats().used);
    try expectRead(&c, "a.sst", 0, 0, a[0..chunk]);
    // The first read of the corrupted chunk hashes all of it, fails,
    // and drops it: every later read misses.
    try expectMiss(&c, "a.sst", 1, 0);
    try expectMiss(&c, "a.sst", 1, 17 * page);
    try testing.expectEqual(@as(u64, 1), c.stats().corrupt);
    try testing.expectEqual(@as(u64, 1), c.stats().used);
    // Forgotten before the restart: not restored.
    try expectMiss(&c, "d.sst", 0, 0);

    // A disagreeing explicit chunk size recreates the file at that
    // size, nothing kept.
    var bad = config(path, 4);
    bad.chunk_bytes = 2 * chunk;
    c.deinit();
    c = try ChunkCache.init(alloc, bad);
    try testing.expectEqual(2 * chunk, c.chunkBytes());
    try testing.expectEqual(@as(u64, 0), c.stats().used);

    // So does another capacity: six slots.
    c.deinit();
    c = try ChunkCache.init(alloc, config(path, 6));
    try testing.expectEqual(@as(u64, 6), c.stats().slots);
    try testing.expectEqual(@as(u64, 0), c.stats().used);
    try expectMiss(&c, "a.sst", 0, 0);
}

test "chunkcache: eviction frees slots for new chunks" {
    const alloc = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try vfs.tmpDirPath(alloc, &tmp);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/slots", .{root});
    defer alloc.free(path);

    var c = try ChunkCache.init(alloc, config(path, 3));
    defer c.deinit();
    const a = try fill(alloc, chunk, 5);
    defer alloc.free(a);
    // Twice the slots: every put must find a slot, none is dropped.
    for (0..6) |i| try c.write("e.sst", i, a[0..chunk]);
    const st = c.stats();
    try testing.expectEqual(@as(u64, 6), st.puts);
    try testing.expectEqual(@as(u64, 0), st.drops);
    try testing.expectEqual(@as(u64, 3), st.used);
    // S holds one, M two: chunks 0 and 1 fill M as S overflows with
    // room to spare; from then on an unread chunk leaving S is gone.
    try expectRead(&c, "e.sst", 5, 0, a[0..chunk]);
    try expectRead(&c, "e.sst", 0, 0, a[0..chunk]);
    try expectMiss(&c, "e.sst", 3, 0);
    // Chunks 2, 3, 4 came and went unread: 3 slots, 96 pages wasted.
    try testing.expectEqual(@as(u64, 96), c.stats().unread_pages);
    try testing.expectEqual(@as(u64, 3), c.stats().unread_chunks);
    // Two more puts: 5 (read) moves to M and pushes out 1 (unread);
    // 6 leaves S unread. 0, read, survives M's pass.
    try c.write("e.sst", 6, a[0..chunk]);
    try c.write("e.sst", 7, a[0..chunk]);
    try testing.expectEqual(@as(u64, 160), c.stats().evicted_pages);
    try testing.expectEqual(@as(u64, 160), c.stats().unread_pages);
    try testing.expectEqual(@as(u64, 5), c.stats().unread_chunks);
    try expectRead(&c, "e.sst", 0, 0, a[0..chunk]);
}

test "chunkcache: disabled without a path; bad sizes refused" {
    const alloc = testing.allocator;
    var off = try ChunkCache.init(alloc, .{});
    defer off.deinit();
    try testing.expect(!off.enabled());
    try testing.expectEqual(ChunkCache.default_chunk_bytes, off.chunkBytes());
    var buf: [16]u8 = undefined;
    try testing.expect(!try off.read("x", &span(0, 0, &buf)));
    try off.write("x", 0, &buf);

    try testing.expectError(error.BadChunkBytes, ChunkCache.init(alloc, .{
        .path = "/nonexistent/x",
        .chunk_bytes = 3 << 20,
    }));
    try testing.expectError(error.BadChunkBytes, ChunkCache.init(alloc, .{
        .path = "/nonexistent/x",
        .chunk_bytes = 4 << 10,
    }));
}

test "chunkcache: restart restores records across header pages" {
    const alloc = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try vfs.tmpDirPath(alloc, &tmp);
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/slots", .{root});
    defer alloc.free(path);

    // 8 KiB chunks, 400 slots: four pages of 128 records.
    const small: u64 = 8 << 10;
    const cfg = ChunkCache.Config{
        .path = path,
        .capacity_bytes = page * 5 + 400 * small,
        .chunk_bytes = small,
    };
    const a = try fill(alloc, small, 21);
    defer alloc.free(a);
    {
        var c = try ChunkCache.init(alloc, cfg);
        defer c.deinit();
        try testing.expectEqual(@as(u64, 400), c.stats().slots);
        // All resident: S holds 40, M promotes up to its reserve, 324.
        for (0..300) |i| try c.write("m.sst", i, a[0..small]);
        try testing.expectEqual(@as(u64, 300), c.stats().used);
    }
    var c = try ChunkCache.init(alloc, cfg);
    defer c.deinit();
    try testing.expectEqual(@as(u64, 300), c.stats().used);
    // Both sides of each page boundary, and the last record.
    for ([_]u64{ 0, 127, 128, 255, 256, 299 }) |i| {
        try expectRead(&c, "m.sst", i, 100, a[100..small]);
    }
    try expectMiss(&c, "m.sst", 300, 0);
}
