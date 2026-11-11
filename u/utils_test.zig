// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const utils = @import("utils");

const ChunkBuffer = utils.ChunkBuffer;

// Fill a ChunkBuffer and an ArrayList with the same appends, and
// check the copy out matches: the chunk boundaries are the only
// thing the buffer can get wrong, so small chunks make many of them.
test "ChunkBuffer appends across chunk boundaries" {
    const alloc = testing.allocator;
    var cb = ChunkBuffer.init(alloc, 16);
    defer cb.deinit();
    var want: std.ArrayList(u8) = .empty;
    defer want.deinit(alloc);

    var prng = std.Random.DefaultPrng.init(7);
    const rand = prng.random();
    var i: usize = 0;
    while (i < 200) : (i += 1) {
        // Pieces shorter than, equal to and longer than a chunk.
        const n = rand.intRangeAtMost(usize, 0, 40);
        var piece: [40]u8 = undefined;
        rand.bytes(piece[0..n]);
        try cb.appendSlice(piece[0..n]);
        try want.appendSlice(alloc, piece[0..n]);
        if (i % 7 == 0) {
            const pad = rand.intRangeAtMost(usize, 0, 20);
            try cb.appendNTimes(0, pad);
            try want.appendNTimes(alloc, 0, pad);
        }
        try testing.expectEqual(want.items.len, cb.len);
    }

    const got = try alloc.alloc(u8, cb.len);
    defer alloc.free(got);
    cb.copyTo(got);
    try testing.expectEqualSlices(u8, want.items, got);
    // Every chunk but the last is full; the last holds the remainder.
    try testing.expectEqual((cb.len + 15) / 16, cb.chunks.items.len);
}

test "ChunkBuffer reset keeps chunks and refills from the start" {
    const alloc = testing.allocator;
    var cb = ChunkBuffer.init(alloc, 8);
    defer cb.deinit();

    try cb.appendSlice("0123456789abcdef0123");
    const chunks_before = cb.chunks.items.len;
    try testing.expectEqual(@as(usize, 3), chunks_before);

    cb.reset();
    try testing.expectEqual(@as(usize, 0), cb.len);
    var empty: [0]u8 = .{};
    cb.copyTo(&empty);

    try cb.appendSlice("hello, chunks");
    try testing.expectEqual(chunks_before, cb.chunks.items.len);
    var got: [13]u8 = undefined;
    cb.copyTo(&got);
    try testing.expectEqualStrings("hello, chunks", &got);
}

test "logEvery: each call site has its own interval" {
    const due = utils.logDue;
    // A site's first call logs, whatever the clock reads.
    try testing.expect(due("site a {d}", 30_000, 0));
    try testing.expect(!due("site a {d}", 30_000, 30_000));
    try testing.expect(due("site a {d}", 30_000, 30_001));
    // Another site logging in between does not hold this one back,
    // nor is it held back by it.
    try testing.expect(due("site b {s}", 1_000, 30_002));
    try testing.expect(due("site b {s}", 1_000, 31_003));
    try testing.expect(!due("site a {d}", 30_000, 59_000));
    try testing.expect(due("site a {d}", 30_000, 60_002));
}
