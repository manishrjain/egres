// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const Fiber = fiber.Fiber;
const Queue = fiber.LocalQueue(.withlock);

// Dummy fibers for the data-structure tests: `push`/`pop`/`stealInto` only
// ever touch a fiber's `qstate` (via assertEnQ/assertDQ) and its pointer
// value — never the rest of the struct — so a zeroed block suffices (qstate
// starts at 0 == "not queued"). No runtime needed: these exercise the ring
// directly, not the scheduler.
fn makeFibers(alloc: std.mem.Allocator, n: usize) ![]Fiber {
    const fs = try alloc.alloc(Fiber, n);
    @memset(std.mem.sliceAsBytes(fs), 0);
    return fs;
}

test "LocalQueue: grows past initial capacity, preserves FIFO order" {
    const alloc = std.testing.allocator;
    var q = try Queue.init(alloc, 2);
    defer q.deinit();
    const fs = try makeFibers(alloc, 10);
    defer alloc.free(fs);

    for (0..10) |i| try q.push(&fs[i]);
    try std.testing.expect(q.capacity() >= 10);
    try std.testing.expectEqual(@as(usize, 10), q.len());
    for (0..10) |i| try std.testing.expectEqual(&fs[i], q.pop().?);
    try std.testing.expect(q.pop() == null);
}

test "LocalQueue: grow with a wrapped live range keeps FIFO" {
    const alloc = std.testing.allocator;
    var q = try Queue.init(alloc, 4);
    defer q.deinit();
    const fs = try makeFibers(alloc, 8);
    defer alloc.free(fs);

    // Advance `head` so the live range wraps before a grow compacts it.
    for (0..3) |i| try q.push(&fs[i]); // [f0 f1 f2 _]
    try std.testing.expectEqual(&fs[0], q.pop().?); // head -> 1
    try std.testing.expectEqual(&fs[1], q.pop().?); // head -> 2, f2 @ slot 2
    // Push 5 more: indices wrap past the end, then size 4 == cap forces a grow
    // whose compaction must unwrap the live range in order.
    for (3..8) |i| try q.push(&fs[i]);
    try std.testing.expectEqual(@as(usize, 6), q.len());
    for (2..8) |i| try std.testing.expectEqual(&fs[i], q.pop().?);
    try std.testing.expect(q.pop() == null);
}

test "LocalQueue: popAt takes the chosen slot and keeps the rest" {
    const alloc = std.testing.allocator;
    var q = try Queue.init(alloc, 4);
    defer q.deinit();
    const fs = try makeFibers(alloc, 6);
    defer alloc.free(fs);

    // Wrap the live range first so the slot arithmetic is exercised.
    for (0..3) |i| try q.push(&fs[i]);
    _ = q.pop().?;
    _ = q.pop().?;
    for (3..6) |i| try q.push(&fs[i]); // live: f2 f3 f4 f5, wrapped

    try std.testing.expect(q.popAt(4) == null); // past the end
    // The front moves into the vacated slot: f2 f3 f4 f5 -> f3 f2 f5.
    try std.testing.expectEqual(&fs[4], q.popAt(2).?);
    try std.testing.expectEqual(@as(usize, 3), q.len());
    try std.testing.expectEqual(&fs[3], q.popAt(0).?); // the front itself
    try std.testing.expectEqual(&fs[5], q.popAt(1).?); // the last: f2 f5
    try std.testing.expectEqual(&fs[2], q.pop().?); // what remains
    try std.testing.expect(q.pop() == null);
    try std.testing.expect(q.popAt(0) == null);
}

test "LocalQueue: stealInto (empty thief) takes ceil(half) from the front" {
    const alloc = std.testing.allocator;
    var q = try Queue.init(alloc, 4);
    defer q.deinit();
    const fs = try makeFibers(alloc, 10);
    defer alloc.free(fs);

    for (0..10) |i| try q.push(&fs[i]); // grew to 16
    var out: [10]*Fiber = undefined;
    const n = q.stealInto(0, out[0..]); // dest_sz 0: half the (size - 0) difference
    try std.testing.expectEqual(@as(usize, 5), n); // ceil(10/2)
    for (0..5) |i| try std.testing.expectEqual(&fs[i], out[i]); // front half
    for (5..10) |i| try std.testing.expectEqual(&fs[i], q.pop().?); // rest, in order
    try std.testing.expect(q.pop() == null);
}

test "LocalQueue: stealInto balances against a non-empty thief" {
    const alloc = std.testing.allocator;
    var q = try Queue.init(alloc, 4);
    defer q.deinit();
    const fs = try makeFibers(alloc, 10);
    defer alloc.free(fs);

    for (0..10) |i| try q.push(&fs[i]); // victim size 10
    var out: [10]*Fiber = undefined;
    // Thief already holds 4: take ceil((10 - 4)/2) = 3, leaving both at 7.
    try std.testing.expectEqual(@as(usize, 3), q.stealInto(4, out[0..]));
    try std.testing.expectEqual(@as(usize, 7), q.len());
    // Thief now holds 7 == victim: nothing left to balance.
    try std.testing.expectEqual(@as(usize, 0), q.stealInto(7, out[0..]));
    // Thief heavier than victim: also nothing.
    try std.testing.expectEqual(@as(usize, 0), q.stealInto(9, out[0..]));
    // A lone fiber is still stealable by an empty thief (ceil keeps it > 0).
    while (q.len() > 1) _ = q.pop();
    try std.testing.expectEqual(@as(usize, 1), q.stealInto(0, out[0..]));
}

test "LocalQueue: concurrent push(grow) + steal lose nothing, duplicate nothing" {
    const alloc = std.testing.allocator;
    const N = 5_000;
    var q = try Queue.init(alloc, 2); // start tiny so the owner grows ~12 times
    defer q.deinit();
    const fs = try makeFibers(alloc, N);
    defer alloc.free(fs);

    const seen = try alloc.alloc(std.atomic.Value(u8), N);
    defer alloc.free(seen);
    for (seen) |*s| s.* = .init(0);

    const Thief = struct {
        fn run(qq: *Queue, base: [*]Fiber, sn: []std.atomic.Value(u8), done: *std.atomic.Value(bool)) void {
            var buf: [32]*Fiber = undefined;
            while (!done.load(.acquire) or qq.len() != 0) {
                const n = qq.stealInto(0, buf[0..]); // empty thief: drains half each call
                for (buf[0..n]) |f| {
                    const idx = (@intFromPtr(f) - @intFromPtr(base)) / @sizeOf(Fiber);
                    _ = sn[idx].fetchAdd(1, .acq_rel);
                }
            }
        }
    };

    var done = std.atomic.Value(bool).init(false);
    const t = try std.Thread.spawn(.{}, Thief.run, .{ &q, fs.ptr, seen, &done });
    for (0..N) |i| try q.push(&fs[i]); // owner pushes + grows while the thief steals
    done.store(true, .release);
    t.join();
    // Drain whatever the thief left (it should be empty, but be defensive).
    while (q.pop()) |f| {
        const idx = (@intFromPtr(f) - @intFromPtr(fs.ptr)) / @sizeOf(Fiber);
        _ = seen[idx].fetchAdd(1, .monotonic);
    }
    // Each fiber must leave the ring exactly once. (A double-steal would already
    // have tripped assertDQ's "dequeued while not queued" panic; this also
    // catches any loss.)
    for (seen) |*s| try std.testing.expectEqual(@as(u8, 1), s.load(.monotonic));
}

test "IdleWorkers: the pick rotates across words, not just within one" {
    // Workers 5 (word 0) and 70 (word 1) parked. The cursor's own word
    // is scanned first (wrapping within it), then the words after it:
    // a cursor in word 1 picks 70 before 5, a cursor in word 0 picks 5
    // before 70. Each claim clears the bit it returns. A 128-worker lane,
    // so the cursor can point into word 1.
    var iw = fiber.IdleWorkers{ .len = 128 };
    iw.markIdle(5);
    iw.markIdle(70);
    iw.cursor.store(71, .monotonic);
    try std.testing.expectEqual(@as(?u8, 70), iw.anyOffset());
    try std.testing.expectEqual(@as(?u8, 70), iw.claim());
    try std.testing.expectEqual(@as(?u8, 5), iw.claim());
    try std.testing.expectEqual(@as(?u8, null), iw.claim());

    iw.markIdle(5);
    iw.markIdle(70);
    iw.cursor.store(10, .monotonic);
    try std.testing.expectEqual(@as(?u8, 5), iw.claim());
    try std.testing.expectEqual(@as(?u8, 70), iw.claim());
}

test "IdleWorkers: claims spread over a 32-worker lane instead of piling on the lowest id" {
    // All 32 workers of a lane are parked the whole time (the bit is
    // re-set after every claim). A fair pick spreads the claims; a pick
    // that walks from bit 0 hands nearly every claim to worker 0, which
    // is what a per-worker wake-path dump shows as W00–W03 doing all the
    // stealing while W12+ sit parked.
    const lane_len: u8 = 32;
    var idle: fiber.IdleWorkers = .{ .len = lane_len };
    var picks: [lane_len]u32 = @splat(0);
    for (0..lane_len) |i| idle.markIdle(@intCast(i));
    const rounds: u32 = 100 * @as(u32, lane_len);
    for (0..rounds) |_| {
        const off = idle.claim().?;
        picks[off] += 1;
        idle.markIdle(off);
    }
    var max_share: u32 = 0;
    for (picks) |n| max_share = @max(max_share, n);
    // Fair is 100 each; allow slack for the wrap of a rotating cursor.
    std.debug.print("idle pick histogram over {d} claims: W00={d} W01={d} W31={d} max={d}\n", .{
        rounds, picks[0], picks[1], picks[31], max_share,
    });
    try std.testing.expect(max_share <= 2 * rounds / lane_len);
}
