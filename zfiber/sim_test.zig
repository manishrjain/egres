// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const coarse = @import("utils").coarse_clock;
const Fiber = fiber.Fiber;
const Sim = fiber.sim.Sim;

// Zeroed fibers: the rings and the pick only touch `qstate` and the
// pointer value (see queue_test.zig).
fn makeFibers(alloc: std.mem.Allocator, n: usize) ![]Fiber {
    const fs = try alloc.alloc(Fiber, n);
    @memset(std.mem.sliceAsBytes(fs), 0);
    for (fs, 0..) |*f, i| f.seq = i;
    return fs;
}

/// A standalone worker (its own xev loop, no runtime) with `n` fibers
/// pushed, the even ones high priority.
fn loadWorker(
    w: *fiber.Worker,
    alloc: std.mem.Allocator,
    fs: []Fiber,
) !void {
    try fiber.Worker.init(w, 0, alloc, null, 0);
    for (fs, 0..) |*f, i| {
        const ring = &w.ready[
            if (i % 2 == 0)
                @intFromEnum(fiber.Priority.high)
            else
                @intFromEnum(fiber.Priority.normal)
        ];
        try ring.push(f);
    }
}

fn pickAll(sim: *Sim, w: *fiber.Worker, out: []u64) usize {
    var n: usize = 0;
    while (sim.pick(w)) |f| : (n += 1) out[n] = f.seq;
    return n;
}

test "Sim: the pick replays from the seed and differs across seeds" {
    const alloc = std.testing.allocator;
    const fs = try makeFibers(alloc, 12);
    defer alloc.free(fs);
    var orders: [3][12]u64 = undefined;
    const seeds = [_]u64{ 7, 7, 8 };
    for (seeds, 0..) |seed, k| {
        var w: fiber.Worker = undefined;
        var sim = try Sim.init(alloc, .{ .seed = seed });
        defer sim.deinit();
        try loadWorker(&w, alloc, fs);
        defer w.deinit();
        const n = pickAll(&sim, &w, &orders[k]);
        try std.testing.expectEqual(@as(usize, 12), n);
    }
    try std.testing.expectEqualSlices(u64, &orders[0], &orders[1]);
    try std.testing.expect(!std.mem.eql(u64, &orders[0], &orders[2]));
    // Random: not production's order (the even seqs first would be).
    var fifo_like = true;
    for (orders[0][0..6]) |s| fifo_like = fifo_like and s % 2 == 0;
    try std.testing.expect(!fifo_like);
}

test "Sim: priority and fifo picks keep high before normal" {
    const alloc = std.testing.allocator;
    const fs = try makeFibers(alloc, 8);
    defer alloc.free(fs);
    var order: [8]u64 = undefined;
    for ([_]fiber.sim.Sched{ .priority_random, .priority_fifo }) |sched| {
        var w: fiber.Worker = undefined;
        var sim = try Sim.init(alloc, .{ .seed = 3, .sched = sched });
        defer sim.deinit();
        try loadWorker(&w, alloc, fs);
        defer w.deinit();
        try std.testing.expectEqual(@as(usize, 8), pickAll(&sim, &w, &order));
        for (order[0..4]) |s| try std.testing.expect(s % 2 == 0);
        for (order[4..]) |s| try std.testing.expect(s % 2 == 1);
        if (sched == .priority_fifo) {
            const fifo = [_]u64{ 0, 2, 4, 6, 1, 3, 5, 7 };
            try std.testing.expectEqualSlices(u64, &fifo, &order);
        }
    }
}

test "Sim: the trace hashes dispatch order, not addresses" {
    const alloc = std.testing.allocator;
    const fs = try makeFibers(alloc, 4);
    defer alloc.free(fs);
    var a = try Sim.init(alloc, .{ .seed = 1 });
    defer a.deinit();
    var b = try Sim.init(alloc, .{ .seed = 1 });
    defer b.deinit();
    for (fs) |*f| a.record(f);
    for (fs) |*f| b.record(f);
    try std.testing.expectEqual(a.traceHash(), b.traceHash());
    b.advance(b.now() + 1);
    b.record(&fs[0]);
    a.record(&fs[0]);
    try std.testing.expect(a.traceHash() != b.traceHash());
}

test "Sim: sleepMs jumps virtual time, and timestampNow reads it" {
    // Only meaningful under `zig build test -- --sim-seed N`. That the
    // sleep took no real time is what scripts/sim_check.sh's wall clock
    // shows; no real clock is readable from here, which is the point.
    const sim = fiber.runtime().simState() orelse return error.SkipZigTest;
    const virt0 = coarse.ms();
    fiber.sleepMs(10 * 1000);
    const virt1 = coarse.ms();
    try std.testing.expect(virt1 - virt0 >= 10 * 1000);
    try std.testing.expectEqual(sim.now(), virt1);
    try std.testing.expectEqual(
        sim.now(),
        coarse.timestampNow(.realtime, .millis),
    );
    try std.testing.expect(sim.outcome == .running);
}
