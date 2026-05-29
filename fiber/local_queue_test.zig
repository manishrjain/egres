const std = @import("std");
const fiber = @import("fiber");

const LocalQueue = fiber.LocalQueue;
const Fiber = fiber.Fiber;

// LocalQueue treats `*Fiber` opaquely — never dereferences it. Tests
// fabricate distinguishable pointers via @ptrFromInt so we avoid
// initializing real Fibers (which heap-allocate + mmap stacks).
fn fakeFiber(id: usize) *Fiber {
    return @ptrFromInt(0x1000 + id * @sizeOf(usize));
}

fn idOf(f: *Fiber) usize {
    return (@intFromPtr(f) - 0x1000) / @sizeOf(usize);
}

fn expectPop(q: *LocalQueue, expected_id: usize) !void {
    const f = q.pop() orelse return error.UnexpectedEmpty;
    try std.testing.expectEqual(expected_id, idOf(f));
}

fn expectStealOne(q: *LocalQueue, expected_id: usize) !void {
    const f = q.stealOne() orelse return error.UnexpectedEmpty;
    try std.testing.expectEqual(expected_id, idOf(f));
}

test "LocalQueue: push/pop is LIFO on a single thread" {
    var q: LocalQueue = try .init(std.testing.allocator, 4);
    defer q.deinit(std.testing.allocator);

    try q.push(fakeFiber(10));
    try q.push(fakeFiber(20));
    try q.push(fakeFiber(30));

    try std.testing.expectEqual(@as(usize, 3), q.len());
    try expectPop(&q, 30);
    try expectPop(&q, 20);
    try expectPop(&q, 10);
    try std.testing.expect(q.pop() == null);
    try std.testing.expectEqual(@as(usize, 0), q.len());
}

test "LocalQueue: stealOne is FIFO relative to push order" {
    var q: LocalQueue = try .init(std.testing.allocator, 4);
    defer q.deinit(std.testing.allocator);

    try q.push(fakeFiber(1));
    try q.push(fakeFiber(2));
    try q.push(fakeFiber(3));

    try expectStealOne(&q, 1);
    try expectStealOne(&q, 2);
    try expectStealOne(&q, 3);
    try std.testing.expect(q.stealOne() == null);
}

test "LocalQueue: empty pop and steal return null" {
    var q: LocalQueue = try .init(std.testing.allocator, 2);
    defer q.deinit(std.testing.allocator);

    try std.testing.expect(q.pop() == null);
    try std.testing.expect(q.stealOne() == null);

    try q.push(fakeFiber(42));
    try expectPop(&q, 42);
    try std.testing.expect(q.pop() == null);
    try std.testing.expect(q.stealOne() == null);
}

test "LocalQueue: push returns error.Full at capacity" {
    var q: LocalQueue = try .init(std.testing.allocator, 4);
    defer q.deinit(std.testing.allocator);

    try q.push(fakeFiber(1));
    try q.push(fakeFiber(2));
    try q.push(fakeFiber(3));
    try q.push(fakeFiber(4));
    try std.testing.expectEqual(@as(usize, 4), q.len());

    try std.testing.expectError(error.Full, q.push(fakeFiber(5)));

    try expectPop(&q, 4);
    try q.push(fakeFiber(99));
    try expectPop(&q, 99);
}

test "LocalQueue: ring wraparound — push, drain, push reuses slots" {
    var q: LocalQueue = try .init(std.testing.allocator, 4);
    defer q.deinit(std.testing.allocator);

    var i: usize = 0;
    while (i < 4) : (i += 1) try q.push(fakeFiber(i));
    try std.testing.expectError(error.Full, q.push(fakeFiber(99)));

    while (i < 8) : (i += 1) try expectStealOne(&q, i - 4);

    while (i < 12) : (i += 1) try q.push(fakeFiber(i));
    try std.testing.expectError(error.Full, q.push(fakeFiber(99)));

    var expected: usize = 12;
    while (expected > 8) {
        expected -= 1;
        try expectPop(&q, expected);
    }
    try std.testing.expect(q.pop() == null);
}

test "LocalQueue: stealBatchTo claims half the victim's items" {
    var victim: LocalQueue = try .init(std.testing.allocator, 16);
    defer victim.deinit(std.testing.allocator);
    var thief: LocalQueue = try .init(std.testing.allocator, 16);
    defer thief.deinit(std.testing.allocator);

    // 10 items in victim → thief should claim ceil(10/2) = 5.
    var i: usize = 0;
    while (i < 10) : (i += 1) try victim.push(fakeFiber(i));

    const result = victim.stealBatchTo(&thief);
    try std.testing.expectEqual(@as(usize, 5), result.count);
    try std.testing.expectEqual(@as(usize, 5), thief.len());
    try std.testing.expectEqual(@as(usize, 5), victim.len());

    // Thief should hold the OLDEST half (0..4), in original order.
    // Stealer's tail-pop is LIFO over what was just appended, so
    // popping returns 4, 3, 2, 1, 0.
    var expected: usize = 5;
    while (expected > 0) {
        expected -= 1;
        try expectPop(&thief, expected);
    }
    // Victim retains the NEWEST half (5..9). LIFO pop → 9, 8, …, 5.
    expected = 10;
    while (expected > 5) {
        expected -= 1;
        try expectPop(&victim, expected);
    }
}

test "LocalQueue: stealBatchTo of size=1 claims 1 (no truncation to 0)" {
    var victim: LocalQueue = try .init(std.testing.allocator, 4);
    defer victim.deinit(std.testing.allocator);
    var thief: LocalQueue = try .init(std.testing.allocator, 4);
    defer thief.deinit(std.testing.allocator);

    try victim.push(fakeFiber(42));
    const r = victim.stealBatchTo(&thief);
    try std.testing.expectEqual(@as(usize, 1), r.count);
    try expectPop(&thief, 42);
}

test "LocalQueue: stealBatchTo on empty victim returns .empty" {
    var victim: LocalQueue = try .init(std.testing.allocator, 4);
    defer victim.deinit(std.testing.allocator);
    var thief: LocalQueue = try .init(std.testing.allocator, 4);
    defer thief.deinit(std.testing.allocator);

    try std.testing.expectEqual(LocalQueue.Steal.empty, victim.stealBatchTo(&thief));
    try std.testing.expectEqual(@as(usize, 0), thief.len());
}

test "LocalQueue: stealBatchTo into full destination returns .dest_full" {
    var victim: LocalQueue = try .init(std.testing.allocator, 4);
    defer victim.deinit(std.testing.allocator);
    var thief: LocalQueue = try .init(std.testing.allocator, 4);
    defer thief.deinit(std.testing.allocator);

    try victim.push(fakeFiber(1));
    try victim.push(fakeFiber(2));

    // Fill thief to its capacity.
    try thief.push(fakeFiber(100));
    try thief.push(fakeFiber(101));
    try thief.push(fakeFiber(102));
    try thief.push(fakeFiber(103));

    try std.testing.expectEqual(LocalQueue.Steal.dest_full, victim.stealBatchTo(&thief));
    try std.testing.expectEqual(@as(usize, 2), victim.len());
}

test "LocalQueue: stealBatchTo clamps to dest's remaining room" {
    var victim: LocalQueue = try .init(std.testing.allocator, 16);
    defer victim.deinit(std.testing.allocator);
    var thief: LocalQueue = try .init(std.testing.allocator, 4);
    defer thief.deinit(std.testing.allocator);

    // Victim has 10 items; half would be 5. But thief only has room
    // for 3 (capacity 4, already holds 1).
    try thief.push(fakeFiber(900));
    var i: usize = 0;
    while (i < 10) : (i += 1) try victim.push(fakeFiber(i));

    const r = victim.stealBatchTo(&thief);
    try std.testing.expectEqual(@as(usize, 3), r.count);
    try std.testing.expectEqual(@as(usize, 4), thief.len());
    try std.testing.expectEqual(@as(usize, 7), victim.len());
}

// ─────────────────────────────────────────────────────────────────────
// Concurrent: 4 thieves repeatedly batch-steal from the victim into
// their own per-thief LocalQueues, then drain to a count vector. The
// owner pushes a total of 50k items (spinning on Full because the
// victim has capacity 256).
// ─────────────────────────────────────────────────────────────────────

const ThiefCtx = struct {
    victim: *LocalQueue,
    own: LocalQueue,
    stolen: std.ArrayList(usize) = .empty,
    stop: *std.atomic.Value(bool),
    allocator: std.mem.Allocator,
};

fn thiefRun(ctx: *ThiefCtx) void {
    while (!ctx.stop.load(.acquire)) {
        switch (ctx.victim.stealBatchTo(&ctx.own)) {
            .count, .empty, .dest_full, .contended => {},
        }
        // Drain whatever we have so room frees up for the next steal.
        while (ctx.own.pop()) |f| ctx.stolen.append(ctx.allocator, idOf(f)) catch unreachable;
    }
    // Final drain after `stop`.
    while (true) {
        switch (ctx.victim.stealBatchTo(&ctx.own)) {
            .count => {},
            .empty, .dest_full, .contended => break,
        }
    }
    while (ctx.own.pop()) |f| ctx.stolen.append(ctx.allocator, idOf(f)) catch unreachable;
}

fn pushBlocking(q: *LocalQueue, f: *Fiber) void {
    while (true) {
        q.push(f) catch |err| switch (err) {
            error.Full => {
                std.atomic.spinLoopHint();
                continue;
            },
        };
        return;
    }
}

test "LocalQueue: 4 thieves batch-steal into own queues — every fiber accounted for" {
    const allocator = std.testing.allocator;
    var victim: LocalQueue = try .init(allocator, 256);
    defer victim.deinit(allocator);

    const total: usize = 50_000;
    var stop: std.atomic.Value(bool) = .init(false);

    var ctxs: [4]ThiefCtx = undefined;
    var threads: [4]std.Thread = undefined;
    for (&ctxs, &threads) |*c, *t| {
        c.* = .{
            .victim = &victim,
            .own = try .init(allocator, 64),
            .stop = &stop,
            .allocator = allocator,
        };
        t.* = try std.Thread.spawn(.{}, thiefRun, .{c});
    }
    defer for (&ctxs) |*c| c.own.deinit(allocator);

    var i: usize = 0;
    while (i < total) : (i += 1) {
        pushBlocking(&victim, fakeFiber(i));
    }

    stop.store(true, .release);
    for (threads) |t| t.join();

    var seen = std.AutoHashMap(usize, void).init(allocator);
    defer seen.deinit();
    for (&ctxs) |*c| {
        for (c.stolen.items) |id| try seen.put(id, {});
        c.stolen.deinit(allocator);
    }
    // Anything left in the victim (e.g. odd tail after thieves stopped).
    while (victim.pop()) |f| try seen.put(idOf(f), {});

    try std.testing.expectEqual(total, seen.count());
    var j: usize = 0;
    while (j < total) : (j += 1) {
        try std.testing.expect(seen.contains(j));
    }
}
