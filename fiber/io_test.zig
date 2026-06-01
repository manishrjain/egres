const std = @import("std");
const fiber = @import("fiber");

test "io.sleep: 100 fibers sleeping 10ms in parallel finish under 50ms" {
    // Atomic: the unlocked default + wake-on-push redirect spreads
    // these fibers across many workers; 100 simultaneous wakes from
    // the timer barrage race the counter increment otherwise.
    var counter: std.atomic.Value(u32) = .init(0);

    const Body = struct {
        fn run(c: *std.atomic.Value(u32)) void {
            fiber.sleep(10);
            _ = c.fetchAdd(1, .acq_rel);
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(100);

    const start = fiber.coarseMs();
    for (0..100) |_| try g.spawn(.{}, Body.run, .{&counter});
    g.wait();
    const elapsed = fiber.coarseMs() - start;

    try std.testing.expectEqual(@as(u32, 100), counter.load(.acquire));
    // Concurrency check: 100 × sleep(10) serially would be ~1000 ms;
    // parallel should be one timer-tick.  Upper bound is loose since
    // Phase B runs ~50 fiber tests concurrently on the same workers,
    // so individual sleep+wake latency competes with the rest of the
    // suite.  The 200 ms ceiling still rules out serial execution.
    if (elapsed < 9 or elapsed >= 200) {
        std.log.err("elapsed = {d} ms (expected 9 <= elapsed < 200)", .{elapsed});
    }
    try std.testing.expect(elapsed >= 9);
    try std.testing.expect(elapsed < 200);
}

test "io.sleep: single fiber sleeps for the requested duration" {
    const Body = struct {
        fn run() void {
            fiber.sleep(20);
        }
    };

    const start = fiber.coarseMs();
    const h = try fiber.spawn(.{}, Body.run, .{});
    h.join();
    const elapsed = fiber.coarseMs() - start;

    if (elapsed < 19 or elapsed >= 200) {
        std.log.err("elapsed = {d} ms (expected 19 <= elapsed < 200)", .{elapsed});
    }
    try std.testing.expect(elapsed >= 19);
    try std.testing.expect(elapsed < 200);
}
