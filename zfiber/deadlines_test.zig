// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");

/// Fresh monotonic ms — same source `Deadlines.register` / `onTimer`
/// use. Direct vDSO read; no cache lag under load.
/// A fresh read, not the coarse clock's loop-tick lag. Virtual under
/// the simulation, as the deadlines are.
fn nowMs() i64 {
    return utils.timestampNow(.monotonic, .millis);
}

/// Minimal `on_fire` context: stamps WHEN the timeout fired. `Timeout` is a
/// pure primitive — `on_fire` can invoke anything — so these tests drive it
/// directly by recording the fire time, with NO fiber wake/park coupling. That
/// coupling was a real hazard: a stack `Timeout` whose callback `wake`s a fiber
/// can fire into a SINCE-FREED fiber (the waiter observes the flag, returns, and
/// is reclaimed before the callback's `wake` runs) → a spurious wake elsewhere.
/// Recording a timestamp can't dangle. `at_ms == 0` means "not fired yet".
const Fired = struct {
    at_ms: std.atomic.Value(i64) = .init(0),

    fn cb(ptr: *anyopaque) void {
        const self: *Fired = @ptrCast(@alignCast(ptr));
        self.at_ms.store(nowMs(), .release);
    }
    fn fired(self: *const Fired) bool {
        return self.at_ms.load(.acquire) != 0;
    }
    fn at(self: *const Fired) i64 {
        return self.at_ms.load(.acquire);
    }
};

test "Deadlines: timer fires after the registered duration" {
    const w = fiber.currentWorker();
    const before = nowMs();

    // `Timeout` + ctx live on this fiber's stack. `on_fire` only stamps the fire
    // time — it wakes nobody — so we poll with `sleepMs` (a real deadline park)
    // until it fires. A fired timeout is already out of the heap, so `defer
    // cancel` is a no-op and nothing dangles when the frame unwinds. (Production
    // code uses the fiber-OWNED timeout; this drives the low-level API directly.)
    var f: Fired = .{};
    var t: fiber.Timeout = .{ .on_fire = &Fired.cb, .on_fire_ctx = &f };
    try w.deadlines.register(&t, 20);
    defer w.deadlines.cancel(&t);
    while (!f.fired()) fiber.sleepMs(1);

    const elapsed = f.at() - before;
    std.log.debug("fired after {d} ms (registered for 20 ms)", .{elapsed});
    try std.testing.expect(elapsed >= 18);
    try std.testing.expect(elapsed < 200);
}

test "Deadlines: cancel(t) before fire prevents the timer" {
    const w = fiber.currentWorker();

    var f_canceled: Fired = .{};
    var f_wake: Fired = .{};
    var t_canceled: fiber.Timeout = .{ .on_fire = &Fired.cb, .on_fire_ctx = &f_canceled };
    var t_wake: fiber.Timeout = .{ .on_fire = &Fired.cb, .on_fire_ctx = &f_wake };

    // Register the to-be-canceled one first (earlier — it's the heap head +
    // the in-flight timer's target). Cancel it, then register a later
    // deadline we actually wait on. The canceled one must never fire.
    try w.deadlines.register(&t_canceled, 5);
    w.deadlines.cancel(&t_canceled);
    try w.deadlines.register(&t_wake, 30);
    defer w.deadlines.cancel(&t_wake);
    while (!f_wake.fired()) fiber.sleepMs(1);

    try std.testing.expect(f_wake.fired());
    try std.testing.expect(!f_canceled.fired());
}

test "Deadlines: re-arm to earlier deadline registered after a later one" {
    // Register the 40 ms deadline FIRST, so the timer arms for 40 ms; then
    // the 10 ms `register` must reset it to fire at ~10 ms. If the re-arm
    // didn't happen, both timers would fire together at ~40 ms — the
    // short-deadline wall-time assertion catches that bug.
    const w = fiber.currentWorker();
    const now = nowMs();

    var f_long: Fired = .{};
    var f_short: Fired = .{};
    var t_long: fiber.Timeout = .{ .on_fire = &Fired.cb, .on_fire_ctx = &f_long };
    var t_short: fiber.Timeout = .{ .on_fire = &Fired.cb, .on_fire_ctx = &f_short };

    try w.deadlines.register(&t_long, 40); // arms timer for 40 ms
    try w.deadlines.register(&t_short, 10); // must reset to 10 ms
    // We poll until both fire (and thus leave the heap); defer-cancel guarantees
    // no stack `Timeout` lingers if the test exits early. Each `on_fire` stamps
    // its own fire time — more accurate than a post-wake read, and wakes nobody.
    defer w.deadlines.cancel(&t_long);
    defer w.deadlines.cancel(&t_short);

    while (!f_short.fired() or !f_long.fired()) fiber.sleepMs(1);

    const short_elapsed = f_short.at() - now;
    const long_elapsed = f_long.at() - now;
    std.log.debug("short_elapsed = {d} ms (expected 8 <= ... < 25), long_elapsed = {d} ms (expected 35 <= ... < 250)", .{ short_elapsed, long_elapsed });
    // Short fires near 10 ms — if the reset didn't happen, this would be
    // near 40 ms instead. Tight gap catches the "reset didn't happen" bug;
    // loose ceiling tolerates scheduler load from concurrent fiber tests.
    try std.testing.expect(short_elapsed >= 8);
    try std.testing.expect(short_elapsed < 25);
    // Long fires near 40 ms — re-armed by onTimer's tail after the short
    // one drained.
    try std.testing.expect(long_elapsed >= 35);
    try std.testing.expect(long_elapsed < 250);
}

test "Deadlines: many ever-earlier deadlines before a loop tick all fire on time" {
    // Register N deadlines, each SHORTER than the last, back-to-back with no
    // park in between — so no loop tick separates them. Every one is a new
    // head, so `armForHead` runs a fresh pooled node for each. The growable
    // pool guarantees a node is always available, so the SHORTEST is not
    // stretched behind a longer armed timer (the hole a fixed node count has).
    const w = fiber.currentWorker();
    const now = nowMs();

    const N = 20;
    var fired: [N]Fired = undefined;
    for (&fired) |*f| f.* = .{};
    var t: [N]fiber.Timeout = undefined;
    // Deadlines 60,58,...,22 ms — strictly decreasing, so each becomes head.
    for (0..N) |i| {
        t[i] = .{ .on_fire = &Fired.cb, .on_fire_ctx = &fired[i] };
        try w.deadlines.register(&t[i], @intCast(60 - i * 2));
    }
    defer for (0..N) |i| w.deadlines.cancel(&t[i]);

    // Poll until the last-registered (shortest, ~22 ms) has fired.
    while (!fired[N - 1].fired()) fiber.sleepMs(1);

    // The shortest deadline (22 ms) fires near its own time — NOT stretched to
    // the longest (60 ms). A generous ceiling tolerates scheduler load.
    const shortest = fired[N - 1].at() - now;
    std.log.debug("shortest of {d} fired at {d} ms (deadline 22)", .{ N, shortest });
    try std.testing.expect(shortest >= 20);
    try std.testing.expect(shortest < 50);
}

// ── sleepMs (the fiber-facing consumer of Deadlines) ────────────────

test "sleepMs: 100 fibers sleeping 10ms in parallel finish under 50ms" {
    // Atomic: the unlocked default + wake-on-push redirect spreads
    // these fibers across many workers; 100 simultaneous wakes from
    // the timer barrage race the counter increment otherwise.
    var counter: std.atomic.Value(u32) = .init(0);

    const Body = struct {
        fn run(c: *std.atomic.Value(u32)) void {
            fiber.sleepMs(10);
            _ = c.fetchAdd(1, .acq_rel);
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(100);

    const start = utils.coarseMs();
    for (0..100) |_| try g.spawn(.{}, Body.run, .{&counter});
    g.wait();
    const elapsed = utils.coarseMs() - start;

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

test "sleepMs: single fiber sleeps for the requested duration" {
    const Body = struct {
        fn run() void {
            fiber.sleepMs(20);
        }
    };

    const start = utils.coarseMs();
    var h = try fiber.spawn(.{}, Body.run, .{});
    h.join();
    const elapsed = utils.coarseMs() - start;

    if (elapsed < 19 or elapsed >= 200) {
        std.log.err("elapsed = {d} ms (expected 19 <= elapsed < 200)", .{elapsed});
    }
    try std.testing.expect(elapsed >= 19);
    try std.testing.expect(elapsed < 200);
}
