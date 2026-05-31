const std = @import("std");
const fiber = @import("fiber");

/// Fresh monotonic ms — same source `Deadlines.register` / `onTimer`
/// use. Direct vDSO read; no cache lag under load.
fn nowMs() i64 {
    return fiber.clockMsUseRarely(.monotonic);
}

test "Deadlines: timer fires after the registered duration" {
    const Ctx = struct {
        fired: bool = false,
        waiter: ?*fiber.Fiber = null,
        fire_at_ms: i64 = 0,
    };
    var ctx: Ctx = .{};

    const Cb = struct {
        fn onFire(opaque_ctx: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(opaque_ctx));
            c.fired = true;
            c.fire_at_ms = nowMs();
            if (c.waiter) |f| fiber.wake(f);
        }
    };

    const me = fiber.currentFiber();
    ctx.waiter = me;

    const w = fiber.currentWorker();
    const before = nowMs();

    // Timeout lives on this fiber's stack; valid until the test
    // returns. We park before that and only resume after the callback
    // wakes us, so the heap never sees a dangling pointer.
    var t: fiber.Timeout = .{
        .deadline_ms = undefined,
        .fiber = me,
        .callback = &Cb.onFire,
        .callback_ctx = &ctx,
    };
    try w.deadlines.register(fiber.runtime().allocator, &t, 20);
    fiber.park();

    const elapsed = ctx.fire_at_ms - before;
    std.log.debug("elapsed = {d} ms, fired = {} (registered for 20 ms)", .{ elapsed, ctx.fired });
    try std.testing.expect(ctx.fired);
    try std.testing.expect(elapsed >= 18);
    try std.testing.expect(elapsed < 200);
}

test "Deadlines: cancel(t) before fire prevents the callback" {
    const Ctx = struct {
        fired_canceled: bool = false,
        fired_wake: bool = false,
        waiter: ?*fiber.Fiber = null,
    };
    var ctx: Ctx = .{};

    const Cb = struct {
        fn onCanceled(opaque_ctx: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(opaque_ctx));
            c.fired_canceled = true;
        }
        fn onWake(opaque_ctx: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(opaque_ctx));
            c.fired_wake = true;
            if (c.waiter) |f| fiber.wake(f);
        }
    };

    const me = fiber.currentFiber();
    ctx.waiter = me;

    const w = fiber.currentWorker();

    var t_canceled: fiber.Timeout = .{
        .deadline_ms = undefined,
        .fiber = me,
        .callback = &Cb.onCanceled,
        .callback_ctx = &ctx,
    };
    var t_wake: fiber.Timeout = .{
        .deadline_ms = undefined,
        .fiber = me,
        .callback = &Cb.onWake,
        .callback_ctx = &ctx,
    };

    // Register the to-be-canceled one first (earlier — would be the
    // heap head + the in-flight timer's target if not removed). Cancel
    // it, then register a later deadline we actually want to wait on.
    try w.deadlines.register(fiber.runtime().allocator, &t_canceled, 5);
    w.deadlines.cancel(&t_canceled);
    try w.deadlines.register(fiber.runtime().allocator, &t_wake, 30);

    fiber.park();

    try std.testing.expect(ctx.fired_wake);
    try std.testing.expect(!ctx.fired_canceled);
}

test "Deadlines: re-arm to earlier deadline registered after a later one" {
    // Two callbacks: one for the 10 ms deadline, one for the 40 ms
    // deadline. We register the 40 ms one FIRST, so the timer starts
    // armed for 40 ms; then the 10 ms `register` must reset it to fire
    // at ~10 ms. If the re-arm didn't happen, both callbacks would
    // fire together when the 40 ms timer expires — the wall-time
    // assertion catches that bug.
    const Ctx = struct {
        short_fire_at: i64 = 0,
        long_fire_at: i64 = 0,
        fired_count: u32 = 0,
        waiter: ?*fiber.Fiber = null,
    };
    var ctx: Ctx = .{};

    const Cb = struct {
        fn onShort(opaque_ctx: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(opaque_ctx));
            c.short_fire_at = nowMs();
            c.fired_count += 1;
            if (c.fired_count == 2) if (c.waiter) |f| fiber.wake(f);
        }
        fn onLong(opaque_ctx: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(opaque_ctx));
            c.long_fire_at = nowMs();
            c.fired_count += 1;
            if (c.fired_count == 2) if (c.waiter) |f| fiber.wake(f);
        }
    };

    const me = fiber.currentFiber();
    ctx.waiter = me;

    const w = fiber.currentWorker();
    const now = nowMs();

    var t_long: fiber.Timeout = .{
        .deadline_ms = undefined,
        .fiber = me,
        .callback = &Cb.onLong,
        .callback_ctx = &ctx,
    };
    var t_short: fiber.Timeout = .{
        .deadline_ms = undefined,
        .fiber = me,
        .callback = &Cb.onShort,
        .callback_ctx = &ctx,
    };

    // 40 ms first — arms timer for 40 ms.
    try w.deadlines.register(fiber.runtime().allocator, &t_long, 40);
    // 10 ms second — must reset timer to 10 ms.
    try w.deadlines.register(fiber.runtime().allocator, &t_short, 10);

    // Only the second callback (fired_count == 2) calls wake, so
    // park resumes after both have run.
    fiber.park();

    const short_elapsed = ctx.short_fire_at - now;
    const long_elapsed = ctx.long_fire_at - now;
    std.log.debug("short_elapsed = {d} ms (expected 8 <= ... < 25), long_elapsed = {d} ms (expected 35 <= ... < 250), fired_count = {d}", .{ short_elapsed, long_elapsed, ctx.fired_count });
    // Short fires near 10 ms — if the reset didn't happen, this would
    // be near 40 ms instead.  Tight gap (15 ms) is what catches the
    // "reset didn't happen" bug; loose ceiling tolerates scheduler
    // load from concurrent fiber tests.
    try std.testing.expect(short_elapsed >= 8);
    try std.testing.expect(short_elapsed < 25);
    // Long fires near 40 ms — re-armed by onTimer's tail after the
    // short one drained.
    try std.testing.expect(long_elapsed >= 35);
    try std.testing.expect(long_elapsed < 250);
    try std.testing.expectEqual(@as(u32, 2), ctx.fired_count);
}
