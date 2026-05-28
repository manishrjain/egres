const std = @import("std");
const fiber = @import("fiber");

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
            c.fire_at_ms = fiber.coarseMs();
            if (c.waiter) |f| fiber.wake(f);
        }
    };

    const me = fiber.currentFiber();
    ctx.waiter = me;

    const w = fiber.currentWorker();
    const before = fiber.coarseMs();

    // Timeout lives on this fiber's stack; valid until the test
    // returns. We park before that and only resume after the callback
    // wakes us, so the heap never sees a dangling pointer.
    var t: fiber.Timeout = .{
        .deadline_ms = undefined,
        .fiber = me,
        .callback = &Cb.onFire,
        .callback_ctx = &ctx,
    };
    try w.deadlines.register(w.rt.allocator,&t, before, 20);
    fiber.park();

    const elapsed = ctx.fire_at_ms - before;
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
    const now = fiber.coarseMs();

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
    try w.deadlines.register(w.rt.allocator,&t_canceled, now, 5);
    w.deadlines.cancel(&t_canceled);
    try w.deadlines.register(w.rt.allocator,&t_wake, now, 30);

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
            c.short_fire_at = fiber.coarseMs();
            c.fired_count += 1;
            if (c.fired_count == 2) if (c.waiter) |f| fiber.wake(f);
        }
        fn onLong(opaque_ctx: *anyopaque) void {
            const c: *Ctx = @ptrCast(@alignCast(opaque_ctx));
            c.long_fire_at = fiber.coarseMs();
            c.fired_count += 1;
            if (c.fired_count == 2) if (c.waiter) |f| fiber.wake(f);
        }
    };

    const me = fiber.currentFiber();
    ctx.waiter = me;

    const w = fiber.currentWorker();
    const now = fiber.coarseMs();

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
    try w.deadlines.register(w.rt.allocator,&t_long, now, 40);
    // 10 ms second — must reset timer to 10 ms.
    try w.deadlines.register(w.rt.allocator,&t_short, now, 10);

    fiber.park();

    const short_elapsed = ctx.short_fire_at - now;
    const long_elapsed = ctx.long_fire_at - now;
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
