const std = @import("std");
const fiber = @import("fiber");

/// Spin in a yield loop for `target_ms` of wall time — every yield
/// is a chance for the scheduler (or a peer's `trySteal`) to pick a
/// different fiber, so the fiber stays a stealing target rather
/// than monopolizing its worker uninterrupted.
fn busyYield(target_ms: i64) void {
    const start = fiber.coarseMs();
    while (fiber.coarseMs() - start < target_ms) {
        fiber.yield();
    }
}

test "Steal: pinned (locked) fibers never migrate" {
    // Pin to worker 0 with explicit `locked = true`. The fiber
    // yields many times across ~5 ms; each yield re-enqueues it.
    // Thieves see it via `popHead(.unlocked)`, but it's locked, so
    // the walker skips it. The fiber must finish on worker 0 with
    // times_stolen == 0.
    const Body = struct {
        fn run() void {
            busyYield(5);
        }
    };

    const h = try fiber.spawn(.{ .worker_id = 0, .locked = true }, Body.run, .{});
    h.join();

    try std.testing.expectEqual(@as(u32, 0), h.fiber.times_stolen);
    try std.testing.expectEqual(@as(u8, 0), h.fiber.worker_id);
}

test "Steal: wake-on-push distributes unlocked spawns to idle peers" {
    // Pin all spawns to worker 0 with default (locked = false).
    // `routeToWorker` should look at the lane's idle_mask, see peers
    // parked in loop.run, and redirect each fiber to a distinct idle
    // peer instead of piling them all onto worker 0. We assert via
    // `worker_id` diversity: at least 2 distinct workers participated.
    const rt = fiber.currentRuntime();
    if (rt.workers.len < 2) return error.SkipZigTest;

    // Yield briefly so peer workers reach the idle path and publish
    // their bits before we start spawning. Without this, the very
    // first spawn might race ahead of any peer's publishIdle and
    // fall back to pushing onto worker 0.
    fiber.yield();

    const N = 8;
    const Body = struct {
        fn run() void {
            busyYield(2);
        }
    };

    var handles: [N]fiber.JoinHandle(void) = undefined;
    for (&handles) |*h| {
        h.* = try fiber.spawn(
            .{ .worker_id = 0 },
            Body.run,
            .{},
        );
    }
    for (handles) |h| h.join();

    var seen: [256]bool = @splat(false);
    var distinct: u32 = 0;
    for (handles) |h| {
        const wid = h.fiber.worker_id;
        if (!seen[wid]) {
            seen[wid] = true;
            distinct += 1;
        }
    }
    // Redirect-at-push fans out across idle peers. Demanding >=2 is
    // safe against scheduling noise; we typically see N distinct.
    try std.testing.expect(distinct >= 2);
}
