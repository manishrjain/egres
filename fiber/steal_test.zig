const std = @import("std");
const fiber = @import("fiber");

/// Spin in a yield loop for `target_ms` of wall time. Yielding keeps
/// the fiber on the local queue, which is what gives peers a chance
/// to steal it.
fn busyYield(target_ms: i64) void {
    const start = fiber.coarseMs();
    while (fiber.coarseMs() - start < target_ms) {
        fiber.yield();
    }
}

test "Steal: many short fibers complete on a multi-worker runtime" {
    // Smoke test for the work-stealing path. With LocalQueue + bulk
    // stealBatchTo, spawning a burst of fibers from one worker should
    // end up running them across all available workers via stealing.
    // We just check that all of them finish.
    const rt = fiber.currentRuntime();
    if (rt.workers.len < 2) return error.SkipZigTest;

    const N = 32;
    const Body = struct {
        fn run() void {
            busyYield(2);
        }
    };

    var handles: [N]fiber.JoinHandle(void) = undefined;
    for (&handles) |*h| {
        h.* = try fiber.spawn(.{}, Body.run, .{});
    }
    for (handles) |h| h.join();
}
