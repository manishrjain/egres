//! Test: can a single zio.coro Coroutine be safely stepped on
//! different OS threads across yields?
//!
//! We allocate a coroutine, then call `step()` from a fresh OS thread
//! each time. The body verifies that (a) stack contents and locals
//! derived from the iteration counter survive each yield, and (b) the
//! thread IDs observed across resumes are distinct (proof that we
//! actually migrated threads). If zio.coro doesn't support this, we
//! expect a crash (SEGV, signal handler confusion, stack corruption).

const std = @import("std");
const zio = @import("zio");
const coro = zio.coro;

const N_YIELDS: u32 = 5;

const State = struct {
    iteration: u32 = 0,
    local_witness_after_resume: u32 = 0,
    expected_witness: u32 = 0,
    stack_check_failures: u32 = 0,
    thread_ids: [N_YIELDS + 1]u64 = .{ 0, 0, 0, 0, 0, 0 },
};

fn coroBody(c: *coro.Coroutine, userdata: ?*anyopaque) void {
    const state: *State = @ptrCast(@alignCast(userdata.?));

    // Local array on the coroutine's mmap-backed stack. Should
    // survive every yield. If we treat fiber stack as thread-local
    // and it gets reallocated/clobbered on resume, this array would
    // have garbage.
    var local_array: [16]u32 = undefined;
    for (&local_array, 0..) |*v, i| v.* = @as(u32, @intCast(i)) * 137 + 5;

    var i: u32 = 0;
    while (i < N_YIELDS) : (i += 1) {
        state.iteration = i + 1;
        state.expected_witness = i * 31 + 7;
        state.thread_ids[i] = std.Thread.getCurrentId();

        c.yield();

        // After resume: the local array should still have the values
        // we wrote before the yield.
        for (local_array, 0..) |v, j| {
            if (v != @as(u32, @intCast(j)) * 137 + 5) {
                state.stack_check_failures += 1;
                break;
            }
        }
        // The expected_witness was computed before yield using i.
        // After resume, re-check using i (which lives in a register
        // / on stack across the yield).
        state.local_witness_after_resume = i * 31 + 7;
    }
    state.thread_ids[N_YIELDS] = std.Thread.getCurrentId();
}

const StepArg = struct { c: *coro.Coroutine };

fn stepOnceInThread(arg: *StepArg) void {
    coro.setupStackGrowth() catch unreachable;
    defer coro.cleanupStackGrowth();
    arg.c.step();
}

test "Cross-thread coro: each step on a fresh OS thread" {
    var state: State = .{};

    var parent_ctx: coro.Context = undefined;
    var c: coro.Coroutine = .{
        .parent_context_ptr = &parent_ctx,
        .context = undefined,
    };

    try coro.stackAlloc(&c.context.stack_info, 1 * 1024 * 1024, 64 * 1024);
    defer coro.stackFree(c.context.stack_info);

    c.setup(&coroBody, &state);

    // N_YIELDS yields from the body's while loop + 1 final yield
    // emitted by zio.coro's entrypoint after the body returns.
    const total_steps: usize = N_YIELDS + 1;

    var i: usize = 0;
    while (i < total_steps) : (i += 1) {
        var arg: StepArg = .{ .c = &c };
        const t = try std.Thread.spawn(.{}, stepOnceInThread, .{&arg});
        t.join();
    }

    std.log.debug("iteration={d} (expected {d})", .{ state.iteration, N_YIELDS });
    std.log.debug("stack_check_failures={d}", .{state.stack_check_failures});
    std.log.debug("local_witness_after_resume={d} (expected {d})", .{
        state.local_witness_after_resume,
        (N_YIELDS - 1) * 31 + 7,
    });

    var distinct = std.AutoHashMap(u64, void).init(std.testing.allocator);
    defer distinct.deinit();
    for (state.thread_ids) |tid| try distinct.put(tid, {});
    std.log.debug("distinct OS threads observed={d}", .{distinct.count()});

    try std.testing.expectEqual(@as(u32, N_YIELDS), state.iteration);
    try std.testing.expectEqual(@as(u32, 0), state.stack_check_failures);
    try std.testing.expectEqual(@as(u32, (N_YIELDS - 1) * 31 + 7), state.local_witness_after_resume);
    // We spawn a fresh thread per step (and join before the next),
    // so the IDs *can* match (OS may recycle), but we expect at
    // least 2 distinct in practice. The real value of this check
    // is that if cross-thread is broken, we'd have crashed before
    // reaching here.
    try std.testing.expect(distinct.count() >= 2);
}
