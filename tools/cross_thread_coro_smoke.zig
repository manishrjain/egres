//! Standalone smoke test for zio.coro cross-thread resume.
//!
//! Bypasses the project's custom test runner — just compile this
//! file with `zig build-exe` and run the resulting binary. The aim
//! is to prove (or disprove) that zio.coro lets a single Coroutine
//! be stepped from different OS threads across its yields without
//! corruption.
//!
//! Usage (from repo root):
//!   zig build-exe \
//!       --name cross_thread_coro_smoke \
//!       --dep zio \
//!       -Mroot=tools/cross_thread_coro_smoke.zig \
//!       -Mzio=/home/mrjn/source/zio/src/zio.zig \
//!       -lc
//!   ./cross_thread_coro_smoke
//!
//! The binary prints diagnostics and exits with code 0 on PASS,
//! non-zero on FAIL. `-lc` is required because zio.os.posix uses
//! `extern "c" fn raise` in its previous-handler chain.

const std = @import("std");
const zio = @import("zio");
const coro = zio.coro;

const N_YIELDS: u32 = 5;

const State = struct {
    iteration: u32 = 0,
    expected_witness: u32 = 0,
    local_witness_after_resume: u32 = 0,
    stack_check_failures: u32 = 0,
    thread_ids: [N_YIELDS + 1]u64 = .{ 0, 0, 0, 0, 0, 0 },
};

fn coroBody(c: *coro.Coroutine, userdata: ?*anyopaque) void {
    const state: *State = @ptrCast(@alignCast(userdata.?));

    // Local array on the coroutine's mmap-backed stack. Should
    // survive every yield. If we accidentally reused the stack
    // across cross-thread resumes, these values would garble.
    var local_array: [16]u32 = undefined;
    for (&local_array, 0..) |*v, i| v.* = @as(u32, @intCast(i)) * 137 + 5;

    var i: u32 = 0;
    while (i < N_YIELDS) : (i += 1) {
        state.iteration = i + 1;
        state.expected_witness = i * 31 + 7;
        state.thread_ids[i] = std.Thread.getCurrentId();

        c.yield();

        // After resume: verify the local array contents are intact.
        for (local_array, 0..) |v, j| {
            if (v != @as(u32, @intCast(j)) * 137 + 5) {
                state.stack_check_failures += 1;
                break;
            }
        }
        // Recompute witness from `i` after resume — `i` lives on the
        // coroutine's stack (or in a callee-saved reg) across the yield.
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

pub fn main() !u8 {
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

    var distinct = std.AutoHashMap(u64, void).init(std.heap.page_allocator);
    defer distinct.deinit();
    for (state.thread_ids) |tid| try distinct.put(tid, {});

    std.debug.print(
        "iteration                      = {d} (expected {d})\n" ++
            "stack_check_failures           = {d}\n" ++
            "local_witness_after_resume     = {d} (expected {d})\n" ++
            "distinct OS threads observed   = {d} (of {d} steps)\n",
        .{
            state.iteration,                  N_YIELDS,
            state.stack_check_failures,
            state.local_witness_after_resume, (N_YIELDS - 1) * 31 + 7,
            distinct.count(),                 total_steps,
        },
    );

    const pass = state.iteration == N_YIELDS and
        state.stack_check_failures == 0 and
        state.local_witness_after_resume == (N_YIELDS - 1) * 31 + 7;

    if (pass) {
        std.debug.print("\nPASS: zio.coro supports cross-thread resume\n", .{});
        return 0;
    }
    std.debug.print("\nFAIL: cross-thread resume corrupted state\n", .{});
    return 1;
}
