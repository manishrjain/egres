//! Thread-pool offload for blocking operations the libxev loop can't do
//! asynchronously — primarily file `fsync` (libxev has no fsync op) and the
//! blocking filesystem metadata syscalls (open/stat/rename/…).
//!
//! A fiber calls `run(func, args)`: the work is queued, the fiber **parks**,
//! a pool thread runs `func(args)` to completion and `wake()`s the fiber.
//! The fiber's worker thread is never blocked — it keeps running other
//! fibers while the syscall is in flight.
//!
//! Two bonuses fall out of running on a pool thread:
//!   - The blocking syscall (and any large path-buffer frame it builds, e.g.
//!     `std.posix.toPosixPath` at ~4 KiB) runs on the pool thread's
//!     full-size OS stack — never a fiber's growable stack, so it can't trip
//!     the stack-growth SEGV hazard.
//!   - The pool is the natural seam for a cross-platform op implementation
//!     later (it can call blocking std-library I/O without blocking a fiber).
//!
//! This module is intentionally I/O-agnostic: it only knows how to run an
//! arbitrary function off-fiber and wake the caller. Callers (e.g. `fs.zig`)
//! supply the actual syscall.

const std = @import("std");
const fb = @import("fiber.zig");
const rt = @import("runtime.zig");

const Fiber = fb.Fiber;

/// A unit of work on the pool. Embedded in the caller's `run` frame (which
/// lives on the parked fiber's stack and so outlives the call).
pub const Job = struct {
    /// Runs the work; recovers the caller's frame via `@fieldParentPtr`.
    run_fn: *const fn (*Job) void,
    /// Fiber to wake once `run_fn` returns.
    fiber: *Fiber,
    /// Intrusive FIFO link, owned by the pool while queued.
    next: ?*Job = null,
};

// This build's std has no standalone `std.Thread.Mutex`/`Condition` — they
// moved to `std.Io.Mutex`/`std.Io.Condition`, which take an `Io` to supply
// the OS futex. A no-alloc single-threaded `std.Io.Threaded` provides exactly
// that; its futex/mutex ops are ordinary OS-thread primitives, safe to use
// across our pool threads (the "single-threaded" only refers to its async
// model, which we never touch). We use the Uncancelable variants — the pool
// has no cancellation.
const Mutex = std.Io.Mutex;
const Condition = std.Io.Condition;

var io_backend: std.Io.Threaded = .init_single_threaded;

/// The pool's blocking `std.Io` — a no-alloc, inline `std.Io.Threaded`. Used
/// internally for the queue's mutex/condvar, and exposed so file/net ops
/// (which run on pool threads, or inline for instant ops) can issue portable
/// `std.Io.File`/`Dir`/`net` syscalls through it instead of raw `std.os.linux`.
pub fn io() std.Io {
    return io_backend.io();
}

const Pool = struct {
    threads: []std.Thread = &.{},
    allocator: std.mem.Allocator = undefined,
    mu: Mutex = .init,
    cond: Condition = .init,
    head: ?*Job = null,
    tail: ?*Job = null,
    stopping: bool = false,
};

var pool: Pool = .{};

/// Start the offload pool with `num_threads` worker threads. Called by
/// `Runtime.init`. `num_threads == 0` is treated as 1.
pub fn start(allocator: std.mem.Allocator, num_threads: usize) !void {
    const n = @max(1, num_threads);
    pool.allocator = allocator;
    pool.mu = .init;
    pool.cond = .init;
    pool.head = null;
    pool.tail = null;
    pool.stopping = false;
    pool.threads = try allocator.alloc(std.Thread, n);
    errdefer allocator.free(pool.threads);

    var spawned: usize = 0;
    errdefer {
        // Tell the already-spawned threads to exit and join them.
        pool.mu.lockUncancelable(io());
        pool.stopping = true;
        pool.cond.broadcast(io());
        pool.mu.unlock(io());
        for (pool.threads[0..spawned]) |t| t.join();
    }
    for (pool.threads) |*t| {
        t.* = try std.Thread.spawn(.{}, workerLoop, .{});
        spawned += 1;
    }
}

/// Stop the pool: signal every worker to drain and exit, then join. Called
/// by `Runtime.deinit` BEFORE the workers/queues are torn down, so no pool
/// thread is mid-`wake()` into freed runtime state.
pub fn stop() void {
    if (pool.threads.len == 0) return;
    pool.mu.lockUncancelable(io());
    pool.stopping = true;
    pool.cond.broadcast(io());
    pool.mu.unlock(io());
    for (pool.threads) |t| t.join();
    pool.allocator.free(pool.threads);
    pool.threads = &.{};
}

fn enqueue(job: *Job) void {
    pool.mu.lockUncancelable(io());
    defer pool.mu.unlock(io());
    job.next = null;
    if (pool.tail) |t| {
        t.next = job;
    } else {
        pool.head = job;
    }
    pool.tail = job;
    pool.cond.signal(io());
}

fn workerLoop() void {
    while (true) {
        pool.mu.lockUncancelable(io());
        while (pool.head == null and !pool.stopping) pool.cond.waitUncancelable(io(), &pool.mu);
        if (pool.head == null) {
            // Woken with no work → stopping.
            pool.mu.unlock(io());
            return;
        }
        const job = pool.head.?;
        pool.head = job.next;
        if (pool.head == null) pool.tail = null;
        pool.mu.unlock(io());

        job.run_fn(job);
        // Cross-thread wake: routes the fiber back to a worker + notifies it.
        rt.wake(job.fiber);
    }
}

fn ReturnOf(comptime func: anytype) type {
    return @typeInfo(@TypeOf(func)).@"fn".return_type.?;
}

/// Run `func(args)` on the offload pool, parking the calling fiber until it
/// completes, and return whatever `func` returned. Must be called from a
/// fiber context (it parks). `func` runs on a plain OS thread, so it must
/// NOT touch fiber-runtime state (no `park`/`yield`/`currentFiber`).
pub fn run(comptime func: anytype, args: std.meta.ArgsTuple(@TypeOf(func))) ReturnOf(func) {
    const Args = @TypeOf(args);
    const Ret = ReturnOf(func);
    const Frame = struct {
        job: Job,
        args: Args,
        result: Ret,
        fn trampoline(j: *Job) void {
            const self: *@This() = @alignCast(@fieldParentPtr("job", j));
            self.result = @call(.auto, func, self.args);
        }
    };

    var frame: Frame = .{
        .job = .{ .run_fn = Frame.trampoline, .fiber = rt.currentFiber() },
        .args = args,
        .result = undefined,
    };
    enqueue(&frame.job);
    // The pool thread may complete + wake before we park; the runtime's
    // awaken-token fast path in `park()` handles that race (and provides the
    // acquire/release ordering that makes `frame.result` visible here).
    rt.park();
    return frame.result;
}
