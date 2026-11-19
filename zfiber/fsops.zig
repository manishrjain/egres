// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

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
//! A queue per lane, the raft lane's taken first. A flush over 20,000
//! tenants is 40,000 jobs here, a create and an fsync per table, and
//! the wal's rename and directory sync at a rotation queued behind
//! every one of them: seconds, under the wal lock. A raft-lane fiber's
//! job now waits for one job to finish, never for the queue.
//!
//! This module is intentionally I/O-agnostic: it only knows how to run an
//! arbitrary function off-fiber and wake the caller. Callers (e.g. `fs.zig`)
//! supply the actual syscall.

const std = @import("std");
const _fb = @import("fiber.zig");
const _rt = @import("runtime.zig");

const Fiber = _fb.Fiber;
const Lane = _fb.Lane;

/// A unit of work on the pool. Embedded in the caller's `run` frame (which
/// lives on the parked fiber's stack).
pub const Job = struct {
    /// Runs the work; recovers the caller's frame via `@fieldParentPtr`.
    run_fn: *const fn (*Job) void,
    /// Fiber to wake once `run_fn` returns.
    fiber: *Fiber,
    done: std.atomic.Value(bool) = .init(false),
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

/// An intrusive FIFO of jobs, under the pool's mutex.
const Queue = struct {
    head: ?*Job = null,
    tail: ?*Job = null,

    fn push(self: *Queue, job: *Job) void {
        job.next = null;
        if (self.tail) |t| {
            t.next = job;
        } else {
            self.head = job;
        }
        self.tail = job;
    }

    fn pop(self: *Queue) ?*Job {
        const job = self.head orelse return null;
        self.head = job.next;
        if (self.head == null) self.tail = null;
        return job;
    }
};

const Pool = struct {
    threads: []std.Thread = &.{},
    allocator: std.mem.Allocator = undefined,
    mu: Mutex = .init,
    cond: Condition = .init,
    /// A queue per lane: the raft lane's jobs, the wal's and the
    /// transport's, ahead of the default lane's, a flush's or a
    /// compaction's syncs by the thousand.
    queues: [Lane.count]Queue = .{ .{}, .{} },
    stopping: bool = false,

    fn push(self: *Pool, job: *Job, lane: Lane) void {
        self.queues[@intFromEnum(lane)].push(job);
    }

    /// The raft lane's first, then the default lane's.
    fn pop(self: *Pool) ?*Job {
        if (self.queues[@intFromEnum(Lane.raft)].pop()) |j| return j;
        return self.queues[@intFromEnum(Lane.default)].pop();
    }
};

var pool: Pool = .{};

/// The simulation (`startInline`): `run` calls the function on the
/// calling worker thread and returns, no pool, no park. One worker
/// means nothing else could run meanwhile anyway, and the order of
/// completion becomes the order of the calls.
var inline_mode: bool = false;

pub fn startInline() void {
    inline_mode = true;
}

/// Start the offload pool with `num_threads` worker threads. Called by
/// `Runtime.init`. `num_threads == 0` is treated as 1.
pub fn start(allocator: std.mem.Allocator, num_threads: usize) !void {
    const n = @max(1, num_threads);
    pool.allocator = allocator;
    pool.mu = .init;
    pool.cond = .init;
    pool.queues = .{ .{}, .{} };
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
    inline_mode = false;
    if (pool.threads.len == 0) return;
    pool.mu.lockUncancelable(io());
    pool.stopping = true;
    pool.cond.broadcast(io());
    pool.mu.unlock(io());
    for (pool.threads) |t| t.join();
    pool.allocator.free(pool.threads);
    pool.threads = &.{};
}

fn enqueue(job: *Job, lane: Lane) void {
    pool.mu.lockUncancelable(io());
    defer pool.mu.unlock(io());
    pool.push(job, lane);
    pool.cond.signal(io());
}

fn workerLoop() void {
    while (true) {
        pool.mu.lockUncancelable(io());
        const job = while (true) {
            if (pool.pop()) |j| break j;
            // Woken with no work: stopping.
            if (pool.stopping) {
                pool.mu.unlock(io());
                return;
            }
            pool.cond.waitUncancelable(io(), &pool.mu);
        };
        pool.mu.unlock(io());

        job.run_fn(job);
        job.done.store(true, .release);
        // Cross-thread wake: routes the fiber back to a worker + notifies it.
        _rt.wake(job.fiber);
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
    if (inline_mode) return @call(.auto, func, args);

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

    const self = _rt.currentFiber();
    var frame: Frame = .{
        .job = .{ .run_fn = Frame.trampoline, .fiber = self },
        .args = args,
        .result = undefined,
    };
    enqueue(&frame.job, self.lane);

    _rt.park();
    // Invariant: this fiber parks and resumes ONLY when the pool ran its job and
    // set `done`. A resume with `!done` is a spurious wake — a bug at its source
    // (e.g. a stale deadline firing into us), not something to silently re-park
    // around. Fail loud so the next such regression is caught here.
    if (!frame.job.done.load(.acquire)) {
        @panic("fsops: fiber woken before its job completed — spurious wake");
    }
    return frame.result;
}
