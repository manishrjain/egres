//! Stackful fiber primitive. A `Fiber` wraps a coroutine (its own
//! mmap-backed stack + saved register context) and the metadata the
//! scheduler needs: priority, home worker, intrusive list pointers,
//! CPU accounting, park diagnostics, atomic state.
//!
//! Two driving modes coexist:
//!   - Manual: `fiber.step()` / `fiber.yield()` — for isolated tests
//!     that don't want a runtime. State is not touched beyond the
//!     body's eventual transition to `.finished`.
//!   - Scheduled: `runtime.zig` drives fibers via `Worker.run`, using
//!     `state` (with awaken-bit CAS) and `intent` to distinguish
//!     yield from park.
//!
//! Stacks are growable. Each fiber reserves `stack_max` virtual bytes
//! (1 MiB default) with `stack_commit` committed up front (16 KiB
//! default); the rest is `PROT_NONE`. The first write past the
//! committed region traps SIGSEGV; the handler installed by
//! `coro.setupStackGrowth` (called per-worker on `Worker.run` entry)
//! commits more pages on the fly. The bottom-most page stays a guard:
//! truly runaway recursion crashes with a real backtrace instead of
//! silently chewing through 1 MiB before dying.

const std = @import("std");
const zio = @import("zio");
const spinlock = @import("spinlock.zig");

pub const coro = zio.coro;
const Coroutine = coro.Coroutine;
const Context = coro.Context;
const Spinlock = spinlock.Spinlock;

pub fn ReturnOf(comptime func: anytype) type {
    return @typeInfo(@TypeOf(func)).@"fn".return_type.?;
}

/// Per-spawn join state. Embedded at the *start* of every Fiber's heap
/// args context, so a `JoinHandle(T)` can recover `*JoinState(T)` from
/// `fiber.arg` via a plain pointer cast — without knowing the args'
/// concrete type. The trampoline writes `result`; the runtime's
/// `on_finished` hook captures any parked joiner under `sl`. The
/// "is the fiber done" signal lives on `Fiber.state.tag` already, so
/// there's no separate finished flag here.
pub fn JoinState(comptime T: type) type {
    return struct {
        /// User function's return value. Written by the trampoline
        /// just before the fiber's body returns; visible to `join()`
        /// once it observes `Fiber.state.tag == .finished` (whose
        /// release-store happens after the trampoline's write).
        result: T = undefined,
        /// At most one fiber parks here; `join()` asserts the slot
        /// is empty before claiming it.
        joiner: ?*Fiber = null,
        /// Serializes the "check fiber finished / register joiner"
        /// critical section in `join()` against the on-finished
        /// hook's "read and clear joiner" critical section.
        sl: Spinlock = .{},
    };
}

/// Heap-allocated context attached to every Fiber. Holds the join
/// sync state plus the user's args tuple. Defined here so `spawn`
/// (in `runtime.zig`) and `Fiber.init` can resolve the same
/// comptime type via `Ctx(func)` and access `sync`/`args` by name
/// without depending on field order.
pub fn Ctx(comptime func: anytype) type {
    const T = ReturnOf(func);
    const Args = std.meta.ArgsTuple(@TypeOf(func));
    return struct {
        state: JoinState(T) = .{},
        args: Args,
    };
}

pub const default_stack_max: usize = 1 * 1024 * 1024;
pub const default_stack_commit: usize = 16 * 1024;

pub const Priority = enum(u1) {
    normal = 0,
    high = 1,

    pub const count = 2;
};

/// Worker-placement lane. Set at spawn; routes the fiber to a worker
/// in the lane's range. `.default` is the catch-all; `.raft` is
/// opt-in for code that wants mainLoop/applyLoop on workers reserved
/// for them.
///
/// If the requested lane has zero workers configured (e.g., tests
/// don't set `.raft`), placement falls back to `.default`.
pub const Lane = enum(u8) {
    default = 0,
    raft = 1,

    pub const count = 2;
};

pub const Tag = enum(u2) { new, ready, waiting, finished };

/// Atomic lifecycle state. The `awaken` pre-wake token closes the race
/// between "fiber decides to park" and "fiber actually saves context
/// and yields": a `wake` arriving in that window sets `awaken=true`
/// on the `.ready` state; the parking path consumes the token and
/// reschedules instead of parking. See `runtime.zig` for the CAS dance.
pub const State = packed struct(u8) {
    tag: Tag,
    awaken: bool,
    _pad: u5 = 0,
};

/// Signal from a yielding fiber to the worker about what to do next.
/// Set just before the fiber calls `coro.yield()`; the worker reads it
/// after the step returns.
pub const YieldIntent = enum(u8) {
    reschedule, // worker pushes back to ready tail
    park, // worker does NOT push; wake() will enqueue when ready
};

pub const SpawnOptions = struct {
    worker_id: ?u8 = null,
    lane: Lane = .default,
    priority: Priority = .normal,
    stack_max: usize = default_stack_max,
    stack_commit: usize = default_stack_commit,
};

/// Type-erased fiber body. A typed-args helper lives in `runtime.zig`.
pub const FiberFn = *const fn (*anyopaque) void;

pub const Fiber = struct {
    coro: Coroutine,
    /// Saved register state of whoever last called `step`. Stored
    /// inline so its address is stable; `coro.parent_context_ptr`
    /// aliases this field.
    parent_context: Context,

    /// Trampoline generated by `init`. Unwraps `ctx` (a heap-allocated
    /// `Ctx { state: JoinState(T), args: ArgsTuple }`) and invokes the
    /// user's typed function, writing the result into `sync.result`.
    func: FiberFn,
    /// Heap-allocated args+sync context. Owned by this Fiber; freed by
    /// `ctx_destroy` on `deinit`. Starts with `JuinSync(T)` so a
    /// `JoinHandle(T)` can recover `*JoinState(T)` via a plain
    /// `@ptrCast(@alignCast(ctx))`.
    ctx: *anyopaque,
    /// Comptime-generated destructor for `ctx`, baked at `init` time.
    /// `deinit` calls it unconditionally — so a fiber that never ran
    /// (init succeeded, scheduling aborted before the trampoline fired)
    /// still cleans up its args.
    ctx_destroy: *const fn (ctx: *anyopaque, allocator: std.mem.Allocator) void,
    /// Fires after the fiber's body returns (state == .finished).
    /// Generated at `init` time so it knows the JoinState layout. Sets
    /// `sync.finished = true` and returns the captured joiner (or
    /// null). The *worker* performs the actual `wake` on the returned
    /// fiber — keeping the wake call inside the runtime avoids a
    /// cyclic `fiber.zig → runtime.zig` import.
    on_finished: *const fn (self: *Fiber) ?*Fiber,

    state: std.atomic.Value(u8),
    intent: YieldIntent,

    /// Concurrency guard: count of workers currently stepping this
    /// fiber. Must never exceed 1 — a fiber is single-owner and only
    /// one worker may run it at a time. `runFiber` increments on entry
    /// and decrements on exit; a value of 2 means the fiber was
    /// double-scheduled onto two queues (a scheduler bug) and we panic
    /// before the shared coro stack corrupts.
    running: std.atomic.Value(u32),

    priority: Priority,
    /// Placement lane (`.default` / `.raft`). Set at spawn; used by
    /// normal-priority routing to pick the right per-lane GlobalQueue.
    lane: Lane,
    home_worker_id: u8,

    cpu_total_ns: u64,
    /// Coarse-clock millisecond timestamp captured by `park()`.
    /// Used by `dumpFibers` to print "parked_for=Xms" — diagnostic
    /// only, so 1 ms resolution is fine and the read is one atomic
    /// load (no syscall).
    parked_since_ms: i64,
    park_trace: [8]usize,

    // Intrusive remote-queue link. Used when a peer worker pushes
    // this fiber onto its home worker's cross-thread injection
    // stack. Disjoint from ready_next — a fiber is either in the
    // remote queue or the local ready queue, never both.
    remote_next: ?*Fiber,
    // Intrusive wait-queue links. A fiber is in at most one wait
    // queue at a time (parked on a Mutex, Condition, Channel, etc.),
    // so a single pair of pointers suffices. Both null when not in
    // any wait queue.
    waker_prev: ?*Fiber,
    waker_next: ?*Fiber,
    /// Slot in the Runtime's `fibers` ArrayList. Maintained by the
    /// runtime's `appendFiber` / `removeFiber` so swap-remove can find
    /// the fiber in O(1). `maxInt` while not registered.
    registry_index: usize,

    /// Create a fiber that will run `func(args...)`. The args tuple is
    /// copied into a heap-allocated context owned by the Fiber; callers
    /// only need to keep alive what the args point to. `func` may
    /// return any type — the return value lands in `JoinState(T).result`,
    /// retrievable via `runtime.JoinHandle(T).join()`.
    pub fn init(
        allocator: std.mem.Allocator,
        opts: SpawnOptions,
        comptime func: anytype,
        args: std.meta.ArgsTuple(@TypeOf(func)),
    ) !*Fiber {
        const T = ReturnOf(func);
        const CtxT = Ctx(func);

        const helpers = struct {
            fn trampoline(opaque_ctx: *anyopaque) void {
                const c: *CtxT = @ptrCast(@alignCast(opaque_ctx));
                if (T == void) {
                    @call(.auto, func, c.args);
                } else {
                    c.state.result = @call(.auto, func, c.args);
                }
            }
            fn destroy(opaque_ctx: *anyopaque, a: std.mem.Allocator) void {
                const c: *CtxT = @ptrCast(@alignCast(opaque_ctx));
                a.destroy(c);
            }
            fn onFinished(self: *Fiber) ?*Fiber {
                const c: *CtxT = @ptrCast(@alignCast(self.ctx));
                c.state.sl.lock();
                defer c.state.sl.unlock();
                const waiter = c.state.joiner;
                c.state.joiner = null;
                return waiter;
            }
        };

        const ctx = try allocator.create(CtxT);
        errdefer allocator.destroy(ctx);
        ctx.* = .{ .args = args };

        const self = try allocator.create(Fiber);
        errdefer allocator.destroy(self);

        const init_state = State{ .tag = .new, .awaken = false };
        self.* = .{
            .coro = .{
                .parent_context_ptr = &self.parent_context,
                .context = undefined,
            },
            .parent_context = undefined,
            .func = &helpers.trampoline,
            .ctx = ctx,
            .ctx_destroy = &helpers.destroy,
            .on_finished = &helpers.onFinished,
            .state = std.atomic.Value(u8).init(@bitCast(init_state)),
            .intent = .reschedule,
            .running = std.atomic.Value(u32).init(0),
            .priority = opts.priority,
            .lane = opts.lane,
            .home_worker_id = opts.worker_id orelse 0,
            .cpu_total_ns = 0,
            .parked_since_ms = 0,
            .park_trace = @splat(0),
            .remote_next = null,
            .waker_prev = null,
            .waker_next = null,
            .registry_index = std.math.maxInt(usize),
        };

        try coro.stackAlloc(
            &self.coro.context.stack_info,
            opts.stack_max,
            opts.stack_commit,
        );
        errdefer coro.stackFree(self.coro.context.stack_info);

        self.coro.setup(&fiberEntry, self);
        return self;
    }

    pub fn deinit(self: *Fiber, allocator: std.mem.Allocator) void {
        self.ctx_destroy(self.ctx, allocator);
        coro.stackFree(self.coro.context.stack_info);
        allocator.destroy(self);
    }

    /// Manual mode: caller -> fiber. Saves caller CPU state into
    /// `parent_context` and resumes the fiber. Returns when the fiber
    /// yields or its body returns. Not for use under a Runtime.
    pub fn step(self: *Fiber) void {
        self.coro.step();
    }

    /// Manual mode: fiber -> caller. Jumps back to the most recent
    /// `step` call frame.
    pub fn yield(self: *Fiber) void {
        self.coro.yield();
    }

    pub fn isFinished(self: *const Fiber) bool {
        return self.loadState().tag == .finished;
    }

    pub fn loadState(self: *const Fiber) State {
        return @bitCast(self.state.load(.acquire));
    }

    /// Atomic CAS on the packed state. Returns the actual prior value
    /// on failure (caller retries with that); `null` on success.
    pub fn casState(self: *Fiber, old: State, new: State) ?State {
        const old_raw: u8 = @bitCast(old);
        const new_raw: u8 = @bitCast(new);
        if (self.state.cmpxchgWeak(old_raw, new_raw, .acq_rel, .acquire)) |actual| {
            return @bitCast(actual);
        }
        return null;
    }

    fn fiberEntry(_: *Coroutine, userdata: ?*anyopaque) void {
        const self: *Fiber = @ptrCast(@alignCast(userdata.?));
        self.func(self.ctx);
        // Only the running fiber writes its own state to .finished, so
        // a plain store suffices. wake() on .finished is a no-op, so
        // dropping a pending awaken bit is harmless.
        const finished = State{ .tag = .finished, .awaken = false };
        self.state.store(@bitCast(finished), .release);
    }
};
