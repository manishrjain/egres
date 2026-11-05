// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

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
//! Each fiber gets a FIXED stack of `stack_size` bytes (1 MiB default),
//! fully committed (PROT_READ|WRITE) up front — there is NO on-demand
//! growth. Physical pages are still backed lazily by the kernel, so a
//! large `stack_size` costs only the pages actually touched, not real RAM.
//! Dropping growth removes the process-wide SIGSEGV stack-growth handler,
//! whose thread-local current-context assumption a work-stealing scheduler
//! violates when it migrates a fiber mid-life. The bottom-most page stays a
//! guard, so recursion past `stack_size` crashes with a normal backtrace.
//! Bodies that recurse deeply (e.g. the SQL parser) pass a larger
//! `stack_size` in `SpawnOptions`.

const std = @import("std");
const utils = @import("utils");
const builtin = @import("builtin");
const zio = @import("zio");
const _spinlock = @import("spinlock.zig");
const _rt = @import("runtime.zig");

pub const coro = zio.coro;
const Coroutine = coro.Coroutine;
const Context = coro.Context;
const Spinlock = _spinlock.Spinlock;
const Ctx = @import("ctx.zig").Ctx;

/// One pending deadline in a per-worker `Deadlines` min-heap. Each `Fiber`
/// OWNS one of these (`Fiber.timeout`) rather than putting it on the caller's
/// stack: a timeout that fires after its op already exited then dereferences
/// STABLE fiber memory and can only ever wake THIS fiber — never an unrelated
/// one through a reused stack frame (the stack-Timeout use-after-free). The
/// deadline machinery in `deadlines.zig` operates on these by pointer.
pub const Timeout = struct {
    /// Invoked (on the owner worker) when the deadline fires before it is
    /// cancelled. For a fiber's own timeout this bumps `Timeout._timed_out` and
    /// wakes the fiber; `ctx` is the `*Fiber`.
    on_fire: *const fn (ctx: *anyopaque) void,
    on_fire_ctx: *anyopaque,
    /// Set to true if timer actually fired.
    _fired: std.atomic.Value(bool) = .init(false),
    /// Absolute monotonic-ms deadline. Filled by `register`.
    _deadline_ms: i64 = 0,
    /// First-fire-wins token: whichever of (deadline fires, op completes) swaps
    /// this false→true owns the wake; the loser is a no-op. Reset per arm.
    _race: std.atomic.Value(bool) = .init(false),
    /// Transient `drainExpired` fire-list link; unused while parked.
    _wake_next: ?*Timeout = null,

    /// didFire would return true if timeout did fire.
    pub inline fn didFire(t: *Timeout) bool {
        return t._fired.load(.acquire);
    }
    /// Win the first-fire race: true if THIS caller claimed it, false if
    /// already claimed. Used by `drainExpired` and op-completes-first paths.
    pub fn winRace(t: *Timeout) bool {
        return !t._race.swap(true, .acq_rel);
    }

    /// reset sets the _done to false, so the caller can claim it and win the
    /// race to wake up the fiber.
    pub fn reset(t: *Timeout) void {
        t._fired = .init(false);
        t._race = .init(false);
    }
};

/// Placeholder `on_fire` for an unarmed fiber timeout. A timed op installs the
/// real callback (`deadlines.fiberTimedOut`) when it arms; an unarmed timeout is
/// never in a heap, so this must never run — make it loud if the invariant breaks.
fn timeoutUnarmed(_: *anyopaque) void {
    @panic("fiber timeout fired while unarmed (in a deadline heap without being armed)");
}

pub fn ReturnOf(comptime func: anytype) type {
    return @typeInfo(@TypeOf(func)).@"fn".return_type.?;
}

/// Per-spawn join state. Embedded at the *start* of every Fiber's heap
/// args context, so a `JoinHandle(T)` can recover `*JoinState(T)` from
/// `fiber.arg` via a plain pointer cast — without knowing the args'
/// concrete type. The trampoline writes `result`; the runtime's
/// `on_finished` hook captures any parked joiner under `sl`.
pub fn JoinState(comptime T: type) type {
    return struct {
        /// User function's return value. Written by the trampoline
        /// just before the fiber's body returns; visible to `join()`
        /// once it observes `released` under `sl`.
        result: T = undefined,
        /// At most one fiber parks here; `join()` asserts the slot
        /// is empty before claiming it.
        joiner: ?*Fiber = null,
        /// Set under `sl` by the runtime's on-finished hook — the
        /// worker's LAST touch of the Fiber. `join()`'s fast path may
        /// only reap (and free the fiber) once it observes this.
        released: bool = false,
        /// Serializes the "check fiber finished / register joiner"
        /// critical section in `join()` against the on-finished
        /// hook's "read and clear joiner" critical section.
        sl: Spinlock = .{},
    };
}

/// Typed handle returned by `spawn`. Wraps the fiber pointer and a
/// `*JoinState(T)` into the fiber's heap context, so `join()` can park
/// on the result without knowing the args' concrete type.
///
/// Call `join()` exactly once and don't touch the
/// handle afterward. (Fire-and-forget work that has no handle uses
/// `spawnDetached`, which frees itself when its body returns.) A handle that is
/// never joined leaves its fiber in the registry until `Runtime.deinit` sweeps
/// it — bounded, but not eager.
pub fn JoinHandle(comptime T: type) type {
    return struct {
        const Self = @This();

        fiber: *Fiber,
        state: *JoinState(T),
        /// Snapshot of `fiber.cpu_total_ns`, captured by `join()` just before
        /// the fiber is freed, so callers can read CPU accounting post-join
        /// (when `fiber` is gone). Zero until `join()` returns.
        cpu_total_ns: u64 = 0,

        /// Block until the fiber's body returns, then return its value. Takes
        /// `*Self` because it writes `cpu_total_ns` back into the handle. Must
        /// be called from a fiber context — `park()` requires `currentFiber()`.
        ///
        /// CONSUMES the handle: captures the result + `cpu_total_ns`, then frees
        /// the fiber. Call exactly ONCE. Enqueues-once / parks-once and PANICS on
        /// a spurious wake — the only legitimate waker of a join-parked fiber is
        /// the finish hook (`on_finished`), run after the body stored `.finished`
        /// and cleared `joiner`, so on resume `isFinished()` must hold.
        pub fn join(self: *Self) T {
            self.state.sl.lock();
            // Fast path keys on `released` (the worker's last touch), NOT
            // `isFinished()`: the body sets `.finished` while the worker
            // still has CPU charging + the on-finished hook to run, so
            // reaping on the tag alone frees the fiber under its feet. A
            // join landing in that window parks instead; the hook wakes it.
            if (self.state.released) {
                self.state.sl.unlock();
                return self.reap();
            }
            // Single-joiner: no OTHER fiber may already be parked on this handle.
            std.debug.assert(self.state.joiner == null);
            self.state.joiner = _rt.currentFiber();
            self.state.sl.unlock();

            _rt.park();
            if (!self.fiber.isFinished()) std.debug.panic(
                "JoinHandle.join: woken but fiber 0x{x} not finished — spurious wake",
                .{@intFromPtr(self.fiber)},
            );
            return self.reap();
        }

        /// Capture the fiber's post-run stats + result, then free the fiber.
        /// Run once the fiber is known finished (fast path or after the park).
        /// Reads happen BEFORE the free so they can't race it; the returned `T`
        /// is a value copy that outlives the fiber.
        fn reap(self: *Self) T {
            self.cpu_total_ns = self.fiber.cpu_total_ns;
            const result = self.state.result;
            _rt.runtime().freeFiber(self.fiber);
            return result;
        }
    };
}

/// Heap-allocated body state attached to every Fiber. Holds the join
/// state plus the user's args tuple. Defined here so `spawn`
/// (in `runtime.zig`) and `Fiber.init` can resolve the same
/// comptime type via `Body(func)` and access `state`/`args` by name
/// without depending on field order.
pub fn Body(comptime func: anytype) type {
    const T = ReturnOf(func);
    const Args = std.meta.ArgsTuple(@TypeOf(func));
    return struct {
        state: JoinState(T) = .{},
        args: Args,
    };
}

pub const default_stack_size: usize = 1 * 1024 * 1024;

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
    /// Lane-relative worker index, wrapping past the lane's size. High
    /// priority: the home worker the fiber is pinned to (required).
    /// Normal priority: the worker it is placed on — for a caller with a
    /// better spread than the scheduler's local push (a listener dealing
    /// connections round-robin); the fiber may still be stolen later.
    /// null: the current worker's ring, or an idle lane peer when
    /// spawned off-lane.
    worker_id: ?u8 = null,
    lane: Lane = .default,
    priority: Priority = .normal,
    /// Bytes of stack reserved AND fully committed for this fiber. Fixed —
    /// no on-demand growth, so a body that recurses past this hits the
    /// guard page and crashes (raise it for deep-recursion bodies). Lazily
    /// backed, so a large value only costs the pages actually touched.
    stack_size: usize = default_stack_size,
};

/// Type-erased fiber body. A typed-args helper lives in `runtime.zig`.
pub const FiberFn = *const fn (*anyopaque) void;

/// Scheduler self-check level — the single knob for both guards below. Each
/// level includes all lower ones and adds a costlier corruption tripwire, so
/// level 2 is for an active hunt, not production.
///   0 — off.
///   1 — `runFiber` dispatch guards: the concurrency (`running`) counter + the
///       ready-state assert (an atomic RMW per dispatch). (`Fiber.assertMagic`
///       is separate — it's a plain load, so it runs at EVERY level incl. 0.)
///   2 — + page-protect each fiber's `Coroutine` (saved register context +
///       stack_info), kept mprotect'd READ-ONLY while the fiber is queued and
///       writable only across `coro.step()`, so a wild write into a *queued*
///       fiber's context faults at the exact culprit instruction. The Coroutine
///       becomes its OWN page-aligned allocation (a `*Coroutine` Fiber field) so
///       Zig's field reordering can't share its page with mutable scheduler state.
/// Default: 1 in safety builds, 0 in release. Set to 2 by hand for a
/// context-corruption hunt.
pub const guard_level: u8 = switch (builtin.mode) {
    .Debug, .ReleaseSafe => 1,
    .ReleaseFast, .ReleaseSmall => 0,
};

// The platform's page: 4 KiB on Linux, 16 KiB on Apple Silicon. An
// mprotect of a smaller span would still cover the whole page there.
const guard_page: usize = std.heap.page_size_min;

/// Magic written into `Fiber.magic` at init and verified at every dispatch via
/// `assertMagic` (always on). A mismatch means EITHER the struct was clobbered (a wild
/// write reached the magic) OR `f` doesn't point at a live Fiber (a stale,
/// freed, or garbage pointer was dispatched). Portable — unlike a "does this
/// pointer look like a code address" range check, it assumes nothing about
/// where code or the heap sit in the address space.
pub const fiber_magic: u64 = 0xF1BE_C0DE_F1BE_C0DE;

pub const Fiber = struct {
    /// Pointer (guard on) so the Coroutine can sit on its own guarded page;
    /// inline (guard off) keeps the production layout unchanged.
    coro: if (guard_level >= 2) *Coroutine else Coroutine,
    /// Saved register state of whoever last called `step`. Stored
    /// inline so its address is stable; `coro.parent_context_ptr`
    /// aliases this field.
    parent_context: Context,

    /// Trampoline generated by `init`. Unwraps `body` (a heap-allocated
    /// `Body { state: JoinState(T), args: ArgsTuple }`) and invokes the
    /// user's typed function, writing the result into `state.result`.
    func: FiberFn,
    /// Heap-allocated args+join-state. Owned by this Fiber; freed by
    /// `body_destroy` on `deinit`. Starts with `JoinState(T)` so a
    /// `JoinHandle(T)` can recover `*JoinState(T)` via a plain
    /// `@ptrCast(@alignCast(body))`.
    body: *anyopaque,
    /// Comptime-generated destructor for `body`, baked at `init` time.
    /// `deinit` calls it unconditionally — so a fiber that never ran
    /// (init succeeded, scheduling aborted before the trampoline fired)
    /// still cleans up its args.
    body_destroy: *const fn (
        body: *anyopaque,
        allocator: std.mem.Allocator,
    ) void,
    /// The fiber's own context (ctx.zig): its I/O class, and the
    /// statement it runs or helps. Filled from the spawner's at spawn;
    /// see `spawnInner`.
    ctx: Ctx = .{},
    /// Fires after the fiber's body returns (state == .finished).
    /// Generated at `init` time so it knows the JoinState layout. Sets
    /// `state.released = true` under the join lock and returns the
    /// captured joiner (or null). The *worker* performs the actual
    /// `wake` on the returned fiber — the wake must come AFTER the
    /// worker's CPU charge, and only `Worker.run` knows when that is.
    on_finished: *const fn (self: *Fiber) ?*Fiber,

    /// Struct-integrity / live-Fiber magic — see `fiber_magic`. Default-init
    /// so every Fiber creation writes the magic and freed/reused memory won't
    /// carry it. Verified at every dispatch via `assertMagic`.
    magic: u64 = fiber_magic,

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
    /// normal-priority routing to pick a worker ring in the right lane.
    lane: Lane,
    /// Absolute index of the worker a HIGH-priority fiber is pinned to;
    /// resolved from the lane-relative `SpawnOptions.worker_id` by
    /// `spawnInner`, read by `routeReady`. Not set for normal priority:
    /// a normal fiber migrates within its lane and has no home.
    home_worker_id: u8,

    cpu_total_ns: u64,
    /// Coarse-clock millisecond timestamp captured by `park()`.
    /// Used by `dumpFibers` to print "parked_for=Xms" — diagnostic
    /// only, so 1 ms resolution is fine and the read is one atomic
    /// load (no syscall).
    parked_since_ms: i64,

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
    /// Spawn order on the runtime, set by `spawn_`: the fiber's identity
    /// in the simulation's dispatch trace, where a pointer would differ
    /// between two runs of one seed.
    seq: u64,

    /// This fiber's OWN deadline timeout (see `Timeout`). Reused across every
    /// timed op — `sleepMs`, watermark timed waits, and future timed I/O. Lives
    /// here (in the leaked Fiber) instead of on the op's stack so a late fire
    /// can't dangle. `on_fire`/`on_fire_ctx` are armed by `deadlines.armSelf`.
    timeout: Timeout,

    /// Queue-membership invariant: 0 = in no ready/global/remote queue,
    /// 1 = in exactly one. `assertEnQ`/`assertDQ` maintain it and panic
    /// on a double-queue (→ sequential double-dispatch, which the `running`
    /// guard can't see) or double-dequeue. Compiled out without
    /// runtime_safety; atomic because peers touch it cross-thread.
    qstate: std.atomic.Value(u8) = .init(0),

    /// Fire-and-forget marker — who frees this fiber. `false` (default) for a
    /// `spawn`ed fiber: it has a `JoinHandle`, and that handle's `join()` is the
    /// ONLY thing that frees it. The runtime never frees a joined fiber — when
    /// the body returns it just leaves the fiber `.finished` in the registry for
    /// `join` to reap (deterministic owner). `true` for a `spawnDetached` fiber:
    /// it has no handle, so `runFiber` frees it itself when the body returns.
    /// Set once at spawn, before the fiber can run; never mutated after.
    detached: bool = false,

    /// Assert this points at a live, un-clobbered Fiber: the magic must still
    /// read `fiber_magic`. A mismatch means the struct was overwritten by a
    /// wild write, or `f` is a stale/freed/garbage pointer that isn't a Fiber
    /// at all. Left ON in every build (including release) — one u64 load +
    /// compare per dispatch is negligible next to a context switch, and a
    /// wild-dispatch crash is worth catching in production too. `inline` so
    /// it's free at the call site.
    pub inline fn assertMagic(self: *const Fiber) void {
        if (self.magic != fiber_magic) std.debug.panic(
            "fiber 0x{x} bad magic=0x{x} (want 0x{x}) — clobbered struct or non-Fiber pointer; state={x}",
            .{ @intFromPtr(self), self.magic, fiber_magic, self.state.load(.monotonic) },
        );
    }

    /// First-fire arbitration for a waker (e.g. `Watermark.advance`/`close`):
    /// returns true if THIS caller won the right to wake the fiber. When a
    /// deadline is armed (a timed wait), it competes on the same one-shot token,
    /// so exactly one of {this waker, the deadline} wins and the loser bows out.
    /// With no deadline armed there's no competitor, so the waker always wins.
    /// Call under whatever lock publishes the waiter, so `timeout_armed` is seen.
    pub inline fn winRace(self: *Fiber) bool {
        return self.timeout.winRace();
    }

    pub fn assertEnQ(self: *Fiber) void {
        if (comptime !std.debug.runtime_safety) return;
        const prev = self.qstate.swap(1, .acq_rel);
        if (prev != 0) std.debug.panic(
            "fiber 0x{x} ENQUEUED while already queued (was {d}) state={x} prio={s}",
            .{ @intFromPtr(self), prev, self.state.load(.monotonic), @tagName(self.priority) },
        );
    }

    pub fn assertDQ(self: *Fiber) void {
        if (comptime !std.debug.runtime_safety) return;
        const prev = self.qstate.swap(0, .acq_rel);
        if (prev != 1) std.debug.panic(
            "fiber 0x{x} DEQUEUED while not queued (was {d}) state={x} prio={s}",
            .{ @intFromPtr(self), prev, self.state.load(.monotonic), @tagName(self.priority) },
        );
    }

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
        const BodyT = Body(func);

        const helpers = struct {
            fn trampoline(opaque_ctx: *anyopaque) void {
                const c: *BodyT = @ptrCast(@alignCast(opaque_ctx));
                if (T == void) {
                    @call(.auto, func, c.args);
                } else {
                    c.state.result = @call(.auto, func, c.args);
                }
            }
            fn destroy(opaque_ctx: *anyopaque, a: std.mem.Allocator) void {
                const c: *BodyT = @ptrCast(@alignCast(opaque_ctx));
                a.destroy(c);
            }
            fn onFinished(self: *Fiber) ?*Fiber {
                const c: *BodyT = @ptrCast(@alignCast(self.body));
                c.state.sl.lock();
                defer c.state.sl.unlock();
                c.state.released = true;
                const waiter = c.state.joiner;
                c.state.joiner = null;
                return waiter;
            }
        };

        const body = try allocator.create(BodyT);
        errdefer allocator.destroy(body);
        body.* = .{ .args = args };

        const self = try allocator.create(Fiber);
        errdefer allocator.destroy(self);

        const init_state = State{ .tag = .new, .awaken = false };
        self.* = .{
            .coro = if (comptime guard_level >= 2) try allocCoro() else .{
                .parent_context_ptr = &self.parent_context,
                .context = undefined,
            },
            .parent_context = undefined,
            .func = &helpers.trampoline,
            .body = body,
            .body_destroy = &helpers.destroy,
            .on_finished = &helpers.onFinished,
            .state = std.atomic.Value(u8).init(@bitCast(init_state)),
            .intent = .reschedule,
            .running = std.atomic.Value(u32).init(0),
            .priority = opts.priority,
            .lane = opts.lane,
            .home_worker_id = undefined, // high priority only; see spawnInner
            .cpu_total_ns = 0,
            .parked_since_ms = 0,
            .waker_prev = null,
            .waker_next = null,
            .registry_index = std.math.maxInt(usize),
            .seq = 0,
            // Owned timeout: ctx is this (stable) fiber; `on_fire` is a tripwire
            // until a timed op arms the real one via `deadlines.armSelf`.
            .timeout = .{ .on_fire = &timeoutUnarmed, .on_fire_ctx = self },
        };
        errdefer if (comptime guard_level >= 2) freeCoro(self.coro);
        if (comptime guard_level >= 2) self.coro.* = .{
            .parent_context_ptr = &self.parent_context,
            .context = undefined,
        };

        try coro.stackAlloc(
            &self.coro.context.stack_info,
            opts.stack_size, // maximum_size
            opts.stack_size, // committed_size == maximum: fully committed, never grows
        );
        errdefer coro.stackFree(self.coro.context.stack_info);

        self.coro.setup(&fiberEntry, self);
        // Context fully initialized → lock its page read-only. From here it is
        // writable only across `coro.step()` (see runtime.runFiber).
        self.protectContext();
        return self;
    }

    /// The Coroutine lives in its own page-aligned allocation so its page can be
    /// mprotect'd independently of the always-mutable scheduler fields.
    fn allocCoro() !*Coroutine {
        const total = std.mem.alignForward(usize, @sizeOf(Coroutine), guard_page);
        const mem = try std.heap.page_allocator.alloc(u8, total);
        return @ptrCast(@alignCast(mem.ptr));
    }
    fn freeCoro(c: *Coroutine) void {
        mprotectCoro(c, .{ .READ = true, .WRITE = true });
        const total = std.mem.alignForward(usize, @sizeOf(Coroutine), guard_page);
        std.heap.page_allocator.free(@as([*]u8, @ptrCast(c))[0..total]);
    }

    fn mprotectCoro(c: *Coroutine, prot: std.c.PROT) void {
        // `c` is page-aligned: it comes from the page allocator
        // (`allocCoro`).
        const err = utils.mprotect(@intFromPtr(c), guard_page, prot);
        if (err != .SUCCESS) std.debug.panic(
            "mprotect coro page failed: {s}",
            .{@tagName(err)},
        );
    }

    /// Make the Coroutine page read-only / read-write. No-op when the guard is
    /// off. `runtime.runFiber` brackets `coro.step()` with these.
    pub fn protectContext(self: *Fiber) void {
        if (comptime guard_level < 2) return;
        mprotectCoro(self.coro, .{ .READ = true });
    }
    pub fn unprotectContext(self: *Fiber) void {
        if (comptime guard_level < 2) return;
        mprotectCoro(self.coro, .{ .READ = true, .WRITE = true });
    }

    pub fn deinit(self: *Fiber, allocator: std.mem.Allocator) void {
        self.body_destroy(self.body, allocator);
        coro.stackFree(self.coro.context.stack_info);
        if (comptime guard_level >= 2) freeCoro(self.coro);
        allocator.destroy(self);
    }

    /// Manual mode: caller -> fiber. Saves caller CPU state into
    /// `parent_context` and resumes the fiber. Returns when the fiber
    /// yields or its body returns. Not for use under a Runtime.
    pub fn step(self: *Fiber) void {
        self.unprotectContext();
        self.coro.step();
        self.protectContext();
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
        self.func(self.body);
        // Only the running fiber writes its own state to .finished, so
        // a plain store suffices. wake() on .finished is a no-op, so
        // dropping a pending awaken bit is harmless.
        const finished = State{ .tag = .finished, .awaken = false };
        self.state.store(@bitCast(finished), .release);
    }
};
