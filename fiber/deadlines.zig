//! Per-worker timeout machinery: an intrusive min-heap of pending
//! deadlines plus one `xev.Timer` re-armed dynamically to the
//! earliest deadline.
//!
//! Each `Worker` owns one `Deadlines` instance. Timeouts registered
//! on worker N live in worker N's heap; cancellation (when a
//! deadline fires) submits an `xev.Loop.cancel` against worker N's
//! loop — never cross-thread. This keeps every operation on a
//! single thread and side-steps libxev's non-thread-safe per-loop
//! state.
//!
//! Heap impl lives in `fiber/heap.zig` as a generic intrusive
//! `MinHeap(T, key_field, index_field)`. We pay one `usize` per
//! `Timeout` (`heap_index`) in exchange for O(log N) push, pop, AND
//! middle-remove. Middle-remove is the load-bearing op: typical
//! pattern is "register a timeout, op completes first, cancel the
//! timeout" — cancel runs roughly once per op, while the deadline
//! firing only happens when things actually go wrong.

const std = @import("std");
const xev = @import("xev");
const fb = @import("fiber.zig");
const heap = @import("heap.zig");
const coarse = @import("coarse_clock.zig");

const Fiber = fb.Fiber;
const Allocator = std.mem.Allocator;

/// Minimum honored timeout. Requests below this are clamped up. The
/// clock cache that consumers measure against has 1 ms granularity,
/// so sub-ms timeouts would either round to zero or fire immediately
/// — neither useful. Pin the floor at the boundary instead.
pub const min_timeout_ms: u64 = 1;

/// One pending deadline. Lives on the consumer's stack (same pattern
/// as the I/O `Waiter`s). The heap holds pointers to these so the
/// caller's frame is the storage.
pub const Timeout = struct {
    deadline_ms: i64,
    fiber: *Fiber,

    /// First-fire-wins token: whichever of (deadline fires, the
    /// underlying I/O op completes) gets to swap this from false to
    /// true owns the wake. The loser is a no-op.
    done: std.atomic.Value(bool) = .init(false),

    /// Invoked when the deadline fires (i.e., expires before
    /// `Deadlines.cancel(t)` is called). The typical implementation
    /// submits an `xev.Loop.cancel` against the still-in-flight op
    /// so the waiter memory isn't touched after the fiber returns
    /// from the timeout call site, and then wakes the parked fiber.
    callback: ?*const fn (ctx: *anyopaque) void = null,
    callback_ctx: ?*anyopaque = null,

    /// Slot in `Deadlines.heap`. Maintained by `MinHeap`; consumers
    /// should not touch.
    heap_index: usize = Heap.not_in_heap,
};

const Heap = heap.MinHeap(Timeout, "deadline_ms", "heap_index");

pub const Deadlines = struct {
    loop: *xev.Loop,
    timer: xev.Timer,
    /// SQE backing the in-flight timer expiration.
    completion: xev.Completion = undefined,
    /// SQE backing the cancellation half of an `xev.Timer.reset` call.
    /// Required by `reset` because the old SQE has to be canceled
    /// independently of the new SQE being submitted.
    cancel_completion: xev.Completion = undefined,
    /// True while a timer SQE is in flight (between `arm`/`reset` and
    /// the matching callback returning `.disarm`). Decides whether
    /// `armForHead` should call `timer.run` (fresh) or `timer.reset`
    /// (cancel-then-re-arm on the same completion).
    armed: bool = false,
    heap: Heap = .{},

    pub fn init(loop: *xev.Loop) !Deadlines {
        return .{
            .loop = loop,
            .timer = try xev.Timer.init(),
        };
    }

    pub fn deinit(self: *Deadlines, allocator: Allocator) void {
        self.heap.deinit(allocator);
        self.timer.deinit();
    }

    /// Register a deadline. `ms_from_now` is clamped up to
    /// `min_timeout_ms`. The `Timeout` lives on the caller's stack;
    /// this call only links it into the heap.
    ///
    /// "Now" is read via `clockMsUseRarely(.monotonic)` — direct vDSO
    /// CLOCK_MONOTONIC, no caching. Using `loop.now()` would hand us
    /// libxev's cached value, which can be arbitrarily stale between
    /// `loop.run` iterations under heavy worker load; that stale
    /// baseline could let `onTimer`'s drainExpired see both a short and
    /// a long deadline "expired" in one sweep when only the short fired.
    ///
    /// Re-arms the xev timer iff this entry becomes the new earliest
    /// deadline. If the heap was non-empty and the existing head
    /// expires sooner, the in-flight timer is left alone — it'll fire
    /// first and the callback's re-arm will pick up the next head.
    pub fn register(
        self: *Deadlines,
        allocator: Allocator,
        t: *Timeout,
        ms_from_now: u64,
    ) !void {
        const current_ms = coarse.clockMsUseRarely(.monotonic);
        const ms = @max(ms_from_now, min_timeout_ms);
        t.deadline_ms = current_ms + @as(i64, @intCast(ms));

        // Snapshot the head BEFORE push so we know whether the new
        // entry will be the new minimum. A tie (equal deadline) is
        // *not* a new minimum — the existing timer fires at the same
        // moment and drainExpired sweeps both.
        const becomes_head = blk: {
            if (self.heap.peekMin()) |h| break :blk t.deadline_ms < h.deadline_ms;
            break :blk true;
        };

        try self.heap.push(allocator, t);

        if (becomes_head) self.armForHead(current_ms);
    }

    /// O(log N) middle removal. Safe even when the entry isn't
    /// enqueued (no-op). Hot path: called from an I/O completion
    /// callback when the op finished before the deadline fired.
    ///
    /// Doesn't reset the timer even when removing the current head —
    /// the in-flight SQE will fire harmlessly (drainExpired finds the
    /// new head's deadline still in the future and bails). The cost
    /// is one spurious wake-up; the win is no extra reset SQE per
    /// op-completes-first case (the common path).
    pub fn cancel(self: *Deadlines, t: *Timeout) void {
        self.heap.remove(t);
    }

    /// Walk from the top of the heap, firing every entry whose
    /// deadline has passed.
    pub fn drainExpired(self: *Deadlines, now: i64) void {
        while (self.heap.peekMin()) |t| {
            if (t.deadline_ms > now) break;
            _ = self.heap.popMin();
            if (t.done.swap(true, .acq_rel)) continue;
            if (t.callback) |cb| cb(t.callback_ctx.?);
        }
    }

    /// Arm (or reset) the timer for the current heap head. Caller
    /// guarantees `heap.peekMin() != null`.
    fn armForHead(self: *Deadlines, current_ms: i64) void {
        const head = self.heap.peekMin().?;
        const delta = head.deadline_ms - current_ms;
        const next_ms: u64 = if (delta < @as(i64, @intCast(min_timeout_ms)))
            min_timeout_ms
        else
            @intCast(delta);

        if (self.armed) {
            // reset cancels the in-flight SQE (callback fires with
            // error.Canceled) and queues a new one for `next_ms`.
            self.timer.reset(
                self.loop,
                &self.completion,
                &self.cancel_completion,
                next_ms,
                Deadlines,
                self,
                onTimer,
            );
        } else {
            self.timer.run(
                self.loop,
                &self.completion,
                next_ms,
                Deadlines,
                self,
                onTimer,
            );
            self.armed = true;
        }
    }
};

fn onTimer(
    ud: ?*Deadlines,
    _: *xev.Loop,
    _: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    const self = ud.?;
    _ = result catch |err| switch (err) {
        // A `reset` fires the old SQE with Canceled. The reset has
        // already queued a fresh run on `self.completion`; just bow
        // out and let that one call us back later.
        error.Canceled => return .disarm,
        // Unexpected: nothing to do, drop the tick. The next register
        // will re-arm.
        error.Unexpected => {
            self.armed = false;
            return .disarm;
        },
    };

    // Normal expiration: drain everything whose deadline has passed,
    // then re-arm for the next entry if any remain. Same monotonic
    // source as `register` — see its comment.
    const now = coarse.clockMsUseRarely(.monotonic);
    self.drainExpired(now);
    self.armed = false;
    if (self.heap.peekMin() != null) self.armForHead(now);
    return .disarm;
}
