// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Per-worker timeout machinery: a flat array of pending deadlines plus a
//! growable pool of `xev.Timer` completions, one always armed for the earliest
//! deadline.
//!
//! To fire earlier we NEVER `timer.reset` the in-flight completion (that path —
//! cancel + re-arm the SAME completion via `op.reset` — had a race that could
//! strand the timer). Instead `armForHead` always runs a FRESH node from the
//! pool for the new head and cancels the superseded one. The pool grows on
//! demand, so the head is always armed even when many ever-earlier deadlines
//! register before a single loop tick.
//!
//! Each `Worker` owns one `Deadlines` instance. Timeouts registered on worker N
//! live in worker N's array, and the xev timers are only ever armed/canceled on
//! worker N's thread — libxev per-loop state is not thread-safe, so
//! `register`/`drainExpired`/`onTimerNode`/`armForHead` stay owner-thread-only.
//!
//! `cancel`, however, IS safe to call from any worker: a fiber that registered a
//! timeout here can migrate and complete on a different worker, then cancel from
//! there. The array is guarded by `sl` (a spinlock) on every access — the only
//! cross-thread mutation is `cancel`'s removal; the timer SQE ops stay off the
//! lock and on the owner thread.

const std = @import("std");
const log = std.log.scoped(.fiber);
const xev = @import("xev");
const _heap = @import("heap.zig");
const _coarse = @import("utils").coarse_clock;
// `sleepMs` (bottom of file) is the one fiber-aware helper here, so we import
// the runtime for park/wake. The `Deadlines` struct itself stays oblivious to
// fibers — this is a mutual file import, which Zig resolves lazily (runtime.zig
// imports us back for the `Deadlines` type).
const _rt = @import("runtime.zig");
const _fb = @import("fiber.zig");
const Fiber = _fb.Fiber;

const Spinlock = @import("spinlock.zig").Spinlock;
const Allocator = std.mem.Allocator;

/// Minimum honored timeout. Requests below this are clamped up. The
/// clock cache that consumers measure against has 1 ms granularity,
/// so sub-ms timeouts would either round to zero or fire immediately
/// — neither useful. Pin the floor at the boundary instead.
pub const min_timeout_ms: u64 = 1;

/// The deadline heap entry. Each `Fiber` OWNS one (`Fiber.timeout`) so a fire
/// that arrives after its op exited dereferences stable fiber memory and can
/// only wake that one fiber — never an unrelated one via a reused stack frame.
/// Definition lives in `fiber.zig` (the fiber owns it); see `Timeout` there.
/// When the deadline fires (before `cancel`), the owner worker invokes
/// `on_fire(on_fire_ctx)` exactly once (gated by `_done`).
pub const Timeout = _fb.Timeout;

/// One reusable timer SQE. `completion` must stay put (io_uring holds its
/// address until the CQE), so nodes are heap-allocated and never moved. A node
/// is either in flight (armed for a deadline) or cached in `free_nodes`.
const TimerNode = struct {
    completion: xev.Completion = .{},
    parent: *Deadlines = undefined,
    deadline_ms: i64 = 0,
};

/// Cap on cached idle nodes. A burst can allocate more than this transiently;
/// as those timers fire, the pool caches up to this many for reuse and frees
/// the rest — so a spike doesn't inflate memory forever.
const max_free_nodes = 16;

pub const Deadlines = struct {
    loop: *xev.Loop,
    timer: xev.Timer,
    /// Backs the `timeouts` array — grown by `register`, freed by `deinit`.
    /// Captured at init (the runtime's allocator) so callers never pass one and
    /// can't mismatch grow vs. free.
    allocator: Allocator,
    /// The node armed for the current earliest deadline (the head), or null.
    /// Superseded by `armForHead` when an earlier deadline arrives.
    active: ?*TimerNode = null,
    /// Cache of idle timer completions for reuse, capped at `max_free_nodes`.
    /// To fire earlier we NEVER reset the in-flight timer (that reset —
    /// cancel-then-re-arm the SAME completion via `op.reset` — had a race that
    /// could strand it). Instead we always `timer.run` a fresh node and
    /// `cancel` the superseded one so it recycles here fast. A burst of
    /// ever-earlier deadlines allocates beyond the cap transiently; the excess
    /// is freed as it recycles, so memory doesn't stay inflated.
    free_nodes: std.ArrayList(*TimerNode) = .empty,
    /// SQE for the `timer.cancel` of a superseded node — one in flight at a
    /// time. When busy, the superseded node just lingers to its own deadline
    /// (still correct; the fresh node covers the head), then recycles.
    cancel_completion: xev.Completion = .{},
    /// Pending timeouts as a flat array of pointers — NOT the `MinHeap`. We scan
    /// it (O(N), N tiny) for the min deadline and for removal. Removal matches by
    /// pointer identity.
    timeouts: std.ArrayList(*Timeout) = .empty,
    /// Guards every access to `timeouts`. The only cross-thread caller is
    /// `cancel` (a migrated waiter removing its own timeout); the owner thread
    /// holds it for the brief mutations in `register`/`drainExpired`/`armForHead`.
    /// Uncontended on the hot path.
    sl: Spinlock = .{},

    pub fn init(loop: *xev.Loop, allocator: Allocator) !Deadlines {
        var self: Deadlines = .{
            .loop = loop,
            .timer = try xev.Timer.init(),
            .allocator = allocator,
        };
        // Preallocate the cap so `recycleNode` never has to grow (or fail).
        try self.free_nodes.ensureTotalCapacity(allocator, max_free_nodes);
        return self;
    }

    pub fn deinit(self: *Deadlines) void {
        // Frees the cached (idle) nodes only. A node still armed at deinit is
        // NOT freed here — it's abandoned. That is safe (no UAF): the run loop
        // has stopped, so nobody reaps its completion, and the kernel writes
        // CQEs to its own ring, never into this memory. The cost is a bounded
        // one-time leak (~the current head timer) at teardown — acceptable for
        // process shutdown; revisit with an owning node list if a leak-checked
        // runtime tears down with live timers.
        for (self.free_nodes.items) |n| self.allocator.destroy(n);
        self.free_nodes.deinit(self.allocator);
        self.timeouts.deinit(self.allocator);
        self.timer.deinit();
    }

    /// Earliest pending deadline, or null if none.
    pub fn minDeadline(self: *Deadlines) ?i64 {
        self.sl.lock();
        defer self.sl.unlock();
        return self.minDeadlineLocked();
    }

    /// Earliest pending deadline, or null if none. Caller holds `sl`.
    fn minDeadlineLocked(self: *const Deadlines) ?i64 {
        var m: ?i64 = null;
        for (self.timeouts.items) |e| {
            if (m == null or e._deadline_ms < m.?) m = e._deadline_ms;
        }
        return m;
    }

    /// Register a deadline. `ms_from_now` is clamped up to `min_timeout_ms`.
    /// Appends `t` to the `timeouts` array (the caller owns `t`'s storage).
    ///
    /// "Now" is read via `timestampNow(.monotonic, .millis)` — direct vDSO
    /// CLOCK_MONOTONIC, no caching. Using `loop.now()` would hand us
    /// libxev's cached value, which can be arbitrarily stale between
    /// `loop.run` iterations under heavy worker load; that stale
    /// baseline could let a fire.s drainExpired see both a short and
    /// a long deadline "expired" in one sweep when only the short fired.
    ///
    /// Re-arms the xev timer iff this entry becomes the new earliest
    /// deadline. If the heap was non-empty and the existing head
    /// expires sooner, the in-flight timer is left alone — it'll fire
    /// first and the callback's re-arm will pick up the next head.
    pub fn register(
        self: *Deadlines,
        t: *Timeout,
        ms_from_now: u64,
    ) !void {
        const current_ms = _coarse.timestampNow(.monotonic, .millis);
        const ms = @max(ms_from_now, min_timeout_ms);
        t._deadline_ms = current_ms + @as(i64, @intCast(ms));

        // Snapshot the min BEFORE append so we know whether the new entry is the
        // new earliest. A tie is *not* a new minimum — the existing timer fires
        // at the same moment and drainExpired sweeps both.
        self.sl.lock();
        if (std.debug.runtime_safety) {
            // Leak tripwire: a `Timeout` must never be registered twice (a
            // missing cancel). With the array this is an O(N) scan, debug-only.
            for (self.timeouts.items) |e| if (e == t) std.debug.panic(
                "deadline register: timeout 0x{x} already registered (missing cancel)",
                .{@intFromPtr(t)},
            );
        }
        const prev_min = self.minDeadlineLocked();
        const becomes_head = prev_min == null or t._deadline_ms < prev_min.?;
        self.timeouts.append(self.allocator, t) catch |err| {
            self.sl.unlock();
            return err;
        };
        self.sl.unlock();

        // Arm outside the lock — `register` only ever runs on the owner
        // thread, so the timer SQE op is safe; keeping it off `sl` avoids
        // pinning a cross-thread `cancel` behind a libxev call.
        if (becomes_head) self.armForHead(current_ms);
    }

    /// O(N) removal by POINTER IDENTITY. Safe even when `t` isn't enqueued
    /// (no-op). Hot path: called from an I/O completion / wait exit when the op
    /// finished before the deadline fired.
    ///
    /// The reason we put spinlock here is to allow for cancel call by fibers
    /// who have moved away to another worker.
    ///
    /// Doesn't reset the timer even when removing the current earliest — the
    /// in-flight SQE fires harmlessly (drainExpired finds the new min still in
    /// the future and bails). One spurious timer tick; no extra reset SQE per
    /// op-completes-first case (the common path).
    pub fn cancel(self: *Deadlines, t: *Timeout) void {
        self.sl.lock();
        defer self.sl.unlock();
        for (self.timeouts.items, 0..) |e, i| {
            if (e == t) {
                _ = self.timeouts.swapRemove(i);
                return;
            }
        }
    }

    /// Fire every entry whose deadline has passed. Owner-thread only.
    ///
    /// Drains the whole expired prefix under a SINGLE `sl` acquisition,
    /// threading the timeouts we claim onto a local fire list, then
    /// invokes their callbacks after unlock. Two reasons not to fire under
    /// the lock: a callback may `wake` (which can route cross-thread —
    /// remote queue + eventfd notify), and a concurrent `cancel` would
    /// otherwise contend on every single expiry instead of once per tick.
    pub fn drainExpired(self: *Deadlines, now: i64) void {
        var fire_list: ?*Timeout = null;
        self.sl.lock();
        var i: usize = 0;
        while (i < self.timeouts.items.len) {
            const t = self.timeouts.items[i];
            if (t._deadline_ms <= now) {
                // swap-remove: the last entry moves into slot i, so DON'T
                // advance i — re-check the element now sitting here.
                _ = self.timeouts.swapRemove(i);
                // Loser (op completed first, already claimed) is dropped — that
                // path owns the cleanup; we just needed it off the array.
                if (t.winRace()) {
                    t._wake_next = fire_list;
                    fire_list = t;
                }
            } else {
                i += 1;
            }
        }
        self.sl.unlock();

        while (fire_list) |t| {
            // Read `next` before firing: an `on_fire` that wakes a fiber may
            // see that fiber free its frame (and thus `t`) the instant it
            // resumes, so touch nothing on `t` after the callback.
            const next = t._wake_next;
            t._wake_next = null;
            t.on_fire(t.on_fire_ctx);
            fire_list = next;
        }
    }

    /// Take a cached idle node, or allocate a fresh one.
    fn acquireNode(self: *Deadlines) !*TimerNode {
        if (self.free_nodes.pop()) |n| return n;
        const n = try self.allocator.create(TimerNode);
        n.* = .{ .parent = self };
        return n;
    }

    /// Return a fired/canceled node. Cache it for reuse (its completion is
    /// done, so reset it), or free it if the cache is already full — so a
    /// burst that allocated many nodes doesn't retain them all.
    fn recycleNode(self: *Deadlines, node: *TimerNode) void {
        if (self.free_nodes.items.len < max_free_nodes) {
            node.completion = .{};
            self.free_nodes.appendAssumeCapacity(node);
        } else {
            self.allocator.destroy(node);
        }
    }

    /// Ensure a timer fires at or before the earliest pending deadline.
    /// Owner-thread only. Always `timer.run`s a fresh node for the head (never
    /// `reset`) and cancels the superseded one so it recycles promptly. The
    /// growable pool guarantees a node is always available, so the head is
    /// never left uncovered. No-op when the active node already covers the
    /// head, or when there are no timeouts.
    fn armForHead(self: *Deadlines, current_ms: i64) void {
        // The sim loop drains by its own clock (`Sim.run`): no timer.
        if (_rt.isSim()) return;

        self.sl.lock();
        const head = self.minDeadlineLocked();
        self.sl.unlock();
        const deadline = head orelse return;

        // Active node already fires at or before the head → nothing to do.
        if (self.active) |a| {
            if (a.deadline_ms <= deadline) return;
        }

        const delta = deadline - current_ms;
        const next_ms: u64 = if (delta < @as(i64, @intCast(min_timeout_ms)))
            min_timeout_ms
        else
            @intCast(delta);

        const node = self.acquireNode() catch {
            log.err("OOM arming timer; head deferred", .{});
            return;
        };
        node.deadline_ms = deadline;
        self.timer.run(self.loop, &node.completion, next_ms, TimerNode, node, onTimerNode);

        // Cancel the superseded node so it recycles now instead of lingering to
        // its own (later) deadline. Clean timer_remove — no `op.reset`, so no
        // re-arm race; its own callback fires Canceled and recycles it. Skipped
        // if a cancel is already in flight: that node just lingers (harmless —
        // the fresh node covers the head) and recycles on its natural fire.
        const superseded = self.active;
        self.active = node;
        if (superseded) |o| {
            if (self.cancel_completion.state() != .active) {
                self.timer.cancel(self.loop, &o.completion, &self.cancel_completion, Deadlines, self, onCancel);
            }
        }
    }
};

fn onCancel(_: ?*Deadlines, _: *xev.Loop, _: *xev.Completion, _: xev.CancelError!void) xev.CallbackAction {
    // The removed node's OWN callback (onTimerNode) recycles it; the remove
    // result itself is irrelevant (NOENT if the node already fired).
    return .disarm;
}

fn onTimerNode(
    ud: ?*TimerNode,
    _: *xev.Loop,
    _: *xev.Completion,
    result: xev.Timer.RunError!void,
) xev.CallbackAction {
    const node = ud.?;
    const self = node.parent;
    if (self.active == node) self.active = null;
    self.recycleNode(node);

    // Canceled (superseded, cancelled to recycle early) or Unexpected: nothing
    // expired here, and the head is covered by the node that superseded us (or
    // a future register re-arms). Don't drain / re-arm.
    _ = result catch return .disarm;

    const now = _coarse.timestampNow(.monotonic, .millis);
    self.drainExpired(now);
    // Re-arm for whatever remains (this node fired, so the head lost its timer).
    self.armForHead(now);
    return .disarm;
}

// ── Per-fiber owned timeout: fire / arm / disarm ────────────────────────
//
// A timed op arms THIS fiber's own `Fiber.timeout`, parks, and disarms on exit.
// Because the Timeout lives in the Fiber rather than on the op's stack, a fire
// that races the disarm can only ever wake this fiber — never an unrelated one
// through a reused frame.

/// `on_fire` for a fiber's own timeout: record the fire (bump the generation)
/// then wake the fiber. Runs on the owner worker (from `drainExpired`); `ctx`
/// is the `*Fiber`.
///
/// NOTE that the caller of this function should already have won the race
/// before calling this.
pub fn wonRaceAndTimedOut(ctx: *anyopaque) void {
    const f: *Fiber = @ptrCast(@alignCast(ctx));
    f.timeout._fired.store(true, .release);
    _rt.wake(f);
}

/// Arm fiber `f`'s own timeout `ms` from now on its current worker's heap.
/// Snapshot `f.timed_out` BEFORE calling to later detect the fire. Returns the
/// OWNING `Deadlines` so a migrated op disarms on the same heap. Asserts the
/// timeout isn't already armed — a leak tripwire: a fiber waits in one place at
/// a time, so a previous op must have disarmed.
pub fn armSelf(f: *Fiber, ms: u64) !*Deadlines {
    f.timeout.reset();
    f.timeout.on_fire = &wonRaceAndTimedOut;
    f.timeout.on_fire_ctx = f;
    const o = &_rt.currentWorker().deadlines;
    try o.register(&f.timeout, ms);
    return o;
}

/// Disarm fiber `f`'s timeout on `owner` (captured by `armSelf`). Claims first —
/// so a late `drainExpired` pop can't fire — then removes it from the heap.
pub fn disarmSelf(f: *Fiber, owner: *Deadlines) void {
    _ = f.timeout.winRace();
    owner.cancel(&f.timeout);
}

/// Park the calling fiber for `ms` milliseconds on the current worker's
/// `Deadlines` (one `xev.Timer` per worker, re-armed to the earliest pending
/// deadline) rather than arming a fresh libxev timer per call. `ms` is clamped
/// up to `min_timeout_ms` — to merely give up the CPU, use `yield`.
///
/// Parks EXACTLY ONCE and panics if resumed before our own deadline fired
/// (`timed_out` unchanged). `fiberTimedOut` bumps `timed_out` (release) before
/// it wakes us, so a resume from our own deadline is always observable here; any
/// other resume is an unexplained spurious wake. We refuse to paper over it with
/// a re-park — a park must only return for a real reason, so a violation is a
/// runtime bug surfaced loudly with an attributable stack.
pub fn sleepMs(ms: u64) void {
    const f = _rt.currentFiber();
    const owner = armSelf(f, ms) catch |err| {
        std.debug.panic("sleepMs: deadline register failed: {}", .{err});
    };
    defer disarmSelf(f, owner);
    _rt.park();
    if (!f.timeout.didFire()) std.debug.panic(
        "sleepMs: resumed before our own deadline fired — spurious wake",
        .{},
    );
}
