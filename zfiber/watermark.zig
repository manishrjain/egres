// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Monotonically-advancing u64 with targeted wait/wake.
//!
//! Built directly on `Spinlock` + a centralized `ArrayList(Entry)`
//! held by the Watermark itself. Walks on `advance` are
//! cache-coherent (contiguous array of 16-byte entries) rather
//! than chasing pointers across fiber stacks.
//!
//! `advance` walks the entries, swap-removes those whose target
//! is satisfied, and wakes only those fibers — higher-target
//! waiters stay parked. No spurious wake / re-park churn and no
//! needless cross-thread routing.
//!
//! Lost-wake race is closed by the spinlock + re-check pattern: a
//! waiter re-reads `value` and `closed` under `sl` before
//! appending its entry; the fiber's awaken-bit handles wake
//! arriving in the sl.unlock → park gap.

const std = @import("std");
const _fb = @import("fiber.zig");
const _rt = @import("runtime.zig");
const _dd = @import("deadlines.zig");

const Fiber = _fb.Fiber;
const Spinlock = _rt.Spinlock;
const Allocator = std.mem.Allocator;

const Entry = struct {
    fiber: *Fiber,
    target: u64,
    /// Set to true if this entry was removed from waiters. Avoids an O(N) loop
    /// for defer removal.
    removed: bool = false,
    /// Set by `reset` before the wake: the value was set under this waiter.
    reset: bool = false,
};

// Timed waits arm the waiter fiber's OWN `Fiber.timeout` (via
// `deadlines.armSelf`) and detect a fire through the `Fiber.timed_out`
// generation — no per-wait stack `Timeout`/ctx, so a late fire can't dangle.

pub const Watermark = struct {
    allocator: Allocator,
    value: std.atomic.Value(u64) = .init(0),
    closed: std.atomic.Value(bool) = .init(false),
    sl: Spinlock = .{},
    waiters: std.ArrayList(*Entry) = .empty,

    pub fn init(allocator: Allocator) Watermark {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Watermark) void {
        self.waiters.deinit(self.allocator);
    }

    /// Lockless load. Cheap — safe on hot paths.
    pub fn load(self: *const Watermark) u64 {
        return self.value.load(.acquire);
    }

    /// Monotonic advance. No-op if `new_value <= current`. On a
    /// successful CAS, walks the waiter array and wakes exactly
    /// those whose target is now satisfied — higher-target
    /// waiters stay parked.
    pub fn advance(self: *Watermark, new_value: u64) void {
        var cur = self.value.load(.monotonic);
        while (new_value > cur) {
            if (self.value.cmpxchgWeak(cur, new_value, .release, .monotonic)) |actual| {
                cur = actual;
                continue;
            }
            self.wakeReached(new_value);
            return;
        }
    }

    /// Raise the value by `count` and wake the waiters it reaches:
    /// for a watermark that counts things done, by fibers that each
    /// know only that theirs is.
    pub fn add(self: *Watermark, count: u64) void {
        const now = self.value.fetchAdd(count, .release) + count;
        self.wakeReached(now);
    }

    /// Mark the watermark closed and wake every waiter with
    /// `error.Closed`. Idempotent. `advance` still works after
    /// close — `closed` only affects the wait paths.
    pub fn close(self: *Watermark) void {
        const was_closed = self.closed.swap(true, .release);
        if (!was_closed) self.wakeAll(.closed);
    }

    /// Set the value, lowering allowed: for a watermark of something
    /// that can go back, like a log index after a truncation. Every
    /// parked waiter wakes: a `waitForOrReset` caller with
    /// `error.Reset`, so it can re-check what it waits for; a plain
    /// `waitFor`/`waitForUntimed` caller re-parks for its target
    /// (`waitFor` re-arms its full timeout) and never sees it.
    pub fn reset(self: *Watermark, new_value: u64) void {
        self.value.store(new_value, .release);
        self.wakeAll(.reset);
    }

    /// Block until `value >= target` or the watermark is closed.
    /// Returns `error.Closed` if `close()` was called or
    /// `error.OutOfMemory` if the waiter table can't grow.
    pub fn waitForUntimed(self: *Watermark, target: u64) error{ Closed, OutOfMemory }!void {
        while (true) {
            return self.waitForImpl(target, null) catch |err| switch (err) {
                error.Closed => error.Closed,
                error.OutOfMemory => error.OutOfMemory,
                error.Reset => continue,
                // A null timeout never arms a deadline, so it can't time out.
                error.Timeout => unreachable,
            };
        }
    }

    /// Like `waitForUntimed`, but gives up after `timeout_ms` with
    /// `error.Timeout`.
    pub fn waitFor(self: *Watermark, target: u64, timeout_ms: u64) error{ Closed, Timeout, OutOfMemory }!void {
        while (true) {
            return self.waitForImpl(target, timeout_ms) catch |err| switch (err) {
                error.Reset => continue,
                else => |e| e,
            };
        }
    }

    /// `waitForUntimed` that reports a `reset` instead of re-parking:
    /// `error.Reset` means the value was set, possibly lowered, while
    /// waiting, and the caller decides what to wait for now.
    pub fn waitForOrReset(self: *Watermark, target: u64) error{ Closed, OutOfMemory, Reset }!void {
        return self.waitForImpl(target, null) catch |err| switch (err) {
            error.Timeout => unreachable,
            else => |e| e,
        };
    }

    /// Shared wait body. `timeout_ms == null` is the untimed wait (no `Deadlines`
    /// entry); a value arms a per-worker deadline that fires, bumps `timed_out`,
    /// and wakes us. Enqueues ONCE and parks ONCE: a parked waiter has exactly
    /// four legitimate wakers — `advance` (sets `value >= target`), `close`
    /// (sets `closed`), `reset` (sets the entry's `reset`), and the deadline
    /// (bumps `timed_out`) — each setting its condition BEFORE the wake. So on
    /// resume one must hold; a wake with none is a spurious wake and panics (a
    /// park must only return for a real reason).
    fn waitForImpl(self: *Watermark, target: u64, timeout_ms: ?u64) error{ Closed, Timeout, OutOfMemory, Reset }!void {
        // Fast paths — skip arming a timer when the answer is already known.
        if (self.value.load(.acquire) >= target) return;
        if (self.closed.load(.acquire)) return error.Closed;

        const me = _rt.currentFiber();

        // `armSelf` captures the owning `Deadlines` so `disarmSelf` hits the
        // same one even after a work-stealing migration.
        const owner: ?*_dd.Deadlines = if (timeout_ms) |ms| try _dd.armSelf(me, ms) else null;
        defer if (owner) |o| _dd.disarmSelf(me, o);

        if (timeout_ms == null) {
            me.timeout.reset();
        }

        // Enqueue ONCE under `sl`, re-checking the predicate to close the
        // lost-wake race: an advance/close between the fast-path check and the
        // enqueue takes `sl`, so it is either seen here (we return without
        // parking) or finds our entry (and wakes us).
        self.sl.lock();
        if (self.value.load(.acquire) >= target) {
            self.sl.unlock();
            return;
        }
        if (self.closed.load(.acquire)) {
            self.sl.unlock();
            return error.Closed;
        }
        var entry = Entry{
            .fiber = me,
            .target = target,
            .removed = false,
        };
        self.waiters.append(self.allocator, &entry) catch |err| {
            self.sl.unlock();
            return err;
        };
        self.sl.unlock();

        defer if (!entry.removed) {
            self.sl.lock();
            self.removeWaiter(me);
            self.sl.unlock();
        };

        _rt.park();

        // Resumed — exactly one waker condition must hold (see the doc comment).
        // Prefer a satisfied/closed result over Timeout if both landed.
        if (self.value.load(.acquire) >= target) return;
        if (self.closed.load(.acquire)) return error.Closed;
        if (entry.reset) return error.Reset;
        if (owner != null and me.timeout.didFire()) return error.Timeout;
        std.debug.panic(
            "watermark.waitFor: woken with value={d} < target={d}, not closed, not timed out — spurious wake",
            .{ self.value.load(.acquire), target },
        );
    }

    /// Remove this fiber's entry from the waiter array if present. Caller
    /// holds `sl`. Used by the timed `waitFor` on the timeout path — the
    /// deadline wakes us but doesn't touch the array.
    fn removeWaiter(self: *Watermark, f: *Fiber) void {
        for (self.waiters.items, 0..) |e, i| {
            if (e.fiber == f) {
                e.removed = true;
                _ = self.waiters.swapRemove(i);
                return;
            }
        }
    }

    /// Walk the waiter array, swap-removing entries whose target
    /// is at or below `new_value`. Collected fibers are linked
    /// via the transient `waker_next` field and woken after sl
    /// is released — cross-thread wake may push into a worker ring
    /// + issue an eventfd notify, neither of which belongs under
    /// our spinlock.
    fn wakeReached(self: *Watermark, new_value: u64) void {
        self.sl.lock();
        var to_wake: ?*Fiber = null;
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            const e = self.waiters.items[i];
            if (e.target <= new_value) {
                // For a timed waiter, win its first-fire token before
                // waking. If the deadline already claimed it, leave the
                // entry — the deadline owns the wake and the waiter will
                // self-remove on resume.
                if (!e.fiber.winRace()) {
                    i += 1;
                    continue;
                }
                e.fiber.waker_next = to_wake;
                to_wake = e.fiber;
                _ = self.waiters.swapRemove(i);
                e.removed = true;
                // Don't increment i — the entry now at slot i is
                // whatever the last element was; we still need
                // to check it.
            } else {
                i += 1;
            }
        }
        self.sl.unlock();

        while (to_wake) |f| {
            const next = f.waker_next;
            f.waker_next = null;
            _rt.wake(f);
            to_wake = next;
        }
    }

    /// Wake every queued waiter, for `close` (they see `closed` when
    /// they re-check and return `error.Closed`) or `reset` (their entry
    /// is marked, and they return `error.Reset` or re-park).
    /// Mirrors `wakeReached`'s first-fire claim: a timed waiter whose
    /// deadline already fired is left for that deadline to wake.
    fn wakeAll(self: *Watermark, why: enum { closed, reset }) void {
        self.sl.lock();
        var to_wake: ?*Fiber = null;
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            const e = self.waiters.items[i];
            // Win the first-fire token (no-op claim for an untimed waiter); if
            // the deadline already claimed it, leave the entry for it.
            if (!e.fiber.winRace()) {
                i += 1;
                continue;
            }
            if (why == .reset) e.reset = true;
            e.fiber.waker_next = to_wake;
            to_wake = e.fiber;
            _ = self.waiters.swapRemove(i);
            e.removed = true;
        }
        self.sl.unlock();

        while (to_wake) |f| {
            const next = f.waker_next;
            f.waker_next = null;
            _rt.wake(f);
            to_wake = next;
        }
    }
};
