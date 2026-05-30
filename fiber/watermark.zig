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
const fb = @import("fiber.zig");
const rt = @import("runtime.zig");

const Fiber = fb.Fiber;
const Spinlock = rt.Spinlock;
const Allocator = std.mem.Allocator;

const Entry = struct {
    fiber: *Fiber,
    target: u64,
};

pub const Watermark = struct {
    allocator: Allocator,
    value: std.atomic.Value(u64) = .init(0),
    closed: std.atomic.Value(bool) = .init(false),
    sl: Spinlock = .{},
    waiters: std.ArrayList(Entry) = .empty,

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

    /// Mark the watermark closed and wake every waiter with
    /// `error.Closed`. Idempotent. `advance` still works after
    /// close — `closed` only affects the wait paths.
    pub fn close(self: *Watermark) void {
        const was_closed = self.closed.swap(true, .release);
        if (!was_closed) self.wakeAll();
    }

    /// Block until `value >= target` or the watermark is closed.
    /// Returns `error.Closed` if `close()` was called or
    /// `error.OutOfMemory` if the waiter table can't grow.
    pub fn waitFor(self: *Watermark, target: u64) error{ Closed, OutOfMemory }!void {
        while (true) {
            // Fast path — predicate or close already observable.
            if (self.value.load(.acquire) >= target) return;
            if (self.closed.load(.acquire)) return error.Closed;

            self.sl.lock();
            // Re-check under sl to close the race between
            // predicate-load and entry-insert.
            if (self.value.load(.acquire) >= target) {
                self.sl.unlock();
                return;
            }
            if (self.closed.load(.acquire)) {
                self.sl.unlock();
                return error.Closed;
            }
            self.waiters.append(self.allocator, .{
                .fiber = rt.currentFiber(),
                .target = target,
            }) catch |err| {
                self.sl.unlock();
                return err;
            };
            self.sl.unlock();
            rt.park();
            // On resume: the waker swap-removed our entry from
            // the array. Loop and re-check predicate / closed.
        }
    }

    /// Walk the waiter array, swap-removing entries whose target
    /// is at or below `new_value`. Collected fibers are linked
    /// via the transient `waker_next` field and woken after sl
    /// is released — `wake` → `scheduleLocal` may itself grab the
    /// global queue spinlock; nesting under ours would serialize
    /// each wake through one critical section.
    fn wakeReached(self: *Watermark, new_value: u64) void {
        self.sl.lock();
        var to_wake: ?*Fiber = null;
        var i: usize = 0;
        while (i < self.waiters.items.len) {
            const e = self.waiters.items[i];
            if (e.target <= new_value) {
                e.fiber.waker_next = to_wake;
                to_wake = e.fiber;
                _ = self.waiters.swapRemove(i);
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
            rt.wake(f);
            to_wake = next;
        }
    }

    /// Wake every queued waiter (used by `close`). They'll see
    /// `closed == true` when they re-check and return
    /// `error.Closed`.
    fn wakeAll(self: *Watermark) void {
        self.sl.lock();
        var to_wake: ?*Fiber = null;
        for (self.waiters.items) |e| {
            e.fiber.waker_next = to_wake;
            to_wake = e.fiber;
        }
        self.waiters.clearRetainingCapacity();
        self.sl.unlock();

        while (to_wake) |f| {
            const next = f.waker_next;
            f.waker_next = null;
            rt.wake(f);
            to_wake = next;
        }
    }
};
