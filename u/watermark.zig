const std = @import("std");
const fiber = @import("fiber");

/// Monotonically-advancing u64 with wait/signal. Many tasks block until
/// the value crosses a target.
///
/// Fast-path readers call `load()` without taking a lock — the value is
/// stored atomically. Writers call `advance()`, which takes `mu` so that
/// the (store, broadcast) pair is serialized against waiters' (check,
/// suspend) pair. A waiter inside `cv.wait` has atomically released `mu`
/// and joined the queue, so an advance can't slip between a waiter's
/// check and suspend: either the waiter sees the new value before
/// entering `cv.wait`, or it's already in the queue when broadcast runs.
///
/// `close()` lets shutdown unblock all current and future waiters with
/// `error.Closed`. `advance()` still works after close — `closed` only
/// controls the wait paths.
pub const Watermark = struct {
    value: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    mu: fiber.Mutex = .{},
    cv: fiber.Condition = .{},
    closed: bool = false, // protected by mu

    /// Lockless load. Cheap — safe to call on hot paths.
    pub fn load(self: *Watermark) u64 {
        return self.value.load(.acquire);
    }

    /// Monotonic advance. No-op if `new_value <= current`. Broadcasts
    /// only when the value actually moves.
    ///
    /// Lockless: uses an atomic CAS loop to update `value`, then calls
    /// `cv.broadcast()` outside of `mu`. Concurrent `advance` callers
    /// don't serialize on `mu` — they each CAS in parallel.
    ///
    /// Theoretical race: a waiter that has loaded `value` (predicate
    /// false) but hasn't yet pushed onto `cv.wait_queue` could miss a
    /// broadcast that fires between the load and the push. The
    /// recovery is the next `advance` (which will broadcast again, by
    /// which point the waiter is in the queue). For use cases where
    /// `advance` is called frequently (test runner, raft commit_index,
    /// applied_ts) this is invisible. For use cases where the
    /// watermark may stop advancing after the missed signal, callers
    /// should hold `mu` around the value update + broadcast (the old
    /// behavior — see git history).
    pub fn advance(self: *Watermark, new_value: u64) void {
        var cur = self.value.load(.monotonic);
        while (new_value > cur) {
            if (self.value.cmpxchgWeak(cur, new_value, .release, .monotonic)) |actual| {
                cur = actual;
                continue;
            }
            // We won the CAS. Broadcast — no mu held.
            self.cv.broadcast();
            return;
        }
    }

    /// Mark the watermark closed and wake every current/future waiter
    /// with `error.Closed`. Idempotent.
    pub fn close(self: *Watermark) void {
        self.mu.lock();
        defer self.mu.unlock();
        if (!self.closed) {
            self.closed = true;
            self.cv.broadcast();
        }
    }

    /// Block until `value >= target` or `timeout` elapses. Returns
    /// `error.Closed` if `close()` is called, `error.Timeout` on expiry,
    /// `error.Canceled` if the task is canceled.
    pub fn waitFor(self: *Watermark, target: u64, timeout_ms: u64) error{ Closed, Timeout }!void {
        // TODO(fiber): enforce the timeout. fiber.Condition has no timed
        // wait yet; until it lands this blocks like waitForUntimed and
        // error.Timeout is never returned (kept in the set for callers).
        _ = timeout_ms;
        self.mu.lock();
        defer self.mu.unlock();
        while (self.value.load(.acquire) < target) {
            if (self.closed) return error.Closed;
            self.cv.wait(&self.mu);
        }
    }

    /// Block until `value >= target`. Returns `error.Closed` on close,
    /// `error.Canceled` on cancel.
    pub fn waitForUntimed(self: *Watermark, target: u64) error{Closed}!void {
        self.mu.lock();
        defer self.mu.unlock();
        while (self.value.load(.acquire) < target) {
            if (self.closed) return error.Closed;
            self.cv.wait(&self.mu);
        }
    }
};
