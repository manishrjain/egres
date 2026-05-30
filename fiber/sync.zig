//! Synchronization primitives. All built on the runtime's `park`/
//! `wake` mechanism; no kernel locking, no `std.Thread.Mutex`.
//!
//! Cross-thread correctness: each primitive owns its own
//! `Spinlock` covering the {state, waiters/waiter} composite —
//! short critical sections (a few pointer writes), no syscalls,
//! never held across a `park`. The spinlock + the fiber's
//! `awaken-bit` state machine together close the classic lost-
//! wakeup race: a wake firing between the slow-path "decide to
//! park" and the actual `coro.yield` lands as `awaken=true` on
//! the fiber's `state`, and `park`'s CAS aborts before yielding.
//!
//! Mutex policy: **hand-off**. When `unlock` finds a waiter it
//! transfers ownership directly rather than releasing the lock
//! byte to 0. The woken waiter holds the lock the moment its
//! `park` returns. Avoids spurious wakes (multi-thread concern)
//! and gives FIFO fairness for free.

const std = @import("std");
const fb = @import("fiber.zig");
const rt = @import("runtime.zig");

const Fiber = fb.Fiber;
const Spinlock = rt.Spinlock;

/// Intrusive doubly-linked list of `*Fiber`. Fibers store their
/// `waker_prev`/`waker_next` slots; a fiber can be in at most one
/// wait queue at a time, so a single pair of pointers suffices.
///
/// Not internally synchronized. Each consumer (Mutex, Notify, …)
/// owns the spinlock that guards its WaitQueue. Reason: composite
/// ops typically need to update both queue state AND a sibling
/// field (Mutex's lock byte, Notify's `pending` flag) atomically;
/// the lock belongs at the level that knows both.
pub const WaitQueue = struct {
    head: ?*Fiber = null,
    tail: ?*Fiber = null,

    pub fn isEmpty(self: *const WaitQueue) bool {
        return self.head == null;
    }

    pub fn pushTail(self: *WaitQueue, f: *Fiber) void {
        f.waker_prev = self.tail;
        f.waker_next = null;
        if (self.tail) |t| t.waker_next = f else self.head = f;
        self.tail = f;
    }

    pub fn popHead(self: *WaitQueue) ?*Fiber {
        const f = self.head orelse return null;
        self.head = f.waker_next;
        if (self.head) |h| h.waker_prev = null else self.tail = null;
        f.waker_prev = null;
        f.waker_next = null;
        return f;
    }

    /// O(1) middle removal given the fiber pointer. Precondition:
    /// `f` is currently enqueued in `self`. Caller is responsible
    /// for knowing that (typical pattern: a parked fiber being
    /// woken by timeout while still in a sync primitive's queue).
    pub fn remove(self: *WaitQueue, f: *Fiber) void {
        if (f.waker_prev) |p| p.waker_next = f.waker_next else self.head = f.waker_next;
        if (f.waker_next) |n| n.waker_prev = f.waker_prev else self.tail = f.waker_prev;
        f.waker_prev = null;
        f.waker_next = null;
    }
};

/// Cooperative mutex. Uncontended lock/unlock is one atomic CAS;
/// contended waiters park on the runtime, not on the kernel — the
/// worker keeps running other fibers in the meantime.
pub const Mutex = struct {
    /// 0 = unlocked, 1 = locked. Atomic. The fast paths
    /// (`tryLock`, uncontended `lock`/`unlock`) operate on this
    /// alone — no spinlock touch.
    state: std.atomic.Value(u32) = .init(0),
    /// Guards composite updates: {`state` transition, `waiters`
    /// mutation}. Held only during the slow path; uncontended
    /// callers bypass it entirely.
    sl: Spinlock = .{},
    waiters: WaitQueue = .{},

    pub fn tryLock(self: *Mutex) bool {
        return self.state.cmpxchgWeak(0, 1, .acquire, .monotonic) == null;
    }

    pub fn lock(self: *Mutex) void {
        if (self.tryLock()) return;

        // Slow path: serialize with a concurrent `unlock` via the
        // spinlock. The re-check under sl closes the lost-wakeup
        // race — if `unlock` ran between our fast-CAS-fail and
        // sl.lock, we'll see state==0 here and grab it directly.
        self.sl.lock();
        if (self.tryLock()) {
            self.sl.unlock();
            return;
        }
        const f = rt.currentFiber();
        self.waiters.pushTail(f);
        self.sl.unlock();
        rt.park();
        // Resumed by `unlock`'s hand-off: we now hold the lock,
        // `state` is still 1, and the critical section is ours.
    }

    pub fn unlock(self: *Mutex) void {
        self.sl.lock();
        const waiter = self.waiters.popHead();
        if (waiter == null) self.state.store(0, .release);
        self.sl.unlock();

        // Wake outside the spinlock: scheduleLocal acquires either a
        // Chase-Lev local queue (lock-free push) or the global queue
        // spinlock; either way it shouldn't be nested under sl.
        if (waiter) |w| rt.wake(w);
    }
};

/// Condition variable. Used together with a `Mutex` to wait for a
/// predicate to become true: caller holds the mutex, checks the
/// predicate, and if false calls `wait(mutex)`. `wait` atomically
/// releases the mutex and parks the fiber; on wake, it reacquires
/// the mutex before returning.
///
/// `signal` wakes one waiter; `broadcast` wakes all. Both are safe
/// to call with or without holding `mutex` — though the canonical
/// pattern is to signal under the mutex so the woken waiter sees
/// the updated predicate immediately.
///
/// Lost-wake protection: the fiber is added to `waiters` *before*
/// releasing the user's mutex. If `signal` fires in the gap between
/// "added to queue" and "park called", `wake` sets the fiber's
/// `awaken` bit on the `.ready` state; the subsequent `park` CAS
/// consumes the bit and returns without yielding. Same pattern as
/// the Mutex slow path.
pub const Condition = struct {
    sl: Spinlock = .{},
    waiters: WaitQueue = .{},

    pub fn wait(self: *Condition, mutex: *Mutex) void {
        const f = rt.currentFiber();

        // Add to wait queue *before* releasing the mutex so a
        // concurrent `signal` from the next mutex-holder can find
        // us. The signal/broadcast may fire before `park` below;
        // the fiber's awaken-bit handles that race.
        self.sl.lock();
        self.waiters.pushTail(f);
        self.sl.unlock();

        mutex.unlock();
        rt.park();
        mutex.lock();
    }

    pub fn signal(self: *Condition) void {
        self.sl.lock();
        const w = self.waiters.popHead();
        self.sl.unlock();
        if (w) |f| rt.wake(f);
    }

    /// Wake every fiber currently in the wait queue. We collect the
    /// fibers under `sl` (using their now-free `waker_next` field
    /// as a transient local-list link) then wake them outside the
    /// spinlock — wake calls scheduleLocal which may itself acquire
    /// the global queue spinlock; nesting under `sl` would serialize
    /// every waker through one critical section.
    pub fn broadcast(self: *Condition) void {
        self.sl.lock();
        var head: ?*Fiber = null;
        while (self.waiters.popHead()) |f| {
            // `popHead` cleared `waker_next`; reuse it for our
            // transient list. Safe because the fiber isn't in any
            // queue right now — nothing else touches the field.
            f.waker_next = head;
            head = f;
        }
        self.sl.unlock();

        while (head) |f| {
            const next = f.waker_next;
            f.waker_next = null;
            rt.wake(f);
            head = next;
        }
    }
};

/// Reader-writer lock with writer-preferred fairness.
///
/// Multiple readers may hold the lock simultaneously; exclusive
/// access blocks both new readers and other writers. A writer
/// waiting in line takes priority over arriving readers — once
/// any writer queues, new `lockShared` calls park rather than
/// joining the active readers. Prevents writer starvation under
/// sustained read load.
///
/// API mirrors zio's `RWMutex`: `lock` / `unlock` / `tryLock` are
/// the exclusive variants; `lockShared` / `unlockShared` /
/// `tryLockShared` are the shared variants.
///
/// Implementation: single spinlock guards the composite
/// {readers, writer, writers_pending, both queues}; readers and
/// writers park on separate WaitQueues so wake can target the
/// right class without the "broadcast or wrong-class wakeup"
/// hazard from a mixed queue.
pub const RWMutex = struct {
    sl: Spinlock = .{},

    /// Active reader count. Mutated only under sl.
    readers: u32 = 0,
    /// True iff a writer currently holds the lock exclusively.
    writer: bool = false,
    /// Count of writers currently in `writers_wq`. New
    /// `lockShared` calls defer when this is > 0 — that's where
    /// writer preference is enforced.
    writers_pending: u32 = 0,

    readers_wq: WaitQueue = .{},
    writers_wq: WaitQueue = .{},

    // ---- shared (read) ----

    pub fn tryLockShared(self: *RWMutex) bool {
        self.sl.lock();
        defer self.sl.unlock();
        if (self.writer or self.writers_pending > 0) return false;
        self.readers += 1;
        return true;
    }

    pub fn lockShared(self: *RWMutex) void {
        const me = rt.currentFiber();
        while (true) {
            self.sl.lock();
            if (!self.writer and self.writers_pending == 0) {
                self.readers += 1;
                self.sl.unlock();
                return;
            }
            self.readers_wq.pushTail(me);
            self.sl.unlock();
            rt.park();
        }
    }

    pub fn unlockShared(self: *RWMutex) void {
        self.sl.lock();
        std.debug.assert(self.readers > 0);
        self.readers -= 1;
        // When the last reader leaves, wake one writer if any are
        // pending. Targeted wake from `writers_wq` — no mixed-
        // queue class confusion.
        if (self.readers == 0 and self.writers_pending > 0) {
            const w = self.writers_wq.popHead();
            self.sl.unlock();
            if (w) |waiter| rt.wake(waiter);
            return;
        }
        self.sl.unlock();
    }

    // ---- exclusive (write) ----

    pub fn tryLock(self: *RWMutex) bool {
        self.sl.lock();
        defer self.sl.unlock();
        if (self.writer or self.readers > 0) return false;
        self.writer = true;
        return true;
    }

    pub fn lock(self: *RWMutex) void {
        const me = rt.currentFiber();

        self.sl.lock();
        self.writers_pending += 1;
        while (self.readers > 0 or self.writer) {
            self.writers_wq.pushTail(me);
            self.sl.unlock();
            rt.park();
            self.sl.lock();
        }
        // It's important to keep this writers_pending counter and have the just
        // woken up writer decrement it. Between the time my waker pops me from
        // the writers_wq and writer_pending -= 1, a reader can't sneak in and
        // acquire the shared lock, pushing the writer back to park. The lock
        // executes a clear preference for writers.
        self.writers_pending -= 1;
        self.writer = true;
        self.sl.unlock();
    }

    pub fn unlock(self: *RWMutex) void {
        self.sl.lock();
        std.debug.assert(self.writer);
        self.writer = false;

        // Writer-preferred: if any writer is queued, hand off
        // exclusively. Otherwise wake every queued reader so they
        // can all acquire concurrently.
        if (self.writers_pending > 0) {
            const w = self.writers_wq.popHead();
            self.sl.unlock();
            if (w) |waiter| rt.wake(waiter);
            return;
        }

        // Wake all readers. Same transient-list trick as
        // `Condition.broadcast`: collect under sl using
        // `waker_next`, wake outside.
        var to_wake: ?*Fiber = null;
        while (self.readers_wq.popHead()) |f| {
            f.waker_next = to_wake;
            to_wake = f;
        }
        self.sl.unlock();

        while (to_wake) |f| {
            const next = f.waker_next;
            f.waker_next = null;
            rt.wake(f);
            to_wake = next;
        }
    }
};

/// Single-shot event. `wait` parks until `notify` fires; if `notify`
/// fires first (no waiter present), the next `wait` returns
/// immediately and clears the pending flag.
///
/// One waiter at a time. Two fibers calling `wait` concurrently is
/// undefined; build on top of `WaitQueue` directly if you need a
/// multi-waiter event.
pub const Notify = struct {
    sl: Spinlock = .{},
    waiter: ?*Fiber = null,
    // pending ensures that if wait arrives before notify, it won't deadlock.
    pending: bool = false,

    pub fn wait(self: *Notify) void {
        self.sl.lock();
        if (self.pending) {
            self.pending = false;
            self.sl.unlock();
            return;
        }
        std.debug.assert(self.waiter == null);
        self.waiter = rt.currentFiber();
        self.sl.unlock();
        rt.park();
    }

    pub fn notify(self: *Notify) void {
        self.sl.lock();
        const w = self.waiter;
        if (w != null) {
            self.waiter = null;
        } else {
            self.pending = true;
        }
        self.sl.unlock();

        // Wake outside the spinlock (same rationale as Mutex.unlock).
        if (w) |f| rt.wake(f);
    }
};
