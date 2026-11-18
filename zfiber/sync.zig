// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

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
//! Mutex policy: **no hand-off**. `unlock` frees the lock and wakes
//! the first waiter, which then competes for it like a newcomer and
//! re-parks if it loses. A hand-off (the previous policy) made the
//! woken waiter the owner while it still sat on a worker's ring
//! behind other work, and every other contender queued behind that:
//! measured at 4 ms per statement on SemaphoreMap under a 16-stream
//! load. Waiters are woken in FIFO order; ownership is not FIFO.
//! RWMutex keeps its hand-off discipline (writer preference), which
//! SemaphoreMap's deadlock detection relies on.

const std = @import("std");
const _fb = @import("fiber.zig");
const _rt = @import("runtime.zig");
const _reg = @import("lock_registry.zig");

const Fiber = _fb.Fiber;
const Spinlock = _rt.Spinlock;

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

/// Whether a `Mutex`/`RWMutex` records its holder for `dumpLocks`. `@src` is a
/// comptime constant (≈free to store), so prefer `.tracked` — the lock then
/// shows up in a wedged-server dump. `.untracked` comptime-elides the record +
/// registry calls and makes the tracking field zero-width `void`, so the lock
/// is byte-identical to the pre-tracking primitive (use it on hot locks).
/// A tracked lock is linked into the registry by address on its first
/// acquire, so the struct holding it must not move after that: build it
/// at its final address (heap, or a field of one), never return it by
/// value once locked.
pub const Track = enum { tracked, untracked };

/// Cooperative mutex. Uncontended lock/unlock is one atomic CAS;
/// contended waiters park on the runtime, not on the kernel — the
/// worker keeps running other fibers in the meantime. `lock`/`tryLock` take the
/// caller's `@src()` so a wedged server can report who holds the lock (see
/// `Track`); the cost is comptime-elided when `.untracked`.
pub fn Mutex(comptime track: Track) type {
    return struct {
        const Self = @This();

        /// 0 = unlocked, 1 = locked. Atomic. The fast paths
        /// (`tryLock`, uncontended `lock`/`unlock`) operate on this
        /// alone — no spinlock touch.
        state: std.atomic.Value(u32) = .init(0),
        /// Guards composite updates: {`state` transition, `waiters`
        /// mutation}. Held only during the slow path; uncontended
        /// callers bypass it entirely.
        sl: Spinlock = .{},
        waiters: WaitQueue = .{},
        /// Contention gauges for the `locks` dump: `contended` counts
        /// acquires that missed the fast CAS, `wakes` the unlocks that
        /// woke a parked waiter (each one a possibly cross-worker wake).
        /// Slow paths only, so the uncontended cost is nil.
        contended: std.atomic.Value(u64) = .init(0),
        wakes: std.atomic.Value(u64) = .init(0),
        /// Holder breadcrumb (acquire `@src` + timestamp + registry link), only
        /// when `.tracked`; `void` otherwise. Holder/waiter counts are derived
        /// from `state`/`waiters` at dump time, never stored here.
        reg_node: if (track == .tracked) _reg.Tracking else void =
            if (track == .tracked) .{} else {},

        /// Derive this lock's live state for `dumpLocks` from its own fields
        /// (racy reads — the system is wedged when we look).
        fn snapshot(ptr: *anyopaque) _reg.Snap {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return .{
                .held = self.state.load(.monotonic),
                .has_waiters = self.waiters.head != null,
                .contended = self.contended.load(.monotonic),
                .wakes = self.wakes.load(.monotonic),
            };
        }
        inline fn record(self: *Self, comptime src: std.builtin.SourceLocation) void {
            // Comptime-elided when untracked — no cost, no `reg_node` access.
            if (track == .tracked) _reg.recordAcquire(&self.reg_node, self, &snapshot, src);
        }

        /// Raw CAS — the old `tryLock` body, used internally by `lock` (which
        /// records once it holds the lock).
        fn tryAcquire(self: *Self) bool {
            return self.state.cmpxchgWeak(0, 1, .acquire, .monotonic) == null;
        }

        pub fn tryLock(self: *Self, comptime src: std.builtin.SourceLocation) bool {
            if (!self.tryAcquire()) return false;
            self.record(src);
            return true;
        }

        pub fn lock(self: *Self, comptime src: std.builtin.SourceLocation) void {
            if (self.tryAcquire()) {
                self.record(src);
                return;
            }

            // TODO: spin on `tryAcquire` for a bounded number of
            // iterations before parking. A holder that is about to
            // finish (a hash-map walk, microseconds) then costs a short
            // wait instead of a park and a cross-worker wake. Needs a
            // benchmark to size the bound.
            //
            // Slow path, no hand-off: the woken fiber competes for the
            // lock like a newcomer and re-parks if it loses, so nobody
            // owns the lock while queued on a ring behind other work.
            // The enqueue and the re-check run under `sl`, as does
            // `unlock`'s store-then-pop, so either the re-check sees the
            // lock free or the unlock pops this waiter: no lost wake.
            _ = self.contended.fetchAdd(1, .monotonic);
            const f = _rt.currentFiber();
            while (true) {
                self.sl.lock();
                if (self.tryAcquire()) {
                    self.sl.unlock();
                    self.record(src);
                    return;
                }
                self.waiters.pushTail(f);
                self.sl.unlock();
                _rt.park();
            }
        }

        pub fn unlock(self: *Self) void {
            // Free the lock and pop a waiter in one `sl` section, so a
            // waiter that enqueued before this sees the lock free on
            // its re-check or is the one popped here.
            self.sl.lock();
            self.state.store(0, .release);
            const waiter = self.waiters.popHead();
            self.sl.unlock();

            // Wake outside the spinlock: cross-thread wake may push into
            // a target worker's ready ring and issue an eventfd
            // notify (kernel syscall). Holding sl during that would
            // pin a peer thread to a cache line for an entire syscall.
            if (waiter) |w| {
                _ = self.wakes.fetchAdd(1, .monotonic);
                _rt.wake(w);
            }
        }

        /// Unlink from the tracking registry before destruction (tracked only;
        /// no-op otherwise). Long-lived locks needn't call it.
        pub fn deinit(self: *Self) void {
            if (track == .tracked) _reg.unregister(&self.reg_node);
        }
    };
}

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

    /// `mutex` is `anytype` so a Condition pairs with either a tracked or
    /// untracked `Mutex` (they're distinct types). The reacquire passes this
    /// site's `@src()` — a cv wake that reacquires shows up as held here.
    pub fn wait(self: *Condition, mutex: anytype) void {
        const f = _rt.currentFiber();

        // Add to wait queue *before* releasing the mutex so a
        // concurrent `signal` from the next mutex-holder can find
        // us. The signal/broadcast may fire before `park` below;
        // the fiber's awaken-bit handles that race.
        self.sl.lock();
        self.waiters.pushTail(f);
        self.sl.unlock();

        mutex.unlock();
        _rt.park();
        mutex.lock(@src());
    }

    pub fn signal(self: *Condition) void {
        self.sl.lock();
        const w = self.waiters.popHead();
        self.sl.unlock();
        if (w) |f| _rt.wake(f);
    }

    /// Wake every fiber currently in the wait queue. We collect the
    /// fibers under `sl` (using their now-free `waker_next` field
    /// as a transient local-list link) then wake them outside the
    /// spinlock — wake may push into a peer worker's ring +
    /// `wake_async.notify()`, and we don't want a syscall under
    /// `sl`.
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
            _rt.wake(f);
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
/// Implementation: one atomic word (`State`) holds the reader
/// count, the pending-writer count and the writer bit. The
/// shared fast path is a single `fetchAdd` on that word: if the
/// previous value shows no writer held or pending, the +1 IS the
/// hold and the spinlock is never touched. `unlockShared` is the
/// matching `fetchSub`. The writer side and every wait-queue
/// mutation run under `sl`, and the two writer fields change only
/// under `sl` — that is what lets a reader in the slow path check
/// them with a plain load once it holds `sl`.
///
/// A reader's +1 that lands while a writer is held or pending is
/// NOT a hold: the reader undoes it with `readerOut` and parks.
/// The undo goes through the same last-reader-out wake as
/// `unlockShared`, because a writer may have parked on that
/// transient count. Readers and writers park on separate
/// WaitQueues so wake can target the right class without the
/// "broadcast or wrong-class wakeup" hazard from a mixed queue.
pub fn RWMutex(comptime track: Track) type {
    return struct {
        const Self = @This();

        /// The lock word. Packed field order is bit order, low to
        /// high: `readers` in bits 0..31, `pending` in 32..62, the
        /// writer bit at 63.
        pub const State = packed struct(u64) {
            /// Active readers, plus transient +1s from readers about
            /// to back off (see `lockSharedSlow`). Changes anywhere.
            readers: u32 = 0,
            /// Writers in `writers_wq`, or popped from it but not yet
            /// holding. New readers defer while this is nonzero —
            /// that is where writer preference is enforced. Changes
            /// only under `sl`.
            pending_w: u31 = 0,
            /// A writer holds the lock. Changes only under `sl`.
            writer_held: bool = false,

            fn bits(s: State) u64 {
                return @bitCast(s);
            }
            fn from(word: u64) State {
                return @bitCast(word);
            }
            /// A reader arriving now must defer.
            fn blocked(s: State) bool {
                return s.writer_held or s.pending_w != 0;
            }
        };
        const one_reader: u64 = State.bits(.{ .readers = 1 });
        const one_pending_writer: u64 = State.bits(.{ .pending_w = 1 });
        const writer_bit: u64 = State.bits(.{ .writer_held = true });

        /// The following is the same as adding a writer_bit and removing a
        /// pending_writer bit. But, we can do both in a single add using this
        /// const.
        const pending_to_writer: u64 = writer_bit -% one_pending_writer;

        state: std.atomic.Value(u64) = .init(0),

        /// Guards the wait queues and the writer-side transitions of
        /// `state`. Never touched by the shared fast path.
        sl: Spinlock = .{},
        readers_wq: WaitQueue = .{},
        writers_wq: WaitQueue = .{},

        /// Holder breadcrumb; `.tracked` only (counts derived from `state`
        /// at dump time). For a shared session only the FIRST reader
        /// records — the reader whose `fetchAdd` saw `readers == 0` is the
        /// race-free "opener", so concurrent readers don't fight over
        /// `holder_src` (no extra state needed).
        reg_node: if (track == .tracked) _reg.Tracking else void =
            if (track == .tracked) .{} else {},

        fn snapshot(ptr: *anyopaque) _reg.Snap {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const s = State.from(self.state.load(.monotonic));
            return .{
                .held = if (s.writer_held) 1 else s.readers,
                .has_waiters = s.pending_w > 0 or self.readers_wq.head != null,
            };
        }
        inline fn record(self: *Self, comptime src: std.builtin.SourceLocation) void {
            if (track == .tracked) _reg.recordAcquire(&self.reg_node, self, &snapshot, src);
        }

        // ---- shared (read) ----

        /// Count one reader in and return the state before it. The +1
        /// is a hold iff `!prev.blocked()`; otherwise the caller owes a
        /// `readerOut`.
        fn readerIn(self: *Self) State {
            return State.from(self.state.fetchAdd(one_reader, .acquire));
        }

        /// Count one reader out — a real hold or a transient +1, the
        /// word does not distinguish. The last reader out with a writer
        /// pending wakes it: that writer parked on our count. Not
        /// called under `sl` (takes it for the pop).
        fn readerOut(self: *Self) void {
            const prev = State.from(self.state.fetchSub(one_reader, .release));
            std.debug.assert(prev.readers > 0);
            if (prev.readers != 1 or prev.writer_held or prev.pending_w == 0) return;
            self.sl.lock();
            const w = self.writers_wq.popHead();
            self.sl.unlock();
            if (w) |waiter| _rt.wake(waiter);
        }

        pub fn tryLockShared(self: *Self, comptime src: std.builtin.SourceLocation) bool {
            const prev = self.readerIn();
            if (prev.blocked()) {
                self.readerOut();
                return false;
            }
            if (prev.readers == 0) self.record(src);
            return true;
        }

        pub fn lockShared(self: *Self, comptime src: std.builtin.SourceLocation) void {
            const prev = self.readerIn();
            if (prev.blocked()) return self.lockSharedSlow(src);
            if (prev.readers == 0) self.record(src);
        }

        /// Entered with a transient +1 that is not a hold. Undo it,
        /// then wait under `sl` like a plain queue: the writer fields
        /// change only under `sl`, so a load taken while holding it
        /// stays true until we release it — a writer's `unlock` cannot
        /// slip between our check and our `pushTail`, which is the
        /// lost-wakeup it would otherwise be.
        fn lockSharedSlow(self: *Self, comptime src: std.builtin.SourceLocation) void {
            self.readerOut();
            const me = _rt.currentFiber();
            while (true) {
                self.sl.lock();
                if (!State.from(self.state.load(.acquire)).blocked()) {
                    const prev = self.readerIn();
                    std.debug.assert(!prev.blocked());
                    self.sl.unlock();
                    if (prev.readers == 0) self.record(src);
                    return;
                }
                self.readers_wq.pushTail(me);
                self.sl.unlock();
                _rt.park();
            }
        }

        pub fn unlockShared(self: *Self) void {
            self.readerOut();
        }

        // ---- exclusive (write) ----

        pub fn tryLock(self: *Self, comptime src: std.builtin.SourceLocation) bool {
            // Under `sl` so the writer bit keeps its only-under-sl rule.
            // A transient reader +1 fails this like a real one; fine for
            // a try.
            self.sl.lock();
            const taken = self.state.cmpxchgStrong(
                0,
                writer_bit,
                .acquire,
                .monotonic,
            ) == null;
            self.sl.unlock();
            if (!taken) return false;
            self.record(src);
            return true;
        }

        pub fn lock(self: *Self, comptime src: std.builtin.SourceLocation) void {
            const me = _rt.currentFiber();

            self.sl.lock();
            // Announce first: every reader whose +1 lands after this RMW
            // sees `pending` and backs off, so the readers counted in
            // `s` are the only ones we wait for. Readers already counted
            // may themselves be transient — their undo wakes us.
            var s = State.from(self.state.fetchAdd(one_pending_writer, .acquire) +% one_pending_writer);
            while (s.readers != 0 or s.writer_held) {
                self.writers_wq.pushTail(me);
                self.sl.unlock();
                _rt.park();
                self.sl.lock();
                s = State.from(self.state.load(.acquire));
            }
            // One RMW for pending -1 and writer bit set. As two, a reader
            // between them would see neither and take a hold. Staying
            // pending until we hold is also what keeps the preference:
            // between our waker's pop and this line no reader can get in.
            _ = self.state.fetchAdd(pending_to_writer, .acquire);
            self.sl.unlock();
            self.record(src);
        }

        pub fn unlock(self: *Self) void {
            self.sl.lock();
            const prev = State.from(self.state.fetchSub(writer_bit, .release));
            std.debug.assert(prev.writer_held);

            // Writer-preferred: if any writer is queued, hand off
            // exclusively. Otherwise wake every queued reader so they
            // can all acquire concurrently.
            if (prev.pending_w > 0) {
                const w = self.writers_wq.popHead();
                self.sl.unlock();
                if (w) |waiter| _rt.wake(waiter);
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
                _rt.wake(f);
                to_wake = next;
            }
        }

        /// Unlink from the tracking registry before destruction (tracked only;
        /// no-op otherwise).
        pub fn deinit(self: *Self) void {
            if (track == .tracked) _reg.unregister(&self.reg_node);
        }

        // ---- held tokens ----

        pub const Mode = enum { shared, exclusive };

        /// Proof of a hold, minted only by `hold`. A function that assumes
        /// this lock takes one as a parameter: it then cannot be called
        /// without the lock, and the parameter's type says which mode. The
        /// token carries the lock it came from, so a callee can check it is
        /// for the lock it expects and, in Debug, that the lock is still
        /// held (`assertHeld`). An exclusive token stands in for a shared
        /// one (`shared`).
        pub fn Held(comptime mode: Mode) type {
            return struct {
                mu: *Self,

                pub fn release(h: @This()) void {
                    switch (mode) {
                        .shared => h.mu.unlockShared(),
                        .exclusive => h.mu.unlock(),
                    }
                }

                /// An exclusive hold satisfies a shared requirement.
                pub fn shared(h: @This()) Held(.shared) {
                    comptime std.debug.assert(mode == .exclusive);
                    return .{ .mu = h.mu };
                }

                /// The mode this token proves; `assertHeld` reads it.
                pub const held_mode = mode;
            };
        }

        /// Debug check that `held` is a token for THIS lock and that the
        /// lock is still held in the token's mode — a token for another
        /// lock, or one used after `release`, fails here. Takes `*const
        /// Self` so a `*const` receiver can check its own lock.
        pub fn assertHeld(self: *const Self, held: anytype) void {
            std.debug.assert(held.mu == self);
            std.debug.assert(self.isHeld(@TypeOf(held).held_mode));
        }

        /// The two token types, so an owner writes `Lock.Shared` /
        /// `Lock.Exclusive` with `Lock` its own alias for this type.
        pub const Shared = Held(.shared);
        pub const Exclusive = Held(.exclusive);

        /// Take the lock in `mode` and mint the token for it; `defer
        /// held.release()`.
        pub fn hold(self: *Self, comptime mode: Mode, comptime src: std.builtin.SourceLocation) Held(mode) {
            switch (mode) {
                .shared => self.lockShared(src),
                .exclusive => self.lock(src),
            }
            return .{ .mu = self };
        }

        /// Whether the lock is currently held in `mode` by someone (an
        /// exclusive hold counts as shared too). A racy read, for debug
        /// asserts only.
        pub fn isHeld(self: *const Self, mode: Mode) bool {
            const s = State.from(self.state.load(.acquire));
            return switch (mode) {
                .exclusive => s.writer_held,
                .shared => s.writer_held or s.readers > 0,
            };
        }
    };
}

/// Single-shot event. `wait` parks until `notify` fires; if `notify`
/// fires first (no waiter present), the next `wait` returns
/// immediately and clears the pending flag.
///
/// One waiter at a time. Two fibers calling `wait` concurrently is
/// undefined; build on top of `WaitQueue` directly if you need a
/// multi-waiter event.
/// Work that is either running or done, with any number of fibers
/// waiting for done: `setRunning` at the start, `setDone` at the end,
/// `waitForDone` from anyone, any time. Mutex + Condition underneath:
/// the state lives under the mutex, so a wait that races the finish
/// is not lost. One piece of work at a time; the transitions assert it.
pub const Event = struct {
    mu: Mutex(.untracked) = .{},
    cond: Condition = .{},
    running: bool = false,

    pub fn setRunning(self: *Event) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        std.debug.assert(!self.running);
        self.running = true;
    }

    pub fn setDone(self: *Event) void {
        self.mu.lock(@src());
        std.debug.assert(self.running);
        self.running = false;
        self.mu.unlock();
        self.cond.broadcast();
    }

    pub fn isRunning(self: *Event) bool {
        self.mu.lock(@src());
        defer self.mu.unlock();
        return self.running;
    }

    /// Returns at once when nothing is running.
    pub fn waitForDone(self: *Event) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        while (self.running) self.cond.wait(&self.mu);
    }
};

/// Pieces of work in flight, `limit` at once: `enter` parks while that
/// many are out, `leave` lets the next one in. For a queue the holder
/// does not own, a device's or a peer's, that should hold a bounded
/// run of ours. Mutex + Condition as `Event`, which is the one-job
/// case with transitions that assert instead of waiting.
pub const Inflight = struct {
    mu: Mutex(.untracked) = .{},
    cond: Condition = .{},
    /// At least 1, or `enter` never returns.
    limit: u32,
    count: u32 = 0,

    pub fn enter(self: *Inflight) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        while (self.count >= self.limit) self.cond.wait(&self.mu);
        self.count += 1;
    }

    pub fn leave(self: *Inflight) void {
        self.mu.lock(@src());
        std.debug.assert(self.count > 0);
        self.count -= 1;
        self.mu.unlock();
        self.cond.signal();
    }

    pub fn inFlight(self: *Inflight) u32 {
        self.mu.lock(@src());
        defer self.mu.unlock();
        return self.count;
    }
};

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
        self.waiter = _rt.currentFiber();
        self.sl.unlock();
        _rt.park();
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
        if (w) |f| _rt.wake(f);
    }
};
