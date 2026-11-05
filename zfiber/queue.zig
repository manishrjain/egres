// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Allocator = std.mem.Allocator;
const Fiber = @import("fiber.zig").Fiber;
const Spinlock = @import("spinlock.zig").Spinlock;

/// Per-worker, per-priority ready queue: a heap-backed ring buffer of
/// `*Fiber` that GROWS (doubling) when full, so `push` never has to reject
/// work — it only fails on allocator OOM. This makes the ring the single
/// work container the scheduler needs: a normal ring absorbs bursts that
/// once spilled to a global queue, and the high ring (only ~1 resident in
/// practice) simply never grows. See `Worker.scheduleLocal`.
///
/// `local_queue_capacity` is the INITIAL slot count each ring is seeded with
/// (a power of two); it doubles from there on demand.
pub const local_queue_capacity: usize = 32;

/// Whether the `LocalQueue` guards its mutating ops with a `Spinlock`.
///   - .none: no locking. Does not allow task stealing.
///   - .withlock: every `push`/`pop` takes a `Spinlock`, so a peer worker
///     could steal without racing the owner. Required for task stealing.
pub const LockMode = enum { none, withlock };

/// `LocalQueue(.none)` is the zero-overhead owner-only ring; `LocalQueue(.withlock)`
/// is the same ring with a `Spinlock` around `push`/`pop`. Both expose the
/// identical API; the lock branches and the `sl` field are comptime-elided
/// in `.none` mode, so it keeps the exact layout/codegen it had before.
pub fn LocalQueue(comptime lock: LockMode) type {
    return struct {
        const Self = @This();

        // Heap-backed ring; `capacity` is `mask + 1`, always a power of two,
        // and doubles via `grow` when full. The live range is
        // `[head, head + size)` taken `& mask`; `tail` is derivable so it
        // isn't stored. Slots are OPTIONAL and zeroed (`null`) the instant a
        // fiber is picked (pop/steal), so a stale/duplicate slot read — e.g.
        // a `head`/`size` drift re-reading an already-stolen slot — surfaces
        // as a LOUD null rather than silently re-dispatching a real fiber
        // that is already running on another worker.
        allocator: Allocator,
        fibers: []?*Fiber,
        mask: usize,
        head: usize = 0,
        // Atomic: the owner mutates it under the lock, but `len()` peeks it
        // WITHOUT the lock (hot-path hint), and a `.withlock` peer mutates it
        // under the lock during a steal.
        size: std.atomic.Value(usize) = .init(0),
        // Present only in `.withlock`; `void` (zero-sized) otherwise.
        sl: if (lock == .withlock) Spinlock else void =
            if (lock == .withlock) .{} else {},

        /// Allocate a ring of `initial_capacity` (power of two, ≥ 2) empty
        /// slots. The runtime seeds both per-priority rings at
        /// `local_queue_capacity`.
        pub fn init(allocator: Allocator, initial_capacity: usize) Allocator.Error!Self {
            std.debug.assert(std.math.isPowerOfTwo(initial_capacity));
            std.debug.assert(initial_capacity >= 2);
            const fibers = try allocator.alloc(?*Fiber, initial_capacity);
            @memset(fibers, null);
            return .{ .allocator = allocator, .fibers = fibers, .mask = initial_capacity - 1 };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.fibers);
        }

        inline fn acquire(self: *Self) void {
            if (lock == .withlock) self.sl.lock();
        }
        inline fn release(self: *Self) void {
            if (lock == .withlock) self.sl.unlock();
        }

        /// Double the ring and compact the live range to start at index 0.
        /// Called from `push` (under the lock) when full; amortized O(1) per
        /// push. New tail slots are null, preserving the pick-zeroes invariant.
        fn grow(self: *Self) Allocator.Error!void {
            const size = self.size.load(.monotonic);
            const new_cap = (self.mask + 1) * 2;
            const new_fibers = try self.allocator.alloc(?*Fiber, new_cap);
            @memset(new_fibers, null);
            for (0..size) |k| new_fibers[k] = self.fibers[(self.head + k) & self.mask];
            self.allocator.free(self.fibers);
            self.fibers = new_fibers;
            self.mask = new_cap - 1;
            self.head = 0;
        }

        /// Append `f` to the back, growing the ring if it is full. The grow
        /// makes overflow impossible, so the only failure is allocator OOM.
        pub fn push(self: *Self, f: *Fiber) Allocator.Error!void {
            self.acquire();
            defer self.release();
            const size = self.size.load(.monotonic);
            if (size == self.mask + 1) try self.grow(); // full → double first
            const idx = (self.head + size) & self.mask;
            std.debug.assert(self.fibers[idx] == null); // slot must be free
            self.fibers[idx] = f;
            self.size.store(size + 1, .monotonic);
            f.assertEnQ();
        }

        /// Remove and return the front fiber, or null if empty.
        pub fn pop(self: *Self) ?*Fiber {
            self.acquire();
            defer self.release();
            const size = self.size.load(.monotonic);
            if (size == 0) return null;
            const f = self.fibers[self.head] orelse std.debug.panic(
                "LocalQueue.pop: NULL slot[{d}] dispatched; size={d} — head/size drift or stale re-read",
                .{ self.head, size },
            );
            self.fibers[self.head] = null; // zero on pick: no stale re-dispatch
            self.head = (self.head + 1) & self.mask;
            self.size.store(size - 1, .monotonic);
            f.assertDQ();
            return f;
        }

        /// Remove and return the fiber `i` slots behind the front, or null
        /// when `i` is past the end. The front fiber moves into its slot
        /// and the front is popped, so this is O(1) and the live range
        /// stays contiguous; the queue's order is no longer FIFO, which
        /// is the point: a seeded scheduler (`InitOptions.shuffle_seed`,
        /// the simulation) draws `i` to explore orderings production
        /// would not produce.
        pub fn popAt(self: *Self, i: usize) ?*Fiber {
            self.acquire();
            defer self.release();
            const size = self.size.load(.monotonic);
            if (i >= size) return null;
            const idx = (self.head + i) & self.mask;
            const f = self.fibers[idx] orelse std.debug.panic(
                "LocalQueue.popAt: NULL slot[{d}] dispatched; size={d}",
                .{ idx, size },
            );
            self.fibers[idx] = self.fibers[self.head];
            self.fibers[self.head] = null;
            self.head = (self.head + 1) & self.mask;
            self.size.store(size - 1, .monotonic);
            f.assertDQ();
            return f;
        }

        /// Steal fibers from the FRONT of this (victim) queue into `out`,
        /// returning the count moved. `dest_sz` is the THIEF's current
        /// ready-ring size: we take `ceil((size - dest_sz) / 2)` — half the
        /// DIFFERENCE — so victim and thief end up balanced (each ≈
        /// `(size + dest_sz) / 2`) rather than the thief blindly taking half
        /// the victim. That adapts when several thieves drain one victim at
        /// once: each stops at the balance point, leaving the rest a share
        /// (vs. the first couple of thieves emptying it). Returns 0 once the
        /// thief is already as big as the victim (`size <= dest_sz`), which
        /// also covers an empty victim. `ceil` keeps a lone fiber stealable
        /// (size 1, dest 0 -> take 1).
        ///
        /// Capped at `out.len` (the thief's scratch); the thief re-locks for
        /// another bite, passing its grown `dest_sz`, until this returns 0.
        /// Called by a PEER under this queue's lock, so the owner's push/pop
        /// serialize against it; the thief enqueues `out` into its own ring
        /// AFTER releasing — never holding two queue locks (deadlock).
        /// `.withlock` only.
        pub fn stealInto(self: *Self, dest_sz: usize, out: []*Fiber) usize {
            if (lock != .withlock) @compileError("stealInto requires .withlock");
            self.acquire();
            defer self.release();

            const size = self.size.load(.monotonic);
            if (size <= dest_sz) return 0; // thief already >= victim
            const want = (size - dest_sz + 1) / 2; // ceil(half the difference)
            const take = @min(out.len, want);
            for (0..take) |k| {
                const idx = (self.head + k) & self.mask;
                out[k] = self.fibers[idx] orelse std.debug.panic(
                    "LocalQueue.steal: NULL slot[{d}]; size={d} take={d} — stale re-read",
                    .{ idx, size, take },
                );
                self.fibers[idx] = null; // zero on pick: no stale re-dispatch
                out[k].assertDQ();
            }
            self.head = (self.head + take) & self.mask;
            self.size.store(size - take, .monotonic);
            return take;
        }

        // Lock-free peeks: a monotonic atomic load — well-defined even while a
        // `.withlock` peer mutates `size` under the lock (the thief's
        // cross-thread `len()` in `trySteal`, the owner's `len() >= 2` pile-up
        // hint in `scheduleLocal`). Returns a recent snapshot, all these hint
        // callers need; the actual push/pop re-checks under the lock.
        pub fn len(self: *const Self) usize {
            return self.size.load(.monotonic);
        }

        /// Allocated slot count (a power of two). The ring only ever GROWS
        /// (doubling), never shrinks, so this doubles as a cheap high-water
        /// gauge.
        pub fn capacity(self: *const Self) usize {
            return self.mask + 1;
        }

        /// `len()` taken UNDER the lock — so the count synchronizes with a
        /// cross-thread `push` (the push's `size` store becomes visible via the
        /// lock release). The lock-free `len()` is a hint that may miss a
        /// just-completed cross-thread push; a parker's pre-park recheck needs
        /// the real answer (see `Worker.hasReady`) or it could park with
        /// work sitting in its ring (lost wakeup). `.withlock` only — a `.none`
        /// ring has no cross-thread writers.
        pub fn lenAcquire(self: *Self) usize {
            if (lock != .withlock) @compileError("lenAcquire requires .withlock");
            self.acquire();
            defer self.release();
            return self.size.load(.monotonic);
        }
    };
}

/// Largest worker count we support; bounds the idle bitmap. Worker ids
/// are `u8`, so 256 is the ceiling.
pub const max_workers: usize = 256;
const idle_words: usize = (max_workers + 63) / 64;

/// Idle-worker bitmap for one lane. Bit `i` set ⇒ the worker at
/// lane-relative offset `i` is parked in `loop.run(.once)` waiting for
/// work. A producer making work available calls `claim` to atomically
/// take one idle worker's bit, then wakes exactly that peer as a
/// SEARCHER — see `Runtime.notifyIdlePeer` for the throttle around it.
/// Lane-agnostic (indexes by offset within a lane) so it unit-tests
/// standalone.
///
/// Bit ownership: a worker only ever sets its OWN bit (`markIdle`); it
/// is cleared exactly once per park cycle, by either a producer's
/// `claim` (a deliberate wake) or the worker's own `selfClear` (it woke
/// for another reason — timer, I/O, stop). Both are single RMWs on the
/// word, so the two sides can never both think they cleared it; that
/// exactly-once handoff is what `Worker.unpark` uses to decide whether
/// it inherited the thieving state.
///
/// Memory ordering policy: EVERY atomic op here is seq_cst, uniformly.
/// Several ops sit in Dekker (store-then-load-other-location) pairs with
/// the producers' push-then-peek sequence, where acquire/release is
/// formally insufficient (a plain acquire load may return a stale word and
/// both sides can miss → lost wakeup); the rest could be weaker but are
/// kept seq_cst so nobody has to re-derive which is which. This is free on
/// our targets: seq_cst RMWs and loads compile to the same instructions as
/// acq_rel RMWs and acquire loads on both x86-64 and aarch64. Two
/// load-bearing invariants regardless: all mutations are RMWs (a plain
/// store would sever the release sequence carrying the num_thieves +1
/// handoff from `claim` to `selfClear`), and RMW reads are never stale (an
/// RMW always reads the latest value in modification order).
pub const IdleWorkers = struct {
    words: [idle_words]std.atomic.Value(u64) align(std.atomic.cache_line) = @splat(.init(0)),
    /// Where the next `claim` / `anyOffset` starts scanning. Advances on
    /// every call so the pick rotates over the parked workers instead of
    /// always landing on the lowest-numbered one: under I/O-bound load the
    /// busiest workers park briefly but constantly, so a lowest-bit pick
    /// kept waking THEM as thieves (they pulled every fiber downhill) and
    /// never reached the long-parked workers above them.
    cursor: std.atomic.Value(u8) align(std.atomic.cache_line) = .init(0),
    /// Workers in this lane, so the cursor rotates over the lane's own
    /// offsets. Rotating over all `max_workers` bits instead sends most
    /// starts into empty words, and the scan then walks the lane's word
    /// from bit 0 — 7 claims in 8 went to the lowest parked worker of a
    /// 32-worker lane. Set by `Runtime.init`; 1 for an empty lane (never
    /// claimed, keeps the modulo safe).
    len: u8 = 1,

    pub fn markIdle(self: *IdleWorkers, i: u8) void {
        const w = @as(usize, i) >> 6;
        const bit = @as(u64, 1) << @as(u6, @intCast(i & 63));
        _ = self.words[w].fetchOr(bit, .seq_cst);
    }

    /// Atomically clear worker `i`'s own bit, returning true if WE cleared
    /// it. False means a producer's `claim` got there first: the wake was
    /// deliberate and the worker was registered as a thief (see
    /// `Runtime.notifyIdlePeer`). The single RMW (vs. a load-then-clear
    /// pair) is what makes the handoff exactly-once.
    pub fn selfClear(self: *IdleWorkers, i: u8) bool {
        const w = @as(usize, i) >> 6;
        const bit = @as(u64, 1) << @as(u6, @intCast(i & 63));
        return (self.words[w].fetchAnd(~bit, .seq_cst) & bit) != 0;
    }

    /// "Is anyone parked?" peek for producers — loads only, so the
    /// all-workers-busy steady state pays no RMW on the notify path.
    pub fn anySet(self: *IdleWorkers) bool {
        for (&self.words) |*word| {
            if (word.load(.seq_cst) != 0) return true;
        }
        return false;
    }

    /// Atomically claim (clear) one set bit and return its offset, or
    /// null if no worker is idle. The successful CAS's write side also
    /// publishes the producer's prior num_thieves fetchAdd to the woken
    /// worker's `selfClear` (the thief +1 handoff).
    pub fn claim(self: *IdleWorkers) ?u8 {
        const start = self.nextStart();
        const first_word = start / 64;
        const start_bit: u6 = @intCast(start % 64);
        for (0..idle_words) |jmp| {
            const wi = (first_word + jmp) % idle_words;
            const word = &self.words[wi];
            var wd = word.load(.seq_cst);
            while (wd != 0) {
                // The start bit belongs to the cursor's own word; the
                // words after it are walked from bit 0.
                const bit_i = firstSetFrom(wd, if (jmp == 0) start_bit else 0);
                const bit = @as(u64, 1) << bit_i;
                if (word.cmpxchgWeak(wd, wd & ~bit, .seq_cst, .seq_cst)) |actual| {
                    wd = actual;
                    continue;
                }
                return @intCast(wi * 64 + bit_i);
            }
        }
        return null;
    }

    /// The rotating scan start over the whole bitmap (a worker index):
    /// each call moves it one along, so consecutive picks walk the parked
    /// set round-robin, and every word takes its turn as the first one.
    fn nextStart(self: *IdleWorkers) usize {
        return self.cursor.fetchAdd(1, .monotonic) % self.len;
    }

    /// The first set bit of `word` at or after `start`, wrapping — `word` must
    /// be non-zero.
    fn firstSetFrom(word: u64, start: u6) u6 {
        const rotated = std.math.rotr(u64, word, start);
        return @intCast((@as(u7, @ctz(rotated)) + start) % 64);
    }

    /// Targeted "is THIS worker parked?" peek — the producer side of the
    /// high-priority push-then-peek Dekker pair (see
    /// `Worker.notifyIfParked`); same shape as `anySet`.
    pub fn isIdle(self: *IdleWorkers, i: u8) bool {
        const w = @as(usize, i) >> 6;
        const bit = @as(u64, 1) << @as(u6, @intCast(i & 63));
        return (self.words[w].load(.seq_cst) & bit) != 0;
    }

    /// Offset of SOME parked worker, or null if none. Unlike `claim` it does
    /// NOT clear the bit — a racy peek used by `Runtime.injectNormal` to
    /// prefer delivering work to an already-parked worker. The woken worker
    /// clears its own bit via `selfClear`, so no thief +1 is involved.
    pub fn anyOffset(self: *IdleWorkers) ?u8 {
        const start = self.nextStart();
        const first_word = start / 64;
        const start_bit: u6 = @intCast(start % 64);
        for (0..idle_words) |k| {
            const wi = (first_word + k) % idle_words;
            const m = self.words[wi].load(.seq_cst);
            if (m != 0) {
                const from: u6 = if (k == 0) start_bit else 0;
                return @intCast(wi * 64 + firstSetFrom(m, from));
            }
        }
        return null;
    }
};
