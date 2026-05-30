//! Per-worker fiber ready queue. Fixed-capacity Chase-Lev work-
//! stealing deque specialized for `*Fiber`.
//!
//! Thieves steal from the HEAD (oldest items, FIFO order to them).
//! The owner pushes and pops at the TAIL (newest items, LIFO to it).
//! Lock-free: owner's `push` is two relaxed loads and one release
//! store — no CAS. `pop` and `steal*` each issue one CAS only when
//! the queue is at or near the boundary where owner and thief can
//! contend.
//!
//! The classic Chase-Lev paper names these `top` (= our HEAD) and
//! `bottom` (= our TAIL); we use head/tail because they match the
//! rest of the codebase and don't require visualizing a stack of
//! plates to keep straight.
//!
//! Capacity is fixed at `init` time and must be a power of two so
//! the slot index reduces to a bitmask. `push` returns
//! `error.Full` when the queue is at capacity — callers decide
//! whether to spill onto a slower-path queue, drop, or block.
//!
//! Stealers transfer fibers directly into their own LocalQueue via
//! `stealBatchTo`, claiming up to half the victim's items in one
//! CAS. The destination's slot array is written speculatively
//! before the CAS; if the CAS loses, the speculative writes are
//! invisible (we never advanced the destination's tail).
//!
//! Memory model follows Lê, Pop, Cohen, Nardelli, "Correct and
//! Efficient Work-Stealing for Weak Memory Models" (PPoPP 2013).
//! The original Chase-Lev paper (2005) is in Java memory-model
//! terms; the 2013 version maps it onto C11 atomics with the
//! minimum-strength orderings that stay safe on ARM/POWER.
//!
//! Zig 0.16 removed `@fence` / `std.atomic.fence`, so where Lê uses
//! `fence(seq_cst)` between two relaxed atomics this implementation
//! upgrades both atomics to seq_cst. The result is equivalent: any
//! `store(seq_cst)` followed by `load(seq_cst)` in one thread is
//! globally ordered, which is exactly the StoreLoad barrier the
//! fence provided.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Fiber = @import("fiber.zig").Fiber;
const Spinlock = @import("spinlock.zig").Spinlock;

pub const LocalQueue = struct {
    const Self = @This();

    pub const Error = error{Full};

    pub const Steal = union(enum) {
        /// Number of fibers transferred into the destination. Always
        /// >= 1 — `.empty` / `.dest_full` / `.contended` are reported
        /// separately.
        count: usize,
        /// Victim was observed empty.
        empty,
        /// Destination had no room for any items.
        dest_full,
        /// A concurrent steal or pop CAS'd the victim's head between
        /// our snapshot and our claim. Caller may retry.
        contended,
    };

    // `head` is written by thieves (CAS) and by `pop`'s last-
    // element CAS. `tail` is written only by the owner. Each lives
    // on its own cache line — owner and thieves don't share.
    head: std.atomic.Value(i64) align(std.atomic.cache_line) = .init(0),
    tail: std.atomic.Value(i64) align(std.atomic.cache_line) = .init(0),

    // Fixed-capacity ring buffer. `mask = capacity - 1`, so the
    // slot for monotonically growing index `i` is `slots[i & mask]`.
    slots: []std.atomic.Value(*Fiber),
    mask: u64,

    pub fn init(allocator: Allocator, cap: usize) !Self {
        std.debug.assert(std.math.isPowerOfTwo(cap));
        std.debug.assert(cap >= 2);
        const slots = try allocator.alloc(std.atomic.Value(*Fiber), cap);
        return .{
            .slots = slots,
            .mask = @as(u64, cap) - 1,
        };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        allocator.free(self.slots);
    }

    pub fn capacity(self: *const Self) usize {
        return @intCast(self.mask + 1);
    }

    inline fn slotIndex(self: *const Self, i: i64) usize {
        return @intCast(@as(u64, @bitCast(i)) & self.mask);
    }

    inline fn slotGet(self: *Self, i: i64) *Fiber {
        return self.slots[self.slotIndex(i)].load(.monotonic);
    }

    inline fn slotPut(self: *Self, i: i64, f: *Fiber) void {
        self.slots[self.slotIndex(i)].store(f, .monotonic);
    }

    /// Owner-only. Append `f` at the tail. Returns `error.Full` if
    /// the queue is at capacity.
    pub fn push(self: *Self, f: *Fiber) Error!void {
        std.debug.assert(@intFromPtr(f) >= 0x1000);
        const t = self.tail.load(.monotonic);
        const h = self.head.load(.acquire);
        if (t - h >= @as(i64, @intCast(self.capacity()))) {
            return error.Full;
        }
        self.slotPut(t, f);
        // Release on tail: any thief that acquire-loads tail and
        // sees `t+1` is guaranteed to also see the slot store above.
        self.tail.store(t + 1, .release);
    }

    /// Owner-only. Remove and return the tail fiber (LIFO).
    /// Returns null when empty.
    pub fn pop(self: *Self) ?*Fiber {
        const t = self.tail.load(.monotonic) - 1;
        // seq_cst on this store pairs with `stealBatchTo`'s seq_cst
        // loads of head + tail: the tail decrement is globally
        // ordered before any thief's subsequent head-CAS, so the
        // owner and a thief never both believe they hold the last
        // element.
        self.tail.store(t, .seq_cst);
        const h = self.head.load(.seq_cst);
        if (h > t) {
            // Empty. Restore tail to a canonical empty state.
            self.tail.store(h, .monotonic);
            return null;
        }
        const f = self.slotGet(t);
        // Tripwire: any *Fiber pointer we return from a queue slot
        // must be a real heap address. A stale-undef-fill value
        // (0xAAAA…) or a stack address here means the slot write
        // raced with something else.
        std.debug.assert(@intFromPtr(f) >= 0x1000);
        if (h != t) return f; // > 1 element left — no race

        // Slow Path: Exactly one element: race with a concurrent steal CAS.
        const lost = self.head.cmpxchgStrong(h, h + 1, .seq_cst, .monotonic) != null;
        // Either way, the queue is now empty: leave head at h+1
        // and put tail one past it.
        self.tail.store(t + 1, .monotonic);
        return if (lost) null else f;
    }

    /// Any thread. Steal up to `min(dest_room, ceil(size/2))`
    /// fibers from `victim`'s head, appending them to `dest`'s
    /// tail. `dest` must be owned by the calling thread.
    ///
    /// On success returns `.count = n`. On loss returns
    /// `.contended` (caller may retry the same or a different
    /// victim). On observed-empty victim returns `.empty`. On
    /// no-room-in-dest returns `.dest_full`.
    ///
    /// Stealing half (not one) is what makes work-stealing scale:
    /// a thief that drains half the victim's backlog amortizes the
    /// CAS / cache-coherency cost across N items rather than
    /// paying it per-item, and it leaves the victim with useful
    /// work instead of stalling it.
    pub fn stealBatchTo(victim: *Self, dest: *Self) Steal {
        // dest is owned by the caller; only this thread writes
        // dest.tail. Other threads (thieves of dest) may advance
        // dest.head, which only INCREASES our available room — so
        // a one-shot snapshot here is sound.
        const dest_t = dest.tail.load(.monotonic);
        const dest_h = dest.head.load(.acquire);
        const dest_used: usize = @intCast(dest_t - dest_h);
        const dest_room = dest.capacity() - dest_used;
        if (dest_room == 0) return .dest_full;

        // Both seq_cst: places (head, tail) reads in the global
        // total order with victim's pop tail-store.
        const h = victim.head.load(.seq_cst);
        const t = victim.tail.load(.seq_cst);
        // Signed diff first, then bound check, THEN cast. If we cast
        // first, a victim caught mid-pop-slow-path can briefly show
        // `head > tail` (owner decremented tail; head not yet
        // restored), and the unsigned cast panics. The single
        // `diff <= 1` check covers all three skip cases:
        //   diff < 0  → race, victim not stable; bail
        //   diff == 0 → empty
        //   diff == 1 → only the owner's last fiber; don't steal it,
        //               that's the size=1 pop/steal race window the
        //               caller is trying to avoid.
        const diff = t - h;
        if (diff <= 1) return .empty;
        const size: usize = @intCast(diff);

        // ceil(size / 2). size is guaranteed >= 2 by the diff check
        // above, so n is always >= 1.
        var n = (size + 1) / 2;
        if (n > dest_room) n = dest_room;

        // Speculatively write into dest's slot array at positions
        // [dest_t, dest_t + n). These slots are invisible to dest's
        // thieves (they check `dest.tail`) until we advance the
        // tail below. If the CAS below loses, we simply don't
        // advance dest.tail — the speculative writes vanish.
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const f = victim.slotGet(h + @as(i64, @intCast(i)));
            // Tripwire: every stolen pointer must be a real heap
            // address. Catches a victim slot that was written
            // out-of-order with respect to victim.tail, or a slot
            // that's still undef-filled (push hasn't run yet).
            std.debug.assert(@intFromPtr(f) >= 0x1000);
            dest.slotPut(dest_t + @as(i64, @intCast(i)), f);
        }

        const new_h = h + @as(i64, @intCast(n));
        if (victim.head.cmpxchgStrong(h, new_h, .seq_cst, .monotonic) != null) {
            return .contended;
        }

        // Commit: publish the new dest tail. Release pairs with
        // dest's thieves' acquire-load of tail.
        dest.tail.store(dest_t + @as(i64, @intCast(n)), .release);
        return .{ .count = n };
    }

    /// Any thread. Single-item shortcut. Returns null on empty or
    /// contended. Convenient when the caller wants to inspect /
    /// run one fiber directly without pushing into a destination
    /// queue.
    pub fn stealOne(self: *Self) ?*Fiber {
        const h = self.head.load(.seq_cst);
        const t = self.tail.load(.seq_cst);
        if (h >= t) return null;
        const f = self.slotGet(h);
        if (self.head.cmpxchgStrong(h, h + 1, .seq_cst, .monotonic) != null) {
            return null;
        }
        return f;
    }

    /// Snapshot length. Approximate under concurrent operations
    /// — fine for stats / debug, not for control flow.
    pub fn len(self: *const Self) usize {
        const t = self.tail.load(.monotonic);
        const h = self.head.load(.monotonic);
        return if (t > h) @intCast(t - h) else 0;
    }
};

/// Drop-in replacement for `LocalQueue` with the same public API,
/// but every operation runs under a single per-queue spinlock and
/// the bookkeeping fields are plain (non-atomic) `usize`s. The
/// logic is intentionally trivial: each method does its work in
/// one critical section, with no cross-thread atomics or memory-
/// model subtleties. Used to A/B against `LocalQueue` when the
/// Chase-Lev variant's pop/steal race is suspected — if a bug
/// reproduces under `LocalQueue` but disappears under
/// `LocalQueue2`, the issue is in the lock-free queue logic.
///
/// Same layout idea as `LocalQueue`: a power-of-two ring buffer
/// where `head` is the next pop-by-thief index and `head + size`
/// is the owner's push/pop slot. Owner ops are LIFO (push/pop at
/// the tail); thief ops are FIFO (steal from the head).
pub const LocalQueue2 = struct {
    const Self = @This();

    pub const Error = error{Full};
    pub const Steal = LocalQueue.Steal;

    sl: Spinlock = .{},
    fibers: []*Fiber,
    head: usize = 0,
    size: usize = 0,
    mask: usize,

    pub fn init(allocator: Allocator, cap: usize) !Self {
        std.debug.assert(std.math.isPowerOfTwo(cap));
        std.debug.assert(cap >= 2);
        const fibers = try allocator.alloc(*Fiber, cap);
        return .{ .fibers = fibers, .mask = cap - 1 };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        allocator.free(self.fibers);
    }

    pub fn capacity(self: *const Self) usize {
        return self.mask + 1;
    }

    /// Owner-only by convention, but the lock makes it safe from
    /// anywhere. Returns `error.Full` at capacity.
    pub fn push(self: *Self, f: *Fiber) Error!void {
        std.debug.assert(@intFromPtr(f) >= 0x1000);
        self.sl.lock();
        defer self.sl.unlock();
        if (self.size == self.capacity()) return error.Full;
        self.fibers[(self.head + self.size) & self.mask] = f;
        self.size += 1;
    }

    /// Owner-only by convention. Pops from the tail (LIFO).
    pub fn pop(self: *Self) ?*Fiber {
        self.sl.lock();
        defer self.sl.unlock();
        if (self.size == 0) return null;
        self.size -= 1;
        const f = self.fibers[(self.head + self.size) & self.mask];
        std.debug.assert(@intFromPtr(f) >= 0x1000);
        return f;
    }

    /// Thief-only. Pops from the head (FIFO).
    pub fn stealOne(self: *Self) ?*Fiber {
        self.sl.lock();
        defer self.sl.unlock();
        if (self.size == 0) return null;
        const f = self.fibers[self.head & self.mask];
        std.debug.assert(@intFromPtr(f) >= 0x1000);
        self.head = (self.head + 1) & self.mask;
        self.size -= 1;
        return f;
    }

    /// Thief-only. Steal up to `ceil(victim.size / 2)` fibers from
    /// `victim`'s head, append to `dest`'s tail. To avoid deadlock
    /// between symmetric (A steals from B while B steals from A)
    /// callers, we lock `victim` first and `tryLock` `dest`. On
    /// `tryLock` failure we report `.contended` rather than
    /// risk a circular wait.
    pub fn stealBatchTo(victim: *Self, dest: *Self) Steal {
        victim.sl.lock();
        // Skip stealing the last fiber — same boundary case the
        // Chase-Lev variant respects to avoid the size=1 owner/
        // thief race window.
        if (victim.size <= 1) {
            victim.sl.unlock();
            return .empty;
        }

        if (!dest.sl.tryLock()) {
            victim.sl.unlock();
            return .contended;
        }

        const dest_room = dest.capacity() - dest.size;
        if (dest_room == 0) {
            dest.sl.unlock();
            victim.sl.unlock();
            return .dest_full;
        }

        var n = (victim.size + 1) / 2;
        if (n > dest_room) n = dest_room;

        var i: usize = 0;
        while (i < n) : (i += 1) {
            const f = victim.fibers[(victim.head + i) & victim.mask];
            std.debug.assert(@intFromPtr(f) >= 0x1000);
            dest.fibers[(dest.head + dest.size + i) & dest.mask] = f;
        }
        victim.head = (victim.head + n) & victim.mask;
        victim.size -= n;
        dest.size += n;

        dest.sl.unlock();
        victim.sl.unlock();
        return .{ .count = n };
    }

    /// Snapshot length. Brief critical section.
    pub fn len(self: *Self) usize {
        self.sl.lock();
        defer self.sl.unlock();
        return self.size;
    }
};

/// Runtime-wide overflow / admission queue. Spinlock-protected
/// ring buffer of `*Fiber`. Capacity starts at `initial_capacity`
/// and doubles on push when full. Used for:
///
///   - Bootstrap spawns from a non-worker thread (the host thread
///     calling `fiber.spawn` before `Runtime.run` has been entered).
///   - Spillover from a worker whose `LocalQueue` is full.
///
/// Multi-producer, multi-consumer; producers (`push`) and consumers
/// (`popBatchTo`) all serialize on the same spinlock. Bulk dequeue
/// writes fibers DIRECTLY into the destination `LocalQueue`'s slot
/// array, then advances `dest.tail` with a single release-store —
/// the same pattern `LocalQueue.stealBatchTo` uses, so peers
/// stealing from `dest` observe the batch atomically rather than
/// piecemeal.
///
/// Not a hot path. Workers prefer their own local queue first,
/// peers via stealing second, and consult the global queue when
/// both are empty.
pub const GlobalQueue = struct {
    allocator: Allocator,
    sl: Spinlock = .{},

    // Ring buffer for *Fiber. Capacity is a power of two so the
    // slot index is `(head + i) & mask`. All access under `sl`, so
    // none of these need atomic ops. `tail` is derivable as
    // `(head + size) & mask` and isn't stored.
    fibers: []*Fiber,
    mask: usize,
    head: usize = 0,
    size: usize = 0,

    pub fn init(allocator: Allocator, initial_capacity: usize) !GlobalQueue {
        std.debug.assert(std.math.isPowerOfTwo(initial_capacity));
        std.debug.assert(initial_capacity >= 2);
        const fibers = try allocator.alloc(*Fiber, initial_capacity);
        return .{
            .allocator = allocator,
            .fibers = fibers,
            .mask = initial_capacity - 1,
        };
    }

    pub fn deinit(self: *GlobalQueue) void {
        self.allocator.free(self.fibers);
    }

    pub fn capacity(self: *const GlobalQueue) usize {
        return self.mask + 1;
    }

    /// Append `f` to the back. Grows the ring (doubling) when full.
    /// Safe from any thread.
    pub fn push(self: *GlobalQueue, f: *Fiber) Allocator.Error!void {
        std.debug.assert(@intFromPtr(f) >= 0x1000);
        self.sl.lock();
        defer self.sl.unlock();

        if (self.size == self.capacity()) try self.grow();
        self.fibers[(self.head + self.size) & self.mask] = f;
        self.size += 1;
    }

    fn grow(self: *GlobalQueue) Allocator.Error!void {
        const new_cap = self.capacity() * 2;
        const new_fibers = try self.allocator.alloc(*Fiber, new_cap);

        // Compact live range into new buffer starting at index 0.
        // The old range may wrap, so check head + size vs capacity.
        const head_to_end = self.capacity() - self.head;
        if (self.size <= head_to_end) {
            // Contiguous in old buffer: one memcpy.
            @memcpy(new_fibers[0..self.size], self.fibers[self.head..][0..self.size]);
        } else {
            // Wraps. Copy [head..capacity) then [0..size - head_to_end).
            @memcpy(new_fibers[0..head_to_end], self.fibers[self.head..]);
            @memcpy(new_fibers[head_to_end..self.size], self.fibers[0 .. self.size - head_to_end]);
        }
        self.allocator.free(self.fibers);
        self.fibers = new_fibers;
        self.mask = new_cap - 1;
        self.head = 0;
    }

    /// Take up to `min(max_take, size, dest_room)` fibers from the
    /// front of the queue and write them directly into `dest`'s
    /// slot array, then advance `dest.tail` with a single
    /// release-store. Caller must own `dest`. Returns the count
    /// transferred.
    /// Pull up to `max_take` fibers from the front of the global
    /// queue into `dest`'s tail. `dest` can be either `LocalQueue`
    /// (lock-free Chase-Lev) or `LocalQueue2` (spinlock-protected)
    /// — both expose `push`/`capacity` so we just call `push` per
    /// fiber. Returns the count transferred (may be less than
    /// `max_take` if dest hits capacity mid-transfer).
    pub fn popBatchTo(
        self: *GlobalQueue,
        dest: anytype,
        max_take: usize,
    ) usize {
        self.sl.lock();
        defer self.sl.unlock();

        if (self.size == 0) return 0;
        const take = @min(max_take, self.size);
        var transferred: usize = 0;
        while (transferred < take) : (transferred += 1) {
            const src_idx = (self.head + transferred) & self.mask;
            const f = self.fibers[src_idx];
            std.debug.assert(@intFromPtr(f) >= 0x1000);
            dest.push(f) catch break; // dest hit capacity
        }
        self.head = (self.head + transferred) & self.mask;
        self.size -= transferred;
        return transferred;
    }

    /// Snapshot length. Takes the spinlock; intended for tests /
    /// stats, not for control flow on the hot path (`popBatchTo`'s
    /// return value already tells you if work was available).
    pub fn len(self: *GlobalQueue) usize {
        self.sl.lock();
        defer self.sl.unlock();
        return self.size;
    }
};
