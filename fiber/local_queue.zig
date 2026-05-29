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
        if (h >= t) return .empty;

        const size: usize = @intCast(t - h);
        // ceil(size / 2): for size=1 we still claim 1.
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
