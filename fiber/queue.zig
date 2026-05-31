const std = @import("std");
const Allocator = std.mem.Allocator;
const Fiber = @import("fiber.zig").Fiber;
const Spinlock = @import("spinlock.zig").Spinlock;

/// Per-worker, per-priority ready queue: a fixed-size ring buffer of
/// `*Fiber`. There is no work stealing, so this is touched ONLY by its
/// owning worker thread — no atomics, no locking. Capacity is fixed at
/// `local_queue_capacity`. `push` returns `error.Full` when the ring is
/// full; the caller decides what to do: spill normal-priority fibers to
/// the `GlobalQueue`, or panic for high priority (high-priority work is
/// pinned and expected to fit). See `Worker.scheduleLocal`.
pub const local_queue_capacity: usize = 16;

pub const LocalQueue = struct {
    comptime {
        std.debug.assert(std.math.isPowerOfTwo(local_queue_capacity));
    }
    const mask: usize = local_queue_capacity - 1;

    // Slot index of the next pop is `head`; the live range is
    // `[head, head + size)` taken `& mask`. `tail` is derivable so it
    // isn't stored.
    fibers: [local_queue_capacity]*Fiber = undefined,
    head: usize = 0,
    size: usize = 0,

    /// Append `f` to the back. Returns `error.Full` if the ring is at
    /// capacity (caller handles the spill/panic policy).
    pub fn push(self: *LocalQueue, f: *Fiber) error{Full}!void {
        std.debug.assert(@intFromPtr(f) >= 0x1000);
        if (self.size == local_queue_capacity) return error.Full;
        self.fibers[(self.head + self.size) & mask] = f;
        self.size += 1;
    }

    /// Remove and return the front fiber, or null if empty.
    pub fn pop(self: *LocalQueue) ?*Fiber {
        if (self.size == 0) return null;
        const f = self.fibers[self.head];
        self.head = (self.head + 1) & mask;
        self.size -= 1;
        return f;
    }

    pub fn len(self: *const LocalQueue) usize {
        return self.size;
    }

    pub fn capacity(_: *const LocalQueue) usize {
        return local_queue_capacity;
    }

    pub fn isEmpty(self: *const LocalQueue) bool {
        return self.size == 0;
    }
};

/// Lock-free Treiber stack for cross-thread fiber injection. Push is
/// one CAS; drain is one atomic swap. Used ONLY for high-priority
/// cross-thread tasks targeted at a specific worker (normal-priority
/// cross-thread tasks go through the `GlobalQueue` instead). Pushed to
/// by peer workers / external threads; drained by the home worker at
/// the top of every outer-loop iteration.
pub const RemoteQueue = struct {
    head: std.atomic.Value(?*Fiber) align(std.atomic.cache_line) = .init(null),

    pub fn push(self: *RemoteQueue, f: *Fiber) void {
        var current = self.head.load(.monotonic);
        while (true) {
            f.remote_next = current;
            if (self.head.cmpxchgWeak(current, f, .release, .monotonic)) |actual| {
                current = actual;
                continue;
            }
            return;
        }
    }

    /// Drain the whole stack and return it oldest-first (FIFO), or null
    /// if empty. The Treiber-stack swap yields newest-first; we reverse
    /// in place via `remote_next` so a burst pushed by a peer keeps its
    /// spawn order. The caller walks `remote_next`.
    pub fn drainFifo(self: *RemoteQueue) ?*Fiber {
        var lifo = self.head.swap(null, .acquire);
        var fifo: ?*Fiber = null;
        while (lifo) |f| {
            lifo = f.remote_next;
            f.remote_next = fifo;
            fifo = f;
        }
        return fifo;
    }
};

/// Largest worker count we support; bounds the idle bitmap. Worker ids
/// are `u8`, so 256 is the ceiling.
pub const max_workers: usize = 256;
const idle_words: usize = (max_workers + 63) / 64;

/// Idle-worker bitmap for one lane. Bit `i` set ⇒ the worker at
/// lane-relative offset `i` is parked in `loop.run(.once)` waiting for
/// work. A producer that pushes to the lane's `GlobalQueue` (or injects
/// a task) calls `claim` to atomically take one idle worker's bit, then
/// wakes exactly that peer — see `Runtime.notifyIdlePeer`. Lane-agnostic
/// (indexes by offset within a lane) so it unit-tests standalone.
///
/// **Why CAS-claim?** Without it, two producers seeing the same set bit
/// would both notify the same peer. The CAS-claim ensures one producer
/// clears one bit and wakes one peer; a racing producer either claims a
/// different bit or gets `null`. Each worker only ever sets its own bit;
/// producers (and the worker itself, on wake) only clear it, so the
/// set/clear race is benign — a spurious wake just re-checks and re-parks.
pub const IdleWorkers = struct {
    words: [idle_words]std.atomic.Value(u64) align(std.atomic.cache_line) = @splat(.init(0)),

    pub fn markIdle(self: *IdleWorkers, i: u8) void {
        const w = @as(usize, i) >> 6;
        const bit = @as(u64, 1) << @as(u6, @intCast(i & 63));
        _ = self.words[w].fetchOr(bit, .release);
    }

    pub fn clearIdle(self: *IdleWorkers, i: u8) void {
        const w = @as(usize, i) >> 6;
        const bit = @as(u64, 1) << @as(u6, @intCast(i & 63));
        _ = self.words[w].fetchAnd(~bit, .release);
    }

    /// Atomically claim (clear) one set bit and return its offset, or
    /// null if no worker is idle.
    pub fn claim(self: *IdleWorkers) ?u8 {
        for (&self.words, 0..) |*word, wi| {
            var m = word.load(.acquire);
            while (m != 0) {
                const bit_i: u6 = @intCast(@ctz(m));
                const bit = @as(u64, 1) << bit_i;
                if (word.cmpxchgWeak(m, m & ~bit, .acq_rel, .acquire)) |actual| {
                    m = actual;
                    continue;
                }
                return @intCast(wi * 64 + bit_i);
            }
        }
        return null;
    }

    pub fn isIdle(self: *IdleWorkers, i: u8) bool {
        const w = @as(usize, i) >> 6;
        const bit = @as(u64, 1) << @as(u6, @intCast(i & 63));
        return (self.words[w].load(.acquire) & bit) != 0;
    }
};

/// Runtime-wide overflow / admission queue. Spinlock-protected ring
/// buffer of `*Fiber`. Capacity starts at `initial_capacity` and
/// doubles on push when full. One per `Lane`; holds **normal-priority
/// tasks only**. Used for:
///
///   - Bootstrap spawns from a non-worker thread (the host thread
///     calling `fiber.spawn` before `Runtime.run` has been entered).
///   - Off-worker / cross-lane normal spawns and wakes.
///   - Spillover from a worker whose normal `LocalQueue` is full.
///
/// Multi-producer, multi-consumer; producers (`push`) and consumers
/// (`popBatchTo`) all serialize on the same spinlock.
///
/// Not a hot path. Workers prefer their own local queue first and
/// consult the global queue only when it is empty.
pub const GlobalQueue = struct {
    allocator: Allocator,
    sl: Spinlock = .{},

    // Ring buffer for *Fiber. Capacity is a power of two so the slot
    // index is `(head + i) & mask`. All access under `sl`, so none of
    // these need atomic ops. `tail` is derivable as `(head + size) &
    // mask` and isn't stored.
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

    /// Pull up to `max_take` fibers from the front of the global queue
    /// into `dest`'s tail. `dest` exposes `push`/`capacity` (the
    /// per-worker `LocalQueue`), so we just call `push` per fiber.
    /// Returns the count transferred (may be less than `max_take` if
    /// `dest` hits capacity mid-transfer). Caller must own `dest`.
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

    /// Snapshot length. Takes the spinlock; intended for tests / stats,
    /// not for control flow on the hot path (`popBatchTo`'s return value
    /// already tells you if work was available).
    pub fn len(self: *GlobalQueue) usize {
        self.sl.lock();
        defer self.sl.unlock();
        return self.size;
    }
};
