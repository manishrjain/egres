const std = @import("std");
const zio = @import("zio");
const builtin = @import("builtin");
pub const compression = @import("compress.zig");
pub const hlc = @import("hlc.zig");

/// WaitGroup provides Go-style Add/Done/Wait synchronization for zio coroutines.
///
/// Usage:
///   var wg: WaitGroup = .{};
///   wg.init();  // must be called at final address (no moves after this)
///   wg.add(1);
///   // ... pass &wg to another coroutine/callback ...
///   try wg.wait();  // blocks until counter reaches 0
///
/// The callback side calls wg.done() to decrement the counter.
/// When the counter reaches zero, all coroutines blocked in wait() are unblocked.
///
/// Safe to call done() from any zio runtime or coroutine.
/// Not reusable — create a new WaitGroup for each synchronization point.
pub const WaitGroup = struct {
    counter: std.atomic.Value(i32) = std.atomic.Value(i32).init(0),
    ch_buf: [1]u8 = undefined,
    ch: zio.Channel(u8) = undefined,

    pub fn init(self: *WaitGroup) void {
        self.ch = zio.Channel(u8).init(&self.ch_buf);
    }

    pub fn add(self: *WaitGroup, delta: i32) void {
        const prev = self.counter.fetchAdd(delta, .acq_rel);
        if (prev + delta == 0) {
            self.ch.close(.graceful);
        }
    }

    pub fn done(self: *WaitGroup) void {
        self.add(-1);
    }

    pub fn isDone(self: *WaitGroup) bool {
        return self.counter.load(.acquire) == 0;
    }

    pub fn wait(self: *WaitGroup) !void {
        if (self.counter.load(.acquire) == 0) return;
        _ = self.ch.receive() catch return;
    }
};

// testPrint helps emit debugging output from tests without affecting non-test builds.
pub fn testPrint(comptime fmt: []const u8, args: anytype) void {
    if (!builtin.is_test) return;
    // std.debug.print(fmt, args);
    _ = fmt;
    _ = args;
}

/// Rate-limited log: each call site (unique comptime instantiation) logs at most once per interval.
pub fn logEvery(comptime interval_secs: i64, comptime fmt: []const u8, args: anytype) void {
    const S = struct {
        var last: i64 = 0;
    };
    const now = std.time.microTimestamp();
    if (now - S.last > interval_secs * 1_000_000) {
        std.log.debug(fmt, args);
        S.last = now;
    }
}

/// search finds the first index in [0, items.len) for which pred(ctx, items[i]) is true,
/// assuming the range is partitioned as [false…false, true…true].
/// Returns items.len when the predicate is never true (mirrors Go's sort.Search).
pub fn search(
    comptime T: type,
    items: []const T,
    ctx: anytype,
    comptime pred: fn (@TypeOf(ctx), T) bool,
) usize {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (pred(ctx, items[mid])) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return lo;
}

/// Lock-free, first-error-wins container. Safe to call set() from many threads;
/// only the first error is retained.
pub const ThreadSafeError = struct {
    raw: std.atomic.Value(u16) = std.atomic.Value(u16).init(0),

    pub fn set(self: *ThreadSafeError, e: anyerror) void {
        _ = self.raw.cmpxchgStrong(0, @intFromError(e), .acq_rel, .monotonic);
    }

    pub fn get(self: *const ThreadSafeError) ?anyerror {
        const v = self.raw.load(.acquire);
        if (v == 0) return null;
        return @errorFromInt(v);
    }
};

/// Heap-allocated, ref-counted byte buffer. Used to share payloads across
/// fan-out send paths (e.g. AE / file push) without per-peer memcpy: caller
/// wraps an owned slice in `RefValue.init`, then `incrRef` per peer and
/// hands each ref off to the transport queue. Each writer fiber `decrRef`s
/// after writing — when the count reaches zero, the data slice and the
/// RefValue itself are freed with the original allocator.
pub const RefValue = struct {
    data: []u8,
    allocator: std.mem.Allocator,
    ref: std.atomic.Value(i32),

    pub fn init(allocator: std.mem.Allocator, data: []u8) !*RefValue {
        const v = try allocator.create(RefValue);
        v.* = .{ .data = data, .allocator = allocator, .ref = std.atomic.Value(i32).init(1) };
        return v;
    }

    pub fn incrRef(self: *RefValue) *RefValue {
        _ = self.ref.fetchAdd(1, .monotonic);
        return self;
    }

    pub fn decrRef(self: *RefValue) void {
        const prev = self.ref.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
        if (prev == 1) {
            self.allocator.free(self.data);
            self.allocator.destroy(self);
        }
    }

    pub fn byteSize(self: *const RefValue) usize {
        return self.data.len;
    }
};

/// DEPRECATED — prefer `zio.Channel(T)` for new code. Channel covers every
/// use case this Queue does:
///   - fail-fast push/pop: `Channel.trySend` / `Channel.tryReceive`
///   - blocking push/pop: `Channel.send` / `Channel.receive` (yields fiber)
///   - bounded capacity with `error.ChannelFull` semantics
/// Benchmarks (see git history of benchmarks/queue_bench.zig) confirmed the
/// two are within a few percent on the fast path; Channel additionally has
/// targeted park-and-wake when callers genuinely need to block, and a
/// direct sender→receiver handoff path that this Queue can't replicate.
/// New callers: use `zio.Channel`. Existing callers: this is kept only for
/// any out-of-tree user; consider migrating.
///
/// Thread-safe FIFO queue: ArrayList-backed, grows on demand up to `max`.
/// `push` returns `error.QueueFull` past the cap; `pop` returns null when
/// empty. Non-blocking — callers wait externally if they need to. MUST be
/// used from a fiber (zio.Mutex).
pub fn Queue(comptime T: type) type {
    return struct {
        const Self = @This();
        pub const Error = error{ QueueClosed, QueueFull, OutOfMemory };

        allocator: std.mem.Allocator,
        items: std.ArrayList(T) = .{},
        head: usize = 0,
        closed: bool = false,
        max: usize,
        mutex: zio.Mutex = .init,

        pub fn init(allocator: std.mem.Allocator, max: usize) Self {
            return .{ .allocator = allocator, .max = max };
        }

        pub fn deinit(q: *Self) void {
            q.items.deinit(q.allocator);
            q.head = 0;
        }

        pub fn push(q: *Self, item: T) Error!void {
            q.mutex.lockUncancelable();
            defer q.mutex.unlock();
            if (q.closed) return error.QueueClosed;
            const live = q.items.items.len - q.head;
            if (live >= q.max) return error.QueueFull;
            try q.items.append(q.allocator, item);
        }

        pub fn pop(q: *Self) ?T {
            q.mutex.lockUncancelable();
            defer q.mutex.unlock();
            if (q.head >= q.items.items.len) return null;
            const item = q.items.items[q.head];
            q.head += 1;
            // Compact when at least half the underlying buffer is dead
            // (head consumes >= half). Keeps memory bounded and amortizes
            // the shift cost over the live entries.
            if (q.head * 2 >= q.items.items.len) {
                const live = q.items.items.len - q.head;
                if (live > 0) {
                    std.mem.copyForwards(T, q.items.items[0..live], q.items.items[q.head..]);
                }
                q.items.items.len = live;
                q.head = 0;
            }
            return item;
        }

        pub fn peek(q: *Self) ?T {
            q.mutex.lockUncancelable();
            defer q.mutex.unlock();
            if (q.head >= q.items.items.len) return null;
            return q.items.items[q.head];
        }

        pub fn count(q: *Self) usize {
            q.mutex.lockUncancelable();
            defer q.mutex.unlock();
            return q.items.items.len - q.head;
        }

        pub fn isClosed(q: *Self) bool {
            q.mutex.lockUncancelable();
            defer q.mutex.unlock();
            return q.closed;
        }

        pub fn close(q: *Self) void {
            q.mutex.lockUncancelable();
            defer q.mutex.unlock();
            q.closed = true;
        }
    };
}

/// Thread-safe wrapper around std.AutoHashMap. All operations are mutex-protected.
/// Init with `.init(allocator)`.
pub fn SafeHashMap(comptime K: type, comptime V: type) type {
    return struct {
        const Self = @This();
        const HashMap = std.AutoHashMap(K, V);

        map: HashMap,
        mu: std.Thread.Mutex = .{},

        pub fn init(allocator: std.mem.Allocator) Self {
            return .{ .map = HashMap.init(allocator) };
        }

        pub fn get(self: *Self, key: K) ?V {
            self.mu.lock();
            defer self.mu.unlock();
            return self.map.get(key);
        }

        pub fn put(self: *Self, key: K, value: V) !void {
            self.mu.lock();
            defer self.mu.unlock();
            try self.map.put(key, value);
        }

        pub fn remove(self: *Self, key: K) bool {
            self.mu.lock();
            defer self.mu.unlock();
            return self.map.remove(key);
        }

        pub fn fetchRemove(self: *Self, key: K) ?HashMap.KV {
            self.mu.lock();
            defer self.mu.unlock();
            return self.map.fetchRemove(key);
        }

        pub fn contains(self: *Self, key: K) bool {
            self.mu.lock();
            defer self.mu.unlock();
            return self.map.contains(key);
        }

        pub fn count(self: *Self) u32 {
            self.mu.lock();
            defer self.mu.unlock();
            return self.map.count();
        }

        pub fn deinit(self: *Self) void {
            self.map.deinit();
        }
    };
}

/// Run all pub test functions (fn names starting with "test") in a module
/// inside a zio runtime. Test functions must be `pub fn testXxx() !void`.
///
/// Usage in a test file:
///   test { try utils.runTestsInZioRuntime(@This()); }
pub fn runTestsInZioRuntime(comptime Module: type) !void {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    const Runner = struct {
        fn run() !void {
            const decls = @typeInfo(Module).@"struct".decls;
            inline for (decls) |d| {
                if (comptime std.mem.startsWith(u8, d.name, "test")) {
                    const func = @field(Module, d.name);
                    const F = @TypeOf(func);
                    if (F == fn () anyerror!void) {
                        std.debug.print("  running {s}...\n", .{d.name});
                        try func();
                    }
                }
            }
        }
    };

    var handle = try rt.spawn(Runner.run, .{});
    try handle.join();
}
