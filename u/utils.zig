const std = @import("std");
const zio = @import("zio");
const builtin = @import("builtin");
pub const compression = @import("compress.zig");

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

// Queue blocks producers when full and consumers when empty.
// Queue is thread safe.
// TODO: Switch with a channel which works nicely with zio.
pub fn Queue(comptime T: type, comptime capacity: usize) type {
    return struct {
        const Self = @This();
        pub const Error = error{QueueClosed};

        buf: [capacity]T = undefined,
        head: usize = 0,
        len: usize = 0,
        closed: bool = false,
        mutex: std.Thread.Mutex = .{},
        not_empty: std.Thread.Condition = .{},
        not_full: std.Thread.Condition = .{},

        pub fn pushOrClosed(q: *Self, item: T) Error!void {
            q.mutex.lock();
            defer q.mutex.unlock();

            while (q.len == capacity and !q.closed) {
                q.not_full.wait(&q.mutex);
            }
            if (q.closed) return error.QueueClosed;

            q.buf[(q.head + q.len) % capacity] = item;
            q.len += 1;
            q.not_empty.signal();
        }

        pub fn tryPush(q: *Self, item: T) bool {
            q.mutex.lock();
            defer q.mutex.unlock();

            if (q.closed or q.len == capacity) return false;
            q.buf[(q.head + q.len) % capacity] = item;
            q.len += 1;
            q.not_empty.signal();
            return true;
        }

        pub fn popOrNull(q: *Self) ?T {
            q.mutex.lock();
            defer q.mutex.unlock();

            while (q.len == 0 and !q.closed) {
                q.not_empty.wait(&q.mutex);
            }
            if (q.len == 0) return null;

            const item = q.buf[q.head];
            q.head = (q.head + 1) % capacity;
            q.len -= 1;
            q.not_full.signal();
            return item;
        }

        pub fn tryPop(q: *Self) ?T {
            q.mutex.lock();
            defer q.mutex.unlock();

            if (q.len == 0) return null;
            const item = q.buf[q.head];
            q.head = (q.head + 1) % capacity;
            q.len -= 1;
            q.not_full.signal();
            return item;
        }

        pub fn peek(q: *Self) ?T {
            q.mutex.lock();
            defer q.mutex.unlock();
            if (q.len == 0) return null;
            return q.buf[q.head];
        }

        pub fn count(q: *Self) usize {
            q.mutex.lock();
            defer q.mutex.unlock();
            return q.len;
        }

        pub fn isClosed(q: *Self) bool {
            q.mutex.lock();
            defer q.mutex.unlock();
            return q.closed;
        }

        pub fn close(q: *Self) void {
            q.mutex.lock();
            defer q.mutex.unlock();
            if (q.closed) return;
            q.closed = true;
            q.not_empty.broadcast();
            q.not_full.broadcast();
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
