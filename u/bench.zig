//! Go-style benchmark harness for `BENCH: …` -prefixed tests.
//!
//! Usage in a test file:
//!
//!     fn benchHash(b: *bench.Bench, size: usize) !void {
//!         const buf = try std.testing.allocator.alloc(u8, size);
//!         defer std.testing.allocator.free(buf);
//!         fillRandom(buf);
//!         b.setBytes(buf.len);
//!         b.resetTimer();         // discount the setup above
//!         var i: u64 = 0;
//!         while (i < b.n) : (i += 1) {
//!             const h = hashFn(buf);
//!             std.mem.doNotOptimizeAway(h);
//!         }
//!     }
//!
//!     test "BENCH: hash 4KiB" { try benchHash(bench.current(), 4096); }
//!
//! The runner calls each `BENCH: …` test repeatedly with growing `b.n`
//! until a sample exceeds a target duration (~200ms). It reads the timer
//! the user controlled (`b.resetTimer()` / `b.stopTimer()` / `b.startTimer()`)
//! and reports `ns/op` (+ `MiB/s` when `setBytes` was called).

const std = @import("std");
const fiber = @import("fiber");
const context = @import("context.zig");

inline fn nowNs() i128 {
    return std.Io.Timestamp.now(context.g().io, .real).nanoseconds;
}

pub const Bench = struct {
    /// Iteration count for the current sample. Set by the runner before
    /// each call. Loop with `var i: u64 = 0; while (i < b.n) : (i += 1) { … }`.
    n: u64 = 1,

    /// Allocator for setup/teardown inside the benchmark. The runner
    /// supplies `std.heap.page_allocator` here — no leak-tracking
    /// overhead, no per-test scope. Free what you allocate.
    alloc: std.mem.Allocator = std.heap.page_allocator,

    /// Cumulative bytes processed across the current sample, populated by
    /// the user calling `b.addBytes(n)`. Reset by the runner before each
    /// sample. If non-zero at the end of the sample, the runner reports
    /// MiB/s alongside ns/op.
    total_bytes: u64 = 0,

    // Timer state. `start_ns` is when the current timed region began;
    // `elapsed_ns` is accumulated time from prior timed regions (when
    // stopTimer/startTimer was used). Together they track "wall time the
    // user wanted measured", which the runner reads via `elapsed()`.
    start_ns: i128 = 0,
    elapsed_ns: i128 = 0,
    timer_running: bool = false,

    /// Discard any time elapsed so far and start the timer fresh from now.
    /// Call after setup work so it isn't counted toward the measurement.
    pub fn resetTimer(self: *Bench) void {
        self.elapsed_ns = 0;
        self.start_ns = nowNs();
        self.timer_running = true;
    }

    /// Pause the timer. Subsequent work won't count until `startTimer()`.
    pub fn stopTimer(self: *Bench) void {
        if (!self.timer_running) return;
        self.elapsed_ns += nowNs() - self.start_ns;
        self.timer_running = false;
    }

    /// Resume the timer. Pairs with a prior `stopTimer()`.
    pub fn startTimer(self: *Bench) void {
        if (self.timer_running) return;
        self.start_ns = nowNs();
        self.timer_running = true;
    }

    /// Add `n` bytes to the cumulative total for this sample. Call from
    /// inside the bench loop (or once after, with the running sum) to tell
    /// the runner how much data was processed — used for the MiB/s
    /// throughput column. The runner resets this between samples.
    ///
    /// Marked `inline` so the call vanishes at every site in ReleaseFast
    /// builds — the per-iter overhead becomes a single `add` to the
    /// `total_bytes` field, comparable to a counter increment the user
    /// would write by hand.
    pub inline fn addBytes(self: *Bench, n: u64) void {
        self.total_bytes += n;
    }

    /// Total time the user-controlled timer has been running. Read by the
    /// runner after the bench function returns.
    pub fn elapsed(self: *const Bench) i128 {
        if (self.timer_running) {
            return self.elapsed_ns + (nowNs() - self.start_ns);
        }
        return self.elapsed_ns;
    }
};

// ---------------------------------------------------------------------------
// Fiber-local registry — same pattern as the test runner's FiberLog.
// ---------------------------------------------------------------------------

var map_mu: std.Io.Mutex = .init;

inline fn mapLock() void {
    map_mu.lockUncancelable(context.g().io);
}
inline fn mapUnlock() void {
    map_mu.unlock(context.g().io);
}
var map: ?std.AutoHashMap(usize, *Bench) = null;

pub fn initRegistry(alloc: std.mem.Allocator) void {
    map = std.AutoHashMap(usize, *Bench).init(alloc);
}

pub fn deinitRegistry() void {
    mapLock();
    defer mapUnlock();
    if (map) |*m| m.deinit();
    map = null;
}

pub fn register(b: *Bench) void {
    const task = fiber.currentFiberOrNull() orelse return;
    mapLock();
    defer mapUnlock();
    if (map) |*m| m.put(@intFromPtr(task), b) catch {};
}

pub fn unregister() void {
    const task = fiber.currentFiberOrNull() orelse return;
    mapLock();
    defer mapUnlock();
    if (map) |*m| _ = m.remove(@intFromPtr(task));
}

/// Fetch the current fiber's `Bench` handle. The runner sets this up
/// before calling each `BENCH: …` test. Panics if called outside one.
pub fn current() *Bench {
    const task = fiber.currentFiberOrNull() orelse @panic("bench.current() requires a fiber context");
    mapLock();
    defer mapUnlock();
    if (map) |*m| {
        return m.get(@intFromPtr(task)) orelse @panic("bench.current() called outside a BENCH: test");
    }
    @panic("bench registry not initialized");
}
