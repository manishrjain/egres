//! Process-wide coarse monotonic clock. One `xev.Timer` armed on the
//! lead worker's loop; fires every 1 ms; the callback reads
//! `loop.now()` and publishes the result through an atomic
//! `cached_ms` that any fiber on any worker reads via `ms()`.
//!
//! libxev's `Loop.now()` and `cached_now` aren't thread-safe: the
//! `cached_now = ts` assignment is two non-atomic 64-bit stores and
//! `flags.now_outdated` is a plain bool. Single-thread is fine;
//! cross-thread reads can witness torn values. The atomic side-steps
//! that for the consumer-facing API.
//!
//! Per-worker deadline heaps for I/O timeouts live in `Deadlines`
//! (`fiber/deadlines.zig`), not here — cancellation has to run on the
//! loop that owns the op, which forces a per-worker design.
//!
//! Staleness contract: `ms()` is at most 1 ms stale *under cooperative
//! scheduling* — i.e., as long as the lead worker calls `loop.run`
//! roughly every 1 ms. A CPU-bound fiber that doesn't yield can
//! stall the clock for the duration of its run. Same property as
//! the existing `u/coarse.zig`; same fix when needed (have the
//! offender yield periodically).
//!
//! Lifecycle: the lead worker calls `start` on entering its run loop
//! and `stop` on exit. Peer workers do not arm the tick — they only
//! read `ms()`.

const std = @import("std");
const builtin = @import("builtin");
const xev = @import("xev");

pub const ClockKind = enum { monotonic, realtime };

/// Read the requested clock as milliseconds. Returns 0 on Windows
/// (would need `QueryPerformanceCounter` / `GetSystemTimeAsFileTime`).
///
/// "Use rarely" — the call is a syscall (vDSO-accelerated on Linux,
/// ~10 ns), fine in init/calibration paths or once-per-tick reads, but
/// the hot-path CPU accounting in `Worker.run` should stick to `rdtsc`.
/// Direct CLOCK_MONOTONIC, no caching — unlike `loop.now()`, which can
/// be arbitrarily stale between `loop.run` iterations under load.
pub fn clockMsUseRarely(comptime kind: ClockKind) i64 {
    const ns: i64 = switch (comptime builtin.os.tag) {
        .windows => 0,
        else => blk: {
            const clk = switch (kind) {
                .monotonic => std.posix.CLOCK.MONOTONIC,
                .realtime => std.posix.CLOCK.REALTIME,
            };
            var tp: std.posix.timespec = undefined;
            _ = std.posix.system.clock_gettime(clk, &tp);
            break :blk @as(i64, tp.sec) * std.time.ns_per_s + tp.nsec;
        },
    };
    return @divTrunc(ns, std.time.ns_per_ms);
}

pub const CoarseClock = struct {
    /// Loop that this clock arms its timer on. Set by `bind` before
    /// `start` is called; null while the lead worker hasn't claimed
    /// the clock yet.
    loop: ?*xev.Loop = null,

    /// Process-wide coarse-clock cache. Single writer (lead worker's
    /// tick callback), many readers (any fiber on any worker via
    /// `ms()`). Own cache line so reader fan-out doesn't invalidate
    /// neighboring fields on the lead worker every 1 ms.
    cached_ms: std.atomic.Value(i64) align(std.atomic.cache_line) = .init(0),

    timer: xev.Timer,
    completion: xev.Completion = undefined,
    /// `start()` flips this to true; `stop()` flips it back. The
    /// callback re-arms only while this is true.
    running: bool = false,

    pub fn init() !CoarseClock {
        return .{ .timer = try xev.Timer.init() };
    }

    pub fn deinit(self: *CoarseClock) void {
        self.timer.deinit();
    }

    /// Attach to a worker's loop and seed the atomic clock cache so
    /// `ms()` returns a valid value even before the first tick fires.
    /// Must happen once during runtime init.
    pub fn bind(self: *CoarseClock, loop: *xev.Loop) void {
        self.loop = loop;
        // Safe — bind runs on the same thread that owns the loop,
        // before any cross-thread reads exist.
        self.cached_ms.store(loop.now(), .release);
    }

    /// Arm the first tick. Caller must have called `bind` first.
    /// Idempotent.
    pub fn start(self: *CoarseClock) void {
        if (self.running) return;
        self.running = true;
        self.arm();
    }

    /// Stop ticking. The callback will fire one more time, observe
    /// `running = false`, and decline to re-arm.
    pub fn stop(self: *CoarseClock) void {
        self.running = false;
    }

    /// Coarse milliseconds since some monotonic epoch. One atomic
    /// load — cheap on every call, safe from any worker thread.
    /// Stale by up to 1 ms; never torn.
    pub inline fn ms(self: *const CoarseClock) i64 {
        return self.cached_ms.load(.acquire);
    }

    fn arm(self: *CoarseClock) void {
        self.timer.run(
            self.loop.?,
            &self.completion,
            1,
            CoarseClock,
            self,
            tickCallback,
        );
    }

    fn onTick(self: *CoarseClock) void {
        // `loop.now()` runs on the lead worker's thread (same one
        // driving the loop), so the un-synchronized libxev cache is
        // fine *here*. We immediately publish through our atomic so
        // cross-thread readers see a consistent value.
        const now = self.loop.?.now();
        self.cached_ms.store(now, .release);
    }
};

fn tickCallback(
    ud: ?*CoarseClock,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.Timer.RunError!void,
) xev.CallbackAction {
    const c = ud.?;
    c.onTick();
    if (c.running) c.arm();
    return .disarm;
}
