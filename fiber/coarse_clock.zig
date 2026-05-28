//! Process-wide coarse wall clock. One `xev.Timer` armed on the
//! lead worker's loop; fires every 1 ms; the callback reads
//! `std.time.milliTimestamp()` and publishes the result through an
//! atomic `cached_ms` that any fiber on any worker reads via `ms()`.
//!
//! Source is wall-clock (CLOCK_REALTIME) so the cached value is a
//! meaningful "ms since Unix epoch" for diagnostics + logs.  The
//! `onTick` callback applies a `@max` floor against the previously
//! cached value so the published clock can only ever move forward
//! — NTP step-back corrections don't make the cache walk backwards.
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

pub const ClockKind = enum {
    /// CLOCK_MONOTONIC — never goes backwards; correct source for
    /// measuring elapsed time (TSC calibration, deadlines).
    monotonic,
    /// CLOCK_REALTIME — wall clock; may step backwards on NTP
    /// adjustments. The right source for "what time is it now?"
    /// timestamps published to logs and metrics.
    realtime,
};

/// Read the requested clock as nanoseconds. Returns 0 on Windows
/// (would need `QueryPerformanceCounter` for monotonic and
/// `GetSystemTimeAsFileTime` for realtime — not implemented).
///
/// "Use rarely" — the call is a syscall (vDSO-accelerated on Linux,
/// ~10 ns), so it's fine in init/calibration paths or once-per-tick
/// reads, but the hot-path CPU accounting in `Worker.run` should
/// stick to `rdtsc` + `cyclesToNs` instead.
pub fn clockNsUseRarely(comptime kind: ClockKind) i64 {
    return switch (comptime builtin.os.tag) {
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
        self.cached_ms.store(@divTrunc(clockNsUseRarely(.realtime), std.time.ns_per_ms), .release);
    }

    /// Arm the first tick. Caller must have called `bind` first.
    /// Idempotent.
    ///
    /// Re-seeds `cached_ms` from the wall clock here, not just at
    /// `bind` time. The runtime can be `init`ed (which calls `bind`)
    /// well before workers actually `run` — e.g., the test runner
    /// initializes the fiber runtime up front for watchdog
    /// visibility but only starts workers in Phase B. Without this
    /// re-seed, the first reader after start sees a `cached_ms`
    /// frozen at init time, and the first tick then jumps it forward
    /// by however long Phase A took (seconds), producing absurd
    /// elapsed deltas for the unlucky reader.
    pub fn start(self: *CoarseClock) void {
        if (self.running) return;
        self.cached_ms.store(@divTrunc(clockNsUseRarely(.realtime), std.time.ns_per_ms), .release);
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
        // Wall-clock source (CLOCK_REALTIME) so the timestamp is
        // meaningful as an absolute "ms since Unix epoch" value to
        // diagnostics + logs. Single-writer (lead worker's tick),
        // so the load+max+store sequence is race-free.
        //
        // The @max defense protects against backwards NTP jumps:
        // if the wall clock steps back, the cache holds at the
        // previous higher value until real time catches up.
        const now = @divTrunc(clockNsUseRarely(.realtime), std.time.ns_per_ms);
        const prev = self.cached_ms.load(.acquire);
        self.cached_ms.store(@max(prev, now), .release);
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
