// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Shutdown — a two-stage "shutdown has begun" signal that is pollable,
//! blockable, and timed-blockable, with optional SIGINT/SIGTERM wiring.
//!
//! The stages, each entered at most once and never left:
//!   draining  no new work is taken, work in flight finishes. Listeners
//!             stop accepting, `Ctx.attach` refuses new statements, and
//!             the loops keep running so the statements in flight
//!             commit on their own.
//!   stopping  every loop exits, and `Ctx.maybeYield` stops whatever is
//!             still running. `startShutdown` begins it, `isStopping`
//!             and `waitForStart(.shutdown, ..)` observe it.
//! A signal enters draining; a second signal, a fatal path, or the
//! binary once the drain is over enters stopping. Stopping implies
//! draining, so a fatal path skips the drain by starting both.
//!
//! One instance lives at process scope (the raft Node owns it): fatal paths
//! call `startShutdown()`, hot loops poll `isStopping()`, fibers that
//! must sleep until shutdown and then act block in
//! `waitForStart(.shutdown, null)`, and periodic background loops
//! replace `sleepMs(interval)` with `waitForStart(.shutdown, interval)`
//! so they keep their cadence but react to shutdown instantly.
//!
//! Built on a `Watermark` that advances once, to 1, when draining
//! starts: a drain waiter parks on target 1, a shutdown waiter on
//! target 2, which is never reached, so their wait ends only via
//! `close()` (= startShutdown) or its own timeout — and the Watermark's
//! timeout/claim machinery is the battle-tested path for exactly that
//! race.
//!
//! Signal path: a POSIX handler may only use async-signal-safe operations —
//! no spinlocks, no rt.wake (pushes onto growable spinlocked worker rings),
//! no allocation — so it cannot call `startShutdown()` directly. The
//! handler just stores a process-wide atomic flag; a watcher fiber polls
//! it and fires `startShutdown()` from fiber context.

const std = @import("std");
const builtin = @import("builtin");
const _rt = @import("runtime.zig");
const _watermark = @import("watermark.zig");
const sleepMs = @import("deadlines.zig").sleepMs;

/// How often the signal watcher checks the handler's flag. Bounds both
/// Ctrl-C latency and how long `deinit`'s join can take.
const poll_ms: u64 = 300;

/// How often `waitForActiveCtxs` re-reads the count.
const ctx_poll_ms: u64 = 20;

/// Watermark targets: reached at drain, and never reached.
const drain_target: u64 = 1;
const stop_target: u64 = 2;

pub const Shutdown = struct {
    /// See the module doc. Only ever rises; read by `Ctx.maybeYield` on
    /// every statement's yield points, so it is an atomic, not a lock.
    stage: std.atomic.Value(u8) = .init(@intFromEnum(Stage.running)),
    /// Contexts attached and not yet detached (`enterCtx`/`leaveCtx`,
    /// called by `Ctx.attach`/`detach` on the installed instance): the
    /// statements in flight, what the drain waits on. The only record
    /// of them; the contexts themselves are never listed.
    active_ctxs: std.atomic.Value(u32) = .init(0),
    wm: _watermark.Watermark,
    watcher: ?_rt.JoinHandle(void) = null,

    pub const Stage = enum(u8) { running, draining, stopping };

    /// Which start a waiter is after: `startDrain` or `startShutdown`.
    pub const Start = enum { drain, shutdown };

    pub fn init(allocator: std.mem.Allocator) Shutdown {
        return .{ .wm = _watermark.Watermark.init(allocator) };
    }

    /// Start first (idempotent) so the watcher's next poll sees it and
    /// exits, then join it — nothing stays parked on freed state.
    /// An installed instance uninstalls itself: a `maybeYield` after
    /// this must not find a freed one.
    pub fn deinit(self: *Shutdown) void {
        self.startShutdown();
        if (self.watcher) |*h| h.join();
        self.wm.deinit();
        _ = process_shutdown.cmpxchgStrong(self, null, .seq_cst, .seq_cst);
    }

    /// Begin shutdown: flip `isStopping` and wake every waiter. Idempotent.
    /// Stopping implies draining, so a drain waiter wakes too. seq_cst:
    /// `Ctx.attach` pairs its counter with this stage.
    pub fn startShutdown(self: *Shutdown) void {
        self.stage.store(@intFromEnum(Stage.stopping), .seq_cst);
        self.wm.close();
    }

    /// Begin the drain stage: `isDraining` flips and a `.drain` waiter
    /// returns, while every shutdown waiter stays parked. Idempotent, and
    /// a no-op once stopping, which never goes back.
    pub fn startDrain(self: *Shutdown) void {
        _ = self.stage.cmpxchgStrong(
            @intFromEnum(Stage.running),
            @intFromEnum(Stage.draining),
            .seq_cst,
            .seq_cst,
        ) orelse self.wm.advance(drain_target);
    }

    pub fn isStopping(self: *const Shutdown) bool {
        return self.currentStage() == .stopping;
    }

    /// True from the drain stage on: stopping implies draining.
    pub fn isDraining(self: *const Shutdown) bool {
        return self.currentStage() != .running;
    }

    pub fn currentStage(self: *const Shutdown) Stage {
        return @enumFromInt(self.stage.load(.seq_cst));
    }

    /// Count a context in: true = counted, the statement may run;
    /// false = refused, the process is draining or stopping. The count
    /// goes up BEFORE the stage is read, and `startDrain`/`startShutdown`
    /// set the stage before `waitForActiveCtxs` reads the count: with
    /// those operations seq_cst, a statement is either refused here or
    /// seen by the drain, never missed. The rest of the counter and
    /// stage traffic is seq_cst too, for uniformity; it runs once a
    /// statement.
    pub fn enterCtx(self: *Shutdown) bool {
        _ = self.active_ctxs.fetchAdd(1, .seq_cst);
        if (self.currentStage() == .running) return true;
        _ = self.active_ctxs.fetchSub(1, .seq_cst);
        return false;
    }

    pub fn leaveCtx(self: *Shutdown) void {
        _ = self.active_ctxs.fetchSub(1, .seq_cst);
    }

    pub fn activeCtxs(self: *const Shutdown) u32 {
        return self.active_ctxs.load(.seq_cst);
    }

    /// Wait until no context is counted in, up to `timeout_ms`: true =
    /// none left, false = timed out. Polled: a `leaveCtx` is not
    /// signalled, and this runs a couple of times per process. A change
    /// of stage while waiting (a second signal moving the drain to the
    /// stop) ends the wait early with false, so the caller can act on
    /// the new stage.
    pub fn waitForActiveCtxs(self: *Shutdown, timeout_ms: u64) bool {
        const stage_at_entry = self.currentStage();
        var waited: u64 = 0;
        while (self.activeCtxs() > 0) {
            if (waited >= timeout_ms) return false;
            sleepMs(ctx_poll_ms);
            waited += ctx_poll_ms;
            if (self.currentStage() != stage_at_entry) return false;
        }
        return true;
    }

    /// Block until that stage has started, or for `timeout_ms` when one
    /// is given: true = started (before or during the wait), false =
    /// timed out. Returns immediately if it already has. The blocking
    /// complement of polling `isDraining` and `isStopping`; a shutdown
    /// start also ends a drain waiter's wait, since stopping implies
    /// draining. With a timeout it is the shutdown-aware replacement for
    /// `sleepMs` in periodic loops:
    ///     if (node.shutdown.waitForStart(.shutdown, interval_ms)) return;
    pub fn waitForStart(
        self: *Shutdown,
        start: Start,
        timeout_ms: ?u64,
    ) bool {
        const target: u64 = switch (start) {
            .drain => drain_target,
            .shutdown => stop_target,
        };
        while (!self.started(start)) {
            if (timeout_ms) |ms| {
                self.wm.waitFor(target, ms) catch |err| switch (err) {
                    error.Closed => return true,
                    error.Timeout => return false,
                    // Degrade to one poll, then re-check the stage.
                    error.OutOfMemory => sleepMs(@min(ms, poll_ms)),
                };
                // A reached target (the drain's) or the OOM fallback: the
                // loop re-checks the stage and either returns or waits on.
                continue;
            }
            self.wm.waitForUntimed(target) catch |err| switch (err) {
                error.Closed => return true,
                error.OutOfMemory => sleepMs(poll_ms), // degrade to polling
            };
        }
        return true;
    }

    fn started(self: *const Shutdown, start: Start) bool {
        return switch (start) {
            .drain => self.isDraining(),
            .shutdown => self.isStopping(),
        };
    }

    /// Install SIGINT/SIGTERM handlers: the first signal starts the drain,
    /// a second one `startShutdown()`s. Call once, from the binary that owns
    /// process lifetime, inside the fiber runtime. This instance then also
    /// counts every statement's context and answers its `maybeYield`.
    /// POSIX-only; Windows console-ctrl handling is a TODO.
    pub fn install(self: *Shutdown) !void {
        if (builtin.os.tag == .windows) return;
        // A signal an earlier instance took (tests) must not fire on this
        // one.
        g_signal.store(0, .release);
        process_shutdown.store(self, .seq_cst);
        self.watcher = try _rt.spawn(.{}, watchSignal, .{self});

        var sa = std.posix.Sigaction{
            .handler = .{ .handler = signalHandler },
            .mask = std.posix.sigemptyset(),
            .flags = std.posix.SA.RESTART,
        };
        std.posix.sigaction(std.posix.SIG.INT, &sa, null);
        std.posix.sigaction(std.posix.SIG.TERM, &sa, null);
    }

    /// Polls the handler's count and fires `startDrain()` on the first
    /// signal and `startShutdown()` on the second, from fiber context;
    /// also exits once shutdown started by any other path (checked
    /// first, so a pre-fired signal or start is seen without sleeping).
    fn watchSignal(self: *Shutdown) void {
        while (!self.isStopping()) {
            const signals = g_signal.load(.acquire);
            if (signals >= 2) {
                self.startShutdown();
                return;
            }
            if (signals == 1) self.startDrain();
            sleepMs(poll_ms);
        }
    }
};

/// Process-wide: a signal handler can't carry context, so the count is a
/// module global no matter how many instances exist (only one is expected
/// to call `install`). Saturates, so a held-down Ctrl-C never wraps to 0.
var g_signal: std.atomic.Value(u8) = .init(0);

/// The instance that called `install`: it counts every statement's
/// context (`Ctx.attach`) and its stage answers `Ctx.maybeYield`. Null
/// until then, as in a test, and neither ever fails. Atomic: the
/// listeners may be running statements by the time it is set.
var process_shutdown: std.atomic.Value(?*Shutdown) = .init(null);

pub fn installed() ?*Shutdown {
    return process_shutdown.load(.seq_cst);
}

fn signalHandler(_: std.posix.SIG) callconv(.c) void {
    var n = g_signal.load(.acquire);
    while (n < 2) {
        n = g_signal.cmpxchgWeak(n, n + 1, .release, .acquire) orelse
            return;
    }
}
