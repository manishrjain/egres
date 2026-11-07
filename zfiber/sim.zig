// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Deterministic simulation mode of the runtime (`InitOptions.sim`).
//! One worker runs every fiber, the next fiber is drawn from the ready
//! rings by a seeded PRNG, and the clock is virtual: `Deadlines` read
//! it, `sleepMs` parks on it, and the loop jumps it to the next
//! deadline when nothing is runnable. A run with one seed replays the
//! same dispatch order; `traceHash` is the fingerprint two runs of the
//! same seed must share. `Sim.run` is the loop, driving the worker's
//! steps (`runFiber`, `chargeCpu`, the rings and deadlines) from here.
//!
//! The pick is random, not FIFO, on purpose: FIFO is deterministic too
//! but every seed would produce the one natural schedule, and the bugs
//! a simulation exists to find are the ones where fiber B runs before
//! fiber A although A became ready first.

const std = @import("std");
const builtin = @import("builtin");
const xev = @import("xev");
const _coarse = @import("utils").coarse_clock;
const _fb = @import("fiber.zig");
const _queue = @import("queue.zig");
const _rt = @import("runtime.zig");
const _simdisk = @import("simdisk.zig");

const Fiber = _fb.Fiber;
const Priority = _fb.Priority;
const Ring = _queue.LocalQueue(.withlock);

/// What one fiber step costs the simulation's cpu meter on average, in
/// the tick unit the tenants' limits are converted to
/// (`ticksPerSecond`): a few microseconds of a real core. `stepCost`
/// draws around it.
pub const step_ticks: u64 = 10_000;

/// How the next fiber is drawn.
pub const Sched = enum {
    /// One draw over every ready fiber in both rings, so a ring is
    /// chosen in proportion to its length and priority is a hint the
    /// pick ignores: a run that holds its invariants under it is the
    /// stronger result, and a client step can land between two raft
    /// steps, which strict priority never allows while raft has work.
    random,
    /// Production's rule, high before normal, random within the ring.
    /// Tells a bug that needs the inversion from one that does not.
    priority_random,
    /// Production's exact order, high before normal and FIFO within the
    /// ring: the baseline, and a production-like schedule to reproduce.
    priority_fifo,
};

pub const Options = struct {
    seed: u64,
    sched: Sched = .random,
    /// Where the virtual clock starts, ms since the Unix epoch:
    /// 2026-01-01T00:00:00Z, so timestamps read as dates. Fixed, so a
    /// replay sees the same clock.
    epoch_ms: i64 = 1_767_225_600_000,
    /// Real milliseconds an idle runtime (nothing ready, no deadline, no
    /// I/O in flight) waits before it is declared deadlocked, for a
    /// fiber an OS thread may still wake: the test suite has such
    /// threads. 0 declares at once; a harness that forbids threads
    /// sets it.
    idle_grace_ms: u64 = 50,
    /// The disk's error rates (`simdisk.Faults`). Zero: the unit suite
    /// expects a disk that works; a harness that tests the error paths
    /// sets them.
    disk_faults: _simdisk.Faults = .{},
};

/// How a run ended. `running` until `Runtime.run` returns.
pub const Outcome = enum {
    running,
    /// `Runtime.stop` was called: the normal end.
    stopped,
    /// Nothing ready, no deadline, no I/O in flight, and fibers still
    /// parked: nothing can ever wake them.
    deadlock,
    /// As `deadlock`, but no fiber is parked either: the program ran
    /// out of work without calling `stop`.
    idle,
};

pub const Sim = struct {
    opts: Options,
    prng: std.Random.DefaultPrng,
    /// The virtual clock, ms since the Unix epoch: what `timestampNow`
    /// answers while the sim runs (`coarse_clock.virtual_ms`), and so
    /// what `update` publishes at the top of every loop pass. Atomic
    /// for a test's OS thread reading the clock while the loop moves it.
    now_ms: std.atomic.Value(i64),
    dispatches: u64 = 0,
    /// Running hash of (fiber seq, virtual ms) per dispatch.
    trace: std.hash.Wyhash,
    outcome: Outcome = .running,
    /// A real xev timer bounding a wait: 1 ms while real I/O is in
    /// flight (this milestone keeps the real disk and network), the idle
    /// grace period otherwise.
    timer: xev.Timer,
    timer_c: xev.Completion = .{},
    timer_fired: bool = false,
    /// Every file op of the run (`fs.submit` asks `Runtime.simDisk`).
    disk: _simdisk.SimDisk,

    pub fn init(alloc: std.mem.Allocator, opts: Options) !Sim {
        var timer = try xev.Timer.init();
        errdefer timer.deinit();
        var disk = try _simdisk.SimDisk.init(alloc, opts.seed);
        disk.root.faults = opts.disk_faults;
        return .{
            .opts = opts,
            .prng = std.Random.DefaultPrng.init(opts.seed),
            .now_ms = .init(opts.epoch_ms),
            .trace = std.hash.Wyhash.init(opts.seed),
            .timer = timer,
            .disk = disk,
        };
    }

    pub fn deinit(self: *Sim) void {
        self.disk.deinit();
        self.timer.deinit();
    }

    /// The next fiber to run from `w`'s rings by `opts.sched`, or null
    /// when both are empty. One worker, so a ring only grows under the
    /// pick and a drawn slot is always there.
    pub fn pick(self: *Sim, w: *_rt.Worker) ?*Fiber {
        const high = &w.ready[@intFromEnum(Priority.high)];
        const normal = &w.ready[@intFromEnum(Priority.normal)];
        switch (self.opts.sched) {
            .priority_fifo => {
                if (high.pop()) |f| return f;
                return normal.pop();
            },
            .priority_random => {
                if (self.randomFrom(high)) |f| return f;
                return self.randomFrom(normal);
            },
            .random => {
                const high_len = high.len();
                const total = high_len + normal.len();
                if (total == 0) return null;

                const i = self.prng.random().uintLessThan(usize, total);
                if (i < high_len) return high.popAt(i) orelse unreachable;
                return normal.popAt(i - high_len) orelse unreachable;
            },
        }
    }

    fn randomFrom(self: *Sim, ring: *Ring) ?*Fiber {
        const n = ring.len();
        if (n == 0) return null;
        const i = self.prng.random().uintLessThan(usize, n);
        return ring.popAt(i) orelse unreachable;
    }

    /// One dispatch into the trace. `Fiber.seq` is the spawn order, the
    /// identity a pointer cannot give across two runs.
    pub fn record(self: *Sim, f: *Fiber) void {
        self.dispatches += 1;
        self.trace.update(std.mem.asBytes(&f.seq));
        const at_ms = self.now();
        self.trace.update(std.mem.asBytes(&at_ms));
    }

    /// The trace so far. Two runs of one seed must agree.
    pub fn traceHash(self: *const Sim) u64 {
        var h = self.trace;
        return h.final();
    }

    /// The cpu a step just cost, for `Worker.chargeCpu`: one to two
    /// `step_ticks`, and one step in a thousand a stall of a hundred,
    /// the page fault or oversized batch a constant never models. Drawn
    /// here, not from the worker's prng: the cost is the environment's,
    /// and so a tenant's throttle point moves with the seed.
    pub fn stepCost(self: *Sim) u64 {
        const rand = self.prng.random();
        if (rand.uintLessThan(u32, 1000) == 0) return step_ticks * 100;
        return step_ticks + rand.uintLessThan(u64, step_ticks);
    }

    /// The virtual time, ms since the Unix epoch.
    pub fn now(self: *const Sim) i64 {
        return self.now_ms.load(.monotonic);
    }

    /// Jump the clock to `to_ms`, never backwards.
    pub fn advance(self: *Sim, to_ms: i64) void {
        _ = self.now_ms.fetchMax(to_ms, .release);
    }

    /// The loop, in place of the worker's `runMain`: one worker, `pick`
    /// over both rings, a virtual clock the deadlines read. With nothing
    /// runnable it jumps the clock to the next deadline. While a real
    /// completion is in flight (this milestone keeps the real disk and
    /// network) the clock cannot jump past it, so it tracks real time
    /// instead, 1 ms per 1 ms, until the I/O is done; an idle runtime
    /// with nothing pending is a deadlock after `idle_grace_ms`,
    /// reported through `outcome`, and the run stops.
    pub fn run(self: *Sim, w: *_rt.Worker) void {
        const prev = _rt.Worker.current_do_not_use_directly;
        _rt.Worker.current_do_not_use_directly = w;
        defer _rt.Worker.current_do_not_use_directly = prev;
        if (builtin.os.tag == .linux) w.tid = std.os.linux.gettid();

        const rt = _rt.runtime();
        std.debug.assert(rt.workers.len == 1);
        std.debug.assert(rt.simState() == self);
        // Armed so a cross-thread `wake` (a test's OS thread, the stop)
        // unsticks a blocked `waitReal`. It is the one completion that
        // is always active, hence the `> 1` below.
        w.wake_async.wait(
            &w.loop,
            &w.wake_completion,
            _rt.Worker,
            w,
            _rt.wakeAsyncCallback,
        );
        w.loop.update_now();

        std.debug.assert(_coarse.virtual_ms == &self.now_ms);
        while (!rt.stopping.load(.acquire)) {
            _coarse.update();
            w.deadlines.drainExpired(self.now());

            if (self.pick(w)) |f| {
                self.record(f);
                const finished = w.runFiber(f);
                _ = w.chargeCpu(f);
                if (finished) {
                    const detached = f.detached;
                    if (f.on_finished(f)) |waiter| _rt.wake(waiter);
                    if (detached) rt.freeFiber(f);
                }
                pump(w);
                continue;
            }

            // Nothing runnable: a last reap, then decide how time moves.
            pump(w);
            if (w.hasReady()) continue;

            const min_deadline = w.deadlines.minDeadline();
            const io_in_flight = w.loop.active > 1;
            if (!io_in_flight) {
                if (min_deadline) |d| {
                    self.advance(d);
                    continue;
                }
                if (self.opts.idle_grace_ms == 0) {
                    self.declareIdle(rt);
                    break;
                }
            }
            // Wait in real time: 1 ms for the I/O, or the grace period for
            // an OS thread that may still wake a fiber. Idle is marked so
            // a cross-thread push notifies us (`injectNormal`,
            // `notifyIfParked`); the thief state that `unpark` would
            // derive from the bit is meaningless with one worker.
            const bound_ms: u64 = if (io_in_flight)
                1
            else
                self.opts.idle_grace_ms;

            w.markIdle();
            const timer_fired = self.waitReal(&w.loop, bound_ms);
            _ = rt.idle[w.lane_idx].selfClear(w.laneOffset());

            if (w.hasReady()) continue;
            if (!timer_fired) continue; // a completion: loop and look
            if (io_in_flight) {
                // A real ms passed with the I/O still out: so does a
                // virtual one, and a deadline due by then fires.
                self.advance(self.now() + 1);
                continue;
            }
            self.declareIdle(rt);
            break;
        }
        if (self.outcome == .running) self.outcome = .stopped;
    }

    /// Reap what the kernel posted, so a fiber a real completion woke
    /// is in the rings for the next pick.
    fn pump(w: *_rt.Worker) void {
        w.loop.run(.no_wait) catch |err| {
            std.debug.panic("xev.Loop.run failed: {}", .{err});
        };
    }

    /// Nothing can happen any more: record which kind and stop the run.
    fn declareIdle(self: *Sim, rt: *_rt.Runtime) void {
        self.outcome = if (rt.stats().waiting > 0) .deadlock else .idle;
        rt.stop();
    }

    /// Block on `loop` until a completion arrives (real I/O, a
    /// cross-thread wake, the stop) or `bound_ms` real ms pass; 0 is no
    /// bound. Returns whether the bound fired. A timer still armed from
    /// an earlier wait is left to fire on its own schedule; bounds are
    /// short, so it fires soon and the loop takes stock again.
    pub fn waitReal(self: *Sim, loop: *xev.Loop, bound_ms: u64) bool {
        self.timer_fired = false;
        if (bound_ms > 0 and self.timer_c.state() != .active) {
            self.timer.run(loop, &self.timer_c, bound_ms, Sim, self, onTimer);
        }
        loop.run(.once) catch |err| {
            std.debug.panic("xev.Loop.run failed: {}", .{err});
        };
        return self.timer_fired;
    }

    fn onTimer(
        ud: ?*Sim,
        _: *xev.Loop,
        _: *xev.Completion,
        result: xev.Timer.RunError!void,
    ) xev.CallbackAction {
        _ = result catch return .disarm;
        ud.?.timer_fired = true;
        return .disarm;
    }
};
