//! Multi-threaded fiber scheduler with home-worker pinning. The
//! `Runtime` owns N `Worker`s, each driving its own `xev.Loop` on
//! its own OS thread (worker 0 on the calling thread, workers
//! 1..N-1 on `std.Thread.spawn`). A fiber's home worker is set at
//! spawn time and never changes — no work stealing.
//!
//! Cross-thread injection: `spawn` / `wake` for a fiber whose home
//! isn't the current worker pushes via a Treiber-stack `RemoteQueue`
//! on the home worker, then `xev.Async.notify()` unsticks the home
//! worker's `loop.run(.once)`. The home worker drains the remote
//! queue at the top of every outer-loop iteration.
//!
//! `yield()` / `park()` / `wake()` are namespace-level free functions
//! that operate on the currently-running fiber, looked up via
//! `currentWorkerOrNull()` (the migration-safe accessor for the
//! `current_do_not_use_directly` thread-local).

const std = @import("std");
const builtin = @import("builtin");
const xev = @import("xev");
const fb = @import("fiber.zig");
const coarse = @import("coarse_clock.zig");
const deadlines = @import("deadlines.zig");
const spinlock = @import("spinlock.zig");
const queue = @import("queue.zig");
const fsops = @import("fsops.zig");

/// Q32.32 fixed point: `ns_per_cycle * 2^32`. Set exactly once by
/// `calibrateTsc` via CAS; zero means "not yet calibrated." This is
/// the global source of truth — each worker copies it into
/// `tsc_factor_tls` once at run() entry so the hot-path `cyclesToNs`
/// avoids the atomic load on every dispatch.
var tsc_ns_per_cycle_q32: std.atomic.Value(u64) = .init(0);

/// Per-worker copy of `tsc_ns_per_cycle_q32`. Loaded once at the top
/// of `Worker.run`; read non-atomically in the hot path. Stays 0 on
/// non-Linux (calibration short-circuits) — `cyclesToNs` then yields
/// 0 and `cpu_total_ns` stays at zero. Safe because the global is
/// set before any worker starts (Runtime.init → calibrateTsc →
/// allocate workers → workers.run).
threadlocal var tsc_factor_tls: u64 = 0;

const Fiber = fb.Fiber;
const State = fb.State;
const Tag = fb.Tag;
const Priority = fb.Priority;
const Lane = fb.Lane;
const SpawnOptions = fb.SpawnOptions;
const FiberFn = fb.FiberFn;
const YieldIntent = fb.YieldIntent;

const LocalQueue = queue.LocalQueue;
const GlobalQueue = queue.GlobalQueue;
const RemoteQueue = queue.RemoteQueue;
const IdleWorkers = queue.IdleWorkers;

pub const CoarseClock = coarse.CoarseClock;
pub const Deadlines = deadlines.Deadlines;
pub const Timeout = deadlines.Timeout;
pub const Spinlock = spinlock.Spinlock;

/// Batch size for pulling normal-priority work out of a lane's
/// GlobalQueue into a worker's local ring on each loop pass. Half the
/// local ring so an idle peer can still grab a share; tunable.
const refill_batch: usize = 8;

/// Initial slot count for each lane's GlobalQueue (grows by doubling).
const global_queue_initial_capacity: usize = 256;

/// Whether the `runFiber` concurrency guard is compiled in. It's a pure
/// debug assertion (an atomic RMW per dispatch); gated to safety builds
/// so the locked increment is fully eliminated from unsafe release builds
/// — used with `if (comptime runfiber_guard)` so no runtime branch remains.
const runfiber_guard = switch (builtin.mode) {
    .Debug, .ReleaseSafe => true,
    .ReleaseFast, .ReleaseSmall => false,
};

/// Process-wide singleton runtime. There is exactly one runtime, so
/// workers carry no backpointer — they and `spawn` resolve it through
/// `runtime()`. Set at the end of `Runtime.init`, cleared by `deinit`.
var g_rt: ?*Runtime = null;

/// The process-singleton runtime. Panics if no runtime has been
/// initialized. Safe from any thread (the singleton is set at the end of
/// `Runtime.init`). `spawn` / `wake` and the workers resolve the runtime
/// through this.
pub fn runtime() *Runtime {
    return g_rt orelse @panic("fiber runtime not initialized");
}

/// Coarse monotonic milliseconds from the singleton runtime's clock.
/// Convenience for code that isn't holding a `*Runtime`. Panics if no
/// runtime is initialized.
pub inline fn coarseMs() i64 {
    return runtime().ms();
}

/// Contiguous worker-id range owned by a lane. `len == 0` means the
/// lane has no workers configured; routing falls back to `.default`.
const LaneRange = struct { start: u8, len: u8 };

pub const Worker = struct {
    /// The worker running on this OS thread. Do NOT read directly — use
    /// `currentWorkerOrNull()` / `currentWorker()`, which are `noinline` +
    /// barriered so the read isn't hoisted across a coro `yield()` (fibers
    /// migrate between worker threads, so a cached value goes stale). Only
    /// `Worker.run` writes it.
    pub threadlocal var current_do_not_use_directly: ?*Worker = null;

    id: u8,
    /// Index into `Runtime.lane_ranges` / `.global` / `.idle` for the
    /// lane this worker belongs to. Set in `Runtime.init`.
    lane_idx: u8 = 0,
    loop: xev.Loop,
    deadlines: Deadlines,
    current_fiber: ?*Fiber = null,
    /// TSC reading at the last libxev pump (see `dispatch`). Lets a
    /// continuously-busy worker still service timers / async wakes about
    /// once per millisecond without blocking.
    last_pump_ticks: u64 = 0,
    /// Two ready rings, one per `Priority`. Owner-thread only — no work
    /// stealing, so no locking. `ready[normal]` spills to / refills from
    /// this lane's GlobalQueue; `ready[high]` is pinned and must not
    /// overflow (see `scheduleLocal`).
    ready: [Priority.count]LocalQueue = @splat(.{}),

    /// Cross-thread fiber injection. Pushed to by peer workers /
    /// external threads via `RemoteQueue.push`; drained by this
    /// worker at the top of every outer-loop iteration.
    remote_queue: RemoteQueue = .{},

    /// Cross-thread wake mechanism. Peer workers that pushed onto
    /// `remote_queue` call `wake_async.notify()` to unstick our
    /// `loop.run(.once)`.
    wake_async: xev.Async,
    wake_completion: xev.Completion = undefined,

    /// OS thread running this worker. `null` for worker 0 (lives on
    /// the calling thread that invoked `Runtime.run`).
    thread: ?std.Thread = null,

    pub fn init(w: *Worker, id: u8) !void {
        w.id = id;
        w.lane_idx = 0; // real value assigned in Runtime.init
        w.loop = try xev.Loop.init(.{});
        errdefer w.loop.deinit();
        w.wake_async = try xev.Async.init();
        errdefer w.wake_async.deinit();
        w.deadlines = try Deadlines.init(&w.loop);
        w.current_fiber = null;
        w.last_pump_ticks = 0;
        w.ready = @splat(.{});
        w.remote_queue = .{};
        w.wake_completion = undefined;
        w.thread = null;
    }

    pub fn deinit(self: *Worker, allocator: std.mem.Allocator) void {
        self.deadlines.deinit(allocator);
        self.wake_async.deinit();
        self.loop.deinit();
    }

    /// Enqueue `f` on this worker's matching priority ring. On overflow:
    /// high priority panics (it's pinned and expected to fit); normal
    /// priority spills to this lane's GlobalQueue and pokes an idle peer.
    pub fn scheduleLocal(self: *Worker, f: *Fiber) void {
        self.ready[@intFromEnum(f.priority)].push(f) catch {
            if (f.priority == .high) {
                @panic("high-priority local queue overflow");
            }
            const rt = runtime();
            rt.global[self.lane_idx].push(f) catch {
                @panic("out of memory spilling fiber to global queue");
            };
            rt.notifyIdlePeer(self.lane_idx);
        };
    }

    /// Commit a park that `park()` deferred. Runs on the worker AFTER the
    /// fiber's `coro.yield()` has fully suspended it (and after the
    /// `runFiber` concurrency guard is released), so the `.ready ->
    /// .waiting` transition — which makes the fiber wakeable, hence
    /// runnable on any worker — can't expose a not-yet-suspended coro. If
    /// a `wake` deposited an awaken token while we were switching away,
    /// reschedule instead of parking. Same CAS dance `park` used to do
    /// inline, relocated to close the handoff race.
    fn commitPark(self: *Worker, f: *Fiber) void {
        var old = f.loadState();
        while (true) {
            std.debug.assert(old.tag == .ready);
            if (old.awaken) {
                const new = State{ .tag = .ready, .awaken = false };
                if (f.casState(old, new)) |actual| {
                    old = actual;
                    continue;
                }
                self.scheduleLocal(f); // token consumed -> stay runnable
                return;
            }
            const new = State{ .tag = .waiting, .awaken = false };
            if (f.casState(old, new)) |actual| {
                old = actual;
                continue;
            }
            return; // parked; a later wake will enqueue us
        }
    }

    fn popNextReady(self: *Worker) ?*Fiber {
        var i: usize = Priority.count;
        while (i > 0) {
            i -= 1;
            if (self.ready[i].pop()) |f| return f;
        }
        return null;
    }

    /// Whether any priority's local ready ring holds work. Lets the run
    /// loop test for work after a find step without popping a fiber it
    /// would then have to dispatch inline.
    fn hasReady(self: *const Worker) bool {
        for (&self.ready) |*q| {
            if (!q.isEmpty()) return true;
        }
        return false;
    }

    /// Move every fiber on our remote queue to our local ready queue.
    /// Called at the top of every outer-loop iteration so cross-thread
    /// (high-priority) arrivals get picked up promptly. `drainFifo`
    /// returns them oldest-first so a burst keeps its spawn order.
    fn drainRemote(self: *Worker) void {
        var f: ?*Fiber = self.remote_queue.drainFifo();
        while (f) |entry| {
            f = entry.remote_next;
            entry.remote_next = null;
            self.scheduleLocal(entry);
        }
    }

    /// Pull a batch of normal-priority work from this lane's GlobalQueue
    /// into the local normal ring, when there's room. Cheap no-op when
    /// the ring is full (skips taking the global spinlock).
    fn refillFromGlobal(self: *Worker) void {
        const dest = &self.ready[@intFromEnum(Priority.normal)];
        const room = dest.capacity() - dest.len();
        if (room == 0) return;
        _ = runtime().global[self.lane_idx].popBatchTo(dest, @min(room, refill_batch));
    }

    /// This worker's bit offset within its lane's idle bitmap.
    fn laneOffset(self: *const Worker) u8 {
        return self.id - runtime().lane_ranges[self.lane_idx].start;
    }

    fn markIdle(self: *Worker) void {
        runtime().idle[self.lane_idx].markIdle(self.laneOffset());
    }

    fn clearIdle(self: *Worker) void {
        runtime().idle[self.lane_idx].clearIdle(self.laneOffset());
    }

    /// Step one fiber, then handle completion (wake any parked joiner)
    /// or its yield intent. CPU-time accounting and the periodic libxev
    /// pump live in the run loop, which brackets this call with
    /// `readTicks`. No live counting — the runtime keeps running until
    /// `stop()`; a finished fiber simply leaves the scheduler.
    fn runFiber(self: *Worker, f: *Fiber) void {
        self.current_fiber = f;

        // Concurrency guard, held across the step AND the finish check:
        // a fiber is single-owner, so only one worker may run it at a
        // time. If `running` reaches 2, the fiber was double-scheduled
        // onto two queues (a scheduler bug) — panic with a stack trace
        // before the two threads corrupt its shared coro stack. The
        // fiber is not in any queue during this window (we popped it), so
        // no peer can legally touch it. We must release the guard BEFORE
        // re-scheduling a `.reschedule` fiber, though: that push can
        // spill to the global queue, where a peer may legitimately pick
        // the fiber up the instant we let go.
        const finished = blk: {
            if (comptime runfiber_guard) {
                const concurrent = f.running.fetchAdd(1, .acq_rel) + 1;
                if (concurrent >= 2) std.debug.panic(
                    "fiber 0x{x} stepped concurrently by {d} workers (this worker = {d})",
                    .{ @intFromPtr(f), concurrent, self.id },
                );
            }
            defer if (comptime runfiber_guard) {
                _ = f.running.fetchSub(1, .release);
            };

            f.coro.step();
            self.current_fiber = null;

            if (f.isFinished()) {
                // The fiber wrote its result into JoinState; the hook
                // flips `finished=true` and returns any parked joiner.
                // We wake it here so `fiber.zig` needn't import the runtime.
                if (f.on_finished(f)) |waiter| wake(waiter);
                break :blk true;
            }
            break :blk false;
        };
        if (finished) return;

        // Guard released and the fiber is now genuinely suspended, so it
        // is safe to make it re-runnable on any worker.
        switch (f.intent) {
            .reschedule => self.scheduleLocal(f),
            .park => self.commitPark(f),
        }
    }

    pub fn run(self: *Worker) void {
        // The one legitimate site that writes the thread-local. Reads go
        // through `currentWorkerOrNull()`; here we own it directly.
        const prev = current_do_not_use_directly;
        current_do_not_use_directly = self;
        defer current_do_not_use_directly = prev;

        // Seed this worker thread's TSC conversion factor from the
        // global once (set during Runtime.init by calibrateTsc).
        // After this, `cyclesToNs` reads `tsc_factor_tls` without an
        // atomic load on every dispatch.
        tsc_factor_tls = tsc_ns_per_cycle_q32.load(.acquire);

        fb.coro.setupStackGrowth() catch |err| {
            std.debug.panic("setupStackGrowth failed: {}", .{err});
        };
        defer fb.coro.cleanupStackGrowth();

        // Lead worker drives the process-wide coarse clock.
        if (self.id == 0) runtime().clock.start();
        defer if (self.id == 0) runtime().clock.stop();

        // Arm the cross-thread wake handle. The callback re-arms
        // automatically; its only job is to unstick `loop.run(.once)`
        // so the outer loop re-checks `stopping` / re-finds work.
        self.wake_async.wait(
            &self.loop,
            &self.wake_completion,
            Worker,
            self,
            wakeAsyncCallback,
        );

        // Seed the pump clock so the first periodic pump lands ~1 ms in.
        self.last_pump_ticks = readTicks();
        self.loop.update_now();

        const rt = runtime();
        while (!rt.stopping.load(.acquire)) {
            // 1. Run the local ready queues to exhaustion first (high
            //    before normal). Only when they're empty do we reach for
            //    more work — drainRemote / refillFromGlobal are
            //    work-*finding* steps, not per-iteration overhead.
            if (self.popNextReady()) |f| {
                const enter = readTicks();
                self.runFiber(f);
                const exit = readTicks();
                // Skip the reading if the counter went backwards (thread
                // migrated mid-step to a CPU with a slightly behind TSC).
                if (exit >= enter) f.cpu_total_ns +%= cyclesToNs(exit - enter);

                // Pump libxev periodically so timers (coarse clock,
                // Deadlines) and async wakes (cross-thread remote pushes)
                // stay responsive even when this worker is continuously
                // busy. `no_wait` returns immediately if no completions
                // are ready — one io_uring_enter syscall per ~1 ms of work.
                if (cyclesToNs(exit -% self.last_pump_ticks) >= std.time.ns_per_ms) {
                    self.loop.run(.no_wait) catch {};
                    self.last_pump_ticks = exit;
                }
                continue;
            }

            // 2. Local empty: find work, then loop back to run it.
            self.drainRemote();
            self.refillFromGlobal();
            if (self.hasReady()) continue;

            // 3. Still nothing. Mark idle BEFORE a final work-find so a
            //    producer that pushes work and then consults the idle
            //    bitmap can't slip into the gap (lost wakeup).
            self.markIdle();
            self.drainRemote();
            self.refillFromGlobal();
            if (self.hasReady()) {
                self.clearIdle();
                continue;
            }

            // 4. Block until a libxev completion (timer / I/O / cross-
            //    thread async wake, including stop's) fires, then clear
            //    our idle bit and re-find work on the next pass.
            self.loop.run(.once) catch |err| {
                std.debug.panic("xev.Loop.run failed: {}", .{err});
            };
            self.clearIdle();
        }
    }
};

fn wakeAsyncCallback(
    _: ?*Worker,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.Async.WaitError!void,
) xev.CallbackAction {
    // The notify's job is just to unstick `loop.run(.once)`. The
    // worker's outer loop will re-find work (remote / global) and
    // re-check `stopping` on its next iteration.
    return .rearm;
}

pub const Stats = struct {
    total: usize,
    new: usize,
    ready: usize,
    waiting: usize,
    finished: usize,
};

pub const InitOptions = struct {
    /// Number of worker threads. `null` defaults to 1 today (so
    /// existing single-thread call sites keep working unchanged).
    /// Callers wanting multi-thread set this explicitly.
    num_workers: ?usize = null,

    /// Workers reserved for the `.raft` lane. These are the LAST
    /// `raft_workers` worker ids; the remaining (lower) ids form the
    /// `.default` lane. Must be `< num_workers` so the default lane
    /// (which owns lead worker 0) is never empty. 0 (default) leaves the
    /// raft lane empty — `.raft` spawns then fall back to `.default`.
    raft_workers: usize = 0,

    /// Threads in the blocking-I/O offload pool (`fsops`) used for fsync and
    /// filesystem metadata syscalls. These run blocking ops off the fiber
    /// workers so fibers keep executing. Clamped to a minimum of 1.
    io_threads: usize = 4,
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    workers: []Worker,
    clock: CoarseClock,
    /// Registry of every fiber the runtime owns. Multi-worker means
    /// multiple threads can `spawn` concurrently → spinlock guards
    /// the ArrayList mutation. Reads (`stats`, `dumpFibers`) take
    /// the spinlock too.
    fibers: std.ArrayList(*Fiber) = .empty,
    fibers_sl: Spinlock = .{},

    /// Set by `stop()`; once true every worker's run loop exits after
    /// its current pass. The runtime otherwise runs forever (workers
    /// park when idle) — there is no live-fiber counting.
    stopping: std.atomic.Value(bool) = .init(false),

    /// Per-lane admission / overflow queues for normal-priority work.
    /// Indexed by `@intFromEnum(Lane)`.
    global: [Lane.count]GlobalQueue = undefined,
    /// Contiguous worker-id range each lane owns. A `len == 0` lane
    /// falls back to `.default` (see `effectiveLaneIdx`).
    lane_ranges: [Lane.count]LaneRange = undefined,
    /// Per-lane idle-worker bitmaps; producers `claim` one to wake a
    /// single parked peer in that lane.
    idle: [Lane.count]IdleWorkers = .{ .{}, .{} },

    pub fn init(self: *Runtime, allocator: std.mem.Allocator, opts: InitOptions) !void {
        const n = opts.num_workers orelse 1;
        std.debug.assert(n >= 1);
        std.debug.assert(n <= std.math.maxInt(u8));
        std.debug.assert(opts.raft_workers < n); // default lane (the fallback) is never empty

        // Pay the 1 ms calibration cost up front so the first fiber
        // dispatch doesn't get charged with it. Idempotent across
        // multiple Runtimes in the same process.
        calibrateTsc();

        self.allocator = allocator;
        self.fibers = .empty;
        self.fibers_sl = .{};
        self.stopping = .init(false);
        self.idle = .{ .{}, .{} };

        // Lane layout: raft takes the first worker ids, default the rest —
        //   raft    = [0, raft_workers)
        //   default = [raft_workers, n)
        const raft_count: u8 = @intCast(opts.raft_workers);
        self.lane_ranges = .{
            .{ .start = raft_count, .len = @intCast(n - opts.raft_workers) }, // .default
            .{ .start = 0, .len = raft_count }, // .raft
        };

        // Per-lane global queues for normal-priority work.
        var g_inited: usize = 0;
        errdefer for (self.global[0..g_inited]) |*gq| gq.deinit();
        for (&self.global) |*gq| {
            gq.* = try GlobalQueue.init(allocator, global_queue_initial_capacity);
            g_inited += 1;
        }

        self.workers = try allocator.alloc(Worker, n);
        errdefer allocator.free(self.workers);

        var initialized: usize = 0;
        errdefer {
            for (self.workers[0..initialized]) |*w| w.deinit(allocator);
        }
        for (self.workers, 0..) |*w, i| {
            try Worker.init(w, @intCast(i));
            w.lane_idx = if (i < raft_count)
                @intFromEnum(Lane.raft)
            else
                @intFromEnum(Lane.default);
            initialized += 1;
        }

        self.clock = try CoarseClock.init();
        self.clock.bind(&self.workers[0].loop);

        // Blocking-I/O offload pool (fsync + fs metadata). Started before we
        // publish the singleton; no jobs arrive until fibers run.
        try fsops.start(allocator, opts.io_threads);
        errdefer fsops.stop();

        // Publish as the process singleton. There is exactly one runtime
        // alive at a time; `spawn` resolves it through `runtime()`.
        g_rt = self;
    }

    pub fn deinit(self: *Runtime) void {
        // Stop the offload pool first: join every pool thread so none is
        // mid-`wake()` into the runtime state we're about to free.
        fsops.stop();
        for (self.fibers.items) |f| {
            f.deinit(self.allocator);
        }
        self.fibers.deinit(self.allocator);
        self.clock.deinit();
        for (self.workers) |*w| w.deinit(self.allocator);
        self.allocator.free(self.workers);
        for (&self.global) |*gq| gq.deinit();
        g_rt = null;
    }

    fn appendFiber(self: *Runtime, f: *Fiber) !void {
        self.fibers_sl.lock();
        defer self.fibers_sl.unlock();
        f.registry_index = self.fibers.items.len;
        try self.fibers.append(self.allocator, f);
    }

    fn removeFiber(self: *Runtime, f: *Fiber) void {
        self.fibers_sl.lock();
        defer self.fibers_sl.unlock();
        const idx = f.registry_index;
        std.debug.assert(idx < self.fibers.items.len);
        std.debug.assert(self.fibers.items[idx] == f);

        _ = self.fibers.swapRemove(idx);
        if (idx < self.fibers.items.len) {
            self.fibers.items[idx].registry_index = idx;
        }
        f.registry_index = std.math.maxInt(usize);
    }

    /// Request shutdown: every worker's run loop exits after its current
    /// pass. Notifies all workers' `wake_async` so parked workers unstick
    /// their `loop.run(.once)` and observe `stopping`. Safe from any
    /// thread — a fiber via `fiber.stop()`, or an external thread.
    pub fn stop(self: *Runtime) void {
        self.stopping.store(true, .release);
        for (self.workers) |*w| w.wake_async.notify() catch {};
    }

    /// Effective lane index for `lane`: itself when it owns workers, else
    /// `.default` (the never-empty fallback lane).
    fn effectiveLaneIdx(self: *Runtime, lane: Lane) u8 {
        const idx: u8 = @intFromEnum(lane);
        if (self.lane_ranges[idx].len == 0) return @intFromEnum(Lane.default);
        return idx;
    }

    /// Wake one parked worker in `lane_idx` to come pull from the global
    /// queue. `claim` atomically takes a single idle bit, so exactly one
    /// producer wakes exactly one peer; no-op when none are idle.
    fn notifyIdlePeer(self: *Runtime, lane_idx: u8) void {
        if (self.idle[lane_idx].claim()) |off| {
            const peer_id: usize = @as(usize, self.lane_ranges[lane_idx].start) + off;
            std.debug.assert(peer_id < self.workers.len);
            self.workers[peer_id].wake_async.notify() catch {};
        }
    }

    /// Start the scheduler. Spawns OS threads for workers 1..N-1,
    /// runs worker 0 on the calling thread, joins the peer threads
    /// after worker 0 returns.
    pub fn run(self: *Runtime) void {
        for (self.workers[1..]) |*w| {
            w.thread = std.Thread.spawn(.{}, workerEntry, .{w}) catch |err| {
                std.debug.panic("worker thread spawn failed: {}", .{err});
            };
        }
        self.workers[0].run();
        for (self.workers[1..]) |*w| {
            if (w.thread) |t| t.join();
            w.thread = null;
        }
    }

    /// Coarse monotonic ms. One atomic load — safe from any worker
    /// thread, cheap on every call.
    pub inline fn ms(self: *const Runtime) i64 {
        return self.clock.ms();
    }

    pub fn stats(self: *Runtime) Stats {
        self.fibers_sl.lock();
        defer self.fibers_sl.unlock();
        var s: Stats = .{ .total = 0, .new = 0, .ready = 0, .waiting = 0, .finished = 0 };
        for (self.fibers.items) |f| {
            s.total += 1;
            switch (f.loadState().tag) {
                .new => s.new += 1,
                .ready => s.ready += 1,
                .waiting => s.waiting += 1,
                .finished => s.finished += 1,
            }
        }
        return s;
    }
};

fn workerEntry(w: *Worker) void {
    w.run();
}

/// Route a ready fiber to a queue. Shared by `spawn` and `wake`.
///
///   - High priority is pinned: it goes to its home worker — directly to
///     the home's local ring when we're already on the home thread, else
///     onto the home's remote queue with an async notify.
///   - Normal priority is lane-bound but worker-free: when we're running
///     on a worker in the fiber's lane, push to that local ring (spilling
///     to the lane's global queue on overflow via `scheduleLocal`);
///     otherwise admit to the lane's global queue and wake an idle peer.
fn routeReady(rt: *Runtime, f: *Fiber) void {
    switch (f.priority) {
        .high => {
            const home = &rt.workers[f.home_worker_id];
            if (currentWorkerOrNull() == home) {
                home.scheduleLocal(f);
            } else {
                home.remote_queue.push(f);
                home.wake_async.notify() catch {};
            }
        },
        .normal => {
            const lane_idx = rt.effectiveLaneIdx(f.lane);
            if (currentWorkerOrNull()) |w| {
                if (w.lane_idx == lane_idx) {
                    w.scheduleLocal(f);
                    return;
                }
            }
            rt.global[lane_idx].push(f) catch {
                @panic("out of memory admitting fiber to global queue");
            };
            rt.notifyIdlePeer(lane_idx);
        },
    }
}

/// Typed handle returned by `spawn`. Wraps the fiber pointer and a
/// `*JoinState(T)` into the fiber's heap context, so `join()` can park
/// on the result without knowing the args' concrete type.
///
/// Lifetime: the underlying `JoinState(T)` lives as long as the Fiber
/// (freed by `Fiber.deinit`, which runs on `rt.deinit`). Call `join()`
/// before tearing down the runtime.
pub fn JoinHandle(comptime T: type) type {
    return struct {
        const Self = @This();

        fiber: *Fiber,
        state: *fb.JoinState(T),

        /// Block until the fiber's body returns, then return its
        /// value. Single-joiner: asserts no other fiber is already
        /// parked on this handle. Must be called from a fiber
        /// context — `park()` requires `currentFiber()`.
        ///
        /// Idempotent after completion: a `join()` issued after the
        /// fiber finished sees `.finished` on the fast path and
        /// returns the cached result without touching the queue.
        pub fn join(self: Self) T {
            while (true) {
                self.state.sl.lock();
                if (self.fiber.isFinished()) {
                    self.state.sl.unlock();
                    return self.state.result;
                }
                std.debug.assert(self.state.joiner == null);
                self.state.joiner = currentFiber();
                self.state.sl.unlock();
                park();
                // Loop: re-check. A spurious wake (someone else
                // happened to call `wake(self)`) just re-parks here.
            }
        }
    };
}

/// Spawn `func(args...)` as a new fiber on the singleton runtime. The
/// args tuple is copied into a heap-allocated context owned by the Fiber
/// (see `Fiber.init`); callers only need to keep alive what the args
/// point to. Returns a `JoinHandle(T)` where `T` is `func`'s return type
/// — `join()` retrieves the value once the body finishes.
///
/// Priority / addressing contract:
///   - High priority is pinned and MUST name a `worker_id`. The id is
///     *lane-relative*: the absolute home worker is
///     `lane.start + worker_id % lane.len` — e.g. worker_id 3 in a
///     2-worker raft lane resolves to lane offset 1.
///   - Normal priority migrates freely within its lane and must NOT name
///     a `worker_id`.
/// `lane` selects the worker range (default `.default`, the fallback when
/// the requested lane has no workers).
pub fn spawn(
    opts: SpawnOptions,
    comptime func: anytype,
    args: std.meta.ArgsTuple(@TypeOf(func)),
) !JoinHandle(fb.ReturnOf(func)) {
    const rt = runtime();
    switch (opts.priority) {
        .high => std.debug.assert(opts.worker_id != null),
        .normal => std.debug.assert(opts.worker_id == null),
    }

    const f = try Fiber.init(rt.allocator, opts, func, args);
    errdefer f.deinit(rt.allocator);

    // `f.lane` is set from `opts` in `Fiber.init`. For high priority,
    // resolve the lane-relative `worker_id` to an absolute home worker.
    if (opts.priority == .high) {
        const range = rt.lane_ranges[rt.effectiveLaneIdx(f.lane)];
        f.home_worker_id = range.start + (opts.worker_id.? % range.len);
    }

    try rt.appendFiber(f);

    f.state.store(@bitCast(State{ .tag = .ready, .awaken = false }), .release);

    routeReady(rt, f);

    // Same comptime-generated Ctx type Fiber.init used to allocate
    // `f.ctx`. Accessing `&ctx.state` by name lets Zig compute the
    // offset — no dependence on field order in `Ctx`.
    const ctx: *fb.Ctx(func) = @ptrCast(@alignCast(f.ctx));
    return .{ .fiber = f, .state = &ctx.state };
}

pub fn yield() void {
    const w = currentWorker();
    const f = w.current_fiber.?;
    f.intent = .reschedule;
    f.coro.yield();
}

/// Park the current fiber. If a `wake` already fired (awaken bit
/// set) the token is consumed and `park` returns without yielding.
/// Otherwise the fiber transitions to `.waiting`, records park
/// metadata, and yields. A later `wake` CAS'es it back to `.ready`
/// and routes it through `routeReady`.
pub fn park() void {
    const w = currentWorker();
    const f = w.current_fiber.?;

    // Fast path: a `wake` that already deposited an awaken token means we
    // shouldn't park at all — consume it and keep running.
    var old = f.loadState();
    while (old.awaken) {
        std.debug.assert(old.tag == .ready);
        const new = State{ .tag = .ready, .awaken = false };
        if (f.casState(old, new)) |actual| {
            old = actual;
            continue;
        }
        return; // token consumed; don't yield
    }

    // No token: record park metadata and yield. We deliberately do NOT
    // transition to `.waiting` here. That commit runs in
    // `Worker.commitPark` AFTER `coro.yield()` has fully suspended us, so
    // a concurrent `wake` can't make us runnable on another worker before
    // our context is saved (which would let two workers step one coro).
    std.debug.assert(old.tag == .ready);
    f.parked_since_ms = runtime().clock.ms();
    captureTrace(&f.park_trace);

    f.intent = .park;
    f.coro.yield();
}

/// Wake a fiber. Safe to call on a fiber in any state: `.finished`
/// is a no-op; `.ready` deposits an awaken token for a concurrent
/// `park` to consume; `.new` / `.waiting` transitions to `.ready`
/// and re-routes the fiber via `routeReady` (high priority → its pinned
/// home worker; normal priority → a local ring in its lane or that
/// lane's global queue).
pub fn wake(f: *Fiber) void {
    var old = f.loadState();
    while (true) {
        switch (old.tag) {
            .finished => return,
            .ready => {
                if (old.awaken) return;
                const new = State{ .tag = .ready, .awaken = true };
                if (f.casState(old, new)) |actual| {
                    old = actual;
                    continue;
                }
                return;
            },
            .new, .waiting => {
                const new = State{ .tag = .ready, .awaken = false };
                if (f.casState(old, new)) |actual| {
                    old = actual;
                    continue;
                }
                break;
            },
        }
    }

    routeReady(runtime(), f);
}

/// Request runtime shutdown from anywhere — typically a fiber that has
/// finished the program's work. Every worker's run loop returns after
/// its current pass, and `Runtime.run` returns on the calling thread.
pub fn stop() void {
    runtime().stop();
}

/// The worker running on the calling OS thread, or null off a worker
/// thread (e.g. a bootstrap `spawn` from `main`). This is the ONLY place
/// the `current_do_not_use_directly` thread-local is read.
///
/// `noinline` keeps this an opaque call LICM won't hoist out of a loop
/// that yields; the empty `volatile` asm with a `memory` clobber also
/// marks it non-readonly, so the guarantee doesn't rest on optimizer
/// heuristics. Both matter because a parked fiber can resume on a
/// DIFFERENT OS thread, so a cached worker pointer would be stale — its
/// `current_fiber` null — after migration (a crash seen only in optimized
/// builds). At the instant it runs the value is always correct.
pub noinline fn currentWorkerOrNull() ?*Worker {
    asm volatile ("" : : : .{ .memory = true });
    return Worker.current_do_not_use_directly;
}

pub fn currentWorker() *Worker {
    return currentWorkerOrNull().?;
}

pub fn currentFiber() *Fiber {
    return currentWorker().current_fiber.?;
}

/// Like `currentFiber` but returns null off a worker thread (or on a
/// worker between fibers) instead of asserting. Used by the test runner
/// to key per-fiber log buffers without knowing if we're on a fiber.
pub fn currentFiberOrNull() ?*Fiber {
    const w = currentWorkerOrNull() orelse return null;
    return w.current_fiber;
}

pub fn dumpFibers(rt: *Runtime, writer: *std.Io.Writer) !void {
    rt.fibers_sl.lock();
    defer rt.fibers_sl.unlock();
    const now_ms = rt.clock.ms();
    for (rt.fibers.items, 0..) |f, i| {
        const s = f.loadState();
        const letter: u8 = switch (s.tag) {
            .new => 'N',
            .ready => 'R',
            .waiting => 'W',
            .finished => 'F',
        };
        try writer.print("{c} {d:>5} fiber=0x{x}", .{ letter, i, @intFromPtr(f) });
        if (s.tag == .waiting) {
            try writer.print(" parked_for={d}ms", .{now_ms - f.parked_since_ms});
        }
        try writer.print(" cpu_total={d}ms\n", .{@divTrunc(f.cpu_total_ns, std.time.ns_per_ms)});
    }
}

/// Read the architecture's fastest monotonic counter. On x86_64
/// and aarch64 this is a cycle count needing `cyclesToNs` to convert.
/// Other architectures fall back to `monoNs()` (which is already in
/// nanoseconds — `cyclesToNs` is the identity in that branch).
inline fn readTicks() u64 {
    return switch (comptime builtin.cpu.arch) {
        .x86_64 => blk: {
            var hi: u32 = undefined;
            var lo: u32 = undefined;
            asm volatile ("rdtsc"
                : [lo] "={eax}" (lo),
                  [hi] "={edx}" (hi),
            );
            break :blk (@as(u64, hi) << 32) | lo;
        },
        .aarch64 => blk: {
            var t: u64 = undefined;
            asm volatile ("mrs %[t], cntvct_el0"
                : [t] "=r" (t),
            );
            break :blk t;
        },
        else => 0,
    };
}

/// Monotonic nanoseconds since some unspecified epoch. Uses
/// `std.posix.system.clock_gettime` — Zig dispatches at comptime to
/// the platform's native `clock_gettime` (Linux syscall on Linux,
/// libc on macOS/BSD). Only called by `calibrateTsc`; the hot-path
/// CPU accounting uses `rdtsc` instead.
///
/// Windows returns 0 — would need `QueryPerformanceCounter` via
/// `std.os.windows`. Until then, TSC calibration short-circuits
/// (`tsc_delta == 0` path) on Windows, leaving `cpu_total_ns` at 0.
fn monoNsUseRarely() i64 {
    return switch (comptime builtin.os.tag) {
        .windows => 0,
        else => blk: {
            var tp: std.posix.timespec = undefined;
            _ = std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &tp);
            break :blk @as(i64, tp.sec) * std.time.ns_per_s + tp.nsec;
        },
    };
}

/// Convert a tick delta to nanoseconds using the per-worker cached
/// factor (`tsc_factor_tls`, populated once at `Worker.run` entry).
/// Returns 0 if the factor hasn't been seeded — either calibration
/// short-circuited on a non-Linux build or `cyclesToNs` was called
/// outside a worker thread (which shouldn't happen in normal use).
inline fn cyclesToNs(cycles: u64) u64 {
    return switch (comptime builtin.cpu.arch) {
        .x86_64, .aarch64 => blk: {
            const factor = tsc_factor_tls;
            if (factor == 0) break :blk 0;
            // u128 intermediate prevents overflow for multi-second
            // cycle deltas (would otherwise overflow u64 around ~1.4s
            // on a 3 GHz TSC).
            break :blk @intCast((@as(u128, cycles) *% @as(u128, factor)) >> 32);
        },
        else => cycles,
    };
}

/// One-time TSC calibration. Takes 30 sample readings, each sleeping
/// ~1 ms to let the ns/cycle ratio stabilize, then stores the median
/// ratio as Q32.32 in `tsc_ns_per_cycle_q32`. The median rejects
/// outliers caused by preemption or scheduling jitter in any single
/// reading. Total cost: ~30 ms, paid once per process.
///
/// Idempotent and thread-safe — racing callers do the full work but
/// only one wins the CAS; the others' computed value is discarded
/// (in practice all 30-sample medians agree within ppm).
/// Read the calibrated `ns_per_cycle * 2^32` factor. Returns 0 if
/// calibration hasn't completed or short-circuited on non-Linux.
/// Mostly for diagnostic / test code that wants to inspect the
/// hardware's TSC frequency (`tsc_hz ≈ 2^32 / factor * 1e9`).
pub fn tscNsPerCycleQ32() u64 {
    return tsc_ns_per_cycle_q32.load(.acquire);
}

fn calibrateTsc() void {
    switch (comptime builtin.os.tag) {
        .windows => return, // see monoNs note; no nanosleep wrapper either
        else => {},
    }
    switch (comptime builtin.cpu.arch) {
        .x86_64, .aarch64 => {},
        else => return,
    }
    if (tsc_ns_per_cycle_q32.load(.acquire) != 0) return;

    const N = 31;
    var samples: [N]u64 = undefined;
    const req = std.posix.timespec{ .sec = 0, .nsec = 1_000_000 };

    var i: usize = 0;
    while (i < N) {
        const ns_start = monoNsUseRarely();
        const tsc_start = readTicks();
        _ = std.posix.system.nanosleep(&req, null);
        const tsc_end = readTicks();
        const ns_end = monoNsUseRarely();

        const ns_delta: u64 = @intCast(ns_end - ns_start);
        const tsc_delta = tsc_end - tsc_start;
        if (tsc_delta == 0) continue; // pathological; retry this slot
        samples[i] = @intCast((@as(u128, ns_delta) << 32) / @as(u128, tsc_delta));
        i += 1;
    }

    std.mem.sort(u64, &samples, {}, std.sort.asc(u64));
    const median = samples[(N / 2) + 1];

    // Convert Q32.32 ns/cycle to GHz: GHz = 2^32 / factor. Smaller
    // factor = fewer ns/cycle = higher GHz, so `samples[0]` (sorted
    // ascending by factor) maps to the *highest* GHz and vice versa.
    const q32_one: f64 = @floatFromInt(@as(u64, 1) << 32);
    const max_ghz = q32_one / @as(f64, @floatFromInt(samples[0]));
    const min_ghz = q32_one / @as(f64, @floatFromInt(samples[N - 1]));
    const med_ghz = q32_one / @as(f64, @floatFromInt(median));
    std.log.info(
        "tsc calibration: median={d:.4} GHz (min={d:.4}, max={d:.4}, samples={d})",
        .{ med_ghz, min_ghz, max_ghz, N },
    );

    _ = tsc_ns_per_cycle_q32.cmpxchgStrong(0, median, .release, .acquire);
}

fn captureTrace(buf: *[8]usize) void {
    @memset(buf, 0);
    _ = std.debug.captureCurrentStackTrace(.{}, buf);
}
