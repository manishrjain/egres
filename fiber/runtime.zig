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
//! that operate on the currently-running fiber, looked up via the
//! `Worker.current` thread-local.

const std = @import("std");
const builtin = @import("builtin");
const xev = @import("xev");
const fb = @import("fiber.zig");
const coarse = @import("coarse_clock.zig");
const deadlines = @import("deadlines.zig");
const spinlock = @import("spinlock.zig");

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
const SpawnOptions = fb.SpawnOptions;
const FiberFn = fb.FiberFn;
const YieldIntent = fb.YieldIntent;

pub const CoarseClock = coarse.CoarseClock;
pub const Deadlines = deadlines.Deadlines;
pub const Timeout = deadlines.Timeout;
pub const Spinlock = spinlock.Spinlock;

/// FIFO ready queue, concurrent-safe via a per-queue spinlock.
///
/// `pushTail` is called only by the worker that owns the queue.
/// `popHead` is called by both the owner (its own pop in
/// `Worker.run`, mode = `.any`) and by thieves (`Worker.trySteal`,
/// mode = `.unlocked`). The spinlock makes the queue safe under
/// that concurrent use without committing to a full lock-free
/// deque — the spin cost dominates only during stealing, which by
/// definition only happens when peers are idle.
const ReadyQueue = struct {
    sl: Spinlock = .{},
    head: ?*Fiber = null,
    tail: ?*Fiber = null,
    count: u32 = 0,

    pub const PopMode = enum {
        /// Pop any fiber at the head — owner mode.
        any,
        /// Pop the first unlocked (stealable) fiber, skipping
        /// pinned ones in place — thief mode.
        unlocked,
    };

    fn pushTail(self: *ReadyQueue, f: *Fiber) void {
        self.sl.lock();
        defer self.sl.unlock();
        f.ready_next = null;
        if (self.tail) |t| {
            t.ready_next = f;
        } else {
            self.head = f;
        }
        self.tail = f;
        self.count += 1;
    }

    fn popHead(self: *ReadyQueue, mode: PopMode) ?*Fiber {
        self.sl.lock();
        defer self.sl.unlock();
        var prev: ?*Fiber = null;
        var cur = self.head;
        while (cur) |f| {
            if (mode == .any or !f.locked) {
                if (prev) |p| p.ready_next = f.ready_next else self.head = f.ready_next;
                if (self.tail == f) self.tail = prev;
                f.ready_next = null;
                self.count -= 1;
                return f;
            }
            prev = f;
            cur = f.ready_next;
        }
        return null;
    }
};

/// Lock-free Treiber stack for cross-thread fiber injection. Push is
/// one CAS; drain is one atomic swap. The drained chain is LIFO —
/// `lifoToFifo` flips it before scheduling.
const RemoteQueue = struct {
    head: std.atomic.Value(?*Fiber) align(std.atomic.cache_line) = .init(null),

    fn push(self: *RemoteQueue, f: *Fiber) void {
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

    fn drain(self: *RemoteQueue) ?*Fiber {
        return self.head.swap(null, .acquire);
    }
};

pub const Worker = struct {
    pub threadlocal var current: ?*Worker = null;

    id: u8,
    rt: *Runtime,
    loop: xev.Loop,
    deadlines: Deadlines,
    current_fiber: ?*Fiber = null,
    ready: [Priority.count]ReadyQueue = @splat(.{}),

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

    /// Round-robin cursor for picking steal victims within our
    /// lane. Initialized to `id` so peers start at distinct offsets
    /// and don't gang up on the same first victim.
    steal_cursor: u8 = 0,

    /// Index into `rt.lane_ranges` / `rt.idle_mask` — cached at init
    /// so the hot idle path skips the linear `myLaneRange` scan.
    lane_idx: u8 = 0,
    /// Which u64 word inside `rt.idle_mask[lane_idx]` holds our bit.
    /// `(id - lane_range.start) / 64`.
    lane_word_idx: u8 = 0,
    /// Our bit within that word: `1 << ((id - lane_range.start) % 64)`.
    lane_bit_mask: u64 = 0,

    /// TSC reading at the most recent `loop.run(.no_wait)` pump.
    /// Under heavy local load we'd otherwise never reach the idle
    /// path where libxev gets polled — timer ticks, async wakes,
    /// and Deadlines completions would all stall. We pump every
    /// ~1 ms of fiber work to keep those responsive.
    last_pump_ticks: u64 = 0,

    pub fn init(w: *Worker, id: u8, rt: *Runtime) !void {
        w.id = id;
        w.rt = rt;
        w.loop = try xev.Loop.init(.{});
        errdefer w.loop.deinit();
        w.wake_async = try xev.Async.init();
        errdefer w.wake_async.deinit();
        w.deadlines = try Deadlines.init(&w.loop);
        w.current_fiber = null;
        w.ready = @splat(.{});
        w.remote_queue = .{};
        w.wake_completion = undefined;
        w.thread = null;
        w.steal_cursor = id;

        // Resolve our lane from `rt.lane_ranges`, populated by
        // `Runtime.init` before this loop. The lane offset
        // (id - lane_range.start) is split into a word index and a
        // bit within that word so masks scale to 256 workers/lane.
        var found: bool = false;
        for (rt.lane_ranges, 0..) |r, lane_i| {
            if (id >= r.start and id < r.end) {
                const off: u8 = id - r.start;
                w.lane_idx = @intCast(lane_i);
                w.lane_word_idx = off / bits_per_word;
                w.lane_bit_mask = @as(u64, 1) << @intCast(off % bits_per_word);
                found = true;
                break;
            }
        }
        std.debug.assert(found);
    }

    pub fn deinit(self: *Worker, allocator: std.mem.Allocator) void {
        self.deadlines.deinit(allocator);
        self.wake_async.deinit();
        self.loop.deinit();
    }

    pub fn scheduleLocal(self: *Worker, f: *Fiber) void {
        self.ready[@intFromEnum(f.priority)].pushTail(f);
    }

    fn popNextReady(self: *Worker, mode: ReadyQueue.PopMode) ?*Fiber {
        var i: usize = Priority.count;
        while (i > 0) {
            i -= 1;
            if (self.ready[i].popHead(mode)) |f| return f;
        }
        return null;
    }

    /// Worker range of the lane this worker belongs to. Uses
    /// `lane_idx` cached at init.
    fn myLaneRange(self: *Worker) Range {
        return self.rt.lane_ranges[self.lane_idx];
    }

    /// Try to steal one ready, unlocked fiber from a peer in our lane. Picks
    /// victims via `steal_cursor` round-robin within the lane. On success,
    /// updates the fiber's `worker_id` to us and returns it; caller is
    /// responsible for pushing it onto our ready queue (or running it
    /// directly).
    fn trySteal(self: *Worker) ?*Fiber {
        const range = self.myLaneRange();
        const width: u8 = range.end - range.start;
        if (width <= 1) return null; // alone in the lane — no peers

        const K: u8 = width - 1;
        var attempts: u8 = 0;
        while (attempts < K) {
            const peer_id: u8 = range.start + (self.steal_cursor % width);
            if (peer_id == self.id) {
                self.steal_cursor +%= 1;
                continue; // don't waste an attempt on ourselves
            }
            attempts += 1;

            const peer = &self.rt.workers[peer_id];
            if (peer.popNextReady(.unlocked)) |f| {
                // It's possible that there're more ready fibers available on
                // this worker. So, don't advance steal_cursor until we've
                // exhausted them.
                f.worker_id = self.id;
                f.times_stolen += 1;
                return f;
            } else {
                self.steal_cursor +%= 1;
            }
        }
        return null;
    }

    /// Move every fiber on our remote queue to our local ready queue.
    /// Called at the top of every outer-loop iteration so cross-thread
    /// arrivals get picked up promptly. The Treiber-stack drain is
    /// newest-first; `lifoToFifo` flips it so spawn order is preserved
    /// when peer threads push a burst of fibers to this worker.
    fn drainRemote(self: *Worker) void {
        var f: ?*Fiber = lifoToFifo(self.remote_queue.drain());
        while (f) |entry| {
            f = entry.remote_next;
            entry.remote_next = null;
            self.scheduleLocal(entry);
        }
    }

    pub fn run(self: *Worker) void {
        const prev = current;
        current = self;
        defer current = prev;

        // Seed this worker thread's TSC conversion factor from the
        // global once (set during Runtime.init by calibrateTsc).
        // After this, `cyclesToNs` reads `tsc_factor_tls` without an
        // atomic load on every dispatch.
        tsc_factor_tls = tsc_ns_per_cycle_q32.load(.acquire);

        // Seed the libxev-pump baseline so the first pump check has
        // a meaningful delta. See `last_pump_ticks` doc.
        self.last_pump_ticks = readTicks();

        fb.coro.setupStackGrowth() catch |err| {
            std.debug.panic("setupStackGrowth failed: {}", .{err});
        };
        defer fb.coro.cleanupStackGrowth();

        // Lead worker drives the process-wide coarse clock.
        if (self.id == 0) self.rt.clock.start();
        defer if (self.id == 0) self.rt.clock.stop();

        // Arm the cross-thread wake handle. The callback re-arms
        // automatically; its only job is to unstick `loop.run(.once)`
        // so the outer loop re-drains `remote_queue`.
        self.wake_async.wait(
            &self.loop,
            &self.wake_completion,
            Worker,
            self,
            wakeAsyncCallback,
        );

        // `published` tracks whether we've already announced ourselves
        // as idle this round. If we have, the next "no work found"
        // pass means we genuinely have nothing to do and should park.
        // Cleared every time we find work (so the next dry spell can
        // publish again) or after returning from `loop.run`.
        var published: bool = false;

        while (self.rt.live_count.load(.acquire) > 0) {
            // Keep libxev's `cached_now` fresh once per dispatch tick.
            // Any `xev.Timer.run` / `TCP.read`/`write`/... a fiber issues
            // computes its absolute kernel deadline as `cached_now +
            // next_ms` (see `timer_next` + `IORING_TIMEOUT_ABS`). Stale
            // cached_now → SQE armed in the past → kernel fires it
            // synchronously, making `sleep(N)` return in ~0 ms. One
            // CLOCK_MONOTONIC vDSO read per outer iteration is ~10 ns
            // and amortizes across however many libxev ops the fibers
            // we dispatch issue.
            self.loop.update_now();

            self.drainRemote();

            if (self.popNextReady(.any)) |f| {
                if (published) {
                    self.clearIdle();
                    published = false;
                }
                self.runFiber(f);
                continue;
            }

            // Nothing local ready. Try to steal from a peer in our
            // lane before falling through to libxev — work stealing
            // happens exactly when this worker would otherwise sit
            // idle, so the contention cost is "free" from the
            // owner's perspective.
            if (self.trySteal()) |stolen| {
                if (published) {
                    self.clearIdle();
                    published = false;
                }
                self.scheduleLocal(stolen);
                continue;
            }

            // First dry pass: announce that we're idle so producers
            // in `routeToWorker` can redirect new stealable fibers
            // straight to us. Then `continue` back to the top — the
            // next iteration's drainRemote/popNextReady/trySteal IS
            // the post-publish recheck, catching anything that
            // landed between our previous checks and the publish.
            if (!published) {
                self.publishIdle();
                published = true;
                continue;
            }

            // Second dry pass with `published == true`: nothing
            // arrived during the recheck, so park until libxev wakes
            // us (timer, I/O completion, or a producer notifying our
            // wake_async after redirecting work to us).
            self.loop.run(.once) catch |err| {
                std.debug.panic("xev.Loop.run failed: {}", .{err});
            };
            self.clearIdle();
            published = false;
        }
    }

    /// Run one ready fiber: bracket `coro.step()` with TSC samples
    /// for CPU accounting, pump libxev periodically so timers stay
    /// responsive under sustained local load, and rebucket the
    /// fiber per its yield intent (or retire it if it finished).
    fn runFiber(self: *Worker, f: *Fiber) void {
        self.current_fiber = f;
        const enter = readTicks();

        f.coro.step();

        const exit = readTicks();
        self.current_fiber = null;
        // Skip the reading if the counter went backwards (e.g.,
        // thread migrated mid-step to a CPU with a slightly behind
        // TSC). Real-world frequency: rare. Metrics aggregators
        // wanting tight invariants can do their own outlier
        // filtering downstream.
        if (exit >= enter) f.cpu_total_ns +%= cyclesToNs(exit - enter);

        // Pump libxev periodically so timers (coarse clock,
        // Deadlines) and async wakes (cross-thread remote pushes)
        // stay responsive even when this worker is continuously
        // busy. `no_wait` returns immediately if no completions are
        // ready — overhead is a single io_uring_enter syscall per
        // ~1 ms of work.
        if (cyclesToNs(exit -% self.last_pump_ticks) >= std.time.ns_per_ms) {
            self.loop.run(.no_wait) catch {};
            self.last_pump_ticks = exit;
        }

        if (f.isFinished()) {
            // The fiber wrote its result into JoinState; let the
            // hook flip `finished=true` and hand back any parked
            // joiner. We do the actual wake here so `fiber.zig`
            // doesn't have to import the runtime.
            if (f.on_finished(f)) |waiter| wake(waiter);
            const remaining = self.rt.live_count.fetchSub(1, .release) - 1;
            if (remaining == 0) self.rt.broadcastStop(self.id);
            return;
        }
        switch (f.intent) {
            .reschedule => self.scheduleLocal(f),
            .park => {},
        }
    }

    /// Set our bit in `rt.idle_mask` so producers can find us and
    /// redirect work directly. Release so a producer reading our
    /// bit happens-after any of our prior queue inspections.
    inline fn publishIdle(self: *Worker) void {
        _ = self.rt.idle_mask[self.lane_idx][self.lane_word_idx]
            .fetchOr(self.lane_bit_mask, .release);
    }

    /// Clear our bit. Acquire so subsequent reads of `remote_queue`
    /// / `ready` observe a producer's pre-notify writes. Idempotent:
    /// `popIdlePeer` may have already cleared the bit by picking us.
    inline fn clearIdle(self: *Worker) void {
        _ = self.rt.idle_mask[self.lane_idx][self.lane_word_idx]
            .fetchAnd(~self.lane_bit_mask, .acquire);
    }
};

/// In-place reverse a `remote_next`-linked list. The Treiber-stack
/// drain produces newest-first; the caller wants oldest-first.
fn lifoToFifo(head: ?*Fiber) ?*Fiber {
    var fifo: ?*Fiber = null;
    var lifo = head;
    while (lifo) |f| {
        lifo = f.remote_next;
        f.remote_next = fifo;
        fifo = f;
    }
    return fifo;
}

fn wakeAsyncCallback(
    _: ?*Worker,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.Async.WaitError!void,
) xev.CallbackAction {
    // The notify's job is just to unstick `loop.run(.once)`. The
    // worker's outer loop will drain `remote_queue` and check
    // `live_count` on its next iteration.
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
    /// Per-lane worker counts. `.default` lane gets whatever workers
    /// are left after the named lanes are assigned (and must end up
    /// with >= 1).
    lanes: LaneConfig = .{},
};

/// Per-lane worker counts at init. `.default` is implicit — it gets
/// `num_workers - raft`. Tests use `LaneConfig{}` (raft = 0), which
/// puts every worker in the default lane.
pub const LaneConfig = struct {
    raft: u8 = 0,
};

/// Inclusive-start, exclusive-end worker range owned by one lane.
const Range = struct { start: u8, end: u8 };

/// Bits per word in the per-lane idle mask. u64 is the largest
/// integer with single-instruction atomics on every target we care
/// about (x86_64, aarch64, RISC-V); going wider would force a libcall
/// fallback on at least one of them.
const bits_per_word: u8 = 64;
/// Worker `id` is u8 (max 256 distinct ids); one full lane therefore
/// fits in `256 / 64 = 4` u64 words. Asserted at `Runtime.init`.
const max_idle_mask_words: u8 = 4;

/// Process-wide singleton — egres expects exactly one fiber `Runtime`
/// at a time. Set by `Runtime.init`, cleared by `Runtime.deinit`.
/// Module-level helpers (`spawn`, `coarseMs`, `Group`) route through
/// it so callers don't have to thread the runtime pointer around.
var g_rt: ?*Runtime = null;

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
    live_count: std.atomic.Value(usize) = .init(0),

    /// Worker range per lane. Indexed by `@intFromEnum(Lane)`. Empty
    /// ranges (start == end) trigger fallback to `.default` at
    /// pickWorker time.
    lane_ranges: [fb.Lane.count]Range = @splat(.{ .start = 0, .end = 0 }),
    /// Per-lane round-robin counter. Used for `.shared` placement and
    /// for tie-breaking in `.exclusive` placement.
    lane_rr: [fb.Lane.count]std.atomic.Value(u8) = @splat(.init(0)),

    /// Per-lane idle worker bitmap. Bit `i` (across all words) set ⇒
    /// worker `lane_range.start + i` is parked in `loop.run(.once)`
    /// waiting for work. Producers in `routeToWorker` ctz this and
    /// CAS-clear a bit to redirect a stealable fiber straight to an
    /// idle peer, so work lands where it'll run instead of going to
    /// the originally-targeted (possibly busy) worker and getting
    /// stolen later. Four u64 words covers the full 256-worker
    /// `u8 id` range. Cache-line aligned to avoid false sharing with
    /// `lane_rr` and other Runtime hot fields.
    idle_mask: [fb.Lane.count][max_idle_mask_words]std.atomic.Value(u64) align(std.atomic.cache_line) = @splat(@splat(.init(0))),

    pub fn init(self: *Runtime, allocator: std.mem.Allocator, opts: InitOptions) !void {
        const n = opts.num_workers orelse 1;
        std.debug.assert(n >= 1);
        std.debug.assert(n <= std.math.maxInt(u8));

        // Lane ranges: contiguous worker IDs by lane. raft takes the
        // first `raft` slots; default takes the rest. Default must end
        // up with >= 1 worker.
        const raft_w: u8 = opts.lanes.raft;
        std.debug.assert(@as(usize, raft_w) < n); // default lane needs >= 1
        const default_start: u8 = raft_w;
        const default_end: u8 = @intCast(n);

        // Pay the 1 ms calibration cost up front so the first fiber
        // dispatch doesn't get charged with it. Idempotent across
        // multiple Runtimes in the same process.
        calibrateTsc();

        self.allocator = allocator;
        self.fibers = .empty;
        self.fibers_sl = .{};
        self.live_count = .init(0);
        self.lane_ranges[@intFromEnum(fb.Lane.raft)] = .{ .start = 0, .end = raft_w };
        self.lane_ranges[@intFromEnum(fb.Lane.default)] = .{ .start = default_start, .end = default_end };
        self.lane_rr = @splat(.init(0));
        self.idle_mask = @splat(@splat(.init(0)));

        // Each lane's bitmap is `max_idle_mask_words` u64s, so any
        // lane width that fits in `u8 id` (== 256) fits in the mask.
        // The asserts here are belt-and-suspenders: invalid configs
        // could still be caught by the `n <= maxInt(u8)` bound above,
        // but spelling it out here documents the invariant.
        const lane_cap: usize = @as(usize, max_idle_mask_words) * bits_per_word;
        for (self.lane_ranges) |r| {
            std.debug.assert(@as(usize, r.end - r.start) <= lane_cap);
        }

        self.workers = try allocator.alloc(Worker, n);
        errdefer allocator.free(self.workers);

        var initialized: usize = 0;
        errdefer {
            for (self.workers[0..initialized]) |*w| w.deinit(allocator);
        }
        for (self.workers, 0..) |*w, i| {
            try Worker.init(w, @intCast(i), self);
            initialized += 1;
        }

        self.clock = try CoarseClock.init();
        self.clock.bind(&self.workers[0].loop);

        // Register as the process-wide singleton. Two concurrent
        // Runtimes would silently fight for `g_rt`; the assertion
        // makes that a hard failure instead of a subtle bug.
        std.debug.assert(g_rt == null);
        g_rt = self;
    }

    pub fn deinit(self: *Runtime) void {
        std.debug.assert(g_rt == self);
        g_rt = null;

        for (self.fibers.items) |f| {
            f.deinit(self.allocator);
        }
        self.fibers.deinit(self.allocator);
        self.clock.deinit();
        for (self.workers) |*w| w.deinit(self.allocator);
        self.allocator.free(self.workers);
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

    /// Called from a worker that just decremented `live_count` to 0.
    /// Notifies every peer worker's `wake_async` so they unstick
    /// their `loop.run(.once)` and observe the zero count.
    fn broadcastStop(self: *Runtime, except_id: u8) void {
        for (self.workers) |*w| {
            if (w.id == except_id) continue;
            w.wake_async.notify() catch {};
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

/// Route a fiber to a worker's ready queue.
///
/// `redirect` controls whether to consult the lane's idle mask. Set
/// only by `spawn` — placing a fresh fiber benefits from picking an
/// idle peer (work lands where it'll run, skipping a steal hop).
/// `wake` passes `false`: resuming an existing fiber wants to stay
/// on whatever worker it was on for cache locality, mirroring how
/// Go's `goready` and Tokio's notify path both keep the wake local
/// and let stealing handle imbalance over time.
///
/// If `redirect` is set AND the fiber is not locked, claim an idle
/// peer in the target's lane via `popIdlePeer` (CAS-clear its bit)
/// and retarget the fiber to it. Otherwise the fiber goes to its
/// originally-assigned `f.worker_id`. Same-thread targets get a
/// direct local push; cross-thread targets land on `remote_queue`
/// + `wake_async.notify()`.
fn routeToWorker(rt: *Runtime, f: *Fiber, redirect: bool) void {
    if (redirect and !f.locked) {
        const lane_idx = rt.workers[f.worker_id].lane_idx;
        if (popIdlePeer(rt, lane_idx)) |idle_id| f.worker_id = idle_id;
    }
    const target = &rt.workers[f.worker_id];
    if (Worker.current == target) {
        target.scheduleLocal(f);
    } else {
        target.remote_queue.push(f);
        target.wake_async.notify() catch {};
    }
}

/// Find one idle worker in the given lane and atomically claim it
/// (CAS-clear its bit). Returns the worker's global id, or null if
/// no peer is currently parked. CAS loop because multiple producers
/// can be racing to redirect into the same lane.
fn popIdlePeer(rt: *Runtime, lane_idx: u8) ?u8 {
    const words = &rt.idle_mask[lane_idx];
    for (words, 0..) |*word, word_i| {
        var m = word.load(.acquire);
        while (m != 0) {
            const bit_i: u6 = @intCast(@ctz(m));
            const bit: u64 = @as(u64, 1) << bit_i;
            if (word.cmpxchgWeak(m, m & ~bit, .acq_rel, .acquire)) |actual| {
                m = actual;
                continue;
            }
            const off: u8 = @intCast(word_i * bits_per_word + bit_i);
            return rt.lane_ranges[lane_idx].start + off;
        }
    }
    return null;
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

/// Pick a home worker for a new fiber: round-robin within the lane's
/// range. If the requested lane has no workers (e.g., tests with the
/// default `LaneConfig` don't configure raft), fall back to
/// `.default`, which is always guaranteed to have >= 1 worker.
fn pickWorker(rt: *Runtime, lane: fb.Lane) u8 {
    var range = rt.lane_ranges[@intFromEnum(lane)];
    if (range.start == range.end) {
        range = rt.lane_ranges[@intFromEnum(fb.Lane.default)];
    }
    const width: u8 = range.end - range.start;
    const rr_start = rt.lane_rr[@intFromEnum(lane)].fetchAdd(1, .monotonic);
    return range.start + @as(u8, @intCast(rr_start % width));
}

/// Spawn `func(args...)` as a new fiber on the process-wide runtime.
/// The args tuple is copied into a heap-allocated context owned by
/// the Fiber (see `Fiber.init`); callers only need to keep alive
/// what the args point to. Returns a `JoinHandle(T)` where `T` is
/// `func`'s return type — `join()` retrieves the value once the body
/// finishes.
pub fn spawn(
    opts: SpawnOptions,
    comptime func: anytype,
    args: std.meta.ArgsTuple(@TypeOf(func)),
) !JoinHandle(fb.ReturnOf(func)) {
    const rt = g_rt orelse @panic("fiber.spawn: Runtime not initialized");
    // Pick home worker: explicit `worker_id` wins, else lane-aware
    // placement via `pickWorker`.
    const home_id: u8 = opts.worker_id orelse pickWorker(rt, opts.lane);
    std.debug.assert(home_id < rt.workers.len);

    const f = try Fiber.init(rt.allocator, opts, func, args);
    errdefer f.deinit(rt.allocator);

    f.worker_id = home_id;
    f.locked = opts.locked;

    try rt.appendFiber(f);

    f.state.store(@bitCast(State{ .tag = .ready, .awaken = false }), .release);

    _ = rt.live_count.fetchAdd(1, .release);
    // Spawn — eligible to redirect to an idle peer in the lane.
    routeToWorker(rt, f, true);

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
/// and routes it through `routeToWorker` — same-thread direct push
/// or cross-thread `remote_queue` + `wake_async.notify`.
pub fn park() void {
    const w = currentWorker();
    const f = w.current_fiber.?;

    var old = f.loadState();
    while (true) {
        std.debug.assert(old.tag == .ready);
        if (old.awaken) {
            const new = State{ .tag = .ready, .awaken = false };
            if (f.casState(old, new)) |actual| {
                old = actual;
                continue;
            }
            return; // token consumed; don't yield
        }
        const new = State{ .tag = .waiting, .awaken = false };
        if (f.casState(old, new)) |actual| {
            old = actual;
            continue;
        }
        break;
    }

    f.parked_since_ms = currentWorker().rt.clock.ms();
    captureTrace(&f.park_trace);

    f.intent = .park;
    f.coro.yield();
}

/// Wake a fiber. Safe to call on a fiber in any state: `.finished`
/// is a no-op; `.ready` deposits an awaken token for a concurrent
/// `park` to consume; `.new` / `.waiting` transitions to `.ready`
/// and routes the fiber to its home worker's queue.
///
/// Routing is home-worker-pinned: if the caller is on the fiber's
/// home thread, the push goes directly to the home's local ready
/// queue. Otherwise the fiber lands on the home's `remote_queue`
/// and we notify the home's `wake_async` to unstick its loop.
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

    const w = currentWorker();
    // Wake — keep the fiber on its current worker for cache locality;
    // stealing will rebalance over time if needed.
    routeToWorker(w.rt, f, false);
}

pub fn currentWorker() *Worker {
    return Worker.current.?;
}

pub fn currentFiber() *Fiber {
    return currentWorker().current_fiber.?;
}

/// Null-safe variant for callers outside the fiber dispatch (signal
/// handlers, test-runner logFn, watchdog) that need to ask "are we
/// inside a fiber right now?" without panicking. Returns null when
/// called from a non-worker thread, or from a worker thread that's
/// between fibers (in `loop.run`, `drainRemote`, etc.).
pub fn currentFiberOrNull() ?*Fiber {
    const w = Worker.current orelse return null;
    return w.current_fiber;
}

/// Process-wide active `Runtime`. Sources from the `g_rt` singleton
/// rather than `Worker.current`, so any thread (not just one
/// currently running a fiber) can call it after `Runtime.init`.
/// Panics if no runtime is initialized.
pub fn currentRuntime() *Runtime {
    return g_rt orelse @panic("fiber.currentRuntime: Runtime not initialized");
}

/// Coarse millisecond clock — same value the in-runtime fibers see.
/// One atomic load, callable from any thread (fiber-context or not)
/// as long as the runtime has been initialized. Panics if called
/// before `Runtime.init` or after `Runtime.deinit`.
pub inline fn coarseMs() i64 {
    const r = g_rt orelse @panic("fiber.coarseMs: Runtime not initialized");
    return r.ms();
}

pub fn dumpFibers(rt: *Runtime, writer: *std.Io.Writer) !void {
    rt.fibers_sl.lock();
    defer rt.fibers_sl.unlock();
    const now_ms = rt.clock.ms();

    // Per-worker queue depths first — the cheap, summary view that
    // shows imbalance at a glance before the per-fiber dump.
    try writer.writeAll("workers:\n");
    for (rt.workers, 0..) |*w, wid| {
        var ready_total: u32 = 0;
        for (w.ready) |q| ready_total += q.count;
        try writer.print(
            "  w{d:>2} ready={d} (normal={d} high={d})\n",
            .{ wid, ready_total, w.ready[@intFromEnum(fb.Priority.normal)].count, w.ready[@intFromEnum(fb.Priority.high)].count },
        );
    }
    try writer.writeAll("fibers:\n");

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
        try writer.print(" cpu_total={d}ms times_stolen={d}\n", .{ @divTrunc(f.cpu_total_ns, std.time.ns_per_ms), f.times_stolen });
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
        const ns_start = coarse.clockNsUseRarely(.monotonic);
        const tsc_start = readTicks();
        _ = std.posix.system.nanosleep(&req, null);
        const tsc_end = readTicks();
        const ns_end = coarse.clockNsUseRarely(.monotonic);

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
