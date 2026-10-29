// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Multi-threaded fiber scheduler. The `Runtime` owns N `Worker`s,
//! each driving its own `xev.Loop` on its own OS thread (worker 0 on
//! the calling thread, workers 1..N-1 on `std.Thread.spawn`).
//! High-priority fibers are pinned to their home worker; normal
//! priority runs anywhere in its lane — an out-of-work worker steals
//! from busy lane peers, throttled by a per-lane `num_thieves`
//! count: producers wake at most one thief at a time, and the last
//! thief to find work wakes the next.
//!
//! Cross-thread injection: `spawn` / `wake` for a fiber that can't go on
//! the current worker pushes STRAIGHT into a target worker's spinlock-
//! guarded ready ring (high → the home worker's `ready[high]`; normal →
//! a lane worker via `injectNormal`), then `xev.Async.notify()` unsticks
//! that worker's `loop.run(.once)` if it is parked. No side queues — the
//! rings are the cross-thread inboxes.
//!
//! `yield()` / `park()` / `wake()` are namespace-level free functions
//! that operate on the currently-running fiber, looked up via
//! `currentWorkerOrNull()` (the migration-safe accessor for the
//! `current_do_not_use_directly` thread-local).

const std = @import("std");
const log = std.log.scoped(.fiber);
const _meter = @import("meter.zig");
const builtin = @import("builtin");
const xev = @import("xev");
const _fb = @import("fiber.zig");
const _coarse = @import("utils").coarse_clock;
const _deadlines = @import("deadlines.zig");
const _spinlock = @import("spinlock.zig");
const _queue = @import("queue.zig");
const _fsops = @import("fsops.zig");
const _context = @import("utils").context;

/// Flush a busy worker's io_uring SQ once this many SQEs have queued, so reads
/// issued by running fibers dispatch promptly instead of waiting for the ~1 ms
/// pump. Small enough to bound dispatch latency, large enough to keep ~1
/// `io_uring_enter` per batch; sub-threshold stragglers still flush on the idle
/// path (when the ready queue drains) and on the pump backstop.
const io_submit_batch: usize = 4;

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

const Fiber = _fb.Fiber;
const State = _fb.State;
const Tag = _fb.Tag;
const Priority = _fb.Priority;
const Lane = _fb.Lane;
const SpawnOptions = _fb.SpawnOptions;
const FiberFn = _fb.FiberFn;
const YieldIntent = _fb.YieldIntent;

const LockedLocalQueue = _queue.LocalQueue(.withlock);
const IdleWorkers = _queue.IdleWorkers;

pub const Deadlines = _deadlines.Deadlines;
pub const Timeout = _deadlines.Timeout;
pub const Spinlock = _spinlock.Spinlock;

/// Scratch chunk for `trySteal`: a thief copies up to this many fibers out of
/// a victim under its lock, releases, enqueues them on its own ring, then
/// RE-locks the victim to take more — looping until it's BALANCED with the
/// victim (geometric load balancing: a burst spreads across the lane in
/// ~log(workers) rounds, no single hot victim). Chunking (vs. one big copy)
/// keeps each lock hold short — ~32 pointers — so several thieves can drain a
/// deep victim concurrently instead of one monopolizing its lock. 32 = the
/// initial ring capacity, so a shallow victim is balanced in a single bite.
const steal_chunk: usize = 32;

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

/// Contiguous worker-id range owned by a lane. `len == 0` means the
/// lane has no workers configured; routing falls back to `.default`.
const LaneRange = struct { start: u8, len: u8 };

/// Cache-line-padded atomic counter; see `Runtime.num_thieves`.
const ThiefCount = struct {
    v: std.atomic.Value(usize) align(std.atomic.cache_line) = .init(0),
};

pub const Worker = struct {
    /// The worker running on this OS thread. Do NOT read directly — use
    /// `currentWorkerOrNull()` / `currentWorker()`, which are `noinline` +
    /// barriered so the read isn't hoisted across a coro `yield()` (fibers
    /// migrate between worker threads, so a cached value goes stale). Only
    /// `Worker.run` writes it.
    pub threadlocal var current_do_not_use_directly: ?*Worker = null;

    id: u8,
    /// Index into `Runtime.lane_ranges` / `.idle` for the lane this worker
    /// belongs to. Set in `Runtime.init`.
    lane_idx: u8 = 0,
    loop: xev.Loop,
    deadlines: Deadlines,
    current_fiber: ?*Fiber = null,

    /// Track the total cpu time spent processing fibers on this worker. Helps
    /// identify if the work is getting distributed evenly across workers.
    cpu_total_ns: u64 = 0,
    /// TSC reading at the last libxev pump (see `dispatch`). Lets a
    /// continuously-busy worker still service timers / async wakes about
    /// once per millisecond without blocking.
    last_pump_ticks: u64 = 0,
    /// TSC reading at this worker's last coarse-clock tick. Gates the
    /// once-per-ms `clock.update()` at the top of the run loop. Every
    /// worker drives the clock now, so every worker carries this.
    last_coarse_clock_ticks: u64 = 0,
    /// Whether this worker holds a +1 in its lane's `num_thieves` — it is
    /// out stealing, or was woken by a producer to steal. Spans from the
    /// wake (or running dry) until work is found, NOT just the steal scan:
    /// the +1 is what suppresses redundant producer wakes while this worker
    /// travels to the work. Worker-private — only this worker writes it; a
    /// producer hands the state off via the claimed idle bit (see `unpark`),
    /// never by touching this field.
    is_thief: bool = false,
    /// xorshift64 state for steal-victim randomization; seeded in `run`.
    rng_state: u64 = 1,
    /// TSC reading when this worker last finished a fiber step; the
    /// pre-park poll runs only within a millisecond of it.
    last_work_ticks: u64 = 0,
    /// TSC reading up to which the running step is charged to its
    /// fiber's tenant: the step's start, moved by each `settleCpu`.
    charged_ticks: u64 = 0,
    /// Wake-path counters, worker-private (only this worker writes them;
    /// `dumpWorkerCpu` reads them racily). Together they say whether an
    /// idle peer is actually told about work — see `WakeStats`.
    wake_stats: WakeStats = .{},
    /// Two ready rings, one per `Priority`, allocated in `Worker.init`.
    /// Spinlock-guarded (`LocalQueue(.withlock)`) so a same-lane peer can
    /// steal from the `ready[normal]` ring (see `trySteal`). The owner's own
    /// push/pop pay an uncontended lock (~1.4 ns); `ready[high]` is pinned and
    /// never stolen, so its lock is always uncontended. Both rings grow on
    /// demand — the normal ring under bursty load; the high ring (≈1 resident
    /// in practice, measured) effectively never — so `push` never rejects.
    ready: [Priority.count]LockedLocalQueue,

    /// Cross-thread wake mechanism. A producer that pushed a fiber into one
    /// of our rings (high direct, normal via `injectNormal`) calls
    /// `wake_async.notify()` to unstick our `loop.run(.once)` if we're parked.
    wake_async: xev.Async,
    wake_completion: xev.Completion = undefined,

    /// OS thread running this worker. `null` for worker 0 (lives on
    /// the calling thread that invoked `Runtime.run`).
    thread: ?std.Thread = null,
    /// The kernel thread id, set by `runMain` on Linux: the profiler
    /// arms a CPU-clock timer per worker with it. 0 elsewhere.
    tid: i32 = 0,

    pub fn init(
        w: *Worker,
        id: u8,
        allocator: std.mem.Allocator,
        io_pool: ?*xev.ThreadPool,
    ) !void {
        // Construct via a struct literal so every field with a declared
        // default actually gets it.
        w.* = .{
            .id = id,
            .loop = try xev.Loop.init(.{ .thread_pool = io_pool }),
            .wake_async = undefined,
            .deadlines = undefined,
            .ready = undefined,
        };
        errdefer w.loop.deinit();
        w.wake_async = try xev.Async.init();
        errdefer w.wake_async.deinit();
        w.deadlines = try Deadlines.init(&w.loop, allocator);
        errdefer w.deadlines.deinit();
        // Allocate the two per-priority ready rings; on a mid-loop failure,
        // deinit the ones already built before propagating.
        var made: usize = 0;
        errdefer for (w.ready[0..made]) |*q| q.deinit();
        for (&w.ready) |*q| {
            q.* = try LockedLocalQueue.init(allocator, _queue.local_queue_capacity);
            made += 1;
        }
    }

    pub fn deinit(self: *Worker) void {
        for (&self.ready) |*q| q.deinit();
        self.deadlines.deinit();
        self.wake_async.deinit();
        self.loop.deinit();
    }

    /// Enqueue `f` on this worker's matching priority ring. The ring grows on
    /// demand, so a push can only fail on allocator OOM (fatal) — there is no
    /// overflow/spill path anymore.
    pub fn scheduleLocal(self: *Worker, f: *Fiber) void {
        self.ready[@intFromEnum(f.priority)].push(f) catch
            @panic("out of memory growing local ready ring");
        // Pushed locally. When normal work piles up (>1 queued) wake one idle
        // lane peer to come steal a share: a peer that already parked won't
        // otherwise notice a burst that fits within our ring (no spill, no
        // notify). This per-push call is the IGNITION of a steal cycle and
        // is only affordable because `notifyIdlePeer` checks
        // `num_thieves == 0` before doing the actual notify. Skipped when
        // stealing is off (a peer couldn't take the work). High priority is
        // pinned and never stolen.
        //
        // `!self.is_thief` short-circuits the whole thing when WE are the
        // thief — the common case here is a `trySteal` batch enqueuing
        // stolen fibers, one `scheduleLocal` each. Our own `+1` is already in
        // `num_thieves`, so `notifyIdlePeer` would just `load() > 0` and
        // bail; skipping avoids re-reading that shared atomic for every fiber
        // in the batch, and the thief handoff happens via the chain
        // (`exitThiefAndNotify`) once we dispatch the loot, not here.
        const rt = runtime();
        if (f.priority == .normal and rt.steal_enabled and !self.is_thief and
            self.ready[@intFromEnum(Priority.normal)].len() >= 2)
        {
            self.wake_stats.notify_tries += 1;
            rt.notifyIdlePeer(self);
        }
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

    /// Pick the next fiber to dispatch: high priority first (pinned,
    /// latency-sensitive), then normal. Ring FIFO order alone gives fairness
    /// now that there's no global queue — a fiber that yields re-queues at the
    /// back, so everything ahead of it drains first; nothing can be starved by
    /// a worker keeping itself locally busy (the old per-N-dispatch global
    /// poll is gone with the queue it served).
    fn nextFiber(self: *Worker) ?*Fiber {
        if (self.ready[@intFromEnum(Priority.high)].pop()) |f| return f;
        return self.ready[@intFromEnum(Priority.normal)].pop();
    }

    /// Pre-park recheck of our own rings, BOTH under their locks so a
    /// cross-thread push can't be missed. A lock-free `size` peek can read a
    /// STALE value — the push stores `size` `.monotonic` under the lock, and
    /// the parker-vs-producer pair is the store-buffering (Dekker) litmus, so
    /// nothing short of locking (or seq_cst on the hot push, which we won't
    /// pay) synchronizes them; the worker could otherwise block with work in
    /// its ring (lost wakeup). The ring lock's acquire reads-from the push's
    /// unlock-release — a genuine reads-from pair — which is the
    /// synchronization the deleted global queue used to provide. Both
    /// priorities now arrive by direct ring push (normal via `injectNormal`,
    /// high via `routeReady`), so both need it.
    fn hasReady(self: *Worker) bool {
        for (&self.ready) |*q| {
            if (q.lenAcquire() > 0) return true;
        }
        return false;
    }

    /// Try to refill our (empty) normal ring by stealing from a busy peer in
    /// the SAME lane. Returns true if any work moved. Only normal-priority
    /// work is stealable — high priority is pinned to its home worker and
    /// never touched. Visits lane peers from a RANDOM start so repeat scans
    /// and concurrent thieves decorrelate.
    ///
    /// For each candidate we first peek its size LOCK-FREE and skip it unless
    /// it holds more than we do — so a scan over mostly-empty rings pays no
    /// lock. From the first victim with a surplus we steal until BALANCED with
    /// it (our normal ring ≈ the victim's), in `steal_chunk`-sized bites: each
    /// `stealInto` takes half the CURRENT difference (we pass our own growing
    /// ring size as `dest_sz`), copies under the victim's lock, releases, then
    /// enqueues locally via `scheduleLocal` (one lock at a time — never two,
    /// so no deadlock) before the next bite. Re-reading the difference each
    /// bite lets several thieves share one deep victim: each stops at the
    /// balance point instead of the first two draining it to empty. `stealInto`
    /// returns 0 once we've reached balance (or the victim drained).
    fn trySteal(self: *Worker) bool {
        const rt = runtime();
        if (!rt.steal_enabled) return false; // stealing off (InitOptions.steal)
        self.wake_stats.steal_scans += 1;

        const range = rt.lane_ranges[self.lane_idx];
        if (range.len <= 1) return false; // lane has no peers to steal from

        const sq = &self.ready[@intFromEnum(Priority.normal)];
        const me = self.laneOffset();
        const r: usize = @intCast(self.nextRand() % range.len);
        var k: usize = 0;
        while (k < range.len) : (k += 1) {
            const off: u8 = @intCast((r + k) % range.len);
            if (off == me) continue;
            const victim = &rt.workers[@as(usize, range.start) + off];
            const vq = &victim.ready[@intFromEnum(Priority.normal)];

            // Lock-free pre-check: only a victim with a surplus over us is
            // worth locking (this is `stealInto`'s `size <= dest_sz` guard,
            // peeked without the lock so we skip empties for free). A lone
            // ready fiber stays put: its owner is one step away from it,
            // and moving it moves its next I/O completion too — that
            // ping-pong ran at ~3 migrations per statement under pgbench.
            const vlen = vq.len();
            const lone = rt.steal_lone.load(.monotonic);
            if ((!lone and vlen < 2) or vlen <= sq.len()) continue;

            var scratch: [steal_chunk]*Fiber = undefined;
            var moved = false;
            // Pass our own (growing) ring size so each bite targets the
            // victim/thief balance point; stops when stealInto returns 0.
            while (true) {
                const n = vq.stealInto(sq.len(), scratch[0..]);
                if (n == 0) break; // balanced with the victim, or it drained
                self.wake_stats.stolen += n;

                for (scratch[0..n]) |f| {
                    if (comptime _fb.guard_level >= 1) {
                        // The stolen fiber must be runnable in the victim's ring.
                        const st = f.loadState();
                        if (st.tag != .ready) std.debug.panic(
                            "stole fiber 0x{x} state=.{s} awaken={} from W{d}→W{d} — non-runnable in victim ring",
                            .{ @intFromPtr(f), @tagName(st.tag), st.awaken, victim.id, self.id },
                        );
                    }
                    self.scheduleLocal(f);
                }
                moved = true;
            }
            if (moved) {
                self.wake_stats.steal_hits += 1;
                return true;
            }
        }
        return false;
    }

    /// This worker's bit offset within its lane's idle bitmap.
    fn laneOffset(self: *const Worker) u8 {
        return self.id - runtime().lane_ranges[self.lane_idx].start;
    }

    fn markIdle(self: *Worker) void {
        runtime().idle[self.lane_idx].markIdle(self.laneOffset());
    }

    /// xorshift64 — cheap per-worker PRNG for steal-victim selection.
    fn nextRand(self: *Worker) u64 {
        var x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        return x;
    }

    /// Try to become a thief (enter the thieving state). Denied — the caller
    /// then skips stealing and parks — when stealing is off, the lane has no
    /// peers, or half the lane is already thieving: more thieves than that
    /// mostly contend on victim spinlocks without finding proportionally more
    /// work. The check-then-add can briefly overshoot the cap under a race;
    /// harmless slack.
    fn becomeThief(self: *Worker, rt: *Runtime) bool {
        if (self.is_thief) return true;
        if (!rt.steal_enabled) return false;

        const range = rt.lane_ranges[self.lane_idx];
        if (range.len <= 1) return false;

        const n = &rt.num_thieves[self.lane_idx].v;
        if (2 * n.load(.seq_cst) >= range.len) return false;

        _ = n.fetchAdd(1, .seq_cst);
        self.is_thief = true;
        return true;
    }

    /// Leave the thieving state because we found work. If we were the LAST
    /// thief, hand the hunt to a parked peer: what we found may not be all
    /// the work, and with `num_thieves` back at zero, producers would
    /// otherwise be the only ones able to restart the hunt.
    fn exitThiefAndNotify(self: *Worker, rt: *Runtime, do_notify: bool) bool {
        // No-op unless we hold a +1: the park path calls this
        // unconditionally, and a worker that was DENIED thieving (cap /
        // steal off / single-worker lane) parks with is_thief == false —
        // an unguarded fetchSub would wrap num_thieves to maxInt and
        // permanently suppress every producer wake in the lane.
        if (!self.is_thief) return false;
        self.is_thief = false;
        if (rt.num_thieves[self.lane_idx].v.fetchSub(1, .seq_cst) == 1) {
            if (do_notify) rt.notifyIdlePeer(self);
            return true;
        }
        return false;
    }

    /// Poll instead of blocking, for up to `prepark_poll_us`: submit what
    /// the fibers queued (their sends), then watch the completion ring —
    /// which also carries a peer's eventfd wake — and our own rings. We
    /// are marked idle throughout, so a producer may claim our bit
    /// meanwhile; that ends the poll too, since the claim comes with a
    /// thief +1 we must go and honour. true = something to do.
    fn pollBeforePark(self: *Worker, rt: *Runtime) bool {
        const budget_ns =
            rt.prepark_poll_us.load(.monotonic) * std.time.ns_per_us;
        const start = readTicks();
        self.loop.run(.no_wait) catch {};
        while (true) {
            if (self.hasReady()) return true;
            if (!rt.idle[self.lane_idx].isIdle(self.laneOffset())) return true;
            if (completionsReady(&self.loop) > 0) {
                self.loop.run(.no_wait) catch {};
                continue;
            }
            const spent = cyclesToNs(readTicks() -% start);
            if (spent >= budget_ns) return false;
            std.atomic.spinLoopHint();
        }
    }

    /// Leave the parked state: atomically take our idle bit back. If a
    /// producer claimed it first, this wake was deliberate — the producer
    /// registered us in `num_thieves` BEFORE claiming (`notifyIdlePeer`),
    /// so we wake up already owning a thief +1 and must adopt the state;
    /// the dispatch / park paths balance it. A spurious wake (own timer,
    /// I/O completion, stop) finds the bit still set and just clears it.
    fn unpark(self: *Worker, rt: *Runtime) void {
        if (!rt.idle[self.lane_idx].selfClear(self.laneOffset())) {
            self.is_thief = true;
            self.wake_stats.thief_wakes += 1;
        }
    }

    /// Pin this worker's OS thread to one CPU: the `id`-th CPU of the
    /// process's allowed set (`sched_getaffinity`), wrapping past the
    /// end when there are more workers than CPUs. A pinned worker never
    /// migrates, so its rings, io_uring and TSC reads stay on one core
    /// and "an idle worker" means "an idle core". Linux only; a failure
    /// is logged and ignored — pinning is a performance property, not a
    /// correctness one.
    // TODO: evaluate placing the raft lane's workers (the main loop and
    // the apply loop) on the two threads of one physical core (CPU i and
    // its SMT sibling, 0 and 16 on the bench box). They are light, hand
    // each other data every batch, and would share L1/L2; the freed
    // cores would go to SQL workers. Needs a worker-to-CPU map that is
    // set before the workers start (setting it later races their first
    // pin), and an A/B on tpcb and select-only.
    fn pinToCpu(self: *Worker) void {
        if (builtin.os.tag != .linux) return;
        const linux = std.os.linux;
        var allowed: linux.cpu_set_t = @splat(0);
        const size = @sizeOf(linux.cpu_set_t);
        const rc = linux.sched_getaffinity(0, size, &allowed);
        if (@as(isize, @bitCast(rc)) < 0) {
            log.warn("W{d}: sched_getaffinity failed", .{self.id});
            return;
        }
        var count: usize = 0;
        for (allowed) |word| count += @popCount(word);
        if (count == 0) return;
        // More workers than allowed CPUs: pinning would stack them on
        // shared CPUs and take the kernel's balancing away. Leave every
        // worker free instead.
        const total = runtime().workers.len;
        if (total > count) {
            if (self.id == 0) log.warn(
                "{d} workers on {d} allowed CPUs: workers left unpinned",
                .{ total, count },
            );
            return;
        }
        // The id-th set bit of the mask is our CPU.
        var nth = @as(usize, self.id) % count;
        var cpu: usize = 0;
        outer: for (allowed, 0..) |word, wi| {
            var bits = word;
            while (bits != 0) : (bits &= bits - 1) {
                if (nth == 0) {
                    cpu = wi * @bitSizeOf(usize) + @ctz(bits);
                    break :outer;
                }
                nth -= 1;
            }
        }
        var set: linux.cpu_set_t = @splat(0);
        const word_bits = @bitSizeOf(usize);
        const shift: std.math.Log2Int(usize) = @intCast(cpu % word_bits);
        set[cpu / word_bits] |= @as(usize, 1) << shift;
        linux.sched_setaffinity(0, &set) catch |err| {
            log.warn("W{d}: pin to cpu {d} failed: {s}", .{
                self.id, cpu, @errorName(err),
            });
        };
    }

    /// Cross-thread wake: issue the `wake_async` notify (an eventfd write —
    /// a syscall) ONLY if this worker is parked. A busy worker finds the
    /// just-pushed work on its own next pass, no syscall needed. The caller
    /// MUST publish the work BEFORE calling this — a `ready[high]` push (high
    /// priority) or a `ready[normal]` push (`injectNormal`). Push-then-peek
    /// against the parker's markIdle-then-recheck is a Dekker pair, closed by
    /// the seq_cst `isIdle` load and `markIdle` fetchOr: either our load sees
    /// the bit and we notify, or it ordered before the fetchOr and then the
    /// parker's later recheck must see our push — and that recheck
    /// (`hasReady`) takes the ring lock, whose acquire reads-from our push's
    /// unlock-release, so it's never stale. Portable; not reliant on x86
    /// locked-RMW fencing.
    inline fn notifyIfParked(self: *Worker) void {
        if (runtime().idle[self.lane_idx].isIdle(self.laneOffset()))
            self.wake_async.notify() catch {};
    }

    /// Step one fiber once. Returns true if its body finished this dispatch.
    /// Does NOT wake the joiner or free the fiber — `Worker.run` does both AFTER
    /// charging CPU time, so the per-fiber charge can't touch a fiber a joiner
    /// (or the detached-free) already reclaimed. A non-finished fiber is
    /// rescheduled / parked here and returns false.
    fn runFiber(self: *Worker, f: *Fiber) bool {
        // Catch a clobbered/garbage dispatch BEFORE the step jumps in, on a
        // valid (the dispatching) stack rather than as a silent SIGSEGV. Always
        // on — one u64 compare per dispatch is negligible next to the switch.
        f.assertMagic();
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
            if (comptime _fb.guard_level >= 1) {
                // Claim the step FIRST: if `running` reaches 2 a peer is
                // concurrently stepping this fiber, so the context reads below
                // would be a torn-read race. Firing this guard (not the rsp one)
                // disambiguates "double-step" from "real context corruption".
                const concurrent = f.running.fetchAdd(1, .acq_rel) + 1;
                if (concurrent >= 2) std.debug.panic(
                    "fiber 0x{x} stepped concurrently by {d} workers (this worker = {d})",
                    .{ @intFromPtr(f), concurrent, self.id },
                );
                // A fiber in a ready ring must be runnable (.ready). If a
                // non-runnable fiber (.waiting/.finished/.new) reaches dispatch,
                // a state/queue desync put it there — catch it before the step
                // corrupts its coro stack. `awaken` may be set (a benign racing
                // wake token); only `tag` matters.
                const st = f.loadState();
                if (st.tag != .ready) std.debug.panic(
                    "dispatch fiber 0x{x} state=.{s} awaken={} on W{d} — non-runnable fiber in ready ring",
                    .{ @intFromPtr(f), @tagName(st.tag), st.awaken, self.id },
                );
            }
            defer if (comptime _fb.guard_level >= 1) {
                _ = f.running.fetchSub(1, .release);
            };

            // SEED HUNT: page 0 (saved context) is read-only while queued;
            // unlock it only for the step itself (which saves/restores rsp/rbp/
            // rip + parent_context). A wild write while it was queued already
            // faulted at the culprit. Compiles to nothing when the guard is off.
            f.unprotectContext();
            f.coro.step();
            f.protectContext();
            self.current_fiber = null;

            break :blk f.isFinished();
        };
        // Finished: leave waking the joiner + freeing to `Worker.run` (after it
        // charges CPU). A finished fiber simply leaves the scheduler — don't
        // reschedule it.
        if (finished) return true;

        // Guard released and the fiber is now genuinely suspended, so it
        // is safe to make it re-runnable on any worker.
        switch (f.intent) {
            .reschedule => self.scheduleLocal(f),
            .park => self.commitPark(f),
        }
        return false;
    }

    /// Charges the step's ticks since `charged_ticks` to the fiber's
    /// tenant, in the tick counter's unit its cpu limit was converted
    /// to, and moves the mark. Once at the end of every step, and from
    /// `Ctx.maybeYield` so a long step pays per batch, not when it
    /// finally parks. Returns the reading.
    pub fn chargeCpu(self: *Worker, f: *Fiber) u64 {
        const now = readTicks();
        const ticks = now -| self.charged_ticks;
        self.charged_ticks = now;
        if (ticks > 0) f.ctx.tenant.charge(
            .cpu,
            f.ctx.class,
            @intCast(ticks),
            _coarse.ms(),
        );
        return now;
    }

    pub fn runMain(self: *Worker) void {
        // The one legitimate site that writes the thread-local. Reads go
        // through `currentWorkerOrNull()`; here we own it directly.
        const prev = current_do_not_use_directly;
        current_do_not_use_directly = self;
        defer current_do_not_use_directly = prev;
        if (builtin.os.tag == .linux) self.tid = std.os.linux.gettid();

        if (runtime().pin_workers) self.pinToCpu();

        // Seed this worker thread's TSC conversion factor from the
        // global once (set during Runtime.init by calibrateTsc).
        // After this, `cyclesToNs` reads `tsc_factor_tls` without an
        // atomic load on every dispatch.
        tsc_factor_tls = tsc_ns_per_cycle_q32.load(.acquire);

        // No `coro.setupStackGrowth`: fiber stacks are fully committed at
        // alloc time and never grow (see `fiber.zig`), so we don't install
        // the SIGSEGV growth handler. A body that overruns its `stack_size`
        // hits the guard page → a normal segfault backtrace.

        // Every worker advances the process-wide coarse clock from the top
        // of its loop (gated to ~once/ms below). Many drivers means the
        // clock stays fresh as long as ANY worker is busy; it only freezes
        // in full-system idle, which is benign (no fiber is running to read
        // it). Seed it once here so ms() is fresh before this worker
        // dispatches its first fiber.
        _coarse.update();

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

        // Seed the pump + coarse-clock TSC gates so the first periodic pump
        // and the next clock tick each land ~1 ms in (we just updated the
        // clock above, so don't re-tick on the very first iteration).
        const seed_ticks = readTicks();
        self.last_pump_ticks = seed_ticks;
        self.last_coarse_clock_ticks = seed_ticks;
        // Steal-victim PRNG: TSC entropy, id-mixed so workers starting the
        // same cycle still diverge; `| 1` keeps the xorshift state nonzero.
        self.rng_state = (seed_ticks ^ (@as(u64, self.id) << 32)) | 1;
        self.loop.update_now();

        const rt = runtime();
        while (!rt.stopping.load(.acquire)) {
            // 1. Run the local ready queues to exhaustion first (high
            //    before normal). Cross-thread arrivals land directly in
            //    these rings, so `nextFiber` (which pops high first) picks
            //    them up every pass — no separate drain step. trySteal is
            //    the only work-*finding* step, taken only when we run dry.

            const enter = readTicks();
            self.charged_ticks = enter;
            if (cyclesToNs(enter -% self.last_coarse_clock_ticks) >= std.time.ns_per_ms) {
                // Advance the coarse clock at most once per ms (TSC-gated, no
                // syscall on the fast path). `enter` is reused for CPU
                // accounting below, so the per-iteration cost is a subtract +
                // compare; the write-guard in `update` keeps it to ~1 store/ms
                // across all workers.
                _coarse.update();
                self.last_coarse_clock_ticks = enter;
            }

            if (self.nextFiber()) |f| {
                // A thief that found work stops thieving; the last one
                // out hands the hunt to a parked peer (wake chain).
                if (self.is_thief) _ = self.exitThiefAndNotify(rt, true);

                const finished = self.runFiber(f);
                const exit = self.chargeCpu(f);
                self.last_work_ticks = exit;
                // Skip the reading if the counter went backwards (thread
                // migrated mid-step to a CPU with a slightly behind TSC).
                const cycNs = cyclesToNs(exit -| enter);
                f.cpu_total_ns +%= cycNs;
                self.cpu_total_ns +%= cycNs;

                // Body returned: CPU is now charged, so a joiner reading
                // `cpu_total_ns` sees this final slice. `on_finished` is our
                // LAST touch of `f`: it sets `released` under the join lock,
                // after which (or after the wake below) the joiner may reap
                // and free `f` at any instant — hence `detached` is read
                // BEFORE the hook, and nothing touches `f` afterward. A
                // JOINED fiber is freed by its handle's `join()`; a DETACHED
                // fiber has no handle, so free it here.
                if (finished) {
                    const detached = f.detached;
                    if (f.on_finished(f)) |waiter| wake(waiter);
                    if (detached) runtime().freeFiber(f);
                }

                // Pump libxev when there's I/O to move, without waiting for the
                // periodic backstop. Both ring peeks are free userspace reads
                // (and 0 on non-io_uring backends):
                //   - completionsReady: CQEs the kernel already posted → reap
                //     now so the woken fibers join the ready queue THIS pass,
                //     not up to ~1 ms later.
                //   - pendingSubmissions: SQEs queued by running fibers → flush
                //     a batch so the reads actually dispatch.
                // `run(.no_wait)` submits + reaps in one syscall and returns
                // immediately. The ~1 ms timer is the backstop (keeps timers /
                // async wakes live when I/O is sparse) and is LAST in the `or`
                // so its division is only computed when neither peek fired.

                if (completionsReady(&self.loop) > 0 or
                    pendingSubmissions(&self.loop) >= io_submit_batch or
                    cyclesToNs(enter -% self.last_pump_ticks) >= std.time.ns_per_ms)
                {
                    self.loop.run(.no_wait) catch {};
                    self.last_pump_ticks = enter;
                }
                continue;
            }

            // 2. `nextFiber` just did a LOCKED pop of both rings and found
            //    nothing, so re-checking our own rings here would be redundant
            //    — become a thief (throttled to half the lane) and try to
            //    steal a batch of normal-priority work from a busy peer. The
            //    spinlock-guarded rings make this race-free against the
            //    victim's own push/pop; high priority is pinned and never
            //    stolen. A stolen batch lands in our ring; the `nextFiber` pop
            //    above then exits the thieving state (chain-notifying a peer
            //    if we were last). A cross-thread push that lands in our OWN
            //    ring after nextFiber is caught by step 3's locked pre-park
            //    recheck, so we don't re-check before stealing.
            if (self.becomeThief(rt) and self.trySteal()) continue;

            // 3. Still nothing — about to park. Drop the thieving state and
            //    markIdle BEFORE the final recheck, because producers publish
            //    work and THEN consult num_thieves / the idle bitmap; doing
            //    it after guarantees the two sides can't both miss (Dekker).
            const last_thief = self.exitThiefAndNotify(rt, false);
            self.markIdle();

            // The recheck has TWO halves,
            // one per way a producer may have SKIPPED its notify expecting
            // us to find the work ourselves:
            //   - saw us not-yet-idle (`notifyIfParked`) → it pushed into
            //     one of OUR rings; `hasReady` (both rings, LOCKED) catches
            //     it now that markIdle has run.
            //   - saw us thieving (`notifyIdlePeer`, e.g. a peer's pile-up)
            //     → the work sits in THAT PEER's ring, un-notified; only
            //     `trySteal` can reach it, and only now that we've left the
            //     thieving state. So this trySteal isn't "thieving again"
            //     — it's the peer-ring half of the lost-wakeup recheck. It
            //     is only ours to do when we were the LAST thief out: with
            //     another thief still hunting, a push suppressed on our
            //     account is that thief's to find (a thief that finds work
            //     hands the hunt on, so the chain never drops it).
            if (self.hasReady() or (last_thief and self.trySteal())) {
                self.unpark(rt);
                continue;
            }

            // 4. About to block. A worker that ran a fiber within the last
            //    millisecond is mid-burst: its connections' next completions
            //    are likely microseconds away, and a park plus the kernel
            //    wake (and a C-state exit on a pinned core) costs more than
            //    that. Poll the completion ring for a short while first.
            const since_work = cyclesToNs(readTicks() -% self.last_work_ticks);
            if (rt.prepark_poll_us.load(.monotonic) > 0 and
                since_work < std.time.ns_per_ms and
                self.pollBeforePark(rt))
            {
                self.wake_stats.poll_hits += 1;
                self.unpark(rt);
                continue;
            }
            // Last look at `stopping` before blocking: any `run(.no_wait)`
            // earlier in this pass (the pump, the poll above) may have
            // consumed stop's wake, and stop brings no work for the
            // rechecks above to find, so a block here would never end.
            if (rt.stopping.load(.acquire)) {
                self.unpark(rt);
                break;
            }
            // Block until a libxev completion (timer / I/O / cross-thread
            // async wake, including stop's) fires, then leave the parked
            // state and re-find work on the next pass.
            self.wake_stats.parks += 1;
            self.loop.run(.once) catch |err| {
                std.debug.panic("xev.Loop.run failed: {}", .{err});
            };
            self.unpark(rt);
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

/// Per-worker counters on the idle-peer wake path. `notify_tries` is
/// how often a local push found the normal ring at 2+ (the trigger in
/// `scheduleLocal`); `gated_thief` and `gated_noidle` are tries that
/// went nowhere because a thief was already out or nobody was parked;
/// `notify_sent` is an eventfd write to a parked peer. `thief_wakes` are
/// wakes this worker got as the claimed thief, `steal_scans` its
/// `trySteal` calls, `steal_hits` the ones that moved anything, `stolen`
/// the fibers moved here, `parks` how often it blocked in the loop.
pub const WakeStats = struct {
    notify_tries: u64 = 0,
    gated_thief: u64 = 0,
    gated_noidle: u64 = 0,
    notify_sent: u64 = 0,
    thief_wakes: u64 = 0,
    steal_scans: u64 = 0,
    steal_hits: u64 = 0,
    stolen: u64 = 0,
    parks: u64 = 0,
    /// Pre-park polls that found work, sparing a park + kernel wake.
    poll_hits: u64 = 0,
};

pub const Stats = struct {
    total: usize,
    new: usize,
    ready: usize,
    waiting: usize,
    finished: usize,
};

pub const InitOptions = struct {
    /// Number of worker threads. 0 (default) means "auto" — use the CPU core
    /// count (`std.Thread.getCpuCount`). A non-zero value is honored verbatim.
    num_workers: usize = 0,

    /// Workers reserved for the `.raft` lane (the FIRST `raft_workers` worker
    /// ids; the remaining higher ids form the `.default` lane).
    ///
    /// `null` (default) means "auto" — `raftWorkerCount` reserves 0 below 8
    /// workers, else ≈ 1/4 of `num_workers` (capped at 4). An explicit value is
    /// honored verbatim, INCLUDING `0`: pass `.raft_workers = 0` to put every
    /// worker in the `.default` lane (no raft reservation). Optional so that a
    /// requested 0 is distinguishable from "unset" — otherwise 0 and auto would
    /// collide. Raft may claim at most half of `num_workers`, so `.default`
    /// (which owns lead worker 0) is never starved; an empty raft lane makes
    /// `.raft` spawns fall back to `.default`.
    raft_workers: ?usize = null,

    /// Threads in the blocking-I/O offload pool (`fsops`) used for fsync and
    /// filesystem metadata syscalls. These run blocking ops off the fiber
    /// workers so fibers keep executing. Clamped to a minimum of 1.
    io_threads: usize = 4,

    /// Work stealing. `true` (default): an idle worker steals normal-priority
    /// work from a busy lane peer. `false`: a fiber runs only on the worker it
    /// was placed on (no migration) — used by the stress harness to A/B the
    /// stealing path. High-priority work is pinned regardless.
    steal: bool = true,

    /// Pin each worker thread to one CPU (Linux only; a no-op elsewhere):
    /// worker i takes the i-th CPU of the process's allowed set; with
    /// more workers than CPUs nobody is pinned. See `Worker.pinToCpu`.
    pin_workers: bool = true,

    /// See `Runtime.steal_lone` / `Runtime.prepark_poll_us`.
    steal_lone: bool = false,
    prepark_poll_us: u64 = 20,
};

/// Resolve how many workers serve the `.raft` lane, as a fraction of the
/// runtime's TOTAL worker count (`num_workers`).
///
/// `requested == null` (the default) means "auto": below 8 workers reserve
/// NONE — a dedicated raft lane isn't worth it on a small runtime, so raft
/// fibers just share the `.default` lane's auto-scheduling. At ≥ 8 workers
/// target a 1/4 ratio capped at 4 (8 → 2, 16 → 4, 32 → 4). An explicit
/// `requested` (including 0) is used as-is. Either way raft may claim at most
/// half of `num_workers`, leaving the majority (and at least one worker) to
/// `.default`.
fn raftWorkerCount(num_workers: usize, requested: ?usize) usize {
    const raft = if (requested) |r|
        r
    else if (num_workers < 8)
        0
    else
        @min(num_workers / 4, 4);

    // Enforced in every build (not a debug-only assert): a raft lane bigger
    // than half the workers would starve `.default`. A misconfigured count is a
    // fatal startup error, so log it (lands in the server log) and then panic
    // with the same message (panic text only reaches stderr).
    if (raft > num_workers / 2) {
        const msg = "fiber.Runtime: raft_workers ({d}) exceeds half of num_workers ({d}); raft must not take more than half the workers";
        log.err(msg, .{ raft, num_workers });
        std.debug.panic(msg, .{ raft, num_workers });
    }
    return raft;
}

/// Whether the libxev backend needs a thread pool for regular-file I/O.
/// io_uring submits `pread`/`pwrite` to the kernel directly; kqueue
/// (macOS) and epoll cannot poll regular files, so libxev runs those ops
/// on pool threads and completes them through the loop — and fails them
/// with EPERM when the loop has no pool.
const needs_io_pool = xev.backend != .io_uring;

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    workers: []Worker,
    /// The file-I/O pool every worker loop shares on backends that need
    /// one (`needs_io_pool`); elsewhere it is built but never handed to
    /// a loop, so it spawns nothing. Threads start lazily on first use.
    /// Pool threads complete into worker loops, so `deinit` stops and
    /// joins the pool before it frees the loops.
    io_pool: xev.ThreadPool,
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

    /// Work stealing on/off (`InitOptions.steal`). Read by `trySteal`.
    steal_enabled: bool = true,
    /// Pin worker threads to CPUs (`InitOptions.pin_workers`); read once
    /// by each worker at the top of `runMain`.
    pin_workers: bool = true,

    /// Tunables a host sets through `apply`, read by every worker on
    /// every pass. `steal_lone`: a thief
    /// may take a victim's only ready fiber (false: it waits for a ring
    /// of 2+, see `trySteal`). `prepark_poll_us`: how long, in
    /// microseconds, a worker that ran a fiber within the last
    /// millisecond polls its completion ring before blocking; 0 = block
    /// at once (see `Worker.pollBeforePark`). Atomics: `apply` stores
    /// them from the governor's fiber while every worker reads.
    steal_lone: std.atomic.Value(bool) = .init(false),
    prepark_poll_us: std.atomic.Value(u64) = .init(20),

    /// Contiguous worker-id range each lane owns. A `len == 0` lane
    /// falls back to `.default` (see `effectiveLaneIdx`).
    lane_ranges: [Lane.count]LaneRange = undefined,
    /// Per-lane idle-worker bitmaps; producers `claim` one to wake a
    /// single parked peer in that lane.
    idle: [Lane.count]IdleWorkers = .{ .{}, .{} },
    /// Per-lane count of workers in the thieving (steal) state — the wake
    /// throttle. Producers skip the idle-peer wake while a thief is out (see
    /// `notifyIdlePeer`); workers cap themselves at half the lane (see
    /// `becomeThief`).
    num_thieves: [Lane.count]ThiefCount = @splat(.{}),

    /// The `--runtime` flags: the tunables and the cpu statements
    /// may consume in all. Applied by `apply`, at startup and then every
    /// governor tick, so a live change takes effect within a tick.
    pub const Cfg = struct {
        steal_lone: bool = false,
        poll_us: u64 = 20,
        cpu_milli: i64 = 0,

        pub const docs = .{
            .self = "Fiber scheduler tunables (see zfiber runtime.zig)" ++
                " and the cpu statements may consume. The tunables'" ++
                " defaults are the measured best on a 32-thread box" ++
                " (pgbench select-only at 1 and 8 clients).",
            .steal_lone = "A thief may take a worker's only ready fiber." ++
                " When false it waits for a ring of 2+, so a fiber about" ++
                " to run on its own worker is not bounced between" ++
                " workers.",
            .poll_us = "Microseconds a worker that had work within the" ++
                " last ms polls its completion ring before blocking. 0 =" ++
                " block at once; above ~100 the spinning starts to cost" ++
                " writes.",
            .cpu_milli = "Millicores the statements of every database" ++
                " may consume between them, 1000 per core, paid in ticks" ++
                " at their yields; the worker threads stay one per core." ++
                " 0 = every worker of the statements' lane.",
        };
    };

    /// Sets the tunables and the meter's cpu capacity from `cfg`.
    pub fn apply(self: *Runtime, cfg: Cfg) void {
        self.steal_lone.store(cfg.steal_lone, .monotonic);
        self.prepark_poll_us.store(cfg.poll_us, .monotonic);
        const workers: i64 = @intCast(self.laneWorkers(.default));
        const milli = if (cfg.cpu_milli > 0) cfg.cpu_milli else workers * 1000;
        if (milli != _meter.capacity.cpu_milli) {
            log.info("cpu capacity {d} milli on {d} workers", .{
                milli, workers,
            });
            _meter.capacity.cpu_milli = milli;
        }
    }

    pub fn init(self: *Runtime, allocator: std.mem.Allocator, opts: InitOptions) !void {
        // num_workers == 0 means "auto" → the CPU core count.
        const n = if (opts.num_workers == 0)
            (std.Thread.getCpuCount() catch 1)
        else
            opts.num_workers;
        std.debug.assert(n >= 1);
        std.debug.assert(n <= std.math.maxInt(u8));
        const raft_workers = raftWorkerCount(n, opts.raft_workers);
        std.debug.assert(raft_workers < n); // default lane (the fallback) is never empty

        // Pay the 1 ms calibration cost up front so the first fiber
        // dispatch doesn't get charged with it. Idempotent across
        // multiple Runtimes in the same process.
        calibrateTsc();

        self.allocator = allocator;
        self.fibers = .empty;
        self.fibers_sl = .{};
        self.stopping = .init(false);
        self.steal_enabled = opts.steal;
        self.pin_workers = opts.pin_workers;
        self.steal_lone = .init(opts.steal_lone);
        self.prepark_poll_us = .init(opts.prepark_poll_us);
        self.idle = .{ .{}, .{} };
        self.num_thieves = @splat(.{});

        // Lane layout: raft takes the first worker ids, default the rest —
        //   raft    = [0, raft_workers)
        //   default = [raft_workers, n)
        const raft_count: u8 = @intCast(raft_workers);
        self.lane_ranges = .{
            .{ .start = raft_count, .len = @intCast(n - raft_workers) }, // .default
            .{ .start = 0, .len = raft_count }, // .raft
        };
        for (&self.idle, self.lane_ranges) |*idle, range| {
            idle.len = @max(range.len, 1);
        }

        // Twice the workers so one worker's outstanding reads overlap on
        // disk instead of queueing behind each other, with a floor for
        // tiny runtimes. Only the backends in `needs_io_pool` ever spawn
        // a thread from it.
        self.io_pool = xev.ThreadPool.init(.{
            .max_threads = @intCast(@max(2 * n, 4)),
        });
        errdefer {
            self.io_pool.shutdown();
            self.io_pool.deinit();
        }
        const io_pool: ?*xev.ThreadPool =
            if (needs_io_pool) &self.io_pool else null;

        self.workers = try allocator.alloc(Worker, n);
        errdefer allocator.free(self.workers);

        var initialized: usize = 0;
        errdefer {
            for (self.workers[0..initialized]) |*w| w.deinit();
        }
        for (self.workers, 0..) |*w, i| {
            try Worker.init(w, @intCast(i), allocator, io_pool);
            w.lane_idx = if (i < raft_count)
                @intFromEnum(Lane.raft)
            else
                @intFromEnum(Lane.default);
            initialized += 1;
        }

        // Seed the process-wide coarse clock so `coarseMs` is valid the
        // instant init returns, before any worker ticks it.
        _coarse.update();

        // Blocking-I/O offload pool (fsync + fs metadata). Started before we
        // publish the singleton; no jobs arrive until fibers run.
        try _fsops.start(allocator, opts.io_threads);
        errdefer _fsops.stop();

        // Publish as the process singleton. There is exactly one runtime
        // alive at a time; `spawn` resolves it through `runtime()`.
        g_rt = self;
    }

    pub fn deinit(self: *Runtime) void {
        // Stop the offload pool first: join every pool thread so none is
        // mid-`wake()` into the runtime state we're about to free.
        _fsops.stop();
        // Same for the file-I/O pool: its threads complete into the
        // worker loops freed just below, so join them first.
        self.io_pool.shutdown();
        self.io_pool.deinit();
        for (self.fibers.items) |f| {
            f.deinit(self.allocator);
        }
        self.fibers.deinit(self.allocator);
        for (self.workers) |*w| w.deinit();
        self.allocator.free(self.workers);
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

    /// Unregister and free a fiber (struct + ctx + coro stack). Single-owner:
    /// a JOINED fiber is freed by its handle's `join()`; a DETACHED fiber by
    /// `Worker.run` when its body returns. The two are mutually exclusive, so
    /// each fiber is freed exactly once. The caller must hold no reference to
    /// `f` afterward. `pub` only for `JoinHandle.reap` (fiber.zig) — not a
    /// general API; nothing else may free fibers.
    pub fn freeFiber(self: *Runtime, f: *Fiber) void {
        self.removeFiber(f);
        f.deinit(self.allocator);
    }

    /// Request shutdown: every worker's run loop exits after its current
    /// pass. Notifies all workers' `wake_async` so parked workers unstick
    /// their `loop.run(.once)` and observe `stopping`. Safe from any
    /// thread — a fiber via `fiber.stop()`, or an external thread.
    pub fn stop(self: *Runtime) void {
        self.stopping.store(true, .release);
        for (self.workers) |*w| w.wake_async.notify() catch {};
    }

    /// Workers serving `lane`, the default lane's when it has none of
    /// its own: the cores a tenant's cpu share is cut from.
    pub fn laneWorkers(self: *Runtime, lane: Lane) usize {
        return self.lane_ranges[self.effectiveLaneIdx(lane)].len;
    }

    /// Effective lane index for `lane`: itself when it owns workers, else
    /// `.default` (the never-empty fallback lane).
    fn effectiveLaneIdx(self: *Runtime, lane: Lane) u8 {
        const idx: u8 = @intFromEnum(lane);
        if (self.lane_ranges[idx].len == 0) return @intFromEnum(Lane.default);
        return idx;
    }

    /// Wake one parked worker in `lane_idx` to come steal work. Throttled:
    /// the `num_thieves == 0` check MUST run before the actual notify — a
    /// thief already out will find the work, or re-check every queue
    /// after decrementing on its way to park, so nothing strands; per-push
    /// callers pay one atomic load in that case. Also a no-op when nobody
    /// is parked (loads only — the all-busy steady state pays no RMW).
    /// Otherwise the 0→1 increment elects exactly ONE producer, which
    /// registers the wakee as a thief FIRST and then CAS-claims one
    /// idle bit — a woken worker can thus never observe its claimed bit
    /// (`Worker.unpark`) without the matching +1 already in place.
    fn notifyIdlePeer(self: *Runtime, w: *Worker) void {
        const lane_idx = w.lane_idx;
        const n = &self.num_thieves[lane_idx].v;
        if (n.load(.seq_cst) > 0) { // a thief is already out
            w.wake_stats.gated_thief += 1;
            return;
        }
        if (!self.idle[lane_idx].anySet()) { // nobody parked to wake
            w.wake_stats.gated_noidle += 1;
            return;
        }

        if (n.fetchAdd(1, .seq_cst) != 0) {
            // Lost the 0→1 election to another producer (or a worker began
            // thieving on its own) — someone is looking; undo, let them.
            _ = n.fetchSub(1, .seq_cst);
            return;
        }

        // num_thieves already incremented before reaching self.idle.claim.
        if (self.idle[lane_idx].claim()) |off| {
            const peer_id: usize = @as(usize, self.lane_ranges[lane_idx].start) + off;
            std.debug.assert(peer_id < self.workers.len);
            // Expensive call: 1us
            self.workers[peer_id].wake_async.notify() catch {};
            w.wake_stats.notify_sent += 1;
        } else {
            // Everyone un-parked on its own between the peek and the
            // claim; the +1 has no owner — undo it.
            _ = n.fetchSub(1, .seq_cst);
        }
    }

    /// Admit a normal-priority fiber to `lane_idx` from off-lane / off-worker
    /// / bootstrap (the cases `routeReady` can't hand to a same-lane current
    /// worker). With no global queue, we push STRAIGHT into a lane worker's
    /// ring — safe cross-thread because the ring is spinlock-guarded — and
    /// poke that worker if it is parked. Idle peers can then steal a share.
    ///
    /// Prefer a parked worker (so the fiber runs promptly); else spread by
    /// hashing the fiber pointer — stateless, so it works from non-worker
    /// inject contexts (bootstrap, pool threads). The idle peek is a racy hint
    /// — correctness rests on the target's LOCKED pre-park recheck
    /// (`hasReady`), which synchronizes with the push here, so the worker can
    /// never park with our fiber unseen in its ring.
    ///
    /// No `num_thieves` involvement: the wake carries its own work, so it's
    /// never a wasted steal (and we don't `claim`, so the woken worker
    /// `selfClear`s its own bit and stays a non-thief).
    fn injectNormal(self: *Runtime, lane_idx: u8, f: *Fiber) void {
        const range = self.lane_ranges[lane_idx];
        std.debug.assert(range.len > 0);
        const off: u8 = self.idle[lane_idx].anyOffset() orelse blk: {
            // Fibonacci mix so clustered pool addresses still scatter.
            const h = (@intFromPtr(f) *% 0x9E3779B97F4A7C15) >> 32;
            break :blk @intCast(h % range.len);
        };
        self.injectTo(@as(usize, range.start) + off, f);
    }

    /// Push a normal-priority fiber straight into worker `idx`'s ring and
    /// wake it if parked — the placement primitive behind `injectNormal`
    /// and behind `routeReady`'s `placed` worker (a spawn that named a
    /// normal-priority `worker_id`).
    fn injectTo(self: *Runtime, idx: usize, f: *Fiber) void {
        const peer = &self.workers[idx];
        peer.ready[@intFromEnum(Priority.normal)].push(f) catch
            @panic("out of memory injecting fiber into a worker ring");
        peer.notifyIfParked();
    }

    /// Start the scheduler. Spawns OS threads for workers 1..N-1,
    /// runs worker 0 on the calling thread, joins the peer threads
    /// after worker 0 returns.
    pub fn run(self: *Runtime) void {
        for (self.workers[1..]) |*w| {
            w.thread = std.Thread.spawn(.{}, Worker.runMain, .{w}) catch |err| {
                std.debug.panic("worker thread spawn failed: {}", .{err});
            };
        }
        self.workers[0].runMain();
        for (self.workers[1..]) |*w| {
            if (w.thread) |t| t.join();
            w.thread = null;
        }
    }

    /// Coarse monotonic ms. One atomic load — safe from any worker
    /// thread, cheap on every call.
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

/// Route a ready fiber to a queue. Shared by `spawn` and `wake`.
///
///   - High priority is pinned: it goes to its home worker's `ready[high]`
///     ring — via `scheduleLocal` when we're already on the home thread,
///     else a direct cross-thread ring push with an async notify.
///   - Normal priority is lane-bound but worker-free. A spawn that named a
///     `worker_id` passes it as `placed` and the fiber goes into that ring
///     (`injectTo`) for this one push only. Otherwise, when we're running
///     on a worker in the fiber's lane, push to that local ring (which
///     grows on demand via `scheduleLocal`); else `injectNormal` pushes
///     straight into a lane worker's ring and pokes it if parked.
fn routeReady(rt: *Runtime, f: *Fiber, placed: ?usize) void {
    switch (f.priority) {
        .high => {
            const home = &rt.workers[f.home_worker_id];
            if (currentWorkerOrNull() == home) {
                home.scheduleLocal(f);
            } else {
                // Push straight into the home's high ring (spinlock-guarded,
                // so the cross-thread push is safe), then wake only if `home`
                // is parked — order matters for the wakeup race (see
                // `notifyIfParked`).
                home.ready[@intFromEnum(Priority.high)].push(f) catch
                    @panic("out of memory pushing high-priority fiber to home ring");
                home.notifyIfParked();
            }
        },
        .normal => {
            if (placed) |idx| return rt.injectTo(idx, f);
            const lane_idx = rt.effectiveLaneIdx(f.lane);
            if (currentWorkerOrNull()) |w| {
                if (w.lane_idx == lane_idx) {
                    w.scheduleLocal(f);
                    return;
                }
            }
            rt.injectNormal(lane_idx, f);
        },
    }
}

/// Lives in `fiber.zig` next to `JoinState`/`onFinished` (the whole join
/// protocol in one file); re-exported here because the handle is what
/// `spawn` returns.
pub const JoinHandle = _fb.JoinHandle;

/// Shared spawn body: init the fiber, register it, and make it runnable.
/// `detached` (comptime) marks the fiber so `Worker.run` frees it on finish (it
/// has no handle) — set BEFORE `routeReady` so a fiber that runs and finishes
/// immediately is still freed. Returns the registered, runnable fiber. When
/// `detached`, the caller must NOT touch the returned fiber: it may already have
/// finished and been freed.
fn spawn_(
    opts: SpawnOptions,
    comptime func: anytype,
    args: std.meta.ArgsTuple(@TypeOf(func)),
    comptime detached: bool,
) !*Fiber {
    const rt = runtime();
    if (opts.priority == .high) std.debug.assert(opts.worker_id != null);

    const f = try Fiber.init(rt.allocator, opts, func, args);
    errdefer f.deinit(rt.allocator);

    // `f.lane` is set from `opts` in `Fiber.init`. Resolve the
    // lane-relative `worker_id` to an absolute worker. High priority:
    // the fiber's permanent home, kept on the fiber for `routeReady`.
    // Normal priority: only the ring it is placed in, so it is not kept —
    // after this one push the fiber is woken and stolen like any other.
    var placed: ?usize = null;
    if (opts.worker_id) |wid| {
        const range = rt.lane_ranges[rt.effectiveLaneIdx(f.lane)];
        const idx = range.start + (wid % range.len);
        switch (opts.priority) {
            .high => f.home_worker_id = idx,
            .normal => placed = idx,
        }
    }

    if (detached) f.detached = true;
    // The child runs at the spawner's I/O class. A joinable helper is
    // joined inside its spawner's frame, so the statement's context
    // outlives it and its records go there; a detached fiber has no
    // such bound and records nothing.
    if (currentFiberOrNull()) |parent| {
        f.ctx.class = parent.ctx.class;
        f.ctx.cache = parent.ctx.cache;
        f.ctx.tenant = parent.ctx.tenant;
        if (!detached) {
            f.ctx.parent = parent.ctx.parent orelse
                if (parent.ctx.arena != null) &parent.ctx else null;
        }
    }

    try rt.appendFiber(f);
    f.state.store(@bitCast(State{ .tag = .ready, .awaken = false }), .release);
    routeReady(rt, f, placed);
    return f;
}

/// Spawn `func(args...)` as a new fiber on the singleton runtime. The args
/// tuple is copied into a heap-allocated context owned by the Fiber (see
/// `Fiber.init`); callers only need to keep alive what the args point to.
/// Returns a `JoinHandle(T)` where `T` is `func`'s return type — you MUST
/// `join()` it (exactly once) to retrieve the value and free the fiber. A
/// never-joined handle leaves its fiber in the registry until `Runtime.deinit`.
///
/// Priority / addressing contract:
///   - High priority is pinned and MUST name a `worker_id`. The id is
///     *lane-relative*: the absolute home worker is
///     `lane.start + worker_id % lane.len` — e.g. worker_id 3 in a
///     2-worker raft lane resolves to lane offset 1.
///   - Normal priority migrates freely within its lane. It MAY name a
///     `worker_id` (same lane-relative addressing) as the worker it is
///     placed on — a listener dealing connections round-robin — and is
///     woken and stolen like any other normal fiber after that.
/// `lane` selects the worker range (default `.default`, the fallback when
/// the requested lane has no workers).
pub fn spawn(
    opts: SpawnOptions,
    comptime func: anytype,
    args: std.meta.ArgsTuple(@TypeOf(func)),
) !JoinHandle(_fb.ReturnOf(func)) {
    const f = try spawn_(opts, func, args, false);
    // Same comptime-generated Body type Fiber.init used to allocate
    // `f.body`. Accessing `&body.state` by name lets Zig compute the
    // offset — no dependence on field order in `Body`. Safe to deref: a
    // non-detached fiber is freed only by `join()`, so it can't have
    // freed itself even if it already ran.
    const body: *_fb.Body(func) = @ptrCast(@alignCast(f.body));
    return .{ .fiber = f, .state = &body.state };
}

/// Like `spawn`, but returns NO handle: the fiber frees itself (via
/// `Worker.run`) when its body returns. For fire-and-forget work whose result
/// you don't need (e.g. per-connection handlers). Any value `func` returns is
/// dropped. Same priority/addressing contract as `spawn`.
pub fn spawnDetached(
    opts: SpawnOptions,
    comptime func: anytype,
    args: std.meta.ArgsTuple(@TypeOf(func)),
) !void {
    _ = try spawn_(opts, func, args, true);
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
    f.parked_since_ms = _coarse.ms();

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

    routeReady(runtime(), f, null);
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
    asm volatile ("" ::: .{ .memory = true });
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

/// Write a per-worker CPU-time distribution: each worker's `cpu_total_ns`
/// (the cumulative fiber-execution time charged to it) in ms, with a `#` bar,
/// followed by total / min / max / avg and a max/avg imbalance ratio (1.0 ==
/// perfectly even, higher == work piled onto one worker). Each row also shows
/// that worker's ready-ring CAPACITIES (`hiCap`/`nmCap`): rings only grow
/// (doubling) and never shrink, so the current capacity is a free high-water
/// gauge — the deepest the ring was ever driven lies in `[cap/2, cap)`. A
/// `nmCap` above the initial size means the normal ring absorbed a burst; the
/// high ring should stay at the initial size (≈1 pinned loop per home worker).
/// Plain text — safe to drop into logs or an admin dump. All counters are
/// read racily; fine for a coarse gauge.
pub fn dumpWorkerCpu(writer: *std.Io.Writer) !void {
    const workers = runtime().workers;
    const hi = @intFromEnum(Priority.high);
    const nm = @intFromEnum(Priority.normal);
    var total_ns: u64 = 0;
    var max_ns: u64 = 0;
    var min_ns: u64 = std.math.maxInt(u64);
    var cap_hi: usize = 0;
    var cap_nm: usize = 0;
    for (workers) |*w| {
        total_ns +%= w.cpu_total_ns;
        max_ns = @max(max_ns, w.cpu_total_ns);
        min_ns = @min(min_ns, w.cpu_total_ns);
        cap_hi = @max(cap_hi, w.ready[hi].capacity());
        cap_nm = @max(cap_nm, w.ready[nm].capacity());
    }
    try writer.print("worker CPU ({d} workers):\n", .{workers.len});
    for (workers) |*w| {
        var bar: [24]u8 = undefined;
        const bar_w: usize = if (max_ns == 0) 0 else @intCast((w.cpu_total_ns *| 24) / max_ns);
        @memset(bar[0..bar_w], '#');
        try writer.print("  W{d:0>2} lane={d} {d:>6} ms  hiCap={d:>4} nmCap={d:>5}  {s}\n", .{
            w.id,                   w.lane_idx,             @divTrunc(w.cpu_total_ns, std.time.ns_per_ms),
            w.ready[hi].capacity(), w.ready[nm].capacity(), bar[0..bar_w],
        });
    }
    const avg_ns = if (workers.len > 0) total_ns / workers.len else 0;
    // max/avg: 1.0 == perfectly balanced, higher == one worker hogged more.
    const imbalance: f64 = if (avg_ns == 0) 0 else @as(f64, @floatFromInt(max_ns)) / @as(f64, @floatFromInt(avg_ns));
    try writer.print("  total={d}ms min={d} max={d} avg={d} (max/avg={d:.2})\n", .{
        @divTrunc(total_ns, std.time.ns_per_ms),
        @divTrunc(if (min_ns == std.math.maxInt(u64)) 0 else min_ns, std.time.ns_per_ms),
        @divTrunc(max_ns, std.time.ns_per_ms),
        @divTrunc(avg_ns, std.time.ns_per_ms),
        imbalance,
    });
    // Largest ring capacity across all workers — since rings never shrink, a
    // cap above the initial size means that priority's ring absorbed a burst
    // (peak depth was in [cap/2, cap)).
    try writer.print("  ring capacity (never shrinks → ≈ peak): hi_max={d} nm_max={d} (init {d})\n", .{
        cap_hi, cap_nm, _queue.local_queue_capacity,
    });
    // The idle-peer wake path, per worker (see WakeStats). Five numbers
    // per worker, each named for what happened; the legend is printed
    // with the table because the dump is read cold, from an admin
    // command or a test run, not next to this code.
    try writer.writeAll(
        "wake path (lifetime counts, per worker):\n" ++
            "  woke     parked peers this worker woke because work piled up in its ring\n" ++
            "  skipped  pile-ups that woke nobody: a thief was already hunting, or nobody was parked\n" ++
            "  stole    fibers this worker took from busier peers\n" ++
            "  parked   times it blocked with nothing to do\n" ++
            "  polled   times the pre-park poll found work, so it did not block\n",
    );
    const col = "{s:>8}";
    try writer.print("        " ++ col ++ col ++ col ++ col ++ col ++ "\n", .{
        "woke", "skipped", "stole", "parked", "polled",
    });
    var total: [5]u64 = .{ 0, 0, 0, 0, 0 };
    for (workers) |*w| {
        const s = w.wake_stats;
        const row: [5]u64 = .{
            s.notify_sent, s.gated_thief + s.gated_noidle, s.stolen, s.parks, s.poll_hits,
        };
        try writer.print("  W{d:0>2}   {d:>8}{d:>8}{d:>8}{d:>8}{d:>8}\n", .{
            w.id, row[0], row[1], row[2], row[3], row[4],
        });
        for (&total, row) |*t, v| t.* += v;
    }
    try writer.print("  total {d:>8}{d:>8}{d:>8}{d:>8}{d:>8}\n", .{
        total[0], total[1], total[2], total[3], total[4],
    });
}

pub fn dumpFibers(rt: *Runtime, writer: *std.Io.Writer) !void {
    rt.fibers_sl.lock();
    defer rt.fibers_sl.unlock();
    const now_ms = _coarse.ms();

    // Only `.waiting` fibers have a stable stack to walk and a `parked_for` to
    // rank by; collect them so we can list the longest-parked first (most
    // likely the root of a hang). Everything else is tallied, not listed: it's
    // either running/runnable (no stable stack), done, or not yet started.
    const Parked = struct { f: *Fiber, for_ms: i64 };
    var parked: std.ArrayList(Parked) = .empty;
    defer parked.deinit(_context.g().gpa);
    var n_running: usize = 0;
    var n_finished: usize = 0;
    var n_new: usize = 0;
    var n_waiting: usize = 0;

    for (rt.fibers.items) |f| {
        switch (f.loadState().tag) {
            .ready => n_running += 1,
            .finished => n_finished += 1,
            .new => n_new += 1,
            .waiting => {
                n_waiting += 1;
                parked.append(_context.g().gpa, .{
                    .f = f,
                    .for_ms = now_ms - f.parked_since_ms,
                }) catch {}; // best-effort: under memory pressure, drop + note below
            },
        }
    }

    // Longest-parked first.
    std.mem.sort(Parked, parked.items, {}, struct {
        fn descByAge(_: void, a: Parked, b: Parked) bool {
            return a.for_ms > b.for_ms;
        }
    }.descByAge);

    try writer.print("dumpFibers summary: {d} running, {d} finished, {d} new, {d} waiting (shown)\n", .{ n_running, n_finished, n_new, parked.items.len });
    if (parked.items.len < n_waiting) {
        try writer.print("({d} more waiting — out of memory to list)\n", .{n_waiting - parked.items.len});
    }
    for (parked.items, 0..) |e, idx| {
        try writer.print("W {d:>5} fiber=0x{x} parked_for={d}ms cpu_total={d}ms\n", .{
            idx, @intFromPtr(e.f), e.for_ms, @divTrunc(e.f.cpu_total_ns, std.time.ns_per_ms),
        });
        try writeStackTrace(writer, e.f); // re-checks `.waiting`; bails if it resumed
    }
}

/// Read the architecture's fastest monotonic counter. On x86_64
/// Queued-but-unsubmitted SQEs in the worker's io_uring SQ — a free userspace
/// read of the ring (no syscall). Returns 0 on backends without a submission
/// ring (the `xev.backend != .io_uring` branch is comptime-dead there, so
/// `loop.ring` is never referenced), where the worker falls back to its pump.
inline fn pendingSubmissions(loop: *xev.Loop) usize {
    if (comptime xev.backend == .io_uring) return loop.ring.sq_ready();
    return 0;
}

/// Completions the kernel has posted to the CQ but we haven't reaped yet —
/// also a free userspace read. 0 on non-io_uring backends, which only learn of
/// readiness from the wait syscall.
inline fn completionsReady(loop: *xev.Loop) usize {
    if (comptime xev.backend == .io_uring) return loop.ring.cq_ready();
    return 0;
}

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

/// The tick counter's rate, ticks per second, from the calibration:
/// one core's worth of cpu, the unit a tenant's cpu is metered in.
/// Zero before `Runtime.init` has calibrated.
pub fn ticksPerSecond() u64 {
    const factor = tscNsPerCycleQ32();
    if (factor == 0) return 0;
    const one: u128 = 1;
    return @intCast((one << 32) * std.time.ns_per_s / factor);
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
    log.info(
        "tsc calibration: median={d:.4} GHz (min={d:.4}, max={d:.4}, samples={d})",
        .{ med_ghz, min_ghz, max_ghz, N },
    );

    _ = tsc_ns_per_cycle_q32.cmpxchgStrong(0, median, .release, .acquire);
}

/// Reconstruct a parked fiber's call stack lazily — ZERO cost on the `park()`
/// hot path (nothing is captured there). A parked (`.waiting`) fiber is
/// SUSPENDED: its coro saved `rip`/`rbp`/`stack_info` at the `coro.yield()`
/// `switchContext`, and that stack won't change until a `wake` resumes it. We
/// collect the saved `rip` (where it yielded), then walk the frame-pointer
/// chain — each frame holds `[saved rbp | return address]` at `[rbp | rbp+8]`
/// (System V x86_64 / AAPCS aarch64 with frame pointers). Every read is bounded
/// by the coro's committed stack region and the chain must climb monotonically,
/// so a racing resume can't fault us (worst case: torn reads, then the walk
/// stops). To avoid PRINTING a torn trace, we re-check after the walk that the
/// fiber is still `.waiting`: its `coro.context` and stack are only mutated
/// while it RUNS, so if it's still parked the walk read a stable snapshot. If
/// it resumed, we omit the trace rather than emit junk.
///
/// We always print the compact raw-address line — resolvable offline with
/// `addr2line`, and the ONLY artifact when debug info or frame pointers are
/// absent (e.g. ReleaseFast omits them, giving a short walk) — then symbolize
/// each frame to `file:line:col in <fn>` via `symbolizeFrames`.
fn writeStackTrace(writer: *std.Io.Writer, f: *const Fiber) !void {
    const max_trace_depth = 32;
    const ctx = &f.coro.context;
    const pc, const start_fp = switch (builtin.cpu.arch) {
        .x86_64 => .{ ctx.rip, ctx.rbp },
        .aarch64 => .{ ctx.pc, ctx.fp },
        else => return, // frame layout not implemented for this arch
    };
    const lo = ctx.stack_info.limit; // committed low bound
    const hi = ctx.stack_info.base; // stack top (high)

    // Walk the frame chain into a fixed buffer. addrs[0] is the resume point;
    // the rest are return addresses climbing the call stack.
    var addrs: [max_trace_depth]usize = undefined;
    addrs[0] = pc;
    var n: usize = 1;
    var fp = start_fp;
    while (n < max_trace_depth) {
        // fp must sit inside the committed stack, be 8-aligned, and leave room
        // to read [fp] (saved fp) and [fp+8] (return address). A garbage or
        // torn fp fails one of these and we stop the walk.
        if (fp < lo or fp +| 16 > hi or fp % 8 != 0) break;
        const slot: [*]const usize = @ptrFromInt(fp);
        const next_fp = slot[0];
        const ret = slot[1];
        if (ret == 0) break;
        addrs[n] = ret;
        n += 1;
        if (next_fp <= fp) break; // chain must climb toward the stack base
        fp = next_fp;
    }

    // Revalidate before trusting `addrs`: if a `wake` resumed the fiber while
    // we walked, its stack was being overwritten, so the chain may be torn.
    // Re-check the state — if it's no longer `.waiting`, the fiber moved; omit
    // the trace rather than emit junk. (The bounds checks above already ruled
    // out a fault; this rules out a misleading trace.) A resume that also
    // re-parks within the walk is missed, but that's vanishingly rare.
    if (f.loadState().tag != .waiting) {
        try writer.writeAll("       (resumed mid-dump; trace omitted)\n");
        return;
    }

    // Always emit the compact raw line (offline addr2line; survives missing
    // debug info / omitted frame pointers).
    try writer.writeAll("       trace");
    for (addrs[0..n]) |a| try writer.print(" 0x{x}", .{a});
    try writer.writeByte('\n');

    // Then symbolize each frame to `file:line:col in <fn>`. Unconditional — a
    // hang dump always wants the trace, and we deliberately do NOT consult
    // `std.options.allow_stack_tracing`: that flag gates the *unwinder* (which
    // raced); symbolization is a line-table lookup, safe when serialized.
    symbolizeFrames(writer, addrs[0..n]);
}

/// Resolve raw return addresses to `file:line:col in <fn>`. This is a DWARF
/// line-table lookup against the binary's OWN embedded debug info (the running
/// executable — present in prod), NOT stack unwinding and NOT reading source
/// files (those don't ship to a prod box). Safe to run regardless of
/// `std.options.allow_stack_tracing`. No external lock needed: `SelfInfo` is
/// internally thread-safe in Zig 0.16.0 (its own `Io.RwLock` serializes module
/// load/lookup — the same fix that made concurrent unwinds safe), and the only
/// caller, `dumpFibers`, already holds `fibers_sl` for its whole pass anyway.
/// Best-effort: stripped / missing debug info degrades to the raw-address line
/// already printed (resolve those offline with `addr2line` against the binary).
fn symbolizeFrames(writer: *std.Io.Writer, addrs: []const usize) void {
    const io = _context.g().io;
    const di = std.debug.getSelfDebugInfo() catch return; // stripped/unsupported
    var arena: std.heap.ArenaAllocator = .init(std.debug.getDebugInfoAllocator());
    defer arena.deinit();
    const a = arena.allocator();

    for (addrs) |ret| {
        // `ret` is the return address (after the call); -1 lands inside the
        // calling instruction so we report its line, not the next one.
        var symbols: std.ArrayList(std.debug.Symbol) = .empty;
        di.getSymbols(io, a, a, ret -| 1, true, &symbols) catch {};
        if (symbols.items.len == 0) {
            writer.print("         0x{x} in ???\n", .{ret}) catch return;
            continue;
        }
        for (symbols.items) |sym| {
            if (sym.source_location) |sl| {
                writer.print("         {s}:{d}:{d} in {s}\n", .{
                    sl.file_name, sl.line, sl.column, sym.name orelse "???",
                }) catch return;
            } else {
                writer.print("         0x{x} in {s}\n", .{ ret, sym.name orelse "???" }) catch return;
            }
        }
    }
}
