// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Stress harness for the fiber timeout / watermark / sleep machinery.
//!
//! Pounds `sleepMs`, timed/untimed `Watermark.waitFor`, CPU-bound compute,
//! io_uring disk reads/writes, and short-lived child fibers (spawn→join→detach,
//! exercising fiber reclaim) from many fibers under configurable concurrency and
//! work-stealing, for a given duration. It is a CORRECTNESS harness, not a
//! benchmark: every op verifies an invariant and the process ABORTS (non-zero)
//! on the first violation or panic. A no-progress watchdog catches hangs and a
//! registry-size watchdog catches a reclaim leak (live fibers growing unbounded).
//! A clean run to the duration prints a summary and exits 0 — i.e. "these
//! primitives held under this configuration."
//!
//! Why it exists: the full test suite reuses worker fibers across tests, which
//! lets one test's timeout/waiter bookkeeping leak and surface as a spurious
//! wake (or worse, a stolen first-fire claim → a hang) in an unrelated test.
//! This harness exercises ONLY the production API (fiber-owned timeouts +
//! watermark waits) in isolation, so a failure here is a real runtime bug, not
//! test cross-talk. Each op asserts its own invariant (a sleep can't return
//! early; a satisfied `waitFor` really reached its target; a `Timeout` result
//! really waited out its deadline). `sleepMs` itself PANICS on any spurious wake
//! (a resume before its own deadline fired), and a no-progress watchdog in the
//! driver turns a hang into an immediate, fiber-naming abort.
//!
//! The runtime always uses ALL cores for its OS worker threads; `--conc` sets
//! the number of concurrent task FIBERS (the load), not the thread count.
//! `--cpu-tasks N` spawns N pure-Fibonacci fibers upfront from one ring — a
//! work-stealing probe; the end-of-run per-worker CPU dump shows the spread.
//!
//!   zig build stress                 # defaults (256 task fibers, steal on, 5s)
//!   ./zig-out/bin/timeout-stress --conc 512 --steal on  --duration-s 30
//!   ./zig-out/bin/timeout-stress --conc 0 --cpu-tasks 64 --duration-s 10
//!   flags: --conc N (task fibers) --cpu-tasks N --steal on|off --duration-s N
//!          --advancers N --watermarks N --seed N

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;

const Config = struct {
    /// Concurrent task fibers — the load knob. The runtime ALWAYS uses all
    /// cores for its OS worker threads (`num_workers = auto`); these fibers are
    /// what gets scheduled across them.
    conc: usize = 256,
    /// CPU-heavy fibers (pure Fibonacci, no I/O) spawned UPFRONT — a work-
    /// stealing probe. They flood one spawner's ready ring with stealable
    /// normal-priority work; the end-of-run per-worker CPU dump shows how evenly
    /// the scheduler spread them. 0 = off.
    cpu_tasks: usize = 0,
    steal: bool = true,
    duration_ms: u64 = 5000,
    advancers: usize = 4,
    watermarks: usize = 8,
    seed: u64 = 0x9E3779B97F4A7C15,
};

/// Scratch file (in the cwd — a real filesystem, unlike a tmpfs /tmp, so the
/// io_uring ops hit an actual block device) for the read/write op. Created once
/// by the driver and truncated to empty on open; each worker owns a disjoint
/// `io_block`-sized region at `idx * io_block`, so concurrent positional pread/
/// pwrite never overlap. The file is NOT pre-sized to all regions — each
/// worker's first pwrite auto-extends it (sparsely) to cover its own region, and
/// because every op writes its region before reading it back, no read ever lands
/// on an unwritten hole.
const io_path = "timeout-stress.io";
const io_block: usize = 512;

const Shared = struct {
    cfg: Config,
    allocator: std.mem.Allocator,
    wms: []fiber.Watermark,
    io_file: fiber.File = undefined, // set by the driver before workers spawn
    stop: std.atomic.Value(bool) = .init(false),
    // op counters (relaxed — diagnostics only)
    n_sleep: std.atomic.Value(u64) = .init(0),
    n_ok: std.atomic.Value(u64) = .init(0),
    n_timeout: std.atomic.Value(u64) = .init(0),
    n_closed: std.atomic.Value(u64) = .init(0),
    n_advance: std.atomic.Value(u64) = .init(0),
    n_cpu: std.atomic.Value(u64) = .init(0),
    n_io: std.atomic.Value(u64) = .init(0),
    n_child: std.atomic.Value(u64) = .init(0),
    n_detached: std.atomic.Value(u64) = .init(0),
    /// CPU-bench mode only: wall-clock ms for all `cpu_tasks` to finish. Written
    /// by the bench fiber, read by `main` after `rt.run()` returns (joined).
    cpu_elapsed_ms: i64 = 0,
};

/// A short-lived child fiber: parks briefly, then returns a value derived from
/// its seed so the parent can verify it actually ran. Spawned and `join`ed by
/// the `child` op below — join reads the result and frees the fiber (the
/// handle is the sole owner). Exercises (and proves bounded) the join-frees path.
fn childTask(seed: u64) u64 {
    fiber.sleepMs(1 + seed % 3);
    return seed *% 0x100000001B3;
}

/// A detached child: does a little work and returns with NO handle, so
/// `Worker.run` frees it on finish. Exercises the `spawnDetached` path the
/// `child` op can't reach (no join, runtime-side free).
fn detachedTask(seed: u64) void {
    fiber.sleepMs(1 + seed % 3);
    std.mem.doNotOptimizeAway(seed *% 0x9E3779B97F4A7C15);
}

/// Burn CPU computing a rolling Fibonacci series until ~`ms` have elapsed,
/// WITHOUT yielding — a CPU-bound fiber that holds its worker. Stresses
/// cooperative scheduling and work-stealing (peers must run other fibers while
/// this one hogs a worker, and this worker's timers fire late but still fire).
/// The clock is read once per chunk of steps, not per step, so the time goes to
/// compute, not to clock syscalls.
fn cpuBurn(ms: u64) void {
    const t0 = monoMs();
    const target: i64 = @intCast(ms);
    var a: u64 = 0;
    var b: u64 = 1;
    while (monoMs() - t0 < target) {
        // Wrapping add so a u64 overflow just wraps — we want CPU work, not an
        // exact value.
        for (0..8192) |_| {
            const c = a +% b;
            a = b;
            b = c;
        }
    }
    // Force the result to be "used" so the optimizer can't delete the loop.
    std.mem.doNotOptimizeAway(b);
}

/// Fixed Fibonacci iterations per CPU-bench task. Sized so one task is ~tens of
/// ms — long enough that steal latency is negligible, short enough that the
/// serial (steal-off) case still finishes promptly.
const cpu_task_iters: u64 = 1000_000_000;

/// One CPU-bench task: a FIXED chunk of pure Fibonacci, no yield/park, then
/// returns. `--cpu-tasks N` spawns N of these from one ring and times how long
/// until ALL finish — a direct work-stealing benchmark (good stealing → all
/// cores busy → fast; bad → they serialize on the spawn worker → slow).
fn cpuTask(iters: u64) void {
    var a: u64 = 0;
    var b: u64 = 1;
    var i: u64 = 0;
    while (i < iters) : (i += 1) {
        const c = a +% b;
        a = b;
        b = c;
    }
    std.mem.doNotOptimizeAway(b);
}

/// Cheap per-fiber xorshift64; no shared state, so it never serializes workers.
fn next(state: *u64) u64 {
    var x = state.*;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    state.* = x;
    return x;
}

fn monoMs() i64 {
    return utils.timestampNow(.monotonic, .millis);
}

/// Random index in `[0, n)` (bridges the u64 RNG and the usize length).
fn pick(st: *u64, n: usize) usize {
    return @intCast(next(st) % @as(u64, @intCast(n)));
}

/// One stress fiber: a random mix of sleeps / timed waits / advances / CPU
/// burns / io_uring disk reads+writes, each with an invariant. Runs until `stop`.
fn worker(sh: *Shared, idx: usize) void {
    var st: u64 = sh.cfg.seed +% (@as(u64, idx) *% 0xA24BAED4963EE407) +% 1;
    while (!sh.stop.load(.acquire)) {
        switch (next(&st) % 8) {
            0 => {
                // sleep again, here checking elapsed time too: must not return early.
                const ms: u64 = 1 + next(&st) % 8;
                const t0 = monoMs();
                fiber.sleepMs(ms);
                const el = monoMs() - t0;
                if (el < @as(i64, @intCast(ms)) - 1) std.debug.panic(
                    "sleepMs({d}) returned after only {d} ms — early/spurious wake",
                    .{ ms, el },
                );
                _ = sh.n_sleep.fetchAdd(1, .monotonic);
            },
            1, 2 => {
                // Timed watermark wait: verify the result against the value.
                const wm = &sh.wms[pick(&st, sh.wms.len)];
                const target = wm.load() + 1 + next(&st) % 50;
                const to: u64 = 1 + next(&st) % 15;
                const t0 = monoMs();
                wm.waitFor(target, to) catch |err| switch (err) {
                    error.Timeout => {
                        const el = monoMs() - t0;
                        if (el < @as(i64, @intCast(to)) - 1) std.debug.panic(
                            "waitFor(timeout {d}) returned Timeout after only {d} ms",
                            .{ to, el },
                        );
                        _ = sh.n_timeout.fetchAdd(1, .monotonic);
                        continue;
                    },
                    error.Closed => {
                        _ = sh.n_closed.fetchAdd(1, .monotonic);
                        continue;
                    },
                    error.OutOfMemory => std.debug.panic("waitFor: OOM", .{}),
                };
                // Returned ok ⇒ the watermark really did reach the target.
                if (wm.load() < target) std.debug.panic(
                    "waitFor returned ok but value {d} < target {d}",
                    .{ wm.load(), target },
                );
                _ = sh.n_ok.fetchAdd(1, .monotonic);
            },
            3 => {
                // CPU burn: hold the worker on a real calculation for a sane,
                // random duration (no park/yield). Verify it actually computed
                // for ~that long — a burn that was optimized away or never ran
                // would return early.
                const ms: u64 = 1 + next(&st) % 20;
                const t0 = monoMs();
                cpuBurn(ms);
                const el = monoMs() - t0;
                if (el < @as(i64, @intCast(ms)) - 1) std.debug.panic(
                    "cpuBurn({d}) returned after only {d} ms — compute optimized away?",
                    .{ ms, el },
                );
                _ = sh.n_cpu.fetchAdd(1, .monotonic);
            },
            4 => {
                // io_uring disk I/O: write a per-fiber pattern to this fiber's
                // OWN region, read it back, and verify it round-trips. Exercises
                // libxev pread/pwrite — the fiber parks on the io_uring
                // completion (the parkAssertDone path), then resumes on the CQE.
                var wbuf: [io_block]u8 = undefined;
                var rbuf: [io_block]u8 = undefined;
                const nonce: u8 = @truncate(next(&st));
                for (&wbuf, 0..) |*x, i| x.* = nonce +% @as(u8, @truncate(i));
                const off = @as(u64, idx) * io_block;
                const nw = fiber.fs.submit(.write, .{
                    .file = sh.io_file,
                    .data = &wbuf,
                    .offset = off,
                }) catch |e|
                    std.debug.panic("io write at {d}: {}", .{ off, e });
                const nr = fiber.fs.submit(.read, .{
                    .file = sh.io_file,
                    .buf = &rbuf,
                    .offset = off,
                }) catch |e|
                    std.debug.panic("io read at {d}: {}", .{ off, e });
                if (nw != wbuf.len or nr != rbuf.len or !std.mem.eql(u8, &wbuf, &rbuf))
                    std.debug.panic("io round-trip mismatch at {d}: wrote {d} read {d}", .{ off, nw, nr });
                _ = sh.n_io.fetchAdd(1, .monotonic);
            },
            5 => {
                // Fiber lifecycle: spawn a short-lived child and join its
                // result (verify it ran). join() reads the result AND frees the
                // child — the handle is the sole owner. The driver's registry
                // watchdog asserts the live-fiber count stays bounded, i.e. the
                // reclaim actually happens and nothing leaks.
                const seed = next(&st);
                var h = fiber.spawn(.{}, childTask, .{seed}) catch |e|
                    std.debug.panic("spawn child: {}", .{e});
                const got = h.join(); // consumes: reads result + frees the child
                if (got != seed *% 0x100000001B3) std.debug.panic(
                    "child result mismatch: got {d} want {d}",
                    .{ got, seed *% 0x100000001B3 },
                );
                _ = sh.n_child.fetchAdd(1, .monotonic);
            },
            6 => {
                // Detached child: no handle, freed by Worker.run on finish.
                // Fire-and-forget — nothing to join or verify; the registry
                // watchdog confirms these don't accumulate.
                fiber.spawnDetached(.{}, detachedTask, .{next(&st)}) catch |e|
                    std.debug.panic("spawnDetached: {}", .{e});
                _ = sh.n_detached.fetchAdd(1, .monotonic);
            },
            else => {
                const wm = &sh.wms[pick(&st, sh.wms.len)];
                wm.advance(wm.load() + 1 + next(&st) % 20);
                _ = sh.n_advance.fetchAdd(1, .monotonic);
            },
        }
    }
}

/// Keep the watermarks moving so timed/untimed waiters get satisfied (not just
/// timed out), exercising the advance-vs-deadline `claim()` arbitration.
fn advancer(sh: *Shared, idx: usize) void {
    var st: u64 = (sh.cfg.seed ^ (@as(u64, idx) *% 0xD1B54A32D192ED03)) | 1;
    while (!sh.stop.load(.acquire)) {
        for (sh.wms) |*wm| wm.advance(wm.load() + 1 + next(&st) % 8);
        fiber.sleepMs(1);
    }
}

/// Sum of all completed-op counters — the harness's liveness signal. A run that
/// is healthy keeps bumping this; a stall means a fiber is wedged.
fn totalOps(sh: *const Shared) u64 {
    return sh.n_sleep.load(.monotonic) +
        sh.n_ok.load(.monotonic) + sh.n_timeout.load(.monotonic) +
        sh.n_closed.load(.monotonic) + sh.n_advance.load(.monotonic) +
        sh.n_cpu.load(.monotonic) + sh.n_io.load(.monotonic) +
        sh.n_child.load(.monotonic) + sh.n_detached.load(.monotonic);
}

/// Dump every fiber's parked location to stderr — called when the watchdog
/// trips so a hang names the wedged fibers (and their park traces) before we
/// abort, instead of leaving an opaque timeout.
fn dumpAllFibers() void {
    var buf: [16384]u8 = undefined;
    var fw = std.Io.File.stderr().writer(utils.context.g().io, &buf);
    fiber.dumpFibers(fiber.runtime(), &fw.interface) catch {};
    fw.interface.flush() catch {};
}

/// Print each worker's cumulative fiber-execution time + the max/avg imbalance
/// ratio — shows how evenly this run spread across workers (1.0 == perfectly
/// balanced, higher == work piled onto a few). Read while the workers are still
/// alive (before `rt.deinit`).
fn printWorkerCpu() void {
    var buf: [4096]u8 = undefined;
    var fw = std.Io.File.stderr().writer(utils.context.g().io, &buf);
    fiber.dumpWorkerCpu(&fw.interface) catch {};
    fw.interface.flush() catch {};
}

/// Periodic progress line for a live-watched soak: elapsed (+MM:SS) as the
/// primary cadence marker, a UTC wall-clock anchor (the box may be on a local
/// TZ, so we don't pretend to print local time), the cumulative counters, and
/// the op rate over the last window. Also doubles as a liveness proof between
/// the start banner and the final summary.
fn heartbeat(sh: *const Shared, elapsed_ms: i64, win_ms: i64, win_ops: u64, live_fibers: usize) void {
    const rt_ms = utils.timestampNow(.realtime, .millis);
    const day: u64 = @intCast(@mod(@divFloor(rt_ms, 1000), 86400));
    const el_s: u64 = @intCast(@divFloor(elapsed_ms, 1000));
    const rate: u64 = if (win_ms > 0) win_ops * 1000 / @as(u64, @intCast(win_ms)) else 0;
    std.debug.print(
        "[+{d:0>2}:{d:0>2} utc {d:0>2}:{d:0>2}:{d:0>2}] sleep={d} ok={d} timeout={d} closed={d} advance={d} cpu={d} io={d} child={d} detached={d} | {d} ops/s | live_fibers={d}\n",
        .{
            el_s / 60,                    el_s % 60,
            day / 3600,                   (day % 3600) / 60,
            day % 60,                     sh.n_sleep.load(.monotonic),
            sh.n_ok.load(.monotonic),     sh.n_timeout.load(.monotonic),
            sh.n_closed.load(.monotonic), sh.n_advance.load(.monotonic),
            sh.n_cpu.load(.monotonic),    sh.n_io.load(.monotonic),
            sh.n_child.load(.monotonic),  sh.n_detached.load(.monotonic),
            rate,                         live_fibers,
        },
    );
}

/// Spawn the fleet, run for the duration, then stop + close to drain waiters.
/// A no-progress watchdog turns a hang (the failure mode we're hunting) into an
/// immediate, attributable abort rather than a silent wedge.
fn driver(sh: *Shared) void {
    defer fiber.stop();

    // One shared scratch file for the io_uring op (created from a fiber — the
    // create parks on the fsops pool). Set before any worker spawns; closed +
    // unlinked after g.wait() drains them all.
    sh.io_file = fiber.fs.submit(.open, .{
        .path = io_path,
        .flags = .{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true },
        .mode = 0o644,
    }) catch |e|
        std.debug.panic("create io scratch {s}: {}", .{ io_path, e });
    defer {
        fiber.fs.submit(.close, sh.io_file);
        fiber.fs.submit(.delete_file, io_path) catch {};
    }

    var g: fiber.Group(void) = .init(sh.allocator);
    defer g.deinit();
    g.ensureTotalCapacity(sh.cfg.conc + sh.cfg.advancers) catch unreachable;

    for (0..sh.cfg.advancers) |i| g.spawn(.{}, advancer, .{ sh, i }) catch unreachable;
    for (0..sh.cfg.conc) |i| g.spawn(.{}, worker, .{ sh, i }) catch unreachable;

    // Watchdog (no-progress → dump + abort) plus a 30 s progress heartbeat.
    const stall_limit_ms: i64 = 3000;
    const heartbeat_ms: i64 = 30_000;
    // Registry bound: steady-state population is the driver + advancers +
    // workers, plus at most one in-flight child per worker (the `child` op
    // joins before spawning the next) and a small finished-but-not-yet-detached
    // tail. 4× that base + 256 leaves ample slack for those transients while
    // still catching a reclaim LEAK — a broken free path piles up finished
    // fibers (thousands/sec) and blows any fixed bound within ~1s.
    const base_fibers = 1 + sh.cfg.advancers + sh.cfg.conc;
    const fiber_bound = base_fibers * 4 + 256;
    const start = monoMs();
    const deadline = start + @as(i64, @intCast(sh.cfg.duration_ms));
    var last_ops: u64 = 0;
    var last_progress = start;
    var next_beat = start + heartbeat_ms;
    var beat_at = start;
    var beat_ops: u64 = 0;
    while (monoMs() < deadline) {
        fiber.sleepMs(100);
        const now = monoMs();
        const ops = totalOps(sh);
        const live = fiber.runtime().stats().total;
        if (live > fiber_bound) {
            dumpAllFibers();
            std.debug.panic(
                "stress: live fiber count {d} exceeded bound {d} — finished fibers are not being reclaimed (leak)",
                .{ live, fiber_bound },
            );
        }
        if (ops != last_ops) {
            last_ops = ops;
            last_progress = now;
        } else if (now - last_progress > stall_limit_ms) {
            dumpAllFibers();
            std.debug.panic("stress: no op completed for >{d}ms — a fiber is wedged (hang)", .{stall_limit_ms});
        }
        if (now >= next_beat) {
            heartbeat(sh, now - start, now - beat_at, ops - beat_ops, live);
            beat_at = now;
            beat_ops = ops;
            next_beat += heartbeat_ms;
        }
    }

    sh.stop.store(true, .release);
    for (sh.wms) |*wm| wm.close(); // unblock anyone parked in a wait
    g.wait();
}

/// CPU-bench mode (`--cpu-tasks N`): spawn N fixed-work `cpuTask` fibers from
/// this one ring, wait for ALL to finish, record the wall-clock time, then exit.
/// Nothing else runs — so both the elapsed time and the per-worker CPU dump
/// measure purely how fast + how evenly the scheduler ran those N CPU tasks.
fn cpuBench(sh: *Shared) void {
    defer fiber.stop();
    var g: fiber.Group(void) = .init(sh.allocator);
    defer g.deinit();
    g.ensureTotalCapacity(sh.cfg.cpu_tasks) catch unreachable;
    const start = monoMs();
    for (0..sh.cfg.cpu_tasks) |_| g.spawn(.{}, cpuTask, .{cpu_task_iters}) catch unreachable;
    g.wait(); // blocks until every fixed-work task has finished
    sh.cpu_elapsed_ms = monoMs() - start;
}

pub fn main(init: std.process.Init) !void {
    utils.context.initGlobal(init);
    const allocator = utils.context.g().gpa;
    const cfg = parseArgs(try utils.context.g().argv());

    const wms = try allocator.alloc(fiber.Watermark, cfg.watermarks);
    defer allocator.free(wms);
    for (wms) |*wm| wm.* = fiber.Watermark.init(allocator);
    defer for (wms) |*wm| wm.deinit();

    var sh: Shared = .{ .cfg = cfg, .allocator = allocator, .wms = wms };

    var rt: fiber.Runtime = undefined;
    // num_workers=0: auto = all cores (the OS worker threads). The load is set
    // by `cfg.conc` task fibers, not the thread count. raft_workers=0: every
    // worker joins the `.default` lane (the stress only spawns there), so no
    // worker sits idle in a reserved raft lane.
    try rt.init(allocator, .{ .num_workers = 0, .steal = cfg.steal, .raft_workers = 0 });
    defer rt.deinit();

    std.debug.print(
        "timeout-stress: threads={d} conc={d} cpu_tasks={d} steal={} duration={d}ms advancers={d} watermarks={d} seed=0x{x}\n",
        .{ rt.workers.len, cfg.conc, cfg.cpu_tasks, cfg.steal, cfg.duration_ms, cfg.advancers, cfg.watermarks, cfg.seed },
    );

    // CPU-bench mode runs ONLY the N CPU tasks and times them; otherwise run the
    // mixed-op stress for the duration. Both end with the per-worker CPU dump.
    if (cfg.cpu_tasks > 0) {
        try fiber.spawnDetached(.{}, cpuBench, .{&sh});
        rt.run(); // returns once cpuBench has joined all tasks and called stop()
        const secs = @as(f64, @floatFromInt(sh.cpu_elapsed_ms)) / 1000.0;
        const tps: f64 = if (secs > 0) @as(f64, @floatFromInt(cfg.cpu_tasks)) / secs else 0;
        std.debug.print(
            "cpu-bench: {d} tasks x {d} iters done in {d} ms ({d:.0} tasks/s)\n",
            .{ cfg.cpu_tasks, cpu_task_iters, sh.cpu_elapsed_ms, tps },
        );
    } else {
        try fiber.spawnDetached(.{}, driver, .{&sh});
        rt.run(); // blocks until driver calls fiber.stop(); a panic aborts before here
        std.debug.print(
            "OK ({d}ms): sleep={d} wait_ok={d} wait_timeout={d} wait_closed={d} advance={d} cpu_burn={d} io={d} child={d} detached={d} live_fibers={d}\n",
            .{
                cfg.duration_ms,              sh.n_sleep.load(.monotonic),
                sh.n_ok.load(.monotonic),     sh.n_timeout.load(.monotonic),
                sh.n_closed.load(.monotonic), sh.n_advance.load(.monotonic),
                sh.n_cpu.load(.monotonic),    sh.n_io.load(.monotonic),
                sh.n_child.load(.monotonic),  sh.n_detached.load(.monotonic),
                rt.stats().total,
            },
        );
    }

    // Per-worker CPU distribution + imbalance — workers are still alive here
    // (rt.deinit is deferred), so their cpu_total_ns is valid to read.
    printWorkerCpu();
}

fn parseArgs(argv: []const String) Config {
    var c: Config = .{};
    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const a = argv[i];
        const val: ?String = if (i + 1 < argv.len) argv[i + 1] else null;
        if (std.mem.eql(u8, a, "--conc")) {
            c.conc = @intCast(parseN(val)); // concurrent task fibers
            i += 1;
        } else if (std.mem.eql(u8, a, "--steal")) {
            const v = val orelse fatal("--steal needs on|off", .{});
            c.steal = std.mem.eql(u8, v, "on") or std.mem.eql(u8, v, "true") or std.mem.eql(u8, v, "1");
            i += 1;
        } else if (std.mem.eql(u8, a, "--duration-s")) {
            c.duration_ms = parseN(val) * 1000;
            i += 1;
        } else if (std.mem.eql(u8, a, "--cpu-tasks")) {
            c.cpu_tasks = @intCast(parseN(val)); // upfront CPU-heavy probe fibers
            i += 1;
        } else if (std.mem.eql(u8, a, "--advancers")) {
            c.advancers = @intCast(parseN(val));
            i += 1;
        } else if (std.mem.eql(u8, a, "--watermarks")) {
            c.watermarks = @intCast(parseN(val));
            i += 1;
        } else if (std.mem.eql(u8, a, "--seed")) {
            c.seed = parseN(val);
            i += 1;
        } else if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            std.debug.print("flags: --conc N (task fibers) --cpu-tasks N --steal on|off --duration-s N --advancers N --watermarks N --seed N\n", .{});
            std.process.exit(0);
        } else {
            fatal("unknown arg: {s}", .{a});
        }
    }
    if (c.watermarks == 0) c.watermarks = 1;
    return c;
}

fn parseN(val: ?String) u64 {
    const v = val orelse fatal("flag needs a value", .{});
    return std.fmt.parseInt(u64, v, 10) catch fatal("bad number: {s}", .{v});
}

fn fatal(comptime fmt: String, args: anytype) noreturn {
    std.debug.print("timeout-stress: " ++ fmt ++ "\n", args);
    std.process.exit(2);
}
