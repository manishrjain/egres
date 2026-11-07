// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const zio = @import("zio");
const fiber = @import("zfiber");
const utils = @import("utils");
const linux = std.os.linux;

const BLOCK_SIZE: usize = 4096;

// ── Blocking syscall shims ───────────────────────────────────────────
//
// Zig 0.16's `std.posix` dropped the positional + close/fsync/unlink
// helpers (only `openat` survives), and the random-I/O bench fundamentally
// needs positional `pread`. Route the blocking primitives straight through
// `std.os.linux`. The read path swallows errors (we only time the call);
// the one-shot setup path propagates them.

fn blockingPread(fd: std.posix.fd_t, buf: []u8, offset: u64) usize {
    const rc = linux.pread(fd, buf.ptr, buf.len, @intCast(offset));
    return if (std.posix.errno(rc) == .SUCCESS) rc else 0;
}

fn blockingPwrite(fd: std.posix.fd_t, buf: []const u8, offset: u64) !usize {
    const rc = linux.pwrite(fd, buf.ptr, buf.len, @intCast(offset));
    return switch (std.posix.errno(rc)) {
        .SUCCESS => rc,
        else => |e| std.posix.unexpectedErrno(e),
    };
}

fn blockingFsync(fd: std.posix.fd_t) !void {
    return switch (std.posix.errno(linux.fsync(fd))) {
        .SUCCESS => {},
        else => |e| std.posix.unexpectedErrno(e),
    };
}

fn blockingClose(fd: std.posix.fd_t) void {
    _ = linux.close(fd);
}

fn blockingUnlink(path: [*:0]const u8) void {
    _ = linux.unlink(path);
}

/// Monotonic nanoseconds. 0.16 removed `std.time.nanoTimestamp`; this is
/// the direct CLOCK_MONOTONIC read (same pattern as `utils.timestampNow`)
/// — and monotonic is the right base for measuring op latencies anyway.
fn nowNs() i64 {
    var tp: std.posix.timespec = undefined;
    _ = std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &tp);
    return @as(i64, tp.sec) * std.time.ns_per_s + tp.nsec;
}

// ── Stats ────────────────────────────────────────────────────────────

const Stats = struct {
    total_ops: u64,
    total_ns: u64,
    latencies: ?[]u64,

    fn opsPerSec(self: Stats) f64 {
        return @as(f64, @floatFromInt(self.total_ops)) /
            (@as(f64, @floatFromInt(self.total_ns)) / 1e9);
    }

    fn mbPerSec(self: Stats) f64 {
        return self.opsPerSec() * @as(f64, @floatFromInt(BLOCK_SIZE)) / (1024.0 * 1024.0);
    }

    fn percentile(self: Stats, p: f64) u64 {
        const lats = self.latencies orelse return 0;
        if (lats.len == 0) return 0;
        const idx: usize = @intFromFloat(@as(f64, @floatFromInt(lats.len)) * p);
        return lats[@min(idx, lats.len - 1)];
    }

    fn p50us(self: Stats) f64 {
        return @as(f64, @floatFromInt(self.percentile(0.50))) / 1000.0;
    }

    fn p99us(self: Stats) f64 {
        return @as(f64, @floatFromInt(self.percentile(0.99))) / 1000.0;
    }
};

// ── I/O work ─────────────────────────────────────────────────────────
//
// Three structurally-identical readers, one per runtime. Each times every
// individual pread so we can build a latency distribution; the only
// difference is which `read` it calls — a blocking syscall (posix), a
// zio-fiber park (zio), or an in-tree-fiber park (fiber). Same fd, same
// offsets, same buffer alignment → the spread is pure runtime overhead.

fn ioWorkPosix(fd: std.posix.fd_t, offsets: []const u64, latencies: []u64) void {
    var buf: [BLOCK_SIZE]u8 align(4096) = undefined;
    for (offsets, 0..) |offset, i| {
        const t0 = nowNs();
        _ = blockingPread(fd, &buf, offset);
        const t1 = nowNs();
        latencies[i] = @intCast(t1 - t0);
    }
}

fn ioWorkZio(fd_int: std.posix.fd_t, offsets: []const u64, latencies: []u64) void {
    var buf: [BLOCK_SIZE]u8 align(4096) = undefined;
    const file = zio.File.fromFd(fd_int);
    for (offsets, 0..) |offset, i| {
        const t0 = nowNs();
        _ = file.read(&buf, offset) catch {};
        const t1 = nowNs();
        latencies[i] = @intCast(t1 - t0);
    }
}

fn ioWorkFiber(fd_int: std.posix.fd_t, offsets: []const u64, latencies: []u64) void {
    var buf: [BLOCK_SIZE]u8 align(4096) = undefined;
    for (offsets, 0..) |offset, i| {
        const t0 = nowNs();
        _ = fiber.fs.submit(.read, .{
            .file = .{ .fd = fd_int },
            .buf = &buf,
            .offset = offset,
        }) catch {};
        const t1 = nowNs();
        latencies[i] = @intCast(t1 - t0);
    }
}

// ── CPU work ─────────────────────────────────────────────────────────

fn cpuWork(count: usize) void {
    var buf: [BLOCK_SIZE]u8 = undefined;
    @memset(&buf, 0xAB);
    var sink: u32 = 0;
    for (0..count) |i| {
        buf[0] = @truncate(i);
        sink ^= std.hash.Crc32.hash(&buf);
    }
    std.mem.doNotOptimizeAway(sink);
}

/// Group-spawnable wrapper around `cpuWork`. Shared by the zio and fiber
/// CPU benches — both Groups want a `fn(usize) void` entry point.
fn cpuWorker(count: usize) void {
    cpuWork(count);
}

// ── Benchmarks ───────────────────────────────────────────────────────

fn benchThreadCpu(num_workers: usize, ops_per_worker: usize) Stats {
    var threads: [1024]std.Thread = undefined;
    const t0 = nowNs();
    for (0..num_workers) |i| {
        threads[i] = std.Thread.spawn(.{}, cpuWork, .{ops_per_worker}) catch @panic("spawn");
    }
    for (0..num_workers) |i| threads[i].join();
    const t1 = nowNs();
    return .{ .total_ops = @intCast(num_workers * ops_per_worker), .total_ns = @intCast(t1 - t0), .latencies = null };
}

fn benchZioCpu(num_workers: usize, ops_per_worker: usize) !Stats {
    var group = zio.Group{};
    defer group.cancel();
    const t0 = nowNs();
    for (0..num_workers) |_| try group.spawn(cpuWorker, .{ops_per_worker});
    try group.wait();
    const t1 = nowNs();
    return .{ .total_ops = @intCast(num_workers * ops_per_worker), .total_ns = @intCast(t1 - t0), .latencies = null };
}

fn benchThreadIo(
    allocator: std.mem.Allocator,
    num_workers: usize,
    total_ops: usize,
    fd: std.posix.fd_t,
    offsets: []const u64,
) Stats {
    const ops_per = total_ops / num_workers;
    const latencies = allocator.alloc(u64, num_workers * ops_per) catch @panic("OOM");
    var threads: [1024]std.Thread = undefined;

    const t0 = nowNs();
    for (0..num_workers) |i| {
        threads[i] = std.Thread.spawn(.{}, ioWorkPosix, .{
            fd,
            offsets[i * ops_per .. (i + 1) * ops_per],
            latencies[i * ops_per .. (i + 1) * ops_per],
        }) catch @panic("spawn");
    }
    for (0..num_workers) |i| threads[i].join();
    const t1 = nowNs();

    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));
    return .{ .total_ops = @intCast(num_workers * ops_per), .total_ns = @intCast(t1 - t0), .latencies = latencies };
}

fn benchZioIo(
    allocator: std.mem.Allocator,
    num_fibers: usize,
    total_ops: usize,
    fd: std.posix.fd_t,
    offsets: []const u64,
) !Stats {
    const ops_per = total_ops / num_fibers;
    const latencies = try allocator.alloc(u64, num_fibers * ops_per);

    var group = zio.Group{};
    defer group.cancel();

    const t0 = nowNs();
    for (0..num_fibers) |i| {
        try group.spawn(ioWorkZio, .{
            fd,
            offsets[i * ops_per .. (i + 1) * ops_per],
            latencies[i * ops_per .. (i + 1) * ops_per],
        });
    }
    try group.wait();
    const t1 = nowNs();

    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));
    return .{ .total_ops = @intCast(num_fibers * ops_per), .total_ns = @intCast(t1 - t0), .latencies = latencies };
}

// ── Fiber (in-tree runtime) benchmarks ───────────────────────────────
//
// Unlike zio's `Group` (callable from any context — `wait()` blocks the
// calling OS thread), the in-tree fiber runtime's `Runtime.run()` takes
// over the calling thread as worker 0 and only returns once a fiber calls
// `fiber.stop()`. So `fiber.spawn`/`fiber.Group` must run *inside* a fiber.
// These two `bench*` helpers therefore run from a bootstrap fiber (see
// `cpuBootstrap`/`ioBootstrap`), writing their result into a caller-owned
// slot so `main` can read it after `run()` returns.

fn benchFiberCpu(allocator: std.mem.Allocator, num_workers: usize, ops_per_worker: usize, pinned: bool) !Stats {
    var group: fiber.Group(void) = .init(allocator);
    defer group.deinit();
    try group.ensureTotalCapacity(num_workers);

    const t0 = nowNs();
    // `pinned`: each fiber gets a distinct worker (high priority + worker_id)
    // so the burst fans out one-per-core. Otherwise normal-priority spawns
    // (`.{}`) all land on the spawner's own local ring (cap 16, no
    // work-stealing) and serialize there.
    for (0..num_workers) |wid| {
        const opts: fiber.SpawnOptions = if (pinned)
            .{ .worker_id = @intCast(wid), .priority = .high }
        else
            .{};
        try group.spawn(opts, cpuWorker, .{ops_per_worker});
    }
    group.wait();
    const t1 = nowNs();

    // Per-fiber and per-worker CPU accounting (TSC-measured running time the
    // scheduler clocked, summed over dispatches). Every fiber runs the same
    // 100k-CRC loop, so the f-totals are ~equal in BOTH modes — proving the
    // work is balanced. The w-totals reveal placement: pinned → ~even across
    // workers; unpinned → the spawner's worker hoards ~16 (cap of its ring)
    // while most sit at 0. Read after wait() (fibers done, workers quiesced),
    // so values are stable; handles outlive wait() (freed at Runtime.deinit).
    std.debug.print("  per-fiber cpu_total (ms):\n", .{});
    for (group.handles.items, 0..) |h, i| {
        std.debug.print("    f{d:<2} {d:>8.2}\n", .{
            i,
            @as(f64, @floatFromInt(h.fiber.cpu_total_ns)) / 1e6,
        });
    }
    std.debug.print("  per-worker cpu_total (ms):\n", .{});
    for (fiber.runtime().workers) |*w| {
        std.debug.print("    w{d:<2} {d:>8.2}\n", .{
            w.id,
            @as(f64, @floatFromInt(w.cpu_total_ns)) / 1e6,
        });
    }

    return .{ .total_ops = @intCast(num_workers * ops_per_worker), .total_ns = @intCast(t1 - t0), .latencies = null };
}

fn benchFiberIo(
    allocator: std.mem.Allocator,
    num_fibers: usize,
    total_ops: usize,
    fd: std.posix.fd_t,
    offsets: []const u64,
) !Stats {
    const ops_per = total_ops / num_fibers;
    const latencies = try allocator.alloc(u64, num_fibers * ops_per);

    var group: fiber.Group(void) = .init(allocator);
    defer group.deinit();
    try group.ensureTotalCapacity(num_fibers);

    const t0 = nowNs();
    for (0..num_fibers) |i| {
        try group.spawn(.{}, ioWorkFiber, .{
            fd,
            offsets[i * ops_per .. (i + 1) * ops_per],
            latencies[i * ops_per .. (i + 1) * ops_per],
        });
    }
    group.wait();
    const t1 = nowNs();

    std.mem.sort(u64, latencies, {}, std.sort.asc(u64));
    return .{ .total_ops = @intCast(num_fibers * ops_per), .total_ns = @intCast(t1 - t0), .latencies = latencies };
}

// ── Fiber bootstrap entry points ─────────────────────────────────────
//
// One bootstrap fiber per fiber-runtime lifecycle. It runs the sub-bench,
// stashes the result in the caller's context, then `fiber.stop()`s so the
// `Runtime.run()` on `main`'s thread unwinds. A failure still calls
// `stop()` — otherwise the workers would park forever and `run()` would
// never return.

const CpuBenchCtx = struct {
    allocator: std.mem.Allocator,
    num_workers: usize,
    ops: usize,
    pinned: bool,
    out: Stats = undefined,
    err: ?anyerror = null,
};

fn cpuBootstrap(ctx: *CpuBenchCtx) void {
    defer fiber.stop();
    ctx.out = benchFiberCpu(ctx.allocator, ctx.num_workers, ctx.ops, ctx.pinned) catch |e| {
        ctx.err = e;
        return;
    };
}

/// Run one fiber CPU benchmark under its OWN runtime (so `Worker.cpu_total_ns`
/// starts at zero and reflects just this run), then print its throughput. The
/// per-fiber/per-worker dumps print from inside `benchFiberCpu` during `run()`.
/// `pinned` selects high-priority `worker_id` pinning vs plain normal-priority
/// spawn — the whole point being to compare the worker-total distribution.
fn fiberCpuPhase(allocator: std.mem.Allocator, num_workers: usize, ops: usize, pinned: bool) !void {
    std.debug.print("\n  fiber.Group [{s}]:\n", .{
        if (pinned) "pinned — high priority + worker_id" else "unpinned — normal priority",
    });
    var frt: fiber.Runtime = undefined;
    // raft_workers=0 → all cores in the default lane (no reserved-lane idleness).
    try frt.init(allocator, .{ .raft_workers = 0 });
    defer frt.deinit();
    var ctx = CpuBenchCtx{ .allocator = allocator, .num_workers = num_workers, .ops = ops, .pinned = pinned };
    _ = try fiber.spawn(.{}, cpuBootstrap, .{&ctx});
    frt.run();
    if (ctx.err) |e| {
        std.debug.print("    throughput: (failed: {s})\n", .{@errorName(e)});
    } else {
        std.debug.print("    throughput: {d:.0} ops/sec\n", .{ctx.out.opsPerSec()});
    }
}

const IoBenchCtx = struct {
    allocator: std.mem.Allocator,
    fd: std.posix.fd_t,
    offsets: []const u64,
    total_ops: usize,
    levels: []const usize,
    out: []Stats, // one slot per concurrency level
};

fn ioBootstrap(ctx: *IoBenchCtx) void {
    defer fiber.stop();
    for (ctx.levels, 0..) |conc, i| {
        // On failure, fall back to a zero-throughput sentinel; the print
        // loop renders it as a near-zero row rather than crashing.
        ctx.out[i] = benchFiberIo(ctx.allocator, conc, ctx.total_ops, ctx.fd, ctx.offsets) catch
            Stats{ .total_ops = 0, .total_ns = 1, .latencies = null };
    }
}

// ── File setup ───────────────────────────────────────────────────────

fn createBenchFile(path: []const u8, size: usize) !void {
    // Plain blocking POSIX for setup — std.fs's create/write helpers moved
    // under std.Io in 0.16, and a one-shot benchmark file doesn't need the
    // async path. O_DIRECT is applied later by `openDirect` on the read fd.
    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .WRONLY,
        .CREAT = true,
        .TRUNC = true,
    }, 0o644);
    defer blockingClose(fd);

    var buf: [BLOCK_SIZE]u8 = undefined;
    @memset(&buf, 0xDE);
    var written: usize = 0;
    while (written < size) {
        const want = @min(BLOCK_SIZE, size - written);
        // Positional write at the running offset (pwrite mirrors the pread
        // path the read benches use). May be short; advance by the count it
        // actually wrote — the payload is uniform, so the split doesn't matter.
        written += try blockingPwrite(fd, buf[0..want], written);
    }
    try blockingFsync(fd);
}

fn openDirect(path: []const u8) !std.posix.fd_t {
    var flags: std.posix.O = .{ .ACCMODE = .RDONLY };
    flags.DIRECT = true;
    return std.posix.openat(std.posix.AT.FDCWD, path, flags, 0) catch |err| {
        if (err == error.BadPathName) {
            std.debug.print("WARNING: O_DIRECT not supported, falling back to buffered I/O\n", .{});
            return std.posix.openat(std.posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY }, 0);
        }
        return err;
    };
}

fn generateOffsets(allocator: std.mem.Allocator, count: usize, file_size: usize) ![]u64 {
    const offsets = try allocator.alloc(u64, count);
    var prng = std.Random.DefaultPrng.init(42);
    const rand = prng.random();
    const max_block = file_size / BLOCK_SIZE;
    for (offsets) |*o| {
        o.* = rand.intRangeLessThan(u64, 0, max_block) * BLOCK_SIZE;
    }
    return offsets;
}

// ── Main ─────────────────────────────────────────────────────────────

const usage =
    \\Usage: bench-zio [cpu] [--help]
    \\
    \\Compares blocking std.Thread, zio fibers, and the in-tree fiber runtime
    \\on a CPU workload (CRC32) and a random-read O_DIRECT I/O sweep.
    \\
    \\Arguments:
    \\  cpu, --cpu    Run only the CPU/scheduler benchmark. Skips the 1 GB
    \\                backing-file creation and the disk I/O sweep.
    \\  -h, --help    Show this help and exit.
    \\
    \\With no arguments, runs both the CPU benchmark and the I/O sweep
    \\(creates /data/zio_bench.dat, ~1 GB, removed on exit).
    \\
;

pub fn main(init: std.process.Init) !void {
    // The fiber runtime's coarse clock reads time via `context.g().io`, so the
    // process context must be initialized before any `fiber.Runtime.init`.
    utils.context.initGlobal(init);

    const allocator = std.heap.smp_allocator;
    const num_cores = std.Thread.getCpuCount() catch 4;

    // `cpu` (or `--cpu`) runs the CPU/scheduler benchmark only — no 1 GB
    // file creation, no disk sweep. Lets you iterate on the CPU number
    // without paying ~10s of file setup every run. `--help`/`-h` prints
    // usage and exits; an unrecognized argument does the same (non-zero
    // exit) so typos don't silently run the wrong thing.
    var cpu_only = false;
    {
        var it = init.minimal.args.iterate();
        _ = it.skip(); // exe name
        while (it.next()) |arg| {
            if (std.mem.eql(u8, arg, "cpu") or std.mem.eql(u8, arg, "--cpu")) {
                cpu_only = true;
            } else if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
                std.debug.print("{s}", .{usage});
                return;
            } else {
                std.debug.print("unknown argument: '{s}'\n\n{s}", .{ arg, usage });
                return error.InvalidArgument;
            }
        }
    }

    const file_size: usize = 1024 * 1024 * 1024; // 1 GB — larger to avoid NVMe cache effects
    const tmp_path = "/data/zio_bench.dat";

    std.debug.print("\nzio benchmark — {d} cores, block={d}B{s}\n", .{
        num_cores,                                                             BLOCK_SIZE,
        if (cpu_only) ", CPU-only (disk benchmark skipped)" else ", O_DIRECT",
    });

    // ── CPU benchmark ──
    //
    // All three at full core count: spawn `num_cores` workers, each doing the
    // same CRC loop. Measures raw scheduling/dispatch overhead — there's no
    // I/O to hide behind, so any gap is the runtime's cost of getting a
    // CPU-bound closure onto a core and back.
    {
        const ops: usize = 100_000;
        std.debug.print("\n=== CPU: CRC32 {d}B, {d} workers x {d} ops ===\n", .{ BLOCK_SIZE, num_cores, ops });

        const thread = benchThreadCpu(num_cores, ops);
        std.debug.print("  std.Thread     {d:>10.0} ops/sec\n", .{thread.opsPerSec()});

        {
            var zrt = try zio.Runtime.init(allocator, .{ .executors = .auto });
            const z = try benchZioCpu(num_cores, ops);
            zrt.deinit();
            std.debug.print("  zio.Group      {d:>10.0} ops/sec\n", .{z.opsPerSec()});
        }

        // Run the fiber CPU bench twice — pinned vs unpinned — each in its own
        // runtime, so the per-worker cpu_total dumps show the placement
        // discrepancy: pinned spreads ~evenly, unpinned hoards on the spawner.
        try fiberCpuPhase(allocator, num_cores, ops, true);
        try fiberCpuPhase(allocator, num_cores, ops, false);
    }

    // CPU-only mode stops here — nothing below touches a disk file.
    if (cpu_only) {
        std.debug.print("\n", .{});
        return;
    }

    // ── I/O concurrency sweep ──
    //
    // For threads: spawning more threads than cores just adds context-switch
    // overhead — each thread blocks in the pread syscall. We cap at 4.
    //
    // For fibers (zio and in-tree): a suspended fiber costs ~zero. We can have
    // hundreds of concurrent io_uring submissions with only a handful of OS
    // threads. This is where io_uring wins — and where the two fiber runtimes
    // can be compared head-to-head at identical worker counts.

    // The 1 GB backing file + read fd are only needed for the disk sweep, so
    // they're created here (after CPU) and torn down at function exit.
    std.debug.print("Creating benchmark file...\n", .{});
    try createBenchFile(tmp_path, file_size);
    defer blockingUnlink(tmp_path);
    const fd = try openDirect(tmp_path);
    defer blockingClose(fd);

    const total_io_ops: usize = 320_000;
    const max_offsets: usize = total_io_ops; // enough for any concurrency level
    const offsets = try generateOffsets(allocator, max_offsets, file_size);
    defer allocator.free(offsets);

    const concurrency_levels = [_]usize{ 1, 2, 4, 8, 16, 32, 64, 128, 256, 512 };
    const n_levels = concurrency_levels.len;

    // Collect each runtime's results across the whole sweep BEFORE printing,
    // so the table can show thread / zio / fiber side-by-side per row. Each
    // runtime lives in its own phase: threads run inline, zio under one
    // zio.Runtime, fiber under one fiber.Runtime driven by a bootstrap fiber.
    var thr_results: [n_levels]Stats = undefined;
    var zio_results: [n_levels]Stats = undefined;
    var fib_results: [n_levels]Stats = undefined;

    // Threads: cap concurrency at 4 — simulates a real app with a small
    // thread pool. This is the constraint io_uring lifts.
    for (concurrency_levels, 0..) |conc, i| {
        thr_results[i] = benchThreadIo(allocator, @min(conc, 4), total_io_ops, fd, offsets);
    }

    // zio fibers: full concurrency on a 4-executor runtime.
    {
        var zrt = try zio.Runtime.init(allocator, .{ .executors = .exact(4) });
        for (concurrency_levels, 0..) |conc, i| {
            zio_results[i] = benchZioIo(allocator, conc, total_io_ops, fd, offsets) catch
                .{ .total_ops = 0, .total_ns = 1, .latencies = null };
        }
        zrt.deinit();
    }

    // In-tree fibers: full concurrency on a 4-worker runtime (matching zio's
    // `.exact(4)` so the head-to-head is fair). All levels run inside one
    // bootstrap fiber under a single `run()`.
    {
        var frt: fiber.Runtime = undefined;
        try frt.init(allocator, .{ .num_workers = 4 });
        var ctx = IoBenchCtx{
            .allocator = allocator,
            .fd = fd,
            .offsets = offsets,
            .total_ops = total_io_ops,
            .levels = &concurrency_levels,
            .out = &fib_results,
        };
        _ = try fiber.spawn(.{}, ioBootstrap, .{&ctx});
        frt.run();
        frt.deinit();
    }

    // ── Print the combined table ──
    std.debug.print("\n=== I/O: random pread {d}B, O_DIRECT, {d} total ops (4 workers) ===\n", .{ BLOCK_SIZE, total_io_ops });
    std.debug.print("  {s:>6} | {s:>11} {s:>8} {s:>8} | {s:>11} {s:>8} {s:>8} | {s:>11} {s:>8} {s:>8}\n", .{
        "conc",
        "thr ops/s",
        "p50us",
        "p99us",
        "zio ops/s",
        "p50us",
        "p99us",
        "fib ops/s",
        "p50us",
        "p99us",
    });
    for (concurrency_levels, 0..) |conc, i| {
        const thr = thr_results[i];
        const z = zio_results[i];
        const fib = fib_results[i];
        std.debug.print("  {d:>6} | {d:>11.0} {d:>8.0} {d:>8.0} | {d:>11.0} {d:>8.0} {d:>8.0} | {d:>11.0} {d:>8.0} {d:>8.0}\n", .{
            conc,
            thr.opsPerSec(),
            thr.p50us(),
            thr.p99us(),
            z.opsPerSec(),
            z.p50us(),
            z.p99us(),
            fib.opsPerSec(),
            fib.p50us(),
            fib.p99us(),
        });
    }

    // Free the per-op latency buffers each I/O Stats owns (CPU Stats carry
    // none). Failed levels left `latencies = null` — `if` guards those.
    for (0..n_levels) |i| {
        if (thr_results[i].latencies) |l| allocator.free(l);
        if (zio_results[i].latencies) |l| allocator.free(l);
        if (fib_results[i].latencies) |l| allocator.free(l);
    }
    std.debug.print("\n", .{});
}
