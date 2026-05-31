const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
const rt = @import("runtime");
const fiber = @import("fiber");
const context = utils.context;

// 0.16 helpers: std.Thread.Mutex / ResetEvent / time.nanoTimestamp are gone.
// These wrap std.Io.Mutex / Event / Timestamp using the process-wide `io`.
// Critical sections in this file are short / CPU-only, so the wrappers are
// safe to call from both zio fibers and the watchdog std.Thread.
inline fn ioOf() std.Io {
    return context.g().io;
}
inline fn nowNs() i128 {
    return std.Io.Timestamp.now(ioOf(), .real).nanoseconds;
}
fn mlock(m: *std.Io.Mutex) void {
    m.lockUncancelable(ioOf());
}
fn munlock(m: *std.Io.Mutex) void {
    m.unlock(ioOf());
}
fn timeoutNs(ns: u64) std.Io.Timeout {
    return .{ .duration = .{ .raw = std.Io.Duration.fromNanoseconds(@intCast(ns)), .clock = .awake } };
}

// Tty / Color were reorganized in 0.16 (→ std.Io.Terminal,
// detect() now takes io + env-var bools). Tests don't need color output to
// pass; stub the whole thing as no-op so the runner compiles. If we want
// color back later, swap the stub for a real Terminal-backed type.
const Tty = struct {
    pub const Color = enum {
        reset,
        dim,
        red,
        green,
        yellow,
        bright_red,
        bright_green,
        bright_yellow,
    };
    pub fn setColor(_: Tty, _: *std.Io.Writer, _: Color) !void {}
    pub fn detect() Tty {
        return .{};
    }
};
// Reach into utils module rather than @import("bench.zig") so the runner
// and any test importing utils.bench end up sharing the same module tree
// (avoids "file exists in two modules" errors).
const utils = @import("utils");
const bench = utils.bench;

// ════════════════════════════════════════════════════════════════════════
// Why we have our own test + bench runner
// ════════════════════════════════════════════════════════════════════════
//
// Zig ships a default test runner. `zig build test` invokes the test
// binary with `--listen=-`; the runner emits a structured event stream
// over stdout, and the build runner reads it into a summary tree. Fine
// for a small project. As egres grew we ran into limits — none fatal in
// isolation, but cumulative:
//
//   1. Test names hidden. Names exist in the IPC stream but the build
//      runner captures them into `+- run test 8 passed 7s`. To see
//      *which* test took 7s you had to run the cached binary directly
//      or maintain a parallel `test-trace` step with `stdio = .inherit`.
//
//   2. No concurrent execution. `zig test` runs tests serially. With
//      ~180 tests, a Debug suite was ~60s of mostly-sequential I/O.
//
//   3. Per-file `addTest()` plumbing. build.zig had ~250 lines of
//      stanzas — one `addTest()` per test file with module imports,
//      compression sources, dependency wiring. Adding a test file meant
//      editing build.zig.
//
//   4. No first-class benchmark support. We had four standalone
//      executables (bench-crc, bench-zio, bench-queue, bench-diskwal)
//      each with its own `pub fn main`, hand-tuned iteration counts,
//      and ad-hoc output format.
//
//   5. Test output interleaving. Once we did get concurrent execution,
//      `info(forest): foo` from one test would land between status
//      lines of another. No way to attribute log output to its test.
//
// What owning the runner gets us
// ────────────────────────────────────────────────────────────────────
//
//   • Visible names + timings on every line — no IPC dance:
//       [142/178]  zdb.cache_test    Cache basic put/get round-trip ... OK (5ms)
//
//   • Concurrent execution via zio fibers gated by a semaphore (default
//     max(2, cpu_count/2)). On a 32-core box the suite drops from ~60s
//     serial to ~8s with default parallelism.
//
//   • Auto-discovery. build.zig walks the source tree for `*_test.zig`
//     and `*_bench.zig` and feeds the union into a single test artifact.
//     New test files are picked up without touching build.zig.
//
//   • Per-fiber log capture. `std.log` calls are routed to a per-task
//     buffer (looked up via `zio.getCurrentTaskOrNull`) and printed
//     indented under each test's status line — concurrent output stops
//     interleaving.
//
//   • Color. OK green, FAIL red, SKIP yellow, dim prefix/timing/log
//     lines. Auto TTY detection respects `NO_COLOR` and `CLICOLOR_FORCE`.
//
//   • Stack trace on FAIL. Error return trace dumps right under the
//     FAIL line, so you see exactly which `try std.testing.expect…`
//     blew up.
//
//   • Auto-retry of failures. Tests that fail under heavy parallelism
//     but pass on serial retry are tagged `flaky` and don't break CI.
//     Tests that fail twice stay failed.
//
//   • Prefix-gated test classes. `BIG: ` for slow tests, `BENCH: ` for
//     benchmarks. `zig build test` runs the regular set; `zig build
//     bench` runs only benches in ReleaseFast mode.
//
//   • Go-style benchmark framework. `BENCH:` tests get a `*Bench`
//     handle: `b.n` (auto-tuned to ~200ms target), `b.alloc` (no
//     leak-tracking overhead), `b.resetTimer()` for setup/measure
//     boundary, `b.addBytes(n)` for throughput tracking. Output reports
//     ns/op, MiB/s, iter count, total bytes, sample duration in
//     fixed-width columns.
//
//   • Runtime flags: `--filter`, `--parallel`, `--bigtest`,
//     `--only-bench`, `--help`. No rebuild to re-target a subset.
//
//   • One binary, one entry point: `zig build test` and `zig build
//     bench`. The fan-out of `test-zdb`, `test-raft`, `test-zbuf`,
//     `bench-crc`, `bench-zio`, etc. is gone.
//
// What we gave up
// ────────────────────────────────────────────────────────────────────
//
//   • IPC build-cache integration. The build runner can't show `cached`
//     for our test step because we don't speak its event protocol. The
//     test binary still benefits from compile cache (incremental); only
//     the *run* always re-executes. Acceptable — the run is fast.
//
//   • `std.debug.print` capture. Zig 0.15's stdlib has no hook for it
//     (writes to a global stderr writer via `std.Progress`). Tests that
//     want attributed output should use `std.log.scoped(...).info(...)`
//     instead. Three test-side `std.debug.print` calls were converted.
//
// Other notes
// ────────────────────────────────────────────────────────────────────
//
// Tests run in `builtin.test_functions` order. Empirical: explicit
// duration-sorted (longest-first) ordering clustered heavy tests at the
// start and made wall time *worse*. Trust the natural order.
//
// CLI flags (forwarded via `zig build test -- ...`):
//   --filter <substr>   Run only tests whose name contains <substr>.
//   --exclude <substr>  Skip tests whose name contains <substr> (composes with --filter).
//   --bigtest           Include `BIG: ...` prefixed tests.
//   --only-bench        Run only `BENCH: ...` tests (used by `zig build bench`).
//   --parallel <N>      Concurrent test fibers. 0 (default) = max(2, cpu_count/2).

/// Override the Io instance that `std.debug` (including `defaultPanic`)
/// uses. The default — `Io.Threaded.global_single_threaded` — is
/// explicitly documented as "does not support concurrency". When
/// multiple fiber-worker threads panic concurrently, they all hit
/// that single-threaded Io and race on its internal state, ending
/// up with corrupted writer pointers (page-aligned `0x...0000`
/// junk) and SEGV inside `Io.Writer.write` deep in the panic-print
/// path. We point it at `std.testing.io_instance` (a real multi-
/// threaded `Io.Threaded.init`) so concurrent panics print safely.
pub const std_options_debug_threaded_io: ?*std.Io.Threaded = &std.testing.io_instance;

/// Custom panic handler. Default `defaultPanic` walks the stack via
/// `Dwarf.SelfUnwinder`, which has thread-safety bugs we hit under
/// heavy parallel test load. This minimal hook writes msg +
/// first_trace_addr (the panic call-site) directly to stderr via
/// raw `write` syscall, then aborts. Resolve the address with:
///   addr2line -fipCe <binary> <addr>
fn minimalPanic(msg: []const u8, first_trace_addr: ?usize) noreturn {
    const marker = "[MINIMAL_PANIC_FIRED]\n";
    _ = std.os.linux.write(2, marker.ptr, marker.len);
    // 4 KiB so a long DOUBLE_PUSH ring dump (~800-1200 bytes) fits.
    // Without this, `rawStderrPrint`'s `bufPrint` returns an empty
    // slice on overflow and we see only "[MINIMAL_PANIC_FIRED]" with
    // no body.
    var buf: [4096]u8 = undefined;
    const tid = std.Thread.getCurrentId();
    const trace_addr = first_trace_addr orelse 0;
    rawStderrPrint(&buf, "\nthread {d} panic: {s}\n  first_trace_addr: 0x{x}\n", .{ tid, msg, trace_addr });
    std.process.abort();
}
pub const panic = std.debug.FullPanic(minimalPanic);

pub const std_options: std.Options = .{
    .log_scope_levels = &.{
        .{ .scope = .zio, .level = .info },
    },
    .logFn = capturingLogFn,

    // TODO: escalate upstream in Zig forum / issue tracker.
    // Zig 0.16's `std.debug.Dwarf.SelfUnwinder` is not thread-safe:
    //   - Lazy init of `SelfInfo` races on first concurrent call.
    //   - Even after init, `nextInner` mutates per-walk FDE/CIE
    //     caches that get clobbered by concurrent walks.
    // Symptoms under our multi-worker fiber runtime: SEGV inside
    // `Dwarf.SelfUnwinder.nextInner(unwinder=0x0, cache_entry=0x0)`,
    // followed by recursive-panic in the panic handler's own
    // stack-walk. Triggered by `std.testing.allocator`'s leak-
    // source recording (one captureCurrentStackTrace per alloc),
    // by our own `captureTrace(&f.park_trace)` in `park()`, and
    // by the panic handler itself.
    //
    // Disabling stack tracing globally for this test binary
    // sidesteps all of the above. `std.testing.allocator` still
    // detects leaks (alloc/free balance) — we just lose the
    // "allocated at:" source pointers in leak reports. Acceptable
    // until the stdlib unwinder gets a mutex (or per-thread state).
    // Our `minimalPanic` doesn't call `captureCurrentStackTrace`,
    // and `std.testing.allocator`'s leak-trace path benefits from
    // being disabled (concurrent Dwarf walks race). Keep off.
    .allow_stack_tracing = false,
};

const default_parallel: u6 = 0;

fn resolveParallel(p: u6) u6 {
    if (p != 0) return p;
    // Half the cores by default — leaves CPU headroom for tests that fork
    // their own threads/fibers (e.g., bandwidth tests, ShardedCache concurrent
    // test) and reduces the timing-sensitive flake rate. Floor of 2 so single-
    // and dual-core boxes still get some parallelism.
    const n = std.Thread.getCpuCount() catch return 2;
    return @intCast(@max(2, @min(n / 2, 64)));
}

const Args = struct {
    filter: ?[]const u8 = null,
    exclude: ?[]const u8 = null,
    run_big: bool = false,
    only_bench: bool = false,
    /// `HANG:`-prefixed tests are watchdog smoke tests that intentionally
    /// never finish. Excluded by default — pass `--run-hang` to include
    /// them, e.g. to verify dump cadence and the `--wd-kill-s` exit.
    run_hang: bool = false,
    parallel: u6 = default_parallel,
    /// Watchdog tunables. `wd_stuck_tick_s` is one knob serving two
    /// purposes — dump threshold AND tick interval — so cadence is
    /// always one-dump-per-window. `wd_kill_s = 0` disables the kill.
    wd_stuck_tick_s: u64 = 20,
    wd_kill_s: u64 = 300,
};

const Counters = struct {
    passed: std.atomic.Value(u32) = .{ .raw = 0 },
    failed: std.atomic.Value(u32) = .{ .raw = 0 },
    skipped: std.atomic.Value(u32) = .{ .raw = 0 },
};

// ---------------------------------------------------------------------------
// Per-fiber std.log capture (Option A).
//
// std.options.logFn intercepts every `std.log.info(...)` etc. call. We look
// up the running zio task and write into a buffer registered under that
// task. The runner then prints the captured lines indented under each
// test's status. logs from contexts without a registered buffer (zio
// internals, runner setup) fall through to the default stderr writer.
// ---------------------------------------------------------------------------

const FiberLog = struct {
    bytes: std.ArrayList(u8) = .empty,
    overflowed: bool = false,

    const max_size: usize = 1 * 1024 * 1024; // 1 MiB cap per test
};

// Map keyed on the `*AnyTask` pointer (cast to usize for AutoHashMap).
// Multi-threaded: zio executors live on separate OS threads and call into
// logFn from any of them. std.Thread.Mutex (not zio.Mutex) because logFn
// must work in any context, including when zio's scheduler is mid-handoff.
var log_map_mu: std.Io.Mutex = .init;
var log_map: ?std.AutoHashMap(usize, *FiberLog) = null;

/// Set in `main()` before the runtime starts; read from `runOne`/`runBench`
/// after each test completion to bump the no-progress timer. A global is
/// simpler than threading a pointer through `driver` and the three
/// `SharedCtx` instances; null until set so a unit-test runner without a
/// watchdog (none today, but easy to imagine) is a no-op call.
var thread_watchdog: ?*ThreadWatchdog = null;

/// `--verbose` flag: print "STARTING <name>" before each test runs.
/// Off by default; flip on to diagnose wedges so the log shows which
/// fiber entered the test body but never finished.
var verbose_start: bool = false;

/// Identity used to key per-fiber log buffers. Phase A tests run as
/// zio tasks; Phase B tests run as fiber.Fibers. Both are pointer
/// types living in disjoint address spaces (different runtimes), so
/// the raw pointer doubles as a unique key once cast to usize.
fn currentLogKey() ?usize {
    if (zio.getCurrentTaskOrNull()) |task| return @intFromPtr(task);
    if (fiber.currentFiberOrNull()) |f| return @intFromPtr(f);
    return null;
}

fn registerFiberLog(log: *FiberLog) void {
    const key = currentLogKey() orelse return;
    mlock(&log_map_mu);
    defer munlock(&log_map_mu);
    if (log_map) |*m| m.put(key, log) catch {};
}

fn unregisterFiberLog() void {
    const key = currentLogKey() orelse return;
    mlock(&log_map_mu);
    defer munlock(&log_map_mu);
    if (log_map) |*m| _ = m.remove(key);
}

fn lookupFiberLog() ?*FiberLog {
    const key = currentLogKey() orelse return null;
    mlock(&log_map_mu);
    defer munlock(&log_map_mu);
    if (log_map) |*m| return m.get(key);
    return null;
}

fn capturingLogFn(
    comptime message_level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: []const u8,
    args: anytype,
) void {
    const log = lookupFiberLog() orelse {
        std.log.defaultLog(message_level, scope, format, args);
        return;
    };
    if (log.bytes.items.len > FiberLog.max_size) {
        log.overflowed = true;
        return;
    }
    const level_txt = comptime message_level.asText();
    const prefix2 = if (scope == .default) ": " else "(" ++ @tagName(scope) ++ "): ";
    log.bytes.print(std.heap.page_allocator, level_txt ++ prefix2 ++ format ++ "\n", args) catch {};
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const help_text =
    \\Usage: zig build test [-- <runner-flags>]
    \\
    \\Runs every discovered test (`*_test.zig`, `test.zig`, plus a few extras)
    \\in one binary, on a zio runtime so test bodies can use zio APIs without
    \\setting up their own runtime. Tests run concurrently as fibers.
    \\
    \\std.log output is captured per-test and printed indented under each
    \\test's status line. std.debug.print is NOT captured (no stdlib hook in
    \\Zig 0.15) — use std.log for attributed output.
    \\
    \\Flags (forward via `zig build test -- ...`):
    \\  --filter <substr>   Run only tests whose name contains <substr>.
    \\  --exclude <substr>  Skip tests whose name contains <substr> (composes with --filter).
    \\  --bigtest           Include `BIG: …` prefixed tests (slow, opt-in).
    \\  --only-bench        Run only `BENCH: …` tests (used by `zig build bench`).
    \\  --run-hang          Include `HANG: …` tests — never finish; exist to
    \\                      exercise the process-level watchdog. Default run
    \\                      gets killed at --wd-kill-s.
    \\  --parallel <N>      Concurrent test fibers. 0 (default) = max(2, cpu_count/2).
    \\  --verbose           Print "STARTING <name>" before each test (debug wedges).
    \\  --wd-stuck-tick-s <N> Watchdog: dump threshold AND tick interval, seconds (default 20).
    \\  --wd-kill-s <N>     Abort process after N seconds of no progress (default 300; 0 disables).
    \\  --help, -h          Show this help and exit.
    \\
    \\Examples:
    \\  zig build test                          # default suite (skips BIG:/BENCH:)
    \\  zig build test -- --bigtest             # include the slow tests
    \\  zig build test -- --filter "forest:"    # only forest tests
    \\  zig build test -- --exclude pgwire      # skip every pgwire test
    \\  zig build bench                         # run benchmarks (always ReleaseFast)
    \\
;

fn parseArgs(alloc: std.mem.Allocator) !Args {
    var args: Args = .{};
    const argv = try context.g().argv();
    _ = alloc;
    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const a = argv[i];
        if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            std.debug.print("{s}", .{help_text});
            std.process.exit(0);
        } else if (std.mem.eql(u8, a, "--filter") and i + 1 < argv.len) {
            args.filter = argv[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, a, "--exclude") and i + 1 < argv.len) {
            args.exclude = argv[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, a, "--bigtest")) {
            args.run_big = true;
        } else if (std.mem.eql(u8, a, "--only-bench")) {
            args.only_bench = true;
        } else if (std.mem.eql(u8, a, "--run-hang")) {
            args.run_hang = true;
        } else if (std.mem.eql(u8, a, "--parallel") and i + 1 < argv.len) {
            args.parallel = try std.fmt.parseInt(u6, argv[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, a, "--verbose")) {
            verbose_start = true;
        } else if (std.mem.eql(u8, a, "--wd-stuck-tick-s") and i + 1 < argv.len) {
            args.wd_stuck_tick_s = try std.fmt.parseInt(u64, argv[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, a, "--wd-kill-s") and i + 1 < argv.len) {
            args.wd_kill_s = try std.fmt.parseInt(u64, argv[i + 1], 10);
            i += 1;
        } else {
            std.debug.print("test_runner: unknown arg: {s}\n\n{s}", .{ a, help_text });
            std.process.exit(2);
        }
    }
    return args;
}

// ---------------------------------------------------------------------------
// Test execution
// ---------------------------------------------------------------------------

// Fixed column widths. Test names overflowing these will push the line
// right but won't bloat the column for everyone else.
const prefix_width: usize = 10; // "[NNN/NNN] " up to 999 tests
const module_width: usize = 28; // longest module today: "zdb-server.protocol_test" (24)
const bench_name_width: usize = 50; // longest BENCH name today: ~45 chars
const bench_nsop_width: usize = 12; // right-aligned ns/op column
const bench_tput_width: usize = 12; // right-aligned throughput column
const bench_iters_width: usize = 8; // right-aligned iters column
const bench_total_width: usize = 10; // right-aligned total-bytes column
const bench_dur_width: usize = 9; // right-aligned sample-duration column

const SharedCtx = struct {
    // Pacing primitive. Each spawned fiber gets a queue position
    // (0-indexed) and waits for the watermark to reach `my_index + 1`
    // before running. The watermark is pre-credited to `permits` so the
    // first `permits` fibers proceed immediately. Each completion
    // advances by 1 (offset by permits), which broadcasts — waiters
    // re-check their target and either proceed or re-park.
    watermark: *utils.Watermark,
    completed_count: *std.atomic.Value(u64),
    permits: u32,
    print_mu: *zio.Mutex,
    counters: *Counters,
    seq: *std.atomic.Value(u32),
    total: u32,
    out: *std.Io.Writer,
    tty: Tty,
    // Set during the parallel round to collect failures for serial retry.
    // Null during the retry round itself (one bite at the apple).
    failures: ?*std.ArrayList(std.builtin.TestFn),
    // Watchdog state: every running test pushes its entry on entry and pops
    // on exit. The watchdog fiber periodically scans this list and dumps
    // diagnostics if any test has been running longer than `stuck_after_s`.
    running: *RunningSet,
};

/// Bookkeeping for the watchdog dump. `list` holds tests that have
/// passed the watermark wait and entered the test body — i.e.
/// "actually running" fibers, never queued ones. `queued` and
/// `total`/`completed` give the dump enough context to say "of N
/// kept, K completed, R running, Q still queued".
///
/// Guarded by a `std.Io.Mutex` for cross-fiber writes; the std.Thread
/// watchdog grabs it via `mlock` (an `lockUncancelable`-on-fiber-or-
/// thread shim) — critical sections here are pure CPU (no yields), so
/// it's safe to take the io-mutex from a thread.
const RunningSet = struct {
    mu: std.Io.Mutex = .init,
    list: std.ArrayList(Entry) = .empty,
    /// Spawned-but-not-yet-past-the-watermark-wait count. Incremented
    /// before `watermark.waitForUntimed` and decremented immediately
    /// after, so at any instant: `queued + list.len + completed ≈ total`.
    queued: std.atomic.Value(u32) = .init(0),
    /// Completed-test counter pointer (shared with SharedCtx.seq) and
    /// the total test count for the current round. Null while no round
    /// is active. Used only by the watchdog for the dump header — not
    /// load-bearing for correctness.
    completed: ?*std.atomic.Value(u32) = null,
    total: u32 = 0,

    const Entry = struct {
        name: []const u8,
        start_ns: i128,
    };

    fn add(self: *RunningSet, name: []const u8) void {
        mlock(&self.mu);
        defer munlock(&self.mu);
        self.list.append(std.heap.page_allocator, .{
            .name = name,
            .start_ns = nowNs(),
        }) catch {};
    }

    fn remove(self: *RunningSet, name: []const u8) void {
        mlock(&self.mu);
        defer munlock(&self.mu);
        for (self.list.items, 0..) |t, i| {
            if (t.name.ptr == name.ptr and t.name.len == name.len) {
                _ = self.list.orderedRemove(i);
                return;
            }
        }
    }

    fn deinit(self: *RunningSet) void {
        self.list.deinit(std.heap.page_allocator);
    }
};

/// Format into `buf` and write to stderr (fd 2) via the raw Linux
/// write syscall — bypasses std.Io entirely. Used by the watchdog
/// so its dumps land even when zio itself is wedged (the previous
/// File.stderr().writer path would have blocked on zio's io).
/// Truncates silently if the formatted string exceeds the buffer
/// or the syscall returns an error.
fn rawStderrWriteAll(bytes: []const u8) void {
    var written: usize = 0;
    while (written < bytes.len) {
        const rc = std.os.linux.write(2, bytes.ptr + written, bytes.len - written);
        // Linux raw syscall: negative encodes errno, positive is byte count.
        // Treat any non-positive return as "stop trying".
        if (rc == 0 or rc > std.math.maxInt(isize)) return;
        written += rc;
    }
}

fn rawStderrPrint(buf: []u8, comptime fmt: []const u8, args: anytype) void {
    const formatted = std.fmt.bufPrint(buf, fmt, args) catch buf[0..0];
    rawStderrWriteAll(formatted);
}

/// std.Thread-based watchdog. Runs OUTSIDE the zio runtime, so it stays
/// alive — and capable of dumping diagnostics — even when the runtime is
/// mid-deinit (which is where a 64-hour wedge was observed). Operation:
///
///   • `touch()` is called from each test completion. It just stores a
///     monotonic-clock timestamp atomically; safe to call from any
///     thread/fiber/anywhere.
///   • The thread wakes every `stuck_tick_s` seconds via `wake.timedWait`
///     — the same value is also the "test has been stuck this long" dump
///     threshold, so the dump cadence is exactly one-per-window.
///   • Fires a dump when EITHER:
///       (a) no test has completed in `stuck_tick_s` seconds, OR
///       (b) some specific running test has been live > `stuck_tick_s`.
///     The dump names the stuck tests (with elapsed-ms) and includes
///     a TrackedMutex snapshot. Keeps watching so a growing wedge gets
///     fresh dumps each tick.
///   • If `kill_after_s > 0` and no progress for that long, the watchdog
///     dumps a final diagnostic and aborts the process. 0 disables the
///     kill (debug-without-time-bomb mode).
///
/// Owns its own RunningSet so the watchdog and test runner share one
/// source of truth on what's currently executing.
///
/// Lifecycle: `init` → `start` → `touch` ... `touch` → `stop`. `stop`
/// flips the wake event and joins; without that the join would block
/// for up to `stuck_tick_s` waiting for the next tick.
const ThreadWatchdog = struct {
    last_progress_ns: std.atomic.Value(i64),
    /// Set by `stop()`. The loop uses `wake.timedWait(stuck_tick_s)`
    /// instead of raw `std.Thread.sleep` so `stop()` can interrupt the
    /// sleep immediately — without this, shutdown would pay up to
    /// `stuck_tick_s` seconds of latency waiting for the next tick.
    wake: std.Io.Event = .unset,
    thread: ?std.Thread = null,
    /// One knob for two purposes: how long a test has to be stuck before
    /// we dump, AND how often we re-check. Holding them equal keeps the
    /// dump cadence clean and removes a footgun (tick > stuck would silently
    /// delay the first dump).
    stuck_tick_s: u64,
    /// Hard timeout: after this many seconds of no progress, dump and
    /// abort the process. 0 disables the kill — useful when attaching a
    /// debugger to a wedged binary.
    kill_after_s: u64,
    running: RunningSet = .{},

    fn init(stuck_tick_s: u64, kill_after_s: u64) ThreadWatchdog {
        return .{
            .last_progress_ns = .init(@intCast(nowNs())),
            .stuck_tick_s = stuck_tick_s,
            .kill_after_s = kill_after_s,
        };
    }

    fn deinit(self: *ThreadWatchdog) void {
        self.running.deinit();
    }

    fn start(self: *ThreadWatchdog) !void {
        self.thread = try std.Thread.spawn(.{}, threadWatchdogLoop, .{self});
    }

    fn touch(self: *ThreadWatchdog) void {
        self.last_progress_ns.store(@intCast(nowNs()), .release);
    }

    fn stop(self: *ThreadWatchdog) void {
        self.wake.set(ioOf());
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }
};

/// Top-level (non-method) entry point so `std.Thread.spawn` doesn't trip
/// the Zig backend bug "genSetReg called with a value larger than dst_reg"
/// when it monomorphizes a method through `spawn(.{}, fn_ptr, ...)`.
fn threadWatchdogLoop(self: *ThreadWatchdog) void {
    // Write diagnostics straight to stderr (bypassing the zio-aware
    // print_mu and io_writer). The runtime may be wedged; the print
    // mutex itself may be the deadlocked lock. Using stderr directly
    // keeps the watchdog independent of any zio state.
    // Wake every 1s regardless of the stuck threshold so the first dump
    // lands within ~1s of the threshold (instead of up to 2× threshold).
    // The threshold below still controls WHEN we declare "stuck"; tick
    // only controls how often we check.
    const tick_ns: u64 = std.time.ns_per_s;
    const threshold_ns: i128 = @as(i128, self.stuck_tick_s) * std.time.ns_per_s;
    // Initialize to 0 (not minInt). `now_s` is wall-clock seconds since
    // epoch (≈ 1.7e9 in 2026), and (now_s - minInt(i64)) would overflow.
    // Starting from 0 means the first eligible dump fires immediately
    // once `stuck_global` is true — which is exactly what we want.
    var last_dumped_at_s: i64 = 0;
    while (true) {
        // timedWait returns error.Timeout on normal tick, void on stop()
        // having called .set(). Either way fall through to the wake check;
        // we exit only when .set() was called.
        self.wake.waitTimeout(ioOf(), timeoutNs(tick_ns)) catch {};
        if (self.wake.isSet()) return;

        const now_i128 = nowNs();
        const now_i64: i64 = @intCast(now_i128);
        const last = self.last_progress_ns.load(.acquire);
        const elapsed_s: i64 = @divTrunc(now_i64 - last, std.time.ns_per_s);
        const stuck_global = elapsed_s >= @as(i64, @intCast(self.stuck_tick_s));

        // Snapshot stuck tests under the lock; release before printing so
        // we never hold it across I/O.
        var stuck: std.ArrayList(RunningSet.Entry) = .empty;
        defer stuck.deinit(std.heap.page_allocator);
        {
            mlock(&self.running.mu);
            defer munlock(&self.running.mu);
            for (self.running.list.items) |t| {
                if (now_i128 - t.start_ns >= threshold_ns) {
                    stuck.append(std.heap.page_allocator, t) catch {};
                }
            }
        }

        if (!stuck_global and stuck.items.len == 0) continue;

        // Throttle dump cadence to once per stuck_tick_s window so
        // tick-every-1s doesn't spam stderr every second once stuck.
        const now_s: i64 = @divTrunc(now_i64, std.time.ns_per_s);
        if (now_s - last_dumped_at_s < @as(i64, @intCast(self.stuck_tick_s))) continue;
        last_dumped_at_s = now_s;

        // Snapshot counters AFTER releasing the list lock. "running" is
        // the post-watermark fiber count; "queued" is spawned-but-still-
        // waiting on its watermark target; "completed" is tests already
        // reported. These three plus the always-zero "not yet spawned"
        // should sum to total.
        const queued = self.running.queued.load(.acquire);
        const completed: u32 = if (self.running.completed) |c| c.load(.acquire) else 0;
        const running_now: u32 = blk: {
            mlock(&self.running.mu);
            defer munlock(&self.running.mu);
            break :blk @intCast(self.running.list.items.len);
        };
        const total = self.running.total;

        // Raw stderr via std.posix.write(2, …). The previous version
        // used `File.stderr().writer(ioOf(), &buf)` which goes through
        // the zio-backed io; if zio itself is wedged (the very case we
        // need to diagnose) that writer's flush blocks and the dump
        // never reaches the user. POSIX write bypasses io entirely.
        var line_buf: [1024]u8 = undefined;
        rawStderrPrint(&line_buf, "\n=== ThreadWatchdog: no progress for {d}s (threshold={d}s) ===\n", .{ elapsed_s, self.stuck_tick_s });
        rawStderrPrint(&line_buf, "  round: total={d} completed={d} running={d} queued={d}\n", .{ total, completed, running_now, queued });
        for (stuck.items) |t| {
            // Divide-then-cast so fractional seconds survive: 21500ms → 21.5s.
            const ms = @divTrunc(now_i128 - t.start_ns, std.time.ns_per_ms);
            const secs = @as(f64, @floatFromInt(ms)) / 1000.0;
            rawStderrPrint(&line_buf, "  RUNNING {d:.1}s: {s}\n", .{ secs, t.name });
        }
        // Zio fiber state: same memory-buffer-then-raw-write trick so
        // the dump bypasses any zio-backed I/O even when the runtime is
        // wedged. `rt.stats()` and `rt.dumpFibers()` both lock
        // `fibers_mutex` (an `os.Mutex` = std.Thread-safe), so calling
        // them from the watchdog thread is fine.
        const zrt = rt.underlying();
        const zs = zrt.stats();
        rawStderrPrint(&line_buf, "zio fibers: total={d} ready={d} blocked={d}\n", .{ zs.total, zs.ready, zs.blocked });
        var dump_writer: std.Io.Writer.Allocating = .init(std.heap.page_allocator);
        defer dump_writer.deinit();
        zrt.dumpFibers(&dump_writer.writer) catch {};
        rawStderrWriteAll(dump_writer.written());

        // Phase B uses a dedicated fiber.Runtime; dump its state too.
        // Safe to call from the watchdog thread — `fiber.Runtime` is
        // initialized at main() entry, so the singleton is always
        // live by the time the watchdog could fire.
        const frt = fiber.runtime();
        const fs = frt.stats();
        rawStderrPrint(&line_buf, "fiber.Runtime: total={d} new={d} ready={d} waiting={d} finished={d}\n", .{ fs.total, fs.new, fs.ready, fs.waiting, fs.finished });
        dump_writer.clearRetainingCapacity();
        fiber.dumpFibers(frt, &dump_writer.writer) catch {};
        rawStderrWriteAll(dump_writer.written());

        // TrackedMutex dump (same buffered-then-raw-write pattern).
        rawStderrPrint(&line_buf, "TrackedMutex state:\n", .{});
        dump_writer.clearRetainingCapacity();
        utils.tracked_mutex.dump(&dump_writer.writer) catch {};
        rawStderrWriteAll(dump_writer.written());
        rawStderrPrint(&line_buf, "\n", .{});

        // Hard timeout: dump and abort the process. The dump above is
        // already printed, so the user sees the final state before we
        // kill. `kill_after_s == 0` disables (debug mode).
        if (self.kill_after_s > 0 and elapsed_s >= @as(i64, @intCast(self.kill_after_s))) {
            rawStderrPrint(&line_buf, "=== ThreadWatchdog: KILL after {d}s of no progress (limit={d}s) ===\n", .{ elapsed_s, self.kill_after_s });
            std.process.exit(124); // 124 mirrors `timeout(1)` exit code
        }
    }
}

fn writeColored(
    out: *std.Io.Writer,
    tty: Tty,
    color: Tty.Color,
    comptime fmt: []const u8,
    args: anytype,
) void {
    tty.setColor(out, color) catch {};
    out.print(fmt, args) catch {};
    tty.setColor(out, .reset) catch {};
}

/// Format a byte count using the largest binary-prefix unit that keeps
/// the leading number under 1024. Returns a slice of `buf`. Pass
/// `per_s = true` to append "/s" (used for throughput); `false` for a
/// raw byte total.
fn formatBinaryBytes(buf: []u8, bytes: f64, per_s: bool) []const u8 {
    const units = [_][]const u8{ "B", "KiB", "MiB", "GiB", "TiB", "PiB" };
    var v = bytes;
    var idx: usize = 0;
    while (v >= 1024.0 and idx + 1 < units.len) : (idx += 1) v /= 1024.0;
    const suffix: []const u8 = if (per_s) "/s" else "";
    return std.fmt.bufPrint(buf, "{d:.1} {s}{s}", .{ v, units[idx], suffix }) catch "??";
}

/// Format a count with a decimal SI suffix (K=1e3, M=1e6, G=1e9, T=1e12).
/// Used for iteration counts in benchmark output.
fn formatCount(buf: []u8, n: u64) []const u8 {
    if (n < 1000) return std.fmt.bufPrint(buf, "{d}", .{n}) catch "??";
    const suffixes = [_]u8{ 'K', 'M', 'G', 'T' };
    var v = @as(f64, @floatFromInt(n)) / 1000.0;
    var idx: usize = 0;
    while (v >= 1000.0 and idx + 1 < suffixes.len) : (idx += 1) v /= 1000.0;
    return std.fmt.bufPrint(buf, "{d:.1}{c}", .{ v, suffixes[idx] }) catch "??";
}

/// Format a duration (in nanoseconds) using the largest unit that keeps
/// the leading number under 1000. Returns a slice of `buf`. Uses ASCII
/// "us" rather than UTF-8 "µs" so byte-width padding aligns visually in
/// monospace output (µ is 2 bytes but renders as 1 column).
fn formatDuration(buf: []u8, ns: f64) []const u8 {
    const units = [_][]const u8{ "ns", "us", "ms", "s " };
    var v = ns;
    var idx: usize = 0;
    while (v >= 1000.0 and idx + 1 < units.len) : (idx += 1) v /= 1000.0;
    return std.fmt.bufPrint(buf, "{d:.1} {s}", .{ v, units[idx] }) catch "??";
}

// Zig formats test names as `<module_path>.test.<user-supplied name>`. The
// `.test.` infix is how the compiler distinguishes test decls from regular
// fns/consts; it's just noise to a reader. Display as `<module_path>  <name>`
// with the module padded to a shared width so test names line up vertically.
fn splitModuleName(name: []const u8) struct { module: []const u8, test_name: []const u8 } {
    if (std.mem.indexOf(u8, name, ".test.")) |p| {
        return .{ .module = name[0..p], .test_name = name[p + ".test.".len ..] };
    }
    return .{ .module = "", .test_name = name };
}

fn writeDisplayName(out: *std.Io.Writer, tty: Tty, name: []const u8) void {
    const split = splitModuleName(name);
    if (split.module.len == 0) {
        out.writeAll(split.test_name) catch {};
        return;
    }
    // Module path in dim so the test name reads first.
    writeColored(out, tty, .dim, "{s}", .{split.module});
    const pad = if (module_width > split.module.len) module_width - split.module.len else 1;
    for (0..pad) |_| out.writeByte(' ') catch {};
    out.writeAll(split.test_name) catch {};
}

fn runOne(t: std.builtin.TestFn, ctx: *SharedCtx, my_index: u64) void {
    // Track the queue wait: `queued` is "spawned but not yet started".
    // Watchdog reads this to label its dump with how many fibers are
    // still queued vs. actually running.
    _ = ctx.running.queued.fetchAdd(1, .seq_cst);
    // The watermark was pre-advanced to `permits` before spawn, so
    // fibers 0..permits-1 satisfy `value >= my_index + 1` immediately
    // without entering cv.wait. Fibers from index `permits` onward
    // park until enough completions arrive. Shielded against cancel
    // so we never bail mid-wait — same contract as the old
    // waitUncancelable.
    {
        ctx.watermark.waitForUntimed(my_index + 1) catch |e| {
            std.log.err("watermark wait failed for {s}: {s}", .{ t.name, @errorName(e) });
            return;
        };
    }
    _ = ctx.running.queued.fetchSub(1, .seq_cst);
    // Defer-last: advance the watermark by 1 past the permit floor for
    // each completion. The pre-credit of `permits` means initial=
    // permits, and after k completions value=permits+k. Every advance
    // broadcasts, so all waiters re-check — no chain wake to lose.
    //
    // Diagnostic: stderr-print on every advance so we can confirm
    // from outside the zio runtime that completions are firing.
    // Bypasses the per-fiber log capture so this lands in the run log
    // even when the runtime is mid-wedge.
    defer {
        const n = ctx.completed_count.fetchAdd(1, .monotonic) + 1;
        ctx.watermark.advance(ctx.permits + n);
    }

    ctx.running.add(t.name);
    defer {
        ctx.running.remove(t.name);
        if (thread_watchdog) |w| w.touch();
    }

    // Optional STARTING log — useful for diagnosing wedges. Disabled by
    // default; the watchdog dump names stuck tests on its own.
    if (verbose_start) {
        ctx.print_mu.lockUncancelable();
        ctx.out.print("  >> STARTING {s}\n", .{t.name}) catch {};
        ctx.out.flush() catch {};
        ctx.print_mu.unlock();
    }

    var fiber_log: FiberLog = .{};
    defer fiber_log.bytes.deinit(std.heap.page_allocator);
    registerFiberLog(&fiber_log);
    defer unregisterFiberLog();

    // Capture the error return trace into owned memory IMMEDIATELY in the
    // catch arm. The trace lives in thread-local state; if the fiber yields
    // and resumes on a different OS thread, the pointer would become stale.
    // Copying the addresses sidesteps that.
    var trace_addrs: []usize = &.{};
    defer std.heap.page_allocator.free(trace_addrs);

    // Per-test deadline: 30 s normally, 60 s for BIG:-prefixed tests
    // that advertise longer runtimes. AutoCancel.set arms a timer on
    // this fiber; when it fires, the next I/O yield (sleep, lock,
    // recv, …) returns error.Canceled and `timeout.triggered` is set.
    // A test stuck in a tight CPU loop without yielding cannot be
    // canceled cooperatively.
    const user_name = if (std.mem.indexOf(u8, t.name, ".test.")) |p|
        t.name[p + ".test.".len ..]
    else
        t.name;
    const is_big = std.mem.startsWith(u8, user_name, "BIG: ");
    const is_hang = std.mem.startsWith(u8, user_name, "HANG: ");

    var timeout = zio.AutoCancel.init;
    defer timeout.clear();
    // HANG: tests intentionally never finish — they exist to exercise the
    // process-level watchdog (--wd-stuck-tick-s dumps + --wd-kill-s abort).
    // Setting a per-test cancel would defeat the point: the fiber would
    // wake at 30/60s with error.Canceled and report a normal failure, and
    // the watchdog would never see a sustained no-progress window.
    if (!is_hang) timeout.set(zio.Timeout.fromSeconds(if (is_big) 60 else 30));

    const t0 = nowNs();
    const result: ?anyerror = if (t.func()) |_| null else |e| blk: {
        if (e != error.SkipZigTest) {
            if (@errorReturnTrace()) |trace| {
                const n = @min(trace.index, trace.instruction_addresses.len);
                trace_addrs = std.heap.page_allocator.dupe(usize, trace.instruction_addresses[0..n]) catch &.{};
            }
        }
        break :blk e;
    };
    const elapsed_ms = @divTrunc(nowNs() - t0, std.time.ns_per_ms);
    const timed_out = if (result) |e| e == error.Canceled and timeout.triggered else false;

    const seq = ctx.seq.fetchAdd(1, .seq_cst) + 1;

    ctx.print_mu.lockUncancelable();
    defer ctx.print_mu.unlock();

    // [N/M] prefix dimmed so the eye lands on the test name + status.
    // Format into a small buffer first so we can pad to a fixed column width.
    var prefix_buf: [32]u8 = undefined;
    const prefix_str = std.fmt.bufPrint(&prefix_buf, "[{d}/{d}]", .{ seq, ctx.total }) catch unreachable;
    writeColored(ctx.out, ctx.tty, .dim, "{s}", .{prefix_str});
    const prefix_pad = if (prefix_width > prefix_str.len) prefix_width - prefix_str.len else 1;
    for (0..prefix_pad) |_| ctx.out.writeByte(' ') catch {};

    writeDisplayName(ctx.out, ctx.tty, t.name);
    ctx.out.print(" ... ", .{}) catch {};

    if (result) |err| switch (err) {
        error.SkipZigTest => {
            _ = ctx.counters.skipped.fetchAdd(1, .seq_cst);
            writeColored(ctx.out, ctx.tty, .bright_yellow, "SKIP\n", .{});
        },
        else => {
            _ = ctx.counters.failed.fetchAdd(1, .seq_cst);
            if (timed_out) {
                writeColored(ctx.out, ctx.tty, .bright_red, "FAIL: TIMEOUT", .{});
            } else {
                writeColored(ctx.out, ctx.tty, .bright_red, "FAIL: {s}", .{@errorName(err)});
            }
            writeColored(ctx.out, ctx.tty, .dim, " ({d}ms)\n", .{elapsed_ms});
            // Capture for serial retry. We're inside print_mu, so the append
            // is implicitly serialized.
            if (ctx.failures) |list| list.append(std.heap.page_allocator, t) catch {};
            // On timeout, log the test name once up front so the diag
            // block self-identifies even if scrolled past the status line.
            if (timed_out) {
                writeColored(ctx.out, ctx.tty, .bright_red, "  | TIMEOUT in '{s}' — diagnostics follow\n", .{user_name});
            }
            // Dump the captured stack trace (writes to stderr directly via
            // its own lock; flush our buffered writer first so order is
            // preserved on the terminal).
            if (trace_addrs.len > 0) {
                ctx.out.flush() catch {};
                // Zig 0.16 split StackTrace into a builtin variant
                // (index + instruction_addresses) and a debug variant
                // (return_addresses + skipped). dumpStackTrace wants the
                // debug variant.
                const dbg_trace: std.debug.StackTrace = .{
                    .return_addresses = trace_addrs,
                    .skipped = .none,
                };
                std.debug.dumpStackTrace(&dbg_trace);
            }
            // On timeout, also dump TrackedMutex state — currently-held
            // locks are the most likely smoking gun for a deadlock.
            if (timed_out) {
                ctx.out.flush() catch {};
                writeColored(ctx.out, ctx.tty, .dim, "  | TrackedMutex state for '{s}':\n", .{user_name});
                utils.tracked_mutex.dump(ctx.out) catch {};
                ctx.out.flush() catch {};
            }
        },
    } else {
        _ = ctx.counters.passed.fetchAdd(1, .seq_cst);
        writeColored(ctx.out, ctx.tty, .bright_green, "OK", .{});
        writeColored(ctx.out, ctx.tty, .dim, " ({d}ms)\n", .{elapsed_ms});
    }

    // Print captured log lines indented under the status line.
    if (fiber_log.bytes.items.len > 0) {
        var lines = std.mem.splitScalar(u8, fiber_log.bytes.items, '\n');
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            writeColored(ctx.out, ctx.tty, .dim, "  | {s}\n", .{line});
        }
    }
    if (fiber_log.overflowed) {
        writeColored(ctx.out, ctx.tty, .dim, "  | (output truncated at {d} bytes)\n", .{FiberLog.max_size});
    }
    ctx.out.flush() catch {};
}

fn runBench(t: std.builtin.TestFn, ctx: *SharedCtx, my_index: u64) void {
    _ = ctx.running.queued.fetchAdd(1, .seq_cst);
    {
        zio.beginShield();
        defer zio.endShield();
        ctx.watermark.waitForUntimed(my_index + 1) catch |e| {
            std.log.err("watermark wait failed for {s}: {s}", .{ t.name, @errorName(e) });
            return;
        };
    }
    _ = ctx.running.queued.fetchSub(1, .seq_cst);
    defer {
        const n = ctx.completed_count.fetchAdd(1, .monotonic) + 1;
        ctx.watermark.advance(ctx.permits + n);
    }

    ctx.running.add(t.name);
    defer {
        ctx.running.remove(t.name);
        if (thread_watchdog) |w| w.touch();
    }

    var fiber_log: FiberLog = .{};
    defer fiber_log.bytes.deinit(std.heap.page_allocator);
    registerFiberLog(&fiber_log);
    defer unregisterFiberLog();

    var b: bench.Bench = .{};
    bench.register(&b);
    defer bench.unregister();

    // Auto-tune: run the bench fn with growing n until a sample exceeds
    // the target duration. The user-controlled timer (b.resetTimer /
    // b.stopTimer / b.startTimer) decides what counts; setup outside
    // those calls isn't measured.
    const target_ns: i128 = 200 * std.time.ns_per_ms;
    const max_n: u64 = 100_000_000_000;
    const max_growth: u64 = 100;

    var sample_ns: i128 = 0;
    var err: ?anyerror = null;
    while (true) {
        // Reset per-sample state so the user starts each sample fresh.
        // resetTimer() in the bench function will move the start point
        // forward to discount setup; addBytes() accumulates into total_bytes.
        b.elapsed_ns = 0;
        b.start_ns = nowNs();
        b.timer_running = true;
        b.total_bytes = 0;
        if (t.func()) |_| {} else |e| {
            err = e;
            break;
        }
        sample_ns = b.elapsed();
        if (sample_ns >= target_ns) break;
        if (b.n >= max_n) break;
        const ns_u: u64 = @intCast(@max(1, sample_ns));
        const target_u: u64 = @intCast(target_ns);
        // Extrapolate: pick n such that sample reaches target. Cap growth
        // factor so a sub-microsecond first sample doesn't request 1e12 iters.
        var next_n = (b.n * target_u) / ns_u + 1;
        if (next_n > b.n * max_growth) next_n = b.n * max_growth;
        if (next_n > max_n) next_n = max_n;
        if (next_n <= b.n) next_n = b.n + 1;
        b.n = next_n;
    }

    const seq = ctx.seq.fetchAdd(1, .seq_cst) + 1;

    ctx.print_mu.lockUncancelable();
    defer ctx.print_mu.unlock();

    var prefix_buf: [32]u8 = undefined;
    const prefix_str = std.fmt.bufPrint(&prefix_buf, "[{d}/{d}]", .{ seq, ctx.total }) catch unreachable;
    writeColored(ctx.out, ctx.tty, .dim, "{s}", .{prefix_str});
    const prefix_pad = if (prefix_width > prefix_str.len) prefix_width - prefix_str.len else 1;
    for (0..prefix_pad) |_| ctx.out.writeByte(' ') catch {};

    // Module column (dim) + test name padded to a fixed width so the `→`
    // and metric columns line up across all benches.
    const split = splitModuleName(t.name);
    writeColored(ctx.out, ctx.tty, .dim, "{s}", .{split.module});
    const mod_pad = if (module_width > split.module.len) module_width - split.module.len else 1;
    for (0..mod_pad) |_| ctx.out.writeByte(' ') catch {};
    ctx.out.writeAll(split.test_name) catch {};
    const name_pad = if (bench_name_width > split.test_name.len) bench_name_width - split.test_name.len else 1;
    for (0..name_pad) |_| ctx.out.writeByte(' ') catch {};

    if (err) |e| switch (e) {
        error.SkipZigTest => {
            _ = ctx.counters.skipped.fetchAdd(1, .seq_cst);
            writeColored(ctx.out, ctx.tty, .bright_yellow, "SKIP\n", .{});
        },
        else => {
            _ = ctx.counters.failed.fetchAdd(1, .seq_cst);
            writeColored(ctx.out, ctx.tty, .bright_red, "FAIL: {s}\n", .{@errorName(e)});
        },
    } else {
        _ = ctx.counters.passed.fetchAdd(1, .seq_cst);
        const ns_per_op: f64 = @as(f64, @floatFromInt(sample_ns)) / @as(f64, @floatFromInt(b.n));
        // Format each metric into a buffer, then right-pad to a fixed
        // column. ns/op auto-scales to ns / µs / ms / s so a 9 ns hash
        // and a 4 ms WAL append are both readable in the same column.
        var ns_buf: [32]u8 = undefined;
        const ns_str = formatDuration(&ns_buf, ns_per_op);
        const ns_pad = if (bench_nsop_width > ns_str.len) bench_nsop_width - ns_str.len else 0;
        for (0..ns_pad) |_| ctx.out.writeByte(' ') catch {};
        writeColored(ctx.out, ctx.tty, .bright_green, "{s}/op", .{ns_str});

        if (b.total_bytes > 0) {
            const total_bytes: f64 = @floatFromInt(b.total_bytes);
            const seconds: f64 = @as(f64, @floatFromInt(sample_ns)) / 1e9;
            const bytes_per_s: f64 = total_bytes / seconds;
            var tput_buf: [32]u8 = undefined;
            const tput = formatBinaryBytes(&tput_buf, bytes_per_s, true);
            ctx.out.writeAll("  ") catch {};
            const tput_pad = if (bench_tput_width > tput.len) bench_tput_width - tput.len else 0;
            for (0..tput_pad) |_| ctx.out.writeByte(' ') catch {};
            writeColored(ctx.out, ctx.tty, .bright_green, "{s}", .{tput});
        }

        var count_buf: [16]u8 = undefined;
        const iters = formatCount(&count_buf, b.n);
        ctx.out.writeAll("  ") catch {};
        const iters_pad = if (bench_iters_width > iters.len) bench_iters_width - iters.len else 0;
        for (0..iters_pad) |_| ctx.out.writeByte(' ') catch {};
        writeColored(ctx.out, ctx.tty, .dim, "{s} iters", .{iters});

        if (b.total_bytes > 0) {
            var total_buf: [32]u8 = undefined;
            const total = formatBinaryBytes(&total_buf, @floatFromInt(b.total_bytes), false);
            ctx.out.writeAll("  ") catch {};
            const total_pad = if (bench_total_width > total.len) bench_total_width - total.len else 0;
            for (0..total_pad) |_| ctx.out.writeByte(' ') catch {};
            writeColored(ctx.out, ctx.tty, .dim, "{s} total", .{total});
        }

        // Wall-clock time the final sample took. Mostly an auto-tune sanity
        // check — should be close to the 200ms target unless a single iter
        // overshot.
        var dur_buf: [32]u8 = undefined;
        const dur_str = formatDuration(&dur_buf, @as(f64, @floatFromInt(sample_ns)));
        ctx.out.writeAll("  ") catch {};
        const dur_pad = if (bench_dur_width > dur_str.len) bench_dur_width - dur_str.len else 0;
        for (0..dur_pad) |_| ctx.out.writeByte(' ') catch {};
        writeColored(ctx.out, ctx.tty, .dim, "in {s}\n", .{dur_str});
    }

    if (fiber_log.bytes.items.len > 0) {
        var lines = std.mem.splitScalar(u8, fiber_log.bytes.items, '\n');
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            writeColored(ctx.out, ctx.tty, .dim, "  | {s}\n", .{line});
        }
    }
    ctx.out.flush() catch {};
}

fn driver(
    args: Args,
    counters: *Counters,
    retry_counters: *Counters,
    bench_counters: *Counters,
    alloc: std.mem.Allocator,
    out: *std.Io.Writer,
    tty: Tty,
) !void {
    // Spawn the coarse.Clock tick fiber on the runner's long-lived runtime
    // so it survives across every test fiber. `Clock.start()` is idempotent
    // and uses a process-global `started` flag — re-spawning it inside an
    // individual test would attach the tick fiber to that test's runtime
    // (or be a no-op if any earlier test had already set the flag), and
    // when that runtime is deinit'd the fiber dies. Result: subsequent
    // tests calling `Clock.ms()` would read a frozen cache. `Clock.stop()`
    // in `main()` runs after all tests finish, draining this fiber.
    try utils.coarse.Clock.start();

    // Currently-running tests, owned by the std.Thread ThreadWatchdog so
    // its diagnostics can name which tests are stuck. The watchdog itself
    // is set up in main() before the zio runtime starts; here we just
    // grab a pointer to its RunningSet for the test ctx.
    const running: *RunningSet = if (thread_watchdog) |w| &w.running else unreachable;

    var watermark: utils.Watermark = .{};
    // Pre-credit `permits` slots so the first `permits` fibers
    // satisfy their `value >= my_index + 1` check immediately
    // without entering cv.wait.
    watermark.advance(args.parallel);
    var completed_count: std.atomic.Value(u64) = .{ .raw = 0 };
    var print_mu: zio.Mutex = .init;
    var seq: std.atomic.Value(u32) = .{ .raw = 0 };
    var failures: std.ArrayList(std.builtin.TestFn) = .empty;
    defer failures.deinit(std.heap.page_allocator);

    // Partition into regular tests and BENCH:-prefixed benchmarks. Regular
    // tests run in parallel (with retry); benchmarks run serially with
    // auto-tuned iteration counts.
    var kept: std.ArrayList(std.builtin.TestFn) = .empty;
    defer kept.deinit(alloc);
    var bench_tests: std.ArrayList(std.builtin.TestFn) = .empty;
    defer bench_tests.deinit(alloc);

    for (builtin.test_functions) |t| {
        if (std.mem.startsWith(u8, t.name, ".test_root.")) continue;
        // fiber.* tests belong to Phase B (fiberDriver on fiber.Runtime).
        // Skip here so we don't run them under zio.
        if (std.mem.startsWith(u8, t.name, "fiber.")) continue;
        const user_name = if (std.mem.indexOf(u8, t.name, ".test.")) |p|
            t.name[p + ".test.".len ..]
        else
            t.name;
        const is_big = std.mem.startsWith(u8, user_name, "BIG: ");
        const is_bench = std.mem.startsWith(u8, user_name, "BENCH: ");
        const is_hang = std.mem.startsWith(u8, user_name, "HANG: ");
        if (args.filter) |f| if (std.mem.indexOf(u8, t.name, f) == null) continue;
        if (args.exclude) |x| if (std.mem.indexOf(u8, t.name, x) != null) continue;
        if (is_bench) {
            if (!args.only_bench) continue;
            try bench_tests.append(alloc, t);
            continue;
        }
        if (args.only_bench) continue;
        if (is_big and !args.run_big) continue;
        if (is_hang and !args.run_hang) continue;
        try kept.append(alloc, t);
    }

    var ctx = SharedCtx{
        .watermark = &watermark,
        .completed_count = &completed_count,
        .permits = args.parallel,
        .print_mu = &print_mu,
        .counters = counters,
        .seq = &seq,
        .total = @intCast(kept.items.len),
        .out = out,
        .tty = tty,
        .failures = &failures,
        .running = running,
    };

    // Parallel round: run all tests, collecting failures. Use
    // `spawnWith` instead of `Group.spawn` so each test fiber carries a
    // spawn-site `@src()` that `rt.dumpFibers()` can print in the
    // watchdog dump.
    {
        running.completed = &seq;
        running.total = @intCast(kept.items.len);
        defer {
            running.completed = null;
            running.total = 0;
        }
        const zrt = rt.underlying();
        var handles: std.ArrayList(zio.JoinHandle(void)) = .empty;
        defer handles.deinit(alloc);
        for (kept.items, 0..) |t, i| {
            const h = try zrt.spawnWith(.{ .spawn_loc = @src() }, runOne, .{ t, &ctx, @as(u64, i) });
            try handles.append(alloc, h);
        }
        for (handles.items) |*h| h.join();
    }

    // Serial retry round: re-run any failures one at a time. Skips when
    // parallel was already 1 (retry would be the same execution mode) or
    // there's nothing to retry.
    if (failures.items.len > 0 and args.parallel > 1) {
        print_mu.lockUncancelable();
        out.writeAll("\n") catch {};
        writeColored(out, tty, .bright_yellow, "Retrying {d} failed test(s) serially...\n", .{failures.items.len});
        out.flush() catch {};
        print_mu.unlock();

        var retry_watermark: utils.Watermark = .{};
        retry_watermark.advance(1); // permits=1 for retry round
        var retry_completed_count: std.atomic.Value(u64) = .{ .raw = 0 };
        var retry_seq: std.atomic.Value(u32) = .{ .raw = 0 };
        var retry_ctx = SharedCtx{
            .watermark = &retry_watermark,
            .completed_count = &retry_completed_count,
            .permits = 1,
            .print_mu = &print_mu,
            .counters = retry_counters,
            .seq = &retry_seq,
            .total = @intCast(failures.items.len),
            .out = out,
            .tty = tty,
            .failures = null, // one round of retry only
            .running = running,
        };

        running.completed = &retry_seq;
        running.total = @intCast(failures.items.len);
        defer {
            running.completed = null;
            running.total = 0;
        }
        const zrt = rt.underlying();
        var retry_handles: std.ArrayList(zio.JoinHandle(void)) = .empty;
        defer retry_handles.deinit(alloc);
        for (failures.items, 0..) |t, i| {
            const h = try zrt.spawnWith(.{ .spawn_loc = @src() }, runOne, .{ t, &retry_ctx, @as(u64, i) });
            try retry_handles.append(alloc, h);
        }
        for (retry_handles.items) |*h| h.join();
    }

    // Bench round: serial, auto-tuned iteration count. Only runs when
    // there are BENCH:-prefixed tests in the kept set.
    if (bench_tests.items.len == 0) return;

    print_mu.lockUncancelable();
    out.writeAll("\n") catch {};
    writeColored(out, tty, .bright_yellow, "Running {d} benchmark(s)...\n", .{bench_tests.items.len});
    out.flush() catch {};
    print_mu.unlock();

    // Sort benchmarks alphabetically by full name. Sequential dispatch
    // means execution order = sort order, so output groups related
    // variants together rather than declaration order.
    std.mem.sort(std.builtin.TestFn, bench_tests.items, {}, struct {
        fn lt(_: void, a: std.builtin.TestFn, b_: std.builtin.TestFn) bool {
            return std.mem.lessThan(u8, a.name, b_.name);
        }
    }.lt);

    var bench_watermark: utils.Watermark = .{};
    bench_watermark.advance(1); // permits=1 for bench round
    var bench_completed_count: std.atomic.Value(u64) = .{ .raw = 0 };
    var bench_seq: std.atomic.Value(u32) = .{ .raw = 0 };
    var bench_ctx = SharedCtx{
        .watermark = &bench_watermark,
        .completed_count = &bench_completed_count,
        .permits = 1,
        .print_mu = &print_mu,
        .counters = bench_counters,
        .seq = &bench_seq,
        .total = @intCast(bench_tests.items.len),
        .out = out,
        .tty = tty,
        .failures = null,
        .running = running,
    };

    const zrt = rt.underlying();
    var bench_handles: std.ArrayList(zio.JoinHandle(void)) = .empty;
    defer bench_handles.deinit(alloc);
    for (bench_tests.items, 0..) |t, i| {
        const h = try zrt.spawnWith(.{ .spawn_loc = @src() }, runBench, .{ t, &bench_ctx, @as(u64, i) });
        try bench_handles.append(alloc, h);
    }
    for (bench_handles.items) |*h| h.join();
}

// ── Phase B: fiber.* tests on a fiber.Runtime ────────────────────────
//
// `fiber.*` tests run as fibers on a dedicated fiber.Runtime — disjoint
// from zio.  No cross-runtime sync: Phase B runs serially after Phase A
// drains, so the output writer / counters have a single owner at a
// time.  fiber tests use `fiber.runtime()` to access the shared
// runtime + `Group.wait()` / `JoinHandle.join()` for sub-fiber sync.

const FiberCtx = struct {
    // Counters target for the current round. Parallel round writes to
    // `counters`; serial retry round swaps in `retry_counters`.
    counters: *Counters,
    retry_counters: *Counters,
    // Filled by the parallel round. The serial round drains it.
    failures: *std.ArrayList(std.builtin.TestFn),
    failures_mu: fiber.Mutex = .{},
    // true while the serial retry round is active.
    is_retry: bool = false,
    print_mu: fiber.Mutex = .{},
    seq: std.atomic.Value(u32) = .{ .raw = 0 },
    total: u32 = 0,
    out: *std.Io.Writer,
    tty: Tty,
    args: Args,
    /// Watchdog's shared running-set so Phase B fiber tests show up
    /// in the `RUNNING ...s: <name>` lines of stuck-state dumps. Same
    /// pointer as Phase A's `SharedCtx.running`.
    running: *RunningSet,
};

fn fiberDriver(fctx: *FiberCtx) void {
    // This runs as the bootstrap fiber. egres2's runtime has no live-fiber
    // counting — it runs until `stop()` — so stop it once the driver has
    // run every fiber test (any return path), which lets `fiber_rt.run()`
    // return on the main thread.
    defer fiber.stop();

    var kept: std.ArrayList(std.builtin.TestFn) = .empty;
    defer kept.deinit(std.heap.page_allocator);

    for (builtin.test_functions) |t| {
        if (!std.mem.startsWith(u8, t.name, "fiber.")) continue;
        if (std.mem.startsWith(u8, t.name, ".test_root.")) continue;
        const user_name = if (std.mem.indexOf(u8, t.name, ".test.")) |p|
            t.name[p + ".test.".len ..]
        else
            t.name;
        const is_big = std.mem.startsWith(u8, user_name, "BIG: ");
        const is_bench = std.mem.startsWith(u8, user_name, "BENCH: ");
        const is_hang = std.mem.startsWith(u8, user_name, "HANG: ");
        if (fctx.args.filter) |f| if (std.mem.indexOf(u8, t.name, f) == null) continue;
        if (fctx.args.exclude) |x| if (std.mem.indexOf(u8, t.name, x) != null) continue;
        if (is_bench) continue; // No fiber benchmarks today; add when needed.
        if (fctx.args.only_bench) continue;
        if (is_big and !fctx.args.run_big) continue;
        if (is_hang and !fctx.args.run_hang) continue;
        kept.append(std.heap.page_allocator, t) catch unreachable;
    }

    fctx.total = @intCast(kept.items.len);
    if (kept.items.len == 0) return;

    // ── Parallel round ────────────────────────────────────────────
    // Worker-pool over a channel: spawn `parallel` worker fibers,
    // each loops `ch.receive()` → run → repeat until the channel
    // closes. Avoids the old "spawn N concurrent fibers" pattern
    // where every test's body races simultaneously — that pegged
    // timer + dispatch latency past tight test budgets (Deadlines,
    // io.sleep, coarse_clock all started flaking under it).
    // Bounded concurrency means at most `parallel` tests are mid-
    // execution at any moment, matching how the user-facing
    // `--parallel` knob is meant to behave.
    {
        const num_workers: usize = @max(1, fctx.args.parallel);
        var ch: fiber.Channel(std.builtin.TestFn) = fiber.Channel(std.builtin.TestFn).initAlloc(std.heap.page_allocator, num_workers) catch unreachable;
        defer ch.deinit();

        var g: fiber.Group(void) = .init(std.heap.page_allocator);
        defer g.deinit();
        g.ensureTotalCapacity(num_workers) catch unreachable;
        for (0..num_workers) |_| g.spawn(.{}, fiberTestWorker, .{ &ch, fctx }) catch unreachable;

        // Producer side: feed every test in. `send` blocks when the
        // channel buffer fills (capacity == num_workers), giving us
        // natural backpressure — no point queueing more than the
        // workers can grab.
        for (kept.items) |t| ch.send(t) catch break;
        ch.close(.graceful);

        g.wait();
    }

    // ── Serial retry round ────────────────────────────────────────
    // Mirrors zioDriver's retry path: re-run any parallel-round
    // failures one at a time, writing into retry_counters so flaky
    // tests (failed parallel, passed serial) don't count as failures
    // in the final summary.
    if (fctx.failures.items.len == 0) return;

    fctx.print_mu.lock();
    fctx.out.writeAll("\n") catch {};
    writeColored(fctx.out, fctx.tty, .bright_yellow, "Retrying {d} failed fiber test(s) serially...\n", .{fctx.failures.items.len});
    fctx.out.flush() catch {};
    fctx.print_mu.unlock();

    fctx.is_retry = true;
    fctx.total = @intCast(fctx.failures.items.len);
    fctx.seq.store(0, .seq_cst);

    for (fctx.failures.items) |t| {
        const h = fiber.spawn(.{}, runFiberOne, .{ t, fctx }) catch unreachable;
        h.join();
    }
}

fn fiberTestWorker(ch: *fiber.Channel(std.builtin.TestFn), fctx: *FiberCtx) void {
    while (true) {
        const t = ch.receive() catch |err| switch (err) {
            error.ChannelClosed => return,
        };
        runFiberOne(t, fctx);
    }
}

fn runFiberOne(t: std.builtin.TestFn, fctx: *FiberCtx) void {
    fctx.running.add(t.name);
    defer {
        fctx.running.remove(t.name);
        if (thread_watchdog) |w| w.touch();
    }

    // Per-test log capture: any `std.log.*` emitted by this test
    // body goes into `fiber_log.bytes` via `capturingLogFn`. We
    // print captured lines indented under the status line below so
    // the test's status appears first, then its logs — instead of
    // logs racing out before the status line.
    var fiber_log: FiberLog = .{};
    defer fiber_log.bytes.deinit(std.heap.page_allocator);
    registerFiberLog(&fiber_log);
    defer unregisterFiberLog();

    const t0 = nowNs();
    const result: ?anyerror = if (t.func()) |_| null else |e| e;
    const elapsed_ms = @divTrunc(nowNs() - t0, std.time.ns_per_ms);

    const target_counters = if (fctx.is_retry) fctx.retry_counters else fctx.counters;
    const seq = fctx.seq.fetchAdd(1, .seq_cst) + 1;

    fctx.print_mu.lock();
    defer fctx.print_mu.unlock();

    var prefix_buf: [32]u8 = undefined;
    const prefix_str = std.fmt.bufPrint(&prefix_buf, "[{d}/{d}]", .{ seq, fctx.total }) catch unreachable;
    writeColored(fctx.out, fctx.tty, .dim, "{s}", .{prefix_str});
    const prefix_pad = if (prefix_width > prefix_str.len) prefix_width - prefix_str.len else 1;
    for (0..prefix_pad) |_| fctx.out.writeByte(' ') catch {};

    writeDisplayName(fctx.out, fctx.tty, t.name);
    fctx.out.print(" ... ", .{}) catch {};

    if (result) |err| switch (err) {
        error.SkipZigTest => {
            _ = target_counters.skipped.fetchAdd(1, .seq_cst);
            writeColored(fctx.out, fctx.tty, .bright_yellow, "SKIP\n", .{});
        },
        else => {
            _ = target_counters.failed.fetchAdd(1, .seq_cst);
            writeColored(fctx.out, fctx.tty, .bright_red, "FAIL: {s}", .{@errorName(err)});
            writeColored(fctx.out, fctx.tty, .dim, " ({d}ms)\n", .{elapsed_ms});
            // Parallel-round failures get queued for the serial
            // retry; failures during retry are final.
            if (!fctx.is_retry) {
                fctx.failures_mu.lock();
                defer fctx.failures_mu.unlock();
                fctx.failures.append(std.heap.page_allocator, t) catch {};
            }
        },
    } else {
        _ = target_counters.passed.fetchAdd(1, .seq_cst);
        writeColored(fctx.out, fctx.tty, .bright_green, "OK", .{});
        writeColored(fctx.out, fctx.tty, .dim, " ({d}ms)\n", .{elapsed_ms});
    }

    // Captured log lines indented under the status line, matching
    // the Phase A `runOne` formatting.
    if (fiber_log.bytes.items.len > 0) {
        var lines = std.mem.splitScalar(u8, fiber_log.bytes.items, '\n');
        while (lines.next()) |line| {
            if (line.len == 0) continue;
            writeColored(fctx.out, fctx.tty, .dim, "  | {s}\n", .{line});
        }
    }
    if (fiber_log.overflowed) {
        writeColored(fctx.out, fctx.tty, .dim, "  | (output truncated at {d} bytes)\n", .{FiberLog.max_size});
    }
    fctx.out.flush() catch {};
}

pub fn main(init: std.process.Init) !void {
    context.initGlobal(init);
    var arena: std.heap.ArenaAllocator = .init(std.heap.page_allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    // std.testing.io_instance is `undefined` by default — any test calling
    // `std.testing.io` (via tmpDir, etc.) hangs/UBs until it's initialized.
    // The stock Zig test_runner does this per-test; we use one shared
    // instance for the whole run.
    std.testing.io_instance = .init(std.heap.page_allocator, .{
        .argv0 = .init(init.minimal.args),
        .environ = init.minimal.environ,
    });
    defer std.testing.io_instance.deinit();

    var args = try parseArgs(alloc);
    args.parallel = resolveParallel(args.parallel);

    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.Io.File.stderr().writer(ioOf(), &stderr_buf);
    const out = &stderr_w.interface;
    const tty = Tty.detect();

    log_map = std.AutoHashMap(usize, *FiberLog).init(std.heap.page_allocator);
    defer if (log_map) |*m| m.deinit();

    bench.initRegistry(std.heap.page_allocator);
    defer bench.deinitRegistry();

    // std.Thread watchdog: runs outside the zio runtime so it stays alive
    // (and dumping) even when the runtime itself is mid-deinit. Defaults
    // are aggressive (20s dump-and-tick, 300s kill) — most tests finish
    // in <1s so a 20s gap already means something's stuck. Override per
    // run with --wd-stuck-tick-s / --wd-kill-s.
    var twatch = ThreadWatchdog.init(args.wd_stuck_tick_s, args.wd_kill_s);
    defer twatch.deinit();
    thread_watchdog = &twatch;
    try twatch.start();
    defer {
        twatch.stop();
        thread_watchdog = null;
    }

    // Use the egres single-runtime seam so deeper code paths
    // (coarse.Clock, raft loops, etc.) that call `runtime.spawnOn`
    // share the same runtime as the test runner.
    try rt.init(std.heap.page_allocator, .{ .total_threads = args.parallel });
    defer rt.deinit();

    // Bring up the Phase B fiber runtime now so `fiber.runtime()`
    // works throughout — including from the watchdog thread during a
    // Phase A wedge. `init` only allocates state and installs the
    // process-wide singleton; worker threads aren't spawned until
    // `fiber_rt.run()` is called in Phase B.
    //
    // Runtime worker count == all available cores. The `--parallel`
    // knob bounds *test dispatch* concurrency (see fiberDriver), not
    // the worker pool. Giving the runtime full hardware concurrency
    // lets a single test's spawned child fibers (e.g. the io.sleep
    // test's 100 sleepers) actually spread across cores, while
    // bounding how many test bodies are mid-execution keeps timer +
    // wake latency from getting squeezed.
    const fiber_workers: usize = std.Thread.getCpuCount() catch @as(usize, args.parallel);
    var fiber_rt: fiber.Runtime = undefined;
    try fiber_rt.init(std.heap.page_allocator, .{ .num_workers = fiber_workers });
    defer fiber_rt.deinit();

    var counters: Counters = .{};
    var retry_counters: Counters = .{};
    var bench_counters: Counters = .{};
    const t0_all = nowNs();

    var handle = try rt.spawnOn(.background, .shared, driver, .{ args, &counters, &retry_counters, &bench_counters, alloc, out, tty });
    try handle.join();
    // Drain the coarse.Clock tick fiber, if any test started it.
    // Without this, `rt.deinit()` asserts task_count == 0 fails.
    utils.coarse.Clock.stop();

    // ── Phase B ─────────────────────────────────────────────────────
    // fiber.* tests run on the dedicated `fiber.Runtime` initialized
    // up at main() entry. Disjoint from zio: the fiber phase runs
    // after the zio phase has drained so the output writer / counters
    // have a single owner.
    {
        var fiber_failures: std.ArrayList(std.builtin.TestFn) = .empty;
        defer fiber_failures.deinit(std.heap.page_allocator);

        var fctx: FiberCtx = .{
            .counters = &counters,
            .retry_counters = &retry_counters,
            .failures = &fiber_failures,
            .out = out,
            .tty = tty,
            .args = args,
            .running = &twatch.running,
        };

        _ = try fiber.spawn(.{}, fiberDriver, .{&fctx});
        fiber_rt.run();
    }

    const total_ms = @divTrunc(nowNs() - t0_all, std.time.ns_per_ms);
    const main_passed = counters.passed.load(.seq_cst);
    const main_failed = counters.failed.load(.seq_cst);
    const main_skipped = counters.skipped.load(.seq_cst);
    const retry_passed = retry_counters.passed.load(.seq_cst);
    const retry_failed = retry_counters.failed.load(.seq_cst);
    const bench_passed = bench_counters.passed.load(.seq_cst);
    const bench_failed = bench_counters.failed.load(.seq_cst);
    const bench_skipped = bench_counters.skipped.load(.seq_cst);

    // Effective totals across rounds. Retry: if a test failed parallel but
    // passed serial, count it as passed and tag it flaky. Bench: passed/
    // failed/skipped counts roll into the totals so the summary line is
    // accurate when only benchmarks ran.
    const had_retry = retry_passed + retry_failed > 0;
    const eff_passed = main_passed + retry_passed + bench_passed;
    const main_eff_failed = if (had_retry) retry_failed else main_failed;
    const eff_failed = main_eff_failed + bench_failed;
    const eff_skipped = main_skipped + bench_skipped;
    const flaky = retry_passed;

    out.writeAll("\n") catch {};
    writeColored(out, tty, .bright_green, "{d} passed", .{eff_passed});
    out.writeAll(", ") catch {};
    if (eff_failed > 0) {
        writeColored(out, tty, .bright_red, "{d} failed", .{eff_failed});
    } else {
        out.print("{d} failed", .{eff_failed}) catch {};
    }
    out.writeAll(", ") catch {};
    if (eff_skipped > 0) {
        writeColored(out, tty, .bright_yellow, "{d} skipped", .{eff_skipped});
    } else {
        out.print("{d} skipped", .{eff_skipped}) catch {};
    }
    if (flaky > 0) {
        out.writeAll(" (") catch {};
        writeColored(out, tty, .bright_yellow, "{d} flaky", .{flaky});
        out.writeAll(")") catch {};
    }
    const total_s = @as(f64, @floatFromInt(total_ms)) / 1000.0;
    writeColored(out, tty, .dim, " in {d:.1}s (parallel={d})\n", .{ total_s, args.parallel });
    out.flush() catch {};

    if (eff_failed > 0) std.process.exit(1);
}
