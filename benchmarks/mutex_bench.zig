// Single-fiber, uncontended micro-benchmarks comparing the TrackedMutex /
// TrackedRwLock wrappers against the underlying zio primitives.
//
// We're measuring the PER-CALL overhead of the tracking layer: one
// atomic counter bump on acquire and release, a pointer write to
// `holder_src` / `last_held_since_ms`, and the `std.time.milliTimestamp()`
// VDSO call. Contention costs aren't measured here — those are dominated
// by the wait queue under the hood, which is the same primitive for both
// flavours. The ratio between the two numbers below is what tells you
// whether tracking is "free" enough to leave on by default.
//
// Run via `zig build -Drelease bench` for accurate numbers (Debug
// bounds checks dominate the loop at this scale).

const std = @import("std");
const zio = @import("zio");
const utils = @import("utils");
const fiber = @import("fiber");
const bench = utils.bench;

fn benchTrackedMutex(b: *bench.Bench) !void {
    // Start the ticker so `recordAcquire` reads the cached clock
    // rather than falling back to std.time.milliTimestamp. Without
    // this, the benchmark would measure the fallback path and falsely
    // suggest tracking is more expensive than it really is in prod.
    var mu: utils.TrackedMutex = .{};
    defer mu.deinit();
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        mu.lockUncancelable(@src());
        mu.unlock();
    }
}

fn benchZioMutex(b: *bench.Bench) !void {
    var mu: zio.Mutex = .init;
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        mu.lockUncancelable();
        mu.unlock();
    }
}

fn benchTrackedRwLockShared(b: *bench.Bench) !void {
    var rw: utils.TrackedRwLock = .{};
    defer rw.deinit();
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        rw.lockSharedUncancelable(@src());
        rw.unlockShared();
    }
}

fn benchZioRwLockShared(b: *bench.Bench) !void {
    var rw: zio.RwLock = .init;
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        rw.lockSharedUncancelable();
        rw.unlockShared();
    }
}

fn benchMilliTimestamp(b: *bench.Bench) !void {
    b.resetTimer();
    var sink: i64 = 0;
    var i: u64 = 0;
    const io = utils.context.g().io;
    while (i < b.n) : (i += 1) sink +%= std.Io.Timestamp.now(io, .real).toMilliseconds();
    std.mem.doNotOptimizeAway(sink);
}

fn benchCoarseClock(b: *bench.Bench) !void {
    b.resetTimer();
    var sink: i64 = 0;
    var i: u64 = 0;
    while (i < b.n) : (i += 1) sink +%= fiber.coarseMs();
    std.mem.doNotOptimizeAway(sink);
}

test "BENCH: tracked-mutex lock-unlock" { try benchTrackedMutex(bench.current()); }
test "BENCH: zio-mutex lock-unlock" { try benchZioMutex(bench.current()); }
test "BENCH: tracked-rwlock shared-lock-unlock" { try benchTrackedRwLockShared(bench.current()); }
test "BENCH: zio-rwlock shared-lock-unlock" { try benchZioRwLockShared(bench.current()); }
test "BENCH: clock std.time.milliTimestamp" { try benchMilliTimestamp(bench.current()); }
test "BENCH: clock fiber.coarseMs" { try benchCoarseClock(bench.current()); }
