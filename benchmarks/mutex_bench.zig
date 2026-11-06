// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// Single-fiber, uncontended micro-benchmarks comparing the TRACKED vs
// UNTRACKED fiber Mutex / RWMutex modes — `fiber.Mutex(.tracked)` vs
// `fiber.Mutex(.untracked)`.
//
// We're measuring the PER-CALL overhead of holder tracking: a tracked acquire
// calls `record()`, which writes the call site (`@src` pointer) + a
// coarse-clock timestamp into the lock's embedded node, after a one-time lazy
// registration (a single atomic-load check on every acquire thereafter). The
// untracked mode compiles all of that out via comptime, leaving the bare CAS +
// spinlock fast path. Contention costs aren't measured here — those are
// dominated by the shared wait-queue machinery, which is identical in both
// modes. The ratio between the two numbers tells you whether tracking is
// "free" enough to leave on by default.
//
// Run via `zig build -Drelease bench` for accurate numbers (Debug bounds
// checks dominate the loop at this scale).

const std = @import("std");
const utils = @import("utils");
const fiber = @import("zfiber");
const bench = fiber.bench;

fn benchTrackedMutex(b: *bench.Bench) !void {
    // `defer deinit()` unregisters the stack lock's node before the frame
    // dies — a tracked lock that registered without deinit would dangle.
    var mu: fiber.Mutex(.tracked) = .{};
    defer mu.deinit();
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        mu.lock(@src());
        mu.unlock();
    }
}

fn benchUntrackedMutex(b: *bench.Bench) !void {
    // `@src()` is still passed (uniform API) but discarded at comptime; no
    // record, no registry, no deinit.
    var mu: fiber.Mutex(.untracked) = .{};
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        mu.lock(@src());
        mu.unlock();
    }
}

fn benchTrackedRwLockShared(b: *bench.Bench) !void {
    var rw: fiber.RWMutex(.tracked) = .{};
    defer rw.deinit();
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        rw.lockShared(@src());
        rw.unlockShared();
    }
}

fn benchUntrackedRwLockShared(b: *bench.Bench) !void {
    var rw: fiber.RWMutex(.untracked) = .{};
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        rw.lockShared(@src());
        rw.unlockShared();
    }
}

// ── Contended: N fibers, one per distinct worker, hammer ONE shared lock ──
// The single-fiber benches above measure the uncontended fast path. These
// measure the lock under real cross-worker contention: each fiber is pinned to
// its own worker (high priority so it stays put rather than piling on the
// spawner's ring) and runs b.n/N acquire-release pairs on the SAME lock.
// ns/op is wall-time per op across the N-way contention.
//
// For RWMutex the first `writers` fibers take the exclusive lock, the rest
// the shared one. All-shared is the reader-scaling case: readers exclude
// nobody, so the cost is purely the lock word bouncing between cores. The
// mixed case exercises the slow path: readers backing off, the writer
// parking on the reader count and being woken by the last reader out.
fn Contended(comptime LockT: type) type {
    return struct {
        lock: *LockT,
        iters: u64,
        shared: bool,
        fn run(self: *@This()) void {
            var i: u64 = 0;
            while (i < self.iters) : (i += 1) {
                if (@hasDecl(LockT, "lockShared") and self.shared) {
                    self.lock.lockShared(@src());
                    self.lock.unlockShared();
                } else {
                    self.lock.lock(@src());
                    self.lock.unlock();
                }
            }
        }
    };
}

fn benchContended(b: *bench.Bench, comptime LockT: type, comptime fibers: usize, comptime writers: usize) !void {
    comptime std.debug.assert(writers <= fibers);
    const Ctx = Contended(LockT);
    var lock: LockT = .{};
    defer lock.deinit(); // no-op when untracked; unregisters when tracked
    const per = @max(b.n / fibers, 1);
    var ctxs: [fibers]Ctx = undefined;
    for (&ctxs, 0..) |*c, idx| c.* = .{ .lock = &lock, .iters = per, .shared = idx >= writers };

    b.resetTimer();
    var group: fiber.Group(void) = .init(b.alloc);
    defer group.deinit();
    try group.ensureTotalCapacity(fibers);
    for (0..fibers) |w| {
        try group.spawn(
            .{ .priority = .high, .worker_id = @intCast(w) },
            Ctx.run,
            .{&ctxs[w]},
        );
    }
    group.wait();
}

fn benchContendedMutexTracked(b: *bench.Bench) !void {
    try benchContended(b, fiber.Mutex(.tracked), 4, 4);
}
fn benchContendedMutexUntracked(b: *bench.Bench) !void {
    try benchContended(b, fiber.Mutex(.untracked), 4, 4);
}
fn benchContendedRwSharedTracked(b: *bench.Bench) !void {
    try benchContended(b, fiber.RWMutex(.tracked), 4, 0);
}
fn benchContendedRwSharedUntracked(b: *bench.Bench) !void {
    try benchContended(b, fiber.RWMutex(.untracked), 4, 0);
}
fn benchContendedRwShared16(b: *bench.Bench) !void {
    // Needs a runtime with at least 16 workers; the bench runtime sizes to
    // the CPU count.
    if (fiber.runtime().workers.len < 16) return error.SkipZigTest;
    try benchContended(b, fiber.RWMutex(.untracked), 16, 0);
}
fn benchContendedRwMixed(b: *bench.Bench) !void {
    try benchContended(b, fiber.RWMutex(.untracked), 4, 1);
}

// ── The idle-bit gate's economics: the check it adds vs the syscall it skips ──
fn benchAsyncNotify(b: *bench.Bench) !void {
    // The exact primitive `wake_async.notify()` uses — an `xev.Async` (an
    // eventfd on Linux); notify() is one 8-byte write syscall. Standalone here
    // (no loop waiting on it), so we measure the write in isolation, no wakeup.
    var async_h = try fiber.xev.Async.init();
    defer async_h.deinit();
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) async_h.notify() catch {};
}

fn benchIdleBitCheck(b: *bench.Bench) !void {
    // The gate's added cost: one acquire-load of the lane idle word + a bit
    // test. Single-threaded + cache-hot here, so this is the floor — the real
    // gate load can miss when the home worker's markIdle/clearIdle dirties the
    // line on another core.
    const idle = &fiber.runtime().idle[0];
    b.resetTimer();
    var sink: u64 = 0;
    var i: u64 = 0;
    while (i < b.n) : (i += 1) sink +%= @intFromBool(idle.isIdle(0));
    std.mem.doNotOptimizeAway(sink);
}

fn benchMilliTimestamp(b: *bench.Bench) !void {
    b.resetTimer();
    var sink: i64 = 0;
    var i: u64 = 0;
    while (i < b.n) : (i += 1) sink +%= utils.timestampNow(.realtime, .millis);
    std.mem.doNotOptimizeAway(sink);
}

fn benchCoarseClock(b: *bench.Bench) !void {
    b.resetTimer();
    var sink: i64 = 0;
    var i: u64 = 0;
    while (i < b.n) : (i += 1) sink +%= utils.coarseMs();
    std.mem.doNotOptimizeAway(sink);
}

test "BENCH: tracked-mutex lock-unlock" {
    try benchTrackedMutex(bench.current());
}
test "BENCH: untracked-mutex lock-unlock" {
    try benchUntrackedMutex(bench.current());
}
test "BENCH: tracked-rwlock shared-lock-unlock" {
    try benchTrackedRwLockShared(bench.current());
}
test "BENCH: untracked-rwlock shared-lock-unlock" {
    try benchUntrackedRwLockShared(bench.current());
}
test "BENCH: contended mutex tracked (4 workers)" {
    try benchContendedMutexTracked(bench.current());
}
test "BENCH: contended mutex untracked (4 workers)" {
    try benchContendedMutexUntracked(bench.current());
}
test "BENCH: contended rwlock-shared tracked (4 workers)" {
    try benchContendedRwSharedTracked(bench.current());
}
test "BENCH: contended rwlock-shared untracked (4 workers)" {
    try benchContendedRwSharedUntracked(bench.current());
}
test "BENCH: contended rwlock-shared untracked (16 workers)" {
    try benchContendedRwShared16(bench.current());
}
test "BENCH: contended rwlock 1 writer + 3 readers untracked (4 workers)" {
    try benchContendedRwMixed(bench.current());
}
test "BENCH: wake xev.Async.notify (syscall)" {
    try benchAsyncNotify(bench.current());
}
test "BENCH: wake idle-bit check" {
    try benchIdleBitCheck(bench.current());
}
test "BENCH: clock std.time.milliTimestamp" {
    try benchMilliTimestamp(bench.current());
}
test "BENCH: clock utils.coarseMs" {
    try benchCoarseClock(bench.current());
}
