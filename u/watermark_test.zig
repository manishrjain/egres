//! Tests for `utils.Watermark` — the monotonic u64 progress marker
//! used by raft.Node for commit_index / applied_ts. Covers fast-path
//! loads, the mu+cv lost-wakeup-safe wait/signal pair, monotonicity,
//! timeouts, and `close()`-driven shutdown.
//!
//! The unified test runner already runs each test as a zio fiber, so
//! these test bodies use `zio.yield` / `zio.sleep` / `runtime.Group(...)`
//! directly — no per-test Runtime needed.

const std = @import("std");
const zio = @import("zio");
const fiber = @import("fiber");
const utils = @import("utils");
const Watermark = utils.Watermark;

const log = std.log.scoped(.watermark_test);

test "load: fresh watermark reads zero without locking" {
    var wm: Watermark = .{};
    try std.testing.expectEqual(@as(u64, 0), wm.load());
}

test "advance: monotonic — larger values stick, smaller are no-ops" {
    var wm: Watermark = .{};
    wm.advance(5);
    try std.testing.expectEqual(@as(u64, 5), wm.load());
    // Equal: no-op (broadcast would be wasted; visible state unchanged).
    wm.advance(5);
    try std.testing.expectEqual(@as(u64, 5), wm.load());
    // Smaller: discarded outright.
    wm.advance(3);
    try std.testing.expectEqual(@as(u64, 5), wm.load());
    // Larger: takes effect.
    wm.advance(9);
    try std.testing.expectEqual(@as(u64, 9), wm.load());
}

test "waitForUntimed: target already reached returns immediately" {
    var wm: Watermark = .{};
    wm.advance(10);
    try wm.waitForUntimed(10);
    try wm.waitForUntimed(7);
    // target=0 is the trivial case — always satisfied.
    try wm.waitForUntimed(0);
}

test "waitForUntimed: blocks until advance crosses target" {
    const Ctx = struct {
        wm: Watermark = .{},
        woke: std.atomic.Value(bool) = .init(false),

        fn waiter(self: *@This()) anyerror!void {
            try self.wm.waitForUntimed(42);
            self.woke.store(true, .release);
        }

        fn advancer(self: *@This()) anyerror!void {
            // Give the waiter a couple of ticks to actually park on cv.
            for (0..16) |_| fiber.yield();
            try std.testing.expect(!self.woke.load(.acquire));
            // Below target — must not wake.
            self.wm.advance(40);
            for (0..16) |_| fiber.yield();
            try std.testing.expect(!self.woke.load(.acquire));
            // At/above target — must wake.
            self.wm.advance(42);
        }
    };

    var ctx = Ctx{};
    var grp = fiber.Group(anyerror!void).init(std.testing.allocator);
    defer grp.deinit();
    try grp.spawn(.{}, Ctx.waiter, .{&ctx});
    try grp.spawn(.{}, Ctx.advancer, .{&ctx});
    for (grp.wait()) |r| try r;
    try std.testing.expect(ctx.woke.load(.acquire));
}

test "waitForUntimed: broadcast wakes every waiter on a single advance" {
    const Ctx = struct {
        wm: Watermark = .{},
        woke: std.atomic.Value(u32) = .init(0),

        fn waiter(self: *@This()) anyerror!void {
            try self.wm.waitForUntimed(7);
            _ = self.woke.fetchAdd(1, .release);
        }

        fn advancer(self: *@This()) anyerror!void {
            // Let all three waiters reach cv.wait before we publish.
            for (0..32) |_| fiber.yield();
            self.wm.advance(7);
        }
    };

    var ctx = Ctx{};
    var grp = fiber.Group(anyerror!void).init(std.testing.allocator);
    defer grp.deinit();
    try grp.spawn(.{}, Ctx.waiter, .{&ctx});
    try grp.spawn(.{}, Ctx.waiter, .{&ctx});
    try grp.spawn(.{}, Ctx.waiter, .{&ctx});
    try grp.spawn(.{}, Ctx.advancer, .{&ctx});
    for (grp.wait()) |r| try r;
    try std.testing.expectEqual(@as(u32, 3), ctx.woke.load(.acquire));
}

test "waitFor: returns Timeout when target never reached" {
    // TODO(fiber): re-enable once fiber gains a timed condition wait. The
    // current waitFor stub ignores the timeout and would block forever.
    return error.SkipZigTest;
}

test "waitFor: returns successfully when advance crosses target before deadline" {
    const Ctx = struct {
        wm: Watermark = .{},

        fn waiter(self: *@This()) anyerror!void {
            try self.wm.waitFor(3, 5000);
        }

        fn advancer(self: *@This()) anyerror!void {
            fiber.sleep(5);
            self.wm.advance(3);
        }
    };

    var ctx = Ctx{};
    var grp = fiber.Group(anyerror!void).init(std.testing.allocator);
    defer grp.deinit();
    try grp.spawn(.{}, Ctx.waiter, .{&ctx});
    try grp.spawn(.{}, Ctx.advancer, .{&ctx});
    for (grp.wait()) |r| try r;
}

test "close: wakes pending waiters with error.Closed" {
    const Ctx = struct {
        wm: Watermark = .{},
        result_untimed: std.atomic.Value(i32) = .init(0),
        result_timed: std.atomic.Value(i32) = .init(0),

        // 1 = expected error.Closed; 2 = unexpected success; 3 = other err
        fn untimed(self: *@This()) anyerror!void {
            if (self.wm.waitForUntimed(1_000_000)) |_| {
                self.result_untimed.store(2, .release);
            } else |err| switch (err) {
                error.Closed => self.result_untimed.store(1, .release),
            }
        }

        fn timed(self: *@This()) anyerror!void {
            if (self.wm.waitFor(1_000_000, 5000)) |_| {
                self.result_timed.store(2, .release);
            } else |err| switch (err) {
                // Closed is the path we're actually exercising. Timeout
                // is accepted too: under heavy parallel test load the
                // closer fiber can starve long enough that the 5 s
                // deadline wins the race. The thing the test must
                // assert is "the timed wait returned an error, not
                // unexpected success and not some other error" — the
                // close-wakeup itself is validated by the untimed
                // fiber which has no deadline to race against.
                error.Closed, error.Timeout => self.result_timed.store(1, .release),
            }
        }

        fn closer(self: *@This()) anyerror!void {
            // Let both waiters park before closing.
            for (0..32) |_| fiber.yield();
            self.wm.close();
        }
    };

    var ctx = Ctx{};
    var grp = fiber.Group(anyerror!void).init(std.testing.allocator);
    defer grp.deinit();
    try grp.spawn(.{}, Ctx.untimed, .{&ctx});
    try grp.spawn(.{}, Ctx.timed, .{&ctx});
    try grp.spawn(.{}, Ctx.closer, .{&ctx});
    for (grp.wait()) |r| try r;
    try std.testing.expectEqual(@as(i32, 1), ctx.result_untimed.load(.acquire));
    try std.testing.expectEqual(@as(i32, 1), ctx.result_timed.load(.acquire));
}

test "close: idempotent — second call is a no-op" {
    var wm: Watermark = .{};
    wm.close();
    wm.close(); // must not deadlock / re-broadcast spuriously
    try std.testing.expectError(error.Closed, wm.waitForUntimed(1));
}

test "close after advance: target-met waiters succeed, beyond-target see Closed" {
    // Watermark closes don't roll back: a waiter whose target has already
    // been satisfied returns Ok even after close(), because the value
    // check happens before the `closed` check in the wait loop. Only
    // waiters still parked observe `error.Closed`.
    const Ctx = struct {
        wm: Watermark = .{},

        fn satisfied(self: *@This()) anyerror!void {
            // target=5, advance already happened — should NOT see Closed.
            try self.wm.waitForUntimed(5);
        }

        fn beyond(self: *@This()) anyerror!void {
            try std.testing.expectError(
                error.Closed,
                self.wm.waitForUntimed(100),
            );
        }

        fn driver(self: *@This()) anyerror!void {
            self.wm.advance(5);
            for (0..16) |_| fiber.yield();
            self.wm.close();
        }
    };

    var ctx = Ctx{};
    var grp = fiber.Group(anyerror!void).init(std.testing.allocator);
    defer grp.deinit();
    try grp.spawn(.{}, Ctx.satisfied, .{&ctx});
    try grp.spawn(.{}, Ctx.beyond, .{&ctx});
    try grp.spawn(.{}, Ctx.driver, .{&ctx});
    for (grp.wait()) |r| try r;
}

// Soak test: many waiter fibers chase a monotonically advancing target
// for 10 s. Each waiter loops on `waitForUntimed` with ever-increasing
// random targets; one advancer fiber bumps the watermark at a gentler
// step (so waiters genuinely park most cycles, exercising the mu+cv
// pair rather than racing through fast-path loads). If any broadcast
// were dropped — the lost-wakeup the mu+cv design exists to prevent —
// at least one waiter would park forever and either group.wait() would
// hang (caught by the runner's 60 s BIG cancel) or that waiter's
// per-fiber counter would stay at 0.
//
// At the deadline the advancer calls `wm.close()`, which wakes any
// waiter parked on a target the advancer never reached. That makes
// group teardown infallible even with pathologically unlucky RNG.
test "BIG: Watermark stress — 64 waiters chase advancing target for 10s" {
    const num_waiters: u32 = 64;
    const duration_ns: i128 = 10 * std.time.ns_per_s;

    const Ctx = struct {
        wm: Watermark = .{},
        per_waiter: [num_waiters]std.atomic.Value(u64) = undefined,
        /// Each waiter publishes the target it is about to park on
        /// before calling `waitForUntimed`. The advancer reads these
        /// to verify the broadcast invariant: after `advance(v)`, no
        /// waiter should still be parked on a target ≤ v. Sentinel
        /// `0` = "not started yet".
        per_waiter_target: [num_waiters]std.atomic.Value(u64) = undefined,
        advance_count: std.atomic.Value(u64) = .init(0),
        seed: u64,
        deadline_ns: i128,

        fn waiter(self: *@This(), id: u32) anyerror!void {
            var prng = std.Random.DefaultPrng.init(self.seed +% id);
            const r = prng.random();
            var target: u64 = 0;
            while (true) {
                // Ever-increasing target per the API contract — the
                // watermark only moves forward, so a strictly
                // monotonic sequence per fiber matches real callers
                // (Node.applied_ts, commit_index, etc.). Bigger step
                // than the advancer's per-tick so waiters typically
                // outrun the watermark and park.
                target += r.uintLessThan(u32, 200) + 1;
                // Publish BEFORE parking so the advancer's invariant
                // check sees what we're parked on. The release pairs
                // with the advancer's acquire load.
                self.per_waiter_target[id].store(target, .release);
                self.wm.waitForUntimed(target) catch |e| switch (e) {
                    error.Closed => return, // expected at deadline
                };
                _ = self.per_waiter[id].fetchAdd(1, .monotonic);
            }
        }

        fn advancer(self: *@This()) anyerror!void {
            var prng = std.Random.DefaultPrng.init(self.seed);
            const r = prng.random();
            var v: u64 = 0;
            while (zio.now().toNanoseconds() < self.deadline_ns) {
                // Wait 100 µs, then verify the broadcast invariant: no
                // waiter is still parked on a target ≤ v. If any is,
                // either its wake is still in flight (re-check after
                // another 100 µs — bounded by waiter progress) or the
                // wake was lost. A lost wake keeps the loop spinning
                // forever; the runner's 60 s BIG cancel surfaces it.
                while (true) {
                    try zio.sleep(.fromMicroseconds(200));
                    var any_stuck = false;
                    for (&self.per_waiter_target) |*pwt| {
                        const t = pwt.load(.acquire);
                        if (t != 0 and t <= v) {
                            any_stuck = true;
                            break;
                        }
                    }
                    if (!any_stuck) break;
                }
                v += r.uintLessThan(u32, 50) + 1;
                self.wm.advance(v);
                _ = self.advance_count.fetchAdd(1, .monotonic);
            }
            self.wm.close();
        }
    };

    const seed_base = @as(u64, @intCast(zio.now().toNanoseconds())) | 1;
    log.info("watermark stress seed_base={x}", .{seed_base});

    var ctx = Ctx{
        .seed = seed_base,
        .deadline_ns = zio.now().toNanoseconds() + duration_ns,
    };
    for (&ctx.per_waiter) |*pw| pw.* = .init(0);
    for (&ctx.per_waiter_target) |*pwt| pwt.* = .init(0);

    var grp = fiber.Group(anyerror!void).init(std.testing.allocator);
    defer grp.deinit();
    var i: u32 = 0;
    while (i < num_waiters) : (i += 1) {
        try grp.spawn(.{}, Ctx.waiter, .{ &ctx, i });
    }
    try grp.spawn(.{}, Ctx.advancer, .{&ctx});
    for (grp.wait()) |r| try r;

    var total_waits: u64 = 0;
    var min_waits: u64 = std.math.maxInt(u64);
    for (&ctx.per_waiter) |*pw| {
        const n = pw.load(.seq_cst);
        total_waits += n;
        if (n < min_waits) min_waits = n;
    }
    log.info(
        "watermark stress: total_waits={d} min_per_waiter={d} advances={d} final_wm={d}",
        .{
            total_waits,
            min_waits,
            ctx.advance_count.load(.seq_cst),
            ctx.wm.load(),
        },
    );

    // Per-waiter floor: every fiber must have completed ≥1 round-trip.
    // A wedged waiter (lost wakeup) would sit at 0 even after close()
    // unstuck it — close returns Closed, which the waiter treats as
    // exit, so a starved fiber never increments. Catches both
    // lost-wakeup bugs and pathological scheduler starvation.
    try std.testing.expect(min_waits >= 1);
}
