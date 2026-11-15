// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");

/// Direct vDSO monotonic ms — accurate elapsed measurement without the
/// coarse clock's loop-tick lag.
fn nowMs() i64 {
    return utils.timestampNow(.monotonic, .millis);
}

test "Watermark: waitForUntimed returns when value crosses target" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    var observed: u64 = 0;

    const Waiter = struct {
        fn run(w: *fiber.Watermark, out: *u64) void {
            w.waitForUntimed(5) catch return;
            out.* = w.load();
        }
    };
    const Bumper = struct {
        fn run(w: *fiber.Watermark) void {
            w.advance(5);
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.spawn(.{}, Waiter.run, .{ &wm, &observed });
    try g.spawn(.{}, Bumper.run, .{&wm});
    g.wait();

    try std.testing.expect(observed >= 5);
}

test "Watermark: advance wakes only waiters whose target is met" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    var low_ok = false;
    var mid_ok = false;
    var high_ok = false;
    var high_err_closed = false;

    const Low = struct {
        fn run(w: *fiber.Watermark, out: *bool) void {
            w.waitForUntimed(3) catch return;
            out.* = true;
        }
    };
    const Mid = struct {
        fn run(w: *fiber.Watermark, out: *bool) void {
            w.waitForUntimed(5) catch return;
            out.* = true;
        }
    };
    const High = struct {
        fn run(w: *fiber.Watermark, ok: *bool, closed: *bool) void {
            w.waitForUntimed(20) catch |err| {
                if (err == error.Closed) closed.* = true;
                return;
            };
            ok.* = true;
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.spawn(.{}, Low.run, .{ &wm, &low_ok });
    try g.spawn(.{}, Mid.run, .{ &wm, &mid_ok });
    try g.spawn(.{}, High.run, .{ &wm, &high_ok, &high_err_closed });

    // Let the waiters park first.
    fiber.yield();
    fiber.yield();
    // Cross 3 and 5 (wake Low + Mid), leave High parked.
    wm.advance(10);
    // Now close; High should wake with error.Closed.
    fiber.yield();
    wm.close();

    g.wait();

    try std.testing.expect(low_ok);
    try std.testing.expect(mid_ok);
    try std.testing.expect(!high_ok);
    try std.testing.expect(high_err_closed);
}

test "Watermark: close wakes everyone with error.Closed" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    var closed_count: std.atomic.Value(u32) = .init(0);

    const Waiter = struct {
        fn run(w: *fiber.Watermark, count: *std.atomic.Value(u32)) void {
            w.waitForUntimed(100) catch |err| switch (err) {
                error.Closed => _ = count.fetchAdd(1, .release),
                else => return,
            };
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(10);
    for (0..10) |_| try g.spawn(.{}, Waiter.run, .{ &wm, &closed_count });

    // Let all waiters park, then close.
    fiber.yield();
    fiber.yield();
    wm.close();

    g.wait();

    try std.testing.expectEqual(@as(u32, 10), closed_count.load(.acquire));
}

test "Watermark: cross-worker — many waiters across multiple advances" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    const N_WAITERS = 100;
    var completed: std.atomic.Value(u32) = .init(0);

    const Waiter = struct {
        fn run(w: *fiber.Watermark, target: u64, count: *std.atomic.Value(u32)) void {
            w.waitForUntimed(target) catch return;
            _ = count.fetchAdd(1, .release);
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N_WAITERS);

    // Targets spread 1..N_WAITERS. All land on the current worker's
    // local queue; stealing distributes them across peers.
    for (0..N_WAITERS) |i| {
        try g.spawn(
            .{},
            Waiter.run,
            .{ &wm, @as(u64, @intCast(i + 1)), &completed },
        );
    }

    // Bump the watermark in steps so multiple advance events partition
    // the waiters into batches.
    var v: u64 = 0;
    while (v < N_WAITERS) : (v += 10) {
        wm.advance(v + 10);
        fiber.yield();
    }

    g.wait();

    try std.testing.expectEqual(@as(u32, N_WAITERS), completed.load(.acquire));
}

test "Watermark: waitFor times out when target never reached" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    const Waiter = struct {
        fn run(w: *fiber.Watermark, got: *bool, elapsed: *i64) void {
            const before = nowMs();
            w.waitFor(100, 20) catch |err| {
                if (err == error.Timeout) got.* = true;
            };
            elapsed.* = nowMs() - before;
        }
    };

    var got = false;
    var elapsed: i64 = 0;
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.spawn(.{}, Waiter.run, .{ &wm, &got, &elapsed });
    g.wait();

    std.log.debug("timed out = {}, elapsed = {d} ms (timeout 20 ms)", .{ got, elapsed });
    try std.testing.expect(got);
    try std.testing.expect(elapsed >= 15);
}

test "Watermark: waitFor returns ok when advance beats the timeout" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    const Waiter = struct {
        fn run(w: *fiber.Watermark, ok: *bool) void {
            if (w.waitFor(3, 5000)) {
                ok.* = true;
            } else |_| {}
        }
    };
    const Bumper = struct {
        fn run(w: *fiber.Watermark) void {
            fiber.sleepMs(5);
            w.advance(3);
        }
    };

    var ok = false;
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.spawn(.{}, Waiter.run, .{ &wm, &ok });
    try g.spawn(.{}, Bumper.run, .{&wm});
    g.wait();

    try std.testing.expect(ok);
}

test "Watermark: waitFor returns Closed when closed before the timeout" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    const Waiter = struct {
        fn run(w: *fiber.Watermark, closed: *bool) void {
            w.waitFor(100, 5000) catch |err| {
                if (err == error.Closed) closed.* = true;
            };
        }
    };
    const Closer = struct {
        fn run(w: *fiber.Watermark) void {
            fiber.sleepMs(5);
            w.close();
        }
    };

    var closed = false;
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.spawn(.{}, Waiter.run, .{ &wm, &closed });
    try g.spawn(.{}, Closer.run, .{&wm});
    g.wait();

    try std.testing.expect(closed);
}

test "Watermark: early completion cancels the timeout (no late fire)" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    const Waiter = struct {
        fn run(w: *fiber.Watermark, ok: *bool) void {
            if (w.waitFor(3, 30)) {
                ok.* = true;
            } else |_| {}
        }
    };

    var ok = false;
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.spawn(.{}, Waiter.run, .{ &wm, &ok });
    // Satisfy well before the 30 ms deadline. The advance runs on this
    // (possibly different) worker, exercising the cross-worker cancel.
    fiber.sleepMs(2);
    wm.advance(3);
    g.wait();
    try std.testing.expect(ok);

    // Linger past the original deadline. A timeout that wasn't cancelled
    // would fire here against the (now dead) waiter frame or trip the heap
    // back-pointer assert — so reaching the end cleanly is the assertion.
    fiber.sleepMs(60);
}

test "Watermark: mixed timed waiters under concurrent advance" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    const N = 50;
    var resolved: std.atomic.Value(u32) = .init(0);

    const Waiter = struct {
        fn run(w: *fiber.Watermark, target: u64, count: *std.atomic.Value(u32)) void {
            // Short timeout: some get satisfied by advance, others expire.
            w.waitFor(target, 10) catch {};
            _ = count.fetchAdd(1, .release);
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N);
    for (0..N) |i| {
        try g.spawn(.{}, Waiter.run, .{ &wm, @as(u64, @intCast(i + 1)), &resolved });
    }

    // Advance partway so the lower targets are satisfied while the rest
    // race their deadlines — stresses claim() arbitration + cancel.
    var v: u64 = 0;
    while (v < N / 2) : (v += 5) {
        wm.advance(v + 5);
        fiber.sleepMs(1);
    }
    g.wait();

    // Every waiter resolved exactly once (ok or Timeout); no hang/crash.
    try std.testing.expectEqual(@as(u32, N), resolved.load(.acquire));
}

test "Watermark: reset lowers the value; waitForOrReset reports it, waitForUntimed re-parks" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();
    wm.advance(100);

    var saw_reset: std.atomic.Value(bool) = .init(false);
    var plain_done: std.atomic.Value(bool) = .init(false);
    const W = struct {
        fn orReset(w: *fiber.Watermark, flag: *std.atomic.Value(bool)) void {
            w.waitForOrReset(150) catch |err| {
                if (err == error.Reset) flag.store(true, .release);
                return;
            };
        }
        fn plain(w: *fiber.Watermark, flag: *std.atomic.Value(bool)) void {
            w.waitForUntimed(150) catch return;
            flag.store(true, .release);
        }
    };
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(2);
    try g.spawn(.{}, W.orReset, .{ &wm, &saw_reset });
    try g.spawn(.{}, W.plain, .{ &wm, &plain_done });
    while (wm.waiters.items.len < 2) fiber.yield();

    wm.reset(49);
    try std.testing.expectEqual(@as(u64, 49), wm.load());
    while (!saw_reset.load(.acquire)) fiber.yield();
    // The plain waiter went back to waiting for 150.
    while (wm.waiters.items.len < 1) fiber.yield();
    try std.testing.expect(!plain_done.load(.acquire));

    wm.advance(150);
    g.wait();
    try std.testing.expect(plain_done.load(.acquire));
}

test "Watermark: add counts across fibers and wakes the waiter at its target" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    const Waiter = struct {
        fn run(w: *fiber.Watermark, reached: *bool) void {
            w.waitForUntimed(64) catch return;
            reached.* = true;
        }
    };
    const Adder = struct {
        fn run(w: *fiber.Watermark) void {
            for (0..8) |_| {
                w.add(1);
                fiber.yield();
            }
        }
    };

    var reached = false;
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.spawn(.{}, Waiter.run, .{ &wm, &reached });
    for (0..8) |_| try g.spawn(.{}, Adder.run, .{&wm});
    g.wait();

    try std.testing.expect(reached);
    try std.testing.expectEqual(64, wm.load());
}
