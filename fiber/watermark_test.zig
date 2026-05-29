const std = @import("std");
const fiber = @import("fiber");

test "Watermark: waitFor returns when value crosses target" {
    var wm = fiber.Watermark.init(std.testing.allocator);
    defer wm.deinit();

    var observed: u64 = 0;

    const Waiter = struct {
        fn run(w: *fiber.Watermark, out: *u64) void {
            w.waitFor(5) catch return;
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
            w.waitFor(3) catch return;
            out.* = true;
        }
    };
    const Mid = struct {
        fn run(w: *fiber.Watermark, out: *bool) void {
            w.waitFor(5) catch return;
            out.* = true;
        }
    };
    const High = struct {
        fn run(w: *fiber.Watermark, ok: *bool, closed: *bool) void {
            w.waitFor(20) catch |err| {
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
            w.waitFor(100) catch |err| switch (err) {
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
            w.waitFor(target) catch return;
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
