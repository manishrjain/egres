const std = @import("std");
const fiber = @import("fiber");

test "Channel: send + receive round-trip" {
    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 4);
    defer ch.deinit();

    var received: [4]u32 = .{ 0, 0, 0, 0 };

    const Body = struct {
        fn run(c: *fiber.Channel(u32), out: *[4]u32) void {
            c.send(10) catch unreachable;
            c.send(20) catch unreachable;
            c.send(30) catch unreachable;
            c.send(40) catch unreachable;
            out[0] = c.receive() catch unreachable;
            out[1] = c.receive() catch unreachable;
            out[2] = c.receive() catch unreachable;
            out[3] = c.receive() catch unreachable;
        }
    };

    const h = try fiber.spawn(.{}, Body.run, .{ &ch, &received });
    h.join();

    try std.testing.expectEqualSlices(u32, &.{ 10, 20, 30, 40 }, &received);
}

test "Channel: bounded blocking handoff with capacity 1" {
    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 1);
    defer ch.deinit();

    var total: u32 = 0;

    const Producer = struct {
        fn run(c: *fiber.Channel(u32)) void {
            // 100 items through a capacity-1 channel — every send
            // after the first blocks until the receiver drains.
            var i: u32 = 1;
            while (i <= 100) : (i += 1) c.send(i) catch return;
            c.close(.graceful);
        }
    };
    const Consumer = struct {
        fn run(c: *fiber.Channel(u32), t: *u32) void {
            while (true) {
                const v = c.receive() catch return;
                t.* += v;
            }
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    // Pin both to worker 0 — `total` is a plain u32, not atomic.
    try g.spawn(.{ }, Producer.run, .{&ch});
    try g.spawn(.{ }, Consumer.run, .{ &ch, &total });
    g.wait();

    try std.testing.expectEqual(@as(u32, 100 * 101 / 2), total);
}

test "Channel: trySend on full returns ChannelFull" {
    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 2);
    defer ch.deinit();

    var saw_full = false;

    const Body = struct {
        fn run(c: *fiber.Channel(u32), out: *bool) void {
            c.trySend(1) catch unreachable;
            c.trySend(2) catch unreachable;
            c.trySend(3) catch |err| switch (err) {
                error.ChannelFull => {
                    out.* = true;
                    return;
                },
                else => unreachable,
            };
        }
    };

    const h = try fiber.spawn(.{}, Body.run, .{ &ch, &saw_full });
    h.join();

    try std.testing.expect(saw_full);
}

test "Channel: tryReceive on empty returns ChannelEmpty; on closed-empty returns ChannelClosed" {
    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 2);
    defer ch.deinit();

    var saw_empty = false;
    var saw_closed = false;

    const Body = struct {
        fn run(c: *fiber.Channel(u32), empty: *bool, closed: *bool) void {
            _ = c.tryReceive() catch |err| switch (err) {
                error.ChannelEmpty => empty.* = true,
                error.ChannelClosed => unreachable,
            };
            c.close(.immediate);
            _ = c.tryReceive() catch |err| switch (err) {
                error.ChannelClosed => closed.* = true,
                error.ChannelEmpty => unreachable,
            };
        }
    };

    const h = try fiber.spawn(.{}, Body.run, .{ &ch, &saw_empty, &saw_closed });
    h.join();

    try std.testing.expect(saw_empty);
    try std.testing.expect(saw_closed);
}

test "Channel: graceful close lets receivers drain buffered items" {
    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 4);
    defer ch.deinit();

    var drained: [3]u32 = .{ 0, 0, 0 };
    var saw_closed = false;

    const Body = struct {
        fn run(c: *fiber.Channel(u32), out: *[3]u32, closed: *bool) void {
            c.send(7) catch unreachable;
            c.send(8) catch unreachable;
            c.send(9) catch unreachable;
            c.close(.graceful);
            out[0] = c.receive() catch unreachable;
            out[1] = c.receive() catch unreachable;
            out[2] = c.receive() catch unreachable;
            _ = c.receive() catch |err| switch (err) {
                error.ChannelClosed => closed.* = true,
            };
        }
    };

    const h = try fiber.spawn(.{}, Body.run, .{ &ch, &drained, &saw_closed });
    h.join();

    try std.testing.expectEqualSlices(u32, &.{ 7, 8, 9 }, &drained);
    try std.testing.expect(saw_closed);
}

test "Channel: immediate close drops buffered items" {
    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 4);
    defer ch.deinit();

    var saw_closed = false;

    const Body = struct {
        fn run(c: *fiber.Channel(u32), closed: *bool) void {
            c.send(1) catch unreachable;
            c.send(2) catch unreachable;
            c.close(.immediate);
            _ = c.receive() catch |err| switch (err) {
                error.ChannelClosed => closed.* = true,
            };
        }
    };

    const h = try fiber.spawn(.{}, Body.run, .{ &ch, &saw_closed });
    h.join();

    try std.testing.expect(saw_closed);
}

test "Channel: cross-worker MPMC stress (4 producers x 4 consumers x cap 8)" {
    var ch = try fiber.Channel(u32).initAlloc(std.testing.allocator, 8);
    defer ch.deinit();

    const N_PRODUCERS = 4;
    const N_CONSUMERS = 4;
    const ITEMS_PER_PRODUCER: u32 = 2000;
    const TOTAL: u64 = N_PRODUCERS * ITEMS_PER_PRODUCER;

    var produced_total: std.atomic.Value(u64) = .init(0);
    var received_total: std.atomic.Value(u64) = .init(0);
    var producers_done: std.atomic.Value(u32) = .init(0);

    const Producer = struct {
        fn run(
            c: *fiber.Channel(u32),
            prod_total: *std.atomic.Value(u64),
            done_counter: *std.atomic.Value(u32),
            n_producers: u32,
            items: u32,
        ) void {
            var i: u32 = 0;
            while (i < items) : (i += 1) {
                c.send(i) catch return;
                _ = prod_total.fetchAdd(1, .release);
            }
            const done = done_counter.fetchAdd(1, .acq_rel) + 1;
            if (done == n_producers) c.close(.graceful);
        }
    };
    const Consumer = struct {
        fn run(c: *fiber.Channel(u32), recv_total: *std.atomic.Value(u64)) void {
            while (true) {
                _ = c.receive() catch return;
                _ = recv_total.fetchAdd(1, .release);
            }
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N_PRODUCERS + N_CONSUMERS);

    for (0..N_PRODUCERS) |_| {
        try g.spawn(
            .{},
            Producer.run,
            .{ &ch, &produced_total, &producers_done, N_PRODUCERS, ITEMS_PER_PRODUCER },
        );
    }
    for (0..N_CONSUMERS) |_| {
        try g.spawn(
            .{},
            Consumer.run,
            .{ &ch, &received_total },
        );
    }
    g.wait();

    try std.testing.expectEqual(TOTAL, produced_total.load(.acquire));
    try std.testing.expectEqual(TOTAL, received_total.load(.acquire));
}
