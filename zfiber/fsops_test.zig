// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");

test "fsops: run round-trips an offloaded function's result through park/wake" {
    const Body = struct {
        fn run(out: *u64) void {
            out.* = fiber.fsops.run(square, .{@as(u64, 7)});
        }
        fn square(x: u64) u64 {
            return x * x;
        }
    };

    var out: u64 = 0;
    var h = try fiber.spawn(.{}, Body.run, .{&out});
    h.join();
    try std.testing.expectEqual(@as(u64, 49), out);
}

test "fsops: many fibers offload concurrently; every result is correct" {
    const N = 32;
    const Body = struct {
        fn run(i: u64, out: *u64) void {
            out.* = fiber.fsops.run(add1, .{i});
        }
        fn add1(x: u64) u64 {
            return x + 1;
        }
    };

    var outs: [N]u64 = undefined;
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N);
    for (&outs, 0..) |*o, i| try g.spawn(.{}, Body.run, .{ @as(u64, @intCast(i)), o });
    g.wait();

    for (outs, 0..) |o, i| try std.testing.expectEqual(@as(u64, @intCast(i + 1)), o);
}

test "fsops: a raft-lane fiber's job goes ahead of the queued bulk jobs" {
    // Twice the pool's threads of slow jobs, then one from a raft-lane
    // fiber: it runs as soon as a thread is free, before the bulk jobs
    // still queued, so at least the last of them finishes after it. A
    // FIFO would run it last of all nine.
    const bulk = 8;
    const Body = struct {
        var order: std.atomic.Value(u32) = .init(0);
        fn run(out: *u32) void {
            out.* = fiber.fsops.run(slow, .{});
        }
        fn slow() u32 {
            // On a pool thread: no fiber sleep here.
            const req = std.posix.timespec{ .sec = 0, .nsec = 20_000_000 };
            _ = std.posix.system.nanosleep(&req, null);
            return order.fetchAdd(1, .monotonic);
        }
    };

    var outs: [bulk]u32 = undefined;
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(bulk);
    for (&outs) |*o| try g.spawn(.{}, Body.run, .{o});
    // The bulk fibers have run to their park, their jobs queued.
    fiber.sleepMs(5);

    var urgent: u32 = undefined;
    var h = try fiber.spawn(.{ .lane = .raft }, Body.run, .{&urgent});
    h.join();
    g.wait();
    try std.testing.expect(urgent < bulk);
}

test "fsops: an offloaded error union propagates back to the fiber" {
    const Body = struct {
        fn run(out: *anyerror!void) void {
            out.* = fiber.fsops.run(fail, .{});
        }
        fn fail() error{Boom}!void {
            return error.Boom;
        }
    };

    var out: anyerror!void = {};
    var h = try fiber.spawn(.{}, Body.run, .{&out});
    h.join();
    try std.testing.expectError(error.Boom, out);
}
