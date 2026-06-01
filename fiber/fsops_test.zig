const std = @import("std");
const fiber = @import("fiber");

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
    const h = try fiber.spawn(.{}, Body.run, .{&out});
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
    const h = try fiber.spawn(.{}, Body.run, .{&out});
    h.join();
    try std.testing.expectError(error.Boom, out);
}
