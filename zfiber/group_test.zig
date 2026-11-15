// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");

test "Group(void): wait synchronizes all fibers" {
    const N = 8;
    var counters = [_]u32{0} ** N;

    const Body = struct {
        fn run(c: *u32) void {
            var i: u32 = 0;
            while (i < 5) : (i += 1) {
                c.* += 1;
                fiber.yield();
            }
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();

    for (&counters) |*c| try g.spawn(.{}, Body.run, .{c});
    g.wait();

    for (counters) |c| try std.testing.expectEqual(@as(u32, 5), c);
}

test "Group(u64): wait returns results in spawn order" {
    const N = 6;

    const Body = struct {
        fn run(i: u64) u64 {
            fiber.yield();
            return i * i;
        }
    };

    var g: fiber.Group(u64) = .init(std.testing.allocator);
    defer g.deinit();

    var i: u64 = 0;
    while (i < N) : (i += 1) try g.spawn(.{}, Body.run, .{i});

    const results = g.wait();
    try std.testing.expectEqual(@as(usize, N), results.len);
    for (results, 0..) |r, idx| {
        const want: u64 = @as(u64, @intCast(idx)) * @as(u64, @intCast(idx));
        try std.testing.expectEqual(want, r);
    }
}

test "Group: empty wait is a no-op" {
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    g.wait();

    var typed: fiber.Group(u32) = .init(std.testing.allocator);
    defer typed.deinit();
    const r = typed.wait();
    try std.testing.expectEqual(@as(usize, 0), r.len);
}

test "Group: ensureTotalCapacity pre-sizes without spawning" {
    var g: fiber.Group(u32) = .init(std.testing.allocator);
    defer g.deinit();

    try g.ensureTotalCapacity(4);
    try std.testing.expect(g.handles.capacity >= 4);
    try std.testing.expect(g.results.capacity >= 4);

    const Body = struct {
        fn run(x: u32) u32 {
            return x + 1;
        }
    };

    var i: u32 = 0;
    while (i < 4) : (i += 1) try g.spawn(.{}, Body.run, .{i});

    const results = g.wait();
    try std.testing.expectEqual(@as(u32, 1), results[0]);
    try std.testing.expectEqual(@as(u32, 4), results[3]);
}

const Visits = struct {
    seen: []std.atomic.Value(u32),
    running: std.atomic.Value(u32) = .init(0),
    most_running: std.atomic.Value(u32) = .init(0),
    fail_at: ?usize = null,

    fn visit(self: *Visits, idx: usize) anyerror!void {
        const now = self.running.fetchAdd(1, .acq_rel) + 1;
        _ = self.most_running.fetchMax(now, .acq_rel);
        defer _ = self.running.fetchSub(1, .acq_rel);
        // Give the other lanes the worker, so they overlap.
        fiber.yield();
        if (self.fail_at) |at| if (idx == at) return error.Stop;
        _ = self.seen[idx].fetchAdd(1, .acq_rel);
    }
};

test "forEachIndex: every index once, never more than the lanes" {
    var seen = [_]std.atomic.Value(u32){.init(0)} ** 100;
    var visits: Visits = .{ .seen = &seen };
    const gpa = std.testing.allocator;
    try fiber.forEachIndex(gpa, 100, 8, &visits, Visits.visit);
    for (&seen) |*s| try std.testing.expectEqual(1, s.load(.acquire));
    try std.testing.expect(visits.most_running.load(.acquire) <= 8);
}

test "forEachIndex: fewer items than lanes, and none" {
    var seen = [_]std.atomic.Value(u32){.init(0)} ** 3;
    var visits: Visits = .{ .seen = &seen };
    const gpa = std.testing.allocator;
    try fiber.forEachIndex(gpa, 3, 8, &visits, Visits.visit);
    for (&seen) |*s| try std.testing.expectEqual(1, s.load(.acquire));
    try std.testing.expect(visits.most_running.load(.acquire) <= 3);
    try fiber.forEachIndex(gpa, 0, 8, &visits, Visits.visit);
}

test "forEachIndex: the first error stops the lanes and is returned" {
    var seen = [_]std.atomic.Value(u32){.init(0)} ** 1000;
    var visits: Visits = .{ .seen = &seen, .fail_at = 5 };
    try std.testing.expectError(error.Stop, fiber.forEachIndex(
        std.testing.allocator,
        1000,
        4,
        &visits,
        Visits.visit,
    ));
    var called: u32 = 0;
    for (&seen) |*s| called += s.load(.acquire);
    try std.testing.expect(called < 1000);
}
