// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const fiber = @import("zfiber");
const engine = @import("engine");
const zdb = @import("zdb");

const ManifestQueue = engine.manifest_queue.ManifestQueue;
const Request = engine.manifest_queue.Request;

/// A submitter fiber: parks in `submit` until the test completes it.
fn submitter(q: *ManifestQueue, req: *Request) anyerror!void {
    return q.submit(req);
}

/// A taker fiber standing in for the executor: the request it got.
fn taker(q: *ManifestQueue) ?*Request {
    return q.take();
}

/// Spawns a submitter for `req` and polls until it is queued: a
/// spawned fiber reaches the queue in its own time, and the order the
/// tests assert is the order of queueing.
fn queue(
    g: *fiber.Group(anyerror!void),
    q: *ManifestQueue,
    req: *Request,
) !void {
    const before = q.status().waiting;
    try g.spawn(.{}, submitter, .{ q, req });
    const class = @intFromEnum(req.class);
    while (q.status().waiting[class] == before[class]) fiber.sleepMs(1);
}

test "manifest queue: foreground goes ahead of waiting background" {
    var q = ManifestQueue.init(testing.allocator);
    defer q.deinit();
    var bg1: Request = .{ .class = .bg, .kind = .{
        .split = .{ .namespace = 1, .tree_id = 1 },
    } };
    var bg2: Request = .{ .class = .bg, .kind = .{
        .change = .{ .cmd = .compaction },
    } };
    var fg: Request = .{ .class = .fg, .kind = .flush_begin };

    var g = fiber.Group(anyerror!void).init(testing.allocator);
    defer g.deinit();
    try queue(&g, &q, &bg1);
    try queue(&g, &q, &bg2);
    try queue(&g, &q, &fg);
    try testing.expectEqual(1, q.status().waiting[0]);
    try testing.expectEqual(2, q.status().waiting[1]);

    // The last in is the first out, being foreground; the background
    // ones follow in their order, each with the result it was given.
    const first = q.take().?;
    try testing.expectEqual(&fg, first);
    ManifestQueue.complete(first, {});
    const second = q.take().?;
    try testing.expectEqual(&bg1, second);
    ManifestQueue.complete(second, error.StaleChange);
    const third = q.take().?;
    try testing.expectEqual(&bg2, third);
    ManifestQueue.complete(third, {});

    const results = g.wait();
    try testing.expectError(error.StaleChange, results[0]);
    try results[1];
    try results[2];
    try testing.expectEqual(0, q.status().waiting[1]);
}

test "manifest queue: a split waits while a flush builds" {
    var q = ManifestQueue.init(testing.allocator);
    defer q.deinit();
    var split: Request = .{ .class = .bg, .kind = .{
        .split = .{ .namespace = 1, .tree_id = 1 },
    } };
    var comp: Request = .{ .class = .bg, .kind = .{
        .change = .{ .cmd = .compaction },
    } };
    var g = fiber.Group(anyerror!void).init(testing.allocator);
    defer g.deinit();
    try queue(&g, &q, &split);
    try queue(&g, &q, &comp);

    // The split is ahead in the queue but held back; the compaction
    // behind it runs.
    q.setFlushBuilding(true);
    const first = q.take().?;
    try testing.expectEqual(&comp, first);
    ManifestQueue.complete(first, {});

    // Nothing else may run: a taker parks until the flush ends.
    var takers = fiber.Group(?*Request).init(testing.allocator);
    defer takers.deinit();
    try takers.spawn(.{}, taker, .{&q});
    fiber.sleepMs(10);
    try testing.expectEqual(1, q.status().waiting[1]);
    try testing.expect(q.status().flush_building);
    q.setFlushBuilding(false);
    const got = takers.wait()[0].?;
    try testing.expectEqual(&split, got);
    ManifestQueue.complete(got, {});
    for (g.wait()) |r| try r;
}

test "manifest queue: takeMoves takes every waiting move and nothing else" {
    var q = ManifestQueue.init(testing.allocator);
    defer q.deinit();
    var none: [0]zdb.manifest.SSTableChange = .{};
    var move1: Request = .{ .class = .bg, .kind = .{
        .compaction = .{ .changes = &none, .move = true },
    } };
    var merge: Request = .{ .class = .bg, .kind = .{
        .compaction = .{ .changes = &none, .move = false },
    } };
    var move2: Request = .{ .class = .bg, .kind = .{
        .compaction = .{ .changes = &none, .move = true },
    } };
    var fg_move: Request = .{ .class = .fg, .kind = .{
        .compaction = .{ .changes = &none, .move = true },
    } };
    var g = fiber.Group(anyerror!void).init(testing.allocator);
    defer g.deinit();
    try queue(&g, &q, &move1);
    try queue(&g, &q, &merge);
    try queue(&g, &q, &move2);
    try queue(&g, &q, &fg_move);

    // The two background moves, oldest first; the merge keeps its
    // place and the foreground one is not a batch's to take.
    var moves: std.ArrayList(*Request) = .empty;
    defer moves.deinit(testing.allocator);
    try q.takeMoves(&moves);
    try testing.expectEqualSlices(*Request, &.{ &move1, &move2 }, moves.items);
    try testing.expectEqual(1, q.status().waiting[1]);
    try q.takeMoves(&moves);
    try testing.expectEqual(2, moves.items.len);
    for (moves.items) |req| ManifestQueue.complete(req, {});

    try testing.expectEqual(&fg_move, q.take().?);
    ManifestQueue.complete(&fg_move, {});
    try testing.expectEqual(&merge, q.take().?);
    ManifestQueue.complete(&merge, {});
    for (g.wait()) |r| try r;
}

test "manifest queue: a begun request is building until submitted or ended" {
    var q = ManifestQueue.init(testing.allocator);
    defer q.deinit();
    var comp: Request = .{ .class = .bg, .tree = .{
        .namespace = 3,
        .tree_id = 4,
    }, .kind = .{
        .compaction = .{ .move = false, .cid = 9, .src_level = 1 },
    } };
    var flush: Request = .{ .class = .fg, .kind = .{ .flush = .{} } };

    // Two builds under way; one is given up.
    q.begin(&comp);
    q.begin(&flush);
    try testing.expectEqual(2, q.status().building);
    q.end(&flush);
    try testing.expectEqual(1, q.status().building);

    // The dump names the build with its tree and id.
    var buf: [256]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try q.write(&w);
    try testing.expect(std.mem.indexOf(
        u8,
        w.buffered(),
        "building compaction tree=3:4 L1 cid=9",
    ) != null);

    // Submitted, it is waiting and no longer building; ending it
    // then is nothing.
    var g = fiber.Group(anyerror!void).init(testing.allocator);
    defer g.deinit();
    try queue(&g, &q, &comp);
    try testing.expectEqual(0, q.status().building);
    q.end(&comp);
    try testing.expectEqual(1, q.status().waiting[1]);
    ManifestQueue.complete(q.take().?, {});
    for (g.wait()) |r| try r;
}

test "manifest queue: closing fails every waiter and every later submit" {
    var q = ManifestQueue.init(testing.allocator);
    defer q.deinit();
    var fg: Request = .{ .class = .fg, .kind = .flush_begin };
    var bg: Request = .{ .class = .bg, .kind = .{ .drop_ns = 7 } };
    var g = fiber.Group(anyerror!void).init(testing.allocator);
    defer g.deinit();
    try queue(&g, &q, &fg);
    try queue(&g, &q, &bg);

    q.close();
    for (g.wait()) |r| try testing.expectError(error.Shutdown, r);
    try testing.expectEqual(null, q.take());
    var late: Request = .{ .class = .fg, .kind = .flush_end };
    try testing.expectError(error.Shutdown, q.submit(&late));
}
