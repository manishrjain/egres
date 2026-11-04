// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");

fn ctxOffFiber(out: **fiber.Ctx) void {
    out.* = fiber.ctx();
}

test "ctx: no statement means record is a no-op; off a fiber, bg" {
    // The test fiber's own context, with no statement started.
    const own = fiber.ctx();
    try std.testing.expect(own.arena == null);
    try std.testing.expectEqual(0, own.name.len);
    own.record("dropped", .{});
    own.record("dropped {d}", .{1});
    try std.testing.expectEqual(0, own.records.items.len);

    // Off any fiber: the shared background context.
    var seen: *fiber.Ctx = undefined;
    const t = try std.Thread.spawn(.{}, ctxOffFiber, .{&seen});
    t.join();
    try std.testing.expectEqual(&fiber.Ctx.bg, seen);
}

test "ctx: records carry labels and offsets, a slow statement is kept" {
    const qctx = fiber.ctx();
    try qctx.start("select 1", false);
    try std.testing.expect(qctx.arena != null);

    fiber.ctx().record("first", .{});
    fiber.sleepMs(3);
    fiber.ctx().record("n={d}", .{7});
    const recs = qctx.records.items;
    try std.testing.expectEqual(2, recs.len);
    try std.testing.expectEqualStrings("first", recs[0].label);
    try std.testing.expectEqualStrings("n=7", recs[1].label);
    try std.testing.expect(recs[1].at_us >= recs[0].at_us + 2_000);

    // Over 1 ms: the sampler keeps it, records included, and the
    // fiber's context is fresh again.
    qctx.finish();
    try std.testing.expect(fiber.ctx().arena == null);
    try std.testing.expectEqual(0, fiber.ctx().records.items.len);
    var body: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer body.deinit();
    try fiber.writeTraces(&body.writer, 0);
    const out = body.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "select 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "n=7") != null);
    // A higher floor than it took hides it.
    var none: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer none.deinit();
    try fiber.writeTraces(&none.writer, 100_000);
    const hidden = none.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, hidden, "select 1") == null);
}

fn recordHelper() void {
    fiber.ctx().record("helper", .{});
}

/// A helper's helper: its records must reach the statement too.
fn nestedHelper() void {
    var h = fiber.spawn(.{}, recordHelper, .{}) catch return;
    h.join();
}

fn noteClass(out: *std.atomic.Value(u8)) void {
    fiber.ctx().record("detached", .{});
    out.store(@intFromEnum(fiber.ctx().class), .release);
}

test "ctx: helpers record into the statement, a detached fiber does not" {
    const qctx = fiber.ctx();
    try qctx.start("select 2", false);
    defer qctx.finish();

    var h = try fiber.spawn(.{}, recordHelper, .{});
    h.join();
    try std.testing.expectEqual(1, qctx.records.items.len);
    try std.testing.expectEqualStrings("helper", qctx.records.items[0].label);

    var h2 = try fiber.spawn(.{}, nestedHelper, .{});
    h2.join();
    try std.testing.expectEqual(2, qctx.records.items.len);

    // A detached fiber inherits the I/O class but records nowhere.
    qctx.class = .bg;
    defer qctx.class = .fg;
    var seen: std.atomic.Value(u8) = .init(255);
    try fiber.spawnDetached(.{}, noteClass, .{&seen});
    while (seen.load(.acquire) == 255) fiber.sleepMs(1);
    try std.testing.expectEqual(
        @intFromEnum(fiber.IoClass.bg),
        seen.load(.acquire),
    );
    try std.testing.expectEqual(2, qctx.records.items.len);
}

test "ctx: a bucket keeps the newest traces and drops the oldest" {
    // Nine statements into one bucket's ring of eight: the first is
    // gone, the last is there, whatever else the suite adds. The start
    // is backdated by 200 ms rather than slept through, so the bucket
    // ([100 ms, 500 ms)) does not depend on how loaded the box is.
    var names: [9][5]u8 = undefined;
    for (&names, 0..) |*name, i| {
        name.* = .{ 'r', 'i', 'n', 'g', '0' + @as(u8, @intCast(i)) };
        const qctx = fiber.ctx();
        try qctx.start(name, false);
        qctx.start_mono_us -= 200_000;
        qctx.finish();
    }
    var body: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer body.deinit();
    try fiber.writeTraces(&body.writer, 0);
    const out = body.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, out, "ring8") != null);
    try std.testing.expect(std.mem.indexOf(u8, out, "ring0") == null);
}

test "ctx: a statement counts on the tenant, which outlives it" {
    const meter = fiber.meter;
    var t = fiber.Tenant.init(31, meter.default_limit, 0);
    const qctx = fiber.ctx();
    qctx.tenant = &t;
    defer qctx.tenant = &meter.shared;

    try qctx.start("select 1", false);
    try std.testing.expectEqual(1, t.totals.statements.load(.monotonic));
    try std.testing.expect(t.isActive(t.last_active_ms.load(.monotonic)));
    qctx.finish();
    // Reset to a fresh fiber's, except for whose it is.
    try std.testing.expectEqual(&t, qctx.tenant);
    try std.testing.expect(qctx.arena == null);
    try qctx.start("select 2", false);
    qctx.finish();
    try std.testing.expectEqual(2, t.totals.statements.load(.monotonic));
}

test "ctx: a statement's arenas count on it and its tenant, to the cap" {
    const meter = fiber.meter;
    var t = fiber.Tenant.init(32, meter.default_limit, 0);
    const qctx = fiber.ctx();
    qctx.tenant = &t;
    defer qctx.tenant = &meter.shared;
    try qctx.start("select 1", false);
    defer qctx.finish();
    var arena = qctx.newArena();
    defer arena.deinit();
    const buf = try arena.allocator().alloc(u8, 100_000);
    try std.testing.expect(qctx.mem_held.load(.monotonic) >= 100_000);
    try std.testing.expectEqual(
        qctx.mem_held.load(.monotonic),
        t.mem_held.load(.monotonic),
    );
    try std.testing.expect(t.totals.mem_peak.load(.monotonic) >= 100_000);

    // Under the cap the yield is free; over it the statement is refused.
    meter.query_mem_bytes.store(1 << 30, .monotonic);
    defer meter.query_mem_bytes.store(0, .monotonic);
    try qctx.maybeYield();
    meter.query_mem_bytes.store(50_000, .monotonic);
    try std.testing.expectError(error.MemLimit, qctx.maybeYield());

    // The arena gives it all back on the same fiber.
    _ = buf;
    arena.deinit();
    arena = qctx.newArena();
    try std.testing.expectEqual(0, qctx.mem_held.load(.monotonic));
    try std.testing.expectEqual(0, t.mem_held.load(.monotonic));
}
