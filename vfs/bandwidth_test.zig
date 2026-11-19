// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const fiber = @import("zfiber");
const utils = @import("utils");
const vfs = @import("vfs");
const Limiter = vfs.bandwidth.Limiter;
const page_size = vfs.page_size;

const expectEqual = testing.expectEqual;
const expect = testing.expect;

/// A tenant at `pages_per_s` on the real coarse clock, on this test
/// fiber's context until the caller resets it.
fn tenantAt(id: u64, pages_per_s: i64) fiber.Tenant {
    return fiber.Tenant.init(id, .{
        .cpu_milli = 1000,
        .io_pages_per_s = pages_per_s,
        .mem_bytes = 1,
    }, utils.coarseMs());
}

test "limiter: only a metered device charges the fiber's tenant" {
    var t = tenantAt(11, 1_000_000);
    const ctx = fiber.ctx();
    ctx.tenant = &t;
    defer ctx.tenant = &fiber.meter.shared;

    var limiter = Limiter.init(.{
        .bytes_per_sec = 1_000_000 * page_size,
        .write_cost = 8,
    });
    // Unmetered: the device counts its pages, the tenant pays nothing.
    try expect(!limiter.metered);
    limiter.grant(5000, 1, .read);
    try expectEqual(2, limiter.total_read_pages.load(.monotonic));
    try expectEqual(0, t.totals.io_pages.load(.monotonic));

    // Metered: a read costs its pages, a write its pages times the
    // write cost, and the device counts pages either way.
    limiter.metered = true;
    limiter.grant(5000, 1, .read);
    limiter.grant(4 * page_size, 1, .write);
    limiter.grant(0, 1, .read);
    try expectEqual(4, limiter.total_read_pages.load(.monotonic));
    try expectEqual(4, limiter.total_write_pages.load(.monotonic));
    try expectEqual(2 + 4 * 8, t.totals.io_pages.load(.monotonic));
    try expectEqual(1_000_000, limiter.status().pages_per_sec);
    try expect(limiter.status().metered);
}

test "limiter: an operation costs at least the device's op pages" {
    var t = tenantAt(12, 1_000_000);
    const ctx = fiber.ctx();
    ctx.tenant = &t;
    defer ctx.tenant = &fiber.meter.shared;

    // 1000 pages a second over 100 operations a second: one operation
    // is worth 10 pages of device time.
    var limiter = Limiter.init(.{
        .bytes_per_sec = 1000 * page_size,
        .read_ops_per_sec = 100,
    });
    limiter.metered = true;
    try expectEqual(10, limiter.pages_per_op);

    // A point read's one page costs the operation; a large read its
    // pages; a batch of small reads its operations.
    limiter.grant(page_size, 1, .read);
    try expectEqual(10, t.totals.io_pages.load(.monotonic));
    limiter.grant(100 * page_size, 1, .read);
    try expectEqual(110, t.totals.io_pages.load(.monotonic));
    limiter.grant(20 * page_size, 20, .read);
    try expectEqual(310, t.totals.io_pages.load(.monotonic));
    try expectEqual(22, t.totals.io_ops.load(.monotonic));
    // The device counts what moved, and in how many requests.
    try expectEqual(121, limiter.total_read_pages.load(.monotonic));
    try expectEqual(22, limiter.status().total_read_ops);

    // Without an iops figure an operation is its pages alone.
    const plain = Limiter.init(.{ .bytes_per_sec = 1000 * page_size });
    try expectEqual(1, plain.pages_per_op);
}

test "limiter: the shared tenant never waits" {
    try expectEqual(&fiber.meter.shared, fiber.ctx().tenant);
    // One page a second, and a thousand asked for twice over.
    var limiter = Limiter.init(.{ .bytes_per_sec = page_size });
    limiter.metered = true;
    const start = utils.coarseMs();
    limiter.grant(1024 * page_size, 1, .read);
    limiter.grant(1024 * page_size, 1, .write);
    try expect(utils.coarseMs() - start < 100);
}

test "limiter: a fiber whose io does not wait is charged and runs on" {
    // 1000 pages/s, a burst of 100.
    var t = tenantAt(13, 1000);
    const ctx = fiber.ctx();
    ctx.tenant = &t;
    defer ctx.tenant = &fiber.meter.shared;
    var limiter = Limiter.init(.{ .bytes_per_sec = 1000 * page_size });
    limiter.metered = true;

    // A flush's worth at once, taken whole and not waited for.
    ctx.waits = false;
    defer ctx.waits = true;
    var start = utils.coarseMs();
    limiter.grant(400 * page_size, 1, .read);
    limiter.grant(400 * page_size, 1, .read);
    try expect(utils.coarseMs() - start < 50);
    try expectEqual(800, t.totals.io_pages.load(.monotonic));

    // The tenant's next paying grant owes the debt: 700 pages, 700 ms.
    ctx.waits = true;
    start = utils.coarseMs();
    limiter.grant(page_size, 1, .read);
    const waited = utils.coarseMs() - start;
    try expect(waited >= 500 and waited <= 2000);
}

test "limiter: direct writes share write_depth slots" {
    var limiter = Limiter.init(.{
        .bytes_per_sec = page_size,
        .write_depth = 3,
    });
    var group = fiber.Group(bool).init(testing.allocator);
    defer group.deinit();
    for (0..12) |_| try group.spawn(.{}, holdSlot, .{&limiter});
    for (group.wait()) |within| try expect(within);
    try expectEqual(0, limiter.status().writes_in_flight);
}

/// One write piece's worth of a slot: whether the writes in flight
/// while it was held stayed within the depth.
fn holdSlot(limiter: *Limiter) bool {
    limiter.writes.enter();
    defer limiter.writes.leave();
    const in_flight = limiter.status().writes_in_flight;
    fiber.sleepMs(5);
    return in_flight >= 1 and in_flight <= 3;
}

test "limiter: a tenant pays fg's debt first, then bg's" {
    // 1000 pages/s, a burst of 100.
    var t = tenantAt(12, 1000);
    const ctx = fiber.ctx();
    ctx.tenant = &t;
    defer ctx.tenant = &fiber.meter.shared;
    var limiter = Limiter.init(.{ .bytes_per_sec = 1000 * page_size });
    limiter.metered = true;

    // fg: 300 pages at once, 200 owed, 200 ms before the next.
    var start = utils.coarseMs();
    limiter.grant(300 * page_size, 1, .read);
    try expect(utils.coarseMs() - start < 50);
    limiter.grant(page_size, 1, .read);
    const fg_waited = utils.coarseMs() - start;
    try expect(fg_waited >= 150 and fg_waited <= 1500);

    // bg: its burst and one more at once, then the next sleeps once
    // for fg to fill and its own page: about 100 ms.
    ctx.class = .bg;
    defer ctx.class = .fg;
    start = utils.coarseMs();
    limiter.grant(101 * page_size, 1, .read);
    try expect(utils.coarseMs() - start < 50);
    limiter.grant(page_size, 1, .read);
    const bg_waited = utils.coarseMs() - start;
    try expect(bg_waited >= 50 and bg_waited <= 1500);
    try expectEqual(300 + 1 + 101 + 1, t.totals.io_pages.load(.monotonic));
}
