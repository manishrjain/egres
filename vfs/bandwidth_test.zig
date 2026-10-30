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
    limiter.grant(5000, .read);
    try expectEqual(2, limiter.total_read_pages.load(.monotonic));
    try expectEqual(0, t.totals.io_pages.load(.monotonic));

    // Metered: a read costs its pages, a write its pages times the
    // write cost, and the device counts pages either way.
    limiter.metered = true;
    limiter.grant(5000, .read);
    limiter.grant(4 * page_size, .write);
    limiter.grant(0, .read);
    try expectEqual(4, limiter.total_read_pages.load(.monotonic));
    try expectEqual(4, limiter.total_write_pages.load(.monotonic));
    try expectEqual(2 + 4 * 8, t.totals.io_pages.load(.monotonic));
    try expectEqual(1_000_000, limiter.status().pages_per_sec);
    try expect(limiter.status().metered);
}

test "limiter: the shared tenant never waits" {
    try expectEqual(&fiber.meter.shared, fiber.ctx().tenant);
    // One page a second, and a thousand asked for twice over.
    var limiter = Limiter.init(.{ .bytes_per_sec = page_size });
    limiter.metered = true;
    const start = utils.coarseMs();
    limiter.grant(1024 * page_size, .read);
    limiter.grant(1024 * page_size, .write);
    try expect(utils.coarseMs() - start < 100);
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
    limiter.grant(300 * page_size, .read);
    try expect(utils.coarseMs() - start < 50);
    limiter.grant(page_size, .read);
    const fg_waited = utils.coarseMs() - start;
    try expect(fg_waited >= 150 and fg_waited <= 1500);

    // bg: its burst and one more at once, then the next sleeps once
    // for fg to fill and its own page: about 100 ms.
    ctx.class = .bg;
    defer ctx.class = .fg;
    start = utils.coarseMs();
    limiter.grant(101 * page_size, .read);
    try expect(utils.coarseMs() - start < 50);
    limiter.grant(page_size, .read);
    const bg_waited = utils.coarseMs() - start;
    try expect(bg_waited >= 50 and bg_waited <= 1500);
    try expectEqual(300 + 1 + 101 + 1, t.totals.io_pages.load(.monotonic));
}
