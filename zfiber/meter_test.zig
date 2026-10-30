// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const meter = fiber.meter;
const Bucket = fiber.Bucket;
const Resource = fiber.Resource;
const Tenant = fiber.Tenant;
const Tenants = fiber.Tenants;
const Limit = Tenant.Limit;

const expectEqual = std.testing.expectEqual;
const expect = std.testing.expect;

test "bucket: take goes as far below zero as the cost says" {
    var b: Bucket = .{ .tokens = .init(100) };
    b.take(30);
    try expectEqual(70, b.balance());
    b.take(1000);
    try expectEqual(-930, b.balance());
}

test "bucket: fill pays the debt, stops at the cap, says what went in" {
    var b: Bucket = .{ .tokens = .init(-50) };
    try expectEqual(60, b.fill(60, 100));
    try expectEqual(10, b.balance());
    try expectEqual(90, b.fill(500, 100));
    try expectEqual(100, b.balance());
    try expectEqual(0, b.fill(500, 100));
}

test "resource: starts full, refills at the rate, caps at the burst" {
    // 1000 units/s, 100 saved up: a tick's worth, in every bucket.
    var r = Resource.init(1000, 100, 0);
    try expectEqual(100, r.balance(.fg, 0));
    try expectEqual(100, r.balance(.bg, 0));
    r.take(.fg, 100, 0);
    try expectEqual(0, r.balance(.fg, 0));

    // 50 ms earns 50; a further 500 ms earns the cap, not 500.
    try expectEqual(50, r.balance(.fg, 50));
    try expectEqual(100, r.balance(.fg, 550));
}

test "resource: a window too short for one unit waits for the next" {
    // 100 units/s: 1 ms earns 0.1, so nothing moves until 10 ms.
    var r = Resource.init(100, 10, 0);
    r.take(.fg, 10, 0);
    try expectEqual(0, r.balance(.fg, 1));
    try expectEqual(0, r.balance(.fg, 9));
    try expectEqual(1, r.balance(.fg, 10));
}

test "resource: fg is filled first, debt and room, bg gets the rest" {
    var r = Resource.init(1000, 100, 0);
    r.take(.fg, 300, 0); // 200 in debt
    r.take(.bg, 100, 0); // empty
    // 100 ms earns 100: all of it to fg's debt.
    try expectEqual(0, r.balance(.bg, 100));
    try expectEqual(-100, r.balance(.fg, 100));
    // 150 more: fg's debt, then fg's room; bg still waits.
    try expectEqual(0, r.balance(.bg, 250));
    try expectEqual(50, r.balance(.fg, 250));
    try expectEqual(0, r.balance(.bg, 300));
    try expectEqual(100, r.balance(.fg, 300));
    // fg full: the next 100 are bg's.
    try expectEqual(100, r.balance(.bg, 400));
    try expectEqual(100, r.balance(.fg, 400));
}

test "resource: waitMs is the time the rate needs to pay the debt" {
    var r = Resource.init(1000, 100, 0);
    try expectEqual(0, r.waitMs(.fg));
    r.take(.fg, 100, 0);
    try expectEqual(0, r.waitMs(.fg));
    r.take(.fg, 1, 0);
    try expectEqual(1, r.waitMs(.fg));
    r.take(.fg, 499, 0);
    try expectEqual(500, r.waitMs(.fg));
    r.setRate(1, 100);
    try expectEqual(Resource.max_wait_ms, r.waitMs(.fg));
    r.setRate(0, 0);
    try expectEqual(Resource.idle_wait_ms, r.waitMs(.fg));
}

test "resource: bg's wait counts the room fg is filled to first" {
    var r = Resource.init(1000, 100, 0);
    // bg one in debt with fg full: one unit, one ms.
    r.take(.bg, 101, 0);
    try expectEqual(1, r.waitMs(.bg));
    // fg 50 short of full: those 50 come first.
    r.take(.fg, 50, 0);
    try expectEqual(51, r.waitMs(.bg));
    // fg 200 in debt: its debt, its burst, then bg's one.
    r.take(.fg, 250, 0);
    try expectEqual(301, r.waitMs(.bg));
    try expectEqual(200, r.waitMs(.fg));
}

test "resource: a zero rate never refills" {
    var r = Resource.init(0, 0, 0);
    r.take(.fg, 10, 0);
    try expectEqual(-10, r.balance(.fg, 10_000));
    // A rate set later earns from the last refill, up to the burst.
    r.setRate(1000, 100);
    try expectEqual(100, r.balance(.fg, 10_100));
}

/// Millicores per core, the unit a plan is sold in.
const vcpu: i64 = 1000;
const gib: i64 = 1 << 30;

/// This machine's ticks per second, one core's worth, as the runtime
/// calibrated it.
fn hz() i64 {
    return @intCast(fiber.ticksPerSecond());
}

/// A 32-worker box, a device at 1M pages/s, 64 GiB for statements.
const box: Limit = .{
    .cpu_milli = 32 * vcpu,
    .io_pages_per_s = 1_000_000,
    .mem_bytes = 64 * gib,
};

fn plan(vcpus: i64) Limit {
    return .{
        .cpu_milli = vcpus * vcpu,
        .io_pages_per_s = vcpus * 10_000,
        .mem_bytes = vcpus * gib,
    };
}

/// The tick takes the shared tenant's spend since the last tick off
/// the box, and every test in the binary charges the shared tenant: io
/// through the limiter, cpu for every fiber step. A first tick this
/// far from zero spreads the lifetime total over so long that it
/// truncates to nothing; a later tick in a test checks memory, which
/// only these tests touch, and bounds cpu and io from above.
const t0: i64 = 1 << 60;

test "tenants: one tenant alone gets the box" {
    meter.capacity = box;
    var ts = Tenants.init(std.testing.allocator, plan(1));
    defer ts.deinit();

    try expect(hz() > 0);
    const a = try ts.get(1, t0);
    try expectEqual(1, ts.count());
    try expectEqual(hz(), a.cpu.rate.load(.monotonic));

    ts.tick(t0);
    try expectEqual(32 * hz(), a.cpu.rate.load(.monotonic));
    try expectEqual(1_000_000, a.io.rate.load(.monotonic));
    try expectEqual(64 * gib, a.mem_share.load(.monotonic));
}

test "tenants: the split is by limit, idle tenants are not in it" {
    meter.capacity = box;
    var ts = Tenants.init(std.testing.allocator, plan(1));
    defer ts.deinit();

    const large = try ts.get(1, t0);
    const small = try ts.get(2, t0);
    const idle = try ts.get(3, t0);
    try ts.setLimit(1, plan(4), t0);
    try expectEqual(large, try ts.get(1, t0));

    // All three active: 4 + 1 + 1 = 6.
    const cpu = 32 * hz();
    ts.tick(t0);
    try expectEqual(@divTrunc(cpu * 4, 6), large.cpu.rate.load(.monotonic));
    try expectEqual(@divTrunc(cpu, 6), small.cpu.rate.load(.monotonic));
    try expectEqual(@divTrunc(cpu, 6), idle.cpu.rate.load(.monotonic));

    // The third goes idle: 4 + 1 = 5, and its own rate is left as is.
    const later = t0 + Tenant.active_window_ms + 1;
    large.charge(.cpu, .fg, 1, later);
    small.touch(later);
    ts.tick(later);
    try near(large.cpu.rate.load(.monotonic), @divTrunc(cpu * 4, 5));
    try near(small.cpu.rate.load(.monotonic), @divTrunc(cpu, 5));
    try expectEqual(@divTrunc(cpu, 6), idle.cpu.rate.load(.monotonic));
    try expect(large.io.rate.load(.monotonic) <= 800_000);
    const mem = large.mem_share.load(.monotonic);
    try expectEqual(@divTrunc(64 * gib * 4, 5), mem);
}

test "tenants: a sold-out box gives every tenant its number" {
    meter.capacity = box;
    var ts = Tenants.init(std.testing.allocator, plan(1));
    defer ts.deinit();

    // 4 large and 16 small: 16 + 16 = 32 vCPUs.
    var id: u64 = 1;
    while (id <= 4) : (id += 1) try ts.setLimit(id, plan(4), t0);
    while (id <= 20) : (id += 1) _ = try ts.get(id, t0);
    ts.tick(t0);
    const large = try ts.get(1, t0);
    const small = try ts.get(20, t0);
    try expectEqual(4 * hz(), large.cpu.rate.load(.monotonic));
    try expectEqual(hz(), small.cpu.rate.load(.monotonic));

    // Oversold twice over: everyone is scaled down in proportion.
    while (id <= 40) : (id += 1) _ = try ts.get(id, t0);
    id = 21;
    while (id <= 24) : (id += 1) try ts.setLimit(id, plan(4), t0);
    ts.tick(t0 + Tenant.active_window_ms - 1);
    try near(large.cpu.rate.load(.monotonic), 2 * hz());
    try near(small.cpu.rate.load(.monotonic), @divTrunc(hz(), 2));
}

/// A cpu share from a tick after the first: the number, less the
/// share of what the other tests' fibers charged the shared tenant between
/// the two ticks. That is microseconds of a window a second long, so
/// well under a sixteenth.
fn near(got: i64, want: i64) !void {
    try expect(got <= want and got > want - @divTrunc(want, 16));
}

test "tenant: an allocator of its own counts on it and the process" {
    var t = Tenant.init(41, meter.default_limit, 0);
    const ctx = fiber.ctx();
    ctx.tenant = &t;
    defer ctx.tenant = &meter.shared;
    const ctx_held = ctx.mem_held.load(.monotonic);

    // The fiber's tenant owns it; nothing lands on the statement.
    const tloc = fiber.allocator(.tenant);
    try expectEqual(@as(*anyopaque, @ptrCast(&t)), tloc.ptr);
    const buf = try tloc.alloc(u8, 64 * 1024);
    try expectEqual(64 * 1024, t.mem_held.load(.monotonic));
    try expectEqual(64 * 1024, t.totals.mem_peak.load(.monotonic));
    try expectEqual(ctx_held, ctx.mem_held.load(.monotonic));

    tloc.free(buf);
    try expectEqual(0, t.mem_held.load(.monotonic));
    try expectEqual(64 * 1024, t.totals.mem_peak.load(.monotonic));
}

test "tenants: what the shared tenant spent comes off the box first" {
    meter.capacity = box;
    var ts = Tenants.init(std.testing.allocator, plan(1));
    defer ts.deinit();
    const a = try ts.get(1, t0);
    ts.tick(t0);
    try expectEqual(1_000_000, a.io.rate.load(.monotonic));

    // Half the device over the next second, as the shared tenant: the
    // tenant gets the other half, less whatever another test charged
    // the shared tenant meanwhile, which never waited.
    meter.shared.charge(.io, .fg, 500_000, t0 + 500);
    a.touch(t0 + 500);
    ts.tick(t0 + 1000);
    const io = a.io.rate.load(.monotonic);
    try expect(io <= 500_000 and io >= 400_000);
    try near(a.cpu.rate.load(.monotonic), 32 * hz());

    // All of it and more: nothing left until the next tick.
    meter.shared.charge(.io, .fg, 5_000_000, t0 + 1500);
    a.touch(t0 + 1500);
    ts.tick(t0 + 2000);
    try expectEqual(0, a.io.rate.load(.monotonic));
    try near(a.cpu.rate.load(.monotonic), 32 * hz());
}

test "tenant: a charge moves the bucket, the totals and the stamp" {
    var t = Tenant.init(7, plan(2), 0);
    try expectEqual(2 * hz(), t.cpu.rate.load(.monotonic));
    const burst = meter.burstFor(2 * hz());
    try expectEqual(burst, t.cpu.bucket(.fg).balance());

    // No floor: three bursts charged leave two owed.
    t.charge(.cpu, .fg, burst * 3, 5);
    try expectEqual(-2 * burst, t.cpu.bucket(.fg).balance());
    try expectEqual(burst * 3, t.totals.cpu_ticks.load(.monotonic));
    try expectEqual(5, t.last_active_ms.load(.monotonic));
    try expect(t.isActive(5 + Tenant.active_window_ms - 1));
    try expect(!t.isActive(5 + Tenant.active_window_ms));

    t.charge(.io, .bg, 40, 6);
    try expectEqual(40, t.totals.io_pages.load(.monotonic));
    try expectEqual(meter.burstFor(20_000) - 40, t.io.bucket(.bg).balance());

    t.holdMem(300);
    t.holdMem(200);
    t.releaseMem(400);
    try expectEqual(100, t.mem_held.load(.monotonic));
    try expectEqual(500, t.totals.mem_peak.load(.monotonic));

    t.touch(9);
    t.failed();
    try expectEqual(9, t.last_active_ms.load(.monotonic));
    try expectEqual(1, t.totals.statements.load(.monotonic));
    try expectEqual(1, t.totals.failed.load(.monotonic));
}

/// A tenant at 10_000 pages/s on the real coarse clock, since
/// `acquire` reads it: a burst of 1000 pages, 100 ms of rate.
fn ioTenant(id: u64) Tenant {
    return Tenant.init(id, .{
        .cpu_milli = vcpu,
        .io_pages_per_s = 10_000,
        .mem_bytes = gib,
    }, utils.coarseMs());
}

test "acquire: fg is granted at once and pays its debt before the next" {
    var t = ioTenant(8);
    const grants = meter.statsFor(.io, .fg).grants.load(.monotonic);

    // 3000 pages at once: 2000 owed, 200 ms at the rate.
    const start = utils.coarseMs();
    t.acquire(.io, .fg, 3000);
    try expect(utils.coarseMs() - start < 50);
    try expect(t.io.bucket(.fg).balance() <= -1900);

    // The next waits the debt out. A lagging clock under load may
    // sleep a little more, never less.
    t.acquire(.io, .fg, 1);
    const waited = utils.coarseMs() - start;
    try expect(waited >= 150 and waited <= 1500);
    try expect(t.totals.parked_ms.load(.monotonic) >= 150);
    try expectEqual(3001, t.totals.io_pages.load(.monotonic));
    try expect(meter.statsFor(.io, .fg).grants.load(.monotonic) >= grants + 2);
}

test "acquire: bg in debt waits until fg is full and its own debt paid" {
    var t = ioTenant(9);

    // bg has its burst from the start and takes one more: in debt by
    // one. fg goes 2000 into debt.
    t.acquire(.io, .bg, 1001);
    try expect(t.io.bucket(.bg).balance() <= -1);
    t.acquire(.io, .fg, 3000);

    // fg's 2000 debt and 1000 room fill first, then bg's one: about
    // 300 ms.
    const start = utils.coarseMs();
    t.acquire(.io, .bg, 1);
    const waited = utils.coarseMs() - start;
    try expect(waited >= 250 and waited <= 2000);
    try expect(t.io.bucket(.fg).balance() >= 0);
}

/// A tenant on the real clock whose cpu is `milli` millicores.
fn cpuTenant(id: u64, milli: i64) Tenant {
    return Tenant.init(id, .{
        .cpu_milli = milli,
        .io_pages_per_s = 1_000_000,
        .mem_bytes = gib,
    }, utils.coarseMs());
}

/// Spins for `ms` on the fiber's own stack, never yielding. On the
/// system clock: the coarse one is advanced by the worker loop, which
/// a spinning fiber keeps from running.
fn burn(ms: i64) void {
    const until = utils.timestampNow(.monotonic, .millis) + ms;
    while (utils.timestampNow(.monotonic, .millis) < until) {
        std.atomic.spinLoopHint();
    }
}

test "cpu: the worker loop charges each step to the fiber's tenant" {
    var t = cpuTenant(20, 32 * vcpu);
    const ctx = fiber.ctx();
    ctx.tenant = &t;
    defer ctx.tenant = &meter.shared;

    // A step is charged when it ends: after a yield the spin before
    // it is on the tenant, at about one core's rate.
    burn(20);
    fiber.yield();
    const after_step = t.totals.cpu_ticks.load(.monotonic);
    try expect(after_step > 0);

    // A yield point charges the step so far without ending it.
    burn(20);
    try ctx.maybeYield();
    try expect(t.totals.cpu_ticks.load(.monotonic) > after_step);
    const ticks = t.totals.cpu_ticks.load(.monotonic);
    const spent = @divTrunc(ticks * 1000, hz());
    try expect(spent >= 35 and spent <= 400);
    try expect(t.cpu.bucket(.fg).balance() < t.cpu.burst.load(.monotonic));
}

test "cpu: maybeYield parks a tenant in debt until its rate covers it" {
    // A tenth of a core: 100 ms of cpu a second, a burst of 10 ms.
    var t = cpuTenant(21, 100);
    const ctx = fiber.ctx();
    ctx.tenant = &t;
    defer ctx.tenant = &meter.shared;

    // 40 ms of cpu on a 10 ms burst, charged by the yield itself:
    // about 30 ms in debt, 300 ms at the rate, paid there.
    burn(40);
    const start = utils.coarseMs();
    try ctx.maybeYield();
    const waited = utils.coarseMs() - start;
    try expect(waited >= 200 and waited <= 2000);
    try expect(t.cpu.bucket(.fg).balance() >= 0);
    try expect(t.totals.parked_ms.load(.monotonic) >= 200);

    // Out of debt, the next yield is free.
    const again = utils.coarseMs();
    try ctx.maybeYield();
    try expect(utils.coarseMs() - again < 50);
}

test "payForMem: a tenant over its share waits only under pressure" {
    var t = Tenant.init(41, meter.default_limit, 0);
    t.mem_share.store(1000, .monotonic);
    t.holdMem(2000);
    defer t.releaseMem(2000);

    // Over the share with the cache still able to give: no wait.
    meter.mem_pressure.store(false, .monotonic);
    var start = utils.coarseMs();
    try t.payForMem();
    try expect(utils.coarseMs() - start < 50);

    // Under pressure: waits the bound out, then refuses.
    meter.mem_wait_ms = 100;
    defer meter.mem_wait_ms = 5000;
    meter.mem_pressure.store(true, .monotonic);
    defer meter.mem_pressure.store(false, .monotonic);
    start = utils.coarseMs();
    try std.testing.expectError(error.MemShare, t.payForMem());
    // The coarse clock can read a ms under the sleeps it measures.
    const waited = utils.coarseMs() - start;
    try expect(waited >= 90 and waited <= 2000);
    try expect(t.totals.parked_ms.load(.monotonic) >= 100);

    // Room given back meanwhile: admitted.
    t.releaseMem(1500);
    defer t.holdMem(1500);
    try t.payForMem();
    // The shared tenant never waits.
    try meter.shared.payForMem();
}
