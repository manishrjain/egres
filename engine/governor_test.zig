// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const engine = @import("engine");
const zdb = @import("zdb");

const Governor = engine.governor.Governor;
const gib: u64 = 1 << 30;

test "governor: the margin rises to what the rest holds at once" {
    var cache = try zdb.cache.ShardedCache(u128).init(
        std.testing.allocator,
        1 << 20,
        1,
    );
    defer cache.deinit();
    var gov = Governor.init(48 * gib, &cache, null);
    gov.ratio.cur = 1.0; // no allocator overhead: live target = total

    // Little held elsewhere: the margin stays at its floor.
    const floor_cap = 48 * gib - Governor.min_margin;
    try std.testing.expectEqual(floor_cap, gov.nextCap(100 << 20));

    // 20 GiB held elsewhere: the margin is 20 GiB this tick.
    try std.testing.expectEqual(28 * gib, gov.nextCap(20 * gib));
    try std.testing.expectEqual(20 * gib, gov.margin);

    // Everything held elsewhere: the floor.
    try std.testing.expectEqual(Governor.min_cap, gov.nextCap(70 * gib));
}

test "governor: the margin falls back over minutes" {
    var cache = try zdb.cache.ShardedCache(u128).init(
        std.testing.allocator,
        1 << 20,
        1,
    );
    defer cache.deinit();
    var gov = Governor.init(48 * gib, &cache, null);
    gov.ratio.cur = 1.0;
    _ = gov.nextCap(20 * gib);

    // Back to 4 GiB held: one tick closes `decay_ticks`th of the gap.
    const cap = gov.nextCap(4 * gib);
    const step = 16 * gib / Governor.decay_ticks;
    try std.testing.expectEqual(20 * gib - step, gov.margin);
    try std.testing.expectEqual(48 * gib - gov.margin, cap);

    // Ten minutes of ticks: within 16 MiB of what is held, never under.
    for (0..6000) |_| _ = gov.nextCap(4 * gib);
    try std.testing.expect(gov.margin >= 4 * gib);
    try std.testing.expect(gov.margin - 4 * gib < 16 << 20);
}

test "governor: the ratio rises at once and falls back as the margin does" {
    var cache = try zdb.cache.ShardedCache(u128).init(
        std.testing.allocator,
        1 << 20,
        1,
    );
    defer cache.deinit();
    var gov = Governor.init(66 * gib, &cache, null);

    // Unmeasured: the default ratio, so 60 GiB of live bytes.
    try std.testing.expectEqual(engine.governor.Ratio.default, gov.ratio.cur);
    try expectNear(60 * gib, gov.liveTarget());

    // A higher reading takes over at once, and the live target and the
    // cap shrink with it: 66 / 1.2 = 55 GiB, less the margin's floor.
    gov.ratio.measured = 1.2;
    gov.ratio.update();
    try std.testing.expectEqual(@as(f64, 1.2), gov.ratio.cur);
    try expectNear(55 * gib, gov.liveTarget());
    try std.testing.expectEqual(
        gov.liveTarget() - Governor.min_margin,
        gov.nextCap(0),
    );

    // A lower one is approached `decay_ticks`th of the gap per tick:
    // within 0.001 after ten minutes of ticks, never under it.
    gov.ratio.measured = 1.05;
    gov.ratio.update();
    try std.testing.expect(gov.ratio.cur < 1.2 and gov.ratio.cur > 1.19);
    for (0..6000) |_| gov.ratio.update();
    try std.testing.expect(gov.ratio.cur >= 1.05 and gov.ratio.cur < 1.051);
}

test "governor: the statement pool is a share of the live target" {
    var cache = try zdb.cache.ShardedCache(u128).init(
        std.testing.allocator,
        1 << 20,
        1,
    );
    defer cache.deinit();
    var gov = Governor.init(48 * gib, &cache, null);
    gov.query_share = 0.25;
    gov.ratio.cur = 1.0;
    try expectNear(12 * gib, gov.queryPool());

    // The pool follows the live target, not the cache's cap: 48 / 1.2
    // live, a quarter of it for statements, whatever the margin.
    gov.ratio.measured = 1.2;
    gov.ratio.update();
    try expectNear(10 * gib, gov.queryPool());
    _ = gov.nextCap(30 * gib);
    try expectNear(10 * gib, gov.queryPool());
}

/// `got` within a KiB of `want`: the ratio divides in floating point.
fn expectNear(want: u64, got: u64) !void {
    const diff = if (got > want) got - want else want - got;
    try std.testing.expect(diff < 1024);
}
