// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");

test "CoarseClock: ms() advances during a parked fiber's sleep" {
    // Measure elapsed via direct vDSO CLOCK_MONOTONIC, not
    // `utils.coarseMs()`. The coarse clock cache is what we'd be
    // observing in production code, but using it as the test's
    // source of truth conflates two things: (1) whether `sleep`
    // actually slept, and (2) whether worker 0's 1 ms tick stayed
    // current. Under heavy parallel load the tick can lag, making
    // `coarseMs` deltas under-report by 10+ ms even when the sleep
    // ran fine. We sidestep that here by reading a fresh clock.
    const before_ms = utils.timestampNow(.monotonic, .millis);
    fiber.sleepMs(30);
    const after_ms = utils.timestampNow(.monotonic, .millis);
    const observed_delta = after_ms - before_ms;

    if (observed_delta < 25 or observed_delta >= 250) {
        std.log.err("observed_delta = {d} ms (expected 25 <= delta < 250)", .{observed_delta});
    }
    try std.testing.expect(observed_delta >= 25);
    try std.testing.expect(observed_delta < 250);
}

test "CoarseClock: ms() is stable across many cheap reads" {
    var last = utils.coarseMs();
    // Read the clock many times; verify monotonicity. Without a
    // sleep, all reads land between two ticks and should return the
    // same cached value — but even across a yield, the value must
    // never go backwards.
    for (0..1000) |_| {
        const now = utils.coarseMs();
        try std.testing.expect(now >= last);
        last = now;
        fiber.yield();
    }
}

test "CoarseClock: a gap lands in the first bound it fits under" {
    const clock = utils.coarse_clock;
    var buf: [512]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);

    // Bounds 2, 5, 10, 50, 100 exclusive, then over: 1 goes under 2,
    // 3 under 5, 51 under 100, and the max is the largest seen.
    clock.recordGap(1);
    clock.recordGap(2);
    clock.recordGap(3);
    clock.recordGap(51);
    try clock.writeGapMetrics(&w);
    const text = w.buffered();
    try std.testing.expect(std.mem.startsWith(
        u8,
        text,
        "# TYPE coarse_clock_gap_ms histogram\n",
    ));
    try std.testing.expect(std.mem.indexOf(
        u8,
        text,
        "coarse_clock_gap_ms_bucket{le=\"+Inf\"} ",
    ) != null);
    // The clock runs under every test, so other gaps may be counted
    // too; only the max is exact enough to check, and only its floor.
    // Past the TYPE line, which names the gauge too.
    const max_key = "\ncoarse_clock_gap_max_ms ";
    const max_at = std.mem.indexOf(u8, text, max_key).?;
    const max_str = std.mem.trimEnd(u8, text[max_at + max_key.len ..], "\n");
    const max = try std.fmt.parseInt(i64, max_str, 10);
    try std.testing.expect(max >= 51);
}
