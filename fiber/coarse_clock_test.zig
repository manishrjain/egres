const std = @import("std");
const fiber = @import("fiber");

test "CoarseClock: ms() advances during a parked fiber's sleep" {
    const before = fiber.coarseMs();
    fiber.sleep(30);
    const after = fiber.coarseMs();
    const observed_delta = after - before;

    // The ticker fires once a millisecond regardless of whether the
    // fiber is running, so the coarse clock should reflect ~30 ms of
    // wall passing during the sleep. Generous bounds for CI noise.
    try std.testing.expect(observed_delta >= 25);
    try std.testing.expect(observed_delta < 250);
}

test "CoarseClock: ms() is stable across many cheap reads" {
    var last = fiber.coarseMs();
    // Read the clock many times; verify monotonicity. Without a
    // sleep, all reads land between two ticks and should return the
    // same cached value — but even across a yield, the value must
    // never go backwards.
    for (0..1000) |_| {
        const now = fiber.coarseMs();
        try std.testing.expect(now >= last);
        last = now;
        fiber.yield();
    }
}
