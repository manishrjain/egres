const std = @import("std");
const fiber = @import("fiber");

fn recordWorker(slot: *u8) void {
    slot.* = fiber.currentWorker().id;
}

test "Placement: .raft lane with zero workers falls back to .default" {
    var observed: u8 = 0;
    const h = try fiber.spawn(.{ .lane = .raft }, recordWorker, .{&observed});
    h.join();

    // Tests run with LaneConfig{} (raft = 0). A .raft spawn must
    // land in the .default range; otherwise pickWorker would divide
    // by zero on an empty range.
    const default_range = fiber.currentRuntime().lane_ranges[@intFromEnum(fiber.Lane.default)];
    try std.testing.expect(observed >= default_range.start);
    try std.testing.expect(observed < default_range.end);
}
