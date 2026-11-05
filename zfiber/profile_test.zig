// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");
const fiber = @import("zfiber");
const utils = @import("utils");

/// CPU to be sampled: a loop the optimizer cannot remove. `noinline`
/// so it has a frame of its own to find in the profile.
noinline fn burn(ms: u64) u64 {
    const end = utils.timestampNow(.monotonic, .millis) +
        @as(i64, @intCast(ms));
    var x: u64 = 0x9e3779b97f4a7c15;
    while (utils.timestampNow(.monotonic, .millis) < end) {
        var i: u32 = 0;
        while (i < 200_000) : (i += 1) {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
        }
    }
    return x;
}

const Body = struct {
    fn run(out: *u64) void {
        out.* = burn(400);
    }
};

/// Whether a hex frame in the folded output lies in `burn`: at or past
/// its entry, within the few hundred bytes the loop compiles to.
fn inBurn(frame: []const u8) bool {
    const addr = std.fmt.parseInt(usize, frame, 0) catch return false;
    const start = @intFromPtr(&burn);
    return addr >= start and addr < start + 4096;
}

// The tests write raw addresses: symbolizing means the standard
// library's scan of this binary's DWARF, minutes for a Debug test
// binary. The server's admin command symbolizes.

/// There is one profiler per process and the tests run in parallel:
/// wait for the other test's run to finish rather than fail on it.
fn startWhenFree(alloc: std.mem.Allocator, opts: fiber.profile.Options) !void {
    while (true) {
        fiber.profile.start(alloc, opts) catch |e| switch (e) {
            error.AlreadyRunning => {
                fiber.sleepMs(10);
                continue;
            },
            else => return e,
        };
        return;
    }
}

test "profile: samples a CPU-bound fiber, leaf frames inside it" {
    if (builtin.os.tag != .linux and builtin.os.tag != .macos)
        return error.SkipZigTest;
    // `burn` spins until the clock moves, which virtual time never does
    // under a fiber that holds the worker.
    if (fiber.isSim()) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    try startWhenFree(alloc, .{ .hz = 500, .secs = 2 });
    try std.testing.expect(fiber.profile.running());

    var sink: u64 = 0;
    var h = try fiber.spawn(.{}, Body.run, .{&sink});
    h.join();

    var body: std.Io.Writer.Allocating = .init(alloc);
    defer body.deinit();
    const report = try fiber.profile.stop(&body.writer, .{
        .folded = .{ .symbolize = false },
    });
    try std.testing.expect(!fiber.profile.running());
    try std.testing.expect(sink != 0);
    // 400 ms of one thread at 500 Hz: about 200 samples; a loaded box
    // may take some, never all.
    try std.testing.expect(report.samples >= 20);
    try std.testing.expect(report.stacks >= 1);

    // Each line: frames root first joined by ';', a space, the count.
    // The counts add up to the samples kept, and a good share of the
    // stacks pass through `burn`: 400 ms at 500 Hz is about 200, but
    // the sampler covers every worker, so other tests running beside
    // this one add stacks of their own.
    var total: usize = 0;
    var in_burn: usize = 0;
    var lines = std.mem.splitScalar(u8, body.writer.buffered(), '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const sp = std.mem.lastIndexOfScalar(u8, line, ' ') orelse
            return error.TestExpectedCount;
        const count = try std.fmt.parseInt(usize, line[sp + 1 ..], 10);
        total += count;
        var frames = std.mem.splitScalar(u8, line[0..sp], ';');
        while (frames.next()) |f| {
            if (inBurn(f)) {
                in_burn += count;
                break;
            }
        }
    }
    try std.testing.expectEqual(report.samples - report.dropped, total);
    try std.testing.expect(in_burn >= 20);

    // A second start after a stop works: the handler and timers were
    // put back.
    try startWhenFree(alloc, .{ .hz = 100, .secs = 1 });
    var scratch: std.Io.Writer.Allocating = .init(alloc);
    defer scratch.deinit();
    _ = try fiber.profile.stop(&scratch.writer, .{
        .folded = .{ .symbolize = false },
    });
}

test "profile: a second start while running is refused" {
    if (builtin.os.tag != .linux and builtin.os.tag != .macos)
        return error.SkipZigTest;
    const alloc = std.testing.allocator;
    try startWhenFree(alloc, .{ .hz = 100, .secs = 1 });
    try std.testing.expectError(
        error.AlreadyRunning,
        fiber.profile.start(alloc, .{ .hz = 100, .secs = 1 }),
    );
    var scratch: std.Io.Writer.Allocating = .init(alloc);
    defer scratch.deinit();
    _ = try fiber.profile.stop(&scratch.writer, .{
        .folded = .{ .symbolize = false },
    });
}
