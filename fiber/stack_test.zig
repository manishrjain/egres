const std = @import("std");
const fiber = @import("fiber");

// Regression guard for fiber stack growth: large frames must grow the stack
// correctly even when the fiber migrates threads (park/wake) and many fibers
// fault concurrently. A ~4 KiB `std.posix.toPosixPath` return-by-value frame
// at depth was once suspected of a growth-handler SEGV under load; this proves
// the handler copes (it does — in Debug and ReleaseFast).

fn deep(n: u32, acc: *u64) void {
    var pad: [1024]u8 = undefined;
    pad[0] = @truncate(n);
    pad[1000] = @truncate(n *% 7);
    std.mem.doNotOptimizeAway(&pad);
    if (n > 0) return deep(n - 1, acc);
    const pz = std.posix.toPosixPath("/tmp/fiber/stack/regression/path/x") catch return;
    acc.* +%= pz[0];
}

fn body(acc: *u64) void {
    var r: u32 = 0;
    while (r < 6) : (r += 1) {
        fiber.sleep(1); // park + likely migrate to another worker
        deep(24, acc); // ~24 KiB recursion + ~8 KiB toPosixPath frame, well past the 16 KiB commit
    }
}

test "stack: large frames grow correctly across migration + concurrent faults" {
    const N = 64;
    var accs = [_]u64{0} ** N;
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N);
    for (&accs) |*a| try g.spawn(.{}, body, .{a});
    g.wait();
    for (accs) |a| try std.testing.expectEqual(@as(u64, '/') * 6, a);
}
