const std = @import("std");
const fiber = @import("fiber");
const utils = @import("utils");

const Mutex = utils.TrackedMutex;

// Helper fiber: blocks on `mu.lock(@src())`, then releases. The `@src()`
// here is the child's lock call site — distinct from the parent test's,
// which is what makes the waiter-source-attribution test meaningful.
const Child = struct {
    fn run(mu: *Mutex) void {
        mu.lock(@src());
        defer mu.unlock();
    }
};

test "TrackedMutex captures holder src and waiter count under contention" {
    var mu: Mutex = .{};
    // deinit() returns the pool entry so this test doesn't leak slots
    // into the static pool across test-suite reruns.
    defer mu.deinit();

    // Parent fiber takes the lock first. After this, the entry has a
    // non-null `holder_src` pointing at the line below.
    mu.lock(@src());

    var child = try fiber.spawn(.{}, Child.run, .{&mu});

    // Spin-yield until the child has entered the slow path. The slow
    // path increments the entry's waiter_count BEFORE suspending, so
    // observing 1 here is a guarantee that the child has already set
    // last_waiter_src.
    const e = mu.entry.load(.acquire) orelse unreachable;
    while (e.waiter_count.load(.monotonic) < 1) fiber.yield();

    try std.testing.expectEqual(@as(u32, 1), e.waiter_count.load(.monotonic));
    try std.testing.expect(e.holder_src != null);
    try std.testing.expect(e.last_waiter_src != null);
    // Holder was acquired in this test file; waiter src is also in
    // this test file (Child.run is defined above). Sanity-check both
    // sites resolve to file paths that end in this test file's name.
    try std.testing.expect(std.mem.endsWith(u8, e.holder_src.?.file, "tracked_mutex_test.zig"));
    try std.testing.expect(std.mem.endsWith(u8, e.last_waiter_src.?.file, "tracked_mutex_test.zig"));

    mu.unlock();
    child.join();

    // After both releases, holder fields are clear and the waiter count
    // is back to 0. `last_waiter_src` is intentionally NOT cleared — it
    // remains as a breadcrumb pointing at the most recent contender.
    try std.testing.expect(e.holder_src == null);
    try std.testing.expectEqual(@as(u32, 0), e.waiter_count.load(.monotonic));
}

test "TrackedMutex.dump emits a line for a registered mutex" {
    var mu: Mutex = .{};
    defer mu.deinit();
    mu.lock(@src());
    defer mu.unlock();

    var body: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer body.deinit();
    try utils.tracked_mutex.dump(&body.writer);
    const text = body.writer.buffered();

    // Each dump line starts with "held_by="; for any currently-held lock,
    // that field carries a file:line pointing at the acquire site.
    try std.testing.expect(std.mem.indexOf(u8, text, "held_by=") != null);
}
