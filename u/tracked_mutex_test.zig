const std = @import("std");
const zio = @import("zio");
const utils = @import("utils");

const Mutex = utils.TrackedMutex;

// Helper fiber: blocks on `mu.lock(@src())`, then releases. The `@src()`
// here is the child's lock call site — distinct from the parent test's,
// which is what makes the waiter-source-attribution test meaningful.
const Child = struct {
    fn run(mu: *Mutex) void {
        mu.lock(@src()) catch return;
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
    try mu.lock(@src());

    var child = try zio.spawn(Child.run, .{&mu});

    // Spin-yield until the child has entered the slow path. The slow
    // path increments the entry's waiter_count BEFORE suspending, so
    // observing 1 here is a guarantee that the child has already set
    // last_waiter_src.
    while (true) {
        const s = mu.stats() orelse unreachable;
        if (s.waiter_count >= 1) break;
        try zio.yield();
    }

    {
        const s = mu.stats() orelse unreachable;
        try std.testing.expectEqual(@as(u32, 1), s.waiter_count);
        try std.testing.expect(s.holder_src != null);
        try std.testing.expect(s.last_waiter_src != null);
        // Holder was acquired in this test file; waiter src is also in
        // this test file (Child.run is defined above). Sanity-check both
        // sites resolve to file paths that end in this test file's name.
        try std.testing.expect(std.mem.endsWith(u8, s.holder_src.?.file, "tracked_mutex_test.zig"));
        try std.testing.expect(std.mem.endsWith(u8, s.last_waiter_src.?.file, "tracked_mutex_test.zig"));
    }

    mu.unlock();
    child.join();

    // After both releases, holder fields are clear and the waiter count
    // is back to 0. `last_waiter_src` is intentionally NOT cleared — it
    // remains as a breadcrumb pointing at the most recent contender.
    const s = mu.stats() orelse unreachable;
    try std.testing.expect(s.holder_src == null);
    try std.testing.expectEqual(@as(u32, 0), s.waiter_count);
}

test "TrackedMutex.dump emits a line for a registered mutex" {
    var mu: Mutex = .{};
    defer mu.deinit();
    try mu.lock(@src());
    defer mu.unlock();

    var body: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer body.deinit();
    try utils.tracked_mutex.dump(&body.writer);
    const text = body.writer.buffered();

    // The dump format reserves "lock=" as the line prefix and includes
    // "held_by=" for any currently-held lock. Both must appear.
    try std.testing.expect(std.mem.indexOf(u8, text, "lock=") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "held_by=") != null);
}
