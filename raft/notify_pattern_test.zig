//! Regression test for the lost-wakeup pattern that bit raft's applyLoop /
//! callbackLoop / proposeAndWaitForApply when they used the bare
//! `Notify.wait()` API.
//!
//! The bug: `zio.Notify` is stateless — `broadcast()` against an empty wait
//! queue is a no-op (the signal is dropped). With the naive
//!     check_state();
//!     if (no work) notify.wait();
//! pattern, a broadcast that fires between the state check and `wait()`'s
//! internal queue push is lost; the next `wait()` parks forever.
//!
//! The fix uses zio's `asyncWait` / `Waiter.wait` two-phase API to flip the
//! order: register first, then re-check state. Once the waiter is on the
//! queue, ANY subsequent broadcast will signal it. The state recheck either
//! sees the new state (we unsubscribe and proceed) or sees old state (we
//! suspend, and the eventual broadcast wakes us). No third case where we
//! both miss the broadcast AND see stale state.

const std = @import("std");
const zio = @import("zio");

// Pattern: subscribe → busy gap → suspend. A broadcast that fires during
// the busy gap MUST land on `w` (asyncWait queued it before the gap started),
// so the subsequent `w.wait` returns immediately.
test "asyncWait queues before suspend so broadcast in busy gap isn't lost" {
    const runtime = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer runtime.deinit();

    const Ctx = struct {
        notify: zio.Notify = .init,
        subscribed: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),

        fn waiter(self: *@This()) !void {
            var w: zio.Waiter = .init();
            _ = self.notify.asyncWait(&w);
            self.subscribed.store(true, .release);
            // Busy gap — the broadcaster fires SOMEWHERE in here. If
            // asyncWait actually queued us, the signal sets w's flag.
            for (0..32) |_| try zio.yield();
            try w.wait(1, .allow_cancel);
            self.done.store(true, .release);
        }

        fn broadcaster(self: *@This()) !void {
            while (!self.subscribed.load(.acquire)) try zio.yield();
            self.notify.broadcast();
        }
    };

    const root = struct {
        fn run(rt: *zio.Runtime) !void {
            var ctx = Ctx{};
            var grp = rt.group();
            defer grp.cancel();
            try grp.spawn(Ctx.waiter, .{&ctx});
            try grp.spawn(Ctx.broadcaster, .{&ctx});
            try grp.wait();
            try std.testing.expect(!grp.hasFailed());
            try std.testing.expect(ctx.done.load(.acquire));
        }
    }.run;

    var handle = try runtime.spawn(root, .{runtime});
    try handle.join();
}

// The other half: a broadcast that fires AFTER `w.wait()` is already
// suspended must also wake the waiter. (This is the "happy path" that
// the bare `Notify.wait()` already handled correctly — included so the
// regression test covers both sides of the busy-gap race.)
test "asyncWait wakes when broadcast fires AFTER wait suspends" {
    const runtime = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer runtime.deinit();

    const Ctx = struct {
        notify: zio.Notify = .init,
        suspended: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),

        fn waiter(self: *@This()) !void {
            var w: zio.Waiter = .init();
            _ = self.notify.asyncWait(&w);
            self.suspended.store(true, .release);
            try w.wait(1, .allow_cancel);
            self.done.store(true, .release);
        }

        fn broadcaster(self: *@This()) !void {
            while (!self.suspended.load(.acquire)) try zio.yield();
            // Give the waiter a tick to actually park inside `w.wait`.
            try zio.sleep(.fromMilliseconds(5));
            self.notify.broadcast();
        }
    };

    const root = struct {
        fn run(rt: *zio.Runtime) !void {
            var ctx = Ctx{};
            var grp = rt.group();
            defer grp.cancel();
            try grp.spawn(Ctx.waiter, .{&ctx});
            try grp.spawn(Ctx.broadcaster, .{&ctx});
            try grp.wait();
            try std.testing.expect(!grp.hasFailed());
            try std.testing.expect(ctx.done.load(.acquire));
        }
    }.run;

    var handle = try runtime.spawn(root, .{runtime});
    try handle.join();
}
