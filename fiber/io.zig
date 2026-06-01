//! Timer-based `sleep` that cooperatively parks the calling fiber on a
//! libxev timer completion. (TCP/file I/O live in `net.zig` / `fs.zig`.)

const std = @import("std");
const xev = @import("xev");
const fb = @import("fiber.zig");
const rt = @import("runtime.zig");

const Fiber = fb.Fiber;

pub fn sleep(ms: u64) void {
    const w = rt.currentWorker();
    const f = w.current_fiber.?;

    var completion: xev.Completion = undefined;
    var waiter: TimerWaiter = .{ .fiber = f };

    const timer = xev.Timer.init() catch |err| {
        std.debug.panic("xev.Timer.init failed: {}", .{err});
    };
    timer.run(&w.loop, &completion, ms, TimerWaiter, &waiter, timerCallback);

    rt.park();
}

const TimerWaiter = struct {
    fiber: *Fiber,
};

fn timerCallback(
    ud: ?*TimerWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.Timer.RunError!void,
) xev.CallbackAction {
    rt.wake(ud.?.fiber);
    return .disarm;
}
