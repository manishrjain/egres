//! I/O operations that cooperatively park the calling fiber on a
//! libxev completion. Each op follows the same pattern:
//!   1. Place an `xev.Completion` + `Waiter` on the caller's stack.
//!   2. Submit the op to the current worker's loop with a callback
//!      that wakes the fiber.
//!   3. Park the fiber. The loop's poll in `Worker.run` will fire
//!      the callback when the op completes, which schedules the
//!      fiber back onto the ready queue.
//!
//! The completion / waiter live on the parked fiber's stack — that
//! stack stays alive across `park`, so the pointers libxev holds
//! remain valid until the op completes and the function returns.

const std = @import("std");
const linux = std.os.linux;
const xev = @import("xev");
const fb = @import("fiber.zig");
const rt = @import("runtime.zig");

const Fiber = fb.Fiber;

// ---------- sleep ----------

pub fn sleep(ms: u64) void {
    const w = rt.currentWorker();
    const f = w.current_fiber.?;

    var completion: xev.Completion = undefined;
    var waiter: TimerWaiter = .{ .fiber = f };

    const timer = xev.Timer.init() catch |err| {
        std.debug.panic("xev.Timer.init failed: {}", .{err});
    };
    // `Worker.run` refreshes `loop.cached_now` once per outer-loop
    // iteration so any libxev op a fiber issues (this `timer.run`,
    // TCP read/write, …) sees a current monotonic baseline.
    timer.run(&w.loop, &completion, ms, TimerWaiter, &waiter, timerCallback);

    // Loop park on `fired`. A bare `park()` is unsafe here for two
    // reasons: (1) the fiber may already have a stale `awaken` token
    // deposited by a previous wake — `park()` would consume it and
    // return without yielding, making `sleep(N)` return in ~0 ms; and
    // (2) returning early while the timer SQE is still in flight
    // leaves the libxev callback with dangling pointers to this
    // function's stack-allocated `completion` + `waiter` (UAR when
    // the timer fires later). Looping until the callback sets
    // `fired` covers both: any spurious wake just re-parks, and the
    // stack stays alive until the callback has actually run.
    while (!waiter.fired.load(.acquire)) {
        rt.park();
    }
}

const TimerWaiter = struct {
    fiber: *Fiber,
    fired: std.atomic.Value(bool) = .init(false),
};

fn timerCallback(
    ud: ?*TimerWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.Timer.RunError!void,
) xev.CallbackAction {
    ud.?.fired.store(true, .release);
    rt.wake(ud.?.fiber);
    return .disarm;
}

// ---------- TCP ----------

pub const Stream = struct {
    tcp: xev.TCP,

    pub fn close(self: *Stream) void {
        _ = linux.close(self.tcp.fd);
    }

    pub fn read(self: *Stream, buf: []u8) xev.ReadError!usize {
        const w = rt.currentWorker();
        const f = w.current_fiber.?;
        var c: xev.Completion = undefined;
        var waiter: ReadWaiter = .{ .fiber = f, .result = 0 };
        self.tcp.read(&w.loop, &c, .{ .slice = buf }, ReadWaiter, &waiter, readCallback);
        while (!waiter.fired.load(.acquire)) rt.park();
        return waiter.result;
    }

    pub fn write(self: *Stream, buf: []const u8) xev.WriteError!usize {
        const w = rt.currentWorker();
        const f = w.current_fiber.?;
        var c: xev.Completion = undefined;
        var waiter: WriteWaiter = .{ .fiber = f, .result = 0 };
        self.tcp.write(&w.loop, &c, .{ .slice = buf }, WriteWaiter, &waiter, writeCallback);
        while (!waiter.fired.load(.acquire)) rt.park();
        return waiter.result;
    }

    /// Write `buf` fully, retrying short writes until everything is sent
    /// or an error occurs. Common case for protocol code that doesn't
    /// want to track partial writes itself.
    pub fn writeAll(self: *Stream, buf: []const u8) xev.WriteError!void {
        var sent: usize = 0;
        while (sent < buf.len) {
            const n = try self.write(buf[sent..]);
            sent += n;
        }
    }
};

pub const Listener = struct {
    tcp: xev.TCP,
    bound_addr: std.Io.net.IpAddress,

    pub fn close(self: *Listener) void {
        _ = linux.close(self.tcp.fd);
    }

    pub fn accept(self: *Listener) xev.AcceptError!Stream {
        const w = rt.currentWorker();
        const f = w.current_fiber.?;
        var c: xev.Completion = undefined;
        var waiter: AcceptWaiter = .{ .fiber = f, .result = undefined };
        self.tcp.accept(&w.loop, &c, AcceptWaiter, &waiter, acceptCallback);
        while (!waiter.fired.load(.acquire)) rt.park();
        const tcp = try waiter.result;
        return .{ .tcp = tcp };
    }
};

pub fn listen(addr: std.Io.net.IpAddress, backlog: u31) !Listener {
    var tcp = try xev.TCP.init(addr);
    errdefer _ = linux.close(tcp.fd);
    try tcp.bind(addr);
    try tcp.listen(backlog);

    // Bind may have assigned an ephemeral port (when addr port == 0);
    // resolve the actual bound address via getsockname so callers
    // (and clients) can target it.
    const bound = try getSockAddr(tcp.fd);
    return .{ .tcp = tcp, .bound_addr = bound };
}

pub fn connect(addr: std.Io.net.IpAddress) !Stream {
    var tcp = try xev.TCP.init(addr);
    errdefer _ = linux.close(tcp.fd);

    const w = rt.currentWorker();
    const f = w.current_fiber.?;
    var c: xev.Completion = undefined;
    var waiter: ConnectWaiter = .{ .fiber = f, .result = {} };
    tcp.connect(&w.loop, &c, addr, ConnectWaiter, &waiter, connectCallback);
    while (!waiter.fired.load(.acquire)) rt.park();
    try waiter.result;
    return .{ .tcp = tcp };
}

// ---------- internals ----------

// Every waiter carries a `fired` flag for the same reason `sleep`
// does (see its comment): callers spin `park` on this flag so any
// stale `awaken` token from a prior wake gets re-absorbed, and the
// stack-allocated `completion` + `waiter` outlive the in-flight
// SQE through to its callback.

const ReadWaiter = struct {
    fiber: *Fiber,
    result: xev.ReadError!usize,
    fired: std.atomic.Value(bool) = .init(false),
};

fn readCallback(
    ud: ?*ReadWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.TCP,
    _: xev.ReadBuffer,
    r: xev.ReadError!usize,
) xev.CallbackAction {
    const w = ud.?;
    w.result = r;
    w.fired.store(true, .release);
    rt.wake(w.fiber);
    return .disarm;
}

const WriteWaiter = struct {
    fiber: *Fiber,
    result: xev.WriteError!usize,
    fired: std.atomic.Value(bool) = .init(false),
};

fn writeCallback(
    ud: ?*WriteWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.TCP,
    _: xev.WriteBuffer,
    r: xev.WriteError!usize,
) xev.CallbackAction {
    const w = ud.?;
    w.result = r;
    w.fired.store(true, .release);
    rt.wake(w.fiber);
    return .disarm;
}

const ConnectWaiter = struct {
    fiber: *Fiber,
    result: xev.ConnectError!void,
    fired: std.atomic.Value(bool) = .init(false),
};

fn connectCallback(
    ud: ?*ConnectWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.TCP,
    r: xev.ConnectError!void,
) xev.CallbackAction {
    const w = ud.?;
    w.result = r;
    w.fired.store(true, .release);
    rt.wake(w.fiber);
    return .disarm;
}

const AcceptWaiter = struct {
    fiber: *Fiber,
    result: xev.AcceptError!xev.TCP,
    fired: std.atomic.Value(bool) = .init(false),
};

fn acceptCallback(
    ud: ?*AcceptWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    r: xev.AcceptError!xev.TCP,
) xev.CallbackAction {
    const w = ud.?;
    w.result = r;
    w.fired.store(true, .release);
    rt.wake(w.fiber);
    return .disarm;
}

fn getSockAddr(fd: linux.fd_t) !std.Io.net.IpAddress {
    var storage: linux.sockaddr.storage = undefined;
    var len: linux.socklen_t = @sizeOf(linux.sockaddr.storage);
    const rc = linux.getsockname(fd, @ptrCast(&storage), &len);
    switch (std.posix.errno(rc)) {
        .SUCCESS => {},
        else => return error.GetSockNameFailed,
    }
    return sockaddrToIpAddress(&storage);
}

fn sockaddrToIpAddress(storage: *const linux.sockaddr.storage) !std.Io.net.IpAddress {
    const sa: *const linux.sockaddr = @ptrCast(storage);
    switch (sa.family) {
        linux.AF.INET => {
            const sin: *const linux.sockaddr.in = @ptrCast(@alignCast(storage));
            const port = std.mem.bigToNative(u16, sin.port);
            const bytes: [4]u8 = @bitCast(sin.addr);
            return .{ .ip4 = .{ .bytes = bytes, .port = port } };
        },
        linux.AF.INET6 => return error.UnsupportedFamily,
        else => return error.UnsupportedFamily,
    }
}
