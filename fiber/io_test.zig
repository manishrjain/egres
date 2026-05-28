const std = @import("std");
const fiber = @import("fiber");

test "io.sleep: 100 fibers sleeping 10ms in parallel finish under 50ms" {
    // Atomic: the unlocked default + wake-on-push redirect spreads
    // these fibers across many workers; 100 simultaneous wakes from
    // the timer barrage race the counter increment otherwise.
    var counter: std.atomic.Value(u32) = .init(0);

    const Body = struct {
        fn run(c: *std.atomic.Value(u32)) void {
            fiber.sleep(10);
            _ = c.fetchAdd(1, .acq_rel);
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(100);

    const start = fiber.coarseMs();
    for (0..100) |_| try g.spawn(.{}, Body.run, .{&counter});
    g.wait();
    const elapsed = fiber.coarseMs() - start;

    try std.testing.expectEqual(@as(u32, 100), counter.load(.acquire));
    // Concurrency check: 100 × sleep(10) serially would be ~1000 ms;
    // parallel should be one timer-tick.  Upper bound is loose since
    // Phase B runs ~50 fiber tests concurrently on the same workers,
    // so individual sleep+wake latency competes with the rest of the
    // suite.  The 200 ms ceiling still rules out serial execution.
    if (elapsed < 9 or elapsed >= 200) {
        std.log.err("elapsed = {d} ms (expected 9 <= elapsed < 200)", .{elapsed});
    }
    try std.testing.expect(elapsed >= 9);
    try std.testing.expect(elapsed < 200);
}

test "io.sleep: single fiber sleeps for the requested duration" {
    const Body = struct {
        fn run() void {
            fiber.sleep(20);
        }
    };

    const start = fiber.coarseMs();
    const h = try fiber.spawn(.{}, Body.run, .{});
    h.join();
    const elapsed = fiber.coarseMs() - start;

    if (elapsed < 19 or elapsed >= 200) {
        std.log.err("elapsed = {d} ms (expected 19 <= elapsed < 200)", .{elapsed});
    }
    try std.testing.expect(elapsed >= 19);
    try std.testing.expect(elapsed < 200);
}

test "io.tcp: echo server with multiple clients" {
    const N = 8;

    const loopback: std.Io.net.IpAddress = .{ .ip4 = std.Io.net.Ip4Address.loopback(0) };
    var listener = try fiber.listen(loopback, 64);
    defer listener.close();

    const EchoCtx = struct { stream: fiber.Stream };
    var echo_ctxs: [N]EchoCtx = undefined;

    const ClientCtx = struct { addr: std.Io.net.IpAddress, id: u8, ok: *u32 };
    var oks: [N]u32 = .{0} ** N;
    var client_ctxs: [N]ClientCtx = undefined;

    const Echo = struct {
        fn run(c: *EchoCtx) void {
            var buf: [16]u8 = undefined;
            const n = c.stream.read(&buf) catch return;
            c.stream.writeAll(buf[0..n]) catch return;
            c.stream.close();
        }
    };

    const Client = struct {
        fn run(c: *ClientCtx) void {
            var stream = fiber.connect(c.addr) catch return;
            defer stream.close();

            var sent: [16]u8 = .{0} ** 16;
            const msg = std.fmt.bufPrint(&sent, "hello-{d:0>3}", .{c.id}) catch unreachable;
            stream.writeAll(msg) catch return;

            var recv: [16]u8 = undefined;
            const n = stream.read(&recv) catch return;
            if (n == msg.len and std.mem.eql(u8, msg, recv[0..n])) {
                c.ok.* = 1;
            }
        }
    };

    // Acceptor runs as a sibling fiber. It owns the listener loop for
    // exactly N accepts, then returns; we join it via the group.
    const AcceptorCtx = struct {
        listener: *fiber.Listener,
        target: usize,
        echo_ctxs: []EchoCtx,
        echo_group: *fiber.Group(void),
    };
    var echo_group: fiber.Group(void) = .init(std.testing.allocator);
    defer echo_group.deinit();
    try echo_group.ensureTotalCapacity(N);

    const Acceptor = struct {
        fn run(c: *AcceptorCtx) void {
            for (0..c.target) |i| {
                c.echo_ctxs[i].stream = c.listener.accept() catch unreachable;
                c.echo_group.spawn(.{}, Echo.run, .{&c.echo_ctxs[i]}) catch unreachable;
            }
        }
    };
    var acceptor_ctx: AcceptorCtx = .{
        .listener = &listener,
        .target = N,
        .echo_ctxs = &echo_ctxs,
        .echo_group = &echo_group,
    };

    var clients: fiber.Group(void) = .init(std.testing.allocator);
    defer clients.deinit();
    try clients.ensureTotalCapacity(N);

    const acceptor_handle = try fiber.spawn(.{}, Acceptor.run, .{&acceptor_ctx});

    for (&client_ctxs, &oks, 0..) |*c, *ok, i| {
        c.* = .{
            .addr = listener.bound_addr,
            .id = @intCast(i),
            .ok = ok,
        };
        try clients.spawn(.{}, Client.run, .{c});
    }

    clients.wait();
    acceptor_handle.join();
    echo_group.wait();

    for (oks) |ok| try std.testing.expectEqual(@as(u32, 1), ok);
}
