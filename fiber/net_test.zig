const std = @import("std");
const fiber = @import("fiber");

const loopback: std.Io.net.IpAddress = .{ .ip4 = std.Io.net.Ip4Address.loopback(0) };

test "net.tcp: raw writeAll/readAll echo" {
    const laddr: fiber.net.IpAddress = .{ .inner = loopback };
    var server = try laddr.listen(.{ .reuse_address = true });
    defer server.close();

    const ServerCtx = struct {
        server: *fiber.net.Server,
        fn run(c: *@This()) void {
            const conn = c.server.accept(.{}) catch return;
            defer conn.close();
            var buf: [5]u8 = undefined;
            conn.readAll(&buf) catch return;
            conn.writeAll(&buf) catch return;
        }
    };
    const ClientCtx = struct {
        addr: fiber.net.IpAddress,
        ok: *bool,
        fn run(c: *@This()) void {
            const conn = c.addr.connect(.{}) catch return;
            defer conn.close();
            conn.writeAll("HELLO") catch return;
            var buf: [5]u8 = undefined;
            conn.readAll(&buf) catch return;
            c.ok.* = std.mem.eql(u8, "HELLO", &buf);
        }
    };

    var ok = false;
    var sctx = ServerCtx{ .server = &server };
    var cctx = ClientCtx{ .addr = .{ .inner = server.bound_addr }, .ok = &ok };
    const sh = try fiber.spawn(.{}, ServerCtx.run, .{&sctx});
    const ch = try fiber.spawn(.{}, ClientCtx.run, .{&cctx});
    ch.join();
    sh.join();

    try std.testing.expect(ok);
}

test "net.tcp: reader/writer std.Io adapter round-trips a length-prefixed frame" {
    const laddr: fiber.net.IpAddress = .{ .inner = loopback };
    var server = try laddr.listen(.{ .reuse_address = true });
    defer server.close();

    const ServerCtx = struct {
        server: *fiber.net.Server,
        got: *[5]u8,
        ok: *bool,
        fn run(c: *@This()) void {
            const conn = c.server.accept(.{}) catch return;
            defer conn.close();
            var rbuf: [64]u8 = undefined;
            var r = conn.reader(&rbuf);
            const ri = &r.interface;
            var hdr: [4]u8 = undefined;
            ri.readSliceAll(&hdr) catch return;
            const len = std.mem.readInt(u32, &hdr, .little);
            if (len != 5) return;
            ri.readSliceAll(c.got) catch return;
            c.ok.* = true;
        }
    };
    const ClientCtx = struct {
        addr: fiber.net.IpAddress,
        fn run(c: *@This()) void {
            const conn = c.addr.connect(.{}) catch return;
            defer conn.close();
            var wbuf: [64]u8 = undefined;
            var wtr = conn.writer(&wbuf);
            const wi = &wtr.interface;
            var hdr: [4]u8 = undefined;
            std.mem.writeInt(u32, &hdr, 5, .little);
            wi.writeAll(&hdr) catch return;
            wi.writeAll("world") catch return;
            wi.flush() catch return;
        }
    };

    var got: [5]u8 = undefined;
    var ok = false;
    var sctx = ServerCtx{ .server = &server, .got = &got, .ok = &ok };
    var cctx = ClientCtx{ .addr = .{ .inner = server.bound_addr } };
    const sh = try fiber.spawn(.{}, ServerCtx.run, .{&sctx});
    const ch = try fiber.spawn(.{}, ClientCtx.run, .{&cctx});
    ch.join();
    sh.join();

    try std.testing.expect(ok);
    try std.testing.expectEqualStrings("world", &got);
}

test "net.tcp: reader adapter takeDelimiterInclusive (internal-buffer fill path)" {
    const laddr: fiber.net.IpAddress = .{ .inner = loopback };
    var server = try laddr.listen(.{ .reuse_address = true });
    defer server.close();

    const ServerCtx = struct {
        server: *fiber.net.Server,
        line1: *[9]u8,
        line2: *[9]u8,
        ok: *bool,
        fn run(c: *@This()) void {
            const conn = c.server.accept(.{}) catch return;
            defer conn.close();
            var rbuf: [64]u8 = undefined;
            var r = conn.reader(&rbuf);
            const ri = &r.interface;
            const l1 = ri.takeDelimiterInclusive('\n') catch return;
            const l2 = ri.takeDelimiterInclusive('\n') catch return;
            if (l1.len != 9 or l2.len != 9) return;
            @memcpy(c.line1, l1);
            @memcpy(c.line2, l2);
            c.ok.* = true;
        }
    };
    const ClientCtx = struct {
        addr: fiber.net.IpAddress,
        fn run(c: *@This()) void {
            const conn = c.addr.connect(.{}) catch return;
            defer conn.close();
            conn.writeAll("line one\nline two\n") catch return;
        }
    };

    var line1: [9]u8 = undefined;
    var line2: [9]u8 = undefined;
    var ok = false;
    var sctx = ServerCtx{ .server = &server, .line1 = &line1, .line2 = &line2, .ok = &ok };
    var cctx = ClientCtx{ .addr = .{ .inner = server.bound_addr } };
    const sh = try fiber.spawn(.{}, ServerCtx.run, .{&sctx});
    const ch = try fiber.spawn(.{}, ClientCtx.run, .{&cctx});
    ch.join();
    sh.join();

    try std.testing.expect(ok);
    try std.testing.expectEqualStrings("line one\n", &line1);
    try std.testing.expectEqualStrings("line two\n", &line2);
}

test "net.IpAddress: parseIpAndPort" {
    const a = try fiber.net.IpAddress.parseIpAndPort("127.0.0.1:5432");
    try std.testing.expectEqual(@as(u16, 5432), a.inner.ip4.port);
    try std.testing.expectEqualSlices(u8, &.{ 127, 0, 0, 1 }, &a.inner.ip4.bytes);
}

test "net.tcp: echo server with multiple concurrent clients" {
    const N = 8;
    const laddr: fiber.net.IpAddress = .{ .inner = loopback };
    var server = try laddr.listen(.{ .reuse_address = true, .backlog = 64 });
    defer server.close();

    const EchoCtx = struct { stream: fiber.net.Stream };
    var echo_ctxs: [N]EchoCtx = undefined;

    const ClientCtx = struct { addr: fiber.net.IpAddress, id: u8, ok: *u32 };
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
            const stream = c.addr.connect(.{}) catch return;
            defer stream.close();
            var sent: [16]u8 = .{0} ** 16;
            const msg = std.fmt.bufPrint(&sent, "hello-{d:0>3}", .{c.id}) catch unreachable;
            stream.writeAll(msg) catch return;
            var recv: [16]u8 = undefined;
            const n = stream.read(&recv) catch return;
            if (n == msg.len and std.mem.eql(u8, msg, recv[0..n])) c.ok.* = 1;
        }
    };

    var echo_group: fiber.Group(void) = .init(std.testing.allocator);
    defer echo_group.deinit();
    try echo_group.ensureTotalCapacity(N);

    const AcceptorCtx = struct {
        server: *fiber.net.Server,
        target: usize,
        echo_ctxs: []EchoCtx,
        echo_group: *fiber.Group(void),
        fn run(c: *@This()) void {
            for (0..c.target) |i| {
                c.echo_ctxs[i].stream = c.server.accept(.{}) catch unreachable;
                c.echo_group.spawn(.{}, Echo.run, .{&c.echo_ctxs[i]}) catch unreachable;
            }
        }
    };
    var acceptor_ctx: AcceptorCtx = .{
        .server = &server,
        .target = N,
        .echo_ctxs = &echo_ctxs,
        .echo_group = &echo_group,
    };

    var clients: fiber.Group(void) = .init(std.testing.allocator);
    defer clients.deinit();
    try clients.ensureTotalCapacity(N);

    const acceptor_handle = try fiber.spawn(.{}, AcceptorCtx.run, .{&acceptor_ctx});
    for (&client_ctxs, &oks, 0..) |*c, *ok, i| {
        c.* = .{ .addr = .{ .inner = server.bound_addr }, .id = @intCast(i), .ok = ok };
        try clients.spawn(.{}, Client.run, .{c});
    }
    clients.wait();
    acceptor_handle.join();
    echo_group.wait();

    for (oks) |ok| try std.testing.expectEqual(@as(u32, 1), ok);
}

test "net.tcp: large payload echo (256 KiB)" {
    const N = 256 * 1024;
    const laddr: fiber.net.IpAddress = .{ .inner = loopback };
    var server = try laddr.listen(.{ .reuse_address = true });
    defer server.close();

    const ServerCtx = struct {
        server: *fiber.net.Server,
        buf: []u8,
        fn run(c: *@This()) void {
            const conn = c.server.accept(.{}) catch return;
            defer conn.close();
            conn.readAll(c.buf) catch return;
            conn.writeAll(c.buf) catch return;
        }
    };
    const ClientCtx = struct {
        addr: fiber.net.IpAddress,
        send: []const u8,
        recv: []u8,
        ok: *bool,
        fn run(c: *@This()) void {
            const conn = c.addr.connect(.{}) catch return;
            defer conn.close();
            // writeAll and readAll run concurrently with the server's, so the
            // socket buffers never deadlock on a payload larger than one.
            conn.writeAll(c.send) catch return;
            conn.readAll(c.recv) catch return;
            c.ok.* = std.mem.eql(u8, c.send, c.recv);
        }
    };

    const send = try std.testing.allocator.alloc(u8, N);
    defer std.testing.allocator.free(send);
    for (send, 0..) |*b, i| b.* = @truncate(i *% 2654435761);
    const srv_buf = try std.testing.allocator.alloc(u8, N);
    defer std.testing.allocator.free(srv_buf);
    const recv = try std.testing.allocator.alloc(u8, N);
    defer std.testing.allocator.free(recv);

    var ok = false;
    var sctx = ServerCtx{ .server = &server, .buf = srv_buf };
    var cctx = ClientCtx{ .addr = .{ .inner = server.bound_addr }, .send = send, .recv = recv, .ok = &ok };
    const sh = try fiber.spawn(.{}, ServerCtx.run, .{&sctx});
    const ch = try fiber.spawn(.{}, ClientCtx.run, .{&cctx});
    ch.join();
    sh.join();
    try std.testing.expect(ok);
}

test "net.tcp: reader/writer adapter streams several frames over one connection" {
    const laddr: fiber.net.IpAddress = .{ .inner = loopback };
    var server = try laddr.listen(.{ .reuse_address = true });
    defer server.close();

    const ServerCtx = struct {
        server: *fiber.net.Server,
        count: *u32,
        fn run(c: *@This()) void {
            const conn = c.server.accept(.{}) catch return;
            defer conn.close();
            var rbuf: [256]u8 = undefined;
            var r = conn.reader(&rbuf);
            const ri = &r.interface;
            var i: u32 = 0;
            while (i < 5) : (i += 1) {
                var hdr: [4]u8 = undefined;
                ri.readSliceAll(&hdr) catch return;
                const len = std.mem.readInt(u32, &hdr, .little);
                if (len > 64) return;
                var payload: [64]u8 = undefined;
                ri.readSliceAll(payload[0..len]) catch return;
                var expect: [64]u8 = undefined;
                const e = std.fmt.bufPrint(&expect, "frame-{d}", .{i}) catch return;
                if (!std.mem.eql(u8, e, payload[0..len])) return;
            }
            c.count.* = i;
        }
    };
    const ClientCtx = struct {
        addr: fiber.net.IpAddress,
        fn run(c: *@This()) void {
            const conn = c.addr.connect(.{}) catch return;
            defer conn.close();
            var wbuf: [256]u8 = undefined;
            var w = conn.writer(&wbuf);
            const wi = &w.interface;
            var i: u32 = 0;
            while (i < 5) : (i += 1) {
                var payload: [64]u8 = undefined;
                const p = std.fmt.bufPrint(&payload, "frame-{d}", .{i}) catch return;
                var hdr: [4]u8 = undefined;
                std.mem.writeInt(u32, &hdr, @intCast(p.len), .little);
                wi.writeAll(&hdr) catch return;
                wi.writeAll(p) catch return;
            }
            wi.flush() catch return;
        }
    };

    var count: u32 = 0;
    var sctx = ServerCtx{ .server = &server, .count = &count };
    var cctx = ClientCtx{ .addr = .{ .inner = server.bound_addr } };
    const sh = try fiber.spawn(.{}, ServerCtx.run, .{&sctx});
    const ch = try fiber.spawn(.{}, ClientCtx.run, .{&cctx});
    ch.join();
    sh.join();
    try std.testing.expectEqual(@as(u32, 5), count);
}

test "net.tcp: shutdown(send) makes the peer observe end-of-stream" {
    const laddr: fiber.net.IpAddress = .{ .inner = loopback };
    var server = try laddr.listen(.{ .reuse_address = true });
    defer server.close();

    const ServerCtx = struct {
        server: *fiber.net.Server,
        fn run(c: *@This()) void {
            const conn = c.server.accept(.{}) catch return;
            defer conn.close();
            conn.writeAll("bye") catch return;
            conn.shutdown(.send) catch return;
            // Keep the connection open (we can still receive) until the client
            // is done, so it observes a clean FIN rather than a reset.
            var b: [1]u8 = undefined;
            _ = conn.read(&b) catch {};
        }
    };
    const ClientCtx = struct {
        addr: fiber.net.IpAddress,
        data_ok: *bool,
        got_eof: *bool,
        fn run(c: *@This()) void {
            const conn = c.addr.connect(.{}) catch return;
            defer conn.close();
            var buf: [3]u8 = undefined;
            conn.readAll(&buf) catch return;
            c.data_ok.* = std.mem.eql(u8, "bye", &buf);
            var one: [1]u8 = undefined;
            const r = conn.read(&one);
            c.got_eof.* = if (r) |n| (n == 0) else |e| (e == error.EOF);
        }
    };

    var data_ok = false;
    var got_eof = false;
    var sctx = ServerCtx{ .server = &server };
    var cctx = ClientCtx{ .addr = .{ .inner = server.bound_addr }, .data_ok = &data_ok, .got_eof = &got_eof };
    const sh = try fiber.spawn(.{}, ServerCtx.run, .{&sctx});
    const ch = try fiber.spawn(.{}, ClientCtx.run, .{&cctx});
    ch.join();
    sh.join();
    try std.testing.expect(data_ok);
    try std.testing.expect(got_eof);
}

test "net.tcp: connect to an unused port fails" {
    // Claim a free ephemeral port, then close it; a connect there should be
    // refused (no listener).
    const laddr: fiber.net.IpAddress = .{ .inner = loopback };
    var server = try laddr.listen(.{ .reuse_address = true });
    const dead: fiber.net.IpAddress = .{ .inner = server.bound_addr };
    server.close();

    const Ctx = struct {
        addr: fiber.net.IpAddress,
        connected: *bool,
        fn run(c: *@This()) void {
            if (c.addr.connect(.{})) |s| {
                c.connected.* = true;
                s.close();
            } else |_| {}
        }
    };
    var connected = false;
    var ctx = Ctx{ .addr = dead, .connected = &connected };
    const h = try fiber.spawn(.{}, Ctx.run, .{&ctx});
    h.join();
    try std.testing.expect(!connected);
}
