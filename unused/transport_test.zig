const std = @import("std");
const zio = @import("zio");
const mux_mod = @import("mux");
const Mux = mux_mod.Mux;
const FrameHeader = mux_mod.FrameHeader;
const transport_mod = @import("transport");
const Transport = transport_mod.Transport;

fn echoHandler(stream: *Mux.Stream) void {
    var frame = stream.recv() catch return;
    defer frame.deinit(stream.mux.allocator);
    stream.send(frame.header.message_type, frame.payload, FrameHeader.fin_flag) catch return;
}

/// Helper: create two connected servers and start them.
/// server_b has server_a as a peer.
/// Caller must defer server_a.deinit(), server_b.deinit().
fn setupTwoTransports(alloc: std.mem.Allocator) !struct { a: *Transport, b: *Transport } {
    // Transport A: node 1, listens on a random port.
    const server_a = try alloc.create(Transport);
    server_a.* = try Transport.init(alloc, .{
        .self_id = 1,
        .cluster_id = 42,
        .listen_addr = "127.0.0.1:0",
        .peers = &.{},
        .on_stream_p0 = &echoHandler,
        .on_stream_p1 = &echoHandler,
    });
    errdefer {
        server_a.deinit();
        alloc.destroy(server_a);
    }

    // Pre-bind to get the actual port.
    server_a.listener = try server_a.listen_addr.listen(.{ .reuse_address = true });
    const port = server_a.listener.?.socket.address.ip.getPort();

    var addr_buf: [64]u8 = undefined;
    const addr_str = std.fmt.bufPrint(&addr_buf, "127.0.0.1:{d}", .{port}) catch unreachable;

    const server_b = try alloc.create(Transport);
    server_b.* = try Transport.init(alloc, .{
        .self_id = 2,
        .cluster_id = 42,
        .listen_addr = "127.0.0.1:0",
        .peers = &.{.{ .id = 1, .addr = addr_str }},
        .on_stream_p0 = &echoHandler,
        .on_stream_p1 = &echoHandler,
    });
    errdefer {
        server_b.deinit();
        alloc.destroy(server_b);
    }

    try server_a.run();
    try server_b.run();

    return .{ .a = server_a, .b = server_b };
}

fn waitForMux(server: *Transport, peer_id: u64, priority: Transport.Priority) !*Mux {
    for (0..100) |_| {
        if (server.getPeerMux(peer_id, priority)) |m| return m;
        zio.sleep(zio.Duration.fromMilliseconds(10)) catch return error.Cancelled;
    }
    return error.PeerNotConnected;
}


test "single server echo via raw client" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();
    var handle = try rt.spawn(singleTransportEchoTest, .{});
    try handle.join();
}

fn singleTransportEchoTest() !void {
    const alloc = std.testing.allocator;

    // Create and start server.
    const srv = try alloc.create(Transport);
    srv.* = try Transport.init(alloc, .{
        .self_id = 1,
        .cluster_id = 42,
        .listen_addr = "127.0.0.1:0",
        .peers = &.{},
        .on_stream_p0 = &echoHandler,
        .on_stream_p1 = &echoHandler,
    });
    defer {
        srv.deinit();
        alloc.destroy(srv);
    }

    srv.listener = try srv.listen_addr.listen(.{ .reuse_address = true });
    try srv.run();

    const port = srv.listener.?.socket.address.ip.getPort();
    var addr_buf: [64]u8 = undefined;
    const addr_str = std.fmt.bufPrint(&addr_buf, "127.0.0.1:{d}", .{port}) catch unreachable;
    const addr = try zio.net.IpAddress.parseIpAndPort(addr_str);

    // Connect as a raw client and do the handshake.
    const conn = try addr.connect(.{});
    conn.socket.setNoDelay(true) catch {};

    try Transport.sendHandshake(conn, 42, 99, .p0); // cluster=42, node=99
    const reply_hs = try Transport.recvHandshake(conn);
    try std.testing.expectEqual(@as(u64, 42), reply_hs.cluster_id);
    try std.testing.expectEqual(@as(u64, 1), reply_hs.node_id);

    // Wrap connection in a Mux (client uses even stream IDs).
    const client_mux = try Mux.init(alloc, conn, 0, &echoHandler);
    defer client_mux.deinit();

    var group = zio.Group{};
    defer group.cancel();
    try group.spawn(Mux.run, .{client_mux});

    // Send a message and get the echo.
    const stream = try client_mux.newStream();
    try stream.send(0x42, "hello server", FrameHeader.fin_flag);

    var reply = try stream.recv();
    defer reply.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 0x42), reply.header.message_type);
    try std.testing.expectEqualSlices(u8, "hello server", reply.payload);
    try std.testing.expect(reply.done());
}

// TODO: fix test process hang on cleanup with two servers
test "two servers connect and exchange on p0" {
    return error.SkipZigTest;
}

test "two servers connect and exchange on p1" {
    return error.SkipZigTest;
}

test "sendMsg convenience" {
    return error.SkipZigTest;
}
