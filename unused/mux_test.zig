const std = @import("std");
const zio = @import("zio");
const mux_mod = @import("mux");
const FrameHeader = mux_mod.FrameHeader;
const Frame = mux_mod.Frame;
const Mux = mux_mod.Mux;
test "FrameHeader encode/decode roundtrip" {
    const header = FrameHeader{
        .length = 0x12345678,
        .message_type = 0xA507,
        .flags = FrameHeader.syn_flag | FrameHeader.fin_flag,
        .stream_id = 0xDEADBEEF,
    };

    var buf: [FrameHeader.encoded_size]u8 = undefined;
    header.encode(&buf);
    const decoded = FrameHeader.decode(&buf);

    try std.testing.expectEqual(header.length, decoded.length);
    try std.testing.expectEqual(header.message_type, decoded.message_type);
    try std.testing.expectEqual(header.flags, decoded.flags);
    try std.testing.expectEqual(header.stream_id, decoded.stream_id);
}

test "FrameHeader zero values" {
    const header = FrameHeader{
        .length = 0,
        .message_type = 0,
        .flags = 0,
        .stream_id = 0,
    };

    var buf: [FrameHeader.encoded_size]u8 = undefined;
    header.encode(&buf);
    const decoded = FrameHeader.decode(&buf);

    try std.testing.expectEqual(@as(u32, 0), decoded.length);
    try std.testing.expectEqual(@as(u16, 0), decoded.message_type);
    try std.testing.expectEqual(@as(u8, 0), decoded.flags);
    try std.testing.expectEqual(@as(u32, 0), decoded.stream_id);
}

test "FrameHeader flag helpers" {
    const syn_only = FrameHeader{ .length = 0, .message_type = 0, .flags = FrameHeader.syn_flag, .stream_id = 0 };
    try std.testing.expect(syn_only.hasSyn());
    try std.testing.expect(!syn_only.hasFin());

    const fin_only = FrameHeader{ .length = 0, .message_type = 0, .flags = FrameHeader.fin_flag, .stream_id = 0 };
    try std.testing.expect(!fin_only.hasSyn());
    try std.testing.expect(fin_only.hasFin());

    const both = FrameHeader{ .length = 0, .message_type = 0, .flags = FrameHeader.syn_flag | FrameHeader.fin_flag, .stream_id = 0 };
    try std.testing.expect(both.hasSyn());
    try std.testing.expect(both.hasFin());
}

test "FrameHeader max values" {
    const header = FrameHeader{
        .length = std.math.maxInt(u32),
        .message_type = std.math.maxInt(u16),
        .flags = 0xFF,
        .stream_id = std.math.maxInt(u32),
    };

    var buf: [FrameHeader.encoded_size]u8 = undefined;
    header.encode(&buf);
    const decoded = FrameHeader.decode(&buf);

    try std.testing.expectEqual(header.length, decoded.length);
    try std.testing.expectEqual(header.message_type, decoded.message_type);
    try std.testing.expectEqual(header.flags, decoded.flags);
    try std.testing.expectEqual(header.stream_id, decoded.stream_id);
}

fn testStreamHandler(stream: *Mux.Stream) void {
    // Echo handler: read one frame, echo it back with fin.
    var frame = stream.recv() catch return;
    defer frame.deinit(stream.mux.allocator);

    stream.send(frame.header.message_type, frame.payload, FrameHeader.fin_flag) catch return;
}

test "Mux open stream and exchange frames" {
    const alloc = std.testing.allocator;

    // Create a loopback TCP pair.
    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    const server = try addr.listen(.{ .reuse_address = true });
    defer server.close();

    const bound_addr = server.socket.address;

    var group = zio.Group{};
    defer group.cancel();

    // Server side: accept one connection, run mux reader.
    try group.spawn(serverSide, .{ server, alloc });

    // Client side.
    const client_stream = try bound_addr.ip.connect(.{});
    try client_stream.socket.setNoDelay(true);

    // Client uses even stream IDs (0, 2, 4, ...).
    const client_mux = try Mux.init(alloc, client_stream, 0, &testStreamHandler);
    defer client_mux.deinit();

    // Run client reader in background.
    try group.spawn(Mux.run, .{client_mux});

    // Open a stream and send data.
    const test_payload = "hello mux";
    const ms = try client_mux.newStream();
    try ms.send(0x42, test_payload, 0);

    // Receive the echo.
    var reply = try ms.recv();
    defer reply.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 0x42), reply.header.message_type);
    try std.testing.expectEqualSlices(u8, test_payload, reply.payload);
    try std.testing.expect(reply.header.hasFin());
}

fn serverSide(server: zio.net.Server, alloc: std.mem.Allocator) !void {
    const conn = try server.accept(.{});
    try conn.socket.setNoDelay(true);

    // Server uses odd stream IDs (1, 3, 5, ...).
    const server_mux = try Mux.init(alloc, conn, 1, &testStreamHandler);
    defer server_mux.deinit();

    server_mux.run();
}

test "Mux multiple concurrent streams" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var handle = try rt.spawn(muxMultiStreamTest, .{});
    try handle.join();
}

fn concatStreamHandler(stream: *Mux.Stream) void {
    // Accumulate all frames until done(), then echo the concatenated payload back with fin.
    const alloc = stream.mux.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);

    var msg_type: u16 = 0;
    while (true) {
        var frame = stream.recv() catch return;
        defer frame.deinit(alloc);

        msg_type = frame.header.message_type;
        buf.appendSlice(alloc, frame.payload) catch return;

        if (frame.done()) break;
    }

    stream.send(msg_type, buf.items, FrameHeader.fin_flag) catch return;
}

fn concatServerSide(server: zio.net.Server, alloc: std.mem.Allocator) !void {
    const conn = try server.accept(.{});
    try conn.socket.setNoDelay(true);

    const server_mux = try Mux.init(alloc, conn, 1, &concatStreamHandler);
    defer server_mux.deinit();

    server_mux.run();
}

test "Mux multi-frame message" {
    const rt = try zio.Runtime.init(std.testing.allocator, .{});
    defer rt.deinit();

    var handle = try rt.spawn(muxMultiFrameTest, .{});
    try handle.join();
}

fn muxMultiFrameTest() !void {
    const alloc = std.testing.allocator;

    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    const server = try addr.listen(.{ .reuse_address = true });
    defer server.close();

    const bound_addr = server.socket.address;

    var group = zio.Group{};
    defer group.cancel();

    try group.spawn(concatServerSide, .{ server, alloc });

    const client_stream = try bound_addr.ip.connect(.{});
    try client_stream.socket.setNoDelay(true);

    const client_mux = try Mux.init(alloc, client_stream, 0, &concatStreamHandler);
    defer client_mux.deinit();

    try group.spawn(Mux.run, .{client_mux});

    // Send 10 frames, each with a chunk of data, fin on the last one.
    const ms = try client_mux.newStream();
    const chunks = [_][]const u8{ "Hello", " ", "world", ", ", "this", " ", "is", " ", "a", " test" };
    for (chunks[0 .. chunks.len - 1]) |chunk| {
        try ms.send(0x99, chunk, 0);
    }
    try ms.send(0x99, chunks[chunks.len - 1], FrameHeader.fin_flag);

    // Receive the concatenated echo.
    var reply = try ms.recv();
    defer reply.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 0x99), reply.header.message_type);
    try std.testing.expectEqualSlices(u8, "Hello world, this is a test", reply.payload);
    try std.testing.expect(reply.done());
}

fn muxMultiStreamTest() !void {
    const alloc = std.testing.allocator;

    const addr = try zio.net.IpAddress.parseIp4("127.0.0.1", 0);
    const server = try addr.listen(.{ .reuse_address = true });
    defer server.close();

    const bound_addr = server.socket.address;

    var group = zio.Group{};
    defer group.cancel();

    try group.spawn(serverSide, .{ server, alloc });

    const client_stream = try bound_addr.ip.connect(.{});
    try client_stream.socket.setNoDelay(true);

    const client_mux = try Mux.init(alloc, client_stream, 0, &testStreamHandler);
    defer client_mux.deinit();

    try group.spawn(Mux.run, .{client_mux});

    // Open 5 streams concurrently.
    const n = 5;
    var streams: [n]*Mux.Stream = undefined;
    for (0..n) |i| {
        var msg_buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "stream-{d}", .{i}) catch unreachable;
        streams[i] = try client_mux.newStream();
        try streams[i].send(0x42, msg, 0);
    }

    // Receive echoes.
    for (0..n) |i| {
        var reply = try streams[i].recv();
        defer reply.deinit(alloc);
        try std.testing.expectEqual(@as(u16, 0x42), reply.header.message_type);
    }
}
