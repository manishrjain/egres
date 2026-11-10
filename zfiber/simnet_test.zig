// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The simulated network, driven through `fiber.net` as the code above
//! it drives a socket. Only a sim has one, so these skip without it.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");

const net = fiber.net;
const String = utils.String;
const testing = std.testing;
const Group = fiber.Group(anyerror!void);

/// A listener on a port of the network's choosing.
fn listenAny() !net.Server {
    _ = fiber.simNet() orelse return error.SkipZigTest;
    return (try net.IpAddress.parseIpAndPort("127.0.0.1:0")).listen(.{});
}

fn addrOf(port: u16) !net.IpAddress {
    var buf: [32]u8 = undefined;
    const text = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{port});
    return net.IpAddress.parseIpAndPort(text);
}

fn dial(port: u16) !net.Stream {
    return (try addrOf(port)).connect(.{});
}

test "simnet: a connect to a port nobody listens on is refused" {
    const srv = try listenAny();
    const port = srv.port();
    srv.close();
    try testing.expectError(error.ConnectionRefused, dial(port));
}

fn helloWorld(srv: net.Server) anyerror!void {
    const s = try srv.accept(.{});
    defer s.close();
    var buf: [5]u8 = undefined;
    try s.readAll(&buf);
    try testing.expectEqualStrings("hello", &buf);
    try s.writeAll("world");
}

test "simnet: bytes go both ways, then the close is the end" {
    const srv = try listenAny();
    defer srv.close();
    var group = Group.init(testing.allocator);
    defer group.deinit();
    try group.spawn(.{}, helloWorld, .{srv});

    const c = try dial(srv.port());
    defer c.close();
    try c.writeAll("hello");
    var buf: [5]u8 = undefined;
    try c.readAll(&buf);
    try testing.expectEqualStrings("world", &buf);
    try testing.expectError(error.EOF, c.read(&buf));
    // The peer is gone: a write has nowhere to go.
    try testing.expectError(error.BrokenPipe, c.write("x"));
    for (group.wait()) |r| try r;
}

test "simnet: a peer reads what was sent before a close" {
    const srv = try listenAny();
    defer srv.close();
    const c = try dial(srv.port());
    const s = try srv.accept(.{});
    defer s.close();

    try c.writeAll("last words");
    c.close();
    var buf: [10]u8 = undefined;
    try s.readAll(&buf);
    try testing.expectEqualStrings("last words", &buf);
    try testing.expectError(error.EOF, s.read(&buf));
}

const Sender = struct {
    stream: net.Stream,
    bytes: String,
    done: bool = false,

    fn run(self: *Sender) anyerror!void {
        try self.stream.writeAll(self.bytes);
        self.done = true;
    }
};

test "simnet: a write past the window waits for the reader" {
    const alloc = testing.allocator;
    const srv = try listenAny();
    defer srv.close();
    const c = try dial(srv.port());
    defer c.close();
    const s = try srv.accept(.{});
    defer s.close();

    // More than the window holds, and not a multiple of it: the bytes
    // cross in pieces and must come out in order.
    const len = 4 * fiber.simnet.window + 123;
    const sent = try alloc.alloc(u8, len);
    defer alloc.free(sent);
    for (sent, 0..) |*b, i| b.* = @truncate(i *% 31);

    var sender: Sender = .{ .stream = c, .bytes = sent };
    var group = Group.init(alloc);
    defer group.deinit();
    try group.spawn(.{}, Sender.run, .{&sender});

    // Long enough for every byte, were there no window.
    fiber.sleepMs(1000);
    try testing.expect(!sender.done);

    const got = try alloc.alloc(u8, len);
    defer alloc.free(got);
    try s.readAll(got);
    try testing.expect(std.mem.eql(u8, sent, got));
    for (group.wait()) |r| try r;
    try testing.expect(sender.done);
}

const Reading = struct {
    stream: net.Stream,
    ended: bool = false,

    fn run(self: *Reading) anyerror!void {
        var buf: [8]u8 = undefined;
        try testing.expectError(error.EOF, self.stream.read(&buf));
        self.ended = true;
    }
};

test "simnet: a shutdown ends this end's parked read and the peer's" {
    const srv = try listenAny();
    defer srv.close();
    const c = try dial(srv.port());
    defer c.close();
    const s = try srv.accept(.{});
    defer s.close();

    var reading: Reading = .{ .stream = c };
    var group = Group.init(testing.allocator);
    defer group.deinit();
    try group.spawn(.{}, Reading.run, .{&reading});
    fiber.sleepMs(50);
    try testing.expect(!reading.ended);

    try c.shutdown(.both);
    for (group.wait()) |r| try r;
    try testing.expect(reading.ended);
    var buf: [8]u8 = undefined;
    try testing.expectError(error.EOF, s.read(&buf));
    try testing.expectError(error.BrokenPipe, c.write("x"));
}

const Accepting = struct {
    srv: net.Server,
    got: bool = false,

    fn run(self: *Accepting) anyerror!void {
        const s = try self.srv.accept(.{});
        s.close();
        self.got = true;
    }
};

test "simnet: an accept waits for a connect, and a port has one listener" {
    const srv = try listenAny();
    const addr = try addrOf(srv.port());
    try testing.expectError(error.AddressInUse, addr.listen(.{}));

    var accepting: Accepting = .{ .srv = srv };
    var group = Group.init(testing.allocator);
    defer group.deinit();
    try group.spawn(.{}, Accepting.run, .{&accepting});
    fiber.sleepMs(50);
    try testing.expect(!accepting.got);

    const c = try dial(srv.port());
    for (group.wait()) |r| try r;
    try testing.expect(accepting.got);
    c.close();

    // Closed, the port is free for the next listener.
    srv.close();
    const again = try addr.listen(.{});
    again.close();
}

fn acceptAborted(srv: net.Server) anyerror!void {
    try testing.expectError(error.ConnectionAborted, srv.accept(.{}));
}

test "simnet: closing a listener ends its parked accept" {
    const srv = try listenAny();
    var group = Group.init(testing.allocator);
    defer group.deinit();
    try group.spawn(.{}, acceptAborted, .{srv});
    fiber.sleepMs(50);
    srv.close();
    for (group.wait()) |r| try r;
}
