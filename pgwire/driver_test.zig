//! End-to-end driver tests. The driver is the only zio-touching layer,
//! so we drive it through a real TCP loopback pair under zio.Runtime
//! — same pattern zio's own tests use for streams.
//!
//! There's no in-memory paired Stream in zio (per a quick scan of
//! the source), so we use 127.0.0.1:0 + a real accept. The cost is
//! marginal and the test exercises the same code path production
//! would.

const std = @import("std");
const testing = std.testing;
const fiber = @import("fiber");
const pgwire = @import("pgwire");

const Channel = fiber.Channel;
const IpAddress = fiber.net.IpAddress;
const Stream = fiber.net.Stream;

/// Stub handler. Records the SQL it sees and emits a single hardcoded
/// row each time. Used by the driver test below.
const StubHandler = struct {
    last_sql: std.ArrayList(u8) = .empty,
    call_count: u32 = 0,
    alloc: std.mem.Allocator,

    pub fn onQuery(
        self: *StubHandler,
        state: *pgwire.ConnState,
        sql: []const u8,
    ) !void {
        self.call_count += 1;
        self.last_sql.clearRetainingCapacity();
        try self.last_sql.appendSlice(self.alloc, sql);

        const fields = [_]pgwire.FieldDescription{
            .{ .name = "greeting", .type_oid = pgwire.oid.text },
        };
        try state.sendRowDescription(&fields);
        const cols = [_]pgwire.DataValue{
            .{ .text = "hello" },
        };
        try state.sendDataRow(&cols);
        try state.sendCommandComplete("SELECT 1");
    }

    pub fn deinit(self: *StubHandler) void {
        self.last_sql.deinit(self.alloc);
    }
};

/// Build the bytes of a Startup3("user=u\0database=d\0\0") message.
fn buildStartup(buf: *std.ArrayList(u8), a: std.mem.Allocator) !void {
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(a);
    try body.appendSlice(a, "user");
    try body.append(a, 0);
    try body.appendSlice(a, "u");
    try body.append(a, 0);
    try body.appendSlice(a, "database");
    try body.append(a, 0);
    try body.appendSlice(a, "d");
    try body.append(a, 0);
    try body.append(a, 0);

    const total: u32 = @intCast(8 + body.items.len);
    var hdr: [8]u8 = undefined;
    std.mem.writeInt(u32, hdr[0..4], total, .big);
    std.mem.writeInt(u32, hdr[4..8], pgwire.types.protocol_version_3, .big);
    try buf.appendSlice(a, &hdr);
    try buf.appendSlice(a, body.items);
}

fn buildQuery(
    buf: *std.ArrayList(u8),
    a: std.mem.Allocator,
    sql: []const u8,
) !void {
    const total: u32 = @intCast(4 + sql.len + 1);
    try buf.append(a, 'Q');
    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, total, .big);
    try buf.appendSlice(a, &len_buf);
    try buf.appendSlice(a, sql);
    try buf.append(a, 0);
}

fn buildTerminate(buf: *std.ArrayList(u8), a: std.mem.Allocator) !void {
    try buf.append(a, 'X');
    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, 4, .big);
    try buf.appendSlice(a, &len_buf);
}

const TestCtx = struct {
    port_ch: *Channel(u16),
    a: std.mem.Allocator,
    handler: *StubHandler,
};

fn serverFn(ctx: *TestCtx) anyerror!void {
    const addr = try IpAddress.parseIpAndPort("127.0.0.1:0");
    const server = try addr.listen(.{});
    defer server.close();

    try ctx.port_ch.send(server.port());

    var stream = try server.accept(.{});
    defer stream.close();

    try pgwire.serve(ctx.a, &stream, ctx.handler, .{});
}

fn clientFn(ctx: *TestCtx) anyerror!void {
    const port = try ctx.port_ch.receive();
    var addr_buf: [32]u8 = undefined;
    const addr_str = try std.fmt.bufPrint(&addr_buf, "127.0.0.1:{d}", .{port});
    const addr = try IpAddress.parseIpAndPort(addr_str);

    var stream = try addr.connect(.{});
    defer stream.close();

    // Send Startup3 + Query("SELECT 'hello'") + Terminate.
    var to_send: std.ArrayList(u8) = .empty;
    defer to_send.deinit(ctx.a);
    try buildStartup(&to_send, ctx.a);
    try buildQuery(&to_send, ctx.a, "SELECT 'hello'");
    try buildTerminate(&to_send, ctx.a);
    try stream.writeAll(to_send.items);

    // Drain the server's response. The driver loop terminates when
    // the peer sends 'X', then the server closes the stream — so
    // reading until EOF here is sufficient.
    var inbox: std.ArrayList(u8) = .empty;
    defer inbox.deinit(ctx.a);
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = try stream.read(&buf);
        if (n == 0) break;
        try inbox.appendSlice(ctx.a, buf[0..n]);
    }

    // Walk the response and assert the message-tag sequence matches a
    // valid simple-query session: AuthOk + ParameterStatus* +
    // BackendKeyData + ReadyForQuery + RowDescription + DataRow +
    // CommandComplete + ReadyForQuery.
    var off: usize = 0;
    var tags: std.ArrayList(u8) = .empty;
    defer tags.deinit(ctx.a);
    while (off < inbox.items.len) {
        const r = try pgwire.proto.decodeMessage(inbox.items[off..]);
        try tags.append(ctx.a, r.msg.tag);
        off += r.consumed;
    }

    // Required tags in order. We don't pin the exact count of 'S'
    // (ParameterStatus) entries — the state machine bundles a few.
    try testing.expectEqual(@as(u8, 'R'), tags.items[0]);
    try expectContainsInOrder(tags.items, &[_]u8{ 'R', 'K', 'Z', 'T', 'D', 'C', 'Z' });

    // Verify DataRow body holds "hello".
    off = 0;
    var saw_data_row = false;
    while (off < inbox.items.len) {
        const r = try pgwire.proto.decodeMessage(inbox.items[off..]);
        if (r.msg.tag == 'D') {
            const cnt = std.mem.readInt(i16, r.msg.body[0..2], .big);
            try testing.expectEqual(@as(i16, 1), cnt);
            const len = std.mem.readInt(i32, r.msg.body[2..6], .big);
            try testing.expectEqual(@as(i32, 5), len);
            try testing.expectEqualStrings("hello", r.msg.body[6..11]);
            saw_data_row = true;
        }
        off += r.consumed;
    }
    try testing.expect(saw_data_row);

    // And the handler must have been called with the right SQL.
    try testing.expectEqual(@as(u32, 1), ctx.handler.call_count);
    try testing.expectEqualStrings(
        "SELECT 'hello'",
        ctx.handler.last_sql.items,
    );
}

/// Assert that `needle` appears in `hay` as a subsequence — every
/// element of `needle` is found in `hay` in order, but other tags may
/// be interleaved.
fn expectContainsInOrder(hay: []const u8, needle: []const u8) !void {
    var ni: usize = 0;
    for (hay) |b| {
        if (ni < needle.len and b == needle[ni]) ni += 1;
    }
    if (ni != needle.len) {
        std.debug.print(
            "expected subsequence {x} in tag stream {x}\n",
            .{ needle, hay },
        );
        return error.MissingTagSubsequence;
    }
}

test "driver: full startup + Query + Terminate round-trip" {
    const a = std.testing.allocator;

    var handler = StubHandler{ .alloc = a };
    defer handler.deinit();

    var port_buf: [1]u16 = undefined;
    var port_ch = Channel(u16).init(&port_buf);

    var ctx = TestCtx{
        .port_ch = &port_ch,
        .a = a,
        .handler = &handler,
    };

    var group = fiber.Group(anyerror!void).init(a);
    defer group.deinit();

    try group.spawn(.{}, serverFn, .{&ctx});
    try group.spawn(.{}, clientFn, .{&ctx});

    for (group.wait()) |r| try r;
}
