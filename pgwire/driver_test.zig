// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! End-to-end driver tests. The driver is the only net-touching layer,
//! so we drive it through a real TCP loopback pair on the fiber runtime
//! — server and client each run as a fiber in a Group. The server
//! side runs the event loop a front-end would (see pgwire.driver)
//! over a stub handler.
//!
//! There's no in-memory paired Stream, so we use 127.0.0.1:0 + a real
//! accept. The cost is marginal and the test exercises the same code
//! path production would.

const std = @import("std");
const testing = std.testing;
const fiber = @import("zfiber");
const pgwire = @import("pgwire");
const utils = @import("utils");
const String = utils.String;

const Channel = fiber.Channel;
const IpAddress = fiber.net.IpAddress;
const Stream = fiber.net.Stream;

/// Stub handler. Records the SQL it sees and emits a single hardcoded
/// row each time. Used by the driver test below.
const StubHandler = struct {
    last_sql: std.ArrayList(u8) = .empty,
    call_count: u32 = 0,
    execute_count: u32 = 0,
    describe_count: u32 = 0,
    database: std.ArrayList(u8) = .empty,
    alloc: std.mem.Allocator,

    /// The connection's event loop (see pgwire.driver).
    fn serve(self: *StubHandler, stream: *Stream) !void {
        const session = try pgwire.Session.open(self.alloc, stream, .{});
        defer session.close();
        while (!session.closed()) {
            const state = &session.state;
            const ev = try session.nextEvent();
            defer state.releaseBuffers();
            switch (ev) {
                .startup_complete => |params| try self.onStartup(state, params),
                .query => |sql| {
                    try self.onQuery(state, sql);
                    try state.endCommand();
                },
                .execute => |ctx| try self.onExecute(state, ctx),
                .describe => |ctx| try self.onDescribe(state, ctx),
                .sync, .flush, .terminate => {},
            }
            if (pgwire.Session.flushAfter(ev)) try session.flush();
        }
    }

    /// Optional hook: capture the session's database (params borrow the
    /// read buffer, so copy).
    pub fn onStartup(
        self: *StubHandler,
        state: *pgwire.ConnState,
        params: pgwire.proto.StartupParams,
    ) !void {
        _ = state;
        if (params.database) |d| try self.database.appendSlice(self.alloc, d);
    }

    pub fn onQuery(
        self: *StubHandler,
        state: *pgwire.ConnState,
        sql: String,
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

    /// Extended-query Execute: the SQL is recorded; "FAIL" errors and
    /// drops the rest of the batch, as the SQL handler does. No
    /// RowDescription here — that is Describe's.
    pub fn onExecute(
        self: *StubHandler,
        state: *pgwire.ConnState,
        ctx: pgwire.ExecuteCtx,
    ) !void {
        self.execute_count += 1;
        self.last_sql.clearRetainingCapacity();
        try self.last_sql.appendSlice(self.alloc, ctx.sql);
        if (std.mem.eql(u8, ctx.sql, "FAIL")) {
            try state.sendError(.{
                .code = pgwire.sqlstate.internal_error,
                .message = "as requested",
            });
            state.ext_failed = true;
            return;
        }
        const cols = [_]pgwire.DataValue{.{ .text = "hello" }};
        try state.sendDataRow(&cols);
        try state.sendCommandComplete("SELECT 1");
    }

    pub fn onDescribe(
        self: *StubHandler,
        state: *pgwire.ConnState,
        ctx: pgwire.DescribeCtx,
    ) !void {
        _ = ctx;
        self.describe_count += 1;
        const fields = [_]pgwire.FieldDescription{
            .{ .name = "greeting", .type_oid = pgwire.oid.text },
        };
        try state.sendRowDescription(&fields);
    }

    pub fn deinit(self: *StubHandler) void {
        self.last_sql.deinit(self.alloc);
        self.database.deinit(self.alloc);
    }
};

const appendFrame = pgwire.client.appendFrame;

/// The startup every client here sends: user "u", database "d".
fn buildStartup(buf: *std.ArrayList(u8), a: std.mem.Allocator) !void {
    try pgwire.client.appendStartup(buf, a, "u", "d");
}

/// Parse: `name` (unnamed = ""), `sql`, no parameter OIDs.
fn buildParse(
    buf: *std.ArrayList(u8),
    a: std.mem.Allocator,
    name: String,
    sql: String,
) !void {
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(a);
    try body.appendSlice(a, name);
    try body.append(a, 0);
    try body.appendSlice(a, sql);
    try body.append(a, 0);
    try body.appendSlice(a, &[_]u8{ 0, 0 }); // n_oids = 0
    try appendFrame(buf, a, .parse, body.items);
}

/// Bind: the unnamed portal over statement `name`, text-format
/// params, text results.
fn buildBind(
    buf: *std.ArrayList(u8),
    a: std.mem.Allocator,
    name: String,
    params: []const String,
) !void {
    var body: std.ArrayList(u8) = .empty;
    defer body.deinit(a);
    try body.append(a, 0); // portal ""
    try body.appendSlice(a, name);
    try body.append(a, 0);
    try body.appendSlice(a, &[_]u8{ 0, 0 }); // n_pfmt = 0 (all text)
    var n: [2]u8 = undefined;
    std.mem.writeInt(u16, &n, @intCast(params.len), .big);
    try body.appendSlice(a, &n);
    for (params) |p| {
        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(i32, &len_buf, @intCast(p.len), .big);
        try body.appendSlice(a, &len_buf);
        try body.appendSlice(a, p);
    }
    try body.appendSlice(a, &[_]u8{ 0, 0 }); // n_rfmt = 0 (all text)
    try appendFrame(buf, a, .bind, body.items);
}

/// Describe the unnamed portal.
fn buildDescribePortal(buf: *std.ArrayList(u8), a: std.mem.Allocator) !void {
    try appendFrame(buf, a, .describe, &[_]u8{ 'P', 0 });
}

/// Execute the unnamed portal, no row limit.
fn buildExecute(buf: *std.ArrayList(u8), a: std.mem.Allocator) !void {
    try appendFrame(buf, a, .execute, &[_]u8{ 0, 0, 0, 0, 0 });
}

fn buildSync(buf: *std.ArrayList(u8), a: std.mem.Allocator) !void {
    try appendFrame(buf, a, .sync, "");
}

fn buildTerminate(buf: *std.ArrayList(u8), a: std.mem.Allocator) !void {
    try appendFrame(buf, a, .terminate, "");
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

    try ctx.handler.serve(&stream);
}

fn clientFn(ctx: *TestCtx) anyerror!void {
    const port = try ctx.port_ch.receive();
    var addr_buf: [32]u8 = undefined;
    const addr_str = try std.fmt.bufPrint(&addr_buf, "127.0.0.1:{d}", .{port});
    const addr = try IpAddress.parseIpAndPort(addr_str);

    // The startup, one query and the Terminate, as `pgwire.Client`
    // sends them; `close` returns once the server has closed its end.
    var arena = std.heap.ArenaAllocator.init(ctx.a);
    defer arena.deinit();
    var client = try pgwire.Client.connect(ctx.a, addr, "d");
    const reply = try client.query(arena.allocator(), "SELECT 'hello'");
    client.close();

    try testing.expect(reply.err == null);
    try testing.expectEqual(pgwire.TxStatus.idle, reply.status);
    try testing.expectEqualStrings("SELECT 1", reply.tag);
    try testing.expectEqual(@as(usize, 1), reply.rows.len);
    try testing.expectEqual(@as(usize, 1), reply.rows[0].len);
    try testing.expectEqualStrings("hello", reply.rows[0][0].?);

    // And the handler must have been called with the right SQL.
    try testing.expectEqual(@as(u32, 1), ctx.handler.call_count);
    try testing.expectEqualStrings(
        "SELECT 'hello'",
        ctx.handler.last_sql.items,
    );
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

    // The onStartup hook saw the session params (buildStartup sends
    // database=d).
    try std.testing.expectEqualStrings("d", handler.database.items);
}

/// Client that sends SSLRequest alone and waits for the 'N' before it
/// sends Startup, the way libpq behaves with sslmode=prefer. A server
/// that buffers the 'N' instead of flushing it leaves both sides
/// waiting; the test runner's hard timeout then fails the test.
fn sslProbeClientFn(ctx: *TestCtx) anyerror!void {
    const port = try ctx.port_ch.receive();
    var addr_buf: [32]u8 = undefined;
    const addr_str = try std.fmt.bufPrint(
        &addr_buf,
        "127.0.0.1:{d}",
        .{port},
    );
    const addr = try IpAddress.parseIpAndPort(addr_str);

    var stream = try addr.connect(.{});
    defer stream.close();

    var probe: [8]u8 = undefined;
    std.mem.writeInt(u32, probe[0..4], 8, .big);
    std.mem.writeInt(u32, probe[4..8], pgwire.types.ssl_request_code, .big);
    try stream.writeAll(&probe);

    var answer: [1]u8 = undefined;
    const got = try stream.read(&answer);
    try testing.expectEqual(@as(usize, 1), got);
    try testing.expectEqual(@as(u8, 'N'), answer[0]);

    var to_send: std.ArrayList(u8) = .empty;
    defer to_send.deinit(ctx.a);
    try buildStartup(&to_send, ctx.a);
    try buildTerminate(&to_send, ctx.a);
    try stream.writeAll(to_send.items);

    var inbox: std.ArrayList(u8) = .empty;
    defer inbox.deinit(ctx.a);
    var buf: [1024]u8 = undefined;
    while (true) {
        const n = stream.read(&buf) catch |e| switch (e) {
            error.EOF => break,
            else => return e,
        };
        if (n == 0) break;
        try inbox.appendSlice(ctx.a, buf[0..n]);
    }

    // AuthenticationOk is the first framed message after the probe.
    try testing.expect(inbox.items.len > 0);
    try testing.expectEqual(@as(u8, 'R'), inbox.items[0]);
}

test "driver: SSLRequest alone is answered before Startup arrives" {
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
    try group.spawn(.{}, sslProbeClientFn, .{&ctx});

    for (group.wait()) |r| try r;

    try std.testing.expectEqualStrings("d", handler.database.items);
}

/// Read the whole reply until the server closes, then the tag
/// sequence of its messages.
fn readTags(ctx: *TestCtx, stream: *Stream, tags: *std.ArrayList(u8)) !void {
    var inbox: std.ArrayList(u8) = .empty;
    defer inbox.deinit(ctx.a);
    var buf: [4096]u8 = undefined;
    while (true) {
        const n = stream.read(&buf) catch |e| switch (e) {
            error.EOF => break,
            else => return e,
        };
        if (n == 0) break;
        try inbox.appendSlice(ctx.a, buf[0..n]);
    }
    var off: usize = 0;
    while (off < inbox.items.len) {
        const r = try pgwire.proto.decodeMessage(inbox.items[off..]);
        try tags.append(ctx.a, r.msg.tag);
        off += r.consumed;
    }
}

/// The extended protocol, the way pgbench drives it: a named Parse
/// once, then Bind + Describe(portal) + Execute + Sync per command;
/// then the unnamed statement; then an Execute the handler fails,
/// which drops the Execute after it until Sync.
fn extendedClientFn(ctx: *TestCtx) anyerror!void {
    const port = try ctx.port_ch.receive();
    var addr_buf: [32]u8 = undefined;
    const addr_str = try std.fmt.bufPrint(&addr_buf, "127.0.0.1:{d}", .{port});
    const addr = try IpAddress.parseIpAndPort(addr_str);

    var stream = try addr.connect(.{});
    defer stream.close();

    var to_send: std.ArrayList(u8) = .empty;
    defer to_send.deinit(ctx.a);
    try buildStartup(&to_send, ctx.a);
    // Prepared: Parse "s1" + Sync (libpq PQprepare), then two
    // Bind/Describe/Execute/Sync rounds over it.
    try buildParse(&to_send, ctx.a, "s1", "SELECT $1");
    try buildSync(&to_send, ctx.a);
    try buildBind(&to_send, ctx.a, "s1", &.{"42"});
    try buildDescribePortal(&to_send, ctx.a);
    try buildExecute(&to_send, ctx.a);
    try buildSync(&to_send, ctx.a);
    try buildBind(&to_send, ctx.a, "s1", &.{"43"});
    try buildDescribePortal(&to_send, ctx.a);
    try buildExecute(&to_send, ctx.a);
    try buildSync(&to_send, ctx.a);
    // Extended: the unnamed statement, no Describe.
    try buildParse(&to_send, ctx.a, "", "SELECT 1");
    try buildBind(&to_send, ctx.a, "", &.{});
    try buildExecute(&to_send, ctx.a);
    try buildSync(&to_send, ctx.a);
    // A failing Execute: the second one in the batch is dropped, Sync
    // still answers.
    try buildParse(&to_send, ctx.a, "", "FAIL");
    try buildBind(&to_send, ctx.a, "", &.{});
    try buildExecute(&to_send, ctx.a);
    try buildParse(&to_send, ctx.a, "", "SELECT 2");
    try buildBind(&to_send, ctx.a, "", &.{});
    try buildExecute(&to_send, ctx.a);
    try buildSync(&to_send, ctx.a);
    try buildTerminate(&to_send, ctx.a);
    try stream.writeAll(to_send.items);

    var tags: std.ArrayList(u8) = .empty;
    defer tags.deinit(ctx.a);
    try readTags(ctx, &stream, &tags);

    // After startup's RFQ: ParseComplete + RFQ; then per prepared
    // round BindComplete, RowDescription (Describe), DataRow,
    // CommandComplete, RFQ — no RowDescription from Execute; then the
    // unnamed round with no RowDescription at all; then
    // ParseComplete, BindComplete, ErrorResponse and straight to RFQ.
    const startup = std.mem.indexOfScalar(u8, tags.items, 'Z').?;
    try testing.expectEqualSlices(
        u8,
        "1Z" ++ "2TDCZ" ++ "2TDCZ" ++ "12DCZ" ++ "12EZ",
        tags.items[startup + 1 ..],
    );
    try testing.expectEqual(@as(u32, 4), ctx.handler.execute_count);
    try testing.expectEqual(@as(u32, 2), ctx.handler.describe_count);
    try testing.expectEqual(@as(u32, 0), ctx.handler.call_count);
    // The dropped Execute never reached the handler: the last SQL it
    // saw is the failing one.
    try testing.expectEqualStrings("FAIL", ctx.handler.last_sql.items);
}

test "driver: extended protocol round-trip; an error drops the batch" {
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
    try group.spawn(.{}, extendedClientFn, .{&ctx});

    for (group.wait()) |r| try r;
}
