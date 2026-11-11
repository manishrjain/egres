// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! A pgwire client, as far as the tests need one: the startup, then
//! one simple query at a time with its rows as text. No TLS, no
//! password, no extended protocol.
//!
//!     var client = try pgwire.Client.connect(alloc, addr, "egres");
//!     defer client.close();
//!     const reply = try client.query(arloc, "SELECT id FROM t");
//!     if (reply.err) |e| ...          // the server refused it
//!     for (reply.rows) |row| ...      // row[0] is the id, as text

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;

const _proto = @import("proto.zig");
const _types = @import("types.zig");

const log = std.log.scoped(.pgclient);

/// The user every connection names. The server has no roles.
const user = "egres";
const read_chunk = 16 << 10;

/// An ErrorResponse's SQLSTATE and message.
pub const ServerError = struct {
    code: [5]u8 = @splat('?'),
    message: String = "",
};

/// A row's columns as text; null is a SQL NULL.
pub const Row = []const ?String;

/// What one query came back with.
pub const Reply = struct {
    rows: []const Row = &.{},
    /// The command tag, `INSERT 0 1`. Empty after an error.
    tag: String = "",
    err: ?ServerError = null,
    /// Whether the session is now idle, in a block or in a failed one.
    status: _types.TxStatus = .idle,
};

pub const Client = struct {
    alloc: std.mem.Allocator,
    stream: fiber.net.Stream,
    /// Bytes from the server; frames before `taken` have been handed
    /// out.
    inbox: std.ArrayList(u8) = .empty,
    taken: usize = 0,
    outbox: std.ArrayList(u8) = .empty,

    /// Connects and starts a session on `database`: returns once the
    /// server is ready for a query.
    pub fn connect(
        alloc: std.mem.Allocator,
        addr: fiber.net.IpAddress,
        database: String,
    ) !Client {
        var self: Client = .{
            .alloc = alloc,
            .stream = try addr.connect(.{}),
        };
        errdefer self.drop();

        try appendStartup(&self.outbox, alloc, user, database);
        try self.stream.writeAll(self.outbox.items);
        while (true) {
            const msg = try self.next();
            switch (msg.tag) {
                'R' => if (!std.mem.eql(u8, msg.body, &.{ 0, 0, 0, 0 }))
                    return error.AuthNotSupported,
                'E' => {
                    const e = try parseError(msg.body);
                    log.err("startup refused: {s} {s}", .{
                        &e.code, e.message,
                    });
                    return error.StartupRefused;
                },
                'Z' => return self,
                // ParameterStatus, BackendKeyData, a notice.
                else => {},
            }
        }
    }

    /// Ends the session with a Terminate, then closes the socket.
    pub fn close(self: *Client) void {
        // The tag and a length that counts only itself.
        self.stream.writeAll(&.{ 'X', 0, 0, 0, 4 }) catch {};
        self.drop();
    }

    fn drop(self: *Client) void {
        self.stream.close();
        self.inbox.deinit(self.alloc);
        self.outbox.deinit(self.alloc);
    }

    /// Sends `sql` as one simple query and reads to its ReadyForQuery.
    /// A statement the server refused is a reply with `err` set, not an
    /// error here. Rows, tag and message are copied into `arloc`.
    pub fn query(
        self: *Client,
        arloc: std.mem.Allocator,
        sql: String,
    ) !Reply {
        self.outbox.clearRetainingCapacity();
        try appendQuery(&self.outbox, self.alloc, sql);
        try self.stream.writeAll(self.outbox.items);

        var reply: Reply = .{};
        var rows: std.ArrayList(Row) = .empty;
        while (true) {
            const msg = try self.next();
            switch (msg.tag) {
                'D' => try rows.append(arloc, try parseRow(arloc, msg.body)),
                'C' => reply.tag = try arloc.dupe(
                    u8,
                    try _proto.parseQuery(msg.body),
                ),
                'E' => {
                    const e = try parseError(msg.body);
                    reply.err = .{
                        .code = e.code,
                        .message = try arloc.dupe(u8, e.message),
                    };
                },
                'Z' => {
                    if (msg.body.len != 1) return error.Truncated;
                    reply.status = std.enums.fromInt(
                        _types.TxStatus,
                        msg.body[0],
                    ) orelse return error.UnexpectedCode;
                    reply.rows = rows.items;
                    return reply;
                },
                // RowDescription, EmptyQueryResponse, a notice.
                else => {},
            }
        }
    }

    /// The next frame from the server. Its body is good until the call
    /// after.
    fn next(self: *Client) !_proto.Message {
        while (true) {
            const unread = self.inbox.items[self.taken..];
            if (_proto.decodeMessage(unread)) |frame| {
                self.taken += frame.consumed;
                return frame.msg;
            } else |err| if (err != error.NeedMore) return err;

            // Part of a frame, or nothing: drop the frames handed out,
            // which moves it to the front, and read more behind it.
            if (self.taken > 0) {
                self.inbox.replaceRangeAssumeCapacity(0, self.taken, &.{});
                self.taken = 0;
            }
            try self.inbox.ensureUnusedCapacity(self.alloc, read_chunk);
            const n = try self.stream.read(self.inbox.unusedCapacitySlice());
            self.inbox.items.len += n;
        }
    }
};

/// A DataRow body: a column count, then per column a length, -1 for
/// NULL, and that many bytes.
fn parseRow(arloc: std.mem.Allocator, body: String) !Row {
    var r = std.Io.Reader.fixed(body);
    const cols = try arloc.alloc(?String, try r.takeInt(u16, .big));
    for (cols) |*col| {
        const len = try r.takeInt(i32, .big);
        col.* = if (len < 0)
            null
        else
            try arloc.dupe(u8, try r.take(@intCast(len)));
    }
    return cols;
}

/// An ErrorResponse body: fields of a type byte and a C string, ended
/// by a zero byte. The message borrows from `body`.
fn parseError(body: String) !ServerError {
    var r = std.Io.Reader.fixed(body);
    var e: ServerError = .{};
    while (true) {
        const field = try r.takeByte();
        if (field == 0) return e;
        const with_nul = try r.takeDelimiterInclusive(0);
        const text = with_nul[0 .. with_nul.len - 1];
        switch (field) {
            'C' => if (text.len == e.code.len) @memcpy(&e.code, text),
            'M' => e.message = text,
            else => {},
        }
    }
}

// What a client sends, appended to `out`. Public for the tests that
// write frames to a socket themselves.

/// StartupMessage, protocol 3.0: `user` and `database`.
pub fn appendStartup(
    out: *std.ArrayList(u8),
    alloc: std.mem.Allocator,
    user_name: String,
    database: String,
) !void {
    const start = out.items.len;
    try out.appendNTimes(alloc, 0, 4);
    try appendInt(out, alloc, _types.protocol_version_3);
    const params = [_]String{ "user", user_name, "database", database };
    for (params) |text| {
        try out.appendSlice(alloc, text);
        try out.append(alloc, 0);
    }
    try out.append(alloc, 0);
    // The length counts itself: there is no tag before it.
    const len: u32 = @intCast(out.items.len - start);
    std.mem.writeInt(u32, out.items[start..][0..4], len, .big);
}

/// One tagged frame: the tag, the length of what follows it, the body.
pub fn appendFrame(
    out: *std.ArrayList(u8),
    alloc: std.mem.Allocator,
    tag: _proto.FrontendTag,
    body: String,
) !void {
    try out.append(alloc, @intFromEnum(tag));
    try appendInt(out, alloc, @intCast(4 + body.len));
    try out.appendSlice(alloc, body);
}

/// A Query frame: `sql` and its NUL.
pub fn appendQuery(
    out: *std.ArrayList(u8),
    alloc: std.mem.Allocator,
    sql: String,
) !void {
    try out.append(alloc, @intFromEnum(_proto.FrontendTag.query));
    try appendInt(out, alloc, @intCast(4 + sql.len + 1));
    try out.appendSlice(alloc, sql);
    try out.append(alloc, 0);
}

fn appendInt(
    out: *std.ArrayList(u8),
    alloc: std.mem.Allocator,
    v: u32,
) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, v, .big);
    try out.appendSlice(alloc, &bytes);
}

test {
    std.testing.refAllDecls(@This());
}
