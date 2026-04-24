const std = @import("std");
const zio = @import("zio");

pub const Command = union(enum) {
    set: SetCmd,
    set_batch: SetBatchCmd,
    get: GetCmd,
    conf: ConfCmd,
    ns_create: NsCreateCmd,
    stats: StatsCmd,
};

pub const NsCreateCmd = struct {
    ns: u32,
};

pub const StatsCmd = struct {};

pub const SetCmd = struct {
    ns: u32,
    key: []const u8,
    value: []const u8,
};

/// Header for a batched write. The body is `count * 2` whitespace-delimited
/// tokens (key1, value1, key2, value2, ...). Whitespace is any run of
/// space / tab / LF / CR — newlines are cosmetic. A token starting with `0x`
/// is hex-decoded; otherwise taken as literal bytes. To send a literal
/// starting with `0x` or containing whitespace, hex-encode the whole token.
pub const SetBatchCmd = struct {
    ns: u32,
    count: u32,
};

pub const GetCmd = struct {
    ns: u32,
    key: []const u8,
    /// Optional read timestamp. 0 means "serve local applied state without
    /// blocking." If > 0, the server waits until this node's apply_ts has
    /// reached read_ts (with a timeout) so the returned view includes every
    /// mutation up to read_ts.
    read_ts: u64 = 0,
};

pub const ConfCmd = struct {
    action: Action,
    node_id: u64,
    addr: []const u8 = "",

    pub const Action = enum {
        add,
        to_voter,
        to_learner,
        remove,
        addr,
    };
};

const max_line_len = 64 * 1024;

/// Read one text command from the buffered reader. Returns null on EOF.
pub fn readCommand(r: *std.Io.Reader, line_buf: []u8) !?Command {
    const line = readLine(r, line_buf) catch |err| switch (err) {
        error.EndOfStream => return null,
        else => return err,
    };
    return try parseLine(line);
}

fn readLine(r: *std.Io.Reader, buf: []u8) ![]const u8 {
    // takeDelimiterExclusive returns a slice into the reader's internal
    // buffer — we copy into `buf` so the line survives subsequent reader use.
    const line = try r.takeDelimiterExclusive('\n');
    if (line.len > buf.len) return error.LineTooLong;
    const end = if (line.len > 0 and line[line.len - 1] == '\r') line.len - 1 else line.len;
    @memcpy(buf[0..end], line[0..end]);
    return buf[0..end];
}

fn isWs(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r';
}

/// Read one whitespace-delimited token. Leading whitespace is skipped; the
/// token ends at the next whitespace byte. If the token starts with `0x`,
/// the bytes after the prefix are hex-decoded; otherwise the token is
/// treated as literal bytes. The returned slice is owned by `alloc`.
pub fn readToken(r: *std.Io.Reader, alloc: std.mem.Allocator) ![]u8 {
    var raw: std.ArrayList(u8) = .empty;
    defer raw.deinit(alloc);

    // Skip whitespace.
    while (true) {
        const b = try r.takeByte();
        if (!isWs(b)) {
            try raw.append(alloc, b);
            break;
        }
    }
    // Collect until next whitespace.
    while (raw.items.len < max_line_len) {
        const b = r.takeByte() catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        if (isWs(b)) break;
        try raw.append(alloc, b);
    }

    const bytes = raw.items;
    if (bytes.len >= 2 and bytes[0] == '0' and bytes[1] == 'x') {
        const hex = bytes[2..];
        if (hex.len % 2 != 0) return error.BadHex;
        const out = try alloc.alloc(u8, hex.len / 2);
        _ = try std.fmt.hexToBytes(out, hex);
        return out;
    }
    return try alloc.dupe(u8, bytes);
}

fn parseLine(line: []const u8) !Command {
    var it = std.mem.splitScalar(u8, line, ' ');
    const cmd = it.next() orelse return error.EmptyCommand;

    if (std.mem.eql(u8, cmd, "set")) {
        const ns_str = it.next() orelse return error.MissingNamespace;
        const key = it.next() orelse return error.MissingKey;
        const value = it.rest();
        if (value.len == 0) return error.MissingValue;

        return .{ .set = .{
            .ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace,
            .key = key,
            .value = value,
        } };
    } else if (std.mem.eql(u8, cmd, "set-batch")) {
        const ns_str = it.next() orelse return error.MissingNamespace;
        const count_str = it.next() orelse return error.MissingCount;
        return .{ .set_batch = .{
            .ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace,
            .count = std.fmt.parseInt(u32, count_str, 10) catch return error.InvalidCount,
        } };
    } else if (std.mem.eql(u8, cmd, "get")) {
        const ns_str = it.next() orelse return error.MissingNamespace;
        const key = it.next() orelse return error.MissingKey;
        // read_ts is optional; if absent, follower serves local applied state.
        const read_ts: u64 = if (it.next()) |ts_str|
            (std.fmt.parseInt(u64, ts_str, 10) catch return error.InvalidTimestamp)
        else
            0;

        return .{ .get = .{
            .ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace,
            .key = key,
            .read_ts = read_ts,
        } };
    } else if (std.mem.eql(u8, cmd, "conf")) {
        const action_str = it.next() orelse return error.MissingAction;
        const node_id_str = it.next() orelse return error.MissingNodeId;
        const action: ConfCmd.Action = if (std.mem.eql(u8, action_str, "add"))
            .add
        else if (std.mem.eql(u8, action_str, "to_voter"))
            .to_voter
        else if (std.mem.eql(u8, action_str, "to_learner"))
            .to_learner
        else if (std.mem.eql(u8, action_str, "remove"))
            .remove
        else if (std.mem.eql(u8, action_str, "addr"))
            .addr
        else
            return error.InvalidAction;

        const node_id = std.fmt.parseInt(u64, node_id_str, 10) catch return error.InvalidNodeId;
        const addr = if (action == .add or action == .addr) (it.next() orelse return error.MissingAddr) else "";

        return .{ .conf = .{
            .action = action,
            .node_id = node_id,
            .addr = addr,
        } };
    } else if (std.mem.eql(u8, cmd, "ns")) {
        // ns create <ns_id>
        const sub = it.next() orelse return error.MissingSubcommand;
        if (!std.mem.eql(u8, sub, "create")) return error.UnknownCommand;
        const ns_str = it.next() orelse return error.MissingNamespace;
        const ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace;
        if (ns == 0) return error.InvalidNamespace; // 0 is a sentinel
        return .{ .ns_create = .{ .ns = ns } };
    } else if (std.mem.eql(u8, cmd, "stats")) {
        return .{ .stats = .{} };
    } else {
        return error.UnknownCommand;
    }
}

/// Write a successful response: "OK\n". Used for conf/ns_create where there's
/// no committed timestamp to surface.
pub fn writeOk(stream: zio.net.Stream) !void {
    try stream.writeAll("OK\n", .none);
}

/// Write a successful set response carrying the committed HLC timestamp:
/// "OK <ts>\n". Clients track the max observed commit ts for read-your-writes.
pub fn writeSetOk(stream: zio.net.Stream, commit_ts: u64) !void {
    var buf: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "OK {d}\n", .{commit_ts}) catch unreachable;
    try stream.writeAll(s, .none);
}

/// Write an error response: "ERR <msg>\n"
pub fn writeErr(stream: zio.net.Stream, msg: []const u8) !void {
    try stream.writeAll("ERR ", .none);
    try stream.writeAll(msg, .none);
    try stream.writeAll("\n", .none);
}

/// Write a not-leader response: "NOT_LEADER <leader_id>\n"
pub fn writeNotLeader(stream: zio.net.Stream, leader_id: u64) !void {
    var buf: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "NOT_LEADER {d}\n", .{leader_id}) catch unreachable;
    try stream.writeAll(s, .none);
}

/// Write a get-found response: "FOUND <ts> <value>\n"
pub fn writeFound(stream: zio.net.Stream, ts: u64, value: []const u8) !void {
    var buf: [64]u8 = undefined;
    const hdr = std.fmt.bufPrint(&buf, "FOUND {d} ", .{ts}) catch unreachable;
    try stream.writeAll(hdr, .none);
    try stream.writeAll(value, .none);
    try stream.writeAll("\n", .none);
}

/// Write a get-not-found response: "NOT_FOUND\n"
pub fn writeNotFound(stream: zio.net.Stream) !void {
    try stream.writeAll("NOT_FOUND\n", .none);
}
