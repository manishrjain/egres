const std = @import("std");
const zio = @import("zio");

pub const Command = union(enum) {
    set: SetCmd,
    set_batch: SetBatchCmd,
    get: GetCmd,
    conf: ConfCmd,
    ns_create: NsCreateCmd,
    stats: StatsCmd,
    info: InfoCmd,
};

pub const NsCreateCmd = struct {
    ns: u32,
};

pub const StatsCmd = struct {};

/// Dump cluster topology + per-peer raft progress (as the queried node
/// sees it). Best-effort snapshot — races against conf changes are
/// possible but tolerated; this is an admin endpoint, not a hot path.
pub const InfoCmd = struct {};

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

/// Read one text command from the buffered reader. Returns null on EOF.
/// Blank lines are skipped silently — common in line-based protocols
/// where intermediate tools (nc, terminal, etc.) sometimes inject a
/// trailing empty line on EOF.
pub fn readCommand(r: *std.Io.Reader, line_buf: []u8) !?Command {
    while (true) {
        const line = readLine(r, line_buf) catch |err| switch (err) {
            error.EndOfStream => return null,
            else => return err,
        };
        if (line.len == 0) continue;
        return try parseLine(line);
    }
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

/// Hard cap on the bytes of any single whitespace-delimited wire token
/// (a key OR a value in a `set-batch` body). Also the per-token unused
/// capacity the caller must reserve on the scratch `ArrayList` — see
/// `readToken`. Set once here so the caller's reservation and the
/// parser's rejection threshold can't drift apart.
pub const max_token_size: usize = 64 * 1024;

/// Read one whitespace-delimited token, appending its bytes onto `buf`.
/// Leading whitespace is skipped; the token ends at the next whitespace
/// byte (or EOF). If prefixed with `0x`, the payload is hex-decoded in
/// place — the decoded output is exactly half the hex input length, and
/// `buf.items.len` is truncated so only the decoded bytes remain.
///
/// Contract with the caller:
/// - Before calling, caller must `ensureUnusedCapacity(max_token_size)`
///   on `buf` so `appendAssumeCapacity` is safe for the whole token.
/// - `readToken` rejects any token that would exceed `max_token_size`
///   with `error.TokenTooLong` before the overflowing append, so a
///   malicious oversized token cannot trip the `appendAssumeCapacity`
///   assertion.
///
/// To recover the token's position, snapshot `buf.items.len` before and
/// after the call — the token lives at `buf.items[before..after]`.
/// Don't eagerly materialize that slice if more `readToken` calls will
/// follow with intervening `ensureUnusedCapacity`, since a reallocation
/// will invalidate it; keep offsets instead and resolve them once the
/// buffer is stable.
pub fn readToken(r: *std.Io.Reader, buf: *std.ArrayList(u8)) !void {
    const start = buf.items.len;
    // Skip leading whitespace, then keep the first non-ws byte. The
    // caller's capacity guarantee covers this single-byte append, so
    // no bounds check needed here (max_token_size is always >= 1).
    while (true) {
        const b = try r.takeByte();
        if (!isWs(b)) {
            buf.appendAssumeCapacity(b);
            break;
        }
    }
    // Collect until the next whitespace (or EOF). Check the length BEFORE
    // each append — so the `max_token_size + 1`-th byte is rejected
    // instead of overflowing the caller's reservation.
    while (true) {
        const b = r.takeByte() catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        if (isWs(b)) break;
        if (buf.items.len - start >= max_token_size) return error.TokenTooLong;
        buf.appendAssumeCapacity(b);
    }

    // Hex-encoded token? Decode in place. Write position (start + i)
    // always trails read position (start + 2 + 2*i), so the overlap is
    // safe — hexToBytes only touches a source byte before the destination
    // byte that would overwrite it.
    const bytes = buf.items[start..];
    if (bytes.len >= 2 and bytes[0] == '0' and bytes[1] == 'x') {
        const hex = bytes[2..];
        if (hex.len % 2 != 0) return error.BadHex;
        _ = try std.fmt.hexToBytes(bytes[0 .. hex.len / 2], hex);
        buf.items.len = start + hex.len / 2;
    }
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
    } else if (std.mem.eql(u8, cmd, "info")) {
        return .{ .info = .{} };
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
