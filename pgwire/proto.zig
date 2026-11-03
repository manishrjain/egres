// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Postgres v3 wire-protocol message codec — pure byte buffers, no I/O.
//!
//! Two message shapes coexist on the wire:
//!  1. Startup-class messages (Startup / SSLRequest / Cancel) lack the
//!     1-byte type tag; the first 4 bytes are the total length (including
//!     the length field), followed by a 4-byte protocol code.
//!  2. Every subsequent message is `Tag(1) | Length(4) | Body(N)` where
//!     Length includes itself but excludes Tag.
//!
//! All multi-byte integers are big-endian. Strings are NUL-terminated
//! C strings (libpq interpretation).
//!
//! This file deliberately exposes byte-level operations only — encoders
//! append to an `std.ArrayList(u8)`, decoders parse from `String`.
//! Anything needing a connection state machine lives in conn_state.zig;
//! anything touching sockets lives in driver.zig.

const std = @import("std");
const utils = @import("utils");
const String = utils.String;
const _types = @import("types.zig");

/// Byte tag a backend may send. Names follow the protocol docs (single
/// uppercase letter on the wire), but we keep readable Zig identifiers.
pub const BackendTag = enum(u8) {
    authentication = 'R',
    backend_key_data = 'K',
    parameter_status = 'S',
    ready_for_query = 'Z',
    row_description = 'T',
    data_row = 'D',
    command_complete = 'C',
    error_response = 'E',
    notice_response = 'N',
    parameter_description = 't',
    parse_complete = '1',
    bind_complete = '2',
    close_complete = '3',
    no_data = 'n',
    portal_suspended = 's',
    empty_query_response = 'I',
    copy_in_response = 'G',
    copy_out_response = 'H',
    copy_both_response = 'W',
    copy_data = 'd',
    copy_done = 'c',
    copy_fail = 'f',
};

/// Tags a frontend may send. Same comment as BackendTag.
pub const FrontendTag = enum(u8) {
    query = 'Q',
    parse = 'P',
    bind = 'B',
    execute = 'E',
    describe = 'D',
    close = 'C',
    sync = 'S',
    flush = 'H',
    terminate = 'X',
    password = 'p',
    function_call = 'F',
    copy_data = 'd',
    copy_done = 'c',
    copy_fail = 'f',
};

/// Authentication subtype tag (first int32 of an AuthenticationXxx body).
pub const AuthRequest = enum(u32) {
    ok = 0,
    kerberos_v5 = 2,
    cleartext_password = 3,
    md5_password = 5,
    scm_credential = 6,
    gss = 7,
    gss_continue = 8,
    sspi = 9,
    sasl = 10,
    sasl_continue = 11,
    sasl_final = 12,
};

/// Decoded view over a single (post-startup) message: tag plus a slice
/// pointing inside the original buffer. The slice is borrowed — callers
/// must not retain it past the next read.
pub const Message = struct {
    tag: u8,
    body: String,
};

pub const DecodeError = error{
    /// Need more bytes before a full header is available.
    NeedMore,
    /// Length field was < 4 (must include itself) or implausibly large.
    BadLength,
    /// String field missing its terminating NUL within the body.
    MissingNul,
    /// Field count or length disagreed with the body slice.
    Truncated,
    /// First int32 of a body did not match the expected sentinel.
    UnexpectedCode,
} || std.Io.Reader.DelimiterError;
// `DelimiterError` is `error{ EndOfStream, StreamTooLong, ReadFailed }`;
// we fold them into our error set so body parsers can `try` straight
// against `std.Io.Reader.fixed(body)` without per-call remapping.
// Callers in conn_state.zig already collapse anything that isn't
// `NeedMore` into `BadProtocol`, so the wider tag set doesn't
// reach the wire.

/// Maximum message body we'll decode. The Postgres protocol caps frame
/// length at int32, but we set a hard ceiling so a malicious client
/// can't make us allocate gigabytes.
pub const max_message_body: u32 = 16 * 1024 * 1024;

/// Try to read a single tagged message from `buf`. Returns the message
/// view and the number of bytes consumed, or `error.NeedMore` if `buf`
/// doesn't yet hold a full frame.
///
/// Caller is responsible for advancing its read cursor by `consumed`
/// bytes after a successful return; `body` borrows from `buf`.
pub fn decodeMessage(buf: String) DecodeError!struct {
    msg: Message,
    consumed: usize,
} {
    if (buf.len < 5) return error.NeedMore;
    const tag = buf[0];
    const len = std.mem.readInt(u32, buf[1..5], .big);
    if (len < 4) return error.BadLength;
    if (len > max_message_body + 4) return error.BadLength;
    const total = 1 + @as(usize, len);
    if (buf.len < total) return error.NeedMore;
    return .{
        .msg = .{ .tag = tag, .body = buf[5..total] },
        .consumed = total,
    };
}

/// Try to read a startup-class message (no tag byte). Returns the
/// 4-byte protocol code, the body after the code, and bytes consumed.
pub fn decodeStartup(buf: String) DecodeError!struct {
    code: u32,
    body: String,
    consumed: usize,
} {
    if (buf.len < 4) return error.NeedMore;
    const len = std.mem.readInt(u32, buf[0..4], .big);
    if (len < 8) return error.BadLength;
    if (len > max_message_body) return error.BadLength;
    if (buf.len < len) return error.NeedMore;
    const code = std.mem.readInt(u32, buf[4..8], .big);
    return .{
        .code = code,
        .body = buf[8..len],
        .consumed = len,
    };
}

/// Parsed StartupMessage parameters. `params` borrows key/value slices
/// from the input buffer — copy them out if you need to keep them.
pub const StartupParams = struct {
    user: ?String = null,
    database: ?String = null,
    application_name: ?String = null,
    client_encoding: ?String = null,

    /// Iterator over every (key, value) pair. Use this for parameters
    /// outside the named ones we hoist into struct fields.
    pub const Iterator = struct {
        body: String,
        idx: usize = 0,

        pub fn next(self: *Iterator) ?struct { key: String, val: String } {
            if (self.idx >= self.body.len) return null;
            const key_end = std.mem.indexOfScalarPos(
                u8,
                self.body,
                self.idx,
                0,
            ) orelse return null;
            // The terminating empty key signals end-of-params. The
            // raw byte sequence is `\0` immediately after a NUL — i.e.
            // a zero-length key.
            if (key_end == self.idx) {
                self.idx = self.body.len;
                return null;
            }
            const key = self.body[self.idx..key_end];
            const val_start = key_end + 1;
            const val_end = std.mem.indexOfScalarPos(
                u8,
                self.body,
                val_start,
                0,
            ) orelse return null;
            const val = self.body[val_start..val_end];
            self.idx = val_end + 1;
            return .{ .key = key, .val = val };
        }
    };

    pub fn iterate(body: String) Iterator {
        return .{ .body = body };
    }
};

/// Decode the body of a Startup3 message. Pulls out user/database/etc.
/// into named fields; any extra params are ignored (caller can re-walk
/// with `StartupParams.iterate` if needed).
pub fn parseStartupParams(body: String) DecodeError!StartupParams {
    var out: StartupParams = .{};
    var it = StartupParams.iterate(body);
    while (it.next()) |kv| {
        if (std.mem.eql(u8, kv.key, "user")) out.user = kv.val;
        if (std.mem.eql(u8, kv.key, "database")) out.database = kv.val;
        if (std.mem.eql(u8, kv.key, "application_name"))
            out.application_name = kv.val;
        if (std.mem.eql(u8, kv.key, "client_encoding"))
            out.client_encoding = kv.val;
    }
    return out;
}

/// Body of a frontend Query message: a single NUL-terminated SQL
/// string. Returns the SQL bytes (without the NUL).
pub fn parseQuery(body: String) DecodeError!String {
    if (body.len == 0) return error.Truncated;
    if (body[body.len - 1] != 0) return error.MissingNul;
    return body[0 .. body.len - 1];
}

// ---------- Extended-query body parsers ----------
//
// Each `parseXxx` opens an `std.Io.Reader.fixed(body)` and pulls
// fields with `takeInt`/`takeByte`/`readCString`/`take`. The reader's
// underlying buffer is the body slice itself, so the returned slices
// borrow in place — no copies for SQL strings, parameter values, or
// names.
//
// Bounds checks come for free via `error.EndOfStream`; missing-NUL
// in a C-string surfaces as `error.EndOfStream` too (the inclusive
// delimiter call returns it when the body ends without a NUL). Both
// flow through `DecodeError`.

/// Read a NUL-terminated C-string from `r`, returning the bytes
/// without the trailing NUL. Uses `takeDelimiterInclusive` so the
/// NUL is consumed from the stream (`takeDelimiterExclusive` leaves
/// it pending — see std/Io/Reader.zig).
fn readCString(r: *std.Io.Reader) DecodeError!String {
    const slice = try r.takeDelimiterInclusive(0);
    return slice[0 .. slice.len - 1];
}

/// Parse('P') body: prepared-statement name + SQL + param OIDs.
/// `name == ""` means the unnamed prepared statement (overwritten on
/// each Parse). `param_oids` may be empty — that means "the backend
/// should infer types"; otherwise it's the explicit list the client
/// requests.
pub const ParseMsg = struct {
    name: String,
    sql: String,
    param_oids: []const u32,
};

/// `oids_storage` is filled in with up to N entries; the returned
/// slice borrows from it. Caller must keep `oids_storage` alive for
/// as long as the returned `ParseMsg.param_oids` is used.
pub fn parseParse(
    body: String,
    oids_storage: []u32,
) DecodeError!ParseMsg {
    var r = std.Io.Reader.fixed(body);
    const name = try readCString(&r);
    const sql = try readCString(&r);
    const n: i16 = try r.takeInt(i16, .big);
    if (n < 0) return error.Truncated;
    const cnt: usize = @intCast(n);
    if (cnt > oids_storage.len) return error.Truncated;
    var i: usize = 0;
    while (i < cnt) : (i += 1) {
        oids_storage[i] = try r.takeInt(u32, .big);
    }
    return .{
        .name = name,
        .sql = sql,
        .param_oids = oids_storage[0..cnt],
    };
}

/// One parameter value as it appears in a Bind message: NULL or a
/// raw byte slice (whose interpretation depends on the format code,
/// not encoded here).
pub const BindParam = ?String;

/// Bind('B') body decoded view. All slices borrow from the input.
/// `param_formats.len` is 0 (text for all), 1 (the single value
/// applied to every param), or `params.len` (per-param). Same for
/// `result_formats` against the eventual RowDescription columns.
pub const BindMsg = struct {
    portal_name: String,
    statement_name: String,
    param_formats: []const i16,
    params: []const BindParam,
    result_formats: []const i16,
};

/// Decode the body of a Bind message. Caller supplies three scratch
/// arrays (param_formats, params, result_formats) sized large enough
/// for the decoded counts; if any is too small we return Truncated.
/// The returned slices borrow from those scratch arrays + the input
/// body.
pub fn parseBind(
    body: String,
    pfmt_storage: []i16,
    params_storage: []BindParam,
    rfmt_storage: []i16,
) DecodeError!BindMsg {
    var r = std.Io.Reader.fixed(body);
    const portal = try readCString(&r);
    const stmt = try readCString(&r);

    const npf: i16 = try r.takeInt(i16, .big);
    if (npf < 0) return error.Truncated;
    const npf_n: usize = @intCast(npf);
    if (npf_n > pfmt_storage.len) return error.Truncated;
    var i: usize = 0;
    while (i < npf_n) : (i += 1) pfmt_storage[i] = try r.takeInt(i16, .big);

    const np: i16 = try r.takeInt(i16, .big);
    if (np < 0) return error.Truncated;
    const np_n: usize = @intCast(np);
    if (np_n > params_storage.len) return error.Truncated;
    i = 0;
    while (i < np_n) : (i += 1) {
        const len = try r.takeInt(i32, .big);
        if (len < 0) {
            params_storage[i] = null;
        } else {
            params_storage[i] = try r.take(@intCast(len));
        }
    }

    const nrf: i16 = try r.takeInt(i16, .big);
    if (nrf < 0) return error.Truncated;
    const nrf_n: usize = @intCast(nrf);
    if (nrf_n > rfmt_storage.len) return error.Truncated;
    i = 0;
    while (i < nrf_n) : (i += 1) rfmt_storage[i] = try r.takeInt(i16, .big);

    return .{
        .portal_name = portal,
        .statement_name = stmt,
        .param_formats = pfmt_storage[0..npf_n],
        .params = params_storage[0..np_n],
        .result_formats = rfmt_storage[0..nrf_n],
    };
}

/// Execute('E') body: portal name + max rows (0 = no limit).
pub const ExecuteMsg = struct {
    portal_name: String,
    max_rows: u32,
};

pub fn parseExecute(body: String) DecodeError!ExecuteMsg {
    var r = std.Io.Reader.fixed(body);
    const portal = try readCString(&r);
    const max_rows = try r.takeInt(u32, .big);
    return .{ .portal_name = portal, .max_rows = max_rows };
}

/// Describe and Close share the same body shape.
pub const DescribeKind = enum(u8) {
    statement = 'S',
    portal = 'P',
};

pub const DescribeMsg = struct {
    kind: DescribeKind,
    name: String,
};

pub fn parseDescribe(body: String) DecodeError!DescribeMsg {
    var r = std.Io.Reader.fixed(body);
    const kind = try r.takeByte();
    if (kind != @intFromEnum(DescribeKind.statement) and
        kind != @intFromEnum(DescribeKind.portal))
    {
        return error.UnexpectedCode;
    }
    const name = try readCString(&r);
    return .{
        .kind = @enumFromInt(kind),
        .name = name,
    };
}

/// Close has identical wire shape to Describe.
pub const CloseMsg = DescribeMsg;
pub const parseClose = parseDescribe;

// ---------- Encoders ----------
//
// Two layers:
//
//   1. Body builders — `encodeXxx` for variable-body messages. These
//      take a `*std.Io.Writer.Allocating` (scratch) and fill it with
//      the body bytes. They never see `out`.
//   2. Frame writers — `writeFrame` / `writeFrameEmpty` ship a
//      `[tag][length][body]` (or empty body) tuple to `out`.
//
// The caller (typically a `sendXxx` method on `ConnState`) glues the
// two: build body in scratch, then ship via `writeFrame`. This mirrors
// the decode side, where `decodeMessage` parses a frame and exposes
// the body separately for caller-driven interpretation.
//
// Fixed-body messages (AuthenticationOk, BackendKeyData,
// ReadyForQuery, the empty-bodied ext-query family) skip scratch
// entirely — they know their body length statically and write
// `[tag][length][body]` straight to `out`.

/// Write `[tag][length=4+body.len][body]` to `out`. The length field
/// includes itself but not the tag, per protocol.
pub fn writeFrame(out: *std.Io.Writer, tag: BackendTag, body: String) std.Io.Writer.Error!void {
    try out.writeByte(@intFromEnum(tag));
    try out.writeInt(u32, @intCast(4 + body.len), .big);
    try out.writeAll(body);
}

/// `[tag][length=4]` — empty-bodied frames (ParseComplete, BindComplete,
/// CloseComplete, NoData, PortalSuspended).
pub fn writeFrameEmpty(out: *std.Io.Writer, tag: BackendTag) std.Io.Writer.Error!void {
    try out.writeByte(@intFromEnum(tag));
    try out.writeInt(u32, 4, .big);
}

fn writeCString(w: *std.Io.Writer, s: String) std.Io.Writer.Error!void {
    // Embedded NUL would split the field on the wire — receiver would
    // parse the inner NUL as the field terminator, corrupting every
    // subsequent field. UTF-8 never produces stray NULs organically,
    // so the only way one slips in is via buggy embedder input. Debug
    // + ReleaseSafe catch it; ReleaseFast skips the scan.
    std.debug.assert(std.mem.indexOfScalar(u8, s, 0) == null);
    try w.writeAll(s);
    try w.writeByte(0);
}

/// Reset scratch's internal cursor without freeing capacity. Subsequent
/// writes append from offset zero. Equivalent of ArrayList
/// clearRetainingCapacity for the Allocating writer.
fn resetScratch(scratch: *std.Io.Writer.Allocating) void {
    scratch.writer.end = 0;
}

/// AuthenticationOk: tag 'R', body is int32(0). We special-case it
/// because it's currently the only authentication response we send.
/// TODO: support real auth methods (md5, scram-sha-256).
pub fn encodeAuthenticationOk(out: *std.Io.Writer) std.Io.Writer.Error!void {
    try out.writeByte(@intFromEnum(BackendTag.authentication));
    try out.writeInt(u32, 8, .big); // length: 4 (self) + 4 (auth code)
    try out.writeInt(u32, @intFromEnum(AuthRequest.ok), .big);
}

/// ParameterStatus body (tag 'S'): two C strings (name, value).
/// Postgres clients rely on a small set of these (server_version,
/// client_encoding, DateStyle) being announced after AuthenticationOk;
/// libpq uses them to decide formatting.
pub fn encodeParameterStatus(
    scratch: *std.Io.Writer.Allocating,
    name: String,
    value: String,
) !void {
    resetScratch(scratch);
    const w = &scratch.writer;
    try writeCString(w, name);
    try writeCString(w, value);
}

/// BackendKeyData: tag 'K', body is int32(pid) + int32(secret_key).
/// Used by the cancel-request flow. We send constant placeholders
/// because libpq still expects the bytes during startup.
/// TODO: support CancelRequest — track per-connection (pid, secret)
/// and route a matching CancelRequest to the executor running on
/// the original connection.
pub fn encodeBackendKeyData(
    out: *std.Io.Writer,
    pid: u32,
    secret: u32,
) std.Io.Writer.Error!void {
    try out.writeByte(@intFromEnum(BackendTag.backend_key_data));
    try out.writeInt(u32, 12, .big); // 4 (self) + 4 (pid) + 4 (secret)
    try out.writeInt(u32, pid, .big);
    try out.writeInt(u32, secret, .big);
}

/// ReadyForQuery: tag 'Z', body is one byte (transaction status).
pub fn encodeReadyForQuery(
    out: *std.Io.Writer,
    status: _types.TxStatus,
) std.Io.Writer.Error!void {
    try out.writeByte(@intFromEnum(BackendTag.ready_for_query));
    try out.writeInt(u32, 5, .big); // 4 (self) + 1 (status byte)
    try out.writeByte(@intFromEnum(status));
}

/// RowDescription body (tag 'T'): int16(field_count) + per-field
/// (cstring name, int32 table_oid, int16 col_attr, int32 type_oid,
/// int16 type_size, int32 type_modifier, int16 format_code).
pub fn encodeRowDescription(
    scratch: *std.Io.Writer.Allocating,
    fields: []const _types.FieldDescription,
) !void {
    resetScratch(scratch);
    const w = &scratch.writer;
    try w.writeInt(i16, @intCast(fields.len), .big);
    for (fields) |f| {
        try writeCString(w, f.name);
        try w.writeInt(u32, f.table_oid, .big);
        try w.writeInt(i16, f.column_attr, .big);
        try w.writeInt(u32, f.type_oid, .big);
        try w.writeInt(i16, f.type_size, .big);
        try w.writeInt(i32, f.type_modifier, .big);
        try w.writeInt(i16, @intFromEnum(f.wirefmt), .big);
    }
}

/// One column's bytes for a DataRow. `null_value` means NULL (encoded
/// as int32(-1) on the wire); otherwise the slice is sent verbatim
/// after a length prefix. The two non-null variants exist for
/// readability — wire-wise they're identical (both are just a length
/// prefix + bytes), but having distinct tags forces the handler to
/// commit to a format per cell, which usually catches the
/// "I forgot to consult result_formats" bug at the type level.
pub const DataValue = union(enum) {
    null_value,
    text: String,
    binary: String,
};

/// DataRow body (tag 'D'): int16(col_count) + per-col (int32 length,
/// length bytes; -1 for NULL).
pub fn encodeDataRow(
    scratch: *std.Io.Writer.Allocating,
    cols: []const DataValue,
) !void {
    resetScratch(scratch);
    const w = &scratch.writer;
    try w.writeInt(i16, @intCast(cols.len), .big);
    for (cols) |c| switch (c) {
        .null_value => try w.writeInt(i32, -1, .big),
        .text, .binary => |bytes| {
            try w.writeInt(i32, @intCast(bytes.len), .big);
            try w.writeAll(bytes);
        },
    };
}

/// Streaming DataRow encoder: format columns one at a time directly into
/// `scratch` (rather than buffering each cell, then copying again), then
/// `finish` writes the whole frame in one go. Each column reserves a 4-byte
/// length placeholder that `endColumn` backpatches once the value's bytes are
/// written. Column count is fixed up front.
pub const DataRowWriter = struct {
    scratch: *std.Io.Writer.Allocating,
    out: *std.Io.Writer,
    col_at: usize = 0, // placeholder offset of the column in progress

    /// Reset the scratch and write the column count.
    pub fn begin(scratch: *std.Io.Writer.Allocating, out: *std.Io.Writer, field_count: u16) !DataRowWriter {
        resetScratch(scratch);
        try scratch.writer.writeInt(i16, @intCast(field_count), .big);
        return .{ .scratch = scratch, .out = out };
    }

    /// A NULL column (length -1, no bytes).
    pub fn writeNull(self: *DataRowWriter) !void {
        try self.scratch.writer.writeInt(i32, -1, .big);
    }

    /// Begin a non-null column: reserve its length and return the writer to
    /// format the value into. Pair with `endColumn`.
    pub fn beginColumn(self: *DataRowWriter) !*std.Io.Writer {
        self.col_at = self.scratch.writer.end;
        try self.scratch.writer.writeInt(i32, 0, .big); // placeholder, backpatched below
        return &self.scratch.writer;
    }

    /// Finish a non-null column: backpatch its byte length.
    pub fn endColumn(self: *DataRowWriter) void {
        const len: i32 = @intCast(self.scratch.writer.end - self.col_at - 4);
        std.mem.writeInt(i32, self.scratch.writer.buffered()[self.col_at..][0..4], len, .big);
    }

    /// Emit the finished DataRow as one frame.
    pub fn finish(self: *DataRowWriter) !void {
        try writeFrame(self.out, .data_row, self.scratch.writer.buffered());
    }
};

/// CommandComplete body (tag 'C'): one C string ("SELECT 1",
/// "INSERT 0 5", etc.).
pub fn encodeCommandComplete(
    scratch: *std.Io.Writer.Allocating,
    tag: String,
) !void {
    resetScratch(scratch);
    try writeCString(&scratch.writer, tag);
}

/// EmptyQueryResponse: tag 'I', empty body. Sent when the query was
/// just whitespace/comments — libpq still needs *something* before
/// ReadyForQuery.
pub fn encodeEmptyQueryResponse(out: *std.Io.Writer) std.Io.Writer.Error!void {
    try writeFrameEmpty(out, .empty_query_response);
}

/// Description of a single ErrorResponse field. The protocol uses a
/// flexible TLV-ish format: each field is `code(1) | C-string`, then
/// a final 0 byte ends the message.
pub const ErrorField = struct {
    /// Severity: 'ERROR', 'FATAL', 'PANIC', etc.
    severity: String = _types.severity_error,
    /// 5-char SQLSTATE.
    code: [5]u8,
    message: String,
    /// Optional detail/hint/position; null if absent.
    detail: ?String = null,
    hint: ?String = null,
    /// 1-based byte position in the offending query, if known.
    position: ?i32 = null,
};

// ---------- Extended-query backend encoders ----------

pub fn encodeParseComplete(out: *std.Io.Writer) std.Io.Writer.Error!void {
    try writeFrameEmpty(out, .parse_complete);
}

pub fn encodeBindComplete(out: *std.Io.Writer) std.Io.Writer.Error!void {
    try writeFrameEmpty(out, .bind_complete);
}

pub fn encodeCloseComplete(out: *std.Io.Writer) std.Io.Writer.Error!void {
    try writeFrameEmpty(out, .close_complete);
}

pub fn encodeNoData(out: *std.Io.Writer) std.Io.Writer.Error!void {
    try writeFrameEmpty(out, .no_data);
}

pub fn encodePortalSuspended(out: *std.Io.Writer) std.Io.Writer.Error!void {
    try writeFrameEmpty(out, .portal_suspended);
}

/// ParameterDescription body (tag 't'): int16(count) + int32 OIDs.
/// Sent in response to Describe('S') before the RowDescription/NoData.
pub fn encodeParameterDescription(
    scratch: *std.Io.Writer.Allocating,
    oids: []const u32,
) !void {
    resetScratch(scratch);
    const w = &scratch.writer;
    try w.writeInt(i16, @intCast(oids.len), .big);
    for (oids) |oid| try w.writeInt(u32, oid, .big);
}

/// ErrorResponse body (tag 'E'): TLV-ish field stream terminated by 0.
pub fn encodeErrorResponse(
    scratch: *std.Io.Writer.Allocating,
    f: ErrorField,
) !void {
    resetScratch(scratch);
    const w = &scratch.writer;
    try w.writeByte('S');
    try writeCString(w, f.severity);
    try w.writeByte('V');
    try writeCString(w, f.severity);
    try w.writeByte('C');
    try writeCString(w, &f.code);
    try w.writeByte('M');
    try writeCString(w, f.message);
    if (f.detail) |d| {
        try w.writeByte('D');
        try writeCString(w, d);
    }
    if (f.hint) |h| {
        try w.writeByte('H');
        try writeCString(w, h);
    }
    if (f.position) |p| {
        try w.writeByte('P');
        var num: [16]u8 = undefined;
        const n = std.fmt.printInt(&num, p, 10, .lower, .{});
        try writeCString(w, num[0..n]);
    }
    try w.writeByte(0);
}

test {
    std.testing.refAllDecls(@This());
}
