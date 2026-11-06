// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Protocol-layer tests. NO runtime, NO sockets — every byte goes through
//! a memory buffer.
//!
//! Goldens are hand-crafted from the documented Postgres v3 wire
//! formats (https://www.postgresql.org/docs/current/protocol-
//! message-formats.html). They match the bytes a real libpq sends
//! verbatim — when you regenerate them, capture from a libpq client
//! and re-paste; the format is locked by spec.

const std = @import("std");
const testing = std.testing;
const pgwire = @import("pgwire");
const utils = @import("utils");
const String = utils.String;

const proto = pgwire.proto;
const types = pgwire.types;
const conn_state = pgwire.conn_state;

// Reset the test-side Allocating writer's cursor without freeing
// capacity. The state machine sends frames into `&aw.writer`; tests
// that drive past one phase and want to inspect *only* the next
// phase's output call this between steps.
fn resetOut(aw: *std.Io.Writer.Allocating) void {
    aw.writer.end = 0;
}

// ---------- Hex helpers ----------

fn hexToBytes(comptime hex: String) [hex.len / 2]u8 {
    @setEvalBranchQuota(8000);
    var out: [hex.len / 2]u8 = undefined;
    var i: usize = 0;
    while (i < hex.len) : (i += 2) {
        out[i / 2] = std.fmt.parseInt(u8, hex[i .. i + 2], 16) catch unreachable;
    }
    return out;
}

// ---------- Golden fixtures ----------

// Startup3 with params "user=alice", "database=test"
//   length(4)  = 0x00000022  (4 + 4 + len(params)=26 → 34)
//   version(4) = 0x00030000
//   params     = "user\0alice\0database\0test\0" + trailing NUL
//
// Wire bytes (decoded length 34):
//   00 00 00 22 00 03 00 00
//   75 73 65 72 00 61 6c 69 63 65 00
//   64 61 74 61 62 61 73 65 00 74 65 73 74 00
//   00
const startup_golden_hex =
    "0000002200030000" ++
    "75736572" ++ "00" ++ "616c696365" ++ "00" ++
    "6461746162617365" ++ "00" ++ "74657374" ++ "00" ++
    "00";

// Frontend Query("SELECT 1")
//   tag(1)    = 'Q' = 0x51
//   length(4) = 0x0000000D  (4 + 9 = 13)
//   body      = "SELECT 1\0"
const query_golden_hex = "51" ++ "0000000d" ++ "53454c454354203100";

// Terminate
//   tag(1)    = 'X' = 0x58
//   length(4) = 0x00000004
const terminate_golden_hex = "58" ++ "00000004";

// SSLRequest sentinel — same length-prefix shape as Startup.
//   length(4) = 0x00000008
//   code(4)   = 0x04D2 162F  (1234 << 16 | 5679)
const ssl_request_golden_hex = "00000008" ++ "04d2162f";

test "decodeStartup: golden Startup3" {
    const bytes = hexToBytes(startup_golden_hex);
    const r = try proto.decodeStartup(&bytes);
    try testing.expectEqual(@as(u32, types.protocol_version_3), r.code);
    try testing.expectEqual(bytes.len, r.consumed);
    const params = try proto.parseStartupParams(r.body);
    try testing.expect(params.user != null);
    try testing.expectEqualStrings("alice", params.user.?);
    try testing.expectEqualStrings("test", params.database.?);
}

test "decodeStartup: SSLRequest sentinel" {
    const bytes = hexToBytes(ssl_request_golden_hex);
    const r = try proto.decodeStartup(&bytes);
    try testing.expectEqual(@as(u32, types.ssl_request_code), r.code);
    try testing.expectEqual(@as(usize, 0), r.body.len);
    try testing.expectEqual(bytes.len, r.consumed);
}

test "decodeMessage: golden Query" {
    const bytes = hexToBytes(query_golden_hex);
    const r = try proto.decodeMessage(&bytes);
    try testing.expectEqual(@as(u8, 'Q'), r.msg.tag);
    try testing.expectEqual(bytes.len, r.consumed);
    const sql = try proto.parseQuery(r.msg.body);
    try testing.expectEqualStrings("SELECT 1", sql);
}

test "decodeMessage: golden Terminate" {
    const bytes = hexToBytes(terminate_golden_hex);
    const r = try proto.decodeMessage(&bytes);
    try testing.expectEqual(@as(u8, 'X'), r.msg.tag);
    try testing.expectEqual(@as(usize, 0), r.msg.body.len);
    try testing.expectEqual(bytes.len, r.consumed);
}

test "decodeMessage: NeedMore on partial header" {
    const partial = [_]u8{ 'Q', 0, 0 };
    try testing.expectError(error.NeedMore, proto.decodeMessage(&partial));
}

test "decodeMessage: NeedMore on partial body" {
    // Tag + length=10, but only 3 body bytes given.
    const partial = [_]u8{ 'Q', 0, 0, 0, 10, 1, 2, 3 };
    try testing.expectError(error.NeedMore, proto.decodeMessage(&partial));
}

test "decodeMessage: BadLength rejects len<4" {
    const bad = [_]u8{ 'Q', 0, 0, 0, 3 };
    try testing.expectError(error.BadLength, proto.decodeMessage(&bad));
}

// ---------- Round-trip: encode then decode ----------

test "encode/decode round-trip: AuthenticationOk" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    try proto.encodeAuthenticationOk(&aw.writer);

    const r = try proto.decodeMessage(aw.writer.buffered());
    try testing.expectEqual(@as(u8, 'R'), r.msg.tag);
    try testing.expectEqual(@as(usize, 4), r.msg.body.len);
    const code = std.mem.readInt(u32, r.msg.body[0..4], .big);
    try testing.expectEqual(@as(u32, 0), code);
}

test "encode/decode round-trip: ParameterStatus" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    var scratch: std.Io.Writer.Allocating = .init(a);
    defer scratch.deinit();
    try proto.encodeParameterStatus(&scratch, "server_version", "13.0");
    try proto.writeFrame(&aw.writer, .parameter_status, scratch.writer.buffered());

    const r = try proto.decodeMessage(aw.writer.buffered());
    try testing.expectEqual(@as(u8, 'S'), r.msg.tag);
    // Body is two C strings: "server_version\0" + "13.0\0".
    try testing.expectEqualStrings(
        "server_version\x0013.0\x00",
        r.msg.body,
    );
}

test "encode/decode round-trip: BackendKeyData" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    try proto.encodeBackendKeyData(&aw.writer, 0xAABBCCDD, 0x11223344);

    const r = try proto.decodeMessage(aw.writer.buffered());
    try testing.expectEqual(@as(u8, 'K'), r.msg.tag);
    try testing.expectEqual(@as(usize, 8), r.msg.body.len);
    const pid = std.mem.readInt(u32, r.msg.body[0..4], .big);
    const sec = std.mem.readInt(u32, r.msg.body[4..8], .big);
    try testing.expectEqual(@as(u32, 0xAABBCCDD), pid);
    try testing.expectEqual(@as(u32, 0x11223344), sec);
}

test "encode/decode round-trip: ReadyForQuery" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    try proto.encodeReadyForQuery(&aw.writer, .idle);

    const r = try proto.decodeMessage(aw.writer.buffered());
    try testing.expectEqual(@as(u8, 'Z'), r.msg.tag);
    try testing.expectEqual(@as(usize, 1), r.msg.body.len);
    try testing.expectEqual(@as(u8, 'I'), r.msg.body[0]);
}

test "encode/decode round-trip: RowDescription" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    var scratch: std.Io.Writer.Allocating = .init(a);
    defer scratch.deinit();
    const fields = [_]types.FieldDescription{
        .{ .name = "id", .type_oid = types.oid.int4, .type_size = 4 },
        .{ .name = "name", .type_oid = types.oid.text },
    };
    try proto.encodeRowDescription(&scratch, &fields);
    try proto.writeFrame(&aw.writer, .row_description, scratch.writer.buffered());

    const r = try proto.decodeMessage(aw.writer.buffered());
    try testing.expectEqual(@as(u8, 'T'), r.msg.tag);
    const count = std.mem.readInt(i16, r.msg.body[0..2], .big);
    try testing.expectEqual(@as(i16, 2), count);

    const off: usize = 2;
    try testing.expectEqualStrings(
        "id\x00",
        r.msg.body[off .. off + 3],
    );
}

test "encode DataRow with mixed text/binary cells" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    var scratch: std.Io.Writer.Allocating = .init(a);
    defer scratch.deinit();

    // int4(101) binary + "hello" text — the same wire shape pgx
    // expects when it asks for `result_format = [1, 0]`.
    var int_bytes: [4]u8 = undefined;
    pgwire.binary.encodeInt4(&int_bytes, 101);

    const cols = [_]proto.DataValue{
        .{ .binary = &int_bytes },
        .{ .text = "hello" },
    };
    try proto.encodeDataRow(&scratch, &cols);
    try proto.writeFrame(&aw.writer, .data_row, scratch.writer.buffered());

    const r = try proto.decodeMessage(aw.writer.buffered());
    try testing.expectEqual(@as(u8, 'D'), r.msg.tag);
    const cnt = std.mem.readInt(i16, r.msg.body[0..2], .big);
    try testing.expectEqual(@as(i16, 2), cnt);

    // Col 0: int32 length=4, then 4 raw bytes encoding 101 BE.
    var off: usize = 2;
    try testing.expectEqual(
        @as(i32, 4),
        std.mem.readInt(i32, r.msg.body[off..][0..4], .big),
    );
    off += 4;
    try testing.expectEqualSlices(
        u8,
        &.{ 0, 0, 0, 101 },
        r.msg.body[off .. off + 4],
    );
    off += 4;
    // Col 1: int32 length=5, then "hello".
    try testing.expectEqual(
        @as(i32, 5),
        std.mem.readInt(i32, r.msg.body[off..][0..4], .big),
    );
    off += 4;
    try testing.expectEqualStrings("hello", r.msg.body[off .. off + 5]);
}

test "encode/decode round-trip: DataRow with NULL" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    var scratch: std.Io.Writer.Allocating = .init(a);
    defer scratch.deinit();
    const cols = [_]proto.DataValue{
        .{ .text = "1" },
        .null_value,
        .{ .text = "abc" },
    };
    try proto.encodeDataRow(&scratch, &cols);
    try proto.writeFrame(&aw.writer, .data_row, scratch.writer.buffered());

    const r = try proto.decodeMessage(aw.writer.buffered());
    try testing.expectEqual(@as(u8, 'D'), r.msg.tag);
    const count = std.mem.readInt(i16, r.msg.body[0..2], .big);
    try testing.expectEqual(@as(i16, 3), count);
    var off: usize = 2;
    try testing.expectEqual(
        @as(i32, 1),
        std.mem.readInt(i32, r.msg.body[off..][0..4], .big),
    );
    off += 4;
    try testing.expectEqualStrings("1", r.msg.body[off .. off + 1]);
    off += 1;
    try testing.expectEqual(
        @as(i32, -1),
        std.mem.readInt(i32, r.msg.body[off..][0..4], .big),
    );
    off += 4;
    try testing.expectEqual(
        @as(i32, 3),
        std.mem.readInt(i32, r.msg.body[off..][0..4], .big),
    );
    off += 4;
    try testing.expectEqualStrings("abc", r.msg.body[off .. off + 3]);
}

test "encode/decode round-trip: CommandComplete" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    var scratch: std.Io.Writer.Allocating = .init(a);
    defer scratch.deinit();
    try proto.encodeCommandComplete(&scratch, "SELECT 1");
    try proto.writeFrame(&aw.writer, .command_complete, scratch.writer.buffered());

    const r = try proto.decodeMessage(aw.writer.buffered());
    try testing.expectEqual(@as(u8, 'C'), r.msg.tag);
    try testing.expectEqualStrings("SELECT 1\x00", r.msg.body);
}

test "encode: ErrorResponse contains expected fields" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    var scratch: std.Io.Writer.Allocating = .init(a);
    defer scratch.deinit();
    try proto.encodeErrorResponse(&scratch, .{
        .code = types.sqlstate.syntax_error,
        .message = "boom",
        .position = 7,
    });
    try proto.writeFrame(&aw.writer, .error_response, scratch.writer.buffered());

    const r = try proto.decodeMessage(aw.writer.buffered());
    try testing.expectEqual(@as(u8, 'E'), r.msg.tag);
    try testing.expect(std.mem.indexOf(u8, r.msg.body, "SERROR\x00") != null);
    try testing.expect(std.mem.indexOf(u8, r.msg.body, "C42601\x00") != null);
    try testing.expect(std.mem.indexOf(u8, r.msg.body, "Mboom\x00") != null);
    try testing.expect(std.mem.indexOf(u8, r.msg.body, "P7\x00") != null);
    try testing.expectEqual(@as(u8, 0), r.msg.body[r.msg.body.len - 1]);
}

// ---------- State machine tests ----------

test "ConnState: startup → AuthOk + ParamStatus + ReadyForQuery" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    const startup = hexToBytes(startup_golden_hex);
    var rdr = std.Io.Reader.fixed(&startup);
    var s = conn_state.ConnState.init(a, &rdr, &aw.writer, .{});
    defer s.deinit();

    const ev = try s.nextEvent();
    try testing.expect(ev == .startup_complete);
    try testing.expectEqual(conn_state.Phase.ready, s.phase);

    // Drain the prepared response stream and assert the tag sequence:
    // R(0) S+(several) K(1) Z(1).
    const out = aw.writer.buffered();
    var off: usize = 0;
    var tags: std.ArrayList(u8) = .empty;
    defer tags.deinit(a);
    while (off < out.len) {
        const r = try proto.decodeMessage(out[off..]);
        try tags.append(a, r.msg.tag);
        off += r.consumed;
    }
    try testing.expect(tags.items.len >= 4);
    try testing.expectEqual(@as(u8, 'R'), tags.items[0]);
    try testing.expectEqual(@as(u8, 'S'), tags.items[1]);
    try testing.expectEqual(@as(u8, 'K'), tags.items[tags.items.len - 2]);
    try testing.expectEqual(@as(u8, 'Z'), tags.items[tags.items.len - 1]);
}

test "ConnState: Query event surfaces SQL text" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    // Single fixed reader holding both startup and query frames —
    // the state machine processes them sequentially across nextEvent
    // calls.
    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, &hexToBytes(startup_golden_hex));
    try input.appendSlice(a, &hexToBytes(query_golden_hex));

    var rdr = std.Io.Reader.fixed(input.items);
    var s = conn_state.ConnState.init(a, &rdr, &aw.writer, .{});
    defer s.deinit();

    _ = try s.nextEvent(); // drive past startup
    resetOut(&aw);

    const ev = try s.nextEvent();
    try testing.expect(ev == .query);
    try testing.expectEqualStrings("SELECT 1", ev.query);
    try testing.expectEqual(conn_state.Phase.in_query, s.phase);
}

test "ConnState: a frame holds its own body only" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    // Startup, a Query with a 2 MiB SQL text, then the golden one.
    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, &hexToBytes(startup_golden_hex));
    const big_len: usize = 2 << 20;
    try input.append(a, 'Q');
    try input.appendSlice(a, &std.mem.toBytes(
        std.mem.nativeToBig(u32, @intCast(4 + big_len + 1)),
    ));
    try input.appendNTimes(a, 'x', big_len);
    try input.append(a, 0);
    try input.appendSlice(a, &hexToBytes(query_golden_hex));

    var rdr = std.Io.Reader.fixed(input.items);
    var s = conn_state.ConnState.init(a, &rdr, &aw.writer, .{});
    defer s.deinit();

    _ = try s.nextEvent(); // drive past startup
    const big = try s.nextEvent();
    try testing.expectEqual(big_len, big.query.len);
    // Sized to the body, not a step past it, and gone once the
    // driver is done with the event.
    try testing.expectEqual(big_len + 1, s.frame.len);
    s.releaseBuffers();
    try testing.expectEqual(0, s.frame.len);
    try testing.expectEqual(0, s.scratch_out.writer.buffer.len);

    const small = try s.nextEvent();
    try testing.expectEqualStrings("SELECT 1", small.query);
    try testing.expectEqual("SELECT 1\x00".len, s.frame.len);
}

test "ConnState: Terminate transitions to closed" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, &hexToBytes(startup_golden_hex));
    try input.appendSlice(a, &hexToBytes(terminate_golden_hex));

    var rdr = std.Io.Reader.fixed(input.items);
    var s = conn_state.ConnState.init(a, &rdr, &aw.writer, .{});
    defer s.deinit();

    _ = try s.nextEvent(); // drive past startup
    resetOut(&aw);

    const ev = try s.nextEvent();
    try testing.expect(ev == .terminate);
    try testing.expectEqual(conn_state.Phase.closed, s.phase);
}

test "ConnState: SSLRequest declined with 'N' + retried" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    // SSL request followed by a real Startup3: the state machine
    // writes 'N' for the SSL probe and then processes the real
    // startup frame in one nextEvent call.
    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, &hexToBytes(ssl_request_golden_hex));
    try input.appendSlice(a, &hexToBytes(startup_golden_hex));

    var rdr = std.Io.Reader.fixed(input.items);
    var s = conn_state.ConnState.init(a, &rdr, &aw.writer, .{});
    defer s.deinit();

    const ev = try s.nextEvent();
    try testing.expect(ev == .startup_complete);

    // The 'N' decline byte should be the first byte written.
    const out = aw.writer.buffered();
    try testing.expect(out.len >= 1);
    try testing.expectEqual(@as(u8, 'N'), out[0]);
}

test "ConnState: clean EOF before next frame yields .terminate" {
    // Replaces the old "split frames over multiple feeds" test:
    // partial-frame handling is now the Reader's job, but a clean
    // EOF where we'd expect the next header still needs to be
    // surfaced as terminate (driver maps that to connection close).
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    // Empty reader — peer disconnected before sending startup.
    var rdr = std.Io.Reader.fixed(&.{});
    var s = conn_state.ConnState.init(a, &rdr, &aw.writer, .{});
    defer s.deinit();

    const ev = try s.nextEvent();
    try testing.expect(ev == .terminate);
    try testing.expectEqual(conn_state.Phase.closed, s.phase);
}

// ---------- Extended-query proto tests ----------

test "decodeMessage: golden Parse + roundtrip" {
    const a = std.testing.allocator;
    // Build a Parse: name="", sql="SELECT $1", n_params=1, oids=[23]
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(a);
    try buf.append(a, 'P');
    const len_pos = buf.items.len;
    try buf.appendSlice(a, &[_]u8{ 0, 0, 0, 0 }); // placeholder len
    try buf.append(a, 0); // name (cstring)
    try buf.appendSlice(a, "SELECT $1");
    try buf.append(a, 0); // sql (cstring)
    try buf.appendSlice(a, &[_]u8{ 0, 1 }); // n_params=1 (i16 BE)
    try buf.appendSlice(a, &[_]u8{ 0, 0, 0, 23 }); // oid=23 (int4)
    const total: u32 = @intCast(buf.items.len - 1);
    std.mem.writeInt(u32, buf.items[len_pos..][0..4], total, .big);

    const r = try proto.decodeMessage(buf.items);
    try testing.expectEqual(@as(u8, 'P'), r.msg.tag);
    var oids: [8]u32 = undefined;
    const p = try proto.parseParse(r.msg.body, &oids);
    try testing.expectEqualStrings("", p.name);
    try testing.expectEqualStrings("SELECT $1", p.sql);
    try testing.expectEqual(@as(usize, 1), p.param_oids.len);
    try testing.expectEqual(@as(u32, 23), p.param_oids[0]);
}

test "decode Bind: text params, NULL handling" {
    const a = std.testing.allocator;
    // Bind: portal="", stmt="", n_pfmt=0, n_params=2 [text "42", NULL],
    //       n_rfmt=1 [0=text]
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(a);
    try buf.append(a, 'B');
    const len_pos = buf.items.len;
    try buf.appendSlice(a, &[_]u8{ 0, 0, 0, 0 });
    try buf.append(a, 0); // portal
    try buf.append(a, 0); // statement
    try buf.appendSlice(a, &[_]u8{ 0, 0 }); // n_pfmt=0
    try buf.appendSlice(a, &[_]u8{ 0, 2 }); // n_params=2
    try buf.appendSlice(a, &[_]u8{ 0, 0, 0, 2 }); // len=2
    try buf.appendSlice(a, "42");
    try buf.appendSlice(a, &[_]u8{ 0xff, 0xff, 0xff, 0xff }); // -1 = NULL
    try buf.appendSlice(a, &[_]u8{ 0, 1 }); // n_rfmt=1
    try buf.appendSlice(a, &[_]u8{ 0, 0 }); // rfmt[0]=0 (text)
    const total: u32 = @intCast(buf.items.len - 1);
    std.mem.writeInt(u32, buf.items[len_pos..][0..4], total, .big);

    const r = try proto.decodeMessage(buf.items);
    try testing.expectEqual(@as(u8, 'B'), r.msg.tag);
    var pfmt: [4]i16 = undefined;
    var params: [4]proto.BindParam = undefined;
    var rfmt: [4]i16 = undefined;
    const b = try proto.parseBind(r.msg.body, &pfmt, &params, &rfmt);
    try testing.expectEqualStrings("", b.portal_name);
    try testing.expectEqualStrings("", b.statement_name);
    try testing.expectEqual(@as(usize, 0), b.param_formats.len);
    try testing.expectEqual(@as(usize, 2), b.params.len);
    try testing.expect(b.params[0] != null);
    try testing.expectEqualStrings("42", b.params[0].?);
    try testing.expect(b.params[1] == null);
    try testing.expectEqual(@as(usize, 1), b.result_formats.len);
    try testing.expectEqual(@as(i16, 0), b.result_formats[0]);
}

test "decode Execute" {
    const a = std.testing.allocator;
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(a);
    try buf.append(a, 'E');
    const len_pos = buf.items.len;
    try buf.appendSlice(a, &[_]u8{ 0, 0, 0, 0 });
    try buf.appendSlice(a, "p1");
    try buf.append(a, 0);
    try buf.appendSlice(a, &[_]u8{ 0, 0, 0, 100 }); // max_rows=100
    const total: u32 = @intCast(buf.items.len - 1);
    std.mem.writeInt(u32, buf.items[len_pos..][0..4], total, .big);

    const r = try proto.decodeMessage(buf.items);
    const e = try proto.parseExecute(r.msg.body);
    try testing.expectEqualStrings("p1", e.portal_name);
    try testing.expectEqual(@as(u32, 100), e.max_rows);
}

test "decode Describe statement vs portal" {
    const stmt = [_]u8{ 'S', 0 };
    const d1 = try proto.parseDescribe(&stmt);
    try testing.expectEqual(proto.DescribeKind.statement, d1.kind);
    try testing.expectEqualStrings("", d1.name);

    const portal = [_]u8{ 'P', 'p', '1', 0 };
    const d2 = try proto.parseDescribe(&portal);
    try testing.expectEqual(proto.DescribeKind.portal, d2.kind);
    try testing.expectEqualStrings("p1", d2.name);

    const bad = [_]u8{ 'X', 0 };
    try testing.expectError(error.UnexpectedCode, proto.parseDescribe(&bad));
}

test "encode empty-bodied ext-query responses" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    try proto.encodeParseComplete(&aw.writer);
    try proto.encodeBindComplete(&aw.writer);
    try proto.encodeCloseComplete(&aw.writer);
    try proto.encodeNoData(&aw.writer);
    try proto.encodePortalSuspended(&aw.writer);

    // Each is exactly 5 bytes (1 tag + 4 length).
    const out = aw.writer.buffered();
    try testing.expectEqual(@as(usize, 25), out.len);

    const expected_tags = [_]u8{ '1', '2', '3', 'n', 's' };
    var off: usize = 0;
    for (expected_tags) |t| {
        const r = try proto.decodeMessage(out[off..]);
        try testing.expectEqual(t, r.msg.tag);
        try testing.expectEqual(@as(usize, 0), r.msg.body.len);
        off += r.consumed;
    }
}

test "encode ParameterDescription round-trip" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    var scratch: std.Io.Writer.Allocating = .init(a);
    defer scratch.deinit();
    const oids = [_]u32{ 23, 25, 16 };
    try proto.encodeParameterDescription(&scratch, &oids);
    try proto.writeFrame(&aw.writer, .parameter_description, scratch.writer.buffered());

    const r = try proto.decodeMessage(aw.writer.buffered());
    try testing.expectEqual(@as(u8, 't'), r.msg.tag);
    const cnt = std.mem.readInt(i16, r.msg.body[0..2], .big);
    try testing.expectEqual(@as(i16, 3), cnt);
    try testing.expectEqual(
        @as(u32, 23),
        std.mem.readInt(u32, r.msg.body[2..6], .big),
    );
    try testing.expectEqual(
        @as(u32, 25),
        std.mem.readInt(u32, r.msg.body[6..10], .big),
    );
    try testing.expectEqual(
        @as(u32, 16),
        std.mem.readInt(u32, r.msg.body[10..14], .big),
    );
}

// ---------- Extended-query state-machine tests ----------

/// Drive the state machine past startup and return it in `.ready`
/// with `out` cleared, ready to be exercised by ext-query fixtures.
/// Caller pre-builds the full input buffer (startup + the ext-query
/// frames they want to test) and constructs the Reader; this helper
/// just consumes the startup prefix and resets the output.
fn primedConnState(
    a: std.mem.Allocator,
    rdr: *std.Io.Reader,
    aw: *std.Io.Writer.Allocating,
) !conn_state.ConnState {
    var s = conn_state.ConnState.init(a, rdr, &aw.writer, .{});
    errdefer s.deinit();
    _ = try s.nextEvent(); // drive past startup
    resetOut(aw);
    return s;
}

/// Build a length-prefixed message in the standard tag-len-body
/// shape for tests. Tag goes first; len = 4 + body.len.
fn frame(
    a: std.mem.Allocator,
    out: *std.ArrayList(u8),
    tag: u8,
    body: String,
) !void {
    try out.append(a, tag);
    var lb: [4]u8 = undefined;
    std.mem.writeInt(u32, &lb, @intCast(4 + body.len), .big);
    try out.appendSlice(a, &lb);
    try out.appendSlice(a, body);
}

test "ext: Parse + Bind + Execute + Sync surfaces execute event" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    // Parse: name="" sql="SELECT $1" param_oids=[23]
    var parse_body: std.ArrayList(u8) = .empty;
    defer parse_body.deinit(a);
    try parse_body.append(a, 0);
    try parse_body.appendSlice(a, "SELECT $1");
    try parse_body.append(a, 0);
    try parse_body.appendSlice(a, &[_]u8{ 0, 1 });
    try parse_body.appendSlice(a, &[_]u8{ 0, 0, 0, 23 });

    // Bind: portal="" stmt="" 0 pfmt; 1 param "42"; 0 rfmt
    var bind_body: std.ArrayList(u8) = .empty;
    defer bind_body.deinit(a);
    try bind_body.append(a, 0); // portal
    try bind_body.append(a, 0); // stmt
    try bind_body.appendSlice(a, &[_]u8{ 0, 0 }); // n_pfmt=0
    try bind_body.appendSlice(a, &[_]u8{ 0, 1 }); // n_params=1
    try bind_body.appendSlice(a, &[_]u8{ 0, 0, 0, 2 }); // len=2
    try bind_body.appendSlice(a, "42");
    try bind_body.appendSlice(a, &[_]u8{ 0, 0 }); // n_rfmt=0

    // Execute: portal="" max_rows=0
    var exec_body: std.ArrayList(u8) = .empty;
    defer exec_body.deinit(a);
    try exec_body.append(a, 0);
    try exec_body.appendSlice(a, &[_]u8{ 0, 0, 0, 0 });

    // Pre-build the entire input (startup + ext-query frames) for the
    // Reader to consume. Reader.fixed wraps a slice; we can't append
    // after construction.
    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, &hexToBytes(startup_golden_hex));
    try frame(a, &input, 'P', parse_body.items);
    try frame(a, &input, 'B', bind_body.items);
    try frame(a, &input, 'E', exec_body.items);
    try frame(a, &input, 'S', ""); // Sync (empty body)

    var rdr = std.Io.Reader.fixed(input.items);
    var s = try primedConnState(a, &rdr, &aw);
    defer s.deinit();

    // The state machine consumes Parse + Bind silently and surfaces
    // the Execute event in one shot.
    const ev_exec = try s.nextEvent();
    try testing.expect(ev_exec == .execute);
    try testing.expectEqualStrings("SELECT $1", ev_exec.execute.sql);
    const exec_params = ev_exec.execute.portal.params;
    try testing.expectEqual(@as(usize, 1), exec_params.values.len);
    try testing.expect(exec_params.values[0] != null);
    try testing.expectEqualStrings("42", exec_params.values[0].?);

    // Now finish the command (handler would emit row + cmd_complete).
    try s.sendCommandComplete("SELECT 1");
    const ev_sync = try s.nextEvent();
    try testing.expect(ev_sync == .sync);

    // Verify the wire output ends with ReadyForQuery.
    const out = aw.writer.buffered();
    var off: usize = 0;
    var last_tag: u8 = 0;
    while (off < out.len) {
        const r = try proto.decodeMessage(out[off..]);
        last_tag = r.msg.tag;
        off += r.consumed;
    }
    try testing.expectEqual(@as(u8, 'Z'), last_tag);
}

test "ext: Bind with binary param format is accepted, surfaces format" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    // Parse: name="" sql="SELECT $1" 0 oids
    var parse_body: std.ArrayList(u8) = .empty;
    defer parse_body.deinit(a);
    try parse_body.append(a, 0);
    try parse_body.appendSlice(a, "SELECT $1");
    try parse_body.append(a, 0);
    try parse_body.appendSlice(a, &[_]u8{ 0, 0 });

    // Bind with binary param format (1) — int4(7) on the wire.
    var bind_body: std.ArrayList(u8) = .empty;
    defer bind_body.deinit(a);
    try bind_body.append(a, 0); // portal=""
    try bind_body.append(a, 0); // stmt=""
    try bind_body.appendSlice(a, &[_]u8{ 0, 1 }); // n_pfmt=1
    try bind_body.appendSlice(a, &[_]u8{ 0, 1 }); // pfmt[0]=1 (binary)
    try bind_body.appendSlice(a, &[_]u8{ 0, 1 }); // n_params=1
    try bind_body.appendSlice(a, &[_]u8{ 0, 0, 0, 4 });
    try bind_body.appendSlice(a, &[_]u8{ 0, 0, 0, 7 }); // int4(7) BE
    try bind_body.appendSlice(a, &[_]u8{ 0, 0 }); // n_rfmt=0

    // Execute "" 0
    var exec_body: std.ArrayList(u8) = .empty;
    defer exec_body.deinit(a);
    try exec_body.append(a, 0);
    try exec_body.appendSlice(a, &[_]u8{ 0, 0, 0, 0 });

    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, &hexToBytes(startup_golden_hex));
    try frame(a, &input, 'P', parse_body.items);
    try frame(a, &input, 'B', bind_body.items);
    try frame(a, &input, 'E', exec_body.items);
    try frame(a, &input, 'S', "");

    var rdr = std.Io.Reader.fixed(input.items);
    var s = try primedConnState(a, &rdr, &aw);
    defer s.deinit();

    // Parse + Bind silent; Execute surfaces with format=binary.
    const ev = try s.nextEvent();
    try testing.expect(ev == .execute);
    try testing.expectEqualStrings("SELECT $1", ev.execute.sql);
    const bound = ev.execute.portal.params;
    try testing.expectEqual(@as(usize, 1), bound.values.len);
    try testing.expectEqual(@as(usize, 1), bound.wirefmts.len);
    try testing.expectEqual(
        pgwire.WireFormat.binary,
        bound.wirefmts[0],
    );
    try testing.expect(bound.values[0] != null);
    // The bytes are the raw binary payload — handler is responsible
    // for decoding via pgwire.binary.decodeInt4.
    try testing.expectEqualSlices(
        u8,
        &.{ 0, 0, 0, 7 },
        bound.values[0].?,
    );

    // Wrap up so out has a clean tail; not asserted further but
    // keeps the connection in a valid state for the deinit path.
    try s.sendCommandComplete("SELECT 1");
    _ = try s.nextEvent(); // .sync

    try testing.expect(!s.ext_failed);
}

test "ext: Bind with format=2 (invalid) errors and skips until Sync" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    var parse_body: std.ArrayList(u8) = .empty;
    defer parse_body.deinit(a);
    try parse_body.append(a, 0);
    try parse_body.appendSlice(a, "SELECT $1");
    try parse_body.append(a, 0);
    try parse_body.appendSlice(a, &[_]u8{ 0, 0 });

    var bind_body: std.ArrayList(u8) = .empty;
    defer bind_body.deinit(a);
    try bind_body.append(a, 0);
    try bind_body.append(a, 0);
    try bind_body.appendSlice(a, &[_]u8{ 0, 1 });
    try bind_body.appendSlice(a, &[_]u8{ 0, 2 }); // format=2 (illegal)
    try bind_body.appendSlice(a, &[_]u8{ 0, 1 });
    try bind_body.appendSlice(a, &[_]u8{ 0, 0, 0, 4 });
    try bind_body.appendSlice(a, &[_]u8{ 0, 0, 0, 7 });
    try bind_body.appendSlice(a, &[_]u8{ 0, 0 });

    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, &hexToBytes(startup_golden_hex));
    try frame(a, &input, 'P', parse_body.items);
    try frame(a, &input, 'B', bind_body.items);
    try frame(a, &input, 'E', "");
    try frame(a, &input, 'S', "");

    var rdr = std.Io.Reader.fixed(input.items);
    var s = try primedConnState(a, &rdr, &aw);
    defer s.deinit();

    const ev = try s.nextEvent();
    try testing.expect(ev == .sync);

    // Output must have ErrorResponse + ReadyForQuery; no BindComplete.
    const out = aw.writer.buffered();
    var off: usize = 0;
    var saw_error = false;
    var saw_bind_complete = false;
    while (off < out.len) {
        const r = try proto.decodeMessage(out[off..]);
        if (r.msg.tag == 'E') saw_error = true;
        if (r.msg.tag == '2') saw_bind_complete = true;
        off += r.consumed;
    }
    try testing.expect(saw_error);
    try testing.expect(!saw_bind_complete);
    try testing.expect(!s.ext_failed);
    try testing.expect(std.mem.indexOf(u8, out, "C22023\x00") != null);
}

test "startup: unsupported protocol version answers 0A000" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    // Startup frame: length 9, version 3.1 (not 3.0), param terminator.
    const input = [_]u8{ 0, 0, 0, 9, 0, 3, 0, 1, 0 };
    var rdr = std.Io.Reader.fixed(&input);
    var s = conn_state.ConnState.init(a, &rdr, &aw.writer, .{});
    defer s.deinit();

    try testing.expect(try s.nextEvent() == .terminate);
    try testing.expect(
        std.mem.indexOf(u8, aw.writer.buffered(), "C0A000\x00") != null,
    );
}

/// Run `frames` (already tagged+length-prefixed) through a fresh
/// connection and return the SQLSTATE of its first ErrorResponse.
fn firstErrorCode(a: std.mem.Allocator, frames: String) !String {
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();
    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, &hexToBytes(startup_golden_hex));
    try input.appendSlice(a, frames);

    var rdr = std.Io.Reader.fixed(input.items);
    var s = try primedConnState(a, &rdr, &aw);
    defer s.deinit();
    _ = try s.nextEvent();

    const out = aw.writer.buffered();
    var off: usize = 0;
    while (off < out.len) {
        const r = try proto.decodeMessage(out[off..]);
        if (r.msg.tag == 'E') {
            const at = std.mem.indexOf(u8, r.msg.body, "\x00C") orelse
                return error.NoCodeField;
            return r.msg.body[at + 2 .. at + 7];
        }
        off += r.consumed;
    }
    return error.NoErrorResponse;
}

test "ext: unknown portal/statement and malformed frame carry their SQLSTATE" {
    const a = std.testing.allocator;
    const cases = [_]struct { tag: u8, body: String, want: String }{
        // Execute: portal="" never bound.
        .{ .tag = 'E', .body = "\x00\x00\x00\x00\x00", .want = "34000" },
        // Describe: kind='S' name="nope".
        .{ .tag = 'D', .body = "Snope\x00", .want = "26000" },
        // Describe: kind byte that is neither 'S' nor 'P'.
        .{ .tag = 'D', .body = "X\x00", .want = "08P01" },
    };
    for (cases) |c| {
        var frames: std.ArrayList(u8) = .empty;
        defer frames.deinit(a);
        try frame(a, &frames, c.tag, c.body);
        try testing.expectEqualStrings(
            c.want,
            try firstErrorCode(a, frames.items),
        );
    }
}

test "ext: Bind with negative param length other than -1 is malformed" {
    const a = std.testing.allocator;

    // Bind: portal="", stmt="", 0 pfmt, 1 param with length -2, 0 rfmt.
    var bind_body: std.ArrayList(u8) = .empty;
    defer bind_body.deinit(a);
    try bind_body.append(a, 0);
    try bind_body.append(a, 0);
    try bind_body.appendSlice(a, &[_]u8{ 0, 0 });
    try bind_body.appendSlice(a, &[_]u8{ 0, 1 });
    try bind_body.appendSlice(a, &[_]u8{ 0xFF, 0xFF, 0xFF, 0xFE });
    try bind_body.appendSlice(a, &[_]u8{ 0, 0 });

    var frames: std.ArrayList(u8) = .empty;
    defer frames.deinit(a);
    try frame(a, &frames, 'B', bind_body.items);
    try testing.expectEqualStrings(
        "08P01",
        try firstErrorCode(a, frames.items),
    );
}

test "ext: Describe(statement) emits ParameterDescription" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    // Parse: name="ps1", sql="SELECT $1::int", oids=[23]
    var parse_body: std.ArrayList(u8) = .empty;
    defer parse_body.deinit(a);
    try parse_body.appendSlice(a, "ps1");
    try parse_body.append(a, 0);
    try parse_body.appendSlice(a, "SELECT $1::int");
    try parse_body.append(a, 0);
    try parse_body.appendSlice(a, &[_]u8{ 0, 1 });
    try parse_body.appendSlice(a, &[_]u8{ 0, 0, 0, 23 });

    // Describe: kind='S' name="ps1"
    var desc_body: std.ArrayList(u8) = .empty;
    defer desc_body.deinit(a);
    try desc_body.append(a, 'S');
    try desc_body.appendSlice(a, "ps1");
    try desc_body.append(a, 0);

    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, &hexToBytes(startup_golden_hex));
    try frame(a, &input, 'P', parse_body.items);
    try frame(a, &input, 'D', desc_body.items);

    var rdr = std.Io.Reader.fixed(input.items);
    var s = try primedConnState(a, &rdr, &aw);
    defer s.deinit();

    // Parse is silent; the very next event is the surfaced Describe.
    const ev = try s.nextEvent();
    try testing.expect(ev == .describe);
    try testing.expectEqual(conn_state.DescribeKind.statement, ev.describe.kind);
    try testing.expectEqualStrings("SELECT $1::int", ev.describe.sql);

    // Output must contain a ParameterDescription with one oid=23.
    var off: usize = 0;
    var saw_param_desc = false;
    while (off < aw.writer.buffered().len) {
        const r = try proto.decodeMessage(aw.writer.buffered()[off..]);
        if (r.msg.tag == 't') {
            saw_param_desc = true;
            const cnt = std.mem.readInt(i16, r.msg.body[0..2], .big);
            try testing.expectEqual(@as(i16, 1), cnt);
            const oid = std.mem.readInt(u32, r.msg.body[2..6], .big);
            try testing.expectEqual(@as(u32, 23), oid);
        }
        off += r.consumed;
    }
    try testing.expect(saw_param_desc);
}

test "ext: Close(statement) drops dependent portals" {
    const a = std.testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(a);
    defer aw.deinit();

    // Parse "ps1" + Bind portal "p1" against it
    var parse_body: std.ArrayList(u8) = .empty;
    defer parse_body.deinit(a);
    try parse_body.appendSlice(a, "ps1");
    try parse_body.append(a, 0);
    try parse_body.appendSlice(a, "SELECT 1");
    try parse_body.append(a, 0);
    try parse_body.appendSlice(a, &[_]u8{ 0, 0 });

    var bind_body: std.ArrayList(u8) = .empty;
    defer bind_body.deinit(a);
    try bind_body.appendSlice(a, "p1");
    try bind_body.append(a, 0);
    try bind_body.appendSlice(a, "ps1");
    try bind_body.append(a, 0);
    try bind_body.appendSlice(a, &[_]u8{ 0, 0 });
    try bind_body.appendSlice(a, &[_]u8{ 0, 0 });
    try bind_body.appendSlice(a, &[_]u8{ 0, 0 });

    // Close: kind='S' name="ps1"
    var close_body: std.ArrayList(u8) = .empty;
    defer close_body.deinit(a);
    try close_body.append(a, 'S');
    try close_body.appendSlice(a, "ps1");
    try close_body.append(a, 0);

    var input: std.ArrayList(u8) = .empty;
    defer input.deinit(a);
    try input.appendSlice(a, &hexToBytes(startup_golden_hex));
    try frame(a, &input, 'P', parse_body.items);
    try frame(a, &input, 'B', bind_body.items);
    try frame(a, &input, 'C', close_body.items);

    var rdr = std.Io.Reader.fixed(input.items);
    var s = try primedConnState(a, &rdr, &aw);
    defer s.deinit();

    // All three messages consumed silently; reader is exhausted, so
    // the next event is .terminate. Verify the state side-effects:
    // the Close should have dropped both the statement and its
    // dependent portal.
    const ev = try s.nextEvent();
    try testing.expect(ev == .terminate);

    try testing.expect(!s.statements.contains("ps1"));
    try testing.expect(!s.portals.contains("p1"));
}

test "pgwire root: re-exports surface" {
    // Smoke-test the public root: each named export should be
    // accessible without further qualification.
    _ = pgwire.Session;
    _ = pgwire.ConnState;
    _ = pgwire.parse;
    _ = pgwire.FieldDescription;
}
