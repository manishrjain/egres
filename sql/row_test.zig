// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const Type = sql.Type;
const Value = sql.Value;
const LogicalColumn = sql.LogicalColumn;

fn col(id: u32, ty: Type) LogicalColumn {
    return .{ .column_id = id, .name = "c", .ty = ty };
}

fn expectNull(v: Value) !void {
    try testing.expect(std.meta.activeTag(v) == .null_value);
}

test "sql.row: end-to-end — logical schema, compile, marshal, read all back" {
    const alloc = testing.allocator;

    // A realistic mixed table: fixed + varlen columns, a NULL, and enough
    // columns (13) to spill the null bitmap into a second byte.
    const columns = [_]LogicalColumn{
        col(1, .int8), // id
        col(2, .bool_), // active
        col(3, .int2), // age
        col(4, .int4), // balance
        col(5, .float8), // score
        col(6, .float4), // ratio
        col(7, .timestamptz), // created
        col(8, .date), // day
        col(9, .interval), // dur
        col(10, .uuid), // uid
        col(11, .text), // name
        col(12, .text), // bio (NULL in this row)
        col(13, .bytea), // avatar
    };
    const logical = sql.LogicalSchema{ .columns = &columns, .current_version = 1 };

    // Logical → physical.
    var ps = try sql.compile(alloc, logical, logical.current_version);
    defer ps.deinit(alloc);
    try testing.expectEqual(@as(usize, 13), ps.columns.len);
    try testing.expectEqual(@as(u16, 3), ps.n_varlen); // name, bio, avatar

    // Write a row. Values are in ps.columns (ascending column_id) order.
    const uid = [16]u8{ 0xaa, 0xbb, 0xcc, 0xdd, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11 };
    const avatar = [_]u8{ 0x89, 'P', 'N', 'G' };
    const values = [_]Value{
        .{ .int8 = 42 },
        .{ .bool_ = true },
        .{ .int2 = 30 },
        .{ .int4 = -100 },
        .{ .float8 = 99.5 },
        .{ .float4 = 0.5 },
        .{ .timestamptz = 1_700_000_000_000_000 },
        .{ .date = 9000 },
        .{ .interval = .{ .months = 1, .days = 2, .micros = 3 } },
        .{ .uuid = uid },
        .{ .text = "alice" },
        .null_value, // bio
        .{ .bytea = &avatar },
    };
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    const bytes = try sql.marshalRow(alloc, &ps, &values, &buf);

    // Read everything back: an empty projection selects all columns, in
    // column_id order, into a reusable Row.
    var r: sql.Row = .{};
    defer r.deinit(alloc);
    try sql.unmarshalRow(alloc, &ps, bytes, &[_]u32{}, &r);
    try testing.expectEqual(@as(usize, 13), r.len());

    try testing.expectEqual(@as(i64, 42), r.at(0).int8);
    try testing.expectEqual(true, r.at(1).bool_);
    try testing.expectEqual(@as(i16, 30), r.at(2).int2);
    try testing.expectEqual(@as(i32, -100), r.at(3).int4);
    try testing.expectEqual(@as(f64, 99.5), r.at(4).float8);
    try testing.expectEqual(@as(f32, 0.5), r.at(5).float4);
    try testing.expectEqual(@as(i64, 1_700_000_000_000_000), r.at(6).timestamptz);
    try testing.expectEqual(@as(i32, 9000), r.at(7).date);
    const iv = r.at(8).interval;
    try testing.expectEqual(@as(i32, 1), iv.months);
    try testing.expectEqual(@as(i32, 2), iv.days);
    try testing.expectEqual(@as(i64, 3), iv.micros);
    const got_uid = r.at(9).uuid;
    try testing.expectEqualSlices(u8, &uid, &got_uid);
    try testing.expectEqualStrings("alice", r.at(10).text);
    try expectNull(r.at(11)); // bio
    try testing.expectEqualSlices(u8, &avatar, r.at(12).bytea);
}

test "sql.row: vector payload round-trips without changing float32 bits" {
    const alloc = testing.allocator;
    const columns = [_]LogicalColumn{col(1, .vector)};
    const logical = sql.LogicalSchema{ .columns = &columns, .current_version = 1 };
    var ps = try sql.compile(alloc, logical, 1);
    defer ps.deinit(alloc);

    const vector = try sql.value.parseVector(alloc, "[0.1,-2,3.25]", 3);
    defer alloc.free(vector.vector);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    const bytes = try sql.marshalRow(alloc, &ps, &.{vector}, &buf);

    var row: sql.Row = .{};
    defer row.deinit(alloc);
    try sql.unmarshalRow(alloc, &ps, bytes, &.{}, &row);
    try testing.expectEqualSlices(u8, vector.vector, row.at(0).vector);
}

test "sql.row: empty projection selects every column in column_id order" {
    const alloc = testing.allocator;
    const cols = [_]LogicalColumn{ col(1, .int4), col(2, .text), col(3, .bool_) };
    var ps = try sql.compile(alloc, .{ .columns = &cols, .current_version = 1 }, 1);
    defer ps.deinit(alloc);
    const vals = [_]Value{ .{ .int4 = 7 }, .{ .text = "hi" }, .{ .bool_ = true } };
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    const bytes = try sql.marshalRow(alloc, &ps, &vals, &buf);

    var r: sql.Row = .{};
    defer r.deinit(alloc);
    try sql.unmarshalRow(alloc, &ps, bytes, &[_]u32{}, &r);
    try testing.expectEqual(@as(usize, 3), r.len());
    try testing.expectEqual(@as(i32, 7), r.at(0).int4);
    try testing.expectEqualStrings("hi", r.at(1).text);
    try testing.expectEqual(true, r.at(2).bool_);
}

test "sql.row: round-trip every type" {
    const alloc = testing.allocator;
    const cols = [_]LogicalColumn{
        col(1, .bool_),  col(2, .int2),   col(3, .int4), col(4, .int8),
        col(5, .float4), col(6, .float8), col(7, .date), col(8, .timestamp),
        col(9, .text),   col(10, .bytea),
    };
    var ps = try sql.compile(alloc, .{ .columns = &cols, .current_version = 1 }, 1);
    defer ps.deinit(alloc);

    const vals = [_]Value{
        .{ .bool_ = true },   .{ .int2 = -7 },                                .{ .int4 = 123456 }, .{ .int8 = -9_000_000_000 },
        .{ .float4 = 3.5 },   .{ .float8 = 2.5 },                             .{ .date = 9000 },   .{ .timestamp = 1_700_000_000_000_000 },
        .{ .text = "hello" }, .{ .bytea = &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF } },
    };

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    const bytes = try sql.marshalRow(alloc, &ps, &vals, &buf);

    const proj = [_]u32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    var r: sql.Row = .{};
    defer r.deinit(alloc);
    try sql.unmarshalRow(alloc, &ps, bytes, &proj, &r);

    try testing.expectEqual(true, r.at(0).bool_);
    try testing.expectEqual(@as(i16, -7), r.at(1).int2);
    try testing.expectEqual(@as(i32, 123456), r.at(2).int4);
    try testing.expectEqual(@as(i64, -9_000_000_000), r.at(3).int8);
    try testing.expectEqual(@as(f32, 3.5), r.at(4).float4);
    try testing.expectEqual(@as(f64, 2.5), r.at(5).float8);
    try testing.expectEqual(@as(i32, 9000), r.at(6).date);
    try testing.expectEqual(@as(i64, 1_700_000_000_000_000), r.at(7).timestamp);
    try testing.expectEqualStrings("hello", r.at(8).text);
    try testing.expectEqualSlices(u8, &[_]u8{ 0xDE, 0xAD, 0xBE, 0xEF }, r.at(9).bytea);
}

test "sql.row: round-trip unsigned ints at full-range maxima" {
    const alloc = testing.allocator;
    const cols = [_]LogicalColumn{ col(1, .uint2), col(2, .uint4), col(3, .uint8) };
    var ps = try sql.compile(alloc, .{ .columns = &cols, .current_version = 1 }, 1);
    defer ps.deinit(alloc);

    // Maxima exceed the signed counterparts (i16/i32/i64), so a round-trip
    // proves true unsigned storage — no sign-bit truncation or wrap.
    const vals = [_]Value{
        .{ .uint2 = std.math.maxInt(u16) },
        .{ .uint4 = std.math.maxInt(u32) },
        .{ .uint8 = std.math.maxInt(u64) },
    };
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    const bytes = try sql.marshalRow(alloc, &ps, &vals, &buf);

    var r: sql.Row = .{};
    defer r.deinit(alloc);
    try sql.unmarshalRow(alloc, &ps, bytes, &[_]u32{}, &r);
    try testing.expectEqual(@as(u16, std.math.maxInt(u16)), r.at(0).uint2);
    try testing.expectEqual(@as(u32, std.math.maxInt(u32)), r.at(1).uint4);
    try testing.expectEqual(@as(u64, std.math.maxInt(u64)), r.at(2).uint8);
}

test "sql.row: round-trip time/timestamptz/interval/uuid" {
    const alloc = testing.allocator;
    const cols = [_]LogicalColumn{
        col(1, .time), col(2, .timestamptz), col(3, .interval), col(4, .uuid),
    };
    var ps = try sql.compile(alloc, .{ .columns = &cols, .current_version = 1 }, 1);
    defer ps.deinit(alloc);

    const my_uuid = [16]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    const vals = [_]Value{
        .{ .time = 3_600_000_000 }, // 01:00:00 as µs since midnight
        .{ .timestamptz = 1_700_000_000_000_000 },
        .{ .interval = .{ .months = 14, .days = 3, .micros = 500 } },
        .{ .uuid = my_uuid },
    };
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    const bytes = try sql.marshalRow(alloc, &ps, &vals, &buf);

    const proj = [_]u32{ 1, 2, 3, 4 };
    var r: sql.Row = .{};
    defer r.deinit(alloc);
    try sql.unmarshalRow(alloc, &ps, bytes, &proj, &r);
    try testing.expectEqual(@as(i64, 3_600_000_000), r.at(0).time);
    try testing.expectEqual(@as(i64, 1_700_000_000_000_000), r.at(1).timestamptz);
    const iv = r.at(2).interval;
    try testing.expectEqual(@as(i32, 14), iv.months);
    try testing.expectEqual(@as(i32, 3), iv.days);
    try testing.expectEqual(@as(i64, 500), iv.micros);
    const got_uuid = r.at(3).uuid;
    try testing.expectEqualSlices(u8, &my_uuid, &got_uuid);
}

test "sql.row: projection fetches a sorted subset; absent id reads null" {
    const alloc = testing.allocator;
    const cols = [_]LogicalColumn{
        col(1, .int4), col(2, .int4), col(3, .int4), col(4, .int4), col(5, .int4),
    };
    var ps = try sql.compile(alloc, .{ .columns = &cols, .current_version = 1 }, 1);
    defer ps.deinit(alloc);
    const vals = [_]Value{ .{ .int4 = 10 }, .{ .int4 = 20 }, .{ .int4 = 30 }, .{ .int4 = 40 }, .{ .int4 = 50 } };
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    const bytes = try sql.marshalRow(alloc, &ps, &vals, &buf);

    // Sorted subset (col2, col4) plus an absent id (99) — exercises the
    // merge-walk skipping a gap and running off the end → null, without
    // resetting the ps.columns cursor.
    const proj = [_]u32{ 2, 4, 99 };
    var r: sql.Row = .{};
    defer r.deinit(alloc);
    try sql.unmarshalRow(alloc, &ps, bytes, &proj, &r);
    try testing.expectEqual(@as(usize, 3), r.len());
    try testing.expectEqual(@as(i32, 20), r.at(0).int4);
    try testing.expectEqual(@as(i32, 40), r.at(1).int4);
    try expectNull(r.at(2)); // 99 absent
}

test "sql.row: null fixed, null varlen, and empty-not-null varlen are distinct" {
    const alloc = testing.allocator;
    const cols = [_]LogicalColumn{
        col(1, .int4), // null
        col(2, .int4), // present
        col(3, .text), // null
        col(4, .text), // empty string, not null
    };
    var ps = try sql.compile(alloc, .{ .columns = &cols, .current_version = 1 }, 1);
    defer ps.deinit(alloc);
    // Values parallel ps.columns (ascending column_id == definition order).
    const vals = [_]Value{ .null_value, .{ .int4 = 99 }, .null_value, .{ .text = "" } };
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    const bytes = try sql.marshalRow(alloc, &ps, &vals, &buf);

    var r: sql.Row = .{};
    defer r.deinit(alloc);
    try sql.unmarshalRow(alloc, &ps, bytes, &[_]u32{}, &r);

    try expectNull(r.at(0)); // null fixed
    try testing.expectEqual(@as(i32, 99), r.at(1).int4);
    try expectNull(r.at(2)); // null varlen
    try testing.expectEqualStrings("", r.at(3).text); // empty, NOT null
}

test "sql.row: varlen directory holds cumulative ends; empty middle slices empty" {
    const alloc = testing.allocator;
    const cols = [_]LogicalColumn{ col(1, .text), col(2, .text), col(3, .text) };
    var ps = try sql.compile(alloc, .{ .columns = &cols, .current_version = 1 }, 1);
    defer ps.deinit(alloc);
    const vals = [_]Value{ .{ .text = "abc" }, .{ .text = "" }, .{ .text = "defgh" } };
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    const bytes = try sql.marshalRow(alloc, &ps, &vals, &buf);

    var r: sql.Row = .{};
    defer r.deinit(alloc);
    try sql.unmarshalRow(alloc, &ps, bytes, &[_]u32{}, &r);
    try testing.expectEqualStrings("abc", r.at(0).text);
    try testing.expectEqualStrings("", r.at(1).text);
    try testing.expectEqualStrings("defgh", r.at(2).text);

    // Directory entries are cumulative ends {3,3,8}.
    const var_base = ps.var_offset_array_start;
    try testing.expectEqual(@as(u32, 3), std.mem.readInt(u32, bytes[var_base..][0..4], .little));
    try testing.expectEqual(@as(u32, 3), std.mem.readInt(u32, bytes[var_base + 4 ..][0..4], .little));
    try testing.expectEqual(@as(u32, 8), std.mem.readInt(u32, bytes[var_base + 8 ..][0..4], .little));
}

test "sql.row: a column added after the row's version reads as null" {
    const alloc = testing.allocator;
    const a = col(1, .int4);
    const b = col(2, .text);
    const c = LogicalColumn{ .column_id = 3, .name = "c", .ty = .int4, .added_in_version = 2 };
    const cols = [_]LogicalColumn{ a, b, c };
    const catalog = sql.LogicalSchema{ .columns = &cols, .current_version = 2 };

    // Encode a v1 row: only A and B exist at v1.
    var v1 = try sql.compile(alloc, catalog, 1);
    defer v1.deinit(alloc);
    try testing.expectEqual(@as(usize, 2), v1.columns.len);
    const v1_vals = [_]Value{ .{ .int4 = 7 }, .{ .text = "old" } };
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    const bytes = try sql.marshalRow(alloc, &v1, &v1_vals, &buf);

    // A query projecting A, B, C against the v1 row: C is absent → null.
    const proj = [_]u32{ 1, 2, 3 };
    var r: sql.Row = .{};
    defer r.deinit(alloc);
    try sql.unmarshalRow(alloc, &v1, bytes, &proj, &r);
    try testing.expectEqual(@as(i32, 7), r.at(0).int4);
    try testing.expectEqualStrings("old", r.at(1).text);
    try expectNull(r.at(2)); // C (added v2) absent in a v1 row
}

test "sql.row: empty schema encodes just the version stamp" {
    const alloc = testing.allocator;
    const cols = [_]LogicalColumn{};
    var ps = try sql.compile(alloc, .{ .columns = &cols, .current_version = 5 }, 5);
    defer ps.deinit(alloc);
    const vals = [_]Value{};
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    const bytes = try sql.marshalRow(alloc, &ps, &vals, &buf);
    try testing.expectEqual(@as(usize, 2), bytes.len); // version u16 only
    try testing.expectEqual(@as(u16, 5), std.mem.readInt(u16, bytes[0..2], .little));

    const proj = [_]u32{ 1, 2 };
    var r: sql.Row = .{};
    defer r.deinit(alloc);
    try sql.unmarshalRow(alloc, &ps, bytes, &proj, &r);
    try expectNull(r.at(0));
    try expectNull(r.at(1));
}

test "sql.row: row and buffer are reusable across rows" {
    const alloc = testing.allocator;
    const cols = [_]LogicalColumn{ col(1, .int4), col(2, .text) };
    var ps = try sql.compile(alloc, .{ .columns = &cols, .current_version = 1 }, 1);
    defer ps.deinit(alloc);

    // One buffer + one Row, reused for both rows.
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    var r: sql.Row = .{};
    defer r.deinit(alloc);

    {
        const vals = [_]Value{ .{ .int4 = 111 }, .{ .text = "aaa" } };
        const bytes = try sql.marshalRow(alloc, &ps, &vals, &buf);
        try sql.unmarshalRow(alloc, &ps, bytes, &[_]u32{}, &r);
        try testing.expectEqual(@as(i32, 111), r.at(0).int4);
        try testing.expectEqualStrings("aaa", r.at(1).text);
    }
    {
        // Reuse buf + r with a longer varlen — both must fully overwrite, no
        // stale bytes or stale cells leaking through.
        const vals = [_]Value{ .{ .int4 = 222 }, .{ .text = "bbbbbb" } };
        const bytes = try sql.marshalRow(alloc, &ps, &vals, &buf);
        try sql.unmarshalRow(alloc, &ps, bytes, &[_]u32{}, &r);
        try testing.expectEqual(@as(i32, 222), r.at(0).int4);
        try testing.expectEqualStrings("bbbbbb", r.at(1).text);
    }
}
