// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const String = sql.value.String;
const pgwire = @import("pgwire");
const Type = sql.Type;
const value = sql.value;
const date = @import("utils").date;

test "sql.value: fixedWidth and isVarlen per type" {
    try testing.expectEqual(@as(?u8, 1), Type.bool_.fixedWidth());
    try testing.expectEqual(@as(?u8, 2), Type.int2.fixedWidth());
    try testing.expectEqual(@as(?u8, 4), Type.int4.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.int8.fixedWidth());
    try testing.expectEqual(@as(?u8, 2), Type.uint2.fixedWidth());
    try testing.expectEqual(@as(?u8, 4), Type.uint4.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.uint8.fixedWidth());
    try testing.expectEqual(@as(?u8, 4), Type.float4.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.float8.fixedWidth());
    try testing.expectEqual(@as(?u8, 4), Type.date.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.time.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.timestamp.fixedWidth());
    try testing.expectEqual(@as(?u8, 8), Type.timestamptz.fixedWidth());
    try testing.expectEqual(@as(?u8, 16), Type.interval.fixedWidth());
    try testing.expectEqual(@as(?u8, 16), Type.uuid.fixedWidth());
    try testing.expectEqual(@as(?u8, null), Type.text.fixedWidth());
    try testing.expectEqual(@as(?u8, null), Type.bpchar.fixedWidth());
    try testing.expectEqual(@as(?u8, null), Type.varchar.fixedWidth());
    try testing.expectEqual(@as(?u8, null), Type.bytea.fixedWidth());
    try testing.expectEqual(@as(?u8, null), Type.vector.fixedWidth());

    try testing.expect(!Type.int4.isVarlen());
    try testing.expect(!Type.timestamptz.isVarlen());
    try testing.expect(!Type.interval.isVarlen());
    try testing.expect(!Type.uuid.isVarlen());
    try testing.expect(Type.text.isVarlen());
    try testing.expect(Type.bytea.isVarlen());
    try testing.expect(Type.vector.isVarlen());
}

test "sql.value: Type.fromName resolves DDL type names" {
    try testing.expectEqual(@as(?Type, .int2), Type.fromName("int2"));
    try testing.expectEqual(@as(?Type, .int4), Type.fromName("int4"));
    try testing.expectEqual(@as(?Type, .int8), Type.fromName("int8"));
    try testing.expectEqual(@as(?Type, .uint2), Type.fromName("uint2"));
    try testing.expectEqual(@as(?Type, .uint4), Type.fromName("uint4"));
    try testing.expectEqual(@as(?Type, .uint8), Type.fromName("uint8"));
    try testing.expectEqual(@as(?Type, .float4), Type.fromName("float4"));
    try testing.expectEqual(@as(?Type, .float8), Type.fromName("float8"));
    try testing.expectEqual(@as(?Type, .bool_), Type.fromName("bool"));
    // varchar folds into text (length modifier dropped).
    try testing.expectEqual(@as(?Type, .text), Type.fromName("text"));
    try testing.expectEqual(@as(?Type, .text), Type.fromName("varchar"));
    try testing.expectEqual(@as(?Type, .bytea), Type.fromName("bytea"));
    try testing.expectEqual(@as(?Type, .vector), Type.fromName("vector"));
    try testing.expectEqual(@as(?Type, .date), Type.fromName("date"));
    try testing.expectEqual(@as(?Type, .timestamptz), Type.fromName("timestamptz"));
    try testing.expectEqual(@as(?Type, .uuid), Type.fromName("uuid"));
    // Unsupported names miss — the DDL layer turns these into errors.
    try testing.expect(Type.fromName("serial") == null);
    try testing.expect(Type.fromName("numeric") == null);
    try testing.expect(Type.fromName("") == null);
}

test "sql.value: vectors parse, validate, and format as pgvector text" {
    const alloc = testing.allocator;
    const vector = try value.parseVector(alloc, "[1, -2.5, 3e1]", 3);
    defer alloc.free(vector.vector);
    try testing.expectEqual(@as(usize, 12), vector.vector.len);

    var buf: [64]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    try value.formatValue(&w, null, vector);
    try testing.expectEqualStrings("[1,-2.5,30]", w.buffered());

    try testing.expectError(
        error.VectorDimensionMismatch,
        value.parseVector(alloc, "[1,2]", 3),
    );
    try testing.expectError(
        error.NonFiniteVector,
        value.parseVector(alloc, "[NaN,2,3]", 3),
    );
    try testing.expectError(
        error.InvalidVector,
        value.parseVector(alloc, "1,2,3", 3),
    );
}

test "sql.value: parseValue coerces literals and rejects unsupported types" {
    try testing.expectEqual(@as(i32, -7), (try value.parseValue(.int4, "-7")).int4);
    try testing.expectEqual(@as(u64, 42), (try value.parseValue(.uint8, "42")).uint8);
    try testing.expectEqual(true, (try value.parseValue(.bool_, "t")).bool_);
    try testing.expectEqual(false, (try value.parseValue(.bool_, "FALSE")).bool_);
    try testing.expectEqual(@as(f64, 1.5), (try value.parseValue(.float8, "1.5")).float8);
    try testing.expectEqualStrings("hi", (try value.parseValue(.text, "hi")).text);
    // Bad numeric / bool → error (executor maps to 22P02).
    try testing.expectError(error.InvalidCharacter, value.parseValue(.int4, "nope"));
    try testing.expectError(error.InvalidBool, value.parseValue(.bool_, "maybe"));
    // Types this slice can't parse.
    try testing.expectError(error.UnsupportedType, value.parseValue(.time, "10:00"));
    try testing.expectError(error.UnsupportedType, value.parseValue(.uuid, "x"));
}

test "sql.value: timestamps parse from and format to Postgres text" {
    // Storage is Unix µs (UTC); the expected numbers come from Python's
    // datetime against the Unix epoch. 2000-01-01 is Postgres's own
    // epoch — only its binary wire format counts from there.
    const sep12: i64 = 1_789_207_200_500_000; // 2026-09-12 10:00:00.5
    const leap_day: i64 = 1_709_164_800_000_000; // 2024-02-29
    const year_one: i64 = -62_135_596_800_000_000; // 0001-01-01
    const cases = [_]struct { text: String, us: i64 }{
        .{ .text = "1970-01-01 00:00:00", .us = 0 },
        .{ .text = "1970-01-01", .us = 0 },
        .{ .text = "2000-01-01 00:00:00", .us = 946_684_800_000_000 },
        .{ .text = "1969-12-31 23:59:59.999999", .us = -1 },
        .{ .text = "2024-02-29", .us = leap_day },
        .{ .text = "2026-09-12 10:00:00.5", .us = sep12 },
        .{ .text = "2026-09-12T10:00:00.500000", .us = sep12 },
        .{ .text = "2026-09-12 10:00:00.5000009", .us = sep12 },
        .{ .text = " 2026-09-12 10:00:00.5 ", .us = sep12 },
        .{ .text = "0001-01-01", .us = year_one },
        .{ .text = "9999-12-31 23:59:59", .us = 253_402_300_799_000_000 },
    };
    for (cases) |c| {
        try testing.expectEqual(c.us, try date.parseTimestamp(c.text, false));
        const cell = try value.parseValue(.timestamp, c.text);
        try testing.expectEqual(c.us, cell.timestamp);
    }
    // A zone offset shifts a timestamptz to UTC; a plain timestamp
    // ignores it. `Z` is UTC either way.
    const noon_plus2 = "2026-09-12 12:00:00.5+02";
    const tz_cases = [_]String{
        noon_plus2,
        "2026-09-12 08:00:00.5-02:00",
        "2026-09-12 07:30:00.5 -0230",
        "2026-09-12 10:00:00.5Z",
    };
    for (tz_cases) |text| {
        try testing.expectEqual(sep12, try date.parseTimestamp(text, true));
    }
    const two_hours: i64 = 2 * 3_600_000_000;
    try testing.expectEqual(
        sep12 + two_hours,
        try date.parseTimestamp(noon_plus2, false),
    );
    const tz_cell = try value.parseValue(.timestamptz, noon_plus2);
    try testing.expectEqual(sep12, tz_cell.timestamptz);

    const bad = [_]String{
        "",
        "nope",
        "2026-13-01",
        "2026-02-30",
        "2023-02-29",
        "2026-09-12 24:00:00",
        "2026-09-12 10:60",
        "2026-9-12",
        "26-09-12",
        "2026-09-12 10:00:00.",
        "2026-09-12 10:00:00 x",
        "2026-09-12 10:00:00+2",
        "2026/09/12",
    };
    for (bad) |text| {
        const res = date.parseTimestamp(text, true);
        try testing.expectError(error.InvalidTimestamp, res);
    }

    // Formatting: the fraction without trailing zeros, `+00` for tz.
    var buf: [64]u8 = undefined;
    const out = [_]struct { v: value.Value, want: String }{
        .{ .v = .{ .timestamp = sep12 }, .want = "2026-09-12 10:00:00.5" },
        .{ .v = .{ .timestamptz = sep12 }, .want = "2026-09-12 10:00:00.5+00" },
        .{ .v = .{ .timestamp = 0 }, .want = "1970-01-01 00:00:00" },
        .{ .v = .{ .timestamp = -1 }, .want = "1969-12-31 23:59:59.999999" },
        .{ .v = .{ .timestamp = 1 }, .want = "1970-01-01 00:00:00.000001" },
        .{ .v = .{ .timestamp = leap_day }, .want = "2024-02-29 00:00:00" },
        .{ .v = .{ .timestamp = year_one }, .want = "0001-01-01 00:00:00" },
    };
    for (out) |c| {
        var w = std.Io.Writer.fixed(&buf);
        try value.formatValue(&w, null, c.v);
        try testing.expectEqualStrings(c.want, w.buffered());
    }
    // Round trip every day of a leap year and its neighbours.
    var day: i64 = -366;
    while (day <= 2 * 366) : (day += 1) {
        var w = std.Io.Writer.fixed(&buf);
        const us = day * 86_400_000_000;
        try date.formatTimestamp(&w, us, false);
        const back = try date.parseTimestamp(w.buffered(), false);
        try testing.expectEqual(us, back);
    }
}

test "sql.value: formatValue renders text-format output" {
    var buf: [64]u8 = undefined;
    const cases = [_]struct { v: value.Value, want: String }{
        .{ .v = .{ .int4 = -7 }, .want = "-7" },
        .{ .v = .{ .uint8 = 42 }, .want = "42" },
        .{ .v = .{ .bool_ = true }, .want = "t" },
        .{ .v = .{ .bool_ = false }, .want = "f" },
        .{ .v = .{ .text = "hello" }, .want = "hello" },
    };
    for (cases) |c| {
        var w = std.Io.Writer.fixed(&buf);
        try value.formatValue(&w, null, c.v);
        try testing.expectEqualStrings(c.want, w.buffered());
    }
}

test "sql.value: pgTypeOid widens unsigned ints" {
    try testing.expectEqual(pgwire.oid.bool_, value.pgTypeOid(.bool_));
    try testing.expectEqual(pgwire.oid.int4, value.pgTypeOid(.int4));
    try testing.expectEqual(pgwire.oid.int4, value.pgTypeOid(.uint2)); // widen
    try testing.expectEqual(pgwire.oid.int8, value.pgTypeOid(.uint4)); // widen
    try testing.expectEqual(pgwire.oid.numeric, value.pgTypeOid(.uint8)); // widen
    try testing.expectEqual(pgwire.oid.text, value.pgTypeOid(.text));
    try testing.expectEqual(pgwire.oid.date, value.pgTypeOid(.date));
    try testing.expectEqual(pgwire.oid.text, value.pgTypeOid(.time)); // no dedicated oid yet
    try testing.expectEqual(pgwire.oid.timestamp, value.pgTypeOid(.timestamp));
    const tstz = value.pgTypeOid(.timestamptz);
    try testing.expectEqual(pgwire.oid.timestamptz, tstz);
}

/// A string column of `ty` and length `n` for the coerceString cases.
fn strCol(ty: value.Type, n: ?u32) sql.table.Column {
    return .{
        .column_id = 1,
        .pos = 0,
        .name = "s",
        .ty = ty,
        .pkey_position = null,
        .type_mod = n,
    };
}

test "sql.value: coerceString strips, counts codepoints, and rejects" {
    const cs = struct {
        fn cs(text: String, ty: value.Type, n: ?u32) !String {
            const col = strCol(ty, n);
            return value.coerceString(text, &col);
        }
    }.cs;
    // bpchar: trailing spaces stripped, then counted; excess that is only
    // spaces is fine.
    try testing.expectEqualStrings("ab", try cs("ab   ", .bpchar, 5));
    try testing.expectEqualStrings("abcde", try cs("abcde   ", .bpchar, 5));
    try testing.expectEqualStrings("", try cs("     ", .bpchar, 2));
    try testing.expectError(error.StringTooLong, cs("abcdef", .bpchar, 5));
    // Only U+0020 is padding: a tab counts and stays.
    try testing.expectEqualStrings("ab\t", try cs("ab\t", .bpchar, 5));
    try testing.expectError(error.StringTooLong, cs("abcde\t", .bpchar, 5));
    // n counts codepoints, not bytes.
    try testing.expectEqualStrings("héllo", try cs("héllo", .bpchar, 5));
    try testing.expectEqualStrings("héllo", try cs("héllo", .varchar, 5));
    try testing.expectError(error.StringTooLong, cs("héllo!", .varchar, 5));
    // varchar: no strip, length checked.
    try testing.expectEqualStrings("ab  ", try cs("ab  ", .varchar, 4));
    try testing.expectError(error.StringTooLong, cs("ab   ", .varchar, 4));
    // text: validated only. A null length skips the check for all.
    try testing.expectEqualStrings("ab   ", try cs("ab   ", .text, null));
    try testing.expectEqualStrings("abcdefgh", try cs("abcdefgh  ", .bpchar, null));
    // Invalid UTF-8 is refused for every string type.
    try testing.expectError(error.InvalidUtf8, cs("a\xff", .text, null));
    try testing.expectError(error.InvalidUtf8, cs("a\xff", .bpchar, 5));
    try testing.expectError(error.InvalidUtf8, cs("a\xff", .varchar, 5));
}

test "sql.value: formatValue pads a char(n) cell to n codepoints" {
    var buf: [16]u8 = undefined;
    var col = sql.table.Column{
        .column_id = 1,
        .pos = 0,
        .name = "c",
        .ty = .bpchar,
        .pkey_position = null,
        .type_mod = 5,
    };
    var w: std.Io.Writer = .fixed(&buf);
    try value.formatValue(&w, &col, .{ .text = "ab" });
    try testing.expectEqualStrings("ab   ", w.buffered());
    col.type_mod = 6;
    w = .fixed(&buf);
    try value.formatValue(&w, &col, .{ .text = "héllo" });
    try testing.expectEqualStrings("héllo ", w.buffered());
    col.type_mod = 3;
    w = .fixed(&buf);
    try value.formatValue(&w, &col, .{ .text = "abcdef" }); // never truncates
    try testing.expectEqualStrings("abcdef", w.buffered());
    // A text column of the same value is not padded.
    col.ty = .text;
    w = .fixed(&buf);
    try value.formatValue(&w, &col, .{ .text = "ab" });
    try testing.expectEqualStrings("ab", w.buffered());
}

test "sql.value: string types share cells, OIDs and modifiers differ" {
    try testing.expect(Type.text.isString() and Type.bpchar.isString() and Type.varchar.isString());
    try testing.expect(!Type.bytea.isString());
    try testing.expect(Type.text.assignableTo(.bpchar));
    try testing.expect(Type.bpchar.assignableTo(.varchar));
    try testing.expect(!Type.bpchar.assignableTo(.bytea));
    try testing.expectEqual(pgwire.oid.bpchar, value.pgTypeOid(.bpchar));
    try testing.expectEqual(pgwire.oid.varchar, value.pgTypeOid(.varchar));
    try testing.expectEqual(@as(i32, 88), value.pgTypeModifier(84));
    try testing.expectEqual(@as(i32, -1), value.pgTypeModifier(null));
    try testing.expectEqual(Type.bpchar, Type.fromName("bpchar").?);
    try testing.expectEqual(Type.bpchar, Type.fromName("char").?);
    try testing.expectEqual(Type.text, Type.fromName("varchar").?);
    const v = try value.parseValue(.bpchar, "abc");
    try testing.expectEqualStrings("abc", v.text);
}

test "sql.value: date parses and prints as YYYY-MM-DD" {
    try testing.expectEqual(@as(i32, 0), try date.parseDate("1970-01-01"));
    try testing.expectEqual(@as(i32, 1), try date.parseDate("1970-01-02"));
    try testing.expectEqual(@as(i32, -1), try date.parseDate("1969-12-31"));
    try testing.expectEqual(@as(i32, 9568), try date.parseDate("1996-03-13")); // a dbgen lineitem date
    try testing.expectEqual(@as(i32, 10957), try date.parseDate("2000-01-01"));
    try testing.expectEqual(@as(i32, 11016), try date.parseDate(" 2000-02-29 ")); // leap day
    // A timestamp literal keeps its day, as in Postgres; a zone is ignored.
    try testing.expectEqual(@as(i32, 9568), try date.parseDate("1996-03-13 10:00"));
    try testing.expectEqual(@as(i32, 9568), try date.parseDate("1996-03-13 23:59:59.999999+05"));
    try testing.expectEqual(@as(i32, -1), try date.parseDate("1969-12-31 23:59:59"));
    try testing.expectError(error.InvalidTimestamp, date.parseDate("2001-02-29"));
    try testing.expectError(error.InvalidTimestamp, date.parseDate("1996-13-01"));
    try testing.expectError(error.InvalidTimestamp, date.parseDate("96-03-13"));
    try testing.expectError(error.InvalidTimestamp, date.parseDate("1996/03/13"));
    try testing.expectError(error.InvalidTimestamp, date.parseDate(""));

    const v = try value.parseValue(.date, "1996-03-13");
    try testing.expectEqual(@as(i32, 9568), v.date);
    try testing.expectEqual(pgwire.oid.date, value.pgTypeOid(.date));

    var buf: [16]u8 = undefined;
    const cases = [_]String{ "1970-01-01", "1969-12-31", "1996-03-13", "2000-02-29", "2024-12-31", "0001-01-01", "9999-12-31" };
    for (cases) |text| {
        var w: std.Io.Writer = .fixed(&buf);
        try value.formatValue(&w, null, .{ .date = try date.parseDate(text) });
        try testing.expectEqualStrings(text, w.buffered());
    }
}
