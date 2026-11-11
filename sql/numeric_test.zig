// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! numeric.zig: parsing and printing, rounding, exact arithmetic, the
//! index key's order and the binary wire round trip.

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const pgwire = @import("pgwire");
const String = sql.value.String;
const Numeric = sql.Numeric;

fn text(a: std.mem.Allocator, d: Numeric) !String {
    var aw: std.Io.Writer.Allocating = .init(a);
    try d.format(&aw.writer);
    return aw.written();
}

test "sql.numeric: parse keeps the scale, format keeps every digit" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const cases = [_]struct { in: String, coef: i128, scale: u8, out: String }{
        .{ .in = "0.321", .coef = 321, .scale = 3, .out = "0.321" },
        .{ .in = "12.50", .coef = 1250, .scale = 2, .out = "12.50" },
        .{ .in = "-0.5", .coef = -5, .scale = 1, .out = "-0.5" },
        .{ .in = "007", .coef = 7, .scale = 0, .out = "7" },
        .{ .in = "0.00", .coef = 0, .scale = 2, .out = "0.00" },
        .{ .in = ".5", .coef = 5, .scale = 1, .out = "0.5" },
        .{ .in = "5.", .coef = 5, .scale = 0, .out = "5" },
        .{ .in = "1.5e3", .coef = 1500, .scale = 0, .out = "1500" },
        .{ .in = "15e-1", .coef = 15, .scale = 1, .out = "1.5" },
        .{ .in = " 42 ", .coef = 42, .scale = 0, .out = "42" },
        // Underscores between digits, in every part.
        .{ .in = "12_000.5_0", .coef = 1200050, .scale = 2, .out = "12000.50" },
        .{ .in = "2e0_3", .coef = 2000, .scale = 0, .out = "2000" },
        .{ .in = "1__0", .coef = 10, .scale = 0, .out = "10" },
        // A whole number in another radix.
        .{ .in = "0x10", .coef = 16, .scale = 0, .out = "16" },
        .{ .in = "-0X_ff", .coef = -255, .scale = 0, .out = "-255" },
        .{ .in = " +0o17 ", .coef = 15, .scale = 0, .out = "15" },
        .{ .in = "0b1_01", .coef = 5, .scale = 0, .out = "5" },
        .{
            .in = "12345678901234567890123456789012345678",
            .coef = 12345678901234567890123456789012345678,
            .scale = 0,
            .out = "12345678901234567890123456789012345678",
        },
    };
    for (cases) |case| {
        const d = try Numeric.parse(case.in);
        try testing.expectEqual(case.coef, d.coef);
        try testing.expectEqual(case.scale, d.scale);
        try testing.expectEqualStrings(case.out, try text(a, d));
    }
    const bad_texts = [_]String{
        "", "-", "1.2.3", "abc", "1e", "NaN", "Infinity", "1 2",
    };
    // An underscore at an edge of the digits, and a radix number
    // with no digits, a point, or a digit past its radix.
    const bad_separators = [_]String{
        "_1", "1_", "1_.5", "1._5", "1_e5", "1e_5", "0x", "0x1.5", "0b12",
    };
    for (bad_separators) |bad| {
        try testing.expectError(error.InvalidNumeric, Numeric.parse(bad));
    }
    for (bad_texts) |bad| {
        try testing.expectError(error.InvalidNumeric, Numeric.parse(bad));
    }
    // 39 digits is past the coefficient; so is a scale past 38.
    try testing.expectError(
        error.NumericOutOfRange,
        Numeric.parse("123456789012345678901234567890123456789"),
    );
    try testing.expectError(error.NumericOutOfRange, Numeric.parse("1e-39"));
}

test "sql.numeric: rescale rounds half away from zero, the others as named" {
    const half = try Numeric.parse("2.5");
    try testing.expectEqual(@as(i128, 3), (try half.rescale(0, .round)).coef);
    const neg_half = try Numeric.parse("-2.5");
    const neg_cases = [_]struct { mode: sql.numeric.Rounding, coef: i128 }{
        .{ .mode = .round, .coef = -3 },
        .{ .mode = .trunc, .coef = -2 },
        .{ .mode = .floor, .coef = -3 },
        .{ .mode = .ceil, .coef = -2 },
    };
    for (neg_cases) |case| {
        const r = try neg_half.rescale(0, case.mode);
        try testing.expectEqual(case.coef, r.coef);
    }
    // Up: zeros appended. 1.5 at scale 3 is 1.500.
    const up = try (try Numeric.parse("1.5")).rescale(3, .round);
    try testing.expectEqual(@as(i128, 1500), up.coef);
    try testing.expectEqual(@as(u8, 3), up.scale);
    // ROUND(149.5, -2) is 100: one rounding, at the hundreds.
    const r = try (try Numeric.parse("149.5")).roundTo(-2, .round);
    try testing.expectEqual(@as(i128, 100), r.coef);
    try testing.expectEqual(@as(u8, 0), r.scale);
    try testing.expectEqual(
        @as(i128, 200),
        (try (try Numeric.parse("150")).roundTo(-2, .round)).coef,
    );
}

test "sql.numeric: a column's precision and scale" {
    // numeric(5, 2) holds up to 999.99.
    var col = sql.table.Column{
        .column_id = 1,
        .pos = 0,
        .name = "n",
        .ty = .numeric,
        .pkey_position = null,
        .type_mod = (5 << 16) | 2,
    };
    const fitted = try sql.value.fitNumeric(try Numeric.parse("1.005"), &col);
    try testing.expectEqual(@as(i128, 101), fitted.coef);
    try testing.expectEqual(@as(u8, 2), fitted.scale);
    try testing.expectEqual(
        @as(i128, 99999),
        (try sql.value.fitNumeric(try Numeric.parse("999.994"), &col)).coef,
    );
    try testing.expectError(
        error.NumericOutOfRange,
        sql.value.fitNumeric(try Numeric.parse("999.995"), &col),
    );
    try testing.expectError(
        error.NumericOutOfRange,
        sql.value.fitNumeric(try Numeric.parse("1000"), &col),
    );
    // Without a modifier the value's own scale stays.
    col.type_mod = null;
    const own = try sql.value.fitNumeric(try Numeric.parse("1.005"), &col);
    try testing.expectEqual(@as(u8, 3), own.scale);
}

test "sql.numeric: order and equality across scales" {
    const a = try Numeric.parse("1.50");
    const b = try Numeric.parse("1.5");
    const c = try Numeric.parse("1.499");
    try testing.expect(a.eql(b));
    try testing.expectEqual(std.math.Order.eq, Numeric.order(a, b));
    try testing.expectEqual(std.math.Order.gt, Numeric.order(a, c));
    try testing.expectEqual(std.math.Order.lt, Numeric.order(c, a));
    try testing.expectEqual(
        std.math.Order.lt,
        Numeric.order(try Numeric.parse("-0.001"), Numeric.zero),
    );
    const n = (try Numeric.parse("1.500")).normalized();
    try testing.expectEqual(@as(i128, 15), n.coef);
    try testing.expectEqual(@as(u8, 1), n.scale);
}

test "sql.numeric: arithmetic is exact" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const x = try Numeric.parse("0.1");
    const y = try Numeric.parse("0.2");
    try testing.expectEqualStrings("0.3", try text(a, try x.add(y)));
    try testing.expectEqualStrings("-0.1", try text(a, try x.sub(y)));
    try testing.expectEqualStrings("0.02", try text(a, try x.mul(y)));
    // 1 / 3 to 16 places, 10 / 4 at the dividend's scale or 16.
    const one = Numeric.fromInt(1);
    try testing.expectEqualStrings(
        "0.3333333333333333",
        try text(a, try one.div(Numeric.fromInt(3))),
    );
    try testing.expectEqualStrings(
        "0.6666666666666667",
        try text(a, try Numeric.fromInt(2).div(Numeric.fromInt(3))),
    );
    try testing.expectError(error.DivisionByZero, one.div(Numeric.zero));
    // 7.5 % 2 is 1.5; the dividend's sign.
    try testing.expectEqualStrings(
        "1.5",
        try text(a, try (try Numeric.parse("7.5")).mod(Numeric.fromInt(2))),
    );
    try testing.expectEqualStrings(
        "-1.5",
        try text(a, try (try Numeric.parse("-7.5")).mod(Numeric.fromInt(2))),
    );
    // A result past 38 digits keeps fewer fractional ones, rounded;
    // out of range only when the integer digits alone are past 38.
    const big = try Numeric.parse("12345678901234567890");
    try testing.expectError(error.NumericOutOfRange, big.mul(big));
    const tiny = try Numeric.parse("0.00000000000000000001"); // 1e-20
    const prod = try tiny.mul(tiny);
    try testing.expectEqual(@as(u8, 38), prod.scale);
    try testing.expectEqual(@as(i128, 0), prod.coef);
    const wide = try Numeric.parse("1234567890.1234567890");
    try testing.expectEqualStrings(
        "1524157875323883675.0190519987501905210",
        try text(a, try wide.mul(wide)),
    );
    const huge = try Numeric.parse("10000000000000000000000000000000000000"); // 1e37
    const sum = try huge.add(tiny);
    try testing.expectEqual(@as(u8, 0), sum.scale);
    try testing.expect(sum.eql(huge));
    // 1e37 + 9e37 is 1e38, a 39th integer digit.
    try testing.expectError(
        error.NumericOutOfRange,
        huge.add(try Numeric.parse("9e37")),
    );
    try testing.expectEqualStrings(
        "333333333333333333333333333333.33333333",
        try text(a, try (try Numeric.parse("1e30")).div(Numeric.fromInt(3))),
    );
    try testing.expectError(
        error.NumericOutOfRange,
        (try Numeric.parse("1e30")).div(tiny),
    );
    const one_half = try Numeric.parse("1.5");
    try testing.expectEqual(@as(f64, 1.5), one_half.toFloat());
    const from_float = try Numeric.fromFloat(1.5);
    try testing.expectEqualStrings("1.5", try text(a, from_float));
}

test "sql.numeric: the index key orders as the values do" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ordered = [_]String{
        "-1000", "-999.9", "-1.5", "-1.23", "-1",  "-0.5", "-0.001",
        "0",     "0.0001", "0.5",  "0.999", "1",   "1.2",  "1.23",
        "9.99",  "10",     "10.5", "99",    "100", "1e10",
    };
    var keys: std.ArrayList(String) = .empty;
    for (ordered) |t| {
        var buf: std.ArrayList(u8) = .empty;
        try (try Numeric.parse(t)).encodeKey(a, &buf);
        try keys.append(a, buf.items);
    }
    for (keys.items[1..], 0..) |k, i| {
        try testing.expect(std.mem.order(u8, keys.items[i], k) == .lt);
    }
    // 1.5 and 1.50 share a key.
    var k1: std.ArrayList(u8) = .empty;
    var k2: std.ArrayList(u8) = .empty;
    try (try Numeric.parse("1.5")).encodeKey(a, &k1);
    try (try Numeric.parse("1.50")).encodeKey(a, &k2);
    try testing.expectEqualSlices(u8, k1.items, k2.items);
}

test "sql.numeric: the binary wire format round-trips through numeric_send" {
    const cases = [_]String{
        "0",               "1",                    "-1",   "1234.5678", "0.321", "0.0005", "12000", "10000",
        "-12345678.90000", "18446744073709551615", "0.00", "1e10",
    };
    for (cases) |t| {
        const d = try Numeric.parse(t);
        var aw: std.Io.Writer.Allocating = .init(testing.allocator);
        defer aw.deinit();
        try pgwire.binary.encodeNumeric(&aw.writer, d.coef, d.scale);
        const back = try Numeric.decodeBinary(aw.written());
        try testing.expectEqual(d.coef, back.coef);
        try testing.expectEqual(d.scale, back.scale);
    }
    // NaN (sign 0xC000) is refused; so is a short frame.
    try testing.expectError(
        error.InvalidNumeric,
        Numeric.decodeBinary(&.{ 0, 0, 0, 0, 0xC0, 0, 0, 0 }),
    );
    try testing.expectError(
        error.InvalidNumeric,
        Numeric.decodeBinary(&.{ 0, 1 }),
    );
}
