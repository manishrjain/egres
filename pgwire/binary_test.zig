// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Binary encoder/decoder tests for the primitive Postgres types.
//! Pure-logic — every byte goes through a stack buffer.

const std = @import("std");
const testing = std.testing;
const pgwire = @import("pgwire");

const binary = pgwire.binary;
const types = pgwire.types;

test "bool round-trip" {
    var buf: [1]u8 = undefined;
    binary.encodeBool(&buf, true);
    try testing.expectEqual(@as(u8, 1), buf[0]);
    try testing.expectEqual(true, try binary.decodeBool(&buf));
    binary.encodeBool(&buf, false);
    try testing.expectEqual(@as(u8, 0), buf[0]);
    try testing.expectEqual(false, try binary.decodeBool(&buf));
    try testing.expectError(error.BadBool, binary.decodeBool(&[_]u8{2}));
    try testing.expectError(error.BadLength, binary.decodeBool(&[_]u8{ 0, 0 }));
}

test "int2/int4/int8 round-trip including extremes" {
    var b2: [2]u8 = undefined;
    binary.encodeInt2(&b2, std.math.minInt(i16));
    try testing.expectEqual(std.math.minInt(i16), try binary.decodeInt2(&b2));
    binary.encodeInt2(&b2, std.math.maxInt(i16));
    try testing.expectEqual(std.math.maxInt(i16), try binary.decodeInt2(&b2));

    var b4: [4]u8 = undefined;
    binary.encodeInt4(&b4, -1);
    try testing.expectEqual(@as(i32, -1), try binary.decodeInt4(&b4));
    // -1 in int32 BE is 0xFFFFFFFF; verify the wire bytes match.
    try testing.expectEqualSlices(u8, &.{ 0xFF, 0xFF, 0xFF, 0xFF }, &b4);
    binary.encodeInt4(&b4, 101);
    try testing.expectEqualSlices(u8, &.{ 0x00, 0x00, 0x00, 0x65 }, &b4);

    var b8: [8]u8 = undefined;
    binary.encodeInt8(&b8, std.math.maxInt(i64));
    try testing.expectEqual(std.math.maxInt(i64), try binary.decodeInt8(&b8));
}

test "float4/float8 round-trip + bit-pattern check" {
    var b4: [4]u8 = undefined;
    binary.encodeFloat4(&b4, 1.0);
    // 1.0 as float32 is 0x3F800000.
    try testing.expectEqualSlices(u8, &.{ 0x3F, 0x80, 0x00, 0x00 }, &b4);
    try testing.expectEqual(@as(f32, 1.0), try binary.decodeFloat4(&b4));

    var b8: [8]u8 = undefined;
    binary.encodeFloat8(&b8, 1.0);
    // 1.0 as float64 is 0x3FF0000000000000.
    try testing.expectEqualSlices(
        u8,
        &.{ 0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 },
        &b8,
    );
    try testing.expectEqual(@as(f64, 1.0), try binary.decodeFloat8(&b8));

    // NaN survives the round-trip in bit-pattern terms (==.NaN
    // never holds, so we compare bits).
    binary.encodeFloat8(&b8, std.math.nan(f64));
    const back = try binary.decodeFloat8(&b8);
    try testing.expect(std.math.isNan(back));
}

test "isBinarySupported covers expected OIDs" {
    try testing.expect(binary.isBinarySupported(types.oid.int4));
    try testing.expect(binary.isBinarySupported(types.oid.bool_));
    try testing.expect(binary.isBinarySupported(types.oid.float8));
    try testing.expect(!binary.isBinarySupported(types.oid.text));
    try testing.expect(!binary.isBinarySupported(types.oid.numeric));
}

test "numeric: base-10000 digits, weight, sign and scale as numeric_send" {
    const Case = struct { coef: i128, scale: u16, bytes: []const u8 };
    const cases = [_]Case{
        // 1234.5678: two digits at weight 0.
        .{ .coef = 12345678, .scale = 4, .bytes = &.{
            0, 2, 0, 0, 0, 0, 0, 4, 0x04, 0xD2, 0x16, 0x2E,
        } },
        // 0.321: the one digit is the first group after the point.
        .{ .coef = 321, .scale = 3, .bytes = &.{
            0, 1, 0xFF, 0xFF, 0, 0, 0, 3, 0x0C, 0x8A,
        } },
        // 0.0005: the group holds the whole fraction.
        .{ .coef = 5, .scale = 4, .bytes = &.{
            0, 1, 0xFF, 0xFF, 0, 0, 0, 4, 0, 5,
        } },
        // Zero at scale 2: no digits, the scale kept.
        .{ .coef = 0, .scale = 2, .bytes = &.{ 0, 0, 0, 0, 0, 0, 0, 2 } },
        // -1: the sign word.
        .{ .coef = -1, .scale = 0, .bytes = &.{
            0, 1, 0, 0, 0x40, 0, 0, 0, 0, 1,
        } },
        // 10000: a trailing zero group is dropped, the weight stays.
        .{ .coef = 10000, .scale = 0, .bytes = &.{
            0, 1, 0, 1, 0, 0, 0, 0, 0, 1,
        } },
        // 12000: [1, 2000] at weight 1.
        .{ .coef = 12000, .scale = 0, .bytes = &.{
            0, 2, 0, 1, 0, 0, 0, 0, 0, 1, 0x07, 0xD0,
        } },
        // The largest uint8, five groups.
        .{ .coef = 18446744073709551615, .scale = 0, .bytes = &.{
            0,    5,    0,    4,    0,    0,    0,    0,    0x07,
            0x34, 0x1A, 0x58, 0x02, 0xE1, 0x03, 0xBB, 0x06, 0x4F,
        } },
    };
    for (cases) |case| {
        var aw: std.Io.Writer.Allocating = .init(testing.allocator);
        defer aw.deinit();
        try binary.encodeNumeric(&aw.writer, case.coef, case.scale);
        try testing.expectEqualSlices(u8, case.bytes, aw.written());
    }
}
