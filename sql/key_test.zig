// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");

const key = sql.key;
const Value = sql.Value;

test "sql.key: egid encodes big-endian and round-trips in key order" {
    const a = key.encodeEgid(1);
    const b = key.encodeEgid(256);
    // Big-endian ⇒ byte order matches numeric order.
    try testing.expect(std.mem.order(u8, &a, &b) == .lt);
    try testing.expectEqual(@as(?u64, 1), key.decodeEgid(&a));
    try testing.expectEqual(@as(?u64, 256), key.decodeEgid(&b));
    // Non-8-byte primaries aren't egid-shaped.
    try testing.expectEqual(@as(?u64, null), key.decodeEgid("abc"));
    try testing.expectEqual(@as(?u64, null), key.decodeEgid(""));
}

// Encode one value into an owned buffer.
fn enc(v: Value) ![]u8 {
    return key.encodeIndexKey(testing.allocator, v, null);
}

// Assert each value encodes strictly above its predecessor.
fn expectAscending(vals: []const Value) !void {
    var prev: ?[]u8 = null;
    defer if (prev) |p| testing.allocator.free(p);
    for (vals) |v| {
        const cur = try enc(v);
        if (prev) |p| {
            try testing.expect(std.mem.order(u8, p, cur) == .lt);
            testing.allocator.free(p);
        }
        prev = cur;
    }
}

fn expectSameKey(x: Value, y: Value) !void {
    const kx = try enc(x);
    defer testing.allocator.free(kx);
    const ky = try enc(y);
    defer testing.allocator.free(ky);
    try testing.expectEqualSlices(u8, kx, ky);
}

test "sql.key: U64 sorts upper-major and round-trips" {
    const a = (key.U64{ .upper = 7, .lower = 2 }).encode();
    const b = (key.U64{ .upper = 7, .lower = 10 }).encode();
    const c = (key.U64{ .upper = 8, .lower = 1 }).encode();
    // Byte order == (upper, lower) order: lower 2 < 10 within upper 7,
    // and upper 7 < 8 regardless of lower.
    try testing.expect(std.mem.order(u8, &a, &b) == .lt);
    try testing.expect(std.mem.order(u8, &b, &c) == .lt);

    const d = key.U64.decode(&b).?;
    try testing.expectEqual(@as(u32, 7), d.upper);
    try testing.expectEqual(@as(u32, 10), d.lower);
    try testing.expectEqual(null, key.U64.decode(b[0..7]));
}

test "sql.key: signed integers sort numerically" {
    try expectAscending(&.{
        .{ .int8 = std.math.minInt(i64) },
        .{ .int8 = -1_000_000 },
        .{ .int8 = -1 },
        .{ .int8 = 0 },
        .{ .int8 = 1 },
        .{ .int8 = 1_000_000 },
        .{ .int8 = std.math.maxInt(i64) },
    });
    // Narrow widths widen losslessly — same ordering.
    try expectAscending(&.{
        .{ .int2 = std.math.minInt(i16) },
        .{ .int2 = -1 },
        .{ .int2 = 0 },
        .{ .int2 = std.math.maxInt(i16) },
    });
    // Temporal payloads are signed counts.
    try expectAscending(&.{
        .{ .date = -30_000 },
        .{ .date = 0 },
        .{ .date = 30_000 },
    });
}

test "sql.key: unsigned integers sort numerically" {
    try expectAscending(&.{
        .{ .uint8 = 0 },
        .{ .uint8 = 1 },
        .{ .uint8 = 256 },
        .{ .uint8 = std.math.maxInt(u64) },
    });
}

test "sql.key: floats sort numerically, NaN last, zeros unify" {
    const inf = std.math.inf(f64);
    try expectAscending(&.{
        .{ .float8 = -inf },
        .{ .float8 = -1.5e300 },
        .{ .float8 = -2.5 },
        .{ .float8 = -1e-300 },
        .{ .float8 = 0 },
        .{ .float8 = 1e-300 },
        .{ .float8 = 2.5 },
        .{ .float8 = 1.5e300 },
        .{ .float8 = inf },
        .{ .float8 = std.math.nan(f64) },
    });
    try expectSameKey(.{ .float8 = 0.0 }, .{ .float8 = -0.0 });
    try expectSameKey(
        .{ .float8 = std.math.nan(f64) },
        .{ .float8 = -std.math.nan(f64) },
    );
    try expectAscending(&.{
        .{ .float4 = -1.5 },
        .{ .float4 = 0 },
        .{ .float4 = 1.5 },
    });
}

test "sql.key: text and bool sort in value order" {
    try expectAscending(&.{
        .{ .text = "" },
        .{ .text = "a" },
        .{ .text = "a\x00" },
        .{ .text = "ab" },
        .{ .text = "b" },
    });
    try expectAscending(&.{ .{ .bool_ = false }, .{ .bool_ = true } });
}

test "sql.key: NULL keys on its tag byte, after every real value" {
    // NULL has its own address — distinct even from '' (which is a
    // real, zero-byte value under the value tag) — and sorts last,
    // Postgres's NULLS LAST.
    try expectAscending(&.{
        .{ .text = "" },
        .{ .text = "zzz" },
        .null_value,
    });
    try expectAscending(&.{
        .{ .int8 = std.math.maxInt(i64) },
        .null_value,
    });
    const nk = try enc(.null_value);
    defer testing.allocator.free(nk);
    try testing.expectEqual(1, nk.len);
}

test "sql.key: a unique index's NULL keys sort after values, by egid" {
    const a = testing.allocator;
    const top = try enc(.{ .int8 = std.math.maxInt(i64) });
    defer a.free(top);
    const n1 = try key.encodeUniqueNullKey(a, 1);
    defer a.free(n1);
    const n2 = try key.encodeUniqueNullKey(a, 256);
    defer a.free(n2);
    const last = try key.encodeUniqueNullKey(a, std.math.maxInt(u64));
    defer a.free(last);

    // Every value < values_end <= NULL keys, in egid order < keys_end.
    try testing.expect(std.mem.lessThan(u8, top, key.values_end));
    try testing.expect(!std.mem.lessThan(u8, n1, key.values_end));
    try testing.expect(std.mem.lessThan(u8, n1, n2));
    try testing.expect(std.mem.lessThan(u8, n2, last));
    try testing.expect(std.mem.lessThan(u8, last, key.keys_end));

    try testing.expect(key.isNullKey(n1));
    try testing.expect(!key.isNullKey(top));
}

test "sql.key: interval is unencodable and leaves the buffer clean" {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(testing.allocator);
    try buf.append(testing.allocator, 'x');
    try testing.expectError(
        error.Unencodable,
        key.encodeIndexKey(testing.allocator, .{ .interval = .{} }, &buf),
    );
    // No stray tag byte on the error path — callers reuse buffers.
    try testing.expectEqualSlices(u8, "x", buf.items);

    // The buffered form returns a view of what it appended.
    const k = try key.encodeIndexKey(testing.allocator, .{ .int8 = 5 }, &buf);
    try testing.expectEqual(9, k.len); // tag + widened i64
    try testing.expectEqualSlices(u8, buf.items[1..], k);
}
