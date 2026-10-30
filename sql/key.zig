// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! SQL key encodings. zdb sees only opaque key bytes; this is where
//! the SQL layer gives them meaning.
//!
//! Row keys: a data table keys each row by its egid — a per-table,
//! monotonically increasing u64 starting at 1 (0 is never a row) —
//! stored as an 8-byte big-endian
//! primary key, so byte order matches numeric order: scans walk rows
//! in insertion order and the largest key is the largest egid.
//!
//! Index keys: an index entry keys on the indexed cell's VALUE,
//! encoded memcomparable — `std.mem.order` over the bytes matches SQL
//! order over the values. Row cells are native-LE (row.zig) and not
//! order-preserving, so index keys get their own codec. Every key
//! leads with a tag byte (see `tag_value`/`tag_null`): it gives NULL
//! its own address — a posting distinct from every real value,
//! including '' — and sorts it after all real values (Postgres's
//! NULLS LAST). Keys are single-column: raw text/bytea bytes need no
//! terminator or escaping — memcmp IS the collation. (Composite keys
//! will switch to per-component framing wholesale; this raw format is
//! its degenerate case.) Integers and floats widen to 64-bit before
//! encoding: widening is lossless and order-preserving, and an
//! index's column type is fixed so key widths never mix.

const std = @import("std");
const _value = @import("value.zig");
const String = _value.String;

/// The 8-byte big-endian primary key for a row with this egid.
pub fn encodeEgid(egid: u64) [8]u8 {
    var k: [8]u8 = undefined;
    std.mem.writeInt(u64, &k, egid, .big);
    return k;
}

/// The egid a primary key encodes, or null if it isn't egid-shaped
/// (8 bytes) — e.g. a raw-KV or catalog key.
pub fn decodeEgid(primary: String) ?u64 {
    if (primary.len != 8) return null;
    return std.mem.readInt(u64, primary[0..8], .big);
}

/// An 8-byte key of two u32 halves, `upper` major. In memory it is a plain
/// host-endian u64 whose numeric order is lexicographic (upper, lower)
/// order — upper fills the high bits; encode() writes it big-endian, so
/// memcmp over the bytes matches too. Both catalog key shapes —
/// (table_id, column_id) and (base_table_id, index_table_id) — are this.
pub const U64 = packed struct(u64) {
    lower: u32, // first field = least significant bits
    upper: u32,

    /// The key's 8 big-endian bytes. The ONLY struct→bytes path: the
    /// struct's native bytes are little-endian and must never be stored.
    pub fn encode(self: U64) [8]u8 {
        var k: [8]u8 = undefined;
        std.mem.writeInt(u64, &k, @bitCast(self), .big);
        return k;
    }

    /// The halves `k` encodes, or null if it isn't 8 bytes.
    pub fn decode(k: String) ?U64 {
        if (k.len != 8) return null;
        return @bitCast(std.mem.readInt(u64, k[0..8], .big));
    }
};

/// Tag byte leading every index key. Comparison hits it first: all
/// real values share `tag_value` and order on their encoding exactly
/// as before, while NULL's lone `tag_null` byte sorts after every
/// real value — NULLS LAST.
const tag_value: u8 = 0x00;
const tag_null: u8 = 0x01;

/// The memcomparable index-key encoding of `v`: [tag_value] ++ value
/// encoding, or the single [tag_null] byte for NULL. With `buf` the
/// key is appended there and the return borrows from it; with null
/// the key comes back as its own exactly-sized `a` allocation. Not
/// encodable: interval (months vs days have no calendar-free order) —
/// `buf` is left untouched. Temporal types are signed day/µs counts
/// (value.zig), so they encode as signed integers.
pub fn encodeIndexKey(
    a: std.mem.Allocator,
    v: _value.Value,
    buf: ?*std.ArrayList(u8),
) ![]u8 {
    var own: std.ArrayList(u8) = .empty;
    errdefer own.deinit(a);
    const out = buf orelse &own;
    const start = out.items.len;
    switch (v) {
        .null_value => try out.append(a, tag_null),
        .interval, .vector => return error.Unencodable,
        else => {
            try out.append(a, tag_value);
            switch (v) {
                .null_value, .interval, .vector => unreachable,
                .bool_ => |b| try out.append(a, @intFromBool(b)),
                inline .int2,
                .int4,
                .int8,
                .date,
                .time,
                .timestamp,
                .timestamptz,
                => |x| try appendSigned(a, x, out),
                inline .uint2, .uint4, .uint8 => |x| try appendUnsigned(a, x, out),
                inline .float4, .float8 => |x| try appendFloat(a, x, out),
                .uuid => |u| try out.appendSlice(a, &u),
                .text, .bytea => |s| try out.appendSlice(a, s),
            }
        },
    }
    if (buf != null) return out.items[start..];
    return own.toOwnedSlice(a);
}

const sign_bit: u64 = 1 << 63;

/// Big-endian bytes of an unsigned integer sort numerically as-is.
fn appendUnsigned(
    a: std.mem.Allocator,
    x: u64,
    buf: *std.ArrayList(u8),
) !void {
    var b: [8]u8 = undefined;
    std.mem.writeInt(u64, &b, x, .big);
    try buf.appendSlice(a, &b);
}

/// Two's-complement bytes almost sort correctly already — the sign bit
/// just puts the negative half above the positive half. Flipping that
/// one bit swaps the halves: minInt→0x00.., -1→0x7F.., 0→0x80..,
/// maxInt→0xFF.. — a plain sortable unsigned.
fn appendSigned(
    a: std.mem.Allocator,
    x: i64,
    buf: *std.ArrayList(u8),
) !void {
    const u: u64 = @bitCast(x);
    try appendUnsigned(a, u ^ sign_bit, buf);
}

/// IEEE bits are sign-magnitude: read as unsigned ints, positive
/// floats already sort correctly; negative floats sort backwards, and
/// above the positives. Setting the sign bit on positives lifts them
/// to the top half; inverting every bit of negatives reverses their
/// order and drops them to the bottom half. Equal values encode
/// identically: -0.0 becomes +0.0, and NaN gets one fixed key above
/// every real value (Postgres sorts NaN last).
fn appendFloat(
    a: std.mem.Allocator,
    f: f64,
    buf: *std.ArrayList(u8),
) !void {
    if (std.math.isNan(f))
        return appendUnsigned(a, std.math.maxInt(u64), buf);
    const x = if (f == 0) 0 else f;
    const u: u64 = @bitCast(x);
    if (x < 0) return appendUnsigned(a, ~u, buf);
    try appendUnsigned(a, u | sign_bit, buf);
}
