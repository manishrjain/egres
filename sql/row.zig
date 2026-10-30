// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Marshal/unmarshal a SQL row to/from its physical byte layout.
//!
//! Layout (all little-endian; offsets precomputed in schema.PhysicalSchema):
//!   [ version u16 ][ null bitmap ][ fixed region ][ varlen u32 directory ]
//!   [ varlen data ]
//! Fixed columns sit at constant offsets; varlen columns are sliced from the
//! data region via cumulative end-offsets in the directory. Any single
//! column is reachable in O(1) — no scan of the columns before it — so a
//! projection touches only the bytes it asks for.

const std = @import("std");
const utils = @import("utils");
const _value = @import("value.zig");
const String = _value.String;
const _schema = @import("schema.zig");
const Value = _value.Value;
const Row = _value.Row;
const PhysicalSchema = _schema.PhysicalSchema;
const ColumnLayout = _schema.ColumnLayout;

/// Marshal a row into `buf` (cleared first; its capacity is reused across
/// calls — the caller owns it and frees it once). `values[i]` corresponds
/// to `ps.columns[i]` (ascending column_id order); `.null_value` encodes
/// SQL NULL. Returns a read-only view into `buf`, invalidated by the next
/// call that reuses `buf`. (A keyed marshal-by-column_id wrapper is a thin
/// follow-up for the executor.)
pub fn marshalRow(
    alloc: std.mem.Allocator,
    ps: *const PhysicalSchema,
    values: []const Value,
    buf: *std.ArrayList(u8),
) !String {
    std.debug.assert(values.len == ps.columns.len);

    // Zero-fill the fixed-size prefix: version + null bitmap + fixed region
    // + varlen directory. Fixed values and directory entries are patched in
    // place; varlen payloads append after it.
    utils.clearRetainCap(buf);
    try buf.appendNTimes(alloc, 0, ps.var_data_start);
    std.mem.writeInt(u16, buf.items[0..2], ps.version, .little);

    var var_end: u32 = 0; // cumulative end offset within the data region
    for (ps.columns, values) |col, val| {
        switch (val) {
            .null_value => {
                setNull(buf.items, col);
                // A null varlen column still needs a directory entry equal
                // to the running end (a zero-length span).
                if (col.ty.isVarlen()) {
                    writeDirEntry(buf.items, ps, col, var_end);
                }
            },
            else => {
                if (col.ty.isVarlen()) {
                    const payload = varlenBytes(val);
                    try buf.appendSlice(alloc, payload);
                    var_end += @intCast(payload.len);
                    writeDirEntry(buf.items, ps, col, var_end);
                } else {
                    writeFixed(buf.items[col.physical_offset..], val);
                }
            },
        }
    }

    return buf.items;
}

/// Decode the columns named in `projection` (a list of column_ids) from a
/// row encoded under `ps` into `row`, one Value per requested column in
/// projection order. `projection` MUST be sorted ascending by column_id: the
/// decoder merge-walks it against ps.columns (also column_id-sorted), so its
/// scan index only advances — O(projection + columns), not a scan per
/// projection. An EMPTY projection selects every column, in column_id order
/// (SELECT *). `row` is reused: its backing store is resized in place, so
/// passing the same Row across calls avoids a per-row allocation. A column
/// absent in this row's version (or whose null bit is set) decodes to
/// `.null_value`. text/bytea Values borrow `buffer`; the Row stays valid
/// only while `buffer` does.
pub fn unmarshalRow(
    alloc: std.mem.Allocator,
    ps: *const PhysicalSchema,
    buffer: String,
    projection: []const u32,
    row: *Row,
) !void {
    // Empty projection = SELECT *: every column, in column_id order. Decode
    // straight from ps.columns (no per-id lookup).
    if (projection.len == 0) {
        try row.values.resize(alloc, ps.columns.len);
        for (ps.columns, row.values.items) |col, *slot| {
            slot.* = parseCol(ps, buffer, col);
        }
        return;
    }

    // Merge-walk: both projection and ps.columns are column_id-sorted, so the
    // ps.columns cursor `j` only advances. A match parks j on the column (not
    // past it), so a duplicate id in projection re-matches it.
    try row.values.resize(alloc, projection.len);
    var j: usize = 0;
    var prev: u32 = 0;
    for (projection, row.values.items) |column_id, *slot| {
        std.debug.assert(column_id >= prev); // projection must be sorted ascending
        prev = column_id;
        while (j < ps.columns.len and ps.columns[j].column_id < column_id) j += 1;
        slot.* = if (j < ps.columns.len and ps.columns[j].column_id == column_id)
            parseCol(ps, buffer, ps.columns[j])
        else
            .null_value;
    }
}

/// Decode ONE column without touching the rest of the row: a null-bit
/// test, then one fixed-offset read or varlen-directory slice — O(1)
/// either way. Filters use this to reject a row before paying for the
/// full unmarshalRow.
pub inline fn parseCol(ps: *const PhysicalSchema, buffer: String, col: ColumnLayout) Value {
    return if (isNull(buffer, col)) .null_value else readValue(ps, buffer, col);
}

// --- internals --------------------------------------------------------------

inline fn isNull(buffer: String, col: ColumnLayout) bool {
    return (buffer[col.null_byte_index] & (@as(u8, 1) << col.null_bit_index)) != 0;
}

inline fn setNull(buffer: []u8, col: ColumnLayout) void {
    buffer[col.null_byte_index] |= (@as(u8, 1) << col.null_bit_index);
}

inline fn writeDirEntry(buffer: []u8, ps: *const PhysicalSchema, col: ColumnLayout, end: u32) void {
    const slot = ps.var_offset_array_start + col.var_offset_index * 4;
    std.mem.writeInt(u32, buffer[slot..][0..4], end, .little);
}

/// Read a present (non-null) column into a typed Value. O(1).
fn readValue(ps: *const PhysicalSchema, buffer: String, col: ColumnLayout) Value {
    const off = col.physical_offset;
    return switch (col.ty) {
        .bool_ => .{ .bool_ = buffer[off] != 0 },
        .int2 => .{ .int2 = std.mem.readInt(i16, buffer[off..][0..2], .little) },
        .int4 => .{ .int4 = std.mem.readInt(i32, buffer[off..][0..4], .little) },
        .int8 => .{ .int8 = std.mem.readInt(i64, buffer[off..][0..8], .little) },
        .uint2 => .{ .uint2 = std.mem.readInt(u16, buffer[off..][0..2], .little) },
        .uint4 => .{ .uint4 = std.mem.readInt(u32, buffer[off..][0..4], .little) },
        .uint8 => .{ .uint8 = std.mem.readInt(u64, buffer[off..][0..8], .little) },
        .float4 => .{ .float4 = @bitCast(std.mem.readInt(u32, buffer[off..][0..4], .little)) },
        .float8 => .{ .float8 = @bitCast(std.mem.readInt(u64, buffer[off..][0..8], .little)) },
        .date => .{ .date = std.mem.readInt(i32, buffer[off..][0..4], .little) },
        .time => .{ .time = std.mem.readInt(i64, buffer[off..][0..8], .little) },
        .timestamp => .{ .timestamp = std.mem.readInt(i64, buffer[off..][0..8], .little) },
        .timestamptz => .{ .timestamptz = std.mem.readInt(i64, buffer[off..][0..8], .little) },
        .interval => .{ .interval = .{
            .micros = std.mem.readInt(i64, buffer[off..][0..8], .little),
            .days = std.mem.readInt(i32, buffer[off + 8 ..][0..4], .little),
            .months = std.mem.readInt(i32, buffer[off + 12 ..][0..4], .little),
        } },
        .uuid => .{ .uuid = buffer[off..][0..16].* },
        .text, .bpchar, .varchar => .{ .text = varlenSlice(ps, buffer, col) },
        .bytea => .{ .bytea = varlenSlice(ps, buffer, col) },
        .vector => .{ .vector = varlenSlice(ps, buffer, col) },
    };
}

/// Slice a varlen column's bytes via its cumulative end-offsets. The span is
/// [prev_end, this_end) — self-contained, no need to find the next column.
inline fn varlenSlice(ps: *const PhysicalSchema, buffer: String, col: ColumnLayout) String {
    const var_base = ps.var_offset_array_start;
    const j = col.var_offset_index;
    const end = std.mem.readInt(u32, buffer[var_base + j * 4 ..][0..4], .little);
    const start = if (j == 0) 0 else std.mem.readInt(u32, buffer[var_base + (j - 1) * 4 ..][0..4], .little);
    return buffer[ps.var_data_start + start .. ps.var_data_start + end];
}

fn writeFixed(dst: []u8, val: Value) void {
    switch (val) {
        .bool_ => |v| dst[0] = if (v) 1 else 0,
        .int2 => |v| std.mem.writeInt(i16, dst[0..2], v, .little),
        .int4 => |v| std.mem.writeInt(i32, dst[0..4], v, .little),
        .int8 => |v| std.mem.writeInt(i64, dst[0..8], v, .little),
        .uint2 => |v| std.mem.writeInt(u16, dst[0..2], v, .little),
        .uint4 => |v| std.mem.writeInt(u32, dst[0..4], v, .little),
        .uint8 => |v| std.mem.writeInt(u64, dst[0..8], v, .little),
        .float4 => |v| std.mem.writeInt(u32, dst[0..4], @bitCast(v), .little),
        .float8 => |v| std.mem.writeInt(u64, dst[0..8], @bitCast(v), .little),
        .date => |v| std.mem.writeInt(i32, dst[0..4], v, .little),
        .time => |v| std.mem.writeInt(i64, dst[0..8], v, .little),
        .timestamp => |v| std.mem.writeInt(i64, dst[0..8], v, .little),
        .timestamptz => |v| std.mem.writeInt(i64, dst[0..8], v, .little),
        .interval => |v| {
            std.mem.writeInt(i64, dst[0..8], v.micros, .little);
            std.mem.writeInt(i32, dst[8..12], v.days, .little);
            std.mem.writeInt(i32, dst[12..16], v.months, .little);
        },
        .uuid => |v| @memcpy(dst[0..16], &v),
        .null_value, .text, .bytea, .vector => unreachable, // null handled by caller; varlen not fixed
    }
}

inline fn varlenBytes(val: Value) String {
    return switch (val) {
        .text, .bytea, .vector => |b| b,
        else => unreachable, // only varlen payloads reach here
    };
}
