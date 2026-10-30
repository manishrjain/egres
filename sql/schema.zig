// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Logical → physical schema compilation for the row codec.
//!
//! The catalog knows a table as a LogicalSchema: a set of columns, each
//! with a stable column_id and an [added, dropped) version lifetime. For a
//! given schema version, `compile` produces the PhysicalSchema — the byte
//! layout (per-column offsets, varlen directory indices, null-bit
//! positions) the codec reads and writes.
//!
//! Layout invariant: column_id is monotonic and offsets are assigned in
//! column_id order, so appending a column never shifts an existing one.
//! Hence any past version V's layout is reconstructable from the current
//! catalog by `compile(catalog, V)` — byte-identical to what was used when
//! V was current. Only the column history + each row's version tag need
//! persisting, never a snapshot of every historical layout.

const std = @import("std");
const _value = @import("value.zig");
const String = _value.String;
const Type = _value.Type;

/// A column as the catalog defines it, across its whole lifetime — the
/// logical schema's column. The codec uses only the layout-relevant subset
/// (column_id, ty, lifetime) and never reads `name`, but a logical column
/// intrinsically has one, and name→column_id resolution reads it here.
/// (nullability / defaults will join it as the catalog lands.)
pub const LogicalColumn = struct {
    /// Stable identity, monotonic, never reused. Drives layout ordering.
    column_id: u32,
    /// SQL column name. Layout-irrelevant (the physical schema is name-free);
    /// the catalog/executor resolves names → column_id through it.
    name: String,
    ty: Type,
    /// First schema version in which this column exists.
    added_in_version: u16 = 1,
    /// Version at which it was dropped; null while still live. A dropped
    /// column is retained in the catalog so older versions' layouts (which
    /// included it) stay reconstructable.
    dropped_in_version: ?u16 = null,
    /// 1-based position of this column in the table's PRIMARY KEY, or null
    /// when not part of it. Layout-irrelevant (compile never reads it);
    /// stored so the catalog fully describes the table. u16 leaves room
    /// for composite keys — today only 1 is ever written.
    pkey_position: ?u16 = null,
    /// The declared length of a `char(n)` / `varchar(n)` column, or the
    /// dimension of a `vector(n)` column; null for every other type.
    /// Layout-irrelevant: all three string types store the same bytes.
    type_mod: ?u32 = null,

    /// Live at version v iff added by v and not yet dropped at v.
    pub inline fn liveAt(self: LogicalColumn, v: u16) bool {
        if (self.added_in_version > v) return false;
        if (self.dropped_in_version) |d| return v < d;
        return true;
    }
};

/// The catalog's full view of a table: every column ever defined (live or
/// dropped), plus the latest version new rows are stamped with.
pub const LogicalSchema = struct {
    columns: []const LogicalColumn,
    current_version: u16,
};

/// Where one column sits in a compiled row. All offsets are ABSOLUTE byte
/// positions from the row's first byte (the version stamp lives at 0..2).
pub const ColumnLayout = struct {
    column_id: u32,
    ty: Type,
    /// Byte holding this column's null bit (the bitmap follows the version).
    null_byte_index: u32,
    null_bit_index: u3,
    /// Fixed types: absolute byte offset of the value. Varlen: unused (0).
    physical_offset: u32,
    /// Varlen types: index into the offset directory. Fixed: unused (0).
    var_offset_index: u32,
};

/// The compiled byte layout for a single schema version. Owns one
/// allocation (`columns`); free with deinit.
pub const PhysicalSchema = struct {
    version: u16,
    /// Live columns at this version, ascending by column_id. Index
    /// positionally, or resolve a column_id with find().
    columns: []ColumnLayout,
    /// Absolute byte offset of the varlen offset directory (u32 entries,
    /// cumulative end offsets relative to data_start).
    var_offset_array_start: u32,
    /// Absolute byte offset of the varlen data region.
    var_data_start: u32,
    /// Number of varlen columns = directory entry count.
    n_varlen: u16,

    /// Look up a column's layout by id, or null if absent in this version.
    /// Columns is small; a linear scan is fine (sort/map is a later opt).
    pub fn find(self: PhysicalSchema, column_id: u32) ?ColumnLayout {
        for (self.columns) |c| {
            if (c.column_id == column_id) return c;
        }
        return null;
    }

    pub fn deinit(self: *PhysicalSchema, alloc: std.mem.Allocator) void {
        alloc.free(self.columns);
    }
};

/// Compile the physical layout for `version` from the columns live at it.
/// Deterministic: a given live-column set always yields the same layout,
/// regardless of definition order or columns added/dropped at other
/// versions. Caller owns the result (deinit).
pub fn compile(
    alloc: std.mem.Allocator,
    logical: LogicalSchema,
    version: u16,
) !PhysicalSchema {
    var live: std.ArrayList(LogicalColumn) = .empty;
    defer live.deinit(alloc);
    for (logical.columns) |c| {
        if (c.liveAt(version)) try live.append(alloc, c);
    }
    std.mem.sort(LogicalColumn, live.items, {}, lessByColumnId);

    const n = live.items.len;
    const cols = try alloc.alloc(ColumnLayout, n);
    errdefer alloc.free(cols);

    // The null bitmap follows the 2-byte version; the fixed region follows
    // the bitmap. Fixed offsets are packed in column_id order (so appending
    // a higher column_id lands after existing fixed columns); varlen columns
    // get directory indices in the same order. null bits run sequentially in
    // column_id order. All three are append-stable.
    const null_bitmap_bytes: u32 = @intCast((n + 7) / 8);
    var fixed_off: u32 = 2 + null_bitmap_bytes;
    var var_idx: u32 = 0;
    for (live.items, 0..) |c, i| {
        const null_byte: u32 = 2 + @as(u32, @intCast(i / 8));
        const null_bit: u3 = @intCast(i % 8);
        if (c.ty.fixedWidth()) |w| {
            cols[i] = .{
                .column_id = c.column_id,
                .ty = c.ty,
                .null_byte_index = null_byte,
                .null_bit_index = null_bit,
                .physical_offset = fixed_off,
                .var_offset_index = 0,
            };
            fixed_off += w;
        } else {
            cols[i] = .{
                .column_id = c.column_id,
                .ty = c.ty,
                .null_byte_index = null_byte,
                .null_bit_index = null_bit,
                .physical_offset = 0,
                .var_offset_index = var_idx,
            };
            var_idx += 1;
        }
    }

    const var_offset_array_start = fixed_off; // just past the fixed region
    return .{
        .version = version,
        .columns = cols,
        .var_offset_array_start = var_offset_array_start,
        .var_data_start = var_offset_array_start + var_idx * 4, // u32 directory
        .n_varlen = @intCast(var_idx),
    };
}

fn lessByColumnId(_: void, a: LogicalColumn, b: LogicalColumn) bool {
    return a.column_id < b.column_id;
}
