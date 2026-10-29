// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The system catalog: table 1 in every namespace.
//!
//! Each namespace (database) carries its own catalog, so a namespace is
//! self-describing: clone its trees and the clone brings every table and its
//! schema along; drop the namespace and the catalog dies with it — no
//! cross-namespace cleanup. (Postgres does the same: pg_attribute is
//! per-database.) Its row KEY is (table_id, column_id) big-endian, so a
//! table_id prefix scan walks one table's columns in column_id order —
//! exactly what schema.compile() consumes — and a full scan of the tree
//! loads the whole database's catalog upfront.
//!
//! The row VALUE describes the column (Postgres pg_attribute model). A
//! table's LogicalSchema is the rows sharing its table_id; reconstruct()
//! gathers them and derives current_version (no per-table row needed). The
//! catalog's OWN schema cannot live in a catalog (that is circular), so
//! `logical_schema` is a const compiled into the binary.
//!
//! Out of scope here: table-name ↔ table_id and database metadata (a separate
//! registry, pg_class's job). On disk the `type` column is the Type enum
//! ordinal, so reordering Type is a catalog-format change.
//!
//! Table 2 is a second system catalog: the INDEX registry (pg_index's job).
//! One row per index, keyed (base_table_id, index_table_id) big-endian, so a
//! base_table_id prefix scan walks a table's indexes — what DROP TABLE and
//! schema load consume. Its own compiled-in schema, same as table 1's.

const std = @import("std");
const _value = @import("value.zig");
const String = _value.String;
const _schema = @import("schema.zig");
const _key = @import("key.zig");

const Type = _value.Type;
const Value = _value.Value;
const Row = _value.Row;
const LogicalColumn = _schema.LogicalColumn;
const LogicalSchema = _schema.LogicalSchema;

/// Every namespace reserves table_id 1 for its column catalog and table_id 2
/// for its index catalog (0 is the raw-KV default table; tids 3–15 stay
/// reserved for future system tables; user tables start at zdb's
/// first_table_id, 16).
pub const table_id: u32 = 1;
pub const index_table_id: u32 = 2;

/// The catalog's own columns, by stable column_id. Their ascending order is
/// also the cell order encodeColumn/decodeColumn use. (`type` is a Zig
/// builtin, hence `column_type`; the SQL column name stays "type".)
pub const Field = enum(u32) {
    table_id = 1,
    column_id = 2,
    name = 3,
    column_type = 4,
    added_in_version = 5,
    dropped_in_version = 6,
    pkey_position = 7,
    type_mod = 8,
    not_null = 9,
};

/// The fixed schema of the catalog table, compiled into the binary (never
/// persisted — that would be circular). Version 1; changing it is a binary
/// upgrade, not a DDL. ids are uint4 (u32), `type` is the Type ordinal in
/// uint2, versions are uint2 (u16); dropped_in_version is NULL while the
/// described column is live, pkey_position is NULL for non-key columns,
/// type_mod (uint4) is the `char(n)` / `varchar(n)` length and NULL for
/// every other column, not_null (bool) is the NOT NULL constraint.
pub const logical_schema: LogicalSchema = .{
    .current_version = 1,
    .columns = &.{
        .{ .column_id = @intFromEnum(Field.table_id), .name = "table_id", .ty = .uint4 },
        .{ .column_id = @intFromEnum(Field.column_id), .name = "column_id", .ty = .uint4 },
        .{ .column_id = @intFromEnum(Field.name), .name = "name", .ty = .text },
        .{ .column_id = @intFromEnum(Field.column_type), .name = "type", .ty = .uint2 },
        .{ .column_id = @intFromEnum(Field.added_in_version), .name = "added_in_version", .ty = .uint2 },
        .{ .column_id = @intFromEnum(Field.dropped_in_version), .name = "dropped_in_version", .ty = .uint2 },
        .{ .column_id = @intFromEnum(Field.pkey_position), .name = "pkey_position", .ty = .uint2 },
        .{ .column_id = @intFromEnum(Field.type_mod), .name = "type_mod", .ty = .uint4 },
        .{ .column_id = @intFromEnum(Field.not_null), .name = "not_null", .ty = .bool_ },
    },
};

/// A decoded catalog row: the column it describes, plus its owning table.
pub const TableColumn = struct {
    table_id: u32,
    column: LogicalColumn,
};

/// A decoded catalog row key: which column of which table. The namespace is
/// implicit — it is the tree the row lives in.
pub const Key = struct {
    table_id: u32,
    column_id: u32,
};

pub const Error = error{CorruptCatalogRow};

/// Build the catalog row cells (column_id order) describing `column` of
/// `owner_table_id`. Pass `&result` to marshalRow with the catalog's compiled
/// PhysicalSchema. `column.name` is borrowed — marshalRow copies it out.
pub fn encodeColumn(owner_table_id: u32, column: LogicalColumn) [9]Value {
    return .{
        .{ .uint4 = owner_table_id },
        .{ .uint4 = column.column_id },
        .{ .text = column.name },
        .{ .uint2 = @intFromEnum(column.ty) },
        .{ .uint2 = column.added_in_version },
        if (column.dropped_in_version) |d| .{ .uint2 = d } else .null_value,
        if (column.pkey_position) |p| .{ .uint2 = p } else .null_value,
        if (column.type_mod) |n| .{ .uint4 = n } else .null_value,
        .{ .bool_ = column.not_null },
    };
}

/// Decode a fully-read catalog row (SELECT *, nine cells in column_id
/// order) into the column it describes. `column.name` borrows `r`'s buffer,
/// so it stays valid only while that buffer does — dupe it to outlive a
/// reused Row.
pub fn decodeColumn(r: *const Row) Error!TableColumn {
    if (r.len() != 9) return error.CorruptCatalogRow;

    const ty = std.enums.fromInt(Type, r.at(3).uint2) orelse return error.CorruptCatalogRow;
    const dropped: ?u16 = switch (r.at(5)) {
        .null_value => null,
        .uint2 => |d| d,
        else => return error.CorruptCatalogRow,
    };
    const pkey: ?u16 = switch (r.at(6)) {
        .null_value => null,
        .uint2 => |p| p,
        else => return error.CorruptCatalogRow,
    };
    const type_mod: ?u32 = switch (r.at(7)) {
        .null_value => null,
        .uint4 => |n| n,
        else => return error.CorruptCatalogRow,
    };
    const not_null = switch (r.at(8)) {
        .bool_ => |b| b,
        else => return error.CorruptCatalogRow,
    };
    return .{
        .table_id = r.at(0).uint4,
        .column = .{
            .column_id = r.at(1).uint4,
            .name = r.at(2).text,
            .ty = ty,
            .added_in_version = r.at(4).uint2,
            .dropped_in_version = dropped,
            .pkey_position = pkey,
            .type_mod = type_mod,
            .not_null = not_null,
        },
    };
}

/// A table's current version is the highest version stamped on any of its
/// columns: every layout-changing DDL bumps the version and records it as
/// some column's added_in or dropped_in. A table is at least version 1
/// (covers an empty/all-dropped table).
pub fn deriveCurrentVersion(columns: []const LogicalColumn) u16 {
    var v: u16 = 1;
    for (columns) |c| {
        v = @max(v, c.added_in_version);
        if (c.dropped_in_version) |d| v = @max(v, d);
    }
    return v;
}

/// Assemble a table's LogicalSchema from its catalog columns. `columns` is
/// borrowed (the caller owns the slice); current_version is derived.
pub fn reconstruct(columns: []const LogicalColumn) LogicalSchema {
    return .{ .columns = columns, .current_version = deriveCurrentVersion(columns) };
}

/// Row key within the catalog's tree: (table_id, column_id) big-endian, so byte
/// order matches numeric order — a prefix scan over a table_id walks its
/// columns in ascending column_id.
pub fn encodeKey(owner_table_id: u32, column_id: u32) [8]u8 {
    return (_key.U64{ .upper = owner_table_id, .lower = column_id }).encode();
}

pub fn decodeKey(k: String) Error!Key {
    const p = _key.U64.decode(k) orelse return error.CorruptCatalogRow;
    return .{ .table_id = p.upper, .column_id = p.lower };
}

// -----------------------------------------------------------------------------
// Index catalog (table 2): one row per index. Value = the index descriptor
// (which base table, which column, unique?, valid from when?); key =
// (base_table_id, index_table_id) big-endian.
// -----------------------------------------------------------------------------

/// The index catalog's columns, by stable column_id — also the cell order
/// encodeIndex/decodeIndex use.
pub const IndexField = enum(u32) {
    base_table_id = 1,
    index_table_id = 2,
    column_id = 3,
    unique = 4,
    valid_ts = 5,
};

/// The fixed schema of the index catalog, compiled into the binary. ids are
/// uint4, `unique` a bool, `valid_ts` a uint8 HLC timestamp. Single-column
/// today; a composite index will widen `column_id` into an ordered list
/// cell.
pub const index_logical_schema: LogicalSchema = .{
    .current_version = 1,
    .columns = &.{
        .{ .column_id = @intFromEnum(IndexField.base_table_id), .name = "base_table_id", .ty = .uint4 },
        .{ .column_id = @intFromEnum(IndexField.index_table_id), .name = "index_table_id", .ty = .uint4 },
        .{ .column_id = @intFromEnum(IndexField.column_id), .name = "column_id", .ty = .uint4 },
        .{ .column_id = @intFromEnum(IndexField.unique), .name = "unique", .ty = .bool_ },
        .{
            .column_id = @intFromEnum(IndexField.valid_ts),
            .name = "valid_ts",
            .ty = .uint8,
        },
    },
};

/// A decoded index catalog row: one index of `base_table_id`, stored in its
/// own tree (`index_table_id`), keyed on `column_id`'s value. `valid_ts`
/// is the timestamp from which the index is complete: a statement whose
/// read_ts is below it does not read the index (its backfilled entries,
/// installed at ts = valid_ts, would be invisible), and a writer's
/// read_ts is bumped to it before a unique probe. `building` while the
/// backfill runs; the backfill's snapshot ts once built; 0 for an index
/// made with its table, which no row precedes.
pub const IndexDef = struct {
    base_table_id: u32,
    index_table_id: u32,
    column_id: u32,
    unique: bool,
    valid_ts: u64 = 0,

    /// The `valid_ts` of an index whose backfill has not finished: no
    /// read_ts reaches it, so no statement reads the index, while every
    /// writer maintains it.
    pub const building: u64 = std.math.maxInt(u64);
};

/// Build the index catalog row cells (column_id order) for `def`. Pass
/// `&result` to marshalRow with the index catalog's compiled PhysicalSchema.
pub fn encodeIndex(def: IndexDef) [5]Value {
    return .{
        .{ .uint4 = def.base_table_id },
        .{ .uint4 = def.index_table_id },
        .{ .uint4 = def.column_id },
        .{ .bool_ = def.unique },
        .{ .uint8 = def.valid_ts },
    };
}

/// Decode a fully-read index catalog row (five cells, column_id order).
pub fn decodeIndex(r: *const Row) Error!IndexDef {
    if (r.len() != 5) return error.CorruptCatalogRow;
    if (r.at(0) != .uint4 or r.at(1) != .uint4 or
        r.at(2) != .uint4 or r.at(3) != .bool_ or r.at(4) != .uint8)
        return error.CorruptCatalogRow;
    return .{
        .base_table_id = r.at(0).uint4,
        .index_table_id = r.at(1).uint4,
        .column_id = r.at(2).uint4,
        .unique = r.at(3).bool_,
        .valid_ts = r.at(4).uint8,
    };
}

/// Index catalog row key: (base_table_id, index_table_id) big-endian, so a
/// base_table_id prefix scan walks a table's indexes in index_table_id order.
/// Same (u32, u32) BE shape as the column catalog's key — delegated to
/// encodeKey so the byte layout has one owner.
/// (Distinct from key.encodeIndexKey, which encodes an index ENTRY's value.)
pub fn encodeIndexKey(base_table_id: u32, index_tid: u32) [8]u8 {
    return encodeKey(base_table_id, index_tid);
}

/// The IndexDef an index catalog row KEY alone determines: base_table_id
/// and index_table_id. The value-borne fields (column_id, unique) are
/// zeroed — decodeIndex reads those. Null if `primary` isn't index-key
/// shaped (8 bytes).
pub fn decodeIndexKey(primary: String) ?IndexDef {
    const k = decodeKey(primary) catch return null;
    return .{
        .base_table_id = k.table_id,
        .index_table_id = k.column_id,
        .column_id = 0,
        .unique = false,
    };
}
