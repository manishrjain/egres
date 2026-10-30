// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Reconstructing a user table's schema from its catalog rows.
//!
//! CREATE TABLE writes one catalog row per column (server.writeCatalogRows);
//! this reads them back so INSERT/SELECT can encode/decode the table's rows.
//! The catalog tree (catalog.table_id) holds every table's columns keyed
//! (table_id, column_id) big-endian, so the 4-byte table_id prefix scans
//! exactly one table's columns in column_id order — what schema.compile
//! consumes. The result owns its column names + compiled layout in one arena;
//! callers `deinit` it once done.

const std = @import("std");
const zdb = @import("zdb");

const _value = @import("value.zig");
const String = _value.String;
const _schema = @import("schema.zig");
const _row = @import("row.zig");
const _key = @import("key.zig");
const _catalog = @import("catalog.zig");

const Forest = zdb.forest.Forest;
const Key = zdb.kv.Key;
const TableKey = zdb.kv.TableKey;
const KeyRange = zdb.kv.KeyRange;
const Item = zdb.kv.Item;

pub const Error = error{UndefinedTable};

// The SQL layer's catalog table ids and the engine's (which provisions the
// trees at ns-create) must agree — drift would silently drop catalog rows
// (buildFlush skips keys whose table has no tree). catalog.zig deliberately
// imports no zdb, so the tie-down lives here.
comptime {
    std.debug.assert(_catalog.table_id == zdb.manifest.catalog_table_id);
    std.debug.assert(_catalog.index_table_id ==
        zdb.manifest.index_catalog_table_id);
}

/// What an index is on its column, for its generated name.
pub const IndexShape = enum { pkey, unique, plain };

/// Postgres's generated name for an index on `table`(`column`):
/// `<t>_pkey` for the primary key, `<t>_<c>_key` for a unique column,
/// `<t>_<c>_idx` otherwise.
pub fn indexName(
    alloc: std.mem.Allocator,
    table: String,
    column: String,
    shape: IndexShape,
) ![]u8 {
    return switch (shape) {
        .pkey => std.fmt.allocPrint(alloc, "{s}_pkey", .{table}),
        .unique => std.fmt.allocPrint(alloc, "{s}_{s}_key", .{ table, column }),
        .plain => std.fmt.allocPrint(alloc, "{s}_{s}_idx", .{ table, column }),
    };
}

/// One live column of a loaded table. `name` is owned by the Table's arena.
/// Statements hold a `*const Column` (into `Table.columns`, stable for the
/// Table's life) and never a bare position or column_id: `column_id` is
/// the catalog's stable identity, what index rows name; `pos` is where
/// the column sits at runtime.
pub const Column = struct {
    column_id: u32,
    /// Slot in `columns`, in `ps.columns`, in a row decoded with the empty
    /// projection (row.unmarshalRow / parseCol), and in INSERT's values —
    /// all four are the live columns in column_id order. Minted once, in
    /// loadTableSchema, where the parallel-arrays invariant is asserted.
    pos: usize,
    name: String,
    ty: _value.Type,
    pkey_position: ?u16,
    /// `char(n)` / `varchar(n)` length or `vector(n)` dimension; null otherwise.
    type_mod: ?u32 = null,

    /// The row-key pseudo-column (`egid_column`).
    pub fn isEgid(self: *const Column) bool {
        return self.column_id == egid_column.column_id;
    }
};

/// The row-key pseudo-column: column_id 0 marks it (user ids start at
/// 1). Its value is the row's egid, read from the KEY, not the row
/// bytes, so it has no position — `pos` is maxInt, so any use as an
/// index faults instead of silently reading column 0. It is not in
/// `Table.columns` — SELECT * and positional INSERT never see it — but
/// `Table.column` resolves it by name, so it can be projected and
/// filtered on; INSERT and UPDATE refuse it.
pub const egid_column = Column{
    .column_id = 0,
    .pos = std.math.maxInt(usize),
    .name = "egid",
    // Egids are u64; uint8 rides the wire as numeric — the one pg type
    // that holds the full range (value.pgTypeOid).
    .ty = .uint8,
    .pkey_position = null,
};

/// One index of a loaded table, its catalog row's column_id resolved to
/// the live `Column` it keys on — the one id → column resolution. DML
/// and the planner compare and read through the pointer.
pub const Index = struct {
    index_table_id: u32,
    unique: bool,
    /// The timestamp from which the index is complete (catalog.IndexDef).
    /// A statement reads the index only when its read_ts is at or above
    /// it; a writer bumps its read_ts to it before probing.
    valid_ts: u64,
    column: *const Column,

    /// Whether the backfill is still running: no statement reads the
    /// index, and no writer bumps to its `valid_ts`.
    pub inline fn isBuilding(self: *const Index) bool {
        return self.valid_ts == _catalog.IndexDef.building;
    }

    /// The tree kind an index of this shape holds.
    pub fn treeKind(self: *const Index) zdb.manifest.TreeKind {
        return if (self.unique) .unique_index else .posting_index;
    }

    /// `indexName` for this index, were it on `table`.
    pub fn nameFor(
        self: *const Index,
        alloc: std.mem.Allocator,
        table: String,
    ) ![]u8 {
        const shape: IndexShape = if (!self.unique)
            .plain
        else if (self.column.pkey_position != null)
            .pkey
        else
            .unique;
        return indexName(alloc, table, self.column.name, shape);
    }

    /// The index keys one cell contributes: none for NULL under a
    /// unique index (NULLS DISTINCT), else exactly one — the cell's
    /// memcomparable encoding; NULL under a non-unique index keys on
    /// the tag_null byte. A slice rather than one key on purpose: a
    /// later index kind (trigram, full text, a cell covering) returns
    /// many from this same call, and every maintenance loop already
    /// runs over the slice. Unencodable is unreachable: DDL refuses
    /// interval columns for every index.
    pub fn keys(
        self: *const Index,
        arloc: std.mem.Allocator,
        cell: _value.Value,
    ) error{OutOfMemory}![]const _value.String {
        if (self.unique and cell == .null_value) return &.{};
        const one = try arloc.alloc(_value.String, 1);
        one[0] = _key.encodeIndexKey(arloc, cell, null) catch |err|
            switch (err) {
                error.Unencodable => unreachable,
                error.OutOfMemory => return error.OutOfMemory,
            };
        return one;
    }
};

/// A user table as the SQL layer sees it: its live columns (logical schema)
/// and compiled byte layout (physical schema) for encoding/decoding rows, plus
/// the next row id to hand out. `loadTableSchema` fills the schema; `next_egid`
/// is runtime state (0 out of the load). Owns one arena — all of `columns`,
/// their names, `indexes`, and `ps` live in it, so `deinit` frees everything.
pub const Table = struct {
    /// The user columns — what SELECT * and positional INSERT walk.
    columns: []Column,
    /// Every index of this table, columns resolved. The PRIMARY KEY's
    /// is the unique one on the column with `pkey_position` set; DML
    /// treats it like any other unique index.
    indexes: []Index,
    ps: _schema.PhysicalSchema,
    version: u16,
    arena: std.heap.ArenaAllocator,
    /// Next egid (row id) to hand out. Maintained on EVERY node by the apply
    /// observer as `max(next_egid, committed_egid + 1)`, so it self-heals
    /// across leader changes without a term; seeded from forest.maxKey on
    /// cache-load (snapshot rows the observer never saw); advanced by the
    /// leader on hand-out. Not from the catalog. See sql.Server.
    ///
    /// Atomic so hand-out (fetchAdd) and the apply observer (fetchMax)
    /// need no lock: the counter carries no other data, so monotonic
    /// ordering is enough. The Table pointer itself stays valid under
    /// the table lock (see sql.Server.forgetTable).
    next_egid: std.atomic.Value(u64) = .init(0),

    /// Raise `next_egid` to at least `floor`: the apply observer's
    /// `max(next_egid, committed_egid + 1)`, and the reload merge.
    pub fn raiseNextEgid(self: *Table, floor: u64) void {
        _ = self.next_egid.fetchMax(floor, .monotonic);
    }

    pub fn deinit(self: *Table) void {
        self.arena.deinit();
    }

    /// The live column with this name (exact match — libpg_query
    /// lowercases unquoted identifiers, matching how names are stored),
    /// the egid pseudo-column, or null. Linear scan (columns is small).
    pub fn column(self: *const Table, name: String) ?*const Column {
        if (std.mem.eql(u8, name, egid_column.name)) return &egid_column;
        for (self.columns) |*c| {
            if (std.mem.eql(u8, c.name, name)) return c;
        }
        return null;
    }

    /// One column's value out of a stored row: the egid from the key,
    /// any other cell by a null-bit test plus one O(1) read
    /// (row.parseCol) — no full-row decode.
    pub fn parseCol(self: *const Table, item: Item, col: *const Column) _value.Value {
        if (col.isEgid()) return .{ .uint8 = _key.decodeEgid(item.key.primary).? };
        const value = item.newest().value;
        return _row.parseCol(&self.ps, value, self.ps.columns[col.pos]);
    }
};

/// The scan window selecting every catalog row owned by `owner_id` in
/// the catalog table `table_key`. Keys are (owner_id, member_id) BE pairs and
/// member ids are 1-based, so the window is [owner.0, (owner+1).0) — an
/// exclusive right edge no real key equals — with the upper bound dropped
/// at owner_id maxInt. The struct owns the boundary key bytes: keep it
/// alive (and in place) for the whole scan, since range() borrows them.
const CatalogWindow = struct {
    table_key: TableKey,
    has_upper: bool,
    left: [8]u8,
    right: [8]u8,

    fn init(table_key: TableKey, owner_id: u32) CatalogWindow {
        return .{
            .table_key = table_key,
            .has_upper = owner_id != std.math.maxInt(u32),
            .left = _catalog.encodeKey(owner_id, 0),
            .right = _catalog.encodeKey(owner_id +% 1, 0),
        };
    }

    fn range(self: *const CatalogWindow) KeyRange {
        return .{
            .left = .{
                .namespace = self.table_key.namespace,
                .table_id = self.table_key.table_id,
                .primary = &self.left,
            },
            .right = if (self.has_upper) .{
                .namespace = self.table_key.namespace,
                .table_id = self.table_key.table_id,
                .primary = &self.right,
            } else Key.inf,
        };
    }
};

/// Every column row of `table_key` in its namespace's catalog, live or
/// dropped, ascending by column_id, in `arloc`. The full history is
/// what a layout compiles from, and what a branch of the table copies.
/// Returns `error.UndefinedTable` if the table has no catalog rows.
pub fn loadColumnRows(
    arloc: std.mem.Allocator,
    forest: *Forest,
    catalog_ps: *const _schema.PhysicalSchema,
    table_key: TableKey,
) ![]_schema.LogicalColumn {
    // `win` owns the boundary key bytes, so it lives for the whole
    // walk below.
    const win = CatalogWindow.init(.{
        .namespace = table_key.namespace,
        .table_id = _catalog.table_id,
    }, table_key.table_id);
    var cur = try forest.openCursor(arloc, win.table_key, win.range(), .{});
    defer cur.deinit();

    var all: std.ArrayList(_schema.LogicalColumn) = .empty;
    var scratch: _value.Row = .{}; // reused across catalog rows
    while (try cur.next()) |item| {
        // Decode all seven catalog cells (column_id order = SELECT *).
        // TODO: How do we know the version of the row, and whether the
        // physical schema corresponds to that version?
        const val = item.newest().value;
        try _row.unmarshalRow(arloc, catalog_ps, val, &.{}, &scratch);
        var lc = (try _catalog.decodeColumn(&scratch)).column;
        // The name borrows the row buffer, which the next item
        // overwrites.
        lc.name = try arloc.dupe(u8, lc.name);
        try all.append(arloc, lc);
    }
    if (all.items.len == 0) return error.UndefinedTable;
    // The walk already yields column_id order; sort defends against any
    // future reordering.
    std.mem.sort(_schema.LogicalColumn, all.items, {}, lessByColumnId);
    return all.items;
}

/// Load `table_key`'s schema from its namespace's catalog. `catalog_ps` is
/// the caller's compiled catalog layout (Server.catalog_ps). Returns
/// `error.UndefinedTable` if the table has no catalog rows.
pub fn loadTableSchema(
    alloc: std.mem.Allocator,
    forest: *Forest,
    catalog_ps: *const _schema.PhysicalSchema,
    index_catalog_ps: *const _schema.PhysicalSchema,
    table_key: TableKey,
) !Table {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();

    const all = try loadColumnRows(a, forest, catalog_ps, table_key);
    // compile() derives the layout from the full history; reconstruct()
    // derives current_version across all columns.
    const logical = _catalog.reconstruct(all);
    const ps = try _schema.compile(a, logical, logical.current_version);

    // Expose only the columns live at the current version, in ps order:
    // `pos` is the slot in both, asserted here where it is minted.
    var cols: std.ArrayList(Column) = .empty;
    for (all) |lc| {
        if (!lc.liveAt(logical.current_version)) continue;
        const pos = cols.items.len;
        std.debug.assert(ps.columns[pos].column_id == lc.column_id);
        try cols.append(a, .{
            .column_id = lc.column_id,
            .pos = pos,
            .name = lc.name,
            .ty = lc.ty,
            .pkey_position = lc.pkey_position,
            .type_mod = lc.type_mod,
        });
    }
    const columns = try cols.toOwnedSlice(a);

    // Indexes (table 2), each resolved to the live column it keys on —
    // an index naming a column the table doesn't have is corrupt
    // catalog state. The pointers target the arena-owned `columns`
    // slice, so they survive the Table being moved into the cache.
    const defs = try loadIndexes(a, forest, index_catalog_ps, table_key);
    const indexes = try a.alloc(Index, defs.len);
    for (defs, indexes) |def, *idx| {
        const col = for (columns) |*c| {
            if (c.column_id == def.column_id) break c;
        } else return error.CorruptCatalogRow;
        idx.* = .{
            .index_table_id = def.index_table_id,
            .unique = def.unique,
            .valid_ts = def.valid_ts,
            .column = col,
        };
    }

    return .{
        .columns = columns,
        .indexes = indexes,
        .ps = ps,
        .version = logical.current_version,
        .arena = arena,
    };
}

/// Load every index of the base table `table_key` from its namespace's
/// index catalog (table 2), prefix-scanned by its table_id (empty if
/// none). The returned
/// slice and its contents are allocated with `alloc`.
pub fn loadIndexes(
    alloc: std.mem.Allocator,
    forest: *Forest,
    index_catalog_ps: *const _schema.PhysicalSchema,
    table_key: TableKey,
) ![]_catalog.IndexDef {
    const win = CatalogWindow.init(.{
        .namespace = table_key.namespace,
        .table_id = _catalog.index_table_id,
    }, table_key.table_id);
    var cur = try forest.openCursor(alloc, win.table_key, win.range(), .{});
    defer cur.deinit();

    var out: std.ArrayList(_catalog.IndexDef) = .empty;
    errdefer out.deinit(alloc);
    var scratch: _value.Row = .{}; // reused across index rows
    defer scratch.deinit(alloc);
    while (try cur.next()) |item| {
        const value = item.newest().value;
        try _row.unmarshalRow(alloc, index_catalog_ps, value, &.{}, &scratch);
        try out.append(alloc, try _catalog.decodeIndex(&scratch));
    }
    return out.toOwnedSlice(alloc);
}

/// What one key-only pass over the index catalog says about a table_id:
/// it is itself an index (`base_table_id` then names its base and
/// `table_ids` is meaningless — the scan stops early), or it is a base
/// table with these index table_ids (empty if none). DROP TABLE rejects
/// the former (dropping an index tree out from under its base would
/// strand the base's PK; Postgres says use DROP INDEX), then appends
/// the base to `tids` and drops the lot together; DROP INDEX requires
/// it and uses `base_table_id` to find the IndexDef.
pub const IndexScan = struct {
    is_index: bool,
    base_table_id: ?u32,
    table_ids: std.ArrayList(u32),
};

/// One full scan of `table_key`'s namespace's small index catalog,
/// classifying `table_key`'s table per IndexScan. Key-only: no row
/// decode, no schema needed.
/// `tids` is allocated with `alloc`.
pub fn scanIndexKeys(
    alloc: std.mem.Allocator,
    forest: *Forest,
    table_key: TableKey,
) !IndexScan {
    const target = table_key.table_id;
    var out: IndexScan = .{
        .is_index = false,
        .base_table_id = null,
        .table_ids = .empty,
    };
    errdefer out.table_ids.deinit(alloc);
    const index_key = TableKey{
        .namespace = table_key.namespace,
        .table_id = _catalog.index_table_id,
    };
    var cur = try forest.openCursor(alloc, index_key, .all, .{});
    defer cur.deinit();
    // Row keys are (base_table_id, index_table_id): both answers come
    // from key bytes alone, no value decode.
    while (try cur.next()) |item| {
        const def = _catalog.decodeIndexKey(item.key.primary) orelse
            return error.CorruptCatalogKey;
        if (def.index_table_id == target) {
            out.is_index = true;
            out.base_table_id = def.base_table_id;
            break; // an index has no indexes; nothing more to learn
        }
        if (def.base_table_id == target)
            try out.table_ids.append(alloc, def.index_table_id);
    }
    return out;
}

fn lessByColumnId(_: void, x: _schema.LogicalColumn, y: _schema.LogicalColumn) bool {
    return x.column_id < y.column_id;
}
