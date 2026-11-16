// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! DDL execution: CREATE/DROP DATABASE and TABLE. Each is one manifest
//! change replicated through raft: the trees it makes or drops, the
//! names it records or forgets, and the catalog rows it writes, all in
//! one entry — the data plane proper lives in dml.zig, and CREATE/DROP
//! INDEX with the online backfill in index_ddl.zig.
//!
//! These take `*Handler` rather than being its methods: Zig keeps a
//! struct's methods in one file, and the handler's DDL half is its own
//! concern from its session/txn half.

const std = @import("std");
const engine = @import("engine");
const pgwire = @import("pgwire");
const zdb = @import("zdb");
const zen = zdb.buffers;

const _catalog = @import("catalog.zig");
const _parse = @import("parse.zig");
const _key = @import("key.zig");
const _schema = @import("schema.zig");
const _table = @import("table.zig");
const _value = @import("value.zig");
const _server = @import("server.zig");

const Handler = _server.Handler;
const String = _value.String;
const log = std.log.scoped(.sql);
const replyError = _server.replyError;
const replyErrorCode = _server.replyErrorCode;
const replyLockErr = _server.replyLockErr;
const default_database = _server.default_database;

/// With a source (TEMPLATE), the new database is a branch of it: its
/// trees share the source's SST files and get the source's unflushed
/// versions, all as of the entry's commit. Both then take writes on
/// their own. Taken under ddl_mu like every DDL, so the source's
/// catalog is not mid-change (a CREATE INDEX backfill, say) when it
/// is copied.
pub fn createDatabase(
    self: *Handler,
    state: *pgwire.ConnState,
    cd: _parse.CreateDatabase,
) !void {
    const eng = self.srv.engine;
    const name = cd.name;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    var src: ?u64 = null;
    if (cd.source) |src_name| {
        src = eng.forest.ground.manifest.lookupNamespace(src_name) orelse
            return replyErrorCode(
                state,
                pgwire.sqlstate.invalid_catalog_name,
                "template database does not exist",
            );
    }
    const built = if (src) |s|
        eng.forest.buildNsBranch(arena.allocator(), s, name)
    else
        eng.forest.buildNsCreate(arena.allocator(), .next, name);
    const created = built catch |err| switch (err) {
        error.NameExists => return replyErrorCode(
            state,
            pgwire.sqlstate.duplicate_database,
            "database already exists",
        ),
        else => |e| {
            log.err("create database '{s}': {s}", .{ name, @errorName(e) });
            return replyError(state, "create database failed");
        },
    };
    eng.proposeMC(created.change) catch |err| {
        log.err("create database '{s}': propose: {s}", .{ name, @errorName(err) });
        return replyError(state, "create database proposal failed");
    };
    log.info("created database '{s}' -> ns={d}", .{ name, created.namespace });
    try state.sendCommandComplete("CREATE DATABASE");
}

pub fn dropDatabase(self: *Handler, state: *pgwire.ConnState, name: String) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    if (std.mem.eql(u8, name, default_database))
        return replyError(state, "cannot drop the default database");
    const ns = eng.forest.ground.manifest.lookupNamespace(name) orelse
        return replyErrorCode(
            state,
            pgwire.sqlstate.invalid_catalog_name,
            "database does not exist",
        );

    // Set `dropped` first, then take and release every table lock
    // exclusive, ONE AT A TIME: each take waits for the statements
    // and open txns holding that table shared, and a statement that
    // takes a table after the flag answers 3D000 (dmlTxn checks
    // after its take), so once the pass is over no txn that touched
    // a table before the flag is still open. Holding one table lock at
    // a time keeps this out of any lock cycle: a DML txn parked
    // behind us waits only for that table's holders to end. A
    // failed propose clears the flag. Parking under ddl_mu is safe
    // (global order ddl_mu → table lock, DML never takes ddl_mu).
    const db = self.srv.database(ns) catch
        return replyError(state, "internal error: database state");
    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    db.dropped.store(true, .release);
    const table_locks = db.allTableLocks(arena.allocator()) catch {
        db.dropped.store(false, .release);
        return replyError(state, "internal error: lock state");
    };
    for (table_locks) |m| {
        m.lock(@src());
        m.unlock();
    }

    eng.dropNamespace(ns) catch |err| {
        log.err("drop database '{s}': {s}", .{ name, @errorName(err) });
        db.dropped.store(false, .release);
        return replyError(state, "drop database failed");
    };
    log.info("dropped database '{s}' (ns={d})", .{ name, ns });
    try state.sendCommandComplete("DROP DATABASE");
}

pub fn createTable(self: *Handler, state: *pgwire.ConnState, ct: _parse.CreateTable) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    const ns = try self.sessionNamespace(state) orelse return;
    if (eng.forest.ground.manifest.lookupTable(ns, ct.name) != null) {
        if (ct.if_not_exists) return state.sendCommandComplete("CREATE TABLE");
        return replyErrorCode(state, pgwire.sqlstate.duplicate_table, "table already exists");
    }

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Mint the column ids ONCE: 1-based declaration position. Everything
    // downstream — the index descriptors and the catalog rows — reads
    // ids from this list, never re-derives them from position. The
    // PRIMARY KEY column (parse allows at most one) and every UNIQUE
    // column get an auto unique index each, in their own trees named
    // "<table>_pkey" and "<table>_<column>_key" (Postgres convention),
    // all created with the table in one atomic change; a name
    // collision on any of them fails the CREATE.
    const columns = try a.alloc(_schema.LogicalColumn, ct.columns.len);
    // The base table, then a unique index per primary key or UNIQUE
    // column.
    var specs: std.ArrayList(zdb.forest.Forest.TableSpec) = .empty;
    try specs.append(a, .{ .name = ct.name, .kind = .rows });

    var unique_cols: std.ArrayList(u32) = .empty;
    for (ct.columns, columns, 1..) |c, *lc, id| {
        lc.* = .{
            .column_id = @intCast(id),
            .name = c.name,
            .ty = c.ty,
            .pkey_position = if (c.pkey) 1 else null,
            .type_mod = c.type_mod,
            .not_null = c.not_null,
        };
        if (!c.pkey and !c.unique) continue;
        // The same rule as CREATE INDEX: no index of any kind on an
        // interval column (it has no memcomparable encoding), so DML
        // never meets an unencodable indexed cell.
        if (c.ty == .interval)
            return replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "indexes on interval columns are not supported",
            );
        try unique_cols.append(a, lc.column_id);
        const index_name = try _table.indexName(
            a,
            ct.name,
            c.name,
            if (c.pkey) .pkey else .unique,
        );
        try specs.append(a, .{ .name = index_name, .kind = .unique_index });
    }

    var created = eng.forest.buildTables(
        a,
        ns,
        specs.items,
        .create_tree,
    ) catch |err| switch (err) {
        error.NameExists => return replyErrorCode(state, pgwire.sqlstate.duplicate_table, "table or index name already exists"),
        else => |e| {
            log.err("create table '{s}': {s}", .{ ct.name, @errorName(e) });
            return replyError(state, "create table failed");
        },
    };
    // The column rows and the index descriptor rows ride the change
    // and apply before its names, so no statement finds the table
    // without its columns. An index made with its table is valid from
    // ts 0: no row precedes it.
    const table_id = created.table_ids[0];
    const indexes = try a.alloc(_catalog.IndexDef, unique_cols.items.len);
    for (unique_cols.items, created.table_ids[1..], indexes) |col, tid, *def| {
        def.* = .{
            .base_table_id = table_id,
            .index_table_id = tid,
            .column_id = col,
            .unique = true,
        };
    }
    created.change.mutations = .{
        .namespace = ns,
        .kvs = try catalogRows(self, a, .{
            .namespace = ns,
            .table_id = table_id,
        }, columns, indexes),
    };
    eng.proposeMC(created.change) catch |err| {
        log.err("create table '{s}': propose: {s}", .{
            ct.name, @errorName(err),
        });
        return replyError(state, "create table proposal failed");
    };

    log.info(
        "created table '{s}' -> ns={d} table_id={d} columns={d} unique={d}",
        .{ ct.name, ns, table_id, ct.columns.len, indexes.len },
    );
    try state.sendCommandComplete("CREATE TABLE");
}

/// CREATE TABLE `name` AS TABLE `source`: the new table is a branch of
/// the source, its rows and its indexes as of the entry's commit, the
/// SST files shared and the memtable versions copied
/// (zdb.forest_ddl.buildTables), then the source's catalog rows
/// written again under the new ids. Postgres copies the rows and not
/// the indexes; here the indexes come too, under the generated names
/// (`name_pkey`, `name_col_key`, `name_col_idx`). WITH NO DATA makes
/// the same tables empty. Under ddl_mu like every DDL, so the source's
/// catalog is not mid-change when it is copied; an index still marked
/// building is a crashed CREATE INDEX, refused rather than branched
/// incomplete.
pub fn createTableAs(
    self: *Handler,
    state: *pgwire.ConnState,
    cta: _parse.CreateTableAs,
) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    const ns = try self.sessionNamespace(state) orelse return;
    if (eng.forest.ground.manifest.lookupTable(ns, cta.name) != null) {
        if (cta.if_not_exists)
            return state.sendCommandComplete("CREATE TABLE AS");
        return replyErrorCode(
            state,
            pgwire.sqlstate.duplicate_table,
            "table already exists",
        );
    }
    const src_tid = eng.forest.ground.manifest.lookupTable(
        ns,
        cta.source,
    ) orelse
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_table,
            "source table does not exist",
        );
    const src_key: zdb.kv.TableKey = .{ .namespace = ns, .table_id = src_tid };
    const src = self.srv.openTable(src_key) catch |err| {
        log.err("create table as '{s}': load schema: {s}", .{
            cta.name, @errorName(err),
        });
        return replyError(state, "failed to load source table schema");
    };

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const arloc = arena.allocator();

    // Every column row of the source, dropped ones included: the rows
    // in the shared files were laid out by that history.
    const columns = _table.loadColumnRows(
        arloc,
        eng.forest,
        &self.srv.catalogs,
        src_key,
    ) catch |err| {
        log.err("create table as '{s}': catalog rows: {s}", .{
            cta.name, @errorName(err),
        });
        return replyError(state, "failed to load source table schema");
    };

    // The new tables: the base, then one per index, each a branch of
    // its source table (or, WITH NO DATA, an empty tree of its kind).
    // Index specs pair with `src_indexes` by position, for their
    // catalog rows below.
    var specs: std.ArrayList(zdb.forest.Forest.TableSpec) = .empty;
    var src_indexes: std.ArrayList(_table.Index) = .empty;
    try specs.append(arloc, .{
        .name = cta.name,
        .kind = .rows,
        .from_table_id = src_tid,
    });

    for (src.indexes) |idx| {
        if (idx.isBuilding())
            return replyErrorCode(
                state,
                pgwire.sqlstate.object_in_use,
                "source table has an index still being built",
            );
        try specs.append(arloc, .{
            .name = try idx.nameFor(arloc, cta.name),
            .kind = idx.treeKind(),
            .from_table_id = idx.index_table_id,
        });
        try src_indexes.append(arloc, idx);
    }

    const built = eng.forest.buildTables(
        arloc,
        ns,
        specs.items,
        if (cta.with_data) .branch_table else .create_tree,
    );
    var created = built catch |err| switch (err) {
        error.NameExists => return replyErrorCode(
            state,
            pgwire.sqlstate.duplicate_table,
            "table or index name already exists",
        ),
        else => |e| {
            log.err("create table as '{s}': {s}", .{ cta.name, @errorName(e) });
            return replyError(state, "create table failed");
        },
    };
    // The catalog rows under the new ids ride the change, as in
    // createTable. The column ids stay the source's, so the index
    // descriptors and the shared rows keep naming the same columns. A
    // branched index is valid from when its source was; an empty one
    // from the start.
    const table_id = created.table_ids[0];
    const indexes = try arloc.alloc(_catalog.IndexDef, src_indexes.items.len);
    for (src_indexes.items, created.table_ids[1..], indexes) |idx, tid, *def| {
        def.* = .{
            .base_table_id = table_id,
            .index_table_id = tid,
            .column_id = idx.column.column_id,
            .unique = idx.unique,
            .valid_ts = if (cta.with_data) idx.valid_ts else 0,
        };
    }
    created.change.mutations = .{
        .namespace = ns,
        .kvs = try catalogRows(self, arloc, .{
            .namespace = ns,
            .table_id = table_id,
        }, columns, indexes),
    };
    eng.proposeMC(created.change) catch |err| {
        log.err("create table as '{s}': propose: {s}", .{
            cta.name, @errorName(err),
        });
        return replyError(state, "create table proposal failed");
    };

    log.info(
        "created table '{s}' as table '{s}' -> ns={d} table_id={d} " ++
            "indexes={d} data={}",
        .{ cta.name, cta.source, ns, table_id, indexes.len, cta.with_data },
    );
    try state.sendCommandComplete("CREATE TABLE AS");
}

/// Marshal one catalog row per column plus one per index descriptor, in
/// `a`: the rows a DDL's change carries (`ManifestChange.mutations`), or
/// an index's descriptor turning valid. Column rows land in table 1,
/// index rows in table 2. `columns` carry their minted ids (createTable
/// assigns them); nothing is derived here.
pub fn catalogRows(
    self: *Handler,
    a: std.mem.Allocator,
    table_key: zdb.kv.TableKey,
    columns: []const _schema.LogicalColumn,
    indexes: []const _catalog.IndexDef,
) ![]zen.KeyValueStruct {
    const eng = self.srv.engine;
    const n = columns.len + indexes.len;
    const kvs = try a.alloc(zen.KeyValueStruct, n);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(eng.allocator);
    const catalogs = &self.srv.catalogs;
    for (columns, 0..) |lc, i| {
        const bytes = try catalogs.columnRow(
            eng.allocator,
            table_key.table_id,
            lc,
            &buf,
        );
        // The row is a view of `buf`, which the next one reuses.
        kvs[i] = .{
            .key = try _table.columnRowKey(a, table_key, lc.column_id),
            .value = try a.dupe(u8, bytes),
        };
    }
    for (indexes, columns.len..) |idx, i| {
        const bytes = try catalogs.indexRow(eng.allocator, idx, &buf);
        const base: zdb.kv.TableKey = .{
            .namespace = table_key.namespace,
            .table_id = idx.base_table_id,
        };
        kvs[i] = .{
            .key = try _table.indexRowKey(a, base, idx.index_table_id),
            .value = try a.dupe(u8, bytes),
        };
    }
    return kvs;
}

/// Tombstones for a dropped table's catalog rows, in `a`: its column
/// rows (table 1) and its index descriptors (table 2). A table with no
/// column rows yields only the descriptors.
pub fn catalogTombstones(
    self: *Handler,
    a: std.mem.Allocator,
    ns: u64,
    table_id: u32,
    index_tids: []const u32,
) ![]zen.KeyValueStruct {
    const table: zdb.kv.TableKey = .{ .namespace = ns, .table_id = table_id };
    const columns = _table.loadColumnRows(
        a,
        self.srv.engine.forest,
        &self.srv.catalogs,
        table,
    ) catch |err| switch (err) {
        error.UndefinedTable => &.{},
        else => return err,
    };
    const deleted = (zdb.kv.Meta{ .kind = .deleted }).toByte();
    var kvs: std.ArrayList(zen.KeyValueStruct) = .empty;
    for (columns) |lc| try kvs.append(a, .{
        .key = try _table.columnRowKey(a, table, lc.column_id),
        .meta = deleted,
    });
    for (index_tids) |index_tid| try kvs.append(a, .{
        .key = try _table.indexRowKey(a, table, index_tid),
        .meta = deleted,
    });
    return kvs.items;
}

pub fn dropTable(self: *Handler, state: *pgwire.ConnState, dt: _parse.DropTable) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.ground.manifest.lookupTable(
        ns,
        dt.name,
    ) orelse {
        if (dt.missing_ok) return state.sendCommandComplete("DROP TABLE");
        return replyErrorCode(state, pgwire.sqlstate.undefined_table, "table does not exist");
    };

    var arena = std.heap.ArenaAllocator.init(eng.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // One key-only catalog scan answers both DROP questions: is this
    // name an index (reject), and which index trees belong to it (drop
    // with the base below). Safe pre-lock: ddl_mu serializes every
    // catalog mutation; the table locks only fence in-flight DML.
    var scan = _table.scanIndexKeys(a, eng.forest, .{
        .namespace = ns,
        .table_id = table_id,
    }) catch |err| {
        log.err("drop table '{s}': index scan: {s}", .{ dt.name, @errorName(err) });
        return replyError(state, "drop table failed");
    };
    if (scan.is_index)
        return replyErrorCode(state, pgwire.sqlstate.wrong_object_type, "cannot DROP TABLE an index; use DROP INDEX");

    // Exclusive table lock: queue behind every open txn's shared
    // hold so the drop never lands mid-transaction. Writer preference
    // parks new DML behind the pending lock. A concurrent DROP
    // DATABASE is serialized by ddl_mu. Safe to park while holding
    // ddl_mu: the global order is ddl_mu → table lock (DML never takes
    // ddl_mu), so no cycle can involve both.
    const db = self.srv.database(ns) catch
        return replyError(state, "internal error: database state");
    const table_lock = db.tableLock(table_id) catch
        return replyError(state, "internal error: lock state");
    table_lock.lock(@src());
    defer table_lock.unlock();

    // The table and its index tables drop together, in one atomic
    // change, their names and their catalog rows with them.
    const tombstones = catalogTombstones(
        self,
        a,
        ns,
        table_id,
        scan.table_ids.items,
    ) catch |err| {
        log.err("drop table '{s}': catalog rows: {s}", .{
            dt.name, @errorName(err),
        });
        return replyError(state, "drop table failed");
    };
    try scan.table_ids.append(a, table_id);
    eng.dropTables(ns, scan.table_ids.items, tombstones) catch |err| {
        log.err("drop table '{s}': {s}", .{ dt.name, @errorName(err) });
        return replyError(state, "drop table failed");
    };
    // Evict the cached Table (safe: we hold the exclusive table
    // table lock, and DML holds its Table only under the shared one).
    // Index table_ids are never cached — they have no columns.
    try self.srv.forgetTable(.{ .namespace = ns, .table_id = table_id });

    log.info("dropped table '{s}' (ns={d} table_id={d} indexes={d})", .{ dt.name, ns, table_id, scan.table_ids.items.len - 1 });
    try state.sendCommandComplete("DROP TABLE");
}
