// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! DDL execution: CREATE/DROP DATABASE and TABLE. Each builds one
//! manifest change (name reservation, tree creation) that carries its
//! catalog rows, replicated through raft as one entry — the data plane
//! proper lives in dml.zig, and CREATE/DROP INDEX with the online
//! backfill in index_ddl.zig.
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
const _names = @import("names.zig");
const _row = @import("row.zig");
const _schema = @import("schema.zig");
const _table = @import("table.zig");
const _value = @import("value.zig");
const _server = @import("server.zig");

const Handler = _server.Handler;
const String = _value.String;
const log = std.log.scoped(.sql);
const replyError = _server.replyError;
const replyErrorCode = _server.replyErrorCode;
const replyNameReadFailed = _server.replyNameReadFailed;
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
    const read_ts = eng.node.appliedTs();
    const sys_ns = zdb.manifest.system_namespace;
    _names.checkNew(eng.forest, read_ts, sys_ns, &.{name}) catch |err|
        switch (err) {
            error.NameExists => return replyError(
                state,
                "database already exists",
            ),
            else => {
                log.err("create database '{s}': name check: {s}", .{
                    name, @errorName(err),
                });
                return replyError(state, "create database failed");
            },
        };
    var src: ?u64 = null;
    if (cd.source) |src_name| {
        const found = _names.namespaceId(
            eng.forest,
            read_ts,
            src_name,
        ) catch |err| return replyNameReadFailed(state, src_name, err);
        src = found orelse
            return replyErrorCode(
                state,
                pgwire.sqlstate.invalid_catalog_name,
                "template database does not exist",
            );
    }
    const built = if (src) |s|
        eng.forest.buildNsBranch(arena.allocator(), s)
    else
        eng.forest.buildNsCreate(arena.allocator(), .next);
    const created = built catch |err| {
        log.err("create database '{s}': {s}", .{ name, @errorName(err) });
        return replyError(state, "create database failed");
    };
    // The database's name row, in the system namespace, commits with
    // its trees.
    var mc = created.change;
    var name_row = [_]zen.KeyValueStruct{
        try _names.dbNameKv(arena.allocator(), name, created.namespace),
    };
    mc.rows = .{ .namespace = sys_ns, .kvs = &name_row };
    eng.proposeMC(mc, .apply) catch |err| {
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
    const found = _names.namespaceId(
        eng.forest,
        eng.node.appliedTs(),
        name,
    ) catch |err| return replyNameReadFailed(state, name, err);
    const ns = found orelse return replyError(state, "database does not exist");

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
    // The name row goes with the trees, in one entry; the database's
    // own table names die with its trees. Built before the flag is
    // set, so a failure here leaves nothing to undo.
    const sys_ns = zdb.manifest.system_namespace;
    var name_row = [_]zen.KeyValueStruct{
        try _names.tombstone(arena.allocator(), sys_ns, name),
    };
    db.dropped.store(true, .release);
    const table_locks = db.allTableLocks(arena.allocator()) catch {
        db.dropped.store(false, .release);
        return replyError(state, "internal error: lock state");
    };
    for (table_locks) |m| {
        m.lock(@src());
        m.unlock();
    }

    var mc = eng.forest.buildNsDrop(arena.allocator(), ns) catch |err| {
        log.err("drop database '{s}': {s}", .{ name, @errorName(err) });
        db.dropped.store(false, .release);
        return replyError(state, "drop database failed");
    };
    mc.rows = .{ .namespace = sys_ns, .kvs = &name_row };
    eng.proposeMC(mc, .apply) catch |err| {
        log.err("drop database '{s}': propose: {s}", .{ name, @errorName(err) });
        db.dropped.store(false, .release);
        return replyError(state, "drop database proposal failed");
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
    const read_ts = eng.node.appliedTs();
    const existing = _names.tableId(
        eng.forest,
        read_ts,
        ns,
        ct.name,
    ) catch |err| return replyNameReadFailed(state, ct.name, err);
    if (existing != null) {
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
    // The base table's rows tree, then a unique index tree per primary
    // key or UNIQUE column.
    var changes: std.ArrayList(zdb.manifest.TreeChange) = .empty;
    try changes.append(a, zdb.manifest.newTableChange(.rows));
    // Every name the statement introduces, in `changes` order.
    var new_names: std.ArrayList(String) = .empty;
    try new_names.append(a, ct.name);

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
        try changes.append(
            a,
            zdb.manifest.newTableChange(.unique_index),
        );
        try new_names.append(a, index_name);
    }
    _names.checkNew(eng.forest, read_ts, ns, new_names.items) catch |err|
        return replyNameError(
            state,
            err,
            pgwire.sqlstate.duplicate_table,
            "table or index name already exists",
        );

    const created = eng.forest.buildTables(
        a,
        ns,
        changes.items,
        .create_tree,
    ) catch |err| {
        log.err("create table '{s}': {s}", .{ ct.name, @errorName(err) });
        return replyError(state, "create table failed");
    };
    // The trees and their catalog rows (columns, index descriptors)
    // go in one entry, so a crash leaves all of the table or none of
    // it. An index made with its table is valid from ts 0: no row
    // precedes it.
    const table_id = created.table_ids[0];
    const indexes = try a.alloc(_catalog.IndexDef, unique_cols.items.len);
    for (
        unique_cols.items,
        created.table_ids[1..],
        new_names.items[1..],
        indexes,
    ) |col, tid, index_name, *def| {
        def.* = .{
            .base_table_id = table_id,
            .index_table_id = tid,
            .column_id = col,
            .unique = true,
            .name = index_name,
        };
    }
    var mc = created.change;
    mc.rows = .{
        .namespace = ns,
        .kvs = try withNameRows(
            a,
            ns,
            try catalogRowKvs(self, a, .{
                .namespace = ns,
                .table_id = table_id,
            }, columns, indexes),
            new_names.items,
            created.table_ids,
        ),
    };
    eng.proposeMC(mc, .apply) catch |err| {
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
    const read_ts = eng.node.appliedTs();
    const existing = _names.tableId(
        eng.forest,
        read_ts,
        ns,
        cta.name,
    ) catch |err| return replyNameReadFailed(state, cta.name, err);
    if (existing != null) {
        if (cta.if_not_exists)
            return state.sendCommandComplete("CREATE TABLE AS");
        return replyErrorCode(
            state,
            pgwire.sqlstate.duplicate_table,
            "table already exists",
        );
    }
    const found = _names.tableId(
        eng.forest,
        read_ts,
        ns,
        cta.source,
    ) catch |err| return replyNameReadFailed(state, cta.source, err);
    const src_tid = found orelse return replyErrorCode(
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
        &self.srv.catalog_ps,
        src_key,
    ) catch |err| {
        log.err("create table as '{s}': catalog rows: {s}", .{
            cta.name, @errorName(err),
        });
        return replyError(state, "failed to load source table schema");
    };

    // The new tables: the base, then one per index, each a branch of
    // its source table (or, WITH NO DATA, an empty tree of its kind).
    // Index changes pair with `src_indexes` by position, for their
    // catalog rows below.
    var changes: std.ArrayList(zdb.manifest.TreeChange) = .empty;
    var src_indexes: std.ArrayList(_table.Index) = .empty;
    // Every name the statement introduces, in `changes` order.
    var new_names: std.ArrayList(String) = .empty;

    var base = zdb.manifest.newTableChange(.rows);
    base.from_table_id = src_tid;
    try changes.append(arloc, base);
    try new_names.append(arloc, cta.name);

    for (src.indexes) |idx| {
        if (idx.isBuilding())
            return replyErrorCode(
                state,
                pgwire.sqlstate.object_in_use,
                "source table has an index still being built",
            );

        const index_name = try idx.nameFor(arloc, cta.name);
        var tc = zdb.manifest.newTableChange(idx.treeKind());
        try new_names.append(arloc, index_name);
        tc.from_table_id = idx.index_table_id;
        try changes.append(arloc, tc);
        try src_indexes.append(arloc, idx);
    }
    _names.checkNew(eng.forest, read_ts, ns, new_names.items) catch |err|
        return replyNameError(
            state,
            err,
            pgwire.sqlstate.duplicate_table,
            "table or index name already exists",
        );

    const built = eng.forest.buildTables(
        arloc,
        ns,
        changes.items,
        if (cta.with_data) .branch_table else .create_tree,
    );
    const created = built catch |err| {
        log.err("create table as '{s}': {s}", .{ cta.name, @errorName(err) });
        return replyError(state, "create table failed");
    };
    // The catalog rows under the new ids ride in the same entry, as in
    // createTable. The column ids stay the source's, so the index
    // descriptors and the shared rows keep naming the same columns. A
    // branched index is valid from when its source was; an empty one
    // from the start.
    const table_id = created.table_ids[0];
    const indexes = try arloc.alloc(_catalog.IndexDef, src_indexes.items.len);
    for (
        src_indexes.items,
        created.table_ids[1..],
        new_names.items[1..],
        indexes,
    ) |idx, tid, index_name, *def| {
        def.* = .{
            .base_table_id = table_id,
            .index_table_id = tid,
            .column_id = idx.column.column_id,
            .unique = idx.unique,
            .valid_ts = if (cta.with_data) idx.valid_ts else 0,
            .name = index_name,
        };
    }
    var mc = created.change;
    mc.rows = .{
        .namespace = ns,
        .kvs = try withNameRows(
            arloc,
            ns,
            try catalogRowKvs(self, arloc, .{
                .namespace = ns,
                .table_id = table_id,
            }, columns, indexes),
            new_names.items,
            created.table_ids,
        ),
    };
    eng.proposeMC(mc, .apply) catch |err| {
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

/// Marshal one catalog row per column plus one per index descriptor,
/// for the caller to propose: column rows land in table 1, index rows
/// in table 2. `columns` carry their minted ids (createTable assigns
/// them); nothing is derived here. The rows are allocated with `a`.
pub fn catalogRowKvs(
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
    for (columns, 0..) |lc, i| {
        const cells = _catalog.encodeColumn(table_key.table_id, lc);
        const bytes = try _row.marshalRow(eng.allocator, &self.srv.catalog_ps, &cells, &buf);
        const primary = _catalog.encodeKey(
            table_key.table_id,
            lc.column_id,
        );
        const k = zdb.kv.Key{
            .namespace = table_key.namespace,
            .table_id = _catalog.table_id,
            .primary = &primary,
        };
        // marshalRow reuses `buf` — copy the row out; marshal copies
        // the key.
        kvs[i] = .{ .key = try k.marshal(a), .value = try a.dupe(u8, bytes) };
    }
    for (indexes, columns.len..) |idx, i| {
        const cells = _catalog.encodeIndex(idx);
        const bytes = try _row.marshalRow(
            eng.allocator,
            &self.srv.index_catalog_ps,
            &cells,
            &buf,
        );
        const primary = _catalog.encodeIndexKey(idx.base_table_id, idx.index_table_id);
        const k = zdb.kv.Key{
            .namespace = table_key.namespace,
            .table_id = _catalog.index_table_id,
            .primary = &primary,
        };
        kvs[i] = .{ .key = try k.marshal(a), .value = try a.dupe(u8, bytes) };
    }
    return kvs;
}

pub fn dropTable(self: *Handler, state: *pgwire.ConnState, dt: _parse.DropTable) !void {
    const eng = self.srv.engine;
    self.srv.ddl_mu.lock(@src());
    defer self.srv.ddl_mu.unlock();
    if (!try self.ddlReady(state)) return;

    const ns = try self.sessionNamespace(state) orelse return;
    const found = _names.tableId(
        eng.forest,
        eng.node.appliedTs(),
        ns,
        dt.name,
    ) catch |err| return replyNameReadFailed(state, dt.name, err);
    const table_id = found orelse {
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

    // The table and its index tables drop together, with their name
    // rows, in one atomic change. Their rows in the CATALOG trees stay
    // behind as unreachable garbage (ids are never reused). TODO: issue
    // tombstones for them.
    try scan.table_ids.append(a, table_id);
    const ids = scan.table_ids.items;
    var mc = eng.forest.buildTableDrop(a, ns, ids) catch |err| {
        log.err("drop table '{s}': {s}", .{ dt.name, @errorName(err) });
        return replyError(state, "drop table failed");
    };
    // The index names, from the index catalog rows: a user-chosen name
    // cannot be derived from its id.
    const defs = _table.loadIndexes(
        a,
        eng.forest,
        &self.srv.index_catalog_ps,
        .{ .namespace = ns, .table_id = table_id },
    ) catch |err| {
        log.err("drop table '{s}': index rows: {s}", .{
            dt.name, @errorName(err),
        });
        return replyError(state, "drop table failed");
    };
    const name_rows = try a.alloc(zen.KeyValueStruct, defs.len + 1);
    name_rows[0] = try _names.tombstone(a, ns, dt.name);
    for (defs, name_rows[1..]) |def, *row|
        row.* = try _names.tombstone(a, ns, def.name);
    mc.rows = .{ .namespace = ns, .kvs = name_rows };
    eng.proposeMC(mc, .apply) catch |err| {
        log.err("drop table '{s}': propose: {s}", .{ dt.name, @errorName(err) });
        return replyError(state, "drop table proposal failed");
    };
    // Evict the cached Table (safe: we hold the exclusive table
    // table lock, and DML holds its Table only under the shared one).
    // Index table_ids are never cached — they have no columns.
    try self.srv.forgetTable(.{ .namespace = ns, .table_id = table_id });

    log.info("dropped table '{s}' (ns={d} table_id={d} indexes={d})", .{ dt.name, ns, table_id, scan.table_ids.items.len - 1 });
    try state.sendCommandComplete("DROP TABLE");
}

/// `catalog` followed by one name row per new table: `new_names[i]`
/// names `table_ids[i]`, in namespace `ns`.
fn withNameRows(
    arloc: std.mem.Allocator,
    ns: u64,
    catalog: []zen.KeyValueStruct,
    new_names: []const String,
    table_ids: []const u32,
) ![]zen.KeyValueStruct {
    std.debug.assert(new_names.len == table_ids.len);
    const out = try arloc.alloc(
        zen.KeyValueStruct,
        catalog.len + new_names.len,
    );
    @memcpy(out[0..catalog.len], catalog);
    for (new_names, table_ids, out[catalog.len..]) |name, id, *row|
        row.* = try _names.tableNameKv(arloc, ns, name, id);
    return out;
}

/// Answer a failed `names.checkNew`: `code` and `taken` for a name in
/// use; any other error is a failed read.
pub fn replyNameError(
    state: *pgwire.ConnState,
    err: anyerror,
    code: [5]u8,
    taken: String,
) !void {
    return switch (err) {
        error.NameExists => replyErrorCode(state, code, taken),
        else => {
            log.err("name check: {s}", .{@errorName(err)});
            return replyError(state, "name check failed");
        },
    };
}
