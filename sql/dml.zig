// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The four DML statement bodies — INSERT, SELECT, DELETE, UPDATE.
//! Each resolves its table and WHERE, plans the read (plan.zig) and
//! runs it (exec.zig) for the rows it acts on, maintains every index
//! (one path for unique entries and postings — see the index
//! maintenance section), and buffers the result into the statement's
//! txn. The restart loop that re-runs these on a read_ts bump lives
//! in server.runDml.
//!
//! Free functions over `*Handler` — see the note in ddl.zig.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const engine = @import("engine");
const pgwire = @import("pgwire");
const zdb = @import("zdb");

const _exec = @import("exec.zig");
const _expr = @import("expr.zig");
const _parse = @import("parse.zig");
const _key = @import("key.zig");
const _plan = @import("plan.zig");
const _pred = @import("pred.zig");
const _row = @import("row.zig");
const _schema = @import("schema.zig");
const _table = @import("table.zig");
const _txn = @import("txn.zig");
const _value = @import("value.zig");
const _server = @import("server.zig");

const ExecCtx = _exec.ExecCtx;
const Handler = _server.Handler;
const PostingOp = _txn.Txn.PostingOp;
const Pred = _pred.Pred;
const SetNode = _expr.SetNode;
const String = _value.String;
const litToValue = _parse.litToValue;
const log = std.log.scoped(.sql);
const replyError = _server.replyError;
const replyErrorCode = _server.replyErrorCode;
const replyLockErr = _server.replyLockErr;

pub fn insert(self: *Handler, state: *pgwire.ConnState, ins: _parse.Insert) !void {
    const eng = self.srv.engine;
    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.manifest.lookupTable(ns, ins.table) orelse
        return replyErrorCode(state, pgwire.sqlstate.undefined_table, "table does not exist");
    const table_key: zdb.kv.TableKey = .{
        .namespace = ns,
        .table_id = table_id,
    };

    var arena = fiber.ctx().newArena();
    defer arena.deinit();
    const arloc = arena.allocator();

    const tx = (try self.dmlTxn(state, table_key)) orelse return;

    // Open AFTER the table lock: a statement that parked behind
    // DDL holding the table lock exclusively (CREATE/DROP INDEX)
    // must see the reloaded schema, not a stale cached Table.
    const tbl = self.openTable(table_key) catch |err| {
        log.err("insert into '{s}': load schema: {s}", .{ ins.table, @errorName(err) });
        return replyError(state, "failed to load table schema");
    };
    if (!try bumpToIndexes(state, tx, tbl)) return;
    fiber.ctx().record("txn begun, table open", .{});

    // The column each VALUES position writes (its cell is Column.pos).
    // Positional insert is the live columns themselves; an explicit
    // list resolves names. Inserting an explicit `egid` isn't
    // supported yet.
    // TODO: allow user-provided egids once unique-index enforcement exists
    // — a below-counter egid needs a data-tree existence check, and
    // concurrent inserts of the same egid need apply-time validation.
    const target_cols: []const _table.Column = blk: {
        const cols = ins.columns orelse break :blk tbl.columns;

        const list = try arloc.alloc(_table.Column, cols.len);
        for (cols, list) |name, *target| {
            const col = tbl.column(name) orelse
                return replyErrorCode(state, pgwire.sqlstate.undefined_column, "column does not exist");
            if (col.isEgid())
                return replyErrorCode(state, pgwire.sqlstate.feature_not_supported, "inserting an explicit egid is not supported yet");
            target.* = col.*;
        }
        break :blk list;
    };

    // Every VALUES row coerced into the cells of one Change, before
    // any lock or write: a Restart re-runs the whole statement.
    const changes = try arloc.alloc(Change, ins.rows.len);
    for (ins.rows, changes, 0..) |row_lits, *change, ri| {
        // A yield point per batch of rows, the executor's stride.
        if (ri % _exec.row_cap == 0) try fiber.ctx().maybeYield();

        if (row_lits.len != target_cols.len)
            return replyErrorCode(state, pgwire.sqlstate.syntax_error, "INSERT has the wrong number of values");

        const cells = try arloc.alloc(_value.Value, tbl.columns.len);
        @memset(cells, .null_value);
        for (row_lits, target_cols) |lit, col| {
            cells[col.pos] = (try coerceLiteral(
                state,
                arloc,
                lit,
                col,
                tx.start_us,
                self.params,
            )) orelse return;
        }
        change.* = .{ .post = cells };
    }
    fiber.ctx().record("{d} rows parsed", .{changes.len});

    // The whole VALUES list commits atomically as one .data entry; an
    // implicit txn waits for apply so an immediate SELECT on this
    // session sees the rows.
    var ctx = ExecCtx{
        .arloc = arloc,
        .tx = tx,
        .tbl = tbl,
        .table_id = table_id,
        .lock = true,
    };
    if (!try write(self, state, &ctx, table_key, ins.table, changes))
        return;

    if (tx.implicit) {
        if (!try self.commitOrReply(state, arloc, tx, "insert", ins.table)) return;
    }

    try state.sendCommandComplete(try std.fmt.allocPrint(arloc, "INSERT 0 {d}", .{ins.rows.len}));
}

pub fn select(self: *Handler, state: *pgwire.ConnState, sel: _parse.Select) !void {
    const eng = self.srv.engine;
    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.manifest.lookupTable(ns, sel.table) orelse
        return replyErrorCode(state, pgwire.sqlstate.undefined_table, "table does not exist");
    const table_key: zdb.kv.TableKey = .{
        .namespace = ns,
        .table_id = table_id,
    };

    var arena = fiber.ctx().newArena();
    defer arena.deinit();
    const arloc = arena.allocator();

    const tx = (try self.dmlTxn(state, table_key)) orelse return;

    // Open AFTER the table lock — see insert's comment.
    const tbl = self.openTable(table_key) catch |err| {
        log.err("select from '{s}': load schema: {s}", .{ sel.table, @errorName(err) });
        return replyError(state, "failed to load table schema");
    };

    const is_count = sel.projection == .count_star;
    const proj = (try selectFields(state, arloc, tbl, sel)) orelse return;
    var emitter = RowEmitter{
        .state = state,
        .tbl = tbl,
        .outs = proj.cols,
        .limit = self.max_rows,
    };

    // One loop for both kinds of plan. Plain SELECT streams each fill
    // to the wire; under FOR UPDATE the root's first `next` gathers
    // every match (values included) and locks every one — none
    // committed above read_ts (that lockRows would have bumped →
    // restarted), so the rows it then serves are the newest versions,
    // frozen by our locks.
    // count(*) counts every match and keeps no row bytes; its LIMIT
    // bounds the one output row, not the rows counted.
    var plan = (try planFor(state, arloc, tbl, sel.where, self.params, .{
        .read_ts = tx.read_ts,
        .keep_value = !is_count,
        .limit = if (is_count) null else sel.limit,
        .count_only = is_count,
    })) orelse return;
    defer plan.deinit();
    var ctx = ExecCtx{
        .arloc = arloc,
        .tx = tx,
        .tbl = tbl,
        .table_id = table_id,
        .lock = sel.for_update,
    };
    const batch = try self.batch();
    // The row description goes out after the first fill — after
    // whatever the root had to do to produce it. For a locking plan
    // that is the whole gather and every lock, so nothing reaches the
    // wire before the last lock and a Restart re-runs a statement
    // that has sent nothing. An empty result still gets its description.
    // On the extended path it is Describe's to send (describeSelect),
    // never Execute's.
    // Execute's row limit: the rows that fit go out, and a row left
    // over suspends the portal instead of completing it, as Postgres
    // does (the client resumes there; here the next Execute on the
    // portal is refused, pgwire.ConnState). A result that fits
    // completes normally — a driver that asks for one row of a
    // statement it expects no rows from gets its CommandComplete.
    var first = true;
    var count: u64 = 0;
    var suspended = false;
    outer: while (true) {
        // An executor error is a read failure — replied; an emit
        // error is a wire error — surfaced to the serve loop.
        const valid = plan.root.next(&ctx, batch) catch |err|
            return replyRunErr(state, "select from", sel.table, err);
        if (first) {
            if (self.mode == .simple) try state.sendRowDescription(proj.fields);
            first = false;
        }
        // An invalid batch is an empty one (RowNode.next): nothing to
        // count or emit, and the plan is exhausted.
        if (!valid) break;
        if (is_count) {
            count += batch.sel.count() + batch.counted;
            continue;
        }
        var it = batch.selected();
        while (it.next()) |row| {
            if (emitter.full()) {
                suspended = true;
                break :outer;
            }
            try emitter.emit(row);
        }
    }
    // count(*) is one row — which only LIMIT 0 drops.
    if (is_count and (sel.limit orelse 1) > 0) try emitter.emitCount(count);

    if (suspended) return state.sendPortalSuspended();
    try state.sendCommandComplete(try std.fmt.allocPrint(arloc, "SELECT {d}", .{emitter.count}));
}

pub fn delete(self: *Handler, state: *pgwire.ConnState, del: _parse.Delete) !void {
    const eng = self.srv.engine;

    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.manifest.lookupTable(ns, del.table) orelse
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_table,
            "table does not exist",
        );
    const table_key: zdb.kv.TableKey = .{
        .namespace = ns,
        .table_id = table_id,
    };
    var arena = fiber.ctx().newArena();
    defer arena.deinit();
    const arloc = arena.allocator();

    const tx = (try self.dmlTxn(state, table_key)) orelse return;

    // Open AFTER the table lock — see insert's comment.
    const tbl = self.openTable(table_key) catch |err| {
        logDeleteErr(del.table, "load schema", err);
        return replyError(state, "failed to load table schema");
    };
    if (!try bumpToIndexes(state, tx, tbl)) return;

    // Plan the WHERE: an egid or indexed term drives point reads,
    // anything else scans; no WHERE → tombstone every live row.
    // Materialize gathers the matches and locks every one; then they
    // are tombstoned from the gathered rows.
    var plan = (try planFor(state, arloc, tbl, del.where, self.params, .{
        .read_ts = tx.read_ts,
        // Indexed tables keep the old bytes so every index entry —
        // a unique key's tombstone, a posting's remove — can be
        // derived from the row being tombstoned.
        .keep_value = tbl.indexes.len > 0,
    })) orelse return;
    defer plan.deinit();

    var ctx = ExecCtx{
        .arloc = arloc,
        .tx = tx,
        .tbl = tbl,
        .table_id = table_id,
        .lock = true,
    };
    const batch = try self.batch();
    const mat = &plan.root.materialize;
    const rows = mat.gatherAndLockRows(&ctx, batch) catch |err|
        return replyRunErr(state, "delete from", del.table, err);

    // Each gathered row is a Change with no post: every index entry
    // removed, the row tombstoned. Past gatherAndLockRows' locks no
    // candidate committed above read_ts — the gathered rows are the
    // newest versions, frozen by our locks, so no re-read.
    const changes = try arloc.alloc(Change, rows.len);
    for (rows, changes) |item, *change| change.* = .{ .pre = item };
    if (!try write(self, state, &ctx, table_key, del.table, changes))
        return;
    const n = rows.len;
    if (n > 0 and tx.implicit) {
        if (!try self.commitOrReply(state, arloc, tx, "delete", del.table)) return;
    }
    const tag = try std.fmt.allocPrint(arloc, "DELETE {d}", .{n});
    try state.sendCommandComplete(tag);
}

pub fn update(self: *Handler, state: *pgwire.ConnState, up: _parse.Update) !void {
    const eng = self.srv.engine;

    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.manifest.lookupTable(ns, up.table) orelse
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_table,
            "table does not exist",
        );
    const table_key: zdb.kv.TableKey = .{
        .namespace = ns,
        .table_id = table_id,
    };
    var arena = fiber.ctx().newArena();
    defer arena.deinit();
    const arloc = arena.allocator();

    const tx = (try self.dmlTxn(state, table_key)) orelse return;

    // Open AFTER the table lock — see insert's comment.
    const tbl = self.openTable(table_key) catch |err| {
        log.err("update '{s}': load schema: {s}", .{ up.table, @errorName(err) });
        return replyError(state, "failed to load table schema");
    };
    if (!try bumpToIndexes(state, tx, tbl)) return;

    const sets = (try bindSets(self, state, arloc, tbl, up.sets)) orelse
        return;

    // Plan the WHERE: an egid or indexed term drives point reads,
    // anything else scans; no WHERE → rewrite every live row.
    // Materialize gathers each match's PRE-image and locks every
    // one; the rewrite happens post-lock.
    var plan = (try planFor(state, arloc, tbl, up.where, self.params, .{
        .read_ts = tx.read_ts,
    })) orelse return;
    defer plan.deinit();

    var ctx = ExecCtx{
        .arloc = arloc,
        .tx = tx,
        .tbl = tbl,
        .table_id = table_id,
        .lock = true,
    };
    const batch = try self.batch();
    const mat = &plan.root.materialize;
    const rows = mat.gatherAndLockRows(&ctx, batch) catch |err|
        return replyRunErr(state, "update", up.table, err);

    // Past gatherAndLockRows' locks no candidate committed above read_ts —
    // the gathered pre-images are the newest versions, frozen by our
    // locks. Each is a Change with its post evaluated from it; an eval
    // error ends the statement before any key is locked.
    const changes = try arloc.alloc(Change, rows.len);
    var row: _value.Row = .{};
    for (rows, changes, 0..) |item, *change, ri| {
        if (ri % _exec.row_cap == 0) try fiber.ctx().maybeYield();

        const post = try evalSets(state, arloc, tbl, item, sets, &row);
        change.* = .{ .pre = item, .post = post orelse return };
    }
    if (!try write(self, state, &ctx, table_key, up.table, changes)) return;
    const n = rows.len;

    if (tx.implicit) {
        if (!try self.commitOrReply(state, arloc, tx, "update", up.table)) return;
    }
    try state.sendCommandComplete(try std.fmt.allocPrint(arloc, "UPDATE {d}", .{n}));
}

/// One row a DML statement changes: `pre`, the stored row it
/// replaces, locked by the gather that found it; `post`, the cells it
/// leaves, by Column.pos. INSERT has a post, DELETE a pre, UPDATE
/// both. `egid` is the row's id, set by `write`.
const Change = struct {
    pre: ?zdb.kv.Item = null,
    post: ?[]const _value.Value = null,
    egid: u64 = 0,
};

/// Write a decided list of changes, every pre locked already — the
/// writer lock order is rows, then keys. A post with NULL in a NOT
/// NULL column is 23502. The index entries are each change's pre keys
/// diffed against its post keys, per index: a key only in the pre is
/// removed, only in the post added, in both left alone — so an insert
/// adds every key, a delete removes every key, and an update moves
/// the keys its SET changed. Then the unique keys are locked, each
/// row gets its egid (a fresh one without a pre) and is put — the
/// post marshalled, or a tombstone — and the index entries are
/// written. Nothing is buffered before the last lock: a Restart
/// re-runs the statement. false = replied.
fn write(
    self: *Handler,
    state: *pgwire.ConnState,
    ectx: *const ExecCtx,
    table_key: zdb.kv.TableKey,
    table_name: String,
    changes: []Change,
) !bool {
    const arloc = ectx.arloc;
    const tbl = ectx.tbl;
    const tx = ectx.tx;

    var keys: std.ArrayList(IndexKey) = .empty;
    for (changes, 0..) |change, ri| {
        if (ri % _exec.row_cap == 0) try fiber.ctx().maybeYield();

        if (change.post) |cells| for (tbl.columns) |*col| {
            if (cells[col.pos] == .null_value and col.not_null) {
                try replyNullViolation(state, arloc, table_name, col);
                return false;
            }
        };
        for (tbl.indexes) |*idx| {
            // A building index gets no entries from DML: nothing reads
            // it yet, and its build merges in every row written
            // meanwhile (index_ddl.zig).
            if (idx.isBuilding()) continue;
            const old: []const String = if (change.pre) |item|
                try idx.keys(arloc, tbl.parseCol(item, idx.column))
            else
                &.{};
            const new: []const String = if (change.post) |cells|
                try idx.keys(arloc, cells[idx.column.pos])
            else
                &.{};
            for (old) |key| if (!containsKey(new, key))
                try keys.append(arloc, .{
                    .idx = idx,
                    .key = key,
                    .row = ri,
                    .op = .remove,
                });
            for (new) |key| if (!containsKey(old, key))
                try keys.append(arloc, .{
                    .idx = idx,
                    .key = key,
                    .row = ri,
                    .op = .add,
                });
        }
    }
    if (!try sortAndLockIndexKeys(state, tx, keys.items)) return false;
    fiber.ctx().record("{d} index keys locked", .{keys.items.len});

    // Every lock is held: no Restart past here (puts don't bump).
    var buf: std.ArrayList(u8) = .empty; // marshalRow resets it
    for (changes) |*change| {
        if (change.pre) |item| {
            change.egid = _key.decodeEgid(item.key.primary).?;
        } else {
            change.egid = self.allocEgid(table_key) catch |err| {
                log.err("write '{s}': alloc egid: {s}", .{
                    table_name, @errorName(err),
                });
                try replyError(state, "row id allocation failed");
                return false;
            };
        }
        const ekey = _key.encodeEgid(change.egid);
        if (change.post) |cells| {
            const bytes = try _row.marshalRow(arloc, &tbl.ps, cells, &buf);
            try tx.put(table_key.table_id, &ekey, bytes, .{});
        } else {
            const meta: zdb.kv.Meta = .{ .kind = .deleted };
            try tx.put(table_key.table_id, &ekey, "", meta);
        }
    }
    fiber.ctx().record("{d} rows buffered", .{changes.len});
    return applyIndexKeys(state, tx, keys.items, changes);
}

/// One SET clause, bound: the expression and its target column. Parse
/// rejects duplicate SET columns.
const Set = struct {
    node: *const SetNode,
    col: *const _table.Column,
};

/// Bind a SET list against `tbl`. null = replied.
fn bindSets(
    self: *Handler,
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    clauses: []const _parse.SetClause,
) !?[]Set {
    const sets = try arloc.alloc(Set, clauses.len);
    for (clauses, sets) |clause, *set| {
        const col = tbl.column(clause.column) orelse {
            try replyUndefinedColumn(state);
            return null;
        };
        if (col.isEgid()) {
            try replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "egid cannot be updated",
            );
            return null;
        }
        const node = (try _expr.bind(
            state,
            arloc,
            tbl,
            col,
            clause.value,
            self.params,
        )) orelse return null;
        set.* = .{ .node = node, .col = col };
    }
    return sets;
}

/// The post of one row under `sets`: the pre-image `item` decoded,
/// each SET evaluated against the pre-image (so `SET a = b, b = a`
/// swaps) and laid over its cell. `row` is the decode scratch, reused
/// across a statement's rows; the cells are copied out of it and
/// borrow the item's value, which the arena keeps alive. null =
/// replied (an eval error).
fn evalSets(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    item: zdb.kv.Item,
    sets: []const Set,
    row: *_value.Row,
) !?[]const _value.Value {
    try _row.unmarshalRow(arloc, &tbl.ps, item.newest().value, &.{}, row);
    for (sets) |set| {
        const col = set.col;
        const val = set.node.eval(arloc, tbl, item, col) catch |err| {
            try replyEvalErr(state, arloc, err, col);
            return null;
        };
        row.set(col.pos, val);
    }
    return try arloc.dupe(_value.Value, row.cells());
}

/// Describe's answer for a SELECT: the RowDescription `select` would
/// send. The Table comes from the database's cache under the cache's
/// shared lock: forgetTable destroys an entry only under the
/// exclusive one, so the Table lives for as long as the hold, and no
/// table lock or txn is needed — which matters, since every
/// prepared SELECT sends a Describe. The names are copied out before
/// the hold ends and the reply goes out after it: nothing may write
/// to the socket under the cache lock. A miss (the first touch of
/// the table in this process) loads the schema the way a statement
/// does, under the table lock (describeSelectLoad).
pub fn describeSelect(
    self: *Handler,
    state: *pgwire.ConnState,
    sel: _parse.Select,
) !void {
    const eng = self.srv.engine;
    const ns = try self.sessionNamespace(state) orelse return;
    const table_id = eng.forest.manifest.lookupTable(ns, sel.table) orelse
        return replyErrorCode(
            state,
            pgwire.sqlstate.undefined_table,
            "table does not exist",
        );
    const table_key: zdb.kv.TableKey = .{
        .namespace = ns,
        .table_id = table_id,
    };

    var arena = fiber.ctx().newArena();
    defer arena.deinit();
    const arloc = arena.allocator();

    const db = try self.sessionDb(ns);
    const hold = db.holdTable(table_id) orelse
        return describeSelectLoad(self, state, arloc, table_key, sel);

    const resolved = blk: {
        // state.send* can block below. Release the table hold here itself.
        defer hold.release();
        const r = projectionOf(arloc, hold.tbl, sel);
        if (r) |proj| {
            for (proj.fields) |*fd| fd.name = try arloc.dupe(u8, fd.name);
        } else |_| {}
        break :blk r;
    };

    const proj = resolved catch |err| switch (err) {
        error.UndefinedColumn => return replyUndefinedColumn(state),
        error.OutOfMemory => return err,
    };
    try state.sendRowDescription(proj.fields);
}

/// describeSelect for a table not in the cache: load it the way a
/// statement does, under the table lock, in the block's txn or an
/// implicit one that ends here.
fn describeSelectLoad(
    self: *Handler,
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    table_key: zdb.kv.TableKey,
    sel: _parse.Select,
) !void {
    defer self.endImplicit();
    _ = (try self.dmlTxn(state, table_key)) orelse return;
    const tbl = self.openTable(table_key) catch |err| {
        log.err(
            "describe select from '{s}': load schema: {s}",
            .{ sel.table, @errorName(err) },
        );
        return replyError(state, "failed to load table schema");
    };
    const proj = (try selectFields(state, arloc, tbl, sel)) orelse return;
    try state.sendRowDescription(proj.fields);
}

/// SELECT constants with no FROM: the one row, straight from the
/// statement; no table, transaction or lock. Describe sends the
/// fields on the extended path, as for select.
pub fn selectRow(
    self: *Handler,
    state: *pgwire.ConnState,
    sr: _parse.SelectRow,
) !void {
    if (self.mode == .simple) try describeSelectRow(state, sr);
    const emit = (sr.limit orelse 1) > 0;
    if (emit) {
        var dw = try state.beginDataRow(@intCast(sr.cells.len));
        for (sr.cells) |cell| {
            if (cell.value == .null_value) {
                try dw.writeNull();
                continue;
            }
            const w = try dw.beginColumn();
            switch (cell.value) {
                .int => |n| try w.print("{d}", .{n}),
                .float, .str => |s| try w.writeAll(s),
                .bool_ => |b| try w.writeAll(if (b) "t" else "f"),
                else => unreachable, // the parser keeps only constants
            }
            dw.endColumn();
        }
        try dw.finish();
    }
    try state.sendCommandComplete(if (emit) "SELECT 1" else "SELECT 0");
}

/// The RowDescription of a SELECT without FROM: each field named by
/// its target and typed by its constant, the widest of its kind. An
/// untyped NULL is text, as Postgres resolves it.
pub fn describeSelectRow(
    state: *pgwire.ConnState,
    sr: _parse.SelectRow,
) !void {
    var arena = fiber.ctx().newArena();
    defer arena.deinit();
    const fields = try arena.allocator().alloc(
        pgwire.FieldDescription,
        sr.cells.len,
    );
    for (sr.cells, fields) |cell, *fd| fd.* = .{
        .name = cell.name,
        .type_oid = switch (cell.value) {
            .int => pgwire.oid.int8,
            .float => pgwire.oid.float8,
            .bool_ => pgwire.oid.bool_,
            .str, .null_value => pgwire.oid.text,
            else => unreachable, // the parser keeps only constants
        },
    };
    try state.sendRowDescription(fields);
}

/// A SELECT's output, resolved against its table: the projected
/// columns and their wire descriptions, one per column. count(*)
/// projects no column — one int8 field.
const SelectFields = struct {
    cols: []const *const _table.Column,
    fields: []pgwire.FieldDescription,
};

/// Resolve `sel`'s projection: `*` is the user columns; by name, egid
/// resolves like any other column (Table.column). Pure:
/// UndefinedColumn for a name the table lacks. The field names
/// borrow the Table.
fn projectionOf(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    sel: _parse.Select,
) error{ UndefinedColumn, OutOfMemory }!SelectFields {
    var cols: std.ArrayList(*const _table.Column) = .empty;
    switch (sel.projection) {
        .star => for (tbl.columns) |*c| try cols.append(arloc, c),
        .columns => |names| for (names) |nm| {
            const c = tbl.column(nm) orelse return error.UndefinedColumn;
            try cols.append(arloc, c);
        },
        .count_star => {},
    }

    const is_count = sel.projection == .count_star;
    const fields = try arloc.alloc(
        pgwire.FieldDescription,
        if (is_count) 1 else cols.items.len,
    );
    if (is_count) {
        fields[0] = .{ .name = "count", .type_oid = pgwire.oid.int8 };
    } else for (cols.items, fields) |c, *fd| fd.* = .{
        .name = c.name,
        .type_oid = _value.pgTypeOid(c.ty),
        .type_modifier = _value.pgTypeModifier(c.type_mod),
        .format_code = 0,
    };
    return .{ .cols = cols.items, .fields = fields };
}

/// projectionOf with the reply: null = 42703 was sent. `select`'s
/// RowEmitter takes the columns.
fn selectFields(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    sel: _parse.Select,
) !?SelectFields {
    return projectionOf(arloc, tbl, sel) catch |err| switch (err) {
        error.UndefinedColumn => {
            try replyUndefinedColumn(state);
            return null;
        },
        error.OutOfMemory => return err,
    };
}

fn replyUndefinedColumn(state: *pgwire.ConnState) !void {
    try replyErrorCode(
        state,
        pgwire.sqlstate.undefined_column,
        "column does not exist",
    );
}

/// Turns each row into a projected DataRow on the wire — plain
/// SELECT's sink per batch, and driven over a FOR UPDATE capture
/// post-lock. Columns format straight into the frame (pgwire
/// DataRowWriter), which the connection's buffered writer batches to
/// the socket.
pub const RowEmitter = struct {
    state: *pgwire.ConnState,
    tbl: *const _table.Table,
    /// The projection — egid is a column too (table.egid_column).
    outs: []const *const _table.Column,
    count: u64 = 0,
    /// Execute's row limit; 0 = none.
    limit: u32 = 0,

    /// Whether the limit's rows are all out.
    pub fn full(self: *const RowEmitter) bool {
        return self.limit != 0 and self.count >= self.limit;
    }

    /// Project and send one row — each output cell decoded on its own
    /// (O(1) each, Table.read), no full-row decode.
    pub fn emit(self: *RowEmitter, item: zdb.kv.Item) !void {
        var dw = try self.state.beginDataRow(@intCast(self.outs.len));
        for (self.outs) |col| {
            const v = self.tbl.parseCol(item, col);
            if (v == .null_value) {
                try dw.writeNull();
            } else {
                try _value.valueToText(try dw.beginColumn(), col, v);
                dw.endColumn();
            }
        }
        try dw.finish();
        self.count += 1;
    }

    /// count(*)'s one output row: the int8 cell.
    pub fn emitCount(self: *RowEmitter, count: u64) !void {
        var dw = try self.state.beginDataRow(1);
        const w = try dw.beginColumn();
        try _value.valueToText(w, null, .{ .int8 = @intCast(count) });
        dw.endColumn();
        try dw.finish();
        self.count += 1;
    }
};

/// Advance a writer's read_ts to every built unique index's
/// `valid_ts`, so its uniqueness probes see everything the build
/// wrote. Called right after openTable, before any planning or row
/// work, so a Restart discards nothing. A no-op for an implicit txn
/// (its read_ts is the applied ts at newTxn) and for any txn that
/// began after the build; an index still building is skipped — the
/// writer neither maintains nor probes it, and its build merges the
/// rows written meanwhile (index_ddl.zig). Readers never bump: the
/// planner scans instead of using an index newer than the snapshot.
/// Returns false when an error was replied (Conflict → 40001); a
/// Restart propagates to the statement driver.
fn bumpToIndexes(
    state: *pgwire.ConnState,
    tx: *_txn.Txn,
    tbl: *const _table.Table,
) !bool {
    for (tbl.indexes) |*idx| {
        if (!idx.unique or idx.isBuilding()) continue;
        tx.bumpTo(idx.valid_ts) catch |err| {
            try replyLockErr(state, err);
            return false;
        };
    }
    return true;
}

/// Bind the WHERE (null: every row) and plan it. null = bind already
/// sent the error reply (the pred.bind convention).
fn planFor(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    where: ?_parse.Where,
    params: _parse.Params,
    opts: _plan.Options,
) !?_plan.Plan {
    var pred: ?*const Pred = null;
    if (where) |tree| {
        pred = (try _pred.bind(state, arloc, tbl, tree, params)) orelse
            return null;
    }
    return try _plan.plan(arloc, tbl, pred, opts);
}

/// litToValue plus INSERT's client-facing error mapping, and the one
/// non-constant literal: CURRENT_TIMESTAMP is the transaction's start
/// (`now_us`, Unix µs), typed by the column it lands in — a timestamp
/// column, else 42804. A `$n` is the bound value (`params`). null =
/// the error reply was already sent (the pred.bind convention).
fn coerceLiteral(
    state: *pgwire.ConnState,
    a: std.mem.Allocator,
    lit: _parse.Literal,
    col: _table.Column,
    now_us: i64,
    params: _parse.Params,
) !?_value.Value {
    if (lit == .current_timestamp) {
        return switch (col.ty) {
            .timestamp => .{ .timestamp = now_us },
            .timestamptz => .{ .timestamptz = now_us },
            else => {
                try replyErrorCode(
                    state,
                    pgwire.sqlstate.datatype_mismatch,
                    "CURRENT_TIMESTAMP needs a timestamp column",
                );
                return null;
            },
        };
    }
    return litToValue(a, lit, &col, params) catch |err| switch (err) {
        error.OutOfMemory => return err,
        error.UnsupportedLiteral, error.UnsupportedType => {
            try replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "unsupported value or column type",
            );
            return null;
        },
        error.StringTooLong => {
            try replyStringTooLong(state, a, &col);
            return null;
        },
        error.InvalidUtf8 => {
            try replyInvalidUtf8(state);
            return null;
        },
        error.TypeMismatch => {
            try replyErrorCode(
                state,
                pgwire.sqlstate.datatype_mismatch,
                "parameter type does not match column type",
            );
            return null;
        },
        // A number the column cannot hold: parsed from text (Overflow)
        // or cast from a wider binary parameter.
        error.Overflow, error.IntOutOfRange, error.FloatOutOfRange => {
            try replyErrorCode(
                state,
                pgwire.sqlstate.numeric_value_out_of_range,
                "value is out of range for column type",
            );
            return null;
        },
        else => {
            try replyErrorCode(
                state,
                pgwire.sqlstate.invalid_text_representation,
                "invalid value for column type",
            );
            return null;
        },
    };
}

const replyStringTooLong = _expr.replyStringTooLong;
const replyInvalidUtf8 = _expr.replyInvalidUtf8;

/// Map an eval failure (expr.eval: arithmetic on one row's SET
/// values, or a string landing in a `char(n)` / `varchar(n)` column)
/// onto the wire. `target` is the column being assigned.
fn replyEvalErr(
    state: *pgwire.ConnState,
    a: std.mem.Allocator,
    err: _expr.EvalError,
    col: *const _table.Column,
) !void {
    return switch (err) {
        error.OutOfMemory => err,
        error.StringTooLong => replyStringTooLong(state, a, col),
        error.InvalidUtf8 => replyInvalidUtf8(state),
        error.IntOutOfRange => replyErrorCode(
            state,
            pgwire.sqlstate.numeric_value_out_of_range,
            "integer out of range",
        ),
        error.FloatOutOfRange => replyErrorCode(
            state,
            pgwire.sqlstate.numeric_value_out_of_range,
            "value out of range: overflow",
        ),
        error.DivisionByZero => replyErrorCode(
            state,
            pgwire.sqlstate.division_by_zero,
            "division by zero",
        ),
    };
}

// --- index maintenance, shared by INSERT, DELETE and UPDATE -----------
//
// A statement collects every index mutation it will make — over all
// its rows and all of `tbl.indexes` — BEFORE any index-key lock, then
// locks the unique keys in sorted order, then writes. A lock may
// Restart the statement, so nothing is buffered until every lock is
// held; and a statement never reads its own writes: every read it
// makes (the row gather, the unique probes) precedes its first write.
// What a probe sees is the committed state at read_ts plus the pending
// writes of this transaction's EARLIER statements, which it must:
// `DELETE pk 5; INSERT 5` finds the pending tombstone, `INSERT 5;
// INSERT 5` the pending put. Lock order for every writer: rows first
// (Materialize), then index keys (IndexKey.lessThan).

/// One index mutation: the entry `key` of `idx` for the statement's
/// row at `row` (whose egid INSERT knows only at write time), added or
/// removed. Sorted by (index table, key) — the lock order — the
/// entries for one unique key sit together, which is how
/// applyIndexKeys decides what the whole statement does to it.
const IndexKey = struct {
    idx: *const _table.Index,
    key: String,
    row: usize,
    op: PostingOp,

    fn lessThan(_: void, x: IndexKey, y: IndexKey) bool {
        const xt = x.idx.index_table_id;
        const yt = y.idx.index_table_id;
        if (xt != yt) return xt < yt;
        return std.mem.lessThan(u8, x.key, y.key);
    }

    fn sameKey(x: IndexKey, y: IndexKey) bool {
        return x.idx == y.idx and std.mem.eql(u8, x.key, y.key);
    }
};

/// Whether `key` is one of `keys` — the diff of a row's old keys
/// against its new ones. A scan: the slices hold 0 or 1 keys today; a
/// sorted merge once an index kind emits hundreds.
fn containsKey(keys: []const String, key: String) bool {
    for (keys) |k| if (std.mem.eql(u8, k, key)) return true;
    return false;
}

/// Sort the statement's index mutations into lock order and lock every
/// unique key, all before any write (lockRows may Restart). Non-unique
/// entries are sorted with the rest but never locked: posting deltas
/// commute. false = an error was replied.
fn sortAndLockIndexKeys(
    state: *pgwire.ConnState,
    tx: *_txn.Txn,
    keys: []IndexKey,
) !bool {
    // Grouping needs adjacency only, and a group's outcome does not
    // depend on the order within it (two adds is an error, one add
    // has one egid), so the unstable sort — no per-call scratch cache
    // to poison-fill — is fine here.
    std.mem.sortUnstable(IndexKey, keys, {}, IndexKey.lessThan);

    var hashes: std.ArrayList(u64) = .empty;
    defer hashes.deinit(tx.allocator);
    for (keys) |ik| {
        if (!ik.idx.unique) continue;
        const h = tx.rowHash(ik.idx.index_table_id, ik.key);
        try hashes.append(tx.allocator, h);
    }
    tx.lockRows(hashes.items) catch |err| {
        try replyLockErr(state, err);
        return false;
    };
    return true;
}

/// Write the statement's index mutations (sorted, every lock held).
/// A non-unique entry is a posting delta, written as it is. A unique
/// key gets ONE write, decided from its group of equal entries, i.e.
/// from what the whole statement does to it: more than one add is two
/// rows given the same value → 23505; no add is a tombstone; one add
/// with no remove probes the index, found → 23505, else puts; one add
/// with a remove is a move between two of this statement's rows — the
/// put alone, no probe, since the committed occupant is the row moving
/// away, its pre-image locked and newest. The probed puts are written
/// after the probe, which reads the pending skiplist and must not
/// find this statement's own entry there; the probes go out in one
/// batch per index table. `changes[ik.row].egid` is each entry's row
/// id. false = replied.
fn applyIndexKeys(
    state: *pgwire.ConnState,
    tx: *_txn.Txn,
    keys: []const IndexKey,
    changes: []const Change,
) !bool {
    for (keys) |ik| {
        if (ik.idx.unique) {
            continue;
        }
        const table_id = ik.idx.index_table_id;
        try tx.putPosting(table_id, ik.key, changes[ik.row].egid, ik.op);
    }

    // The lone puts, in key order, so one index table's probes are
    // one run.
    var probes: std.ArrayList(IndexKey) = .empty;
    defer probes.deinit(tx.allocator);
    var i: usize = 0;
    while (i < keys.len) {
        const first = keys[i];
        const end = groupEnd(keys, i);
        defer i = end;
        if (!first.idx.unique) continue;
        const table_id = first.idx.index_table_id;
        const group = groupOps(keys[i..end], changes);
        if (group.adds >= 2) return replyUniqueViolation(state);
        if (group.adds == 0) {
            try tx.put(table_id, first.key, "", .{ .kind = .deleted });
            continue;
        }
        if (group.removes == 0) {
            try probes.append(tx.allocator, first);
            continue;
        }
        const ekey = _key.encodeEgid(group.egid);
        try tx.put(table_id, first.key, &ekey, .{});
    }

    if (!try probeUnique(state, tx, probes.items)) return false;
    fiber.ctx().record("{d} unique keys probed", .{probes.items.len});
    for (probes.items) |ik| {
        const ekey = _key.encodeEgid(changes[ik.row].egid);
        try tx.put(ik.idx.index_table_id, ik.key, &ekey, .{});
    }
    return true;
}

/// Where the run of entries sharing keys[i]'s index and key ends.
fn groupEnd(keys: []const IndexKey, i: usize) usize {
    var end = i + 1;
    while (end < keys.len and keys[end].sameKey(keys[i])) end += 1;
    return end;
}

/// What one unique key's group does: how many rows add it, how many
/// remove it, and the egid of the (last) adding row.
fn groupOps(
    group: []const IndexKey,
    changes: []const Change,
) struct { adds: usize, removes: usize, egid: u64 } {
    var adds: usize = 0;
    var removes: usize = 0;
    var egid: u64 = 0;
    for (group) |ik| switch (ik.op) {
        .add => {
            adds += 1;
            egid = changes[ik.row].egid;
        },
        .remove => removes += 1,
        .noop => unreachable,
    };
    return .{ .adds = adds, .removes = removes, .egid = egid };
}

/// Whether every probed unique key is free: one batch read per index
/// table over `probes`, entries in sorted order. Post-lock, read_ts
/// covers each key's newest commit, so a live entry means the value
/// exists. Only a unique index table may be read this way: a
/// non-unique one's pending posting ops sit in `writes` keyed with
/// timestamp = egid. false = replied (23505, or an internal read
/// failure).
fn probeUnique(
    state: *pgwire.ConnState,
    tx: *_txn.Txn,
    probes: []const IndexKey,
) !bool {
    var names: std.ArrayList(String) = .empty;
    defer names.deinit(tx.allocator);
    var i: usize = 0;
    while (i < probes.len) {
        // One index read per group: a yield point each.
        try fiber.ctx().maybeYield();

        const index_table_id = probes[i].idx.index_table_id;
        utils.clearRetainCap(&names);

        while (i < probes.len and
            probes[i].idx.index_table_id == index_table_id) : (i += 1)
        {
            try names.append(tx.allocator, probes[i].key);
        }
        var res = tx.multiGet(index_table_id, names.items) catch |err| {
            log.err("unique probe index={d}: {s}", .{
                index_table_id,
                @errorName(err),
            });
            try replyError(state, "unique index probe failed");
            return false;
        };
        defer res.deinit();
        for (names.items) |key| {
            if (res.item(key) != null) return replyUniqueViolation(state);
        }
    }
    return true;
}

/// Reply 23505; false, for the `!bool` helpers' "replied" convention.
fn replyUniqueViolation(state: *pgwire.ConnState) !bool {
    try replyErrorCode(
        state,
        pgwire.sqlstate.unique_violation,
        "duplicate key value violates unique constraint",
    );
    return false;
}

/// NULL into a NOT NULL column (the PRIMARY KEY included), by INSERT
/// or UPDATE: 23502, in Postgres's wording. A unique index takes NULL
/// as "no entry" (NULLS DISTINCT); only the constraint refuses it.
fn replyNullViolation(
    state: *pgwire.ConnState,
    a: std.mem.Allocator,
    table_name: String,
    col: *const _table.Column,
) !void {
    return replyErrorCode(
        state,
        pgwire.sqlstate.not_null_violation,
        try std.fmt.allocPrint(
            a,
            "null value in column \"{s}\" of relation \"{s}\" " ++
                "violates not-null constraint",
            .{ col.name, table_name },
        ),
    );
}

fn logDeleteErr(table_name: String, what: String, err: anyerror) void {
    log.err(
        "delete from '{s}': {s}: {s}",
        .{ table_name, what, @errorName(err) },
    );
}

/// Map an executor failure (root.next / gatherAndLockRows) onto the wire:
/// row-lock errors (a locking plan locks the matches) ride
/// replyLockErr — Restart propagates to the statement driver — and
/// anything else is an internal read failure.
fn replyRunErr(
    state: *pgwire.ConnState,
    comptime verb: String,
    table_name: String,
    err: anyerror,
) !void {
    return switch (err) {
        error.Restart,
        error.Conflict,
        error.Deadlock,
        error.Upgrade,
        => replyLockErr(state, err),
        // A yield point stopped the scan: the serve loop ends the
        // session with the FATAL, so no reply here.
        error.Shutdown => err,
        error.MemLimit => _server.replyMemLimit(state),
        error.ReadBelowMinRead => _server.replySnapshotTooOld(state),
        else => {
            log.err(
                verb ++ " '{s}': scan: {s}",
                .{ table_name, @errorName(err) },
            );
            return replyError(state, "scan failed");
        },
    };
}
