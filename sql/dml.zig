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
const Node = _expr.Node;
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
    if (!try bumpToIndexes(state, arloc, tx, tbl)) return;
    fiber.ctx().record("txn begun, table open", .{});

    // INSERT INTO t (a, b): the column each VALUES position writes
    // (its cell is Column.pos). INSERT INTO t without a list is the
    // live columns in order. The egid is never a target: the server
    // allocates it (write), by design.
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
        // NOT NULL is checked on the proposed row, before ON CONFLICT
        // may drop it: INSERT INTO t VALUES (1, NULL) ON CONFLICT DO
        // NOTHING is 23502 even when id 1 exists, as in Postgres.
        for (tbl.columns) |*col| {
            if (cells[col.pos] == .null_value and col.not_null)
                return replyNullViolation(state, arloc, ins.table, col);
        }
        change.* = .{ .post = cells };
    }
    fiber.ctx().record("{d} rows parsed", .{changes.len});

    // INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET n = 7:
    // the stored row with id 1 is gathered and locked here, before any
    // index key — the writer lock order (rows, then keys) — and the
    // action rewrites the change that proposed id 1.
    var ctx = ExecCtx{
        .arloc = arloc,
        .scratch = .init(tx.allocator),
        .tx = tx,
        .tbl = tbl,
        .table_id = table_id,
        .lock = true,
    };
    defer ctx.deinit();
    var uniques: []Unique = &.{};
    if (ins.on_conflict) |oc| {
        uniques = (try uniqueIndexes(state, arloc, tbl, oc)) orelse return;
        const ok = try resolveConflicts(
            self,
            state,
            &ctx,
            ins.table,
            oc,
            uniques,
            changes,
        );
        if (!ok) return;
    }

    // The whole VALUES list commits atomically as one .data entry; an
    // implicit txn waits for apply so an immediate SELECT on this
    // session sees the rows.
    const affected = (try write(
        self,
        state,
        &ctx,
        table_key,
        ins.table,
        changes,
        uniques,
    )) orelse return;

    if (tx.implicit) {
        if (!try self.commitOrReply(state, arloc, tx, "insert", ins.table)) return;
    }

    // Postgres counts the updated rows in the tag too.
    const tag = try std.fmt.allocPrint(arloc, "INSERT 0 {d}", .{affected});
    try state.sendCommandComplete(tag);
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
    const proj = (try selectFields(state, arloc, tbl, sel, self.params)) orelse
        return;
    var emitter = RowEmitter{
        .state = state,
        .tbl = tbl,
        .outs = proj.outs,
        .scratch = .init(tx.allocator),
        .limit = self.max_rows,
    };
    defer emitter.scratch.deinit();

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
        .scratch = .init(tx.allocator),
        .tx = tx,
        .tbl = tbl,
        .table_id = table_id,
        .lock = sel.for_update,
    };
    defer ctx.deinit();
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
            // A target that fails to compute ends the result with the
            // error, as in Postgres; a wire error goes to the serve loop.
            emitter.emit(row) catch |err| switch (err) {
                error.IntOutOfRange,
                error.FloatOutOfRange,
                error.DivisionByZero,
                error.SubstringLength,
                error.PowerArgument,
                error.InvalidUtf8,
                => return replyExprErr(state, @errorCast(err)),
                else => return err,
            };
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
    if (!try bumpToIndexes(state, arloc, tx, tbl)) return;

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
        .scratch = .init(tx.allocator),
        .tx = tx,
        .tbl = tbl,
        .table_id = table_id,
        .lock = true,
    };
    defer ctx.deinit();
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
    const n = (try write(
        self,
        state,
        &ctx,
        table_key,
        del.table,
        changes,
        &.{},
    )) orelse return;
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
    if (!try bumpToIndexes(state, arloc, tx, tbl)) return;

    // UPDATE t SET a = expr, ...: bound once, before any lock.
    const sets = (try bindSets(self, state, arloc, tbl, up.sets, null)) orelse
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
        .scratch = .init(tx.allocator),
        .tx = tx,
        .tbl = tbl,
        .table_id = table_id,
        .lock = true,
    };
    defer ctx.deinit();
    const batch = try self.batch();
    const mat = &plan.root.materialize;
    const rows = mat.gatherAndLockRows(&ctx, batch) catch |err|
        return replyRunErr(state, "update", up.table, err);

    // SET, per gathered row. Past gatherAndLockRows' locks no candidate
    // committed above read_ts — the pre-images are the newest versions,
    // frozen by our locks. Each is a Change with its post evaluated
    // from it; an eval error ends the statement before any key is
    // locked.
    const changes = try arloc.alloc(Change, rows.len);
    var row: _value.Row = .{};
    for (rows, changes, 0..) |item, *change, ri| {
        if (ri % _exec.row_cap == 0) try fiber.ctx().maybeYield();

        const post = (try evalSets(
            state,
            arloc,
            tbl,
            up.table,
            item,
            sets,
            &row,
        )) orelse return;
        change.* = .{ .pre = item, .post = post };
    }
    const n = (try write(
        self,
        state,
        &ctx,
        table_key,
        up.table,
        changes,
        &.{},
    )) orelse return;

    if (tx.implicit) {
        if (!try self.commitOrReply(state, arloc, tx, "update", up.table)) return;
    }
    try state.sendCommandComplete(try std.fmt.allocPrint(arloc, "UPDATE {d}", .{n}));
}

/// One row a DML statement changes: `pre`, the stored row it
/// replaces, locked by the gather that found it; `post`, the cells it
/// leaves, by Column.pos. INSERT has a post, DELETE a pre, UPDATE
/// both; neither is a proposed row ON CONFLICT dropped, which `write`
/// skips. `egid` is the row's id, set by `write`.
const Change = struct {
    pre: ?zdb.kv.Item = null,
    post: ?[]const _value.Value = null,
    egid: u64 = 0,
};

/// Write a decided list of changes, every pre locked already — the
/// writer lock order is rows, then keys. The index entries are each
/// change's pre keys diffed against its post keys, per index: a key
/// only in the pre is removed, only in the post added, in both left
/// alone — so an insert adds every key, a delete removes every key,
/// and an update moves the keys its SET changed. Then the unique keys
/// are locked, each row gets its egid (a fresh one without a pre) and
/// is put — the post marshalled, or a tombstone — and the index
/// entries are written. Nothing is buffered before the last lock: a
/// Restart re-runs the statement. `uniques` are the indexes an ON
/// CONFLICT resolved against, for the probe; empty otherwise. Returns
/// the rows written; null = replied.
fn write(
    self: *Handler,
    state: *pgwire.ConnState,
    ctx: *const ExecCtx,
    table_key: zdb.kv.TableKey,
    table_name: String,
    changes: []Change,
    uniques: []const Unique,
) !?usize {
    const arloc = ctx.arloc;
    const tbl = ctx.tbl;
    const tx = ctx.tx;

    var keys: std.ArrayList(IndexKey) = .empty;
    for (changes, 0..) |change, ri| {
        if (ri % _exec.row_cap == 0) try fiber.ctx().maybeYield();

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
    if (!try sortAndLockIndexKeys(state, tx, keys.items)) return null;
    fiber.ctx().record("{d} index keys locked", .{keys.items.len});

    // Every lock is held: no Restart past here (puts don't bump).
    var buf: std.ArrayList(u8) = .empty; // marshalRow resets it
    var count: usize = 0;
    for (changes) |*change| {
        if (change.pre == null and change.post == null) continue;
        count += 1;
        if (change.pre) |item| {
            change.egid = _key.decodeEgid(item.key.primary).?;
        } else {
            change.egid = self.allocEgid(table_key) catch |err| {
                log.err("write '{s}': alloc egid: {s}", .{
                    table_name, @errorName(err),
                });
                try replyError(state, "row id allocation failed");
                return null;
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
    fiber.ctx().record("{d} rows buffered", .{count});
    if (!try applyIndexKeys(state, tx, keys.items, changes, uniques))
        return null;
    return count;
}

/// A unique index ON CONFLICT resolves against, with the change that
/// proposes each of its keys. For
///   INSERT INTO t VALUES (1, 5), (2, 6) ON CONFLICT (id) DO NOTHING
/// the pkey index with {1 → change 0, 2 → change 1}: a stored row with
/// id 1 finds its proposer in one lookup, and a key proposed twice is
/// noticed.
const Unique = struct {
    idx: *const _table.Index,
    change_by_key: std.StringHashMapUnmanaged(usize) = .empty,
};

/// The indexes an ON CONFLICT collides on. ON CONFLICT (id) is the
/// unique indexes on id; a bare ON CONFLICT DO NOTHING every unique
/// index. null = replied: ON CONFLICT (nosuch) is 42703, ON CONFLICT
/// (n) with no unique index on n is 42P10 (Postgres's wording).
fn uniqueIndexes(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    oc: _parse.OnConflict,
) !?[]Unique {
    var col: ?*const _table.Column = null;
    if (oc.column) |name| {
        col = tbl.column(name) orelse {
            try replyUndefinedColumn(state);
            return null;
        };
    }
    const idxs = try tbl.uniqueIndexes(arloc, col);
    if (col != null and idxs.len == 0) {
        try replyErrorCode(
            state,
            pgwire.sqlstate.invalid_column_reference,
            "there is no unique or exclusion constraint matching the " ++
                "ON CONFLICT specification",
        );
        return null;
    }
    const uniques = try arloc.alloc(Unique, idxs.len);
    for (idxs, uniques) |idx, *unique| unique.* = .{ .idx = idx };
    return uniques;
}

/// Rewrite the changes under ON CONFLICT. For
///   INSERT INTO t VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET n = 7
/// with (1, 10) stored: the stored row becomes the change's pre,
/// gathered and locked like an UPDATE's match (the gather is
/// pending-aware, so a row this transaction inserted earlier collides
/// too), and the action sets its post to (1, 7). false = replied.
fn resolveConflicts(
    self: *Handler,
    state: *pgwire.ConnState,
    ctx: *ExecCtx,
    table_name: String,
    oc: _parse.OnConflict,
    uniques: []Unique,
    changes: []Change,
) !bool {
    const arloc = ctx.arloc;
    const tbl = ctx.tbl;

    // The keys the proposed rows would add: for VALUES (1, 5), (2, 6)
    // ON CONFLICT (id) the predicate id = 1 OR id = 2, each key noted
    // under its change.
    const pred = collisionPred(arloc, uniques, changes, oc.action) catch |err|
        switch (err) {
            error.OutOfMemory => return err,
            error.RowTwice => return replyRowTwice(state),
        } orelse return true;

    // DO UPDATE SET n = 7 WHERE t.n < 10: the WHERE, bound once, before
    // any lock.
    var where: ?*const Pred = null;
    if (oc.action == .update) if (oc.action.update.where) |cond| {
        where = (try _pred.bindOrReply(
            state,
            arloc,
            tbl,
            cond,
            self.params,
        )) orelse return false;
    };

    // The stored rows that hold one of those keys, read from the
    // unique indexes and locked.
    var plan = try _plan.plan(arloc, tbl, pred, .{ .read_ts = ctx.tx.read_ts });
    defer plan.deinit();

    const batch = try self.batch();
    const gather = &plan.root.materialize;
    const hits = gather.gatherAndLockRows(ctx, batch) catch |err| {
        try replyRunErr(state, "insert", table_name, err);
        return false;
    };
    fiber.ctx().record("{d} colliding rows locked", .{hits.len});

    // Each stored row is the pre of the change that proposed its key:
    // stored (1, 10) → the change proposing id 1. Under ON CONFLICT
    // (id) a key has one stored row and one proposer (collisionPred),
    // so a DO UPDATE change gets one pre; under a bare ON CONFLICT a
    // change may collide on several indexes, and DO NOTHING drops it.
    for (hits) |item| for (uniques) |unique| {
        const cell = tbl.parseCol(item, unique.idx.column);
        for (try unique.idx.keys(arloc, cell)) |key| {
            const ci = unique.change_by_key.get(key) orelse continue;
            changes[ci].pre = item;
        }
    };

    // The action, per collided change. DO NOTHING drops it; DO UPDATE
    // SET n = 7 WHERE t.n < 10 drops it when the stored row fails the
    // WHERE. Else the change updates the stored row: SET bound per
    // change, since EXCLUDED is that change's proposed row ((1, 5) for
    // VALUES (1, 5)), and evaluated against the stored row.
    var row: _value.Row = .{};
    for (changes) |*change| {
        const item = change.pre orelse continue;
        const keep = switch (oc.action) {
            .nothing => false,
            .update => if (where) |cond|
                cond.matches(ctx.row(item)) catch |err| {
                    if (err == error.OutOfMemory) return err;
                    try replyExprErr(state, err);
                    return false;
                }
            else
                true,
        };
        if (!keep) {
            change.* = .{};
            continue;
        }
        const up = oc.action.update;
        const sets = (try bindSets(
            self,
            state,
            arloc,
            tbl,
            up.sets,
            change.post,
        )) orelse return false;
        change.post = (try evalSets(
            state,
            arloc,
            tbl,
            table_name,
            item,
            sets,
            &row,
        )) orelse return false;
    }
    return true;
}

/// One `col = cell` leaf per (change, unique index) with a non-NULL
/// cell — NULL never collides (NULLS DISTINCT) — ORed into one
/// predicate, and each key noted under its change in the index's map;
/// null when no change has a key to collide on. For
///   INSERT INTO t VALUES (1, 5), (1, 6) ON CONFLICT (id) ...
/// id 1 is change 0's: DO NOTHING drops change 1 (Postgres skips the
/// second of a pair), DO UPDATE is RowTwice (21000).
fn collisionPred(
    arloc: std.mem.Allocator,
    uniques: []Unique,
    changes: []Change,
    action: _parse.OnConflict.Action,
) error{ OutOfMemory, RowTwice }!?*const Pred {
    var leaves: std.ArrayList(Pred) = .empty;
    for (changes, 0..) |*change, ci| for (uniques) |*unique| {
        const cells = change.post orelse continue;
        const cell = cells[unique.idx.column.pos];
        for (try unique.idx.keys(arloc, cell)) |key| {
            const slot = try unique.change_by_key.getOrPut(arloc, key);
            if (slot.found_existing) switch (action) {
                .nothing => {
                    change.* = .{};
                    continue;
                },
                .update => return error.RowTwice,
            };
            slot.value_ptr.* = ci;
            try leaves.append(
                arloc,
                try _pred.colCmp(arloc, unique.idx.column, .eq, cell),
            );
        }
    };
    if (leaves.items.len == 0) return null;

    const root = try arloc.create(Pred);
    root.* = if (leaves.items.len == 1)
        leaves.items[0]
    else
        .{ .or_ = leaves.items };
    return root;
}

/// 21000: INSERT INTO t VALUES (1, 5), (1, 6) ON CONFLICT (id) DO
/// UPDATE ... would update the row with id 1 twice.
fn replyRowTwice(state: *pgwire.ConnState) !bool {
    try replyErrorCode(
        state,
        pgwire.sqlstate.cardinality_violation,
        "ON CONFLICT DO UPDATE command cannot affect row a second time",
    );
    return false;
}

/// One SET clause, bound: the expression and its target column. Parse
/// rejects duplicate SET columns.
const Set = struct {
    node: *const Node,
    col: *const _table.Column,
};

/// Bind a SET list against `tbl`. `excluded` is the proposed row,
/// every column in Column.pos order, that EXCLUDED.col reads in
///   INSERT INTO t (id, n) VALUES (1, 5)
///     ON CONFLICT (id) DO UPDATE SET n = t.n + EXCLUDED.n
/// (EXCLUDED.n is 5, a constant once bound); null for an UPDATE,
/// which has no proposed row. null = replied.
fn bindSets(
    self: *Handler,
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    clauses: []const _parse.SetClause,
    excluded: ?[]const _value.Value,
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
        const node = (try _expr.bindOrReply(
            state,
            arloc,
            tbl,
            col,
            clause.value,
            self.params,
            excluded,
        )) orelse return null;
        set.* = .{ .node = node, .col = col };
    }
    return sets;
}

/// The post of one row under `sets`: the pre-image `item` decoded,
/// each SET evaluated against the pre-image (so `SET a = b, b = a`
/// swaps) and laid over its cell; NULL into a NOT NULL column is
/// 23502. `row` is the decode scratch, reused across a statement's
/// rows; the cells are copied out of it and borrow the item's value,
/// which the arena keeps alive. null = replied.
fn evalSets(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    table_name: String,
    item: zdb.kv.Item,
    sets: []const Set,
    row: *_value.Row,
) !?[]const _value.Value {
    try _row.unmarshalRow(arloc, &tbl.ps, item.newest().value, &.{}, row);
    // The SET values outlive the row, so they compute into the arena.
    const pre: _expr.Row = .{ .scratch = arloc, .tbl = tbl, .item = item };
    for (sets) |set| {
        const col = set.col;
        const val = set.node.eval(pre, col) catch |err| {
            try replyEvalErr(state, arloc, err, col);
            return null;
        };
        if (val == .null_value and col.not_null) {
            try replyNullViolation(state, arloc, table_name, col);
            return null;
        }
        row.set(col.pos, val);
    }
    return try arloc.dupe(_value.Value, row.cells());
}

/// Describe of a prepared `SELECT id, n FROM t`, before any Execute:
/// the RowDescription `select` would send. The Table comes from the
/// database's cache under the cache's shared lock: forgetTable
/// destroys an entry only under the exclusive one, so the Table lives
/// for as long as the hold, and no table lock or txn is needed — which
/// matters, since every prepared SELECT sends a Describe. The names
/// are copied out before the hold ends and the reply goes out after
/// it: nothing may write to the socket under the cache lock. A miss
/// (the first touch of the table in this process) loads the schema the
/// way a statement does, under the table lock (describeSelectLoad).
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
        const r = projectionOf(arloc, hold.tbl, sel, self.params);
        if (r) |proj| {
            for (proj.fields) |*fd| fd.name = try arloc.dupe(u8, fd.name);
        } else |_| {}
        break :blk r;
    };

    const proj = resolved catch |err| {
        if (err == error.OutOfMemory) return err;
        return _expr.replyBindErr(state, arloc, err, null);
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
    const proj = (try selectFields(state, arloc, tbl, sel, self.params)) orelse
        return;
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
    /// The output expressions; egid is a column like any other.
    outs: []const *const Node,
    fields: []pgwire.FieldDescription,
};

/// Bind `sel`'s projection: `*` is the user columns; a target is an
/// expression over the row (expr.bind), typed by what it computes.
/// Pure: the expression binder's errors. The field names borrow the
/// Table or the statement.
fn projectionOf(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    sel: _parse.Select,
    params: _parse.Params,
) _expr.BindError!SelectFields {
    var outs: std.ArrayList(*const Node) = .empty;
    var names: std.ArrayList(String) = .empty;
    switch (sel.projection) {
        .star => for (tbl.columns) |*c| {
            const node = try arloc.create(Node);
            node.* = .{ .column = c };
            try outs.append(arloc, node);
            try names.append(arloc, c.name);
        },
        .targets => |targets| for (targets) |t| {
            try outs.append(
                arloc,
                try _expr.bind(arloc, tbl, t.expr, null, params, null),
            );
            try names.append(arloc, t.name);
        },
        .count_star => {},
    }

    const is_count = sel.projection == .count_star;
    const fields = try arloc.alloc(
        pgwire.FieldDescription,
        if (is_count) 1 else outs.items.len,
    );
    if (is_count) {
        fields[0] = .{ .name = "count", .type_oid = pgwire.oid.int8 };
    } else for (outs.items, names.items, fields) |node, name, *fd| {
        // A NULL-only target has no type; text is what Postgres
        // advertises for an unknown.
        const col: ?*const _table.Column = switch (node.*) {
            .column => |c| c,
            else => null,
        };
        fd.* = .{
            .name = name,
            .type_oid = _value.pgTypeOid(node.ty() orelse .text),
            .type_modifier = _value.pgTypeModifier(
                if (col) |c| c.type_mod else null,
            ),
            .format_code = 0,
        };
    }
    return .{ .outs = outs.items, .fields = fields };
}

/// projectionOf with the reply: null = the bind error was sent.
/// `select`'s RowEmitter takes the expressions.
fn selectFields(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    sel: _parse.Select,
    params: _parse.Params,
) !?SelectFields {
    return projectionOf(arloc, tbl, sel, params) catch |err| {
        if (err == error.OutOfMemory) return err;
        try _expr.replyBindErr(state, arloc, err, null);
        return null;
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
    outs: []const *const Node,
    /// What a computed target needs for one row; reset per row.
    scratch: std.heap.ArenaAllocator,
    count: u64 = 0,
    /// Execute's row limit; 0 = none.
    limit: u32 = 0,

    /// Whether the limit's rows are all out.
    pub fn full(self: *const RowEmitter) bool {
        return self.limit != 0 and self.count >= self.limit;
    }

    /// Project and send one row. A bare column is decoded on its own
    /// (O(1), Table.read, no full-row decode) and printed with its
    /// column's padding; any other target is computed into `scratch`.
    pub fn emit(self: *RowEmitter, item: zdb.kv.Item) !void {
        _ = self.scratch.reset(.retain_capacity);
        const row: _expr.Row = .{
            .scratch = self.scratch.allocator(),
            .tbl = self.tbl,
            .item = item,
        };
        var dw = try self.state.beginDataRow(@intCast(self.outs.len));
        for (self.outs) |node| {
            const col: ?*const _table.Column = switch (node.*) {
                .column => |c| c,
                else => null,
            };
            const v = if (col) |c|
                self.tbl.parseCol(item, c)
            else
                try node.cell(row);
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
/// began after the build. Readers never bump: the planner scans
/// instead of using an index newer than the snapshot. Returns false
/// when an error was replied (Conflict → 40001); a Restart propagates
/// to the statement driver.
fn bumpToIndexes(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tx: *_txn.Txn,
    tbl: *const _table.Table,
) !bool {
    for (try tbl.uniqueIndexes(arloc, null)) |idx| {
        tx.bumpTo(idx.valid_ts) catch |err| {
            try replyLockErr(state, err);
            return false;
        };
    }
    return true;
}

/// Bind the WHERE (null: every row) and plan it. null = bind already
/// sent the error reply (the pred.bindOrReply convention).
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
        pred = (try _pred.bindOrReply(state, arloc, tbl, tree, params)) orelse
            return null;
    }
    return try _plan.plan(arloc, tbl, pred, opts);
}

/// litToValue plus INSERT's client-facing error mapping, and the one
/// non-constant literal: CURRENT_TIMESTAMP is the transaction's start
/// (`now_us`, Unix µs), typed by the column it lands in — a timestamp
/// column, else 42804. A `$n` is the bound value (`params`). null =
/// the error reply was already sent (the pred.bindOrReply convention).
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
        else => replyExprErr(state, err),
    };
}

/// An expression's own failure, no column involved: arithmetic, a
/// function's argument, bytes that are not UTF-8.
fn replyExprErr(state: *pgwire.ConnState, err: _expr.EvalError) !void {
    return switch (err) {
        error.OutOfMemory, error.StringTooLong => unreachable,
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
        error.SubstringLength => replyErrorCode(
            state,
            pgwire.sqlstate.substring_error,
            "negative substring length not allowed",
        ),
        error.PowerArgument => replyErrorCode(
            state,
            pgwire.sqlstate.invalid_argument_for_power_function,
            "invalid argument for power function",
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
/// id; `uniques` goes to the probe. false = replied.
fn applyIndexKeys(
    state: *pgwire.ConnState,
    tx: *_txn.Txn,
    keys: []const IndexKey,
    changes: []const Change,
    uniques: []const Unique,
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

    if (!try probeUnique(state, tx, probes.items, changes, uniques))
        return false;
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
/// exists: 23505 — unless the key is a new row's under an index ON
/// CONFLICT resolved against (`uniques`): INSERT INTO t VALUES (1, 5)
/// ON CONFLICT (id) DO NOTHING found no id 1 at the gather and another
/// transaction committed one before the key lock; nothing is written
/// yet, and the statement restarts to gather it. Only a unique index table may
/// be read this way: a non-unique one's pending posting ops sit in
/// `writes` keyed with timestamp = egid. false = replied (23505, or an
/// internal read failure).
fn probeUnique(
    state: *pgwire.ConnState,
    tx: *_txn.Txn,
    probes: []const IndexKey,
    changes: []const Change,
    uniques: []const Unique,
) !bool {
    var names: std.ArrayList(String) = .empty;
    defer names.deinit(tx.allocator);
    var i: usize = 0;
    while (i < probes.len) {
        // One index read per group: a yield point each.
        try fiber.ctx().maybeYield();

        const index_table_id = probes[i].idx.index_table_id;
        utils.clearRetainCap(&names);

        const start = i;
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

        for (probes[start..i], names.items) |ik, key| {
            if (res.item(key) == null) continue;
            if (changes[ik.row].pre == null and isUnique(uniques, ik.idx))
                return error.Restart;
            return replyUniqueViolation(state);
        }
    }
    return true;
}

/// Whether `idx` is one the ON CONFLICT resolved against.
fn isUnique(uniques: []const Unique, idx: *const _table.Index) bool {
    for (uniques) |unique| if (unique.idx == idx) return true;
    return false;
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
        // The WHERE computed something it could not: PG's reply.
        error.IntOutOfRange,
        error.FloatOutOfRange,
        error.DivisionByZero,
        error.SubstringLength,
        error.PowerArgument,
        error.InvalidUtf8,
        => replyExprErr(state, @errorCast(err)),
        else => {
            log.err(
                verb ++ " '{s}': scan: {s}",
                .{ table_name, @errorName(err) },
            );
            return replyError(state, "scan failed");
        },
    };
}
