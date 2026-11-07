// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The planner: the one place that chooses index vs. scan. A bound
//! Pred (pred.zig) in, a tree of exec nodes out — `materialize →
//! [sort →] (seq_scan | fetch(bitmap | walk))` — with the WHOLE
//! predicate re-checked on the source, so a bitmap only has to be a
//! SUPERSET of the rows and no remainder is ever computed. Files form
//! a DAG: plan → exec → pred; dml.zig calls `plan` and drives the
//! tree.

const std = @import("std");
const zdb = @import("zdb");

const _exec = @import("exec.zig");
const _key = @import("key.zig");
const _parse = @import("parse.zig");
const _pred = @import("pred.zig");
const _table = @import("table.zig");
const _value = @import("value.zig");
const String = _value.String;

const BitmapNode = _exec.BitmapNode;
const IndexRange = _exec.IndexRange;
const IndexWalk = _exec.IndexWalk;
const Pred = _pred.Pred;
const RowNode = _exec.RowNode;
const zroar = zdb.zroar;

/// Planning allocates and nothing else can fail.
const Error = std.mem.Allocator.Error;

pub const Options = struct {
    /// The statement's snapshot. An index is read only when it is
    /// complete at this ts (`Index.valid_ts <= read_ts`); otherwise its
    /// backfilled entries would be invisible, so the rows are scanned.
    read_ts: u64,
    /// Copy row values into the batch; false only for a key-only
    /// consumer (unindexed DELETE).
    keep_value: bool = true,
    /// LIMIT: stop after this many rows. Under a locking statement
    /// that is how many rows are gathered and locked.
    limit: ?usize = null,
    /// count(*): the rows are never looked at, so a sequential scan
    /// counts the ones that pass the predicate into `Batch.counted`
    /// instead of appending them.
    count_only: bool = false,
    /// ORDER BY: the bound keys, first key first; empty = the source's
    /// order. An index walked in key order is the ordering when it can
    /// be (walkFor); else a Sort node gathers every row before the
    /// first goes out.
    order_by: []const _exec.SortKey = &.{},
    /// OFFSET: rows dropped before the first one produced, after the
    /// ordering.
    offset: usize = 0,
};

pub const Plan = struct {
    /// Always a Materialize — the root that bounds the result and, for
    /// a locking statement (ExecCtx.lock), gathers and locks it — over
    /// the source: a seq_scan, or a fetch over a bitmap tree or an
    /// index walk.
    root: *RowNode,

    /// Frees the tree's resources; the nodes' memory belongs to the
    /// statement arena.
    pub fn deinit(self: *Plan) void {
        self.root.deinit();
    }
};

/// Build the plan for `pred` (null: every row) over `tbl`. Nodes,
/// literal egid sets and encoded index keys live in `arloc`, the
/// statement arena. Choosing is only about which rows are READ — the
/// source re-checks the whole predicate per row, so a term the bitmap
/// already decided costs one more cell compare, never a wrong answer.
pub fn plan(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    pred: ?*const Pred,
    opts: Options,
) !Plan {
    const src = try arloc.create(RowNode);
    const walk = try walkFor(arloc, tbl, opts.read_ts, pred, opts.order_by);
    if (walk) |w| {
        src.* = .{ .fetch = .{
            .egids = .{ .walk = w },
            .pred = pred,
            .keep_value = opts.keep_value,
        } };
    } else {
        var bitmap: ?BitmapNode = null;
        if (pred) |p| bitmap = try bitmapFor(arloc, tbl, opts.read_ts, p);
        errdefer if (bitmap) |*bm| bm.deinit();

        if (bitmap) |bm| {
            const bnode = try arloc.create(BitmapNode);
            bnode.* = bm;
            src.* = .{ .fetch = .{
                .egids = .{ .set = .{ .node = bnode } },
                .pred = pred,
                .keep_value = opts.keep_value,
            } };
        } else {
            src.* = .{ .seq_scan = .{
                .pred = pred,
                .keep_value = opts.keep_value,
                .count_only = opts.count_only,
            } };
        }
    }
    var child = src;
    if (walk == null and opts.order_by.len > 0) {
        child = try arloc.create(RowNode);
        child.* = .{ .sort = .{ .child = src, .keys = opts.order_by } };
    }
    const root = try arloc.create(RowNode);
    root.* = .{ .materialize = .{
        .child = child,
        .remaining = opts.limit,
        .skip = opts.offset,
    } };
    return .{ .root = root };
}

/// `SELECT * FROM t WHERE n > 5 ORDER BY n`: the index on n walked in
/// key order IS the ordering — ASC with NULLs last, the key encoding's
/// own — so the rows need no sort; `ORDER BY n DESC`, NULLs first, is
/// the same walk in reverse. The two other NULL placements sort. Only
/// for one key, a bare indexed column, and a WHERE that is comparisons
/// on that column (each one narrows the walk) or absent: any other
/// term would have the walk read the table through the index and
/// filter, which a bitmap on that term plus a sort beats without
/// knowing the table.
fn walkFor(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    read_ts: u64,
    pred: ?*const Pred,
    order_by: []const _exec.SortKey,
) Error!?IndexWalk {
    if (order_by.len != 1) return null;

    const key = order_by[0];
    if (key.desc != key.nulls_first) return null;
    const dir: zdb.kv.Direction = if (key.desc) .reverse else .forward;

    const col = switch (key.node.*) {
        .column => |col| col,
        else => return null,
    };
    const idx = validIndex(tbl, col, read_ts) orelse return null;

    var range = IndexRange{
        .index_table_id = idx.index_table_id,
        .lo = _key.values_from,
        .hi = _key.keys_end,
        .unique = idx.unique,
    };
    const terms: []const Pred = if (pred) |p| p.topLevelAnds() else &.{};
    for (terms) |*term| {
        const cmp = switch (term.*) {
            .cmp => |cmp| cmp,
            else => return null,
        };
        if (cmp.lhs.* != .column or cmp.lhs.column != col) return null;
        const val = switch (cmp.rhs.*) {
            .value => |val| val,
            else => return null,
        };
        if (val == .null_value) return null;
        const term_range = (try termRange(arloc, idx, cmp.op, val)) orelse
            return null;
        range.intersect(term_range);
    }
    return .{ .range = range, .dir = dir };
}

/// The egid set `p` can be answered from — a SUPERSET of its rows
/// when the tree is only partly indexed — or null when it cannot be:
/// then the rows are found by scanning. Leaves are a bare column
/// against a constant, the shape bind leaves them in; any other
/// expression is the residual's. `egid = N` is the one-id set; `col =
/// v` on a valid index its entry (unique: the one row; non-unique: the
/// posting bitmap, as `IS NOT DISTINCT FROM v` and `IS TRUE`);
/// `col IS NULL` on a valid index its NULL keys (nullLookup). A
/// comparison against NULL can never be true (3VL), and `never` is
/// false by construction, so both are the EMPTY set. An AND intersects
/// every indexable child and can drop the rest, so one indexable child
/// is enough to route it (the dropped terms are the residual's job);
/// an OR must union EVERY child, so all of them must be indexable; NOT
/// never is.
fn bitmapFor(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    read_ts: u64,
    p: *const Pred,
) Error!?BitmapNode {
    switch (p.*) {
        .cmp => |cmp| {
            const col = switch (cmp.lhs.*) {
                .column => |col| col,
                else => return null,
            };
            const val = switch (cmp.rhs.*) {
                .value => |val| val,
                else => return null,
            };
            // `col IS NOT DISTINCT FROM NULL` is `col IS NULL`; an egid
            // is never NULL.
            if (val == .null_value) {
                if (cmp.op != .same or col.isEgid())
                    return try emptySet(arloc);
                return try nullLookup(arloc, tbl, col, read_ts);
            }
            // The egid is the row key: `egid = N` is the one-id set, and
            // a range of them is the table itself, a scan.
            if (col.isEgid()) return switch (cmp.op) {
                .eq, .same => try oneEgid(arloc, val.uint8),
                else => null,
            };
            const idx = validIndex(tbl, col, read_ts) orelse return null;
            return switch (cmp.op) {
                .eq, .same => try lookup(arloc, idx, val),
                .ne, .differs => null,
                .lt, .le, .gt, .ge => try rangeLookup(arloc, idx, cmp.op, val),
            };
        },
        .is_null => |nt| {
            if (nt.negate) return null;
            const col = switch (nt.expr.*) {
                .column => |col| col,
                else => return null,
            };
            return try nullLookup(arloc, tbl, col, read_ts);
        },
        // `col IS TRUE` is the entry under true; the negated forms
        // would need the NULL entries too, so they scan.
        .bool_test => |bt| {
            if (bt.negate) return null;
            const col = switch (bt.node.*) {
                .column => |col| col,
                else => return null,
            };
            if (bt.is == .unknown)
                return try nullLookup(arloc, tbl, col, read_ts);
            const idx = validIndex(tbl, col, read_ts) orelse return null;
            return try lookup(arloc, idx, .{ .bool_ = bt.is == .true_ });
        },
        .and_ => |kids| return try andOf(arloc, tbl, read_ts, kids),
        .or_ => |kids| return try orOf(arloc, tbl, read_ts, kids),
        .not => return null, // We can't evaluate NOT via an index. So, make BitmapNode null.
        .never => return try emptySet(arloc),
    }
}

/// `col IS NULL` from an index: the posting under the NULL tag key,
/// or under a unique index its key per NULL row, all of them in
/// [values_end, keys_end).
fn nullLookup(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    col: *const _table.Column,
    read_ts: u64,
) Error!?BitmapNode {
    const idx = validIndex(tbl, col, read_ts) orelse return null;
    if (!idx.unique) return try lookup(arloc, idx, .null_value);
    // A unique index built before NULL keys existed has no entries
    // for NULL rows; docs/design/indexes.md requires rebuilding it.
    return .{ .index_range = .{
        .index_table_id = idx.index_table_id,
        .lo = _key.values_end,
        .hi = _key.keys_end,
        .unique = true,
    } };
}

/// The index on `col` that is complete at `read_ts`, if any. An index
/// still being built, or built from a snapshot after read_ts, is
/// maintained by DML but must not be read: its backfilled entries sit
/// at ts = valid_ts, invisible to an older snapshot.
fn validIndex(
    tbl: *const _table.Table,
    col: *const _table.Column,
    read_ts: u64,
) ?*const _table.Index {
    for (tbl.indexes) |*idx| {
        if (idx.column == col and idx.valid_ts <= read_ts) return idx;
    }
    return null;
}

/// The entry of `idx` under `val`'s key — null when `val` has no
/// memcomparable encoding (interval): then the rows are found by
/// scanning.
fn lookup(
    arloc: std.mem.Allocator,
    idx: *const _table.Index,
    val: _value.Value,
) Error!?BitmapNode {
    const key = _key.encodeIndexKey(arloc, val, null) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return null;
    };
    const node = BitmapNode{ .index_lookup = .{
        .index_table_id = idx.index_table_id,
        .key = key,
        .unique = idx.unique,
    } };
    return node;
}

/// `col op v` on an index, for `< <= > >=`: the entries between two
/// memcomparable bounds, [lo, hi), read into one bitmap. `=` is
/// lookup's, a point read.
fn rangeLookup(
    arloc: std.mem.Allocator,
    idx: *const _table.Index,
    op: _parse.CmpOp,
    val: _value.Value,
) Error!?BitmapNode {
    std.debug.assert(op != .eq and op != .same);
    const range = (try termRange(arloc, idx, op, val)) orelse return null;
    return .{ .index_range = range };
}

/// The index-key bounds [lo, hi) of `col op v`, for `= < <= > >=`.
/// Every value key starts with the value tag and NULL's lone tag byte
/// sorts after them all, so an open side ends at those
/// (key.values_from / values_end); the key followed by a zero byte is
/// the smallest key above it, which makes a bound exclusive on the
/// left or inclusive on the right. `<>` has no bounds (null), nor
/// does a value with no encoding.
fn termRange(
    arloc: std.mem.Allocator,
    idx: *const _table.Index,
    op: _parse.CmpOp,
    val: _value.Value,
) Error!?IndexRange {
    const key = _key.encodeIndexKey(arloc, val, null) catch |err| {
        if (err == error.OutOfMemory) return error.OutOfMemory;
        return null;
    };
    const after = try std.mem.concat(arloc, u8, &.{ key, &.{0} });
    const lo: String, const hi: String = switch (op) {
        .eq, .same => .{ key, after },
        .lt => .{ _key.values_from, key },
        .le => .{ _key.values_from, after },
        .gt => .{ after, _key.values_end },
        .ge => .{ key, _key.values_end },
        .ne, .differs => return null,
    };
    return .{
        .index_table_id = idx.index_table_id,
        .lo = lo,
        .hi = hi,
        .unique = idx.unique,
    };
}

/// `egid = N`: the one-id set. Literal sets are built here and owned
/// by the plan; Fetch evaluates them by clone.
fn oneEgid(arloc: std.mem.Allocator, egid: u64) Error!BitmapNode {
    var bm = try zroar.Bitmap.init(arloc);
    errdefer bm.deinit();
    _ = try bm.set(egid);
    return .{ .egids = bm };
}

/// No row at all — a comparison against NULL. Reads nothing, records
/// nothing.
fn emptySet(arloc: std.mem.Allocator) Error!BitmapNode {
    return .{ .egids = try zroar.Bitmap.init(arloc) };
}

fn isEmptySet(node: BitmapNode) bool {
    return node == .egids and node.egids.isEmpty();
}

/// AND: the intersection of every indexable child — a superset of the
/// AND's rows when some child is not. One empty child empties it all
/// and is then the whole answer: nothing is read for the others. Two
/// ranges on one index become the one range between them (narrowRange).
fn andOf(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    read_ts: u64,
    kids: []const Pred,
) Error!?BitmapNode {
    var nodes: std.ArrayList(BitmapNode) = .empty;
    errdefer deinitNodes(arloc, &nodes);

    for (kids) |*kid| {
        var node = (try bitmapFor(arloc, tbl, read_ts, kid)) orelse continue;
        errdefer node.deinit();
        if (isEmptySet(node)) {
            deinitNodes(arloc, &nodes);
            return node;
        }
        if (node == .index_range and narrowRange(nodes.items, node.index_range))
            continue;
        try nodes.append(arloc, node);
    }
    if (nodes.items.len == 0) return null;
    return try collapse(arloc, &nodes, .and_);
}

/// `n BETWEEN 2 AND 5` reaches andOf as `n >= 2 AND n <= 5`, two
/// ranges on one index, [2, end) and [start, 5]: one read of [2, 5]
/// answers both. Narrows the range already in `nodes` on the same
/// index to the overlap and reports whether `range` was taken up.
fn narrowRange(nodes: []BitmapNode, range: _exec.IndexRange) bool {
    for (nodes) |*node| {
        if (node.* != .index_range) continue;
        if (node.index_range.index_table_id != range.index_table_id) continue;
        node.index_range.intersect(range);
        return true;
    }
    return false;
}

/// OR: the union of every child — only when each one is indexable,
/// or the rows the rest would add are lost. An empty child adds
/// nothing and is dropped.
fn orOf(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    read_ts: u64,
    kids: []const Pred,
) Error!?BitmapNode {
    var nodes: std.ArrayList(BitmapNode) = .empty;
    errdefer deinitNodes(arloc, &nodes);
    for (kids) |*kid| {
        var node = (try bitmapFor(arloc, tbl, read_ts, kid)) orelse {
            deinitNodes(arloc, &nodes);
            return null;
        };
        errdefer node.deinit();
        if (isEmptySet(node)) {
            node.deinit();
            continue;
        }
        try nodes.append(arloc, node);
    }
    if (nodes.items.len == 0) return try emptySet(arloc);
    return try collapse(arloc, &nodes, .or_);
}

/// One node stands alone; more are joined under `tag`. Consumes the
/// list.
fn collapse(
    arloc: std.mem.Allocator,
    nodes: *std.ArrayList(BitmapNode),
    comptime tag: std.meta.Tag(BitmapNode),
) Error!BitmapNode {
    std.debug.assert(nodes.items.len > 0);
    if (nodes.items.len == 1) {
        const one = nodes.items[0];
        nodes.deinit(arloc);
        return one;
    }
    const kids = try nodes.toOwnedSlice(arloc);
    return @unionInit(BitmapNode, @tagName(tag), kids);
}

fn deinitNodes(
    arloc: std.mem.Allocator,
    nodes: *std.ArrayList(BitmapNode),
) void {
    for (nodes.items) |*node| node.deinit();
    nodes.deinit(arloc);
}
