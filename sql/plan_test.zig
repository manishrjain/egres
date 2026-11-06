// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Planner tests: the tree a WHERE turns into, on a hand-built table
//! — `a int4` the PK behind a unique index, `b int4` behind a
//! non-unique one, `c text` unindexed, `d bool` behind a non-unique
//! one. Each case is the SQL text of the WHERE, parsed and bound the
//! way a statement's is.

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const zdb = @import("zdb");
const String = sql.value.String;

const BitmapNode = sql.exec.BitmapNode;
const Plan = sql.plan.Plan;
const Pred = sql.pred.Pred;
const RowNode = sql.exec.RowNode;
const Table = sql.table.Table;

const pkey_index_id: u32 = 17;
const b_index_id: u32 = 18;
const d_index_id: u32 = 19;
/// The fixture's indexes are complete from ts 10; a snapshot at 50
/// reads both.
const read_ts: u64 = 50;
const built_ts: u64 = 10;
const stream = sql.plan.Options{ .read_ts = read_ts };

const Fixture = struct {
    arena: std.heap.ArenaAllocator,
    tbl: Table,

    fn init() !Fixture {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        errdefer arena.deinit();
        const a = arena.allocator();

        const logical = [_]sql.schema.LogicalColumn{
            .{ .column_id = 1, .name = "a", .ty = .int4 },
            .{ .column_id = 2, .name = "b", .ty = .int4 },
            .{ .column_id = 3, .name = "c", .ty = .text },
            .{ .column_id = 4, .name = "d", .ty = .bool_ },
        };
        const ps = try sql.schema.compile(
            a,
            .{ .columns = &logical, .current_version = 1 },
            1,
        );
        const columns = try a.alloc(sql.table.Column, 4);
        columns[0] = .{
            .column_id = 1,
            .pos = 0,
            .name = "a",
            .ty = .int4,
            .pkey_position = 0,
        };
        columns[1] = .{
            .column_id = 2,
            .pos = 1,
            .name = "b",
            .ty = .int4,
            .pkey_position = null,
        };
        columns[2] = .{
            .column_id = 3,
            .pos = 2,
            .name = "c",
            .ty = .text,
            .pkey_position = null,
        };
        columns[3] = .{
            .column_id = 4,
            .pos = 3,
            .name = "d",
            .ty = .bool_,
            .pkey_position = null,
        };
        const indexes = try a.alloc(sql.table.Index, 3);
        indexes[0] = .{
            .index_table_id = pkey_index_id,
            .unique = true,
            .valid_ts = built_ts,
            .column = &columns[0],
        };
        indexes[1] = .{
            .index_table_id = b_index_id,
            .unique = false,
            .valid_ts = built_ts,
            .column = &columns[1],
        };
        indexes[2] = .{
            .index_table_id = d_index_id,
            .unique = false,
            .valid_ts = built_ts,
            .column = &columns[3],
        };
        return .{ .arena = arena, .tbl = .{
            .columns = columns,
            .indexes = indexes,
            .ps = ps,
            .version = 1,
            .arena = std.heap.ArenaAllocator.init(testing.allocator),
        } };
    }

    fn deinit(self: *Fixture) void {
        self.tbl.deinit();
        self.arena.deinit();
    }

    /// The plan of `SELECT * FROM t WHERE <where>`; an empty `where`
    /// is no WHERE at all.
    fn planFor(
        self: *Fixture,
        where: String,
        opts: sql.plan.Options,
    ) !Plan {
        const a = self.arena.allocator();
        if (where.len == 0) return sql.plan.plan(a, &self.tbl, null, opts);
        const text = try std.fmt.allocPrint(
            a,
            "SELECT * FROM t WHERE {s}",
            .{where},
        );
        const stmt = try sql.parse.statement(a, text);
        const pred = try a.create(Pred);
        pred.* = try sql.pred.bind(a, &self.tbl, stmt.select.where.?, .{});
        return sql.plan.plan(a, &self.tbl, pred, opts);
    }

    /// `ORDER BY <name> [DESC] [NULLS FIRST]`'s key, bound to the
    /// column as dml's sortKeysOrReply binds it.
    fn sortKey(
        self: *Fixture,
        name: String,
        desc: bool,
        nulls_first: bool,
    ) !sql.exec.SortKey {
        const node = try self.arena.allocator().create(sql.expr.Node);
        node.* = .{ .column = self.tbl.column(name).? };
        return .{ .node = node, .desc = desc, .nulls_first = nulls_first };
    }
};

/// The node under the root Materialize.
fn source(p: *const Plan) *RowNode {
    return p.root.materialize.child;
}

/// The source must be a fetch; its bitmap tree.
fn bitmapOf(p: *const Plan) !*const BitmapNode {
    const src = source(p);
    try testing.expect(src.* == .fetch);
    return src.fetch.egids.set.node;
}

fn expectScan(p: *const Plan) !void {
    try testing.expect(source(p).* == .seq_scan);
}

/// The source must be a fetch over an index walk with these bounds,
/// and nothing sorts above it.
fn expectWalk(
    p: *const Plan,
    index_table_id: u32,
    unique: bool,
    lo: String,
    hi: String,
    dir: zdb.kv.Direction,
) !void {
    const src = source(p);
    try testing.expect(src.* == .fetch);
    try testing.expect(src.fetch.egids == .walk);
    try testing.expectEqual(dir, src.fetch.egids.walk.dir);
    const range = src.fetch.egids.walk.range;
    try testing.expectEqual(index_table_id, range.index_table_id);
    try testing.expectEqual(unique, range.unique);
    try testing.expectEqualSlices(u8, lo, range.lo);
    try testing.expectEqualSlices(u8, hi, range.hi);
}

/// A Sort sits under the root; its child is the source.
fn expectSorted(p: *const Plan) !*RowNode {
    const src = source(p);
    try testing.expect(src.* == .sort);
    return src.sort.child;
}

fn expectLookup(
    node: *const BitmapNode,
    index_table_id: u32,
    unique: bool,
    key: String,
) !void {
    try testing.expect(node.* == .index_lookup);
    const lu = node.index_lookup;
    try testing.expectEqual(index_table_id, lu.index_table_id);
    try testing.expectEqual(unique, lu.unique);
    try testing.expectEqualSlices(u8, key, lu.key);
}

fn expectEgids(node: *const BitmapNode, ids: []const u64) !void {
    try testing.expect(node.* == .egids);
    try testing.expectEqual(@as(u64, ids.len), node.egids.getCardinality());
    for (ids) |id| try testing.expect(node.egids.contains(id));
}

fn int4Key(a: std.mem.Allocator, v: i32) ![]u8 {
    return sql.key.encodeIndexKey(a, .{ .int4 = v }, null);
}

test "sql.plan: one equality — unique lookup, posting lookup, or scan" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const a = fx.arena.allocator();

    const k1 = try int4Key(a, 1);
    var on_a = try fx.planFor("a = 1", stream);
    defer on_a.deinit();
    try expectLookup(try bitmapOf(&on_a), pkey_index_id, true, k1);
    // The residual is the whole predicate, even when the index
    // decided it.
    try testing.expect(source(&on_a).fetch.pred.?.* == .cmp);

    var on_b = try fx.planFor("b = 2", stream);
    defer on_b.deinit();
    try expectLookup(try bitmapOf(&on_b), b_index_id, false, try int4Key(a, 2));

    var on_c = try fx.planFor("c = 'x'", stream);
    defer on_c.deinit();
    try expectScan(&on_c);
    try testing.expect(source(&on_c).seq_scan.pred.?.* == .cmp);

    var all = try sql.plan.plan(a, &fx.tbl, null, stream);
    defer all.deinit();
    try expectScan(&all);
    try testing.expect(source(&all).seq_scan.pred == null);
}

test "sql.plan: AND takes any indexable term, OR needs every one" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const a = fx.arena.allocator();
    const k1 = try int4Key(a, 1);
    const k2 = try int4Key(a, 2);

    // The unindexed term rides along as the residual; no and_ of one.
    var mixed = try fx.planFor("a = 1 AND c = 'x'", stream);
    defer mixed.deinit();
    try expectLookup(try bitmapOf(&mixed), pkey_index_id, true, k1);
    try testing.expect(source(&mixed).fetch.pred.?.* == .and_);

    var both = try fx.planFor("b = 1 AND b = 2", stream);
    defer both.deinit();
    const both_src = try bitmapOf(&both);
    try testing.expect(both_src.* == .and_);
    try testing.expectEqual(@as(usize, 2), both_src.and_.len);
    try expectLookup(&both_src.and_[0], b_index_id, false, k1);
    try expectLookup(&both_src.and_[1], b_index_id, false, k2);

    var either = try fx.planFor("b = 1 OR b = 2", stream);
    defer either.deinit();
    const either_src = try bitmapOf(&either);
    try testing.expect(either_src.* == .or_);
    try testing.expectEqual(@as(usize, 2), either_src.or_.len);
    try expectLookup(&either_src.or_[0], b_index_id, false, k1);
    try expectLookup(&either_src.or_[1], b_index_id, false, k2);

    var half = try fx.planFor("b = 1 OR c = 'x'", stream);
    defer half.deinit();
    try expectScan(&half);

    // A nested AND is indexable through any one of its terms.
    var nested = try fx.planFor("(a = 1 AND c = 'x') OR b = 2", stream);
    defer nested.deinit();
    const nested_src = try bitmapOf(&nested);
    try testing.expect(nested_src.* == .or_);
    try expectLookup(&nested_src.or_[0], pkey_index_id, true, k1);
    try expectLookup(&nested_src.or_[1], b_index_id, false, k2);

    var negated = try fx.planFor("NOT b = 1", stream);
    defer negated.deinit();
    try expectScan(&negated);
}

test "sql.plan: egid literals and NULL comparisons are egid sets" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const a = fx.arena.allocator();

    var by_egid = try fx.planFor("egid = 7", stream);
    defer by_egid.deinit();
    try expectEgids(try bitmapOf(&by_egid), &.{7});

    // `= NULL` is never true: the empty set, read from no index...
    var never = try fx.planFor("b = NULL", stream);
    defer never.deinit();
    try expectEgids(try bitmapOf(&never), &.{});

    // ...and it empties any AND it sits in, from either side, so the
    // other terms are not even looked up.
    var and_last = try fx.planFor("a = 1 AND b = NULL", stream);
    defer and_last.deinit();
    try expectEgids(try bitmapOf(&and_last), &.{});
    var and_first = try fx.planFor("b = NULL AND a = 1", stream);
    defer and_first.deinit();
    try expectEgids(try bitmapOf(&and_first), &.{});

    // In an OR it adds nothing and drops out.
    const k1 = try int4Key(a, 1);
    var or_rest = try fx.planFor("b = NULL OR a = 1", stream);
    defer or_rest.deinit();
    try expectLookup(try bitmapOf(&or_rest), pkey_index_id, true, k1);
    var or_none = try fx.planFor("b = NULL OR c = NULL", stream);
    defer or_none.deinit();
    try expectEgids(try bitmapOf(&or_none), &.{});
}

test "sql.plan: IN, BETWEEN, IS NOT DISTINCT FROM and IS TRUE read an index" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const a = fx.arena.allocator();
    const k1 = try int4Key(a, 1);
    const k2 = try int4Key(a, 2);
    const null_key = try sql.key.encodeIndexKey(a, .null_value, null);

    // b IN (1, 2) is the union of two entries; NOT IN scans.
    var in = try fx.planFor("b IN (1, 2)", stream);
    defer in.deinit();
    const in_src = try bitmapOf(&in);
    try testing.expect(in_src.* == .or_);
    try expectLookup(&in_src.or_[0], b_index_id, false, k1);
    try expectLookup(&in_src.or_[1], b_index_id, false, k2);
    var not_in = try fx.planFor("b NOT IN (1, 2)", stream);
    defer not_in.deinit();
    try expectScan(&not_in);

    // b BETWEEN 1 AND 2 is one range read, [1, 2]: the lower bound of
    // `>= 1` and the upper of `<= 2` (the key followed by a zero byte).
    var between = try fx.planFor("b BETWEEN 1 AND 2", stream);
    defer between.deinit();
    const bt_src = try bitmapOf(&between);
    try testing.expect(bt_src.* == .index_range);
    try testing.expectEqualSlices(u8, k1, bt_src.index_range.lo);
    const k2_after = try std.mem.concat(a, u8, &.{ k2, &.{0} });
    try testing.expectEqualSlices(u8, k2_after, bt_src.index_range.hi);
    // The same two comparisons written out, and a third range on the
    // same index, narrow to one node too; a lookup beside them stays.
    var written = try fx.planFor("b >= 1 AND b <= 2 AND b < 2 AND a = 1", stream);
    defer written.deinit();
    const wr_src = try bitmapOf(&written);
    try testing.expect(wr_src.* == .and_);
    try testing.expectEqual(@as(usize, 2), wr_src.and_.len);
    try testing.expect(wr_src.and_[0] == .index_range);
    try testing.expectEqualSlices(u8, k1, wr_src.and_[0].index_range.lo);
    try testing.expectEqualSlices(u8, k2, wr_src.and_[0].index_range.hi);
    try expectLookup(&wr_src.and_[1], pkey_index_id, true, k1);

    // IS NOT DISTINCT FROM 1 is the entry under 1; FROM NULL the NULL
    // posting; IS DISTINCT FROM scans.
    var same = try fx.planFor("b IS NOT DISTINCT FROM 1", stream);
    defer same.deinit();
    try expectLookup(try bitmapOf(&same), b_index_id, false, k1);
    var same_null = try fx.planFor("b IS NOT DISTINCT FROM NULL", stream);
    defer same_null.deinit();
    try expectLookup(try bitmapOf(&same_null), b_index_id, false, null_key);
    var dist = try fx.planFor("b IS DISTINCT FROM 1", stream);
    defer dist.deinit();
    try expectScan(&dist);

    // d IS TRUE and a bare d are the entry under true; IS UNKNOWN the
    // NULL posting; IS NOT TRUE scans.
    const true_key = try sql.key.encodeIndexKey(a, .{ .bool_ = true }, null);
    var is_true = try fx.planFor("d IS TRUE", stream);
    defer is_true.deinit();
    try expectLookup(try bitmapOf(&is_true), d_index_id, false, true_key);
    var bare = try fx.planFor("d", stream);
    defer bare.deinit();
    try expectLookup(try bitmapOf(&bare), d_index_id, false, true_key);
    var unknown = try fx.planFor("d IS UNKNOWN", stream);
    defer unknown.deinit();
    try expectLookup(try bitmapOf(&unknown), d_index_id, false, null_key);
    var not_true = try fx.planFor("d IS NOT TRUE", stream);
    defer not_true.deinit();
    try expectScan(&not_true);
}

test "sql.plan: IS NULL reads a posting index, never a unique one" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const a = fx.arena.allocator();

    var on_b = try fx.planFor("b IS NULL", stream);
    defer on_b.deinit();
    const null_key = try sql.key.encodeIndexKey(a, .null_value, null);
    try expectLookup(try bitmapOf(&on_b), b_index_id, false, null_key);

    // Unique NULL entries are not used for this predicate yet.
    var on_a = try fx.planFor("a IS NULL", stream);
    defer on_a.deinit();
    try expectScan(&on_a);

    var not_null = try fx.planFor("b IS NOT NULL", stream);
    defer not_null.deinit();
    try expectScan(&not_null);

    var on_c = try fx.planFor("c IS NULL", stream);
    defer on_c.deinit();
    try expectScan(&on_c);
}

test "sql.plan: a mid-backfill index is not read" {
    var fx = try Fixture.init();
    defer fx.deinit();
    fx.tbl.indexes[1].valid_ts = sql.catalog.IndexDef.building;

    var on_b = try fx.planFor("b = 2", stream);
    defer on_b.deinit();
    try expectScan(&on_b);

    // Not even as one OR arm.
    var either = try fx.planFor("a = 1 OR b = 2", stream);
    defer either.deinit();
    try expectScan(&either);
}

test "sql.plan: an index is read only by a snapshot at or after its build" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const a = fx.arena.allocator();
    const older = sql.plan.Options{ .read_ts = built_ts - 1 };

    // A snapshot older than the build cannot see the backfilled
    // entries: it scans, whichever index it is.
    var before = try fx.planFor("b = 2", older);
    defer before.deinit();
    try expectScan(&before);
    var pk_before = try fx.planFor("a = 1", older);
    defer pk_before.deinit();
    try expectScan(&pk_before);

    // An index made with its table (valid from 0) serves any snapshot.
    fx.tbl.indexes[0].valid_ts = 0;
    var pk_old = try fx.planFor("a = 1", older);
    defer pk_old.deinit();
    const k1 = try int4Key(a, 1);
    try expectLookup(try bitmapOf(&pk_old), pkey_index_id, true, k1);

    // Exactly at the build's ts the entries are visible.
    var at = try fx.planFor("b = 2", .{ .read_ts = built_ts });
    defer at.deinit();
    try expectLookup(try bitmapOf(&at), b_index_id, false, try int4Key(a, 2));
}

test "sql.plan: the root materializes; keep_value and limit reach it" {
    var fx = try Fixture.init();
    defer fx.deinit();

    var plain = try fx.planFor("b = 2", stream);
    defer plain.deinit();
    try testing.expect(plain.root.* == .materialize);
    try testing.expectEqual(null, plain.root.materialize.remaining);
    const src = source(&plain);
    try testing.expect(src.* == .fetch);
    try testing.expect(src.fetch.keep_value);

    var keys_only = try fx.planFor("c = 'x'", .{
        .read_ts = read_ts,
        .keep_value = false,
    });
    defer keys_only.deinit();
    try testing.expect(keys_only.root.* == .materialize);
    try testing.expect(!source(&keys_only).seq_scan.keep_value);

    // LIMIT sits on the root, under which the source is bounded — so a
    // locking statement gathers and locks only that many.
    var capped = try fx.planFor("b = 2", .{ .read_ts = read_ts, .limit = 5 });
    defer capped.deinit();
    try testing.expectEqual(@as(?usize, 5), capped.root.materialize.remaining);
    try testing.expect(source(&capped).* == .fetch);
}

test "sql.plan: never is the empty set, and a scan under NOT" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const a = fx.arena.allocator();

    const never: Pred = .never;
    var alone = try sql.plan.plan(a, &fx.tbl, &never, stream);
    defer alone.deinit();
    try expectEgids(try bitmapOf(&alone), &.{});

    // It empties an AND and drops out of an OR, like `= NULL`.
    const a_eq_1 = try sql.pred.colCmp(a, fx.tbl.column("a").?, .eq, .{ .int4 = 1 });
    const and_kids = [_]Pred{ a_eq_1, never };
    const and_ = Pred{ .and_ = &and_kids };
    var both = try sql.plan.plan(a, &fx.tbl, &and_, stream);
    defer both.deinit();
    try expectEgids(try bitmapOf(&both), &.{});

    const or_kids = [_]Pred{ never, a_eq_1 };
    const or_ = Pred{ .or_ = &or_kids };
    var either = try sql.plan.plan(a, &fx.tbl, &or_, stream);
    defer either.deinit();
    try expectLookup(try bitmapOf(&either), pkey_index_id, true, try int4Key(a, 1));

    // NOT never is every row: no index answers it, so a scan.
    const not_ = Pred{ .not = &never };
    var negated = try sql.plan.plan(a, &fx.tbl, &not_, stream);
    defer negated.deinit();
    try expectScan(&negated);
}

fn expectRange(
    node: *const BitmapNode,
    index_table_id: u32,
    unique: bool,
    lo: String,
    hi: String,
) !void {
    try testing.expect(node.* == .index_range);
    const r = node.index_range;
    try testing.expectEqual(index_table_id, r.index_table_id);
    try testing.expectEqual(unique, r.unique);
    try testing.expectEqualSlices(u8, lo, r.lo);
    try testing.expectEqualSlices(u8, hi, r.hi);
}

test "sql.plan: an ordered comparison on an indexed column is an index range" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const a = fx.arena.allocator();
    const k1 = try int4Key(a, 1);
    const after1 = try std.mem.concat(a, u8, &.{ k1, &.{0} });
    const from = sql.key.values_from;
    const end = sql.key.values_end;

    var gt = try fx.planFor("a > 1", stream);
    defer gt.deinit();
    try expectRange(try bitmapOf(&gt), pkey_index_id, true, after1, end);
    var ge = try fx.planFor("1 <= a", stream);
    defer ge.deinit();
    try expectRange(try bitmapOf(&ge), pkey_index_id, true, k1, end);
    var lt = try fx.planFor("b < 1", stream);
    defer lt.deinit();
    try expectRange(try bitmapOf(&lt), b_index_id, false, from, k1);
    var le = try fx.planFor("b <= 1", stream);
    defer le.deinit();
    try expectRange(try bitmapOf(&le), b_index_id, false, from, after1);
    // The residual is still the whole predicate.
    try testing.expect(source(&le).fetch.pred.?.* == .cmp);

    // Two ranges intersect; a range beside an unindexed OR term scans.
    var both = try fx.planFor("a > 1 AND b <= 1", stream);
    defer both.deinit();
    const kids = (try bitmapOf(&both)).and_;
    try testing.expectEqual(@as(usize, 2), kids.len);
    try expectRange(&kids[0], pkey_index_id, true, after1, end);
    try expectRange(&kids[1], b_index_id, false, from, after1);
    var mixed = try fx.planFor("a > 1 OR c = 'x'", stream);
    defer mixed.deinit();
    try expectScan(&mixed);

    // No index reads `<>`, an unindexed column, an egid range, or NULL.
    var ne = try fx.planFor("a <> 1", stream);
    defer ne.deinit();
    try expectScan(&ne);
    var on_c = try fx.planFor("c > 'x'", stream);
    defer on_c.deinit();
    try expectScan(&on_c);
    var egid = try fx.planFor("egid > 2", stream);
    defer egid.deinit();
    try expectScan(&egid);
    var null_cmp = try fx.planFor("a > NULL", stream);
    defer null_cmp.deinit();
    try expectEgids(try bitmapOf(&null_cmp), &.{});
}

test "sql.plan: an expression leaf scans; a bare-column leaf beside it indexes" {
    var fx = try Fixture.init();
    defer fx.deinit();

    // No index answers a computed side: the whole predicate is residual.
    var computed = try fx.planFor("a + 1 = 2", stream);
    defer computed.deinit();
    try expectScan(&computed);
    var called = try fx.planFor("lower(c) = 'x'", stream);
    defer called.deinit();
    try expectScan(&called);
    var nulled = try fx.planFor("nullif(a, 1) IS NULL", stream);
    defer nulled.deinit();
    try expectScan(&nulled);

    // In an AND the indexable term drives the read; the expression is
    // re-checked on the rows it yields.
    var mixed = try fx.planFor("a = 1 AND length(c) > 2", stream);
    defer mixed.deinit();
    try expectLookup(try bitmapOf(&mixed), pkey_index_id, true, try int4Key(fx.arena.allocator(), 1));
    try testing.expect(source(&mixed).fetch.pred.?.* == .and_);
}

test "sql.plan: ORDER BY on an indexed column walks the index in key order" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const a = fx.arena.allocator();
    const k1 = try int4Key(a, 1);
    const after1 = try std.mem.concat(a, u8, &.{ k1, &.{0} });
    const k5 = try int4Key(a, 5);
    const after5 = try std.mem.concat(a, u8, &.{ k5, &.{0} });
    const from = sql.key.values_from;
    const end = sql.key.values_end;
    const keys_end = sql.key.keys_end;
    const by_b = sql.plan.Options{
        .read_ts = read_ts,
        .order_by = &.{try fx.sortKey("b", false, false)},
    };

    // No WHERE: the whole index, NULL's entry last. A term on the
    // column narrows the walk; `=` is one key's span.
    var whole = try fx.planFor("", by_b);
    defer whole.deinit();
    try expectWalk(&whole, b_index_id, false, from, keys_end, .forward);
    var gt = try fx.planFor("b > 1", by_b);
    defer gt.deinit();
    try expectWalk(&gt, b_index_id, false, after1, end, .forward);
    try testing.expect(source(&gt).fetch.pred.?.* == .cmp);
    var between = try fx.planFor("b > 1 AND b <= 5", by_b);
    defer between.deinit();
    try expectWalk(&between, b_index_id, false, after1, after5, .forward);
    var eq = try fx.planFor("b = 5", by_b);
    defer eq.deinit();
    try expectWalk(&eq, b_index_id, false, k5, after5, .forward);
    var unique = try fx.planFor("a >= 1", .{
        .read_ts = read_ts,
        .order_by = &.{try fx.sortKey("a", false, false)},
    });
    defer unique.deinit();
    try expectWalk(&unique, pkey_index_id, true, k1, end, .forward);

    // A term on another column, or one no range answers, sorts the
    // bitmap's rows instead: the walk would read the table through
    // the index.
    var other = try fx.planFor("b > 1 AND a = 1", by_b);
    defer other.deinit();
    const under = try expectSorted(&other);
    try testing.expect(under.* == .fetch and under.fetch.egids == .set);
    var or_ = try fx.planFor("b > 1 OR b < 0", by_b);
    defer or_.deinit();
    _ = try expectSorted(&or_);
    var ne = try fx.planFor("b <> 1", by_b);
    defer ne.deinit();
    try testing.expect((try expectSorted(&ne)).* == .seq_scan);
    var null_cmp = try fx.planFor("b = NULL", by_b);
    defer null_cmp.deinit();
    _ = try expectSorted(&null_cmp);

    // DESC, NULLs first, is the index order reversed, over the same
    // range a WHERE narrows it to; the other two NULL placements, two
    // keys and an unindexed column sort.
    var desc = try fx.planFor("", .{
        .read_ts = read_ts,
        .order_by = &.{try fx.sortKey("b", true, true)},
    });
    defer desc.deinit();
    try expectWalk(&desc, b_index_id, false, from, keys_end, .reverse);
    var desc_gt = try fx.planFor("b > 1", .{
        .read_ts = read_ts,
        .order_by = &.{try fx.sortKey("b", true, true)},
    });
    defer desc_gt.deinit();
    try expectWalk(&desc_gt, b_index_id, false, after1, end, .reverse);
    var desc_nulls_last = try fx.planFor("", .{
        .read_ts = read_ts,
        .order_by = &.{try fx.sortKey("b", true, false)},
    });
    defer desc_nulls_last.deinit();
    _ = try expectSorted(&desc_nulls_last);
    var nulls_first = try fx.planFor("", .{
        .read_ts = read_ts,
        .order_by = &.{try fx.sortKey("b", false, true)},
    });
    defer nulls_first.deinit();
    _ = try expectSorted(&nulls_first);
    var two = try fx.planFor("", .{
        .read_ts = read_ts,
        .order_by = &.{
            try fx.sortKey("b", false, false),
            try fx.sortKey("a", false, false),
        },
    });
    defer two.deinit();
    _ = try expectSorted(&two);
    var on_c = try fx.planFor("", .{
        .read_ts = read_ts,
        .order_by = &.{try fx.sortKey("c", false, false)},
    });
    defer on_c.deinit();
    try testing.expect((try expectSorted(&on_c)).* == .seq_scan);
    // An index still being built is not walked either.
    var early = try fx.planFor("", .{
        .read_ts = built_ts - 1,
        .order_by = &.{try fx.sortKey("b", false, false)},
    });
    defer early.deinit();
    _ = try expectSorted(&early);
}

test "sql.plan: ORDER BY walks a unique index through its NULL keys" {
    var fx = try Fixture.init();
    defer fx.deinit();
    // b behind a unique index, nullable: its NULL rows have keys too,
    // so the whole walk is every row (#23).
    fx.tbl.indexes[1].unique = true;
    const from = sql.key.values_from;
    const keys_end = sql.key.keys_end;

    var asc = try fx.planFor("", .{
        .read_ts = read_ts,
        .order_by = &.{try fx.sortKey("b", false, false)},
    });
    defer asc.deinit();
    try expectWalk(&asc, b_index_id, true, from, keys_end, .forward);
    var desc = try fx.planFor("", .{
        .read_ts = read_ts,
        .order_by = &.{try fx.sortKey("b", true, true)},
    });
    defer desc.deinit();
    try expectWalk(&desc, b_index_id, true, from, keys_end, .reverse);
}
