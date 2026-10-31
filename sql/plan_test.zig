// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Planner tests: the tree a WHERE turns into, on a hand-built table
//! — `a int4` the PK behind a unique index, `b int4` behind a
//! non-unique one, `c text` unindexed. Each case is the SQL text of
//! the WHERE, parsed and bound the way a statement's is.

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const String = sql.value.String;

const BitmapNode = sql.exec.BitmapNode;
const Plan = sql.plan.Plan;
const Pred = sql.pred.Pred;
const RowNode = sql.exec.RowNode;
const Table = sql.table.Table;

const pkey_index_id: u32 = 17;
const b_index_id: u32 = 18;
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
        };
        const ps = try sql.schema.compile(
            a,
            .{ .columns = &logical, .current_version = 1 },
            1,
        );
        const columns = try a.alloc(sql.table.Column, 3);
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
        const indexes = try a.alloc(sql.table.Index, 2);
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

    /// The plan of `SELECT * FROM t WHERE <where>`.
    fn planFor(
        self: *Fixture,
        where: String,
        opts: sql.plan.Options,
    ) !Plan {
        const a = self.arena.allocator();
        const text = try std.fmt.allocPrint(
            a,
            "SELECT * FROM t WHERE {s}",
            .{where},
        );
        const stmt = try sql.parse.statement(a, text);
        const pred = try a.create(Pred);
        pred.* = try sql.pred.bindNode(a, &self.tbl, stmt.select.where.?, .{});
        return sql.plan.plan(a, &self.tbl, pred, opts);
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
    return src.fetch.bnode;
}

fn expectScan(p: *const Plan) !void {
    try testing.expect(source(p).* == .seq_scan);
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

test "sql.plan: IS NULL reads a posting index, never a unique one" {
    var fx = try Fixture.init();
    defer fx.deinit();
    const a = fx.arena.allocator();

    var on_b = try fx.planFor("b IS NULL", stream);
    defer on_b.deinit();
    const null_key = try sql.key.encodeIndexKey(a, .null_value, null);
    try expectLookup(try bitmapOf(&on_b), b_index_id, false, null_key);

    // A unique index holds no NULL entries.
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
    const a_eq_1 = Pred{ .cmp = .{ .col = fx.tbl.column("a").?, .val = .{ .int4 = 1 } } };
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
