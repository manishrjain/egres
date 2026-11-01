// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const String = sql.value.String;
const zdb = @import("zdb");

const pred = sql.pred;
const Pred = pred.Pred;
const Value = sql.value.Value;
const Item = zdb.kv.Item;

/// (a int4, b text) assembled by hand — no catalog, no forest. Everything
/// lives in one arena, dropped by the test.
const Fixture = struct {
    a: std.mem.Allocator,
    tbl: sql.table.Table,

    fn init(a: std.mem.Allocator) !Fixture {
        const logical = [_]sql.schema.LogicalColumn{
            .{ .column_id = 1, .name = "a", .ty = .int4 },
            .{ .column_id = 2, .name = "b", .ty = .text },
        };
        const ps = try sql.schema.compile(a, .{ .columns = &logical, .current_version = 1 }, 1);
        const columns = try a.alloc(sql.table.Column, 2);
        columns[0] = .{ .column_id = 1, .pos = 0, .name = "a", .ty = .int4, .pkey_position = null };
        columns[1] = .{ .column_id = 2, .pos = 1, .name = "b", .ty = .text, .pkey_position = null };
        return .{ .a = a, .tbl = .{
            .columns = columns,
            .indexes = try a.alloc(sql.table.Index, 0),
            .ps = ps,
            .version = 1,
            .arena = std.heap.ArenaAllocator.init(a),
        } };
    }

    /// `item` as the row a predicate evaluates against, computing
    /// into the fixture's allocator.
    fn row(self: *const Fixture, stored: Item) sql.expr.Row {
        return .{ .scratch = self.a, .tbl = &self.tbl, .item = stored };
    }

    fn col(self: *const Fixture, name: String) *const sql.table.Column {
        return self.tbl.column(name).?;
    }

    /// A stored row: egid in the key, `cells` marshalled under the
    /// table's layout.
    fn item(self: *const Fixture, egid: u64, cells: []const Value) !Item {
        var buf: std.ArrayList(u8) = .empty;
        const bytes = try sql.row.marshalRow(self.a, &self.tbl.ps, cells, &buf);
        return .{
            .key = .{
                .namespace = 1,
                .table_id = 16,
                .primary = try self.a.dupe(u8, &sql.key.encodeEgid(egid)),
            },
            .versions = .{ .one = .{
                .ts = 1,
                .meta = .{},
                .value = bytes,
            } },
        };
    }
};

test "sql.pred: bind resolves columns, coerces literals, and checks the egid" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fx = try Fixture.init(a);
    defer fx.tbl.deinit();

    const stmt = try sql.parse.statement(
        a,
        "SELECT * FROM t WHERE a = 1 AND (b = 'x' OR NOT egid = 3) AND b IS NOT NULL;",
    );
    const p = try pred.bind(a, &fx.tbl, stmt.select.where.?, .{});
    const top = p.and_;
    try testing.expectEqual(@as(usize, 3), top.len);
    try testing.expectEqual(fx.col("a"), top[0].cmp.lhs.column);
    try testing.expectEqual(Value{ .int4 = 1 }, top[0].cmp.rhs.value);
    const or2 = top[1].or_;
    try testing.expectEqual(fx.col("b"), or2[0].cmp.lhs.column);
    try testing.expectEqualStrings("x", or2[0].cmp.rhs.value.text);
    const inner = or2[1].not.*;
    try testing.expect(inner.cmp.lhs.column.isEgid());
    try testing.expectEqual(Value{ .uint8 = 3 }, inner.cmp.rhs.value);
    try testing.expectEqual(fx.col("b"), top[2].is_null.expr.column);
    try testing.expect(top[2].is_null.negate);

    // topLevelAnds: a root AND's children; a leaf is its own single term.
    try testing.expectEqual(@as(usize, 3), p.topLevelAnds().len);
    const leaf = top[0];
    try testing.expectEqual(@as(usize, 1), leaf.topLevelAnds().len);
    try testing.expectEqual(&leaf, &leaf.topLevelAnds()[0]);

    // Errors, wherever they sit in the tree.
    const cases = [_]struct { sql: String, err: pred.BindError }{
        .{ .sql = "SELECT * FROM t WHERE a = 1 AND nope = 2;", .err = error.UndefinedColumn },
        .{ .sql = "SELECT * FROM t WHERE NOT a = 'zz';", .err = error.InvalidValue },
        .{ .sql = "SELECT * FROM t WHERE egid = 0;", .err = error.EgidZero },
        .{ .sql = "SELECT * FROM t WHERE egid = -1;", .err = error.InvalidValue },
        .{ .sql = "SELECT * FROM t WHERE nope IS NULL;", .err = error.UndefinedColumn },
        .{ .sql = "SELECT * FROM t WHERE a = b;", .err = error.NoSuchOperator },
        .{ .sql = "SELECT * FROM t WHERE a > 'x';", .err = error.InvalidValue },
        .{ .sql = "SELECT * FROM t WHERE lower(a) = 'x';", .err = error.NoSuchOperator },
    };
    for (cases) |case| {
        const s = try sql.parse.statement(a, case.sql);
        try testing.expectError(
            case.err,
            pred.bind(a, &fx.tbl, s.select.where.?, .{}),
        );
    }
}

test "sql.pred: eval follows SQL three-valued logic" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fx = try Fixture.init(a);
    defer fx.tbl.deinit();

    const r1 = try fx.item(1, &.{ .{ .int4 = 1 }, .{ .text = "x" } });
    const r2 = try fx.item(2, &.{ .null_value, .{ .text = "x" } });
    const r3 = try fx.item(3, &.{ .{ .int4 = 2 }, .null_value });

    const a_eq_1 = try pred.colCmp(a, fx.col("a"), .eq, .{ .int4 = 1 });
    const a_eq_2 = try pred.colCmp(a, fx.col("a"), .eq, .{ .int4 = 2 });
    const a_eq_null = try pred.colCmp(a, fx.col("a"), .eq, .null_value);
    const b_eq_x = try pred.colCmp(a, fx.col("b"), .eq, .{ .text = "x" });
    const b_col = try a.create(sql.expr.Node);
    b_col.* = .{ .column = fx.col("b") };
    const b_is_null = Pred{ .is_null = .{ .expr = b_col, .negate = false } };
    const b_not_null = Pred{ .is_null = .{ .expr = b_col, .negate = true } };
    const egid_3 = try pred.colCmp(a, fx.col("egid"), .eq, .{ .uint8 = 3 });

    const T: ?bool = true;
    const F: ?bool = false;
    const U: ?bool = null;

    // A leaf: true, unknown (NULL cell), false.
    try testing.expectEqual(T, try a_eq_1.eval(fx.row(r1)));
    try testing.expectEqual(U, try a_eq_1.eval(fx.row(r2)));
    try testing.expectEqual(F, try a_eq_1.eval(fx.row(r3)));
    // `= NULL` is unknown for every row, NULL cell or not.
    try testing.expectEqual(U, try a_eq_null.eval(fx.row(r1)));
    try testing.expectEqual(U, try a_eq_null.eval(fx.row(r2)));
    // IS [NOT] NULL is exact, never unknown.
    try testing.expectEqual(F, try b_is_null.eval(fx.row(r1)));
    try testing.expectEqual(T, try b_is_null.eval(fx.row(r3)));
    try testing.expectEqual(T, try b_not_null.eval(fx.row(r1)));
    try testing.expectEqual(F, try b_not_null.eval(fx.row(r3)));
    // The egid pseudo-column reads from the key.
    try testing.expectEqual(F, try egid_3.eval(fx.row(r1)));
    try testing.expectEqual(T, try egid_3.eval(fx.row(r3)));

    // NOT keeps unknown unknown.
    const not_a1 = Pred{ .not = &a_eq_1 };
    try testing.expectEqual(F, try not_a1.eval(fx.row(r1)));
    try testing.expectEqual(U, try not_a1.eval(fx.row(r2)));
    try testing.expectEqual(T, try not_a1.eval(fx.row(r3)));
    const not_a_null = Pred{ .not = &a_eq_null };
    try testing.expectEqual(U, try not_a_null.eval(fx.row(r1)));

    // AND: false dominates unknown; unknown dominates true.
    const and_kids = [_]Pred{ a_eq_1, b_eq_x };
    const and_ = Pred{ .and_ = &and_kids };
    try testing.expectEqual(T, try and_.eval(fx.row(r1)));
    try testing.expectEqual(U, try and_.eval(fx.row(r2))); // unknown AND true
    try testing.expectEqual(F, try and_.eval(fx.row(r3))); // false AND unknown
    // OR: true dominates unknown; unknown dominates false.
    const or_kids = [_]Pred{ a_eq_1, b_eq_x };
    const or_ = Pred{ .or_ = &or_kids };
    try testing.expectEqual(T, try or_.eval(fx.row(r1)));
    try testing.expectEqual(T, try or_.eval(fx.row(r2))); // unknown OR true
    try testing.expectEqual(U, try or_.eval(fx.row(r3))); // false OR unknown
    const or_2_kids = [_]Pred{ a_eq_2, egid_3 };
    const or_2 = Pred{ .or_ = &or_2_kids };
    try testing.expectEqual(T, try or_2.eval(fx.row(r3)));
    try testing.expectEqual(F, try or_2.eval(fx.row(r1)));

    // matches: only TRUE passes — `= NULL` selects nothing.
    try testing.expect(try a_eq_1.matches(fx.row(r1)));
    try testing.expect(!try a_eq_1.matches(fx.row(r2)));
    try testing.expect(!try a_eq_null.matches(fx.row(r2)));
    try testing.expect(!try or_.matches(fx.row(r3)));
}

test "sql.pred: a literal longer than its char(n) column binds to never" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // A one-column table (c char(3)), assembled like Fixture.
    const logical = [_]sql.schema.LogicalColumn{
        .{ .column_id = 1, .name = "c", .ty = .bpchar, .type_mod = 3 },
    };
    const ps = try sql.schema.compile(a, .{ .columns = &logical, .current_version = 1 }, 1);
    const columns = try a.alloc(sql.table.Column, 1);
    columns[0] = .{ .column_id = 1, .pos = 0, .name = "c", .ty = .bpchar, .pkey_position = null, .type_mod = 3 };
    var tbl = sql.table.Table{
        .columns = columns,
        .indexes = try a.alloc(sql.table.Index, 0),
        .ps = ps,
        .version = 1,
        .arena = std.heap.ArenaAllocator.init(a),
    };
    defer tbl.deinit();

    // Within the length (spaces stripped): an ordinary comparison.
    const fits = try sql.parse.statement(a, "SELECT * FROM t WHERE c = 'ab  ';");
    const p_fits = try pred.bind(a, &tbl, fits.select.where.?, .{});
    try testing.expectEqualStrings("ab", p_fits.cmp.rhs.value.text);

    // Over the length: never, wherever it sits, and NOT flips it.
    const over = try sql.parse.statement(a, "SELECT * FROM t WHERE c = 'abcd' OR NOT c = 12345;");
    const p_over = try pred.bind(a, &tbl, over.select.where.?, .{});
    try testing.expect(p_over.or_[0] == .never);
    try testing.expect(p_over.or_[1].not.* == .never);

    // never is false; NOT never is true (never unknown).
    var buf: std.ArrayList(u8) = .empty;
    const bytes = try sql.row.marshalRow(a, &tbl.ps, &.{.{ .text = "ab" }}, &buf);
    const row = Item{
        .key = .{ .namespace = 1, .table_id = 16, .primary = try a.dupe(u8, &sql.key.encodeEgid(1)) },
        .versions = .{ .one = .{ .ts = 1, .meta = .{}, .value = bytes } },
    };
    const on: sql.expr.Row = .{ .scratch = a, .tbl = &tbl, .item = row };
    try testing.expectEqual(@as(?bool, false), try p_over.or_[0].eval(on));
    try testing.expectEqual(@as(?bool, true), try p_over.or_[1].eval(on));
    try testing.expect(try p_over.matches(on));
}

test "sql.pred: ordered comparisons follow the column's order, NULL unknown" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var fx = try Fixture.init(a);
    defer fx.tbl.deinit();

    const r1 = try fx.item(1, &.{ .{ .int4 = 1 }, .{ .text = "apple" } });
    const r2 = try fx.item(2, &.{ .null_value, .{ .text = "banana" } });
    const r3 = try fx.item(3, &.{ .{ .int4 = 3 }, .null_value });
    const T: ?bool = true;
    const F: ?bool = false;
    const U: ?bool = null;

    const Case = struct { op: sql.parse.CmpOp, r1: ?bool, r3: ?bool };
    // a against 2: r1 has 1, r3 has 3, r2 is NULL.
    const cases = [_]Case{
        .{ .op = .lt, .r1 = T, .r3 = F },
        .{ .op = .le, .r1 = T, .r3 = F },
        .{ .op = .gt, .r1 = F, .r3 = T },
        .{ .op = .ge, .r1 = F, .r3 = T },
        .{ .op = .ne, .r1 = T, .r3 = T },
        .{ .op = .eq, .r1 = F, .r3 = F },
    };
    for (cases) |case| {
        const p = try pred.colCmp(a, fx.col("a"), case.op, .{ .int4 = 2 });
        try testing.expectEqual(case.r1, try p.eval(fx.row(r1)));
        try testing.expectEqual(U, try p.eval(fx.row(r2)));
        try testing.expectEqual(case.r3, try p.eval(fx.row(r3)));
    }
    // Text orders bytewise; a NULL cell is unknown whatever the operator.
    const b_lt = try pred.colCmp(a, fx.col("b"), .lt, .{ .text = "b" });
    try testing.expectEqual(T, try b_lt.eval(fx.row(r1)));
    try testing.expectEqual(F, try b_lt.eval(fx.row(r2)));
    try testing.expectEqual(U, try b_lt.eval(fx.row(r3)));
    // A comparison against NULL is unknown for every row.
    const a_gt_null = try pred.colCmp(a, fx.col("a"), .gt, .null_value);
    try testing.expectEqual(U, try a_gt_null.eval(fx.row(r1)));
    // The egid pseudo-column orders by key.
    const egid_ge_2 = try pred.colCmp(a, fx.col("egid"), .ge, .{ .uint8 = 2 });
    try testing.expectEqual(F, try egid_ge_2.eval(fx.row(r1)));
    try testing.expectEqual(T, try egid_ge_2.eval(fx.row(r3)));
}

test "sql.pred: expressions on either side compute per row" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fx = try Fixture.init(a);
    defer fx.tbl.deinit();
    const tbl = &fx.tbl;
    const T: ?bool = true;
    const F: ?bool = false;
    const U: ?bool = null;

    const r1 = try fx.item(1, &.{ .{ .int4 = 1 }, .{ .text = "Ab" } });
    const r2 = try fx.item(2, &.{ .{ .int4 = 2 }, .null_value });

    const Case = struct { where: String, r1: ?bool, r2: ?bool };
    const cases = [_]Case{
        .{ .where = "a + 1 = 2", .r1 = T, .r2 = F },
        .{ .where = "2 = a + 1", .r1 = T, .r2 = F },
        .{ .where = "lower(b) = 'ab'", .r1 = T, .r2 = U },
        .{ .where = "length(b) > a", .r1 = T, .r2 = U },
        .{ .where = "a * 2.5 >= 5", .r1 = F, .r2 = T },
        .{ .where = "5 > 6", .r1 = F, .r2 = F },
        .{ .where = "nullif(a, 2) IS NULL", .r1 = F, .r2 = T },
        .{ .where = "coalesce(b, 'z') <> 'z'", .r1 = T, .r2 = F },
    };
    for (cases) |case| {
        const text = try std.fmt.allocPrint(a, "SELECT * FROM t WHERE {s};", .{case.where});
        const stmt = try sql.parse.statement(a, text);
        const p = try pred.bind(a, tbl, stmt.select.where.?, .{});
        try testing.expectEqual(case.r1, try p.eval(fx.row(r1)));
        try testing.expectEqual(case.r2, try p.eval(fx.row(r2)));
    }

    // The bare column goes left whichever side it was written on, so
    // the planner sees `column op constant`.
    const turned = try sql.parse.statement(a, "SELECT * FROM t WHERE 1 < a;");
    const p = try pred.bind(a, tbl, turned.select.where.?, .{});
    try testing.expectEqual(fx.col("a"), p.cmp.lhs.column);
    try testing.expectEqual(sql.parse.CmpOp.gt, p.cmp.op);
    try testing.expectEqual(Value{ .int4 = 1 }, p.cmp.rhs.value);

    // An expression that fails to compute fails the row's evaluation.
    const bad = try sql.parse.statement(a, "SELECT * FROM t WHERE a / 0 = 1;");
    const pb = try pred.bind(a, tbl, bad.select.where.?, .{});
    try testing.expectError(error.DivisionByZero, pb.eval(fx.row(r1)));
}
