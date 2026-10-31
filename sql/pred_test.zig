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

test "sql.pred: bindNode binds columns, coerces literals, and checks the egid" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fx = try Fixture.init(a);
    defer fx.tbl.deinit();

    const stmt = try sql.parse.statement(
        a,
        "SELECT * FROM t WHERE a = 1 AND (b = 'x' OR NOT egid = 3) AND b IS NOT NULL;",
    );
    const p = try pred.bindNode(a, &fx.tbl, stmt.select.where.?, .{});
    const top = p.and_;
    try testing.expectEqual(@as(usize, 3), top.len);
    try testing.expectEqual(fx.col("a"), top[0].cmp.col);
    try testing.expectEqual(Value{ .int4 = 1 }, top[0].cmp.val);
    const or2 = top[1].or_;
    try testing.expectEqual(fx.col("b"), or2[0].cmp.col);
    try testing.expectEqualStrings("x", or2[0].cmp.val.text);
    const inner = or2[1].not.*;
    try testing.expect(inner.cmp.col.isEgid());
    try testing.expectEqual(Value{ .uint8 = 3 }, inner.cmp.val);
    try testing.expectEqual(fx.col("b"), top[2].is_null.col);
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
    };
    for (cases) |case| {
        const s = try sql.parse.statement(a, case.sql);
        try testing.expectError(
            case.err,
            pred.bindNode(a, &fx.tbl, s.select.where.?, .{}),
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

    const a_eq_1 = Pred{ .cmp = .{ .col = fx.col("a"), .val = .{ .int4 = 1 } } };
    const a_eq_2 = Pred{ .cmp = .{ .col = fx.col("a"), .val = .{ .int4 = 2 } } };
    const a_eq_null = Pred{ .cmp = .{ .col = fx.col("a"), .val = .null_value } };
    const b_eq_x = Pred{ .cmp = .{ .col = fx.col("b"), .val = .{ .text = "x" } } };
    const b_is_null = Pred{ .is_null = .{ .col = fx.col("b"), .negate = false } };
    const b_not_null = Pred{ .is_null = .{ .col = fx.col("b"), .negate = true } };
    const egid_3 = Pred{ .cmp = .{ .col = fx.col("egid"), .val = .{ .uint8 = 3 } } };

    const T: ?bool = true;
    const F: ?bool = false;
    const U: ?bool = null;
    const tbl = &fx.tbl;

    // A leaf: true, unknown (NULL cell), false.
    try testing.expectEqual(T, a_eq_1.eval(tbl, r1));
    try testing.expectEqual(U, a_eq_1.eval(tbl, r2));
    try testing.expectEqual(F, a_eq_1.eval(tbl, r3));
    // `= NULL` is unknown for every row, NULL cell or not.
    try testing.expectEqual(U, a_eq_null.eval(tbl, r1));
    try testing.expectEqual(U, a_eq_null.eval(tbl, r2));
    // IS [NOT] NULL is exact, never unknown.
    try testing.expectEqual(F, b_is_null.eval(tbl, r1));
    try testing.expectEqual(T, b_is_null.eval(tbl, r3));
    try testing.expectEqual(T, b_not_null.eval(tbl, r1));
    try testing.expectEqual(F, b_not_null.eval(tbl, r3));
    // The egid pseudo-column reads from the key.
    try testing.expectEqual(F, egid_3.eval(tbl, r1));
    try testing.expectEqual(T, egid_3.eval(tbl, r3));

    // NOT keeps unknown unknown.
    const not_a1 = Pred{ .not = &a_eq_1 };
    try testing.expectEqual(F, not_a1.eval(tbl, r1));
    try testing.expectEqual(U, not_a1.eval(tbl, r2));
    try testing.expectEqual(T, not_a1.eval(tbl, r3));
    const not_a_null = Pred{ .not = &a_eq_null };
    try testing.expectEqual(U, not_a_null.eval(tbl, r1));

    // AND: false dominates unknown; unknown dominates true.
    const and_ = Pred{ .and_ = &.{ a_eq_1, b_eq_x } };
    try testing.expectEqual(T, and_.eval(tbl, r1));
    try testing.expectEqual(U, and_.eval(tbl, r2)); // unknown AND true
    try testing.expectEqual(F, and_.eval(tbl, r3)); // false AND unknown
    // OR: true dominates unknown; unknown dominates false.
    const or_ = Pred{ .or_ = &.{ a_eq_1, b_eq_x } };
    try testing.expectEqual(T, or_.eval(tbl, r1));
    try testing.expectEqual(T, or_.eval(tbl, r2)); // unknown OR true
    try testing.expectEqual(U, or_.eval(tbl, r3)); // false OR unknown
    const or_2 = Pred{ .or_ = &.{ a_eq_2, egid_3 } };
    try testing.expectEqual(T, or_2.eval(tbl, r3));
    try testing.expectEqual(F, or_2.eval(tbl, r1));

    // matches: only TRUE passes — `= NULL` selects nothing.
    try testing.expect(a_eq_1.matches(tbl, r1));
    try testing.expect(!a_eq_1.matches(tbl, r2));
    try testing.expect(!a_eq_null.matches(tbl, r2));
    try testing.expect(!or_.matches(tbl, r3));
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
    const p_fits = try pred.bindNode(a, &tbl, fits.select.where.?, .{});
    try testing.expectEqualStrings("ab", p_fits.cmp.val.text);

    // Over the length: never, wherever it sits, and NOT flips it.
    const over = try sql.parse.statement(a, "SELECT * FROM t WHERE c = 'abcd' OR NOT c = 12345;");
    const p_over = try pred.bindNode(a, &tbl, over.select.where.?, .{});
    try testing.expect(p_over.or_[0] == .never);
    try testing.expect(p_over.or_[1].not.* == .never);

    // never is false; NOT never is true (never unknown).
    var buf: std.ArrayList(u8) = .empty;
    const bytes = try sql.row.marshalRow(a, &tbl.ps, &.{.{ .text = "ab" }}, &buf);
    const row = Item{
        .key = .{ .namespace = 1, .table_id = 16, .primary = try a.dupe(u8, &sql.key.encodeEgid(1)) },
        .versions = .{ .one = .{ .ts = 1, .meta = .{}, .value = bytes } },
    };
    try testing.expectEqual(@as(?bool, false), p_over.or_[0].eval(&tbl, row));
    try testing.expectEqual(@as(?bool, true), p_over.or_[1].eval(&tbl, row));
    try testing.expect(p_over.matches(&tbl, row));
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
    const tbl = &fx.tbl;
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
        const p = Pred{ .cmp = .{ .col = fx.col("a"), .op = case.op, .val = .{ .int4 = 2 } } };
        try testing.expectEqual(case.r1, p.eval(tbl, r1));
        try testing.expectEqual(U, p.eval(tbl, r2));
        try testing.expectEqual(case.r3, p.eval(tbl, r3));
    }
    // Text orders bytewise; a NULL cell is unknown whatever the operator.
    const b_lt = Pred{ .cmp = .{ .col = fx.col("b"), .op = .lt, .val = .{ .text = "b" } } };
    try testing.expectEqual(T, b_lt.eval(tbl, r1));
    try testing.expectEqual(F, b_lt.eval(tbl, r2));
    try testing.expectEqual(U, b_lt.eval(tbl, r3));
    // A comparison against NULL is unknown for every row.
    const a_gt_null = Pred{ .cmp = .{ .col = fx.col("a"), .op = .gt, .val = .null_value } };
    try testing.expectEqual(U, a_gt_null.eval(tbl, r1));
    // The egid pseudo-column orders by key.
    const egid_ge_2 = Pred{ .cmp = .{ .col = fx.col("egid"), .op = .ge, .val = .{ .uint8 = 2 } } };
    try testing.expectEqual(F, egid_ge_2.eval(tbl, r1));
    try testing.expectEqual(T, egid_ge_2.eval(tbl, r3));
}
