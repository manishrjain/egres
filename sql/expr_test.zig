// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const pgwire = @import("pgwire");
const String = sql.value.String;
const zdb = @import("zdb");

const expr = sql.expr;
const Node = expr.Node;
const Value = sql.value.Value;
const Type = sql.value.Type;
const Item = zdb.kv.Item;

/// (a int4, b text, c float8, d int2, e int8, f float4) assembled by
/// hand — no catalog, no forest. Everything lives in one arena,
/// dropped by the test.
const Fixture = struct {
    arloc: std.mem.Allocator,
    tbl: sql.table.Table,

    fn init(arloc: std.mem.Allocator) !Fixture {
        const logical = [_]sql.schema.LogicalColumn{
            .{ .column_id = 1, .name = "a", .ty = .int4 },
            .{ .column_id = 2, .name = "b", .ty = .text },
            .{ .column_id = 3, .name = "c", .ty = .float8 },
            .{ .column_id = 4, .name = "d", .ty = .int2 },
            .{ .column_id = 5, .name = "e", .ty = .int8 },
            .{ .column_id = 6, .name = "f", .ty = .float4 },
        };
        const ps = try sql.schema.compile(
            arloc,
            .{ .columns = &logical, .current_version = 1 },
            1,
        );
        const columns = try arloc.alloc(sql.table.Column, logical.len);
        for (logical, columns, 0..) |lc, *slot, pos| slot.* = .{
            .column_id = lc.column_id,
            .pos = pos,
            .name = lc.name,
            .ty = lc.ty,
            .pkey_position = null,
        };
        return .{ .arloc = arloc, .tbl = .{
            .columns = columns,
            .indexes = try arloc.alloc(sql.table.Index, 0),
            .ps = ps,
            .version = 1,
            .arena = std.heap.ArenaAllocator.init(arloc),
        } };
    }

    fn col(self: *const Fixture, name: String) *const sql.table.Column {
        return self.tbl.column(name).?;
    }

    /// A stored row: egid in the key, `cells` marshalled under the
    /// table's layout.
    fn item(self: *const Fixture, egid: u64, cells: []const Value) !Item {
        var buf: std.ArrayList(u8) = .empty;
        const ps = &self.tbl.ps;
        const bytes = try sql.row.marshalRow(self.arloc, ps, cells, &buf);
        return .{
            .key = .{
                .namespace = 1,
                .table_id = 16,
                .primary = try self.arloc.dupe(u8, &sql.key.encodeEgid(egid)),
            },
            .versions = .{ .one = .{
                .ts = 1,
                .meta = .{},
                .value = bytes,
            } },
        };
    }

    /// A SET assignment bound against its target column.
    const Set = struct {
        target: *const sql.table.Column,
        root: *const Node,
    };

    /// Parse `UPDATE t SET <assignment>` and bind its one SET value.
    fn bind(self: *const Fixture, assignment: String) expr.BindError!Set {
        return self.bindWith(assignment, .{});
    }

    /// `bind` with the values a Bind message would have supplied.
    fn bindWith(
        self: *const Fixture,
        assignment: String,
        params: sql.parse.Params,
    ) expr.BindError!Set {
        return self.bindFull(assignment, params, null);
    }

    /// `bind` for an ON CONFLICT DO UPDATE: `excluded` is the proposed
    /// row, one cell per column.
    fn bindExcluded(
        self: *const Fixture,
        assignment: String,
        excluded: []const Value,
    ) expr.BindError!Set {
        return self.bindFull(assignment, .{}, excluded);
    }

    fn bindFull(
        self: *const Fixture,
        assignment: String,
        params: sql.parse.Params,
        excluded: ?[]const Value,
    ) expr.BindError!Set {
        const text = try std.fmt.allocPrint(
            self.arloc,
            "UPDATE t SET {s};",
            .{assignment},
        );
        const stmt = sql.parse.statement(self.arloc, text) catch
            return error.OutOfMemory;
        const clause = stmt.update.sets[0];
        const target = self.tbl.column(clause.column) orelse
            return error.UndefinedColumn;
        const tbl = &self.tbl;
        const root = try expr.bind(
            self.arloc,
            tbl,
            clause.value,
            target,
            params,
            excluded,
        );
        return .{ .target = target, .root = root };
    }

    /// `item` as the row a SET evaluates against; the arena is the
    /// scratch, as for a statement's SET.
    fn row(self: *const Fixture, stored: Item) expr.Row {
        return .{ .scratch = self.arloc, .tbl = &self.tbl, .item = stored };
    }

    /// Bind then evaluate one assignment against `stored`.
    fn eval(self: *const Fixture, assignment: String, stored: Item) !Value {
        const set = try self.bind(assignment);
        return set.root.eval(self.row(stored), set.target);
    }
};

/// The example row every eval test reads.
fn sampleRow(fx: *const Fixture) !Item {
    return fx.item(3, &.{
        .{ .int4 = 5 },
        .{ .text = "ab" },
        .{ .float8 = 2.5 },
        .{ .int2 = 7 },
        .{ .int8 = 1 << 40 },
        .{ .float4 = 1.5 },
    });
}

fn expectEval(
    fx: *const Fixture,
    row: Item,
    want: Value,
    sql_set: String,
) !void {
    try testing.expectEqual(want, try fx.eval(sql_set, row));
}

fn expectEvalErr(
    fx: *const Fixture,
    row: Item,
    want: anyerror,
    sql_set: String,
) !void {
    try testing.expectError(want, fx.eval(sql_set, row));
}

test "sql.expr: bind — literals, columns, operators, and the type rules" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const fx = try Fixture.init(arena.allocator());

    // A bare literal is a cell of the column's type, NULL included.
    const lit = try fx.bind("a = 1");
    try testing.expectEqual(@as(i32, 1), lit.root.value.int4);
    const str = try fx.bind("b = 'x'");
    try testing.expectEqualStrings("x", str.root.value.text);
    const nul = try fx.bind("a = NULL");
    try testing.expect(nul.root.value == .null_value);
    try testing.expectError(error.InvalidValue, fx.bind("a = 'zz'"));

    // A bare column is the column, of the same type or of a numeric
    // one that converts; anything else is a mismatch.
    const copy = try fx.bind("b = b");
    try testing.expectEqual(fx.col("b"), copy.root.column);
    const widen = try fx.bind("a = e");
    try testing.expectEqual(fx.col("a"), widen.target);
    try testing.expectEqual(fx.col("e"), widen.root.column);
    try testing.expectError(error.TypeMismatch, fx.bind("a = b"));
    try testing.expectError(error.TypeMismatch, fx.bind("a = c"));

    // Operators: numbers under arithmetic, text under `||`; the
    // result must fit the target column.
    const arith = try fx.bind("a = a * 2 + 1");
    try testing.expectEqual(sql.parse.Expr.Op.add, arith.root.binop.op);
    try testing.expectEqual(sql.value.Type.int8, arith.root.ty().?);
    const mixed = try fx.bind("c = a + 1.5"); // int with a decimal → numeric
    try testing.expectEqual(sql.value.Type.numeric, mixed.root.ty().?);
    _ = try fx.bind("c = a + 1"); // an integer result into a float column
    const nulls = try fx.bind("a = NULL + NULL"); // NULL-only fits anything
    try testing.expect(nulls.root.ty() == null);
    _ = try fx.bind("b = 'p' || b");
    try testing.expectError(error.TypeMismatch, fx.bind("a = a * 1.5"));
    try testing.expectError(error.TypeMismatch, fx.bind("b = 1 + 2"));
    try testing.expectError(error.TypeMismatch, fx.bind("a = b || 'x'"));
    try testing.expectError(error.NoSuchOperator, fx.bind("b = b + 1"));
    try testing.expectError(error.NoSuchOperator, fx.bind("a = 'x' + 1"));
    try testing.expectError(error.NoSuchOperator, fx.bind("a = true + 1"));
    try testing.expectError(error.NoSuchOperator, fx.bind("c = c % 2"));
    try testing.expectError(error.NoSuchOperator, fx.bind("b = a || 'x'"));
    try testing.expectError(error.NoSuchOperator, fx.bind("b = b || 1"));
    try testing.expectError(error.UndefinedColumn, fx.bind("a = zz + 1"));
}

test "sql.expr: eval — integer arithmetic, NULLs, and range errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const fx = try Fixture.init(arena.allocator());
    const row = try sampleRow(&fx);

    try expectEval(&fx, row, .{ .int4 = 6 }, "a = a + 1");
    try expectEval(&fx, row, .{ .int4 = 11 }, "a = a * 2 + 1");
    try expectEval(&fx, row, .{ .int4 = -5 }, "a = -a");
    try expectEval(&fx, row, .{ .int4 = 30 }, "a = egid * 10");
    // `/` truncates toward zero; `%` takes the dividend's sign.
    try expectEval(&fx, row, .{ .int4 = 3 }, "a = d / 2");
    try expectEval(&fx, row, .{ .int4 = -3 }, "a = -d / 2");
    try expectEval(&fx, row, .{ .int4 = -1 }, "a = -d % 3");
    // A NULL operand makes the result NULL.
    try expectEval(&fx, row, .null_value, "a = a + NULL");
    // Widening and narrowing between integer columns, with the range
    // check on assignment. (An integer literal too wide for int4
    // arrives as float text and must still count as an integer.)
    try expectEval(&fx, row, .{ .int8 = 5 }, "e = a");
    try expectEval(&fx, row, .{ .int4 = 1 }, "a = e / 1099511627776");
    try expectEvalErr(&fx, row, error.IntOutOfRange, "a = e");
    try expectEvalErr(&fx, row, error.IntOutOfRange, "a = a + 2147483647");
    try expectEvalErr(&fx, row, error.IntOutOfRange, "d = d + 40000");
    const wide = "e = a * 9223372036854775807";
    try expectEvalErr(&fx, row, error.IntOutOfRange, wide);
    try expectEvalErr(&fx, row, error.DivisionByZero, "a = a / 0");
    try expectEvalErr(&fx, row, error.DivisionByZero, "a = a % 0");
    try expectEvalErr(&fx, row, error.DivisionByZero, "a = NULL + a / 0");
}

test "sql.expr: eval — floats, text, and copies" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const fx = try Fixture.init(arena.allocator());
    const row = try sampleRow(&fx);

    try expectEval(&fx, row, .{ .float8 = 5.0 }, "c = c * 2");
    try expectEval(&fx, row, .{ .float8 = 6.0 }, "c = a + 1");
    // Integer division stays integer before it widens (as in PG).
    try expectEval(&fx, row, .{ .float8 = 2.0 }, "c = a / 2");
    try expectEval(&fx, row, .{ .float4 = 3.0 }, "f = f * 2");
    try expectEval(&fx, row, .{ .float8 = 1.5 }, "c = f");
    try expectEvalErr(&fx, row, error.DivisionByZero, "c = c / 0");
    try expectEvalErr(&fx, row, error.FloatOutOfRange, "c = c * 1e308");
    try expectEvalErr(&fx, row, error.FloatOutOfRange, "f = c * 1e39");

    const bang = try fx.eval("b = b || '!'", row);
    try testing.expectEqualStrings("ab!", bang.text);
    const wrapped = try fx.eval("b = 'p' || b || 's'", row);
    try testing.expectEqualStrings("pabs", wrapped.text);
    const consts = try fx.eval("b = 'x' || 'y'", row);
    try testing.expectEqualStrings("xy", consts.text);
    try expectEval(&fx, row, .null_value, "b = b || NULL");

    // A copy borrows the row's bytes.
    const copy = try fx.eval("b = b", row);
    try testing.expectEqualStrings("ab", copy.text);
    try expectEval(&fx, row, .{ .int4 = 5 }, "a = a");
}

test "sql.expr: bind — a $n is typed by its column, or by its text" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const fx = try Fixture.init(arena.allocator());

    const text = sql.parse.Params{
        .values = &.{ "5", "x", null, "1.5" },
        .formats = &.{ 0, 0, 0, 0 },
    };
    // At the root: a cell of the target column, as a quoted literal is.
    const root = try fx.bindWith("a = $1", text);
    try testing.expectEqual(Value{ .int4 = 5 }, root.root.value);
    try testing.expectError(error.InvalidValue, fx.bindWith("a = $2", text));
    // Under an operator: an integer, a float, or text by what the
    // bytes say; the operator then type-checks as usual.
    const add = try fx.bindWith("a = a + $1", text);
    try testing.expectEqual(Value{ .int8 = 5 }, add.root.binop.rhs.value);
    const cat = try fx.bindWith("b = b || $2", text);
    try testing.expectEqualStrings("x", cat.root.binop.rhs.value.text);
    const nul = try fx.bindWith("a = a + $3", text);
    try testing.expect(nul.root.binop.rhs.value == .null_value);
    const mixed = try fx.bindWith("c = a * $4", text);
    try testing.expectEqual(
        sql.Numeric{ .coef = 15, .scale = 1 },
        mixed.root.binop.rhs.value.numeric,
    );
    try testing.expectError(
        error.NoSuchOperator,
        fx.bindWith("a = a + $2", text),
    );
    try testing.expectError(
        error.TypeMismatch,
        fx.bindWith("a = a * $4", text),
    );

    // A binary value is its declared type, at the root (cast to the
    // column: an int8 into the int4 column) and under an operator.
    // With no declared type its bytes cannot be read.
    const binary = sql.parse.Params{
        .values = &.{ &.{ 0, 0, 0, 0, 0, 0, 0, 5 }, &.{ 0, 0, 0, 5 } },
        .formats = &.{ 1, 1 },
        .oids = &.{pgwire.oid.int8},
    };
    const typed = try fx.bindWith("a = $1", binary);
    try testing.expectEqual(Value{ .int4 = 5 }, typed.root.value);
    const under = try fx.bindWith("a = a + $1", binary);
    try testing.expectEqual(Value{ .int8 = 5 }, under.root.binop.rhs.value);
    try testing.expectError(
        error.UnsupportedValue,
        fx.bindWith("a = a + $2", binary),
    );

    // Before Bind, as a statement's Describe binds: the declared type
    // with no value, and none declared is resolved by the operator,
    // as a NULL is.
    const declared = sql.parse.Params{ .oids = &.{pgwire.oid.int4} };
    const desc = try fx.bindWith("a = a + $1", declared);
    try testing.expectEqual(@as(?Type, .int4), desc.root.binop.rhs.param);
    try testing.expectEqual(@as(?Type, .int8), desc.root.binop.ty);
    const unknown = try fx.bindWith("b = b || $2", declared);
    try testing.expectEqual(@as(?Type, null), unknown.root.binop.rhs.param);
    try testing.expectEqual(@as(?Type, .text), unknown.root.binop.ty);
}

test "sql.expr: CONCAT renders and skips NULL, COALESCE takes the first value" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const arloc = arena.allocator();
    var fx = try Fixture.init(arloc);
    defer fx.tbl.deinit();
    const row = try sampleRow(&fx);

    // a = 5, b = "ab", c = 2.5; every kind renders, NULL is skipped.
    try testing.expectEqualStrings(
        "ab,5|2.5",
        (try fx.eval("b = CONCAT(b, ',', a, NULL, '|', c)", row)).text,
    );
    try testing.expectEqualStrings(
        "x",
        (try fx.eval("b = CONCAT(NULL, 'x')", row)).text,
    );
    // CONCAT of nothing but NULLs is the empty string, not NULL.
    try testing.expectEqualStrings(
        "",
        (try fx.eval("b = CONCAT(NULL, NULL)", row)).text,
    );
    try testing.expectEqualStrings(
        "ab",
        (try fx.eval("b = COALESCE(NULL, b, 'z')", row)).text,
    );
    try testing.expectEqual(Value{ .int4 = 5 }, try fx.eval("a = COALESCE(a, 0)", row));
    try testing.expectEqual(Value{ .int4 = 7 }, try fx.eval("a = COALESCE(NULL, 7)", row));
    try testing.expectEqual(Value{ .float8 = 2.5 }, try fx.eval("c = COALESCE(c, 1)", row));
    try testing.expectEqual(Value.null_value, try fx.eval("a = COALESCE(NULL, NULL)", row));
    // A call is an operand like any other.
    try testing.expectEqual(Value{ .int4 = 6 }, try fx.eval("a = COALESCE(a, 0) + 1", row));
    try testing.expectEqualStrings(
        "ab!",
        (try fx.eval("b = CONCAT(b) || '!'", row)).text,
    );

    // COALESCE's arguments are of one kind; both calls take only what
    // a Scalar holds; the result must fit the target.
    try testing.expectError(error.NoSuchOperator, fx.bind("b = COALESCE(b, a)"));
    try testing.expectError(error.TypeMismatch, fx.bind("a = CONCAT(a, b)"));
    try testing.expectError(error.TypeMismatch, fx.bind("a = COALESCE(c, 1)"));
}

test "sql.expr: EXCLUDED.column is the proposed row's cell, absent in a plain UPDATE" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const arloc = arena.allocator();
    var fx = try Fixture.init(arloc);
    defer fx.tbl.deinit();
    const row = try sampleRow(&fx);
    // The proposed row: a = 9, b = "new", the rest NULL.
    const proposed = [_]Value{
        .{ .int4 = 9 }, .{ .text = "new" }, .null_value,
        .null_value,    .null_value,        .null_value,
    };

    const concat = try fx.bindExcluded("b = CONCAT(b, ',', EXCLUDED.b)", &proposed);
    try testing.expectEqualStrings(
        "ab,new",
        (try concat.root.eval(fx.row(row), concat.target)).text,
    );
    const copy = try fx.bindExcluded("a = EXCLUDED.a", &proposed);
    try testing.expectEqual(
        Value{ .int4 = 9 },
        try copy.root.eval(fx.row(row), copy.target),
    );
    const sum = try fx.bindExcluded("a = a + EXCLUDED.a", &proposed);
    try testing.expectEqual(
        Value{ .int4 = 14 },
        try sum.root.eval(fx.row(row), sum.target),
    );
    // A NULL proposed cell is NULL; the type check still applies.
    const nul = try fx.bindExcluded("c = EXCLUDED.c", &proposed);
    try testing.expectEqual(
        Value.null_value,
        try nul.root.eval(fx.row(row), nul.target),
    );
    try testing.expectError(error.TypeMismatch, fx.bindExcluded("a = EXCLUDED.b", &proposed));
    try testing.expectError(error.UndefinedColumn, fx.bindExcluded("a = EXCLUDED.zz", &proposed));
    // Without a proposed row there is no EXCLUDED.
    try testing.expectError(error.UndefinedColumn, fx.bind("a = EXCLUDED.a"));
}

test "sql.expr: the function table's string, number and NULL forms" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const arloc = arena.allocator();
    var fx = try Fixture.init(arloc);
    defer fx.tbl.deinit();
    // a = 5, b = "ab", c = 2.5.
    const row = try sampleRow(&fx);
    const text = struct {
        fn of(f: *const Fixture, r: Item, want: String, set: String) !void {
            try testing.expectEqualStrings(want, (try f.eval(set, r)).text);
        }
    }.of;

    try text(&fx, row, "AB", "b = upper(b)");
    try text(&fx, row, "abc", "b = lower('ABC')");
    try expectEval(&fx, row, .{ .int4 = 5 }, "a = length('héllo')");
    try expectEval(&fx, row, .{ .int2 = 1 }, "d = char_length('x')");
    try text(&fx, row, "bcd", "b = substring('abcdef', 2, 3)");
    try text(&fx, row, "a", "b = substring('abcdef', -1, 3)");
    try text(&fx, row, "b", "b = substr(b, 2)");
    try text(&fx, row, "", "b = substring(b, 9)");
    try text(&fx, row, "a-b-c", "b = replace('a.b.c', '.', '-')");
    try text(&fx, row, "x", "b = btrim('  x  ')");
    try text(&fx, row, "x", "b = trim(both 'é' from 'éxé')");
    try text(&fx, row, "a", "b = ltrim('xxa', 'x')");
    try text(&fx, row, "a", "b = rtrim('axx', 'x')");
    try text(&fx, row, "ab", "b = left('abcdef', 2)");
    try text(&fx, row, "abcd", "b = left('abcdef', -2)");
    try text(&fx, row, "ef", "b = right('abcdef', 2)");
    try text(&fx, row, "a-ab", "b = concat_ws('-', 'a', NULL, b)");

    try expectEval(&fx, row, .{ .int4 = 5 }, "a = abs(-a)");
    try expectEval(&fx, row, .{ .float8 = 2.5 }, "c = abs(-c)");
    try expectEval(&fx, row, .{ .float8 = 2 }, "c = floor(c)");
    try expectEval(&fx, row, .{ .float8 = 3 }, "c = ceiling(c)");
    try expectEval(&fx, row, .{ .float8 = 3 }, "c = round(2.5)");
    try expectEval(&fx, row, .{ .float8 = -3 }, "c = round(-2.5)");
    try expectEval(&fx, row, .{ .float8 = 2.38 }, "c = round(2.375, 2)");
    try expectEval(&fx, row, .{ .float8 = 2 }, "c = trunc(2.75)");
    try expectEval(&fx, row, .{ .int4 = 10 }, "a = round(a, -1)");
    try expectEval(&fx, row, .{ .int4 = 1200 }, "a = round(1249, -2)");
    try expectEval(&fx, row, .{ .int4 = -1300 }, "a = round(-1250, -2)");
    try expectEval(&fx, row, .{ .int4 = 1200 }, "a = trunc(1299, -2)");
    try expectEval(&fx, row, .{ .int4 = 7 }, "a = floor(7)");
    try expectEval(&fx, row, .{ .int4 = -1 }, "a = sign(-3)");
    try expectEval(&fx, row, .{ .float8 = -1 }, "c = sign(-2.5)");
    try expectEval(&fx, row, .{ .int4 = 1 }, "a = mod(7, 3)");
    try expectEval(&fx, row, .{ .int4 = -1 }, "a = mod(-7, 3)");
    try expectEval(&fx, row, .{ .float8 = 1024 }, "c = power(2, 10)");
    try expectEval(&fx, row, .{ .float8 = 4 }, "c = sqrt(16)");

    try expectEval(&fx, row, .null_value, "a = nullif(a, 5)");
    try expectEval(&fx, row, .{ .int4 = 5 }, "a = nullif(a, 6)");
    try expectEval(&fx, row, .{ .int4 = 5 }, "a = nullif(a, NULL)");
    try text(&fx, row, "ab", "b = nullif(b, 'zz')");
    try expectEval(&fx, row, .{ .int4 = 9 }, "a = greatest(a, 9, 3)");
    try expectEval(&fx, row, .{ .float8 = 1 }, "c = least(c, 1)");
    try expectEval(&fx, row, .null_value, "a = greatest(NULL, NULL)");
    try text(&fx, row, "a", "b = least('b', 'a')");
    // A strict function is NULL on a NULL argument.
    try expectEval(&fx, row, .null_value, "a = abs(NULL)");
    try expectEval(&fx, row, .null_value, "b = upper(NULL)");

    try expectEvalErr(&fx, row, error.SubstringLength, "b = substr(b, 1, -1)");
    try expectEvalErr(&fx, row, error.PowerArgument, "c = sqrt(-1)");
    try expectEvalErr(&fx, row, error.PowerArgument, "c = power(0, -1)");
    try expectEvalErr(&fx, row, error.DivisionByZero, "a = mod(a, 0)");
    // Argument kinds are checked at bind, per position or across.
    try testing.expectError(error.NoSuchOperator, fx.bind("b = lower(a)"));
    try testing.expectError(error.NoSuchOperator, fx.bind("a = abs(b)"));
    try testing.expectError(error.NoSuchOperator, fx.bind("b = nullif(b, 1)"));
    try testing.expectError(error.NoSuchOperator, fx.bind("b = left(b, b)"));
    try testing.expectError(error.TypeMismatch, fx.bind("a = lower(b)"));
}
