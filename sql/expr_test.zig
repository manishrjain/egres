// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const pgwire = @import("pgwire");
const String = sql.value.String;
const zdb = @import("zdb");

const expr = sql.expr;
const SetNode = expr.SetNode;
const Value = sql.value.Value;
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
        root: *const SetNode,
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
        const root = try expr.bindNode(
            self.arloc,
            tbl,
            clause.value,
            target,
            params,
        );
        return .{ .target = target, .root = root };
    }

    /// Bind then evaluate one assignment against `row`.
    fn eval(self: *const Fixture, assignment: String, row: Item) !Value {
        const set = try self.bind(assignment);
        return set.root.eval(self.arloc, &self.tbl, row, set.target);
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
    const mixed = try fx.bind("c = a + 1.5"); // int with float → float8
    try testing.expectEqual(sql.value.Type.float8, mixed.root.ty().?);
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
    try testing.expectEqual(Value{ .float8 = 1.5 }, mixed.root.binop.rhs.value);
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
}
