// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Expressions over stored rows: a SET value, a WHERE operand, a
//! SELECT target. `Node` is the resolved form — parse.Expr with its
//! columns bound to the table, its literals typed, and its operators
//! and calls type-checked — `Node.cell` computes it against one row,
//! and `Node.eval` assigns the result to a column. `bind` is pure and
//! reports an error set; `bindOrReply` adds the client-facing mapping.

const std = @import("std");
const pgwire = @import("pgwire");
const zdb = @import("zdb");

const _funcs = @import("funcs.zig");
const _parse = @import("parse.zig");
const _table = @import("table.zig");
const _value = @import("value.zig");
const String = _value.String;
const _server = @import("server.zig");

const replyErrorCode = _server.replyErrorCode;
const Column = _table.Column;
const Table = _table.Table;
const Value = _value.Value;
const Numeric = _value.Numeric;
const Type = _value.Type;
const Op = _parse.Expr.Op;
const Func = _funcs.Func;

/// An expression bound to its table: parse.Expr with its leaves
/// resolved — a literal is a typed cell, a column is the table's —
/// and every operator's result type fixed. A root literal is typed by
/// the column it meets, when there is one (parse.litToValue: `SET a =
/// 1`, `WHERE name = 'x'`); any other literal is the widest cell of
/// its kind (int8 / numeric / text). At eval, leaves lift to a Scalar
/// — integers compute in i128, floats in f64, text is bytes for `||`
/// — and the result is coerced to the target column or widened to a
/// cell of its own type, unless the root is a leaf: then it IS the
/// cell (any type; Scalar never sees a date or a uuid). Children are
/// pointers only because a union cannot hold itself by value.
/// One stored row an expression is evaluated against: its table, the
/// item, and where a computed value lives. The executor passes its
/// per-row scratch, reset after the row; a SET passes the statement
/// arena, since the value outlives the row. Text a leaf yields
/// borrows `item.value` either way.
pub const Row = struct {
    scratch: std.mem.Allocator,
    tbl: *const Table,
    item: zdb.kv.Item,
};

pub const Node = union(enum) {
    value: Value,
    column: *const Column,
    /// A `$n` bound before Bind, `SELECT n + $1 FROM t` under a
    /// statement's Describe: the type Parse declared, null when it
    /// declared none (resolved by its context, as a NULL leaf is).
    /// Describe reads only types; Execute binds anew with values.
    param: ?Type,
    binop: BinOp,
    call: CallNode,

    pub const BinOp = struct {
        op: Op,
        lhs: *const Node,
        rhs: *const Node,
        /// What the operator yields (resultType), fixed at bind.
        ty: ?Type,
    };
    pub const CallNode = struct {
        func: *const Func,
        args: []const *const Node,
        /// What the call yields (Func.resultType), fixed at bind.
        ty: ?Type,
    };

    /// The type this node evaluates to, or null for a NULL-only
    /// subtree (which takes the other operand's type, and fits any
    /// column).
    pub fn ty(self: *const Node) ?Type {
        return switch (self.*) {
            .value => |v| v.ty(),
            .column => |col| col.ty,
            .param => |t| t,
            .binop => |b| b.ty,
            .call => |call| call.ty,
        };
    }

    /// Whether this tree's result may be assigned to `target`.
    pub fn fits(self: *const Node, target: *const Column) bool {
        const result = self.ty() orelse return true;
        return result.assignableTo(target.ty);
    }

    /// This tree's value for one row as a cell of its own type: a leaf
    /// is the cell itself, whatever its type; a computed result is the
    /// widest cell of its kind (int8, float8, text). Text borrows
    /// `row.item` or lives in `row.scratch`.
    pub fn cell(self: *const Node, row: Row) EvalError!Value {
        switch (self.*) {
            .value => |v| return v,
            .column => |col| return row.tbl.parseCol(row.item, col),
            .param => unreachable, // Describe's tree is never evaluated
            .binop, .call => {},
        }
        const s = try self.scalar(row) orelse return .null_value;
        return switch (s) {
            .int => .{ .int8 = try intCast(i64, s) },
            .float => |f| .{ .float8 = f },
            .numeric => |d| .{ .numeric = d },
            .text => |t| .{ .text = t },
        };
    }

    /// Evaluate against a row's pre-image, as a cell of `col` — the
    /// column this tree was bound against. A text result borrows
    /// `row.item` (a column, a text literal) or lives in `row.scratch`
    /// (a `||` result), the statement arena for a SET; either outlives
    /// the row's rewrite.
    pub fn eval(
        self: *const Node,
        row: Row,
        col: *const Column,
    ) EvalError!Value {
        std.debug.assert(self.fits(col));
        // A leaf already of the target's type is the cell itself.
        // (A string column of another length is not a leaf of the
        // target's type: its cell must go through the length check.)
        switch (self.*) {
            .value => |v| if (v == .null_value or v.ty() == col.ty) return v,
            .column => |scol| if (scol.ty == col.ty and scol.type_mod == col.type_mod)
                return row.tbl.parseCol(row.item, scol),
            .param => unreachable, // Describe's tree is never evaluated
            .binop, .call => {},
        }
        const result = try self.scalar(row) orelse return .null_value;
        return coerceScalar(result, col);
    }

    /// This subtree's value for one row, before assignment; null = SQL
    /// NULL.
    fn scalar(self: *const Node, row: Row) EvalError!?Scalar {
        switch (self.*) {
            .value => |v| return scalarOf(v),
            .column => |col| return scalarOf(row.tbl.parseCol(row.item, col)),
            .param => unreachable, // Describe's tree is never evaluated
            .binop => |b| {
                // Both sides first, then the NULL check: PG raises on
                // `NULL + 1/0` too.
                const lhs = try b.lhs.scalar(row);
                const rhs = try b.rhs.scalar(row);
                const l = lhs orelse return null;
                const r = rhs orelse return null;
                return try applyOp(row.scratch, b.op, l, r);
            },
            .call => |call| {
                // Every argument first, as for an operator: PG raises
                // on `COALESCE(NULL, 1/0)` too.
                const vals = try row.scratch.alloc(?Scalar, call.args.len);
                for (call.args, vals) |arg, *val| val.* = try arg.scalar(row);
                switch (call.func.eval) {
                    .strict => |f| {
                        const plain = try row.scratch.alloc(Scalar, vals.len);
                        for (vals, plain) |val, *slot|
                            slot.* = val orelse return null;
                        return try f(row.scratch, plain);
                    },
                    .lax => |f| return try f(row.scratch, vals),
                }
            },
        }
    }
};

pub const BindError = error{
    UndefinedColumn,
    InvalidValue,
    NumericOutOfRange,
    UnsupportedValue,
    TypeMismatch,
    NoSuchOperator,
    StringTooLong,
    InvalidUtf8,
    OutOfMemory,
};

/// Bind one node and, recursively, its children, pure: names →
/// columns, literals and `$n` values → typed cells, operators
/// type-checked against their operands. `target` is the column being
/// assigned — set at the root, null under an operator. A literal
/// takes its type (as PG resolves an "unknown" literal from where it
/// stands: `'7'` into an int, `'t'` into a bool), and the root's
/// result must fit it. `arloc` (the statement arena) backs the tree.
/// `bindOrReply` adds the wire; pub for the unit tests, which have no
/// connection to reply on.
pub fn bind(
    arloc: std.mem.Allocator,
    tbl: *const Table,
    expr: _parse.Expr,
    target: ?*const Column,
    params: _parse.Params,
    excluded: ?[]const Value,
) BindError!*const Node {
    const node = try arloc.create(Node);
    switch (expr) {
        // `SELECT n + $1 FROM t` under a statement's Describe: no
        // value for $1 yet, so the leaf is the type Parse declared.
        .literal => |lit| node.* = if (lit == .param and
            !params.bound(lit.param))
            .{ .param = params.declared(lit.param) }
        else
            .{ .value = try literalValue(arloc, lit, target, params) },
        .column => |name| node.* = .{
            .column = tbl.column(name) orelse return error.UndefinedColumn,
        },
        // EXCLUDED.col in ON CONFLICT DO UPDATE SET: the proposed
        // row's cell, already of its column's type, bound as a
        // constant. A SET with no proposed row (UPDATE) has no EXCLUDED.
        .excluded => |name| {
            const cells = excluded orelse return error.UndefinedColumn;
            const col = tbl.column(name) orelse return error.UndefinedColumn;
            node.* = .{ .value = cells[col.pos] };
        },
        .binop => |b| {
            const lhs = try bind(arloc, tbl, b.lhs.*, null, params, excluded);
            const rhs = try bind(arloc, tbl, b.rhs.*, null, params, excluded);
            node.* = .{ .binop = .{
                .op = b.op,
                .lhs = lhs,
                .rhs = rhs,
                .ty = try resultType(b.op, lhs.ty(), rhs.ty()),
            } };
        },
        .call => |call| {
            const args = try arloc.alloc(*const Node, call.args.len);
            for (call.args, args) |arg, *slot|
                slot.* = try bind(arloc, tbl, arg, null, params, excluded);
            const tys = try arloc.alloc(?Type, args.len);
            for (args, tys) |arg, *t| t.* = arg.ty();
            node.* = .{ .call = .{
                .func = call.func,
                .args = args,
                .ty = try call.func.resultType(tys),
            } };
        },
    }
    if (target) |col| {
        if (!node.fits(col)) return error.TypeMismatch;
    }
    return node;
}

/// A literal as a cell: of `target`'s type when there is one
/// (parse.litToValue), else the widest cell of its own kind — int8,
/// numeric, text. A `$n` under an operator is its declared type when
/// it came in binary; a text one has only its text to go by, and is
/// read the way an unknown-typed literal would be: an integer, else a
/// numeric, else text (`abalance + $1` computes in int8 and is
/// range-checked on assignment). (A non-literal item never parses —
/// parseExpr rejects it.)
fn literalValue(
    arloc: std.mem.Allocator,
    lit: _parse.Literal,
    target: ?*const Column,
    params: _parse.Params,
) BindError!Value {
    if (target) |col| {
        return _parse.litToValue(
            arloc,
            lit,
            col,
            params,
        ) catch |err| switch (err) {
            error.OutOfMemory => return error.OutOfMemory,
            error.UnsupportedLiteral,
            error.UnsupportedType,
            => return error.UnsupportedValue,
            error.StringTooLong => return error.StringTooLong,
            error.InvalidUtf8 => return error.InvalidUtf8,
            else => return error.InvalidValue,
        };
    }
    return switch (lit) {
        .int => |n| .{ .int8 = n },
        .float => |text| try numberFromText(text),
        .str => |s| .{ .text = s },
        .bool_ => |b| .{ .bool_ = b },
        .null_value => .null_value,
        .param => |n| {
            const v = params.untyped(n) catch |err| switch (err) {
                error.UnsupportedType => return error.UnsupportedValue,
                else => return error.InvalidValue,
            };
            return if (v == .text) untypedText(v.text) else v;
        },
        // parseExpr yields neither: a SET value is a constant or a
        // column, never a value function.
        .unsupported, .current_timestamp => unreachable,
    };
}

/// A numeric literal under an operator, from the text the scanner kept:
/// an int8 when it parses as one, else a numeric, as Postgres types an
/// unknown `1.5` (so `0.1 + 0.2` is exactly 0.3). An integer too wide
/// for int8 is a numeric too; one past numeric's 38 digits, `1e308`,
/// is a float8, the only cell that holds it.
fn numberFromText(text: String) BindError!Value {
    if (std.fmt.parseInt(i64, text, 10)) |n| return .{ .int8 = n } else |_| {}
    const d = Numeric.parse(text) catch |err| switch (err) {
        error.NumericOutOfRange => {
            const f = std.fmt.parseFloat(f64, text) catch
                return error.InvalidValue;
            return .{ .float8 = f };
        },
        else => return error.InvalidValue,
    };
    return .{ .numeric = d };
}

/// A bound value with no type to read it by: a number when it is
/// one, else text.
fn untypedText(text: String) Value {
    return numberFromText(text) catch .{ .text = text };
}

/// The type `lhs op rhs` yields, or NoSuchOperator when the operator
/// has no such operands: `||` takes two texts and yields text;
/// arithmetic takes two numbers and yields float8 if either is a
/// float, numeric if either is one, else int8 (the compute is i128 /
/// f64 / exact regardless, and the width is checked on assignment);
/// `%` has no float form (as in PG). A NULL-only side takes the
/// other's type.
fn resultType(op: Op, lhs: ?Type, rhs: ?Type) BindError!?Type {
    const l = lhs orelse rhs orelse return null;
    const r = rhs orelse l;
    if (op == .concat) {
        if (!l.isString() or !r.isString()) return error.NoSuchOperator;
        return .text;
    }
    if (!l.isNumeric() or !r.isNumeric()) return error.NoSuchOperator;
    if (l.isFloat() or r.isFloat()) {
        if (op == .mod) return error.NoSuchOperator;
        return .float8;
    }
    if (l == .numeric or r == .numeric) return .numeric;
    return .int8;
}

/// Bind one SET assignment to its table and target column, replying
/// the client-facing error. null = the SQL error was already sent and
/// the statement is over; only OOM propagates.
pub fn bindOrReply(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const Table,
    target: *const Column,
    expr: _parse.Expr,
    params: _parse.Params,
    excluded: ?[]const Value,
) !?*const Node {
    return bind(arloc, tbl, expr, target, params, excluded) catch |err| {
        if (err == error.OutOfMemory) return err;
        try replyBindErr(state, arloc, err, target);
        return null;
    };
}

/// A bind failure on the wire. `target` is the column the expression
/// is assigned to, when there is one: a too-long string names it.
pub fn replyBindErr(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    err: BindError,
    target: ?*const Column,
) !void {
    const code = pgwire.sqlstate;
    switch (err) {
        error.OutOfMemory => return err,
        error.UndefinedColumn => try replyErrorCode(
            state,
            code.undefined_column,
            "column does not exist",
        ),
        error.NumericOutOfRange => try replyErrorCode(
            state,
            pgwire.sqlstate.numeric_value_out_of_range,
            "numeric field overflow",
        ),
        error.InvalidValue => try replyErrorCode(
            state,
            code.invalid_text_representation,
            "invalid value for column type",
        ),
        error.UnsupportedValue => try replyErrorCode(
            state,
            code.feature_not_supported,
            "unsupported value or column type",
        ),
        error.TypeMismatch => try replyErrorCode(
            state,
            code.datatype_mismatch,
            "expression type does not match column type",
        ),
        error.NoSuchOperator => try replyErrorCode(
            state,
            code.undefined_function,
            "operator does not exist for these operand types",
        ),
        error.StringTooLong => if (target) |col|
            try replyStringTooLong(state, arloc, col)
        else
            try replyErrorCode(
                state,
                code.string_data_right_truncation,
                "value too long",
            ),
        error.InvalidUtf8 => try replyInvalidUtf8(state),
    }
}

/// 22001 with Postgres's wording: `value too long for type
/// character(84)` / `character varying(20)`. Shared with dml.zig
/// (INSERT literals, SET results at eval).
pub fn replyStringTooLong(
    state: *pgwire.ConnState,
    a: std.mem.Allocator,
    col: *const Column,
) !void {
    const type_name: String = if (col.ty == .bpchar) "character" else "character varying";
    const msg = try std.fmt.allocPrint(
        a,
        "value too long for type {s}({d})",
        .{ type_name, col.type_mod orelse 1 },
    );
    try replyErrorCode(state, pgwire.sqlstate.string_data_right_truncation, msg);
}

/// 22021, Postgres's wording.
pub fn replyInvalidUtf8(state: *pgwire.ConnState) !void {
    try replyErrorCode(
        state,
        pgwire.sqlstate.character_not_in_repertoire,
        "invalid byte sequence for encoding \"UTF8\"",
    );
}

pub const EvalError = error{
    IntOutOfRange,
    NumericOutOfRange,
    FloatOutOfRange,
    DivisionByZero,
    SubstringLength,
    PowerArgument,
    StringTooLong,
    InvalidUtf8,
    OutOfMemory,
};

/// What a subtree evaluates to before assignment. Integers of every
/// width compute as i128, floats as f64, numerics exactly.
pub const Scalar = union(enum) {
    int: i128,
    float: f64,
    numeric: Numeric,
    text: String,
};

/// A cell as an operand; null for SQL NULL and for a cell of a kind
/// no operator or function takes (a date, a uuid), which bind keeps
/// away from them.
fn scalarOf(v: Value) ?Scalar {
    return switch (v) {
        inline .int2, .int4, .int8, .uint2, .uint4, .uint8 => |n| .{ .int = n },
        inline .float4, .float8 => |f| .{ .float = f },
        .numeric => |d| .{ .numeric = d },
        .text => |s| .{ .text = s },
        else => null,
    };
}

/// Whether `lhs op rhs` has a meaning: two numbers, two strings, or
/// two cells of one type; a NULL-only side compares with anything.
pub fn comparable(lhs: ?Type, rhs: ?Type) bool {
    const l = lhs orelse return true;
    const r = rhs orelse return true;
    if (l.isNumeric() and r.isNumeric()) return true;
    if (l.isString() and r.isString()) return true;
    return l == r;
}

/// The order of two non-null cells a `comparable` pair yields: numbers
/// numerically across widths and kinds, strings bytewise, any other
/// type by value.orderValues.
pub fn compare(x: Value, y: Value) std.math.Order {
    if (scalarOf(x)) |l| if (scalarOf(y)) |r| return orderScalars(l, r);
    return _value.orderValues(x, y);
}

/// Two scalars of one kind: text bytewise, numbers as floats when
/// either is one, exactly otherwise.
pub fn orderScalars(x: Scalar, y: Scalar) std.math.Order {
    if (x == .text) return std.mem.order(u8, x.text, y.text);
    if (x == .float or y == .float)
        return std.math.order(toFloat(x), toFloat(y));
    if (x == .int and y == .int) return std.math.order(x.int, y.int);
    return Numeric.order(toNumeric(x), toNumeric(y));
}

fn applyOp(
    arloc: std.mem.Allocator,
    op: Op,
    lhs: Scalar,
    rhs: Scalar,
) EvalError!Scalar {
    if (op == .concat) {
        const joined = try std.mem.concat(arloc, u8, &.{ lhs.text, rhs.text });
        return .{ .text = joined };
    }
    if (lhs == .int and rhs == .int)
        return .{ .int = try intOp(op, lhs.int, rhs.int) };
    if (lhs == .float or rhs == .float)
        return .{ .float = try floatOp(op, toFloat(lhs), toFloat(rhs)) };
    return .{ .numeric = try numericOp(op, toNumeric(lhs), toNumeric(rhs)) };
}

fn toFloat(s: Scalar) f64 {
    return switch (s) {
        .int => |n| @floatFromInt(n),
        .float => |f| f,
        .numeric => |d| d.toFloat(),
        .text => unreachable,
    };
}

/// An integer or numeric operand as a decimal: what the exact
/// operations compute over.
pub fn toNumeric(s: Scalar) Numeric {
    return switch (s) {
        .int => |n| Numeric.fromInt(n),
        .numeric => |d| d,
        .float, .text => unreachable,
    };
}

/// Exact arithmetic (numeric.zig); a result past 38 digits is
/// NumericOutOfRange, as a coefficient the type cannot hold.
fn numericOp(op: Op, x: Numeric, y: Numeric) EvalError!Numeric {
    const r = switch (op) {
        .add => x.add(y),
        .sub => x.sub(y),
        .mul => x.mul(y),
        .div => x.div(y),
        .mod => x.mod(y),
        .concat => unreachable,
    };
    return r catch |err| switch (err) {
        error.DivisionByZero => error.DivisionByZero,
        error.NumericOutOfRange,
        error.InvalidNumeric,
        => error.NumericOutOfRange,
    };
}

/// Checked i128 arithmetic; every overflow is IntOutOfRange (the
/// assignment's range check catches what fits i128 but not the
/// column). `/` truncates toward zero and `%` takes the dividend's
/// sign, as in PG.
fn intOp(op: Op, x: i128, y: i128) EvalError!i128 {
    switch (op) {
        .add => {
            const r = @addWithOverflow(x, y);
            if (r[1] != 0) return error.IntOutOfRange;
            return r[0];
        },
        .sub => {
            const r = @subWithOverflow(x, y);
            if (r[1] != 0) return error.IntOutOfRange;
            return r[0];
        },
        .mul => {
            const r = @mulWithOverflow(x, y);
            if (r[1] != 0) return error.IntOutOfRange;
            return r[0];
        },
        .div, .mod => {
            if (y == 0) return error.DivisionByZero;
            // minInt / -1 overflows and minInt % -1 traps: treat -1
            // as a negation so neither can happen.
            if (y == -1) return if (op == .div) intOp(.sub, 0, x) else 0;
            return if (op == .div) @divTrunc(x, y) else @rem(x, y);
        },
        .concat => unreachable,
    }
}

/// f64 arithmetic. Finite operands producing an infinity have
/// overflowed (PG: "value out of range: overflow"); division by zero
/// raises as PG's float8div does. `%` never binds for floats.
fn floatOp(op: Op, x: f64, y: f64) EvalError!f64 {
    const r = switch (op) {
        .add => x + y,
        .sub => x - y,
        .mul => x * y,
        .div => if (y == 0) return error.DivisionByZero else x / y,
        .mod, .concat => unreachable,
    };
    if (std.math.isInf(r) and !std.math.isInf(x) and !std.math.isInf(y))
        return error.FloatOutOfRange;
    return r;
}

/// Assign a computed scalar to `target`: integers with a range check,
/// an int into a float widened, an f64 into a float4 narrowed
/// (overflow → FloatOutOfRange), text through the column's string
/// rules (value.fitString: `char(n)` stripped, lengths checked).
/// Any other pairing never binds.
fn coerceScalar(s: Scalar, target: *const Column) EvalError!Value {
    switch (target.ty) {
        .int2 => return .{ .int2 = try intCast(i16, s) },
        .int4 => return .{ .int4 = try intCast(i32, s) },
        .int8 => return .{ .int8 = try intCast(i64, s) },
        .uint2 => return .{ .uint2 = try intCast(u16, s) },
        .uint4 => return .{ .uint4 = try intCast(u32, s) },
        .uint8 => return .{ .uint8 = try intCast(u64, s) },
        .float8 => return .{ .float8 = toFloat(s) },
        .float4 => {
            const wide = toFloat(s);
            const narrow: f32 = @floatCast(wide);
            if (std.math.isInf(narrow) and !std.math.isInf(wide))
                return error.FloatOutOfRange;
            return .{ .float4 = narrow };
        },
        .text, .bpchar, .varchar => return .{
            .text = try _value.fitString(s.text, target),
        },
        .numeric => {
            const d = if (s == .float)
                Numeric.fromFloat(s.float) catch return error.NumericOutOfRange
            else
                toNumeric(s);
            return .{ .numeric = try _value.fitNumeric(d, target) };
        },
        else => unreachable,
    }
}

fn intCast(comptime T: type, s: Scalar) EvalError!T {
    return std.math.cast(T, s.int) orelse error.IntOutOfRange;
}
