// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! SET value expressions over stored rows. `SetNode` is the resolved
//! form of one `column = expr` assignment — parse.Expr with its
//! columns bound to the table, its literals typed, and its operators
//! type-checked — and `SetNode.eval` computes it against one row's
//! pre-image. The pred.zig pattern: `bindNode` is pure and reports an
//! error set, `bind` adds the client-facing error mapping.

const std = @import("std");
const pgwire = @import("pgwire");
const zdb = @import("zdb");

const _parse = @import("parse.zig");
const _table = @import("table.zig");
const _value = @import("value.zig");
const String = _value.String;
const _server = @import("server.zig");

const replyErrorCode = _server.replyErrorCode;
const Column = _table.Column;
const Table = _table.Table;
const Value = _value.Value;
const Type = _value.Type;
const Op = _parse.Expr.Op;

/// A SET value bound to its table: parse.Expr with its leaves
/// resolved — a literal is a typed cell, a column is the table's —
/// and every operator's result type fixed. The root literal is typed
/// by the target column (parse.litToValue: `SET a = 1`, `SET name =
/// 'x'`, `SET a = NULL`); a literal under an operator is the widest
/// cell of its kind (int8 / float8 / text). At eval, leaves lift to a
/// Scalar — integers compute in i128, floats in f64, text is bytes for
/// `||` — and the result is coerced to the target column, unless the
/// root is a leaf already of the target's type: then it IS the cell
/// (any type; Scalar never sees a date or a uuid). Children are
/// pointers only because a union cannot hold itself by value.
pub const SetNode = union(enum) {
    value: Value,
    column: *const Column,
    binop: BinOp,

    pub const BinOp = struct {
        op: Op,
        lhs: *const SetNode,
        rhs: *const SetNode,
        /// What the operator yields (resultType), fixed at bind.
        ty: ?Type,
    };

    /// The type this node evaluates to, or null for a NULL-only
    /// subtree (which takes the other operand's type, and fits any
    /// column).
    pub fn ty(self: *const SetNode) ?Type {
        return switch (self.*) {
            .value => |v| v.ty(),
            .column => |col| col.ty,
            .binop => |b| b.ty,
        };
    }

    /// Whether this tree's result may be assigned to `target`.
    pub fn fits(self: *const SetNode, target: *const Column) bool {
        const result = self.ty() orelse return true;
        return result.assignableTo(target.ty);
    }

    /// Evaluate against a row's pre-image, as a cell of `target` — the
    /// column this tree was bound against. A text result borrows
    /// `item.value` (a column, a text literal) or lives in `arloc` (a
    /// `||` result); either outlives the row's rewrite.
    pub fn eval(
        self: *const SetNode,
        arloc: std.mem.Allocator,
        tbl: *const Table,
        item: zdb.kv.Item,
        target: *const Column,
    ) EvalError!Value {
        std.debug.assert(self.fits(target));
        // A leaf already of the target's type is the cell itself.
        // (A string column of another length is not a leaf of the
        // target's type: its cell must go through the length check.)
        switch (self.*) {
            .value => |v| if (v == .null_value or v.ty() == target.ty) return v,
            .column => |col| if (col.ty == target.ty and col.type_mod == target.type_mod)
                return tbl.parseCol(item, col),
            .binop => {},
        }
        const result = try self.scalar(arloc, tbl, item) orelse
            return .null_value;
        return coerceScalar(result, target);
    }

    /// This subtree's value for one row, before assignment; null = SQL
    /// NULL.
    fn scalar(
        self: *const SetNode,
        arloc: std.mem.Allocator,
        tbl: *const Table,
        item: zdb.kv.Item,
    ) EvalError!?Scalar {
        switch (self.*) {
            .value => |v| return scalarOf(v),
            .column => |col| return scalarOf(tbl.parseCol(item, col)),
            .binop => |b| {
                // Both sides first, then the NULL check: PG raises on
                // `NULL + 1/0` too.
                const lhs = try b.lhs.scalar(arloc, tbl, item);
                const rhs = try b.rhs.scalar(arloc, tbl, item);
                const l = lhs orelse return null;
                const r = rhs orelse return null;
                return try applyOp(arloc, b.op, l, r);
            },
        }
    }
};

pub const BindError = error{
    UndefinedColumn,
    InvalidValue,
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
/// `bind` adds the wire; pub for the unit tests, which have no
/// connection to reply on.
pub fn bindNode(
    arloc: std.mem.Allocator,
    tbl: *const Table,
    expr: _parse.Expr,
    target: ?*const Column,
    params: _parse.Params,
) BindError!*const SetNode {
    const node = try arloc.create(SetNode);
    switch (expr) {
        .literal => |lit| node.* = .{
            .value = try literalValue(arloc, lit, target, params),
        },
        .column => |name| node.* = .{
            .column = tbl.column(name) orelse return error.UndefinedColumn,
        },
        .binop => |b| {
            const lhs = try bindNode(arloc, tbl, b.lhs.*, null, params);
            const rhs = try bindNode(arloc, tbl, b.rhs.*, null, params);
            node.* = .{ .binop = .{
                .op = b.op,
                .lhs = lhs,
                .rhs = rhs,
                .ty = try resultType(b.op, lhs.ty(), rhs.ty()),
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
/// float8, text. A `$n` under an operator is its declared type when
/// it came in binary; a text one has only its text to go by, and is
/// read the way an unknown-typed literal would be: an integer, else a
/// float, else text (`abalance + $1` computes in int8 and is
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

/// A numeric literal under an operator, from the text the scanner kept.
/// An integer too wide for int4 arrives as float TEXT; it stays an
/// integer when it parses as one (as textToValue does for a root
/// literal).
fn numberFromText(text: String) BindError!Value {
    if (std.fmt.parseInt(i64, text, 10)) |n| return .{ .int8 = n } else |_| {}
    const f = std.fmt.parseFloat(f64, text) catch return error.InvalidValue;
    return .{ .float8 = f };
}

/// A bound value with no type to read it by: a number when it is
/// one, else text.
fn untypedText(text: String) Value {
    return numberFromText(text) catch .{ .text = text };
}

/// The type `lhs op rhs` yields, or NoSuchOperator when the operator
/// has no such operands: `||` takes two texts and yields text;
/// arithmetic takes two numbers and yields float8 if either is a
/// float, else int8 (the compute is i128 / f64 regardless, and the
/// width is checked on assignment); `%` has no float form (as in
/// PG). A NULL-only side takes the other's type.
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
    return .int8;
}

/// Bind one SET assignment to its table and target column, replying
/// the client-facing error. null = the SQL error was already sent and
/// the statement is over; only OOM propagates.
pub fn bind(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const Table,
    target: *const Column,
    expr: _parse.Expr,
    params: _parse.Params,
) !?*const SetNode {
    const code = pgwire.sqlstate;
    return bindNode(arloc, tbl, expr, target, params) catch |err| {
        switch (err) {
            error.OutOfMemory => return err,
            error.UndefinedColumn => try replyErrorCode(
                state,
                code.undefined_column,
                "column does not exist",
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
            error.StringTooLong => try replyStringTooLong(state, arloc, target),
            error.InvalidUtf8 => try replyInvalidUtf8(state),
        }
        return null;
    };
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
    FloatOutOfRange,
    DivisionByZero,
    StringTooLong,
    InvalidUtf8,
    OutOfMemory,
};

/// What a subtree evaluates to before assignment. Integers of every
/// width compute as i128, floats as f64.
const Scalar = union(enum) { int: i128, float: f64, text: String };

/// A cell as an operand; null = SQL NULL. Only the types bind admits
/// reach here.
fn scalarOf(v: Value) ?Scalar {
    return switch (v) {
        .null_value => null,
        inline .int2, .int4, .int8, .uint2, .uint4, .uint8 => |n| .{ .int = n },
        inline .float4, .float8 => |f| .{ .float = f },
        .text => |s| .{ .text = s },
        else => unreachable,
    };
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
    return .{ .float = try floatOp(op, toFloat(lhs), toFloat(rhs)) };
}

fn toFloat(s: Scalar) f64 {
    return switch (s) {
        .int => |n| @floatFromInt(n),
        .float => |f| f,
        .text => unreachable,
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
        else => unreachable,
    }
}

fn intCast(comptime T: type, s: Scalar) EvalError!T {
    return std.math.cast(T, s.int) orelse error.IntOutOfRange;
}
