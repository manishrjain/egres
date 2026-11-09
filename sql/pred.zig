// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Boolean WHERE predicates over stored rows. `Pred` is the resolved
//! tree — parse.Where with its expressions bound to the table
//! (expr.Node) — and `eval` is its SQL three-valued evaluation against
//! one row. Choosing HOW rows are read (index vs. scan) is plan.zig's
//! job; a Pred only says which rows match.

const std = @import("std");
const pgwire = @import("pgwire");
const zdb = @import("zdb");

const _expr = @import("expr.zig");
const _parse = @import("parse.zig");
const _table = @import("table.zig");
const _value = @import("value.zig");
const _server = @import("server.zig");

const replyErrorCode = _server.replyErrorCode;
const Node = _expr.Node;
const EvalError = _expr.EvalError;

/// Children of AND/OR live inline: a slice is already an indirection
/// and children are never shared between parents. NOT is a pointer
/// only because a union cannot contain itself by value.
pub const Pred = union(enum) {
    cmp: Cmp,
    is_null: NullTest,
    bool_test: BoolTest,
    and_: []const Pred,
    or_: []const Pred,
    not: *const Pred,
    /// Constant false: a comparison bind already knows can never hold,
    /// such as a literal longer than its `char(n)` / `varchar(n)`
    /// column. The planner reads no rows for it. Though, if this is part of a
    /// NOT Pred, then it would always be true.
    never,

    /// `lhs op rhs`. A bare column against a constant has the column
    /// on the left (the parser turns the operator) and the constant
    /// typed by it: the shape the planner answers from an index.
    pub const Cmp = struct {
        lhs: *const Node,
        op: _parse.CmpOp = .eq,
        rhs: *const Node,
    };
    /// `expr IS NULL` (negate = false) / `expr IS NOT NULL`.
    pub const NullTest = struct { expr: *const Node, negate: bool };
    /// `expr IS [NOT] TRUE | FALSE | UNKNOWN` over a boolean.
    pub const BoolTest = struct {
        node: *const Node,
        is: _parse.Where.BoolIs,
        negate: bool,
    };

    /// The top-level AND terms: a root AND's children, else the root
    /// alone. Every one must hold for a row to match, which is what
    /// lets a source pick ANY of them to drive the read. Only the root
    /// is looked at — `a AND (b AND c)` parses right-nested and yields
    /// two terms, the second an AND.
    pub fn topLevelAnds(self: *const Pred) []const Pred {
        return switch (self.*) {
            .and_ => |kids| kids,
            else => self[0..1],
        };
    }

    /// SQL three-valued logic: null = unknown. A comparison is unknown
    /// when either side is NULL, except IS [NOT] DISTINCT FROM, which
    /// a NULL decides; IS [NOT] NULL and IS [NOT] TRUE are never
    /// unknown; AND / OR / NOT follow Kleene. A leaf of two bare cells
    /// reads only those (Table.read — a null-bit test plus one O(1)
    /// read each; no full-row decode); an expression computes into
    /// `row.scratch`.
    pub fn eval(self: *const Pred, row: _expr.Row) EvalError!?bool {
        switch (self.*) {
            .cmp => |c| {
                const lhs = try c.lhs.cell(row);
                const rhs = try c.rhs.cell(row);
                // WHERE a IS DISTINCT FROM NULL: true unless a is NULL.
                if (lhs == .null_value or rhs == .null_value) {
                    const both = (lhs == .null_value) == (rhs == .null_value);
                    return switch (c.op) {
                        .differs => !both,
                        .same => both,
                        else => null,
                    };
                }
                return c.op.holds(_expr.compare(lhs, rhs));
            },
            .is_null => |n| {
                const v = try n.expr.cell(row);
                return (v == .null_value) != n.negate;
            },
            // WHERE active IS NOT TRUE: a NULL active is not true.
            .bool_test => |t| {
                const v = try t.node.cell(row);
                const hit = switch (t.is) {
                    .true_ => v == .bool_ and v.bool_,
                    .false_ => v == .bool_ and !v.bool_,
                    .unknown => v == .null_value,
                };
                return hit != t.negate;
            },
            .and_ => |kids| {
                var unknown = false;
                // TODO: We should have a mechanism to surface false evaluations
                // quickly instead of going serially through every kid.eval.
                for (kids) |*kid| {
                    if (try kid.eval(row)) |b| {
                        if (!b) return false;
                    } else unknown = true;
                }
                return if (unknown) null else true;
            },
            .or_ => |kids| {
                var unknown = false;
                for (kids) |*kid| {
                    if (try kid.eval(row)) |b| {
                        if (b) return true;
                    } else unknown = true;
                }
                return if (unknown) null else false;
            },
            .not => |child| {
                const b = try child.eval(row) orelse return null;
                return !b;
            },
            .never => return false,
        }
    }

    /// A row matches only when the predicate is TRUE — unknown rejects,
    /// so `WHERE col = NULL` matches nothing.
    pub fn matches(self: *const Pred, row: _expr.Row) EvalError!bool {
        return try self.eval(row) orelse false;
    }
};

pub const BindError = _expr.BindError || error{EgidZero};

/// Bind one WHERE node — and, recursively, its children — to `tbl`:
/// its expressions through expr.bind. Pure; `bindOrReply` adds the
/// client-facing error mapping. `arloc` (the statement arena) backs
/// the tree.
pub fn bind(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    where: _parse.Where,
    params: pgwire.Params,
) BindError!Pred {
    switch (where) {
        .cmp => |f| {
            // The trailing null: a WHERE has no proposed row, so
            // EXCLUDED is unknown.
            const lhs = try _expr.bind(arloc, tbl, f.lhs.*, null, params, null);
            // A constant against a bare column takes the column's type,
            // as a literal does from any column it meets: WHERE name =
            // 'x' binds 'x' as name's type. So does one under a cast,
            // `id = '6'::int8` as a driver inlines a value, `f1 = int2
            // '0'`: the cast is dropped, and the leaf stays a column
            // against a cell of its type, the shape the planner answers
            // from an index.
            var rhs_expr = f.rhs;
            var target: ?*const _table.Column = null;
            if (lhs.* == .column) if (literalUnderCasts(f.rhs)) |lit| {
                rhs_expr = lit;
                target = lhs.column;
            };
            const rhs = _expr.bind(
                arloc,
                tbl,
                rhs_expr.*,
                target,
                params,
                null,
            ) catch |err| switch (err) {
                // Too long for a char(n) / varchar(n) column is not
                // an error in Postgres: every stored cell is at most
                // n codepoints, so `=` never matches, and the other
                // comparisons see the whole literal, as text.
                error.StringTooLong => if (f.op == .eq or f.op == .same)
                    return .never
                else
                    try _expr.bind(arloc, tbl, rhs_expr.*, null, params, null),
                else => return err,
            };

            if (!_expr.comparable(lhs.ty(), rhs.ty())) {
                return error.NoSuchOperator;
            }
            // The egid IS the row key. Its column is uint8, so a
            // negative literal already failed to coerce; zero (never a
            // row) is rejected for `=`, and `egid = NULL` is left to be
            // unknown like any other comparison.
            if (lhs.* == .column and lhs.column.isEgid() and f.op == .eq and
                rhs.* == .value and rhs.value == .uint8 and
                rhs.value.uint8 == 0) return error.EgidZero;
            return .{ .cmp = .{ .lhs = lhs, .op = f.op, .rhs = rhs } };
        },
        .is_null => |nt| return .{ .is_null = .{
            .expr = try _expr.bind(arloc, tbl, nt.expr.*, null, params, null),
            .negate = nt.negate,
        } },
        // WHERE n IS TRUE on an int column is 42804, as in Postgres.
        .bool_test => |bt| {
            const node = try _expr.bind(arloc, tbl, bt.expr.*, null, params, null);
            if (node.ty()) |ty| if (ty != .bool_) return error.TypeMismatch;
            return .{ .bool_test = .{
                .node = node,
                .is = bt.is,
                .negate = bt.negate,
            } };
        },
        .and_, .or_ => |kids| {
            const out = try arloc.alloc(Pred, kids.len);
            for (kids, out) |kid, *slot|
                slot.* = try bind(arloc, tbl, kid, params);
            return if (where == .and_) .{ .and_ = out } else .{ .or_ = out };
        },
        .not => |child| {
            const out = try arloc.create(Pred);
            out.* = try bind(arloc, tbl, child.*, params);
            return .{ .not = out };
        },
    }
}

/// The literal under zero or more casts, `'6'`, `'6'::int8`,
/// `'6'::int4::int8`; null for anything else.
fn literalUnderCasts(expr: *const _parse.Expr) ?*const _parse.Expr {
    var e = expr;
    while (e.* == .cast) e = e.cast.expr;
    return if (e.* == .literal) e else null;
}

/// `col op val` as a tree, for a predicate the server builds itself
/// (INSERT's arbiter probe) and for the tests.
pub fn colCmp(
    arloc: std.mem.Allocator,
    col: *const _table.Column,
    op: _parse.CmpOp,
    val: _value.Value,
) std.mem.Allocator.Error!Pred {
    const lhs = try arloc.create(Node);
    lhs.* = .{ .column = col };
    const rhs = try arloc.create(Node);
    rhs.* = .{ .value = val };
    return .{ .cmp = .{ .lhs = lhs, .op = op, .rhs = rhs } };
}

/// Bind a statement's WHERE to its table: bind plus the
/// client-facing error mapping. null = the SQL error was already sent
/// and the statement is over; only OOM propagates.
pub fn bindOrReply(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    where: _parse.Where,
    params: pgwire.Params,
) !?*const Pred {
    const p = try arloc.create(Pred);
    p.* = bind(arloc, tbl, where, params) catch |err| {
        switch (err) {
            error.OutOfMemory => return err,
            error.EgidZero => try replyErrorCode(
                state,
                pgwire.sqlstate.invalid_text_representation,
                "egid must be a positive integer",
            ),
            else => |e| try _expr.replyBindErr(
                state,
                arloc,
                @errorCast(e),
                null,
            ),
        }
        return null;
    };
    return p;
}
