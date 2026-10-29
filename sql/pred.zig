// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Boolean WHERE predicates over stored rows. `Pred` is the resolved
//! tree — parse.Where with its columns bound to the table and its
//! literals coerced to the columns' types — and `eval` is its SQL
//! three-valued evaluation against one row. Choosing HOW rows are read
//! (index vs. scan) is plan.zig's job; a Pred only says which rows
//! match.

const std = @import("std");
const pgwire = @import("pgwire");
const zdb = @import("zdb");

const _parse = @import("parse.zig");
const _table = @import("table.zig");
const _value = @import("value.zig");
const _server = @import("server.zig");

const replyErrorCode = _server.replyErrorCode;

/// Children of AND/OR live inline: a slice is already an indirection
/// and children are never shared between parents. NOT is a pointer
/// only because a union cannot contain itself by value.
pub const Pred = union(enum) {
    eq: Cmp,
    is_null: NullTest,
    and_: []const Pred,
    or_: []const Pred,
    not: *const Pred,
    /// Constant false: a comparison bind already knows can never hold,
    /// such as a literal longer than its `char(n)` / `varchar(n)`
    /// column. The planner reads no rows for it. Though, if this is part of a
    /// NOT Pred, then it would always be true.
    never,

    /// `col = val`, `val` already coerced to `col.ty` (for the egid
    /// pseudo-column: a positive uint8, or NULL).
    pub const Cmp = struct { col: *const _table.Column, val: _value.Value };
    /// `col IS NULL` (negate = false) / `col IS NOT NULL`.
    pub const NullTest = struct { col: *const _table.Column, negate: bool };

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

    /// SQL three-valued logic: null = unknown. `eq` is unknown when
    /// either side is NULL; IS [NOT] NULL is never unknown; AND / OR /
    /// NOT follow Kleene. Reads only the cells the tree names
    /// (Table.read — a null-bit test plus one O(1) read each; no
    /// full-row decode).
    pub fn eval(
        self: *const Pred,
        tbl: *const _table.Table,
        item: zdb.kv.Item,
    ) ?bool {
        switch (self.*) {
            .eq => |c| {
                if (c.val == .null_value) return null;
                const cell = tbl.parseCol(item, c.col);
                if (cell == .null_value) return null;
                return _value.valuesEqual(cell, c.val);
            },
            .is_null => |n| {
                const is_null = tbl.parseCol(item, n.col) == .null_value;
                return is_null != n.negate;
            },
            .and_ => |kids| {
                var unknown = false;
                // TODO: We should have a mechanism to surface false evaluations
                // quickly instead of going serially through every kid.eval.
                for (kids) |*kid| {
                    if (kid.eval(tbl, item)) |b| {
                        if (!b) return false;
                    } else unknown = true;
                }
                return if (unknown) null else true;
            },
            .or_ => |kids| {
                var unknown = false;
                for (kids) |*kid| {
                    if (kid.eval(tbl, item)) |b| {
                        if (b) return true;
                    } else unknown = true;
                }
                return if (unknown) null else false;
            },
            .not => |child| {
                const b = child.eval(tbl, item) orelse return null;
                return !b;
            },
            .never => return false,
        }
    }

    /// A row matches only when the predicate is TRUE — unknown rejects,
    /// so `WHERE col = NULL` matches nothing.
    pub fn matches(
        self: *const Pred,
        tbl: *const _table.Table,
        item: zdb.kv.Item,
    ) bool {
        return self.eval(tbl, item) orelse false;
    }
};

pub const BindError = error{
    UndefinedColumn,
    InvalidValue,
    /// A binary-format `$n` of a declared type with no binary decoder.
    UnsupportedValue,
    /// A binary-format `$n` of a type the column cannot take.
    TypeMismatch,
    EgidZero,
    OutOfMemory,
};

/// Bind one WHERE node — and, recursively, its children — to `tbl`:
/// names → columns, literals and `$n` values → typed cells
/// (parse.litToValue). Pure; `bind` adds the client-facing error
/// mapping. `arloc` (the statement arena) backs the tree.
pub fn bindNode(
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    where: _parse.Where,
    params: _parse.Params,
) BindError!Pred {
    switch (where) {
        .eq => |f| {
            const col = tbl.column(f.column) orelse return error.UndefinedColumn;
            const val = _parse.litToValue(arloc, f.literal, col, //
                params) catch |err| switch (err) {
                error.OutOfMemory => return error.OutOfMemory,
                // A literal longer than a `char(n)` / `varchar(n)` column
                // is not an error in Postgres, it just never matches:
                // every stored cell is at most n codepoints.
                error.StringTooLong => return .never,
                error.UnsupportedType => return error.UnsupportedValue,
                error.TypeMismatch => return error.TypeMismatch,
                else => return error.InvalidValue,
            };
            // The egid IS the row key. Its column is uint8, so a
            // negative literal already failed to coerce; zero (never a
            // row) is rejected here, and `egid = NULL` is left to be
            // unknown like any other comparison.
            if (col.isEgid() and val == .uint8 and val.uint8 == 0)
                return error.EgidZero;
            return .{ .eq = .{ .col = col, .val = val } };
        },
        .is_null => |nt| {
            const col = tbl.column(nt.column) orelse return error.UndefinedColumn;
            return .{ .is_null = .{ .col = col, .negate = nt.negate } };
        },
        .and_, .or_ => |kids| {
            const out = try arloc.alloc(Pred, kids.len);
            for (kids, out) |kid, *slot|
                slot.* = try bindNode(arloc, tbl, kid, params);
            return if (where == .and_) .{ .and_ = out } else .{ .or_ = out };
        },
        .not => |child| {
            const out = try arloc.create(Pred);
            out.* = try bindNode(arloc, tbl, child.*, params);
            return .{ .not = out };
        },
    }
}

/// Bind a statement's WHERE to its table: bindNode plus the
/// client-facing error mapping. null = the SQL error was already sent
/// and the statement is over; only OOM propagates.
pub fn bind(
    state: *pgwire.ConnState,
    arloc: std.mem.Allocator,
    tbl: *const _table.Table,
    where: _parse.Where,
    params: _parse.Params,
) !?*const Pred {
    const bad_value = pgwire.sqlstate.invalid_text_representation;
    const p = try arloc.create(Pred);
    p.* = bindNode(arloc, tbl, where, params) catch |err| {
        switch (err) {
            error.OutOfMemory => return err,
            error.UndefinedColumn => try replyErrorCode(
                state,
                pgwire.sqlstate.undefined_column,
                "column does not exist",
            ),
            error.InvalidValue => try replyErrorCode(state, bad_value, "invalid value in WHERE"),
            error.UnsupportedValue => try replyErrorCode(
                state,
                pgwire.sqlstate.feature_not_supported,
                "unsupported value or column type",
            ),
            error.TypeMismatch => try replyErrorCode(
                state,
                pgwire.sqlstate.datatype_mismatch,
                "parameter type does not match column type",
            ),
            error.EgidZero => try replyErrorCode(
                state,
                bad_value,
                "egid must be a positive integer",
            ),
        }
        return null;
    };
    return p;
}
