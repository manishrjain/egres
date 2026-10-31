// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The functions an expression may call, one table entry each: the
//! name the parser resolves, the arity it checks, how bind types the
//! call and how eval computes it. A function is added here and
//! nowhere else: parse, bind and eval read the entry.

const std = @import("std");

const _expr = @import("expr.zig");
const _value = @import("value.zig");
const String = _value.String;
const Type = _value.Type;
const Scalar = _expr.Scalar;
const EvalError = _expr.EvalError;

pub const Func = struct {
    name: String,
    /// How many arguments a call takes; `max_args` null is any number
    /// from `min_args` up.
    min_args: u8,
    max_args: ?u8,
    args: Args,
    result: Result,
    eval: Eval,

    /// What a call's arguments may be. Every function takes what a
    /// Scalar holds, strings and numbers; a NULL-only argument fits
    /// any function.
    pub const Args = enum {
        /// Strings and numbers mixed freely.
        scalar,
        /// All strings or all numbers.
        one_kind,
    };

    pub const Result = union(enum) {
        fixed: Type,
        /// The widest type of the arguments' kind, text, float8 or
        /// int8, as an operator yields; `one_kind` arguments only.
        widest,
    };

    /// Postgres's strictness: a `strict` function is NULL on any NULL
    /// argument and never sees one; a `lax` one is handed the NULLs.
    pub const Eval = union(enum) {
        strict: *const fn (
            arloc: std.mem.Allocator,
            args: []const Scalar,
        ) EvalError!?Scalar,
        lax: *const fn (
            arloc: std.mem.Allocator,
            args: []const ?Scalar,
        ) EvalError!?Scalar,
    };

    /// The type a call yields, or NoSuchOperator for arguments the
    /// function has no form for.
    pub fn resultType(
        self: *const Func,
        args: []const ?Type,
    ) error{NoSuchOperator}!?Type {
        var ty: ?Type = null;
        for (args) |arg| {
            const t = arg orelse continue;
            if (!t.isString() and !t.isNumeric()) return error.NoSuchOperator;
            const prev = ty orelse {
                ty = if (t.isString()) .text else if (t.isFloat())
                    .float8
                else
                    .int8;
                continue;
            };
            if (prev.isString() != t.isString()) {
                if (self.args == .one_kind) return error.NoSuchOperator;
            } else if (t.isFloat()) {
                ty = .float8;
            }
        }
        return switch (self.result) {
            .fixed => |t| t,
            .widest => ty,
        };
    }

    /// Whether a call with `n` arguments has a form.
    pub fn takes(self: *const Func, n: usize) bool {
        const max = self.max_args orelse return n >= self.min_args;
        return n >= self.min_args and n <= max;
    }
};

/// CONCAT skips NULL arguments and renders the rest as text; CONCAT
/// of nothing but NULLs is the empty string.
pub const concat: Func = .{
    .name = "concat",
    .min_args = 1,
    .max_args = null,
    .args = .scalar,
    .result = .{ .fixed = .text },
    .eval = .{ .lax = evalConcat },
};

/// COALESCE is the first non-NULL argument. The grammar hands it over
/// as its own node, not a FuncCall; the parser maps it here.
pub const coalesce: Func = .{
    .name = "coalesce",
    .min_args = 1,
    .max_args = null,
    .args = .one_kind,
    .result = .widest,
    .eval = .{ .lax = evalCoalesce },
};

const table = [_]*const Func{ &concat, &coalesce };

const by_name = std.StaticStringMap(*const Func).initComptime(blk: {
    var kvs: [table.len]struct { String, *const Func } = undefined;
    for (table, &kvs) |func, *kv| kv.* = .{ func.name, func };
    break :blk kvs;
});

/// The function called `name`, lowercased as the grammar leaves an
/// unquoted identifier.
pub fn byName(name: String) ?*const Func {
    return by_name.get(name);
}

fn evalConcat(
    arloc: std.mem.Allocator,
    args: []const ?Scalar,
) EvalError!?Scalar {
    var out: std.ArrayList(u8) = .empty;
    for (args) |arg| {
        const s = arg orelse continue;
        try out.appendSlice(arloc, try scalarText(arloc, s));
    }
    return .{ .text = out.items };
}

fn evalCoalesce(
    _: std.mem.Allocator,
    args: []const ?Scalar,
) EvalError!?Scalar {
    for (args) |arg| {
        if (arg) |s| return s;
    }
    return null;
}

/// A scalar as text: an integer in decimal, a float as a float8 cell
/// prints, text as it is.
pub fn scalarText(arloc: std.mem.Allocator, s: Scalar) EvalError!String {
    switch (s) {
        .text => |t| return t,
        .int => |n| return std.fmt.allocPrint(arloc, "{d}", .{n}),
        .float => |f| {
            var aw: std.Io.Writer.Allocating = .init(arloc);
            _value.valueToText(&aw.writer, null, .{ .float8 = f }) catch
                return error.OutOfMemory;
            return aw.written();
        },
    }
}
