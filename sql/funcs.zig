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

pub const StrictEval = *const fn (
    arloc: std.mem.Allocator,
    args: []const Scalar,
) EvalError!?Scalar;

pub const Func = struct {
    name: String,
    /// How many arguments a call takes; `max_args` null is any number
    /// from `min_args` up.
    min_args: u8,
    max_args: ?u8,
    args: Args,
    result: Result,
    eval: Eval,

    pub const Kind = enum { string, number };

    /// What a call's arguments may be. Every function takes what a
    /// Scalar holds, strings and numbers; a NULL-only argument fits
    /// any function.
    pub const Args = union(enum) {
        /// Strings and numbers mixed freely.
        scalar,
        /// All strings or all numbers.
        one_kind,
        /// A kind per position; past the last, it repeats.
        each: []const Kind,
    };

    pub const Result = union(enum) {
        fixed: Type,
        /// The widest type of the arguments' kind, text, float8 or
        /// int8, as an operator yields.
        widest,
    };

    /// Postgres's strictness: a `strict` function is NULL on any NULL
    /// argument and never sees one; a `lax` one is handed the NULLs.
    pub const Eval = union(enum) {
        strict: StrictEval,
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
        for (args, 0..) |arg, pos| {
            const t = arg orelse continue;
            if (!t.isString() and !t.isNumeric()) return error.NoSuchOperator;
            switch (self.args) {
                .scalar => {},
                .one_kind => if (ty) |prev| {
                    if (prev.isString() != t.isString()) {
                        return error.NoSuchOperator;
                    }
                },
                .each => |kinds| {
                    const want = kinds[@min(pos, kinds.len - 1)];
                    if ((want == .string) != t.isString()) {
                        return error.NoSuchOperator;
                    }
                },
            }
            const wide: Type = if (t.isString()) .text else if (t.isFloat())
                .float8
            else
                .int8;
            const prev = ty orelse {
                ty = wide;
                continue;
            };
            if (prev.isNumeric() and wide == .float8) ty = .float8;
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

const text_arg = &[_]Func.Kind{.string};
const text_args = &[_]Func.Kind{ .string, .string };
const text_count = &[_]Func.Kind{ .string, .number };
const num_arg = &[_]Func.Kind{.number};
const num_args = &[_]Func.Kind{ .number, .number };

/// `name` for the same function: Postgres's second spelling.
fn alias(name: String, of: Func) Func {
    var func = of;
    func.name = name;
    return func;
}

// ---- NULL handling --------------------------------------------------

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

fn evalCoalesce(
    _: std.mem.Allocator,
    args: []const ?Scalar,
) EvalError!?Scalar {
    for (args) |arg| {
        if (arg) |s| return s;
    }
    return null;
}

/// NULLIF(a, b) is NULL when the two are equal, else a. Its own
/// grammar node as well.
pub const nullif: Func = .{
    .name = "nullif",
    .min_args = 2,
    .max_args = 2,
    .args = .one_kind,
    .result = .widest,
    .eval = .{ .lax = evalNullif },
};

fn evalNullif(
    _: std.mem.Allocator,
    args: []const ?Scalar,
) EvalError!?Scalar {
    const a = args[0] orelse return null;
    const b = args[1] orelse return a;
    return if (order(a, b) == .eq) null else a;
}

/// GREATEST and LEAST skip NULL arguments and are NULL only when every
/// argument is. Their own grammar node as well.
pub const greatest: Func = .{
    .name = "greatest",
    .min_args = 1,
    .max_args = null,
    .args = .one_kind,
    .result = .widest,
    .eval = .{ .lax = evalGreatest },
};
pub const least: Func = .{
    .name = "least",
    .min_args = 1,
    .max_args = null,
    .args = .one_kind,
    .result = .widest,
    .eval = .{ .lax = evalLeast },
};

fn evalGreatest(
    _: std.mem.Allocator,
    args: []const ?Scalar,
) EvalError!?Scalar {
    return extreme(args, .gt);
}

fn evalLeast(
    _: std.mem.Allocator,
    args: []const ?Scalar,
) EvalError!?Scalar {
    return extreme(args, .lt);
}

/// The argument that orders `want` against every other, NULLs skipped.
fn extreme(args: []const ?Scalar, want: std.math.Order) ?Scalar {
    var best: ?Scalar = null;
    for (args) |arg| {
        const s = arg orelse continue;
        const cur = best orelse {
            best = s;
            continue;
        };
        if (order(s, cur) == want) best = s;
    }
    return best;
}

/// Two scalars of one kind (bind's `one_kind`): text bytewise, numbers
/// as floats unless both are integers.
fn order(x: Scalar, y: Scalar) std.math.Order {
    if (x == .text) return std.mem.order(u8, x.text, y.text);
    if (x == .int and y == .int) return std.math.order(x.int, y.int);
    return std.math.order(toFloat(x), toFloat(y));
}

// ---- strings --------------------------------------------------------

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

/// CONCAT_WS(', ', 'a', NULL, 'b') is 'a, b': the non-NULL arguments
/// joined by the separator. A NULL separator gives NULL.
pub const concat_ws: Func = .{
    .name = "concat_ws",
    .min_args = 1,
    .max_args = null,
    .args = .scalar,
    .result = .{ .fixed = .text },
    .eval = .{ .lax = evalConcatWs },
};

fn evalConcatWs(
    arloc: std.mem.Allocator,
    args: []const ?Scalar,
) EvalError!?Scalar {
    const sep = try scalarText(arloc, args[0] orelse return null);
    var out: std.ArrayList(u8) = .empty;
    var first = true;
    for (args[1..]) |arg| {
        const s = arg orelse continue;
        if (!first) try out.appendSlice(arloc, sep);
        first = false;
        try out.appendSlice(arloc, try scalarText(arloc, s));
    }
    return .{ .text = out.items };
}

/// LOWER and UPPER fold ASCII letters only; Postgres folds by the
/// collation (docs/postgres-compatibility.md).
pub const lower: Func = .{
    .name = "lower",
    .min_args = 1,
    .max_args = 1,
    .args = .{ .each = text_arg },
    .result = .{ .fixed = .text },
    .eval = .{ .strict = evalLower },
};
pub const upper: Func = .{
    .name = "upper",
    .min_args = 1,
    .max_args = 1,
    .args = .{ .each = text_arg },
    .result = .{ .fixed = .text },
    .eval = .{ .strict = evalUpper },
};

fn evalLower(
    arloc: std.mem.Allocator,
    args: []const Scalar,
) EvalError!?Scalar {
    const buf = try arloc.alloc(u8, args[0].text.len);
    return .{ .text = std.ascii.lowerString(buf, args[0].text) };
}

fn evalUpper(
    arloc: std.mem.Allocator,
    args: []const Scalar,
) EvalError!?Scalar {
    const buf = try arloc.alloc(u8, args[0].text.len);
    return .{ .text = std.ascii.upperString(buf, args[0].text) };
}

/// LENGTH counts characters, not bytes.
pub const length: Func = .{
    .name = "length",
    .min_args = 1,
    .max_args = 1,
    .args = .{ .each = text_arg },
    .result = .{ .fixed = .int8 },
    .eval = .{ .strict = evalLength },
};
pub const char_length = alias("char_length", length);
pub const character_length = alias("character_length", length);

fn evalLength(_: std.mem.Allocator, args: []const Scalar) EvalError!?Scalar {
    return .{ .int = @intCast(try charCount(args[0].text)) };
}

/// SUBSTRING(s, start [, count]): characters from the 1-based start,
/// count of them or to the end. A start before 1 shortens the count,
/// as in Postgres; a negative count is an error.
pub const substring: Func = .{
    .name = "substring",
    .min_args = 2,
    .max_args = 3,
    .args = .{ .each = &[_]Func.Kind{ .string, .number, .number } },
    .result = .{ .fixed = .text },
    .eval = .{ .strict = evalSubstring },
};
pub const substr = alias("substr", substring);

fn evalSubstring(
    _: std.mem.Allocator,
    args: []const Scalar,
) EvalError!?Scalar {
    const text = args[0].text;
    const len: i128 = @intCast(try charCount(text));
    // 0-based, end exclusive; a start before 1 eats into the count.
    var lo = try intArg(args[1]) - 1;
    var hi = len;
    if (args.len == 3) {
        const count = try intArg(args[2]);
        if (count < 0) return error.SubstringLength;
        hi = @min(hi, lo + count);
    }
    lo = @max(lo, 0);
    if (hi <= lo) return .{ .text = "" };
    return .{ .text = try charSlice(text, @intCast(lo), @intCast(hi)) };
}

/// REPLACE(s, from, to), every occurrence; an empty `from` changes
/// nothing.
pub const replace: Func = .{
    .name = "replace",
    .min_args = 3,
    .max_args = 3,
    .args = .{ .each = text_arg },
    .result = .{ .fixed = .text },
    .eval = .{ .strict = evalReplace },
};

fn evalReplace(
    arloc: std.mem.Allocator,
    args: []const Scalar,
) EvalError!?Scalar {
    const text = args[0].text;
    const from = args[1].text;
    if (from.len == 0) return .{ .text = text };
    const out = std.mem.replaceOwned(u8, arloc, text, from, args[2].text) catch
        return error.OutOfMemory;
    return .{ .text = out };
}

/// TRIM's three forms: BTRIM, LTRIM and RTRIM(s [, chars]) strip the
/// characters of `chars` (a space without it) from both ends, the
/// start or the end. The grammar turns `TRIM(BOTH 'x' FROM s)` into
/// btrim(s, 'x').
pub const btrim: Func = .{
    .name = "btrim",
    .min_args = 1,
    .max_args = 2,
    .args = .{ .each = text_args },
    .result = .{ .fixed = .text },
    .eval = .{ .strict = trimmer(true, true) },
};
pub const ltrim: Func = .{
    .name = "ltrim",
    .min_args = 1,
    .max_args = 2,
    .args = .{ .each = text_args },
    .result = .{ .fixed = .text },
    .eval = .{ .strict = trimmer(true, false) },
};
pub const rtrim: Func = .{
    .name = "rtrim",
    .min_args = 1,
    .max_args = 2,
    .args = .{ .each = text_args },
    .result = .{ .fixed = .text },
    .eval = .{ .strict = trimmer(false, true) },
};

/// The strict eval that trims the given ends: BTRIM, LTRIM and RTRIM
/// differ only in them.
fn trimmer(comptime front: bool, comptime back: bool) StrictEval {
    return struct {
        fn eval(_: std.mem.Allocator, args: []const Scalar) EvalError!?Scalar {
            return try trim(args, front, back);
        }
    }.eval;
}

/// `args[0]` without the leading and trailing characters found in
/// `args[1]` (a space without it). Whole characters: a character's
/// bytes match in `chars` only at one of its own characters, since
/// no continuation byte can start one.
fn trim(args: []const Scalar, front: bool, back: bool) EvalError!Scalar {
    const chars = if (args.len > 1) args[1].text else " ";
    var text = args[0].text;
    if (front) while (text.len > 0) {
        const n = std.unicode.utf8ByteSequenceLength(text[0]) catch
            return error.InvalidUtf8;
        if (n > text.len) return error.InvalidUtf8;
        if (std.mem.indexOf(u8, chars, text[0..n]) == null) break;
        text = text[n..];
    };
    if (back) while (text.len > 0) {
        var start = text.len - 1;
        while (start > 0 and text[start] & 0xC0 == 0x80) start -= 1;
        if (std.mem.indexOf(u8, chars, text[start..]) == null) break;
        text = text[0..start];
    };
    return .{ .text = text };
}

/// LEFT(s, n) and RIGHT(s, n): the first or last n characters; a
/// negative n drops that many from the other end instead.
pub const left: Func = .{
    .name = "left",
    .min_args = 2,
    .max_args = 2,
    .args = .{ .each = text_count },
    .result = .{ .fixed = .text },
    .eval = .{ .strict = cutter(.left) },
};
pub const right: Func = .{
    .name = "right",
    .min_args = 2,
    .max_args = 2,
    .args = .{ .each = text_count },
    .result = .{ .fixed = .text },
    .eval = .{ .strict = cutter(.right) },
};

const Side = enum { left, right };

/// The strict eval for LEFT or RIGHT: the kept characters taken from
/// that end.
fn cutter(comptime side: Side) StrictEval {
    return struct {
        fn eval(_: std.mem.Allocator, args: []const Scalar) EvalError!?Scalar {
            const text = args[0].text;
            const len = try charCount(text);
            const keep = keepCount(len, try intArg(args[1]));
            if (side == .left) return .{ .text = try charSlice(text, 0, keep) };
            return .{ .text = try charSlice(text, len - keep, len) };
        }
    }.eval;
}

/// How many of `len` characters LEFT or RIGHT keeps: n of them, or
/// all but -n when n is negative.
fn keepCount(len: usize, n: i128) usize {
    const l: i128 = @intCast(len);
    return @intCast(if (n >= 0) @min(n, l) else @max(l + n, 0));
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

fn charCount(text: String) EvalError!usize {
    return std.unicode.utf8CountCodepoints(text) catch error.InvalidUtf8;
}

/// The bytes of characters [lo, hi) of `text`: one walk, the end
/// found from the start rather than from the front again.
fn charSlice(text: String, lo: usize, hi: usize) EvalError!String {
    std.debug.assert(lo <= hi);
    const start = try charOffset(text, lo);
    const end = start + try charOffset(text[start..], hi - lo);
    return text[start..end];
}

/// The byte offset where character `n` starts; the end past the last.
fn charOffset(text: String, n: usize) EvalError!usize {
    var off: usize = 0;
    var i: usize = 0;
    while (i < n and off < text.len) : (i += 1) {
        off += std.unicode.utf8ByteSequenceLength(text[off]) catch
            return error.InvalidUtf8;
    }
    return @min(off, text.len);
}

// ---- numbers --------------------------------------------------------

/// ABS and SIGN keep their argument's kind: an integer stays an
/// integer.
pub const abs: Func = .{
    .name = "abs",
    .min_args = 1,
    .max_args = 1,
    .args = .{ .each = num_arg },
    .result = .widest,
    .eval = .{ .strict = evalAbs },
};

fn evalAbs(_: std.mem.Allocator, args: []const Scalar) EvalError!?Scalar {
    return switch (args[0]) {
        .int => |n| .{ .int = try negated(n, n < 0) },
        .float => |f| .{ .float = @abs(f) },
        .text => unreachable,
    };
}

pub const sign: Func = .{
    .name = "sign",
    .min_args = 1,
    .max_args = 1,
    .args = .{ .each = num_arg },
    .result = .widest,
    .eval = .{ .strict = evalSign },
};

fn evalSign(_: std.mem.Allocator, args: []const Scalar) EvalError!?Scalar {
    return switch (args[0]) {
        .int => |n| .{ .int = std.math.sign(n) },
        .float => |f| .{ .float = std.math.sign(f) },
        .text => unreachable,
    };
}

/// FLOOR, CEIL, ROUND(x [, places]) and TRUNC(x [, places]) keep
/// their argument's kind. ROUND rounds half away from zero, TRUNC
/// toward zero; negative places round an integer to tens, hundreds.
pub const floor: Func = .{
    .name = "floor",
    .min_args = 1,
    .max_args = 1,
    .args = .{ .each = num_arg },
    .result = .widest,
    .eval = .{ .strict = rounder(.floor) },
};
pub const ceil: Func = .{
    .name = "ceil",
    .min_args = 1,
    .max_args = 1,
    .args = .{ .each = num_arg },
    .result = .widest,
    .eval = .{ .strict = rounder(.ceil) },
};
pub const ceiling = alias("ceiling", ceil);
pub const round: Func = .{
    .name = "round",
    .min_args = 1,
    .max_args = 2,
    .args = .{ .each = num_args },
    .result = .widest,
    .eval = .{ .strict = rounder(.round) },
};
pub const trunc: Func = .{
    .name = "trunc",
    .min_args = 1,
    .max_args = 2,
    .args = .{ .each = num_args },
    .result = .widest,
    .eval = .{ .strict = rounder(.trunc) },
};

const Rounding = enum { round, trunc, floor, ceil };

/// The strict eval that rounds in `mode`: FLOOR, CEIL, ROUND and
/// TRUNC differ only in it.
fn rounder(comptime mode: Rounding) StrictEval {
    return struct {
        fn eval(_: std.mem.Allocator, args: []const Scalar) EvalError!?Scalar {
            return try rounded(args, mode);
        }
    }.eval;
}

/// `args[0]` rounded to `args[1]` decimal places (0 without it): a
/// float through its scaled value, an integer only for negative
/// places, to a power of ten.
fn rounded(args: []const Scalar, mode: Rounding) EvalError!Scalar {
    const places = if (args.len > 1) try intArg(args[1]) else 0;
    switch (args[0]) {
        .float => |f| {
            if (places == 0) return .{ .float = roundFloat(f, mode) };
            const scale = std.math.pow(f64, 10, @floatFromInt(places));
            return .{ .float = roundFloat(f * scale, mode) / scale };
        },
        .int => |n| {
            if (places >= 0) return args[0];
            const scale = std.math.powi(i128, 10, @intCast(-places)) catch
                return error.IntOutOfRange;
            const quot = @divTrunc(n, scale);
            const rem = @rem(n, scale);
            const adjust: i128 = switch (mode) {
                .trunc => 0,
                .round => if (2 * @abs(rem) >= @as(u128, @intCast(scale)))
                    std.math.sign(rem)
                else
                    0,
                .floor => if (rem < 0) -1 else 0,
                .ceil => if (rem > 0) 1 else 0,
            };
            const r = @mulWithOverflow(quot + adjust, scale);
            if (r[1] != 0) return error.IntOutOfRange;
            return .{ .int = r[0] };
        },
        .text => unreachable,
    }
}

fn roundFloat(f: f64, mode: Rounding) f64 {
    return switch (mode) {
        .round => @round(f),
        .trunc => @trunc(f),
        .floor => @floor(f),
        .ceil => @ceil(f),
    };
}

/// MOD(x, y) takes the dividend's sign, as `%` does.
pub const mod: Func = .{
    .name = "mod",
    .min_args = 2,
    .max_args = 2,
    .args = .{ .each = num_args },
    .result = .widest,
    .eval = .{ .strict = evalMod },
};

fn evalMod(_: std.mem.Allocator, args: []const Scalar) EvalError!?Scalar {
    if (args[0] == .int and args[1] == .int) {
        if (args[1].int == 0) return error.DivisionByZero;
        return .{ .int = @rem(args[0].int, args[1].int) };
    }
    const y = toFloat(args[1]);
    if (y == 0) return error.DivisionByZero;
    return .{ .float = @rem(toFloat(args[0]), y) };
}

/// POWER and SQRT compute in float8, as Postgres does for
/// double precision.
pub const power: Func = .{
    .name = "power",
    .min_args = 2,
    .max_args = 2,
    .args = .{ .each = num_args },
    .result = .{ .fixed = .float8 },
    .eval = .{ .strict = evalPower },
};
pub const pow = alias("pow", power);
pub const sqrt: Func = .{
    .name = "sqrt",
    .min_args = 1,
    .max_args = 1,
    .args = .{ .each = num_arg },
    .result = .{ .fixed = .float8 },
    .eval = .{ .strict = evalSqrt },
};

fn evalPower(_: std.mem.Allocator, args: []const Scalar) EvalError!?Scalar {
    const x = toFloat(args[0]);
    const y = toFloat(args[1]);
    // Undefined in the reals, as Postgres reports them.
    if (x == 0 and y < 0) return error.PowerArgument;
    if (x < 0 and @trunc(y) != y) return error.PowerArgument;
    const r = std.math.pow(f64, x, y);
    if (!std.math.isFinite(r)) return error.FloatOutOfRange;
    return .{ .float = r };
}

fn evalSqrt(_: std.mem.Allocator, args: []const Scalar) EvalError!?Scalar {
    const x = toFloat(args[0]);
    if (x < 0) return error.PowerArgument;
    return .{ .float = @sqrt(x) };
}

fn toFloat(s: Scalar) f64 {
    return switch (s) {
        .int => |n| @floatFromInt(n),
        .float => |f| f,
        .text => unreachable,
    };
}

/// A number argument used as a count or a position: an integer as it
/// is, a float truncated.
fn intArg(s: Scalar) EvalError!i128 {
    return switch (s) {
        .int => |n| n,
        .float => |f| if (std.math.isFinite(f) and @abs(f) < 1e38)
            @intFromFloat(@trunc(f))
        else
            error.IntOutOfRange,
        .text => unreachable,
    };
}

/// `-n` when `neg`, else `n`; the one i128 with no negation fails.
fn negated(n: i128, neg: bool) EvalError!i128 {
    if (!neg) return n;
    if (n == std.math.minInt(i128)) return error.IntOutOfRange;
    return -n;
}

// ---- lookup ---------------------------------------------------------

const table = [_]*const Func{
    &coalesce, &nullif,      &greatest,         &least,
    &concat,   &concat_ws,   &lower,            &upper,
    &length,   &char_length, &character_length, &substring,
    &substr,   &replace,     &btrim,            &ltrim,
    &rtrim,    &left,        &right,            &abs,
    &floor,    &ceil,        &ceiling,          &sign,
    &round,    &trunc,       &mod,              &power,
    &pow,      &sqrt,
};

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
