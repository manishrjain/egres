// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Exact decimals for `numeric`: a coefficient and a scale, so 12.50
//! is 1250 at scale 2 and 0.321 is 321 at scale 3. The coefficient is
//! an i128, 38 digits, which every ORM's decimal column fits in;
//! Postgres allows more, and a value past it is out of range here
//! (22003). Arithmetic computes wider and rounds fractional digits
//! away until the result fits, failing only when its integer digits
//! alone do not. NaN and Infinity are not values here.

const std = @import("std");
const utils = @import("utils");
const String = utils.String;

/// The most digits a coefficient holds, the limit of a column's
/// precision and of a cell's scale.
pub const max_digits: u8 = 38;

pub const Error = error{ InvalidNumeric, NumericOutOfRange, DivisionByZero };

/// How a value is brought to fewer fractional digits. `round` is
/// Postgres's: half away from zero.
pub const Rounding = enum { round, trunc, floor, ceil };

fn isDigitOrSeparator(ch: u8) bool {
    return std.ascii.isDigit(ch) or ch == '_';
}

/// The radix a `0x` / `0o` / `0b` prefix names, either case.
fn radixOf(text: String) ?u8 {
    if (text.len < 2 or text[0] != '0') return null;
    return switch (text[1]) {
        'x', 'X' => 16,
        'o', 'O' => 8,
        'b', 'B' => 2,
        else => null,
    };
}

pub const Numeric = struct {
    // value = coef × 10^-scale
    coef: i128,
    scale: u8,

    pub const zero: Numeric = .{ .coef = 0, .scale = 0 };

    pub fn fromInt(n: i128) Numeric {
        return .{ .coef = n, .scale = 0 };
    }

    /// Postgres's numeric input: an optional sign, then digits with
    /// an optional point and an optional exponent, or a whole number
    /// in another radix (`0x`, `0o`, `0b`). Underscores among the
    /// digits are separators and are skipped, more freely than
    /// Postgres allows. Leading zeros are not significant; the
    /// scale is the digits after the point, less the exponent, and a
    /// negative one is multiplied out so a cell's scale is never
    /// negative.
    pub fn parse(raw_text: String) Error!Numeric {
        var text = std.mem.trim(u8, raw_text, " \t\r\n");
        const neg = text.len > 0 and text[0] == '-';
        if (text.len > 0 and (text[0] == '-' or text[0] == '+'))
            text = text[1..];

        if (radixOf(text)) |base| {
            // One underscore may follow the prefix, `0x_FF`.
            const at: usize = if (text.len > 2 and text[2] == '_') 3 else 2;
            const mag = std.fmt.parseInt(i256, text[at..], base) catch |err|
                return switch (err) {
                    error.Overflow => error.NumericOutOfRange,
                    error.InvalidCharacter => error.InvalidNumeric,
                };
            return .{ .coef = try fit(if (neg) -mag else mag), .scale = 0 };
        }

        // `1.5e3`: the exponent only moves the scale, so take it off
        // first.
        var exp: i32 = 0;
        if (std.mem.indexOfAny(u8, text, "eE")) |at| {
            exp = std.fmt.parseInt(i32, text[at + 1 ..], 10) catch
                return error.InvalidNumeric;
            text = text[0..at];
        }

        // `12.50`: every digit into the coefficient, the point's place
        // remembered by the digits after it. A second point, or
        // anything else, is not a number.
        var coef: i256 = 0;
        var digits: usize = 0;
        var frac: ?i32 = null;
        for (text, 0..) |ch, i| switch (ch) {
            '0'...'9' => {
                digits += 1;
                if (frac) |*n| n.* += 1;
                coef = coef * 10 + (ch - '0');
                if (coef >= pow10(max_digits + 1))
                    return error.NumericOutOfRange;
            },
            '.' => if (frac == null) {
                frac = 0;
            } else return error.InvalidNumeric,
            // A separator sits among digits, as std reads one in an
            // integer: a run of them is fine, an edge is not.
            '_' => if (i == 0 or i + 1 == text.len or
                !isDigitOrSeparator(text[i - 1]) or
                !isDigitOrSeparator(text[i + 1]))
                return error.InvalidNumeric,
            else => return error.InvalidNumeric,
        };
        if (digits == 0) return error.InvalidNumeric;

        // The scale is the digits after the point less the exponent,
        // never negative: `15e-1` is 1.5 at scale 1, `1.5e3` is 1500.
        var scale = (frac orelse 0) - exp;
        if (scale < 0) {
            if (-scale > max_digits) return error.NumericOutOfRange;
            coef *= pow10(@intCast(-scale));
            scale = 0;
        }
        if (scale > max_digits) return error.NumericOutOfRange;
        return .{
            .coef = try fit(if (neg) -coef else coef),
            .scale = @intCast(scale),
        };
    }

    /// The text form: the coefficient with the point placed by the
    /// scale, every fractional digit kept (`12.50` at scale 2), as
    /// Postgres prints a numeric.
    pub fn format(self: Numeric, w: *std.Io.Writer) !void {
        var buf: [40]u8 = undefined;
        const mag: u128 = @abs(self.coef);
        const digits = std.fmt.bufPrint(&buf, "{d}", .{mag}) catch unreachable;

        if (self.coef < 0) try w.writeByte('-');
        if (self.scale == 0) return w.writeAll(digits);
        if (digits.len > self.scale) {
            try w.writeAll(digits[0 .. digits.len - self.scale]);
            try w.writeByte('.');
            try w.writeAll(digits[digits.len - self.scale ..]);
        } else {
            try w.writeAll("0.");
            try w.splatByteAll('0', self.scale - digits.len);
            try w.writeAll(digits);
        }
    }

    /// The nearest f64, for an operation with a float.
    pub fn toFloat(self: Numeric) f64 {
        const c: f64 = @floatFromInt(self.coef);
        return c / std.math.pow(f64, 10, @floatFromInt(self.scale));
    }

    /// A float as a decimal, from its shortest round-trip text: 1.5
    /// is 15 at scale 1, not the binary expansion.
    pub fn fromFloat(f: f64) Error!Numeric {
        if (!std.math.isFinite(f)) return error.InvalidNumeric;
        var buf: [64]u8 = undefined;
        const text = std.fmt.bufPrint(&buf, "{d}", .{f}) catch
            return error.NumericOutOfRange;
        return parse(text);
    }

    /// The same value at `scale`, more fractional digits as zeros,
    /// fewer by `mode`.
    pub fn rescale(self: Numeric, scale: u8, mode: Rounding) Error!Numeric {
        if (scale == self.scale) return self;
        if (scale > self.scale) {
            const wide = @as(i256, self.coef) * pow10(scale - self.scale);
            return .{ .coef = try fit(wide), .scale = scale };
        }
        const wide: i256 = self.coef;
        const q = roundDiv(wide, pow10(self.scale - scale), mode);
        return .{ .coef = try fit(q), .scale = scale };
    }

    /// ROUND(x, places) and its kin: to `places` fractional digits, or
    /// for a negative count to tens, hundreds, as a whole number.
    pub fn roundTo(self: Numeric, places: i32, mode: Rounding) Error!Numeric {
        if (places >= 0) {
            const scale: u8 = @intCast(@min(places, max_digits));
            return self.rescale(scale, mode);
        }
        const k: u32 = @intCast(-places);
        if (k + self.scale > 2 * max_digits) return .{ .coef = 0, .scale = 0 };
        const wide: i256 = self.coef;
        const q = roundDiv(wide, pow10(@intCast(k + self.scale)), mode);
        return .{ .coef = try fit(q * pow10(@intCast(k))), .scale = 0 };
    }

    /// Whether the value fits a column of `precision` digits at this
    /// scale: Postgres's "numeric field overflow", an absolute value
    /// at or past 10^(precision - scale).
    pub fn fitsPrecision(self: Numeric, precision: u8) bool {
        return @as(i256, @abs(self.coef)) < pow10(precision);
    }

    /// The value with no trailing fractional zeros: 1.50 as 1.5, the
    /// form equality and the index key read.
    pub fn normalized(self: Numeric) Numeric {
        var d = self;
        while (d.scale > 0 and @rem(d.coef, 10) == 0) {
            d.coef = @divExact(d.coef, 10);
            d.scale -= 1;
        }
        return d;
    }

    pub fn order(a: Numeric, b: Numeric) std.math.Order {
        const s = @max(a.scale, b.scale);
        const x = @as(i256, a.coef) * pow10(s - a.scale);
        const y = @as(i256, b.coef) * pow10(s - b.scale);
        return std.math.order(x, y);
    }

    pub fn eql(a: Numeric, b: Numeric) bool {
        return order(a, b) == .eq;
    }

    /// Sum and difference at the wider scale, product at the summed
    /// scale, each brought back to `max_digits` (fitRounded).
    pub fn add(a: Numeric, b: Numeric) Error!Numeric {
        const s = @max(a.scale, b.scale);
        const x = @as(i256, a.coef) * pow10(s - a.scale);
        const y = @as(i256, b.coef) * pow10(s - b.scale);
        return fitRounded(x + y, s);
    }

    pub fn sub(a: Numeric, b: Numeric) Error!Numeric {
        return add(a, .{ .coef = -b.coef, .scale = b.scale });
    }

    pub fn mul(a: Numeric, b: Numeric) Error!Numeric {
        const wide = @as(i256, a.coef) * @as(i256, b.coef);
        return fitRounded(wide, @as(u16, a.scale) + b.scale);
    }

    /// Quotient at the dividend's scale or 16 fractional digits,
    /// whichever is more, rounded half away from zero: 1 / 3 is
    /// 0.3333333333333333. Postgres keeps a few digits more for a
    /// large quotient; the common cases agree.
    pub fn div(a: Numeric, b: Numeric) Error!Numeric {
        if (b.coef == 0) return error.DivisionByZero;
        const rs: u8 = @max(a.scale, 16);
        // q at scale rs: a.coef × 10^(rs - a.scale + b.scale) / b.coef,
        // the divisor made positive so roundDiv rounds by the sign of
        // the quotient.
        const sign: i512 = std.math.sign(b.coef);
        const e = rs - a.scale + b.scale;
        const num = sign * @as(i512, a.coef) * pow10wide(e);
        var den = sign * @as(i512, b.coef);
        // 10^30 / 3 is past max_digits at scale 16: the divisor grows
        // by the excess, so the quotient is rounded once, at scale 8.
        const excess = digitsOf(@divTrunc(num, den)) -| max_digits;
        if (excess > rs) return error.NumericOutOfRange;
        den *= pow10wide(@intCast(excess));
        return .{
            .coef = try fit(roundDiv(num, den, .round)),
            .scale = @intCast(rs - excess),
        };
    }

    /// Remainder with the dividend's sign, at the wider scale, as
    /// Postgres's numeric `%`.
    pub fn mod(a: Numeric, b: Numeric) Error!Numeric {
        if (b.coef == 0) return error.DivisionByZero;
        const s = @max(a.scale, b.scale);
        const x = @as(i256, a.coef) * pow10(s - a.scale);
        const y = @as(i256, b.coef) * pow10(s - b.scale);
        return .{ .coef = try fit(@rem(x, y)), .scale = s };
    }

    pub fn negated(self: Numeric) Error!Numeric {
        if (self.coef == std.math.minInt(i128)) return error.NumericOutOfRange;
        return .{ .coef = -self.coef, .scale = self.scale };
    }

    /// The memcomparable form, appended to `out`: every negative sorts
    /// before zero before every positive; within a sign, the adjusted
    /// exponent (the count of integer digits, negative for a fraction
    /// with leading zeros) then the significant digits, a negative
    /// number's bytes complemented so its order reverses. Trailing
    /// zeros are dropped first, so 1.5 and 1.50 share a key.
    pub fn encodeKey(
        self: Numeric,
        a: std.mem.Allocator,
        out: *std.ArrayList(u8),
    ) !void {
        const n = self.normalized();
        if (n.coef == 0) return out.append(a, 1);

        const neg = n.coef < 0;
        try out.append(a, if (neg) 0 else 2);

        var buf: [40]u8 = undefined;
        const mag: u128 = @abs(n.coef);
        const digits = std.fmt.bufPrint(&buf, "{d}", .{mag}) catch unreachable;
        const exp: i32 = @as(i32, @intCast(digits.len)) - n.scale;
        var eb: [4]u8 = undefined;
        std.mem.writeInt(u32, &eb, @as(u32, @bitCast(exp)) ^ 0x8000_0000, .big);
        if (!neg) {
            try out.appendSlice(a, &eb);
            try out.appendSlice(a, digits);
            try out.append(a, 0);
        } else {
            for (eb) |b| try out.append(a, ~b);
            for (digits) |d| try out.append(a, '9' - (d - '0'));
            try out.append(a, 0xFF);
        }
    }

    /// Postgres's binary numeric (pgwire.binary.encodeNumeric is the
    /// other direction): base-10000 digits at a weight, a sign, the
    /// display scale. NaN and the infinities are refused.
    pub fn decodeBinary(bytes: String) Error!Numeric {
        if (bytes.len < 8) return error.InvalidNumeric;
        const ndigits = std.mem.readInt(u16, bytes[0..2], .big);
        const weight = std.mem.readInt(i16, bytes[2..4], .big);
        const sign = std.mem.readInt(u16, bytes[4..6], .big);
        const dscale = std.mem.readInt(u16, bytes[6..8], .big);
        if (bytes.len != 8 + 2 * @as(usize, ndigits))
            return error.InvalidNumeric;
        if (sign != 0 and sign != 0x4000) return error.InvalidNumeric;
        if (dscale > max_digits) return error.NumericOutOfRange;
        var acc: i512 = 0;
        for (0..ndigits) |i| {
            const d = std.mem.readInt(u16, bytes[8 + 2 * i ..][0..2], .big);
            if (d > 9999) return error.InvalidNumeric;
            if (d == 0) continue;
            // This digit's place at the display scale: 10^e for the
            // group at 10000^(weight - i).
            const e: i32 = 4 * (@as(i32, weight) - @as(i32, @intCast(i))) +
                @as(i32, dscale);
            if (e > 2 * max_digits) return error.NumericOutOfRange;
            if (e >= 0) {
                acc += @as(i512, d) * pow10wide(@intCast(e));
            } else {
                // Digits past the display scale are zero in a value
                // Postgres rounded to it; a stray one is dropped.
                acc += @divTrunc(@as(i512, d), pow10wide(@intCast(-e)));
            }
        }
        if (sign == 0x4000) acc = -acc;
        return .{ .coef = try fit(acc), .scale = @intCast(dscale) };
    }
};

/// `x / div` brought to a whole number by `mode`; `div` is positive.
/// `x` is an i256 or an i512, the widths the arithmetic computes in.
fn roundDiv(x: anytype, div: @TypeOf(x), mode: Rounding) @TypeOf(x) {
    const quot = @divTrunc(x, div);
    const rem = @rem(x, div);
    const adjust: @TypeOf(x) = switch (mode) {
        .trunc => 0,
        .round => if (2 * @abs(rem) >= @abs(div)) std.math.sign(rem) else 0,
        .floor => if (rem < 0) -1 else 0,
        .ceil => if (rem > 0) 1 else 0,
    };
    return quot + adjust;
}

/// The decimal digits of `x`'s magnitude; zero has none.
fn digitsOf(x: i512) u16 {
    var n: u16 = 0;
    var m: u512 = @abs(x);
    while (m != 0) : (m /= 10) n += 1;
    return n;
}

/// A result as the type holds it: `x` at `scale`, its fractional
/// digits rounded away (half away from zero) until at most
/// `max_digits` remain and the scale is within them. The square of
/// 0.10000000000000000000 is
/// 0.0100000000000000000000000000000000000000, scale 40, two digits past
/// `max_digits`; both dropped digits are zeros, so the result
/// 0.01000000000000000000000000000000000000 at scale 38 is exact. The
/// 39-digit 1234567890123456789.01234567890123456_789 at scale 20 has
/// one digit too many: dropping its last 9 rounds the one before it
/// up, giving 1234567890123456789.01234567890123456_79 at scale 19.
/// For 10^37 + 10^-38, 76 digits at scale 38, the excess is 38 and
/// the result is 10^37 at scale 0. Out of range only when the integer
/// digits alone exceed `max_digits`: 10^38 at scale 0 has an excess
/// of one digit and no scale to take it from.
fn fitRounded(x: i256, scale: u16) Error!Numeric {
    const excess = @max(digitsOf(x) -| max_digits, scale -| max_digits);
    if (excess > scale) return error.NumericOutOfRange;
    const q = roundDiv(x, pow10(@intCast(excess)), .round);
    // Rounding can carry into one digit more: 38 nines and a half.
    return .{ .coef = try fit(q), .scale = @intCast(scale - excess) };
}

fn pow10(n: u8) i256 {
    var r: i256 = 1;
    for (0..n) |_| r *= 10;
    return r;
}

fn pow10wide(n: u8) i512 {
    var r: i512 = 1;
    for (0..n) |_| r *= 10;
    return r;
}

/// A coefficient the type holds: at most `max_digits` digits.
fn fit(x: anytype) Error!i128 {
    if (@abs(x) >= pow10wide(max_digits)) return error.NumericOutOfRange;
    return @intCast(x);
}
