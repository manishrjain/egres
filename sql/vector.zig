// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Exact dense-vector distance functions over the row codec's float32 bytes.
//!
//! Vectors are stored as little-endian IEEE-754 float32 elements (value.zig).
//! Accumulating in float64 keeps the SIMD operators stable for practical
//! embedding dimensions without changing the stored representation.

const std = @import("std");
const _value = @import("value.zig");

const String = _value.String;
const lanes = 8;
const Float32x8 = @Vector(lanes, f32);
const Float64x8 = @Vector(lanes, f64);

pub const Error = error{
    DimensionMismatch,
    InvalidVector,
    ZeroVector,
    NonFiniteResult,
};

/// Euclidean (`<->`) distance between equally dimensioned vectors.
pub fn l2(lhs: String, rhs: String) Error!f64 {
    try validatePair(lhs, rhs);
    const sum = squaredDistance(lhs, rhs);
    const result = std.math.sqrt(sum);
    if (!std.math.isFinite(result)) return error.NonFiniteResult;
    return result;
}

/// Cosine (`<=>`) distance: one minus cosine similarity. A zero vector has
/// no cosine direction and is rejected instead of receiving an arbitrary rank.
pub fn cosine(lhs: String, rhs: String) Error!f64 {
    try validatePair(lhs, rhs);
    const values = cosineParts(lhs, rhs);
    const dot = values.dot;
    const lhs_norm = values.lhs_norm;
    const rhs_norm = values.rhs_norm;
    if (lhs_norm == 0 or rhs_norm == 0) return error.ZeroVector;
    const result = 1 - dot / std.math.sqrt(lhs_norm * rhs_norm);
    if (!std.math.isFinite(result)) return error.NonFiniteResult;
    return result;
}

/// Negative inner-product (`<#>`) distance, so ascending order ranks the
/// greatest similarity first, matching pgvector's operator convention.
pub fn negativeInnerProduct(lhs: String, rhs: String) Error!f64 {
    try validatePair(lhs, rhs);
    const result = -dotProduct(lhs, rhs);
    if (!std.math.isFinite(result)) return error.NonFiniteResult;
    return result;
}

fn validatePair(lhs: String, rhs: String) Error!void {
    if (lhs.len % 4 != 0 or rhs.len % 4 != 0) return error.InvalidVector;
    if (lhs.len != rhs.len) return error.DimensionMismatch;
    for (0..lhs.len / 4) |i| {
        if (!std.math.isFinite(element(lhs, i)) or
            !std.math.isFinite(element(rhs, i))) return error.InvalidVector;
    }
}

fn squaredDistance(lhs: String, rhs: String) f64 {
    const count = lhs.len / 4;
    const lhs_ptr: [*]align(1) const f32 = @ptrCast(lhs.ptr);
    const rhs_ptr: [*]align(1) const f32 = @ptrCast(rhs.ptr);
    var accumulator: Float64x8 = @splat(0);
    var i: usize = 0;
    while (i + lanes <= count) : (i += lanes) {
        const left: Float32x8 = lhs_ptr[i..][0..lanes].*;
        const right: Float32x8 = rhs_ptr[i..][0..lanes].*;
        const delta: Float64x8 = @floatCast(left - right);
        accumulator += delta * delta;
    }
    var sum: f64 = @reduce(.Add, accumulator);
    while (i < count) : (i += 1) {
        const delta = element(lhs, i) - element(rhs, i);
        sum += delta * delta;
    }
    return sum;
}

fn dotProduct(lhs: String, rhs: String) f64 {
    const count = lhs.len / 4;
    const lhs_ptr: [*]align(1) const f32 = @ptrCast(lhs.ptr);
    const rhs_ptr: [*]align(1) const f32 = @ptrCast(rhs.ptr);
    var accumulator: Float64x8 = @splat(0);
    var i: usize = 0;
    while (i + lanes <= count) : (i += lanes) {
        const left32: Float32x8 = @bitCast(lhs_ptr[i..][0..lanes].*);
        const right32: Float32x8 = @bitCast(rhs_ptr[i..][0..lanes].*);
        const left: Float64x8 = @floatCast(left32);
        const right: Float64x8 = @floatCast(right32);
        accumulator += left * right;
    }
    var sum: f64 = @reduce(.Add, accumulator);
    while (i < count) : (i += 1) sum += element(lhs, i) * element(rhs, i);
    return sum;
}

const CosineParts = struct { dot: f64, lhs_norm: f64, rhs_norm: f64 };

fn cosineParts(lhs: String, rhs: String) CosineParts {
    const count = lhs.len / 4;
    const lhs_ptr: [*]align(1) const f32 = @ptrCast(lhs.ptr);
    const rhs_ptr: [*]align(1) const f32 = @ptrCast(rhs.ptr);
    var dot: Float64x8 = @splat(0);
    var lhs_norm: Float64x8 = @splat(0);
    var rhs_norm: Float64x8 = @splat(0);
    var i: usize = 0;
    while (i + lanes <= count) : (i += lanes) {
        const left32: Float32x8 = @bitCast(lhs_ptr[i..][0..lanes].*);
        const right32: Float32x8 = @bitCast(rhs_ptr[i..][0..lanes].*);
        const left: Float64x8 = @floatCast(left32);
        const right: Float64x8 = @floatCast(right32);
        dot += left * right;
        lhs_norm += left * left;
        rhs_norm += right * right;
    }
    var result: CosineParts = .{
        .dot = @reduce(.Add, dot),
        .lhs_norm = @reduce(.Add, lhs_norm),
        .rhs_norm = @reduce(.Add, rhs_norm),
    };
    while (i < count) : (i += 1) {
        const left = element(lhs, i);
        const right = element(rhs, i);
        result.dot += left * right;
        result.lhs_norm += left * left;
        result.rhs_norm += right * right;
    }
    return result;
}

fn element(bytes: String, index: usize) f64 {
    const bits = std.mem.readInt(u32, bytes[index * 4 ..][0..4], .little);
    return @as(f32, @bitCast(bits));
}
