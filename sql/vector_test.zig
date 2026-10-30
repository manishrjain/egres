// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");

test "sql.vector: exact distances follow pgvector operator semantics" {
    const alloc = testing.allocator;
    const a = try sql.value.parseVector(alloc, "[1,0,0]", 3);
    defer alloc.free(a.vector);
    const b = try sql.value.parseVector(alloc, "[0,1,0]", 3);
    defer alloc.free(b.vector);
    const c = try sql.value.parseVector(alloc, "[2,0,0]", 3);
    defer alloc.free(c.vector);

    try testing.expectApproxEqAbs(@as(f64, 1.4142135623730951), try sql.vector.l2(a.vector, b.vector), 1e-12);
    try testing.expectApproxEqAbs(@as(f64, 1), try sql.vector.cosine(a.vector, b.vector), 1e-12);
    try testing.expectApproxEqAbs(@as(f64, -2), try sql.vector.negativeInnerProduct(a.vector, c.vector), 1e-12);
}

test "sql.vector: invalid dimensions and zero cosine inputs are rejected" {
    const alloc = testing.allocator;
    const two = try sql.value.parseVector(alloc, "[1,2]", 2);
    defer alloc.free(two.vector);
    const three = try sql.value.parseVector(alloc, "[1,2,3]", 3);
    defer alloc.free(three.vector);
    const zero = try sql.value.parseVector(alloc, "[0,0]", 2);
    defer alloc.free(zero.vector);

    try testing.expectError(error.DimensionMismatch, sql.vector.l2(two.vector, three.vector));
    try testing.expectError(error.ZeroVector, sql.vector.cosine(two.vector, zero.vector));
    try testing.expectError(error.InvalidVector, sql.vector.l2("bad", two.vector));
}

test "sql.vector: SIMD batches and scalar tails match a scalar reference" {
    const alloc = testing.allocator;
    const dimensions_to_test = [_]usize{ 8, 9, 768 };
    for (&dimensions_to_test) |dimensions| {
        const left = try sequence(alloc, dimensions, 1);
        defer alloc.free(left.vector);
        const right = try sequence(alloc, dimensions, 2);
        defer alloc.free(right.vector);

        try testing.expectApproxEqAbs(
            scalarL2(left.vector, right.vector),
            try sql.vector.l2(left.vector, right.vector),
            1e-9,
        );
        try testing.expectApproxEqAbs(
            scalarCosine(left.vector, right.vector),
            try sql.vector.cosine(left.vector, right.vector),
            1e-9,
        );
        try testing.expectApproxEqAbs(
            -scalarDot(left.vector, right.vector),
            try sql.vector.negativeInnerProduct(left.vector, right.vector),
            1e-9,
        );
    }
}

fn sequence(
    alloc: std.mem.Allocator,
    dimensions: usize,
    offset: usize,
) !sql.value.Value {
    const bytes = try alloc.alloc(u8, dimensions * 4);
    for (0..dimensions) |i| {
        const value: f32 = @floatFromInt(i + offset);
        std.mem.writeInt(u32, bytes[i * 4 ..][0..4], @bitCast(value), .little);
    }
    return .{ .vector = bytes };
}

fn scalarL2(left: []const u8, right: []const u8) f64 {
    var sum: f64 = 0;
    for (0..left.len / 4) |i| {
        const delta = scalarElement(left, i) - scalarElement(right, i);
        sum += delta * delta;
    }
    return std.math.sqrt(sum);
}

fn scalarCosine(left: []const u8, right: []const u8) f64 {
    const dot = scalarDot(left, right);
    var left_norm: f64 = 0;
    var right_norm: f64 = 0;
    for (0..left.len / 4) |i| {
        const x = scalarElement(left, i);
        const y = scalarElement(right, i);
        left_norm += x * x;
        right_norm += y * y;
    }
    return 1 - dot / std.math.sqrt(left_norm * right_norm);
}

fn scalarDot(left: []const u8, right: []const u8) f64 {
    var sum: f64 = 0;
    for (0..left.len / 4) |i|
        sum += scalarElement(left, i) * scalarElement(right, i);
    return sum;
}

fn scalarElement(bytes: []const u8, index: usize) f64 {
    const bits = std.mem.readInt(u32, bytes[index * 4 ..][0..4], .little);
    return @as(f32, @bitCast(bits));
}
