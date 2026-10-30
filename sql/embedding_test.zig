// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");

const Provider = sql.embedding.Provider;

test "sql.embedding: registry owns validated provider configuration" {
    var registry = try sql.embedding.Registry.init(testing.allocator);
    defer registry.deinit();
    const provider = Provider{ .name = "docs", .endpoint = "https://example.com/v1", .model = "test", .dimensions = 3, .credential_ref = "docs-key" };
    try registry.add(provider);
    try testing.expectEqual(@as(u32, 3), registry.get("docs").?.dimensions);
    try testing.expectError(error.DuplicateProvider, registry.add(provider));
}

test "sql.embedding: registry rejects invalid provider configuration" {
    var registry = try sql.embedding.Registry.init(testing.allocator);
    defer registry.deinit();

    try testing.expectError(error.InvalidEndpoint, registry.add(.{
        .name = "docs",
        .endpoint = "ftp://example.com",
        .model = "test",
        .dimensions = 3,
        .credential_ref = "docs-key",
    }));
    try testing.expectError(error.InvalidDimensions, registry.add(.{
        .name = "docs",
        .endpoint = "https://example.com/v1",
        .model = "test",
        .dimensions = 0,
        .credential_ref = "docs-key",
    }));
    try testing.expectError(error.InvalidProvider, registry.add(.{
        .name = "docs",
        .endpoint = "https://example.com/v1",
        .model = "test",
        .dimensions = 3,
        .credential_ref = "",
    }));
    try testing.expectError(error.InvalidLimits, registry.add(.{
        .name = "docs",
        .endpoint = "https://example.com/v1",
        .model = "test",
        .dimensions = 3,
        .credential_ref = "docs-key",
        .max_concurrency = 0,
    }));
}

test "sql.embedding: response validation returns canonical vector bytes" {
    const provider = Provider{ .name = "docs", .endpoint = "https://example.com/v1", .model = "test", .dimensions = 3, .credential_ref = "docs-key" };
    const vector = try sql.embedding.validateEmbedding(testing.allocator, provider, &.{ 1.0, -2.5, 3.25 });
    defer testing.allocator.free(vector.vector);
    try testing.expectEqual(@as(usize, 12), vector.vector.len);
    try testing.expectError(error.DimensionMismatch, sql.embedding.validateEmbedding(testing.allocator, provider, &.{1.0}));
    try testing.expectError(error.NonFiniteEmbedding, sql.embedding.validateEmbedding(testing.allocator, provider, &.{ std.math.nan(f64), 0.0, 0.0 }));
    try testing.expectError(error.NonFiniteEmbedding, sql.embedding.validateEmbedding(testing.allocator, provider, &.{ 1e100, 0.0, 0.0 }));
}

test "sql.embedding: embed resolves a provider and validates its response" {
    var registry = try sql.embedding.Registry.init(testing.allocator);
    defer registry.deinit();
    try registry.add(.{ .name = "docs", .endpoint = "https://example.com/v1", .model = "test", .dimensions = 2, .credential_ref = "docs-key" });

    const Invoke = struct {
        fn call(_: *anyopaque, _: Provider, _: []const u8) ![]const f64 {
            return &.{ 1.0, 2.0 };
        }
    };
    var context: u8 = 0;
    const vector = try sql.embedding.embed(testing.allocator, &registry, "docs", "hello", &context, Invoke.call);
    defer testing.allocator.free(vector.vector);
    try testing.expectEqual(@as(usize, 8), vector.vector.len);
    try testing.expectError(error.UnknownProvider, sql.embedding.embed(testing.allocator, &registry, "nope", "hello", &context, Invoke.call));
}

test "sql.embedding: embed preserves invocation and response failures" {
    var registry = try sql.embedding.Registry.init(testing.allocator);
    defer registry.deinit();
    try registry.add(.{
        .name = "docs",
        .endpoint = "https://example.com/v1",
        .model = "test",
        .dimensions = 2,
        .credential_ref = "docs-key",
    });

    const Invoke = struct {
        fn fails(_: *anyopaque, _: Provider, _: []const u8) ![]const f64 {
            return error.ProviderUnavailable;
        }

        fn wrongDimensions(_: *anyopaque, _: Provider, _: []const u8) ![]const f64 {
            return &.{1.0};
        }
    };
    var context: u8 = 0;
    try testing.expectError(error.EmptyInput, sql.embedding.embed(
        testing.allocator,
        &registry,
        "docs",
        "",
        &context,
        Invoke.fails,
    ));
    try testing.expectError(error.ProviderUnavailable, sql.embedding.embed(
        testing.allocator,
        &registry,
        "docs",
        "hello",
        &context,
        Invoke.fails,
    ));
    try testing.expectError(error.DimensionMismatch, sql.embedding.embed(
        testing.allocator,
        &registry,
        "docs",
        "hello",
        &context,
        Invoke.wrongDimensions,
    ));
}
