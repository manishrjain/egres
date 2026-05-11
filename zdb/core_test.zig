const std = @import("std");
const zdb = @import("zdb");
const Key = zdb.key.Key;

test "config validate rejects single compactor and in-memory dir" {
    const bad_compactors = zdb.config.Config{
        .num_compactors = 1,
    };
    try std.testing.expectError(
        zdb.config.Error.InvalidNumCompactors,
        bad_compactors.validate(),
    );

    const bad_in_memory_dir = zdb.config.Config{
        .in_memory = true,
        .dir = "/tmp/db",
    };
    try std.testing.expectError(
        zdb.config.Error.InvalidDirForInMemory,
        bad_in_memory_dir.validate(),
    );
}

test "keyWithTs parseTs parseKey and compareWithTs match badger ordering" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    const alloc = arena.allocator();

    const a_v9 = try (Key{ .primary = "a", .timestamp = 9 }).marshal(alloc);
    const a_v7 = try (Key{ .primary = "a", .timestamp = 7 }).marshal(alloc);
    const b_v1 = try (Key{ .primary = "b", .timestamp = 1 }).marshal(alloc);

    try std.testing.expectEqual(@as(u64, 9), Key.init(a_v9).timestamp);
    try std.testing.expectEqualStrings("a", Key.init(a_v9).primary);

    // Same user key: larger version must sort first.
    try std.testing.expectEqual(std.math.Order.lt, Key.init(a_v9).order(Key.init(a_v7)));
    try std.testing.expectEqual(std.math.Order.gt, Key.init(a_v7).order(Key.init(a_v9)));

    // User key still sorts before timestamp suffix.
    try std.testing.expectEqual(std.math.Order.lt, Key.init(a_v7).order(Key.init(b_v1)));
}

test "entry flags and estimate size" {
    const e = zdb.types.Entry
        .init("hello", "world")
        .withVersion(7)
        .withUserMeta(3)
        .withDelete()
        .withDiscardEarlierVersions();

    try std.testing.expectEqual(@as(usize, 12), e.estimateSize());
    try std.testing.expect(e.isDelete());
    try std.testing.expect(e.discardEarlierVersions());
    try std.testing.expectEqual(@as(u64, 7), e.version);
    try std.testing.expectEqual(@as(u8, 3), e.user_meta);
}
