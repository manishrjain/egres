const std = @import("std");
const testing = std.testing;
const skiplist = @import("zdb").skl;
const Key = @import("zdb").key.Key;
const u = @import("utils");

fn writeKey(buf: []u8, idx: usize) ![]const u8 {
    return std.fmt.bufPrint(buf, "k{d:0>10}", .{idx});
}

fn fillValueForIndex(allocator: std.mem.Allocator, rand: *std.Random) ![]u8 {
    const min_len = 4 * 1024;
    const max_len = 1024 * 1024;
    const len = rand.intRangeAtMost(usize, min_len, max_len);
    const value = try allocator.alloc(u8, len);
    rand.bytes(value);
    return value;
}

test "skiplist basic put and get with timestamps" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    // Put at ts=1, get at ts=1 should find it.
    try sl.put(.{ .primary = "key1", .timestamp = 1 }, "val1");
    try testing.expectEqual(@as(usize, 1), sl.numNodes());

    const item = sl.get(.{ .primary = "key1", .timestamp = 1 }).?;
    try testing.expectEqualStrings("val1", item.value);
    try testing.expectEqual(@as(u64, 1), item.key.timestamp);

    // Get at higher ts should also find it.
    const item2 = sl.get(.{ .primary = "key1", .timestamp = 100 }).?;
    try testing.expectEqualStrings("val1", item2.value);

    // Get at lower ts should miss.
    try testing.expect(sl.get(.{ .primary = "key1", .timestamp = 0 }) == null);

    // Different key should miss.
    try testing.expect(sl.get(.{ .primary = "key2", .timestamp = 100 }) == null);
}

test "skiplist update same key same timestamp" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .primary = "key1", .timestamp = 5 }, "first");
    try sl.put(.{ .primary = "key1", .timestamp = 5 }, "second");

    // Should return the updated value.
    const item = sl.get(.{ .primary = "key1", .timestamp = 5 }).?;
    try testing.expectEqualStrings("second", item.value);
    try testing.expectEqual(@as(usize, 1), sl.numNodes());
}

test "skiplist multiple versions of same key" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .primary = "key1", .timestamp = 10 }, "v10");
    try sl.put(.{ .primary = "key1", .timestamp = 20 }, "v20");
    try sl.put(.{ .primary = "key1", .timestamp = 30 }, "v30");
    try testing.expectEqual(@as(usize, 3), sl.numNodes());

    // Get at exact timestamps returns that version.
    try testing.expectEqualStrings("v30", sl.get(.{ .primary = "key1", .timestamp = 30 }).?.value);
    try testing.expectEqualStrings("v20", sl.get(.{ .primary = "key1", .timestamp = 20 }).?.value);
    try testing.expectEqualStrings("v10", sl.get(.{ .primary = "key1", .timestamp = 10 }).?.value);

    // Get at ts between versions returns the latest version <= ts.
    try testing.expectEqualStrings("v20", sl.get(.{ .primary = "key1", .timestamp = 25 }).?.value);
    try testing.expectEqualStrings("v10", sl.get(.{ .primary = "key1", .timestamp = 15 }).?.value);

    // Get at ts > all versions returns the latest.
    try testing.expectEqualStrings("v30", sl.get(.{ .primary = "key1", .timestamp = 100 }).?.value);

    // Get at ts < all versions misses.
    try testing.expect(sl.get(.{ .primary = "key1", .timestamp = 5 }) == null);
}

test "skiplist contains with timestamps" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .primary = "key1", .timestamp = 10 }, "val");
    try testing.expect(sl.contains(.{ .primary = "key1", .timestamp = 10 }));
    try testing.expect(sl.contains(.{ .primary = "key1", .timestamp = 100 }));
    try testing.expect(!sl.contains(.{ .primary = "key1", .timestamp = 5 }));
    try testing.expect(!sl.contains(.{ .primary = "key2", .timestamp = 100 }));
}

test "skiplist namespace isolation" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .namespace = 1, .primary = "key1", .timestamp = 10 }, "ns1");
    try sl.put(.{ .namespace = 2, .primary = "key1", .timestamp = 10 }, "ns2");

    const item1 = sl.get(.{ .namespace = 1, .primary = "key1", .timestamp = 10 }).?;
    try testing.expectEqualStrings("ns1", item1.value);

    const item2 = sl.get(.{ .namespace = 2, .primary = "key1", .timestamp = 10 }).?;
    try testing.expectEqualStrings("ns2", item2.value);

    // Wrong namespace misses.
    try testing.expect(sl.get(.{ .namespace = 3, .primary = "key1", .timestamp = 10 }) == null);
}

test "skiplist iterator ordering and seek" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    const data = [_]struct { key: []const u8, ts: u64, value: []const u8 }{
        .{ .key = "5", .ts = 1, .value = "50" },
        .{ .key = "2", .ts = 2, .value = "20" },
        .{ .key = "8", .ts = 3, .value = "80" },
        .{ .key = "1", .ts = 4, .value = "10" },
        .{ .key = "9", .ts = 5, .value = "90" },
        .{ .key = "3", .ts = 6, .value = "30" },
    };
    for (data) |item| try sl.put(.{ .primary = item.key, .timestamp = item.ts }, item.value);

    var it = sl.iterator();
    defer it.deinit();

    const expected = [_][]const u8{ "1", "2", "3", "5", "8", "9" };
    for (expected) |k| {
        const e = it.itemRef().?;
        try testing.expectEqualStrings(k, e.key.primary);
        try it.next();
    }
    try testing.expect(it.itemRef() == null);

    var seek_it = sl.iterator();
    defer seek_it.deinit();
    try seek_it.seekRange(.{ .primary = "4", .timestamp = 1 }, .{});
    try testing.expectEqualStrings("5", seek_it.itemRef().?.key.primary);
    try seek_it.next();
    try testing.expectEqualStrings("8", seek_it.itemRef().?.key.primary);
}

test "skiplist refcount usage with explicit acquire/release" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .primary = "1", .timestamp = 1 }, "10");
    try sl.put(.{ .primary = "2", .timestamp = 2 }, "20");

    sl.acquire();
    sl.acquire();

    const item = sl.get(.{ .primary = "1", .timestamp = 1 }).?;
    try testing.expectEqualStrings("10", item.value);

    sl.release();
    sl.release();

    const item2 = sl.get(.{ .primary = "1", .timestamp = 1 }).?;
    try testing.expectEqualStrings("10", item2.value);
}

test "skiplist grows beyond 64 MiB up to 256 MiB and validates full iteration" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    const target_capacity = 256 * 1024 * 1024;
    try testing.expect(sl.memoryCapacity() >= 64 * 1024 * 1024);

    var expected_values = std.ArrayList([]u8).empty;
    defer expected_values.deinit(testing.allocator);

    var tmp_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    _ = try tmp_arena.allocator().alloc(u8, target_capacity);
    _ = tmp_arena.reset(.{ .retain_with_limit = target_capacity });
    defer tmp_arena.deinit();

    var value_prng = std.Random.DefaultPrng.init(@intCast(std.time.nanoTimestamp()));
    var value_rand = value_prng.random();

    const insert_start_ns = std.time.nanoTimestamp();
    var inserted: usize = 0;
    var ts: u64 = 1;
    while (sl.memoryCapacity() < target_capacity) : (inserted += 1) {
        var key_buf: [32]u8 = undefined;
        const key = try writeKey(&key_buf, inserted);
        const value = try fillValueForIndex(tmp_arena.allocator(), &value_rand);

        try sl.put(.{ .primary = key, .timestamp = ts }, value);
        ts += 1;
        try expected_values.append(testing.allocator, value);

        if ((inserted + 1) % 100 == 0) {
            u.testPrint(
                "[zdb.skl] inserted keys={d} capacity={d} usage={d}\n",
                .{ inserted + 1, sl.memoryCapacity(), sl.memoryUsage() },
            );
        }
    }
    const insert_elapsed_ns: i128 = std.time.nanoTimestamp() - insert_start_ns;

    try testing.expect(sl.memoryCapacity() >= target_capacity);
    try testing.expectEqual(inserted, sl.numNodes());
    try testing.expectEqual(inserted, expected_values.items.len);
    u.testPrint(
        "[zdb.skl] final inserted keys={d} insert_elapsed_ms={d}\n",
        .{ inserted, @divTrunc(insert_elapsed_ns, std.time.ns_per_ms) },
    );

    const verify_start_ns = std.time.nanoTimestamp();
    var it = sl.iterator();
    defer it.deinit();

    var idx: usize = 0;
    while (idx < inserted) : (idx += 1) {
        const entry = it.itemRef() orelse return error.TestUnexpectedResult;

        var expected_key_buf: [32]u8 = undefined;
        const expected_key = try writeKey(&expected_key_buf, idx);
        try testing.expectEqualStrings(expected_key, entry.key.primary);

        const expected_value = expected_values.items[idx];
        try testing.expectEqualSlices(u8, expected_value, entry.value);
        try it.next();
    }
    try testing.expect(it.itemRef() == null);
    const verify_elapsed_ns: i128 = std.time.nanoTimestamp() - verify_start_ns;
    u.testPrint(
        "[zdb.skl] verification complete keys={d} verify_elapsed_ms={d}\n",
        .{ inserted, @divTrunc(verify_elapsed_ns, std.time.ns_per_ms) },
    );
}
