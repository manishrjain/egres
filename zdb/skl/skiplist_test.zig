// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const skiplist = @import("zdb").skl;
const Key = @import("zdb").kv.Key;
const Meta = @import("zdb").kv.Meta;
const Kind = @import("zdb").kv.Kind;
const u = @import("utils");
const String = u.String;

/// The value of `primary`'s newest version at or below `ts`, in the
/// default namespace; null when the key has no such version.
fn valueAt(sl: *skiplist.SkipList, primary: String, ts: u64) ?String {
    const ver = sl.getVersion(.{ .primary = primary }, ts) orelse return null;
    return ver.value;
}

fn writeKey(buf: []u8, idx: usize) !String {
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
    try sl.put(.{ .primary = "key1" }, 1, "val1");
    try testing.expectEqual(@as(usize, 1), sl.numNodes());

    const ver = sl.getVersion(.{ .primary = "key1" }, 1).?;
    try testing.expectEqualStrings("val1", ver.value);
    try testing.expectEqual(@as(u64, 1), ver.ts);

    // Get at higher ts should also find it.
    const ver2 = sl.getVersion(.{ .primary = "key1" }, 100).?;
    try testing.expectEqualStrings("val1", ver2.value);

    // Get at lower ts should miss.
    try testing.expect(sl.getVersion(.{ .primary = "key1" }, 0) == null);

    // Different key should miss.
    try testing.expect(sl.getVersion(.{ .primary = "key2" }, 100) == null);
}

test "skiplist update same key same timestamp" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .primary = "key1" }, 5, "first");
    try sl.put(.{ .primary = "key1" }, 5, "second");

    // Should return the updated value.
    const ver = sl.getVersion(.{ .primary = "key1" }, 5).?;
    try testing.expectEqualStrings("second", ver.value);
    try testing.expectEqual(@as(usize, 1), sl.numNodes());
}

test "skiplist multiple versions of same key" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .primary = "key1" }, 10, "v10");
    try sl.put(.{ .primary = "key1" }, 20, "v20");
    try sl.put(.{ .primary = "key1" }, 30, "v30");
    // One node per key, whatever its version count.
    try testing.expectEqual(@as(usize, 1), sl.numNodes());

    // Get at exact timestamps returns that version.
    try testing.expectEqualStrings("v30", valueAt(sl, "key1", 30).?);
    try testing.expectEqualStrings("v20", valueAt(sl, "key1", 20).?);
    try testing.expectEqualStrings("v10", valueAt(sl, "key1", 10).?);

    // Get at ts between versions returns the latest version <= ts.
    try testing.expectEqualStrings("v20", valueAt(sl, "key1", 25).?);
    try testing.expectEqualStrings("v10", valueAt(sl, "key1", 15).?);

    // Get at ts > all versions returns the latest.
    try testing.expectEqualStrings("v30", valueAt(sl, "key1", 100).?);

    // Get at ts < all versions misses.
    try testing.expect(sl.getVersion(.{ .primary = "key1" }, 5) == null);

    // The iterator hands the key over as one item, newest first.
    var it = try sl.iterator(testing.allocator);
    defer it.deinit();
    const item = it.itemRef().?;
    const vers = item.versions.slice();
    try testing.expectEqualStrings("key1", item.key.primary);
    try testing.expectEqual(@as(usize, 3), vers.len);
    try testing.expectEqual(@as(u64, 30), vers[0].ts);
    try testing.expectEqualStrings("v20", vers[1].value);
    try testing.expectEqual(@as(u64, 10), vers[2].ts);
    try it.next();
    try testing.expect(it.itemRef() == null);
}

test "skiplist: versions arrive in any order and read back newest first" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    // A txn's posting ops carry egids in the timestamp and arrive in
    // egid order; a re-put after a read_ts bump lands above the first.
    try sl.put(.{ .primary = "k" }, 20, "v20");
    try sl.put(.{ .primary = "k" }, 10, "v10");
    try sl.put(.{ .primary = "k" }, 30, "v30");
    try sl.put(.{ .primary = "k" }, 15, "v15");

    var it = try sl.iterator(testing.allocator);
    defer it.deinit();
    const item = it.itemRef().?;
    const vers = item.versions.slice();
    const want_ts = [_]u64{ 30, 20, 15, 10 };
    try testing.expectEqual(want_ts.len, vers.len);
    for (vers, want_ts) |ver, ts| {
        try testing.expectEqual(ts, ver.ts);
    }
    try testing.expectEqualStrings("v15", valueAt(sl, "k", 17).?);
}

test "skiplist: a same-timestamp put replaces its version node" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .primary = "k" }, 30, "v30");
    try sl.put(.{ .primary = "k" }, 20, "old");
    try sl.put(.{ .primary = "k" }, 10, "v10");
    // The middle version node is replaced; its neighbours stay.
    try sl.put(.{ .primary = "k" }, 20, "new");

    var it = try sl.iterator(testing.allocator);
    defer it.deinit();
    const item = it.itemRef().?;
    const vers = item.versions.slice();
    try testing.expectEqual(@as(usize, 3), vers.len);
    try testing.expectEqualStrings("v30", vers[0].value);
    try testing.expectEqualStrings("new", vers[1].value);
    try testing.expectEqualStrings("v10", vers[2].value);
    try testing.expectEqual(@as(usize, 1), sl.numNodes());
}

test "skiplist: a parked reader keeps its values while the writer inserts" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .primary = "k" }, 20, "v20");
    try sl.put(.{ .primary = "k" }, 10, "v10");

    var it = try sl.iterator(testing.allocator);
    defer it.deinit();
    const before = it.itemRef().?;
    const before_vers = before.versions.slice();
    const held_newest = before_vers[0].value;
    const held_oldest = before_vers[1].value;

    // The writer publishes version nodes above, between and below the ones
    // the reader holds, and a neighbouring key.
    try sl.put(.{ .primary = "k" }, 30, "v30");
    try sl.put(.{ .primary = "k" }, 15, "v15");
    try sl.put(.{ .primary = "k" }, 5, "v5");
    try sl.put(.{ .primary = "j" }, 1, "j1");

    // The reader's borrows are untouched: version nodes are never mutated.
    try testing.expectEqualStrings("v20", held_newest);
    try testing.expectEqualStrings("v10", held_oldest);
    try testing.expectEqual(@as(usize, 2), before_vers.len);

    // A fresh position sees the whole chain.
    try it.seekRange(.{ .primary = "k" }, Key.inf);
    const after = it.itemRef().?;
    const after_vers = after.versions.slice();
    const want_ts = [_]u64{ 30, 20, 15, 10, 5 };
    try testing.expectEqual(want_ts.len, after_vers.len);
    for (after_vers, want_ts) |ver, ts| {
        try testing.expectEqual(ts, ver.ts);
    }
}

test "skiplist contains with timestamps" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .primary = "key1" }, 10, "val");
    try testing.expect(sl.contains(.{ .primary = "key1" }, 10));
    try testing.expect(sl.contains(.{ .primary = "key1" }, 100));
    try testing.expect(!sl.contains(.{ .primary = "key1" }, 5));
    try testing.expect(!sl.contains(.{ .primary = "key2" }, 100));
}

test "skiplist namespace isolation" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .namespace = 1, .primary = "key1" }, 10, "ns1");
    try sl.put(.{ .namespace = 2, .primary = "key1" }, 10, "ns2");

    const v1 = sl.getVersion(.{ .namespace = 1, .primary = "key1" }, 10).?;
    try testing.expectEqualStrings("ns1", v1.value);

    const v2 = sl.getVersion(.{ .namespace = 2, .primary = "key1" }, 10).?;
    try testing.expectEqualStrings("ns2", v2.value);

    // Wrong namespace misses.
    const ns3 = Key{ .namespace = 3, .primary = "key1" };
    try testing.expect(sl.getVersion(ns3, 10) == null);
}

test "skiplist iterator ordering and seek" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    const data = [_]struct { key: String, ts: u64, value: String }{
        .{ .key = "5", .ts = 1, .value = "50" },
        .{ .key = "2", .ts = 2, .value = "20" },
        .{ .key = "8", .ts = 3, .value = "80" },
        .{ .key = "1", .ts = 4, .value = "10" },
        .{ .key = "9", .ts = 5, .value = "90" },
        .{ .key = "3", .ts = 6, .value = "30" },
    };
    for (data) |d| try sl.put(.{ .primary = d.key }, d.ts, d.value);

    var it = try sl.iterator(testing.allocator);
    defer it.deinit();

    const expected = [_]String{ "1", "2", "3", "5", "8", "9" };
    for (expected) |k| {
        const e = it.itemRef().?;
        try testing.expectEqualStrings(k, e.key.primary);
        try it.next();
    }
    try testing.expect(it.itemRef() == null);

    var seek_it = try sl.iterator(testing.allocator);
    defer seek_it.deinit();
    try seek_it.seekRange(.{ .primary = "4" }, Key.inf);
    try testing.expectEqualStrings("5", seek_it.itemRef().?.key.primary);
    try seek_it.next();
    try testing.expectEqualStrings("8", seek_it.itemRef().?.key.primary);
}

test "skiplist refcount usage with explicit acquire/release" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.put(.{ .primary = "1" }, 1, "10");
    try sl.put(.{ .primary = "2" }, 2, "20");

    sl.acquire();
    sl.acquire();

    const ver = sl.getVersion(.{ .primary = "1" }, 1).?;
    try testing.expectEqualStrings("10", ver.value);

    sl.release();
    sl.release();

    const ver2 = sl.getVersion(.{ .primary = "1" }, 1).?;
    try testing.expectEqualStrings("10", ver2.value);
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

    var value_prng = std.Random.DefaultPrng.init(
        @intCast(u.timestampNow(.realtime, .nanos)),
    );
    var value_rand = value_prng.random();

    const insert_start_ns = u.timestampNow(.monotonic, .nanos);
    var inserted: usize = 0;
    var ts: u64 = 1;
    while (sl.memoryCapacity() < target_capacity) : (inserted += 1) {
        var key_buf: [32]u8 = undefined;
        const key = try writeKey(&key_buf, inserted);
        const value = try fillValueForIndex(tmp_arena.allocator(), &value_rand);

        try sl.put(.{ .primary = key }, ts, value);
        ts += 1;
        try expected_values.append(testing.allocator, value);

        if ((inserted + 1) % 100 == 0) {
            u.testPrint(
                "[zdb.skl] inserted keys={d} capacity={d} usage={d}\n",
                .{ inserted + 1, sl.memoryCapacity(), sl.memoryUsage() },
            );
        }
    }
    const insert_elapsed_ns =
        u.timestampNow(.monotonic, .nanos) - insert_start_ns;

    try testing.expect(sl.memoryCapacity() >= target_capacity);
    try testing.expectEqual(inserted, sl.numNodes());
    try testing.expectEqual(inserted, expected_values.items.len);
    u.testPrint(
        "[zdb.skl] final inserted keys={d} insert_elapsed_ms={d}\n",
        .{ inserted, @divTrunc(insert_elapsed_ns, std.time.ns_per_ms) },
    );

    const verify_start_ns = u.timestampNow(.monotonic, .nanos);
    var it = try sl.iterator(testing.allocator);
    defer it.deinit();

    var idx: usize = 0;
    while (idx < inserted) : (idx += 1) {
        const entry = it.itemRef() orelse return error.TestUnexpectedResult;

        var expected_key_buf: [32]u8 = undefined;
        const expected_key = try writeKey(&expected_key_buf, idx);
        try testing.expectEqualStrings(expected_key, entry.key.primary);

        const expected_value = expected_values.items[idx];
        try testing.expectEqualSlices(u8, expected_value, entry.newest().value);
        try it.next();
    }
    try testing.expect(it.itemRef() == null);
    const verify_elapsed_ns =
        u.timestampNow(.monotonic, .nanos) - verify_start_ns;
    u.testPrint(
        "[zdb.skl] verification complete keys={d} verify_elapsed_ms={d}\n",
        .{ inserted, @divTrunc(verify_elapsed_ns, std.time.ns_per_ms) },
    );
}

test "skiplist: meta survives put and get" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    const meta: Meta = .{ .kind = .deleted, .encoding = .roaring };
    try sl.putItem(.{
        .key = .{ .primary = "key1" },
        .versions = .{ .one = .{ .ts = 1, .meta = meta, .value = "val1" } },
    });

    const got = sl.getVersion(.{ .primary = "key1" }, 1) orelse
        return error.TestExpectedItem;
    try testing.expectEqual(meta, got.meta);
    try testing.expectEqualStrings("val1", got.value);

    // The plain put() wrapper must still mean "raw, no flags".
    try sl.put(.{ .primary = "key2" }, 1, "val2");
    const plain = sl.getVersion(.{ .primary = "key2" }, 1) orelse
        return error.TestExpectedItem;
    try testing.expectEqual(Meta.raw, plain.meta);
}

test "skiplist: overwrite in place updates meta, not just value" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    // Same key AND same timestamp takes the overwrite-in-place branch, where
    // it'd be easy to swap the value and forget meta.
    try sl.putItem(.{
        .key = .{ .primary = "key1" },
        .versions = .{ .one = .{
            .ts = 5,
            .meta = .{ .encoding = .roaring },
            .value = "first",
        } },
    });
    try sl.putItem(.{
        .key = .{ .primary = "key1" },
        .versions = .{ .one = .{
            .ts = 5,
            .meta = .{ .kind = .deleted, .encoding = .raw },
            .value = "second",
        } },
    });

    const got = sl.getVersion(.{ .primary = "key1" }, 5) orelse
        return error.TestExpectedItem;
    try testing.expectEqualStrings("second", got.value);
    try testing.expectEqual(Meta{ .kind = .deleted, .encoding = .raw }, got.meta);
}

test "skiplist: meta reaches the iterator" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    try sl.putItem(.{
        .key = .{ .primary = "a" },
        .versions = .{ .one = .{
            .ts = 1,
            .meta = .{ .encoding = .roaring },
            .value = "va",
        } },
    });
    try sl.putItem(.{
        .key = .{ .primary = "b" },
        .versions = .{ .one = .{
            .ts = 1,
            .meta = .{ .kind = .deleted },
            .value = "vb",
        } },
    });

    var it = try sl.iterator(testing.allocator);
    defer it.deinit();
    try it.seekRange(Key.inf, Key.inf);

    const first = it.itemRef() orelse return error.TestExpectedItem;
    try testing.expectEqualStrings("a", first.key.primary);
    const first_meta = first.newest().meta;
    try testing.expectEqual(Meta{ .encoding = .roaring }, first_meta);

    try it.next();
    const second = it.itemRef() orelse return error.TestExpectedItem;
    try testing.expectEqualStrings("b", second.key.primary);
    const second_meta = second.newest().meta;
    try testing.expectEqual(Meta{ .kind = .deleted }, second_meta);
}

/// The version timestamps the iterator hands out for key "k" under
/// `limit`, newest first.
fn expectVersions(
    sl: *skiplist.SkipList,
    limit: skiplist.Iterator.Limit,
    want: []const u64,
) !void {
    var it = try skiplist.Iterator.init(
        testing.allocator,
        sl,
        limit,
        .forward,
    );
    defer it.deinit();
    try it.seekRange(Key.inf, Key.inf);
    const item = it.itemRef() orelse return error.TestExpectedItem;
    const vers = item.versions.slice();
    try testing.expectEqual(want.len, vers.len);
    for (want, vers) |ts, ver| try testing.expectEqual(ts, ver.ts);
}

test "skiplist: iterator limit copies only what a reader can use" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();
    // Read back newest first: 50 delta, 40 delta, 30 state, 20, 10.
    const puts = [_]struct { ts: u64, kind: Kind }{
        .{ .ts = 10, .kind = .state }, .{ .ts = 20, .kind = .state },
        .{ .ts = 30, .kind = .state }, .{ .ts = 40, .kind = .delta },
        .{ .ts = 50, .kind = .delta },
    };
    for (puts) |p| try sl.putItem(.{
        .key = .{ .primary = "k" },
        .versions = .{ .one = .{
            .ts = p.ts,
            .meta = .{ .kind = p.kind },
            .value = "v",
        } },
    });

    // The default: everything, as a flush needs it.
    try expectVersions(sl, .{}, &.{ 50, 40, 30, 20, 10 });
    // A read at 45 wanting one version: the delta at 40 folds down
    // to the state at 30; 50 is not visible.
    try expectVersions(sl, .{ .read_ts = 45, .want = 1 }, &.{ 40, 30 });
    // At 35 the newest visible version is already a state.
    try expectVersions(sl, .{ .read_ts = 35, .want = 1 }, &.{30});
    // Nothing visible at 5: empty.
    try expectVersions(sl, .{ .read_ts = 5 }, &.{});
}

test "skiplist: reverse iterator walks keys descending within the range" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();

    var empty = try skiplist.Iterator.init(
        testing.allocator,
        sl,
        .{},
        .reverse,
    );
    defer empty.deinit();
    try empty.seekRange(Key.inf, Key.inf);
    try testing.expect(empty.itemRef() == null);

    for ([_]String{ "5", "2", "8", "1", "9", "3" }) |k| {
        try sl.put(.{ .primary = k }, 1, k);
    }

    var it = try skiplist.Iterator.init(
        testing.allocator,
        sl,
        .{},
        .reverse,
    );
    defer it.deinit();

    const Case = struct { start: Key, end: Key, want: []const String };
    const cases = [_]Case{
        .{
            .start = Key.inf,
            .end = Key.inf,
            .want = &.{ "9", "8", "5", "3", "2", "1" },
        },
        // `end` is a stored key: excluded, the walk starts below it.
        .{
            .start = .{ .primary = "2" },
            .end = .{ .primary = "8" },
            .want = &.{ "5", "3", "2" },
        },
        // Both bounds between keys.
        .{
            .start = .{ .primary = "4" },
            .end = .{ .primary = "7" },
            .want = &.{"5"},
        },
        .{ .start = .{ .primary = "a" }, .end = Key.inf, .want = &.{} },
        .{ .start = Key.inf, .end = .{ .primary = "1" }, .want = &.{} },
        .{ .start = .{ .primary = "9" }, .end = Key.inf, .want = &.{"9"} },
    };
    for (cases) |c| {
        try it.seekRange(c.start, c.end);
        for (c.want) |k| {
            const item = it.itemRef() orelse return error.TestExpectedItem;
            try testing.expectEqualStrings(k, item.key.primary);
            try it.next();
        }
        try testing.expect(it.itemRef() == null);
    }
}

test "skiplist: a reverse reader steps onto a key inserted below it" {
    var sl = try skiplist.SkipList.init(testing.allocator);
    defer sl.release();
    try sl.put(.{ .primary = "k" }, 1, "k1");
    try sl.put(.{ .primary = "m" }, 1, "m1");

    var it = try skiplist.Iterator.init(
        testing.allocator,
        sl,
        .{},
        .reverse,
    );
    defer it.deinit();
    try it.seekRange(Key.inf, Key.inf);
    try testing.expectEqualStrings("m", it.itemRef().?.key.primary);
    try it.next();
    try testing.expectEqualStrings("k", it.itemRef().?.key.primary);

    // The writer links keys on both sides of the parked reader. The
    // step down follows k's `prev`, swung to the new neighbour.
    try sl.put(.{ .primary = "j" }, 1, "j1");
    try sl.put(.{ .primary = "l" }, 1, "l1");
    try it.next();
    try testing.expectEqualStrings("j", it.itemRef().?.key.primary);
    try it.next();
    try testing.expect(it.itemRef() == null);
}
