// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const zdb = @import("zdb");
const Meta = zdb.kv.Meta;
const Kind = zdb.kv.Kind;
const Encoding = zdb.kv.Encoding;
const Item = zdb.kv.Item;
const Key = zdb.kv.Key;
const Version = zdb.kv.Version;

const expectEqual = std.testing.expectEqual;

// These bit positions are on-disk state: every SST entry stores a Meta byte
// (see zdb/table/builder.zig). Reordering the fields in Meta silently
// reinterprets every table ever written, so pin the layout here — this test is
// what makes it safe to have no separate bit_delete/bit_* constants to
// disagree with the struct.
test "Meta: bit layout is frozen" {
    // kind occupies the low two bits.
    try expectEqual(@as(u8, 0), (Meta{ .kind = .state }).toByte());
    try expectEqual(@as(u8, 1), (Meta{ .kind = .delta }).toByte());
    try expectEqual(@as(u8, 2), (Meta{ .kind = .deleted }).toByte());

    // Encoding occupies the high nibble.
    try expectEqual(@as(u8, 0x00), (Meta{ .encoding = .raw }).toByte());
    try expectEqual(@as(u8, 0x10), (Meta{ .encoding = .roaring }).toByte());

    // kind and encoding must not smear into each other.
    const both = Meta{ .kind = .deleted, .encoding = .roaring };
    try expectEqual(@as(u8, 0x12), both.toByte());

    try expectEqual(@as(usize, 1), @sizeOf(Meta));
    try expectEqual(@as(usize, 8), @bitSizeOf(Meta));
}

test "Meta: default is raw with no flags" {
    try expectEqual(@as(u8, 0), (Meta{}).toByte());
    try expectEqual(@as(u8, 0), Meta.raw.toByte());
    // A KeyValue written before the meta field existed reads back 0, and this
    // is why that decodes as a plain state row rather than something surprising.
    const from_absent_field = Meta.fromByte(0);
    try expectEqual(Kind.state, from_absent_field.kind);
    try expectEqual(Encoding.raw, from_absent_field.encoding);
}

// Meta is @bitCast from bytes read off disk, so every one of the 256 possible
// bytes has to survive the round trip. This is the guard on Encoding staying
// non-exhaustive: make it exhaustive and the undefined tags (2..15) turn this
// into a panic in Debug and UB in ReleaseFast.
test "Meta: every byte round-trips" {
    for (0..256) |i| {
        const b: u8 = @intCast(i);
        try expectEqual(b, Meta.fromByte(b).toByte());
    }
}

test "Meta: reserved bits survive a round trip" {
    // A future writer may set bits we don't know about yet. We must carry them
    // through untouched rather than normalizing them away.
    const unknown = Meta.fromByte(0b1000_1100);
    try expectEqual(@as(u2, 0b11), unknown._reserved);
    try expectEqual(@as(u8, 0b1000_1100), unknown.toByte());
}

/// Three versions of one row, newest first, for the Item tests below.
const three = [_]Version{
    .{ .ts = 30, .meta = .{ .kind = .delta }, .value = "c" },
    .{ .ts = 20, .meta = .{ .kind = .delta }, .value = "b" },
    .{ .ts = 10, .meta = .{ .kind = .state }, .value = "a" },
};

test "Item: visibleAt cuts at the newest version not above ts" {
    const item = Item{
        .key = .{ .primary = "k" },
        .versions = .{ .many = &three },
    };

    // At a version's own ts: that version is visible.
    try expectEqual(3, item.visibleAt(30, .all).?.versions.len());
    try expectEqual(30, item.visibleAt(30, .all).?.newest().ts);
    try expectEqual(10, item.visibleAt(10, .all).?.newest().ts);
    try expectEqual(1, item.visibleAt(10, .all).?.versions.len());

    // Between two versions: the older one wins.
    try expectEqual(20, item.visibleAt(25, .all).?.newest().ts);
    try expectEqual(2, item.visibleAt(25, .all).?.versions.len());

    // Above every version: the whole item.
    try expectEqual(3, item.visibleAt(99, .all).?.versions.len());

    // Below every version: nothing is visible yet, at either depth.
    try expectEqual(null, item.visibleAt(9, .all));
    try expectEqual(null, item.visibleAt(9, .fold));

    // The key travels with the window.
    const at_25 = item.visibleAt(25, .all).?;
    try std.testing.expectEqualStrings("k", at_25.key.primary);
}

test "Item: visibleAt .fold stops at a state, a tombstone, or the end" {
    const latest = std.math.maxInt(u64);
    const at_state = Item{
        .key = .{ .primary = "k" },
        .versions = .{ .many = &three },
    };
    // Two deltas plus the state they fold onto.
    const state_chain = at_state.visibleAt(latest, .fold).?;
    try expectEqual(3, state_chain.versions.len());
    try expectEqual(10, state_chain.versions.slice()[2].ts);

    const history = [_]Version{
        .{ .ts = 40, .meta = .{ .kind = .delta }, .value = "d" },
        .{ .ts = 30, .meta = .{ .kind = .delta }, .value = "c" },
        .{ .ts = 20, .meta = .{ .kind = .deleted }, .value = "" },
        .{ .ts = 10, .meta = .{ .kind = .state }, .value = "a" },
    };
    const with_tomb = Item{
        .key = .{ .primary = "k" },
        .versions = .{ .many = &history },
    };
    // The tombstone ends the chain, and the state under it is left out.
    const tomb_chain = with_tomb.visibleAt(latest, .fold).?;
    try expectEqual(3, tomb_chain.versions.len());
    try expectEqual(20, tomb_chain.versions.slice()[2].ts);

    // A read below the newest delta folds from the one it sees.
    const older = with_tomb.visibleAt(35, .fold).?;
    try expectEqual(2, older.versions.len());
    try expectEqual(30, older.newest().ts);
    try expectEqual(20, older.versions.slice()[1].ts);

    // A read at the tombstone gets it alone; one below it, the state.
    try expectEqual(1, with_tomb.visibleAt(20, .fold).?.versions.len());
    try expectEqual(10, with_tomb.visibleAt(15, .fold).?.newest().ts);

    // All deltas: the chain is every version, and stays open.
    const deltas = [_]Version{
        .{ .ts = 30, .meta = .{ .kind = .delta }, .value = "c" },
        .{ .ts = 20, .meta = .{ .kind = .delta }, .value = "b" },
    };
    const open = Item{
        .key = .{ .primary = "k" },
        .versions = .{ .many = &deltas },
    };
    try expectEqual(2, open.visibleAt(latest, .fold).?.versions.len());

    // A state at the head is its own chain.
    const solo = Item{
        .key = .{ .primary = "k" },
        .versions = .{ .one = .{ .ts = 30, .meta = .{}, .value = "c" } },
    };
    try expectEqual(1, solo.visibleAt(latest, .fold).?.versions.len());
    try expectEqual(null, solo.visibleAt(29, .fold));
}

test "visibleVersions: a slice of the versions given, empty for none" {
    const visible = zdb.kv.visibleVersions;

    // The rest from the first visible version, or down to the state.
    try expectEqual(three[1..].ptr, visible(&three, 25, .all).ptr);
    try expectEqual(2, visible(&three, 25, .all).len);
    try expectEqual(3, visible(&three, 99, .fold).len);
    try expectEqual(1, visible(&three, 10, .fold).len);

    // Nothing visible, and nothing to look at: an iterator under a
    // limit can hand over an item with no versions.
    try expectEqual(0, visible(&three, 9, .all).len);
    try expectEqual(0, visible(&.{}, 99, .all).len);
    try expectEqual(0, visible(&.{}, 99, .fold).len);
}

test "Item: dupe copies every version, free releases them" {
    const alloc = std.testing.allocator;
    const item = Item{
        .key = .{ .namespace = 7, .table_id = 3, .primary = "k" },
        .versions = .{ .many = &three },
    };

    const copy = try item.dupe(alloc);
    defer copy.free(alloc);

    try expectEqual(@as(u64, 7), copy.key.namespace);
    try expectEqual(@as(u32, 3), copy.key.table_id);
    try std.testing.expectEqualStrings("k", copy.key.primary);
    try expectEqual(@as(usize, 3), copy.versions.len());
    for (item.versions.slice(), copy.versions.slice()) |src, dst| {
        try expectEqual(src.ts, dst.ts);
        try expectEqual(src.meta.toByte(), dst.meta.toByte());
        try std.testing.expectEqualStrings(src.value, dst.value);
        // Owned storage, not the original bytes.
        try std.testing.expect(src.value.ptr != dst.value.ptr);
    }
    try std.testing.expect(item.key.primary.ptr != copy.key.primary.ptr);
}

test "Key: marshal round trip carries no timestamp" {
    const alloc = std.testing.allocator;
    const key = Key{ .namespace = 9, .table_id = 4, .primary = "row" };

    const raw = try key.marshal(alloc);
    defer alloc.free(raw);
    // 8-byte namespace, 4-byte table_id, then the primary — nothing else.
    try expectEqual(@as(usize, 12 + 3), raw.len);
    try expectEqual(key.len(), raw.len);

    const back = Key.init(raw);
    try expectEqual(key.namespace, back.namespace);
    try expectEqual(key.table_id, back.table_id);
    try std.testing.expectEqualStrings(key.primary, back.primary);

    // Order is namespace, then table_id, then primary bytes, which is
    // also the bytewise order of the marshaled keys.
    const b = Key{ .namespace = 9, .table_id = 4, .primary = "rox" };
    const raw_b = try b.marshal(alloc);
    defer alloc.free(raw_b);
    try expectEqual(std.math.Order.lt, key.order(b));
    try expectEqual(std.math.Order.lt, std.mem.order(u8, raw, raw_b));

    const other_tid = Key{ .namespace = 9, .table_id = 5, .primary = "a" };
    try expectEqual(std.math.Order.lt, key.order(other_tid));
    // `inf` is the one bound with a stored form: nothing, and it reads
    // back as `inf`. The bound decides, not the primary's length.
    try expectEqual(@as(usize, 0), Key.inf.len());
    const raw_inf = try Key.inf.marshal(alloc);
    defer alloc.free(raw_inf);
    try expectEqual(@as(usize, 0), raw_inf.len);
    try std.testing.expect(Key.init(raw_inf).isInf());
    try expectEqual(@as(usize, 12), (Key{ .namespace = 9 }).len());
}

test "Key: after sorts past its primary and before every longer one" {
    const k: Key = .{ .table_id = 7, .primary = "k" };
    const k_end = (Key{ .table_id = 7, .primary = "k" }).after();
    const k_nul: Key = .{ .table_id = 7, .primary = "k\x00" };
    const ka: Key = .{ .table_id = 7, .primary = "ka" };
    const j: Key = .{ .table_id = 7, .primary = "j" };
    try std.testing.expectEqual(std.math.Order.lt, k.order(k_end));
    try std.testing.expectEqual(std.math.Order.gt, k_end.order(k));
    try std.testing.expectEqual(std.math.Order.eq, k_end.order(k_end));
    // The old bound was primary ++ "\x00"; `after` admits the same set.
    try std.testing.expectEqual(std.math.Order.lt, k_end.order(k_nul));
    try std.testing.expectEqual(std.math.Order.lt, k_end.order(ka));
    try std.testing.expectEqual(std.math.Order.gt, k_end.order(j));
    // Never crosses a table or namespace boundary.
    const other: Key = .{ .table_id = 8, .primary = "a" };
    try std.testing.expectEqual(std.math.Order.lt, k_end.order(other));
}

test "orderBytes: agrees with std.mem.order at any length and prefix" {
    // Pairs that share a prefix of any length up to four words and
    // then differ, end, or carry on: the word loop, the byte tail and
    // the length each decide some of them.
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rand = prng.random();
    var a_buf: [32]u8 = undefined;
    var b_buf: [32]u8 = undefined;
    for (0..20_000) |_| {
        rand.bytes(&a_buf);
        rand.bytes(&b_buf);
        const shared = rand.uintAtMost(usize, a_buf.len);
        @memcpy(b_buf[0..shared], a_buf[0..shared]);
        const a = a_buf[0..rand.uintAtMost(usize, a_buf.len)];
        const b = b_buf[0..rand.uintAtMost(usize, b_buf.len)];
        try expectEqual(std.mem.order(u8, a, b), zdb.kv.orderBytes(a, b));
    }
}

test "Key: table edges sort outside every key of their table" {
    const tk: zdb.kv.TableKey = .{ .namespace = 1, .table_id = 7 };
    const left = Key.tableLeft(tk);
    const right = Key.tableRight(tk);
    const lowest: Key = .{ .namespace = 1, .table_id = 7, .primary = "\x00" };
    const high: Key = .{ .namespace = 1, .table_id = 7, .primary = "\xff\xff" };
    const prev: Key = .{ .namespace = 1, .table_id = 6, .primary = "\xff" };
    const next: Key = .{ .namespace = 1, .table_id = 8, .primary = "" };
    try std.testing.expectEqual(std.math.Order.lt, left.order(lowest));
    try std.testing.expectEqual(std.math.Order.lt, left.order(right));
    try std.testing.expectEqual(std.math.Order.gt, right.order(high));
    try std.testing.expectEqual(std.math.Order.gt, right.order(high.after()));
    try std.testing.expectEqual(std.math.Order.eq, left.order(left));
    try std.testing.expectEqual(std.math.Order.eq, right.order(right));
    // Between the neighbouring tables' keys, never among them.
    try std.testing.expectEqual(std.math.Order.gt, left.order(prev));
    try std.testing.expectEqual(std.math.Order.lt, right.order(next));
    try std.testing.expect(left.isTableEdge() and right.isTableEdge());
    try std.testing.expect(!lowest.isTableEdge() and !Key.inf.isTableEdge());
}
