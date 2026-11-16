// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const zdb = @import("zdb");
const zbuf = @import("zbuf").zbuf;
const vfs = zdb.vfs;
const snapshot = zdb.snapshot;

/// Bytes that compress some, as a manifest's do: random, with a run
/// every 64 bytes.
fn manifestLike(alloc: std.mem.Allocator, len: usize) ![]u8 {
    const out = try alloc.alloc(u8, len);
    var prng = std.Random.DefaultPrng.init(7);
    prng.random().bytes(out);
    var i: usize = 0;
    while (i + 24 <= out.len) : (i += 64) @memset(out[i .. i + 24], 0);
    return out;
}

/// The `SnapshotObject` a leader would propose for `object`, packed
/// from `raw`.
fn snapOf(
    object: snapshot.Object,
    raw: []const u8,
) zdb.buffers.SnapshotObjectStruct {
    return .{
        .id = 1,
        .size = object.bytes.items.len,
        .raw_size = raw.len,
        .hash = object.hash,
    };
}

test "snapshot: an object of several blocks gives its manifest back" {
    const alloc = testing.allocator;
    // Three and a bit blocks, and one short of a block: the last block
    // is short either way.
    for ([_]usize{
        3 * snapshot.block_size + 1000,
        snapshot.block_size - 1,
        1,
    }) |len| {
        const raw = try manifestLike(alloc, len);
        defer alloc.free(raw);

        var object = try snapshot.pack(alloc, raw);
        defer object.deinit(alloc);
        // As `Store.put` takes it.
        try testing.expect(object.bytes.items.len % vfs.page_size == 0);
        try testing.expect(object.bytes.items.len > 0);

        const got = try snapshot.unpack(
            alloc,
            object.bytes.items,
            snapOf(object, raw),
        );
        defer alloc.free(got);
        try testing.expectEqualSlices(u8, raw, got);
    }
}

test "snapshot: a flipped byte in any block fails its hash" {
    const alloc = testing.allocator;
    const raw = try manifestLike(alloc, 2 * snapshot.block_size + 10);
    defer alloc.free(raw);
    var object = try snapshot.pack(alloc, raw);
    defer object.deinit(alloc);

    const snap = snapOf(object, raw);

    // In the first block's lz4 bytes, and one halfway through.
    for ([_]usize{ 20, object.bytes.items.len / 2 }) |at| {
        object.bytes.items[at] ^= 0x40;
        defer object.bytes.items[at] ^= 0x40;
        try testing.expectError(
            error.CorruptSnapshot,
            snapshot.unpack(alloc, object.bytes.items, snap),
        );
    }
    // A manifest longer than the object holds runs into the padding.
    var longer = snap;
    longer.raw_size += 1;
    try testing.expectError(
        error.CorruptSnapshot,
        snapshot.unpack(alloc, object.bytes.items, longer),
    );
    // An object cut short, as a truncated file reads back.
    const short = object.bytes.items[0 .. object.bytes.items.len - 1];
    try testing.expectError(
        error.CorruptSnapshot,
        snapshot.unpack(alloc, short, snap),
    );
}

test "snapshot: blocks that each pass, out of order, fail the manifest's hash" {
    const alloc = testing.allocator;
    const bs = snapshot.block_size;
    const raw = try manifestLike(alloc, 2 * bs);
    defer alloc.free(raw);
    var object = try snapshot.pack(alloc, raw);
    defer object.deinit(alloc);
    const snap = snapOf(object, raw);

    // The two frames, swapped: each still a whole frame that passes
    // its own hash, and the lengths add up as before.
    const first = (try zbuf.Frame.readPrefix(object.bytes.items)).frameLen();
    const second = (try zbuf.Frame.readPrefix(object.bytes.items[first..]))
        .frameLen();
    const swapped = try alloc.alloc(u8, object.bytes.items.len);
    defer alloc.free(swapped);
    @memset(swapped, 0);
    @memcpy(swapped[0..second], object.bytes.items[first..][0..second]);
    @memcpy(swapped[second..][0..first], object.bytes.items[0..first]);
    _ = try zbuf.Frame.read(swapped);
    _ = try zbuf.Frame.read(swapped[second..]);

    try testing.expectError(
        error.CorruptSnapshot,
        snapshot.unpack(alloc, swapped, snap),
    );

    // Only the hash told: given the swapped manifest's own, the same
    // object unpacks to it.
    const reordered =
        try std.mem.concat(alloc, u8, &.{ raw[bs..], raw[0..bs] });
    defer alloc.free(reordered);
    var theirs = snap;
    theirs.hash = std.hash.XxHash3.hash(0, reordered);
    const got = try snapshot.unpack(alloc, swapped, theirs);
    defer alloc.free(got);
    try testing.expectEqualSlices(u8, reordered, got);
}

test "snapshot: put and read back through a file store" {
    const alloc = testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    var store = try vfs.Store.initFile(alloc, tmp.path, vfs.g());
    defer store.deinit();

    const raw = try manifestLike(alloc, 5 * snapshot.block_size);
    defer alloc.free(raw);
    var object = try snapshot.pack(alloc, raw);
    defer object.deinit(alloc);
    // Smaller than the manifest: the blocks compressed.
    try testing.expect(object.bytes.items.len < raw.len);

    var snap = snapOf(object, raw);
    snap.id = snapshot.newId(snap.id);
    const name = try snapshot.objectName(alloc, snap.id);
    defer alloc.free(name);
    try store.put(name, object.bytes.items);
    try store.sync();

    // The record goes through raft as the snapshot's data.
    const data = try zdb.buffers.marshalRoot(alloc, snap);
    defer alloc.free(data);
    const reader = try zdb.buffers.SnapshotObjectReader.parseRoot(data);
    const parsed = try reader.toStructValue(alloc);
    try testing.expectEqual(snap, parsed);

    const got = try snapshot.read(alloc, &store, parsed);
    defer alloc.free(got);
    try testing.expectEqualSlices(u8, raw, got);
}

test "snapshot: object names parse back, and nothing else does" {
    const alloc = testing.allocator;
    const name = try snapshot.objectName(alloc, 0xabc);
    defer alloc.free(name);
    try testing.expectEqualStrings("0000000000000abc.snap", name);
    try testing.expectEqual(@as(?u64, 0xabc), snapshot.parseName(name));

    const sst = try zdb.sstName(alloc, 0xabc);
    defer alloc.free(sst);
    try testing.expectEqual(@as(?u64, null), snapshot.parseName(sst));
    try testing.expectEqual(@as(?u64, null), snapshot.parseName("x.snap"));
    try testing.expectEqual(
        @as(?u64, null),
        snapshot.parseName("000000000000zzzz.snap"),
    );
}
