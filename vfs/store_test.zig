// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const utils = @import("utils");
const vfs = @import("vfs");
const fiber = @import("zfiber");

test "store: file backend put/get/exists/delete round-trip" {
    const alloc = testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const root = tmp.path;
    const depot = try std.fmt.allocPrint(alloc, "{s}/depot", .{root});
    defer alloc.free(depot);

    var store = try vfs.Store.initFile(alloc, depot, vfs.g());
    defer store.deinit();

    // Missing object: exists false, open fails.
    try testing.expect(!try store.exists("0001_000000000001.sst"));
    try testing.expectError(
        error.FileNotFound,
        store.open("0001_000000000001.sst", vfs.page_size, .{}),
    );

    // put writes O_DIRECT: page-aligned, page-multiple buffers only
    // (SST buffers are both by construction).
    const payload = try alloc.alignedAlloc(u8, vfs.page_alignment, vfs.page_size);
    defer alloc.free(payload);
    for (payload, 0..) |*b, i| b.* = @truncate(i *% 31);

    try store.put("0001_000000000001.sst", payload);
    try testing.expect(try store.exists("0001_000000000001.sst"));

    // Whole-object and ranged reads through the handle.
    var obj = try store.open("0001_000000000001.sst", vfs.page_size, .{});
    defer obj.close();
    try testing.expectEqual(@as(u64, vfs.page_size), obj.size());
    const got = try obj.readAligned(alloc, 0, vfs.page_size);
    defer got.free();
    try testing.expectEqualSlices(u8, payload, got.data);
    const ar = try obj.readAligned(alloc, 100, 32);
    defer ar.free();
    try testing.expectEqualSlices(u8, payload[100..132], ar.data);

    // Overwrite is idempotent (write-once contract: same bytes).
    try store.put("0001_000000000001.sst", payload);
    const again = try obj.readAligned(alloc, 0, vfs.page_size);
    defer again.free();
    try testing.expectEqualSlices(u8, payload, again.data);

    // Unaligned buffers (address or length) are rejected.
    try testing.expectError(error.UnalignedPut, store.put("bad.sst", payload[1..]));
    try testing.expectError(error.UnalignedPut, store.put("bad.sst", payload[0 .. vfs.page_size - 1]));

    // Delete, then gone.
    try store.delete("0001_000000000001.sst");
    try testing.expect(!try store.exists("0001_000000000001.sst"));
}

test "store: file probeRead times an O_DIRECT ranged read" {
    // The probe times a real read; virtual time does not move across
    // one, so the timings are 0 under the simulation.
    if (fiber.isSim()) return error.SkipZigTest;
    const alloc = testing.allocator;
    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();
    const root = tmp.path;
    const depot = try std.fmt.allocPrint(alloc, "{s}/depot", .{root});
    defer alloc.free(depot);

    var store = try vfs.Store.initFile(alloc, depot, vfs.g());
    defer store.deinit();

    // A small probe-patterned object (position-dependent bytes from 0);
    // page-aligned because put writes O_DIRECT.
    const data = try alloc.alignedAlloc(u8, vfs.page_alignment, 64 << 10);
    defer alloc.free(data);
    for (data, 0..) |*b, i| b.* = vfs.lookahead.probeByte(i);
    try store.put(vfs.lookahead.probe_object, data);

    // Unaligned request: offset rounds down to a page boundary, the
    // window keeps its length; both timings are real (the fixed-cost
    // page and the ranged read are separate preads, so no ordering
    // between them is guaranteed).
    const p = try store.file.probeRead(5000, 8192);
    try testing.expectEqual(@as(u64, 8192), p.size);
    try testing.expect(p.ttfb_ns > 0 and p.total_ns > 0);

    // A probe object holding the wrong bytes must fail verification.
    @memset(data, 0);
    try store.put(vfs.lookahead.probe_object, data);
    try testing.expectError(error.BadProbeData, store.file.probeRead(0, 4096));
}
