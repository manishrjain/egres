const std = @import("std");
const gen = @import("generated.gen.zig");

fn buildTableIndex(alloc: std.mem.Allocator) !struct { buf: std.ArrayList(u8), root_off: u32 } {
    var buf: std.ArrayList(u8) = .{};

    var bo1 = try gen.BlockOffsetBuilder.start(alloc, &buf);
    try bo1.setKey("k1");
    try bo1.setOffset(10);
    try bo1.setLen(3);
    const bo1_off = bo1.finish();

    var bo2 = try gen.BlockOffsetBuilder.start(alloc, &buf);
    try bo2.setKey("k2");
    try bo2.setOffset(20);
    try bo2.setLen(4);
    const bo2_off = bo2.finish();

    var ti = try gen.TableIndexBuilder.start(alloc, &buf);
    try ti.setMaxVersion(42);
    try ti.setKeyCount(2);
    try ti.setUncompressedSize(100);
    try ti.setOnDiskSize(80);
    try ti.setStaleDataSize(10);
    try ti.setBloomFilter("bf");
    const offs = [_]u32{ bo1_off, bo2_off };
    try ti.setOffsets(&offs);
    const root_off = ti.finish();

    return .{ .buf = buf, .root_off = root_off };
}

test "TableIndex encode/decode zero-copy" {
    var built = try buildTableIndex(std.testing.allocator);
    defer built.buf.deinit(std.testing.allocator);

    const view = try gen.TableIndexView.init(built.buf.items, built.root_off);

    const bloom = view.bloomFilter();
    try std.testing.expectEqualSlices(u8, "bf", bloom);

    try std.testing.expectEqual(@as(u64, 42), view.maxVersion());
    try std.testing.expectEqual(@as(u32, 2), view.keyCount());
    try std.testing.expectEqual(@as(u32, 100), view.uncompressedSize());
    try std.testing.expectEqual(@as(u32, 80), view.onDiskSize());
    try std.testing.expectEqual(@as(u32, 10), view.staleDataSize());

    try std.testing.expectEqual(@as(u32, 2), view.offsetsLen());

    const o0 = try view.offsetsAt(0);
    const o1 = try view.offsetsAt(1);

    try std.testing.expectEqualSlices(u8, "k1", o0.key());
    try std.testing.expectEqual(@as(u32, 10), o0.offset());
    try std.testing.expectEqual(@as(u32, 3), o0.len());

    try std.testing.expectEqualSlices(u8, "k2", o1.key());
    try std.testing.expectEqual(@as(u32, 20), o1.offset());
    try std.testing.expectEqual(@as(u32, 4), o1.len());

    try o0.verify();
    try o1.verify();
    try view.verify();
}

// Simulate a V2 append-only field: bump size_bytes beyond V1 and ensure V1 reader ignores.
test "TableIndex V2 size_bytes compatibility" {
    var built = try buildTableIndex(std.testing.allocator);
    defer built.buf.deinit(std.testing.allocator);

    const base = built.root_off;
    const v1_size: u16 = gen.TableIndexView.SIZE_BYTES;
    const v2_size: u16 = v1_size + 4; // pretend new u32 appended

    // Patch size_bytes to V2 size (append-only). Buffer already large enough.
    const slice = built.buf.items[base + 2 .. base + 4];
    const ptr = @as(*[2]u8, @ptrCast(slice.ptr));
    std.mem.writeInt(u16, ptr, v2_size, .little);

    const view = try gen.TableIndexView.init(built.buf.items, built.root_off);
    try std.testing.expectEqual(@as(u32, 2), view.offsetsLen());
}
