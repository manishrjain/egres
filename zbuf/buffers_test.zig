const std = @import("std");
const gen = @import("buffers_gen");

fn buildTable(alloc: std.mem.Allocator, set_root_node: bool) !gen.TableBuffer {
    var buf: std.ArrayList(u8) = .{};
    defer buf.deinit(alloc);

    var l1 = try gen.CellWriter.initWithBuf(alloc, &buf);
    try l1.set(.{
        .tag = 1,
        .data = "l1",
        .cell_hot = true,
    });
    const l1_res = try l1.finish();
    try std.testing.expect(!l1_res.has_tail);
    try std.testing.expect(l1_res.bytes == null);
    const l1_off = l1_res.offset;

    var l2 = try gen.CellWriter.initWithBuf(alloc, &buf);
    try l2.set(.{
        .tag = 2,
        .data = "l2",
    });
    try l2.cellFlagsSetBit(3);
    try l2.cellFlagsClearBit(3);
    const l2_res = try l2.finish();

    var n1 = try gen.RowWriter.initWithBuf(alloc, &buf);
    try n1.setName("n1");
    try n1.set(.{
        .value = 1000,
        .is_active = true,
        .is_bootstrapping = true,
    });
    const leaves = [_]u32{ l1_off, l2_res.offset };
    try n1.setCells(&leaves);
    try n1.setRowCell(l1_off);
    try n1.statusSetBit(33);
    const n1_res = try n1.finish();
    const n1_off = n1_res.offset;

    var n2 = try gen.RowWriter.initWithBuf(alloc, &buf);
    try n2.setName("n2");
    try n2.setValue(2000);
    const no_leaves = [_]u32{};
    try n2.setCells(&no_leaves);
    try n2.setRowCell(gen.NullOffset);
    try n2.statusSet(0x8);
    try n2.statusClearBit(3);
    try n2.setIsDraining(true);
    try n2.lifecycleSetBit(41);
    const n2_res = try n2.finish();
    const n2_off = n2_res.offset;

    var root = try gen.TableWriter.initWithBuf(alloc, &buf);
    const nodes = [_]u32{ n1_off, n2_off };
    try root.setRows(&nodes);
    try root.setPayload("payload");
    try root.setCount(2);
    if (set_root_node) {
        try root.setTableRow(n1_off);
    } else {
        try root.setTableRow(gen.NullOffset);
    }
    var root_res = try root.finishWithTail();
    root_res.bytes = try buf.toOwnedSlice(alloc); // Own the bytes.
    return root_res;
}

test "zbuf format roundtrip" {
    var buf = try buildTable(std.testing.allocator, true);
    defer buf.deinit();
    const bytes = buf.bytes orelse return error.Invalid;

    const msg = try gen.parseMessageWithTail(bytes);
    const root = switch (msg) {
        .Table => |v| v,
        else => return error.Invalid,
    };
    try std.testing.expectEqual(@as(u32, 2), root.rowsLen());
    try std.testing.expectEqual(@as(u32, 2), root.count());
    try std.testing.expectEqualSlices(u8, "payload", root.payload());
    const root_node = root.tableRow() orelse return error.Invalid;
    try std.testing.expectEqualSlices(u8, "n1", root_node.name());
    try std.testing.expectEqual(@as(u64, 1000), root_node.value());
    const node_leaf = root_node.rowCell() orelse return error.Invalid;
    try std.testing.expectEqual(@as(u32, 1), node_leaf.tag());
    try std.testing.expectEqualSlices(u8, "l1", node_leaf.data());
    try std.testing.expect(node_leaf.cellHot());
    try std.testing.expect(node_leaf.cellFlagsHasBit(0));

    const n0 = try root.rowsAt(0);
    try std.testing.expectEqualSlices(u8, "n1", n0.name());
    try std.testing.expectEqual(@as(u64, 1000), n0.value());
    try std.testing.expectEqual(@as(u32, 2), n0.cellsLen());

    const l0 = try n0.cellsAt(0);
    const l1 = try n0.cellsAt(1);
    try std.testing.expectEqual(@as(u32, 1), l0.tag());
    try std.testing.expectEqual(@as(u32, 2), l1.tag());
    try std.testing.expectEqualSlices(u8, "l1", l0.data());
    try std.testing.expectEqualSlices(u8, "l2", l1.data());
    try std.testing.expect(l0.cellHot());
    try std.testing.expect(!l1.cellHot());
    try std.testing.expect(!l1.cellFlagsHasBit(3));
    try std.testing.expect(n0.isActive());
    try std.testing.expect(n0.statusHasBit(33));
    try std.testing.expect(n0.isBootstrapping());
    try std.testing.expect(n0.lifecycleHasBit(1));
    try std.testing.expect(!n0.lifecycleHasBit(41));

    const n1 = try root.rowsAt(1);
    try std.testing.expectEqualSlices(u8, "n2", n1.name());
    try std.testing.expectEqual(@as(u64, 2000), n1.value());
    try std.testing.expectEqual(@as(u32, 0), n1.cellsLen());
    try std.testing.expect(n1.isDraining());
    try std.testing.expect(n1.statusHasBit(33));
    try std.testing.expect(!n1.statusHasBit(3));
    try std.testing.expect(n1.isRetiring());
    try std.testing.expect(n1.lifecycleHasBit(41));
    try std.testing.expect(!n1.lifecycleHasBit(1));

    try root.verify();
}

test "bitset direct operations" {
    var buf: std.ArrayList(u8) = .{};
    defer buf.deinit(std.testing.allocator);

    var node = try gen.RowWriter.initWithBuf(std.testing.allocator, &buf);
    try node.statusSetBit(0);
    try node.statusSetBit(63);
    try std.testing.expect(try node.statusHasBit(0));
    try std.testing.expect(try node.statusHasBit(63));
    try node.statusClearBit(0);
    try std.testing.expect(!(try node.statusHasBit(0)));
    try node.statusSet(0);

    const off_res = try node.finish();
    const off = off_res.offset;
    var payload: std.ArrayList(u8) = .{};
    defer payload.deinit(std.testing.allocator);
    try payload.appendSlice(std.testing.allocator, buf.items);
    try gen.addTail(std.testing.allocator, &payload, gen.RowReader, off);

    const msg = try gen.parseMessageWithTail(payload.items);
    const view = switch (msg) {
        .Row => |v| v,
        else => return error.Invalid,
    };
    try std.testing.expect(!view.statusHasBit(0));
    try std.testing.expect(!view.statusHasBit(63));
}

test "multiple b64 backing fields stay independent" {
    var buf: std.ArrayList(u8) = .{};
    defer buf.deinit(std.testing.allocator);

    var node = try gen.RowWriter.initWithBuf(std.testing.allocator, &buf);
    try node.statusSetBit(10);
    try node.lifecycleSetBit(10);
    try node.lifecycleSetBit(41);
    try node.statusClearBit(10);

    try std.testing.expect(!(try node.statusHasBit(10)));
    try std.testing.expect(try node.lifecycleHasBit(10));
    try std.testing.expect(try node.lifecycleHasBit(41));

    const off_res = try node.finish();
    const off = off_res.offset;
    var payload: std.ArrayList(u8) = .{};
    defer payload.deinit(std.testing.allocator);
    try payload.appendSlice(std.testing.allocator, buf.items);
    try gen.addTail(std.testing.allocator, &payload, gen.RowReader, off);

    const msg = try gen.parseMessageWithTail(payload.items);
    const view = switch (msg) {
        .Row => |v| v,
        else => return error.Invalid,
    };
    try std.testing.expect(!view.statusHasBit(10));
    try std.testing.expect(view.lifecycleHasBit(10));
    try std.testing.expect(view.lifecycleHasBit(41));
    try std.testing.expect(!view.isActive());
    try std.testing.expect(view.isRetiring());
}

test "direct table null" {
    var buf = try buildTable(std.testing.allocator, false);
    defer buf.deinit();

    const root = try buf.initReader();
    try std.testing.expect(root.tableRow() == null);
}

// Simulate a V2 append-only field by increasing size_bytes.
test "size_bytes compatibility" {
    var buf = try buildTable(std.testing.allocator, true);
    defer buf.deinit();
    const bytes = buf.bytes orelse return error.Invalid;

    const tail = try @import("zbuf").zbuf.readMessageTail(bytes);
    const base = tail.offset;
    const v1_size: u16 = gen.TableReader.SIZE_BYTES;
    const v2_size: u16 = v1_size + 4;

    const slice = bytes[base + 2 .. base + 4];
    const ptr = @as(*[2]u8, @ptrCast(slice.ptr));
    std.mem.writeInt(u16, ptr, v2_size, .little);

    const msg2 = try gen.parseMessageWithTail(bytes);
    const view = switch (msg2) {
        .Table => |v| v,
        else => return error.Invalid,
    };
    try std.testing.expectEqual(@as(u32, 2), view.rowsLen());
}

test "owned writer buffer can init reader" {
    var writer = try gen.CellWriter.init(std.testing.allocator);
    defer writer.deinit();

    try writer.set(.{
        .tag = 9,
        .data = "xyz",
        .cell_hot = true,
    });
    var out = try writer.finish();
    defer out.deinit();
    try std.testing.expect(!out.has_tail);
    _ = out.bytes orelse return error.Invalid;

    const reader = try out.initReader();
    try std.testing.expectEqual(@as(u32, 9), reader.tag());
    try std.testing.expectEqualSlices(u8, "xyz", reader.data());
    try std.testing.expect(reader.cellHot());
}

test "finishWithTail returns tail-wrapped owned bytes" {
    var writer = try gen.TableWriter.init(std.testing.allocator);
    defer writer.deinit();

    try writer.setCount(7);
    var out = try writer.finishWithTail();
    defer out.deinit();
    try std.testing.expect(out.has_tail);
    _ = out.bytes orelse return error.Invalid;

    const table = try out.initReader();
    try std.testing.expectEqual(@as(u32, 7), table.count());
}
