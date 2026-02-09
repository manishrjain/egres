const std = @import("std");
const gen = @import("buffers_gen");

fn buildRoot(alloc: std.mem.Allocator, set_root_node: bool) !std.ArrayList(u8) {
    var buf: std.ArrayList(u8) = .{};

    var l1 = try gen.LeafBuilder.start(alloc, &buf);
    try l1.setTag(1);
    try l1.setData("l1");
    const l1_off = l1.finish();

    var l2 = try gen.LeafBuilder.start(alloc, &buf);
    try l2.setTag(2);
    try l2.setData("l2");
    const l2_off = l2.finish();

    var n1 = try gen.NodeBuilder.start(alloc, &buf);
    try n1.setName("n1");
    try n1.setValue(1000);
    const leaves = [_]u32{ l1_off, l2_off };
    try n1.setLeaves(&leaves);
    try n1.setNodeLeaf(l1_off);
    const n1_off = n1.finish();

    var n2 = try gen.NodeBuilder.start(alloc, &buf);
    try n2.setName("n2");
    try n2.setValue(2000);
    const no_leaves = [_]u32{};
    try n2.setLeaves(&no_leaves);
    try n2.setNodeLeaf(@import("zbuf").zbuf.NullOffset);
    const n2_off = n2.finish();

    var root = try gen.RootBuilder.start(alloc, &buf);
    const nodes = [_]u32{ n1_off, n2_off };
    try root.setNodes(&nodes);
    try root.setPayload("payload");
    try root.setCount(2);
    if (set_root_node) {
        try root.setRootNode(n1_off);
    } else {
        try root.setRootNode(@import("zbuf").zbuf.NullOffset);
    }
    const root_off = root.finish();
    try gen.addTail(alloc, &buf, gen.RootView, root_off);

    return buf;
}

test "zbuf format roundtrip" {
    var buf = try buildRoot(std.testing.allocator, true);
    defer buf.deinit(std.testing.allocator);

    const msg = try gen.parseMessageFromTail(buf.items);
    const root = switch (msg) {
        .Root => |v| v,
        else => return error.Invalid,
    };
    try std.testing.expectEqual(@as(u32, 2), root.nodesLen());
    try std.testing.expectEqual(@as(u32, 2), root.count());
    try std.testing.expectEqualSlices(u8, "payload", root.payload());
    const root_node = root.rootNode() orelse return error.Invalid;
    try std.testing.expectEqualSlices(u8, "n1", root_node.name());
    try std.testing.expectEqual(@as(u64, 1000), root_node.value());
    const node_leaf = root_node.nodeLeaf() orelse return error.Invalid;
    try std.testing.expectEqual(@as(u32, 1), node_leaf.tag());
    try std.testing.expectEqualSlices(u8, "l1", node_leaf.data());

    const n0 = try root.nodesAt(0);
    try std.testing.expectEqualSlices(u8, "n1", n0.name());
    try std.testing.expectEqual(@as(u64, 1000), n0.value());
    try std.testing.expectEqual(@as(u32, 2), n0.leavesLen());

    const l0 = try n0.leavesAt(0);
    const l1 = try n0.leavesAt(1);
    try std.testing.expectEqual(@as(u32, 1), l0.tag());
    try std.testing.expectEqual(@as(u32, 2), l1.tag());
    try std.testing.expectEqualSlices(u8, "l1", l0.data());
    try std.testing.expectEqualSlices(u8, "l2", l1.data());

    const n1 = try root.nodesAt(1);
    try std.testing.expectEqualSlices(u8, "n2", n1.name());
    try std.testing.expectEqual(@as(u64, 2000), n1.value());
    try std.testing.expectEqual(@as(u32, 0), n1.leavesLen());

    try root.verify();
}

test "direct table null" {
    var buf = try buildRoot(std.testing.allocator, false);
    defer buf.deinit(std.testing.allocator);

    const msg = try gen.parseMessageFromTail(buf.items);
    const root = switch (msg) {
        .Root => |v| v,
        else => return error.Invalid,
    };
    try std.testing.expect(root.rootNode() == null);
}

// Simulate a V2 append-only field by increasing size_bytes.
test "size_bytes compatibility" {
    var buf = try buildRoot(std.testing.allocator, true);
    defer buf.deinit(std.testing.allocator);

    const tail = try @import("zbuf").zbuf.readMessageTail(buf.items);
    const base = tail.root_off;
    const v1_size: u16 = gen.RootView.SIZE_BYTES;
    const v2_size: u16 = v1_size + 4;

    const slice = buf.items[base + 2 .. base + 4];
    const ptr = @as(*[2]u8, @ptrCast(slice.ptr));
    std.mem.writeInt(u16, ptr, v2_size, .little);

    const msg2 = try gen.parseMessageFromTail(buf.items);
    const view = switch (msg2) {
        .Root => |v| v,
        else => return error.Invalid,
    };
    try std.testing.expectEqual(@as(u32, 2), view.nodesLen());
}
