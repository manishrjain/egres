const std = @import("std");
const gen = @import("buffers_gen");
const zbuf = @import("zbuf").zbuf;

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
        .lifecycle = .bootstrapping,
        .is_pinned = true,
    });
    const leaves = [_]u32{ l1_off, l2_res.offset };
    try n1.setCells(&leaves);
    try n1.setRowCell(l1_off);
    const n1_u32_values = [_]u32{ 11, 22, 33 };
    const n1_u64_values = [_]u64{ 100, 200 };
    try n1.setU32Values(&n1_u32_values);
    try n1.setU64Values(&n1_u64_values);
    try n1.flagsSetBit(33);
    const n1_res = try n1.finish();
    const n1_off = n1_res.offset;

    var n2 = try gen.RowWriter.initWithBuf(alloc, &buf);
    try n2.setName("n2");
    try n2.setValue(2000);
    const no_leaves = [_]u32{};
    try n2.setCells(&no_leaves);
    const n2_no_u32_values = [_]u32{};
    const n2_u64_values = [_]u64{ 9000 };
    try n2.setU32Values(&n2_no_u32_values);
    try n2.setU64Values(&n2_u64_values);
    try n2.setRowCell(gen.NullOffset);
    try n2.flagsSet(0x8);
    try n2.flagsClearBit(3);
    try n2.setLifecycle(.retiring);
    try n2.setIsDeleted(true);
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
    root_res.ownership = .owned;
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
    try std.testing.expectEqual(gen.LifecycleEnum.bootstrapping, n0.lifecycle());
    try std.testing.expect(n0.isPinned());
    try std.testing.expect(!n0.isDeleted());
    try std.testing.expect(n0.flagsHasBit(33));
    try std.testing.expectEqual(@as(u32, 3), n0.u32ValuesLen());
    try std.testing.expectEqual(@as(u32, 2), n0.u64ValuesLen());
    try std.testing.expectEqual(@as(u32, 11), try n0.u32ValuesAt(0));
    try std.testing.expectEqual(@as(u32, 22), try n0.u32ValuesAt(1));
    try std.testing.expectEqual(@as(u32, 33), try n0.u32ValuesAt(2));
    try std.testing.expectEqual(@as(u64, 100), try n0.u64ValuesAt(0));
    try std.testing.expectEqual(@as(u64, 200), try n0.u64ValuesAt(1));

    const n1 = try root.rowsAt(1);
    try std.testing.expectEqualSlices(u8, "n2", n1.name());
    try std.testing.expectEqual(@as(u64, 2000), n1.value());
    try std.testing.expectEqual(@as(u32, 0), n1.cellsLen());
    try std.testing.expectEqual(gen.LifecycleEnum.retiring, n1.lifecycle());
    try std.testing.expect(n1.isDeleted());
    try std.testing.expect(!n1.isPinned());
    try std.testing.expect(!n1.flagsHasBit(3));
    try std.testing.expect(!n1.flagsHasBit(33));
    try std.testing.expectEqual(@as(u32, 0), n1.u32ValuesLen());
    try std.testing.expectEqual(@as(u32, 1), n1.u64ValuesLen());
    try std.testing.expectEqual(@as(u64, 9000), try n1.u64ValuesAt(0));

    try root.verify();
}

test "bitset direct operations" {
    var buf: std.ArrayList(u8) = .{};
    defer buf.deinit(std.testing.allocator);

    var node = try gen.RowWriter.initWithBuf(std.testing.allocator, &buf);
    try node.flagsSetBit(0);
    try node.flagsSetBit(63);
    try std.testing.expect(try node.flagsHasBit(0));
    try std.testing.expect(try node.flagsHasBit(63));
    try node.flagsClearBit(0);
    try std.testing.expect(!(try node.flagsHasBit(0)));
    try node.flagsSet(0);

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
    try std.testing.expect(!view.flagsHasBit(0));
    try std.testing.expect(!view.flagsHasBit(63));
}

test "enum field roundtrips and is independent of flag bits" {
    var buf: std.ArrayList(u8) = .{};
    defer buf.deinit(std.testing.allocator);

    // Set the enum to .draining and a couple of independent flag bits.
    var node = try gen.RowWriter.initWithBuf(std.testing.allocator, &buf);
    try node.setLifecycle(.draining);
    try node.flagsSetBit(0); // is_pinned
    try node.flagsSetBit(1); // is_deleted

    // Mutating flags must not affect the enum slot, and vice versa.
    try node.setLifecycle(.retiring);
    try std.testing.expect(try node.flagsHasBit(0));
    try std.testing.expect(try node.flagsHasBit(1));

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
    try std.testing.expectEqual(gen.LifecycleEnum.retiring, view.lifecycle());
    try std.testing.expect(view.isPinned());
    try std.testing.expect(view.isDeleted());
}

test "enum unknown tag falls into catch-all" {
    // Forward-compat: a wire byte whose value isn't a named variant must still
    // produce a well-formed enum value (the `_,` catch-all variant). We
    // simulate a "future" schema by writing an unknown tag through the same
    // public API — the enum's catch-all variant lets us round-trip arbitrary
    // u8 values without losing them.
    var buf: std.ArrayList(u8) = .{};
    defer buf.deinit(std.testing.allocator);

    var node = try gen.RowWriter.initWithBuf(std.testing.allocator, &buf);
    try node.setLifecycle(@enumFromInt(99));
    const res = try node.finish();

    const reader = try gen.RowReader.init(buf.items, res.offset);
    const got = reader.lifecycle();
    try std.testing.expectEqual(@as(u8, 99), @intFromEnum(got));
    // Switch on it: the catch-all `_` arm catches unknown tags safely.
    const named: bool = switch (got) {
        .active, .draining, .bootstrapping, .retiring => true,
        _ => false,
    };
    try std.testing.expect(!named);
}

test "array toSliceAlloc for u16/u32/u64" {
    const alloc = std.testing.allocator;

    {
        var buf: std.ArrayList(u8) = .{};
        defer buf.deinit(alloc);
        var w = try gen.U16ArrayOnlyWriter.initWithBuf(alloc, &buf);
        const vals = [_]u16{ 1, 2, 65535 };
        try w.setValues(&vals);
        const res = try w.finish();
        const r = try gen.U16ArrayOnlyReader.init(buf.items, res.offset);
        const got = try r.valuesToSliceAlloc(alloc);
        defer alloc.free(got);
        try std.testing.expectEqualSlices(u16, &vals, got);
    }

    {
        var buf: std.ArrayList(u8) = .{};
        defer buf.deinit(alloc);
        var w = try gen.U32ArrayOnlyWriter.initWithBuf(alloc, &buf);
        const vals = [_]u32{ 10, 20, 30 };
        try w.setValues(&vals);
        const res = try w.finish();
        const r = try gen.U32ArrayOnlyReader.init(buf.items, res.offset);
        const got = try r.valuesToSliceAlloc(alloc);
        defer alloc.free(got);
        try std.testing.expectEqualSlices(u32, &vals, got);
    }

    {
        var buf: std.ArrayList(u8) = .{};
        defer buf.deinit(alloc);
        var w = try gen.U64ArrayOnlyWriter.initWithBuf(alloc, &buf);
        const vals = [_]u64{ 100, 200, 300 };
        try w.setValues(&vals);
        const res = try w.finish();
        const r = try gen.U64ArrayOnlyReader.init(buf.items, res.offset);
        const got = try r.valuesToSliceAlloc(alloc);
        defer alloc.free(got);
        try std.testing.expectEqualSlices(u64, &vals, got);
    }
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

test "empty u32 array overhead is header-only" {
    const empty_u32 = [_]u32{};
    {
        var writer = try gen.U32ArrayOnlyWriter.init(std.testing.allocator);
        defer writer.deinit();
        try writer.setValues(&empty_u32);

        var without_tail = try writer.finish();
        defer without_tail.deinit();
        const bytes_without_tail = without_tail.bytes orelse return error.Invalid;
        try std.testing.expectEqual(@as(usize, gen.U32ArrayOnlyWriter.SIZE_BYTES), bytes_without_tail.len);
    }
    {
        var writer = try gen.U32ArrayOnlyWriter.init(std.testing.allocator);
        defer writer.deinit();
        try writer.setValues(&empty_u32);

        var with_tail = try writer.finishWithTail();
        defer with_tail.deinit();
        const bytes_with_tail = with_tail.bytes orelse return error.Invalid;
        const expected_with_tail =
            @as(usize, gen.U32ArrayOnlyWriter.SIZE_BYTES) +
            @as(usize, @import("zbuf").zbuf.message_tail_size);
        try std.testing.expectEqual(expected_with_tail, bytes_with_tail.len);
    }
}

test "u32 array roundtrip and bounds" {
    var writer = try gen.U32ArrayOnlyWriter.init(std.testing.allocator);
    defer writer.deinit();

    const values = [_]u32{ 7, 42, 99, 12345 };
    try writer.setValues(&values);

    var out = try writer.finishWithTail();
    defer out.deinit();
    const reader = try out.initReader();

    try std.testing.expectEqual(@as(u32, values.len), reader.valuesLen());
    try std.testing.expectEqual(@as(u32, 7), try reader.valuesAt(0));
    try std.testing.expectEqual(@as(u32, 42), try reader.valuesAt(1));
    try std.testing.expectEqual(@as(u32, 99), try reader.valuesAt(2));
    try std.testing.expectEqual(@as(u32, 12345), try reader.valuesAt(3));
    try std.testing.expectError(zbuf.Error.OutOfBounds, reader.valuesAt(4));
}

test "u64 array roundtrip and bounds" {
    var writer = try gen.U64ArrayOnlyWriter.init(std.testing.allocator);
    defer writer.deinit();

    const values = [_]u64{
        0,
        1,
        0x1122_3344_5566_7788,
        std.math.maxInt(u64),
    };
    try writer.setValues(&values);

    var out = try writer.finishWithTail();
    defer out.deinit();
    const reader = try out.initReader();

    try std.testing.expectEqual(@as(u32, values.len), reader.valuesLen());
    try std.testing.expectEqual(@as(u64, 0), try reader.valuesAt(0));
    try std.testing.expectEqual(@as(u64, 1), try reader.valuesAt(1));
    try std.testing.expectEqual(@as(u64, 0x1122_3344_5566_7788), try reader.valuesAt(2));
    try std.testing.expectEqual(std.math.maxInt(u64), try reader.valuesAt(3));
    try std.testing.expectError(zbuf.Error.OutOfBounds, reader.valuesAt(4));
}

test "clone deep copies nested struct" {
    const alloc = std.testing.allocator;

    // Build a table with nested rows, cells, optional table, bytes, scalar arrays.
    var buf = try buildTable(alloc, true);
    defer buf.deinit();
    const bytes = buf.bytes orelse return error.Invalid;

    const msg = try gen.parseMessageWithTail(bytes);
    const root = switch (msg) {
        .Table => |v| v,
        else => return error.Invalid,
    };
    const original = try gen.TableStruct.fromReader(alloc, root);
    defer original.destroy(alloc);

    // Clone — returns heap-allocated pointer, cleaned up via destroy.
    const cloned = try original.clone(alloc);
    defer cloned.destroy(alloc);

    // Scalars.
    try std.testing.expectEqual(original.count, cloned.count);
    try std.testing.expectEqualSlices(u8, original.payload, cloned.payload);

    // Bytes are independent allocations.
    try std.testing.expect(original.payload.ptr != cloned.payload.ptr);

    // Nested rows array.
    try std.testing.expectEqual(original.rows.len, cloned.rows.len);
    try std.testing.expect(original.rows.ptr != cloned.rows.ptr);

    // Row 0: scalars, bytes, nested cell arrays, optional table, scalar arrays.
    const r0_orig = original.rows[0];
    const r0_clone = cloned.rows[0];
    try std.testing.expectEqual(r0_orig.value, r0_clone.value);
    try std.testing.expectEqual(r0_orig.lifecycle, r0_clone.lifecycle);
    try std.testing.expect(r0_orig.is_pinned == r0_clone.is_pinned);
    try std.testing.expectEqualSlices(u8, r0_orig.name, r0_clone.name);
    try std.testing.expect(r0_orig.name.ptr != r0_clone.name.ptr);

    // Cells array — deep copied.
    try std.testing.expectEqual(r0_orig.cells.len, r0_clone.cells.len);
    try std.testing.expect(r0_orig.cells.ptr != r0_clone.cells.ptr);
    try std.testing.expectEqual(r0_orig.cells[0].tag, r0_clone.cells[0].tag);
    try std.testing.expectEqualSlices(u8, r0_orig.cells[0].data, r0_clone.cells[0].data);
    try std.testing.expect(r0_orig.cells[0].data.ptr != r0_clone.cells[0].data.ptr);
    try std.testing.expect(r0_orig.cells[0].cell_hot == r0_clone.cells[0].cell_hot);
    try std.testing.expectEqual(r0_orig.cells[1].tag, r0_clone.cells[1].tag);

    // Optional nested table (row_cell).
    try std.testing.expect(r0_clone.row_cell != null);
    try std.testing.expectEqual(r0_orig.row_cell.?.tag, r0_clone.row_cell.?.tag);
    try std.testing.expectEqualSlices(u8, r0_orig.row_cell.?.data, r0_clone.row_cell.?.data);
    try std.testing.expect(r0_orig.row_cell.?.data.ptr != r0_clone.row_cell.?.data.ptr);

    // Scalar arrays — u32 and u64.
    try std.testing.expectEqualSlices(u32, r0_orig.u32_values, r0_clone.u32_values);
    try std.testing.expect(r0_orig.u32_values.ptr != r0_clone.u32_values.ptr);
    try std.testing.expectEqualSlices(u64, r0_orig.u64_values, r0_clone.u64_values);
    try std.testing.expect(r0_orig.u64_values.ptr != r0_clone.u64_values.ptr);

    // Row 1: null row_cell, empty cells, b64 + enum fields.
    const r1_orig = original.rows[1];
    const r1_clone = cloned.rows[1];
    try std.testing.expectEqual(r1_orig.value, r1_clone.value);
    try std.testing.expectEqual(r1_orig.lifecycle, r1_clone.lifecycle);
    try std.testing.expect(r1_orig.is_deleted == r1_clone.is_deleted);
    try std.testing.expectEqual(r1_orig.cells.len, r1_clone.cells.len);
    try std.testing.expect(r1_clone.row_cell == null);
    try std.testing.expectEqual(r1_orig.flags, r1_clone.flags);

    // Optional nested table on the root.
    try std.testing.expect(cloned.table_row != null);
    try std.testing.expectEqualSlices(u8, original.table_row.?.name, cloned.table_row.?.name);
    try std.testing.expect(original.table_row.?.name.ptr != cloned.table_row.?.name.ptr);
}

test "clone of struct with null optional table" {
    const alloc = std.testing.allocator;

    var buf = try buildTable(alloc, false);
    defer buf.deinit();
    const bytes = buf.bytes orelse return error.Invalid;

    const msg = try gen.parseMessageWithTail(bytes);
    const root = switch (msg) {
        .Table => |v| v,
        else => return error.Invalid,
    };
    const original = try gen.TableStruct.fromReader(alloc, root);
    defer original.destroy(alloc);

    try std.testing.expect(original.table_row == null);

    const cloned = try original.clone(alloc);
    defer cloned.destroy(alloc);

    try std.testing.expect(cloned.table_row == null);
    try std.testing.expectEqual(original.count, cloned.count);
    try std.testing.expectEqualSlices(u8, original.payload, cloned.payload);
}

test "clone of simple scalar-only struct" {
    const alloc = std.testing.allocator;

    var original = gen.CellStruct{
        .tag = 42,
        .cell_hot = true,
        .cell_flags = 0xFF,
    };

    const cloned = try original.clone(alloc);
    defer cloned.destroy(alloc);

    try std.testing.expectEqual(@as(u32, 42), cloned.tag);
    try std.testing.expect(cloned.cell_hot);
    try std.testing.expectEqual(@as(u64, 0xFF), cloned.cell_flags);
    try std.testing.expectEqualSlices(u8, &.{}, cloned.data);
}

test "clone roundtrip matches marshal" {
    const alloc = std.testing.allocator;

    // Build, read, convert to struct.
    var buf = try buildTable(alloc, true);
    defer buf.deinit();
    const bytes = buf.bytes orelse return error.Invalid;

    const msg = try gen.parseMessageWithTail(bytes);
    const root = switch (msg) {
        .Table => |v| v,
        else => return error.Invalid,
    };
    const original = try gen.TableStruct.fromReader(alloc, root);
    defer original.destroy(alloc);

    // Clone, then marshal both and compare.
    const cloned = try original.clone(alloc);
    defer cloned.destroy(alloc);

    var out_orig: std.ArrayList(u8) = .{};
    defer out_orig.deinit(alloc);
    const off_orig = try original.marshalTo(alloc, &out_orig);

    var out_clone: std.ArrayList(u8) = .{};
    defer out_clone.deinit(alloc);
    const off_clone = try cloned.marshalTo(alloc, &out_clone);

    // Both should produce identical wire bytes.
    try std.testing.expectEqual(off_orig, off_clone);
    try std.testing.expectEqualSlices(u8, out_orig.items, out_clone.items);
}
