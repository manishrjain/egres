const schema = @import("schema");

pub const BlockOffset = schema.Table(.{
    .{ .id = 1, .name = "key", .ty = schema.Bytes },
    .{ .id = 2, .name = "offset", .ty = schema.U32 },
    .{ .id = 3, .name = "len", .ty = schema.U32 },
});

pub const TableIndex = schema.Table(.{
    .{ .id = 1, .name = "offsets", .ty = schema.Array(BlockOffset) },
    .{ .id = 2, .name = "bloom_filter", .ty = schema.Bytes },
    .{ .id = 3, .name = "max_version", .ty = schema.U64 },
    .{ .id = 4, .name = "key_count", .ty = schema.U32 },
    .{ .id = 5, .name = "uncompressed_size", .ty = schema.U32 },
    .{ .id = 6, .name = "on_disk_size", .ty = schema.U32 },
    .{ .id = 7, .name = "stale_data_size", .ty = schema.U32 },
});

pub const tables = .{
    .{ .name = "BlockOffset", .ty = BlockOffset, .id = 1 },
    .{ .name = "TableIndex", .ty = TableIndex, .id = 2 },
};
