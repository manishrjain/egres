const schema = @import("schema");

// Port of badger/fb/flatbuffer.fbs:
// - table BlockOffset { key:[ubyte]; offset:uint; len:uint; }
// - table TableIndex { offsets:[BlockOffset]; bloom_filter:[ubyte]; max_version:uint64; ... }
pub const BlockIndex = schema.Table(.{
    .{ .id = 1, .name = "key", .ty = schema.Bytes },
    .{ .id = 2, .name = "offset", .ty = schema.U32 },
    .{ .id = 3, .name = "len", .ty = schema.U32 },
});

pub const BinaryFuseFilter = schema.Table(.{
    .{ .id = 1, .name = "seed", .ty = schema.U64 },
    .{ .id = 2, .name = "segment_length", .ty = schema.U32 },
    .{ .id = 3, .name = "segment_length_mask", .ty = schema.U32 },
    .{ .id = 4, .name = "segment_count", .ty = schema.U32 },
    .{ .id = 5, .name = "segment_count_length", .ty = schema.U32 },
    .{ .id = 6, .name = "fingerprints", .ty = schema.Array(schema.U16) },
});

pub const TableIndex = schema.Table(.{
    .{ .id = 1, .name = "max_version", .ty = schema.U64 },
    .{ .id = 2, .name = "key_count", .ty = schema.U32 },
    .{ .id = 3, .name = "uncompressed_size", .ty = schema.U32 },
    .{ .id = 4, .name = "compressed_size", .ty = schema.U32 },
    .{ .id = 5, .name = "stale_data_size", .ty = schema.U32 },

    .{ .id = 6, .name = "compression", .ty = schema.B64 },
    .{ .name = "is_none", .ty = schema.Bool(.{ .field_id = 6, .bit = 0 }) },
    .{ .name = "is_lz4", .ty = schema.Bool(.{ .field_id = 6, .bit = 1 }) },
    .{ .name = "is_zstd", .ty = schema.Bool(.{ .field_id = 6, .bit = 2 }) },

    .{ .id = 7, .name = "last_key", .ty = schema.Bytes },
    .{ .id = 8, .name = "blocks", .ty = schema.Array(BlockIndex) },
    .{ .id = 9, .name = "binary_fuse", .ty = BinaryFuseFilter },
});

// TreeChange is used to create a new tree, delete a tree or update a tree's range.
pub const TreeChange = schema.Table(.{
    .{ .id = 1, .name = "op", .ty = schema.U32 }, // 0=CREATE, 1=DELETE, 2=UPDATE
    .{ .id = 2, .name = "namespace_id", .ty = schema.U32 },
    .{ .id = 3, .name = "tree_id", .ty = schema.U64 },
    .{ .id = 4, .name = "left_key", .ty = schema.Bytes },
    .{ .id = 5, .name = "right_key", .ty = schema.Bytes },
});

// Table changes tell us which tree the table belongs to, and which level.
// Note that a table can belong to multiple trees. Table deletion should
// only happen when a table's ref count reaches 0.
pub const TableChange = schema.Table(.{
    .{ .id = 1, .name = "op", .ty = schema.U32 }, // 0=CREATE, 1=DELETE
    .{ .id = 2, .name = "level", .ty = schema.U32 }, // 0 for DELETE
    .{ .id = 3, .name = "table_id", .ty = schema.U64 },
    .{ .id = 4, .name = "tree_id", .ty = schema.U64 },
    .{ .id = 5, .name = "checksum", .ty = schema.U32 }, // CRC32 of SST file (CREATE only)
    .{ .id = 6, .name = "size", .ty = schema.U64 }, // file size in bytes (CREATE only)
});

pub const ManifestChange = schema.Table(.{
    .{ .id = 1, .name = "tree_changes", .ty = schema.Array(TreeChange) },
    .{ .id = 2, .name = "table_changes", .ty = schema.Array(TableChange) },
    .{ .id = 3, .name = "cmd", .ty = schema.U32 }, // 1=new_memtable, 2=flush, 3=compaction, 4=tree_split, 5=ns_create
    .{ .id = 4, .name = "memtable_id", .ty = schema.U64 }, // for flush: which memtable was flushed
});

/// A single key-value tuple. Carried inside `Mutations` which groups them
/// by namespace — so the per-tuple envelope only holds the bytes themselves.
pub const KeyValue = schema.Table(.{
    .{ .id = 1, .name = "key", .ty = schema.Bytes },
    .{ .id = 2, .name = "value", .ty = schema.Bytes },
});

/// One or more key-value writes proposed through raft as a single entry.
/// The enclosing `LogEntry.timestamp` is the commit HLC for the whole batch
/// (all kvs share it). Namespace is at the Mutations level so every kv in a
/// proposal lands in the same tree — keeps routing + apply simple.
pub const Mutations = schema.Table(.{
    .{ .id = 1, .name = "namespace", .ty = schema.U32 },
    .{ .id = 2, .name = "kvs", .ty = schema.Array(KeyValue) },
});

// zbuf tail stores table_id as u16, so use a module namespace in high bits and
// OR local ids into low bits.
const ZDB_TABLE_NS: u16 = 0xB600;

fn tableId(comptime local_id: u16) u16 {
    return ZDB_TABLE_NS | local_id;
}

pub const tables = .{
    .{ .name = "BlockIndex", .ty = BlockIndex, .id = tableId(1) },
    .{ .name = "TableIndex", .ty = TableIndex, .id = tableId(2) },
    .{ .name = "BinaryFuseFilter", .ty = BinaryFuseFilter, .id = tableId(3) },

    .{ .name = "TreeChange", .ty = TreeChange, .id = tableId(4) },
    .{ .name = "TableChange", .ty = TableChange, .id = tableId(5) },
    .{ .name = "ManifestChange", .ty = ManifestChange, .id = tableId(6) },
    .{ .name = "KeyValue", .ty = KeyValue, .id = tableId(7) },
    .{ .name = "Mutations", .ty = Mutations, .id = tableId(8) },
};
