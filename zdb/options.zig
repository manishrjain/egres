const std = @import("std");
const utils = @import("utils");

pub const Error = error{
    InvalidNumCompactors,
    InvalidMemtableSize,
    InvalidNumMemtables,
    InvalidBlockSize,
    InvalidMaxLevels,
    InvalidDirForInMemory,
};

pub const Options = struct {
    dir: []const u8 = "",
    in_memory: bool = false,
    read_only: bool = false,

    memtable_size: usize = 64 * 1024 * 1024,
    num_memtables: u32 = 15,

    block_size: usize = 64 * 1024,
    compression: utils.compression.Codec = .lz4,
    num_compactors: u8 = 2,

    // Level sizes
    num_level_zero_tables: u32 = 5,
    base_level_size: i64 = 800 * 1024 * 1024,
    level_size_multiplier: i64 = 5,
    max_levels: u8 = 4,

    // Table sizes
    base_table_size: i64 = 32 * 1024 * 1024,
    table_size_multiplier: i64 = 2,
    max_table_size: i64 = 128 * 1024 * 1024,

    // Tree split threshold. Compared against the bottom level (Lmax) size
    // after each compaction; when Lmax exceeds this, the tree splits at the
    // median key into two trees. Lmax holds ~90% of an LSM tree's data, so
    // this is effectively the per-tree size cap.
    tree_split_size: i64 = 5 * 1024 * 1024 * 1024, // 5 GiB

    // L0 backpressure (RocksDB-style write slowdown). When a tree's L0
    // table count exceeds `l0_slowdown_table_count`, mutations sleep
    // `(excess) * l0_slowdown_step_us` microseconds before proposing,
    // capped at `l0_slowdown_max_us`. The intent is to give flush +
    // compaction a chance to drain L0 instead of letting it grow into
    // 10s-of-GiB monolithic compaction targets. With defaults, a tree at
    // 8 L0 tables imposes 0 ms; at 18 tables, ~10 ms; at 108+ tables, the
    // cap of 100 ms. Set `l0_slowdown_table_count = 0` to disable.
    l0_slowdown_table_count: u32 = 8,
    l0_slowdown_step_us: u32 = 1000,
    l0_slowdown_max_us: u32 = 100_000,

    // Cap on the number of L0 tables consumed by a single compaction.
    // L0 → Lbase compaction picks the OLDEST `max_l0_compaction_tables`
    // L0 tables and merges them with the overlapping Lbase tables.
    // Without a cap, a single compaction can consume 100+ L0 tables (10+
    // GiB) in one shot, blocking the compaction loop for minutes while
    // L0 keeps growing. RocksDB amortizes equivalent work across
    // subcompactions + parallel workers; we don't have those yet, so we
    // bound the per-compaction work instead. Many small compactions also
    // give backpressure room to react before L0 explodes.
    max_l0_compaction_tables: u32 = 8,

    pub fn validate(self: Options) Error!void {
        // Badger semantics: exactly one compactor causes pathological level behavior.
        if (self.num_compactors == 1) return error.InvalidNumCompactors;
        if (self.memtable_size == 0) return error.InvalidMemtableSize;
        if (self.num_memtables == 0) return error.InvalidNumMemtables;
        if (self.block_size == 0) return error.InvalidBlockSize;
        if (self.max_levels == 0) return error.InvalidMaxLevels;
        if (self.in_memory and self.dir.len != 0) return error.InvalidDirForInMemory;
    }
};

pub fn sstPath(allocator: std.mem.Allocator, dir: []const u8, file_id: u64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}/{d:0>6}.sst", .{ dir, file_id });
}
