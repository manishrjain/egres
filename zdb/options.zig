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

    // Level sizes. Lmax target is derived from `tree_split_size`; upper
    // levels cascade by dividing by `level_size_multiplier`. There is no
    // separate base-level-size knob — every tree caps at the split size.
    level_size_multiplier: i64 = 5,
    max_levels: u8 = 4,

    // Per-level SST file size. L0 inherits the memtable size. L1 uses
    // `l1_table_size`; each level below ramps by `table_size_multiplier`
    // up to `max_table_size`.
    l1_table_size: i64 = 32 * 1024 * 1024,
    table_size_multiplier: i64 = 2,
    max_table_size: i64 = 128 * 1024 * 1024,

    // Tree split threshold. Compared against the bottom level (Lmax) size
    // after each compaction; when Lmax exceeds this, the tree splits at the
    // median key into two trees. Lmax holds ~90% of an LSM tree's data, so
    // this is effectively the per-tree size cap.
    tree_split_size: i64 = 5 * 1024 * 1024 * 1024, // 5 GiB

    // L0 backpressure (RocksDB-style write slowdown). When a tree's L0
    // table count exceeds `l0_backpressure_start_count`, mutations sleep
    // `(excess) * l0_backpressure_step_us` microseconds before proposing,
    // capped at `l0_backpressure_max_us`. The intent is to give flush +
    // compaction a chance to drain L0 instead of letting it grow into
    // 10s-of-GiB monolithic compaction targets. This is a *trigger*, not
    // a cap — L0 may still grow past it; writes just slow down. With
    // defaults, a tree at the trigger imposes 0 ms; +10 tables ~10 ms;
    // +100 tables hits the cap of 100 ms. Set
    // `l0_backpressure_start_count = 0` to disable backpressure entirely.
    l0_backpressure_start_count: u32 = 8,
    l0_backpressure_step_us: u32 = 1000,
    l0_backpressure_max_us: u32 = 100_000,

    // Per-compaction cap on L0 input tables: the L0→L1 picker takes the
    // OLDEST `l0_tables_per_compaction` and merges them with the
    // overlapping L1 tables. Backpressure normally keeps L0 short, so
    // this only matters as a safety bound — without it, a backlog of L0
    // tables (e.g., during a flush burst) would land in one giant
    // compaction that blocks the loop for minutes while L0 keeps growing.
    l0_tables_per_compaction: u32 = 8,

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
