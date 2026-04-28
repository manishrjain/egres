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

    block_size: usize = 8 * 1024,
    compression: utils.compression.Codec = .lz4,
    num_compactors: u8 = 2,

    // Level sizes
    num_level_zero_tables: u32 = 5,
    base_level_size: i64 = 800 * 1024 * 1024,
    level_size_multiplier: i64 = 5,
    max_levels: u8 = 5,

    // Table sizes
    base_table_size: i64 = 32 * 1024 * 1024,
    table_size_multiplier: i64 = 2,
    max_table_size: i64 = 128 * 1024 * 1024,

    // Tree split threshold. Compared against the bottom level (Lmax) size
    // after each compaction; when Lmax exceeds this, the tree splits at the
    // median key into two trees. Lmax holds ~90% of an LSM tree's data, so
    // this is effectively the per-tree size cap.
    tree_split_size: i64 = 5 * 1024 * 1024 * 1024, // 5 GiB

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
