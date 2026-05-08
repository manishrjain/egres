const std = @import("std");
const Key = @import("../key.zig").Key;
const buffers = @import("../buffers.gen.zig");
const compress = @import("utils").compression;
const binaryfuse = @import("../binaryfusefilter.zig");
const vfs = @import("vfs");
const zbuf = @import("zbuf").zbuf;

// Table layout (Badger-inspired, with zbuf for TableIndex/BlockOffset):
//
// [Block 0]
// [Block 1]
// ...
// [Block N]
// [TableIndex bytes]
// [u32 index_len]
// [u32 index_crc32]
//
// Block layout is set as:
// | ------------------------------------------------------------------------|
// | --- entries_region --- | --- offsets to entries, each offset is u32 --- |
// | --  entry count u32 -- | -- block crc u64 -- |
// | ------------------------------------------------------------------------|
//
// Note that we only store the start offset of each entry i, not the end, because
// the end offset of entry i = the start offset of entry i+1.
//
// Each entry is stored as such:
// | ----------------------------------------------------------------------|
// | -- u16 key overlap with base -- | -- u16 key diff length from base -- |
// | -- key diff bytes -- | -- value bytes ------------------------------- |
// | ----------------------------------------------------------------------|
//
// Notes:
// - Keys are prefix-compressed against the block base key (first key in block),
//   not against the previous key. This is useful because it allows us random
//   access to any idx on the block entry, and derive its key.
//
// - entry_off_* values point to entry starts within the block.
// - CRCs are CRC32 over the uncompressed block bytes prior to the checksum field.
//   Blocks are then optionally compressed; the stored CRC remains over uncompressed data.
// - TableIndex (zbuf) contains block offsets, max_version, key_count, sizes,
//   and currently an empty bloom filter.
// - All integer fields are little-endian.

pub const Error = error{
    KeyOrderViolation,
    KeyTooLarge,
    ValueTooLarge,
    TooManyBlocks,
    IntegerOverflow,
} || buffers.Error || compress.Error || std.mem.Allocator.Error || binaryfuse.Error;

pub const Config = struct {
    target_block_size: usize = 32 * 1024,
    codec: compress.Codec = .none,
    priority: vfs.Priority = .p2,
};

const BlockInfo = struct {
    // base_key is the first key of the block. It is used to prefix-decode
    // all keys in that block.
    base_key: []u8,
    // offset/len point to the finalized block bytes inside table_bytes.
    offset: u32,
    len: u32,
};

pub const TableBuffer = struct {
    allocator: std.mem.Allocator,
    bytes: []u8,

    pub fn deinit(self: *TableBuffer) void {
        self.allocator.free(self.bytes);
    }
};

pub const Builder = struct {
    allocator: std.mem.Allocator,
    cfg: Config,
    table_bytes: std.ArrayList(u8),
    cur_block: std.ArrayList(u8),
    cur_offsets: std.ArrayList(u32),
    cur_base_key: std.ArrayList(u8),
    block_infos: std.ArrayList(BlockInfo),
    last_key: std.ArrayList(u8),
    bf_hashes: std.ArrayList(u64),
    key_count: u32,
    max_version: u64,
    uncompressed_size: u32,
    compressed_size: u32,
    stale_data_size: u32,

    pub fn init(allocator: std.mem.Allocator, cfg: Config) Builder {
        // TODO: Choose sane defaults for table_bytes and cur_block.
        return .{
            .allocator = allocator,
            .cfg = cfg,
            .table_bytes = .empty,
            .cur_block = .empty,
            .cur_offsets = .empty,
            .cur_base_key = .empty,
            .block_infos = .empty,
            .last_key = .empty,
            .bf_hashes = .empty,
            .key_count = 0,
            .max_version = 0,
            .uncompressed_size = 0,
            .compressed_size = 0,
            .stale_data_size = 0,
        };
    }

    pub fn deinit(self: *Builder) void {
        for (self.block_infos.items) |info| {
            self.allocator.free(info.base_key);
        }
        self.block_infos.deinit(self.allocator);
        self.last_key.deinit(self.allocator);
        self.bf_hashes.deinit(self.allocator);
        self.cur_base_key.deinit(self.allocator);
        self.cur_offsets.deinit(self.allocator);
        self.cur_block.deinit(self.allocator);
        self.table_bytes.deinit(self.allocator);
    }

    /// reset clears all state so the builder can produce a new table without
    /// reallocating its backing buffers (especially table_bytes).
    pub fn reset(self: *Builder) void {
        for (self.block_infos.items) |info| {
            self.allocator.free(info.base_key);
        }

        self.block_infos.clearRetainingCapacity();
        self.last_key.clearRetainingCapacity();
        self.bf_hashes.clearRetainingCapacity();
        self.cur_base_key.clearRetainingCapacity();
        self.cur_offsets.clearRetainingCapacity();
        self.cur_block.clearRetainingCapacity();
        self.table_bytes.clearRetainingCapacity();
        self.key_count = 0;
        self.max_version = 0;
        self.uncompressed_size = 0;
        self.compressed_size = 0;
        self.stale_data_size = 0;
    }

    pub fn lastKey(self: *Builder) Key {
        if (self.last_key.items.len == 0) return .{};
        return Key.init(self.last_key.items);
    }

    pub fn add(self: *Builder, key: Key, value: []const u8) Error!void {
        if (key.len() > std.math.maxInt(u16)) return error.KeyTooLarge;
        if (value.len > std.math.maxInt(u32)) return error.ValueTooLarge;

        if (self.last_key.items.len > 0) {
            const last = self.lastKey();
            if (key.order(last) != .gt) {
                return error.KeyOrderViolation;
            }
            if (!key.equalPrimary(last)) {
                try self.bf_hashes.append(self.allocator, key.hash());
            }
        } else {
            try self.bf_hashes.append(self.allocator, key.hash());
        }

        // Marshal into last_key scratch buffer for prefix compression.
        self.last_key.clearRetainingCapacity();
        _ = try key.marshalTo(self.allocator, &self.last_key);
        const entry_key = self.last_key.items;

        const overlap = if (self.cur_base_key.items.len == 0)
            0
        else
            commonPrefixLen(self.cur_base_key.items, entry_key);
        const diff_len = entry_key.len - overlap;

        // Estimate the encoded size for this entry:
        // 2 bytes overlap + 2 bytes diff_len + key diff + value.
        const entry_sz = @as(usize, 4) + diff_len + value.len;
        if (self.shouldFinishBlock(entry_sz)) {
            try self.finishBlock();
        }

        if (self.cur_base_key.items.len == 0) {
            try self.cur_base_key.appendSlice(self.allocator, entry_key);
        }

        const overlap2 = commonPrefixLen(self.cur_base_key.items, entry_key);
        if (overlap2 > std.math.maxInt(u16)) return error.KeyTooLarge;
        const diff = entry_key[overlap2..];
        if (diff.len > std.math.maxInt(u16)) return error.KeyTooLarge;

        // cur_offsets stores the start offset of each entry in cur_block.
        try self.cur_offsets.append(self.allocator, @intCast(self.cur_block.items.len));
        // Per-entry encoding: [u16 overlap][u16 diff_len][diff bytes][value bytes]
        // Value boundary is derived from the next entry offset (or end of entries region).
        try appendU16(&self.cur_block, self.allocator, @intCast(overlap2));
        try appendU16(&self.cur_block, self.allocator, @intCast(diff.len));
        try self.cur_block.appendSlice(self.allocator, diff);
        try self.cur_block.appendSlice(self.allocator, value);

        if (self.key_count == std.math.maxInt(u32)) return error.IntegerOverflow;
        self.key_count += 1;
        if (key.timestamp > self.max_version) self.max_version = key.timestamp;
    }

    pub fn finish(self: *Builder) Error!TableBuffer {
        // Flush the final in-progress block, if any.
        try self.finishBlock();

        if (self.block_infos.items.len == 0) {
            return .{ .allocator = self.allocator, .bytes = try self.allocator.alloc(u8, 0) };
        }

        var index_bytes = std.ArrayList(u8).empty;
        defer index_bytes.deinit(self.allocator);

        // Build index bytes in a separate buffer, then append to table tail.
        var index_writer = try buffers.TableIndexWriter.initWithBuf(self.allocator, &index_bytes);

        var block_offsets = try self.allocator.alloc(u32, self.block_infos.items.len);
        defer self.allocator.free(block_offsets);

        for (self.block_infos.items, 0..) |info, i| {
            // BlockIndex records each block's base key and byte range in table bytes.
            var block_writer = try buffers.BlockIndexWriter.initWithBuf(
                self.allocator,
                &index_bytes,
            );
            try block_writer.set(.{
                .key = info.base_key,
                .offset = info.offset,
                .len = info.len,
            });
            const block_buf = try block_writer.finish();
            block_offsets[i] = block_buf.offset;
        }

        var binary_fuse_off: ?u32 = null;
        if (self.bf_hashes.items.len > 0) {
            var filter = try binaryfuse.BinaryFuse(u16).init(self.allocator, self.bf_hashes.items.len);
            defer filter.deinit(self.allocator);
            try filter.populate(self.allocator, self.bf_hashes.items);

            var bf_writer = try buffers.BinaryFuseFilterWriter.initWithBuf(self.allocator, &index_bytes);
            try bf_writer.set(.{
                .seed = filter.seed,
                .segment_length = filter.segment_length,
                .segment_length_mask = filter.segment_length_mask,
                .segment_count = filter.segment_count,
                .segment_count_length = filter.segment_count_length,
                .fingerprints = filter.fingerprints,
            });
            const bf_buf = try bf_writer.finish();
            binary_fuse_off = bf_buf.offset;
        }

        try index_writer.set(.{
            .blocks = block_offsets,
            .last_key = self.last_key.items,
            .max_version = self.max_version,
            .key_count = self.key_count,
            .uncompressed_size = self.uncompressed_size,
            .compressed_size = self.compressed_size,
            .stale_data_size = self.stale_data_size,
        });
        try index_writer.setCompression(@enumFromInt(@intFromEnum(self.cfg.codec)));
        if (binary_fuse_off) |off| {
            try index_writer.setBinaryFuse(off);
        }

        _ = try index_writer.finish();

        const index_len: u32 = @intCast(index_bytes.items.len);
        const index_checksum = zbuf.hash(index_bytes.items);

        // Pad with zeros between the data blocks and the index so the final
        // file size is a multiple of `vfs.page_size`. The reader locates the
        // index via the trailer's `index_len` (offset = file_end - trailer
        // - index_len), so padding before the index is invisible to it.
        // Page-aligned files dodge any O_DIRECT / iomap edge case where an
        // unaligned-tail write could be silently dropped on some kernels;
        // the cost is at most page_size-1 zero bytes per SST.
        const trailer_size: usize = 4 + 8;
        const tail_size = index_bytes.items.len + trailer_size;
        const total_unpadded = self.table_bytes.items.len + tail_size;
        const remainder = total_unpadded % vfs.page_size;
        const pad_bytes = if (remainder == 0) 0 else vfs.page_size - remainder;
        if (pad_bytes > 0) try self.table_bytes.appendNTimes(self.allocator, 0, pad_bytes);

        try self.table_bytes.appendSlice(self.allocator, index_bytes.items);
        try appendU32(&self.table_bytes, self.allocator, index_len);
        try appendU64(&self.table_bytes, self.allocator, index_checksum);

        std.debug.assert(self.table_bytes.items.len % vfs.page_size == 0);

        const bytes = try self.table_bytes.toOwnedSlice(self.allocator);
        self.table_bytes = .empty;
        return .{ .allocator = self.allocator, .bytes = bytes };
    }

    fn shouldFinishBlock(self: *Builder, new_entry_est: usize) bool {
        if (self.cur_offsets.items.len == 0) return false;

        // Block tail layout:
        // (entry_count + 1) offsets * 4 bytes each
        // + 4 bytes entry_count
        // + 8 bytes checksum (xxhash3, see zbuf.hash)
        const tail_est =
            (self.cur_offsets.items.len + 1) * 4 + 12;
        const estimated = self.cur_block.items.len + new_entry_est + tail_est;
        return estimated > self.cfg.target_block_size;
    }

    fn finishBlock(self: *Builder) Error!void {
        if (self.cur_offsets.items.len == 0) return;
        if (self.block_infos.items.len >= std.math.maxInt(u32)) return error.TooManyBlocks;

        // Capture block base key before scratch buffers are reset.
        const base_key = try self.allocator.dupe(u8, self.cur_base_key.items);
        errdefer self.allocator.free(base_key);

        // Block tail: [u32 entry_off_0 ...][u32 entry_count][u64 block_hash]
        for (self.cur_offsets.items) |off| {
            try appendU32(&self.cur_block, self.allocator, off);
        }
        try appendU32(&self.cur_block, self.allocator, @intCast(self.cur_offsets.items.len));

        const checksum = zbuf.hash(self.cur_block.items);
        try appendU64(&self.cur_block, self.allocator, checksum);

        // Track pre-compression size; CRC was computed over the uncompressed bytes.
        const raw_len: u32 = @intCast(self.cur_block.items.len);
        std.debug.assert(self.uncompressed_size <= std.math.maxInt(u32) - raw_len);
        self.uncompressed_size += raw_len;

        const block_off: u32 = @intCast(self.table_bytes.items.len);
        if (self.cfg.codec == .none) {
            try self.table_bytes.appendSlice(self.allocator, self.cur_block.items);
        } else {
            const clen: u32 = @intCast(try compress.compress(self.allocator, self.cfg.codec, self.cur_block.items, &self.table_bytes));
            std.debug.assert(self.compressed_size <= std.math.maxInt(u32) - clen);
            self.compressed_size += clen;
        }
        const block_len: u32 = @intCast(self.table_bytes.items.len - block_off);

        try self.block_infos.append(self.allocator, .{
            .base_key = base_key,
            .offset = block_off,
            .len = block_len,
        });

        // Reuse capacity for the next block to avoid re-allocating every time.
        self.cur_block.clearRetainingCapacity();
        self.cur_offsets.clearRetainingCapacity();
        self.cur_base_key.clearRetainingCapacity();
    }
};

fn commonPrefixLen(a: []const u8, b: []const u8) usize {
    const n = @min(a.len, b.len);
    var i: usize = 0;
    while (i < n and a[i] == b[i]) : (i += 1) {}
    return i;
}

fn appendU16(out: *std.ArrayList(u8), alloc: std.mem.Allocator, v: u16) !void {
    var tmp: [2]u8 = undefined;
    std.mem.writeInt(u16, &tmp, v, .little);
    try out.appendSlice(alloc, tmp[0..]);
}

fn appendU32(out: *std.ArrayList(u8), alloc: std.mem.Allocator, v: u32) !void {
    var tmp: [4]u8 = undefined;
    std.mem.writeInt(u32, &tmp, v, .little);
    try out.appendSlice(alloc, tmp[0..]);
}

fn appendU64(out: *std.ArrayList(u8), alloc: std.mem.Allocator, v: u64) !void {
    var tmp: [8]u8 = undefined;
    std.mem.writeInt(u64, &tmp, v, .little);
    try out.appendSlice(alloc, tmp[0..]);
}
