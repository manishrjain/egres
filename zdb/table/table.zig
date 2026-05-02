const std = @import("std");
const zbuf = @import("zbuf").zbuf;
const key_mod = @import("../key.zig");
const Key = key_mod.Key;
pub const Item = key_mod.Item;
const idx = @import("index.zig");
const buffers = @import("../buffers.gen.zig");
const utils = @import("utils");
const compress = utils.compression;
const binaryfuse = @import("../binaryfusefilter.zig");
const vfs = @import("vfs");
const cache_mod = @import("../cache.zig");

/// Module-level block cache singleton. Set before opening tables to enable caching.
/// When null (default), readBlock always reads from disk.
pub var block_cache: ?*cache_mod.ShardedCache(u64) = null;

pub const Error = idx.Error || buffers.Error || compress.Error ||
    std.mem.Allocator.Error || error{
    ChecksumMismatch,
    CorruptTable,
};

// TODO: Consider using zbuf to store this.
pub const BlockLayout = struct {
    offsets_off: usize,
    entry_count: usize,
};

pub const Table = struct {
    allocator: std.mem.Allocator,
    handle: vfs.FileHandle,
    index: *buffers.TableIndexStruct,
    codec: compress.Codec,
    binary_fuse: ?binaryfuse.BinaryFuse(u16),
    ref: std.atomic.Value(i32),
    smallest: Key,
    biggest: Key,
    file_id: u44, // parsed from SST filename; 0 for in-memory tables
    // Owned path split into atomic pointer + length for thread-safe detach.
    // Use getPath() to read, claimPath() to atomically take ownership.
    path_ptr: std.atomic.Value(?[*]const u8),
    path_len: usize,

    pub fn fileSize(self: *const Table) u64 {
        return self.handle.file_size;
    }

    /// open opens an SST file through the global VFS instance (vfs.g).
    /// The file loading mode is determined by vfs.g(); readAligned
    /// transparently handles mmap, direct, and memory-backed file handles.
    pub fn open(allocator: std.mem.Allocator, path: []const u8, priority: vfs.Priority) !*Table {
        const handle = try vfs.g().openFile(allocator, path, .{});
        const t = try initFromHandle(allocator, handle, priority);
        errdefer t.decrRef();
        t.file_id = vfs.fileIdFromPath(path);
        const duped = try allocator.dupe(u8, path);
        t.path_ptr = std.atomic.Value(?[*]const u8).init(duped.ptr);
        t.path_len = duped.len;
        return t;
    }

    /// openHandle creates a Table from a pre-opened FileHandle.
    /// Takes ownership of handle (closes it on error or when Table is freed).
    /// The resulting table has no path (decrRef frees memory only, no file deletion).
    pub fn openHandle(allocator: std.mem.Allocator, handle: vfs.FileHandle, priority: vfs.Priority) !*Table {
        return initFromHandle(allocator, handle, priority);
    }

    /// Single code path: reads trailer + index via handle.readAligned.
    fn initFromHandle(allocator: std.mem.Allocator, handle: vfs.FileHandle, priority: vfs.Priority) !*Table {
        errdefer handle.close();

        if (handle.file_size == 0) return error.CorruptTable;
        const size: usize = @intCast(handle.file_size);

        const t = try allocator.create(Table);
        errdefer allocator.destroy(t);

        // Read the trailer: [u32 index_len][u64 index_hash] (idx.trailer_len bytes).
        if (size < idx.trailer_len) return error.CorruptTable;
        const trailer_offset: u64 = @intCast(size - idx.trailer_len);
        const trailer_result = try handle.readAligned(allocator, trailer_offset, idx.trailer_len, priority);
        defer trailer_result.free();

        if (trailer_result.data.len < idx.trailer_len) return error.CorruptTable;
        const index_len = idx.readU32LE(trailer_result.data, 0) catch return error.CorruptTable;
        const stored_checksum = idx.readU64LE(trailer_result.data, 4) catch return error.CorruptTable;

        // Read index bytes.
        const index_end: u64 = @intCast(size - idx.trailer_len);
        if (index_len > index_end) return error.CorruptTable;
        const index_offset = index_end - index_len;
        const index_result = try handle.readAligned(allocator, index_offset, index_len, priority);
        defer index_result.free();

        if (index_result.data.len < index_len) return error.CorruptTable;
        const index_bytes = index_result.data[0..index_len];
        const got_checksum = zbuf.hash(index_bytes);
        if (got_checksum != stored_checksum) return error.ChecksumMismatch;

        const index_reader = try buffers.TableIndexReader.init(index_bytes, 0);
        var table_index = try buffers.TableIndexStruct.fromReader(allocator, index_reader);
        errdefer table_index.deinit(allocator);

        const codec: compress.Codec = @enumFromInt(@intFromEnum(table_index.compression));

        // Steal the fingerprints slice from the decoded index struct into a
        // BinaryFuse(u16) that we keep for the lifetime of this Table.  After
        // the transfer table_index.binary_fuse holds no heap memory (fingerprints is
        // &.{}) so index.deinit() becomes a no-op for that field.
        const binary_fuse: ?binaryfuse.BinaryFuse(u16) = if (table_index.binary_fuse) |*bf| blk: {
            const filter = binaryfuse.BinaryFuse(u16){
                .seed = bf.seed,
                .segment_length = bf.segment_length,
                .segment_length_mask = bf.segment_length_mask,
                .segment_count = bf.segment_count,
                .segment_count_length = bf.segment_count_length,
                .fingerprints = bf.fingerprints,
            };
            bf.fingerprints = &.{};
            table_index.binary_fuse = null;
            break :blk filter;
        } else null;

        const smallest_raw = if (table_index.blocks.len > 0) table_index.blocks[0].key else &.{};
        t.* = .{
            .allocator = allocator,
            .handle = handle,
            .index = table_index,
            .codec = codec,
            .binary_fuse = binary_fuse,
            .smallest = if (smallest_raw.len >= 16) Key.init(smallest_raw) else .{},
            .biggest = if (table_index.last_key.len >= 16) Key.init(table_index.last_key) else .{},
            .ref = std.atomic.Value(i32).init(1),
            .file_id = 0,
            .path_ptr = std.atomic.Value(?[*]const u8).init(null),
            .path_len = 0,
        };
        return t;
    }

    /// Atomically claim ownership of the path, returning the slice if we won the race.
    fn claimPath(self: *Table) ?[]const u8 {
        const ptr = self.path_ptr.swap(null, .acq_rel);
        if (ptr) |p| return p[0..self.path_len];
        return null;
    }

    fn deinit(self: *Table) void {
        if (self.claimPath()) |p| self.allocator.free(p);
        self.index.destroy(self.allocator);
        if (self.binary_fuse) |*bf| bf.deinit(self.allocator);
        self.handle.close();
    }

    /// detach forgets the SST file path without deleting it.
    /// Used by Level.deinit() (database close) so that the subsequent
    /// decrRef at zero frees memory only, leaving the file on disk intact.
    /// Thread-safe: concurrent calls race on an atomic swap; only the winner frees.
    pub fn detach(self: *Table) void {
        if (self.claimPath()) |p| self.allocator.free(p);
    }

    /// Increment the reference count. Call this when sharing a Table pointer.
    pub fn incrRef(self: *Table) void {
        _ = self.ref.fetchAdd(1, .monotonic);
    }

    /// Decrement the reference count. When it reaches zero the table's resources are freed
    /// and, for file-backed tables, the SST file is deleted from disk.
    pub fn decrRef(self: *Table) void {
        const prev = self.ref.fetchSub(1, .acq_rel);
        std.debug.assert(prev > 0);
        if (prev == 1) {
            if (self.claimPath()) |p| {
                // TODO: We should be deleting this file only after we are sure that the Raft WAL
                // snapshot no longer needs this file to be present for a replay. Because, now the
                // LSM tree manifest is also essentially an extension of the Raft Snapshot.
                std.fs.deleteFileAbsolute(p) catch {};
                self.allocator.free(p);
            }
            self.deinit();
        }
    }

    pub fn getIndex(self: *const Table) *const buffers.TableIndexStruct {
        return &self.index;
    }

    pub fn containsKey(self: *const Table, key: Key) bool {
        if (self.binary_fuse) |bf| {
            const h = key.hash();
            return bf.contain(h);
        }
        return true;
    }

    pub fn blockCount(self: Table) usize {
        return self.index.blocks.len;
    }

    pub fn blockBaseKey(self: Table, block_idx: usize) Error![]const u8 {
        if (block_idx >= self.index.blocks.len) return error.CorruptTable;
        return self.index.blocks[block_idx].key;
    }

    pub fn readBlock(self: *const Table, block_idx: usize, priority: vfs.Priority) Error!*cache_mod.Value {
        if (block_idx >= self.index.blocks.len) return error.CorruptTable;

        // Cache key: 44 bits file_id (MSB) | 20 bits block_idx (LSB).
        // null on overflow → cache_key = 0 → skip caching.
        const fid: u64 = self.file_id;
        const bidx: u64 = std.math.cast(u20, block_idx) orelse 0;
        const cache_key: u64 = if (fid != 0 and bidx != 0) (fid << 20) | bidx else 0;

        if (block_cache != null and cache_key != 0) {
            if (block_cache.?.get(cache_key)) |val| return val; // incrRef'd by get.
        }

        const bi = self.index.blocks[block_idx];
        if (bi.len < 4) return error.CorruptTable;

        // Read raw bytes (temporary)
        const result = self.handle.readAligned(
            self.allocator,
            @intCast(bi.offset),
            bi.len,
            priority,
        ) catch return error.CorruptTable;
        defer result.free();

        // Decompress (temporary)
        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.allocator);
        const sz = try compress.decompress(self.allocator, self.codec, result.data, &out);
        // Block must contain at least the trailing u64 hash (see zbuf.hash).
        if (sz < 8) return error.CorruptTable;
        std.debug.assert(out.items.len == sz);

        const checksum_off = sz - 8;
        const stored_checksum = try idx.readU64LE(out.items, checksum_off);
        const got_checksum = zbuf.hash(out.items[0..checksum_off]);
        if (stored_checksum != got_checksum) return error.ChecksumMismatch;

        // Create ref-counted Value with owned copy
        const owned_data = try out.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(owned_data);

        // val has ref=1
        const val = cache_mod.Value.init(self.allocator, owned_data) catch return error.OutOfMemory;
        errdefer val.decrRef();

        // Cache insert (best-effort); put incrRefs on success → ref=2
        if (block_cache != null and cache_key != 0) {
            block_cache.?.put(cache_key, val) catch {};
        }

        return val; // caller holds ref (1 if not cached, or 1+cache_ref if cached)
    }
};

/// BlockIterator decodes entries from a single block lazily, one at a time.
/// Block data is caller-owned; key_buf is self-owned.
pub const BlockIterator = struct {
    alloc: std.mem.Allocator,
    key_buf: std.ArrayList(u8),
    block: []const u8,
    entries_region: []const u8,
    layout: BlockLayout,
    base_key: []const u8,
    entry_idx: usize,
    val: []const u8,
    is_valid: bool,

    pub fn deinit(self: *BlockIterator) void {
        self.key_buf.deinit(self.alloc);
    }

    fn parseBlockLayout(block: []const u8) Error!BlockLayout {
        // Trailer is [u32 entry_count][u64 block_hash] (see builder.zig
        // finishBlock); parseBlockLayout never validates the hash itself —
        // that's already done in readBlockUncached's decompress step.
        if (block.len < 12) return error.CorruptTable;

		// TODO: We should be running block checksums.
        const checksum_off = block.len - 8;
        const count_off = checksum_off - 4;
        const entry_count_u32 = try idx.readU32LE(block, count_off);
        const entry_count: usize = @intCast(entry_count_u32);
        if (entry_count == 0) return error.CorruptTable;

        const offsets_bytes = entry_count * 4;
        if (count_off < offsets_bytes) return error.CorruptTable;
        const offsets_off = count_off - offsets_bytes;

        return .{ .offsets_off = offsets_off, .entry_count = entry_count };
    }

    pub fn init(
        alloc: std.mem.Allocator,
        block: []const u8,
        base_key: []const u8,
    ) !BlockIterator {
        // See Table Layout diagram in builder.zig.
        // This should be a copy of that. If not, use builder.zig as the source of truth.
        //
        // Block layout is set as:
        // | ------------------------------------------------------------------------||
        // | --- entries_region --- | --- offsets to entries, each offset is u32 --- |
        // | --  entry count u32 -- | -- block crc u32 -- |
        // | ------------------------------------------------------------------------||
        //
        // Note that we only store the start offset of each entry i, not the end, because
        // the end offset of entry i = the start offset of entry i+1.
        //
        // Each entry is stored as such:
        // | ----------------------------------------------------------------------||
        // | -- u16 key overlap with base -- | -- u16 key diff length from base -- |
        // | -- key diff bytes -- | -- value bytes ------------------------------- |
        // | ----------------------------------------------------------------------||
        const layout = try parseBlockLayout(block);

        return .{
            .alloc = alloc,
            .key_buf = .empty,
            .block = block,
            .layout = layout,
            .entries_region = block[0..layout.offsets_off],
            .base_key = base_key,
            .entry_idx = 0,
            .val = &[_]u8{},
            .is_valid = false,
        };
    }

    pub fn seekToFirst(self: *BlockIterator) Error!void {
        try self.setIdx(0);
    }

    pub fn seekToLast(self: *BlockIterator) Error!void {
        if (self.layout.entry_count == 0) {
            self.is_valid = false;
            return;
        }
        try self.setIdx(self.layout.entry_count - 1);
    }

    pub fn next(self: *BlockIterator) Error!void {
        try self.setIdx(self.entry_idx + 1);
    }

    /// Binary search for the first entry with key >= target.
    pub fn seekGe(self: *BlockIterator, target: Key) Error!void {
        // This binary search is only possible because we're diffing keys
        // against the base key and not against the previous key.
        var lo: usize = 0;
        var hi: usize = self.layout.entry_count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            try self.setIdx(mid);
            if (!self.is_valid) {
                hi = mid;
                continue;
            }
            if (Key.init(self.key_buf.items).order(target) == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo < self.layout.entry_count) {
            try self.setIdx(lo);
        } else {
            self.is_valid = false;
        }
    }

    pub fn itemRef(self: *const BlockIterator) ?Item {
        if (!self.is_valid) return null;
        return .{ .key = Key.init(self.key_buf.items), .value = self.val };
    }

    inline fn getOffset(self: *BlockIterator, i: usize) Error!usize {
        const offset = try idx.readU32LE(self.block, self.layout.offsets_off + (i * 4));
        return @intCast(offset);
    }

    fn setIdx(self: *BlockIterator, i: usize) Error!void {
        if (i >= self.layout.entry_count) {
            self.is_valid = false;
            return;
        }

        const off = try self.getOffset(i);
        if (off + 4 > self.entries_region.len) return error.CorruptTable;

        const key_overlap: usize = @intCast(try idx.readU16LE(self.entries_region, off));
        const key_diff_len: usize = @intCast(try idx.readU16LE(self.entries_region, off + 2));
        if (key_overlap > self.base_key.len) return error.CorruptTable;

        const diff_start = off + 4;
        const diff_end = diff_start + key_diff_len;
        if (diff_end > self.entries_region.len) return error.CorruptTable;
        const diff = self.entries_region[diff_start..diff_end];

        // Reconstruct full key into reusable buffer: base[0..overlap] ++ diff.
        self.key_buf.clearRetainingCapacity();
        try self.key_buf.appendSlice(self.alloc, self.base_key[0..key_overlap]);
        try self.key_buf.appendSlice(self.alloc, diff);

        // Value boundary: next entry's offset, or end of entries_region for the last entry.
        const value_start = diff_end;
        const value_end = if (i + 1 < self.layout.entry_count) try self.getOffset(i + 1) else self.entries_region.len;
        if (value_end < value_start or value_end > self.entries_region.len) return error.CorruptTable;

        self.val = self.entries_region[value_start..value_end];
        self.entry_idx = i;
        self.is_valid = true;
    }
};

/// TableIterator navigates all entries across all blocks in a table.
pub const TableIterator = struct {
    allocator: std.mem.Allocator,
    table: *Table,
    bpos: usize,
    bi: ?BlockIterator,
    current_block: ?*cache_mod.Value = null,
    valid: bool,
    priority: vfs.Priority,
    end_key: ?Key = null, // exclusive upper bound; owned (duped)
    end_bpos: usize = 0, // block index where end_key lives

    pub fn init(allocator: std.mem.Allocator, table: *Table, priority: vfs.Priority) TableIterator {
        table.incrRef();
        return .{
            .allocator = allocator,
            .table = table,
            .bpos = 0,
            .bi = null,
            .current_block = null,
            .valid = false,
            .priority = priority,
        };
    }

    pub fn deinit(self: *TableIterator) void {
        if (self.end_key) |ek| ek.free(self.allocator);
        self.table.decrRef();
        if (self.bi) |*bi| bi.deinit();
        if (self.current_block) |b| b.decrRef();
    }

    /// Seek to the first entry >= start, stopping before any entry with key >= end.
    /// Empty start means seek to first; empty end means no upper bound.
    /// Dupes end so it remains valid for the lifetime of this iterator.
    pub fn seekRange(self: *TableIterator, start: Key, end: Key) !void {
        // Set up end bound.
        if (self.end_key) |ek| ek.free(self.allocator);
        self.end_key = null;

        if (end.len() > 0) {
            if (end.order(self.table.smallest) != .gt) {
                // end <= smallest: nothing to return.
                self.valid = false;
                return;
            }
            if (end.order(self.table.biggest) != .gt) {
                // The end key is within this table's key range.
                self.end_key = try end.dupe(self.allocator);
                // Find the block containing end_key: last block with base key <= end.
                const blocks = self.table.index.blocks;
                const ei = utils.search(buffers.BlockIndexStruct, blocks, end, struct {
                    fn f(ek: Key, b: buffers.BlockIndexStruct) bool {
                        return Key.init(b.key).order(ek) == .gt;
                    }
                }.f);
                self.end_bpos = if (ei > 0) ei - 1 else 0;
            }
        }

        // Seek.
        if (start.len() == 0) {
            try self.seekToFirst();
        } else {
            try self.seekForward(start);
        }
    }

    fn seekToFirst(self: *TableIterator) Error!void {
        if (self.table.blockCount() == 0) {
            self.valid = false;
            return;
        }
        try self.setBlock(0);
        try self.bi.?.seekToFirst();
        self.valid = self.bi.?.is_valid;
    }

    fn seekForward(self: *TableIterator, target: Key) Error!void {
        self.valid = false;
        const blocks = self.table.index.blocks;
        if (blocks.len == 0) return;

        // First block whose base key > target.
        const bi_idx = utils.search(buffers.BlockIndexStruct, blocks, target, struct {
            fn f(sk: Key, b: buffers.BlockIndexStruct) bool {
                return Key.init(b.key).order(sk) == .gt;
            }
        }.f);

        // The block we want is idx-1 (last block with base key <= target),
        // or 0 if all base keys are > target.
        const lo = if (bi_idx > 0) bi_idx - 1 else 0;

        try self.setBlock(lo);
        try self.bi.?.seekGe(target);
        if (self.bi.?.is_valid) {
            self.valid = true;
            return;
        }

        // Not found in block lo — target may sit at the start of the next block.
        if (lo + 1 < blocks.len) {
            try self.setBlock(lo + 1);
            try self.bi.?.seekToFirst();
            self.valid = self.bi.?.is_valid;
        }
    }

    pub fn next(self: *TableIterator) Error!void {
        if (!self.valid) return;
        defer self.checkEndKey(); // Always do it.

        try self.bi.?.next();
        if (self.bi.?.is_valid) {
            return;
        }

        // End of current block — advance to the next.
        const next_bpos = self.bpos + 1;
        if (self.end_key != null and next_bpos > self.end_bpos) {
            // No need to fetch the block.
            self.valid = false;
            return;
        }
        if (next_bpos >= self.table.blockCount()) {
            self.valid = false;
            return;
        }
        try self.setBlock(next_bpos);
        try self.bi.?.seekToFirst();
        self.valid = self.bi.?.is_valid;
    }

    /// Invalidates the iterator if current item >= end_key.
    inline fn checkEndKey(self: *TableIterator) void {
        if (!self.valid) return;
        const ek = self.end_key orelse return;

        if (self.bpos > self.end_bpos) {
            self.valid = false;
            return;
        }
        if (self.bpos < self.end_bpos) {
            return;
        }
        const item = self.bi.?.itemRef() orelse return;
        if (item.key.order(ek) != .lt) {
            self.valid = false;
        }
    }

    pub fn itemRef(self: *const TableIterator) ?Item {
        if (!self.valid) return null;
        return self.bi.?.itemRef();
    }

    fn setBlock(self: *TableIterator, bpos: usize) Error!void {
        if (self.bi) |*bi| bi.deinit();
        if (self.current_block) |b| b.decrRef();
        self.current_block = null;

        const val = try self.table.readBlock(bpos, self.priority);
        self.current_block = val;

        const base_key = try self.table.blockBaseKey(bpos);
        self.bi = try BlockIterator.init(self.allocator, val.data, base_key);
        self.bpos = bpos;
    }
};
