// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const kv = @import("../kv.zig");
const zbuf = @import("zbuf").zbuf;
const Key = @import("../kv.zig").Key;
const Direction = @import("../kv.zig").Direction;
const KeyRange = @import("../kv.zig").KeyRange;
const TableKey = @import("../kv.zig").TableKey;
const Meta = @import("../kv.zig").Meta;
pub const Item = @import("../kv.zig").Item;
const Version = @import("../kv.zig").Version;
const _idx = @import("index.zig");
const _buffers = @import("../buffers.gen.zig");
const utils = @import("utils");
const String = utils.String;
const compress = utils.compression;
const _binaryfuse = @import("../binaryfusefilter.zig");
const zroar = @import("zroar");
const vfs = @import("vfs");
const fiber = @import("zfiber");
const _cache = @import("../cache.zig");

/// Decoded blocks by key: the file id in the high 64 bits, the block
/// index in the low ones. Each in its own half means no hash combine
/// and no collision across the node_id-namespaced file ids. A table
/// gets its cache from its store (`Store.blockCache`).
pub const BlockCache = vfs.BlockCache;

/// What an open table pins in memory for its lifetime, in bytes: the
/// block index (its serialized size less the filter and bitmap it
/// carries, within a few percent of the decoded struct, plus the
/// `cached` bitset), the filter, and the egid bitmap. Summed over the
/// open tables in `resident`, what the block cache's budget has to
/// leave room for.
pub const Resident = struct {
    index: u64 = 0,
    filter: u64 = 0,
    egids: u64 = 0,
};

pub const ResidentTotals = struct {
    tables: std.atomic.Value(u64) = .init(0),
    index: std.atomic.Value(u64) = .init(0),
    filter: std.atomic.Value(u64) = .init(0),
    egids: std.atomic.Value(u64) = .init(0),

    fn add(self: *ResidentTotals, r: Resident) void {
        _ = self.tables.fetchAdd(1, .monotonic);
        _ = self.index.fetchAdd(r.index, .monotonic);
        _ = self.filter.fetchAdd(r.filter, .monotonic);
        _ = self.egids.fetchAdd(r.egids, .monotonic);
    }

    fn sub(self: *ResidentTotals, r: Resident) void {
        _ = self.tables.fetchSub(1, .monotonic);
        _ = self.index.fetchSub(r.index, .monotonic);
        _ = self.filter.fetchSub(r.filter, .monotonic);
        _ = self.egids.fetchSub(r.egids, .monotonic);
    }
};

pub var resident: ResidentTotals = .{};

/// The store object name of SST `file_id`: `<id:016x>.sst` — fixed-width
/// hex, so a store listing sorts by id lexicographically. Caller owns the
/// returned slice.
pub fn sstName(allocator: std.mem.Allocator, file_id: u64) ![]u8 {
    return std.fmt.allocPrint(allocator, "{x:0>16}.sst", .{file_id});
}

pub const Error = _idx.Error || _buffers.Error || compress.Error ||
    std.mem.Allocator.Error || error{
    ChecksumMismatch,
    CorruptTable,
    UnsupportedTableFormat,
};

const log = std.log.scoped(.table);

/// A block's base key as a primary-only Key: an SST stores no namespace
/// or table_id (see `TableIterator.init`).
inline fn baseKeyOf(b: _buffers.BlockIndexStruct) Key {
    return .{ .primary = b.key };
}

/// The smallest stored key: one primary byte.
const min_key_len: usize = 1;

pub const Table = struct {
    allocator: std.mem.Allocator,
    /// The SST's bytes, wherever the store keeps them.
    obj: vfs.Object,
    index: *_buffers.TableIndexStruct,
    /// The table's filter, one of the two (see `builder.Config.kind`),
    /// or neither for a table written without one.
    binary_fuse: ?_binaryfuse.BinaryFuse(u16),
    egid_bm: ?zroar.Bitmap,
    ref: std.atomic.Value(i32),
    /// The table's key bounds, primary only (namespace and table_id 0):
    /// compare them with `Key.orderPrimary`.
    smallest: Key,
    biggest: Key,
    /// The id the manifest lists this file under, supplied by the caller
    /// that opened it. 0 for in-memory tables — also the sentinel "skip
    /// block-cache" value, since 0 is never handed out as a file id.
    file_id: u64,
    /// What the manifest records for the file; a re-list of this table
    /// (move, split, branch) copies it into the new CREATE.
    extent: Extent,
    /// Where `readBlock` keeps this table's blocks, or null for
    /// nowhere. The store's, taken at `open`; a forest clears it as
    /// it closes.
    block_cache: ?*BlockCache,
    /// One bit per block: set when this table put the block into the
    /// block cache, so `purgeCachedBlocks` can drop exactly this
    /// file's entries without walking the cache. A set bit for a block
    /// the cache has since evicted on its own only costs a no-op remove.
    /// Atomic words, since readers on different workers insert
    /// concurrently.
    cached: []std.atomic.Value(u64),
    /// This table's share of `resident`, taken back when it is freed.
    resident: Resident,

    pub fn fileSize(self: *const Table) u64 {
        return self.obj.size();
    }

    pub const Extent = _idx.Extent;

    /// Open SST `file_id` from the store, with the store's block
    /// cache. A file store reads it in its own mode, readahead off.
    /// `extent` is what the manifest recorded at create: the open is
    /// then one read of the index and trailer, no stat and no trailer
    /// round trip.
    pub fn open(
        allocator: std.mem.Allocator,
        store: *vfs.Store,
        file_id: u64,
        extent: Extent,
    ) !*Table {
        const name = try sstName(allocator, file_id);
        defer allocator.free(name);
        const obj = try store.open(name, extent.size, .{ .random = true });
        const t = try openObject(allocator, obj, extent);
        t.file_id = file_id;
        t.block_cache = store.blockCache();
        return t;
    }

    /// A Table over an already-open object (a memory-backed handle in
    /// tests and benches): one read of [index_offset, size) — the index
    /// and its trailer. Takes ownership of `obj_in`, closing it on error
    /// or when the table is freed. file_id stays 0.
    pub fn openObject(
        allocator: std.mem.Allocator,
        obj_in: vfs.Object,
        extent: Extent,
    ) !*Table {
        var obj = obj_in;
        errdefer obj.close();

        const size = extent.size;
        if (size == 0 or size != obj.size()) return error.CorruptTable;
        if (extent.index_offset + _idx.trailer_len > size) {
            return error.CorruptTable;
        }
        const tail_len: usize = @intCast(size - extent.index_offset);
        const index_len: usize = tail_len - _idx.trailer_len;

        const t = try allocator.create(Table);
        errdefer allocator.destroy(t);

        const tail = try obj.readAligned(
            allocator,
            extent.index_offset,
            tail_len,
        );
        defer tail.free();

        if (tail.data.len < tail_len) return error.CorruptTable;
        const index_bytes = tail.data[0..index_len];
        const trailer = tail.data[index_len..tail_len];

        // Trailer: [u32 index_len][u64 index_hash]. The length must
        // agree with the manifest's extent; the hash covers the index.
        const trailer_len = _idx.readU32LE(trailer, 0) catch
            return error.CorruptTable;
        if (trailer_len != index_len) return error.CorruptTable;
        const stored_checksum = _idx.readU64LE(trailer, 4) catch
            return error.CorruptTable;
        if (zbuf.hash(index_bytes) != stored_checksum) {
            return error.ChecksumMismatch;
        }

        const index_reader = try _buffers.TableIndexReader.init(index_bytes, 0);
        var table_index = try index_reader.toStruct(allocator);
        // destroy, not deinit: toStruct hands back a created pointer, and
        // deinit frees only what the struct points at. Same call Table.deinit
        // makes on the success path.
        errdefer table_index.destroy(allocator);
        const cached = try allocator.alloc(
            std.atomic.Value(u64),
            (table_index.blocks.len + 63) / 64,
        );
        errdefer allocator.free(cached);
        for (cached) |*w| w.* = .init(0);

        // Refuse any SST whose entry encoding we don't speak. The block
        // checksums can't catch this: a stale table's blocks are internally
        // consistent, so they verify fine and then decode into keys shifted by
        // a byte. There is no migration path — wipe the data dir and reload.
        if (table_index.format_version != _idx.format_version) {
            log.err(
                "SST format_version={d}, expected {d}: wipe the data dir and reload",
                .{ table_index.format_version, _idx.format_version },
            );
            return error.UnsupportedTableFormat;
        }

        // Steal the fingerprints slice from the decoded index struct into a
        // BinaryFuse(u16) that we keep for the lifetime of this Table.  After
        // the transfer table_index.binary_fuse holds no heap memory (fingerprints is
        // &.{}) so index.deinit() becomes a no-op for that field.
        const binary_fuse: ?_binaryfuse.BinaryFuse(u16) = if (table_index.binary_fuse) |*bf| blk: {
            const filter = _binaryfuse.BinaryFuse(u16){
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

        // Copied out of the decoded field: zroar wants its buffer 8-byte
        // aligned, and a decoded field is not. The field's own copy is
        // then dead weight for the table's lifetime, so it is freed here
        // and left empty for index.deinit, as with the fingerprints.
        const egid_bm: ?zroar.Bitmap = if (table_index.egid_bitmap.len > 0)
            try zroar.Bitmap.fromBufferCopy(allocator, table_index.egid_bitmap)
        else
            null;
        errdefer if (egid_bm) |*bm| @constCast(bm).deinit();

        // The serialized index carries the filter and the bitmap too;
        // `index` is the rest: the block list and the `cached` bitset.
        const filter_bytes: u64 = if (binary_fuse) |*bf|
            bf.sizeInBytes()
        else
            0;
        const egid_bytes: u64 = table_index.egid_bitmap.len;
        const res: Resident = .{
            .index = (index_len -| (filter_bytes + egid_bytes)) +
                cached.len * @sizeOf(std.atomic.Value(u64)),
            .filter = filter_bytes,
            .egids = egid_bytes,
        };

        allocator.free(table_index.egid_bitmap);
        table_index.egid_bitmap = &.{};

        const smallest_raw = if (table_index.blocks.len > 0) table_index.blocks[0].key else &.{};
        t.* = .{
            .allocator = allocator,
            .obj = obj,
            .index = table_index,
            .binary_fuse = binary_fuse,
            .egid_bm = egid_bm,
            .resident = res,
            .smallest = if (smallest_raw.len >= min_key_len)
                Key{ .primary = smallest_raw }
            else
                .{},
            .biggest = if (table_index.last_key.len >= min_key_len)
                Key{ .primary = table_index.last_key }
            else
                .{},
            .ref = std.atomic.Value(i32).init(1),
            .file_id = 0,
            .extent = extent,
            .block_cache = null,
            .cached = cached,
        };
        resident.add(res);
        return t;
    }

    fn deinit(self: *Table) void {
        // The `cached` bits are the only record of which blocks this
        // table put into the block cache, and they go with the table.
        // Nobody can reach the table any more, so its blocks are dead
        // weight: drop them now, while the bits still say which they are.
        if (self.block_cache) |cache| _ = self.purgeCachedBlocks(cache);

        resident.sub(self.resident);
        self.allocator.free(self.cached);
        self.index.destroy(self.allocator);
        if (self.binary_fuse) |*bf| bf.deinit(self.allocator);
        if (self.egid_bm) |*bm| bm.deinit();
        self.obj.close();
    }

    /// Increment the reference count. Call this when sharing a Table pointer.
    pub fn incrRef(self: *Table) void {
        const prev = self.ref.fetchAdd(1, .monotonic);
        if (prev <= 0) @panic("Table.incrRef on a freed table");
    }

    /// Decrement the reference count. The last `decrRef` frees the table:
    /// index, filter, object handle and the `*Table` allocation itself,
    /// and the blocks it put into the block cache are dropped from it.
    /// Every holder of a `*Table` holds a ref (the level entry, the table
    /// cache, a reader that pinned it), so whoever releases last frees it,
    /// and nobody destroys a table by hand.
    ///
    /// This frees memory only; it never unlinks the SST file. File
    /// lifetime is the manifest's: the entry that removes a file's last
    /// listing retires it (`Manifest.decrSST`), and `Forest.deleteFiles`
    /// unlinks it once the node's snapshot index passes that entry.
    pub fn decrRef(self: *Table) void {
        const prev = self.ref.fetchSub(1, .acq_rel);
        if (prev <= 0) @panic("Table.decrRef below zero (double release)");
        if (prev == 1) {
            // The last decrRef is run by TableCache. So, this table has already
            // been evicted from that cache.
            const allocator = self.allocator;
            self.deinit();
            allocator.destroy(self);
        }
    }

    pub fn getIndex(self: *const Table) *const _buffers.TableIndexStruct {
        return &self.index;
    }

    /// Whether the table may hold the row with this primary key:
    /// certain for an `egid` table, probabilistic (no false negatives)
    /// for a `bytes` one, always true for a table without a filter.
    pub fn containsPrimary(self: *const Table, primary: String) bool {
        if (self.egid_bm) |*bm| return containsEgid(bm, primary);
        return self.containsHash(kv.bloomHash(primary));
    }

    /// `containsPrimary` for a caller that hashed the primary once
    /// (kv.bloomHash) and probes many tables with it.
    pub fn containsPrimaryHash(
        self: *const Table,
        primary: String,
        hash: u64,
    ) bool {
        if (self.egid_bm) |*bm| return containsEgid(bm, primary);
        return self.containsHash(hash);
    }

    fn containsEgid(bm: *const zroar.Bitmap, primary: String) bool {
        // Not an egid at all: an `egid` table cannot hold it.
        if (primary.len != 8) return false;
        return bm.contains(std.mem.readInt(u64, primary[0..8], .big));
    }

    fn containsHash(self: *const Table, hash: u64) bool {
        if (self.binary_fuse) |bf| return bf.contain(hash);
        return true;
    }

    pub fn blockCount(self: Table) usize {
        return self.index.blocks.len;
    }

    pub fn blockBaseKey(self: Table, block_idx: usize) Error!String {
        if (block_idx >= self.index.blocks.len) return error.CorruptTable;
        return self.index.blocks[block_idx].key;
    }

    /// The block a seek for `target` lands in: the last block whose base
    /// key is at or below it, block 0 when every base key is above, null
    /// for an empty table. The block `TableIterator.seekForward` opens,
    /// so a prefetch of it makes that seek a cache hit. One entry holds
    /// every version of a key, so a block never splits a key.
    pub fn blockFor(self: *const Table, target: Key) ?usize {
        const blocks = self.index.blocks;
        if (blocks.len == 0) return null;
        // First block whose base key > target.
        const Base = _buffers.BlockIndexStruct;
        const above = utils.search(Base, blocks, target, struct {
            fn f(sk: Key, b: Base) bool {
                return baseKeyOf(b).gtPrimary(sk);
            }
        }.f);
        return if (above > 0) above - 1 else 0;
    }

    /// The table's bytes on either side of `k`, from the block index
    /// alone: a block counts left when its base key is below `k`, else
    /// right, so the block `k` lands in goes left whole. Blocks are
    /// laid out back to back from offset 0, so a block's offset is the
    /// bytes before it. No I/O.
    pub fn splitAround(self: *const Table, k: Key) Sizes {
        const blocks = self.index.blocks;
        if (blocks.len == 0) return .{};
        const Base = _buffers.BlockIndexStruct;
        const from = utils.search(Base, blocks, k, struct {
            fn f(sk: Key, b: Base) bool {
                return baseKeyOf(b).gePrimary(sk);
            }
        }.f);
        const last = blocks[blocks.len - 1];
        const total: u64 = last.offset + last.len;
        const left: u64 = if (from < blocks.len) blocks[from].offset else total;
        return .{ .left = left, .right = total - left };
    }

    pub const Sizes = struct { left: u64 = 0, right: u64 = 0 };

    /// The block cache key: file_id in the high 64 bits, block_idx in
    /// the low 64 — no hashing, no collision. 0 = "skip cache" (file_id
    /// 0 is the in-memory / non-SST marker).
    fn cacheKey(self: *const Table, block_idx: usize) u128 {
        if (self.file_id == 0) return 0;
        return (@as(u128, self.file_id) << 64) | @as(u128, block_idx);
    }

    /// Record that block `idx` was put into the block cache.
    fn setCachedBit(self: *const Table, idx: usize) void {
        _ = self.cached[idx / 64].fetchOr(@as(u64, 1) << @intCast(idx % 64), .monotonic);
    }

    /// Drop every block this table put into `cache`. Run when the last
    /// ref on the table goes (`deinit`): with the table freed, its cached
    /// blocks would be dead weight until the queues happened to cycle
    /// past them. Clears the bits it acted on. Returns how many entries
    /// the cache still held.
    pub fn purgeCachedBlocks(self: *const Table, cache: *_cache.ShardedCache(u128)) usize {
        if (self.file_id == 0) return 0;

        var removed: usize = 0;
        for (self.cached, 0..) |*word, wi| {
            var bits = word.swap(0, .monotonic);
            while (bits != 0) {
                const bit: usize = @ctz(bits);
                bits &= bits - 1;
                if (cache.remove(self.cacheKey(wi * 64 + bit))) removed += 1;
            }
        }
        return removed;
    }

    /// Checksum and decode one block's stored bytes into a ref-counted
    /// Value holding its payload; the one ref is the caller's. The
    /// block is a `buffers.Block`: its crc, checked over the bytes as
    /// stored before any decoding, then its own codec byte saying how.
    fn decodeBlock(self: *const Table, raw: String) Error!*_cache.Value {
        // The envelope must be there before the reader is trusted.
        if (raw.len < _buffers.BlockReader.payload_off)
            return error.CorruptTable;

        const block = _buffers.BlockReader.init(raw);
        // Everything after the crc: the codec byte and the payload.
        const hashed = raw[_buffers.BlockReader.codec_off..];
        if (block.crc() != zbuf.hash(hashed)) return error.ChecksumMismatch;
        const codec = std.enums.fromInt(compress.Codec, block.codec()) orelse
            return error.CorruptTable;

        var out = std.ArrayList(u8).empty;
        defer out.deinit(self.allocator);
        _ = try compress.decompress(
            self.allocator,
            codec,
            block.payload(),
            &out,
        );
        const owned_data = try out.toOwnedSlice(self.allocator);
        errdefer self.allocator.free(owned_data);
        return _cache.Value.init(self.allocator, owned_data) catch
            return error.OutOfMemory;
    }

    pub fn readBlock(self: *const Table, block_idx: usize) Error!*_cache.Value {
        return self.readBlock_(self.block_cache, block_idx);
    }

    /// `readBlock` through `cache` instead of the table's own.
    pub fn readBlock_(
        self: *const Table,
        cache: ?*_cache.ShardedCache(u128),
        block_idx: usize,
    ) Error!*_cache.Value {
        if (block_idx >= self.index.blocks.len) return error.CorruptTable;

        // A bypass reader (compaction) takes a resident block without
        // marking it reused, and keeps the blocks it reads out of the
        // cache: each is wanted once.
        const bypass = fiber.ctx().cache == .bypass;
        const cache_key = self.cacheKey(block_idx);
        if (cache != null and cache_key != 0) {
            const hit: _cache.Hit = if (bypass) .once else .reuse;
            if (cache.?.get(cache_key, hit)) |val| return val; // incrRef'd.
        }

        const bi = self.index.blocks[block_idx];
        if (bi.len < 4) return error.CorruptTable;

        // Read raw bytes (temporary)
        const result = self.obj.readAligned(
            self.allocator,
            @intCast(bi.offset),
            bi.len,
        ) catch return error.CorruptTable;
        defer result.free();

        const val = try self.decodeBlock(result.data);
        errdefer val.decrRef();

        // Cache insert (best-effort); put incrRefs on success → ref=2
        if (cache != null and cache_key != 0 and !bypass) {
            if (cache.?.put(cache_key, val)) |_| {
                self.setCachedBit(block_idx);
            } else |err| {
                log.debug("block cache put failed: {s}; file_id={d} " ++
                    "block={d}", .{ @errorName(err), self.file_id, block_idx });
            }
        }

        return val; // caller holds ref (1 if not cached, or 1+cache_ref if cached)
    }

    /// Read the blocks of `idxs` (ascending, no repeats) that `cache` does
    /// not hold, in one io_uring submission, and insert them into it, so
    /// the seeks that follow hit. Returns how many were read. The batch
    /// is capped at half the cache's admission budget, estimated by the
    /// table's mean decompressed block size: a new entry sits in the
    /// small queue until its first hit, and a burst past that budget
    /// would evict its own earlier blocks before their seeks. Blocks
    /// past the cap are left to the seeks, in key order. A no-op for a
    /// table with no cache key or whose file is in memory (no I/O to
    /// batch).
    pub fn prefetchBlocks(
        self: *const Table,
        cache: *_cache.ShardedCache(u128),
        allocator: std.mem.Allocator,
        idxs: []const usize,
    ) Error!usize {
        if (self.file_id == 0) return 0;
        if (self.obj.isResident()) return 0;

        const blocks = self.index.blocks;
        if (blocks.len == 0) return 0;

        const per_block = @max(self.index.uncompressed_size / blocks.len, 1);
        const max_blocks = cache.admissionBudget() / 2 / per_block;
        if (max_blocks == 0) return 0;

        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const arloc = arena.allocator();

        var reqs: std.ArrayList(vfs.FileHandle.ReadRequest) = .empty;
        var wanted: std.ArrayList(usize) = .empty;
        for (idxs) |idx| {
            if (wanted.items.len == max_blocks) break;
            if (idx >= blocks.len) return error.CorruptTable;
            if (cache.get(self.cacheKey(idx), .reuse)) |val| {
                val.decrRef();
                continue;
            }
            const bi = blocks[idx];
            if (bi.len < 4) return error.CorruptTable;
            try reqs.append(arloc, .{
                .offset = @intCast(bi.offset),
                .len = bi.len,
            });
            try wanted.append(arloc, idx);
        }
        if (reqs.items.len == 0) return 0;

        self.obj.batchRead(&arena, reqs.items) catch
            return error.CorruptTable;

        for (reqs.items, wanted.items) |req, idx| {
            const val = try self.decodeBlock(req.data);
            defer val.decrRef(); // the cache holds its own ref
            //
            if (cache.put(self.cacheKey(idx), val)) |_| {
                self.setCachedBit(idx);
            } else |err| {
                log.debug("block cache put failed: {s}; file_id={d} " ++
                    "block={d}", .{ @errorName(err), self.file_id, idx });
            }
        }
        return wanted.items.len;
    }
};

/// BlockIterator decodes entries from a single block lazily, one at a
/// time: one entry is one key with every version of it. Block data is
/// caller-owned; key_buf and vers are self-owned.
///
/// The block is a decoded `buffers.BlockPayload` (builder.zig has the
/// layout), read through the generated packed readers. Those trust
/// their bytes: decodeBlock verified the block's crc, so the only
/// checks left here are the ones a well-formed block can still fail
/// by meaning, an overlap past the base key, an empty key, version
/// offsets that do not tile the values, and an end table that runs
/// backwards, which zbuf reports as Invalid.
pub const BlockIterator = struct {
    alloc: std.mem.Allocator,
    /// The current entry's stored key: the primary.
    key_buf: std.ArrayList(u8),
    /// Stamped onto every emitted key: the tree this block is read for.
    table_key: TableKey,
    /// The payload's entries, located once (a `buffers.BlockPayload`).
    entries: zbuf.ArrayVar(_buffers.BlockEntryReader),
    base_key: String,
    entry_idx: usize,
    /// The current entry's versions, newest first, decoded by setIdx
    /// from its packed value; the values are slices into the block.
    vers: std.ArrayList(Version),
    is_valid: bool,

    pub fn deinit(self: *BlockIterator) void {
        self.vers.deinit(self.alloc);
        self.key_buf.deinit(self.alloc);
    }

    pub fn init(
        alloc: std.mem.Allocator,
        block: String,
        base_key: String,
        table_key: TableKey,
    ) !BlockIterator {
        // The payload's one field is the entries array: [u32 count]
        // [u32 end × count][entries]. The count is read before anything
        // is trusted through it: the table must fit, and the last end
        // must be the region's length, so no entry can reach past the
        // block. See the layout diagram at the top of builder.zig.
        if (block.len < 4) return error.CorruptTable;
        const payload = _buffers.BlockPayloadReader.init(block);
        const entry_count = payload.entries().len();
        if (entry_count == 0) return error.CorruptTable;
        const table_len = 4 + @as(u64, entry_count) * 4;
        if (table_len > block.len) return error.CorruptTable;
        if (payload.size() != block.len) return error.CorruptTable;

        return .{
            .alloc = alloc,
            .key_buf = .empty,
            .table_key = table_key,
            .entries = payload.entries(),
            .base_key = base_key,
            .entry_idx = 0,
            .vers = .empty,
            .is_valid = false,
        };
    }

    pub fn seekToFirst(self: *BlockIterator) Error!void {
        try self.setIdx(0);
    }

    /// Entries in the block; at least one (init refuses an empty one).
    pub fn entryCount(self: *const BlockIterator) usize {
        return self.entries.len();
    }

    pub fn seekToLast(self: *BlockIterator) Error!void {
        try self.setIdx(self.entryCount() - 1);
    }

    pub fn next(self: *BlockIterator) Error!void {
        try self.setIdx(self.entry_idx + 1);
    }

    /// A stored key, read in place: base_key[0..overlap] ++ diff, without
    /// pasting the two together. Lets a search compare an entry without
    /// materializing it — the whole point of seeking this way.
    const StoredKey = struct {
        base: String,
        diff: String,

        fn len(self: StoredKey) usize {
            return self.base.len + self.diff.len;
        }

        fn at(self: StoredKey, i: usize) u8 {
            if (i < self.base.len) return self.base[i];
            return self.diff[i - self.base.len];
        }
    };

    /// Read entry i's key without copying it.
    fn storedKeyAt(self: *BlockIterator, i: usize) Error!StoredKey {
        const entry = try self.entries.at(@intCast(i));
        const overlap: usize = entry.keyOverlap();
        const diff = entry.keyDiff();
        if (overlap > self.base_key.len) return error.CorruptTable;
        if (overlap + diff.len < min_key_len) return error.CorruptTable;
        return .{ .base = self.base_key[0..overlap], .diff = diff };
    }

    /// Order entry i against a target primary, bytewise: `Key.orderPrimary`
    /// without `after` (only primaries are stored, and they can differ in
    /// length, where shorter-is-less is what we want).
    ///
    /// `shared` is how many leading bytes the TARGET shares with
    /// base_key. Bytes below it that the entry also takes from base_key
    /// are equal on both sides and need no comparing — hence the start at
    /// min(shared, overlap): past `overlap` the entry leaves the base key
    /// and may differ there, so skipping further would miss it. Keys in a
    /// block share a long prefix, so the loop normally runs a byte or two.
    fn cmpStoredKey(
        self: *BlockIterator,
        i: usize,
        target_key: String,
        shared_w_base: usize,
    ) Error!std.math.Order {
        const sk = try self.storedKeyAt(i);
        const slen = sk.len();
        const tlen = target_key.len;

        var at = @min(shared_w_base, sk.base.len);
        const end = @min(slen, tlen);
        while (at < end) : (at += 1) {
            // byte by byte comparison
            const a = sk.at(at);
            const b = target_key[at];
            if (a != b) return if (a < b) .lt else .gt;
        }
        return std.math.order(slen, tlen);
    }

    /// Binary search for the first entry with key >= target. `traw` is
    /// Seek to the first entry whose primary is at or past `target_key`,
    /// a primary. Probes compare stored bytes in place; only the landing
    /// entry is materialized.
    pub fn seekGe(self: *BlockIterator, target_key: String) Error!void {
        return self.seekGeFrom(target_key, 0);
    }

    /// `seekGe` over the entries from `from` on: a caller that knows the
    /// target is at or past its current entry narrows the search to
    /// what follows.
    pub fn seekGeFrom(
        self: *BlockIterator,
        target_key: String,
        from: usize,
    ) Error!void {
        const lo = try self.lowerBound(target_key, from);
        if (lo < self.entryCount()) {
            try self.setIdx(lo);
        } else {
            self.is_valid = false;
        }
    }

    /// Seek to the last entry whose primary is below `target_key`.
    pub fn seekLt(self: *BlockIterator, target_key: String) Error!void {
        const lo = try self.lowerBound(target_key, 0);
        if (lo == 0) {
            self.is_valid = false;
        } else {
            try self.setIdx(lo - 1);
        }
    }

    /// The index of the first entry at or past `target_key` among the
    /// entries from `from` on; the count when every one is below it.
    /// Probes compare stored bytes in place.
    fn lowerBound(
        self: *BlockIterator,
        target_key: String,
        from: usize,
    ) Error!usize {
        // This binary search is only possible because we're diffing keys
        // against the base key and not against the previous key.
        if (target_key.len == 0) return 0;
        if (target_key.len < min_key_len) return error.CorruptTable;

        // Bytes the target shares with the base key, computed once for
        // the whole search; cmpEntry decides per entry how much of that
        // it can actually skip.
        const shared = std.mem.findDiff(u8, target_key, self.base_key) orelse
            @min(target_key.len, self.base_key.len);

        var lo: usize = from;
        var hi: usize = self.entryCount();
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const ord = try self.cmpStoredKey(mid, target_key, shared);
            if (ord == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    pub fn prev(self: *BlockIterator) Error!void {
        if (self.entry_idx == 0) {
            self.is_valid = false;
            return;
        }
        try self.setIdx(self.entry_idx - 1);
    }

    pub fn itemRef(self: *const BlockIterator) ?Item {
        if (!self.is_valid) return null;
        // Built per call rather than cached in setIdx: a few loads off a
        // hot key_buf, where caching costs a 40-byte store plus a reload,
        // and every caller reads it about once per position. Measured:
        // caching made a key walk ~20% SLOWER.
        return .{
            .key = .{
                .namespace = self.table_key.namespace,
                .table_id = self.table_key.table_id,
                .primary = self.key_buf.items,
            },
            .versions = .{ .many = self.vers.items },
        };
    }

    fn setIdx(self: *BlockIterator, i: usize) Error!void {
        if (i >= self.entryCount()) {
            self.is_valid = false;
            return;
        }

        const entry = try self.entries.at(@intCast(i));
        const key_overlap: usize = entry.keyOverlap();
        const diff = entry.keyDiff();
        if (key_overlap > self.base_key.len) return error.CorruptTable;
        if (key_overlap + diff.len < min_key_len) return error.CorruptTable;

        // Reconstruct full key into reusable buffer: base[0..overlap] ++ diff.
        utils.clearRetainCap(&self.key_buf);
        try self.key_buf.appendSlice(self.alloc, self.base_key[0..key_overlap]);
        try self.key_buf.appendSlice(self.alloc, diff);

        try self.unpackVersions(entry);
        self.entry_idx = i;
        self.is_valid = true;
    }

    /// Decode an entry's versions (see builder.zig) into `vers`: each
    /// header's `off` is checked to start where the previous version's
    /// bytes ended, so the versions tile the entry's values, and a
    /// tombstone's offset equals the next one (no bytes).
    fn unpackVersions(
        self: *BlockIterator,
        entry: _buffers.BlockEntryReader,
    ) Error!void {
        const vers = entry.versions();
        const count: usize = vers.len();
        if (count == 0) return error.CorruptTable;
        const bytes = entry.values();

        utils.clearRetainCap(&self.vers);
        try self.vers.ensureTotalCapacity(self.alloc, count);

        if (count == 1) {
            // Nearly every entry after compaction: the value is the
            // whole bytes region.
            const hdr = vers.at(0) catch unreachable;
            if (hdr.off() != 0) return error.CorruptTable;
            self.vers.appendAssumeCapacity(.{
                .ts = hdr.ts(),
                .meta = Meta.fromByte(hdr.meta()),
                .value = bytes,
            });
            return;
        }

        var off: usize = 0;
        for (0..count) |v| {
            const hdr = vers.at(@intCast(v)) catch unreachable;
            const start: usize = hdr.off();
            if (start != off) return error.CorruptTable;

            const end: usize = if (v + 1 < count)
                (vers.at(@intCast(v + 1)) catch unreachable).off()
            else
                bytes.len;
            if (end < start or end > bytes.len) return error.CorruptTable;

            self.vers.appendAssumeCapacity(.{
                .ts = hdr.ts(),
                .meta = Meta.fromByte(hdr.meta()),
                .value = bytes[start..end],
            });
            off = end;
        }
    }
};

/// TableIterator navigates all entries across all blocks in a table.
/// Walks one table's entries for one tree. An SST stores primaries only:
/// a file can be listed by more than one tree (a split shares them), so
/// the tree opening the iterator says which namespace and table_id its
/// keys carry, and every emitted key is stamped with them. Seeks take
/// keys of that tree (asserted) and use their primaries; a table edge
/// (`Key.tableLeft`/`tableRight`) is no bound on its side.
pub const TableIterator = struct {
    allocator: std.mem.Allocator,
    table: *Table,
    /// Stamped onto every emitted key.
    table_key: TableKey,
    bpos: usize,
    bi: ?BlockIterator,
    current_block: ?*_cache.Value = null,
    valid: bool,
    dir: Direction,
    /// Kept from `seekRange`, bounds borrowed: the walk ends at the
    /// bound ahead of it, `right` forward and `left` in reverse.
    range: KeyRange = .all,
    /// The block that bound falls in, when it is inside the table:
    /// blocks past it are never opened. Null when it is not.
    stop_bpos: ?usize = null,
    /// Set by a reader that walks the table once (compaction): raw
    /// bytes are read ahead in spans of up to this many bytes, whole
    /// blocks, and decoded out of the span, so the table costs a few
    /// large reads rather than one per block. The block cache is not
    /// consulted or filled. 0 reads block by block through `readBlock`.
    window_bytes: u64 = 0,
    window: ?vfs.ReadResult = null,
    pub fn init(
        allocator: std.mem.Allocator,
        table: *Table,
        table_key: TableKey,
        dir: Direction,
    ) TableIterator {
        table.incrRef();
        return .{
            .allocator = allocator,
            .table = table,
            .table_key = table_key,
            .dir = dir,
            .bpos = 0,
            .bi = null,
            .current_block = null,
            .valid = false,
        };
    }

    pub fn deinit(self: *TableIterator) void {
        self.table.decrRef();
        if (self.bi) |*bi| bi.deinit();
        if (self.current_block) |b| b.decrRef();
        if (self.window) |w| w.free();
    }

    /// Position at the first entry of `[start, end)` in the iterator's
    /// direction: the first at or past `start` forward, the last below
    /// `end` in reverse. `Key.inf` or a table edge on a side is no
    /// bound there. The bound the walk ends at is borrowed, checked on
    /// every `next`: its primary must outlive the iterator's use of it,
    /// as the skiplist iterator's must.
    pub fn seekRange(self: *TableIterator, start: Key, end: Key) !void {
        if (!start.isInf()) self.assertOwn(start);
        if (!end.isInf()) self.assertOwn(end);
        // The read-ahead window walks a table once, forward.
        std.debug.assert(self.dir == .forward or self.window_bytes == 0);

        // The block of the bound ahead, when it is inside the table; a
        // bound beyond the table on the far side means nothing to
        // return.
        self.range = .{ .left = start, .right = end };
        self.stop_bpos = null;
        switch (self.dir) {
            .forward => if (!end.isInf() and !end.isTableEdge()) {
                if (end.lePrimary(self.table.smallest)) {
                    self.valid = false;
                    return;
                }
                if (end.lePrimary(self.table.biggest)) {
                    self.stop_bpos = self.table.blockFor(end) orelse 0;
                }
            },
            .reverse => if (!start.isInf() and !start.isTableEdge()) {
                if (start.gtPrimary(self.table.biggest)) {
                    self.valid = false;
                    return;
                }
                if (start.gtPrimary(self.table.smallest)) {
                    self.stop_bpos = self.table.blockFor(start) orelse 0;
                }
            },
        }

        // Seek. The landing item can be past the stop bound (a start
        // beyond the last in-range key lands on whatever follows), so
        // the bound applies here just as it does after every next().
        switch (self.dir) {
            .forward => if (start.isInf() or start.isTableEdge()) {
                try self.seekToFirst();
            } else {
                try self.seekForward(start);
            },
            .reverse => if (end.isInf() or end.isTableEdge()) {
                try self.seekToLast();
            } else {
                try self.seekBackward(end);
            },
        }
        self.checkStopKey();
    }

    /// A seek key must be a key of the tree this iterator reads for; one
    /// of another table is a caller's bug, never a case to handle.
    fn assertOwn(self: *const TableIterator, key: Key) void {
        std.debug.assert(key.namespace == self.table_key.namespace and
            key.table_id == self.table_key.table_id);
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

    fn seekToLast(self: *TableIterator) Error!void {
        const count = self.table.blockCount();
        if (count == 0) {
            self.valid = false;
            return;
        }
        try self.setBlock(count - 1);
        try self.bi.?.seekToLast();
        self.valid = self.bi.?.is_valid;
    }

    /// Position at the last entry below `target`: in the block a seek
    /// for it lands in, else the last entry of the block before, since
    /// every entry of the landing block is then at or past the target.
    fn seekBackward(self: *TableIterator, target: Key) Error!void {
        self.valid = false;
        const lo = self.table.blockFor(target) orelse return;
        if (self.bi == null or self.bpos != lo) try self.setBlock(lo);
        try self.bi.?.seekLt(target.primary);
        if (self.bi.?.is_valid) {
            self.valid = true;
            return;
        }
        if (lo > 0) {
            try self.setBlock(lo - 1);
            try self.bi.?.seekToLast();
            self.valid = self.bi.?.is_valid;
        }
    }

    fn seekForward(self: *TableIterator, target: Key) Error!void {
        self.valid = false;
        const blocks = self.table.index.blocks;
        const lo = self.table.blockFor(target) orelse return;

        // The in-block search compares stored primaries against this.
        const traw = target.primary;

        // The block is still open from the last seek: search it in
        // place, with no cache lookup and no iterator rebuild, and from
        // the current entry on when the target is not behind it.
        var from: usize = 0;
        if (self.bi != null and self.bpos == lo) {
            if (self.bi.?.itemRef()) |cur| {
                if (cur.key.lePrimary(target)) from = self.bi.?.entry_idx;
            }
        } else {
            try self.setBlock(lo);
        }
        try self.bi.?.seekGeFrom(traw, from);
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

    /// Step in the iterator's direction: within the block, else into
    /// the neighbouring block, unless the stop bound says that block
    /// holds nothing in range, which saves fetching it.
    pub fn next(self: *TableIterator) Error!void {
        if (!self.valid) return;
        defer self.checkStopKey(); // Always do it.

        switch (self.dir) {
            .forward => {
                try self.bi.?.next();
                if (self.bi.?.is_valid) return;
                const next_bpos = self.bpos + 1;
                if (self.stop_bpos) |stop_bpos| if (next_bpos > stop_bpos) {
                    self.valid = false;
                    return;
                };
                if (next_bpos >= self.table.blockCount()) {
                    self.valid = false;
                    return;
                }
                try self.setBlock(next_bpos);
                try self.bi.?.seekToFirst();
            },
            .reverse => {
                try self.bi.?.prev();
                if (self.bi.?.is_valid) return;
                if (self.bpos == 0) {
                    self.valid = false;
                    return;
                }
                const prev_bpos = self.bpos - 1;
                if (self.stop_bpos) |stop_bpos| if (prev_bpos < stop_bpos) {
                    self.valid = false;
                    return;
                };
                try self.setBlock(prev_bpos);
                try self.bi.?.seekToLast();
            },
        }
        self.valid = self.bi.?.is_valid;
    }

    /// Invalidates the iterator once the current item is past the
    /// bound ahead: at or past `right` forward, below `left` in reverse.
    inline fn checkStopKey(self: *TableIterator) void {
        if (!self.valid) return;
        const stop_bpos = self.stop_bpos orelse return;

        const past_block = switch (self.dir) {
            .forward => self.bpos > stop_bpos,
            .reverse => self.bpos < stop_bpos,
        };
        if (past_block) {
            self.valid = false;
            return;
        }
        if (self.bpos != stop_bpos) return;
        const item = self.bi.?.itemRef() orelse return;
        if (!self.range.contains(item.key)) self.valid = false;
    }

    pub fn itemRef(self: *const TableIterator) ?Item {
        if (!self.valid) return null;
        return self.bi.?.itemRef();
    }

    /// The refcounted block backing the current item's value, or null
    /// when not positioned. A caller that keeps item.value past next()
    /// incrRefs this pin (and decrRefs when done) instead of copying
    /// the bytes — the block stays alive even after the iterator
    /// advances or dies.
    pub fn currentBlock(self: *const TableIterator) ?*_cache.Value {
        if (!self.valid) return null;
        return self.current_block;
    }

    fn setBlock(self: *TableIterator, bpos: usize) Error!void {
        // Null before the read: a failed one returns with nothing for
        // `deinit` to free a second time.
        if (self.bi) |*bi| bi.deinit();
        self.bi = null;
        if (self.current_block) |b| b.decrRef();
        self.current_block = null;

        const val = if (self.window_bytes > 0)
            try self.readWindowed(bpos)
        else
            try self.table.readBlock(bpos);
        self.current_block = val;

        const base_key = try self.table.blockBaseKey(bpos);
        self.bi = try BlockIterator.init(
            self.allocator,
            val.data,
            base_key,
            self.table_key,
        );
        self.bpos = bpos;
    }

    /// Block `bpos` decoded out of the window, which is read anew when
    /// it does not hold the block: from the block's start through the
    /// last whole block within `window_bytes`, the block itself at
    /// least. Blocks are laid out back to back, so the span is exact.
    fn readWindowed(self: *TableIterator, bpos: usize) Error!*_cache.Value {
        const blocks = self.table.index.blocks;
        if (bpos >= blocks.len) return error.CorruptTable;

        const block = blocks[bpos];
        const held = if (self.window) |w|
            block.offset >= w.offset and
                block.offset + block.len <= w.offset + w.data.len
        else
            false;

        if (!held) {
            if (self.window) |w| w.free();
            self.window = null;

            var len: usize = block.len;
            for (blocks[bpos + 1 ..]) |cur| {
                if (len + cur.len > self.window_bytes) break;
                len += cur.len;
            }
            self.window = self.table.obj.readAligned(
                self.allocator,
                block.offset,
                len,
            ) catch return error.CorruptTable;
        }
        const at: usize = @intCast(block.offset - self.window.?.offset);
        return self.table.decodeBlock(self.window.?.data[at..][0..block.len]);
    }
};
