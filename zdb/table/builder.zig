// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const kv = @import("../kv.zig");
const Key = @import("../kv.zig").Key;
const Item = @import("../kv.zig").Item;
const Version = @import("../kv.zig").Version;
const _idx = @import("index.zig");
const _buffers = @import("../buffers.gen.zig");
const utils = @import("utils");
const String = utils.String;
const _compress = utils.compression;
const _binaryfuse = @import("../binaryfusefilter.zig");
const zroar = @import("zroar");
const vfs = @import("vfs");
const zbuf = @import("zbuf").zbuf;

const log = std.log.scoped(.table_builder);

// Table layout (Badger-inspired, with zbuf for TableIndex/BlockOffset):
//
// [Block 0]
// [Block 1]
// ...
// [Block N]
// [zero padding, so the file length is a multiple of vfs.page_size]
// [TableIndex bytes]
// [u32 index_len]
// [u64 index_hash]
//
// A block on disk is an envelope around its payload, `buffers.Block`:
// | ------------------------------------------------------------------------|
// | -- block crc u64 -- | -- codec u8 -- | -- payload -- |
// | ------------------------------------------------------------------------|
// The crc (xxhash3, zbuf.hash) covers the codec byte and the payload as
// stored, so a reader verifies a block before decoding it and a scrub
// verifies a table without decoding anything. The codec byte says how
// THIS block's payload is stored; blocks of one table may differ (see
// Config.codec). The payload, decoded, is a `buffers.BlockPayload`, one
// packed array of variable-size entries:
// | ------------------------------------------------------------------------|
// | -- entry count u32 -- |
// | --- end offset of each entry, u32 each, entry_count of them --- |
// | --- entries region --- |
// | ------------------------------------------------------------------------|
//
// The table holds where each entry ENDS within the entries region, so
// entry i is [end(i-1), end(i)) with end(-1) = 0, and the last end is
// the region's length: a reader bounds every entry from the table alone.
//
// Each entry is one key with every version of it, a `buffers.BlockEntry`:
// | ----------------------------------------------------------------------|
// | -- u16 key overlap with base -- | -- u16 key diff length from base -- |
// | -- key diff bytes -- | -- packed versions ------------------------- |
// | ----------------------------------------------------------------------|
//
// The packed versions are, newest first, a count of `buffers.BlockVersion`
// headers and then the bytes of every version:
// | -- u32 count -- | count x { u64 ts, u8 meta, u32 off } -- | -- bytes -- |
// where version i's value is bytes[off_i .. off_(i+1)), the last one's
// runs to the end, and off_0 is 0. A tombstone is a version with no bytes.
// The headers are fixed width, so the version at or below a read
// timestamp is a binary search, and a value is a slice into the block.
//
// The packed records' generated readers and writers (zdb/buffers.gen.zig)
// are the layout's source of truth: `addItem` writes an entry with
// `BlockEntryWriter`, `finishBlock` the payload's table and envelope by
// hand because both stream (the ends are known before the entries are
// laid down, the envelope before the payload it covers).
//
// Notes:
// - Everything needed to locate anything sits at the FRONT, in a header of
//   known shape: the entry count, then the ends. The reader walks it
//   forward at fixed strides instead of unwinding a chain of subtractions
//   from the block's end. The envelope's crc covers every byte after it,
//   so it is written last, backpatched into the 8 bytes reserved for it.
//
// - Entry ends are relative to the entries region, so they need no
//   rewriting once the header's size (which follows from the entry count)
//   is known.
//
// - Keys and values are interleaved per entry rather than split into
//   separate regions. Splitting them was tried and reverted: it cost 4
//   bytes per entry for a parallel value-offset array (~9% fewer keys per
//   block) and measured no gain, because reading a block already streams
//   every one of its bytes through cache — decompress, checksum, then copy
//   into the block cache — so a key walk finds the value bytes resident
//   whatever the layout. Separation only pays where it lets you avoid
//   LOADING bytes, which within one block it never does.
//
// - An entry's stored key is the primary alone (the namespace and
//   table_id are the tree's, see addItem); a version's timestamp lives
//   in its packed header. Entries ascend by key, which is bytewise order.
//
// - Keys are prefix-compressed against the block base key (first key in block),
//   not against the previous key. This is useful because it allows us random
//   access to any idx on the block entry, and derive its key.
//
// - An entry's value length is implicit — it runs to the entry's end —
//   so the packed versions carry their own count and offsets up front,
//   where the reader's bounds checks cover them before any value math.
//
// - Checksums are xxhash3 u64 (zbuf.hash) over the block's stored bytes
//   after the checksum field: the codec byte and the payload, compressed
//   or not. A corrupt block is rejected before any decoding.
// - TableIndex (zbuf) contains block offsets, max_version, key_count, sizes,
//   format_version, and a binary fuse filter over the keys' primaries.
// - All integer fields are little-endian. (Keys marshal big-endian — see
//   kv.zig — so they order correctly bytewise; that's a separate concern.)

pub const Error = error{
    KeyOrderViolation,
    KeyTooLarge,
    ValueTooLarge,
    TooManyBlocks,
    IntegerOverflow,
    /// A key of a rows tree that is not an 8-byte egid.
    KeyNotEgid,
    UnknownTreeKind,
} || _buffers.Error || _compress.Error || std.mem.Allocator.Error ||
    _binaryfuse.Error;

pub const Config = struct {
    /// A block is cut once it reaches this. The forest passes its own
    /// `block_bytes`; the default matches it for a builder on its own.
    block_bytes: usize = 64 * 1024,
    /// The codec the table is asked for. Each block carries its own
    /// codec byte, and whether a block is stored compressed, and
    /// whether later blocks are tried at all, is the `Election`'s.
    codec: _compress.Codec = .none,
    /// The tree the table is built for (`buffers.TreeKind`), which
    /// picks its filter. A rows table keeps its keys, 8-byte big-endian
    /// egids, exactly in a roaring bitmap: a miss is certain, a hit is
    /// certain, and a table of dense ids costs 8 KiB per 64k-id chunk,
    /// against about two bytes per key for a fuse filter (sparse ids
    /// cost the same two bytes each). An index table hashes its keys
    /// into a binary fuse filter, the default: a table built for no
    /// tree in particular (tests, tools) gets the filter that holds
    /// for any key.
    kind: _buffers.TreeKindEnum = .posting_index,
};

/// A table's block compression: the scratch it runs in, and whether
/// it is worth it, decided per block and once for the table. A block
/// that saves less than `min_saving` of its bytes is stored raw, so a
/// mixed table keeps compression on the blocks that earn it. The table
/// also elects whether to keep trying: a payload that does not
/// compress (random bytes, base64 of them) still costs the codec's
/// full pass, and on such data lz4 was three quarters of a flush's
/// CPU and zstd two thirds of a compaction's for nothing, so once the
/// first `elect_bytes` of blocks have saved less than `min_saving` in
/// total, no later block is compressed. A reader takes each block's
/// codec byte; nothing records the table's.
pub const Compression = struct {
    alloc: std.mem.Allocator,
    /// Where a block is compressed; `compress` returns it when the
    /// block is stored compressed.
    buf: std.ArrayList(u8) = .empty,
    /// The codec the table was configured with, and asks for until the
    /// election.
    configured: _compress.Codec,
    /// The election's choice, once held: `configured` or `.none`.
    elected: ?_compress.Codec = null,
    /// The sample: raw and stored totals of every block so far. A
    /// block stored raw counts as no saving.
    sample_in: u64 = 0,
    sample_out: u64 = 0,

    /// The least a block, and the sample, must shrink for the codec to
    /// be worth it: one byte in eight. Base64 of random bytes sits
    /// right at this line under zstd (six bits of entropy per byte, so
    /// about a quarter saved) and well under it with lz4 (no repeats
    /// to find).
    pub const min_saving: f64 = 1.0 / 8.0;

    /// Raw bytes of blocks the election looks at before deciding: 16
    /// blocks at the default block size, 3% of a 32 MiB L1 table and
    /// under 1% of a flush's. A table with less than this never holds
    /// one: every block of it is tried.
    pub const elect_bytes: usize = 1 << 20;

    pub fn init(alloc: std.mem.Allocator, codec: _compress.Codec) Compression {
        return .{ .alloc = alloc, .configured = codec };
    }

    pub fn deinit(self: *Compression) void {
        self.buf.deinit(self.alloc);
    }

    /// The next table holds its own election; the scratch keeps its
    /// capacity.
    pub fn reset(self: *Compression) void {
        utils.clearRetainCap(&self.buf);
        self.elected = null;
        self.sample_in = 0;
        self.sample_out = 0;
    }

    /// A block's `payload` as the block stores it: compressed with the
    /// codec the table asks for when that shrinks it enough, else the
    /// payload itself under `.none` (`compress.maybePack`). The block
    /// also joins the sample until the election, which is held as soon
    /// as the sample is large enough. The bytes are valid until the
    /// next call.
    pub fn compress(
        self: *Compression,
        payload: String,
    ) Error!_compress.Packed {
        const codec = self.elected orelse self.configured;
        // Per block, whatever the table decides: any size is tried.
        const stored = try _compress.maybePack(self.alloc, payload, .{
            .codec = codec,
            .min_bytes = 0,
            .min_saving = min_saving,
        }, &self.buf);

        // The election, over the sample's totals so one odd block does
        // not decide alone: a codec that has not earned its pass is
        // not tried on any later block.
        if (self.elected == null and codec != .none) {
            self.sample_in += payload.len;
            self.sample_out += stored.data.len;
            if (self.sample_in >= elect_bytes) {
                const earned = _compress.worthKeeping(
                    self.sample_in,
                    self.sample_out,
                    min_saving,
                );
                self.elected = if (earned) codec else .none;
            }
        }
        return stored;
    }
};

const BlockInfo = struct {
    // base_key is the first key of the block. It is used to prefix-decode
    // all keys in that block.
    base_key: []u8,
    // offset/len point to the finalized block bytes inside table_buf.
    offset: u32,
    len: u32,
};

pub const TableBuffer = struct {
    allocator: std.mem.Allocator,
    // Page-aligned so `RemoteStore.put` (O_DIRECT in a file store) can take
    // it as is: the buffer ADDRESS as well as its length must be page
    // aligned, and `finish()` already pads the length to a page multiple.
    bytes: []align(vfs.page_size) u8,
    /// Where the index starts; with `bytes.len` it is the table's
    /// extent, recorded in the manifest so an open is one read.
    index_offset: u64,

    pub fn extent(self: *const TableBuffer) _idx.Extent {
        return .{ .size = self.bytes.len, .index_offset = self.index_offset };
    }

    pub fn deinit(self: *TableBuffer) void {
        self.allocator.free(self.bytes);
    }
};

pub const Builder = struct {
    allocator: std.mem.Allocator,
    cfg: Config,
    compression: Compression,

    /// The table so far: every finished block, then at `finish` the
    /// index and trailer. Chunked so that growing never copies what
    /// is already written (an ArrayList spent two thirds of a flush's
    /// `addItem` time copying itself at doublings) and needs no guess
    /// of the table's size: the one copy is into the page-aligned
    /// result.
    table_buf: utils.ChunkBuffer,
    /// Where finishBlock assembles a block's payload: its end table,
    /// then the entries from entries_buf. Kept separate because the
    /// table's size isn't known until the block is complete.
    block_buf: std.ArrayList(u8),
    /// The open block's entries, encoded as they are added.
    entries_buf: std.ArrayList(u8),

    /// Where each entry of the open block ends in entries_buf: the
    /// payload's end table, filled as entries are added.
    entry_ends: std.ArrayList(u32),
    cur_base_key: std.ArrayList(u8),
    block_infos: std.ArrayList(BlockInfo),
    last_key: std.ArrayList(u8),
    /// For the table's filter, one entry per key: `bf_hashes` the key
    /// hashes of a `bytes` table, `egids` the keys themselves of an
    /// `egid` table, kept as the bitmap the index stores. Only one is
    /// filled; `egids` is created on the first egid key.
    bf_hashes: std.ArrayList(u64),
    egid_bm: ?zroar.Bitmap,
    key_count: u32,
    max_version: u64,
    uncompressed_size: u32,
    compressed_size: u32,
    /// Estimated bytes of shadowed (dead-or-dying) data written into this
    /// table: versions that sit below their primary's newest terminal (a newer
    /// full `state` or a `deleted`), so no read past the horizon needs them —
    /// reclaimable once snapshot_ts rises past them. Fed by `addStaleBytes`
    /// from compaction, snapshotted into the table index at `finish()`.
    /// A user-data estimate (key.primary + value), so it under-approximates
    /// on-disk footprint — fine as a signal.
    stale_data_size: u32,

    /// table_buf grows by this much at a time: a few blocks' worth of
    /// wasted tail per table, and a small table stays one allocation.
    const table_chunk_bytes: usize = 8 << 20;

    pub fn init(allocator: std.mem.Allocator, cfg: Config) Builder {
        return .{
            .allocator = allocator,
            .cfg = cfg,
            .compression = Compression.init(allocator, cfg.codec),
            .table_buf = utils.ChunkBuffer.init(
                allocator,
                table_chunk_bytes,
            ),
            .block_buf = .empty,
            .entries_buf = .empty,
            .entry_ends = .empty,
            .cur_base_key = .empty,
            .block_infos = .empty,
            .last_key = .empty,
            .bf_hashes = .empty,
            .egid_bm = null,
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
        if (self.egid_bm) |*bm| bm.deinit();
        self.cur_base_key.deinit(self.allocator);
        self.entry_ends.deinit(self.allocator);
        self.entries_buf.deinit(self.allocator);
        self.block_buf.deinit(self.allocator);
        self.table_buf.deinit();
        self.compression.deinit();
    }

    /// reset clears all state so the builder can produce a new table without
    /// reallocating its backing buffers (especially table_buf).
    pub fn reset(self: *Builder) void {
        for (self.block_infos.items) |info| {
            self.allocator.free(info.base_key);
        }

        utils.clearRetainCap(&self.block_infos);
        utils.clearRetainCap(&self.last_key);
        utils.clearRetainCap(&self.bf_hashes);
        // A bitmap cannot be emptied in place; the next egid key makes
        // a new one.
        if (self.egid_bm) |*bm| bm.deinit();
        self.egid_bm = null;
        utils.clearRetainCap(&self.cur_base_key);
        utils.clearRetainCap(&self.entry_ends);
        utils.clearRetainCap(&self.entries_buf);
        utils.clearRetainCap(&self.block_buf);
        self.table_buf.reset();
        self.compression.reset();
        self.key_count = 0;
        self.max_version = 0;
        self.uncompressed_size = 0;
        self.compressed_size = 0;
        self.stale_data_size = 0;
    }

    /// The key of the last entry written.
    /// The last key added, as a primary-only key (the table stores no
    /// namespace or table_id).
    pub fn lastKey(self: *Builder) Key {
        return .{ .primary = self.last_key.items };
    }

    /// Record `n` bytes of shadowed data in the table being built. Saturates
    /// rather than overflowing: max_table_size caps a real table well under
    /// u32, so this only guards against a pathological input.
    pub inline fn addStaleBytes(self: *Builder, n: usize) void {
        const clamped: u32 = @intCast(@min(n, std.math.maxInt(u32)));
        self.stale_data_size +|= clamped;
    }

    /// Convenience for callers with nothing to say about the row: raw
    /// encoding, no flags. Production writers use addItem, so meta travels
    /// with the row it describes.
    pub fn add(self: *Builder, key: Key, ts: u64, value: String) Error!void {
        return self.addItem(.{
            .key = key,
            .versions = .{ .one = .{ .ts = ts, .meta = .{}, .value = value } },
        });
    }

    /// Write `item` as one entry: its key, then its versions packed
    /// newest first. Keys must arrive strictly ascending, one item per
    /// key; an item's versions must descend strictly in timestamp.
    pub fn addItem(self: *Builder, item: Item) Error!void {
        const key = item.key;
        const vers = item.versions.slice();

        // A real row, never a bound sentinel: an empty primary would leave
        // last_key empty and quietly disarm both the ordering check below
        // and the base-key comparison further down.
        std.debug.assert(key.primary.len > 0);
        if (key.primary.len > std.math.maxInt(u16)) return error.KeyTooLarge;

        var prev_ts: ?u64 = null;
        const ver_hdr = _buffers.BlockVersionReader.SIZE;
        var packed_len = 4 + ver_hdr * vers.len;
        for (vers) |ver| {
            if (prev_ts) |pt| {
                if (ver.ts >= pt) return error.KeyOrderViolation;
            }
            prev_ts = ver.ts;
            packed_len += ver.value.len;
        }
        // Every offset is a u32 into the bytes region.
        if (packed_len > std.math.maxInt(u32)) return error.ValueTooLarge;

        // Keys of one table are keys of one tree, so their primaries
        // alone order them.
        if (self.last_key.items.len > 0) {
            if (key.lePrimary(self.lastKey())) {
                return error.KeyOrderViolation;
            }
        }
        switch (self.cfg.kind) {
            .unique_index, .posting_index => try self.bf_hashes.append(
                self.allocator,
                kv.bloomHash(key.primary),
            ),
            .rows => {
                if (key.primary.len != 8) return error.KeyNotEgid;
                if (self.egid_bm == null) {
                    self.egid_bm = try zroar.Bitmap.init(self.allocator);
                }
                const egid = std.mem.readInt(u64, key.primary[0..8], .big);
                _ = try self.egid_bm.?.set(egid);
            },
            // None named, or a kind this build does not know (the wire
            // enum is open): no filter it can write.
            .none, _ => return error.UnknownTreeKind,
        }

        // The stored key is the primary alone: the namespace and table_id
        // are the tree's, and the iterator a tree opens stamps them back
        // on (`TableIterator.init`). Kept in last_key for prefix
        // compression.
        utils.clearRetainCap(&self.last_key);
        try self.last_key.appendSlice(self.allocator, key.primary);
        const entry_key = self.last_key.items;

        // Overlap against the base key currently in force. Only used to
        // size this entry for the fits-in-the-open-block question below.
        // findDiff yields null only for wholly equal slices: entries are
        // strictly ascending, so none repeats its block's base key, and a
        // fresh block's empty base differs in length from any real key.
        const base = self.cur_base_key.items;
        const fit_overlap =
            std.mem.findDiff(u8, base, entry_key) orelse unreachable;

        // Estimate the encoded size for this entry:
        // 2 bytes overlap + 2 bytes diff_len + key diff + packed versions.
        const entry_sz = @as(usize, 4) + (entry_key.len - fit_overlap) +
            packed_len;
        if (self.shouldFinishBlock(entry_sz)) {
            try self.finishBlock();
        }

        // finishBlock clears the base key, so a block that just rolled over
        // — like the very first one — adopts THIS key as its base: the
        // overlap is then the whole key and the stored diff is empty. With
        // no rollover the overlap measured above still stands, which is why
        // it isn't recomputed.
        const overlap = if (self.cur_base_key.items.len == 0) blk: {
            try self.cur_base_key.appendSlice(self.allocator, entry_key);
            break :blk entry_key.len;
        } else fit_overlap;

        if (overlap > std.math.maxInt(u16)) return error.KeyTooLarge;
        const diff = entry_key[overlap..];
        if (diff.len > std.math.maxInt(u16)) return error.KeyTooLarge;

        // The entry, a BlockEntry (see the layout notes at the top):
        // the key, the versions' headers newest first with each `off`
        // where its bytes start among the values, then the values.
        const out = &self.entries_buf;
        try out.ensureUnusedCapacity(self.allocator, 4 + diff.len + packed_len);

        var entry = _buffers.BlockEntryWriter.initWithBuf(self.allocator, out);
        try entry.setKeyOverlap(@intCast(overlap));
        try entry.setKeyDiff(diff);
        try entry.versionCount(vers.len);
        var off: u32 = 0;
        for (vers) |ver| {
            try entry.addVersion(.{
                .ts = ver.ts,
                .meta = ver.meta.toByte(),
                .off = off,
            });
            off += @intCast(ver.value.len);
        }
        for (vers) |ver| try entry.addValues(ver.value);
        entry.finish();

        // The payload's table holds where each entry ENDS.
        try self.entry_ends.append(self.allocator, @intCast(out.items.len));

        // Newest first, so the item's max is its first version.
        self.max_version = @max(self.max_version, item.newest().ts);
        const n: u32 = @intCast(vers.len);
        if (self.key_count > std.math.maxInt(u32) - n) {
            return error.IntegerOverflow;
        }
        self.key_count += n;
    }

    /// How many entries of `bf_hashes` repeat another one. Only for the
    /// filter-failure error line. Sorts `bf_hashes` in place: the filter
    /// build was their only reader and it has already failed.
    fn sortedDupCheckForError(self: *Builder) usize {
        const hashes = self.bf_hashes.items;
        std.mem.sort(u64, hashes, {}, std.sort.asc(u64));
        var dups: usize = 0;
        for (hashes[0..hashes.len -| 1], hashes[@min(1, hashes.len)..]) |prev, cur| {
            if (prev == cur) dups += 1;
        }
        return dups;
    }

    pub fn finish(self: *Builder) Error!TableBuffer {
        // Flush the final in-progress block, if any.
        try self.finishBlock();

        if (self.block_infos.items.len == 0) {
            return .{
                .allocator = self.allocator,
                .bytes = try self.allocator.alignedAlloc(u8, vfs.page_alignment, 0),
                .index_offset = 0,
            };
        }

        var index_bytes = std.ArrayList(u8).empty;
        defer index_bytes.deinit(self.allocator);

        // Build index bytes in a separate buffer, then append to table tail.
        var index_writer = try _buffers.TableIndexWriter.initWithBuf(self.allocator, &index_bytes);

        var block_offsets = try self.allocator.alloc(u32, self.block_infos.items.len);
        defer self.allocator.free(block_offsets);

        for (self.block_infos.items, 0..) |info, i| {
            // BlockIndex records each block's base key and byte range in table bytes.
            var block_writer = try _buffers.BlockIndexWriter.initWithBuf(
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
            // A set of distinct hashes builds on the first attempt (see
            // `BinaryFuse.init` for the sizing that once made ~11.5k-key
            // sets fail); if it somehow does not, the table is written
            // without a filter: lookups still work through the index,
            // only the miss shortcut is lost. That is an error
            // line, not a crash, since the failure is tied to the key set
            // and a panic would repeat on every retry of the same
            // compaction. Duplicate hashes fail every size, and only a
            // caller bug produces them (`addItem` appends one hash per
            // key), so the line says how many there were.
            const filter = _binaryfuse.BinaryFuse(u16).build(self.allocator, self.bf_hashes.items) catch |err| blk: {
                log.err("binary fuse build failed: {s}; n_hashes={d} duplicates={d}; writing table without a filter", .{
                    @errorName(err), self.bf_hashes.items.len, self.sortedDupCheckForError(),
                });
                break :blk null;
            };
            if (filter) |f| {
                defer f.deinit(self.allocator);
                var bf_writer = try _buffers.BinaryFuseFilterWriter.initWithBuf(self.allocator, &index_bytes);
                try bf_writer.set(.{
                    .seed = f.seed,
                    .segment_length = f.segment_length,
                    .segment_length_mask = f.segment_length_mask,
                    .segment_count = f.segment_count,
                    .segment_count_length = f.segment_count_length,
                    .fingerprints = f.fingerprints,
                });
                const bf_buf = try bf_writer.finish();
                binary_fuse_off = bf_buf.offset;
            }
        }

        try index_writer.set(.{
            .blocks = block_offsets,
            .last_key = self.last_key.items,
            .max_version = self.max_version,
            .key_count = self.key_count,
            .uncompressed_size = self.uncompressed_size,
            .compressed_size = self.compressed_size,
            .stale_data_size = self.stale_data_size,
            .format_version = _idx.format_version,
        });
        if (binary_fuse_off) |off| {
            try index_writer.setBinaryFuse(off);
        }
        // The egid filter: the keys, exactly, as the bitmap `addItem`
        // filled.
        if (self.egid_bm) |*bm| {
            try bm.compact();
            try index_writer.setEgidBitmap(bm.toBuffer());
        }

        _ = try index_writer.finish();

        const index_len: u32 = @intCast(index_bytes.items.len);
        const index_checksum = zbuf.hash(index_bytes.items);
        // The trailer goes after the index in the same buffer, so the
        // tail reaches table_buf in one append.
        try zbuf.appendInt(u32, &index_bytes, self.allocator, index_len);
        try zbuf.appendInt(
            u64,
            &index_bytes,
            self.allocator,
            index_checksum,
        );
        // The reader seeks by idx.trailer_len, so what we wrote has to be
        // exactly that wide — the padding below is sized on it too.
        std.debug.assert(index_bytes.items.len - index_len ==
            _idx.trailer_len);

        // Pad with zeros between the data blocks and the index so the final
        // file size is a multiple of `vfs.page_size`. The reader locates the
        // index via the trailer's `index_len` (offset = file_end - trailer
        // - index_len), so padding before the index is invisible to it.
        // Page-aligned files dodge any O_DIRECT / iomap edge case where an
        // unaligned-tail write could be silently dropped on some kernels;
        // the cost is at most page_size-1 zero bytes per SST.
        const total_unpadded = self.table_buf.len + index_bytes.items.len;
        const remainder = total_unpadded % vfs.page_size;
        const pad_bytes =
            if (remainder == 0) 0 else vfs.page_size - remainder;
        try self.table_buf.appendNTimes(0, pad_bytes);
        try self.table_buf.appendSlice(index_bytes.items);
        std.debug.assert(self.table_buf.len % vfs.page_size == 0);

        // Gather the chunks into one page-aligned buffer, the address
        // alignment O_DIRECT asks for. table_buf keeps its chunks for
        // the next SST.
        const len = self.table_buf.len;
        const bytes = try self.allocator.alignedAlloc(u8, vfs.page_alignment, len);
        self.table_buf.copyTo(bytes);
        self.table_buf.reset();
        return .{
            .allocator = self.allocator,
            .bytes = bytes,
            .index_offset = len - index_bytes.items.len,
        };
    }

    fn shouldFinishBlock(self: *Builder, new_entry_est: usize) bool {
        if (self.entry_ends.items.len == 0) return false;

        // Block layout, counting the entry we're about to add: the
        // envelope (crc and codec, up to the payload) + 4 bytes
        // entry_count + (entry_count + 1) ends * 4 bytes each
        const n = self.entry_ends.items.len;
        const envelope = _buffers.BlockReader.payload_off;
        const header_est = envelope + 4 + (n + 1) * 4;
        const estimated = self.entries_buf.items.len +
            new_entry_est + header_est;
        return estimated > self.cfg.block_bytes;
    }

    fn finishBlock(self: *Builder) Error!void {
        if (self.entry_ends.items.len == 0) return;
        if (self.block_infos.items.len >= std.math.maxInt(u32)) return error.TooManyBlocks;

        // Capture block base key before scratch buffers are reset.
        const base_key = try self.allocator.dupe(u8, self.cur_base_key.items);
        errdefer self.allocator.free(base_key);

        // The payload, a BlockPayload: [u32 entry_count][u32 end × n]
        // [entries], the entries already encoded in entries_buf and
        // bounded by entry_ends.
        const out = &self.block_buf;
        utils.clearRetainCap(out);
        var payload = _buffers.BlockPayloadWriter.initWithBuf(
            self.allocator,
            out,
        );
        try payload.entryCount(self.entry_ends.items.len);
        var start: usize = 0;
        for (self.entry_ends.items) |end| {
            try payload.addEntryBytes(self.entries_buf.items[start..end]);
            start = end;
        }
        payload.finish();
        // The entries are in the payload now; entries_buf is scratch
        // for the envelope below.
        utils.clearRetainCap(&self.entries_buf);

        const raw_len: u32 = @intCast(out.items.len);
        std.debug.assert(self.uncompressed_size <= std.math.maxInt(u32) - raw_len);
        self.uncompressed_size += raw_len;

        // The block, a Block: its envelope then the payload as stored,
        // the crc over the codec byte and those bytes as the reader
        // hashes them.
        const stored = try self.compression.compress(out.items);
        if (stored.codec != .none) {
            const clen: u32 = @intCast(stored.data.len);
            std.debug.assert(self.compressed_size <= std.math.maxInt(u32) - clen);
            self.compressed_size += clen;
        }

        // The envelope is laid down in scratch and the payload appended
        // after it (a `.rest` field the writer may leave empty), so the
        // stored bytes are copied once, into table_buf.
        const env = &self.entries_buf;
        var block = _buffers.BlockWriter.initWithBuf(self.allocator, env);
        const codec_byte = [_]u8{@intFromEnum(stored.codec)};
        try block.setCrc(zbuf.hashParts(&.{ &codec_byte, stored.data }));
        try block.setCodec(codec_byte[0]);
        block.finish();
        std.debug.assert(env.items.len == _buffers.BlockReader.payload_off);

        const block_off: u32 = @intCast(self.table_buf.len);
        try self.table_buf.appendSlice(env.items);
        try self.table_buf.appendSlice(stored.data);
        const block_len: u32 = @intCast(self.table_buf.len - block_off);

        try self.block_infos.append(self.allocator, .{
            .base_key = base_key,
            .offset = block_off,
            .len = block_len,
        });

        // Reuse capacity for the next block to avoid re-allocating every time.
        utils.clearRetainCap(&self.entries_buf);
        utils.clearRetainCap(&self.entry_ends);
        utils.clearRetainCap(&self.cur_base_key);
    }
};
