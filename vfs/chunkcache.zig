// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Local-disk cache of remote objects, in fixed-size chunks: chunk
//! `idx` of an object is its bytes [idx*C, (idx+1)*C) for C =
//! `chunkBytes()`, a grid over the object's bytes that knows nothing of
//! block boundaries. The chunk is both what a miss fetches and the
//! unit of eviction: a few blocks per round trip, small enough that
//! random reads keep every slot full.
//!
//! One preallocated slot file, direct I/O (a page fault would stall the
//! worker thread, not the fiber; and the page cache would hold a second
//! copy of what the block cache holds decoded):
//!
//!   page 0            FileHeader: chunk size and slot count
//!   header region     one 32-byte record per slot, 128 to a page:
//!                     key, length, hash of the
//!                     chunk's bytes; a record that does not validate
//!                     is a free slot
//!   then              N slots of C bytes
//!
//! The in-memory state is the S3-FIFO cache (`cache` module) over
//! slots, rebuilt from the header records at startup. A put writes the
//! chunk, then its record's page, composed from the in-memory records
//! of every slot on that page. The first read of a slot after a
//! restart hashes the whole chunk against its record (`Slot.verified`)
//! and drops it on a mismatch. That check is what makes the ordering
//! not matter: a torn page write, a page that lost a race with a
//! neighbour's put, a record from a previous occupant landing after
//! the slot was reused — each reads as a free slot or a mismatch,
//! never wrong bytes.
//!
//! The chunk size is fixed for the file's life: an existing file keeps
//! its own; a new one takes the configured size, or the default.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const _cache = @import("cache");
const _vfs = @import("vfs.zig");
const String = utils.String;
const Allocator = std.mem.Allocator;
const page_size = _vfs.page_size;
const log = std.log.scoped(.chunkcache);

/// One chunk of an object: `chunk_len` bytes, the chunk size or the
/// last chunk's tail. `req_buf` holds its bytes [req_lo, req_hi), the
/// span a read wants.
pub const Chunk = struct {
    idx: u64,
    chunk_len: u64,
    req_lo: u64,
    req_hi: u64,
    req_buf: []u8 = &.{},

    /// The loaded bytes of [lo, hi), a range inside this chunk.
    pub fn bytes(self: Chunk, lo: u64, hi: u64) []u8 {
        return self.req_buf[lo - self.req_lo .. hi - self.req_lo];
    }
};

pub const ChunkCache = struct {
    alloc: Allocator,
    cfg: Config,
    /// The open slot file; null while disabled.
    open: ?*Open = null,

    pub const Config = struct {
        /// Slot file path. Empty = no disk cache.
        path: String = "",
        /// The slot file's size. An existing file of another size is
        /// recreated, cold.
        capacity_bytes: u64 = 0,
        /// Chunk size when the slot file is created; 0 = the default.
        /// An existing file keeps its own.
        chunk_bytes: u64 = 0,
        /// The VFS the slot file opens through, carrying its device's
        /// limiter. Null = the global one.
        vfs: ?*const _vfs.VFS = null,
    };

    /// The fetch and eviction unit. Smaller tracks hot blocks more
    /// closely and costs a cache entry (about 100 bytes of memory)
    /// per slot: a 1 TiB cache of 8 KiB chunks is 128M slots.
    pub const default_chunk_bytes: u64 = 128 << 10;
    pub const min_chunk_bytes: u64 = 8 << 10;
    pub const max_chunk_bytes: u64 = 8 << 20;

    pub const Stats = struct {
        /// Slots in the file, and how many hold a chunk.
        slots: u64 = 0,
        used: u64 = 0,
        chunk_bytes: u64 = 0,
        hits: u64 = 0,
        misses: u64 = 0,
        puts: u64 = 0,
        /// Puts dropped for want of a free slot: every slot the cache
        /// evicted was still held by a reader.
        drops: u64 = 0,
        /// Slots whose bytes did not match their record on first read.
        corrupt: u64 = 0,
        /// Wastage: of the pages in slots retired so far (evicted or
        /// forgotten), how many were never read while cached, and how
        /// many of those slots were never read at all.
        evicted_pages: u64 = 0,
        unread_pages: u64 = 0,
        unread_chunks: u64 = 0,
    };

    /// Opens the slot file at `cfg.path`, creating it when absent.
    pub fn init(a: Allocator, cfg: Config) !ChunkCache {
        var self = ChunkCache{ .alloc = a, .cfg = cfg };
        if (cfg.path.len == 0) return self;
        if (cfg.chunk_bytes != 0) try checkChunkBytes(cfg.chunk_bytes);

        if (_vfs.access(cfg.path)) |_| {
            if (try Open.load(a, cfg)) |open| {
                self.open = open;
                return self;
            }
        } else |_| {}
        self.open = try Open.create(a, cfg, self.chunkBytes());
        return self;
    }

    pub fn deinit(self: *ChunkCache) void {
        if (self.open) |o| o.deinit();
        self.open = null;
    }

    fn checkChunkBytes(chunk_bytes: u64) !void {
        if (chunk_bytes < min_chunk_bytes or chunk_bytes > max_chunk_bytes or
            !std.math.isPowerOfTwo(chunk_bytes))
        {
            log.err("chunk size must be a power of two in [{d}, {d}] KiB", .{
                min_chunk_bytes >> 10, max_chunk_bytes >> 10,
            });
            return error.BadChunkBytes;
        }
    }

    /// False while there is no slot file: reads then fetch exact
    /// ranges from the remote.
    pub fn enabled(self: *const ChunkCache) bool {
        return self.open != null;
    }

    /// The grid size: the slot file's once open, else what a file
    /// would be created with.
    pub fn chunkBytes(self: *const ChunkCache) u64 {
        if (self.open) |o| return o.chunk_bytes;
        return if (self.cfg.chunk_bytes != 0)
            self.cfg.chunk_bytes
        else
            default_chunk_bytes;
    }

    pub fn stats(self: *const ChunkCache) Stats {
        const o = self.open orelse return .{};
        o.mu.lock(@src());
        defer o.mu.unlock();

        var st = o.stats;
        st.slots = o.slots.len;
        st.used = o.cache.count();
        st.chunk_bytes = o.chunk_bytes;
        return st;
    }

    /// Fill `chunk.req_buf` with bytes [req_lo, req_hi) of chunk `idx` of
    /// `name`.
    /// False on a miss, or when the slot holds fewer bytes (an
    /// object's tail) than the span reaches. The first read of a slot
    /// after a restart hashes the whole chunk against its record and
    /// drops it on a mismatch. Atomic to the caller: the slot is
    /// referenced for the whole read, so an eviction meanwhile cannot
    /// hand its bytes to another chunk.
    pub fn read(
        self: *ChunkCache,
        name: String,
        chunk: *const Chunk,
    ) !bool {
        const so = self.open orelse return false;
        const dst = chunk.req_buf;
        const off = chunk.req_lo;
        if (dst.len == 0) return true;
        const key = keyOf(hashName(name), chunk.idx);

        so.mu.lock(@src());
        const found = so.cache.get(key, .reuse);
        const slot = found orelse {
            so.stats.misses += 1;
            so.mu.unlock();
            return false;
        };
        if (chunk.req_hi > slot.len) {
            so.stats.misses += 1;
            slot.decrRef();
            so.mu.unlock();
            return false;
        }
        so.mu.unlock();

        // Our ref keeps the slot's bytes in place while the lock is
        // free; every exit below drops it under the lock.
        errdefer {
            so.mu.lock(@src());
            slot.decrRef();
            so.mu.unlock();
        }
        if (!slot.verified and !try so.verify(slot)) {
            so.mu.lock(@src());
            defer so.mu.unlock();
            so.stats.corrupt += 1;
            so.stats.misses += 1;
            // A racing put may have mapped the key to a fresh slot;
            // removing that too costs one miss, nothing more.
            _ = so.cache.remove(key);
            slot.decrRef();
            return false;
        }

        const res = try so.handle.readAligned(
            self.alloc,
            so.dataOff(slot.id) + off,
            dst.len,
        );
        defer res.free();
        if (res.data.len != dst.len) return error.ShortRead;
        @memcpy(dst, res.data);

        so.mu.lock(@src());
        defer so.mu.unlock();
        so.stats.hits += 1;
        so.markRead(slot.id, off, dst.len);
        slot.decrRef();
        return true;
    }

    /// Store `data` as chunk `idx` of `name`: the whole chunk, or the
    /// object's tail for its last chunk (the last page is padded with
    /// zeros). Dropped when disabled, or when the chunk is already
    /// cached.
    pub fn write(
        self: *ChunkCache,
        fname: String,
        idx: u64,
        data: String,
    ) !void {
        const o = self.open orelse return;
        if (data.len == 0) return;
        if (data.len > o.chunk_bytes) return error.BadRange;

        const key = keyOf(hashName(fname), idx);
        // Hashed before taking the lock: up to a few MiB of work.
        const hash = std.hash.Wyhash.hash(0, data);

        o.mu.lock(@src());
        if (o.cache.contains(key)) {
            o.mu.unlock();
            return;
        }
        const slot = o.takeSlot() orelse {
            o.stats.drops += 1;
            o.mu.unlock();
            return;
        };
        o.mu.unlock();

        // The slot is in no map while its bytes are written: no reader
        // can see it. Our ref alone keeps it off the free list, and
        // dropping it on a failed write returns the slot.
        errdefer {
            o.mu.lock(@src());
            slot.decrRef();
            o.mu.unlock();
        }
        try o.writeData(slot.id, data);

        o.mu.lock(@src());
        defer o.mu.unlock();
        slot.key = key;
        slot.len = @intCast(data.len);
        slot.hash = hash;
        slot.verified = true;
        // The record reaches disk from the flusher, best effort.
        o.markDirty(SlotRecord.pageOf(slot.id));
        o.stats.puts += 1;
        // A racing put of the same key landed first: the cache
        // releases the one it replaces, which frees that slot.
        try o.cache.put(key, slot);
        slot.decrRef();
    }

    /// Drop every chunk of `name` (the object was deleted). The slots
    /// return to the free list as their readers finish; their records
    /// are cleared so a restart does not bring the chunks back.
    pub fn forget(self: *ChunkCache, name: String) void {
        const o = self.open orelse return;
        const file = hashName(name);

        var keys: std.ArrayList(u128) = .empty;
        defer keys.deinit(self.alloc);
        o.mu.lock(@src());
        defer o.mu.unlock();
        // The map is keyed by (file, idx); the object's chunks are
        // found by a scan of its keys, microseconds for the tens of
        // thousands a large slot file holds.
        const map = &o.cache.map;
        for (map.keys(), map.values()) |key, entry| {
            if (fileOf(key) != file) continue;
            keys.append(self.alloc, key) catch break;
            // Record cleared now; the slot itself frees when its
            // last reader lets go.
            o.retire(entry.val);
            o.markDirty(SlotRecord.pageOf(entry.val.id));
        }
        for (keys.items) |key| _ = o.cache.remove(key);
    }

    fn hashName(name: String) u64 {
        return std.hash.Wyhash.hash(0, name);
    }

    /// Key of chunk `idx` of the object whose name hashes to `file`.
    fn keyOf(file: u64, idx: u64) u128 {
        return (@as(u128, file) << 64) | idx;
    }

    fn fileOf(key: u128) u64 {
        return @intCast(key >> 64);
    }

    fn idxOf(key: u128) u64 {
        return @truncate(key);
    }
};

/// Page 0 of the slot file.
const FileHeader = extern struct {
    magic: u64 = magic_value,
    chunk_bytes: u64 = 0,
    slots: u64 = 0,
    crc: u32 = 0,
    _pad: u32 = 0,

    const magic_value: u64 = 0x30_54_4f_4c_53_47_45; // "EGSLOT0"

    fn checksum(self: *const FileHeader) u32 {
        return std.hash.Crc32.hash(std.mem.asBytes(self)[0..@offsetOf(
            FileHeader,
            "crc",
        )]);
    }

    fn valid(self: *const FileHeader) bool {
        return self.magic == magic_value and self.crc == self.checksum();
    }
};

/// One slot's record in the header region. Zeroed (or anything else
/// that fails to validate) means a free slot; the file header's magic
/// vouches for the layout, so a record carries none. Little-endian on
/// disk: egres targets are.
const SlotRecord = extern struct {
    file: u64 = 0,
    idx: u64 = 0,
    /// Wyhash of the slot's `len` bytes.
    hash: u64 = 0,
    /// Bytes the slot holds: the chunk size, or an object's tail.
    len: u32 = 0,
    crc: u32 = 0,

    const per_page = page_size / @sizeOf(SlotRecord);
    comptime {
        std.debug.assert(@sizeOf(SlotRecord) == 32 and per_page == 128);
    }

    /// The page holding slot `id`'s record: records start on page 1,
    /// after the file header.
    fn pageOf(id: u64) u64 {
        return 1 + id / per_page;
    }

    /// Pages the records of `n` slots take.
    fn pages(n: u64) u64 {
        return (n + per_page - 1) / per_page;
    }

    /// Byte offset of slot `id`'s record in the file.
    fn offsetOf(id: u64) u64 {
        return pageOf(id) * page_size + (id % per_page) * @sizeOf(SlotRecord);
    }

    fn checksum(self: *const SlotRecord) u32 {
        return std.hash.Crc32.hash(std.mem.asBytes(self)[0..@offsetOf(
            SlotRecord,
            "crc",
        )]);
    }

    fn valid(self: *const SlotRecord) bool {
        return self.len > 0 and self.crc == self.checksum();
    }
};

/// One slot of the file, as the cache's value: `refs` counts the
/// cache's hold on it plus every read or put in flight, and the slot
/// returns to the free list at zero. All under `Open.mu`.
const Slot = struct {
    id: u32,
    refs: u32 = 0,
    /// Bytes held: the chunk size, or an object's tail. 0 = no chunk,
    /// which is what its record says too.
    len: u32 = 0,
    key: u128 = 0,
    hash: u64 = 0,
    /// The bytes on disk were hashed against the record in this
    /// process: by the put that wrote them, or by the first read after
    /// a restart. Until then a read checks the whole chunk.
    verified: bool = false,
    free: *std.ArrayList(u32),

    pub fn incrRef(self: *Slot) *Slot {
        self.refs += 1;
        return self;
    }

    pub fn decrRef(self: *Slot) void {
        std.debug.assert(self.refs > 0);
        self.refs -= 1;
        if (self.refs == 0) self.free.appendAssumeCapacity(self.id);
    }

    /// The budget is counted in slots.
    pub fn cost(_: *const Slot) usize {
        return 1;
    }
};

const Cache = _cache.Cache(u128, *Slot, .cost_only);

/// The open slot file and the state over it.
const Open = struct {
    a: Allocator,
    handle: _vfs.FileHandle,
    chunk_bytes: u64,
    slots: []Slot,
    /// Slot ids nobody references; capacity `slots.len`, so a push
    /// never allocates (decrRef runs under the lock, from the cache).
    free: std.ArrayList(u32),
    cache: Cache,
    /// Per slot, one bit per page read while cached (`read_words`
    /// words each): what retirement counts as used.
    read_bits: []u64,
    read_words: u32,
    mu: fiber.Mutex(.untracked) = .{},
    stats: ChunkCache.Stats = .{},
    /// Record pages whose records changed since the flusher last wrote
    /// them: one bit per page, under `mu`.
    dirty: std.DynamicBitSetUnmanaged,
    /// Rung by `markDirty`; the flusher sleeps on it. One slot: a
    /// burst rings once, and a ring never blocks.
    wake: fiber.Channel(void),
    /// The one writer of record pages (`flushLoop`), so two puts on a
    /// page can never land their writes out of order.
    flusher: ?fiber.JoinHandle(void) = null,

    /// Slots a capacity holds: each costs its chunk plus its record,
    /// after the file header's page.
    fn slotCount(cfg: ChunkCache.Config, chunk_bytes: u64) !u64 {
        if (cfg.capacity_bytes < 2 * page_size + chunk_bytes) {
            log.err("chunk cache: capacity {d} MiB holds no {d} KiB chunk", .{
                cfg.capacity_bytes >> 20, chunk_bytes >> 10,
            });
            return error.CacheTooSmall;
        }
        // Near the answer from the per-slot cost, then exact.
        const per = chunk_bytes + @sizeOf(SlotRecord);
        var n = (cfg.capacity_bytes - page_size) / per;
        while (fileSize(n, chunk_bytes) > cfg.capacity_bytes) n -= 1;
        return n;
    }

    fn fileSize(n: u64, chunk_bytes: u64) u64 {
        return page_size * (1 + SlotRecord.pages(n)) + n * chunk_bytes;
    }

    fn dataOff(self: *const Open, id: u64) u64 {
        const headers = page_size * (1 + SlotRecord.pages(self.slots.len));
        return headers + id * self.chunk_bytes;
    }

    /// A free slot, referenced for the caller and in no map; null when
    /// every slot the cache would give up is still referenced.
    fn takeSlot(self: *Open) ?*Slot {
        // Every slot is in the cache (cost 1 each, so its cost is its
        // slot count), on the free list, or held outside both: a put in
        // flight, an evicted slot a reader still references. The held
        // ones are not free either, so the cache must make room for
        // them too before a slot comes free.
        const cached = self.cache.totalCost();

        // held would consider all the available slots (slots.len) - the cached
        // slots - the freed slots. There are slots which the cache has given up
        // on, but have not yet been freed. Those are what 'held' counts.
        const held = self.slots.len - cached - self.free.items.len;
        self.cache.makeRoom(held + 1);

        const id = self.free.pop() orelse return null;
        const slot = &self.slots[id];
        self.retire(slot);
        return slot.incrRef();
    }

    fn readBits(self: *const Open, id: u64) []u64 {
        return self.read_bits[id * self.read_words ..][0..self.read_words];
    }

    /// Note the pages of [off, off+len) of slot `id` as read.
    fn markRead(self: *Open, id: u64, off: u64, len: usize) void {
        const bits = self.readBits(id);
        var p = off / page_size;
        const last = (off + len - 1) / page_size;
        while (p <= last) : (p += 1) {
            bits[p / 64] |= @as(u64, 1) << @intCast(p % 64);
        }
    }

    /// The slot's chunk is gone (evicted and reused, or forgotten):
    /// count what was fetched and never read, and clear the slot.
    fn retire(self: *Open, slot: *Slot) void {
        if (slot.len > 0) {
            const pages = (slot.len + page_size - 1) / page_size;
            var read: u64 = 0;
            for (self.readBits(slot.id)) |w| read += @popCount(w);

            self.stats.evicted_pages += pages;
            self.stats.unread_pages += pages - @min(read, pages);
            if (read == 0) self.stats.unread_chunks += 1;
        }
        slot.len = 0;
        @memset(self.readBits(slot.id), 0);
    }

    /// Header-region page `page` into `out` (one page, aligned): the
    /// records of every slot on it, from the slots' in-memory state;
    /// a slot without a chunk is a zero record.
    fn composeSlotRecordPage(self: *const Open, page: u64, out: []u8) void {
        @memset(out, 0);
        const first = (page - 1) * SlotRecord.per_page;
        const last = @min(first + SlotRecord.per_page, self.slots.len);
        for (first..last) |id| {
            const slot = &self.slots[id];
            if (slot.len == 0) continue;
            var rec = SlotRecord{
                .file = ChunkCache.fileOf(slot.key),
                .idx = ChunkCache.idxOf(slot.key),
                .hash = slot.hash,
                .len = slot.len,
            };
            rec.crc = rec.checksum();
            const at = SlotRecord.offsetOf(id) - page * page_size;
            @memcpy(out[at..][0..@sizeOf(SlotRecord)], std.mem.asBytes(&rec));
        }
    }

    /// Hash the slot's bytes against its record, once per process.
    fn verify(self: *Open, slot: *Slot) !bool {
        const res = try self.handle.readAligned(
            self.a,
            self.dataOff(slot.id),
            slot.len,
        );
        defer res.free();
        const ok = res.data.len == slot.len and
            std.hash.Wyhash.hash(0, res.data) == slot.hash;
        if (ok) {
            self.mu.lock(@src());
            defer self.mu.unlock();
            slot.verified = true;
        }
        return ok;
    }

    /// The chunk's bytes into slot `id`. Direct I/O wants an aligned
    /// buffer of whole pages: written as given when it is one (the
    /// fetch paths allocate chunks that way), else through a copy —
    /// an object's tail chunk, or a caller with a plain buffer.
    fn writeData(self: *Open, id: u32, data: String) !void {
        const off = self.dataOff(id);
        const aligned = @intFromPtr(data.ptr) % page_size == 0 and
            data.len % page_size == 0;
        if (aligned) return self.handle.write(data, off);

        const len = std.mem.alignForward(usize, data.len, page_size);
        const buf = try self.a.alignedAlloc(u8, _vfs.page_alignment, len);
        defer self.a.free(buf);
        @memcpy(buf[0..data.len], data);
        @memset(buf[data.len..], 0);
        try self.handle.write(buf, off);
    }

    const open_opts: _vfs.FileOptions = .{
        .mode = .direct,
        .flags = .{ .writable = true },
    };

    /// A new slot file: sized with one truncate (sparse; a read of an
    /// unwritten page yields zeros, free slots), header page written.
    fn create(a: Allocator, cfg: ChunkCache.Config, chunk_bytes: u64) !*Open {
        const n = try slotCount(cfg, chunk_bytes);
        const v = cfg.vfs orelse _vfs.g();
        const handle = try v.createFile(cfg.path, open_opts);
        errdefer handle.close();

        // A half-made file would fail to load next boot; leave none.
        errdefer _vfs.deleteFile(cfg.path) catch {};
        try handle.truncate(fileSize(n, chunk_bytes));

        var hdr = FileHeader{ .chunk_bytes = chunk_bytes, .slots = n };
        hdr.crc = hdr.checksum();
        try writePage(a, handle, 0, std.mem.asBytes(&hdr));
        try handle.syncData();
        log.info("chunk cache: created {s}: {d} slots of {d} KiB", .{
            cfg.path, n, chunk_bytes >> 10,
        });
        return try build(a, handle, chunk_bytes, n);
    }

    /// An existing slot file. One of ours that does not match the
    /// config (slot count, an explicit chunk size) or its own header
    /// (length) is deleted, and null says to create one: the records
    /// sit before the data, so a file cannot grow in place, and a cache
    /// may start cold. A file that is not ours is an error: deleting
    /// it is the operator's call.
    fn load(a: Allocator, cfg: ChunkCache.Config) !?*Open {
        const v = cfg.vfs orelse _vfs.g();
        const handle = try v.openFile(a, cfg.path, open_opts);
        errdefer handle.close();

        var hdr: FileHeader = undefined;
        try readPage(a, handle, 0, std.mem.asBytes(&hdr));
        if (!hdr.valid()) {
            log.err("chunk cache: {s} is not a slot file", .{cfg.path});
            return error.BadCacheFile;
        }
        if (cfg.chunk_bytes != 0 and cfg.chunk_bytes != hdr.chunk_bytes) {
            log.warn("chunk cache: {s} has {d} KiB chunks, flag says {d}", .{
                cfg.path, hdr.chunk_bytes >> 10, cfg.chunk_bytes >> 10,
            });
            return discard(cfg.path, handle);
        }
        if (handle.file_size != fileSize(hdr.slots, hdr.chunk_bytes)) {
            log.warn("chunk cache: {s} is {d} bytes, header says {d}", .{
                cfg.path,                             handle.file_size,
                fileSize(hdr.slots, hdr.chunk_bytes),
            });
            return discard(cfg.path, handle);
        }
        const want = try slotCount(cfg, hdr.chunk_bytes);
        if (want != hdr.slots) {
            log.warn("chunk cache: {s} holds {d} slots, capacity asks {d}", .{
                cfg.path, hdr.slots, want,
            });
            return discard(cfg.path, handle);
        }
        return try build(a, handle, hdr.chunk_bytes, hdr.slots);
    }

    /// Delete a slot file of ours that `load` cannot use as configured;
    /// null tells `init` to create one.
    fn discard(path: String, handle: _vfs.FileHandle) !?*Open {
        log.warn("chunk cache: recreating {s}, cold", .{path});
        // Unlinked while open: a failed delete leaves the handle to
        // load's errdefer, which closes it once.
        try _vfs.deleteFile(path);
        handle.close();
        return null;
    }

    /// The in-memory state from the header records: every valid record
    /// is a chunk restored into the cache's main queue, unverified.
    fn build(
        a: Allocator,
        handle: _vfs.FileHandle,
        chunk_bytes: u64,
        n: u64,
    ) !*Open {
        const self = try a.create(Open);
        errdefer a.destroy(self);

        const slots = try a.alloc(Slot, n);
        errdefer a.free(slots);

        var free = try std.ArrayList(u32).initCapacity(a, n);
        errdefer free.deinit(a);

        var cache = try Cache.init(a, n);
        errdefer cache.deinit();

        const read_words: u32 = @intCast((chunk_bytes / page_size + 63) / 64);
        const read_bits = try a.alloc(u64, n * read_words);
        errdefer a.free(read_bits);
        @memset(read_bits, 0);

        // Indexed by page number; page 0, the file header, stays clear.
        var dirty = try std.DynamicBitSetUnmanaged.initEmpty(
            a,
            1 + SlotRecord.pages(n),
        );
        errdefer dirty.deinit(a);
        var wake = try fiber.Channel(void).initAlloc(a, 1);
        errdefer wake.deinit();

        self.* = .{
            .a = a,
            .handle = handle,
            .chunk_bytes = chunk_bytes,
            .slots = slots,
            .free = free,
            .cache = cache,
            .read_bits = read_bits,
            .read_words = read_words,
            .dirty = dirty,
            .wake = wake,
        };
        for (self.slots, 0..) |*s, id| {
            s.* = .{ .id = @intCast(id), .free = &self.free };
        }

        // The header region in runs of a few MiB.
        const run_pages: u64 = 1024;
        const header_pages = SlotRecord.pages(n);
        var restored: u64 = 0;
        var page: u64 = 1;
        while (page <= header_pages) : (page += run_pages) {
            const count = @min(run_pages, header_pages + 1 - page);
            const res = try handle.readAligned(
                a,
                page * page_size,
                count * page_size,
            );
            defer res.free();

            const first_id = (page - 1) * SlotRecord.per_page;
            const last_id = @min(first_id + count * SlotRecord.per_page, n);
            for (first_id..last_id) |id| {
                const slot = &self.slots[id];
                const at = SlotRecord.offsetOf(id) - page * page_size;
                const rec = std.mem.bytesToValue(
                    SlotRecord,
                    res.data[at..][0..@sizeOf(SlotRecord)],
                );
                if (!rec.valid() or rec.len > chunk_bytes) {
                    self.free.appendAssumeCapacity(slot.id);
                    continue;
                }
                slot.key = ChunkCache.keyOf(rec.file, rec.idx);
                slot.len = rec.len;
                slot.hash = rec.hash;
                // Two slots with one key (the older one's record was
                // never cleared): restore keeps the later, frees the
                // other.
                try self.cache.restore(slot.key, slot);
                restored += 1;
            }
        }
        log.info("chunk cache: {d} of {d} slots restored", .{ restored, n });
        self.flusher = try fiber.spawn(.{}, flushLoop, .{self});
        return self;
    }

    fn deinit(self: *Open) void {
        // The flusher's last pass writes every dirty page: a clean
        // restart restores all of them.
        self.wake.close(.graceful);
        if (self.flusher) |*f| f.join();

        self.wake.deinit();
        self.dirty.deinit(self.a);
        self.cache.deinit();
        self.free.deinit(self.a);
        self.a.free(self.read_bits);
        self.a.free(self.slots);
        self.handle.close();
        self.a.destroy(self);
    }

    /// Under `mu`: the page's records changed; the flusher writes it.
    fn markDirty(self: *Open, page: u64) void {
        self.dirty.set(page);
        self.wake.trySend({}) catch {};
    }

    /// Writes dirty record pages until closed, then once more.
    fn flushLoop(self: *Open) void {
        // Record writes are housekeeping, behind every reader.
        fiber.ctx().class = .bg;
        const pg = self.a.alignedAlloc(
            u8,
            _vfs.page_alignment,
            page_size,
        ) catch {
            log.err("chunk cache: no page buffer; records stay in memory", .{});
            return;
        };
        defer self.a.free(pg);

        while (true) {
            const open = if (self.wake.receive()) |_| true else |_| false;
            self.flushDirty(pg);
            if (!open) return;
        }
    }

    /// Every dirty page, composed from the records as they are now. A
    /// failed write leaves its page dirty for the next ring rather than
    /// retrying at once.
    fn flushDirty(self: *Open, pg: []u8) void {
        while (true) {
            self.mu.lock(@src());
            const page = self.dirty.toggleFirstSet() orelse {
                self.mu.unlock();
                return;
            };
            self.composeSlotRecordPage(page, pg);
            self.mu.unlock();

            self.handle.write(pg, page * page_size) catch |err| {
                log.warn("chunk cache: record page {d} not written: {s}", .{
                    page, @errorName(err),
                });
                self.mu.lock(@src());
                self.dirty.set(page);
                self.mu.unlock();
                return;
            };
        }
    }
};

/// `bytes` at the start of the page at `off`, the rest zero.
fn writePage(
    a: Allocator,
    handle: _vfs.FileHandle,
    off: u64,
    bytes: String,
) !void {
    const buf = try a.alignedAlloc(u8, _vfs.page_alignment, page_size);
    defer a.free(buf);
    @memcpy(buf[0..bytes.len], bytes);
    @memset(buf[bytes.len..], 0);
    try handle.write(buf, off);
}

fn readPage(a: Allocator, handle: _vfs.FileHandle, off: u64, out: []u8) !void {
    const res = try handle.readAligned(a, off, out.len);
    defer res.free();
    if (res.data.len != out.len) return error.ShortRead;
    @memcpy(out, res.data);
}
