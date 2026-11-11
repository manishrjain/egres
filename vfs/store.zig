// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The store that holds every SST, backed by a FileStore
//! (a directory, local or a shared mount) or an S3Store. The creator
//! uploads an object BEFORE proposing the manifest change that references
//! it, so store durability precedes consensus; every node opens the same
//! object to read it. Objects are keyed by name (callers own the naming —
//! e.g. the SST filename), write-once, and single-writer by construction,
//! which is what lets the `file` backend share a plain directory between
//! nodes without any locking.
//!
//! Reads go through an `Object` handle (open once, read ranges). It has
//! the read shape `Table` wants — `readAligned`, `batchRead` — so a table
//! over a file object reads exactly as a local file did, and one over an
//! s3 object turns each read into a ranged GET.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const _vfs = @import("vfs.zig");
const _s3 = @import("s3.zig");
const _lookahead = @import("lookahead.zig");

pub const S3Config = _s3.S3Config;

pub const Store = union(enum) {
    file: FileStore,
    s3: _s3.S3Store,

    /// `vfs` is the depot device's VFS: what every object opens
    /// through.
    pub fn initFile(
        a: std.mem.Allocator,
        depot: String,
        vfs: *const _vfs.VFS,
    ) !Store {
        return .{ .file = try FileStore.init(a, depot, vfs) };
    }

    pub fn initS3(a: std.mem.Allocator, cfg: S3Config) !Store {
        return .{ .s3 = try _s3.S3Store.init(a, cfg) };
    }

    pub fn deinit(self: *Store) void {
        switch (self.*) {
            .file => |*f| f.deinit(),
            .s3 => |*s| s.deinit(),
        }
    }

    /// The file backend's depot path; empty for s3. For the callers that
    /// need a local device to probe (the `--io` calibration).
    pub fn localPath(self: *const Store) String {
        return switch (self.*) {
            .file => |*f| f.depot,
            .s3 => "",
        };
    }

    /// Upload a whole object. Idempotent: re-putting an existing name
    /// rewrites identical bytes (write-once contract). `data` must be
    /// page-aligned and a page multiple — the file backend writes
    /// O_DIRECT and rejects anything else (error.UnalignedPut). SST
    /// buffers satisfy this by construction.
    pub fn put(self: *Store, name: String, data: String) !void {
        switch (self.*) {
            .file => |*f| try f.put(name, data),
            .s3 => |*s| try s.put(name, data),
        }
    }

    /// Durability barrier for prior puts. put fsyncs each object's BYTES;
    /// the file backend leaves the NAME's durability to a depot
    /// directory fsync — done here, once per batch, instead of per put.
    /// s3: no-op, every PUT is durable when the response arrives. Call
    /// after a batch of puts, before proposing changes that reference them.
    /// On an error no name of the batch is known durable: the caller
    /// puts the batch again.
    pub fn sync(self: *Store) !void {
        switch (self.*) {
            .file => |*f| try f.sync(),
            .s3 => {},
        }
    }

    pub fn exists(self: *Store, name: String) !bool {
        switch (self.*) {
            .file => |*f| return f.exists(name),
            .s3 => |*s| return s.exists(name),
        }
    }

    pub fn delete(self: *Store, name: String) !void {
        switch (self.*) {
            .file => |*f| try f.delete(name),
            .s3 => |*s| try s.delete(name),
        }
    }

    /// Drop the object's chunks from the disk cache: it left the
    /// manifest, so no reader will ask for them again. The object
    /// itself stays until `delete`. Nothing to do for a file store.
    pub fn forget(self: *Store, name: String) void {
        switch (self.*) {
            .file => {},
            .s3 => |*s| s.cache.forget(name),
        }
    }

    /// One object's size and modification time. error.FileNotFound if
    /// absent. For a caller that needs a few objects out of a large
    /// store: `list(.stat)` stats every one.
    pub fn stat(self: *Store, name: String) !_vfs.FileStat {
        switch (self.*) {
            .file => |*f| return f.stat(name),
            .s3 => |*s| return s.stat(name),
        }
    }

    /// Every object in the store: its name, and with `.stat` its size and
    /// modification time. Caller releases the result with
    /// `_vfs.freeObjectInfos`.
    ///
    /// The file backend lists the regular files at the top of the
    /// depot. A put that a crash cut short is one of them, under its
    /// final name and with part of its bytes. The s3 backend pages
    /// through ListObjectsV2 under the configured prefix.
    pub fn list(
        self: *Store,
        alloc: std.mem.Allocator,
        detail: _vfs.ListDetail,
    ) ![]_vfs.ObjectInfo {
        switch (self.*) {
            .file => |*f| return _vfs.listFiles(alloc, f.depot, detail),
            .s3 => |*s| return s.list(alloc, detail),
        }
    }

    /// Open an object for ranged reads; `size` is its size, which the
    /// caller knows (the manifest records every SST's; `stat` for the
    /// rest), so no request goes out here. error.FileNotFound if absent,
    /// from the file backend; s3 reports it on the first read. `opts` is
    /// how the file backend opens the file (direct, mmap, ...); s3
    /// ignores it.
    pub fn open(
        self: *Store,
        name: String,
        size: u64,
        opts: _vfs.FileOptions,
    ) !Object {
        switch (self.*) {
            .file => |*f| return f.open(name, opts),
            .s3 => |*s| return .{ .s3 = .{
                .store = s,
                .name = try s.a.dupe(u8, name),
                .sz = size,
            } },
        }
    }

    /// Measure the ideal readahead for this store endpoint (see
    /// lookahead.calibrate): the size whose transfer time equals the
    /// per-request fixed cost — a network round trip for s3, syscall +
    /// device latency for file. Run by `--probe`, not at startup:
    /// nothing in the server consumes it.
    pub fn calibrateLookahead(self: *Store, block_size: u64) !u64 {
        switch (self.*) {
            .file => |*f| return f.calibrateLookahead(block_size),
            .s3 => |*s| return s.calibrateLookahead(block_size),
        }
    }

    /// The disk chunk cache in front of this store's reads; none for a
    /// file store, whose objects are already local.
    pub fn chunkCache(self: *Store) ?*_vfs.ChunkCache {
        switch (self.*) {
            .file => return null,
            .s3 => |*s| return &s.cache,
        }
    }
};

/// A readable object: open once, read many ranges. `.file` is a VFS
/// handle — a file object opened as the caller asked, or a memory-backed
/// handle (tests, benches) — so direct I/O, mmap and the bandwidth
/// limiter apply as they do to any local file. `.s3` reads are ranged
/// GETs.
pub const Object = union(enum) {
    file: _vfs.FileHandle,
    s3: S3Object,

    pub fn size(self: *const Object) u64 {
        switch (self.*) {
            .file => |*f| return f.file_size,
            .s3 => |*o| return o.sz,
        }
    }

    /// True when reads are memory copies, not I/O: a memory-backed
    /// file. Nothing to gain from batching those.
    pub fn isResident(self: *const Object) bool {
        switch (self.*) {
            .file => |*f| return f.owned_buf != null,
            .s3 => return false,
        }
    }

    /// `FileHandle.readAligned` over this object: a page-aligned buffer
    /// (or a slice into resident bytes) that `result.free()` releases.
    pub fn readAligned(
        self: *const Object,
        a: std.mem.Allocator,
        offset: u64,
        len: usize,
    ) !_vfs.ReadResult {
        switch (self.*) {
            .file => |*f| return f.readAligned(a, offset, len),
            .s3 => |*o| {
                // The same aligned-buffer contract as the file path, so a
                // caller cannot tell which backend served it.
                const buf = try a.alignedAlloc(u8, _vfs.page_alignment, len);
                errdefer a.free(buf);
                try o.store.read(o.name, o.sz, offset, buf);
                return .{
                    .buf = buf,
                    .data = buf,
                    .offset = offset,
                    .allocator = a,
                };
            },
        }
    }

    /// `FileHandle.batchRead` over this object: one io_uring submission
    /// for a file, coalesced concurrent GETs for s3 (S3Store.batchRead).
    /// Either way `data` lives in `arena`.
    pub fn batchRead(
        self: *const Object,
        arena: *std.heap.ArenaAllocator,
        requests: []_vfs.FileHandle.ReadRequest,
    ) !void {
        switch (self.*) {
            .file => |*f| return f.batchRead(arena, requests),
            .s3 => |*o| return o.store.batchRead(
                o.name,
                o.sz,
                arena,
                requests,
            ),
        }
    }

    pub fn close(self: *Object) void {
        switch (self.*) {
            .file => |*f| f.close(),
            .s3 => |*o| o.store.a.free(o.name),
        }
    }
};

/// Each read is one or more ranged GETs on the store's connection slots.
const S3Object = struct {
    store: *_s3.S3Store,
    name: String,
    sz: u64,
};

/// A write-once blob store in a local directory, for one node. put()
/// writes an object under its final name, so a name can hold part of
/// an object while the put runs or after a crash cut it short. No
/// reader opens a name before the manifest lists it, which is after
/// the put and `sync` returned, and `Forest.removeOrphans` deletes
/// the ones no manifest change ever named.
pub const FileStore = struct {
    a: std.mem.Allocator,
    depot: String,
    vfs: *const _vfs.VFS,

    pub fn init(
        a: std.mem.Allocator,
        depot: String,
        vfs: *const _vfs.VFS,
    ) !FileStore {
        try _vfs.createDirPath(depot);
        return .{ .a = a, .depot = try a.dupe(u8, depot), .vfs = vfs };
    }

    pub fn deinit(self: *FileStore) void {
        self.a.free(self.depot);
    }

    fn objectPath(self: *FileStore, name: String) ![]u8 {
        return std.fmt.allocPrint(self.a, "{s}/{s}", .{ self.depot, name });
    }

    pub fn put(self: *FileStore, name: String, data: String) !void {
        // O_DIRECT requires it, and every legit caller already complies
        // (the SST builder pads + aligns its output) — reject rather
        // than bounce.
        if (@intFromPtr(data.ptr) % _vfs.page_size != 0 or data.len % _vfs.page_size != 0)
            return error.UnalignedPut;

        const path = try self.objectPath(name);
        defer self.a.free(path);
        // The bytes are synced here: an upload must be durable before
        // the manifest change that names it is proposed. The name is
        // the caller's barrier, one `Store.sync` after all its puts.
        // Direct I/O: a fresh SST is read back through its own direct
        // handle, and the write must not churn the page cache.
        // truncate: a put that a crash cut short left part of the file
        // under this name, and putting it again overwrites that. The
        // write runs at the caller's I/O class, the flush or compaction
        // whose output this is.
        try _vfs.writeAndSync(
            path,
            data,
            .{ .mode = .direct, .flags = .{ .truncate = true } },
        );
    }

    fn sync(self: *FileStore) !void {
        try _vfs.syncDir(self.depot);
    }

    fn exists(self: *FileStore, name: String) !bool {
        const path = try self.objectPath(name);
        defer self.a.free(path);

        _vfs.access(path) catch return false;
        return true;
    }

    fn delete(self: *FileStore, name: String) !void {
        const path = try self.objectPath(name);
        defer self.a.free(path);

        try _vfs.deleteFile(path);
    }

    fn open(self: *FileStore, name: String, opts: _vfs.FileOptions) !Object {
        const path = try self.objectPath(name);
        defer self.a.free(path);
        return .{ .file = try self.vfs.openFile(self.a, path, opts) };
    }

    /// Object size in bytes; errors if absent.
    pub fn head(self: *FileStore, name: String) !u64 {
        return (try self.stat(name)).size;
    }

    fn stat(self: *FileStore, name: String) !_vfs.FileStat {
        const path = try self.objectPath(name);
        defer self.a.free(path);
        return _vfs.fileStat(path);
    }

    /// Measure this depot's ideal readahead (policy in vfs/lookahead.zig;
    /// this store contributes probeRead and the object ops). Meaningful
    /// locally too: the knee is device latency × device bandwidth (~100s
    /// of KB on NVMe) instead of network round trip × link bandwidth
    /// (~MBs).
    pub fn calibrateLookahead(self: *FileStore, block_size: u64) !u64 {
        return _lookahead.calibrate(self.a, self, block_size);
    }

    /// One timed ranged read of the probe object — the disk analog of
    /// s3's timed ranged GET, and the measurement primitive behind
    /// lookahead.calibrate. Same contract as the s3 probe: total_ns =
    /// end-to-end cost of a production read of this size (one pread, its
    /// own fixed cost included), ttfb_ns = a sample of the fixed
    /// per-request cost alone. A positional read has no observable
    /// "first byte" mid-syscall, so the fixed cost is sampled as its OWN
    /// single-page pread at an unrelated offset — splitting the main
    /// read instead would charge every probe two serial device round
    /// trips that production reads never pay. Opened O_DIRECT so the
    /// page cache can't serve repeats and turn the numbers into memory
    /// bandwidth.
    pub fn probeRead(self: *FileStore, off: u64, len: u64) !_lookahead.Probe {
        const path = try self.objectPath(_lookahead.probe_object);
        defer self.a.free(path);
        // O_DIRECT needs page-aligned offset/length/buffer; align the
        // request itself — any aligned window of ~len bytes probes
        // equally well.
        const a_off = off & ~@as(u64, _vfs.page_size - 1);
        const a_len: usize = std.mem.alignForward(usize, @intCast(@max(len, _vfs.page_size)), _vfs.page_size);
        const buf = try self.a.alignedAlloc(u8, _vfs.page_alignment, a_len);
        defer self.a.free(buf);

        const file = try _vfs.openDirect(path, .{ .mode = .direct });
        defer fiber.fs.submit(.close, file);
        const fsize = (try fiber.fs.submit(.stat, file)).size;
        if (a_off + a_len > fsize) return error.ShortRead;

        // The fixed-cost page: hashed from the (already random) offset so
        // repeats can't sit in one device cache slot, dodged out of the
        // main window so it can't pre-warm the ranged read.
        const pages = fsize / _vfs.page_size;
        var page_off = ((off *% 0x9E3779B97F4A7C15) % pages) * _vfs.page_size;
        if (page_off >= a_off and page_off < a_off + a_len) {
            page_off = ((page_off + a_len) / _vfs.page_size % pages) * _vfs.page_size;
        }

        const t0 = utils.timestampNow(.monotonic, .nanos);
        const fixed = try fiber.fs.submit(.read, .{
            .file = file,
            .buf = buf[0.._vfs.page_size],
            .offset = page_off,
        });
        if (fixed != _vfs.page_size) return error.ShortRead;

        const fixed_ns: u64 =
            @intCast(utils.timestampNow(.monotonic, .nanos) - t0);
        if (!_lookahead.verifyProbeData(page_off, buf[0.._vfs.page_size])) return error.BadProbeData;

        const t1 = utils.timestampNow(.monotonic, .nanos);
        var got: usize = 0;
        while (got < a_len) {
            const n = try fiber.fs.submit(.read, .{
                .file = file,
                .buf = buf[got..],
                .offset = a_off + got,
            });
            if (n == 0) return error.ShortRead;
            got += n;
        }
        const total: u64 =
            @intCast(utils.timestampNow(.monotonic, .nanos) - t1);
        if (!_lookahead.verifyProbeData(a_off, buf)) return error.BadProbeData;
        return .{ .size = a_len, .ttfb_ns = fixed_ns, .total_ns = total };
    }
};
