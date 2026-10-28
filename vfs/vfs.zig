// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
pub const bandwidth = @import("bandwidth.zig");
pub const remotestore = @import("remotestore.zig");
pub const RemoteStore = remotestore.RemoteStore;
pub const ChunkCache = @import("chunkcache.zig").ChunkCache;
pub const Chunk = @import("chunkcache.zig").Chunk;
pub const Object = remotestore.Object;
pub const sigv4 = @import("sigv4.zig");
pub const s3 = @import("s3.zig");
pub const lookahead = @import("lookahead.zig");
pub const calibrate = @import("calibrate.zig");

inline fn fdToFile(fd: posix.fd_t) std.Io.File {
    return .{ .handle = fd, .flags = .{ .nonblocking = false } };
}

inline fn fdSize(fd: posix.fd_t) !u64 {
    return (try fdToFile(fd).stat(utils.context.g().io)).size;
}

inline fn closeFd(fd: posix.fd_t) void {
    fdToFile(fd).close(utils.context.g().io);
}

pub const Flags = struct {
    /// Open for reading and writing instead of read-only.
    writable: bool = false,
    /// Truncate an existing file instead of failing with O_EXCL.
    truncate: bool = false,
    /// Open with O_DSYNC so every `write()` returns only after the data
    /// (and any metadata required to read it back) is durable. Removes the
    /// need for an explicit `fdatasync` after each write — useful for small
    /// metadata files where the cost of a separate fsync syscall dominates
    /// the actual I/O. POSIX-portable; on macOS the kernel treats DSYNC
    /// like full SYNC, which is still correct (data is durable).
    dsync: bool = false,
    /// Reads land at random offsets: the kernel reads nothing ahead of
    /// them (POSIX_FADV_RANDOM on Linux, F_RDAHEAD off on macOS). For
    /// SST files read through the page cache, which readahead would
    /// fill with blocks no one asked for.
    random: bool = false,
};

pub const FileOptions = struct {
    mode: Mode = .default,
    flags: Flags = .{},
};

pub const page_size: usize = 4096;
pub const page_alignment = std.mem.Alignment.fromByteUnits(page_size);

pub const Mode = enum {
    default, // default via filesystem
    direct, // O_DIRECT, aligned reads/writes
    mmap, // mmap the file, reads are slices into the mapped region
    memory, // read entire file into owned buffer
};

/// The class of a read or write: which of the tenant's two buckets
/// pays, foreground's filled first. The current fiber's context
/// carries it (`fiber.ctx().class`), so nothing passes it down.
pub const IoClass = fiber.IoClass;

pub const IOKind = enum(u1) {
    read,
    write,
};

pub const ReadResult = struct {
    buf: ?[]align(page_size) u8,
    data: String,
    /// File offset of `data[0]`.
    offset: u64,
    allocator: std.mem.Allocator,

    pub fn free(self: ReadResult) void {
        if (self.buf) |b| self.allocator.free(b);
    }
};

pub const FileHandle = struct {
    file: ?fiber.File = null,
    file_size: u64,
    /// Aligned to the OS page, not `page_size`: munmap wants the
    /// former, and on aarch64-macos it is 16 KiB.
    mapped: ?[]align(std.heap.page_size_min) const u8 = null,
    owned_buf: ?[]u8 = null, // memory mode: entire file read into this buffer
    buf_allocator: ?std.mem.Allocator = null, // for freeing owned_buf on close
    vfs: *const VFS,
    /// The limiter of the VFS this file was opened through, charged
    /// for every read and write through the handle.
    bw: *bandwidth.Limiter,
    /// fd was actually opened O_DIRECT (Linux + mode==.direct, no tmpfs
    /// fallback) → writes must be page-aligned, so `write` bounce-buffers
    /// unaligned data. False for default/mmap/memory handles.
    direct: bool = false,

    pub fn readAligned(
        self: FileHandle,
        allocator: std.mem.Allocator,
        offset: u64,
        len: usize,
    ) !ReadResult {
        // In-memory: slice from owned buffer (no allocation, no bandwidth).
        if (self.owned_buf) |buf| {
            return .{
                .buf = null,
                .data = buf[offset .. offset + len],
                .offset = offset,
                .allocator = allocator,
            };
        }

        // Round offset down to page boundary.
        const aligned_offset = offset & ~@as(u64, page_size - 1);
        const prefix: usize = @intCast(offset - aligned_offset);

        // Round total size up to page boundary.
        const aligned_size = std.mem.alignForward(usize, prefix + len, page_size);

        if (self.mapped) |mapped| {
            // MMAP: slice from mapped memory. Fault in pages under bandwidth
            // control so mmap I/O respects the same rate limits as pread.
            self.bw.grant(aligned_size, .read);
            // Touch one byte per page to trigger page faults.
            var sink: u8 = 0;
            var pos: usize = 0;
            while (pos < aligned_size) : (pos += page_size) {
                sink ^= mapped[aligned_offset + pos];
            }
            std.mem.doNotOptimizeAway(sink);
            return .{
                .buf = null,
                .data = mapped[offset .. offset + len],
                .offset = offset,
                .allocator = allocator,
            };
        }

        // Allocate page-aligned buffer for O_DIRECT compatibility.
        const buf = try allocator.alignedAlloc(u8, page_alignment, aligned_size);
        errdefer allocator.free(buf);

        const f = self.file.?;
        self.bw.grant(aligned_size, .read);
        var total_read: usize = 0;
        while (total_read < aligned_size) {
            const rest = buf[total_read..];
            const n = try f.read(rest, aligned_offset + total_read);
            total_read += n;
            // A short read is the end of the file: one more at that
            // offset is an EOF error, not zero bytes.
            if (n < rest.len) break;
        }

        // It's possible that buf was set up for a bigger read, but the file ended before that.
        // In that case, the end offset could be total_read.
        const end = @min(prefix + len, total_read);
        return .{
            .buf = buf,
            .data = buf[prefix..end],
            .offset = offset,
            .allocator = allocator,
        };
    }

    pub const ReadRequest = struct {
        offset: u64,
        len: usize,
        data: String = "", // filled by batchRead
        ctx: ?*anyopaque = null, // opaque pointer for caller use
        /// The file to read; `FileHandle.batchRead` fills it in,
        /// `VFS.batchRead` callers set it per request.
        file: fiber.File = undefined,
    };

    /// `VFS.batchRead` over this one file: every request reads it.
    pub fn batchRead(
        self: FileHandle,
        arena: *std.heap.ArenaAllocator,
        requests: []ReadRequest,
    ) !void {
        for (requests) |*req| req.file = self.file.?;
        return self.vfs.batchRead(self.bw, arena, requests);
    }

    pub fn write(
        self: FileHandle,
        data: String,
        offset: u64,
    ) !void {
        const f = self.file.?;

        // O_DIRECT requires the buffer ADDRESS, LENGTH, and file OFFSET to all
        // be page-aligned. Every direct-mode writer is in-tree and hands us an
        // aligned buffer at the source (the SST builder page-aligns + pads its
        // output; the snapshot-install path copies network data into an aligned
        // buffer first), so we assert the contract here rather than bounce-
        // buffer on every write — a violation fails loudly in debug instead of
        // opaquely with EINVAL.
        if (self.direct) {
            std.debug.assert(@intFromPtr(data.ptr) % page_size == 0);
            std.debug.assert(data.len % page_size == 0);
            std.debug.assert(offset % page_size == 0);
        }

        self.bw.grant(data.len, .write);
        var written: usize = 0;
        while (written < data.len) {
            const n = try f.write(data[written..], offset + written);
            if (n == 0) return error.WriteFailed;
            written += n;
        }
        // Every chunk was written in full above; a mismatch here would
        // be a bug, surfaced as `error.ShortWrite` so the file is not
        // installed in the manifest as if it were complete.
        if (written != data.len) return error.ShortWrite;
    }

    pub fn sync(self: FileHandle) !void {
        if (self.file) |f| f.sync(.{}) catch {};
    }

    /// fdatasync, errors propagated: the data and the metadata needed
    /// to read it back are durable on return. The WAL's durability point
    /// (raft/diskwal.zig); a failure here must reach the caller.
    pub fn syncData(self: FileHandle) !void {
        const f = self.file orelse return;
        try f.sync(.{ .data_only = true });
    }

    pub fn truncate(self: FileHandle, size: u64) !void {
        try self.file.?.setSize(size);
    }

    pub fn close(self: FileHandle) void {
        if (self.owned_buf) |b| {
            if (self.buf_allocator) |a| a.free(b);
            return; // memory-backed: no file or mapping to release
        }
        if (self.mapped) |m| posix.munmap(m);
        if (self.file) |f| f.close();
    }
};

var g_limiter: bandwidth.Limiter = undefined;
var g_vfs: VFS = undefined;
var g_initialized = false;

/// Returns the global VFS instance. Lazily initialized with a generous-but-
/// finite default (1 TiB/s) — high enough to be effectively unthrottled
/// for any realistic disk workload. Call `VFS.initGlobal()` to override.
pub fn g() *VFS {
    if (!g_initialized) {
        g_limiter = bandwidth.Limiter.init(.{ .bytes_per_sec = 1024 * 1024 * 1024 * 1024 });
        g_vfs = .{ .bw = &g_limiter };
        g_initialized = true;
    }
    return &g_vfs;
}

/// A VFS is a device's view of the disk: every file opened through it
/// charges `bw`, the device's budget in device time, whatever the file
/// holds (SSTs, the WAL, the chunk cache). Each owner opens through
/// the VFS of its directory's device (`calibrate.deviceVfs`); `g()` is
/// the global one, for tests and files on no calibrated device.
pub const VFS = struct {
    bw: *bandwidth.Limiter,

    pub const Config = bandwidth.Config;

    /// Batch reads, across any files, into io_uring submissions of at
    /// most `max_batch_size` requests each. Allocates page-aligned
    /// buffers from the arena for O_DIRECT compatibility and fills each
    /// request's .data with the read result. Arena owns all internal
    /// buffers — no manual cleanup needed.
    // Max requests per io_uring submission. Must stay under the default
    // SQ depth (256) to avoid a race where flushing the SQ mid-add
    // triggers completions that prematurely signal the group.
    // Must stay under io_uring SQ depth (default 256). getSqe retries once
    // on SubmissionQueueFull, but if all SQEs are still in-flight, the retry
    // also fails. Keep a margin below 256 for other concurrent I/O.
    const max_batch_size: usize = 200;

    /// `bw` is the limiter of the device the requests' files are on:
    /// one batch, one device (a WAL's segments share its directory).
    pub fn batchRead(
        self: *const VFS,
        bw: *bandwidth.Limiter,
        arena: *std.heap.ArenaAllocator,
        requests: []FileHandle.ReadRequest,
    ) !void {
        if (requests.len == 0) return;

        // Process in chunks to stay within io_uring SQ depth.
        var offset: usize = 0;
        while (offset < requests.len) {
            const end = @min(offset + max_batch_size, requests.len);
            try self.batchReadChunk(bw, arena, requests[offset..end]);
            offset = end;
        }
    }

    fn batchReadChunk(
        self: *const VFS,
        bw: *bandwidth.Limiter,
        arena: *std.heap.ArenaAllocator,
        requests: []FileHandle.ReadRequest,
    ) !void {
        _ = self;
        const alloc = arena.allocator();

        // Bandwidth: acquire total bytes for this chunk before submitting.
        {
            var total: usize = 0;
            for (requests) |r| total += r.len;
            bw.grant(total, .read);
        }

        // One BatchOp + page-aligned buffer per request, heap-allocated from
        // the arena (fiber stacks are small). `preadBatch` submits them all
        // in a single libxev submission, whatever files they name, and
        // parks once until every read completes.
        const ops = try alloc.alloc(fiber.File.BatchOp, requests.len);
        const bufs = try alloc.alloc([]u8, requests.len);

        for (requests, 0..) |*req, i| {
            // Page-aligned offset and size for O_DIRECT compatibility.
            const aligned_offset = req.offset & ~@as(u64, page_size - 1);
            const prefix: usize = @intCast(req.offset - aligned_offset);
            const aligned_size =
                std.mem.alignForward(usize, prefix + req.len, page_size);

            bufs[i] = try alloc.alignedAlloc(u8, page_alignment, aligned_size);
            ops[i] = .{
                .file = req.file,
                .buf = bufs[i],
                .offset = aligned_offset,
            };
        }

        try fiber.fs.preadBatch(alloc, ops);

        // Fill request .data fields from the aligned read buffers.
        for (requests, 0..) |*req, i| {
            _ = try ops[i].result;
            const prefix: usize = @intCast(req.offset & (page_size - 1));
            req.data = bufs[i][prefix .. prefix + req.len];
        }
    }

    /// Initialize the global VFS instance at `config`'s budget.
    pub fn initGlobal(config: Config) void {
        g_limiter = bandwidth.Limiter.init(config);
        g_vfs = .{ .bw = &g_limiter };
        g_initialized = true;
    }

    /// Create a VFS with a caller-owned limiter.
    pub fn new(limiter: *bandwidth.Limiter) VFS {
        return .{ .bw = limiter };
    }

    pub fn openFile(self: *const VFS, allocator: std.mem.Allocator, path: String, opts: FileOptions) !FileHandle {
        switch (opts.mode) {
            .mmap => {
                // mmap mode opens RO+default for the underlying fd; mapped
                // semantics live above the fd, not in its open flags.
                const fd = try openDirect(path, .{});
                errdefer closeFd(fd);

                const file_size = try fdSize(fd);
                const mapped = try posix.mmap(null, file_size, .{ .READ = true }, .{ .TYPE = .PRIVATE }, fd, 0);
                return .{
                    .file = fiber.File.fromFd(fd),
                    .file_size = file_size,
                    .mapped = mapped,
                    .vfs = self,
                    .bw = self.bw,
                };
            },
            .default, .direct => {
                const fd = try openDirect(path, opts);
                errdefer closeFd(fd);

                const file_size = try fdSize(fd);
                return .{
                    .file = fiber.File.fromFd(fd),
                    .file_size = file_size,
                    .vfs = self,
                    .bw = self.bw,
                    .direct = builtin.os.tag == .linux and opts.mode == .direct,
                };
            },
            .memory => {
                // memory mode reads the file once into a buffer; underlying fd is RO+default.
                const fd = try openDirect(path, .{});
                defer closeFd(fd); // closed after reading; FileHandle won't own it

                const io = utils.context.g().io;
                const file = fdToFile(fd);
                const file_size = (try file.stat(io)).size;
                const size: usize = @intCast(file_size);
                const buf = try allocator.alloc(u8, size);
                errdefer allocator.free(buf);
                _ = try file.readPositionalAll(io, buf, 0);
                return .{
                    .file_size = file_size,
                    .owned_buf = buf,
                    .buf_allocator = allocator,
                    .vfs = self,
                    .bw = self.bw, // never charged: no I/O after this
                };
            },
        }
    }

    pub fn createFile(self: *const VFS, path: String, opts: FileOptions) !FileHandle {
        if (opts.mode == .mmap or opts.mode == .memory) return error.InvalidMode;

        var open_flags: posix.O = .{ .ACCMODE = .RDWR, .CREAT = true };
        if (opts.flags.truncate) {
            open_flags.TRUNC = true;
        } else {
            open_flags.EXCL = true;
        }
        if (builtin.os.tag == .linux and opts.mode == .direct) open_flags.DIRECT = true;
        if (opts.flags.dsync) open_flags.DSYNC = true;

        // Offload the (potentially disk-blocking) open to the fsops pool via
        // fiber.fs rather than syscalling on the worker thread. The O_DIRECT
        // policy + tmpfs fallback stay here; only the syscall moves off-worker.
        const fd = fiber.fs.openPosix(path, open_flags, 0o644) catch |err| switch (err) {
            // tmpfs doesn't support O_DIRECT — fall back to plain open.
            error.BadPathName => return self.createFile(path, .{ .flags = opts.flags }),
            else => return err,
        };

        if (builtin.os.tag == .macos and opts.mode == .direct) {
            _ = std.c.fcntl(fd, std.c.F.NOCACHE, @as(c_int, 1));
        }

        return .{
            .file = fiber.File.fromFd(fd),
            .file_size = 0,
            .vfs = self,
            .bw = self.bw,
            .direct = builtin.os.tag == .linux and opts.mode == .direct,
        };
    }
};

/// The device (`st_dev`) holding `fd`. `std.Io.File.Stat` leaves it
/// out, so the system call is made here: statx on Linux, fstat on
/// macOS.
fn deviceOf(fd: posix.fd_t) !u64 {
    switch (builtin.os.tag) {
        .linux => {
            const linux = std.os.linux;
            var stx: linux.Statx = undefined;
            const rc = linux.statx(fd, "", linux.AT.EMPTY_PATH, .{}, &stx);
            if (linux.errno(rc) != .SUCCESS) return error.StatFailed;
            return (@as(u64, stx.dev_major) << 32) | stx.dev_minor;
        },
        .macos => {
            var st: std.c.Stat = undefined;
            if (std.c.fstat(fd, &st) != 0) return error.StatFailed;
            return @bitCast(@as(i64, st.dev));
        },
        else => return error.Unsupported,
    }
}

/// The device (`st_dev`) holding the directory `path`: what tells
/// directories on one disk from those on different ones.
pub fn deviceOfPath(path: String) !u64 {
    const fd = try openDirect(path, .{});
    defer closeFd(fd);
    return deviceOf(fd);
}

/// Create `path`, write `data` through the accounted FileHandle path,
/// fsync, and close — the durable whole-file write behind SST flushes
/// and dir-store puts. Callers whose buffers are page-aligned pass
/// `.direct` (the SST builders pad + align their output); arbitrary
/// heap buffers must use `.default` — direct-mode writes assert
/// alignment rather than bounce.
pub fn writeAndSync(
    path: String,
    data: String,
    opts: FileOptions,
) !void {
    const wh = try g().createFile(path, opts);
    defer wh.close();
    try wh.write(data, 0);
    try wh.sync();
}

// ═══════════════════════════════════════════════════════════════════════
// Module-level metadata ops
// ═══════════════════════════════════════════════════════════════════════
//
// These are control-plane operations: they don't go through bandwidth
// accounting or O_DIRECT semantics. Production code, tests, and tools all
// route through these so the project's "always use vfs" rule applies to
// the whole filesystem surface, not just data-plane I/O.
//
// Where fiber.fs has the op (`fiber.Dir.cwd().deleteFile/rename/access`,
// …), we wrap it for naming consistency.
// Where fiber.fs is missing higher-level conveniences (`createDirPath`,
// `deleteTree`, file iteration), we wrap `std.Io.Dir` with the global
// `io` from `utils.context`.

pub fn access(path: String) !void {
    try fiber.Dir.cwd().access(path, .{});
}

/// Recursive mkdir-p. Wraps std.Io.Dir.createDirPath (fiber.fs doesn't have it).
pub fn createDirPath(path: String) !void {
    try std.Io.Dir.cwd().createDirPath(utils.context.g().io, path);
}

/// Recursive delete. Wraps std.Io.Dir.deleteTree (fiber.fs doesn't have it).
pub fn deleteTree(path: String) !void {
    try std.Io.Dir.cwd().deleteTree(utils.context.g().io, path);
}

pub fn renameFile(old_path: String, new_path: String) !void {
    try fiber.Dir.cwd().rename(old_path, fiber.Dir.cwd(), new_path);
}

pub fn deleteFile(path: String) !void {
    try fiber.Dir.cwd().deleteFile(path);
}

/// Best-effort directory fsync so newly created filenames are durable.
/// Silently ignores errors (tmpfs, some network mounts don't support dir fsync).
pub fn syncDir(path: String) void {
    const fd = posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .DIRECTORY = true }, 0) catch return;
    const f = fiber.File.fromFd(fd);
    defer f.close();
    f.sync(.{}) catch {};
}

/// Filesystem magic numbers (statfs `f_type`) that decide how a file is
/// best laid out; see raft/diskwal.zig `Prezero`.
pub const fs_magic = struct {
    pub const ext4: i64 = 0xef53;
    pub const xfs: i64 = 0x58465342;
    pub const zfs: i64 = 0x2fc12fc1;
    pub const btrfs: i64 = 0x9123683e;
    pub const tmpfs: i64 = 0x01021994;
};

/// The filesystem magic of `path` (statfs `f_type`), or null when it
/// cannot be read. Linux only: the magic numbers and the statfs layout
/// are Linux's; elsewhere the answer is null.
pub fn fsMagic(path: String) ?i64 {
    if (builtin.os.tag == .linux) return linuxFsMagic(path);
    return null;
}

fn linuxFsMagic(path: String) ?i64 {
    // struct statfs on 64-bit Linux; only f_type is read.
    const Statfs = extern struct {
        f_type: i64,
        f_bsize: i64,
        f_blocks: u64,
        f_bfree: u64,
        f_bavail: u64,
        f_files: u64,
        f_ffree: u64,
        f_fsid: [2]i32,
        f_namelen: i64,
        f_frsize: i64,
        f_flags: i64,
        f_spare: [4]i64,
    };
    comptime std.debug.assert(@sizeOf(Statfs) == 120);
    const path_z = posix.toPosixPath(path) catch return null;
    var st: Statfs = undefined;
    const rc = std.os.linux.syscall2(.statfs, @intFromPtr(&path_z), @intFromPtr(&st));
    if (std.os.linux.errno(rc) != .SUCCESS) return null;
    return st.f_type;
}

// ═══════════════════════════════════════════════════════════════════════
// High-level file-content helpers (one-shot create/read)
// ═══════════════════════════════════════════════════════════════════════
//
// These are convenience wrappers for test setup / small-file scenarios.
// For SST-style I/O, use `VFS.openFile` / `VFS.createFile` + `FileHandle`.

/// Create (truncating if it exists), write `data` in one shot, and fsync:
/// whole-file writers want the bytes durable, not dirty page cache — the
/// file store renames these into place as upload targets, and a rename
/// must never expose a name whose data could still be lost.
pub fn writeFile(path: String, data: String) !void {
    const f = try fiber.Dir.cwd().createFile(path, .{});
    defer f.close();
    var off: u64 = 0;
    while (off < data.len) {
        off += try f.write(data[off..], off);
    }
    try f.sync(.{});
}

/// Open and read the whole file. Caller owns the returned buffer.
pub fn readFile(path: String, allocator: std.mem.Allocator) ![]u8 {
    const f = try fiber.Dir.cwd().openFile(path, .{});
    defer f.close();
    const info = try f.stat();
    const buf = try allocator.alloc(u8, @intCast(info.size));
    errdefer allocator.free(buf);
    var off: u64 = 0;
    while (off < buf.len) {
        const n = try f.read(buf[off..], off);
        if (n == 0) break;
        off += n;
    }
    return buf;
}

/// Stat a path for its size in bytes.
pub fn fileSize(path: String) !u64 {
    return (try fileStat(path)).size;
}

/// A file's size and its modification time in whole seconds since the
/// UTC epoch on the realtime clock.
pub const FileStat = struct { size: u64, mtime_s: i64 };

/// Stat one path. For a caller that needs a few files out of a large
/// directory: `listFiles(.stat)` stats every entry, this one only what is
/// asked.
pub fn fileStat(path: String) !FileStat {
    const f = try fiber.Dir.cwd().openFile(path, .{});
    defer f.close();
    const st = try f.stat();
    return .{ .size = st.size, .mtime_s = st.mtime_s };
}

// ═══════════════════════════════════════════════════════════════════════
// Directory listing
// ═══════════════════════════════════════════════════════════════════════

/// One regular file in a listed directory or one object in a store: its
/// name (owned) and, when the listing was asked for it, its stat. The dir
/// backend's listing and an S3 `ListObjectsV2` result are one type.
pub const ObjectInfo = struct {
    name: String,
    /// Size and mtime. Null when the listing was taken with `.names`, so a
    /// caller that needs them cannot read a zero by mistake.
    stat: ?FileStat,
};

/// How much `listFiles` reports per entry. `names` is one readdir;
/// `stat` adds an open + stat + close per file, which over a data dir of
/// hundreds of thousands of SSTs is the whole cost of the listing.
pub const ListDetail = enum { names, stat };

/// Free a slice returned by `listFiles`, names included.
pub fn freeObjectInfos(alloc: std.mem.Allocator, infos: []const ObjectInfo) void {
    for (infos) |i| alloc.free(i.name);
    alloc.free(infos);
}

/// Every regular file directly in `dir`. Anything that is not a regular
/// file — a subdirectory, a symlink — is skipped, so a depot's `tmp/`
/// staging directory never shows up. The one way to list a directory
/// above zfiber: `fiber.Dir.list` is the readdir underneath, and only
/// this function calls it. Caller releases the result with
/// `freeObjectInfos`.
///
/// With `.stat`, an entry that vanishes between the listing and its stat
/// is dropped, not an error: a concurrent writer is allowed.
pub fn listFiles(alloc: std.mem.Allocator, dir: String, detail: ListDetail) ![]ObjectInfo {
    const entries = try fiber.Dir.cwd().list(alloc, dir);
    defer fiber.freeDirEntries(alloc, entries);

    var out: std.ArrayList(ObjectInfo) = .empty;
    errdefer freeObjectInfos(alloc, out.toOwnedSlice(alloc) catch &.{});

    for (entries) |entry| {
        if (entry.kind != .file) continue;

        var stat: ?FileStat = null;
        if (detail == .stat) {
            const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ dir, entry.name });
            defer alloc.free(path);
            stat = fileStat(path) catch continue;
        }

        const name = try alloc.dupe(u8, entry.name);
        errdefer alloc.free(name);
        try out.append(alloc, .{ .name = name, .stat = stat });
    }
    return out.toOwnedSlice(alloc);
}

/// Open a file according to `opts`. Honors `mode = .direct` (O_DIRECT,
/// Linux only) and `flags.{ writable, dsync }`. Falls back to a plain
/// open without O_DIRECT if the filesystem rejects it (e.g. tmpfs returns
/// EINVAL → BadPathName); the fallback keeps O_DSYNC because every fs we
/// care about supports it.
///
/// Takes `FileOptions` rather than expanding into one parameter per flag
/// so this function's signature stays stable as new flags are added.
/// Pub for the file store's calibration probe reads, which need O_DIRECT
/// without a FileHandle's bandwidth accounting.
pub fn openDirect(path: String, opts: FileOptions) !posix.fd_t {
    const direct_io = opts.mode == .direct;
    const accmode: posix.ACCMODE = if (opts.flags.writable) .RDWR else .RDONLY;
    var flags: posix.O = .{ .ACCMODE = accmode };
    if (builtin.os.tag == .linux and direct_io) flags.DIRECT = true;
    if (opts.flags.dsync) flags.DSYNC = true;

    // Offload to the fsops pool (see VFS.createFile) instead of a blocking
    // openat on the fiber's worker thread.
    const fd = fiber.fs.openPosix(path, flags, 0) catch |err| switch (err) {
        error.BadPathName => {
            var fb: posix.O = .{ .ACCMODE = accmode };
            if (opts.flags.dsync) fb.DSYNC = true;
            return fiber.fs.openPosix(path, fb, 0);
        },
        else => return err,
    };

    if (builtin.os.tag == .macos and direct_io) {
        // F_NOCACHE: bypass the unified buffer cache. Best effort.
        _ = std.c.fcntl(fd, std.c.F.NOCACHE, @as(c_int, 1));
    }
    if (opts.flags.random and !direct_io) adviseRandom(fd);

    return fd;
}

/// Turns readahead off for `fd` (`Flags.random`); best effort.
fn adviseRandom(fd: posix.fd_t) void {
    switch (builtin.os.tag) {
        .linux => {
            const linux = std.os.linux;
            _ = linux.fadvise(fd, 0, 0, linux.POSIX_FADV.RANDOM);
        },
        // F_RDAHEAD 0 turns readahead off. Best effort.
        .macos => _ = std.c.fcntl(fd, std.c.F.RDAHEAD, @as(c_int, 0)),
        else => {},
    }
}
