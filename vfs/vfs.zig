const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const fiber = @import("fiber");
const utils = @import("utils");
pub const bandwidth = @import("bandwidth.zig");

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
};

pub const FileOptions = struct {
    mode: Mode = .default,
    flags: Flags = .{},
};

pub const page_size: usize = 4096;
pub const page_alignment = std.mem.Alignment.fromByteUnits(page_size);

pub const Mode = enum {
    default, // default via filesystem
    direct, // O_DIRECT + zio reads/writes
    mmap, // mmap the file, reads are slices into the mapped region
    memory, // read entire file into owned buffer
};

pub const Priority = enum(u2) {
    p0, // highest — Any user facing real-time reads
    p1, // medium  — WAL and L0 compaction RW
    p2, // lowest  — L1+ compaction RW
};

pub const IOKind = enum(u1) {
    read,
    write,
};

pub const ReadResult = struct {
    buf: ?[]align(page_size) u8,
    data: []const u8,
    allocator: std.mem.Allocator,

    pub fn free(self: ReadResult) void {
        if (self.buf) |b| self.allocator.free(b);
    }
};


pub const FileHandle = struct {
    file: ?fiber.File = null,
    file_size: u64,
    mapped: ?[]align(page_size) const u8 = null,
    owned_buf: ?[]u8 = null, // memory mode: entire file read into this buffer
    buf_allocator: ?std.mem.Allocator = null, // for freeing owned_buf on close
    vfs: *const VFS,

    pub fn readAligned(
        self: FileHandle,
        allocator: std.mem.Allocator,
        offset: u64,
        len: usize,
        priority: Priority,
    ) !ReadResult {
        // In-memory: slice from owned buffer (no allocation, no bandwidth).
        if (self.owned_buf) |buf| {
            return .{ .buf = null, .data = buf[offset .. offset + len], .allocator = allocator };
        }

        // Round offset down to page boundary.
        const aligned_offset = offset & ~@as(u64, page_size - 1);
        const prefix: usize = @intCast(offset - aligned_offset);

        // Round total size up to page boundary.
        const aligned_size = std.mem.alignForward(usize, prefix + len, page_size);

        if (self.mapped) |mapped| {
            // MMAP: slice from mapped memory. Fault in pages under bandwidth
            // control so mmap I/O respects the same rate limits as pread.
            var s = self.vfs.bw.spinner(priority, aligned_size, .read);
            defer s.deinit();
            var sink: u8 = 0;
            var total_touched: usize = 0;
            while (true) {
                const read_len = s.acquireBytes();
                if (read_len == 0) break;
                // Touch one byte per page to trigger page faults.
                var pos: usize = total_touched;
                while (pos < total_touched + read_len) : (pos += page_size) {
                    sink ^= mapped[aligned_offset + pos];
                }
                total_touched += read_len;
            }
            std.mem.doNotOptimizeAway(sink);
            return .{ .buf = null, .data = mapped[offset .. offset + len], .allocator = allocator };
        }

        // Allocate page-aligned buffer for O_DIRECT compatibility.
        const buf = try allocator.alignedAlloc(u8, page_alignment, aligned_size);
        errdefer allocator.free(buf);

        const f = self.file.?;
        var s = self.vfs.bw.spinner(priority, aligned_size, .read);
        defer s.deinit();
        var total_read: usize = 0;
        while (true) {
            const read_len = s.acquireBytes();
            if (read_len == 0) break;
            const n = try f.read(buf[total_read .. total_read + read_len], aligned_offset + total_read);
            if (n == 0) break; // EOF
            total_read += n;
        }

        // It's possible that buf was set up for a bigger read, but the file ended before that.
        // In that case, the end offset could be total_read.
        const end = @min(prefix + len, total_read);
        return .{
            .buf = buf,
            .data = buf[prefix..end],
            .allocator = allocator,
        };
    }

    pub const ReadRequest = struct {
        offset: u64,
        len: usize,
        data: []const u8 = "", // filled by batchRead
        ctx: ?*anyopaque = null, // opaque pointer for caller use
    };

    /// Batch multiple reads into a single io_uring submission.
    /// Allocates page-aligned buffers from the arena for O_DIRECT compatibility.
    /// Fills each request's .data field with the read result.
    /// Arena owns all internal buffers — no manual cleanup needed.
    // Max requests per io_uring submission. Must stay under the default SQ depth (256)
    // to avoid a race where flushing the SQ mid-add triggers completions that prematurely
    // signal the group.
    // Must stay under io_uring SQ depth (default 256). getSqe retries once
    // on SubmissionQueueFull, but if all SQEs are still in-flight, the retry
    // also fails. Keep a margin below 256 for other concurrent I/O.
    const max_batch_size: usize = 200;

    pub fn batchRead(
        self: FileHandle,
        arena: *std.heap.ArenaAllocator,
        requests: []ReadRequest,
        priority: Priority,
    ) !void {
        if (requests.len == 0) return;

        // Process in chunks to stay within io_uring SQ depth.
        var offset: usize = 0;
        while (offset < requests.len) {
            const end = @min(offset + max_batch_size, requests.len);
            try self.batchReadChunk(arena, requests[offset..end], priority);
            offset = end;
        }
    }

    fn batchReadChunk(
        self: FileHandle,
        arena: *std.heap.ArenaAllocator,
        requests: []ReadRequest,
        priority: Priority,
    ) !void {
        const alloc = arena.allocator();

        // Bandwidth: acquire total bytes for this chunk before submitting.
        {
            var total: usize = 0;
            for (requests) |r| total += r.len;
            var s = self.vfs.bw.spinner(priority, total, .read);
            defer s.deinit();
            while (true) {
                if (s.acquireBytes() == 0) break;
            }
        }

        const f = self.file.?;

        // One BatchOp + page-aligned buffer per request, heap-allocated from
        // the arena (fiber stacks are small). `preadBatch` submits them all
        // in a single libxev submission and parks once until every read
        // completes.
        const ops = try alloc.alloc(fiber.File.BatchOp, requests.len);
        const bufs = try alloc.alloc([]u8, requests.len);

        for (requests, 0..) |*req, i| {
            // Page-aligned offset and size for O_DIRECT compatibility.
            const aligned_offset = req.offset & ~@as(u64, page_size - 1);
            const prefix: usize = @intCast(req.offset - aligned_offset);
            const aligned_size = std.mem.alignForward(usize, prefix + req.len, page_size);

            bufs[i] = try alloc.alignedAlloc(u8, page_alignment, aligned_size);
            ops[i] = .{ .buf = bufs[i], .offset = aligned_offset };
        }

        try f.preadBatch(alloc, ops);

        // Fill request .data fields from the aligned read buffers.
        for (requests, 0..) |*req, i| {
            _ = try ops[i].result;
            const prefix: usize = @intCast(req.offset & (page_size - 1));
            req.data = bufs[i][prefix .. prefix + req.len];
        }
    }

    pub fn write(
        self: FileHandle,
        data: []const u8,
        offset: u64,
        priority: Priority,
    ) !void {
        const f = self.file.?;
        var s = self.vfs.bw.spinner(priority, data.len, .write);
        defer s.deinit();
        var written: usize = 0;
        while (true) {
            const chunk_len = s.acquireBytes();
            if (chunk_len == 0) break;
            var chunk_written: usize = 0;
            while (chunk_written < chunk_len) {
                const n = try f.write(data[written + chunk_written .. written + chunk_len], offset + written + chunk_written);
                if (n == 0) return error.WriteFailed;
                chunk_written += n;
            }
            written += chunk_len;
        }
        // Defensive: the spinner accounts what we ASKED to grant, so a
        // return of 0 means "fully granted". If for any reason `written`
        // doesn't match `data.len` the caller has been silently lied to —
        // surface as `error.ShortWrite` so the file isn't installed in the
        // manifest as if it were complete.
        if (written != data.len) return error.ShortWrite;
    }

    pub fn sync(self: FileHandle) !void {
        if (self.file) |f| f.sync(.{}) catch {};
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
/// for any realistic disk workload, low enough that `tryRefill`'s
/// `elapsed_ms * pages_per_sec` arithmetic stays well within u64 even with
/// the per-call elapsed clamp. Call `VFS.initGlobal()` to override.
pub fn g() *VFS {
    if (!g_initialized) {
        g_limiter = bandwidth.Limiter.init(.{ .bytes_per_sec = 1024 * 1024 * 1024 * 1024 });
        g_vfs = .{ .bw = &g_limiter };
        g_initialized = true;
    }
    return &g_vfs;
}

pub const VFS = struct {
    bw: *bandwidth.Limiter,

    pub const Config = bandwidth.Config;

    /// Initialize the global VFS instance with a specific bandwidth config.
    pub fn initGlobal(config: Config) void {
        g_limiter = bandwidth.Limiter.init(config);
        g_vfs = .{ .bw = &g_limiter };
        g_initialized = true;
    }

    /// Create a VFS with a caller-owned limiter.
    pub fn new(limiter: *bandwidth.Limiter) VFS {
        return .{ .bw = limiter };
    }

    pub fn openFile(self: *const VFS, allocator: std.mem.Allocator, path: []const u8, opts: FileOptions) !FileHandle {
        switch (opts.mode) {
            .mmap => {
                // mmap mode opens RO+default for the underlying fd; mapped
                // semantics live above the fd, not in its open flags.
                const fd = try openDirect(path, .{});
                errdefer closeFd(fd);

                const file_size = try fdSize(fd);
                const mapped = try posix.mmap(null, file_size, .{ .READ = true }, .{ .TYPE = .PRIVATE }, fd, 0);
                return .{ .file = fiber.File.fromFd(fd), .file_size = file_size, .mapped = mapped, .vfs = self };
            },
            .default, .direct => {
                const fd = try openDirect(path, opts);
                errdefer closeFd(fd);

                const file_size = try fdSize(fd);
                return .{ .file = fiber.File.fromFd(fd), .file_size = file_size, .vfs = self };
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
                };
            },
        }
    }

    pub fn createFile(self: *const VFS, path: []const u8, opts: FileOptions) !FileHandle {
        if (opts.mode == .mmap or opts.mode == .memory) return error.InvalidMode;

        var open_flags: posix.O = .{ .ACCMODE = .RDWR, .CREAT = true };
        if (opts.flags.truncate) {
            open_flags.TRUNC = true;
        } else {
            open_flags.EXCL = true;
        }
        if (builtin.os.tag == .linux and opts.mode == .direct) open_flags.DIRECT = true;
        if (opts.flags.dsync) open_flags.DSYNC = true;

        const fd = posix.openat(posix.AT.FDCWD, path, open_flags, 0o644) catch |err| switch (err) {
            // tmpfs doesn't support O_DIRECT — fall back to plain open.
            error.BadPathName => return self.createFile(path, .{ .flags = opts.flags }),
            else => return err,
        };

        if (builtin.os.tag == .macos and opts.mode == .direct) {
            _ = posix.fcntl(fd, 48, 1) catch {};
        }

        return .{ .file = fiber.File.fromFd(fd), .file_size = 0, .vfs = self };
    }

};

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

pub fn access(path: []const u8) !void {
    try fiber.Dir.cwd().access(path, .{});
}

/// Recursive mkdir-p. Wraps std.Io.Dir.createDirPath (fiber.fs doesn't have it).
pub fn createDirPath(path: []const u8) !void {
    try std.Io.Dir.cwd().createDirPath(utils.context.g().io, path);
}

/// Recursive delete. Wraps std.Io.Dir.deleteTree (fiber.fs doesn't have it).
pub fn deleteTree(path: []const u8) !void {
    try std.Io.Dir.cwd().deleteTree(utils.context.g().io, path);
}

pub fn renameFile(old_path: []const u8, new_path: []const u8) !void {
    try fiber.Dir.cwd().rename(old_path, fiber.Dir.cwd(), new_path);
}

pub fn deleteFile(path: []const u8) !void {
    try fiber.Dir.cwd().deleteFile(path);
}

/// Best-effort directory fsync so newly created filenames are durable.
/// Silently ignores errors (tmpfs, some network mounts don't support dir fsync).
pub fn syncDir(path: []const u8) void {
    const fd = posix.openat(posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .DIRECTORY = true }, 0) catch return;
    const f = fiber.File.fromFd(fd);
    defer f.close();
    f.sync(.{}) catch {};
}

// ═══════════════════════════════════════════════════════════════════════
// High-level file-content helpers (one-shot create/read)
// ═══════════════════════════════════════════════════════════════════════
//
// These are convenience wrappers for test setup / small-file scenarios.
// For SST-style I/O, use `VFS.openFile` / `VFS.createFile` + `FileHandle`.

/// Create (truncating if it exists) and write `data` in one shot.
pub fn writeFile(path: []const u8, data: []const u8) !void {
    const f = try fiber.Dir.cwd().createFile(path, .{});
    defer f.close();
    var off: u64 = 0;
    while (off < data.len) {
        off += try f.write(data[off..], off);
    }
}

/// Open and read the whole file. Caller owns the returned buffer.
pub fn readFile(path: []const u8, allocator: std.mem.Allocator) ![]u8 {
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
pub fn fileSize(path: []const u8) !u64 {
    const f = try fiber.Dir.cwd().openFile(path, .{});
    defer f.close();
    return (try f.stat()).size;
}

// ═══════════════════════════════════════════════════════════════════════
// Directory iteration
// ═══════════════════════════════════════════════════════════════════════
//
// fiber.fs doesn't expose iteration today. We wrap std.Io.Dir's iterator
// so callers don't have to thread `io` into every `next()` call.

pub const DirIter = struct {
    inner: std.Io.Dir,
    iter: std.Io.Dir.Iterator,

    pub const Entry = std.Io.Dir.Entry;

    pub fn next(self: *DirIter) !?Entry {
        return self.iter.next(utils.context.g().io);
    }

    pub fn deinit(self: *DirIter) void {
        self.inner.close(utils.context.g().io);
    }
};

pub fn iterateDir(path: []const u8) !DirIter {
    const io = utils.context.g().io;
    const d = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true });
    return .{ .inner = d, .iter = d.iterate() };
}

/// Open a file according to `opts`. Honors `mode = .direct` (O_DIRECT,
/// Linux only) and `flags.{ writable, dsync }`. Falls back to a plain
/// open without O_DIRECT if the filesystem rejects it (e.g. tmpfs returns
/// EINVAL → BadPathName); the fallback keeps O_DSYNC because every fs we
/// care about supports it.
///
/// Takes `FileOptions` rather than expanding into one parameter per flag
/// so this function's signature stays stable as new flags are added.
fn openDirect(path: []const u8, opts: FileOptions) !posix.fd_t {
    const direct_io = opts.mode == .direct;
    const accmode: posix.ACCMODE = if (opts.flags.writable) .RDWR else .RDONLY;
    var flags: posix.O = .{ .ACCMODE = accmode };
    if (builtin.os.tag == .linux and direct_io) flags.DIRECT = true;
    if (opts.flags.dsync) flags.DSYNC = true;

    const fd = posix.openat(posix.AT.FDCWD, path, flags, 0) catch |err| switch (err) {
        error.BadPathName => {
            var fb: posix.O = .{ .ACCMODE = accmode };
            if (opts.flags.dsync) fb.DSYNC = true;
            return posix.openat(posix.AT.FDCWD, path, fb, 0);
        },
        else => return err,
    };

    if (builtin.os.tag == .macos and direct_io) {
        // F_NOCACHE = 48 on macOS — bypass the unified buffer cache.
        _ = posix.fcntl(fd, 48, 1) catch {};
    }

    return fd;
}
