// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! File I/O for fibers, split by how each op is serviced:
//!
//!   - **Data plane** (`read`/`write`/`preadBatch`): positional `pread`/
//!     `pwrite` submitted to the current worker's libxev loop; the fiber
//!     parks and the completion callback wakes it. Fully async.
//!   - **Blocking ops** (`sync`, `setSize`, the `Dir` path ops open/
//!     create/access/rename/delete/mkdir/rmtree, and `Dir.list`): libxev
//!     can't do these async, so they run on the `fsops` thread pool — the
//!     fiber parks, a pool thread runs the syscall and wakes it. The
//!     worker thread stays free, and the big path-buffer frames run on
//!     the pool thread's full-size OS stack.
//!   - **Instant ops** (`close`, `stat`, `device`, the readahead and
//!     cache hints, `fsMagic`): plain inline syscalls (no disk I/O), so a
//!     pool round-trip would be pure overhead.
//!
//! The blocking/instant syscalls go through `std.Io.File`/`std.Io.Dir` on the
//! pool's blocking `std.Io` (`fsops.io()`) rather than raw `std.os.linux` —
//! cross-platform, and no errno/path-buffer boilerplate. Only the libxev data
//! plane is fiber-native.
//!
//! Every file operation in the process comes through here: nothing above
//! calls `std.Io.Dir`, `std.posix` or a raw syscall for a file. That is
//! what lets the simulation's disk replace this one layer.

const std = @import("std");
const builtin = @import("builtin");
const xev = @import("xev");
const _fb = @import("fiber.zig");
const _rt = @import("runtime.zig");
const _fsops = @import("fsops.zig");

const Fiber = _fb.Fiber;
const SioFile = std.Io.File;
const SioDir = std.Io.Dir;

/// Wrap a raw fd as a `std.Io.File` (blocking) for a `std.Io` syscall.
fn sioFile(fd: SioFile.Handle) SioFile {
    return .{ .handle = fd, .flags = .{ .nonblocking = false } };
}

// ───────────────────────── File ─────────────────────────

pub const Stat = struct {
    size: u64,
    /// Last modification time in whole seconds since the UTC epoch, on
    /// the realtime clock — the same scale as
    /// `timestampNow(.realtime, .secs)`, so an age is a subtraction.
    mtime_s: i64,
};

/// Flags accepted by `File.sync` for zio API parity. Ignored — `std.Io`'s
/// `sync` is a full flush.
pub const SyncFlags = struct {
    /// fdatasync: data and the metadata needed to read it back (size,
    /// block map), not timestamps. The WAL's durability point.
    data_only: bool = false,
};

pub const File = struct {
    fd: SioFile.Handle,

    pub fn fromFd(fd: SioFile.Handle) File {
        return .{ .fd = fd };
    }

    /// Positional read. Parks until the op completes. Returns the number
    /// of bytes read (`0` = EOF). Async via libxev (io_uring submits the
    /// op directly; the epoll backend offloads to libxev's threadpool) —
    /// never blocks the worker thread.
    pub fn read(self: File, buf: []u8, offset: u64) xev.ReadError!usize {
        const w = _rt.currentWorker();
        const f = w.current_fiber.?;
        const xf = xev.File.initFd(self.fd);
        var c: xev.Completion = undefined;
        var waiter: ReadWaiter = .{ .fiber = f, .result = 0 };
        xf.pread(&w.loop, &c, .{ .slice = buf }, offset, ReadWaiter, &waiter, preadCallback);
        parkAssertDone(&waiter.done);
        return waiter.result;
    }

    /// Positional write. Parks until the op completes. Returns the number
    /// of bytes written (may be short).
    pub fn write(self: File, data: []const u8, offset: u64) xev.WriteError!usize {
        const w = _rt.currentWorker();
        const f = w.current_fiber.?;
        const xf = xev.File.initFd(self.fd);
        var c: xev.Completion = undefined;
        var waiter: WriteWaiter = .{ .fiber = f, .result = 0 };
        xf.pwrite(&w.loop, &c, .{ .slice = data }, offset, WriteWaiter, &waiter, pwriteCallback);
        parkAssertDone(&waiter.done);
        return waiter.result;
    }

    /// A single positional read request for `preadBatch`, on `file`.
    /// `result` is filled by the completion callback.
    pub const BatchOp = struct {
        file: File,
        buf: []u8,
        offset: u64,
        result: xev.ReadError!usize = 0,
    };

    /// Flush data + metadata to disk (data only with `data_only`). libxev
    /// has no fsync op, so the blocking syscall runs on the `fsops` pool
    /// while this fiber parks — the worker thread stays free for other
    /// fibers.
    pub fn sync(self: File, flags: SyncFlags) !void {
        return _fsops.run(syncImpl, .{ self.fd, flags.data_only });
    }

    /// Truncate / extend the file to `length` bytes. Offloaded (can block on
    /// disk).
    pub fn setSize(self: File, length: u64) !void {
        return _fsops.run(setLengthImpl, .{ self.fd, length });
    }

    /// Stat the open file: its size and modification time. A fast `fstat`
    /// (cached inode), so it runs inline.
    pub fn stat(self: File) !Stat {
        const st = try sioFile(self.fd).stat(_fsops.io());
        return .{ .size = st.size, .mtime_s = @intCast(@divFloor(st.mtime.nanoseconds, std.time.ns_per_s)) };
    }

    pub fn size(self: File) !u64 {
        return (try self.stat()).size;
    }

    /// Close releases the fd (instant), so it runs inline.
    pub fn close(self: File) void {
        sioFile(self.fd).close(_fsops.io());
    }

    /// The device (`st_dev`) holding the file: what tells directories
    /// on one disk from those on different ones. `std.Io.File.Stat`
    /// leaves it out, so the system call is made here: statx on Linux,
    /// fstat on macOS. Inline, as `stat` is.
    pub fn device(self: File) !u64 {
        switch (builtin.os.tag) {
            .linux => {
                const linux = std.os.linux;
                var stx: linux.Statx = undefined;
                const rc = linux.statx(
                    self.fd,
                    "",
                    linux.AT.EMPTY_PATH,
                    .{},
                    &stx,
                );
                if (linux.errno(rc) != .SUCCESS) return error.StatFailed;
                return (@as(u64, stx.dev_major) << 32) | stx.dev_minor;
            },
            .macos => {
                var st: std.c.Stat = undefined;
                if (std.c.fstat(self.fd, &st) != 0) return error.StatFailed;
                return @bitCast(@as(i64, st.dev));
            },
            else => return error.Unsupported,
        }
    }

    /// Turns readahead off for the file; best effort.
    pub fn adviseRandom(self: File) void {
        switch (builtin.os.tag) {
            .linux => {
                const linux = std.os.linux;
                _ = linux.fadvise(self.fd, 0, 0, linux.POSIX_FADV.RANDOM);
            },
            // F_RDAHEAD 0 turns readahead off.
            .macos => _ = std.c.fcntl(self.fd, std.c.F.RDAHEAD, @as(c_int, 0)),
            else => {},
        }
    }

    /// macOS's stand-in for O_DIRECT: F_NOCACHE bypasses the unified
    /// buffer cache. Best effort, a no-op elsewhere.
    pub fn noCache(self: File) void {
        if (builtin.os.tag == .macos)
            _ = std.c.fcntl(self.fd, std.c.F.NOCACHE, @as(c_int, 1));
    }
};

fn syncImpl(fd: SioFile.Handle, data_only: bool) !void {
    if (builtin.os.tag.isDarwin()) return fullFsync(fd);
    // A data-only sync leaves the inode's timestamps out, which is what
    // a log append wants. Where the OS has no fdatasync a full sync is
    // the honest equivalent.
    const has_fdatasync = switch (builtin.os.tag) {
        .linux, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => true,
        else => false,
    };
    if (data_only and has_fdatasync) return std.posix.fdatasync(fd);
    return sioFile(fd).sync(_fsops.io());
}

/// macOS: fsync(2) only hands the data to the drive, so a commit would
/// not survive a power cut. F_FULLFSYNC also asks the drive to flush to
/// the media, which is what fsync and fdatasync already mean on Linux.
/// A filesystem without it (network mounts) answers ENOTSUP and gets
/// the plain fsync. std.posix has no fcntl wrapper, hence the raw call.
fn fullFsync(fd: SioFile.Handle) !void {
    const rc = std.c.fcntl(fd, std.c.F.FULLFSYNC, @as(c_int, 0));
    switch (std.posix.errno(rc)) {
        .SUCCESS => return,
        .OPNOTSUPP, .INVAL => return sioFile(fd).sync(_fsops.io()),
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        else => |e| return std.posix.unexpectedErrno(e),
    }
}

fn setLengthImpl(fd: SioFile.Handle, length: u64) !void {
    return sioFile(fd).setLength(_fsops.io(), length);
}

fn parkAssertDone(done: *std.atomic.Value(bool)) void {
    _rt.park();
    // Invariant: resume ONLY when the io_uring completion fired and set `done`.
    // A resume with `!done` is a spurious wake — fail loud at the source rather
    // than silently re-parking.
    if (!done.load(.acquire)) {
        @panic("fs: fiber woken before its I/O completed — spurious wake");
    }
}

const ReadWaiter = struct {
    fiber: *Fiber,
    result: xev.ReadError!usize,
    done: std.atomic.Value(bool) = .init(false),
};

fn preadCallback(
    ud: ?*ReadWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.File,
    _: xev.ReadBuffer,
    r: xev.ReadError!usize,
) xev.CallbackAction {
    const w = ud.?;
    w.result = r;
    w.done.store(true, .release);
    _rt.wake(w.fiber);
    return .disarm;
}

const WriteWaiter = struct {
    fiber: *Fiber,
    result: xev.WriteError!usize,
    done: std.atomic.Value(bool) = .init(false),
};

fn pwriteCallback(
    ud: ?*WriteWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.File,
    _: xev.WriteBuffer,
    r: xev.WriteError!usize,
) xev.CallbackAction {
    const w = ud.?;
    w.result = r;
    w.done.store(true, .release);
    _rt.wake(w.fiber);
    return .disarm;
}

/// Submit every request in `ops` to the worker loop at once and park a
/// single time until all complete — one loop poll, one wake. The kernel
/// sees all SQEs in one submission (the batching that `vfs.batchRead`
/// relies on) without needing scatter-gather: each op is an independent
/// single-buffer pread at its own offset on its own file, so one batch
/// can span files.
///
/// `allocator` backs the transient completion + context arrays; they
/// only need to outlive this call (the fiber stays parked until the
/// last completion fires, so they survive across the park).
pub fn preadBatch(allocator: std.mem.Allocator, ops: []File.BatchOp) !void {
    if (ops.len == 0) return;
    const w = _rt.currentWorker();
    const f = w.current_fiber.?;

    const comps = try allocator.alloc(xev.Completion, ops.len);
    defer allocator.free(comps);
    const ctxs = try allocator.alloc(BatchCtx, ops.len);
    defer allocator.free(ctxs);

    // Single shared waiter. Callbacks run on this worker's loop (one
    // thread), so the `remaining` countdown needs no atomics.
    var batch: BatchWaiter = .{ .fiber = f, .remaining = .init(ops.len) };
    for (ops, comps, ctxs) |*op, *c, *ctx| {
        ctx.* = .{ .batch = &batch, .op = op };
        const xf = xev.File.initFd(op.file.fd);
        xf.pread(
            &w.loop,
            c,
            .{ .slice = op.buf },
            op.offset,
            BatchCtx,
            ctx,
            batchCallback,
        );
    }
    // Invariant: the LAST completion (which drives `remaining` to 0) is what
    // wakes us, so on resume every CQE has been reaped and the stack `comps`
    // (freed when this frame returns) safely outlived their ops. A resume
    // with `remaining != 0` is a spurious wake — fail loud at the source.
    _rt.park();
    if (batch.remaining.load(.acquire) != 0)
        @panic("fs.preadBatch: woken before all reads completed");
}

const BatchWaiter = struct {
    fiber: *Fiber,
    /// Outstanding ops. Atomic: the owner thread decrements it in callbacks,
    /// but the parked fiber loads it in its re-park loop (it may resume on a
    /// different worker), so a plain field would be a data race.
    remaining: std.atomic.Value(usize),
};

const BatchCtx = struct {
    batch: *BatchWaiter,
    op: *File.BatchOp,
};

fn batchCallback(
    ud: ?*BatchCtx,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.File,
    _: xev.ReadBuffer,
    r: xev.ReadError!usize,
) xev.CallbackAction {
    const ctx = ud.?;
    ctx.op.result = r;
    // `- 1` returns the OLD value, so the op that drove it to 0 wakes the fiber.
    if (ctx.batch.remaining.fetchSub(1, .acq_rel) == 1) _rt.wake(ctx.batch.fiber);
    return .disarm;
}

// ───────────────────────── Dir / metadata ─────────────────────────
//
// Control-plane filesystem ops, offloaded to the `fsops` pool (they can block
// on disk, and run on a pool thread's full-size stack). API-compatible with
// the `zio.Dir.cwd().<op>` forms `vfs.zig` uses. The caller's `path` slice
// stays valid across the park (the fiber's stack persists while parked).

/// Flags accepted for zio API parity; ignored (defaults used).
pub const AccessFlags = struct {};
pub const CreateFlags = struct {};
pub const OpenFlags = struct {};

pub const Dir = struct {
    pub fn cwd() Dir {
        return .{};
    }

    pub fn access(self: Dir, path: []const u8, flags: AccessFlags) !void {
        _ = self;
        _ = flags;
        return _fsops.run(accessImpl, .{path});
    }

    pub fn rename(self: Dir, old_path: []const u8, new_dir: Dir, new_path: []const u8) !void {
        _ = self;
        _ = new_dir;
        return _fsops.run(renameImpl, .{ old_path, new_path });
    }

    pub fn deleteFile(self: Dir, path: []const u8) !void {
        _ = self;
        return _fsops.run(deleteImpl, .{path});
    }

    /// Create (truncating) a file with read+write access, returning a fiber `File`.
    // TODO: Set up flags to replicate vfs.createFile.
    pub fn createFile(self: Dir, path: []const u8, flags: CreateFlags) !File {
        _ = self;
        _ = flags;
        return _fsops.run(createImpl, .{path});
    }

    /// Open an existing file read-only, returning a fiber `File`.
    pub fn openFile(self: Dir, path: []const u8, flags: OpenFlags) !File {
        _ = self;
        _ = flags;
        return _fsops.run(openImpl, .{path});
    }

    /// Recursive mkdir -p. Offloaded: directory metadata can block.
    pub fn createDirPath(self: Dir, path: []const u8) !void {
        _ = self;
        return _fsops.run(createDirPathImpl, .{path});
    }

    /// Recursive delete of `path` and everything under it. Offloaded.
    pub fn deleteTree(self: Dir, path: []const u8) !void {
        _ = self;
        return _fsops.run(deleteTreeImpl, .{path});
    }

    /// Open the directory `path` read-only as a `File`, for `sync`: a
    /// directory fsync is what makes a created or renamed name durable.
    pub fn openDir(self: Dir, path: []const u8) !File {
        _ = self;
        const fd = try openPosix(
            path,
            .{ .ACCMODE = .RDONLY, .DIRECTORY = true },
            0,
        );
        return File.fromFd(fd);
    }

    /// Every entry of `path`, names duped out of `alloc`. Release the
    /// result with `freeEntries`. Offloaded: libxev has no readdir op, and
    /// the loop blocks once per getdents buffer — on the pool thread that
    /// costs a worker nothing, and the iterator's ~1 KiB path frames run
    /// on a full-size OS stack.
    pub fn list(self: Dir, alloc: std.mem.Allocator, path: []const u8) ![]Entry {
        _ = self;
        return _fsops.run(listImpl, .{ alloc, path });
    }
};

/// One directory entry: the name (owned by the caller's allocator) and
/// what it is.
pub const Entry = struct {
    name: []const u8,
    kind: Kind,
};

pub const Kind = SioFile.Kind;

/// Free a slice returned by `Dir.list`, names included.
pub fn freeEntries(alloc: std.mem.Allocator, entries: []const Entry) void {
    for (entries) |e| alloc.free(e.name);
    alloc.free(entries);
}

fn listImpl(alloc: std.mem.Allocator, path: []const u8) ![]Entry {
    var dir = try SioDir.cwd().openDir(_fsops.io(), path, .{ .iterate = true });
    defer dir.close(_fsops.io());

    var out: std.ArrayList(Entry) = .empty;
    errdefer {
        for (out.items) |e| alloc.free(e.name);
        out.deinit(alloc);
    }

    var it = dir.iterate();
    while (try it.next(_fsops.io())) |entry| {
        const name = try alloc.dupe(u8, entry.name);
        errdefer alloc.free(name);
        try out.append(alloc, .{ .name = name, .kind = entry.kind });
    }
    return out.toOwnedSlice(alloc);
}

fn accessImpl(path: []const u8) !void {
    return SioDir.cwd().access(_fsops.io(), path, .{});
}

fn createDirPathImpl(path: []const u8) !void {
    return SioDir.cwd().createDirPath(_fsops.io(), path);
}

fn deleteTreeImpl(path: []const u8) !void {
    return SioDir.cwd().deleteTree(_fsops.io(), path);
}

fn renameImpl(old_path: []const u8, new_path: []const u8) !void {
    return SioDir.cwd().rename(old_path, SioDir.cwd(), new_path, _fsops.io());
}

fn deleteImpl(path: []const u8) !void {
    return SioDir.cwd().deleteFile(_fsops.io(), path);
}

fn createImpl(path: []const u8) !File {
    const f = try SioDir.cwd().createFile(_fsops.io(), path, .{ .read = true, .truncate = true });
    return File.fromFd(f.handle);
}

fn openImpl(path: []const u8) !File {
    const f = try SioDir.cwd().openFile(_fsops.io(), path, .{ .mode = .read_only });
    return File.fromFd(f.handle);
}

/// Open `path` with raw posix flags, offloaded to the `fsops` pool — open can
/// block on disk metadata, and this way it runs on a pool thread's full-size
/// stack instead of stalling the fiber's worker. Returns the raw fd.
///
/// Exists because callers needing O_DIRECT / O_DSYNC (e.g. `vfs`) can't express
/// those through `std.Io.File`'s portable open flags, so they build the raw
/// `posix.O` themselves and hand it here. `path` stays valid across the park
/// (it lives on the parked fiber's stack).
pub fn openPosix(
    path: []const u8,
    flags: std.posix.O,
    mode: std.posix.mode_t,
) std.posix.OpenError!std.posix.fd_t {
    return _fsops.run(openPosixImpl, .{ path, flags, mode });
}

fn openPosixImpl(
    path: []const u8,
    flags: std.posix.O,
    mode: std.posix.mode_t,
) std.posix.OpenError!std.posix.fd_t {
    return std.posix.openat(std.posix.AT.FDCWD, path, flags, mode);
}

/// The filesystem magic of `path` (statfs `f_type`), or null when it
/// cannot be read. Linux only: the magic numbers and the statfs layout
/// are Linux's; elsewhere the answer is null. Inline: one call at a
/// WAL's open.
pub fn fsMagic(path: []const u8) ?i64 {
    if (builtin.os.tag != .linux) return null;
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
    const path_z = std.posix.toPosixPath(path) catch return null;
    var st: Statfs = undefined;
    const rc = std.os.linux.syscall2(
        .statfs,
        @intFromPtr(&path_z),
        @intFromPtr(&st),
    );
    if (std.os.linux.errno(rc) != .SUCCESS) return null;
    return st.f_type;
}
