// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! File I/O for fibers. Every file operation in the process is one `Op`
//! handed to `submit`, which services it one of three ways:
//!
//!   - **Data plane** (`read`, `write`, `read_batch`): positional `pread`/
//!     `pwrite` submitted to the current worker's libxev loop; the fiber
//!     parks and the completion callback wakes it. Fully async.
//!   - **Blocking ops** (`sync`, `set_size`, `open`, and the path ops
//!     access/rename/delete/mkdir/rmtree/list/set_mtime): libxev can't do these
//!     async, so they run on the `fsops` thread pool — the fiber parks, a
//!     pool thread runs the syscall and wakes it. The worker thread stays
//!     free, and the big path-buffer frames run on the pool thread's
//!     full-size OS stack.
//!   - **Instant ops** (`close`, `stat`, `device`, the readahead and
//!     cache hints, `fs_magic`): plain inline syscalls (no disk I/O), so
//!     a pool round-trip would be pure overhead.
//!
//! `vfs` is the typed face over `submit` and its only caller: nothing
//! above this file calls `std.Io.Dir`, `std.posix` or a raw syscall for
//! a file, so one function sees every file operation. Under a sim that
//! function hands every op to `simdisk.zig` instead of the kernel.
//!
//! The blocking/instant syscalls go through `std.Io.File`/`std.Io.Dir`
//! on the pool's blocking `std.Io` (`fsops.io()`) rather than raw
//! `std.os.linux` — cross-platform, and no errno/path-buffer boilerplate.
//! Only the libxev data plane is fiber-native.

const std = @import("std");
const builtin = @import("builtin");
const xev = @import("xev");
const _fb = @import("fiber.zig");
const _rt = @import("runtime.zig");
const _fsops = @import("fsops.zig");

const String = @import("utils").String;
const Allocator = std.mem.Allocator;
const Fiber = _fb.Fiber;
const SioFile = std.Io.File;
const SioDir = std.Io.Dir;
const Fd = SioFile.Handle;

/// The one handle for an open file, returned by `open` and carried by
/// every op on it. Only this file reads `fd`: above zfiber a `File` is
/// opaque, so the simulation's disk can hand out its own.
pub const File = struct {
    fd: Fd,
};

/// Wrap a raw fd as a `std.Io.File` (blocking) for a `std.Io` syscall.
fn sioFile(fd: Fd) SioFile {
    return .{ .handle = fd, .flags = .{ .nonblocking = false } };
}

// ───────────────────────── Op / submit ─────────────────────────

/// Every file operation there is, with its arguments; `submit` runs it.
pub const Op = union(enum) {
    read: Read,
    write: Write,
    read_batch: ReadBatch,
    sync: Sync,
    set_size: SetSize,
    stat: File,
    close: File,
    device: File,
    advise_random: File,
    no_cache: File,
    open: Open,
    access: String,
    rename: Rename,
    delete_file: String,
    create_dir_path: String,
    delete_tree: String,
    list: List,
    fs_magic: String,
    set_mtime: SetMtime,

    pub const Tag = std.meta.Tag(Op);

    /// The payload of one `Op`: what `submit(tag, ...)` takes.
    pub fn Args(comptime tag: Tag) type {
        return @FieldType(Op, @tagName(tag));
    }

    pub const Read = struct { file: File, buf: []u8, offset: u64 };
    pub const Write = struct { file: File, data: String, offset: u64 };
    /// Independent single-buffer preads, one park for all of them.
    pub const ReadBatch = struct { alloc: Allocator, ops: []BatchOp };
    pub const Sync = struct { file: File, data_only: bool };
    pub const SetSize = struct { file: File, length: u64 };
    /// Raw posix flags: O_DIRECT and O_DSYNC have no portable spelling.
    pub const Open = struct {
        path: String,
        flags: std.posix.O,
        mode: std.posix.mode_t,
    };
    pub const Rename = struct { old_path: String, new_path: String };
    pub const List = struct { alloc: Allocator, path: String };
    /// Whole seconds since the Unix epoch, the scale `Stat.mtime_s` is.
    pub const SetMtime = struct { path: String, mtime_s: i64 };
};

/// What `submit(tag, ...)` returns. Any error: the real disk answers
/// with std's and libxev's names, the simulated one with its own, and
/// no set borrowed from either library holds both.
pub fn Result(comptime tag: Op.Tag) type {
    return switch (tag) {
        .read, .write => anyerror!usize,
        .stat => anyerror!Stat,
        .device => anyerror!u64,
        .open => anyerror!File,
        .list => anyerror![]Entry,
        .read_batch,
        .sync,
        .set_size,
        .access,
        .rename,
        .delete_file,
        .create_dir_path,
        .delete_tree,
        .set_mtime,
        => anyerror!void,
        .close, .advise_random, .no_cache => void,
        .fs_magic => ?i64,
    };
}

/// The one door: runs `tag` with `args` and returns its result. Must be
/// called on a fiber (the data-plane and blocking ops park). The caller's
/// slices stay valid across the park: a parked fiber's stack persists.
pub fn submit(comptime tag: Op.Tag, args: Op.Args(tag)) Result(tag) {
    if (_rt.simDisk()) |disk| {
        return disk.apply(tag, args);
    }
    switch (tag) {
        // Data plane: on this worker's libxev loop, parked on completion.
        .read => return readImpl(args),
        .write => return writeImpl(args),
        .read_batch => return readBatchImpl(args),
        // May block on disk: on the fsops pool, parked until it returns.
        .sync => return _fsops.run(syncImpl, .{args}),
        .set_size => return _fsops.run(setSizeImpl, .{args}),
        .open => return _fsops.run(openImpl, .{args}),
        .access => return _fsops.run(accessImpl, .{args}),
        .rename => return _fsops.run(renameImpl, .{args}),
        .delete_file => return _fsops.run(deleteFileImpl, .{args}),
        .create_dir_path => return _fsops.run(createDirPathImpl, .{args}),
        .delete_tree => return _fsops.run(deleteTreeImpl, .{args}),
        .list => return _fsops.run(listImpl, .{args}),
        .set_mtime => return _fsops.run(setMtimeImpl, .{args}),
        // No disk I/O: inline.
        .stat => return statImpl(args),
        .close => return closeImpl(args),
        .device => return deviceImpl(args),
        .advise_random => return adviseRandomImpl(args),
        .no_cache => return noCacheImpl(args),
        .fs_magic => return fsMagicImpl(args),
    }
}

// ───────────────────────── Shared types ─────────────────────────

pub const Stat = struct {
    size: u64,
    /// Last modification time in whole seconds since the UTC epoch, on
    /// the realtime clock — the same scale as
    /// `timestampNow(.realtime, .secs)`, so an age is a subtraction.
    mtime_s: i64,
};

/// A single positional read in a `read_batch`. `result` is filled by
/// the completion callback.
pub const BatchOp = struct {
    file: File,
    buf: []u8,
    offset: u64,
    result: anyerror!usize = 0,
};

/// One directory entry: the name (owned by the caller's allocator) and
/// what it is.
pub const Entry = struct {
    name: String,
    kind: Kind,
};

pub const Kind = SioFile.Kind;

/// Free a slice returned by `.list`, names included.
pub fn freeEntries(alloc: Allocator, entries: []const Entry) void {
    for (entries) |e| alloc.free(e.name);
    alloc.free(entries);
}

// ───────────────────────── Data plane (libxev) ─────────────────────────

/// Async via libxev (io_uring submits the op directly; the epoll backend
/// offloads to libxev's threadpool) — never blocks the worker thread.
fn readImpl(op: Op.Read) !usize {
    const w = _rt.currentWorker();
    const f = w.current_fiber.?;
    const xf = xev.File.initFd(op.file.fd);
    var c: xev.Completion = undefined;
    var waiter: ReadWaiter = .{ .fiber = f, .result = 0 };
    xf.pread(
        &w.loop,
        &c,
        .{ .slice = op.buf },
        op.offset,
        ReadWaiter,
        &waiter,
        preadCallback,
    );
    parkAssertDone(&waiter.done);
    return waiter.result;
}

fn writeImpl(op: Op.Write) !usize {
    const w = _rt.currentWorker();
    const f = w.current_fiber.?;
    const xf = xev.File.initFd(op.file.fd);
    var c: xev.Completion = undefined;
    var waiter: WriteWaiter = .{ .fiber = f, .result = 0 };
    xf.pwrite(
        &w.loop,
        &c,
        .{ .slice = op.data },
        op.offset,
        WriteWaiter,
        &waiter,
        pwriteCallback,
    );
    parkAssertDone(&waiter.done);
    return waiter.result;
}

fn parkAssertDone(done: *std.atomic.Value(bool)) void {
    _rt.park();
    // Invariant: resume ONLY when the io_uring completion fired and set
    // `done`. A resume with `!done` is a spurious wake — fail loud at the
    // source rather than silently re-parking.
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

/// Every request in `op.ops` goes to the worker loop at once and the
/// fiber parks a single time until all complete — one loop poll, one
/// wake. The kernel sees all SQEs in one submission (the batching that
/// `vfs.batchRead` relies on) without scatter-gather: each op is an
/// independent single-buffer pread at its own offset on its own file,
/// so one batch can span files. `op.alloc` backs the transient
/// completion + context arrays, which only need to outlive this call.
fn readBatchImpl(op: Op.ReadBatch) !void {
    const ops = op.ops;
    if (ops.len == 0) return;
    const w = _rt.currentWorker();
    const f = w.current_fiber.?;

    const comps = try op.alloc.alloc(xev.Completion, ops.len);
    defer op.alloc.free(comps);
    const ctxs = try op.alloc.alloc(BatchCtx, ops.len);
    defer op.alloc.free(ctxs);

    // Single shared waiter. Callbacks run on this worker's loop (one
    // thread), so the `remaining` countdown needs no atomics.
    var batch: BatchWaiter = .{ .fiber = f, .remaining = .init(ops.len) };
    for (ops, comps, ctxs) |*req, *c, *ctx| {
        ctx.* = .{ .batch = &batch, .op = req };
        const xf = xev.File.initFd(req.file.fd);
        xf.pread(
            &w.loop,
            c,
            .{ .slice = req.buf },
            req.offset,
            BatchCtx,
            ctx,
            batchCallback,
        );
    }
    // Invariant: the LAST completion (which drives `remaining` to 0) is
    // what wakes us, so on resume every CQE has been reaped and the stack
    // `comps` (freed when this frame returns) safely outlived their ops.
    // A resume with `remaining != 0` is a spurious wake — fail loud.
    _rt.park();
    if (batch.remaining.load(.acquire) != 0)
        @panic("fs.read_batch: woken before all reads completed");
}

const BatchWaiter = struct {
    fiber: *Fiber,
    /// Outstanding ops. Atomic: the owner thread decrements it in
    /// callbacks, but the parked fiber loads it on resume (it may resume
    /// on a different worker), so a plain field would be a data race.
    remaining: std.atomic.Value(usize),
};

const BatchCtx = struct {
    batch: *BatchWaiter,
    op: *BatchOp,
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
    // `- 1` returns the OLD value, so the op that drove it to 0 wakes.
    if (ctx.batch.remaining.fetchSub(1, .acq_rel) == 1)
        _rt.wake(ctx.batch.fiber);
    return .disarm;
}

// ───────────────────────── Blocking (fsops pool) ─────────────────────────
//
// Each runs on a pool thread: no fiber-runtime calls in here.

fn syncImpl(op: Op.Sync) !void {
    if (builtin.os.tag.isDarwin()) return fullFsync(op.file.fd);
    // A data-only sync leaves the inode's timestamps out, which is what
    // a log append wants. Where the OS has no fdatasync a full sync is
    // the honest equivalent.
    const has_fdatasync = switch (builtin.os.tag) {
        .linux, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => true,
        else => false,
    };
    if (op.data_only and has_fdatasync)
        return std.posix.fdatasync(op.file.fd);
    return sioFile(op.file.fd).sync(_fsops.io());
}

/// macOS: fsync(2) only hands the data to the drive, so a commit would
/// not survive a power cut. F_FULLFSYNC also asks the drive to flush to
/// the media, which is what fsync and fdatasync already mean on Linux.
/// A filesystem without it (network mounts) answers ENOTSUP and gets
/// the plain fsync. std.posix has no fcntl wrapper, hence the raw call.
fn fullFsync(fd: Fd) !void {
    const rc = std.c.fcntl(fd, std.c.F.FULLFSYNC, @as(c_int, 0));
    switch (std.posix.errno(rc)) {
        .SUCCESS => return,
        .OPNOTSUPP, .INVAL => return sioFile(fd).sync(_fsops.io()),
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        else => |e| return std.posix.unexpectedErrno(e),
    }
}

fn setSizeImpl(op: Op.SetSize) !void {
    return sioFile(op.file.fd).setLength(_fsops.io(), op.length);
}

/// The one open. CLOEXEC on every fd: a forked child (none today) must
/// not inherit a WAL or an O_DIRECT handle.
fn openImpl(op: Op.Open) !File {
    var flags = op.flags;
    if (@hasField(std.posix.O, "CLOEXEC")) flags.CLOEXEC = true;
    const fd = try std.posix.openat(
        std.posix.AT.FDCWD,
        op.path,
        flags,
        op.mode,
    );
    return .{ .fd = fd };
}

fn accessImpl(path: String) !void {
    return SioDir.cwd().access(_fsops.io(), path, .{});
}

fn renameImpl(op: Op.Rename) !void {
    return SioDir.cwd().rename(
        op.old_path,
        SioDir.cwd(),
        op.new_path,
        _fsops.io(),
    );
}

fn deleteFileImpl(path: String) !void {
    return SioDir.cwd().deleteFile(_fsops.io(), path);
}

fn createDirPathImpl(path: String) !void {
    return SioDir.cwd().createDirPath(_fsops.io(), path);
}

fn deleteTreeImpl(path: String) !void {
    return SioDir.cwd().deleteTree(_fsops.io(), path);
}

/// libxev has no readdir op, and the loop blocks once per getdents
/// buffer — on the pool thread that costs a worker nothing, and the
/// iterator's ~1 KiB path frames run on a full-size OS stack.
fn listImpl(op: Op.List) ![]Entry {
    const alloc = op.alloc;
    var dir = try SioDir.cwd().openDir(_fsops.io(), op.path, .{
        .iterate = true,
    });
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

/// A test's way to age a file without waiting: the orphan scans judge
/// by mtime.
fn setMtimeImpl(op: Op.SetMtime) !void {
    const ns: i96 = @as(i96, op.mtime_s) * std.time.ns_per_s;
    return SioDir.cwd().setTimestamps(_fsops.io(), op.path, .{
        .modify_timestamp = .{ .new = .{ .nanoseconds = ns } },
    });
}

// ───────────────────────── Instant (inline) ─────────────────────────

/// A fast `fstat` (cached inode).
fn statImpl(file: File) !Stat {
    const st = try sioFile(file.fd).stat(_fsops.io());
    return .{
        .size = st.size,
        .mtime_s = @intCast(@divFloor(st.mtime.nanoseconds, std.time.ns_per_s)),
    };
}

fn closeImpl(file: File) void {
    sioFile(file.fd).close(_fsops.io());
}

/// `std.Io.File.Stat` leaves `st_dev` out, so the system call is made
/// here: statx on Linux, fstat on macOS.
fn deviceImpl(file: File) !u64 {
    const fd = file.fd;
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

fn adviseRandomImpl(file: File) void {
    const fd = file.fd;
    switch (builtin.os.tag) {
        .linux => {
            const linux = std.os.linux;
            _ = linux.fadvise(fd, 0, 0, linux.POSIX_FADV.RANDOM);
        },
        // F_RDAHEAD 0 turns readahead off.
        .macos => _ = std.c.fcntl(fd, std.c.F.RDAHEAD, @as(c_int, 0)),
        else => {},
    }
}

fn noCacheImpl(file: File) void {
    if (builtin.os.tag == .macos)
        _ = std.c.fcntl(file.fd, std.c.F.NOCACHE, @as(c_int, 1));
}

/// The filesystem magic of `path` (statfs `f_type`), or null when it
/// cannot be read. Linux only: the magic numbers and the statfs layout
/// are Linux's; elsewhere the answer is null. One call at a WAL's open.
fn fsMagicImpl(path: String) ?i64 {
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
