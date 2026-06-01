//! File I/O for fibers, split by how each op is serviced:
//!
//!   - **Data plane** (`read`/`write`/`preadBatch`): positional `pread`/
//!     `pwrite` submitted to the current worker's libxev loop; the fiber
//!     parks and the completion callback wakes it. Fully async.
//!   - **Blocking ops** (`sync`, `setSize`, and the `Dir` path ops open/
//!     create/access/rename/delete): libxev can't do these async, so they
//!     run on the `fsops` thread pool — the fiber parks, a pool thread runs
//!     the syscall and wakes it. The worker thread stays free, and the big
//!     path-buffer frames run on the pool thread's full-size OS stack.
//!   - **Instant ops** (`close`, `stat`): plain inline syscalls (no disk
//!     I/O), so a pool round-trip would be pure overhead.
//!
//! The blocking/instant syscalls go through `std.Io.File`/`std.Io.Dir` on the
//! pool's blocking `std.Io` (`fsops.io()`) rather than raw `std.os.linux` —
//! cross-platform, and no errno/path-buffer boilerplate. Only the libxev data
//! plane is fiber-native.

const std = @import("std");
const xev = @import("xev");
const fb = @import("fiber.zig");
const rt = @import("runtime.zig");
const fsops = @import("fsops.zig");

const Fiber = fb.Fiber;
const SioFile = std.Io.File;
const SioDir = std.Io.Dir;

/// Wrap a raw fd as a `std.Io.File` (blocking) for a `std.Io` syscall.
fn sioFile(fd: SioFile.Handle) SioFile {
    return .{ .handle = fd, .flags = .{ .nonblocking = false } };
}

// ───────────────────────── File ─────────────────────────

pub const Stat = struct {
    size: u64,
};

/// Flags accepted by `File.sync` for zio API parity. Ignored — `std.Io`'s
/// `sync` is a full flush.
pub const SyncFlags = struct {};

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
        const w = rt.currentWorker();
        const f = w.current_fiber.?;
        const xf = xev.File.initFd(self.fd);
        var c: xev.Completion = undefined;
        var waiter: ReadWaiter = .{ .fiber = f, .result = 0 };
        xf.pread(&w.loop, &c, .{ .slice = buf }, offset, ReadWaiter, &waiter, preadCallback);
        rt.park();
        return waiter.result;
    }

    /// Positional write. Parks until the op completes. Returns the number
    /// of bytes written (may be short).
    pub fn write(self: File, data: []const u8, offset: u64) xev.WriteError!usize {
        const w = rt.currentWorker();
        const f = w.current_fiber.?;
        const xf = xev.File.initFd(self.fd);
        var c: xev.Completion = undefined;
        var waiter: WriteWaiter = .{ .fiber = f, .result = 0 };
        xf.pwrite(&w.loop, &c, .{ .slice = data }, offset, WriteWaiter, &waiter, pwriteCallback);
        rt.park();
        return waiter.result;
    }

    /// A single positional read request for `preadBatch`. `result` is
    /// filled by the completion callback.
    pub const BatchOp = struct {
        buf: []u8,
        offset: u64,
        result: xev.ReadError!usize = 0,
    };

    /// Submit every request in `ops` to the worker loop at once and park
    /// a single time until all complete — one loop poll, one wake. The
    /// kernel sees all SQEs in one submission (the batching that
    /// `vfs.batchRead` relies on) without needing scatter-gather: each op
    /// is an independent single-buffer pread at its own offset.
    ///
    /// `allocator` backs the transient completion + context arrays; they
    /// only need to outlive this call (the fiber stays parked until the
    /// last completion fires, so they survive across the park).
    pub fn preadBatch(self: File, allocator: std.mem.Allocator, ops: []BatchOp) !void {
        if (ops.len == 0) return;
        const w = rt.currentWorker();
        const f = w.current_fiber.?;
        const xf = xev.File.initFd(self.fd);

        const comps = try allocator.alloc(xev.Completion, ops.len);
        defer allocator.free(comps);
        const ctxs = try allocator.alloc(BatchCtx, ops.len);
        defer allocator.free(ctxs);

        // Single shared waiter. Callbacks run on this worker's loop (one
        // thread), so the `remaining` countdown needs no atomics.
        var batch: BatchWaiter = .{ .fiber = f, .remaining = ops.len };
        for (ops, comps, ctxs) |*op, *c, *ctx| {
            ctx.* = .{ .batch = &batch, .op = op };
            xf.pread(&w.loop, c, .{ .slice = op.buf }, op.offset, BatchCtx, ctx, batchCallback);
        }
        rt.park();
    }

    /// Flush data + metadata to disk. libxev has no fsync op, so the blocking
    /// `fsync` runs on the `fsops` pool while this fiber parks — the worker
    /// thread stays free for other fibers.
    pub fn sync(self: File, flags: SyncFlags) !void {
        _ = flags;
        return fsops.run(syncImpl, .{self.fd});
    }

    /// Truncate / extend the file to `length` bytes. Offloaded (can block on
    /// disk).
    pub fn setSize(self: File, length: u64) !void {
        return fsops.run(setLengthImpl, .{ self.fd, length });
    }

    /// Stat the open file (currently just its size). A fast `fstat` (cached
    /// inode), so it runs inline.
    pub fn stat(self: File) !Stat {
        const st = try sioFile(self.fd).stat(fsops.io());
        return .{ .size = st.size };
    }

    pub fn size(self: File) !u64 {
        return (try self.stat()).size;
    }

    /// Close releases the fd (instant), so it runs inline.
    pub fn close(self: File) void {
        sioFile(self.fd).close(fsops.io());
    }

    /// Fsync then close in a *single* pool round-trip: the fiber parks once
    /// and wakes only after both have run (sequentially, on one pool thread).
    /// Cheaper than `try f.sync(.{}); f.close()` — which would offload twice —
    /// for the common "finished writing a file, make it durable, release it"
    /// path (e.g. an SST after a flush). `close` runs even if `sync` fails.
    ///
    /// This is the general "batch blocking ops" pattern: any sequence can be
    /// run with one park/wake by putting it in a single function handed to
    /// `fsops.run`.
    pub fn syncAndClose(self: File) !void {
        return fsops.run(syncCloseImpl, .{self.fd});
    }
};

fn syncImpl(fd: SioFile.Handle) !void {
    return sioFile(fd).sync(fsops.io());
}

fn syncCloseImpl(fd: SioFile.Handle) !void {
    const f = sioFile(fd);
    // Both ops run inline here — we're already on the pool thread.
    defer f.close(fsops.io());
    try f.sync(fsops.io());
}

fn setLengthImpl(fd: SioFile.Handle, length: u64) !void {
    return sioFile(fd).setLength(fsops.io(), length);
}

const ReadWaiter = struct {
    fiber: *Fiber,
    result: xev.ReadError!usize,
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
    rt.wake(w.fiber);
    return .disarm;
}

const WriteWaiter = struct {
    fiber: *Fiber,
    result: xev.WriteError!usize,
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
    rt.wake(w.fiber);
    return .disarm;
}

const BatchWaiter = struct {
    fiber: *Fiber,
    remaining: usize,
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
    ctx.batch.remaining -= 1;
    if (ctx.batch.remaining == 0) rt.wake(ctx.batch.fiber);
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
        return fsops.run(accessImpl, .{path});
    }

    pub fn rename(self: Dir, old_path: []const u8, new_dir: Dir, new_path: []const u8) !void {
        _ = self;
        _ = new_dir;
        return fsops.run(renameImpl, .{ old_path, new_path });
    }

    pub fn deleteFile(self: Dir, path: []const u8) !void {
        _ = self;
        return fsops.run(deleteImpl, .{path});
    }

    /// Create (truncating) a file with read+write access, returning a fiber `File`.
    pub fn createFile(self: Dir, path: []const u8, flags: CreateFlags) !File {
        _ = self;
        _ = flags;
        return fsops.run(createImpl, .{path});
    }

    /// Open an existing file read-only, returning a fiber `File`.
    pub fn openFile(self: Dir, path: []const u8, flags: OpenFlags) !File {
        _ = self;
        _ = flags;
        return fsops.run(openImpl, .{path});
    }
};

fn accessImpl(path: []const u8) !void {
    return SioDir.cwd().access(fsops.io(), path, .{});
}

fn renameImpl(old_path: []const u8, new_path: []const u8) !void {
    return SioDir.cwd().rename(old_path, SioDir.cwd(), new_path, fsops.io());
}

fn deleteImpl(path: []const u8) !void {
    return SioDir.cwd().deleteFile(fsops.io(), path);
}

fn createImpl(path: []const u8) !File {
    const f = try SioDir.cwd().createFile(fsops.io(), path, .{ .read = true, .truncate = true });
    return File.fromFd(f.handle);
}

fn openImpl(path: []const u8) !File {
    const f = try SioDir.cwd().openFile(fsops.io(), path, .{ .mode = .read_only });
    return File.fromFd(f.handle);
}
