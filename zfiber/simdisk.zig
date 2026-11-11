// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The simulation's disk: every `fs.submit` under a sim lands here
//! instead of the kernel (`Sim.disk`). A tree of directories and files
//! in memory, with the semantics the code above relies on: a create in
//! a missing directory fails, an unlinked file stays readable through
//! its open handles, a read at the end of a file is `EOF`, O_DIRECT
//! demands page alignment, and mtimes come from the virtual clock. The
//! disk starts with `/tmp`, as a box does; everything else is created
//! through the ops.
//!
//! Every op that reaches the device on the real disk parks here too, for
//! a virtual duration drawn from the disk's own PRNG (`wait`): an I/O
//! stays a yield point, so fibers interleave across it as they do on
//! io_uring, and a run of one seed replays the same durations.
//!
//! Durability is the device's, a page at a time. A file is 4096-byte
//! pages; a write to one page lands whole or not at all, and nothing
//! orders two pages until a sync. A written page is dirty until then,
//! and keeps the image the device still holds. `fsync` and `fdatasync`
//! make every dirty page and the size durable, an O_DSYNC handle makes
//! each write's pages durable as it lands. A directory's names are
//! visible at once and durable after a sync of the directory, and only
//! that, which is all POSIX promises: a file's own sync commits its
//! bytes and size, never its name. ext4 often commits a new file's
//! entry with the file's first sync, and code that leans on that would
//! pass here and lose the file elsewhere, so the disk does not grant
//! it. `crash` is the power cut for one directory tree, a node's data
//! directory: every handle into it dies, every name not durable goes,
//! every file shows its synced size and, per dirty page, its old image
//! or with `lose_some` a coin's pick. The rest of the disk, another
//! test's files in the same run, is untouched.
//!
//! Faults are the device's errors, each at a rate per thousand ops and
//! drawn from the same PRNG: a read or write that fails, a write that
//! lands in part, a sync or a resize that fails. Each is `InputOutput`,
//! the kernel's EIO, so the code's error paths run under schedules
//! production rarely gives them. `setFaults` puts them on a directory
//! tree, a test's own, since the suite's tests share the disk;
//! `--sim-faults` puts them on the root. The unit suite expects a disk
//! that works.

const std = @import("std");
const _coarse = @import("utils").coarse_clock;
const _dl = @import("deadlines.zig");
const _fs = @import("fs.zig");
const _rt = @import("runtime.zig");

const String = @import("utils").String;
const Allocator = std.mem.Allocator;
const Op = _fs.Op;
const File = _fs.File;
const Kind = _fs.Kind;

/// The device's page: a write to one lands whole or not at all, a short
/// write stops at one, and O_DIRECT wants its buffer, length and offset
/// in them.
pub const page_size: usize = 4096;

/// What `fs_magic` answers: ext4, the filesystem of the dev boxes, so
/// the WAL takes the same layout under a sim as on them.
const ext4_magic: i64 = 0xef53;

/// One op in a thousand stalls this much longer: the queue depth spike
/// or firmware pause a constant latency never models.
const stall_ms: u64 = 50;

/// What a crash does to a file's dirty pages, those written since its
/// last sync.
pub const Crash = enum {
    /// None of them reached the device.
    lose_all,
    /// Each one did or did not, a coin per page: the device had
    /// reordered them in its queue, so a write across pages is torn.
    lose_some,
};

/// Fault rates per thousand ops. Zero is a disk that works.
pub const Faults = struct {
    /// A read fails with `InputOutput`.
    read: u16 = 0,
    /// A write fails the same way, nothing written.
    write: u16 = 0,
    /// A write lands in part: fewer bytes than asked, whole pages on an
    /// O_DIRECT handle. The caller's loop issues the rest.
    short_write: u16 = 0,
    /// A sync fails with `InputOutput`; the pending writes stay pending,
    /// so a retry can land them.
    sync: u16 = 0,
    /// A resize fails with `InputOutput`.
    set_size: u16 = 0,

    /// Every kind at one rate.
    pub fn all(per_mille: u16) Faults {
        return .{
            .read = per_mille,
            .write = per_mille,
            .short_write = per_mille,
            .sync = per_mille,
            .set_size = per_mille,
        };
    }
};

pub const SimDisk = struct {
    alloc: Allocator,
    /// The durations, the coins of a crash. Its own, apart from the
    /// scheduler's draws, so a disk op added to a test does not move the
    /// schedule's picks.
    prng: std.Random.DefaultPrng,
    root: *Node,
    /// Open handles by `File.fd`; a closed slot is null and reused.
    opens: std.ArrayList(?Open) = .empty,
    /// Crashes so far; the current one's number marks its tree.
    crash_seq: u32 = 0,

    const Names = std.StringArrayHashMapUnmanaged(*Node);

    const Node = struct {
        kind: Kind,
        /// Regular file: the bytes as a reader sees them.
        data: std.ArrayList(u8) = .empty,
        /// Regular file: the size on the device, from the last sync.
        durable_len: usize = 0,
        /// Regular file: for each page written since the last sync, the
        /// image the device still holds. Only pages inside
        /// `durable_len` are kept: past it, a crash cuts the file.
        old_pages: Pages = .empty,
        /// Directory: name to child as a lookup sees it.
        children: Names = .empty,
        /// Directory: the names on the device, from its last sync.
        durable_children: Names = .empty,
        mtime_ms: i64,
        /// The directory this node was created in or last renamed into:
        /// `faults` are looked up along the chain. Null for the root and
        /// once that directory is gone.
        parent: ?*Node = null,
        /// Directory: the error rates for everything under it, until a
        /// directory below sets its own (`setFaults`).
        faults: ?Faults = null,
        /// The crash this node was last found under (`crash_seq`), so
        /// the handles into a tree are told apart from the others.
        crash_mark: u32 = 0,
        /// Holders: entries in either map of a directory and open
        /// handles. Freed at zero, so a node a crash drops from every
        /// map lives on through a handle until that is closed, and an
        /// unlinked file stays readable through its handle as on the
        /// real disk.
        refs: u32 = 0,
    };

    const Pages = std.AutoArrayHashMapUnmanaged(usize, *[page_size]u8);

    const Open = struct {
        node: *Node,
        writable: bool,
        direct: bool,
        /// O_DSYNC: every write is durable when it returns.
        dsync: bool,
        /// Opened before the last crash: the process that held it is
        /// gone, so every op but close fails as on a vanished device.
        dead: bool = false,
    };

    /// Where a path's last name lives: its directory and the name, ""
    /// for the root itself.
    const Place = struct { dir: *Node, name: String };

    pub fn init(alloc: Allocator, seed: u64) Allocator.Error!SimDisk {
        const root = try newNode(alloc, .directory);
        root.refs = 1;
        var disk: SimDisk = .{
            .alloc = alloc,
            .prng = std.Random.DefaultPrng.init(seed ^ 0xd15c),
            .root = root,
        };
        errdefer disk.deinit();
        try disk.makeDir(root, "tmp");
        return disk;
    }

    pub fn deinit(self: *SimDisk) void {
        for (self.opens.items) |slot| if (slot) |o| self.release(o.node);
        self.opens.deinit(self.alloc);
        self.release(self.root);
    }

    /// Run one op against the disk, after its wait. An error carries the
    /// name std gives the kernel's errno for it, so a caller's handling
    /// of `FileNotFound` or `EOF` is the same on both disks.
    pub fn apply(
        self: *SimDisk,
        comptime tag: Op.Tag,
        args: Op.Args(tag),
    ) _fs.Result(tag) {
        record(tag, args);
        self.wait(tag);
        switch (tag) {
            .read => return self.read(args),
            .write => return self.write(args),
            .read_batch => {
                for (args.ops) |*req| req.result = self.read(.{
                    .file = req.file,
                    .buf = req.buf,
                    .offset = req.offset,
                });
            },
            .sync => {
                const o = self.open(args.file);
                if (o.dead) return error.InputOutput;
                if (self.fault(faultsOf(o.node).sync))
                    return error.InputOutput;
                self.fold(o.node);
            },
            .set_size => return self.setSize(args),
            .stat => {
                const o = self.open(args);
                if (o.dead) return error.InputOutput;
                return .{
                    .size = o.node.data.items.len,
                    .mtime_s = @divFloor(o.node.mtime_ms, std.time.ms_per_s),
                };
            },
            .close => return self.close(args),
            .device => {
                if (self.open(args).dead) return error.StatFailed;
                return 1;
            },
            .advise_random, .no_cache => _ = self.open(args),
            .open => return self.openPath(args),
            // A file on the way is "not there" to access(2) as std maps
            // ENOTDIR for it.
            .access => _ = self.lookup(args) catch |err| switch (err) {
                error.NotDir => return error.FileNotFound,
                else => |e| return e,
            },
            .rename => return self.rename(args),
            .delete_file => return self.deleteFile(args),
            .create_dir_path => return self.createDirPath(args),
            .delete_tree => return self.deleteTree(args),
            .list => return self.list(args),
            .fs_magic => {
                _ = self.lookup(args) catch return null;
                return ext4_magic;
            },
            .set_mtime => {
                const node = try self.lookup(args.path);
                node.mtime_ms = args.mtime_s * std.time.ms_per_s;
            },
        }
    }

    /// The op into the run's trace, when there is a run.
    fn record(comptime tag: Op.Tag, args: Op.Args(tag)) void {
        const sim = _rt.runtime().simState() orelse return;
        sim.recordOp("fs." ++ @tagName(tag), args);
    }

    /// Park for what `tag` takes on the device, in whole virtual ms: a
    /// data op or metadata op 0 or 1, a sync 1 to 5, and the rare stall
    /// on top. 0 ms still yields, so the fiber gives up the worker as a
    /// real completion would make it. The ops the real disk answers
    /// inline (`stat`, `close`, the hints) do not park here either.
    fn wait(self: *SimDisk, comptime tag: Op.Tag) void {
        const rand = self.prng.random();
        var ms: u64 = switch (tag) {
            .sync => 1 + rand.uintLessThan(u64, 5),
            .read,
            .write,
            .read_batch,
            .set_size,
            .open,
            .access,
            .rename,
            .delete_file,
            .create_dir_path,
            .delete_tree,
            .list,
            .set_mtime,
            => rand.uintLessThan(u64, 2),
            .stat,
            .close,
            .device,
            .advise_random,
            .no_cache,
            .fs_magic,
            => return,
        };
        if (rand.uintLessThan(u32, 1000) == 0) ms += stall_ms;
        if (ms == 0) _rt.yield() else _dl.sleepMs(ms);
    }

    /// The error rates for the directory `path` and everything under it
    /// that has no rates of its own. `.{}` turns them off again.
    pub fn setFaults(self: *SimDisk, path: String, faults: Faults) !void {
        const dir = try self.lookup(path);
        if (dir.kind != .directory) return error.NotDir;
        dir.faults = faults;
    }

    /// The rates in force for `node`: the nearest directory up the chain
    /// that set any.
    fn faultsOf(node: *const Node) Faults {
        var at: ?*const Node = node;
        while (at) |n| : (at = n.parent) {
            if (n.faults) |f| return f;
        }
        return .{};
    }

    /// One draw at `per_mille`; none at all at zero, so a disk without
    /// faults draws the same durations as before faults existed.
    fn fault(self: *SimDisk, per_mille: u16) bool {
        if (per_mille == 0) return false;
        return self.prng.random().uintLessThan(u16, 1000) < per_mille;
    }

    // ───────────────────────── crash ─────────────────────────

    /// The power cut under the directory `path`. Every open handle into
    /// it is dead from here: the process that held it is gone, and a
    /// test's old objects fail on it rather than write into the
    /// restarted disk. Every directory under it shows its durable names
    /// again, which drops what was created or renamed in since its last
    /// sync and brings back what was deleted since. Every file shows
    /// its durable bytes, with `mode`'s share of its pending writes
    /// folded in first. Nothing at `path` is a no-op: the tree was
    /// never durable.
    pub fn crash(self: *SimDisk, path: String, mode: Crash) void {
        const top = self.lookup(path) catch return;
        self.crash_seq += 1;
        self.mark(top);
        for (self.opens.items) |*slot| if (slot.*) |*o| {
            if (o.node.crash_mark == self.crash_seq) o.dead = true;
        };
        self.restore(top, mode);
    }

    /// Every node a lookup or a crash could reach under `node`, both
    /// maps, carries this crash's number.
    fn mark(self: *SimDisk, node: *Node) void {
        node.crash_mark = self.crash_seq;
        if (node.kind != .directory) return;
        for (node.children.values()) |child| self.mark(child);
        for (node.durable_children.values()) |child| {
            if (child.crash_mark != self.crash_seq) self.mark(child);
        }
    }

    fn restore(self: *SimDisk, node: *Node, mode: Crash) void {
        if (node.kind != .directory) {
            self.crashFile(node, mode);
            return;
        }
        // The visible map goes first: a child only it named is released,
        // and with it everything under that child.
        self.clearNames(&node.children);
        var it = node.durable_children.iterator();
        while (it.next()) |kv| {
            self.putName(&node.children, kv.key_ptr.*, kv.value_ptr.*) catch
                @panic("simdisk: out of memory restoring a directory");
        }
        for (node.children.values()) |child| self.restore(child, mode);
    }

    fn crashFile(self: *SimDisk, node: *Node, mode: Crash) void {
        self.crashPages(node, &node.data, mode);
        self.dropOldPages(node);
    }

    /// `data`, holding `node`'s visible bytes, becomes what a crash
    /// leaves of the file: the synced size, and each dirty page's old
    /// image unless `mode` lets the new one stand.
    fn crashPages(
        self: *SimDisk,
        node: *const Node,
        data: *std.ArrayList(u8),
        mode: Crash,
    ) void {
        const rand = self.prng.random();
        // Grow to whole pages if short, so each saved image fits. The
        // copy below runs either way, and the last line sets the size.
        const full = pageEnd(node.durable_len);
        if (data.items.len < full) self.resize(data, full);

        var it = node.old_pages.iterator();
        while (it.next()) |kv| {
            if (mode == .lose_some and rand.boolean()) continue;
            const start = kv.key_ptr.* * page_size;
            @memcpy(data.items[start..][0..page_size], kv.value_ptr.*);
        }
        self.resize(data, node.durable_len);
    }

    /// What a power cut now would leave of the directory `src`, as the
    /// new directory `dst`. `src` and the handles into it are untouched:
    /// the process that owns them cannot be killed mid-step, so it runs
    /// on, shuts down in its own time, and whatever it does after this
    /// instant is not in the copy a restart opens.
    pub fn crashCopy(
        self: *SimDisk,
        src: String,
        dst: String,
        mode: Crash,
    ) !void {
        const from = try self.lookup(src);
        if (from.kind != .directory) return error.NotDir;

        const to = try self.placeOf(dst);
        if (to.name.len == 0 or to.dir.children.get(to.name) != null)
            return error.PathAlreadyExists;

        const copy = try self.crashCopyOf(from, mode);
        errdefer self.release(copy);
        // Held here until the two names hold it.
        copy.refs = 1;
        copy.parent = to.dir;
        try self.putName(&to.dir.children, to.name, copy);
        errdefer self.removeName(&to.dir.children, to.name);
        try self.putName(&to.dir.durable_children, to.name, copy);
        self.release(copy);
    }

    /// A copy of `node` as a crash would leave it, in no directory yet:
    /// durable names only, each file's bytes as `crashPages` leaves them.
    fn crashCopyOf(
        self: *SimDisk,
        node: *const Node,
        mode: Crash,
    ) Allocator.Error!*Node {
        const copy = try newNode(self.alloc, node.kind);
        copy.refs = 1;
        errdefer self.release(copy);
        copy.mtime_ms = node.mtime_ms;
        if (node.kind != .directory) {
            try copy.data.appendSlice(self.alloc, node.data.items);
            self.crashPages(node, &copy.data, mode);
            copy.durable_len = node.durable_len;
            copy.refs = 0;
            return copy;
        }
        var it = node.durable_children.iterator();
        while (it.next()) |kv| {
            const child = try self.crashCopyOf(kv.value_ptr.*, mode);
            child.refs = 1;
            errdefer self.release(child);
            child.parent = copy;
            try self.putName(&copy.children, kv.key_ptr.*, child);
            try self.putName(&copy.durable_children, kv.key_ptr.*, child);
            self.release(child);
        }
        copy.refs = 0;
        return copy;
    }

    // ───────────────────────── durability ─────────────────────────

    /// A sync: a directory's names reach the device, or a file's
    /// pending writes and size do. A file's sync leaves its name to the
    /// directory's.
    fn fold(self: *SimDisk, node: *Node) void {
        if (node.kind == .directory) {
            self.foldNames(node);
            return;
        }
        node.durable_len = node.data.items.len;
        self.dropOldPages(node);
    }

    fn foldNames(self: *SimDisk, dir: *Node) void {
        self.clearNames(&dir.durable_children);
        var it = dir.children.iterator();
        while (it.next()) |kv| {
            self.putName(
                &dir.durable_children,
                kv.key_ptr.*,
                kv.value_ptr.*,
            ) catch @panic("simdisk: out of memory syncing a directory");
        }
    }

    /// The pages holding `data[off..end]` are about to change: each one
    /// inside the durable size keeps its image from before, once. A
    /// page past that size has no image on the device to keep.
    fn keepOldPages(
        self: *SimDisk,
        node: *Node,
        off: usize,
        end: usize,
    ) Allocator.Error!void {
        var idx = off / page_size;
        while (idx * page_size < @min(end, node.durable_len)) : (idx += 1) {
            if (node.old_pages.contains(idx)) continue;
            const img = try self.alloc.create([page_size]u8);
            errdefer self.alloc.destroy(img);
            @memset(img, 0);
            const start = idx * page_size;
            const have = @min(start + page_size, node.data.items.len);
            if (have > start)
                @memcpy(img[0 .. have - start], node.data.items[start..have]);
            try node.old_pages.putNoClobber(self.alloc, idx, img);
        }
    }

    /// The pages holding `data[off..end]` reach the device, and the size
    /// grows to `end` as the kernel's size update with a write does.
    fn foldPages(self: *SimDisk, node: *Node, off: usize, end: usize) void {
        var idx = off / page_size;
        while (idx * page_size < end) : (idx += 1) {
            if (node.old_pages.fetchSwapRemove(idx)) |kv|
                self.alloc.destroy(kv.value);
        }
        node.durable_len = @max(node.durable_len, end);
    }

    fn dropOldPages(self: *SimDisk, node: *Node) void {
        for (node.old_pages.values()) |img| self.alloc.destroy(img);
        node.old_pages.clearRetainingCapacity();
    }

    fn pageEnd(len: usize) usize {
        return (len + page_size - 1) / page_size * page_size;
    }

    // ───────────────────────── handles ─────────────────────────

    fn open(self: *SimDisk, file: File) Open {
        const i: usize = @intCast(file.fd);
        return self.opens.items[i] orelse
            std.debug.panic("simdisk: fd {d} is not open", .{file.fd});
    }

    fn openPath(self: *SimDisk, op: Op.Open) !File {
        const want_dir = op.flags.DIRECTORY;
        const writable = op.flags.ACCMODE != .RDONLY;
        const place = try self.placeOf(op.path);
        const existing: ?*Node = if (place.name.len == 0)
            place.dir
        else
            place.dir.children.get(place.name);

        var node: *Node = undefined;
        if (existing) |n| {
            if (op.flags.CREAT and op.flags.EXCL)
                return error.PathAlreadyExists;
            if (n.kind == .directory) {
                if (writable) return error.IsDir;
            } else if (want_dir) return error.NotDir;
            if (op.flags.TRUNC and writable) self.truncate(n, 0);
            node = n;
        } else {
            if (!op.flags.CREAT or want_dir) return error.FileNotFound;
            node = try newNode(self.alloc, .file);
            node.parent = place.dir;
            // The name is visible now and durable after a sync; a node in
            // no map yet is held by the put below.
            self.putName(&place.dir.children, place.name, node) catch |err| {
                destroy(self.alloc, node);
                return err;
            };
            place.dir.mtime_ms = nowMs();
        }

        const direct = if (@hasField(std.posix.O, "DIRECT"))
            op.flags.DIRECT
        else
            false;
        const slot = try self.freeSlot();
        self.opens.items[slot] = .{
            .node = node,
            .writable = writable,
            .direct = direct,
            .dsync = op.flags.DSYNC,
        };
        node.refs += 1;
        return .{ .fd = @intCast(slot) };
    }

    fn freeSlot(self: *SimDisk) Allocator.Error!usize {
        for (self.opens.items, 0..) |slot, i| if (slot == null) return i;
        try self.opens.append(self.alloc, null);
        return self.opens.items.len - 1;
    }

    fn close(self: *SimDisk, file: File) void {
        const o = self.open(file);
        self.opens.items[@intCast(file.fd)] = null;
        self.release(o.node);
    }

    // ───────────────────────── data ─────────────────────────

    fn read(self: *SimDisk, op: Op.Read) !usize {
        const o = self.open(op.file);
        if (o.dead) return error.InputOutput;
        if (self.fault(faultsOf(o.node).read)) return error.InputOutput;

        if (o.direct) assertDirect(op.buf.ptr, op.buf.len, op.offset);
        if (o.node.kind != .file) return error.IsDir;

        const data = o.node.data.items;
        if (op.buf.len == 0) return 0;
        // What the kernel's zero-byte pread becomes in libxev.
        if (op.offset >= data.len) return error.EOF;

        const off: usize = @intCast(op.offset);
        const n = @min(op.buf.len, data.len - off);
        @memcpy(op.buf[0..n], data[off .. off + n]);
        return n;
    }

    fn write(self: *SimDisk, op: Op.Write) !usize {
        const o = self.open(op.file);
        if (o.dead) return error.InputOutput;

        if (o.direct) assertDirect(op.data.ptr, op.data.len, op.offset);
        // A directory is never open for writing: `openPath` refuses.
        if (!o.writable) return error.NotOpenForWriting;

        const faults = faultsOf(o.node);
        if (self.fault(faults.write)) return error.InputOutput;

        const len = self.writeLen(op.data.len, o.direct, faults.short_write);
        const off: usize = @intCast(op.offset);
        const end = off + len;
        try self.keepOldPages(o.node, off, end);

        if (end > o.node.data.items.len) self.resize(&o.node.data, end);
        @memcpy(o.node.data.items[off..end], op.data[0..len]);
        o.node.mtime_ms = nowMs();
        if (o.dsync) self.foldPages(o.node, off, end);
        return len;
    }

    /// How much of a `len`-byte write lands: all of it, or under a
    /// short-write fault a part, at least a byte and short of the whole,
    /// in whole pages on an O_DIRECT handle.
    fn writeLen(
        self: *SimDisk,
        len: usize,
        direct: bool,
        per_mille: u16,
    ) usize {
        const unit: usize = if (direct) page_size else 1;
        if (len < 2 * unit or !self.fault(per_mille)) return len;

        const units = len / unit;
        const rand = self.prng.random();
        return unit * (1 + rand.uintLessThan(usize, units - 1));
    }

    fn setSize(self: *SimDisk, op: Op.SetSize) !void {
        const o = self.open(op.file);
        if (o.dead) return error.InputOutput;
        if (o.node.kind != .file) return error.NonResizable;
        if (self.fault(faultsOf(o.node).set_size)) return error.InputOutput;
        self.truncate(o.node, @intCast(op.length));
    }

    /// The visible size becomes `len`, zero-filled when it grows; the
    /// durable size follows at the sync. A shrink keeps the images of
    /// the pages it cuts, since a crash brings them back.
    fn truncate(self: *SimDisk, node: *Node, len: usize) void {
        const old_len = node.data.items.len;
        if (len < old_len) self.keepOldPages(node, len, old_len) catch
            @panic("simdisk: out of memory on a truncate");
        self.resize(&node.data, len);
        node.mtime_ms = nowMs();
    }

    /// `data` is `len` bytes, zero-filled where it grew.
    fn resize(self: *SimDisk, data: *std.ArrayList(u8), len: usize) void {
        const old_len = data.items.len;
        data.resize(self.alloc, len) catch
            @panic("simdisk: out of memory resizing a file");
        if (len > old_len) @memset(data.items[old_len..len], 0);
    }

    fn assertDirect(ptr: [*]const u8, len: usize, offset: u64) void {
        std.debug.assert(@intFromPtr(ptr) % page_size == 0);
        std.debug.assert(len % page_size == 0);
        std.debug.assert(offset % page_size == 0);
    }

    // ───────────────────────── names ─────────────────────────

    fn rename(self: *SimDisk, op: Op.Rename) !void {
        const from = try self.placeOf(op.old_path);
        const src = from.dir.children.get(from.name) orelse
            return error.FileNotFound;

        const to = try self.placeOf(op.new_path);
        if (to.name.len == 0) return error.IsDir;
        if (to.dir.children.get(to.name)) |dst| {
            if (dst == src) return;
            if (dst.kind == .directory) {
                if (src.kind != .directory) return error.IsDir;
                if (dst.children.count() > 0) return error.DirNotEmpty;
            } else if (src.kind == .directory) return error.NotDir;
            self.removeName(&to.dir.children, to.name);
        }
        try self.putName(&to.dir.children, to.name, src);
        self.removeName(&from.dir.children, from.name);
        src.parent = to.dir;
        from.dir.mtime_ms = nowMs();
        to.dir.mtime_ms = nowMs();
    }

    fn deleteFile(self: *SimDisk, path: String) !void {
        const place = try self.placeOf(path);
        const node = place.dir.children.get(place.name) orelse
            return error.FileNotFound;
        if (node.kind == .directory) return error.IsDir;
        self.removeName(&place.dir.children, place.name);
        place.dir.mtime_ms = nowMs();
    }

    /// Every directory made here is durable at once, name included: a
    /// data directory exists long before any crash the sim models, and
    /// nothing above syncs a parent after a mkdir.
    fn createDirPath(self: *SimDisk, path: String) !void {
        var dir = self.root;
        var it = std.mem.tokenizeScalar(u8, path, '/');
        while (it.next()) |name| {
            if (std.mem.eql(u8, name, ".")) continue;
            if (dir.children.get(name)) |child| {
                if (child.kind != .directory) return error.NotDir;
                dir = child;
                continue;
            }
            try self.makeDir(dir, name);
            dir = dir.children.get(name).?;
        }
    }

    fn makeDir(self: *SimDisk, dir: *Node, name: String) Allocator.Error!void {
        const child = try newNode(self.alloc, .directory);
        errdefer destroy(self.alloc, child);
        child.parent = dir;
        try self.putName(&dir.children, name, child);
        // The device may still hold this name, from a tree deleted with
        // no sync since: the new directory takes its place there too.
        if (dir.durable_children.contains(name))
            self.removeName(&dir.durable_children, name);
        try self.putName(&dir.durable_children, name, child);
        dir.mtime_ms = nowMs();
    }

    /// Nothing at `path` is not an error, as `std.Io.Dir.deleteTree`.
    fn deleteTree(self: *SimDisk, path: String) !void {
        const place = self.placeOf(path) catch |err| switch (err) {
            error.FileNotFound => return,
            else => |e| return e,
        };
        if (place.name.len == 0) {
            self.clearNames(&self.root.children);
            return;
        }
        if (place.dir.children.get(place.name) == null) return;
        self.removeName(&place.dir.children, place.name);
        place.dir.mtime_ms = nowMs();
    }

    fn list(self: *SimDisk, op: Op.List) ![]_fs.Entry {
        const dir = try self.lookup(op.path);
        if (dir.kind != .directory) return error.NotDir;

        // Sorted: the order a real readdir gives is the filesystem's,
        // so nothing above may depend on it, and a fixed one replays.
        const names = try op.alloc.dupe(String, dir.children.keys());
        defer op.alloc.free(names);
        std.mem.sort(String, names, {}, lessThan);

        var out: std.ArrayList(_fs.Entry) = .empty;
        errdefer {
            for (out.items) |e| op.alloc.free(e.name);
            out.deinit(op.alloc);
        }
        for (names) |name| {
            const owned = try op.alloc.dupe(u8, name);
            errdefer op.alloc.free(owned);
            try out.append(op.alloc, .{
                .name = owned,
                .kind = dir.children.get(name).?.kind,
            });
        }
        return out.toOwnedSlice(op.alloc);
    }

    fn lessThan(_: void, a: String, b: String) bool {
        return std.mem.lessThan(u8, a, b);
    }

    // ───────────────────────── tree ─────────────────────────

    /// The node at `path`, every component resolved.
    fn lookup(self: *SimDisk, path: String) error{
        FileNotFound,
        NotDir,
    }!*Node {
        const place = try self.placeOf(path);
        if (place.name.len == 0) return place.dir;
        return place.dir.children.get(place.name) orelse error.FileNotFound;
    }

    /// The directory holding `path`'s last name, every directory on
    /// the way resolved: a missing one is `FileNotFound`, a file in
    /// the way `NotDir`. Relative paths start at the root; `..` climbs.
    fn placeOf(self: *SimDisk, path: String) error{
        FileNotFound,
        NotDir,
    }!Place {
        var dir = self.root;
        var parents: [64]*Node = undefined;
        var depth: usize = 0;
        var last: String = "";
        var it = std.mem.tokenizeScalar(u8, path, '/');
        while (it.next()) |name| {
            // The previous name was a directory on the way, not the last.
            if (last.len > 0) {
                const child = dir.children.get(last) orelse
                    return error.FileNotFound;
                if (child.kind != .directory) return error.NotDir;
                if (depth == parents.len) return error.NotDir;
                parents[depth] = dir;
                depth += 1;
                dir = child;
            }
            if (std.mem.eql(u8, name, ".")) {
                last = "";
            } else if (std.mem.eql(u8, name, "..")) {
                if (depth > 0) {
                    depth -= 1;
                    dir = parents[depth];
                }
                last = "";
            } else {
                last = name;
            }
        }
        return .{ .dir = dir, .name = last };
    }

    fn newNode(alloc: Allocator, kind: Kind) Allocator.Error!*Node {
        const node = try alloc.create(Node);
        node.* = .{ .kind = kind, .mtime_ms = nowMs() };
        return node;
    }

    /// `name` in `map` is `node`, which the entry now holds; the name
    /// is copied.
    fn putName(
        self: *SimDisk,
        map: *Names,
        name: String,
        node: *Node,
    ) Allocator.Error!void {
        const owned = try self.alloc.dupe(u8, name);
        errdefer self.alloc.free(owned);
        try map.putNoClobber(self.alloc, owned, node);
        node.refs += 1;
    }

    /// `name` leaves `map`, which lets its node go.
    fn removeName(self: *SimDisk, map: *Names, name: String) void {
        const kv = map.fetchOrderedRemove(name).?;
        self.alloc.free(kv.key);
        self.release(kv.value);
    }

    fn clearNames(self: *SimDisk, map: *Names) void {
        while (map.count() > 0) self.removeName(map, map.keys()[0]);
    }

    /// One holder fewer; the last one frees the node, and a directory's
    /// maps let go of their children in turn.
    fn release(self: *SimDisk, node: *Node) void {
        node.refs -= 1;
        if (node.refs > 0) return;
        // A file living on through a handle must not look for its
        // faults in a freed directory.
        for (node.children.values()) |child| child.parent = null;
        for (node.durable_children.values()) |child| child.parent = null;
        self.clearNames(&node.children);
        self.clearNames(&node.durable_children);
        self.dropOldPages(node);
        destroy(self.alloc, node);
    }

    fn destroy(alloc: Allocator, node: *Node) void {
        std.debug.assert(node.children.count() == 0);
        std.debug.assert(node.durable_children.count() == 0);
        node.children.deinit(alloc);
        node.durable_children.deinit(alloc);
        std.debug.assert(node.old_pages.count() == 0);
        node.old_pages.deinit(alloc);
        node.data.deinit(alloc);
        alloc.destroy(node);
    }

    fn nowMs() i64 {
        return _coarse.timestampNow(.realtime, .millis);
    }
};
