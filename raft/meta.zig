// MetaStore — durable HardState + Snapshot for the Raft log.
//
//   state.bin     — 8 KiB, two-slot double-buffered HardState. Opened with
//                   O_DSYNC so each pwrite is durable on return — no
//                   explicit fdatasync syscall per write.
//   snapshot.bin  — variable size, atomically replaced via `snapshot.tmp` +
//                   rename + dir-fsync. The tmp file is also O_DSYNC, so a
//                   single write is durable on return (POSIX guarantees the
//                   file-size update is part of data-integrity sync, since
//                   without it the new bytes wouldn't be retrievable). The
//                   payload is wrapped in a zbuf frame so we detect torn
//                   bytes on read.
//
// The double-buffer protects HardState against torn writes: each slot is a
// zbuf frame around a serialized HardState, and HardState carries its own
// monotonic `seq` field. On recovery we pick the slot whose decoded
// HardState has the highest seq. A torn write to the just-written slot
// fails the frame's hash check and recovery silently returns the previous
// (older) durable state.
//
// HardState changes (votes, term bumps) are durable when `setHardState`
// returns. Snapshots are durable when `setSnapshot` returns. There is no
// separate `MetaStore.sync()`
//
// All file I/O goes through `vfs`. Directory creation and existence checks
// use `std.fs.cwd()` because they're path-metadata operations, not data I/O.

const std = @import("std");
const vfs = @import("vfs");
const zen = @import("buffers_gen");
const zbuf = @import("zbuf").zbuf;

const slot_size: usize = 4096;
const slot_count: usize = 2;
const state_file_size: usize = slot_size * slot_count;

const state_file_name = "state.bin";
const snapshot_file_name = "snapshot.bin";
const snapshot_tmp_name = "snapshot.tmp";

pub const Error = error{
    MetaCorrupt,
    SnapshotCorrupt,
    PayloadTooLarge,
    InvalidEncoding,
};

pub const MetaStore = struct {
    allocator: std.mem.Allocator,
    /// Owned dup of the directory path. All file paths are derived from this.
    dir_path: []u8,
    /// Open with O_DSYNC so per-write fdatasync is implicit.
    state_file: vfs.FileHandle,

    /// Authoritative in-memory state. Updated only after the corresponding
    /// disk write has been durable.
    /// HardState is stored by value because all its fields are scalars; copy
    /// is free. Snapshot is heap-owned because it carries `data: []const u8`
    /// (and a `conf_state` with its own peers slice) — keeping a single
    /// `*Snapshot` lets the swap path use `clone`/`destroy`, which is what
    /// the legacy metaFile did and avoids any shallow-copy ownership puzzles.
    hard_state: zen.HardStateStruct,
    snapshot: *zen.SnapshotStruct,

    /// Index of the currently-durable slot (0 = slot A at offset 0,
    /// 1 = slot B at offset 4096). The next write goes to `1 - active_slot`.
    /// The recovery sequence number lives in `hard_state.seq`.
    active_slot: u8,

    pub fn openPath(path: []const u8, alloc: std.mem.Allocator) !MetaStore {
        // Directory creation is path metadata, not data I/O. std.fs is fine.
        const cwd = std.fs.cwd();
        cwd.access(path, .{}) catch |err| switch (err) {
            error.FileNotFound => try cwd.makePath(path),
            else => return err,
        };

        const dir_path = try alloc.dupe(u8, path);
        errdefer alloc.free(dir_path);

        const state_path = try std.fs.path.join(alloc, &.{ path, state_file_name });
        defer alloc.free(state_path);

        // Open the existing state.bin if present; otherwise create + initialise.
        // Both branches set DSYNC so subsequent writes are durable on return —
        // we never call state_file.sync() in the hot path.
        const state_opts: vfs.FileOptions = .{ .flags = .{ .writable = true, .dsync = true } };
        var fresh = false;
        var state_file = vfs.g().openFile(alloc, state_path, state_opts) catch |err| switch (err) {
            error.FileNotFound => blk: {
                fresh = true;
                break :blk try vfs.g().createFile(state_path, .{
                    .flags = .{ .writable = true, .truncate = true, .dsync = true },
                });
            },
            else => return err,
        };
        errdefer state_file.close();

        if (fresh) {
            try state_file.truncate(state_file_size);
        }

        // Allocate a default-empty Snapshot pointer up front so subsequent
        // `recoverSnapshot` / `setSnapshot` calls have a uniform "destroy
        // old, install new" path. The empty struct has `.data = &.{}` so
        // `destroy` is a no-op for the default.
        const empty_snap = try alloc.create(zen.SnapshotStruct);
        errdefer alloc.destroy(empty_snap);
        empty_snap.* = .{};

        var ms: MetaStore = .{
            .allocator = alloc,
            .dir_path = dir_path,
            .state_file = state_file,
            .hard_state = .{},
            .snapshot = empty_snap,
            .active_slot = 0,
        };

        if (fresh) {
            // Seed slot A with seq=0 and slot B with seq=1; both default-zero
            // HardState. After this the highest-seq winner is slot B.
            try ms.writeSlot(.{ .seq = 0 }, 0);
            try ms.writeSlot(.{ .seq = 1 }, 1);
            ms.hard_state = .{ .seq = 1 };
            ms.active_slot = 1;
        } else {
            try ms.recoverHardState();
        }

        // Clean up a stale snapshot.tmp left by a previous crashed setSnapshot.
        // The old snapshot.bin (if any) is unaffected because rename hadn't
        // run yet — the half-written tmp is just discarded.
        const tmp_path = try std.fs.path.join(alloc, &.{ dir_path, snapshot_tmp_name });
        defer alloc.free(tmp_path);
        vfs.VFS.deleteFile(tmp_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };

        try ms.recoverSnapshot();

        return ms;
    }

    pub fn deinit(self: *MetaStore) void {
        // destroy walks deinit (frees data + conf_state peers) then the
        // outer allocator.destroy. Safe even for the default-empty pointer.
        self.snapshot.destroy(self.allocator);
        self.state_file.close();
        self.allocator.free(self.dir_path);
    }

    /// Returns a pointer to the in-memory authoritative HardState. Valid
    /// until the next setHardState call. Cheap; no allocation.
    pub fn hardState(self: *const MetaStore) *const zen.HardStateStruct {
        return &self.hard_state;
    }

    /// Returns a pointer to the in-memory authoritative Snapshot. Valid
    /// until the next setSnapshot call. The pointed-to Snapshot's `data`
    /// slice is owned by MetaStore and must not be freed by the caller.
    pub fn snapshotState(self: *const MetaStore) *const zen.SnapshotStruct {
        return self.snapshot;
    }

    /// Persist a new HardState. Returns only after the new value is durable
    /// (one O_DSYNC pwrite). Raft only calls us on real changes, so we
    /// don't gate on a no-op-skip comparison here.
    ///
    /// `seq` is internal bookkeeping: any caller-supplied value is discarded
    /// and the durable record carries `self.hard_state.seq + 1`.
    pub fn setHardState(self: *MetaStore, hs: zen.HardStateStruct) !void {
        var next = hs;
        next.seq = self.hard_state.seq + 1;

        const inactive_slot: u8 = 1 - self.active_slot;
        try self.writeSlot(next, inactive_slot);
        self.hard_state = next;
        self.active_slot = inactive_slot;
    }

    /// Persist a new Snapshot. Variable-size payload, so we use the
    /// `tmp + rename + dir-fsync` atomic-replace pattern. The tmp file is
    /// O_DSYNC-opened so the bytes (and the file-size update) are durable
    /// on `write()` return — no explicit tmp.sync() syscall. dir-fsync is
    /// still required to make the rename itself crash-safe.
    pub fn setSnapshot(self: *MetaStore, snap: zen.SnapshotStruct) !void {
        // Serialize the snapshot via the struct's own marshalTo, then append
        // the zbuf tail. Passing `off` lets zbuf validate the root-at-zero
        // invariant even though the tail only stores the table id.
        var zbuf_bytes: std.ArrayList(u8) = .empty;
        defer zbuf_bytes.deinit(self.allocator);
        const off = try snap.marshalTo(self.allocator, &zbuf_bytes);
        try zbuf.appendMessageTail(&zbuf_bytes, self.allocator, zen.SnapshotReader.TABLE_ID, off);

        // Wrap with a zbuf frame so we detect a torn pwrite (e.g. a power
        // loss mid-write of a multi-MB snapshot) on the next recovery.
        var file_buf: std.ArrayList(u8) = .empty;
        defer file_buf.deinit(self.allocator);
        _ = try zbuf.Frame.append(&file_buf, self.allocator, zbuf_bytes.items);

        const tmp_path = try std.fs.path.join(self.allocator, &.{ self.dir_path, snapshot_tmp_name });
        defer self.allocator.free(tmp_path);
        const final_path = try std.fs.path.join(self.allocator, &.{ self.dir_path, snapshot_file_name });
        defer self.allocator.free(final_path);

        // Stage the bytes in snapshot.tmp. O_DSYNC means write() returns
        // only after the bytes AND the file-size update are durable.
        {
            var tmp = try vfs.g().createFile(tmp_path, .{
                .flags = .{ .writable = true, .truncate = true, .dsync = true },
            });
            defer tmp.close();
            try tmp.write(file_buf.items, 0, .p0);
        }

        // Atomic rename — POSIX guarantees the directory entry flips
        // either fully to the new inode or stays at the old one.
        try vfs.VFS.renameFile(tmp_path, final_path);

        // dir-fsync so the rename itself is durable across power loss.
        // VFS.syncDir is best-effort (some filesystems don't support it).
        vfs.VFS.syncDir(self.dir_path);

        // Replace the in-memory snapshot. clone returns a heap-allocated
        // *SnapshotStruct that deeply owns its data + conf_state, so after
        // destroying the old pointer we just install the new one.
        const new_snap = try snap.clone(self.allocator);
        self.snapshot.destroy(self.allocator);
        self.snapshot = new_snap;
    }

    // --- internals -----------------------------------------------------

    /// Build slot bytes in memory and pwrite them. The state_file is opened
    /// with O_DSYNC, so the write returns only after the bytes are durable.
    /// The slot is a zbuf frame `[u64 hash | u32 len | HardState payload]`
    /// followed by zero padding to fill the 4 KiB slot.
    fn writeSlot(self: *MetaStore, hs: zen.HardStateStruct, slot_idx: u8) !void {
        // Page-aligned heap buffer (zio fibers have small stacks; we don't
        // put 4 KiB on the stack). The alignment is forward-compat for a
        // possible later O_DIRECT switch.
        const page = try self.allocator.alignedAlloc(u8, vfs.page_alignment, slot_size);
        defer self.allocator.free(page);
        @memset(page, 0);

        // Marshal HardState directly into the slot's payload region (the
        // bytes after the frame prefix). Wrap the writer with the message
        // tail so HardStateReader.parseRoot can decode it on recovery.
        var scratch = std.ArrayList(u8).initBuffer(page[zbuf.Frame.hdr_size..]);
        const off = try hs.marshalTo(self.allocator, &scratch);
        try zbuf.appendMessageTail(&scratch, self.allocator, zen.HardStateReader.TABLE_ID, off);

        // Stamp the [hash | len] frame prefix over the in-place payload
        // (Frame.write's zero-copy fast path detects the alias).
        try zbuf.Frame.write(page, scratch.items);

        const offset: u64 = @as(u64, slot_idx) * slot_size;
        try self.state_file.write(page, offset, .p0);
    }

    fn recoverHardState(self: *MetaStore) !void {
        const result = try self.state_file.readAligned(self.allocator, 0, state_file_size, .p0);
        defer result.free();
        if (result.data.len < state_file_size) return Error.MetaCorrupt;

        const slot_a = result.data[0..slot_size];
        const slot_b = result.data[slot_size..state_file_size];

        const a_ok = parseSlot(slot_a);
        const b_ok = parseSlot(slot_b);

        // Pick the slot whose HardState carries the higher `seq`. On tie
        // (shouldn't happen — we always increment) prefer slot B as a
        // deterministic tiebreaker.
        const winner: ?struct { idx: u8, hs: zen.HardStateReader } = blk: {
            if (a_ok != null and b_ok != null) {
                if (b_ok.?.seq() >= a_ok.?.seq()) {
                    break :blk .{ .idx = 1, .hs = b_ok.? };
                } else {
                    break :blk .{ .idx = 0, .hs = a_ok.? };
                }
            }
            if (a_ok) |r| break :blk .{ .idx = 0, .hs = r };
            if (b_ok) |r| break :blk .{ .idx = 1, .hs = r };
            break :blk null;
        };
        const w = winner orelse return Error.MetaCorrupt;

        self.hard_state = try zen.HardStateStruct.fromReaderValue(self.allocator, w.hs);
        self.active_slot = w.idx;
    }

    /// Validate a single slot's frame and decode the HardState. Returns
    /// null on any validation failure (invalid hash, length out of range,
    /// or HardState parseRoot failure).
    fn parseSlot(slot: []const u8) ?zen.HardStateReader {
        const payload = zbuf.Frame.read(slot) catch return null;
        return zen.HardStateReader.parseRoot(payload) catch null;
    }

    fn recoverSnapshot(self: *MetaStore) !void {
        const final_path = try std.fs.path.join(self.allocator, &.{ self.dir_path, snapshot_file_name });
        defer self.allocator.free(final_path);

        var handle = vfs.g().openFile(self.allocator, final_path, .{}) catch |err| switch (err) {
            error.FileNotFound => return, // No snapshot yet; keep default-zero.
            else => return err,
        };
        defer handle.close();

        if (handle.file_size == 0) return; // Treat empty file as no snapshot.

        const result = try handle.readAligned(self.allocator, 0, @intCast(handle.file_size), .p0);
        defer result.free();
        const buf = result.data;

        // zbuf.frame validates length-fits-in-buf and verifies the hash
        // in one pass; either failure mode means the snapshot is corrupt.
        const payload = zbuf.Frame.read(buf) catch return Error.SnapshotCorrupt;

        const reader = try zen.SnapshotReader.parseRoot(payload);
        // fromReader returns a *SnapshotStruct that owns its data + conf_state.
        // Same destroy/install pattern as setSnapshot.
        const new_snap = try zen.SnapshotStruct.fromReader(self.allocator, reader);
        self.snapshot.destroy(self.allocator);
        self.snapshot = new_snap;
    }
};
