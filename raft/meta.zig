// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// MetaStore — durable HardState for the Raft log.
//
//   state.bin     — 8 KiB, two-slot double-buffered HardState. Opened with
//                   O_DSYNC so each pwrite is durable on return — no
//                   explicit fdatasync syscall per write.
//
// The snapshot has no file of its own: it is the `.snapshot` entry in the
// log that `HardState.snapshot_entry_index` names (DiskWAL.setSnapshot).
//
// The double-buffer protects HardState against torn writes: each slot is a
// zbuf frame around a serialized HardState, and HardState carries its own
// monotonic `seq` field. On recovery we pick the slot whose decoded
// HardState has the highest seq. A torn write to the just-written slot
// fails the frame's hash check and recovery silently returns the previous
// (older) durable state.
//
// HardState changes (votes, term bumps, the snapshot's entry) are durable
// when `setHardState` returns. There is no separate `MetaStore.sync()`.
//
// All file I/O goes through `vfs`. Directory creation and existence checks
// use `std.fs.cwd()` because they're path-metadata operations, not data I/O.

const std = @import("std");
const _vfs = @import("vfs");
const utils = @import("utils");
const String = utils.String;
const zen = @import("buffers_gen");
const zbuf = @import("zbuf").zbuf;

const slot_size: usize = 4096;
const slot_count: usize = 2;
const state_file_size: usize = slot_size * slot_count;

const state_file_name = "state.bin";

pub const Error = error{
    MetaCorrupt,
    PayloadTooLarge,
    InvalidEncoding,
};

pub const MetaStore = struct {
    allocator: std.mem.Allocator,
    /// Open with O_DSYNC so per-write fdatasync is implicit, through the
    /// WAL device's VFS.
    state_file: _vfs.FileHandle,

    /// Authoritative in-memory state. Updated only after the corresponding
    /// disk write has been durable. By value: all its fields are scalars.
    hard_state: zen.HardStateStruct,

    /// Index of the currently-durable slot (0 = slot A at offset 0,
    /// 1 = slot B at offset 4096). The next write goes to `1 - active_slot`.
    /// The recovery sequence number lives in `hard_state.seq`.
    active_slot: u8,

    pub fn openPath(
        vfs: *const _vfs.VFS,
        path: String,
        alloc: std.mem.Allocator,
    ) !MetaStore {
        _vfs.access(path) catch |err| switch (err) {
            error.FileNotFound => try _vfs.createDirPath(path),
            else => return err,
        };

        const state_path = try std.fs.path.join(alloc, &.{ path, state_file_name });
        defer alloc.free(state_path);

        // Open the existing state.bin if present; otherwise create + initialise.
        // Both branches set DSYNC so subsequent writes are durable on return —
        // we never call state_file.sync() in the hot path.
        const state_opts: _vfs.FileOptions = .{
            .flags = .{ .writable = true, .dsync = true },
        };
        var fresh = false;
        var state_file = vfs.openFile(
            alloc,
            state_path,
            state_opts,
        ) catch |err| switch (err) {
            error.FileNotFound => blk: {
                fresh = true;
                break :blk try vfs.createFile(state_path, .{
                    .flags = .{ .writable = true, .truncate = true, .dsync = true },
                });
            },
            else => return err,
        };
        errdefer state_file.close();

        if (fresh) {
            try state_file.truncate(state_file_size);
            // O_DSYNC makes the file's bytes durable, not its name.
            try _vfs.syncDir(path);
        }

        var ms: MetaStore = .{
            .allocator = alloc,
            .state_file = state_file,
            .hard_state = .{},
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

        return ms;
    }

    pub fn deinit(self: *MetaStore) void {
        self.state_file.close();
    }

    /// Returns a pointer to the in-memory authoritative HardState. Valid
    /// until the next setHardState call. Cheap; no allocation.
    pub fn hardState(self: *const MetaStore) *const zen.HardStateStruct {
        return &self.hard_state;
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

    // --- internals -----------------------------------------------------

    /// Build slot bytes in memory and pwrite them. The state_file is opened
    /// with O_DSYNC, so the write returns only after the bytes are durable.
    /// The slot is a zbuf frame `[u64 hash | u32 len | HardState payload]`
    /// followed by zero padding to fill the 4 KiB slot.
    fn writeSlot(self: *MetaStore, hs: zen.HardStateStruct, slot_idx: u8) !void {
        // Page-aligned heap buffer (fibers have small stacks; we don't
        // put 4 KiB on the stack). The alignment is forward-compat for a
        // possible later O_DIRECT switch.
        const page = try self.allocator.alignedAlloc(
            u8,
            _vfs.page_alignment,
            slot_size,
        );
        defer self.allocator.free(page);
        utils.fill(page, 0);

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
        try self.state_file.write(page, offset);
    }

    fn recoverHardState(self: *MetaStore) !void {
        const result = try self.state_file.readAligned(
            self.allocator,
            0,
            state_file_size,
        );
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

        self.hard_state = try w.hs.toStructValue(self.allocator);
        self.active_slot = w.idx;
    }

    /// Validate a single slot's frame and decode the HardState. Returns
    /// null on any validation failure (invalid hash, length out of range,
    /// or HardState parseRoot failure).
    fn parseSlot(slot: String) ?zen.HardStateReader {
        const payload = zbuf.Frame.read(slot) catch return null;
        return zen.HardStateReader.parseRoot(payload) catch null;
    }
};
