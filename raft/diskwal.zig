const std = @import("std");
const builtin = @import("builtin");
const raftlog = @import("raftwal.zig");
const zen = @import("buffers_gen");
const zbuf = @import("zbuf").zbuf;
const vfs_mod = @import("vfs");
const fiber = @import("fiber");
const utils = @import("utils");
const meta = @import("meta.zig");

// DiskWAL handles disk access and writes for the Raft write-ahead log.
// Directory layout contains `wal.meta` and `<start-idx>.wal` segment files.
//
// === wal.meta file ===
// This file is small and intended to stay in one page.
//
// Layout:
// 00-08 bytes: Raft ID
// 08-16 bytes: Group ID
// 16-24 bytes: Checkpoint index
// 512 bytes: HardState (marshalled)
// 1024-1032 bytes: Snapshot index
// 1032-1040 bytes: Snapshot term
// 1040 bytes: Snapshot (marshalled)
//
// --- <0000i>.wal files ---
// These files contain raft log entries.
//
// Each file stores a base entry in a small fixed header:
// - base index: u64 (last index of previous file)
// - base term: u64 (term at base index)
//
// Slot 0 is reserved as this base entry. Real entries start from slot 1 and are
// encoded as deltas from base, plus type, payload offset and payload length:
// - term_delta: u32
// - index_delta: u16
// - type: u16
// - data offset: u32
// - data length: u32
//
// Slot size is 16 bytes. We keep a fixed 1 MiB slot region (`log_file_offset`).
//
// We pre-allocate and zero this 1 MiB slot region. Zeroed slots allow restart recovery
// to find the first empty slot safely.
//
// Entry payload bytes are stored after `log_file_offset`. Entry slots keep offsets into
// this payload area. After ~60k entries, rotate to a new segment file.
//
// --- cleanup ---
// If snapshot index is `idx_s`, keep the first segment whose first entry is <= `idx_s`,
// and all segments after it. Segments strictly below that can be deleted.
//
// --- sync ---
// mmap generally handles process crash safety well enough for default mode.
// If hard sync is enabled, call msync/fdatasync after writes.

// 1 MiB header / 41 bytes-per-slot (37-byte WalSlot + 4-byte offset entry
// in the WalHeader's slots array) ≈ 25,500 max slots; round down to 20k
// to leave margin for the WalHeader struct, the frame prefix, and any
// future schema growth (zbuf's size_bytes lets us add fields without
// breaking older WAL files).
pub const max_num_entries: usize = 20_000;
pub const log_file_offset: usize = 1 << 20;
pub const log_file_size: usize = 256 << 20;
pub const log_suffix = ".wal";

pub const Error = error{
    CorruptLogFile,
    InvalidLogOffset,
    InvalidLogHeader,
    WriteFailed,
    CrcMismatch,
} || std.posix.MMapError || std.posix.MSyncError || std.posix.SyncError ||
    std.Io.Dir.OpenError || std.Io.Dir.CreateDirPathError || std.Io.File.OpenError ||
    std.Io.Dir.DeleteFileError ||
    std.Io.File.StatError || std.Io.File.SetLengthError ||
    std.mem.Allocator.Error || std.fmt.BufPrintError || zen.Error;

pub const err_not_found = error{NotFound};

pub const EntryType = raftlog.EntryType;

/// In-memory slot entry. Same shape as the on-disk zbuf format
/// (`zen.WalSlotStruct`) so we can read the disk slot directly with no
/// translation. Fields default to zero so an all-zero buffer slot decodes
/// as the empty sentinel (`index == 0`).
pub const entry = zen.WalSlotStruct;

pub const logFile = struct {
    fid: u64,
    handle: vfs_mod.FileHandle,
    allocator: std.mem.Allocator,
    slots: std.ArrayList(entry), // each slots[i] is a real data entry
    write_offset: u64, // next write position (meaningful for current file)
    is_current: bool,

    fn fname(fid: u64, buf: *[32]u8) Error![]const u8 {
        return std.fmt.bufPrint(buf, "{d:0>5}" ++ log_suffix, .{fid});
    }

    fn fullPath(alloc: std.mem.Allocator, dir_path: []const u8, name: []const u8) ![]u8 {
        return std.fmt.allocPrint(alloc, "{s}/{s}", .{ dir_path, name });
    }

    /// Inverse of writeHeader. Reads the 1 MiB header region from `handle`,
    /// peels back the [crc32c | len] frame, parses the WalHeader zbuf
    /// message, and returns the data-slot list (each entry is a real WAL
    /// record). Caller owns the returned list.
    pub fn readHeader(
        alloc: std.mem.Allocator,
        handle: vfs_mod.FileHandle,
    ) !std.ArrayList(entry) {
        // Read the entire 1 MiB header region in one shot.
        const hdr_result = try handle.readAligned(alloc, 0, log_file_offset, .p0);
        defer hdr_result.free();
        const hdr = hdr_result.data;

        // Frame prefix [crc32c | len] gates the WalHeader bytes — strips the
        // zero padding after the message and verifies CRC in one pass.
        const msg_bytes = zbuf.Frame.read(hdr) catch return error.InvalidLogHeader;

        // parseRoot validates the zbuf message tail (table_id 42255 = WalHeader).
        const hdr_reader = zen.WalHeaderReader.parseRoot(msg_bytes) catch
            return error.InvalidLogHeader;

        var slots_list = std.ArrayList(entry).empty;
        errdefer slots_list.deinit(alloc);

        const slot_count = hdr_reader.slotsLen();
        var i: u32 = 0;
        while (i < slot_count) : (i += 1) {
            const sr = try hdr_reader.slotsAt(i);
            const srv = try sr.toStructValue(alloc);
            try slots_list.append(alloc, srv);
        }
        return slots_list;
    }

    /// Open a rotated <fid>.wal file. Reads header to populate in-memory slots.
    pub fn openRotated(
        vfs_inst: *const vfs_mod.VFS,
        alloc: std.mem.Allocator,
        dir_path: []const u8,
        fid: u64,
    ) !logFile {
        var name_buf: [32]u8 = undefined;
        const name = try fname(fid, &name_buf);
        const path = try fullPath(alloc, dir_path, name);
        defer alloc.free(path);

        var handle = try vfs_inst.openFile(alloc, path, .{
            .mode = .default,
            .flags = .{ .writable = true, .dsync = true },
        });
        errdefer handle.close();

        var slots_list = try readHeader(alloc, handle);
        errdefer slots_list.deinit(alloc);

        var wo: u64 = log_file_offset;
        if (slots_list.items.len > 0) {
            const last = slots_list.items[slots_list.items.len - 1];
            wo = last.data_offset + zbuf.Frame.hdr_size + last.data_len;
        }

        return .{
            .fid = fid,
            .handle = handle,
            .allocator = alloc,
            .slots = slots_list,
            .write_offset = wo,
            .is_current = false,
        };
    }

    /// Open the current (highest fid) WAL file and recover entries by scanning
    /// CRC-framed records. Reads the entire payload region in one read, then
    /// scans in-memory.
    pub fn openCurrent(
        vfs_inst: *const vfs_mod.VFS,
        alloc: std.mem.Allocator,
        dir_path: []const u8,
        fid: u64,
    ) !logFile {
        var name_buf: [32]u8 = undefined;
        const path = try fullPath(alloc, dir_path, try fname(fid, &name_buf));
        defer alloc.free(path);

        var handle = try vfs_inst.openFile(alloc, path, .{
            .mode = .default,
            .flags = .{ .writable = true, .dsync = true },
        });
        errdefer handle.close();

        // If the file is smaller than the header region, it was partially created
        // (crash during createCurrent). Re-create it.
        if (handle.file_size < log_file_offset) {
            handle.close();
            return createCurrent(vfs_inst, alloc, dir_path, fid);
        }

        var slots_list = std.ArrayList(entry).empty;
        errdefer slots_list.deinit(alloc);

        // Single read of entire payload region.
        const payload_region_size: usize = @intCast(handle.file_size - log_file_offset);
        const result = handle.readAligned(alloc, log_file_offset, payload_region_size, .p0) catch {
            return .{
                .fid = fid,
                .handle = handle,
                .allocator = alloc,
                .slots = slots_list,
                .write_offset = log_file_offset,
                .is_current = true,
            };
        };
        defer result.free();
        const data = result.data;

        // Scan zbuf-framed records in the buffer. Each call to readFrame
        // verifies CRC and slices out the payload in one shot.
        var pos: usize = 0;
        while (pos < data.len) {
            const payload = zbuf.Frame.read(data[pos..]) catch |err| switch (err) {
                // Truncated tail (final frame extends past file or fewer
                // than 8 prefix bytes left) — leave the file alone, just stop.
                zbuf.Error.OutOfBounds => break,
                // CRC mismatch — drop this frame and everything after it.
                zbuf.Error.Invalid => {
                    handle.truncate(log_file_offset + pos) catch {};
                    break;
                },
            };
            // Empty payload = zero-filled prefix in the pre-zeroed region,
            // i.e. end of real data. Real entries always carry a non-empty
            // LogEntry zbuf message.
            if (payload.len == 0) break;

            const reader = zen.LogEntryReader.parseRoot(payload) catch break;
            try slots_list.append(alloc, .{
                .term = reader.term(),
                .index = reader.index(),
                .data_offset = @intCast(log_file_offset + pos),
                .data_len = @intCast(payload.len),
                .entry_type = reader.entryType(),
            });

            pos += zbuf.Frame.hdr_size + payload.len;
        }

        return .{
            .fid = fid,
            .handle = handle,
            .allocator = alloc,
            .slots = slots_list,
            .write_offset = log_file_offset + pos,
            .is_current = true,
        };
    }

    /// Create a new <fid>.wal file with zeroed header region. The empty
    /// header parses cleanly via openRotated; the wal layer derives the
    /// first append's index from the previous file's lastEntry.
    pub fn createCurrent(
        vfs_inst: *const vfs_mod.VFS,
        alloc: std.mem.Allocator,
        dir_path: []const u8,
        fid: u64,
    ) !logFile {
        var name_buf: [32]u8 = undefined;
        const path = try fullPath(alloc, dir_path, try fname(fid, &name_buf));
        defer alloc.free(path);

        const handle = try vfs_inst.createFile(path, .{
            .mode = .default,
            .flags = .{ .writable = true, .truncate = true, .dsync = true },
        });

        // Build the logFile up front so a single deinit covers both the
        // handle and the (initially empty) slots list. Struct construction
        // is infallible, so no error path runs between this line and the
        // errdefer below — there's no window where ownership is in limbo.
        var lf = logFile{
            .fid = fid,
            .handle = handle,
            .allocator = alloc,
            .slots = std.ArrayList(entry).empty,
            .write_offset = log_file_offset,
            .is_current = true,
        };
        errdefer lf.deinit();

        // Initialize the header region with a valid empty WalHeader (no
        // data slots). If the process crashes before the first append,
        // this file is reopened via openCurrent (which scans the data
        // region and ignores the header). If it later ends up read by
        // openRotated, the empty WalHeader still parses cleanly.
        try lf.writeHeader();

        return lf;
    }

    pub fn deinit(self: *logFile) void {
        self.slots.deinit(self.allocator);
        self.handle.close();
    }

    pub fn syncFile(self: *logFile) !void {
        // NOTE:We are opening files with dsync =  true, we can just turn this off.
        // try self.handle.sync();
        _ = self;
        return;
    }

    pub fn truncateFile(self: *logFile, size: u64) !void {
        try self.handle.truncate(size);
    }

    pub fn numEntries(self: *const logFile) usize {
        return self.slots.items.len;
    }

    // --- In-memory slot access (no disk I/O) ---

    pub fn getEntry(self: *const logFile, slot_index: usize) ?entry {
        if (slot_index >= self.slots.items.len) return null;
        return self.slots.items[slot_index];
    }

    /// First raft index this file holds, or null if the file has no
    /// entries (only the current file can be empty).
    pub fn firstIndex(self: *const logFile) ?u64 {
        if (self.slots.items.len == 0) return null;
        return self.slots.items[0].index;
    }

    pub fn firstEmptySlot(self: *const logFile) usize {
        return self.slots.items.len;
    }

    pub fn lastEntry(self: *const logFile) ?entry {
        if (self.slots.items.len == 0) return null;
        return self.slots.items[self.slots.items.len - 1];
    }

    /// Returns slot position for a raft index.
    /// null = before file range (or file is empty);
    /// max_num_entries = beyond file range.
    pub fn slotGe(self: *const logFile, raft_index: u64) ?usize {
        const fi = self.firstIndex() orelse return null;
        if (raft_index < fi) return null;
        const slot: usize = @intCast(raft_index - fi);
        if (slot >= self.slots.items.len) return max_num_entries;
        std.debug.assert(self.slots.items[slot].index == raft_index);
        return slot;
    }

    /// Truncate slots and file from slot_index onward.
    pub fn truncateFromSlot(self: *logFile, from_slot: usize) void {
        if (from_slot == 0) {
            self.slots.shrinkRetainingCapacity(0);
            self.write_offset = log_file_offset;
        } else if (from_slot < self.slots.items.len) {
            self.write_offset = self.slots.items[from_slot].data_offset;
            self.slots.shrinkRetainingCapacity(from_slot);
        } else {
            return;
        }
        self.truncateFile(self.write_offset) catch {};
    }

    // --- Disk I/O ---

    /// Append a CRC-framed record to a write buffer (no I/O). Updates in-memory slots.
    pub fn appendRecordToBuffer(
        self: *logFile,
        buf: *std.ArrayList(u8),
        entry_bytes: []const u8,
        reader: zen.LogEntryReader,
    ) !void {
        const sz = try zbuf.Frame.append(buf, self.allocator, entry_bytes);

        // Update in-memory slot table.
        try self.slots.append(self.allocator, .{
            .term = reader.term(),
            .index = reader.index(),
            .timestamp = reader.timestamp(),
            .data_offset = @intCast(self.write_offset),
            .data_len = @intCast(entry_bytes.len),
            .entry_type = reader.entryType(),
        });
        self.write_offset += sz;
    }

    /// Flush a write buffer to disk in one call.
    pub fn flushWriteBuffer(self: *logFile, buf: []const u8) !void {
        if (buf.len == 0) return;
        try self.handle.write(buf, self.write_offset - buf.len, .p0);
    }

    /// Extract and verify a single entry's payload from a pre-read buffer.
    /// `buf` starts at `buf_offset` in the file. Returns a slice into `buf` (no copy).
    pub fn readPayloadFromBuf(
        self: *const logFile,
        slot_index: usize,
        buf: []const u8,
        buf_offset: u64,
    ) ![]const u8 {
        const ent = self.getEntry(slot_index) orelse return error.InvalidLogOffset;

        const rec_start = ent.data_offset - @as(u32, @intCast(buf_offset));
        const rec_end = rec_start + zbuf.Frame.hdr_size + ent.data_len;
        if (rec_end > buf.len) return error.InvalidLogOffset;

        // Frame.read validates the CRC over the payload bytes; mismatched
        // bytes (e.g. partial torn write captured in our pre-read) surface
        // here as zbuf.Error.Invalid instead of silently returning bad data.
        return zbuf.Frame.read(buf[rec_start..rec_end]) catch error.CrcMismatch;
    }

    /// Build the WAL header region as a single WalHeader zbuf message and
    /// write it in one syscall. `slots` stores every data entry in arrival
    /// order; the per-file `first_index` is implicit in slots[0].index for
    /// non-empty files (and discoverable by walking the chain across files
    /// for empty ones), so it isn't persisted. The encoded message must
    /// fit within `log_file_offset`; the unused tail of the region is
    /// zero-padded. The zbuf message tail (table_id + root offset) lets
    /// the reader validate the format with `WalHeaderReader.parseRoot`,
    /// replacing the legacy magic/version check.
    pub fn writeHeader(self: *logFile) !void {
        const hdr_region = try self.allocator.alloc(u8, log_file_offset);
        defer self.allocator.free(hdr_region);
        @memset(hdr_region, 0);

        var msg_buf = std.ArrayList(u8).initBuffer(hdr_region[zbuf.Frame.hdr_size..]);
        var hdr_writer = try zen.WalHeaderWriter.initWithBuf(self.allocator, &msg_buf);

        // Each data entry becomes a WalSlot child; collect their offsets
        // for setSlots, which writes them as the array index.
        if (self.slots.items.len > 0) {
            const offsets = try self.allocator.alloc(u32, self.slots.items.len);
            defer self.allocator.free(offsets);
            for (self.slots.items, 0..) |ent, i| {
                offsets[i] = try ent.marshalTo(self.allocator, &msg_buf);
            }
            try hdr_writer.setSlots(offsets);
        }

        _ = try hdr_writer.finishWithTail();

        // The whole encoded header plus the 8-byte frame prefix must fit
        // in the reserved region; otherwise we'd corrupt the data region.
        if (zbuf.Frame.hdr_size + msg_buf.items.len > log_file_offset) {
            return error.InvalidLogHeader;
        }

        // Wrap the zbuf message in a [crc32c | len] frame so the read path
        // can find its end inside the zero-padded region and verify it.
        try zbuf.Frame.write(hdr_region, msg_buf.items);

        try self.handle.write(hdr_region, 0, .p0);
    }
};

/// Inner WAL implementation. NOT internally synchronized — assumes the
/// caller (`DiskWAL`) holds `DiskWAL.mu` in the appropriate mode (shared
/// for reads, exclusive for mutations). Exposing `wal` directly without
/// going through `DiskWAL` is unsafe under concurrency.
pub const wal = struct {
    allocator: std.mem.Allocator,
    vfs_inst: *const vfs_mod.VFS,
    dir_path: []u8,
    files: std.ArrayList(logFile), // all files; last is current
    current: *logFile, // points to files.items[files.items.len - 1]
    cache: EntryCache,
    next_fid: u64,
    /// Buffered framed records for the current file. `addEntries` appends
    /// here without touching disk; `sync` flushes via one pwrite (durable
    /// because the file was opened with O_DSYNC, so the syscall return
    /// implies on-disk persistence). On rotation, this is flushed to the
    /// outgoing file before creating the next one.
    write_buf: std.ArrayList(u8) = .empty,
    /// Highest log index whose framed bytes are on disk. `addEntries`
    /// consults this before letting the cache evict: if the entry about
    /// to be evicted has index > last_synced_index, we flush write_buf
    /// first. Preserves the invariant "every entry is in cache OR on disk."
    last_synced_index: u64 = 0,

    const Pos = struct {
        file_idx: usize,
        /// null = raft_index falls before the file's range (compacted);
        /// max_num_entries = past the file's last data slot; otherwise
        /// the in-file slot offset.
        slot_idx: ?usize,
    };

    pub fn parseWalFid(name: []const u8) ?u64 {
        if (!std.mem.endsWith(u8, name, log_suffix)) return null;
        const base = name[0 .. name.len - log_suffix.len];
        if (base.len == 0) return null;
        var fid: u64 = 0;
        for (base) |c| {
            if (c < '0' or c > '9') return null;
            fid = fid * 10 + (c - '0');
        }
        return if (fid == 0) null else fid;
    }

    fn deleteFileByName(alloc: std.mem.Allocator, dir_path: []const u8, name: []const u8) void {
        const path = logFile.fullPath(alloc, dir_path, name) catch return;
        defer alloc.free(path);
        vfs_mod.deleteFile(path) catch {};
    }

    fn deleteFileByFid(self: *wal, fid: u64) void {
        var name_buf: [32]u8 = undefined;
        const name = logFile.fname(fid, &name_buf) catch return;
        deleteFileByName(self.allocator, self.dir_path, name);
    }

    fn deleteFidFiles(alloc: std.mem.Allocator, dir_path: []const u8, fids: []const u64) void {
        for (fids) |fid| {
            var name_buf: [32]u8 = undefined;
            const name = logFile.fname(fid, &name_buf) catch continue;
            deleteFileByName(alloc, dir_path, name);
        }
    }

    pub fn openPath(
        path: []const u8,
        alloc: std.mem.Allocator,
        cache_capacity: usize,
    ) !wal {
        const vfs_inst = vfs_mod.g();
        const io = utils.context.g().io;
        const cwd = std.Io.Dir.cwd();
        cwd.access(io, path, .{}) catch |err| switch (err) {
            error.FileNotFound => try cwd.createDirPath(io, path),
            else => return err,
        };
        const dir_copy = try alloc.dupe(u8, path);
        errdefer alloc.free(dir_copy);

        var dir = try cwd.openDir(io, path, .{ .iterate = true });
        defer dir.close(io);

        // Discover all segment file ids, sorted ascending.
        var found_fids = std.ArrayList(u64).empty;
        defer found_fids.deinit(alloc);

        var it = dir.iterate();
        while (try it.next(io)) |entry_dir| {
            if (entry_dir.kind != .file) continue;
            const fid = parseWalFid(entry_dir.name) orelse continue;
            try found_fids.append(alloc, fid);
        }
        std.mem.sort(u64, found_fids.items, {}, comptime std.sort.asc(u64));

        // No files: create the first one.
        if (found_fids.items.len == 0) {
            var files = std.ArrayList(logFile).empty;
            errdefer {
                for (files.items) |*lf| lf.deinit();
                files.deinit(alloc);
            }
            try files.append(alloc, try logFile.createCurrent(vfs_inst, alloc, path, 1));
            var entry_cache = try EntryCache.init(alloc, cache_capacity);
            errdefer entry_cache.deinit();
            return .{
                .allocator = alloc,
                .vfs_inst = vfs_inst,
                .dir_path = dir_copy,
                .files = files,
                .current = &files.items[files.items.len - 1],
                .cache = entry_cache,
                .next_fid = 2,
            };
        }

        // Open all files except the last as rotated (read slots from header).
        // The last (highest fid) is the current file (scan CRC from data).
        var opened = std.ArrayList(logFile).empty;
        errdefer {
            for (opened.items) |*lf| lf.deinit();
            opened.deinit(alloc);
        }
        var first_bad: ?usize = null;
        // Open all files except the one with highest fid -- that's the current file.
        // Cross-file chain validation lives in verifyChain (called from
        // DiskWAL.openPath); this loop only bails on outright open
        // failures (corrupt header, can't parse).
        for (0..found_fids.items.len - 1) |idx| {
            const fid = found_fids.items[idx];
            const lf = logFile.openRotated(vfs_inst, alloc, path, fid) catch |err| switch (err) {
                error.InvalidLogHeader, error.CorruptLogFile => {
                    first_bad = idx;
                    break;
                },
                else => return err,
            };
            try opened.append(alloc, lf);
        }

        // If any rotated file was malformed, drop it and every later fid
        // (including the would-be current). The current file gets created
        // fresh after the good rotated files; otherwise we open the
        // existing highest fid as current.
        if (first_bad) |bad_idx| {
            if (!builtin.is_test) {
                std.log.err(
                    "raft/diskwal: dropping malformed trailing WAL files from fid={d} in {s}",
                    .{ found_fids.items[bad_idx], path },
                );
            }
            deleteFidFiles(alloc, path, found_fids.items[bad_idx..]);
        }

        const cur_lf = if (first_bad != null) blk: {
            const new_fid: u64 = if (opened.items.len > 0)
                opened.items[opened.items.len - 1].fid + 1
            else
                1;
            break :blk try logFile.createCurrent(vfs_inst, alloc, path, new_fid);
        } else try logFile.openCurrent(vfs_inst, alloc, path, found_fids.items[found_fids.items.len - 1]);

        try opened.append(alloc, cur_lf);

        var entry_cache = try EntryCache.init(alloc, cache_capacity);
        errdefer entry_cache.deinit();

        return .{
            .allocator = alloc,
            .vfs_inst = vfs_inst,
            .dir_path = dir_copy,
            .files = opened,
            .current = &opened.items[opened.items.len - 1],
            .cache = entry_cache,
            .next_fid = cur_lf.fid + 1,
        };
    }

    pub fn deinit(self: *wal) void {
        self.cache.deinit();
        for (self.files.items) |*lf| lf.deinit();
        self.files.deinit(self.allocator);
        self.write_buf.deinit(self.allocator);
        self.allocator.free(self.dir_path);
    }

    pub fn firstIndex(self: *wal) u64 {
        // Walk forward to the first non-empty file. Empty files only
        // occur for the current file when nothing's been appended yet;
        // an empty WAL falls back to the start-of-log index 1.
        for (self.files.items) |*lf| {
            if (lf.firstIndex()) |fi| return fi;
        }
        return 1;
    }

    /// The newest slot across all files (current first, then rotated
    /// in reverse), or null if the WAL is empty. Both `lastIndex` and
    /// `lastTimestamp` resolve through this — same scan, different
    /// field. Reads in-memory slot cache only (no I/O, no parseRoot).
    fn lastSlot(self: *wal) ?entry {
        var i = self.files.items.len;
        while (i > 0) : (i -= 1) {
            if (self.files.items[i - 1].lastEntry()) |e| return e;
        }
        return null;
    }

    pub fn lastIndex(self: *wal) u64 {
        return if (self.lastSlot()) |s| s.index else 0;
    }

    pub fn lastTimestamp(self: *wal) u64 {
        return if (self.lastSlot()) |s| s.timestamp else 0;
    }

    fn getEntryFile(self: *wal, file_idx: usize) ?*logFile {
        if (file_idx >= self.files.items.len) return null;
        return &self.files.items[file_idx];
    }

    fn slotGe(self: *wal, raft_index: u64) Pos {
        // Fast path: most reads hit the current (last) file. A non-null
        // result means this raft_index is at-or-after the current file's
        // first slot — so it can't be in any earlier file.
        if (self.current.slotGe(raft_index)) |cur_off| {
            return .{ .file_idx = self.files.items.len - 1, .slot_idx = cur_off };
        }

        // Binary-search the rotated files by their first index. Current
        // is excluded — the fast path above already ruled it out, and
        // current may be empty (no on-disk firstIndex) in which case
        // including it here would require a fallback. Rotated files are
        // always non-empty by construction (rotation only happens after
        // appending data), so firstIndex() is non-null for them.
        var lo: usize = 0;
        var hi: usize = self.files.items.len - 1;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.files.items[mid].firstIndex().? >= raft_index) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if (lo < self.files.items.len - 1 and self.files.items[lo].firstIndex().? == raft_index) {
            return .{ .file_idx = lo, .slot_idx = 0 };
        }

        if (lo > 0) lo -= 1;
        return .{ .file_idx = lo, .slot_idx = self.files.items[lo].slotGe(raft_index) };
    }

    fn seekEntry(self: *wal, raft_index: u64) (Error || raftlog.RaftLogError || err_not_found)!entry {
        if (raft_index == 0) {
            return .{ .term = 0, .index = 0, .data_offset = 0, .data_len = 0, .entry_type = .any };
        }

        const pos = self.slotGe(raft_index);
        const slot_idx = pos.slot_idx orelse return raftlog.RaftLogError.Compacted;
        if (slot_idx >= max_num_entries) return raftlog.RaftLogError.Unavailable;

        const lf = self.getEntryFile(pos.file_idx) orelse return raftlog.RaftLogError.Unavailable;
        const ent = lf.getEntry(slot_idx) orelse return raftlog.RaftLogError.Unavailable;
        if (ent.index != raft_index) return err_not_found.NotFound;
        return ent;
    }

    pub fn term(self: *wal, idx: u64) (Error || raftlog.RaftLogError || err_not_found)!u64 {
        const ent = try self.seekEntry(idx);
        return ent.term;
    }

    const ReadPlan = struct {
        scan_start: usize,
        scan_end: usize,
        match_count: usize,
        disk_first_offset: u32,
        disk_end_offset: u32,
        disk_bytes: u64,
        hit_hi: bool, // stopped because we reached opts.hi
        hit_budget: bool, // stopped because of max_bytes
    };

    /// Scan a file's in-memory slots to plan the read strategy.
    /// Checks type filter, cache, and byte budget. No I/O.
    fn planFileRead(
        self: *wal,
        lf: *const logFile,
        start: usize,
        opts: raftlog.EntriesRangeOpts,
        bytes_so_far: u64,
    ) ReadPlan {
        var plan = ReadPlan{
            .scan_start = start,
            .scan_end = start, // scan_end points one past valid entry
            .match_count = 0,
            .disk_first_offset = 0,
            .disk_end_offset = 0,
            .disk_bytes = 0,
            .hit_hi = false,
            .hit_budget = false,
        };

        const last_slot = lf.firstEmptySlot();
        var scan_idx = start;
        while (scan_idx < last_slot) : (scan_idx += 1) {
            const ent = lf.getEntry(scan_idx) orelse break;
            if (ent.index >= opts.hi) {
                plan.hit_hi = true;
                break;
            }

            // Apply type filter.
            if (opts.ty != .any and ent.entry_type != opts.ty) continue;

            // Check budget before including this entry.
            if (plan.match_count > 0 and opts.max_bytes > 0 and
                bytes_so_far + plan.disk_bytes + ent.data_len > opts.max_bytes)
            {
                plan.hit_budget = true;
                break;
            }

            plan.match_count += 1;

            // Only count toward disk span if not in cache.
            if (self.cache.lookup(ent.index) == null) {
                const rec_end: u32 = ent.data_offset + @as(u32, zbuf.Frame.hdr_size) + ent.data_len;
                if (plan.disk_bytes == 0) plan.disk_first_offset = ent.data_offset;
                plan.disk_end_offset = rec_end;
                plan.disk_bytes += ent.data_len;
            }
        }
        plan.scan_end = scan_idx;

        return plan;
    }

    /// Read entries matching opts. Returns EntriesResult backed by an arena.
    /// Two-pass approach per file:
    ///   1. Plan: scan in-memory slots, check cache, compute disk span.
    ///   2. Read: bulk read if disk matches are dense (>= 50% of span),
    ///      individual reads if sparse. Cache hits copied into arena.
    /// Caller must call result.deinit() when done.
    pub fn allEntries(
        self: *wal,
        parent_alloc: std.mem.Allocator,
        opts: raftlog.EntriesRangeOpts,
    ) !raftlog.EntriesResult {
        var arena = std.heap.ArenaAllocator.init(parent_alloc);
        errdefer arena.deinit();
        const alloc = arena.allocator();

        var out = std.ArrayList(raftlog.StoredEntry).empty;
        var eof = true;

        const pos = self.slotGe(opts.lo);
        // null slot_idx means opts.lo lands before this file's range —
        // start scanning from slot 0 of that file.
        var total_size: u64 = 0;
        var file_idx = pos.file_idx;
        var slot_idx: usize = pos.slot_idx orelse 0;

        outer: while (file_idx < self.files.items.len) {
            const lf = &self.files.items[file_idx];

            if (slot_idx >= lf.firstEmptySlot()) {
                file_idx += 1;
                slot_idx = 0;
                continue;
            }

            // === Pass 1: Plan — scan this file's slots, check cache, compute disk span ===
            const plan = self.planFileRead(lf, @intCast(slot_idx), opts, total_size);
            if (plan.hit_budget) eof = false;

            if (plan.match_count == 0) {
                slot_idx = @intCast(plan.scan_end);
                if (plan.hit_hi) break :outer;
                continue;
            }

            // === Pass 2: Read — bulk or individual based on density of uncached entries ===
            var bulk_buf: ?[]const u8 = null;

            if (plan.disk_bytes > 0) {
                const span = plan.disk_end_offset - plan.disk_first_offset;
                const use_bulk = (span <= 2 * plan.disk_bytes);
                if (use_bulk) {
                    // One read for the whole span of uncached entries.
                    const result = try lf.handle.readAligned(alloc, plan.disk_first_offset, span, .p0);
                    // result.buf (if any) lives in the arena, so no need to free.
                    bulk_buf = result.data;
                }
            }

            // Resolve cache/bulk hits and collect disk read requests in one pass.
            var requests = std.ArrayList(vfs_mod.FileHandle.ReadRequest).empty;
            for (plan.scan_start..plan.scan_end) |si| {
                const ent = lf.getEntry(si) orelse continue;
                if (opts.ty != .any and ent.entry_type != opts.ty) continue;

                // TODO: type-filtered (.ty != .any) reads via the bulk
                // buffer return wrong bytes for some entries — manifests in
                // particular fail `LogEntryReader.parseRoot` ~10-20% of
                // the time with OutOfBounds. Probable cause is the
                // `disk_first_offset` / `disk_end_offset` math in
                // `planRange` not accounting for gaps left by skipped
                // non-matching entries, so per-entry slicing here lands at
                // the wrong offset. zdb-server's `maybeProposeSnapshot`
                // workaround: ask for `.ty = .any` and filter user-side.
                // Re-check budget (rounding safety).
                if (out.items.len > 0 and opts.max_bytes > 0 and total_size + ent.data_len > opts.max_bytes) {
                    eof = false;
                    break :outer;
                }
                total_size += ent.data_len;

                try out.append(alloc, .{ .index = ent.index, .term = ent.term });
                const sep = &out.items[out.items.len - 1];

                if (self.cache.lookup(ent.index)) |ce| {
                    sep.bytes = try alloc.dupe(u8, ce.bytes);
                } else if (bulk_buf) |buf| {
                    sep.bytes = lf.readPayloadFromBuf(si, buf, plan.disk_first_offset) catch |err| {
                        const e = lf.getEntry(si) orelse ent;
                        std.log.err("readPayloadFromBuf failed si={d} idx={d} data_off={d} data_len={d} buf_off={d} buf_len={d} err={}", .{
                            si, e.index, e.data_offset, e.data_len, plan.disk_first_offset, buf.len, err,
                        });
                        return err;
                    };
                } else {
                    // Needs disk read. Append placeholder, queue a read request
                    // with ctx pointing at the StoredEntry to fill later.
                    try requests.append(alloc, .{
                        .offset = ent.data_offset,
                        .len = zbuf.Frame.hdr_size + ent.data_len,
                        .ctx = sep,
                    });
                }
            }

            // Batch read all entries needing disk I/O.
            if (requests.items.len > 0) {
                try lf.handle.batchRead(&arena, requests.items, .p0);
                for (requests.items) |req| {
                    const se: *raftlog.StoredEntry = @ptrCast(@alignCast(req.ctx.?));
                    se.bytes = try zbuf.Frame.read(req.data);
                }
            }

            slot_idx = @intCast(plan.scan_end);
            if (plan.hit_hi) break :outer;
        }
        return .{
            .entries = try out.toOwnedSlice(alloc),
            .arena = arena,
            .eof = eof,
        };
    }

    /// Flush pending writes, write header to current file, then create a new file.
    fn rotate(self: *wal) !void {
        // Mid-batch rotation: flush whatever's accumulated for the outgoing
        // file before we move to a new one. With O_DSYNC the pwrite is the
        // durable point, so no separate fdatasync is needed.
        try self.current.flushWriteBuffer(self.write_buf.items);
        self.write_buf.clearRetainingCapacity();
        // rotate is only called from addEntries after at least one append,
        // so current.lastEntry() is non-null here.
        self.last_synced_index = self.current.lastEntry().?.index;

        // Write header with slot metadata, truncate to actual data.
        try self.current.writeHeader();
        try self.current.truncateFile(self.current.write_offset);

        // Mark current as rotated.
        self.current.is_current = false;

        // Create the next WAL file and append to list.
        try self.files.append(self.allocator, try logFile.createCurrent(self.vfs_inst, self.allocator, self.dir_path, self.next_fid));
        self.current = &self.files.items[self.files.items.len - 1];
        self.next_fid += 1;
    }

    pub fn addEntries(self: *wal, entries: []const raftlog.LogEntryBuffer) !void {
        if (entries.len == 0) return;

        // If the leader is overwriting conflicting suffix entries,
        // truncate from the first new entry's index onward.
        const first_reader = try entries[0].initReader();
        const first_index = first_reader.index();
        if (first_index <= self.lastIndex()) {
            try self.truncateFrom(first_index);
        }

        // Raft invariant: written indices must be strictly contiguous.
        // The next expected index is one past the last entry seen in any
        // file; for an empty WAL it's 1 (start of log).
        const expected_first: u64 = if (self.lastSlot()) |s| s.index + 1 else 1;
        if (first_index != expected_first) return raftlog.RaftLogError.Invalid;

        // Append into self.write_buf — the actual pwrite is deferred to
        // sync(). Cache + slot table get updated immediately so reads see
        // the new entries before they hit disk.
        var expected_index = first_index;
        for (entries) |entry_buf| {
            const reader = try entry_buf.initReader();
            if (reader.index() != expected_index) return raftlog.RaftLogError.Invalid;
            expected_index += 1;

            const bytes = entry_buf.bytes orelse return raftlog.RaftLogError.Invalid;
            if (self.current.firstEmptySlot() >= max_num_entries or
                self.current.write_offset + zbuf.Frame.hdr_size + bytes.len > log_file_size)
            {
                // Segment full: rotate (which flushes self.write_buf to the
                // outgoing file before creating the new one).
                try self.rotate();
            }

            // Buffer the CRC-framed record (no I/O), update in-memory slots.
            try self.current.appendRecordToBuffer(&self.write_buf, bytes, reader);

            // If the cache is about to evict an entry that hasn't been
            // pwritten yet, flush write_buf first. Otherwise reads of the
            // evicted index would miss in cache AND on disk.
            if (self.cache.isFull()) {
                if (self.cache.oldestIndex()) |oldest| {
                    if (oldest > self.last_synced_index) {
                        try self.sync(true);
                    }
                }
            }
            // Populate cache so reads see the entry pre-sync.
            self.cache.append(reader.index(), reader.term(), bytes) catch {};
        }
    }

    pub fn deleteBefore(self: *wal, raft_index: u64) void {
        const pos = self.slotGe(raft_index);
        // null = raft_index falls before everything we have on disk —
        // nothing to drop.
        if (pos.slot_idx == null) return;

        for (self.files.items[0..pos.file_idx]) |*lf| {
            self.deleteFileByFid(lf.fid);
            lf.deinit();
        }
        self.files.replaceRangeAssumeCapacity(0, pos.file_idx, &.{});
        self.current = &self.files.items[self.files.items.len - 1];
        self.cache.evictBefore(raft_index);
    }

    pub fn reset(self: *wal) void {
        while (self.files.items.len > 0) {
            var ef = self.files.pop().?;
            self.deleteFileByFid(ef.fid);
            ef.deinit();
        }
        self.current.slots.shrinkRetainingCapacity(0);
        self.current.write_offset = log_file_offset;
        self.cache.clear();
        self.write_buf.clearRetainingCapacity();
        self.last_synced_index = 0;
    }

    /// Walk in-memory slots starting at `start_index` and verify they
    /// form an unbroken chain. On any gap (within a file or across
    /// rotations), truncate from the first stranded index — drops the
    /// unreachable tail without rewriting anything. Caller passes
    /// `snap.index + 1` so pre-snapshot slots are skipped (apply_index
    /// jumps over them at startup anyway).
    ///
    /// Three branches:
    ///   * `start_index` below firstIndex — entire WAL is past where it
    ///     should start; wipe it via truncateFrom(firstIndex).
    ///   * `start_index` above lastIndex — nothing in scope, no-op.
    ///   * Otherwise walk the slots; truncate at the first index that
    ///     doesn't match `expected`.
    pub fn verifyChain(self: *wal, start_index: u64) !void {
        if (start_index == 0) return;

        const start = self.slotGe(start_index);
        const slot_idx = start.slot_idx orelse {
            if (self.lastIndex() > 0) {
                std.log.warn(
                    "raft/diskwal: WAL starts past start_index in {s} (start_index={d}, firstIndex={d}); wiping WAL",
                    .{ self.dir_path, start_index, self.firstIndex() },
                );
                try self.truncateFrom(self.firstIndex());
            }
            return;
        };
        if (slot_idx >= max_num_entries) return;

        var fi = start.file_idx;
        var si: usize = slot_idx;
        var expected: u64 = start_index;
        while (fi < self.files.items.len) : (fi += 1) {
            const lf = &self.files.items[fi];
            while (si < lf.slots.items.len) : (si += 1) {
                const ent = lf.slots.items[si];
                if (ent.index != expected) {
                    std.log.warn(
                        "raft/diskwal: WAL gap in {s} (expected={d}, found={d}); truncating from {d}",
                        .{ self.dir_path, expected, ent.index, expected },
                    );
                    try self.truncateFrom(expected);
                    return;
                }
                expected += 1;
            }
            si = 0;
        }
    }

    // Drops entries from `from_index` onward, keeping prefix entries intact.
    // This is used by Raft conflict resolution when a leader overwrites suffix.
    pub fn truncateFrom(self: *wal, from_index: u64) !void {
        // Flush any pending unsynced records first. write_buf may contain
        // records BELOW from_index that we want to preserve, plus records
        // AT/ABOVE from_index we want to discard. Splitting it in-place is
        // O(n) and fiddly; sync gets the prefix on disk, then we truncate
        // disk + slots in one shot.
        if (self.write_buf.items.len > 0) try self.sync(true);

        const pos = self.slotGe(from_index);
        const slot_idx = pos.slot_idx orelse return;

        // Delete all files after the target file.
        while (self.files.items.len > pos.file_idx + 1) {
            var extra = self.files.pop().?;
            self.deleteFileByFid(extra.fid);
            extra.deinit();
        }

        // Truncate the target file's slots and update current pointer.
        self.current = &self.files.items[pos.file_idx];
        self.current.truncateFromSlot(slot_idx);
        self.current.is_current = true;
        self.cache.truncateFrom(from_index);
        // last_synced_index can't exceed what's still in the log.
        if (from_index > 0 and self.last_synced_index >= from_index) {
            self.last_synced_index = from_index - 1;
        }
    }

    pub fn sync(self: *wal, _: bool) !void {
        // Flush the buffered framed records via one pwrite. With O_DSYNC the
        // syscall returns only after the data is on disk, so no separate
        // fdatasync is needed. (Older segments were flushed at rotation.)
        if (self.write_buf.items.len == 0) return;
        try self.current.flushWriteBuffer(self.write_buf.items);
        self.write_buf.clearRetainingCapacity();
        self.last_synced_index = self.lastIndex();
    }
};

pub const DiskWAL = struct {
    allocator: std.mem.Allocator,
    /// MetaStore owns the in-memory authoritative HardState + Snapshot, so
    /// the old `hard_state_cache` + `snapshot_cache` pointers go away.
    meta: meta.MetaStore,
    wal_store: wal,
    /// Single rwlock guarding all DiskWAL state — covers both `meta` and
    /// `wal_store` since the public API (hardState/snapshot/append/...)
    /// can read/mutate either. Shared for reads, exclusive for writes.
    mu: fiber.RWMutex = .{},

    pub fn openPath(
        path: []const u8,
        allocator: std.mem.Allocator,
        cache_capacity: usize,
    ) !DiskWAL {
        var ms = try meta.MetaStore.openPath(path, allocator);
        errdefer ms.deinit();

        var wf = try wal.openPath(path, allocator, cache_capacity);
        errdefer wf.deinit();

        // Verify the WAL forms an unbroken chain from snap.index+1
        // forward. Pre-snapshot entries on disk are fine — apply_index
        // jumps over them. A gap is data loss past the snapshot; the
        // verify routine truncates the stranded tail and lets raft
        // re-replicate from the leader.
        try wf.verifyChain(ms.snapshotState().index + 1);

        // Recovery: every entry the wal just discovered is on disk and durable.
        wf.last_synced_index = wf.lastIndex();

        return .{
            .allocator = allocator,
            .meta = ms,
            .wal_store = wf,
        };
    }

    pub fn deinit(self: *DiskWAL) void {
        self.wal_store.deinit();
        self.meta.deinit();
    }

    fn mapToRaftLogError(err: anyerror) raftlog.RaftLogError {
        return switch (err) {
            raftlog.RaftLogError.Compacted => raftlog.RaftLogError.Compacted,
            raftlog.RaftLogError.Unavailable => raftlog.RaftLogError.Unavailable,
            err_not_found.NotFound => raftlog.RaftLogError.Unavailable,
            error.OutOfMemory => error.OutOfMemory,
            else => {
                std.log.err("mapToRaftLogError: unmapped error: {}", .{err});
                return raftlog.RaftLogError.Unavailable;
            },
        };
    }

    // --- Public API: each method takes the appropriate lock inline. Mode
    // matches the prior vtable wrapping (shared for reads, exclusive for
    // writes, _uncancelable for cheap struct-field reads where blocking
    // on cancellation isn't useful).

    pub fn hardState(self: *DiskWAL) *const raftlog.HardState {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.meta.hardState();
    }

    /// Atomic field-set update. The merge of `update` over the current
    /// HardState happens under `self.mu`, which also serializes with
    /// every other DiskWAL writer, so concurrent calls (e.g. raft
    /// mainLoop persisting term/vote, commitPersistLoop persisting
    /// commit_index) can't race-clobber each other's fields.
    pub fn setHardState(self: *DiskWAL, update: raftlog.HardStateUpdate) raftlog.RaftLogError!void {
        self.mu.lock();
        defer self.mu.unlock();
        var next = self.meta.hardState().*;
        if (update.current_term) |v| next.current_term = v;
        if (update.voted_for) |v| next.voted_for = v;
        if (update.raft_id) |v| next.raft_id = v;
        if (update.checkpoint_index) |v| next.checkpoint_index = v;
        if (update.commit_index) |v| next.commit_index = v;
        if (update.max_file_id) |v| next.max_file_id = v;
        self.meta.setHardState(next) catch |err| return mapToRaftLogError(err);
    }

    /// Persist a new `max_file_id` ceiling. Designed to be passed as a
    /// callback to `zdb.file_ids.FileIds.bind` (matches the
    /// `*anyopaque, u64 → !void` shape it expects). The DiskWAL itself
    /// is the persistence backend, so the callback lives here.
    pub fn persistMaxFileId(ctx: *anyopaque, new_max: u64) anyerror!void {
        const self: *DiskWAL = @ptrCast(@alignCast(ctx));
        try self.setHardState(.{ .max_file_id = new_max });
    }

    pub fn snapshot(self: *DiskWAL) *const raftlog.Snapshot {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.meta.snapshotState();
    }

    pub fn setSnapshot(self: *DiskWAL, snap: raftlog.Snapshot) raftlog.RaftLogError!void {
        self.mu.lock();
        defer self.mu.unlock();
        self.meta.setSnapshot(snap) catch |err| return mapToRaftLogError(err);
    }

    /// Find the highest raft index whose tail (slots from there to the head)
    /// reaches `min_bytes`, OR whose HLC second is `min_age_seconds` older
    /// than wall-clock now. Returns null if neither threshold is met.
    /// No payload reads — uses slot.data_len and slot.timestamp directly.
    /// The replay-safe horizon (oldest in-memory memtable) is applied by the
    /// caller via @min on the returned value.
    pub fn findIndexOlderThan(
        self: *DiskWAL,
        min_bytes: u64,
        min_age_seconds: u64,
    ) ?u64 {
        self.mu.lockShared();
        defer self.mu.unlockShared();

        const head = self.wal_store.lastIndex();
        if (head == 0) return null;

        const now_secs: u32 = @intCast(std.Io.Timestamp.now(utils.context.g().io, .real).toSeconds());
        var bytes_acc: u64 = 0;

        var fi: usize = self.wal_store.files.items.len;
        while (fi > 0) {
            fi -= 1;
            const f = &self.wal_store.files.items[fi];
            var si: usize = f.slots.items.len;
            while (si > 0) {
                si -= 1;
                const slot = f.slots.items[si];
                bytes_acc += slot.data_len;
                const ts = utils.hlc.HLC.fromU64(slot.timestamp);
                const age_ok = ts.seconds != 0 and now_secs >= ts.seconds and
                    @as(u64, now_secs - ts.seconds) >= min_age_seconds;
                if (age_ok or bytes_acc >= min_bytes) return slot.index;
            }
        }
        return null;
    }

    pub fn firstIndex(self: *DiskWAL) u64 {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.wal_store.firstIndex();
    }

    pub fn lastIndex(self: *DiskWAL) u64 {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.wal_store.lastIndex();
    }

    pub fn lastTimestamp(self: *DiskWAL) u64 {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.wal_store.lastTimestamp();
    }

    pub fn lastTerm(self: *DiskWAL) u64 {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        const last_idx = self.wal_store.lastIndex();
        const snap_term = self.meta.snapshotState().term;
        if (last_idx == 0) return snap_term;
        return self.wal_store.term(last_idx) catch snap_term;
    }

    pub fn term(self: *DiskWAL, idx: u64) raftlog.RaftLogError!?u64 {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        if (idx == 0) return 0;
        return self.wal_store.term(idx) catch |err| switch (err) {
            raftlog.RaftLogError.Compacted => null,
            raftlog.RaftLogError.Unavailable => null,
            err_not_found.NotFound => null,
            else => return mapToRaftLogError(err),
        };
    }

    pub fn entriesRange(self: *DiskWAL, opts: raftlog.EntriesRangeOpts) raftlog.RaftLogError!raftlog.EntriesResult {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.wal_store.allEntries(self.allocator, opts) catch |err| {
            return mapToRaftLogError(err);
        };
    }

    pub fn firstIndexOfTerm(self: *DiskWAL, term_value: u64) u64 {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        var idx = self.wal_store.firstIndex();
        const last = self.wal_store.lastIndex();
        while (idx <= last) : (idx += 1) {
            const t = self.wal_store.term(idx) catch continue;
            if (t == term_value) return idx;
        }
        return 0;
    }

    pub fn append(self: *DiskWAL, entries: []const raftlog.LogEntryBuffer) raftlog.RaftLogError!void {
        self.mu.lock();
        defer self.mu.unlock();
        self.wal_store.addEntries(entries) catch |err| return mapToRaftLogError(err);
    }

    pub fn truncateFrom(self: *DiskWAL, from_index: u64) raftlog.RaftLogError!void {
        self.mu.lock();
        defer self.mu.unlock();
        self.wal_store.truncateFrom(from_index) catch |err| return mapToRaftLogError(err);
    }

    pub fn compactUntil(self: *DiskWAL, index: u64) raftlog.RaftLogError!void {
        self.mu.lock();
        defer self.mu.unlock();
        self.wal_store.deleteBefore(index);
    }

    pub fn sync(self: *DiskWAL) raftlog.RaftLogError!void {
        // MetaStore writes are durable inline (state.bin opens with O_DSYNC,
        // snapshot.bin uses tmp+rename+dir-sync), so only the WAL needs an
        // explicit sync here.
        self.mu.lockShared();
        defer self.mu.unlockShared();
        self.wal_store.sync(true) catch |err| return mapToRaftLogError(err);
    }
};

// =========================================================================
// New types for VFS-based WAL (used in upcoming refactor)
// =========================================================================

/// Fixed-capacity ring buffer of recent WAL entries. Entries are contiguous
/// by raft index — append at tail, evict from head. O(1) append, O(1) lookup.
pub const EntryCache = struct {
    allocator: std.mem.Allocator,
    buf: []CachedEntry,
    head: usize, // index of oldest entry in buf
    len: usize, // number of live entries
    hits: u64,
    misses: u64,

    pub const CachedEntry = struct {
        index: u64,
        term: u64,
        bytes: []const u8, // allocator-owned
    };

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !EntryCache {
        const buf = try allocator.alloc(CachedEntry, capacity);
        return .{
            .allocator = allocator,
            .buf = buf,
            .head = 0,
            .len = 0,
            .hits = 0,
            .misses = 0,
        };
    }

    pub fn deinit(self: *EntryCache) void {
        self.clear();
        self.allocator.free(self.buf);
    }

    pub fn clear(self: *EntryCache) void {
        for (0..self.len) |i| {
            self.allocator.free(self.buf[(self.head + i) % self.buf.len].bytes);
        }
        self.head = 0;
        self.len = 0;
    }

    /// Raft index of the oldest entry — the one a full-cache `append`
    /// would overwrite. `null` if cache is empty or disabled.
    pub fn oldestIndex(self: *const EntryCache) ?u64 {
        if (self.buf.len == 0 or self.len == 0) return null;
        return self.buf[self.head].index;
    }

    /// True if the next `append` would have to evict an existing entry.
    pub fn isFull(self: *const EntryCache) bool {
        return self.buf.len > 0 and self.len == self.buf.len;
    }

    pub fn append(self: *EntryCache, index: u64, term_val: u64, bytes: []const u8) !void {
        if (self.buf.len == 0) return;

        const owned = try self.allocator.dupe(u8, bytes);

        if (self.len == self.buf.len) {
            // Ring full — overwrite oldest.
            self.allocator.free(self.buf[self.head].bytes);
            self.buf[self.head] = .{ .index = index, .term = term_val, .bytes = owned };
            self.head = (self.head + 1) % self.buf.len;
        } else {
            const tail = (self.head + self.len) % self.buf.len;
            self.buf[tail] = .{ .index = index, .term = term_val, .bytes = owned };
            self.len += 1;
        }
    }

    /// O(1) lookup by raft index.
    pub fn lookup(self: *EntryCache, index: u64) ?CachedEntry {
        if (self.len == 0) {
            self.misses += 1;
            return null;
        }
        const base = self.buf[self.head].index;
        if (index < base or index - base >= self.len) {
            self.misses += 1;
            return null;
        }
        const ce = self.buf[(self.head + @as(usize, @intCast(index - base))) % self.buf.len];
        if (ce.index != index) {
            self.misses += 1;
            return null;
        }
        self.hits += 1;
        return ce;
    }

    /// Drop all entries with index >= from_index.
    pub fn truncateFrom(self: *EntryCache, from_index: u64) void {
        if (self.len == 0) return;
        const base = self.buf[self.head].index;
        if (from_index <= base) {
            self.clear();
            return;
        }
        const keep: usize = @intCast(@min(from_index - base, self.len));
        for (keep..self.len) |i| {
            self.allocator.free(self.buf[(self.head + i) % self.buf.len].bytes);
        }
        self.len = keep;
    }

    /// Drop all entries with index < before_index.
    pub fn evictBefore(self: *EntryCache, before_index: u64) void {
        if (self.len == 0) return;
        const base = self.buf[self.head].index;
        if (before_index <= base) return;
        const drop: usize = @intCast(@min(before_index - base, self.len));
        for (0..drop) |i| {
            self.allocator.free(self.buf[(self.head + i) % self.buf.len].bytes);
        }
        self.head = (self.head + drop) % self.buf.len;
        self.len -= drop;
    }
};
