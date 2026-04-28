const std = @import("std");
const builtin = @import("builtin");
const raftlog = @import("raftwal.zig");
const zen = @import("buffers_gen");
const vfs_mod = @import("vfs");
const zio = @import("zio");
const meta = @import("meta.zig");

fn readInt(comptime T: type, buf: []const u8, endian: std.builtin.Endian) T {
    const n = @divExact(@typeInfo(T).int.bits, 8);
    std.debug.assert(buf.len >= n);
    const p: *const [n]u8 = @ptrCast(buf.ptr);
    return std.mem.readInt(T, p, endian);
}

fn writeInt(comptime T: type, buf: []u8, value: T, endian: std.builtin.Endian) void {
    const n = @divExact(@typeInfo(T).int.bits, 8);
    std.debug.assert(buf.len >= n);
    const p: *[n]u8 = @ptrCast(buf.ptr);
    std.mem.writeInt(T, p, value, endian);
}

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

pub const max_num_entries: usize = 60_000;
pub const log_file_offset: usize = 1 << 20;
pub const log_file_size: usize = 256 << 20;
pub const entry_size: usize = 16;
pub const log_suffix = ".wal";
pub const log_header_magic: u32 = 0x57414C32; // "WAL2"
pub const log_header_version: u32 = 1;
pub const log_header_size: usize = 24;
pub const first_data_slot: usize = 1;

pub const Error = error{
    CorruptLogFile,
    InvalidLogOffset,
    InvalidLogHeader,
    WriteFailed,
    CrcMismatch,
} || std.posix.MMapError || std.posix.MSyncError || std.posix.SyncError ||
    std.fs.Dir.OpenError || std.fs.Dir.MakeError || std.fs.File.OpenError ||
    std.fs.Dir.DeleteFileError ||
    std.fs.File.StatError || std.fs.File.SetEndPosError ||
    std.mem.Allocator.Error || std.fmt.BufPrintError || zen.Error;

pub const err_not_found = error{NotFound};

pub const EntryType = raftlog.EntryType;

pub const entry = struct {
    term: u64 = 0,
    index: u64 = 0,
    data_offset: u32 = 0,
    data_len: u32 = 0,
    ty: EntryType = .any,
};

pub const logFile = struct {
    fid: u64,
    handle: vfs_mod.FileHandle,
    allocator: std.mem.Allocator,
    slots: std.ArrayList(entry), // slots[0] = base entry, slots[1..] = data entries
    write_offset: u64, // next write position (meaningful for current file)
    is_current: bool,

    fn fname(fid: u64, buf: *[32]u8) Error![]const u8 {
        return std.fmt.bufPrint(buf, "{d:0>5}" ++ log_suffix, .{fid});
    }

    fn fullPath(alloc: std.mem.Allocator, dir_path: []const u8, name: []const u8) ![]u8 {
        return std.fmt.allocPrint(alloc, "{s}/{s}", .{ dir_path, name });
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

        // Read header region to get base entry and slots.
        const hdr_result = try handle.readAligned(alloc, 0, log_file_offset, .p0);
        defer hdr_result.free();
        const hdr = hdr_result.data;

        if (hdr.len < log_header_size) return error.InvalidLogHeader;
        const magic = readInt(u32, hdr[0..4], .little);
        const version = readInt(u32, hdr[4..8], .little);
        if (magic != log_header_magic or version != log_header_version) {
            return error.InvalidLogHeader;
        }

        const base = entry{
            .index = readInt(u64, hdr[8..16], .little),
            .term = readInt(u64, hdr[16..24], .little),
            .data_offset = 0,
            .data_len = 0,
            .ty = .any,
        };

        // slots[0] = base entry, slots[1..] = data entries decoded from header.
        var slots_list = std.ArrayList(entry).empty;
        errdefer slots_list.deinit(alloc);
        try slots_list.append(alloc, base);

        var slot_idx: usize = first_data_slot;
        while (slot_idx < max_num_entries) : (slot_idx += 1) {
            const off = log_header_size + slot_idx * entry_size;
            if (off + entry_size > hdr.len) break;

            const slot = hdr[off .. off + entry_size];
            const index_delta = readInt(u16, slot[4..6], .little);
            if (index_delta == 0) break;

            try slots_list.append(alloc, .{
                .term = base.term + readInt(u32, slot[0..4], .little),
                .index = base.index + index_delta,
                .ty = @enumFromInt(readInt(u16, slot[6..8], .little)),
                .data_offset = readInt(u32, slot[8..12], .little),
                .data_len = readInt(u32, slot[12..16], .little),
            });
        }

        var wo: u64 = log_file_offset;
        if (slots_list.items.len > 1) {
            const last = slots_list.items[slots_list.items.len - 1];
            wo = last.data_offset + record_header_size + last.data_len;
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
    /// CRC-framed records. Reads the entire payload region in one read, then scans in-memory.
    pub fn openCurrent(
        vfs_inst: *const vfs_mod.VFS,
        alloc: std.mem.Allocator,
        dir_path: []const u8,
        fid: u64,
        base: entry,
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
            return createCurrent(vfs_inst, alloc, dir_path, fid, base);
        }

        var slots_list = std.ArrayList(entry).empty;
        errdefer slots_list.deinit(alloc);
        try slots_list.append(alloc, base);

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

        // Scan CRC-framed records in the buffer.
        var pos: usize = 0;
        while (pos + record_header_size <= data.len) {
            const stored_crc = readInt(u32, data[pos .. pos + 4], .little);
            const payload_len = readInt(u32, data[pos + 4 .. pos + 8], .little);
            if (payload_len == 0) break;
            if (pos + record_header_size + payload_len > data.len) break;

            const payload = data[pos + record_header_size .. pos + record_header_size + payload_len];
            if (computeCrc(payload) != stored_crc) {
                // CRC mismatch — truncate file here.
                handle.truncate(log_file_offset + pos) catch {};
                break;
            }

            const reader = zen.LogEntryReader.parseRoot(payload) catch break;
            try slots_list.append(alloc, .{
                .term = reader.term(),
                .index = reader.index(),
                .data_offset = @intCast(log_file_offset + pos),
                .data_len = payload_len,
                .ty = if (reader.isConfChange()) .conf else .data,
            });

            pos += record_header_size + payload_len;
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

    /// Create a new <fid>.wal file with zeroed header region.
    pub fn createCurrent(
        vfs_inst: *const vfs_mod.VFS,
        alloc: std.mem.Allocator,
        dir_path: []const u8,
        fid: u64,
        base: entry,
    ) !logFile {
        var name_buf: [32]u8 = undefined;
        const path = try fullPath(alloc, dir_path, try fname(fid, &name_buf));
        defer alloc.free(path);

        var handle = try vfs_inst.createFile(path, .{
            .mode = .default,
            .flags = .{ .writable = true, .truncate = true, .dsync = true },
        });
        errdefer handle.close();

        // Write header with magic, version, and base entry. Slot region stays zeroed.
        const hdr = try alloc.alloc(u8, log_file_offset);
        defer alloc.free(hdr);
        @memset(hdr, 0);
        writeInt(u32, hdr[0..4], log_header_magic, .little);
        writeInt(u32, hdr[4..8], log_header_version, .little);
        writeInt(u64, hdr[8..16], base.index, .little);
        writeInt(u64, hdr[16..24], base.term, .little);
        try handle.write(hdr, 0, .p0);

        var slots_list = std.ArrayList(entry).empty;
        try slots_list.append(alloc, base);
        return .{
            .fid = fid,
            .handle = handle,
            .allocator = alloc,
            .slots = slots_list,
            .write_offset = log_file_offset,
            .is_current = true,
        };
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

    /// Number of data entries (excludes base at slots[0]).
    pub fn numEntries(self: *const logFile) usize {
        if (self.slots.items.len == 0) return 0;
        return self.slots.items.len - 1;
    }

    // --- In-memory slot access (no disk I/O) ---

    pub fn getEntry(self: *const logFile, slot_index: usize) entry {
        if (slot_index >= self.slots.items.len) {
            return .{ .term = 0, .index = 0, .data_offset = 0, .data_len = 0, .ty = .any };
        }
        return self.slots.items[slot_index];
    }

    pub fn baseEntry(self: *const logFile) entry {
        return self.slots.items[0];
    }

    pub fn firstIndex(self: *const logFile) u64 {
        return self.baseEntry().index + 1;
    }

    pub fn firstEmptySlot(self: *const logFile) usize {
        return self.slots.items.len;
    }

    pub fn lastEntry(self: *const logFile) entry {
        return self.slots.items[self.slots.items.len - 1];
    }

    /// Returns slot position for a raft index.
    /// 0 = before file range (compacted), max_num_entries = beyond file range.
    pub fn slotGe(self: *const logFile, raft_index: u64) usize {
        if (raft_index <= self.baseEntry().index) return 0;

        const slot: usize = @intCast(raft_index - self.baseEntry().index);
        if (slot >= self.slots.items.len) return max_num_entries;
        std.debug.assert(self.slots.items[slot].index == raft_index);
        return slot;
    }

    /// Truncate slots and file from slot_index onward.
    pub fn truncateFromSlot(self: *logFile, from_slot: usize) void {
        if (from_slot <= first_data_slot) {
            self.slots.shrinkRetainingCapacity(1); // keep base at [0]
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
        const payload_len: u32 = @intCast(entry_bytes.len);
        const crc = computeCrc(entry_bytes);

        // Append [crc32c][payload_len][payload_bytes] to buffer.
        const needed = record_header_size + entry_bytes.len;
        try buf.ensureUnusedCapacity(self.allocator, needed);
        var hdr_buf: [record_header_size]u8 = undefined;
        writeInt(u32, hdr_buf[0..4], crc, .little);
        writeInt(u32, hdr_buf[4..8], payload_len, .little);
        buf.appendSliceAssumeCapacity(&hdr_buf);
        buf.appendSliceAssumeCapacity(entry_bytes);

        // Update in-memory slot table.
        try self.slots.append(self.allocator, .{
            .term = reader.term(),
            .index = reader.index(),
            .data_offset = @intCast(self.write_offset),
            .data_len = payload_len,
            .ty = if (reader.isConfChange()) .conf else .data,
        });
        self.write_offset += needed;
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
        const ent = self.getEntry(slot_index);
        if (ent.index == 0) return error.InvalidLogOffset;

        const rec_start = ent.data_offset - @as(u32, @intCast(buf_offset));
        const rec_end = rec_start + record_header_size + ent.data_len;
        if (rec_end > buf.len) return error.InvalidLogOffset;

        // const stored_crc = readInt(u32, buf[rec_start .. rec_start + 4], .little);
        const payload = buf[rec_start + record_header_size .. rec_end];
        // if (stored_crc != computeCrc(payload)) return error.CrcMismatch;

        return payload;
    }

    /// Write the header region for rotation: magic, version, base entry, delta-encoded slots.
    pub fn writeHeader(self: *logFile) !void {
        var hdr_buf = try self.allocator.alloc(u8, log_file_offset);
        defer self.allocator.free(hdr_buf);
        @memset(hdr_buf, 0);

        writeInt(u32, hdr_buf[0..4], log_header_magic, .little);
        writeInt(u32, hdr_buf[4..8], log_header_version, .little);
        writeInt(u64, hdr_buf[8..16], self.baseEntry().index, .little);
        writeInt(u64, hdr_buf[16..24], self.baseEntry().term, .little);

        // Write data entries (skip base at [0]).
        for (1..self.slots.items.len) |si| {
            const off = log_header_size + si * entry_size;
            if (off + entry_size > hdr_buf.len) break;
            const ent = self.slots.items[si];

            writeInt(u32, hdr_buf[off .. off + 4], @intCast(ent.term - self.baseEntry().term), .little);
            writeInt(u16, hdr_buf[off + 4 .. off + 6], @intCast(ent.index - self.baseEntry().index), .little);
            writeInt(u16, hdr_buf[off + 6 .. off + 8], @intFromEnum(ent.ty), .little);
            writeInt(u32, hdr_buf[off + 8 .. off + 12], ent.data_offset, .little);
            writeInt(u32, hdr_buf[off + 12 .. off + 16], ent.data_len, .little);
        }

        try self.handle.write(hdr_buf, 0, .p0);
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
        slot_idx: usize, // 0 = not found / compacted
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
        vfs_mod.VFS.deleteFile(path) catch {};
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
        const cwd = std.fs.cwd();
        cwd.access(path, .{}) catch |err| switch (err) {
            error.FileNotFound => try cwd.makePath(path),
            else => return err,
        };
        const dir_copy = try alloc.dupe(u8, path);
        errdefer alloc.free(dir_copy);

        var dir = try cwd.openDir(path, .{ .iterate = true });
        defer dir.close();

        // Discover all segment file ids, sorted ascending.
        var found_fids = std.ArrayList(u64).empty;
        defer found_fids.deinit(alloc);

        var it = dir.iterate();
        while (try it.next()) |entry_dir| {
            if (entry_dir.kind != .file) continue;
            const fid = parseWalFid(entry_dir.name) orelse continue;
            try found_fids.append(alloc, fid);
        }
        std.mem.sort(u64, found_fids.items, {}, comptime std.sort.asc(u64));

        // No files: create the first one.
        if (found_fids.items.len == 0) {
            const base = entry{};
            var files = std.ArrayList(logFile).empty;
            errdefer {
                for (files.items) |*lf| lf.deinit();
                files.deinit(alloc);
            }
            try files.append(alloc, try logFile.createCurrent(vfs_inst, alloc, path, 1, base));
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
        var prev_entry: ?entry = null;
        // Open all files except the one with highest fid -- that's the current file.
        for (0..found_fids.items.len - 1) |idx| {
            const fid = found_fids.items[idx];
            var lf = logFile.openRotated(vfs_inst, alloc, path, fid) catch |err| switch (err) {
                error.InvalidLogHeader, error.CorruptLogFile => {
                    first_bad = idx;
                    break;
                },
                else => return err,
            };

            // Every next file must chain from previous file's last entry.
            if (prev_entry) |prev| {
                if (lf.baseEntry().index != prev.index or lf.baseEntry().term != prev.term) {
                    lf.deinit();
                    first_bad = idx;
                    break;
                }
            }

            // If this segment has no data entries but there are newer files,
            // treat this and later files as invalid trailing data.
            if (lf.numEntries() == 0 and idx + 1 < found_fids.items.len - 1) {
                lf.deinit();
                first_bad = idx;
                break;
            }

            prev_entry = lf.lastEntry();
            try opened.append(alloc, lf);
        }

        if (first_bad) |bad_idx| {
            if (!builtin.is_test) {
                std.log.err(
                    "raft/diskwal: dropping malformed trailing WAL files from fid={d} in {s}",
                    .{ found_fids.items[bad_idx], path },
                );
            }
            // Delete malformed file. This would also delete the would-be current file
            // (highest fid) since it's in the bad range.
            deleteFidFiles(alloc, path, found_fids.items[bad_idx..]);

            // Good rotated files remain. Create a new current file after them.
            if (opened.items.len > 0) {
                const last = opened.items[opened.items.len - 1];
                const current = try logFile.createCurrent(vfs_inst, alloc, path, last.fid + 1, last.lastEntry());
                try opened.append(alloc, current);

                var entry_cache = try EntryCache.init(alloc, cache_capacity);
                errdefer entry_cache.deinit();
                return .{
                    .allocator = alloc,
                    .vfs_inst = vfs_inst,
                    .dir_path = dir_copy,
                    .files = opened,
                    .current = &opened.items[opened.items.len - 1],
                    .cache = entry_cache,
                    .next_fid = current.fid + 1,
                    };
            }

            // All files were malformed — start fresh.
            const base = entry{};
            var files = std.ArrayList(logFile).empty;
            errdefer {
                for (files.items) |*lf| lf.deinit();
                files.deinit(alloc);
            }
            try files.append(alloc, try logFile.createCurrent(vfs_inst, alloc, path, 1, base));
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

        // Open the highest fid as the current file (CRC scan recovery).
        const cur_fid = found_fids.items[found_fids.items.len - 1];
        const cur_base = if (opened.items.len > 0)
            opened.items[opened.items.len - 1].lastEntry()
        else
            entry{};

        try opened.append(alloc, try logFile.openCurrent(vfs_inst, alloc, path, cur_fid, cur_base));

        var entry_cache = try EntryCache.init(alloc, cache_capacity);
        errdefer entry_cache.deinit();

        return .{
            .allocator = alloc,
            .vfs_inst = vfs_inst,
            .dir_path = dir_copy,
            .files = opened,
            .current = &opened.items[opened.items.len - 1],
            .cache = entry_cache,
            .next_fid = cur_fid + 1,
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
        const fi = if (self.files.items.len == 0)
            self.current.firstIndex()
        else
            self.files.items[0].firstIndex();
        return if (fi == 0) 1 else fi;
    }

    pub fn lastIndex(self: *wal) u64 {
        if (self.current.slots.items.len > 0) {
            return self.current.slots.items[self.current.slots.items.len - 1].index;
        }
        var i = self.files.items.len;
        while (i > 0) : (i -= 1) {
            const e = self.files.items[i - 1].lastEntry();
            if (e.index > 0) return e.index;
        }
        return 0;
    }

    fn getEntryFile(self: *wal, file_idx: usize) ?*logFile {
        if (file_idx >= self.files.items.len) return null;
        return &self.files.items[file_idx];
    }

    fn slotGe(self: *wal, raft_index: u64) Pos {
        if (self.files.items.len == 0) return .{ .file_idx = 0, .slot_idx = 0 };

        // Fast path: most reads hit the current (last) file.
        const cur_off = self.current.slotGe(raft_index);
        if (cur_off > 0) return .{ .file_idx = self.files.items.len - 1, .slot_idx = cur_off };

        // Binary-search all files by their first index.
        var lo: usize = 0;
        var hi: usize = self.files.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.files.items[mid].firstIndex() >= raft_index) {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        if (lo < self.files.items.len and self.files.items[lo].firstIndex() == raft_index) {
            return .{ .file_idx = lo, .slot_idx = first_data_slot };
        }

        if (lo > 0) lo -= 1;
        return .{ .file_idx = lo, .slot_idx = self.files.items[lo].slotGe(raft_index) };
    }

    fn seekEntry(self: *wal, raft_index: u64) (Error || raftlog.RaftLogError || err_not_found)!entry {
        if (raft_index == 0) {
            return .{ .term = 0, .index = 0, .data_offset = 0, .data_len = 0, .ty = .any };
        }

        const pos = self.slotGe(raft_index);
        if (pos.slot_idx == 0) return raftlog.RaftLogError.Compacted;
        if (pos.slot_idx >= max_num_entries) return raftlog.RaftLogError.Unavailable;

        const lf = self.getEntryFile(pos.file_idx) orelse return raftlog.RaftLogError.Unavailable;
        const ent = lf.getEntry(pos.slot_idx);
        if (ent.index == 0) return raftlog.RaftLogError.Unavailable;
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
            const ent = lf.getEntry(scan_idx);
            if (ent.index == 0) break;
            if (ent.index >= opts.hi) {
                plan.hit_hi = true;
                break;
            }

            // Apply type filter.
            if (opts.ty != .any and ent.ty != opts.ty) continue;

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
                const rec_end: u32 = ent.data_offset + @as(u32, record_header_size) + ent.data_len;
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

        var pos = self.slotGe(opts.lo);
        if (pos.slot_idx == 0) pos.slot_idx = first_data_slot;

        var total_size: u64 = 0;
        var file_idx = pos.file_idx;
        var slot_idx = pos.slot_idx;

        outer: while (file_idx < self.files.items.len) {
            const lf = &self.files.items[file_idx];

            if (slot_idx >= lf.firstEmptySlot()) {
                file_idx += 1;
                slot_idx = first_data_slot;
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
                const ent = lf.getEntry(si);
                if (ent.index == 0) continue;
                if (opts.ty != .any and ent.ty != opts.ty) continue;

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
                        const e = lf.getEntry(si);
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
                        .len = record_header_size + ent.data_len,
                        .ctx = sep,
                    });
                }
            }

            // Batch read all entries needing disk I/O.
            if (requests.items.len > 0) {
                try lf.handle.batchRead(&arena, requests.items, .p0);
                for (requests.items) |req| {
                    const se: *raftlog.StoredEntry = @ptrCast(@alignCast(req.ctx.?));
                    se.bytes = req.data[record_header_size..];
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
        self.last_synced_index = self.current.lastEntry().index;

        const prev = self.current.lastEntry();

        // Write header with slot metadata, truncate to actual data.
        try self.current.writeHeader();
        try self.current.truncateFile(self.current.write_offset);

        // Mark current as rotated.
        self.current.is_current = false;

        // Create the next WAL file and append to list.
        try self.files.append(self.allocator, try logFile.createCurrent(self.vfs_inst, self.allocator, self.dir_path, self.next_fid, prev));
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
        if (first_index != self.current.lastEntry().index + 1) return raftlog.RaftLogError.Invalid;

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
                self.current.write_offset + record_header_size + bytes.len > log_file_size)
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
        if (pos.slot_idx == 0) return;

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
        self.current.slots.items[0] = .{};
        self.current.slots.shrinkRetainingCapacity(1);
        self.current.write_offset = log_file_offset;
        self.cache.clear();
        self.write_buf.clearRetainingCapacity();
        self.last_synced_index = 0;
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
        if (pos.slot_idx == 0) return;

        // Delete all files after the target file.
        while (self.files.items.len > pos.file_idx + 1) {
            var extra = self.files.pop().?;
            self.deleteFileByFid(extra.fid);
            extra.deinit();
        }

        // Truncate the target file's slots and update current pointer.
        self.current = &self.files.items[pos.file_idx];
        self.current.truncateFromSlot(pos.slot_idx);
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
    mu: zio.RwLock = .init,

    pub fn openPath(
        path: []const u8,
        allocator: std.mem.Allocator,
        cache_capacity: usize,
    ) !DiskWAL {
        var ms = try meta.MetaStore.openPath(path, allocator);
        errdefer ms.deinit();

        var wf = try wal.openPath(path, allocator, cache_capacity);
        errdefer wf.deinit();

        const snap = ms.snapshotState();
        if (snap.index > 0 and wf.firstIndex() != snap.index + 1) {
            return error.CorruptLogFile;
        }

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
        self.mu.lockSharedUncancelable();
        defer self.mu.unlockShared();
        return self.meta.hardState();
    }

    pub fn setHardState(self: *DiskWAL, hs: raftlog.HardState) raftlog.RaftLogError!void {
        try self.mu.lock();
        defer self.mu.unlock();
        self.meta.setHardState(hs) catch |err| return mapToRaftLogError(err);
    }

    pub fn snapshot(self: *DiskWAL) *const raftlog.Snapshot {
        self.mu.lockSharedUncancelable();
        defer self.mu.unlockShared();
        return self.meta.snapshotState();
    }

    pub fn setSnapshot(self: *DiskWAL, snap: raftlog.Snapshot) raftlog.RaftLogError!void {
        try self.mu.lock();
        defer self.mu.unlock();
        self.meta.setSnapshot(snap) catch |err| return mapToRaftLogError(err);
    }

    pub fn firstIndex(self: *DiskWAL) u64 {
        self.mu.lockSharedUncancelable();
        defer self.mu.unlockShared();
        return self.wal_store.firstIndex();
    }

    pub fn lastIndex(self: *DiskWAL) u64 {
        self.mu.lockSharedUncancelable();
        defer self.mu.unlockShared();
        return self.wal_store.lastIndex();
    }

    pub fn lastTerm(self: *DiskWAL) u64 {
        self.mu.lockSharedUncancelable();
        defer self.mu.unlockShared();
        const last_idx = self.wal_store.lastIndex();
        const snap_term = self.meta.snapshotState().term;
        if (last_idx == 0) return snap_term;
        return self.wal_store.term(last_idx) catch snap_term;
    }

    pub fn term(self: *DiskWAL, idx: u64) raftlog.RaftLogError!?u64 {
        try self.mu.lockShared();
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
        try self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.wal_store.allEntries(self.allocator, opts) catch |err| {
            return mapToRaftLogError(err);
        };
    }

    pub fn firstIndexOfTerm(self: *DiskWAL, term_value: u64) u64 {
        self.mu.lockSharedUncancelable();
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
        try self.mu.lock();
        defer self.mu.unlock();
        self.wal_store.addEntries(entries) catch |err| return mapToRaftLogError(err);
    }

    pub fn truncateFrom(self: *DiskWAL, from_index: u64) raftlog.RaftLogError!void {
        try self.mu.lock();
        defer self.mu.unlock();
        self.wal_store.truncateFrom(from_index) catch |err| return mapToRaftLogError(err);
    }

    pub fn compactUntil(self: *DiskWAL, index: u64) raftlog.RaftLogError!void {
        try self.mu.lock();
        defer self.mu.unlock();
        self.wal_store.deleteBefore(index);
    }

    pub fn sync(self: *DiskWAL) raftlog.RaftLogError!void {
        // MetaStore writes are durable inline (state.bin opens with O_DSYNC,
        // snapshot.bin uses tmp+rename+dir-sync), so only the WAL needs an
        // explicit sync here.
        try self.mu.lockShared();
        defer self.mu.unlockShared();
        self.wal_store.sync(true) catch |err| return mapToRaftLogError(err);
    }
};

// =========================================================================
// New types for VFS-based WAL (used in upcoming refactor)
// =========================================================================

pub const record_header_size: usize = 8; // crc32c (4) + payload_len (4)

pub fn computeCrc(payload: []const u8) u32 {
    return std.hash.crc.Crc32Iscsi.hash(payload);
}

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
