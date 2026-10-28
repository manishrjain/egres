// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const log = std.log.scoped(.diskwal);
const builtin = @import("builtin");
const zen = @import("buffers_gen");
const zbuf = @import("zbuf").zbuf;
const _vfs = @import("vfs");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const DiskWAL = @import("diskwal.zig").DiskWAL;

// One log file of the raft log, and what surrounds it: the file
// layout and its options (`OpenOptions`), the log file itself
// (`LogFile`), the one prepared ahead of rotation (`NextLog`), and the
// batch buffer that feeds the current one (`LogBuf`). The log file list
// and the locked API are in diskwal.zig.
//
// --- <fid>.wal ---
// One log file holds a run of consecutive raft entries. Its name is the
// five-digit (or longer) file id, increasing with time; the highest id
// is the file being appended to.
//
// Layout:
//   [0, 16 MiB)             header region (`log_file_offset`)
//   [16 MiB, log_file_size) records, back to back
//
// The header region holds one zbuf `WalHeader` message inside a zbuf
// frame ([hash | len | payload], `zbuf.Frame`); the rest of the region
// is zeros, written by the fill or never written (a hole reads as
// zeros). The message is an array of `WalSlot`s, one per record:
// term, index, HLC timestamp, the record's offset and length in the
// file, flags, and the entry type. It is written when the file rotates
// out; until then the header is the empty message from `createNamed`.
//
// Each record is a zbuf frame around one `LogEntry` message. The frame's
// hash covers the payload, so a torn tail is detected on open.
//
// A file is created at `log_file_size` and, with `prezero` on,
// zero-filled to that size before its first record, so no append ever
// grows it (see `OpenOptions.log_file_size`). It rotates out when it
// holds `max_num_entries` records, the most the header can list, or
// when the next record would cross `log_file_size`, whichever comes
// first; rotation writes the header, truncates the file to its records,
// and switches to the next file, which `NextLog` created ahead of time
// as <fid>.wal.tmp and which rotation renames into place.
//
// On open, the highest id is indexed from its records: every intact
// frame up to the first zero or torn one, which the next append
// overwrites. Every lower id is indexed from its header alone, one read
// per file.
//
// A <fid>.wal.tmp is a spare: not part of the log, and the next file
// when one is needed. It is the file prepared ahead of rotation, or a
// file a trim dropped and kept (`NextLog.adoptLogFile`, `reuse`): its
// blocks are written already, so it needs no fill. A kept file's old
// records stay in it; their indexes are below every new one, which is
// how a scan tells them apart. On open the spares are taken up again;
// one cut short, a fill a crash interrupted, is deleted.

// 16 MiB header / 41 bytes-per-slot (37-byte WalSlot + 4-byte offset
// entry in the WalHeader's slots array) ≈ 409,000 max slots; round down
// to 380k to leave margin for the WalHeader struct, the frame prefix,
// and any future schema growth (zbuf's size_bytes lets us add fields
// without breaking older WAL files). `writeHeader` checks the fit. The
// header is sized so that a 256 MiB file of ~700-byte records fills
// its bytes before its slots: at 20k slots a file of 1 KiB records
// rotated every 8 MiB, a rotation every two seconds under pgbench.
pub const max_num_entries: usize = 380_000;
/// Slot lookups return this for "past the last slot".
pub const no_slot: usize = std.math.maxInt(usize);
pub const log_file_offset: usize = 16 << 20;
/// Zero-fill writes go out in pieces this big, at the lowest I/O
/// priority. Two reasons: a batch write that arrives while a log file is
/// being prepared waits behind at most one piece (about 2 ms on the
/// bench NVMe), never behind 256 MiB of zeros in one O_DSYNC write,
/// which a full bandwidth bucket would otherwise grant; and the zeros
/// are one buffer of this size, not a 256 MiB one.
const zero_piece: usize = 1 << 20;
/// The zeros themselves: one block in .bss, only ever read, so its
/// pages are the kernel's shared zero page and cost nothing.
var zero_block: [zero_piece]u8 = @splat(0);

/// Whether a log file is zero-filled to `log_file_size` when it is
/// created, so that no append ever grows the file. See `OpenOptions`.
pub const Prezero = enum {
    /// Off on ZFS, btrfs and tmpfs, on elsewhere; decided by statfs
    /// on the WAL directory at open. Off on macOS, where APFS is
    /// copy-on-write.
    auto,
    true,
    false,

    /// The setting to apply to log files under `dir_path`: `true` or
    /// `false`, never `auto`. The wal calls this once at open and hands
    /// the settled value down; the log file functions never see `auto`.
    pub fn resolve(self: Prezero, dir_path: String) Prezero {
        if (self != .auto) return self;
        if (builtin.os.tag != .linux) return .false;
        const magic = _vfs.fsMagic(dir_path) orelse return .true;
        return switch (magic) {
            _vfs.fs_magic.zfs, _vfs.fs_magic.btrfs, _vfs.fs_magic.tmpfs => .false,
            else => .true,
        };
    }
};

pub const OpenOptions = struct {
    /// The VFS the log files and meta open through, which carries the
    /// WAL device's limiter. Null = the global one.
    vfs: ?*const _vfs.VFS = null,
    /// Entries kept in memory for reads (`EntryCache`).
    cache_capacity: usize = 1024,
    /// Log file size. Every log file is created at this size, zero-filled
    /// (see `prezero`), before it is written to, so no append ever grows
    /// a file: a write that grows a file journals the size (6 ms on the
    /// bench NVMe, O_DSYNC or not), a write into zeroed blocks is at the
    /// device's floor. A record that would cross it, or the one past
    /// `max_num_entries`, rotates to the next log file, which
    /// `prepareNext` created ahead of time.
    log_file_size: u64 = 256 << 20,
    /// Zero-fill a log file to `log_file_size` when it is created. Right
    /// for ext4 and xfs, where the first write into a block journals
    /// its allocation, so an overwrite of a written block is the only
    /// write that costs nothing but the data. Pointless on
    /// copy-on-write filesystems (ZFS, btrfs), where every write
    /// allocates anew and zeros are elided, and on tmpfs, where growing
    /// a file is free; `.auto` tells them apart by statfs.
    prezero: Prezero = .auto,
    /// The most log files kept for reuse after a trim drops them (see
    /// `NextLog.adoptLogFile`). The pool holds fewer when fewer were needed,
    /// and 0 turns reuse off.
    max_spare_log_files: usize = 64,
};

pub const log_suffix = ".wal";
/// Suffix of a spare log file: one prepared ahead of rotation, or one
/// a trim dropped and kept for reuse. See `NextLog`.
pub const tmp_suffix = ".tmp";

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

pub const EntryType = zen.EntryTypeEnum;

/// In-memory slot entry. Same shape as the on-disk zbuf format
/// (`zen.WalSlotStruct`) so we can read the disk slot directly with no
/// translation. Fields default to zero so an all-zero buffer slot decodes
/// as the empty sentinel (`index == 0`).
pub const entry = zen.WalSlotStruct;

pub const LogFile = struct {
    fid: u64,
    handle: _vfs.FileHandle,
    allocator: std.mem.Allocator,
    slots: std.ArrayList(entry), // each slots[i] is a real data entry
    write_offset: u64, // next write position (meaningful for current file)
    is_current: bool,
    /// Zero-filled to `log_file_size`, so no append grows the file.
    /// False for a file handed out while its prepare was still running
    /// (NextLog.swapIn): its appends extend it, and the wal rotates off
    /// it as soon as the prepared file is ready.
    filled: bool = true,

    /// Room for any log file name: the 20 digits of a u64 fid (the
    /// `0>5` is a minimum width, not a cap) plus `.wal` and `.tmp`, 28
    /// bytes at most. `bufPrint` fails with NoSpaceLeft rather than
    /// overrun if this arithmetic ever goes stale.
    pub const NameBuf = [32]u8;

    pub fn fname(fid: u64, buf: *NameBuf) Error!String {
        return std.fmt.bufPrint(buf, "{d:0>5}" ++ log_suffix, .{fid});
    }

    pub fn tmpName(fid: u64, buf: *NameBuf) Error!String {
        return std.fmt.bufPrint(buf, "{d:0>5}" ++ log_suffix ++ tmp_suffix, .{fid});
    }

    pub fn fullPath(alloc: std.mem.Allocator, dir_path: String, name: String) ![]u8 {
        return std.fmt.allocPrint(alloc, "{s}/{s}", .{ dir_path, name });
    }

    /// Inverse of writeHeader. Reads the framed WalHeader at the start
    /// of `handle`'s header region, parses the zbuf message, and returns
    /// the data-slot list (each entry is a real WAL record). Caller owns
    /// the returned list. The first read is one page, not the 12-byte
    /// frame prefix: the disk moves a whole block either way, and a
    /// page holds a header with a few slots whole, so most files cost
    /// one read. A larger header is read a second time at exactly its
    /// frame length, never the whole region.
    pub fn readHeader(
        alloc: std.mem.Allocator,
        handle: _vfs.FileHandle,
    ) !std.ArrayList(entry) {
        const page = try handle.readAligned(alloc, 0, _vfs.page_size);
        defer page.free();

        const prefix = zbuf.Frame.readPrefix(page.data) catch
            return error.InvalidLogHeader;
        const frame_len = prefix.frameLen();
        if (frame_len > log_file_offset) return error.InvalidLogHeader;
        if (frame_len <= page.data.len) return parseHeader(alloc, page.data);

        const whole = try handle.readAligned(alloc, 0, frame_len);
        defer whole.free();
        return parseHeader(alloc, whole.data);
    }

    /// The slot list of the framed WalHeader at the start of `hdr`.
    fn parseHeader(alloc: std.mem.Allocator, hdr: String) !std.ArrayList(entry) {
        // Frame prefix [hash | len] gates the WalHeader bytes and
        // verifies the hash in one pass.
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

    /// Open a rotated <fid>.wal file: its slots come from its header.
    /// A header that is torn, or that lists no record, is the crash
    /// between a rotation and the outgoing file's header write: the
    /// file still has the empty header it was created with. Its
    /// records are then found by a scan (`openScanned`).
    pub fn openRotated(
        vfs: *const _vfs.VFS,
        alloc: std.mem.Allocator,
        dir_path: String,
        fid: u64,
    ) !LogFile {
        var name_buf: LogFile.NameBuf = undefined;
        const name = try fname(fid, &name_buf);
        const path = try fullPath(alloc, dir_path, name);
        defer alloc.free(path);

        // O_DSYNC as for the current log file: `truncateFrom` can make
        // this one current again, and the batch writes rely on it.
        var handle = try vfs.openFile(alloc, path, .{
            .mode = .default,
            .flags = .{ .writable = true, .dsync = true },
        });

        // The handle is closed on every path that leaves before the
        // errdefer below: `openScanned` opens the file itself.
        var slots_list = readHeader(alloc, handle) catch |err| {
            handle.close();
            if (err != error.InvalidLogHeader) return err;
            return openScanned(vfs, alloc, dir_path, fid);
        };
        if (slots_list.items.len == 0) {
            slots_list.deinit(alloc);
            handle.close();
            return openScanned(vfs, alloc, dir_path, fid);
        }
        errdefer handle.close();
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

    /// Index the current log file from its frames: every well-formed
    /// record up to the first zero or torn frame. Reads the entire
    /// payload region in one read, then scans in memory. Rotated
    /// log files carry their slot list in the header instead.
    fn scanFrames(
        fid: u64,
        handle: _vfs.FileHandle,
        alloc: std.mem.Allocator,
    ) !LogFile {
        var slots_list = std.ArrayList(entry).empty;
        errdefer slots_list.deinit(alloc);

        // Single read of entire payload region.
        const payload_region_size: usize = @intCast(handle.file_size - log_file_offset);
        const result = handle.readAligned(
            alloc,
            log_file_offset,
            payload_region_size,
        ) catch {
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
        // verifies the hash and slices out the payload in one shot.
        var pos: usize = 0;
        while (pos < data.len) {
            const payload = zbuf.Frame.read(data[pos..]) catch |err| switch (err) {
                // Truncated tail (final frame extends past file or fewer
                // than 8 prefix bytes left) — leave the file alone, just stop.
                zbuf.Error.OutOfBounds => break,
                // Hash mismatch — drop this frame and everything after it.
                // A torn frame: the log ends here and the next append
                // overwrites it. Not truncated: the zeros past it are
                // what keep the appends from growing the file.
                zbuf.Error.Invalid => break,
            };
            // Empty payload = zero-filled prefix in the pre-zeroed region,
            // i.e. end of real data. Real entries always carry a non-empty
            // LogEntry zbuf message.
            if (payload.len == 0) break;

            const reader = zen.LogEntryReader.parseRoot(payload) catch break;
            // A reused file's old records are intact frames past the
            // new ones. Their indexes are below the snapshot that
            // freed the file, so one never follows the record before.
            if (slots_list.getLastOrNull()) |prev| {
                if (reader.index() != prev.index + 1) break;
            }
            try slots_list.append(alloc, .{
                .term = reader.term(),
                .index = reader.index(),
                .timestamp = reader.timestamp(),
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

    /// Open the current (highest fid) log file: its entries come from
    /// `scanFrames`, whatever the header says.
    pub fn openCurrent(
        vfs: *const _vfs.VFS,
        alloc: std.mem.Allocator,
        dir_path: String,
        fid: u64,
        opts: OpenOptions,
    ) !LogFile {
        std.debug.assert(opts.prezero != .auto);
        var name_buf: LogFile.NameBuf = undefined;
        const path = try fullPath(alloc, dir_path, try fname(fid, &name_buf));
        defer alloc.free(path);

        var handle = try vfs.openFile(alloc, path, .{
            .mode = .default,
            .flags = .{ .writable = true, .dsync = true },
        });
        errdefer handle.close();

        // If the file is smaller than the header region, it was partially created
        // (crash during createCurrent). Re-create it.
        if (handle.file_size < log_file_offset) {
            handle.close();
            return createCurrent(vfs, alloc, dir_path, fid, opts);
        }

        var lf = try scanFrames(fid, handle, alloc);
        // A log file from before pre-zeroing, or one whose preparation
        // was cut short: bring it to size so appends never grow it.
        if (lf.handle.file_size < opts.log_file_size and opts.prezero == .true) {
            try lf.zeroFill(lf.handle.file_size, opts.log_file_size);
            try lf.handle.syncData();
        }
        lf.filled = opts.prezero == .true;
        return lf;
    }

    /// Create a new <fid>.wal file with zeroed header region. The empty
    /// header parses cleanly via openRotated; the wal layer derives the
    /// first append's index from the previous file's lastEntry.
    pub fn createCurrent(
        vfs: *const _vfs.VFS,
        alloc: std.mem.Allocator,
        dir_path: String,
        fid: u64,
        opts: OpenOptions,
    ) !LogFile {
        var name_buf: LogFile.NameBuf = undefined;
        const name = try fname(fid, &name_buf);
        return createNamed(vfs, alloc, dir_path, name, fid, opts);
    }

    /// `createCurrent` under a caller-chosen name: the log file prepared
    /// ahead of rotation is created as <fid>.wal.tmp (see `wal.rotate`).
    pub fn createNamed(
        vfs: *const _vfs.VFS,
        alloc: std.mem.Allocator,
        dir_path: String,
        name: String,
        fid: u64,
        opts: OpenOptions,
    ) !LogFile {
        std.debug.assert(opts.prezero != .auto);
        const path = try fullPath(alloc, dir_path, name);
        defer alloc.free(path);

        // Log files are O_DSYNC: every pwrite returns durable, so a batch
        // costs one write and no fdatasync (and no fsops pool trip). That
        // is only as cheap as fdatasync (1.7 ms per batch on the bench
        // NVMe) because the log file is zero-filled to its full size when
        // it is created, so no write ever grows it; a write that extends
        // a file journals the size and costs 6 ms, O_DSYNC or not.
        const handle = try vfs.createFile(path, .{
            .mode = .default,
            .flags = .{ .writable = true, .truncate = true, .dsync = true },
        });

        // Build the LogFile up front so a single deinit covers both the
        // handle and the (initially empty) slots list. Struct construction
        // is infallible, so no error path runs between this line and the
        // errdefer below — there's no window where ownership is in limbo.
        var lf = LogFile{
            .fid = fid,
            .handle = handle,
            .allocator = alloc,
            .slots = std.ArrayList(entry).empty,
            .write_offset = log_file_offset,
            .is_current = true,
            .filled = opts.prezero == .true,
        };
        errdefer lf.deinit();

        // Initialize the header region with a valid empty WalHeader (no
        // data slots). If the process crashes before the first append,
        // this file is reopened via openCurrent (which scans the data
        // region and ignores the header). If it later ends up read by
        // openRotated, the empty WalHeader still parses cleanly.
        try lf.writeHeader();
        if (opts.prezero == .true) {
            try lf.zeroFill(log_file_offset, opts.log_file_size);
        }
        try lf.handle.syncData();
        return lf;
    }

    /// Write zeros over [from, end), in pieces, at the fiber's I/O
    /// class: the lowest from `prepare`, the caller's at open.
    fn zeroFill(self: *LogFile, from: u64, end: u64) !void {
        var at: u64 = from;
        while (at < end) {
            const n: usize = @intCast(@min(zero_piece, end - at));
            try self.handle.write(zero_block[0..n], at);
            at += n;
        }
    }
    pub fn deinit(self: *LogFile) void {
        self.slots.deinit(self.allocator);
        self.handle.close();
    }

    pub fn truncateFile(self: *LogFile, size: u64) !void {
        try self.handle.truncate(size);
    }

    pub fn numEntries(self: *const LogFile) usize {
        return self.slots.items.len;
    }

    // --- In-memory slot access (no disk I/O) ---

    pub fn getEntry(self: *const LogFile, slot_index: usize) ?entry {
        if (slot_index >= self.slots.items.len) return null;
        return self.slots.items[slot_index];
    }

    /// First raft index this file holds, or null if the file has no
    /// entries (only the current file can be empty).
    pub fn firstIndex(self: *const LogFile) ?u64 {
        if (self.slots.items.len == 0) return null;
        return self.slots.items[0].index;
    }

    pub fn firstEmptySlot(self: *const LogFile) usize {
        return self.slots.items.len;
    }

    pub fn lastEntry(self: *const LogFile) ?entry {
        if (self.slots.items.len == 0) return null;
        return self.slots.items[self.slots.items.len - 1];
    }

    /// Returns slot position for a raft index.
    /// null = before file range (or file is empty);
    /// no_slot = beyond file range.
    pub fn slotGe(self: *const LogFile, raft_index: u64) ?usize {
        const fi = self.firstIndex() orelse return null;
        if (raft_index < fi) return null;
        const slot: usize = @intCast(raft_index - fi);
        if (slot >= self.slots.items.len) return no_slot;
        std.debug.assert(self.slots.items[slot].index == raft_index);
        return slot;
    }

    /// Truncate slots and file from slot_index onward.
    pub fn truncateFromSlot(self: *LogFile, from_slot: usize) void {
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

    /// Append a framed record to a write buffer (no I/O). Updates in-memory slots.
    pub fn appendRecordToBuffer(
        self: *LogFile,
        buf: *std.ArrayList(u8),
        entry_bytes: String,
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
    pub fn flushWriteBuffer(self: *LogFile, buf: String) !void {
        if (buf.len == 0) return;
        try self.handle.write(buf, self.write_offset - buf.len);
    }

    /// Build the WAL header as a single WalHeader zbuf message and write
    /// it in one syscall. `slots` stores every data entry in arrival
    /// order; the per-file `first_index` is implicit in slots[0].index for
    /// non-empty files (and discoverable by walking the chain across files
    /// for empty ones), so it isn't persisted. The encoded message must
    /// fit within `log_file_offset`; only the frame is written, the rest
    /// of the region stays as it is (zeros from the fill, or a hole). The
    /// zbuf message tail (table_id + root offset) lets the reader validate
    /// the format with `WalHeaderReader.parseRoot`, replacing the legacy
    /// magic/version check.
    pub fn writeHeader(self: *LogFile) !void {
        const frame = try self.encodeHeader();
        defer self.allocator.free(frame);
        try self.handle.write(frame, 0);
    }

    /// The framed WalHeader for the slot table as it is now: what
    /// `writeHeader` puts at offset 0. Caller frees. Built under the
    /// lock at rotation so the write can happen without it.
    pub fn encodeHeader(self: *const LogFile) ![]u8 {
        const hdr_region = try self.allocator.alloc(u8, log_file_offset);
        errdefer self.allocator.free(hdr_region);

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

        // Wrap the zbuf message in a [hash | len] frame so the read path
        // can find its end and verify it.
        try zbuf.Frame.write(hdr_region, msg_buf.items);

        const frame_len = @as(usize, zbuf.Frame.hdr_size) + msg_buf.items.len;
        // Shrink to the frame: the region is a 16 MiB reservation, the
        // frame a few hundred KiB at most.
        if (self.allocator.resize(hdr_region, frame_len)) {
            return hdr_region[0..frame_len];
        }
        const frame = try self.allocator.dupe(u8, hdr_region[0..frame_len]);
        self.allocator.free(hdr_region);
        return frame;
    }

    /// Open a rotated file whose header is missing or torn: its slot
    /// table comes from a scan of its records, as for the current file,
    /// and the header is written again so the next open reads it. A
    /// crash between a rotation and the writer's finalization of the
    /// outgoing file leaves exactly this.
    pub fn openScanned(
        vfs: *const _vfs.VFS,
        alloc: std.mem.Allocator,
        dir_path: String,
        fid: u64,
    ) !LogFile {
        var name_buf: LogFile.NameBuf = undefined;
        const path = try fullPath(alloc, dir_path, try fname(fid, &name_buf));
        defer alloc.free(path);

        var handle = try vfs.openFile(alloc, path, .{
            .mode = .default,
            .flags = .{ .writable = true, .dsync = true },
        });
        errdefer handle.close();

        var lf = try scanFrames(fid, handle, alloc);
        errdefer lf.slots.deinit(alloc);

        lf.is_current = false;
        try lf.writeHeader();
        try lf.truncateFile(lf.write_offset);
        try lf.handle.syncData();
        return lf;
    }
};

/// The log file after `current`. Once `current` is half full, `prepare`
/// spawns a fiber that creates and zero-fills it under a temporary name
/// (<fid>.wal.tmp, which `openPath` never reads as a log file, so a
/// restart never takes it for the current log file while the real one,
/// whose header is written only when it rotates, sits below it with an
/// empty header). `swapIn` renames the prepared file into place at rotation,
/// under a fid taken then, so file order is time order whichever file
/// it hands back; a prepare still running does not delay the rotation
/// (see swapIn). Every method runs with the wal lock held; the fiber
/// itself only hands back its result through the join handle.
pub const NextLog = struct {
    allocator: std.mem.Allocator,
    vfs: *const _vfs.VFS,
    dir_path: String,
    opts: OpenOptions,
    /// Every log file fid comes from here, prepared or created inline.
    next_fid: u64,
    /// The create fiber, from `prepare` until `swapIn` or `discard`
    /// joins it. A spawned fiber is freed by its join, so exactly once.
    creating: ?fiber.JoinHandle(anyerror!LogFile) = null,
    /// Spare log files, by fid, each on disk as <fid>.wal.tmp for
    /// `prepare` to reuse: files a trim dropped, and the ones the last
    /// run left. Touched only under the wal lock.
    spares: std.ArrayList(u64) = .empty,
    /// How many files `spares` may hold: `max_spare_log_files` from
    /// open (`adoptByIds`), then as many as were rotated in between
    /// the last two trims (`startTrim`), never above that maximum.
    spare_cap: usize = 0,
    /// Rotations since the last trim.
    rotations: usize = 0,

    /// Which of a fid's two names: the log file, or the spare.
    const Kind = enum { log, tmp };

    /// <dir>/<fid>.wal, or <dir>/<fid>.wal.tmp. Caller frees.
    fn fullPath(self: *const NextLog, fid: u64, kind: Kind) ![]u8 {
        var buf: LogFile.NameBuf = undefined;
        return LogFile.fullPath(self.allocator, self.dir_path, switch (kind) {
            .log => try LogFile.fname(fid, &buf),
            .tmp => try LogFile.tmpName(fid, &buf),
        });
    }

    /// Start preparing the next log file, unless one is being prepared
    /// or is ready. With prezero off there is nothing to do ahead: the
    /// file is then a header write and one dsync, the same cost inline
    /// at rotation. A spawn failure is logged and skipped: `swapIn`
    /// then creates inline, unfilled.
    pub fn prepare(self: *NextLog) void {
        if (self.opts.prezero == .false) return;
        if (self.creating != null) return;

        // A spare is prepared under the name it has; a new file takes
        // the next fid. Popped here, under the wal lock.
        const spare = self.spares.pop();
        const fid = spare orelse self.takeFid();
        self.creating = fiber.spawn(
            .{ .lane = .raft },
            createAndFill,
            .{ self, fid, spare != null },
        ) catch |err| {
            // The pop left the capacity in place.
            if (spare) |s| self.spares.appendAssumeCapacity(s);
            log.warn("no fiber to prepare log file {d}: {s}", .{
                fid, @errorName(err),
            });
            return;
        };
    }

    /// The next log file as <fid>.wal.tmp: that spare when `is_spare`,
    /// else a new file, zero-filled. A reuse that fails falls back to
    /// a new file under the same name.
    fn createAndFill(
        self: *NextLog,
        fid: u64,
        is_spare: bool,
    ) anyerror!LogFile {
        std.debug.assert(self.opts.prezero == .true);
        // The fill runs at the lowest I/O class, behind every commit.
        fiber.ctx().class = .bg;
        if (is_spare) {
            if (self.reuse(fid)) |lf| {
                return lf;
            } else |err| {
                log.warn(
                    "reuse of log file {d}: {s}; " ++
                        "creating a new one",
                    .{ fid, @errorName(err) },
                );
            }
        }
        var name_buf: LogFile.NameBuf = undefined;
        return LogFile.createNamed(
            self.vfs,
            self.allocator,
            self.dir_path,
            try LogFile.tmpName(fid, &name_buf),
            fid,
            self.opts,
        );
    }

    /// The spare <fid>.wal.tmp made ready as the next file, in place:
    /// what `createNamed` returns, without the fill. Written here: an
    /// empty header, zeros over the first record's place so the file
    /// scans as empty, and zeros over the tail rotation cut off. The
    /// old records in between stay (see `scanFrames`).
    pub fn reuse(self: *NextLog, fid: u64) !LogFile {
        std.debug.assert(self.opts.prezero == .true);
        const alloc = self.allocator;
        const path = try self.fullPath(fid, .tmp);
        defer alloc.free(path);

        const handle = try self.vfs.openFile(alloc, path, .{
            .mode = .default,
            .flags = .{ .writable = true, .dsync = true },
        });
        var lf = LogFile{
            .fid = fid,
            .handle = handle,
            .allocator = alloc,
            .slots = std.ArrayList(entry).empty,
            .write_offset = log_file_offset,
            .is_current = true,
            .filled = true,
        };
        errdefer lf.deinit();

        const full_size = self.opts.log_file_size;
        const size = lf.handle.file_size;
        if (size > full_size) try lf.truncateFile(full_size);
        try lf.writeHeader();

        // One page of zeros where the first record goes.
        const first_record: u64 = log_file_offset;
        const first_end = first_record + _vfs.page_size;
        try lf.zeroFill(first_record, first_end);

        // And zeros from where the file ends to its full size.
        const tail = @max(size, first_end);
        if (tail < full_size) try lf.zeroFill(tail, full_size);
        try lf.handle.syncData();
        return lf;
    }

    /// The pool at open: `fids` are the <fid>.wal.tmp files the last
    /// run left, the one it had prepared and the ones it kept. A kept
    /// file's records are below the snapshot, as when it was dropped.
    /// One not worth reusing, such as a fill cut short, is deleted.
    pub fn adoptByIds(self: *NextLog, fids: []const u64) !void {
        // Until the first trim sets it from rotations, the pool may
        // hold as many as it is ever allowed.
        self.spare_cap = self.opts.max_spare_log_files;
        for (fids) |fid| {
            const path = try self.fullPath(fid, .tmp);
            defer self.allocator.free(path);
            const size = if (_vfs.fileStat(path)) |st| st.size else |_| 0;

            if (self.canAdopt(size)) {
                try self.spares.append(self.allocator, fid);
            } else {
                _vfs.deleteFile(path) catch {};
            }
        }
    }

    /// A log file the trim is dropping: kept as the spare
    /// <fid>.wal.tmp, or deleted (`canAdopt`).
    pub fn adoptLogFile(self: *NextLog, lf: *const LogFile) void {
        if (self.canAdopt(lf.write_offset)) {
            if (self.renameToSpare(lf.fid)) return else |_| {}
        }
        const path = self.fullPath(lf.fid, .log) catch return;
        defer self.allocator.free(path);
        _vfs.deleteFile(path) catch {};
    }

    /// Whether a file with `bytes` of it written joins the pool. Not
    /// when the pool is full (`spare_cap`), when log files are not
    /// zero-filled here, or when the file is cut well short of
    /// `log_file_size` and would need most of a fill anyway.
    fn canAdopt(self: *const NextLog, bytes: u64) bool {
        if (self.opts.prezero != .true) return false;
        if (self.spares.items.len >= self.spare_cap) return false;
        const full_size = self.opts.log_file_size;
        return bytes + full_size / 16 >= full_size;
    }

    /// Fids continue above `last`, the highest log file on disk, and
    /// above every spare's, so a new log file never shares a fid, and
    /// so a name, with a spare. Never below where they already are.
    pub fn bumpFid(self: *NextLog, last: u64) void {
        var max = last;
        for (self.spares.items) |fid| max = @max(max, fid);
        self.next_fid = @max(self.next_fid, max + 1);
    }

    /// A trim begins. The pool keeps as many files as the wal rotated
    /// in since the last trim, so a file nobody took for a whole
    /// interval is unlinked here, and an idle wal's pool empties.
    pub fn startTrim(self: *NextLog) void {
        self.spare_cap = @min(
            self.rotations,
            self.opts.max_spare_log_files,
        );
        self.rotations = 0;
        while (self.spares.items.len > self.spare_cap) {
            const fid = self.spares.pop().?;
            deleteTmpByFid(self.allocator, self.dir_path, fid);
        }
    }

    /// Unlink every pooled file.
    pub fn dropSpares(self: *NextLog) void {
        for (self.spares.items) |fid| {
            deleteTmpByFid(self.allocator, self.dir_path, fid);
        }
        self.spares.clearRetainingCapacity();
    }

    /// The prepared file and the pooled ones stay on disk: the next
    /// open takes them up as spares.
    pub fn deinit(self: *NextLog) void {
        if (self.creating) |*handle| {
            if (handle.join()) |lf| {
                var prepared = lf;
                prepared.deinit();
            } else |_| {}
            self.creating = null;
        }
        self.spares.deinit(self.allocator);
    }

    /// <fid>.wal becomes the spare <fid>.wal.tmp, in the pool. Not
    /// synced: a crash that undoes the rename leaves a log file below
    /// the snapshot, as a crash before an unlink does.
    fn renameToSpare(self: *NextLog, fid: u64) !void {
        try self.spares.append(self.allocator, fid);
        errdefer _ = self.spares.pop();

        const path = try self.fullPath(fid, .log);
        defer self.allocator.free(path);
        const tmp_path = try self.fullPath(fid, .tmp);
        defer self.allocator.free(tmp_path);

        try _vfs.renameFile(path, tmp_path);
    }

    /// Whether a prepared log file is waiting for rotation.
    pub fn isReady(self: *const NextLog) bool {
        const h = self.creating orelse return false;
        return h.fiber.isFinished();
    }

    /// The next current log file, at once. The prepared one, renamed
    /// into place under a fresh fid, when its fill is done; otherwise a
    /// file created here, never zero-filled: rotation runs under the
    /// wal lock, and a fill here (log_file_size at the lowest I/O priority,
    /// starved whenever compaction is writing) would stall every commit
    /// in flight. An unfilled file's appends extend it, a few ms of
    /// journal work each, bounded by the file's life: the wal rotates
    /// again as soon as a prepared file is ready. A prepare still
    /// running is left running for that rotation.
    pub fn swapIn(self: *NextLog) !LogFile {
        self.rotations += 1;
        var inline_opts = self.opts;
        inline_opts.prezero = .false;
        if (self.creating) |*handle| {
            if (!handle.fiber.isFinished()) {
                log.warn(
                    "next log file not ready at rotation; " ++
                        "using an unfilled one",
                    .{},
                );
                return LogFile.createCurrent(
                    self.vfs,
                    self.allocator,
                    self.dir_path,
                    self.takeFid(),
                    inline_opts,
                );
            }

            // Zero-filled file is ready. Use it.
            defer self.creating = null;
            if (handle.join()) |lf| {
                var prepared = lf;
                const tmp_fid = prepared.fid;
                // The tmp fid is still the next one unless a rotation
                // went by meanwhile, or the file is a spare under its
                // old fid; then a fresh fid keeps file order equal to
                // time order.
                const fid = if (tmp_fid + 1 == self.next_fid)
                    tmp_fid
                else
                    self.takeFid();
                self.rename(tmp_fid, fid) catch |err| {
                    prepared.deinit();
                    deleteTmpByFid(self.allocator, self.dir_path, tmp_fid);
                    return err;
                };
                prepared.fid = fid;
                return prepared;
            } else |err| {
                log.err("prepared log file failed: {s}; creating inline", .{
                    @errorName(err),
                });
            }
        }
        return LogFile.createCurrent(
            self.vfs,
            self.allocator,
            self.dir_path,
            self.takeFid(),
            inline_opts,
        );
    }

    /// Join a creation in flight and delete what it made.
    pub fn discard(self: *NextLog) void {
        if (self.creating) |*handle| {
            defer self.creating = null;
            var lf = handle.join() catch return;
            deleteTmpByFid(self.allocator, self.dir_path, lf.fid);
            lf.deinit();
        }
    }

    pub fn takeFid(self: *NextLog) u64 {
        const fid = self.next_fid;
        self.next_fid += 1;
        return fid;
    }

    /// <tmp_fid>.wal.tmp becomes <fid>.wal, and the name is made durable
    /// before anything is appended under it. The two differ for a
    /// spare, and when a rotation went by during the prepare.
    fn rename(self: *NextLog, tmp_fid: u64, fid: u64) !void {
        const tmp_path = try self.fullPath(tmp_fid, .tmp);
        defer self.allocator.free(tmp_path);
        const path = try self.fullPath(fid, .log);
        defer self.allocator.free(path);

        try _vfs.renameFile(tmp_path, path);
        _vfs.syncDir(self.dir_path);
    }
};

/// The batch write path. Records are framed into `write_buf` under the
/// wal lock by `append`, which also wakes the writer fiber; the writer
/// takes the buffer as an `Item` and writes it with the lock released,
/// so appends keep landing while the disk is busy. A rotation queues
/// the outgoing file's leftovers as items ahead of `write_buf`. The
/// writer is the only fiber that writes in steady state: everyone else
/// waits on `synced`, the highest log index on disk, to reach what
/// they appended.
pub const LogBuf = struct {
    allocator: std.mem.Allocator,
    /// Buffered framed records for the current file. `addEntries`
    /// appends here without touching disk.
    write_buf: std.ArrayList(u8) = .empty,
    /// Disk work the writer owes ahead of `write_buf`, in order: a
    /// rotated-out file's last records and header. Under the wal lock,
    /// like `write_buf`.
    pending: std.ArrayList(Item) = .empty,
    /// The buffer the writer hands back once an item is down: `take`
    /// refills `write_buf` from it, so the steady state allocates
    /// nothing. Touched by the writer, or under the lock with nothing
    /// in flight; a rotation leaves it alone and lets `write_buf`
    /// grow again.
    spare: std.ArrayList(u8) = .empty,
    /// Running while an item is being written: from `take` until the
    /// writer's write returns, or across a lock-held write in `drain`
    /// and `flush`. Anything that writes, moves or truncates log files
    /// under the lock waits it out (`waitInflight`).
    inflight: fiber.Event = .{},
    /// Highest log index whose framed bytes are on disk: the leader's
    /// own vote toward commit, what `addEntries` consults before
    /// letting the cache evict (an entry above it is flushed first, so
    /// every entry is in cache OR on disk), and what `sync` waits on.
    /// Read without the lock; advanced by the writer after its write,
    /// reset under the lock when a truncation cuts the log below it.
    synced: fiber.Watermark,
    /// Highest log index framed into `write_buf`, advanced by the
    /// caller once per batch of appends (`advance`): the writer parks
    /// until it is past `synced`, i.e. there is something to write.
    /// Reset with `synced` on truncation, so the pair keeps meaning
    /// "pending or in flight" rather than a stale high-water mark.
    buffered: fiber.Watermark,
    /// The writer fiber, from `start` until `stop` joins it.
    writer: ?fiber.JoinHandle(void) = null,
    stopping: std.atomic.Value(bool) = .init(false),

    /// One unit of the writer's work: `buf` for a file at `offset` and,
    /// for a file rotated out, its `header`: written at 0 once the
    /// records are down, then the file is cut to its data end and
    /// fsynced, after which it opens from the header rather than a
    /// scan. `handle` is a copy, so a log file list that moves cannot
    /// dangle it (nothing closes a file without `drain`).
    pub const Item = struct {
        handle: _vfs.FileHandle,
        offset: u64,
        buf: std.ArrayList(u8),
        /// What `synced` becomes once the item is down.
        last_index: u64,
        header: ?[]u8 = null,

        fn dataEnd(self: *const Item) u64 {
            return self.offset + self.buf.items.len;
        }

        fn deinit(self: *Item, allocator: std.mem.Allocator) void {
            self.buf.deinit(allocator);
            if (self.header) |h| allocator.free(h);
        }
    };

    pub fn init(allocator: std.mem.Allocator) LogBuf {
        return .{
            .allocator = allocator,
            .synced = fiber.Watermark.init(allocator),
            .buffered = fiber.Watermark.init(allocator),
        };
    }

    pub fn deinit(self: *LogBuf) void {
        self.stop();
        self.dropPending();
        self.pending.deinit(self.allocator);
        self.spare.deinit(self.allocator);
        self.write_buf.deinit(self.allocator);
        self.synced.deinit();
        self.buffered.deinit();
    }

    /// Spawn the writer fiber, once. `dw` must be at its final address:
    /// the fiber takes its lock and reads its wal for the life of the
    /// DiskWAL.
    pub fn start(self: *LogBuf, dw: *DiskWAL) !void {
        if (self.writer != null) return;
        self.writer = try fiber.spawn(.{ .lane = .raft }, writerLoop, .{ self, dw });
    }

    /// Wake every waiter with error.Closed, the writer included, and
    /// join the writer. Before the log files are closed.
    pub fn stop(self: *LogBuf) void {
        self.stopping.store(true, .release);
        self.buffered.close();
        self.synced.close();
        if (self.writer) |*h| {
            defer self.writer = null;
            h.join();
        }
    }

    /// Frame a record into `write_buf` (the log file's slot list and
    /// write offset advance with it). Does not wake the writer: the
    /// caller `advance`s once after its batch, so a batch of appends
    /// is one write, not several. Lock held.
    pub fn append(self: *LogBuf, current: *LogFile, entry_bytes: String, reader: zen.LogEntryReader) !void {
        try current.appendRecordToBuffer(&self.write_buf, entry_bytes, reader);
    }

    /// Everything through `index` is in `write_buf`: wakes the writer.
    /// Lock held.
    pub fn advance(self: *LogBuf, index: u64) void {
        self.buffered.advance(index);
    }

    /// Park until `index` is on disk. `error.Reset`: the synced index
    /// was lowered meanwhile (a truncation); the caller re-checks what
    /// it waits for.
    pub fn waitSynced(self: *LogBuf, index: u64) error{ Closed, OutOfMemory, Reset }!void {
        return self.synced.waitForOrReset(index);
    }

    pub fn isInflight(self: *LogBuf) bool {
        return self.inflight.isRunning();
    }

    /// Park until no batch is in flight.
    pub fn waitInflight(self: *LogBuf) void {
        self.inflight.waitForDone();
    }

    pub fn syncedIndex(self: *const LogBuf) u64 {
        return self.synced.load();
    }

    /// For the log shrinking under it (truncation) or recovery finding
    /// everything on disk. Lock held.
    pub fn setSynced(self: *LogBuf, index: u64) void {
        self.synced.reset(index);
        self.buffered.reset(index);
    }

    /// Make everything appended so far durable, on the caller's fiber,
    /// with the lock held: what truncation and cache eviction need
    /// before they touch the log files.
    pub fn flush(self: *LogBuf, current: *LogFile, last_index: u64) !void {
        try self.drain();
        if (self.write_buf.items.len == 0) return;

        var item = self.takeWriteBuf(current, last_index);
        std.mem.swap(std.ArrayList(u8), &self.write_buf, &self.spare);
        self.inflight.setRunning();
        if (!self.blockingWrite(&item)) return error.Closed;
    }

    /// Rotation, lock held: what was buffered for `current` and the
    /// file's finalization go on the writer's queue, so the only I/O
    /// left under the lock is the swap itself. Two rotations within
    /// one write are two items.
    pub fn pushToPending(self: *LogBuf, current: *LogFile) !void {
        try self.pending.ensureUnusedCapacity(self.allocator, 1);

        const header = try current.encodeHeader();
        const last = if (current.lastEntry()) |e| e.index else 0;
        var item = self.takeWriteBuf(current, last);
        item.header = header;
        self.pending.appendAssumeCapacity(item);
    }

    /// Everything the writer owes, written: waits out the write in
    /// flight and writes what it has not taken on the caller's fiber.
    /// Lock held. What truncation, log file deletion and cache
    /// eviction need before touching the files.
    pub fn drain(self: *LogBuf) !void {
        self.waitInflight();
        while (self.pending.items.len > 0) {
            var item = self.pending.orderedRemove(0);
            self.inflight.setRunning();

            if (!self.blockingWrite(&item)) return error.Closed;
        }
    }

    /// Wait out the write in flight, drop what is buffered, nothing
    /// durable. For `wal.reset`.
    pub fn reset(self: *LogBuf) void {
        self.waitInflight();
        self.dropPending();
        utils.clearRetainCap(&self.write_buf);
        self.synced.reset(0);
        self.buffered.reset(0);
    }

    /// Free every queued item. Nothing in flight.
    fn dropPending(self: *LogBuf) void {
        for (self.pending.items) |*item| item.deinit(self.allocator);
        utils.clearRetainCap(&self.pending);
    }

    /// The writer: drains everything pending one item at a time, then
    /// parks until `buffered` moves past `synced`. A truncation resets
    /// both, which wakes it to recompute the target rather than wait
    /// for an index that may never come back. Exits as soon as `stop`
    /// is seen, leaving the rest for the closing flush.
    fn writerLoop(self: *LogBuf, dw: *DiskWAL) void {
        while (true) {
            while (!self.stopping.load(.acquire) and self.writeOne(dw)) {}

            if (self.stopping.load(.acquire)) return;

            self.buffered.waitForOrReset(self.synced.load() + 1) catch |err| switch (err) {
                error.Reset => continue,
                error.Closed, error.OutOfMemory => return,
            };
        }
    }

    /// One item: take under the lock, write with it released. False
    /// when nothing was pending, or the item was dropped at stop.
    fn writeOne(self: *LogBuf, dw: *DiskWAL) bool {
        const held = dw.mu.hold(.exclusive, @src());
        const ws = &dw.wal_store;
        const taken = self.take(held, ws.current, ws.lastIndex());
        held.release();

        var item = taken orelse return false;
        const first_index = self.synced.load() + 1;
        const started = utils.timestampNow(.monotonic, .micros);
        if (!self.blockingWrite(&item)) return false;
        const done = utils.timestampNow(.monotonic, .micros);
        if (dw.on_written) |hook| hook.call(hook.ctx, .{
            .first_index = first_index,
            .last_index = item.last_index,
            .started_us = started,
            .done_us = done,
        });
        return true;
    }

    /// The next item for the writer: the queue's head, else the
    /// records buffered for `current`. `held` is the wal lock; nothing
    /// is in flight, since only the writer takes. Null when nothing is
    /// pending.
    fn take(
        self: *LogBuf,
        held: DiskWAL.Lock.Exclusive,
        current: *const LogFile,
        last_index: u64,
    ) ?Item {
        held.mu.assertHeld(held);
        std.debug.assert(!self.isInflight());

        var item: Item = undefined;
        if (self.pending.items.len > 0) {
            item = self.pending.orderedRemove(0);
            //
        } else if (self.write_buf.items.len > 0) {
            item = self.takeWriteBuf(current, last_index);
            std.mem.swap(std.ArrayList(u8), &self.write_buf, &self.spare);
            //
        } else {
            return null;
        }
        self.inflight.setRunning();
        return item;
    }

    /// Moves `write_buf` out as an item for `current`, leaving an empty
    /// buffer behind. Lock held.
    fn takeWriteBuf(
        self: *LogBuf,
        current: *const LogFile,
        last_index: u64,
    ) Item {
        const buf = self.write_buf;
        self.write_buf = .empty;
        return .{
            .handle = current.handle,
            .offset = current.write_offset - buf.items.len,
            .buf = buf,
            .last_index = last_index,
        };
    }

    /// One item to disk, `inflight` set by the caller: retried after a
    /// pause until it lands, logging every failure, since nothing is
    /// durable until it does. The writer calls it with the lock
    /// released, `drain` and `flush` with it held. False when the WAL
    /// stopped meanwhile; the item is then dropped, nothing of it
    /// having been acknowledged.
    fn blockingWrite(self: *LogBuf, item: *Item) bool {
        defer self.inflight.setDone();
        while (true) {
            self.write_(item) catch |err| {
                log.err("write through index {d}: {s}", .{
                    item.last_index, @errorName(err),
                });
                if (self.stopping.load(.acquire)) {
                    item.deinit(self.allocator);
                    return false;
                }
                fiber.sleepMs(100);
                continue;
            };
            // The buffer becomes the spare, or is freed when there is
            // one already (a rotation's buffer, once written).
            if (self.spare.capacity == 0) {
                utils.clearRetainCap(&item.buf);
                self.spare = item.buf;
            } else item.buf.deinit(self.allocator);
            return true;
        }
    }

    /// The I/O of one item; the log file is O_DSYNC, so a write
    /// returns durable, and `synced` moves as soon as the records are
    /// down: waiters need the records, not the header, and a crash
    /// before the header opens the file from a scan. A failure leaves
    /// the item whole for a retry: the writes are idempotent, the
    /// watermark only rises, and the header is freed at the end.
    fn write_(self: *LogBuf, item: *Item) !void {
        const bytes = item.buf.items;
        if (bytes.len > 0) try item.handle.write(bytes, item.offset);
        self.synced.advance(item.last_index);

        if (item.header) |header| {
            try item.handle.write(header, 0);
            try item.handle.truncate(item.dataEnd());
            try item.handle.syncData();
            self.allocator.free(header);
            item.header = null;
        }
    }
};

/// Delete `<dir_path>/<name>`, errors ignored.
pub fn deleteFileByName(alloc: std.mem.Allocator, dir_path: String, name: String) void {
    const path = LogFile.fullPath(alloc, dir_path, name) catch return;
    defer alloc.free(path);
    _vfs.deleteFile(path) catch {};
}

/// Delete the temporary file of a log file prepared ahead of rotation.
pub fn deleteTmpByFid(alloc: std.mem.Allocator, dir_path: String, fid: u64) void {
    var name_buf: LogFile.NameBuf = undefined;
    const name = LogFile.tmpName(fid, &name_buf) catch return;
    deleteFileByName(alloc, dir_path, name);
}
