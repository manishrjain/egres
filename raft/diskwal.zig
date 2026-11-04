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
const _meta = @import("meta.zig");

// DiskWAL handles disk access and writes for the Raft write-ahead log.
// Directory layout contains `wal.meta` and `<start-idx>.wal` log files.
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
// --- cleanup ---
// If snapshot index is `idx_s`, keep the first log file whose first entry is <= `idx_s`,
// and all log files after it. Log files strictly below that can be deleted.
//
// --- sync ---
// mmap generally handles process crash safety well enough for default mode.
// If hard sync is enabled, call msync/fdatasync after writes.

const _lf = @import("log_file.zig");
// The log file and its buffer live in log_file.zig; the names the
// code below and the callers use are re-exported here.
pub const LogFile = _lf.LogFile;
pub const NextLog = _lf.NextLog;
pub const LogBuf = _lf.LogBuf;
pub const Prezero = _lf.Prezero;
pub const OpenOptions = _lf.OpenOptions;
pub const Error = _lf.Error;
pub const EntryType = _lf.EntryType;
pub const entry = _lf.entry;
pub const max_num_entries = _lf.max_num_entries;
pub const no_slot = _lf.no_slot;
pub const log_file_offset = _lf.log_file_offset;
pub const log_suffix = _lf.log_suffix;
pub const tmp_suffix = _lf.tmp_suffix;

pub const err_not_found = error{NotFound};

pub const NodeId = u64;

// Wire-format enums live in raft/buffers.zig; the Zig types are
// re-exported so consumers can write `diskwal.ConfChangeType` instead
// of reaching into the generated zbuf module directly.
pub const ConfChangeType = zen.ChangeTypeEnum;

pub const ConfChange = struct {
    change_type: ConfChangeType,
    node_id: NodeId,
    addr: String = "",
};
pub const LogEntryBuffer = zen.LogEntryBuffer;
pub const HardState = zen.HardStateStruct;

/// Field-set update for HardState. Callers pass only the fields they want
/// to change; the WAL reads the current HardState, merges, and writes
/// atomically under its own write lock — eliminating the read-modify-write
/// race that would exist if multiple writers (e.g. raft mainLoop bumping
/// term/vote vs. commitPersistLoop bumping commit_index) each constructed
/// a full HardState from a stale read.
pub const HardStateUpdate = struct {
    current_term: ?u64 = null,
    voted_for: ?u64 = null,
    raft_id: ?u64 = null,
    checkpoint_index: ?u64 = null,
    commit_index: ?u64 = null,
};

pub const Snapshot = zen.SnapshotStruct;
pub const ConfState = zen.ConfStateStruct;
pub const ConfStatePeer = zen.ConfStatePeerStruct;

pub const StoredEntry = struct {
    index: u64,
    term: u64,
    bytes: String = "",
};

pub const RaftLogError = error{
    Compacted,
    Unavailable,
    Canceled,
} || std.mem.Allocator.Error || zen.Error;

pub const EntriesRangeOpts = struct {
    lo: u64,
    hi: u64 = std.math.maxInt(u64),
    max_bytes: u64 = 64 * 1024 * 1024, // 64 MiB
    /// `.any` (tag 0) is the wildcard: an entry's wire-encoded type is
    /// never `.any`. Any other value returns only entries of that type,
    /// decided from the in-memory slot table, so the entries skipped
    /// are never read from disk.
    ty: EntryType = .any,
};

/// Result of an entries range query. All entry bytes live in the arena.
/// Caller must call deinit() when done.
pub const EntriesResult = struct {
    entries: []StoredEntry,
    arena: std.heap.ArenaAllocator,
    /// True if all matching entries in the range were returned (not
    /// truncated by max_bytes).
    eof: bool = true,

    pub fn deinit(self: *EntriesResult) void {
        self.arena.deinit();
    }
};

pub fn deinitOwnedEntries(
    alloc: std.mem.Allocator,
    entries: []LogEntryBuffer,
) void {
    for (entries) |*entry_buf| {
        entry_buf.deinit();
    }
    alloc.free(entries);
}

/// The index ranges that lock-free reads still need on disk, and the
/// cuts of the log waiting for them to finish. Its own mutex rather
/// than the wal lock, since a read unpins without holding that.
const Pins = struct {
    list: std.ArrayList(Pin) = .empty,
    mu: fiber.Mutex(.untracked) = .{},
    /// Broadcast on every unpin: a cut parked on an overlapping pin
    /// wakes and re-checks. A read is disk round trips, milliseconds,
    /// so the waiter parks rather than yields.
    cv: fiber.Condition = .{},

    /// One read's range.
    pub const Pin = struct {
        lo: u64,
        hi: u64,

        fn overlaps(a: Pin, b: Pin) bool {
            return a.lo < b.hi and b.lo < a.hi;
        }
    };

    fn deinit(self: *Pins, alloc: std.mem.Allocator) void {
        self.list.deinit(alloc);
    }

    /// Registers a read's range: no cut of the log may take it away
    /// until `unpin`.
    fn pin(self: *Pins, alloc: std.mem.Allocator, p: Pin) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        try self.list.append(alloc, p);
    }

    /// Ends a read: the range may be cut again.
    pub fn unpin(self: *Pins, p: Pin) void {
        {
            self.mu.lock(@src());
            defer self.mu.unlock();
            // Every pin is registered once and unpinned once, so the
            // first equal entry is this read's.
            const at = for (self.list.items, 0..) |q, i| {
                if (std.meta.eql(q, p)) break i;
            } else unreachable;
            _ = self.list.swapRemove(at);
        }
        self.cv.broadcast();
    }

    /// Parks the caller until no in-flight read overlaps [lo, hi). Called
    /// with the wal held exclusively, so no new read can start meanwhile;
    /// only the reads already planned have to drain.
    fn waitFor(self: *Pins, lo: u64, hi: u64) void {
        const cut: Pin = .{ .lo = lo, .hi = hi };
        self.mu.lock(@src());
        defer self.mu.unlock();
        while (self.anyOverlaps(cut)) self.cv.wait(&self.mu);
    }

    /// Caller holds `mu`.
    fn anyOverlaps(self: *const Pins, cut: Pin) bool {
        for (self.list.items) |p| if (Pin.overlaps(p, cut)) return true;
        return false;
    }
};

/// Inner WAL implementation. NOT internally synchronized — assumes the
/// caller (`DiskWAL`) holds `DiskWAL.mu` in the appropriate mode (shared
/// for reads, exclusive for mutations). Exposing `wal` directly without
/// going through `DiskWAL` is unsafe under concurrency.
pub const wal = struct {
    allocator: std.mem.Allocator,
    vfs: *const _vfs.VFS,
    dir_path: []u8,
    files: std.ArrayList(LogFile), // all files; last is current
    current: *LogFile, // points to files.items[files.items.len - 1]
    cache: EntryCache,
    opts: OpenOptions,
    /// The log file after `current`, prepared ahead of rotation.
    next: NextLog,
    /// The batch write path; see `LogBuf`.
    buf: LogBuf,
    /// Range reads in flight with no wal lock held (see `planEntries`).
    pins: Pins = .{},
    pub const Pin = Pins.Pin;

    const Pos = struct {
        file_idx: usize,
        /// null = raft_index falls before the file's range (compacted);
        /// no_slot = past the file's last data slot; otherwise
        /// the in-file slot offset.
        slot_idx: ?usize,
    };

    pub fn parseWalFid(name: String) ?u64 {
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

    fn deleteFileByFid(self: *wal, fid: u64) void {
        var name_buf: LogFile.NameBuf = undefined;
        const name = LogFile.fname(fid, &name_buf) catch return;
        _lf.deleteFileByName(self.allocator, self.dir_path, name);
    }

    fn deleteFidFiles(alloc: std.mem.Allocator, dir_path: String, fids: []const u64) void {
        for (fids) |fid| {
            var name_buf: LogFile.NameBuf = undefined;
            const name = LogFile.fname(fid, &name_buf) catch continue;
            _lf.deleteFileByName(alloc, dir_path, name);
        }
    }

    pub fn openPath(
        path: String,
        alloc: std.mem.Allocator,
        opts: OpenOptions,
    ) !wal {
        // The header takes the first MiB; a log file needs room past it.
        std.debug.assert(opts.log_file_size >= log_file_offset + (1 << 20));
        const vfs = opts.vfs orelse _vfs.g();
        const io = utils.context.g().io;
        const cwd = std.Io.Dir.cwd();
        cwd.access(io, path, .{}) catch |err| switch (err) {
            error.FileNotFound => try cwd.createDirPath(io, path),
            else => return err,
        };
        // Settle `.auto` once, here: every log file created under this
        // wal then sees a plain on/off.
        var settled = opts;
        settled.prezero = opts.prezero.resolve(path);
        if (opts.prezero == .auto and !builtin.is_test) {
            log.info("{s}: prezero {s} (fs magic 0x{x})", .{
                path, @tagName(settled.prezero), _vfs.fsMagic(path) orelse 0,
            });
        }
        const dir_copy = try alloc.dupe(u8, path);
        errdefer alloc.free(dir_copy);

        var dir = try cwd.openDir(io, path, .{ .iterate = true });
        defer dir.close(io);

        // Discover all log file ids, sorted ascending.
        var found_fids = std.ArrayList(u64).empty;
        defer found_fids.deinit(alloc);

        var spare_fids = std.ArrayList(u64).empty;
        defer spare_fids.deinit(alloc);

        var it = dir.iterate();
        while (try it.next(io)) |entry_dir| {
            if (entry_dir.kind != .file) continue;

            const name = entry_dir.name;
            if (std.mem.endsWith(u8, name, tmp_suffix)) {
                // A spare the last run left: the file it had prepared,
                // or one it kept for reuse.
                const base = name[0 .. name.len - tmp_suffix.len];
                const fid = parseWalFid(base) orelse continue;
                try spare_fids.append(alloc, fid);
                continue;
            }

            const fid = parseWalFid(name) orelse continue;
            try found_fids.append(alloc, fid);
        }
        // Its fids start below, once the highest log file is known
        // (`bumpFid`).
        var next_log = NextLog{
            .allocator = alloc,
            .vfs = vfs,
            .dir_path = dir_copy,
            .opts = settled,
            .next_fid = 0,
        };
        errdefer next_log.deinit();

        try next_log.adoptByIds(spare_fids.items);
        std.mem.sort(u64, found_fids.items, {}, comptime std.sort.asc(u64));

        // No files: create the first one.
        if (found_fids.items.len == 0) {
            var files = std.ArrayList(LogFile).empty;
            errdefer {
                for (files.items) |*lf| lf.deinit();
                files.deinit(alloc);
            }

            next_log.bumpFid(0);
            try files.append(alloc, try LogFile.createCurrent(
                vfs,
                alloc,
                path,
                next_log.takeFid(),
                settled,
            ));

            var entry_cache = try EntryCache.init(
                alloc,
                opts.cache_len,
                opts.cache_bytes,
            );
            errdefer entry_cache.deinit();

            return .{
                .allocator = alloc,
                .vfs = vfs,
                .dir_path = dir_copy,
                .files = files,
                .current = &files.items[files.items.len - 1],
                .cache = entry_cache,
                .opts = settled,
                .next = next_log,
                .buf = LogBuf.init(alloc),
            };
        }

        // Open all files except the last as rotated (read slots from header).
        // The last (highest fid) is the current file (indexed from its records).
        var opened = std.ArrayList(LogFile).empty;
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
            // A missing or torn header is rebuilt from the records
            // inside openRotated; only a file whose records are
            // corrupt drops the tail.
            var lf = LogFile.openRotated(
                vfs,
                alloc,
                path,
                fid,
            ) catch |err| switch (err) {
                error.CorruptLogFile => {
                    first_bad = idx;
                    break;
                },
                else => return err,
            };
            // No record on disk even after a scan. The wal had moved
            // on to the next file while this one's records were still
            // buffered in memory, and the process stopped before the
            // writer wrote them. It leaves the log, as a spare or
            // deleted: `slotGe` needs every file but the last to hold
            // a record. Any gap this leaves in the log is handled by
            // `verifyChain`.
            if (lf.slots.items.len == 0) {
                next_log.adoptLogFile(&lf);
                lf.deinit();
                continue;
            }
            try opened.append(alloc, lf);
        }

        // If any rotated file was malformed, drop it and every later fid
        // (including the would-be current). The current file gets created
        // fresh after the good rotated files; otherwise we open the
        // existing highest fid as current.
        if (first_bad) |bad_idx| {
            if (!builtin.is_test) {
                log.err(
                    "dropping malformed trailing WAL files from fid={d} in {s}",
                    .{ found_fids.items[bad_idx], path },
                );
            }
            deleteFidFiles(alloc, path, found_fids.items[bad_idx..]);
        }

        const cur_lf = if (first_bad != null) blk: {
            const last_fid: u64 = if (opened.items.len > 0)
                opened.items[opened.items.len - 1].fid
            else
                0;
            next_log.bumpFid(last_fid);

            break :blk try LogFile.createCurrent(
                vfs,
                alloc,
                path,
                next_log.takeFid(),
                settled,
            );
        } else try LogFile.openCurrent(
            vfs,
            alloc,
            path,
            found_fids.items[found_fids.items.len - 1],
            settled,
        );

        try opened.append(alloc, cur_lf);

        var entry_cache = try EntryCache.init(
            alloc,
            opts.cache_len,
            opts.cache_bytes,
        );
        errdefer entry_cache.deinit();

        next_log.bumpFid(cur_lf.fid);
        return .{
            .allocator = alloc,
            .vfs = vfs,
            .dir_path = dir_copy,
            .files = opened,
            .current = &opened.items[opened.items.len - 1],
            .cache = entry_cache,
            .opts = settled,
            .next = next_log,
            .buf = LogBuf.init(alloc),
        };
    }

    pub fn deinit(self: *wal) void {
        // The writer stops without draining, and it stops first: no
        // lock is held here, so a flush beside a running writer would
        // race it for the same bytes. What it has not written yet goes
        // down here, a rotated file's leftovers included.
        self.buf.stop();
        self.syncLocked() catch |err| {
            log.err("flush at close: {s}", .{@errorName(err)});
        };
        self.next.deinit();
        self.cache.deinit();
        for (self.files.items) |*lf| lf.deinit();
        self.files.deinit(self.allocator);
        self.buf.deinit();
        self.pins.deinit(self.allocator);
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

    fn getEntryFile(self: *wal, file_idx: usize) ?*LogFile {
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

    fn seekEntry(
        self: *wal,
        raft_index: u64,
    ) (Error || RaftLogError || err_not_found)!entry {
        if (raft_index == 0) {
            return .{
                .term = 0,
                .index = 0,
                .data_offset = 0,
                .data_len = 0,
                .entry_type = .any,
            };
        }

        const pos = self.slotGe(raft_index);
        const slot_idx = pos.slot_idx orelse return RaftLogError.Compacted;
        if (slot_idx >= no_slot) return RaftLogError.Unavailable;

        const lf = self.getEntryFile(pos.file_idx) orelse
            return RaftLogError.Unavailable;
        const ent = lf.getEntry(slot_idx) orelse
            return RaftLogError.Unavailable;
        if (ent.index != raft_index) return err_not_found.NotFound;
        return ent;
    }

    pub fn term(
        self: *wal,
        idx: u64,
    ) (Error || RaftLogError || err_not_found)!u64 {
        const ent = try self.seekEntry(idx);
        return ent.term;
    }

    /// The disk reads of one file, issued after the wal lock is dropped.
    /// A range read between its two halves: planned under the lock,
    /// read without it. Each request names its file (the file list may
    /// grow meanwhile, and the pin keeps the file from being deleted or
    /// truncated under the read) and fills `sentries[sentry_offs[i]]`.
    pub const PlannedRead = struct {
        arena: std.heap.ArenaAllocator,
        sentries: std.ArrayList(StoredEntry) = .empty,
        reqs: std.ArrayList(_vfs.FileHandle.ReadRequest) = .empty,
        sentry_offs: std.ArrayList(usize) = .empty,
        eof: bool = true,
        pin: Pin,
    };

    /// The first half of a range read, under the caller's lock: walks
    /// the in-memory slot table for the entries `opts` asks for, copies
    /// the ones the cache holds, and lists the disk reads. No
    /// I/O happens here, so the lock is held for microseconds and an
    /// append never waits on a reader. The pin registered here keeps
    /// `deleteBefore` and `truncateFrom` off the range until `unpin`.
    pub fn planEntries(
        self: *wal,
        parent_alloc: std.mem.Allocator,
        opts: EntriesRangeOpts,
    ) !PlannedRead {
        var planned: PlannedRead = .{
            .arena = std.heap.ArenaAllocator.init(parent_alloc),
            .pin = .{ .lo = opts.lo, .hi = opts.hi },
        };
        errdefer planned.arena.deinit();
        const arloc = planned.arena.allocator();

        var total: u64 = 0;
        const pos = self.slotGe(opts.lo);
        // null = opts.lo lands before this file's range: start at slot 0.
        var file_idx = pos.file_idx;
        var slot_idx: usize = pos.slot_idx orelse 0;
        outer: while (file_idx < self.files.items.len) : ({
            file_idx += 1;
            slot_idx = 0;
        }) {
            const lf = &self.files.items[file_idx];
            while (slot_idx < lf.firstEmptySlot()) : (slot_idx += 1) {
                const ent = lf.getEntry(slot_idx) orelse break;
                if (ent.index >= opts.hi) break :outer;

                // Skip all entries which don't match the type.
                if (opts.ty != .any and ent.entry_type != opts.ty) continue;

                // The budget never refuses the first entry, so a reader
                // always makes progress.
                if (planned.sentries.items.len > 0 and opts.max_bytes > 0 and
                    total + ent.data_len > opts.max_bytes)
                {
                    planned.eof = false;
                    break :outer;
                }
                total += ent.data_len;

                var stored: StoredEntry = .{
                    .index = ent.index,
                    .term = ent.term,
                };
                if (self.cache.lookup(ent.index)) |ce| {
                    stored.bytes = try arloc.dupe(u8, ce.bytes);
                } else {
                    try planned.reqs.append(arloc, .{
                        .file = lf.handle.file.?,
                        .offset = ent.data_offset,
                        .len = zbuf.Frame.hdr_size + ent.data_len,
                    });
                    const off = planned.sentries.items.len;
                    try planned.sentry_offs.append(arloc, off);
                }
                try planned.sentries.append(arloc, stored);
            }
        }

        try self.pins.pin(self.allocator, planned.pin);
        return planned;
    }

    /// The second half: every disk read in one batch, whatever files
    /// they span, then the frames are decoded in place. A frame whose
    /// hash does not verify (an entry not yet on disk and no longer
    /// cached) fails the read rather than returning bad bytes. No wal
    /// lock is held; the caller unpins after.
    pub fn readPlanned(self: *const wal, planned: *PlannedRead) !EntriesResult {
        // Every segment is in the WAL directory: one device, charged
        // through the current file's limiter.
        try self.vfs.batchRead(
            self.current.handle.bw,
            &planned.arena,
            planned.reqs.items,
        );

        for (planned.reqs.items, planned.sentry_offs.items) |req, off| {
            planned.sentries.items[off].bytes = try zbuf.Frame.read(req.data);
        }
        const alloc = planned.arena.allocator();
        return .{
            .entries = try planned.sentries.toOwnedSlice(alloc),
            .arena = planned.arena,
            .eof = planned.eof,
        };
    }

    /// Queue the current file's last records and header for the writer
    /// and swap the prepared next one in. The outgoing file's cut and
    /// fsync happen on the writer with no lock held; until they land
    /// the file opens from a scan of its records (`openScanned`).
    fn rotate(self: *wal) !void {
        try self.buf.pushToPending(self.current);
        self.current.is_current = false;

        const nf = try self.next.swapIn();
        try self.files.append(self.allocator, nf);
        self.current = &self.files.items[self.files.items.len - 1];
    }

    pub fn addEntries(self: *wal, entries: []const LogEntryBuffer) !void {
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
        if (first_index != expected_first) return RaftLogError.Invalid;

        // Append into self.write_buf — the actual pwrite is deferred to
        // sync(). Cache + slot table get updated immediately so reads see
        // the new entries before they hit disk.
        var expected_index = first_index;
        for (entries) |entry_buf| {
            const reader = try entry_buf.initReader();
            if (reader.index() != expected_index) return RaftLogError.Invalid;
            expected_index += 1;

            const bytes = entry_buf.bytes orelse return RaftLogError.Invalid;
            // Rotate when the log file is full, or as soon as the prepared
            // file is ready if the current one was handed out unfilled
            // (every append into it costs a journal write). Rotation
            // flushes self.write_buf to the outgoing file first.
            const next_ready = !self.current.filled and self.next.isReady();
            if (self.current.firstEmptySlot() >= max_num_entries or
                self.current.write_offset + zbuf.Frame.hdr_size + bytes.len >
                    self.opts.log_file_size or next_ready)
            {
                try self.rotate();
            }

            // Buffer the framed record (no I/O), update in-memory slots.
            try self.buf.append(self.current, bytes, reader);

            // If the cache is about to evict an entry that hasn't been
            // pwritten yet, flush write_buf first. Otherwise reads of the
            // evicted index would miss in cache AND on disk.
            if (self.cache.wouldEvict(bytes.len)) {
                if (self.cache.oldestIndex()) |oldest| {
                    if (oldest > self.buf.syncedIndex()) {
                        try self.syncLocked();
                    }
                }
            }
            // Populate cache so reads see the entry pre-sync.
            self.cache.append(reader.index(), reader.term(), bytes) catch {};
        }
        // One wake for the whole batch: the writer takes it as one write.
        self.buf.advance(expected_index - 1);
        // The next log file is created and zero-filled by its own fiber
        // from the moment the current one is in use (a no-op while one
        // is in flight), so the fill has the current file's whole life
        // to finish and rotation finds it ready.
        self.next.prepare();
    }

    pub fn deleteBefore(self: *wal, raft_index: u64) void {
        // A rotated file's last records and header still queued for
        // the writer must land before that file can be closed under it.
        self.buf.drain() catch |err| {
            log.err("drain before delete: {s}", .{
                @errorName(err),
            });
            return;
        };
        self.pins.waitFor(0, raft_index);

        // At every trim, also one that drops nothing: this is what
        // empties the pool of a wal that stopped rotating.
        self.next.startTrim();

        const pos = self.slotGe(raft_index);
        // null = raft_index falls before everything we have on disk —
        // nothing to drop.
        if (pos.slot_idx == null) return;

        // A dropped file is kept for reuse while the pool takes it,
        // and unlinked otherwise.
        for (self.files.items[0..pos.file_idx]) |*lf| {
            self.next.adoptLogFile(lf);
            lf.deinit();
        }
        self.files.replaceRangeAssumeCapacity(0, pos.file_idx, &.{});
        self.current = &self.files.items[self.files.items.len - 1];
        self.cache.evictBefore(raft_index);
    }

    pub fn reset(self: *wal) void {
        self.buf.reset();
        self.next.discard();
        self.next.dropSpares();
        while (self.files.items.len > 0) {
            var ef = self.files.pop().?;
            self.deleteFileByFid(ef.fid);
            ef.deinit();
        }
        self.current.slots.shrinkRetainingCapacity(0);
        self.current.write_offset = log_file_offset;
        self.cache.clear();
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
                log.warn(
                    "WAL starts past start_index in {s} (start_index={d}, " ++
                        "firstIndex={d}); wiping WAL",
                    .{ self.dir_path, start_index, self.firstIndex() },
                );
                try self.truncateFrom(self.firstIndex());
            }
            return;
        };
        if (slot_idx >= no_slot) return;

        var fi = start.file_idx;
        var si: usize = slot_idx;
        var expected: u64 = start_index;
        while (fi < self.files.items.len) : (fi += 1) {
            const lf = &self.files.items[fi];
            while (si < lf.slots.items.len) : (si += 1) {
                const ent = lf.slots.items[si];
                if (ent.index != expected) {
                    log.warn(
                        "WAL gap in {s} (expected={d}, found={d}); " ++
                            "truncating from {d}",
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
        try self.syncLocked();
        self.pins.waitFor(from_index, std.math.maxInt(u64));

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
        // The synced index cannot exceed what's still in the log.
        if (from_index > 0 and self.buf.syncedIndex() >= from_index) {
            self.buf.setSynced(from_index - 1);
        }
    }

    /// Make everything appended so far durable, on the caller's fiber,
    /// with the lock held: what rotation, truncation and cache eviction
    /// need before they touch the log files.
    pub fn syncLocked(self: *wal) !void {
        return self.buf.flush(self.current, self.lastIndex());
    }
};

pub const DiskWAL = struct {
    allocator: std.mem.Allocator,
    /// MetaStore owns the in-memory authoritative HardState + Snapshot, so
    /// the old `hard_state_cache` + `snapshot_cache` pointers go away.
    meta: _meta.MetaStore,
    wal_store: wal,
    /// Single rwlock guarding all DiskWAL state — covers both `meta` and
    /// `wal_store` since the public API (hardState/snapshot/append/...)
    /// can read/mutate either. Shared for reads, exclusive for writes.
    mu: Lock = .{},
    /// Called by the writer fiber after each batch, with the lock
    /// released: the raft node's timeline mark and wake-up.
    on_written: ?WrittenHook = null,

    /// The lock's type, for `Lock.Exclusive` tokens that prove a
    /// callee runs under it.
    pub const Lock = fiber.RWMutex(.tracked);

    pub const WrittenHook = struct {
        ctx: *anyopaque,
        call: *const fn (*anyopaque, Written) void,
    };

    pub fn openPath(
        path: String,
        allocator: std.mem.Allocator,
        opts: OpenOptions,
    ) !DiskWAL {
        var ms = try _meta.MetaStore.openPath(
            opts.vfs orelse _vfs.g(),
            path,
            allocator,
        );
        errdefer ms.deinit();

        var wf = try wal.openPath(path, allocator, opts);
        errdefer wf.deinit();

        // Verify the WAL forms an unbroken chain from snap.index+1
        // forward. Pre-snapshot entries on disk are fine — apply_index
        // jumps over them. A gap is data loss past the snapshot; the
        // verify routine truncates the stranded tail and lets raft
        // re-replicate from the leader.
        try wf.verifyChain(ms.snapshotState().index + 1);

        // Recovery: every entry the wal just discovered is on disk and durable.
        wf.buf.setSynced(wf.lastIndex());

        return .{
            .allocator = allocator,
            .meta = ms,
            .wal_store = wf,
        };
    }

    pub fn deinit(self: *DiskWAL) void {
        self.wal_store.deinit();
        self.meta.deinit();
        // `mu` is .tracked — unlink it from the lock registry before the
        // struct's memory dies, else a later dumpLocks/unregister walk dangles.
        self.mu.deinit();
    }

    fn mapToRaftLogError(err: anyerror) RaftLogError {
        return switch (err) {
            RaftLogError.Compacted => RaftLogError.Compacted,
            RaftLogError.Unavailable => RaftLogError.Unavailable,
            err_not_found.NotFound => RaftLogError.Unavailable,
            error.OutOfMemory => error.OutOfMemory,
            error.Closed => RaftLogError.Unavailable,
            else => {
                log.err("mapToRaftLogError: unmapped error: {}", .{err});
                return RaftLogError.Unavailable;
            },
        };
    }

    // --- Public API: each method takes the appropriate lock inline. Mode
    // matches the prior vtable wrapping (shared for reads, exclusive for
    // writes, _uncancelable for cheap struct-field reads where blocking
    // on cancellation isn't useful).

    pub fn hardState(self: *DiskWAL) *const HardState {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.meta.hardState();
    }

    /// Atomic field-set update. The merge of `update` over the current
    /// HardState happens under `self.mu`, which also serializes with
    /// every other DiskWAL writer, so concurrent calls (e.g. raft
    /// mainLoop persisting term/vote, commitPersistLoop persisting
    /// commit_index) can't race-clobber each other's fields.
    pub fn setHardState(self: *DiskWAL, update: HardStateUpdate) RaftLogError!void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        var next = self.meta.hardState().*;
        if (update.current_term) |v| next.current_term = v;
        if (update.voted_for) |v| next.voted_for = v;
        if (update.raft_id) |v| next.raft_id = v;
        if (update.checkpoint_index) |v| next.checkpoint_index = v;
        if (update.commit_index) |v| next.commit_index = v;
        self.meta.setHardState(next) catch |err| return mapToRaftLogError(err);
    }

    pub fn snapshot(self: *DiskWAL) *const Snapshot {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.meta.snapshotState();
    }

    pub fn setSnapshot(self: *DiskWAL, snap: Snapshot) RaftLogError!void {
        self.mu.lock(@src());
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
        self.mu.lockShared(@src());
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
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.wal_store.firstIndex();
    }

    pub fn lastIndex(self: *DiskWAL) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.wal_store.lastIndex();
    }

    pub fn lastTimestamp(self: *DiskWAL) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.wal_store.lastTimestamp();
    }

    pub fn lastTerm(self: *DiskWAL) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        const last_idx = self.wal_store.lastIndex();
        const snap_term = self.meta.snapshotState().term;
        if (last_idx == 0) return snap_term;
        return self.wal_store.term(last_idx) catch snap_term;
    }

    pub fn term(self: *DiskWAL, idx: u64) RaftLogError!?u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        if (idx == 0) return 0;
        return self.wal_store.term(idx) catch |err| switch (err) {
            RaftLogError.Compacted => null,
            RaftLogError.Unavailable => null,
            err_not_found.NotFound => null,
            else => return mapToRaftLogError(err),
        };
    }

    /// The slot of the entry at `idx`, whatever its type: its term,
    /// timestamp, type and size, with no payload read. Null when the
    /// log no longer (or does not yet) hold that index.
    pub fn slotAt(self: *DiskWAL, idx: u64) RaftLogError!?entry {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();

        if (idx == 0) return null;
        return self.wal_store.seekEntry(idx) catch |err| switch (err) {
            RaftLogError.Compacted => null,
            RaftLogError.Unavailable => null,
            err_not_found.NotFound => null,
            else => return mapToRaftLogError(err),
        };
    }

    /// The lock covers the slot walk only; the disk reads run without
    /// it, so an append never waits on a reader (a snapshot's read of
    /// the manifest entries once held it for seconds).
    pub fn entriesRange(
        self: *DiskWAL,
        opts: EntriesRangeOpts,
    ) RaftLogError!EntriesResult {
        var planned = try self.planEntries(opts);
        errdefer planned.arena.deinit();

        defer self.wal_store.pins.unpin(planned.pin);
        return self.wal_store.readPlanned(&planned) catch |err|
            return mapToRaftLogError(err);
    }

    fn planEntries(
        self: *DiskWAL,
        opts: EntriesRangeOpts,
    ) RaftLogError!wal.PlannedRead {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.wal_store.planEntries(self.allocator, opts) catch |err|
            return mapToRaftLogError(err);
    }

    pub fn firstIndexOfTerm(self: *DiskWAL, term_value: u64) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        var idx = self.wal_store.firstIndex();
        const last = self.wal_store.lastIndex();
        while (idx <= last) : (idx += 1) {
            const t = self.wal_store.term(idx) catch continue;
            if (t == term_value) return idx;
        }
        return 0;
    }

    pub fn addEntries(
        self: *DiskWAL,
        entries: []const LogEntryBuffer,
    ) RaftLogError!void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        self.wal_store.addEntries(entries) catch |err| return mapToRaftLogError(err);
        self.startWriter();
    }

    /// The writer fiber starts on the first append rather than in
    /// `openPath`, which returns the DiskWAL by value: the fiber keeps a
    /// pointer to it, so it has to be at its final address first. Lock
    /// held; a no-op once started.
    fn startWriter(self: *DiskWAL) void {
        self.wal_store.buf.start(self) catch |err| {
            std.debug.panic("raft/diskwal: cannot start the writer fiber: {s}", .{
                @errorName(err),
            });
        };
    }

    pub fn truncateFrom(self: *DiskWAL, from_index: u64) RaftLogError!void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        self.wal_store.truncateFrom(from_index) catch |err| return mapToRaftLogError(err);
    }

    pub fn compactUntil(self: *DiskWAL, index: u64) RaftLogError!void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        self.wal_store.deleteBefore(index);
    }

    /// Park until everything appended so far is on disk. Every append
    /// already woke the writer fiber, so this only waits, with no lock
    /// held. Returns early if the log is cut below the target meanwhile
    /// (a follower truncating a conflicting suffix): what was waited for
    /// is then gone, or is being re-appended and gets waited for anew.
    pub fn sync(self: *DiskWAL) RaftLogError!void {
        // MetaStore writes are durable inline (state.bin opens with O_DSYNC,
        // snapshot.bin uses tmp+rename+dir-sync), so only the WAL needs an
        // explicit sync here.
        while (true) {
            const index = self.lastIndex();
            self.wal_store.buf.waitSynced(index) catch |err| switch (err) {
                error.Reset => {
                    if (self.lastIndex() < index) return;
                    continue;
                },
                else => return mapToRaftLogError(err),
            };
            return;
        }
    }

    /// What one batch write covered: through which index, and when the
    /// write started and returned (monotonic, us). For the per-commit
    /// timeline.
    pub const Written = struct {
        first_index: u64,
        last_index: u64,
        started_us: i64,
        done_us: i64,
    };

    /// Highest index durable on disk. No lock: an atomic the sync
    /// fiber publishes.
    pub fn syncedIndex(self: *DiskWAL) u64 {
        return self.wal_store.buf.syncedIndex();
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
    /// Bytes the live entries hold, and the most they may: past it
    /// the oldest go, so a cache of 16 MiB rows is bounded too.
    bytes: usize,
    max_bytes: usize,
    hits: u64,
    misses: u64,

    pub const CachedEntry = struct {
        index: u64,
        term: u64,
        bytes: String, // allocator-owned
    };

    pub fn init(
        allocator: std.mem.Allocator,
        max_len: usize,
        max_bytes: usize,
    ) !EntryCache {
        const buf = try allocator.alloc(CachedEntry, max_len);
        return .{
            .allocator = allocator,
            .buf = buf,
            .head = 0,
            .len = 0,
            .bytes = 0,
            .max_bytes = max_bytes,
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
        self.bytes = 0;
    }

    /// Raft index of the oldest entry — the one a full-cache `append`
    /// would overwrite. `null` if cache is empty or disabled.
    pub fn oldestIndex(self: *const EntryCache) ?u64 {
        if (self.buf.len == 0 or self.len == 0) return null;
        return self.buf[self.head].index;
    }

    /// Whether an `append` of `n` bytes evicts the oldest entry: the
    /// ring is full, or the bytes held would pass `max_bytes`.
    pub fn wouldEvict(self: *const EntryCache, n: usize) bool {
        if (self.buf.len == 0 or self.len == 0) return false;
        return self.len == self.buf.len or self.bytes + n > self.max_bytes;
    }

    pub fn append(self: *EntryCache, index: u64, term_val: u64, bytes: String) !void {
        if (self.buf.len == 0) return;

        const owned = try self.allocator.dupe(u8, bytes);
        // The oldest go until the entry fits. One past the cap on its
        // own is kept: a reader must find every unsynced entry here.
        while (self.wouldEvict(owned.len)) self.dropOldest();
        const tail = (self.head + self.len) % self.buf.len;
        self.buf[tail] = .{ .index = index, .term = term_val, .bytes = owned };
        self.len += 1;
        self.bytes += owned.len;
    }

    fn dropOldest(self: *EntryCache) void {
        const oldest = &self.buf[self.head];
        self.bytes -= oldest.bytes.len;
        self.allocator.free(oldest.bytes);
        self.head = (self.head + 1) % self.buf.len;
        self.len -= 1;
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
            const dropped = self.buf[(self.head + i) % self.buf.len].bytes;
            self.bytes -= dropped.len;
            self.allocator.free(dropped);
        }
        self.len = keep;
    }

    /// Drop all entries with index < before_index.
    pub fn evictBefore(self: *EntryCache, before_index: u64) void {
        if (self.len == 0) return;
        const base = self.buf[self.head].index;
        if (before_index <= base) return;
        const drop: usize = @intCast(@min(before_index - base, self.len));
        for (0..drop) |_| self.dropOldest();
    }
};
