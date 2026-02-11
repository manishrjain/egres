const std = @import("std");
const msg = @import("message.zig");

pub const NodeId = msg.NodeId;
pub const LogEntry = msg.LogEntry;

pub const HardState = struct {
    current_term: u64,
    voted_for: ?NodeId,
};

pub const Snapshot = struct {
    index: u64,
    term: u64,
    data: []const u8,
};

pub const RaftLogError = error{
    Compacted,
    Unavailable,
    OutOfMemory,
};

pub const RaftLog = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        hardState: *const fn (*anyopaque) HardState,
        setHardState: *const fn (*anyopaque, HardState) RaftLogError!void,
        snapshot: *const fn (*anyopaque) Snapshot,
        setSnapshot: *const fn (*anyopaque, Snapshot) RaftLogError!void,
        firstIndex: *const fn (*anyopaque) u64,
        lastIndex: *const fn (*anyopaque) u64,
        lastTerm: *const fn (*anyopaque) u64,
        term: *const fn (*anyopaque, u64) RaftLogError!u64,
        entry: *const fn (*anyopaque, u64) ?LogEntry,
        entriesFrom: *const fn (*anyopaque, u64, u32) []const LogEntry,
        entriesRange: *const fn (*anyopaque, u64, u64, u64) []const LogEntry,
        firstIndexOfTerm: *const fn (*anyopaque, u64) u64,
        lastIndexOfTerm: *const fn (*anyopaque, u64) u64,
        append: *const fn (*anyopaque, []const LogEntry) RaftLogError!void,
        truncateFrom: *const fn (*anyopaque, u64) void,
        compactUntil: *const fn (*anyopaque, u64) void,
        sync: *const fn (*anyopaque) RaftLogError!void,
    };

    pub fn hardState(self: RaftLog) HardState {
        return self.vtable.hardState(self.ctx);
    }

    pub fn setHardState(self: RaftLog, hs: HardState) RaftLogError!void {
        return self.vtable.setHardState(self.ctx, hs);
    }

    pub fn snapshot(self: RaftLog) Snapshot {
        return self.vtable.snapshot(self.ctx);
    }

    pub fn setSnapshot(self: RaftLog, snap: Snapshot) RaftLogError!void {
        return self.vtable.setSnapshot(self.ctx, snap);
    }

    pub fn firstIndex(self: RaftLog) u64 {
        return self.vtable.firstIndex(self.ctx);
    }

    pub fn lastIndex(self: RaftLog) u64 {
        return self.vtable.lastIndex(self.ctx);
    }

    pub fn lastTerm(self: RaftLog) u64 {
        return self.vtable.lastTerm(self.ctx);
    }

    pub fn term(self: RaftLog, index: u64) RaftLogError!u64 {
        return self.vtable.term(self.ctx, index);
    }

    pub fn entry(self: RaftLog, index: u64) ?LogEntry {
        return self.vtable.entry(self.ctx, index);
    }

    pub fn entriesFrom(self: RaftLog, start_index: u64, max_count: u32) []const LogEntry {
        return self.vtable.entriesFrom(self.ctx, start_index, max_count);
    }

    pub fn entriesRange(self: RaftLog, lo: u64, hi: u64, max_bytes: u64) []const LogEntry {
        return self.vtable.entriesRange(self.ctx, lo, hi, max_bytes);
    }

    pub fn firstIndexOfTerm(self: RaftLog, term_value: u64) u64 {
        return self.vtable.firstIndexOfTerm(self.ctx, term_value);
    }

    pub fn lastIndexOfTerm(self: RaftLog, term_value: u64) u64 {
        return self.vtable.lastIndexOfTerm(self.ctx, term_value);
    }

    pub fn append(self: RaftLog, entries: []const LogEntry) RaftLogError!void {
        return self.vtable.append(self.ctx, entries);
    }

    pub fn truncateFrom(self: RaftLog, from_index: u64) void {
        return self.vtable.truncateFrom(self.ctx, from_index);
    }

    pub fn compactUntil(self: RaftLog, index: u64) void {
        return self.vtable.compactUntil(self.ctx, index);
    }

    pub fn sync(self: RaftLog) RaftLogError!void {
        return self.vtable.sync(self.ctx);
    }
};

pub const MemRaftLog = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(LogEntry),
    hard_state: HardState,
    snapshot_info: Snapshot,

    pub fn init(allocator: std.mem.Allocator) MemRaftLog {
        return .{
            .allocator = allocator,
            .entries = std.ArrayList(LogEntry).empty,
            .hard_state = .{ .current_term = 0, .voted_for = null },
            .snapshot_info = .{ .index = 0, .term = 0, .data = &[_]u8{} },
        };
    }

    pub fn deinit(self: *MemRaftLog) void {
        self.clearEntries();
        if (self.snapshot_info.data.len != 0) {
            self.allocator.free(self.snapshot_info.data);
        }
        self.entries.deinit(self.allocator);
    }

    pub fn asRaftLog(self: *MemRaftLog) RaftLog {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn fromCtx(ctx: *anyopaque) *MemRaftLog {
        return @ptrCast(@alignCast(ctx));
    }

    fn hardState(ctx: *anyopaque) HardState {
        const self = fromCtx(ctx);
        return self.hard_state;
    }

    fn setHardState(ctx: *anyopaque, hs: HardState) RaftLogError!void {
        const self = fromCtx(ctx);
        self.hard_state = hs;
    }

    fn snapshot(ctx: *anyopaque) Snapshot {
        const self = fromCtx(ctx);
        return self.snapshot_info;
    }

    fn setSnapshot(ctx: *anyopaque, snap: Snapshot) RaftLogError!void {
        const self = fromCtx(ctx);
        if (self.snapshot_info.data.len != 0) {
            self.allocator.free(self.snapshot_info.data);
        }
        const data_copy = if (snap.data.len == 0) &[_]u8{} else try self.allocator.dupe(u8, snap.data);
        self.snapshot_info = .{ .index = snap.index, .term = snap.term, .data = data_copy };
    }

    fn firstIndex(ctx: *anyopaque) u64 {
        const self = fromCtx(ctx);
        if (self.snapshot_info.index != 0) return self.snapshot_info.index + 1;
        if (self.entries.items.len == 0) return 1;
        return self.entries.items[0].index;
    }

    fn lastIndex(ctx: *anyopaque) u64 {
        const self = fromCtx(ctx);
        if (self.entries.items.len == 0) return self.snapshot_info.index;
        return self.entries.items[self.entries.items.len - 1].index;
    }

    fn lastTerm(ctx: *anyopaque) u64 {
        const self = fromCtx(ctx);
        if (self.entries.items.len == 0) return self.snapshot_info.term;
        return self.entries.items[self.entries.items.len - 1].term;
    }

    fn term(ctx: *anyopaque, index: u64) RaftLogError!u64 {
        if (index == 0) return 0;
        const self = fromCtx(ctx);
        if (self.snapshot_info.index != 0 and index == self.snapshot_info.index) return self.snapshot_info.term;
        const log_entry = entry(ctx, index) orelse return RaftLogError.Unavailable;
        return log_entry.term;
    }

    fn entry(ctx: *anyopaque, index: u64) ?LogEntry {
        const self = fromCtx(ctx);
        if (index == 0) return null;
        if (self.entries.items.len == 0) return null;
        const base = self.entries.items[0].index;
        if (index < base) return null;
        const offset: usize = @intCast(index - base);
        if (offset >= self.entries.items.len) return null;
        return self.entries.items[offset];
    }

    fn entriesFrom(ctx: *anyopaque, start_index: u64, max_count: u32) []const LogEntry {
        const self = fromCtx(ctx);
        if (self.entries.items.len == 0) return &[_]LogEntry{};
        const base = self.entries.items[0].index;
        if (start_index < base) return &[_]LogEntry{};
        const offset: usize = @intCast(start_index - base);
        if (offset >= self.entries.items.len) return &[_]LogEntry{};
        const start = offset;
        const max: usize = @intCast(max_count);
        const end = if (start + max <= self.entries.items.len) start + max else self.entries.items.len;
        return self.entries.items[start..end];
    }

    fn entriesRange(ctx: *anyopaque, lo: u64, hi: u64, max_bytes: u64) []const LogEntry {
        const self = fromCtx(ctx);
        if (lo >= hi) return &[_]LogEntry{};
        if (self.entries.items.len == 0) return &[_]LogEntry{};
        const base = self.entries.items[0].index;
        if (hi <= base) return &[_]LogEntry{};
        const start_index = if (lo > base) lo else base;
        const start_offset: usize = @intCast(start_index - base);
        if (start_offset >= self.entries.items.len) return &[_]LogEntry{};

        var end_offset: usize = start_offset;
        var size: u64 = 0;
        while (end_offset < self.entries.items.len) : (end_offset += 1) {
            const log_entry = self.entries.items[end_offset];
            if (log_entry.index >= hi) break;
            size += @intCast(log_entry.data.len);
            if (max_bytes > 0 and end_offset > start_offset and size > max_bytes) break;
        }
        return self.entries.items[start_offset..end_offset];
    }

    fn firstIndexOfTerm(ctx: *anyopaque, term_value: u64) u64 {
        const self = fromCtx(ctx);
        for (self.entries.items) |log_entry| {
            if (log_entry.term == term_value) return log_entry.index;
        }
        return 0;
    }

    fn lastIndexOfTerm(ctx: *anyopaque, term_value: u64) u64 {
        const self = fromCtx(ctx);
        var i: usize = self.entries.items.len;
        while (i > 0) : (i -= 1) {
            const log_entry = self.entries.items[i - 1];
            if (log_entry.term == term_value) return log_entry.index;
        }
        return 0;
    }

    fn append(ctx: *anyopaque, new_entries: []const LogEntry) RaftLogError!void {
        const self = fromCtx(ctx);
        if (new_entries.len == 0) return;

        const first = new_entries[0].index;
        if (self.entries.items.len > 0) {
            const base = self.entries.items[0].index;
            const last = self.entries.items[self.entries.items.len - 1].index;
            if (first <= last and first >= base) {
                truncateFrom(ctx, first);
            } else if (first < base) {
                self.clearEntries();
            }
        }

        for (new_entries) |log_entry| {
            const data_copy = if (log_entry.data.len == 0) &[_]u8{} else try self.allocator.dupe(u8, log_entry.data);
            try self.entries.append(self.allocator, .{
                .index = log_entry.index,
                .term = log_entry.term,
                .data = data_copy,
                .ty = log_entry.ty,
                .conf_change = log_entry.conf_change,
            });
        }
    }

    fn truncateFrom(ctx: *anyopaque, from_index: u64) void {
        const self = fromCtx(ctx);
        if (self.entries.items.len == 0) return;
        const base = self.entries.items[0].index;
        if (from_index <= base) {
            self.clearEntries();
            return;
        }
        const offset: usize = @intCast(from_index - base);
        if (offset >= self.entries.items.len) return;
        for (self.entries.items[offset..]) |log_entry| {
            if (log_entry.data.len != 0) self.allocator.free(log_entry.data);
        }
        self.entries.shrinkRetainingCapacity(offset);
    }

    fn compactUntil(ctx: *anyopaque, index: u64) void {
        const self = fromCtx(ctx);
        if (self.entries.items.len == 0) return;
        const base = self.entries.items[0].index;
        if (index <= base) return;
        const offset: usize = @intCast(index - base);
        if (offset >= self.entries.items.len) {
            self.clearEntries();
            return;
        }
        for (self.entries.items[0..offset]) |log_entry| {
            if (log_entry.data.len != 0) self.allocator.free(log_entry.data);
        }
        self.entries.items = self.entries.items[offset..];
    }

    fn sync(ctx: *anyopaque) RaftLogError!void {
        _ = ctx;
    }

    fn clearEntries(self: *MemRaftLog) void {
        for (self.entries.items) |log_entry| {
            if (log_entry.data.len != 0) self.allocator.free(log_entry.data);
        }
        self.entries.shrinkRetainingCapacity(0);
    }

    const vtable = RaftLog.VTable{
        .hardState = hardState,
        .setHardState = setHardState,
        .snapshot = snapshot,
        .setSnapshot = setSnapshot,
        .firstIndex = firstIndex,
        .lastIndex = lastIndex,
        .lastTerm = lastTerm,
        .term = term,
        .entry = entry,
        .entriesFrom = entriesFrom,
        .entriesRange = entriesRange,
        .firstIndexOfTerm = firstIndexOfTerm,
        .lastIndexOfTerm = lastIndexOfTerm,
        .append = append,
        .truncateFrom = truncateFrom,
        .compactUntil = compactUntil,
        .sync = sync,
    };
};
