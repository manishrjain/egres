const std = @import("std");
const msg = @import("message.zig");

pub const LogEntry = msg.LogEntry;

pub const Log = struct {
    allocator: std.mem.Allocator,
    entries: std.ArrayList(LogEntry),

    pub fn init(allocator: std.mem.Allocator) Log {
        return .{ .allocator = allocator, .entries = std.ArrayList(LogEntry).empty };
    }

    pub fn deinit(self: *Log) void {
        self.entries.deinit(self.allocator);
    }

    pub fn lastIndex(self: *const Log) u64 {
        if (self.entries.items.len == 0) return 0;
        return self.entries.items[self.entries.items.len - 1].index;
    }

    pub fn lastTerm(self: *const Log) u64 {
        if (self.entries.items.len == 0) return 0;
        return self.entries.items[self.entries.items.len - 1].term;
    }

    pub fn getEntry(self: *const Log, index: u64) ?LogEntry {
        if (index == 0) return null;
        const base = self.entries.items[0].index;
        if (index < base) return null;
        const offset: usize = @intCast(index - base);
        if (offset >= self.entries.items.len) return null;
        return self.entries.items[offset];
    }

    pub fn append(self: *Log, entry: LogEntry) !void {
        try self.entries.append(self.allocator, entry);
    }

    pub fn appendSlice(self: *Log, entries: []const LogEntry) !void {
        try self.entries.appendSlice(self.allocator, entries);
    }

    pub fn truncateFrom(self: *Log, from_index: u64) void {
        if (self.entries.items.len == 0) return;
        const base = self.entries.items[0].index;
        if (from_index <= base) {
            self.entries.shrinkRetainingCapacity(0);
            return;
        }
        const offset: usize = @intCast(from_index - base);
        if (offset >= self.entries.items.len) return;
        self.entries.shrinkRetainingCapacity(offset);
    }

    pub fn entriesFrom(self: *const Log, start_index: u64, max_count: u32) []const LogEntry {
        if (self.entries.items.len == 0) return &[_]LogEntry{};
        const base = self.entries.items[0].index;
        if (start_index < base) return &[_]LogEntry{};
        const offset: usize = @intCast(start_index - base);
        if (offset >= self.entries.items.len) return &[_]LogEntry{};
        const start: usize = offset;
        const max: usize = @intCast(max_count);
        const end = if (start + max <= self.entries.items.len) start + max else self.entries.items.len;
        return self.entries.items[start..end];
    }

    pub fn firstIndexOfTerm(self: *const Log, term: u64) u64 {
        for (self.entries.items) |entry| {
            if (entry.term == term) return entry.index;
        }
        return 0;
    }

    pub fn lastIndexOfTerm(self: *const Log, term: u64) u64 {
        var i: usize = self.entries.items.len;
        while (i > 0) : (i -= 1) {
            const entry = self.entries.items[i - 1];
            if (entry.term == term) return entry.index;
        }
        return 0;
    }

    pub fn rebuild(self: *Log, entries: []const LogEntry) !void {
        self.entries.shrinkRetainingCapacity(0);
        try self.entries.appendSlice(self.allocator, entries);
    }
};
