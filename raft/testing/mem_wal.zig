const std = @import("std");
const raft = @import("raft");

pub const WalError = raft.WalError;
pub const HardState = raft.HardState;
pub const NodeId = raft.NodeId;
pub const LogEntry = raft.LogEntry;

pub const MemWal = struct {
    allocator: std.mem.Allocator,
    hard_state: HardState,
    entries: std.ArrayList(LogEntry),

    pub fn init(allocator: std.mem.Allocator) MemWal {
        return .{
            .allocator = allocator,
            .hard_state = .{ .current_term = 0, .voted_for = null },
            .entries = std.ArrayList(LogEntry).empty,
        };
    }

    pub fn deinit(self: *MemWal) void {
        self.entries.deinit(self.allocator);
    }

    pub fn asWal(self: *MemWal) raft.Wal {
        return .{ .ctx = self, .vtable = &vtable };
    }

    fn writeHardState(ctx: *anyopaque, term: u64, voted_for: ?NodeId) WalError!void {
        const self: *MemWal = @ptrCast(@alignCast(ctx));
        self.hard_state.current_term = term;
        self.hard_state.voted_for = voted_for;
    }

    fn appendEntries(ctx: *anyopaque, entries: []const LogEntry) WalError!void {
        const self: *MemWal = @ptrCast(@alignCast(ctx));
        try self.entries.appendSlice(self.allocator, entries);
    }

    fn truncateFrom(ctx: *anyopaque, from_index: u64) WalError!void {
        const self: *MemWal = @ptrCast(@alignCast(ctx));
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

    fn sync(ctx: *anyopaque) WalError!void {
        _ = ctx;
    }

    fn replay(ctx: *anyopaque, allocator: std.mem.Allocator) WalError!struct { HardState, []LogEntry } {
        const self: *MemWal = @ptrCast(@alignCast(ctx));
        const dup = try allocator.dupe(LogEntry, self.entries.items);
        return .{ self.hard_state, dup };
    }

    const vtable = raft.Wal.VTable{
        .writeHardState = writeHardState,
        .appendEntries = appendEntries,
        .truncateFrom = truncateFrom,
        .sync = sync,
        .replay = replay,
    };
};
