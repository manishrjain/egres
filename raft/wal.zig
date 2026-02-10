const std = @import("std");
const msg = @import("message.zig");

pub const NodeId = msg.NodeId;
pub const LogEntry = msg.LogEntry;

pub const WalError = error{
    WriteFailed,
    SyncFailed,
    Corrupted,
};

pub const HardState = struct {
    current_term: u64,
    voted_for: ?NodeId,
};

pub const Wal = struct {
    ctx: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        writeHardState: fn (ctx: *anyopaque, term: u64, voted_for: ?NodeId) WalError!void,
        appendEntries: fn (ctx: *anyopaque, entries: []const LogEntry) WalError!void,
        truncateFrom: fn (ctx: *anyopaque, from_index: u64) WalError!void,
        sync: fn (ctx: *anyopaque) WalError!void,
        replay: fn (ctx: *anyopaque, allocator: std.mem.Allocator) WalError!struct { HardState, []LogEntry },
    };

    pub fn writeHardState(self: Wal, term: u64, voted_for: ?NodeId) WalError!void {
        return self.vtable.writeHardState(self.ctx, term, voted_for);
    }

    pub fn appendEntries(self: Wal, entries: []const LogEntry) WalError!void {
        return self.vtable.appendEntries(self.ctx, entries);
    }

    pub fn truncateFrom(self: Wal, from_index: u64) WalError!void {
        return self.vtable.truncateFrom(self.ctx, from_index);
    }

    pub fn sync(self: Wal) WalError!void {
        return self.vtable.sync(self.ctx);
    }

    pub fn replay(self: Wal, allocator: std.mem.Allocator) WalError!struct { HardState, []LogEntry } {
        return self.vtable.replay(self.ctx, allocator);
    }
};
