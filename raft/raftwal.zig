const std = @import("std");
const zen = @import("buffers_gen");

pub const NodeId = u64;

// Wire-format enums live in raft/buffers.zig; we just re-export the Zig types
// so consumers can write `raftwal.ConfChangeType` instead of reaching into the
// generated zbuf module directly.
pub const ConfChangeType = zen.ChangeTypeEnum;
pub const EntryType = zen.EntryTypeEnum;

pub const ConfChange = struct {
    change_type: ConfChangeType,
    node_id: NodeId,
    addr: []const u8 = "",
};
pub const LogEntryBuffer = zen.LogEntryBuffer;
pub const HardState = zen.HardStateStruct;
pub const Snapshot = zen.SnapshotStruct;
pub const ConfState = zen.ConfStateStruct;
pub const ConfStatePeer = zen.ConfStatePeerStruct;

pub const StoredEntry = struct {
    index: u64,
    term: u64,
    bytes: []const u8 = "",
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
    // .any (tag 0) is the wildcard filter — an entry's wire-encoded type is
    // .data or .conf, never .any.
    ty: EntryType = .any,
};

/// Result of an entries range query. All entry bytes live in the arena.
/// Caller must call deinit() when done.
pub const EntriesResult = struct {
    entries: []StoredEntry,
    arena: std.heap.ArenaAllocator,
    /// True if all matching entries in the range were returned (not truncated by max_bytes).
    eof: bool = true,

    pub fn deinit(self: *EntriesResult) void {
        self.arena.deinit();
    }
};

pub fn deinitOwnedEntries(alloc: std.mem.Allocator, entries: []LogEntryBuffer) void {
    for (entries) |*entry_buf| {
        entry_buf.deinit();
    }
    alloc.free(entries);
}
