const std = @import("std");
const forest_mod = @import("forest.zig");
const Forest = forest_mod.Forest;
const Item = @import("key.zig").Item;

// ---------------------------------------------------------------------------
// StorageApplier
// ---------------------------------------------------------------------------

/// StorageApplier bridges Raft committed mutations to the Forest memtable.
/// Structural changes (flush, compaction, split) are applied inline by the
/// storage interface, not queued here.
pub const StorageApplier = struct {
    forest: *Forest,
    allocator: std.mem.Allocator,
    last_applied: u64,
    mu: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, forest: *Forest) StorageApplier {
        return .{
            .forest = forest,
            .allocator = allocator,
            .last_applied = 0,
            .mu = .{},
        };
    }

    pub fn deinit(self: *StorageApplier) void {
        _ = self;
    }

    /// Apply a batch of key-value mutations to the current memtable.
    /// Returns true if the memtable should be frozen (size threshold exceeded).
    pub fn applyMutations(self: *StorageApplier, index: u64, items: []const Item) !bool {
        self.mu.lock();
        defer self.mu.unlock();

        const should_freeze = try self.forest.put(items);
        self.last_applied = index;
        return should_freeze;
    }
};
