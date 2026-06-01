const std = @import("std");
const zen = @import("buffers.gen.zig");
const Key = @import("key.zig").Key;
const vfs = @import("vfs");
const zbuf = @import("zbuf").zbuf;
const fiber = @import("fiber");

// -------------------------------------------------------------------------
// Error set
// -------------------------------------------------------------------------

pub const ManifestError = error{
    TreeNotFound,
};

// -------------------------------------------------------------------------
// State types
// -------------------------------------------------------------------------

pub const LevelState = struct {
    tables: std.AutoHashMap(u64, void), // set of table_ids at this level
};

pub const TreeState = struct {
    namespace_id: u32 = 0,
    levels: std.ArrayList(LevelState),
    left: Key = .{},
    right: Key = .{},
};

/// In-memory manifest state. Durability comes from the Raft log + periodic
/// snapshots — this struct is never persisted to its own file. `serialize`
/// produces the bytes that go into a Raft snapshot's `data` field;
/// `restoreFrom` is the inverse, used by recovery to install the manifest
/// before WAL replay.
pub const ForestState = struct {
    allocator: std.mem.Allocator,
    trees: std.AutoHashMap(u64, TreeState),
    mu: fiber.Mutex = .{},

    pub fn init(allocator: std.mem.Allocator) ForestState {
        return .{
            .allocator = allocator,
            .trees = std.AutoHashMap(u64, TreeState).init(allocator),
        };
    }

    fn freeTree(self: *ForestState, tree: *TreeState) void {
        for (tree.levels.items) |*lm| lm.tables.deinit();
        tree.levels.deinit(self.allocator);
        if (tree.left.primary.len > 0) self.allocator.free(tree.left.primary);
        if (tree.right.primary.len > 0) self.allocator.free(tree.right.primary);
    }

    pub fn deinit(self: *ForestState) void {
        var it = self.trees.iterator();
        while (it.next()) |kv| self.freeTree(kv.value_ptr);
        self.trees.deinit();
    }

    /// Applies a single table CREATE or DELETE to the tree's levels.
    pub fn applyTableChange(self: *ForestState, tc: TableChange) !void {
        if (tc.op == .create) {
            const tree = self.trees.getPtr(tc.tree_id) orelse return ManifestError.TreeNotFound;
            while (tree.levels.items.len <= @as(usize, tc.level)) {
                const lm = LevelState{ .tables = std.AutoHashMap(u64, void).init(self.allocator) };
                try tree.levels.append(self.allocator, lm);
            }
            try tree.levels.items[tc.level].tables.put(tc.table_id, {});
        } else if (tc.op == .delete) {
            const tree = self.trees.getPtr(tc.tree_id) orelse return ManifestError.TreeNotFound;
            // Scan levels to find and remove the table.
            for (tree.levels.items) |*lm| {
                if (lm.tables.remove(tc.table_id)) {
                    break;
                }
            }
        }
    }

    /// Applies a tree CREATE, DELETE, or UPDATE.
    /// DELETE automatically removes all tables owned by the tree.
    /// UPDATE changes the tree's key range.
    pub fn applyTreeChange(self: *ForestState, tc: TreeChange) !void {
        if (tc.op == .create) {
            try self.trees.put(tc.tree_id, .{
                .namespace_id = tc.namespace_id,
                .levels = std.ArrayList(LevelState).empty,
                .left = try Key.initAlloc(self.allocator, tc.left_key),
                .right = try Key.initAlloc(self.allocator, tc.right_key),
            });
        } else if (tc.op == .delete) {
            if (self.trees.fetchRemove(tc.tree_id)) |kv| {
                var tree = kv.value;
                self.freeTree(&tree);
            }
        } else if (tc.op == .update) {
            const tree = self.trees.getPtr(tc.tree_id) orelse return ManifestError.TreeNotFound;
            tree.left.free(self.allocator);
            tree.right.free(self.allocator);

            tree.left = try Key.initAlloc(self.allocator, tc.left_key);
            tree.right = try Key.initAlloc(self.allocator, tc.right_key);
        }
    }

    /// Apply a batch of tree + table changes under the mutex. In-memory only:
    /// durability comes from the surrounding Raft log entry, not from any
    /// disk write here.
    pub fn commit(
        self: *ForestState,
        table_changes: []const TableChange,
        tree_changes: []const TreeChange,
    ) !void {
        if (table_changes.len == 0 and tree_changes.len == 0) return;

        self.mu.lock();
        defer self.mu.unlock();

        for (tree_changes) |tc| try self.applyTreeChange(tc);
        for (table_changes) |tc| try self.applyTableChange(tc);
    }

    /// Count total table entries across all trees and levels.
    pub fn tableCount(self: *ForestState) u32 {
        var count: u32 = 0;
        var it = self.trees.iterator();
        while (it.next()) |kv| {
            for (kv.value_ptr.levels.items) |lm| {
                count += @intCast(lm.tables.count());
            }
        }
        return count;
    }

    /// Serialize the entire ForestState into a self-describing byte blob
    /// suitable for embedding in a Raft snapshot's `data` field. Captures
    /// every tree, every per-level table list, and the tree key ranges +
    /// namespace assignments. Counters (`next_file_id`, `next_tree_id`)
    /// are *not* persisted — they are re-derived from the maximum table
    /// and tree ids on `restoreFrom` by the Forest layer.
    ///
    /// Wire format reuses the existing `ManifestChange` zbuf shape: a list
    /// of `tree_changes` (CREATE per tree) plus a list of `table_changes`
    /// (CREATE per table at its level).
    pub fn serialize(self: *const ForestState, allocator: std.mem.Allocator) ![]u8 {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();

        var tree_changes: std.ArrayList(TreeChange) = .empty;
        var table_changes: std.ArrayList(TableChange) = .empty;

        var it = self.trees.iterator();
        while (it.next()) |kv| {
            const tid = kv.key_ptr.*;
            const tree = kv.value_ptr.*;
            const left_raw = try tree.left.marshal(a);
            const right_raw = try tree.right.marshal(a);
            try tree_changes.append(a, createTreeChange(tid, tree.namespace_id, left_raw, right_raw));
            for (tree.levels.items, 0..) |lm, level_idx| {
                var tit = lm.tables.iterator();
                while (tit.next()) |tkv| {
                    try table_changes.append(a, createTableChange(
                        tkv.key_ptr.*,
                        tid,
                        @intCast(level_idx),
                    ));
                }
            }
        }

        return encodeChangeSet(allocator, table_changes.items, tree_changes.items);
    }

    /// Replace the receiver's tree contents with the deserialized state from
    /// `bytes` (as produced by `serialize`). Failure mid-decode leaves the
    /// state empty, not partial.
    pub fn restoreFrom(self: *ForestState, bytes: []const u8) !void {
        // Drop existing trees first; restore is a full replace, not a merge.
        var it = self.trees.iterator();
        while (it.next()) |kv| self.freeTree(kv.value_ptr);
        self.trees.clearRetainingCapacity();

        const reader = try zen.ManifestChangeReader.parseRoot(bytes);

        // Order matters: trees must exist before tables can attach to them.
        const tc_len = reader.treeChangesLen();
        var i: u32 = 0;
        while (i < tc_len) : (i += 1) {
            const tc = try reader.treeChangesAt(i);
            try self.applyTreeChange(.{
                .op = tc.op(),
                .tree_id = tc.treeId(),
                .namespace_id = tc.namespaceId(),
                .left_key = @constCast(tc.leftKey()),
                .right_key = @constCast(tc.rightKey()),
            });
        }

        const tbl_len = reader.tableChangesLen();
        i = 0;
        while (i < tbl_len) : (i += 1) {
            const tc = try reader.tableChangesAt(i);
            try self.applyTableChange(.{
                .op = tc.op(),
                .table_id = tc.tableId(),
                .tree_id = tc.treeId(),
                .level = tc.level(),
            });
        }
    }

    /// Deep-copy this ForestState into a fresh standalone copy. Used when
    /// building a snapshot draft so the live state is never mutated by the
    /// snapshot pipeline.
    pub fn clone(self: *const ForestState, allocator: std.mem.Allocator) !ForestState {
        const bytes = try self.serialize(allocator);
        defer allocator.free(bytes);
        var out = ForestState.init(allocator);
        errdefer out.deinit();
        try out.restoreFrom(bytes);
        return out;
    }
};

// -------------------------------------------------------------------------
// Change types — aliases to generated structs
// -------------------------------------------------------------------------

pub const TableChange = zen.TableChangeStruct;
pub const TreeChange = zen.TreeChangeStruct;
pub const ManifestChange = zen.ManifestChangeStruct;

pub fn createTableChange(table_id: u64, tree_id: u64, level: u32) TableChange {
    return .{ .op = .create, .table_id = table_id, .tree_id = tree_id, .level = level };
}

pub fn deleteTableChange(table_id: u64, tree_id: u64, level: u32) TableChange {
    return .{ .op = .delete, .table_id = table_id, .tree_id = tree_id, .level = level };
}

pub fn createTreeChange(tree_id: u64, namespace_id: u32, left_key: []const u8, right_key: []const u8) TreeChange {
    return .{ .op = .create, .tree_id = tree_id, .namespace_id = namespace_id, .left_key = @constCast(left_key), .right_key = @constCast(right_key) };
}

pub fn deleteTreeChange(tree_id: u64) TreeChange {
    return .{ .op = .delete, .tree_id = tree_id };
}

pub fn updateTreeChange(tree_id: u64, left_key: []const u8, right_key: []const u8) TreeChange {
    return .{ .op = .update, .tree_id = tree_id, .left_key = @constCast(left_key), .right_key = @constCast(right_key) };
}

// -------------------------------------------------------------------------
// Encoding helpers
// -------------------------------------------------------------------------

fn encodeChangeSet(
    allocator: std.mem.Allocator,
    table_changes: []const TableChange,
    tree_changes: []const TreeChange,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    var mw = try zen.ManifestChangeWriter.initWithBuf(allocator, &buf);

    if (tree_changes.len > 0) {
        const offsets = try allocator.alloc(u32, tree_changes.len);
        defer allocator.free(offsets);

        for (tree_changes, 0..) |tc, i| {
            var tw = try zen.TreeChangeWriter.initWithBuf(allocator, &buf);
            try tw.setOp(tc.op);
            try tw.setTreeId(tc.tree_id);
            if (tc.namespace_id != 0) try tw.setNamespaceId(tc.namespace_id);
            if (tc.left_key.len > 0) try tw.setLeftKey(tc.left_key);
            if (tc.right_key.len > 0) try tw.setRightKey(tc.right_key);
            const result = try tw.finish();
            offsets[i] = result.offset;
        }

        try mw.setTreeChanges(offsets);
    }

    if (table_changes.len > 0) {
        const offsets = try allocator.alloc(u32, table_changes.len);
        defer allocator.free(offsets);

        for (table_changes, 0..) |tc, i| {
            var tw = try zen.TableChangeWriter.initWithBuf(allocator, &buf);
            try tw.setOp(tc.op);
            try tw.setLevel(tc.level);
            try tw.setTableId(tc.table_id);
            try tw.setTreeId(tc.tree_id);
            const result = try tw.finish();
            offsets[i] = result.offset;
        }

        try mw.setTableChanges(offsets);
    }

    _ = try mw.finishWithTail();
    return buf.toOwnedSlice(allocator);
}
