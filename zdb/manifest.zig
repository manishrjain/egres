const std = @import("std");
const buffers = @import("buffers.gen.zig");
const Key = @import("key.zig").Key;
const vfs = @import("vfs");
const zbuf = @import("zbuf").zbuf;

// -------------------------------------------------------------------------
// Constants
// -------------------------------------------------------------------------

const manifest_magic: u32 = 0x666E4D5A; // "ZMnf" little-endian
const manifest_version: u16 = 1;
const manifest_filename = "MANIFEST";
const manifest_rewrite_filename = "MANIFEST-REWRITE";
const default_deletions_rewrite_threshold: u32 = 10_000;
const deletions_ratio: u32 = 10;
const op_create: u32 = 0;
const op_delete: u32 = 1;
const op_update: u32 = 2;

// -------------------------------------------------------------------------
// Error set
// -------------------------------------------------------------------------

pub const ManifestError = error{
    BadMagic,
    UnsupportedVersion,
    ChecksumMismatch,
    CorruptManifest,
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

pub const ForestState = struct {
    allocator: std.mem.Allocator,
    trees: std.AutoHashMap(u64, TreeState),
    creations: u32,
    deletions: u32,
    // I/O fields — null/undefined until openOrCreate sets them.
    handle: ?vfs.FileHandle,
    dir_path: []const u8,
    write_pos: u64,
    deletions_rewrite_threshold: u32,
    mu: std.Thread.Mutex,

    fn initMaps(allocator: std.mem.Allocator) ForestState {
        return .{
            .allocator = allocator,
            .trees = std.AutoHashMap(u64, TreeState).init(allocator),
            .creations = 0,
            .deletions = 0,
            .handle = null,
            .dir_path = undefined,
            .write_pos = 0,
            .deletions_rewrite_threshold = undefined,
            .mu = undefined,
        };
    }

    fn freeTree(self: *ForestState, tree: *TreeState) void {
        for (tree.levels.items) |*lm| lm.tables.deinit();
        tree.levels.deinit(self.allocator);
        if (tree.left.primary.len > 0) self.allocator.free(tree.left.primary);
        if (tree.right.primary.len > 0) self.allocator.free(tree.right.primary);
    }

    fn freeMaps(self: *ForestState) void {
        var it = self.trees.iterator();
        while (it.next()) |kv| self.freeTree(kv.value_ptr);
        self.trees.deinit();
    }

    pub fn close(self: *ForestState) void {
        if (self.handle) |h| h.close();
        self.allocator.free(self.dir_path);
        self.freeMaps();
    }

    /// Applies a single table CREATE or DELETE to the tree's levels.
    pub fn applyTableChange(self: *ForestState, tc: TableChange) !void {
        if (tc.op == op_create) {
            const tree = self.trees.getPtr(tc.tree_id) orelse return ManifestError.TreeNotFound;
            while (tree.levels.items.len <= @as(usize, tc.level)) {
                const lm = LevelState{ .tables = std.AutoHashMap(u64, void).init(self.allocator) };
                try tree.levels.append(self.allocator, lm);
            }
            try tree.levels.items[tc.level].tables.put(tc.table_id, {});
            self.creations += 1;
        } else if (tc.op == op_delete) {
            const tree = self.trees.getPtr(tc.tree_id) orelse return ManifestError.TreeNotFound;
            // Scan levels to find and remove the table.
            for (tree.levels.items) |*lm| {
                if (lm.tables.remove(tc.table_id)) {
                    break;
                }
            }
            self.deletions += 1;
        }
    }

    /// Applies a tree CREATE, DELETE, or UPDATE.
    /// DELETE automatically removes all tables owned by the tree.
    /// UPDATE changes the tree's key range.
    pub fn applyTreeChange(self: *ForestState, tc: TreeChange) !void {
        if (tc.op == op_create) {
            try self.trees.put(tc.tree_id, .{
                .namespace_id = tc.namespace_id,
                .levels = std.ArrayList(LevelState).empty,
                .left = try Key.initAlloc(self.allocator, tc.left_key),
                .right = try Key.initAlloc(self.allocator, tc.right_key),
            });
        } else if (tc.op == op_delete) {
            if (self.trees.fetchRemove(tc.tree_id)) |kv| {
                var tree = kv.value;
                self.freeTree(&tree);
            }
        } else if (tc.op == op_update) {
            const tree = self.trees.getPtr(tc.tree_id) orelse return ManifestError.TreeNotFound;
            tree.left.free(self.allocator);
            tree.right.free(self.allocator);

            tree.left = try Key.initAlloc(self.allocator, tc.left_key);
            tree.right = try Key.initAlloc(self.allocator, tc.right_key);
        }
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

    fn replay(self: *ForestState) !void {
        std.debug.assert(self.trees.count() == 0);

        const handle = self.handle.?;
        const file_size = handle.file_size;

        const result = try handle.readAligned(self.allocator, 0, @intCast(file_size), .p1);
        defer result.free();
        const data = result.data;

        if (data.len < 8) return ManifestError.BadMagic;

        const magic = std.mem.readInt(u32, data[0..4], .little);
        if (magic != manifest_magic) return ManifestError.BadMagic;

        const version = std.mem.readInt(u16, data[6..8], .little);
        if (version != manifest_version) return ManifestError.UnsupportedVersion;

        var pos: usize = 8;
        var trunc_offset: usize = 8;

        while (true) {
            trunc_offset = pos;

            // Each record is a zbuf frame: [u64 hash | u32 len | payload].
            // OutOfBounds = clean EOF (truncate happens after the loop);
            // Invalid = CRC/hash mismatch — surface as ChecksumMismatch.
            const payload = zbuf.Frame.read(data[pos..]) catch |err| switch (err) {
                zbuf.Error.OutOfBounds => break,
                zbuf.Error.Invalid => return ManifestError.ChecksumMismatch,
            };
            pos += zbuf.Frame.hdr_size + payload.len;

            const reader = try buffers.ManifestChangeReader.parseRoot(payload);

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

            trunc_offset = pos;
        }

        try handle.truncate(trunc_offset);
        self.write_pos = trunc_offset;
    }

    fn rewrite(self: *ForestState) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const rewrite_path = try std.fs.path.join(alloc, &.{ self.dir_path, manifest_rewrite_filename });
        const manifest_path = try std.fs.path.join(alloc, &.{ self.dir_path, manifest_filename });

        // Build the entire file contents in memory before touching the filesystem.
        var out: std.ArrayList(u8) = .empty;

        var file_header: [8]u8 = undefined;
        std.mem.writeInt(u32, file_header[0..4], manifest_magic, .little);
        std.mem.writeInt(u16, file_header[4..6], 0, .little);
        std.mem.writeInt(u16, file_header[6..8], manifest_version, .little);
        try out.appendSlice(alloc, &file_header);

        if (self.trees.count() > 0) {
            var tree_changes: std.ArrayList(TreeChange) = .empty;
            var table_changes: std.ArrayList(TableChange) = .empty;

            var it = self.trees.iterator();
            while (it.next()) |kv| {
                const tree_id = kv.key_ptr.*;
                const tree = kv.value_ptr.*;
                const left_raw = try tree.left.marshal(alloc);
                const right_raw = try tree.right.marshal(alloc);
                try tree_changes.append(alloc, createTreeChange(tree_id, tree.namespace_id, left_raw, right_raw));

                for (tree.levels.items, 0..) |lm, level_idx| {
                    var tit = lm.tables.iterator();
                    while (tit.next()) |tkv| {
                        try table_changes.append(alloc, createTableChange(
                            tkv.key_ptr.*,
                            tree_id,
                            @intCast(level_idx),
                        ));
                    }
                }
            }

            const payload = try encodeChangeSet(alloc, table_changes.items, tree_changes.items);
            _ = try zbuf.Frame.append(&out, alloc, payload);
        }

        // Create file, write everything in one call, sync, then rename into place.
        const tmp = try vfs.g().createFile(rewrite_path, .{ .flags = .{ .truncate = true } });
        errdefer vfs.VFS.deleteFile(rewrite_path) catch {};
        try tmp.write(out.items, 0, .p0);
        try tmp.sync();
        tmp.close();

        try vfs.VFS.renameFile(rewrite_path, manifest_path);

        // openFile should use the self.allocator, so it's valid after returning from this func.
        const new_handle = try vfs.g().openFile(self.allocator, manifest_path, .{ .flags = .{ .writable = true } });
        errdefer new_handle.close();

        if (self.handle) |h| h.close();
        self.handle = new_handle;
        self.write_pos = new_handle.file_size;
        self.creations = self.tableCount();
        self.deletions = 0;
    }

    pub fn commit(
        self: *ForestState,
        table_changes: []const TableChange,
        tree_changes: []const TreeChange,
    ) !void {
        if (table_changes.len == 0 and tree_changes.len == 0) return;

        self.mu.lock();
        defer self.mu.unlock();

        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const alloc = arena.allocator();

        const payload = try encodeChangeSet(alloc, table_changes, tree_changes);

        var out = std.ArrayList(u8).empty;
        const written = try zbuf.Frame.append(&out, alloc, payload);

        try self.handle.?.write(out.items, self.write_pos, .p0);
        self.write_pos += written;
        try self.handle.?.sync();
        _ = arena.reset(.free_all); // reclaim memory

        for (tree_changes) |tc| {
            try self.applyTreeChange(tc);
        }
        for (table_changes) |tc| {
            try self.applyTableChange(tc);
        }

        const live = self.creations -| self.deletions;
        const should_rewrite = self.deletions > self.deletions_rewrite_threshold and
            self.deletions > deletions_ratio *| live;

        if (should_rewrite) {
            try self.rewrite();
        }
    }
};

// -------------------------------------------------------------------------
// Change types — aliases to generated structs
// -------------------------------------------------------------------------

pub const TableChange = buffers.TableChangeStruct;
pub const TreeChange = buffers.TreeChangeStruct;
pub const ManifestChange = buffers.ManifestChangeStruct;

pub fn createTableChange(table_id: u64, tree_id: u64, level: u32) TableChange {
    return .{ .op = op_create, .table_id = table_id, .tree_id = tree_id, .level = level };
}

pub fn deleteTableChange(table_id: u64, tree_id: u64, level: u32) TableChange {
    return .{ .op = op_delete, .table_id = table_id, .tree_id = tree_id, .level = level };
}

pub fn createTreeChange(tree_id: u64, namespace_id: u32, left_key: []const u8, right_key: []const u8) TreeChange {
    return .{ .op = op_create, .tree_id = tree_id, .namespace_id = namespace_id, .left_key = @constCast(left_key), .right_key = @constCast(right_key) };
}

pub fn deleteTreeChange(tree_id: u64) TreeChange {
    return .{ .op = op_delete, .tree_id = tree_id };
}

pub fn updateTreeChange(tree_id: u64, left_key: []const u8, right_key: []const u8) TreeChange {
    return .{ .op = op_update, .tree_id = tree_id, .left_key = @constCast(left_key), .right_key = @constCast(right_key) };
}

// -------------------------------------------------------------------------
// openOrCreate
// -------------------------------------------------------------------------

pub fn openOrCreate(
    allocator: std.mem.Allocator,
    dir_path: []const u8,
) !ForestState {
    var state = ForestState.initMaps(allocator);
    errdefer state.freeMaps();

    state.dir_path = try allocator.dupe(u8, dir_path);
    errdefer allocator.free(state.dir_path);
    state.deletions_rewrite_threshold = default_deletions_rewrite_threshold;
    state.mu = .{};

    const manifest_path = try std.fs.path.join(allocator, &.{ dir_path, manifest_filename });
    defer allocator.free(manifest_path);

    const handle = vfs.g().openFile(allocator, manifest_path, .{
        .flags = .{ .writable = true },
    }) catch |err| {
        if (err != error.FileNotFound) return err;
        try state.rewrite();
        return state;
    };
    errdefer handle.close();

    state.handle = handle;
    try state.replay();
    return state;
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

    var mw = try buffers.ManifestChangeWriter.initWithBuf(allocator, &buf);

    if (tree_changes.len > 0) {
        const offsets = try allocator.alloc(u32, tree_changes.len);
        defer allocator.free(offsets);

        for (tree_changes, 0..) |tc, i| {
            var tw = try buffers.TreeChangeWriter.initWithBuf(allocator, &buf);
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
            var tw = try buffers.TableChangeWriter.initWithBuf(allocator, &buf);
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
