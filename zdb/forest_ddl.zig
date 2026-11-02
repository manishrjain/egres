// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Forest's DDL methods for the namespace, table and split commands: the
//! leader-side `build*` functions, which read the manifest and the live
//! levels and return a ManifestChange for the caller to propose, and the
//! node-side `apply*` functions, which run on every node after Raft commits
//! that change — they load the files, apply the change to the manifest, and
//! update the live trees (install, close, swap) to match. Tree installation
//! and the manifest-driven recovery walks are here too. Nothing in this file
//! mutates the manifest except through `Manifest.apply` (or, on the two
//! index-0 paths with no Raft entry behind them, `Manifest.commit`).

const std = @import("std");
const log = std.log.scoped(.forest);
const utils = @import("utils");
const String = utils.String;
const _manifest = @import("manifest.zig");
const KeyRange = @import("kv.zig").KeyRange;
const TableKey = @import("kv.zig").TableKey;
const Key = @import("kv.zig").Key;
const makeTreeId = @import("kv.zig").makeTreeId;
const treeTableId = @import("kv.zig").treeTableId;
const Forest = @import("forest.zig").Forest;
const TableTrees = @import("forest.zig").TableTrees;
const Tree = @import("tree.zig").Tree;

/// installTree opens the in-memory Tree for the given id and adds it to its
/// table's TableTrees (keyed by the high 32 bits of tree_id). Tree.open
/// reads the tree's key range from the manifest, so the caller must have
/// already committed the TreeChange. The open — which loads the tree's
/// SSTs, file I/O — holds nothing; the publish is the map's own insert
/// plus one list append under the table's lock, and the tree is
/// invisible until that append.
pub fn installTree(self: *Forest, namespace_id: u64, tree_id: u64) !void {
    const tree = try Tree.open(
        self.allocator,
        self.live,
        namespace_id,
        tree_id,
        &self.manifest,
        &self.table_cache,
    );
    errdefer tree.release();

    const tbl_trees = try self.getOrCreateTable(.{
        .namespace = namespace_id,
        .table_id = treeTableId(tree_id),
    });
    const held = tbl_trees.mu.hold(.exclusive, @src());
    defer held.release();
    try tbl_trees.addTree(held, tree);
}

/// Walk `self.manifest.trees` and install each tree (Tree + TableTrees entries).
/// Used after `Manifest.restoreFrom(snapshot.data)` at recovery — the
/// manifest already names every tree and table, but the in-memory Tree
/// objects and TableTrees need to be rebuilt from it. Tree ids are
/// derived (table_id<<32 | split_id), so there is no counter to advance.
///
/// File ids need no recovery either: they are drawn at random by
/// `Manifest.newFileId`, so there is no counter or lease to restore.
pub fn openTablesFromManifest(self: *Forest) !void {
    // installTree opens each Tree (which dupes its key range from the
    // manifest), so we only need the keys here.
    const keys = try self.manifest.treeKeys(self.allocator, null);
    defer self.allocator.free(keys);
    for (keys) |tk| try self.installTree(tk.namespace, tk.tree_id);
}

/// Which namespace id a create takes.
pub const NsId = union(enum) {
    /// The next unused id, reserved here (SQL: CREATE DATABASE).
    /// applyTrees advances the same counter on every node, so a
    /// re-elected leader never re-issues an id.
    next,
    /// An id the caller holds (the admin protocol's create_ns, genesis).
    given: u64,
};

/// A namespace create or branch: the id it takes and the change to
/// propose. Caller owns `change.tree_changes`.
pub const NsResult = struct {
    namespace: u64,
    change: _manifest.ManifestChange,
};

/// buildNsCreate is Phase A of the Raft-replicated namespace create:
/// returns a ManifestChange with one TreeChange per system table
/// (manifest.namespace_system_table_ids) — the namespace's default
/// (raw-KV) table (table_id 0, tree_id 0, carrying the namespace's `name`:
/// a SQL database name; empty for unnamed namespaces) and its SQL
/// catalogs — all with an infinite range (empty left+right). No state
/// mutates, so a failed propose costs nothing. Caller owns the returned
/// slice; `name` must outlive the change (it is marshalled at propose).
///
/// Errors with `NamespaceAlreadyExists` if a given id's default table
/// already exists, `NameExists` if another namespace holds `name`.
pub fn buildNsCreate(
    self: *Forest,
    alloc: std.mem.Allocator,
    id: NsId,
    name: String,
) !NsResult {
    const namespace_id = switch (id) {
        .next => self.manifest.nextNamespace(),
        .given => |n| n,
    };
    std.debug.assert(namespace_id > 0);
    // The system namespace has no default table, so hasNamespace
    // misses it; genesis made it, and nothing creates it again.
    if (namespace_id == _manifest.system_namespace or
        self.hasNamespace(namespace_id)) return error.NamespaceAlreadyExists;
    if (self.manifest.lookupNamespace(name)) |_| return error.NameExists;

    // One tree per system table: the default (raw-KV) table plus the SQL
    // catalogs, born with the namespace — catalog rows must never race
    // tree creation, because buildFlush silently drops keys whose
    // (ns, table_id) has no tree.
    const ids = _manifest.namespace_system_table_ids;
    const tree_changes = try alloc.alloc(_manifest.TreeChange, ids.len);
    // Bytes keys, and the raw-KV table takes roaring values from the
    // admin protocol: a posting index for both.
    for (ids, tree_changes) |table_id, *tc|
        tc.* = _manifest.createTreeChange(
            makeTreeId(table_id, 0),
            namespace_id,
            "",
            "",
            .posting_index,
        );
    // table_id 0 carries the namespace's name.
    tree_changes[0].name = @constCast(name);

    return .{
        .namespace = namespace_id,
        .change = .{
            .tree_changes = tree_changes,
            .sstable_changes = &.{},
            .cmd = .create_ns,
            .memtable_id = 0,
        },
    };
}

/// applyTrees is Phase B of create_ns, create_tree, drop_ns and
/// drop_tree. The manifest apply does what the command means (records
/// names, advances or forgets counters, retires files whose last
/// listing a drop removes); then each created tree the forest lacks is
/// installed and each deleted one torn down. Idempotent, per tree: a
/// re-apply finds the creates present and the deletes gone. Called on
/// all nodes after Raft commit, on the apply loop.
pub fn applyTrees(
    self: *Forest,
    index: u64,
    mc: _manifest.ManifestChange,
) !void {
    try self.manifest.apply(index, mc);
    for (mc.tree_changes) |tc| switch (tc.op) {
        .create => {
            if (self.hasTree(tc.namespace_id, tc.tree_id)) continue;
            try self.installTree(tc.namespace_id, tc.tree_id);
        },
        .delete => dropTree(self, tc),
        // A range update is a split's (applySplit), never a create or
        // drop's; the enum is open on the wire.
        .update, _ => unreachable,
    };
}

/// Removes a dropped tree from its table and tears it down. The tree
/// leaves the list under the table lock, so no reader finds it from
/// here on; dropping the list's ref closes it once readers that hold
/// it let go. The TableTrees stays in the map, empty: table ids are
/// never reused, and a reader may still hold its pointer.
fn dropTree(self: *Forest, tc: _manifest.TreeChange) void {
    const trees = self.getTable(.{
        .namespace = tc.namespace_id,
        .table_id = treeTableId(tc.tree_id),
    }) orelse return;

    const gone = blk: {
        const held = trees.mu.hold(.exclusive, @src());
        defer held.release();
        break :blk trees.removeTree(held, tc.tree_id);
    };

    if (gone) |tree| tree.release();
}

/// buildNsBranch is Phase A of CREATE DATABASE `name` TEMPLATE
/// `source`: reserves the target's namespace id and returns the
/// branch_ns change naming the source. The copy itself is derived at
/// apply (Manifest.applyBranch), so nothing here goes stale between
/// propose and apply. Errors `NamespaceNotFound` for a source that
/// does not exist and `NameExists` for a taken name. Caller owns
/// `change.tree_changes`; `name` must outlive the change.
pub fn buildNsBranch(
    self: *Forest,
    alloc: std.mem.Allocator,
    source: u64,
    name: String,
) !NsResult {
    if (!self.hasNamespace(source)) return error.NamespaceNotFound;
    if (self.manifest.lookupNamespace(name)) |_| return error.NameExists;

    const namespace = self.manifest.nextNamespace();
    // A carrier for the target's id and name: the branch's trees take
    // the source's kinds (Manifest.applyBranch).
    const tree_changes = try alloc.alloc(_manifest.TreeChange, 1);
    tree_changes[0] = _manifest.createTreeChange(
        makeTreeId(0, 0),
        namespace,
        "",
        "",
        .none,
    );
    tree_changes[0].name = @constCast(name);
    return .{
        .namespace = namespace,
        .change = .{
            .tree_changes = tree_changes,
            .sstable_changes = &.{},
            .cmd = .branch_ns,
            .memtable_id = 0,
            .branch_from = source,
        },
    };
}

/// applyNsBranch is Phase B: the manifest lists the target's
/// trees over the source's files, each is installed, and the source's
/// versions still in memtables, those committed before the entry
/// (`ts`, its commit timestamp), are written again under the target
/// (Memtables.fork). Together that is every version the source
/// held as of the entry; later writes to either side stay on that
/// side. Called on all nodes after Raft commit (branch_ns); skips
/// trees the forest already holds (re-apply).
pub fn applyNsBranch(
    self: *Forest,
    index: u64,
    ts: u64,
    mc: _manifest.ManifestChange,
) !void {
    try self.manifest.apply(index, mc);
    const dst = mc.tree_changes[0].namespace_id;
    const keys = try self.manifest.treeKeys(self.allocator, dst);
    defer self.allocator.free(keys);
    for (keys) |tk| {
        if (self.hasTree(tk.namespace, tk.tree_id)) continue;
        try self.installTree(tk.namespace, tk.tree_id);
    }
    const src = mc.branch_from;
    std.debug.assert(dst > src);
    const copied = try self.memtables.fork(.{
        .left = Key.tableLeft(.{ .namespace = src, .table_id = 0 }),
        .right = Key.tableRight(.{
            .namespace = src,
            .table_id = std.math.maxInt(u32),
        }),
    }, ts, .{ .namespace = dst, .table_id = null });
    log.info("branch ns={d} -> ns={d} at ts={d}: {d} memtable keys", .{
        src, dst, ts, copied,
    });
}

/// Result of buildTables: one allocated table_id per change (same
/// order) plus the single ManifestChange creating them all, listing
/// the caller's changes. Caller owns `table_ids`.
pub const TableResult = struct {
    table_ids: []u32,
    change: _manifest.ManifestChange,
};

/// applyTableBranch is Phase B: the manifest lists each new table's
/// trees over its source's files, each is installed, and the source's
/// versions still in memtables, those committed before the entry
/// (`ts`), are written again under the new table (Memtables.fork),
/// as applyNsBranch does for a namespace. Called on all nodes after
/// Raft commit (branch_table); skips trees the forest already holds.
pub fn applyTableBranch(
    self: *Forest,
    index: u64,
    ts: u64,
    mc: _manifest.ManifestChange,
) !void {
    try self.manifest.apply(index, mc);
    for (mc.tree_changes) |tc| {
        const ns = tc.namespace_id;
        const dst_tid = treeTableId(tc.tree_id);
        const keys = try self.manifest.treeKeys(self.allocator, ns);
        defer self.allocator.free(keys);

        for (keys) |tk| {
            if (treeTableId(tk.tree_id) != dst_tid) continue;
            if (self.hasTree(tk.namespace, tk.tree_id)) continue;
            try self.installTree(tk.namespace, tk.tree_id);
        }

        const src: TableKey = .{
            .namespace = ns,
            .table_id = tc.from_table_id,
        };
        std.debug.assert(dst_tid > src.table_id);
        const copied = try self.memtables.fork(.{
            .left = Key.tableLeft(src),
            .right = Key.tableRight(src),
        }, ts, .{ .namespace = ns, .table_id = dst_tid });
        log.info("branch ns={d} table={d} -> table={d} at ts={d}: {d} " ++
            "memtable keys", .{ ns, src.table_id, dst_tid, ts, copied });
    }
}

/// buildTables is Phase A of CREATE TABLE and CREATE INDEX (`cmd`
/// create_tree) and of CREATE TABLE AS TABLE (branch_table): N sibling
/// tables in ONE change — a SQL table plus its index tables, so they
/// commit atomically. `changes` are the caller's
/// (`manifest.newTableChange`), `name` set, and `kind` for a create or
/// `from_table_id` for a branch; each gets a freshly allocated table_id
/// here. A create's apply installs one empty tree of its kind per
/// table and registers every name → table_id on all nodes, advancing
/// the id counter past each. A branch's apply lists its source's trees
/// again, files shared (Manifest.applyTableBranch), so a branch change
/// names no kind. A non-empty name already taken in the namespace
/// errors `NameExists` (sound while the caller serializes DDL); an
/// empty name registers nothing (system trees). Names must be pairwise
/// distinct — an intra-batch duplicate is the caller's bug (the second
/// would install unnamed). A branch of a table with no trees errors
/// `TableNotFound`. Caller owns `table_ids` and `changes`, which the
/// result's change lists; each name must outlive the change
/// (marshalled at propose).
pub fn buildTables(
    self: *Forest,
    alloc: std.mem.Allocator,
    namespace_id: u64,
    changes: []_manifest.TreeChange,
    cmd: _manifest.Command,
) !TableResult {
    std.debug.assert(cmd == .create_tree or cmd == .branch_table);
    for (changes) |tc| {
        if (tc.name.len > 0 and
            self.manifest.lookupTable(namespace_id, tc.name) != null)
            return error.NameExists;
        if (cmd == .branch_table and !self.hasTable(.{
            .namespace = namespace_id,
            .table_id = tc.from_table_id,
        })) return error.TableNotFound;
    }

    const table_ids = try alloc.alloc(u32, changes.len);
    errdefer alloc.free(table_ids);
    for (changes, table_ids) |*tc, *table_id| {
        table_id.* = try self.manifest.nextTableId(namespace_id);
        tc.op = .create;
        tc.namespace_id = namespace_id;
        tc.tree_id = makeTreeId(table_id.*, 0);
        if (cmd == .branch_table) tc.kind = .none;
    }
    return .{
        .table_ids = table_ids,
        .change = .{
            .tree_changes = changes,
            .sstable_changes = &.{},
            .cmd = cmd,
            .memtable_id = 0,
        },
    };
}

/// buildTableDrop is Phase A of DROP TABLE: emit a delete change for
/// every tree of every given table_id — a table can span >1 tree after
/// splits, and a SQL table drops alongside its index tables in one atomic
/// change. table_id counters are NOT rewound (ids are never reused, since
/// a dropped SST may still be clone-shared). drop_tree. Returns an empty
/// change set if none of the tables have trees. Caller owns the slice.
///
/// The dropped tables' rows in the namespace's CATALOG trees are NOT
/// touched — with their names gone and table_ids never reused they are
/// unreachable garbage. TODO: issue tombstones for them.
pub fn buildTableDrop(
    self: *Forest,
    alloc: std.mem.Allocator,
    namespace_id: u64,
    table_ids: []const u32,
) !_manifest.ManifestChange {
    return .{
        .tree_changes = try buildTreeDeletes(
            self,
            alloc,
            namespace_id,
            table_ids,
        ),
        .sstable_changes = &.{},
        .cmd = .drop_tree,
        .memtable_id = 0,
    };
}

/// buildNsDrop is Phase A of DROP DATABASE: emit a delete change for
/// every tree in every table of the namespace — the default table, system
/// tables (catalog etc.), and user tables alike. drop_ns. Namespace ids are
/// never reused, so no counter rewind is needed; the apply forgets
/// the counter entirely. Returns an empty change set for an unknown
/// namespace (idempotent). Caller owns the returned slice.
pub fn buildNsDrop(
    self: *Forest,
    alloc: std.mem.Allocator,
    namespace_id: u64,
) !_manifest.ManifestChange {
    return .{
        .tree_changes = try buildTreeDeletes(self, alloc, namespace_id, null),
        .sstable_changes = &.{},
        .cmd = .drop_ns,
        .memtable_id = 0,
    };
}

/// A delete change for every tree of `table_ids` in the namespace, or
/// of every table in it when null. Each table's tree list is read under
/// its shared lock. Caller owns the slice.
fn buildTreeDeletes(
    self: *Forest,
    alloc: std.mem.Allocator,
    namespace_id: u64,
    table_ids: ?[]const u32,
) ![]_manifest.TreeChange {
    var changes: std.ArrayList(_manifest.TreeChange) = .empty;
    defer changes.deinit(alloc);
    if (table_ids) |ids| {
        for (ids) |table_id| {
            const trees = self.getTable(.{
                .namespace = namespace_id,
                .table_id = table_id,
            }) orelse continue;
            try appendTreeDeletes(&changes, alloc, trees, namespace_id);
        }
    } else {
        var it = self.tables.iterator();
        defer it.deinit();
        while (it.next()) |kv| {
            if (kv.key_ptr.namespace != namespace_id) continue;
            const trees = kv.value_ptr.*;
            try appendTreeDeletes(&changes, alloc, trees, namespace_id);
        }
    }
    return changes.toOwnedSlice(alloc);
}

fn appendTreeDeletes(
    changes: *std.ArrayList(_manifest.TreeChange),
    alloc: std.mem.Allocator,
    trees: *TableTrees,
    namespace_id: u64,
) !void {
    const held = trees.mu.hold(.shared, @src());
    defer held.release();
    for (trees.list(held)) |e| try changes.append(
        alloc,
        _manifest.deleteTreeChange(e.tree_id, namespace_id),
    );
}

/// buildSplit is Phase A: finds the split key, classifies tables, and returns
/// a ManifestChange with the table and tree changes. All allocations go on
/// the provided allocator (typically an arena). The caller proposes the
/// ManifestChange through Raft. The new half stays in the parent's table
/// table, so its tree_id is the table's next free split index.
pub fn buildSplit(
    self: *Forest,
    alloc: std.mem.Allocator,
    namespace_id: u64,
    tree_id: u64,
) !?_manifest.ManifestChange {
    std.debug.assert(namespace_id > 0);
    const left_tree = self.getTree(namespace_id, tree_id) orelse return null;
    defer left_tree.release();

    // Find split key from the lowest level in old tree.
    const split_key = (try left_tree.findSplitKey(alloc)) orelse return null;

    // The old tree's bounds, copied out under the manifest lock: this
    // runs on the split-propose fiber while the apply loop may commit
    // (rehashing the map, freeing a dropped tree's keys).
    const old_range = try self.manifest.treeRange(alloc, .{
        .namespace = namespace_id,
        .tree_id = tree_id,
    }) orelse return null;
    const old_kind = self.manifest.treeKind(.{
        .namespace = namespace_id,
        .tree_id = tree_id,
    }) orelse return null;
    const new_tree_id = self.nextTreeId(.{
        .namespace = namespace_id,
        .table_id = treeTableId(tree_id),
    });

    // Classify tables per level and build manifest changes.
    var table_changes: std.ArrayList(_manifest.SSTableChange) = .empty;

    const held = left_tree.mu.hold(.shared, @src());
    defer held.release();
    for (left_tree.levels, 0..) |*llv, llv_idx| {
        for (llv.entries(held)) |tbe| {
            if (tbe.table.smallest.orderPrimary(split_key) != .lt) { // .eq or .gt
                // Right-only: remove from old tree, add to new tree.
                try table_changes.append(alloc, _manifest.deleteSSTableChange(
                    tbe.id,
                    namespace_id,
                    tree_id,
                    @intCast(llv_idx),
                ));
                try table_changes.append(alloc, _manifest.createSSTableChange(
                    tbe.id,
                    namespace_id,
                    new_tree_id,
                    @intCast(llv_idx),
                    tbe.table.extent,
                ));
            } else if (tbe.table.biggest.orderPrimary(
                split_key,
            ) != .lt) { // .eq or .gt
                // order: smallest key < split key <= biggest key
                // Therefore, the split key falls within the table.
                // Shared: keep in old tree, also add to new tree.
                try table_changes.append(alloc, _manifest.createSSTableChange(
                    tbe.id,
                    namespace_id,
                    new_tree_id,
                    @intCast(llv_idx),
                    tbe.table.extent,
                ));
            }
            // Left-only: no manifest change needed.
        }
    }

    const left_left_raw_key = try old_range.left.marshal(alloc);
    const right_right_raw_key = try old_range.right.marshal(alloc);
    const split_raw = try split_key.marshal(alloc);

    const tree_changes = try alloc.alloc(_manifest.TreeChange, 2);
    tree_changes[0] = _manifest.updateTreeChange(
        tree_id,
        namespace_id,
        left_left_raw_key,
        split_raw,
    );
    tree_changes[1] = _manifest.createTreeChange(
        new_tree_id,
        namespace_id,
        split_raw,
        right_right_raw_key,
        old_kind,
    );

    return .{
        .sstable_changes = try table_changes.toOwnedSlice(alloc),
        .tree_changes = tree_changes,
        .cmd = .tree_split,
    };
}

/// applySplit is Phase B: commits the manifest changes, opens FRESH tree
/// objects for both halves from the post-commit manifest — the left
/// keeps its id with the narrowed range and only its tables, the right
/// is new — swaps them into the table's list, and retires the old
/// object. The old one is never mutated: whoever still holds a ref on
/// it (a reader mid-selection, a proposer) keeps seeing its full range
/// and full table set until they let go, which is what makes a split
/// safe against readers without any of them taking a lock across their
/// use of a tree. Called on all nodes after Raft commit. The new tree_id
/// is derived deterministically (table's next split index), so there is
/// no counter for a new leader to re-issue.
pub fn applySplit(
    self: *Forest,
    index: u64,
    mc: _manifest.ManifestChange,
) !void {
    // Commit MANIFEST so we can recover from a crash.
    try self.manifest.commit(index, mc.sstable_changes, mc.tree_changes);

    // Find the old tree_id (UPDATE) and new tree_id (CREATE) from tree_changes.
    // Both changes carry the same namespace (a split stays within one).
    var old_tree_id: u64 = 0;
    var new_tree_id: u64 = 0;
    var namespace_id: u64 = 0;
    for (mc.tree_changes) |tc| {
        if (tc.op == .update) { // UPDATE
            old_tree_id = tc.tree_id;
        } else if (tc.op == .create) { // CREATE
            new_tree_id = tc.tree_id;
        }
        namespace_id = tc.namespace_id;
    }

    // Both halves are opened from the post-commit manifest while the old
    // object still lists every table, so loadTables finds the shared
    // ones in the cache with ref > 0. Their key ranges come from the
    // manifest too: the left's right edge is the split key.
    // Refs start at 1, so releasing a half that never gets published
    // closes and frees it.
    const left = try Tree.open(
        self.allocator,
        self.live,
        namespace_id,
        old_tree_id,
        &self.manifest,
        &self.table_cache,
    );
    errdefer left.release();
    const right = try Tree.open(
        self.allocator,
        self.live,
        namespace_id,
        new_tree_id,
        &self.manifest,
        &self.table_cache,
    );
    errdefer right.release();

    // Fresh candidates against the post-split state. Without this the
    // left half would carry the old tree's split verdict
    // even though half its tables just moved out, and the next
    // levels-loop iteration would fire another split.
    try left.updateCompactCandidate();
    try right.updateCompactCandidate();

    // The swap: both halves in, the old object out, under one exclusive
    // hold of the table's lock (same table — a split shares the high
    // 32 bits of the tree_id). Capacity is reserved first so the two
    // adds cannot fail between the old tree leaving and them landing.
    std.debug.assert(treeTableId(new_tree_id) == treeTableId(old_tree_id));
    const trees = self.getTable(.{
        .namespace = namespace_id,
        .table_id = treeTableId(old_tree_id),
    }) orelse {
        left.release();
        right.release();
        return;
    };
    const old = blk: {
        const held = trees.mu.hold(.exclusive, @src());
        defer held.release();
        // gone (dropped) — nothing to split
        if (trees.byId(held.shared(), old_tree_id) == null) {
            left.release();
            right.release();
            return;
        }
        try trees.entries.ensureUnusedCapacity(trees.allocator, 2);
        const old = trees.removeTree(held, old_tree_id).?;
        trees.addTree(held, left) catch unreachable; // capacity reserved above
        trees.addTree(held, right) catch unreachable;
        break :blk old;
    };

    // The list's ref on the old object. Readers that still hold it keep
    // its full range and tables; the last of them closes it.
    old.release();
}

/// splitTreeForTests splits the given tree at the median key of its
/// bottommost level, locally, without going through Raft. **Test-only**
/// — production must propose splits through the manifest pipeline so
/// all nodes apply the same split deterministically; see
/// `proposeSplit` in the engine. Takes the locks it needs itself, like
/// applySplit on the apply path.
pub fn splitTreeForTests(self: *Forest, namespace_id: u64, tree_id: u64) !void {
    var arena = std.heap.ArenaAllocator.init(self.allocator);
    defer arena.deinit();
    const mc = try self.buildSplit(
        arena.allocator(),
        namespace_id,
        tree_id,
    ) orelse return;
    // Test-only path: no raft entry behind it, so index 0.
    try self.applySplit(0, mc);
}
