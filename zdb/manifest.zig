// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const log = std.log.scoped(.manifest);
const _zen = @import("buffers.gen.zig");
const Key = @import("kv.zig").Key;
const KeyRange = @import("kv.zig").KeyRange;
const TreeKey = @import("kv.zig").TreeKey;
const TableKey = @import("kv.zig").TableKey;
const treeTableId = @import("kv.zig").treeTableId;
const treeSplitId = @import("kv.zig").treeSplitId;
const makeTreeId = @import("kv.zig").makeTreeId;
const fiber = @import("zfiber");
const utils = @import("utils");
const vfs = @import("vfs");
pub const Extent = @import("table/index.zig").Extent;
const String = utils.String;

/// The manifest lock. Shared for the copy-out readers, exclusive for
/// the apply path's commits.
pub const Lock = fiber.RWMutex(.tracked);

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
    tables: std.AutoHashMap(u64, void), // set of sst_ids at this level
};

/// What the manifest knows about one SST file, independent of which trees
/// list it.
pub const SstInfo = struct {
    /// Log index of the entry that first listed this id. L0 recency comes
    /// from this, not from the id: ids are random, log indexes are not.
    raft_index: u64,
    /// Size and index start, from the CREATE that first listed the file;
    /// every later open (apply, recovery, re-list, snapshot) reads just
    /// that extent.
    extent: Extent,
    /// How many (tree, level) listings name this id. A split hands the
    /// same file to a second tree, so one file can be listed more than
    /// once; the record dies when the last listing goes.
    refs: u32,
};

pub const TreeState = struct {
    namespace_id: u64 = 0,
    // The SQL table is derived from the tree_id (Key.treeTableId), not
    // stored — the tree_id is the map key alongside namespace_id (Key.TreeKey).
    levels: std.ArrayList(LevelState),
    left: Key = Key.inf,
    right: Key = Key.inf,
    /// See `buffers.TreeKind`. Set at create, inherited by a split's new
    /// tree, kept by the snapshot manifest.
    kind: TreeKind = .none,
};

pub const TreeKind = _zen.TreeKindEnum;

/// table_ids 0–15 are reserved per namespace (0 = raw-KV / default table,
/// 1 = the SQL column catalog, 2 = the index catalog, 3 = names, the rest
/// for future system tables); user tables start here. nextTableId floors
/// every namespace's counter at this.
pub const first_table_id: u32 = 16;

/// The namespace's SQL catalog table (schema rows for its tables). Its tree
/// is created with the namespace itself (see forest.buildNsCreate).
pub const catalog_table_id: u32 = 1;

/// The namespace's SQL index catalog (one row per index). Like the catalog,
/// its tree is born with the namespace.
pub const index_catalog_table_id: u32 = 2;

/// Names, in every namespace that has them: in a database, its table
/// and index names (name -> table_id); in the system namespace, the
/// database names (name -> namespace id). Rows the SQL layer owns; zdb
/// only creates the tree.
pub const names_table_id: u32 = 3;

/// Every tree a database namespace is born with, by table_id: the
/// default raw-KV table, the SQL catalogs and the names table.
/// forest.buildNsCreate and the genesis seeder both loop over this —
/// extend the list to add a system tree.
pub const namespace_system_table_ids = [_]u32{
    0,
    catalog_table_id,
    index_catalog_table_id,
    names_table_id,
};

/// The system namespace: not a database, so it has no raw-KV table and
/// no SQL catalog, only the trees in `system_namespace_trees`. Genesis
/// creates it; no login can reach it.
pub const system_namespace: u64 = 1;

/// The system namespace's trees, by table_id. Kept apart from
/// `namespace_system_table_ids`, which is what a database is born with.
pub const system_namespace_trees = [_]u32{names_table_id};

/// namespaces 1–15 are reserved for the engine (1 is the system
/// namespace). User databases start here. nextNamespace floors
/// next_ns_id at this value.
pub const first_namespace: u64 = 16;

/// In-memory manifest state. Durability comes from the Raft log + periodic
/// snapshots — this struct is never persisted to its own file. `serialize`
/// produces the bytes that go into a Raft snapshot's `data` field;
/// `restoreFrom` is the inverse, used by recovery to install the manifest
/// before WAL replay.
///
/// The manifest also owns SST file lifetime: a file lives as long as some
/// tree lists it, and the entry that removes its last listing retires it
/// (see `to_delete`). `Forest.deleteFiles` unlinks a retired file once the
/// node's durable snapshot index has passed that entry's index.
pub const Manifest = struct {
    allocator: std.mem.Allocator,
    trees: std.AutoHashMap(TreeKey, TreeState),
    /// Per-SST records, keyed by sst_id. One entry per file any tree
    /// lists; guarded by `mu` like `trees`.
    ssts: std.AutoHashMap(u64, SstInfo),
    /// Files to delete from disk: every file that left the manifest,
    /// keyed by sst_id → the log index of the entry whose apply removed
    /// its last listing. `takeToDelete` hands them to
    /// `Forest.deleteFiles`, which unlinks each one once the node's
    /// durable snapshot index reaches that entry; until then a restart
    /// may replay an entry that opens the file again.
    ///
    /// Not persisted, and not part of `serialize`/`restoreFrom`: a snapshot
    /// manifest lists only live files, and recovery re-records every
    /// entry above the snapshot index because replay re-applies them.
    /// A record at or below the snapshot index is lost with the map, which
    /// is what `Forest.removeOrphans` cleans up at startup.
    to_delete: std.AutoHashMap(u64, u64),
    /// Files to evict from memory: the ids whose last listing went since
    /// the last `takeToEvict`, in order. Written at the same point as
    /// `to_delete` (`decrSST`), for a different consumer: the forest
    /// drains it after every apply (`Forest.evictTables`) and drops each
    /// file's table cache entry right away, and the table's blocks go
    /// with its last ref. Memory does not wait for the snapshot; the
    /// file does. Per-process, like `to_delete`; a draft manifest's list
    /// dies with the draft.
    to_evict: std.ArrayList(u64),
    /// Per-namespace manifest state (see `Namespace`), snapshot-persisted and
    /// maintained by the apply paths on every node — so a re-elected leader
    /// never re-issues a table_id. A namespace with no entry allocates from
    /// `first_table_id`. Names are not here: they are rows the SQL layer
    /// owns (table 3, `names_table_id`).
    namespaces: std.AutoHashMap(u64, Namespace),
    /// Next-free namespace id (global). Same allocate-on-leader / advance-on-
    /// apply / snapshot-persist discipline as the per-namespace table_id
    /// counter, one level up.
    next_ns_id: u64 = first_namespace,
    mu: Lock = .{},

    pub const Namespace = struct {
        /// Next-free user table_id: the only authoritative source of
        /// table_id allocation — bumped on apply of every create
        /// (advanceTableId), reserved by the leader (nextTableId).
        next_table_id: u32 = first_table_id,
    };

    pub fn init(allocator: std.mem.Allocator) Manifest {
        return .{
            .allocator = allocator,
            .trees = std.AutoHashMap(TreeKey, TreeState).init(allocator),
            .ssts = std.AutoHashMap(u64, SstInfo).init(allocator),
            .to_delete = std.AutoHashMap(u64, u64).init(allocator),
            .to_evict = .empty,
            .namespaces = std.AutoHashMap(u64, Namespace).init(allocator),
        };
    }

    /// Reserve the next namespace id (leader path). Floors at
    /// `first_namespace`. Like table_ids, a burned id (failed propose) is never
    /// reused.
    pub fn nextNamespace(self: *Manifest) u64 {
        self.mu.lock(@src());
        defer self.mu.unlock();
        if (self.next_ns_id < first_namespace)
            self.next_ns_id = first_namespace;
        const id = self.next_ns_id;
        self.next_ns_id = id + 1;
        return id;
    }

    /// Raise the namespace counter past `namespace` (apply path, all nodes).
    /// Idempotent; never lowers it; floors at first_namespace.
    pub fn advanceNamespace(self: *Manifest, namespace: u64) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        if (self.next_ns_id < first_namespace)
            self.next_ns_id = first_namespace;
        const next = namespace +| 1;
        if (next > self.next_ns_id) self.next_ns_id = next;
    }

    /// Reserve the next table_id for `namespace` (leader path). Floors at
    /// `first_table_id` for a namespace's first user table. The reserved id is
    /// only a hint until the create_tree commits — a burned id (failed propose)
    /// is never reused because advanceTableId only ever raises the counter.
    pub fn nextTableId(self: *Manifest, namespace: u64) !u32 {
        self.mu.lock(@src());
        defer self.mu.unlock();

        const slot = try self.namespaces.getOrPut(namespace);
        if (!slot.found_existing) slot.value_ptr.* = .{};

        // Hand out the current counter (never below the floor), reserve next.
        const id = @max(slot.value_ptr.next_table_id, first_table_id);
        slot.value_ptr.next_table_id = id + 1;
        return id;
    }

    /// Raise the table's namespace counter past its table_id (apply path,
    /// all nodes). Idempotent; never lowers it (so re-applying or applying
    /// a low system table leaves a higher user counter untouched). Floors
    /// at first_table_id.
    pub fn advanceTableId(
        self: *Manifest,
        table_key: TableKey,
    ) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();

        const slot = try self.namespaces.getOrPut(table_key.namespace);
        if (!slot.found_existing) slot.value_ptr.* = .{};

        // Floor the current counter, then raise it past table_id; never lowers.
        const floor = @max(slot.value_ptr.next_table_id, first_table_id);
        slot.value_ptr.next_table_id = @max(floor, table_key.table_id +| 1);
    }

    /// Forget `namespace`'s table_id counter (drop_ns apply path). Safe
    /// because namespace ids are never reused: no later create can see the
    /// missing entry as a lowered floor.
    pub fn dropNamespace(self: *Manifest, namespace: u64) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        _ = self.namespaces.remove(namespace);
    }

    /// A tree's key range, duped into `alloc` under the manifest lock
    /// (`inf` set as `Tree.open` sets it: both bounds empty). `commit` on
    /// the apply loop rehashes `trees` and, on a delete or a range update,
    /// frees the very bytes a stored `TreeState` borrows — so a caller off
    /// that loop copies under the lock instead of holding a `TreeState`
    /// across its own work. Null if the tree is not listed.
    /// What the tree holds, null for a tree the manifest does not list.
    pub fn treeKind(self: *Manifest, key: TreeKey) ?TreeKind {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        const tree = self.trees.getPtr(key) orelse return null;
        return tree.kind;
    }

    pub fn treeRange(
        self: *Manifest,
        alloc: std.mem.Allocator,
        key: TreeKey,
    ) !?KeyRange {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        const tree = self.trees.getPtr(key) orelse return null;
        const range = KeyRange{ .left = tree.left, .right = tree.right };
        return try range.dupe(alloc);
    }

    /// Every listed tree's key — one namespace's when `namespace` is
    /// given — copied out under the lock. Caller frees the slice.
    pub fn treeKeys(
        self: *Manifest,
        alloc: std.mem.Allocator,
        namespace: ?u64,
    ) ![]TreeKey {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        var out: std.ArrayList(TreeKey) = .empty;
        errdefer out.deinit(alloc);
        var it = self.trees.keyIterator();
        while (it.next()) |tk| {
            if (namespace) |ns| if (tk.namespace != ns) continue;
            try out.append(alloc, tk.*);
        }
        return out.toOwnedSlice(alloc);
    }

    /// A fresh SST file id, drawn from the process CSPRNG.
    ///
    /// Ids are unique by probability: with n ids in play the chance that
    /// any two of them collide is about n²/2⁶⁵, which stays negligible
    /// for any number of files a cluster can hold. Zero is skipped — it
    /// is the "no id" sentinel. A draw that hits an id the manifest
    /// already lists is redrawn, which makes a collision with a known id
    /// impossible rather than merely unlikely. Both sets are checked:
    /// `ssts` for a live file, `to_delete` for one whose file may still sit
    /// on disk under that name until the next sweep.
    pub fn newFileId(self: *Manifest) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();

        while (true) {
            var bytes: [8]u8 = undefined;
            utils.context.g().io.random(&bytes);
            const id = std.mem.readInt(u64, &bytes, .little);

            if (id == 0 or self.ssts.contains(id)) continue;
            if (self.to_delete.contains(id)) continue;
            return id;
        }
    }

    /// Whether the manifest knows `id` at all — listed by some tree, or
    /// retired and awaiting its unlink. `Forest.removeOrphans` asks this of
    /// every SST-shaped name it finds in the store; anything unknown is
    /// not the manifest's file.
    pub fn knowsFile(self: *Manifest, id: u64) bool {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.ssts.contains(id) or self.to_delete.contains(id);
    }

    /// The ids of the SST-named regular files in `entries` that the
    /// manifest does not know: neither listed by a tree nor retired and
    /// awaiting its unlink. Caller frees the slice.
    ///
    /// Takes a `.names` listing, no stat: a store holds hundreds of
    /// thousands of SSTs and nearly all of them are the manifest's, so
    /// the caller stats just the few ids this returns. One shared hold
    /// for the whole pass rather than a lock per name.
    ///
    /// Unknown is not yet orphan: a file some node is still writing, or
    /// has written but not yet proposed, is in no manifest either. The
    /// caller's cutoff (`Forest.removeOrphans`) decides that.
    pub fn unknownFiles(
        self: *Manifest,
        alloc: std.mem.Allocator,
        entries: []const vfs.ObjectInfo,
    ) ![]u64 {
        var out: std.ArrayList(u64) = .empty;
        errdefer out.deinit(alloc);

        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        for (entries) |entry| {
            const id = parseSstName(entry.name) orelse continue;
            if (self.ssts.contains(id) or self.to_delete.contains(id)) continue;
            try out.append(alloc, id);
        }
        return out.toOwnedSlice(alloc);
    }

    /// The file id in an SST filename (`<id:016x>.sst`), or null when the
    /// name is not one — a WAL segment, a lock file, anything else a
    /// directory holds. Those are never the manifest's files.
    fn parseSstName(name: String) ?u64 {
        const ext = ".sst";
        if (name.len != 16 + ext.len) return null;
        if (!std.mem.endsWith(u8, name, ext)) return null;
        return std.fmt.parseInt(u64, name[0..16], 16) catch null;
    }

    /// One retired file: its id and the log index of the entry that removed
    /// its last listing.
    pub const ToDelete = struct { sst_id: u64, index: u64 };

    /// Take every file retired at or below `max_index` out of the `to_delete` map.
    /// Caller owns (and frees) the returned slice; an entry is handed out
    /// exactly once, so the caller is the only one that can unlink it.
    pub fn takeToDelete(
        self: *Manifest,
        alloc: std.mem.Allocator,
        max_index: u64,
    ) ![]ToDelete {
        const held = self.mu.hold(.exclusive, @src());
        defer held.release();

        var out: std.ArrayList(ToDelete) = .empty;
        errdefer out.deinit(alloc);
        var it = self.to_delete.iterator();
        while (it.next()) |kv| {
            if (kv.value_ptr.* > max_index) continue;
            try out.append(alloc, .{
                .sst_id = kv.key_ptr.*,
                .index = kv.value_ptr.*,
            });
        }
        for (out.items) |r| _ = self.to_delete.remove(r.sst_id);
        return out.toOwnedSlice(alloc);
    }

    /// The extent a listed file was created with. Caller holds the lock.
    pub fn extent(self: *Manifest, held: Lock.Shared, sst_id: u64) ?Extent {
        self.mu.assertHeld(held);
        const info = self.ssts.get(sst_id) orelse return null;
        return info.extent;
    }

    /// A tree's manifest record, borrowed for as long as `held` is: the
    /// record's key bytes are freed by a delete or range update, so this
    /// exists only under a caller's hold. Null if the tree is not listed.
    pub fn treeState(
        self: *Manifest,
        held: Lock.Shared,
        key: TreeKey,
    ) ?*const TreeState {
        self.mu.assertHeld(held);
        return self.trees.getPtr(key);
    }

    fn freeTree(self: *Manifest, tree_state: *TreeState) void {
        for (tree_state.levels.items) |*lm| lm.tables.deinit();
        tree_state.levels.deinit(self.allocator);
        if (tree_state.left.primary.len > 0)
            self.allocator.free(tree_state.left.primary);
        if (tree_state.right.primary.len > 0)
            self.allocator.free(tree_state.right.primary);
    }

    pub fn deinit(self: *Manifest) void {
        var it = self.trees.iterator();
        while (it.next()) |kv| self.freeTree(kv.value_ptr);
        self.trees.deinit();
        self.ssts.deinit();
        self.to_delete.deinit();
        self.to_evict.deinit(self.allocator);
        self.namespaces.deinit();
        self.mu.deinit();
    }

    /// Applies a single table CREATE or DELETE to the tree's levels, and
    /// maintains the file's `SstInfo`. `index` is the log index of the
    /// entry being applied; a CREATE that does not carry its own
    /// `raft_index` (every live proposal) takes it. Caller holds the
    /// manifest lock exclusively — `held` is the proof.
    pub fn applySSTableChange(
        self: *Manifest,
        held: Lock.Exclusive,
        index: u64,
        tc: SSTableChange,
    ) !void {
        self.mu.assertHeld(held);
        const key = TreeKey{
            .namespace = tc.namespace_id,
            .tree_id = tc.tree_id,
        };
        if (tc.op == .create) {
            const tree = self.trees.getPtr(key) orelse
                return ManifestError.TreeNotFound;
            while (tree.levels.items.len <= @as(usize, tc.level)) {
                const lm = LevelState{
                    .tables = std.AutoHashMap(u64, void).init(self.allocator),
                };
                try tree.levels.append(self.allocator, lm);
            }
            // Already listed at this level: a re-apply during WAL replay
            // over snapshot-restored state must not bump refs a second time.
            if (tree.levels.items[tc.level].tables.contains(tc.sst_id)) return;
            try tree.levels.items[tc.level].tables.put(tc.sst_id, {});
            // A split re-lists a file another tree already holds: that
            // finds the existing record and keeps its original
            // raft_index, so the file's L0 recency never moves.
            const slot = try self.ssts.getOrPut(tc.sst_id);
            if (!slot.found_existing) slot.value_ptr.* = .{
                .raft_index = if (tc.raft_index != 0) tc.raft_index else index,
                .extent = .{
                    .size = tc.size,
                    .index_offset = tc.index_offset,
                },
                .refs = 0,
            };
            slot.value_ptr.refs += 1;
        } else if (tc.op == .delete) {
            const tree = self.trees.getPtr(key) orelse
                return ManifestError.TreeNotFound;
            // The named level first: a trivial move lists the file at
            // two levels of one tree while its entry applies (creates go
            // first), and the delete must drop the source listing, not
            // whichever level a scan meets first.
            if (tc.level < tree.levels.items.len and
                tree.levels.items[tc.level].tables.remove(tc.sst_id))
            {
                try self.decrSST(index, tc.sst_id);
                return;
            }
            // Scan levels to find and remove the table.
            for (tree.levels.items) |*lm| {
                if (lm.tables.remove(tc.sst_id)) {
                    try self.decrSST(index, tc.sst_id);
                    break;
                }
            }
        }
    }

    /// Drop one listing of `id`, forgetting the file once the last one
    /// goes. `index` is the log index of the entry being applied. This is
    /// the single point at which a file is queued in `to_evict` and
    /// `to_delete`: every path that can remove a listing (SST delete,
    /// tree delete) goes through here, so a file can never leave the
    /// manifest without both records.
    /// Caller holds the manifest lock exclusively.
    fn decrSST(self: *Manifest, index: u64, sst_id: u64) !void {
        // Every listed id has a record; a delete for an unknown id is a
        // bug in the change that carried it, not a case to tolerate.
        const info = self.ssts.getPtr(sst_id).?;
        std.debug.assert(info.refs > 0);
        info.refs -= 1;
        if (info.refs != 0) return;

        _ = self.ssts.remove(sst_id);
        try self.to_evict.append(self.allocator, sst_id);
        // index 0 means "no raft entry behind this apply" (restoreFrom,
        // and the single-node/test paths that pass 0 to `commit`). There
        // is no snapshot index that can ever pass 0, so retiring the file
        // would only pin it in the map forever; leave it to
        // `Forest.removeOrphans`.
        if (index == 0) return;
        try self.to_delete.put(sst_id, index);
    }

    /// Hand over the ids unlisted since the last call, oldest first, and
    /// forget them. Freed by the caller with the manifest's allocator.
    pub fn takeToEvict(self: *Manifest) ![]u64 {
        self.mu.lock(@src());
        defer self.mu.unlock();
        return self.to_evict.toOwnedSlice(self.allocator);
    }

    /// The log index of the entry that first listed `sst_id`. Asking for
    /// an id the manifest does not list is a bug, so it panics.
    ///
    /// Only for installing a file that already exists — `Tree.open`
    /// reading a level list, and the split that re-lists the parent's
    /// files under the child. A path installing a NEW file (flush,
    /// compaction, backfill) uses its own applying index instead: that is
    /// what the manifest stored for it a moment earlier.
    pub fn raftIndexOf(self: *Manifest, sst_id: u64) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.ssts.get(sst_id).?.raft_index;
    }

    /// Applies a tree CREATE, DELETE, or UPDATE.
    /// DELETE automatically removes all tables owned by the tree, retiring
    /// each one whose last listing this delete takes away. UPDATE changes
    /// the tree's key range. `index` is the log index of the applying entry
    /// (only the DELETE arm reads it). Caller holds the manifest lock
    /// exclusively — `held` is the proof.
    pub fn applyTreeChange(
        self: *Manifest,
        held: Lock.Exclusive,
        index: u64,
        tc: TreeChange,
    ) !void {
        self.mu.assertHeld(held);
        const key = TreeKey{
            .namespace = tc.namespace_id,
            .tree_id = tc.tree_id,
        };
        if (tc.op == .create) {
            // Every create names what the tree holds; the filter, move
            // and rollup rules all read it.
            std.debug.assert(tc.kind != .none);
            // A re-apply during WAL replay over snapshot-restored state is a
            // no-op: putting over the existing record would leak its keys.
            if (self.trees.contains(key)) return;
            try self.trees.put(key, .{
                .namespace_id = tc.namespace_id,
                .levels = std.ArrayList(LevelState).empty,
                .left = try Key.initAlloc(self.allocator, tc.left_key),
                .right = try Key.initAlloc(self.allocator, tc.right_key),
                .kind = tc.kind,
            });
        } else if (tc.op == .delete) {
            if (self.trees.fetchRemove(key)) |kv| {
                var tree = kv.value;
                // The record is already out of the map, so its levels and
                // keys go whether or not a retire below fails to record.
                defer self.freeTree(&tree);
                for (tree.levels.items) |lm| {
                    var tit = lm.tables.keyIterator();
                    while (tit.next()) |id| try self.decrSST(index, id.*);
                }
            }
        } else if (tc.op == .update) {
            const tree = self.trees.getPtr(key) orelse
                return ManifestError.TreeNotFound;
            tree.left.free(self.allocator);
            tree.right.free(self.allocator);

            tree.left = try Key.initAlloc(self.allocator, tc.left_key);
            tree.right = try Key.initAlloc(self.allocator, tc.right_key);
        }
    }

    /// Apply a batch of tree + table changes under the mutex. In-memory only:
    /// durability comes from the surrounding Raft log entry, not from any
    /// disk write here. `index` is that entry's log index: a new SST takes
    /// it as its `raft_index`, which is what orders L0. Paths with no raft
    /// entry behind them (single-node `createTree`, tests) pass 0.
    pub fn commit(
        self: *Manifest,
        index: u64,
        table_changes: []const SSTableChange,
        tree_changes: []const TreeChange,
    ) !void {
        if (table_changes.len == 0 and tree_changes.len == 0) return;

        const held = self.mu.hold(.exclusive, @src());
        defer held.release();

        for (tree_changes) |tc| try self.applyTreeChange(held, index, tc);
        try self.applySSTableChanges(held, index, table_changes);
    }

    /// Apply one entry's SST changes: every create, then every delete.
    /// A split lists a moved file twice in one entry, a delete from the
    /// old tree and a create in the new one. Applied in that order the
    /// delete would take the refcount to zero and retire the file, and
    /// the create would re-list it with the retire record still standing:
    /// the next sweep would unlink a listed file. Creates first keep the
    /// record alive (refs 1 -> 2 -> 1), so a file is retired only when an
    /// entry leaves it with no listing at all.
    fn applySSTableChanges(
        self: *Manifest,
        held: Lock.Exclusive,
        index: u64,
        table_changes: []const SSTableChange,
    ) !void {
        for (table_changes) |tc| {
            if (tc.op == .create) try self.applySSTableChange(held, index, tc);
        }
        for (table_changes) |tc| {
            if (tc.op != .create) try self.applySSTableChange(held, index, tc);
        }
    }

    /// Apply one whole ManifestChange: its structural changes (trees, then
    /// SSTs — trees first, because an SST can only attach to a tree that
    /// exists) plus the id-counter effects its `cmd` implies. This is the
    /// single implementation of what a committed DDL entry does to
    /// manifest state: the live apply path (Forest.applyTrees) and the
    /// snapshot draft replay both go through it, so a counter can never
    /// be advanced on one path and missed on the other. The entry's
    /// `rows` are not manifest state: the engine writes them. `index` is
    /// the log index of the applying entry, used as a new SST's
    /// `raft_index` exactly as in `commit`.
    ///
    /// Two phases: the structural changes under one exclusive hold, then
    /// the counter helpers, each of which takes the lock itself.
    /// Idempotent — re-applying an entry during WAL replay over
    /// snapshot-restored state changes nothing.
    pub fn apply(self: *Manifest, index: u64, mc: ManifestChange) !void {
        if (mc.cmd == .branch_ns) return self.applyNsBranch(mc);
        if (mc.cmd == .branch_table) return self.applyTableBranch(mc);
        {
            const held = self.mu.hold(.exclusive, @src());
            defer held.release();
            for (mc.tree_changes) |tc|
                try self.applyTreeChange(held, index, tc);
            try self.applySSTableChanges(held, index, mc.sstable_changes);
        }

        switch (mc.cmd) {
            .create_ns => for (mc.tree_changes) |tc| {
                if (tc.op != .create) continue;
                self.advanceNamespace(tc.namespace_id);
            },
            .create_tree => for (mc.tree_changes) |tc| {
                if (tc.op != .create) continue;
                try self.advanceTableId(.{
                    .namespace = tc.namespace_id,
                    .table_id = treeTableId(tc.tree_id),
                });
            },
            .drop_ns => if (mc.tree_changes.len > 0) {
                self.dropNamespace(mc.tree_changes[0].namespace_id);
            },
            else => {},
        }
    }

    /// branch_ns: the target namespace (`mc.tree_changes[0]`, which
    /// carries its id) gets a tree for every tree of `mc.branch_from`
    /// with the same id, range, kind and files. A file listed again
    /// keeps its record and its raft_index, so the target's L0 reads in
    /// the source's order, and its refcount keeps it on disk until both
    /// let go. The source's table-id counter comes along, so the target
    /// never reuses an id the source minted; its table names come with
    /// the names tree. Read from this
    /// node's manifest, which every node holds identically at this
    /// index. A target that already has trees was applied before
    /// (replay): nothing changes.
    fn applyNsBranch(self: *Manifest, mc: ManifestChange) !void {
        std.debug.assert(mc.tree_changes.len == 1);
        const target = mc.tree_changes[0];
        const dst = target.namespace_id;
        const src = mc.branch_from;
        {
            const held = self.mu.hold(.exclusive, @src());
            defer held.release();

            // The keys first: `put` below may rehash the map.
            var keys: std.ArrayList(TreeKey) = .empty;
            defer keys.deinit(self.allocator);
            var it = self.trees.keyIterator();
            while (it.next()) |tk| {
                if (tk.namespace == dst) return; // This ns was already applied.
                if (tk.namespace == src) try keys.append(self.allocator, tk.*);
            }
            for (keys.items) |sk| try self.copyTree(sk, .{
                .namespace = dst,
                .tree_id = sk.tree_id,
            });

            // The counter, under the same hold.
            const dst_slot = try self.namespaces.getOrPut(dst);
            if (!dst_slot.found_existing) dst_slot.value_ptr.* = .{};
            if (self.namespaces.getPtr(src)) |src_ns|
                dst_slot.value_ptr.next_table_id = src_ns.next_table_id;
        }
        self.advanceNamespace(dst);
    }

    /// branch_table: each change names a new table (its tree-0 change,
    /// for the id) and `from_table_id`, the table whose trees it lists
    /// again, split by split, with the same ranges, kinds and files, as
    /// applyBranch does for a namespace. Then the counter, as a
    /// create_tree. A new table that already has
    /// trees was applied before (replay): nothing changes.
    fn applyTableBranch(self: *Manifest, mc: ManifestChange) !void {
        {
            const held = self.mu.hold(.exclusive, @src());
            defer held.release();
            for (mc.tree_changes) |tc| {
                const ns = tc.namespace_id;
                const dst_tid = treeTableId(tc.tree_id);
                // The keys first: `put` below may rehash the map.
                var keys: std.ArrayList(TreeKey) = .empty;
                defer keys.deinit(self.allocator);
                var it = self.trees.keyIterator();
                while (it.next()) |tk| {
                    if (tk.namespace != ns) continue;
                    if (treeTableId(tk.tree_id) == dst_tid) return;
                    if (treeTableId(tk.tree_id) == tc.from_table_id)
                        try keys.append(self.allocator, tk.*);
                }
                for (keys.items) |sk| try self.copyTree(sk, .{
                    .namespace = ns,
                    .tree_id = makeTreeId(dst_tid, treeSplitId(sk.tree_id)),
                });
            }
        }
        for (mc.tree_changes) |tc| {
            try self.advanceTableId(.{
                .namespace = tc.namespace_id,
                .table_id = treeTableId(tc.tree_id),
            });
        }
    }

    /// Lists tree `sk` again as `dk`, a key no tree holds yet: a copy of
    /// its record, every file it lists taking one more ref. Caller
    /// holds the lock exclusively.
    fn copyTree(self: *Manifest, sk: TreeKey, dk: TreeKey) !void {
        var tree: TreeState = .{
            .namespace_id = dk.namespace,
            .levels = .empty,
        };
        errdefer self.freeTree(&tree);
        {
            const s = self.trees.getPtr(sk).?;
            tree.left = try s.left.dupe(self.allocator);
            tree.right = try s.right.dupe(self.allocator);
            tree.kind = s.kind;
            for (s.levels.items) |lm| {
                var copy = LevelState{
                    .tables = std.AutoHashMap(u64, void).init(self.allocator),
                };
                errdefer copy.tables.deinit();
                var tit = lm.tables.keyIterator();
                while (tit.next()) |id| try copy.tables.put(id.*, {});
                try tree.levels.append(self.allocator, copy);
            }
        }
        try self.trees.put(dk, tree);
        for (tree.levels.items) |lm| {
            var tit = lm.tables.keyIterator();
            while (tit.next()) |id| self.ssts.getPtr(id.*).?.refs += 1;
        }
    }

    /// Count total table entries across all trees and levels.
    pub fn tableCount(self: *Manifest) u32 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        var count: u32 = 0;
        var it = self.trees.iterator();
        while (it.next()) |kv| {
            for (kv.value_ptr.levels.items) |lm| {
                count += @intCast(lm.tables.count());
            }
        }
        return count;
    }

    /// Serialize the entire Manifest into a self-describing byte blob
    /// suitable for embedding in a Raft snapshot's `data` field. Captures
    /// every tree, every per-level table list, the tree key ranges + namespace
    /// assignments, the per-namespace `next_table_id` high-water-marks, and
    /// each SST's `raft_index` (restore needs it to rebuild L0 order). No
    /// file-id counter is persisted — ids are random. Tree ids are derived
    /// (table_id<<32 | split_id), so there is no tree-id counter either.
    ///
    /// Wire format reuses the existing `ManifestChange` zbuf shape: a list of
    /// `tree_changes` (CREATE per tree), `sstable_changes` (CREATE per SST),
    /// and `next_table_ids` (one per namespace).
    pub fn serialize(self: *Manifest, allocator: std.mem.Allocator) ![]u8 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();

        var tree_changes: std.ArrayList(TreeChange) = .empty;
        var table_changes: std.ArrayList(SSTableChange) = .empty;

        var it = self.trees.iterator();
        while (it.next()) |kv| {
            const tk = kv.key_ptr.*;
            const tree = kv.value_ptr.*;
            const left_raw = try tree.left.marshal(a);
            const right_raw = try tree.right.marshal(a);
            try tree_changes.append(a, createTreeChange(
                tk.tree_id,
                tk.namespace,
                left_raw,
                right_raw,
                tree.kind,
            ));
            for (tree.levels.items, 0..) |lm, level_idx| {
                var tit = lm.tables.iterator();
                while (tit.next()) |tkv| {
                    const info = self.ssts.get(tkv.key_ptr.*).?;
                    var sst_change = createSSTableChange(
                        tkv.key_ptr.*,
                        tk.namespace,
                        tk.tree_id,
                        @intCast(level_idx),
                        info.extent,
                    );
                    sst_change.raft_index = info.raft_index;
                    try table_changes.append(a, sst_change);
                }
            }
        }

        var namespaces: std.ArrayList(_zen.NamespaceStruct) = .empty;
        var cit = self.namespaces.iterator();
        while (cit.next()) |kv| {
            try namespaces.append(a, .{
                .id = kv.key_ptr.*,
                .next_table_id = kv.value_ptr.next_table_id,
            });
        }

        // The arena holds the source changes; marshalRoot copies them into a
        // fresh `allocator`-owned payload, so the arena can be torn down after.
        return _zen.marshalRoot(allocator, ManifestChange{
            .tree_changes = tree_changes.items,
            .sstable_changes = table_changes.items,
            .namespaces = namespaces.items,
            .next_ns_id = self.next_ns_id,
        });
    }

    /// Replace the receiver's tree contents with the deserialized state from
    /// `bytes` (as produced by `serialize`). Failure mid-decode leaves the
    /// state empty, not partial. Recovery-time, before the apply barrier
    /// lifts: nothing else touches the manifest meanwhile.
    pub fn restoreFrom(self: *Manifest, bytes: String) !void {
        const reader = try _zen.ManifestChangeReader.parseRoot(bytes);
        {
            const held = self.mu.hold(.exclusive, @src());
            defer held.release();

            // Drop existing trees + namespace state first; restore is a full
            // replace.
            var it = self.trees.iterator();
            while (it.next()) |kv| self.freeTree(kv.value_ptr);
            self.trees.clearRetainingCapacity();
            self.ssts.clearRetainingCapacity();
            // A snapshot manifest lists only live files, so nothing is
            // retired at restore time. Replay of the entries above the
            // snapshot index re-records every retire above it.
            self.to_delete.clearRetainingCapacity();
            self.namespaces.clearRetainingCapacity();

            // Order matters: trees must exist before tables can attach to them.
            const tc_len = reader.treeChangesLen();
            var i: u32 = 0;
            while (i < tc_len) : (i += 1) {
                const tc = try reader.treeChangesAt(i);
                // index 0: a serialized manifest carries only
                // creates, so nothing here reaches `decrSST`.
                try self.applyTreeChange(held, 0, .{
                    .op = tc.op(),
                    .tree_id = tc.treeId(),
                    .namespace_id = tc.namespaceId(),
                    .left_key = @constCast(tc.leftKey()),
                    .right_key = @constCast(tc.rightKey()),
                    // Without this a restored rows tree came back as an
                    // index: fuse filters from then on, no moves.
                    .kind = tc.kind(),
                });
            }

            const tbl_len = reader.sstableChangesLen();
            i = 0;
            while (i < tbl_len) : (i += 1) {
                const tc = try reader.sstableChangesAt(i);
                // Every serialized create carries its own raft_index, so
                // the applying-entry index is irrelevant here.
                std.debug.assert(tc.raftIndex() != 0);
                try self.applySSTableChange(held, 0, .{
                    .op = tc.op(),
                    .sst_id = tc.sstId(),
                    .namespace_id = tc.namespaceId(),
                    .tree_id = tc.treeId(),
                    .level = tc.level(),
                    .size = tc.size(),
                    .index_offset = tc.indexOffset(),
                    .raft_index = tc.raftIndex(),
                });
            }

            const ns_len = reader.namespacesLen();
            i = 0;
            while (i < ns_len) : (i += 1) {
                const c = try reader.namespacesAt(i);
                const slot = try self.namespaces.getOrPut(c.id());
                if (!slot.found_existing) slot.value_ptr.* = .{};
                slot.value_ptr.next_table_id = c.nextTableId();
            }

            self.next_ns_id = @max(reader.nextNsId(), first_namespace);
        }
    }

    /// Deep-copy this manifest into `out`, a fresh `init`, so a snapshot
    /// draft never mutates the live state. The copy's `to_delete` map
    /// starts empty: retires belong to the live manifest that applied
    /// them, and the draft is never the thing that sweeps. `out` must be
    /// at its final address: the restore takes `out.mu`, and a tracked
    /// lock is registered by address on first acquire (see `Track`).
    pub fn cloneInto(self: *Manifest, out: *Manifest) !void {
        const bytes = try self.serialize(out.allocator);
        defer out.allocator.free(bytes);
        try out.restoreFrom(bytes);
    }
};

// -------------------------------------------------------------------------
// Change types — aliases to generated structs
// -------------------------------------------------------------------------

pub const SSTableChange = _zen.SSTableChangeStruct;
pub const TreeChange = _zen.TreeChangeStruct;
pub const ManifestChange = _zen.ManifestChangeStruct;
pub const Command = _zen.CommandEnum;

pub fn createSSTableChange(
    sst_id: u64,
    namespace_id: u64,
    tree_id: u64,
    level: u32,
    extent: Extent,
) SSTableChange {
    return .{
        .op = .create,
        .sst_id = sst_id,
        .namespace_id = namespace_id,
        .tree_id = tree_id,
        .level = level,
        .size = extent.size,
        .index_offset = extent.index_offset,
    };
}

pub fn deleteSSTableChange(
    sst_id: u64,
    namespace_id: u64,
    tree_id: u64,
    level: u32,
) SSTableChange {
    return .{
        .op = .delete,
        .sst_id = sst_id,
        .namespace_id = namespace_id,
        .tree_id = tree_id,
        .level = level,
    };
}

pub fn createTreeChange(
    tree_id: u64,
    namespace_id: u64,
    left_key: String,
    right_key: String,
    kind: TreeKind,
) TreeChange {
    return .{
        .op = .create,
        .tree_id = tree_id,
        .namespace_id = namespace_id,
        .left_key = @constCast(left_key),
        .right_key = @constCast(right_key),
        .kind = kind,
    };
}

/// A table to create, for Forest.buildTables: `kind` for a create,
/// `from_table_id` (set after) for a branch; the builder fills the ids.
pub fn newTableChange(kind: TreeKind) TreeChange {
    return .{ .op = .create, .kind = kind };
}

pub fn deleteTreeChange(tree_id: u64, namespace_id: u64) TreeChange {
    return .{ .op = .delete, .tree_id = tree_id, .namespace_id = namespace_id };
}

pub fn updateTreeChange(
    tree_id: u64,
    namespace_id: u64,
    left_key: String,
    right_key: String,
) TreeChange {
    return .{
        .op = .update,
        .tree_id = tree_id,
        .namespace_id = namespace_id,
        .left_key = @constCast(left_key),
        .right_key = @constCast(right_key),
    };
}
