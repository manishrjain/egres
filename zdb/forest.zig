const std = @import("std");
const zio = @import("zio");
const utils = @import("utils");
const options = @import("options.zig");
const manifest = @import("manifest.zig");
const level = @import("level.zig");
const table_mod = @import("table.zig");
const vfs = @import("vfs");
const tree_mod = @import("tree.zig");
const skl = @import("skl/skiplist.zig");

const Key = @import("key.zig").Key;
const KeyRange = level.KeyRange;
const Tree = tree_mod.Tree;
const TableCache = tree_mod.TableCache;
const Table = table_mod.Table;
const Item = table_mod.table.Item;
const SkipList = skl.SkipList;
const Builder = table_mod.builder.Builder;

// ---------------------------------------------------------------------------
// RouteTable
// ---------------------------------------------------------------------------

/// RouteTable maps keys to tree_ids within a single namespace.
/// Entries are kept sorted by range.left so we can binary-search for the
/// tree that owns a given key.
pub const RouteTable = struct {
    allocator: std.mem.Allocator,
    namespace_id: u32,
    entries: std.ArrayList(RouteEntry),

    pub const RouteEntry = struct {
        tree_id: u64,
        range: KeyRange,
    };

    pub fn init(allocator: std.mem.Allocator, namespace_id: u32) RouteTable {
        return .{
            .allocator = allocator,
            .namespace_id = namespace_id,
            .entries = .{},
        };
    }

    pub fn deinit(self: *RouteTable) void {
        self.entries.deinit(self.allocator);
    }

    /// Add a route entry and keep sorted by range.left.
    pub fn addRoute(self: *RouteTable, tree_id: u64, range: KeyRange) !void {
        try self.entries.append(self.allocator, .{ .tree_id = tree_id, .range = range });
        std.mem.sort(RouteEntry, self.entries.items, {}, struct {
            fn lt(_: void, a: RouteEntry, b: RouteEntry) bool {
                return a.range.left.order(b.range.left) == .lt;
            }
        }.lt);
    }

    /// Remove a route entry by tree_id.
    pub fn removeRoute(self: *RouteTable, tree_id: u64) void {
        var i: usize = self.entries.items.len;
        while (i > 0) {
            i -= 1;
            if (self.entries.items[i].tree_id == tree_id) {
                _ = self.entries.orderedRemove(i);
                return;
            }
        }
    }

    /// Update the range for an existing route.
    pub fn updateRange(self: *RouteTable, tree_id: u64, range: KeyRange) void {
        for (self.entries.items) |*e| {
            if (e.tree_id == tree_id) {
                e.range = range;
                break;
            }
        }
        std.mem.sort(RouteEntry, self.entries.items, {}, struct {
            fn lt(_: void, a: RouteEntry, b: RouteEntry) bool {
                return a.range.left.order(b.range.left) == .lt;
            }
        }.lt);
    }

    /// Find the tree_id that owns this key, or null if none.
    pub fn lookup(self: *const RouteTable, search_key: Key) ?u64 {
        if (self.entries.items.len == 1) return self.entries.items[0].tree_id;
        if (self.entries.items.len == 0) return null;

        var lo: usize = 0;
        var hi: usize = self.entries.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (self.entries.items[mid].range.left.order(search_key) != .gt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo == 0) return null;
        const candidate = &self.entries.items[lo - 1];
        if (candidate.range.inf) return candidate.tree_id;
        if (candidate.range.right.primary.len > 0 and search_key.order(candidate.range.right) == .gt) {
            return null;
        }
        return candidate.tree_id;
    }
};

// ---------------------------------------------------------------------------

pub const Memtables = struct {
    allocator: std.mem.Allocator,
    mu: std.Thread.RwLock,
    current: *SkipList,
    immutables: std.ArrayList(*SkipList), // oldest at index 0

    pub fn init(allocator: std.mem.Allocator, current: *SkipList) Memtables {
        return .{
            .allocator = allocator,
            .mu = .{},
            .current = current,
            .immutables = .{},
        };
    }

    pub fn deinit(self: *Memtables) void {
        self.current.release();
        for (self.immutables.items) |imm| imm.release();
        self.immutables.deinit(self.allocator);
    }

    pub fn put(self: *Memtables, key: Key, value: []const u8) !void {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        try self.current.put(key, value);
    }

    pub fn memoryUsage(self: *Memtables) usize {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.current.memoryUsage();
    }

    /// Look up a key across current and immutable memtables.
    /// Returns a duped Item owned by the caller.
    pub fn get(self: *Memtables, allocator: std.mem.Allocator, search_key: Key) !?Item {
        self.mu.lockShared();
        defer self.mu.unlockShared();

        if (self.current.get(search_key)) |item| {
            return try item.dupe(allocator);
        }
        // Newest first (highest index).
        var i: usize = self.immutables.items.len;
        while (i > 0) {
            i -= 1;
            if (self.immutables.items[i].get(search_key)) |item| {
                return try item.dupe(allocator);
            }
        }
        return null;
    }

    /// Freeze the current memtable: move it to immutables, create a new current.
    pub fn swapCurrentOut(self: *Memtables, new_id: u64) !void {
        self.mu.lock();
        defer self.mu.unlock();

        try self.immutables.append(self.allocator, self.current);
        const new_mt = try SkipList.init(self.allocator);
        new_mt.id = new_id;
        self.current = new_mt;
    }

    /// Remove and return the oldest immutable memtable (for flushing).
    /// Returns null if there are no immutables.
    pub fn popOldest(self: *Memtables) ?*SkipList {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.immutables.items.len == 0) return null;
        return self.immutables.orderedRemove(0);
    }

    /// Return the oldest immutable memtable without removing it.
    pub fn peekOldest(self: *Memtables) ?*SkipList {
        self.mu.lockShared();
        defer self.mu.unlockShared();

        if (self.immutables.items.len == 0) return null;
        return self.immutables.items[0];
    }
};

// ---------------------------------------------------------------------------
// Forest
// ---------------------------------------------------------------------------

pub const Forest = struct {
    allocator: std.mem.Allocator,
    opts: options.Options,
    ms: manifest.ForestState,
    next_tree_id: std.atomic.Value(u64),
    next_file_id: std.atomic.Value(u64),
    trees: std.AutoHashMap(u64, *Tree),
    ns_routes: std.AutoHashMap(u32, *RouteTable),
    table_cache: TableCache,
    mu: std.Thread.RwLock,
    memtables: Memtables,

    /// Open or create a forest from a directory.
    /// Rebuilds trees from the manifest state.
    /// Note: namespace routing is not persisted yet — routes must be
    /// re-established after open via createTree or addRoute.
    pub fn open(allocator: std.mem.Allocator, opts: options.Options) !*Forest {
        var ms = try manifest.openOrCreate(allocator, opts.dir);
        errdefer ms.close();

        const self = try allocator.create(Forest);
        errdefer allocator.destroy(self);

        // Allocate memtable; id will be updated after scanning max IDs.
        const mt = try SkipList.init(allocator);
        errdefer mt.release();

        self.* = .{
            .allocator = allocator,
            .opts = opts,
            .ms = ms,
            .next_tree_id = std.atomic.Value(u64).init(1),
            .next_file_id = std.atomic.Value(u64).init(1),
            .trees = std.AutoHashMap(u64, *Tree).init(allocator),
            .ns_routes = std.AutoHashMap(u32, *RouteTable).init(allocator),
            .table_cache = TableCache.init(allocator, opts.dir),
            .mu = .{},
            .memtables = Memtables.init(allocator, mt),
        };
        errdefer self.trees.deinit();
        errdefer self.ns_routes.deinit();
        errdefer self.table_cache.deinit();

        var max_tid: u64 = 0;
        var max_fid: u64 = 0;
        {
            var it = self.ms.trees.iterator();
            while (it.next()) |kv| {
                const tree_id = kv.key_ptr.*;
                const tree_state = kv.value_ptr.*;
                if (tree_id > max_tid) max_tid = tree_id;

                // Track the highest table ID to seed next_file_id.
                for (tree_state.levels.items) |lm| {
                    var fit = lm.tables.keyIterator();
                    while (fit.next()) |fid| {
                        if (fid.* > max_fid) max_fid = fid.*;
                    }
                }

                var tree = try Tree.open(allocator, opts, tree_id, &self.ms, &self.table_cache);
                errdefer tree.close();

                const tree_ptr = try allocator.create(Tree);
                tree_ptr.* = tree;
                try self.trees.put(tree_id, tree_ptr);

                // Rebuild namespace routing from persisted tree state.
                if (tree_state.namespace_id != 0) {
                    const rt = try self.getOrCreateRouteTable(tree_state.namespace_id);
                    try rt.addRoute(tree_id, .{ .left = tree_state.left, .right = tree_state.right });
                }
            }
        }

        self.next_tree_id = std.atomic.Value(u64).init(max_tid + 1);
        self.next_file_id = std.atomic.Value(u64).init(max_fid + 1);

        // Assign the first memtable a proper file ID.
        self.memtables.current.id = self.next_file_id.fetchAdd(1, .monotonic);
        return self;
    }

    pub fn close(self: *Forest) void {
        self.memtables.deinit();

        var tree_it = self.trees.iterator();
        while (tree_it.next()) |kv| {
            kv.value_ptr.*.close();
            self.allocator.destroy(kv.value_ptr.*);
        }
        self.trees.deinit();

        var rt_it = self.ns_routes.iterator();
        while (rt_it.next()) |kv| {
            kv.value_ptr.*.deinit();
            self.allocator.destroy(kv.value_ptr.*);
        }
        self.ns_routes.deinit();

        self.ms.close();
        self.table_cache.deinit();
        self.allocator.destroy(self);
    }

    /// Put a batch of key-value pairs into the current memtable.
    /// Returns true if the memtable size exceeds the threshold and a freeze
    /// should be proposed via Raft.
    pub fn put(self: *Forest, items: []const Item) !bool {
        for (items) |item| {
            try self.memtables.put(item.key, item.value);
        }
        return self.memtables.memoryUsage() >= self.opts.memtable_size;
    }

    /// Freeze the current memtable and create a new one.
    /// Called when the memtable freeze is committed via Raft.
    pub fn freezeMemtable(self: *Forest) !void {
        const new_id = self.next_file_id.fetchAdd(1, .monotonic);
        try self.memtables.swapCurrentOut(new_id);
    }

    /// buildFlush is Phase A of the memtable flush: iterates the oldest frozen
    /// memtable, routes each key to its tree, builds one L0 SST per tree,
    /// and returns the manifest changes. The caller proposes these through Raft.
    pub const FlushResult = struct {
        changes: std.ArrayList(manifest.TableChange),
        memtable_id: u64,
    };

    pub fn buildFlush(self: *Forest) !?FlushResult {
        const mt = self.memtables.peekOldest() orelse return null;
        const memtable_id = mt.id;
        var iter = mt.iterator();
        defer iter.deinit();

        // Per-tree builders. Keys within each tree are already in sorted order
        // because the skiplist is globally sorted and each tree covers a
        // contiguous key range within its namespace.
        var builders = std.AutoHashMap(u64, *Builder).init(self.allocator);
        defer {
            var bit = builders.valueIterator();
            while (bit.next()) |bld| {
                bld.*.deinit();
                self.allocator.destroy(bld.*);
            }
            builders.deinit();
        }

        const cfg = table_mod.builder.Config{
            .target_block_size = self.opts.block_size,
            .codec = self.opts.compression,
            .priority = .p0,
        };

        // Route each key to a tree and add to the appropriate builder.
        while (iter.next()) |item| {
            const tree_id = blk: {
                self.mu.lockShared();
                defer self.mu.unlockShared();
                const rt = self.ns_routes.get(item.key.namespace) orelse continue;
                break :blk rt.lookup(item.key) orelse continue;
            };

            const bld = if (builders.get(tree_id)) |b| b else blk: {
                const b = try self.allocator.create(Builder);
                b.* = Builder.init(self.allocator, cfg);
                try builders.put(tree_id, b);
                break :blk b;
            };
            try bld.add(item.key, item.value);
        }

        // Flush all builders to SST files. writeAndSyncTable handles the full
        // write path (finish → create file → write → sync → close) and returns
        // the manifest TableChange directly.
        var handles: std.ArrayList(zio.JoinHandle(?manifest.TableChange)) = .{};
        defer {
            for (handles.items) |*h| _ = h.join();
            handles.deinit(self.allocator);
        }

        var bit = builders.iterator();
        while (bit.next()) |kv| {
            const tree_id = kv.key_ptr.*;
            const bld = kv.value_ptr.*;

            var tb = try bld.finish();
            if (tb.bytes.len == 0) {
                tb.deinit();
                continue;
            }

            const file_id = self.next_file_id.fetchAdd(1, .monotonic);
            const path = try std.fmt.allocPrint(
                self.allocator,
                "{s}/{d:0>6}.sst",
                .{ self.opts.dir, file_id },
            );

            try handles.append(self.allocator, try zio.spawn(writeAndSyncTable, .{
                self.allocator, tb, path, file_id, tree_id,
            }));
        }

        // Join fibers and collect manifest changes.
        var changes: std.ArrayList(manifest.TableChange) = .{};
        for (handles.items) |*h| {
            // TODO: We should be handling errors here, not ignoring them.
            const tc = h.join() orelse continue;
            try changes.append(self.allocator, tc);
        }
        handles.clearAndFree(self.allocator);

        // Best-effort directory sync.
        if (self.opts.dir.len > 0) {
            var dir_sync = try zio.spawn(vfs.VFS.syncDir, .{self.opts.dir});
            defer dir_sync.join();
        }

        return .{ .changes = changes, .memtable_id = memtable_id };
    }

    /// Writes table bytes to a new SST file, syncs, and closes.
    /// Returns a manifest.TableChange (CREATE at level 0) on success.
    fn writeAndSyncTable(
        allocator: std.mem.Allocator,
        tb: table_mod.builder.TableBuffer,
        path: []const u8,
        file_id: u64,
        tree_id: u64,
    ) ?manifest.TableChange {
        defer allocator.free(path);
        var mtb = tb;
        defer mtb.deinit();

        const wh = vfs.g().createFile(path, .{ .mode = .direct }) catch return null;
        wh.write(mtb.bytes, 0, .p0) catch return null;
        wh.sync() catch return null;
        wh.close();
        return manifest.createTableChange(file_id, tree_id, 0);
    }

    /// applyFlush is Phase B: commits the manifest changes from a memtable flush
    /// and updates in-memory levels. Called on all nodes after Raft commit.
    /// Also removes the flushed memtable from the immutable list.
    pub fn applyFlush(self: *Forest, changes: []const manifest.TableChange, memtable_id: u64) !void {
        if (changes.len > 0) {
            try self.ms.commit(changes, &.{});
        }

        // Open new L0 tables and add to each tree's level 0.
        for (changes) |c| {
            std.debug.assert(c.op == 0); // flush only creates
            const tree = self.trees.get(c.tree_id) orelse continue;

            const path = try std.fmt.allocPrint(
                self.allocator,
                "{s}/{d:0>6}.sst",
                .{ self.opts.dir, c.table_id },
            );
            defer self.allocator.free(path);

            const t = try Table.open(self.allocator, path, .p0);
            errdefer t.decrRef();
            try self.table_cache.own(c.table_id, t);
            try tree.levels[c.level].addEntry(c.table_id, t);
            t.decrRef(); // drop open ref; level holds its own
        }

        // Remove the oldest flushed memtable.
        if (self.memtables.popOldest()) |imm| {
            std.debug.assert(imm.id == memtable_id);
            imm.release();
        }

        // Update compaction candidates for affected trees.
        var seen_trees = std.AutoHashMap(u64, void).init(self.allocator);
        defer seen_trees.deinit();
        for (changes) |c| {
            if (!seen_trees.contains(c.tree_id)) {
                try seen_trees.put(c.tree_id, {});
                if (self.trees.get(c.tree_id)) |tree| {
                    try tree.updateCompactCandidate();
                }
            }
        }
    }

    /// Get a value by key. Checks memtables first (current, then immutables
    /// newest-to-oldest), then routes to the correct tree.
    /// Returned Item is owned by the caller; call item.free(allocator) when done.
    pub fn get(self: *Forest, allocator: std.mem.Allocator, search_key: Key, priority: vfs.Priority) !?Item {
        if (try self.memtables.get(allocator, search_key)) |item| {
            return item;
        }

        self.mu.lockShared();
        defer self.mu.unlockShared();

        const rt = self.ns_routes.get(search_key.namespace) orelse return null;
        const tree_id = rt.lookup(search_key) orelse return null;
        const tree = self.trees.get(tree_id) orelse return null;
        return tree.get(search_key, priority);
    }

    /// Create a new tree for the given namespace and key range.
    /// Returns the new tree_id.
    pub fn createTree(self: *Forest, namespace_id: u32, range: KeyRange) !u64 {
        self.mu.lock();
        defer self.mu.unlock();

        const tree_id = self.next_tree_id.fetchAdd(1, .monotonic);

        // Commit tree creation to manifest.
        const left_raw = try range.left.marshal(self.allocator);
        defer self.allocator.free(left_raw);

        const right_raw = try range.right.marshal(self.allocator);
        defer self.allocator.free(right_raw);

        try self.ms.commit(&.{}, &.{manifest.createTreeChange(tree_id, namespace_id, left_raw, right_raw)});

        // Create in-memory tree (empty — no tables yet).
        var tree = try Tree.open(self.allocator, self.opts, tree_id, &self.ms, &self.table_cache);
        errdefer tree.close();

        const tree_ptr = try self.allocator.create(Tree);
        tree_ptr.* = tree;
        try self.trees.put(tree_id, tree_ptr);

        // Set up namespace routing.
        const rt = try self.getOrCreateRouteTable(namespace_id);
        try rt.addRoute(tree_id, range);

        return tree_id;
    }

    /// Get a tree by its id.
    pub fn getTree(self: *Forest, tree_id: u64) ?*Tree {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.trees.get(tree_id);
    }

    /// Compact finds the tree with the highest compaction score and runs one
    /// compaction on it. Returns true if a compaction was executed, false if
    /// no tree needs compaction.
    pub const CompactionTarget = struct {
        tree: *Tree,
        tree_id: u64,
        candidate: tree_mod.CompactCandidate,
    };

    /// Find the best tree/level to compact without performing compaction.
    pub fn bestCompactionCandidate(self: *Forest) ?CompactionTarget {
        self.mu.lockShared();
        defer self.mu.unlockShared();

        var best: ?CompactionTarget = null;
        var it = self.trees.iterator();
        while (it.next()) |kv| {
            const tree = kv.value_ptr.*;
            const candidate = tree.compact_candidate orelse continue;
            if (best == null or candidate.score > best.?.candidate.score) {
                best = .{ .tree = tree, .tree_id = kv.key_ptr.*, .candidate = candidate };
            }
        }
        return best;
    }

    /// Check if the current memtable should be frozen.
    pub fn shouldFreeze(self: *Forest) bool {
        return self.memtables.memoryUsage() >= self.opts.memtable_size;
    }

    /// Check if there are frozen memtables waiting to be flushed.
    pub fn hasFrozenMemtables(self: *Forest) bool {
        return self.memtables.peekOldest() != null;
    }

    /// Check if an SST file exists on disk.
    pub fn hasFile(self: *Forest, file_id: u64) bool {
        var buf: [256]u8 = undefined;
        const path = std.fmt.bufPrint(&buf, "{s}/{d:0>6}.sst", .{ self.opts.dir, file_id }) catch return false;
        std.fs.cwd().access(path, .{}) catch return false;
        return true;
    }

    pub fn compact(self: *Forest) !bool {
        const b = self.bestCompactionCandidate() orelse return false;
        try b.tree.doCompact(b.candidate.level, &self.next_file_id);
        self.table_cache.clearStale();

        const linfo = b.tree.lowestLevelSize();
        if (linfo.size > self.opts.tree_split_size) {
            self.mu.lock();
            defer self.mu.unlock();
            try self.splitTree(b.tree_id);
        }

        return true;
    }

    /// splitTree splits the given tree at the median key of its bottommost level.
    /// Caller must hold the write lock.
    pub fn splitTree(self: *Forest, tree_id: u64) !void {
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const mc = try self.buildSplit(arena.allocator(), tree_id) orelse return;
        try self.applySplit(mc);
    }

    /// buildSplit is Phase A: finds the split key, classifies tables, and returns
    /// a ManifestChange with the table and tree changes. All allocations go on
    /// the provided allocator (typically an arena). The caller proposes the
    /// ManifestChange through Raft.
    pub fn buildSplit(self: *Forest, alloc: std.mem.Allocator, tree_id: u64) !?manifest.ManifestChange {
        const left_tree = self.trees.get(tree_id) orelse return null;

        // Find split key from the lowest level in old tree.
        const split_key = try left_tree.findSplitKey(alloc);
        if (split_key.len() == 0) {
            return null;
        }

        // Capture old tree state before the manifest commit.
        const left_tree_state = self.ms.trees.get(tree_id) orelse return null;
        const new_tree_id = self.next_tree_id.fetchAdd(1, .monotonic);

        // Classify tables per level and build manifest changes.
        var table_changes: std.ArrayList(manifest.TableChange) = .{};

        for (left_tree.levels, 0..) |*llv, llv_idx| {
            llv.mu.lockShared();
            defer llv.mu.unlockShared();

            for (llv.tables.items) |tbe| {
                if (tbe.table.smallest.order(split_key) != .lt) { // .eq or .gt
                    // Right-only: remove from old tree, add to new tree.
                    try table_changes.append(alloc, manifest.deleteTableChange(
                        tbe.id,
                        tree_id,
                        @intCast(llv_idx),
                    ));
                    try table_changes.append(alloc, manifest.createTableChange(
                        tbe.id,
                        new_tree_id,
                        @intCast(llv_idx),
                    ));
                } else if (tbe.table.biggest.order(split_key) != .lt) { // .eq or .gt
                    // order: smallest key < split key <= biggest key
                    // Therefore, the split key falls within the table.
                    // Shared: keep in old tree, also add to new tree.
                    try table_changes.append(alloc, manifest.createTableChange(tbe.id, new_tree_id, @intCast(llv_idx)));
                }
                // Left-only: no manifest change needed.
            }
        }

        const left_left_raw_key = try left_tree_state.left.marshal(alloc);
        const right_right_raw_key = try left_tree_state.right.marshal(alloc);
        const split_raw = try split_key.marshal(alloc);
        const namespace_id = left_tree_state.namespace_id;
        std.debug.assert(namespace_id > 0);

        const tree_changes = try alloc.alloc(manifest.TreeChange, 2);
        tree_changes[0] = manifest.updateTreeChange(tree_id, left_left_raw_key, split_raw);
        tree_changes[1] = manifest.createTreeChange(new_tree_id, namespace_id, split_raw, right_right_raw_key);

        return .{
            .table_changes = try table_changes.toOwnedSlice(alloc),
            .tree_changes = tree_changes,
            .cmd = 3, // tree_split
        };
    }

    /// applySplit is Phase B: commits the manifest changes, opens the new tree,
    /// updates the old tree's key range, removes migrated tables, and updates
    /// namespace routing. Called on all nodes after Raft commit.
    pub fn applySplit(self: *Forest, mc: manifest.ManifestChange) !void {
        // Commit MANIFEST so we can recover from a crash.
        try self.ms.commit(mc.table_changes, mc.tree_changes);

        // Find the old tree_id (UPDATE) and new tree_id (CREATE) from tree_changes.
        var old_tree_id: u64 = 0;
        var new_tree_id: u64 = 0;
        var namespace_id: u32 = 0;
        for (mc.tree_changes) |tc| {
            if (tc.op == 2) { // UPDATE
                old_tree_id = tc.tree_id;
            } else if (tc.op == 0) { // CREATE
                new_tree_id = tc.tree_id;
                namespace_id = tc.namespace_id;
            }
        }

        // Open the new right tree BEFORE deleting from old tree, so that
        // loadTables finds the shared tables in the cache with ref > 0.
        // It would use MANIFEST state to figure out which tables it owns.
        var new_tree = try Tree.open(self.allocator, self.opts, new_tree_id, &self.ms, &self.table_cache);
        errdefer new_tree.close();

        const left_tree = self.trees.get(old_tree_id) orelse return;

        // Update old tree's key range from the manifest state (post-commit).
        const ms_left = self.ms.trees.getPtr(old_tree_id).?;
        const new_right = try ms_left.right.dupe(left_tree.allocator);
        errdefer new_right.free(left_tree.allocator);
        left_tree.key_range.right.free(left_tree.allocator);
        left_tree.key_range.right = new_right;
        left_tree.key_range.inf = false;

        // Remove migrated tables from old tree.
        for (mc.table_changes) |c| {
            if (c.op == 1 and c.tree_id == old_tree_id) { // DELETE from old tree
                left_tree.levels[c.level].deleteEntries(&.{.{ .id = c.table_id, .table = undefined }});
            }
        }

        // Add new tree to the forest.
        const new_ptr = try self.allocator.create(Tree);
        errdefer self.allocator.destroy(new_ptr);
        new_ptr.* = new_tree;
        try self.trees.put(new_tree_id, new_ptr);

        // Update route table using manifest-owned Keys (post-commit).
        const rt = try self.getOrCreateRouteTable(namespace_id);
        const ms_tree_right = self.ms.trees.getPtr(new_tree_id).?;
        rt.updateRange(old_tree_id, .{ .left = ms_left.left, .right = ms_left.right });
        try rt.addRoute(new_tree_id, .{ .left = ms_tree_right.left, .right = ms_tree_right.right });
    }

    fn getOrCreateRouteTable(self: *Forest, namespace_id: u32) !*RouteTable {
        if (self.ns_routes.get(namespace_id)) |rt| return rt;

        const rt = try self.allocator.create(RouteTable);
        rt.* = RouteTable.init(self.allocator, namespace_id);
        try self.ns_routes.put(namespace_id, rt);
        return rt;
    }
};
