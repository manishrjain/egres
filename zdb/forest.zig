const std = @import("std");
const log = std.log.scoped(.forest);
const zio = @import("zio");
const utils = @import("utils");
const config = @import("config.zig");
const manifest = @import("manifest.zig");
const level = @import("level.zig");
const table_mod = @import("table.zig");
const vfs = @import("vfs");
const tree_mod = @import("tree.zig");
const skl = @import("skl/skiplist.zig");
const metrics = @import("metrics.zig");
const fi = @import("file_ids.zig");

const Key = @import("key.zig").Key;
const KeyRange = level.KeyRange;
const Tree = tree_mod.Tree;
const TableCache = tree_mod.TableCache;
const Table = table_mod.Table;
const Item = table_mod.table.Item;
const SkipList = skl.SkipList;
const Builder = table_mod.builder.Builder;
const merge_iterator = table_mod.merge_iterator;

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

    /// Find the tree_id that owns this key, or null if none. Callers
    /// that also need the tree's key range can read it directly from
    /// `Tree.key_range` after looking up the tree (kept in sync by
    /// `applySplit`); the route table is just for routing.
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
    mu: zio.RwLock,
    current: *SkipList,
    immutables: std.ArrayList(*SkipList), // oldest at index 0

    pub fn init(allocator: std.mem.Allocator, current: *SkipList) Memtables {
        return .{
            .allocator = allocator,
            .mu = .init,
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
        self.mu.lockSharedUncancelable();
        defer self.mu.unlockShared();
        try self.current.put(key, value);
    }

    pub fn memoryUsage(self: *Memtables) usize {
        self.mu.lockSharedUncancelable();
        defer self.mu.unlockShared();
        return self.current.memoryUsage();
    }

    /// Get iterators across the active memtable + every immutable
    /// for use in a scan merge. Each `skl.Iterator.init` acquires the
    /// underlying skiplist — release happens via `deinit` on the
    /// returned iterator (transitively from `MergeIterator.deinit`).
    /// Doing the acquires under our shared lock guarantees that even if
    /// a concurrent flush + `popOldestIfMatches` releases the original
    /// ref a moment later, the skiplist stays alive until the iterator
    /// drops its ref. Caller frees the slice itself.
    pub fn getIterators(self: *Memtables, allocator: std.mem.Allocator) ![]merge_iterator.Iterator {
        self.mu.lockSharedUncancelable();
        defer self.mu.unlockShared();
        const total = self.immutables.items.len + 1;
        const out = try allocator.alloc(merge_iterator.Iterator, total);
        out[0] = .{ .skiplist = skl.Iterator.init(self.current) };
        for (self.immutables.items, 0..) |imm, i| {
            out[i + 1] = .{ .skiplist = skl.Iterator.init(imm) };
        }
        return out;
    }

    /// id of the current memtable. 0 means the initial placeholder is still
    /// in place — no `newMemtable` has been applied yet, and any writes that
    /// land in `current` will be discarded by the first `swapCurrentOut`.
    pub fn currentId(self: *Memtables) u64 {
        self.mu.lockSharedUncancelable();
        defer self.mu.unlockShared();
        return self.current.id;
    }

    /// Aggregate sizes for the active + immutable memtables, taken under
    /// one lock. Used by the stats dump so we don't have to chain three
    /// separate lookups (each of which would re-acquire the lock).
    pub const Stats = struct {
        active_bytes: usize,
        immutable_bytes: usize,
        immutable_count: usize,
    };

    pub fn stats(self: *Memtables) Stats {
        self.mu.lockSharedUncancelable();
        defer self.mu.unlockShared();
        var imm_bytes: usize = 0;
        for (self.immutables.items) |imm| imm_bytes += imm.memoryUsage();
        return .{
            .active_bytes = self.current.memoryUsage(),
            .immutable_bytes = imm_bytes,
            .immutable_count = self.immutables.items.len,
        };
    }

    /// Look up a key across current and immutable memtables.
    /// Returns a duped Item owned by the caller.
    pub fn get(self: *Memtables, allocator: std.mem.Allocator, search_key: Key) !?Item {
        self.mu.lockSharedUncancelable();
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
    /// `log_index` is the apply index of the Raft `newMemtable` entry; it pins
    /// the snapshot horizon (`snapshot.index <= oldest.created_at_log_index - 1`).
    ///
    /// Special case: the id=0 placeholder (created by `Forest.open` before any
    /// real `newMemtable` has been applied) is *discarded*, not frozen — it
    /// would otherwise sit in immutables forever, since no flush ever targets
    /// id=0.
    pub fn swapCurrentOut(self: *Memtables, new_id: u64, log_index: u64) !void {
        self.mu.lockUncancelable();
        defer self.mu.unlock();

        if (self.current.id == 0) {
            self.current.release();
        } else {
            try self.immutables.append(self.allocator, self.current);
        }
        const new_mt = try SkipList.init(self.allocator);
        new_mt.id = new_id;
        new_mt.created_at_log_index = log_index;
        self.current = new_mt;
    }

    /// Log index of the oldest in-memory memtable. Returns 0 when the only
    /// thing around is the initial placeholder (no real `newMemtable` entry
    /// has been applied yet), which the caller treats as "no snapshot yet."
    pub fn oldestInMemoryLogIndex(self: *Memtables) u64 {
        self.mu.lockSharedUncancelable();
        defer self.mu.unlockShared();
        if (self.immutables.items.len > 0) return self.immutables.items[0].created_at_log_index;
        return self.current.created_at_log_index;
    }

    /// Drop any phantom immutables whose id is strictly less than `expected_id`,
    /// then pop+release the oldest IFF its id equals `expected_id`. No-op when
    /// the matched memtable isn't found.
    ///
    /// Used by `applyFlush`. Three cases reflect leader/follower divergence
    /// during recovery replay:
    ///   - oldest.id < expected_id  → phantom: flush already happened
    ///     pre-snap on leader, SST is in snap.data, but we still have the
    ///     local memtable (e.g., its `newMemtable` was post-snap and
    ///     replayed, but its `flush` was pre-snap and compacted). Release
    ///     and continue checking the next oldest.
    ///   - oldest.id == expected_id → normal: pop and release.
    ///   - oldest.id > expected_id  → flush's target memtable was popped on
    ///     the leader pre-snap and never re-created here (its `newMemtable`
    ///     proposal was below snap.index). Nothing local to remove; the SST
    ///     install above handles the data side.
    /// Steady-state: FIFO ordering means oldest always equals expected.
    pub fn popOldestIfMatches(self: *Memtables, expected_id: u64) void {
        self.mu.lockUncancelable();
        defer self.mu.unlock();

        while (self.immutables.items.len > 0 and self.immutables.items[0].id < expected_id) {
            const phantom = self.immutables.orderedRemove(0);
            log.info(
                "popOldestIfMatches: releasing phantom immutable id={d} (expected_id={d})",
                .{ phantom.id, expected_id },
            );
            phantom.release();
        }

        if (self.immutables.items.len == 0) {
            log.info("popOldestIfMatches: skipping pop, immutables empty (expected_id={d})", .{expected_id});
            return;
        }
        const oldest_id = self.immutables.items[0].id;
        if (oldest_id != expected_id) {
            log.info(
                "popOldestIfMatches: skipping pop, oldest.id={d} > expected_id={d} (recovery: pre-snap flush, memtable not re-created locally)",
                .{ oldest_id, expected_id },
            );
            return;
        }
        const imm = self.immutables.orderedRemove(0);
        imm.release();
    }

    /// Return the oldest immutable memtable without removing it.
    pub fn peekOldest(self: *Memtables) ?*SkipList {
        self.mu.lockSharedUncancelable();
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
    opts: config.Config,
    ms: manifest.ForestState,
    next_tree_id: std.atomic.Value(u64),
    /// Per-node file_id allocator. Wired at startup via `bindWorker`.
    /// All callers go through `file_ids.allocate()`; raw `fetchAdd` is
    /// never correct because it skips the HardState lease.
    file_ids: fi.FileIds,
    trees: std.AutoHashMap(u64, *Tree),
    ns_routes: std.AutoHashMap(u32, *RouteTable),
    table_cache: TableCache,
    mu: utils.TrackedRwLock,
    memtables: Memtables,

    /// Open a forest with empty in-memory state. Persistent state is restored
    /// later via `installManifest` (called by recovery if a Raft snapshot is
    /// present); fresh starts go straight to `createTree` / Raft replay.
    pub fn open(allocator: std.mem.Allocator, opts: config.Config) !*Forest {
        var ms = manifest.ForestState.init(allocator);
        errdefer ms.deinit();

        const self = try allocator.create(Forest);
        errdefer allocator.destroy(self);

        // Initial placeholder memtable. id and created_at_log_index both 0
        // are the sentinel values that mark "no real memtable yet" — the
        // first replayed `newMemtable` swaps it out (Phase 5).
        const mt = try SkipList.init(allocator);
        errdefer mt.release();

        self.* = .{
            .allocator = allocator,
            .opts = opts,
            .ms = ms,
            .next_tree_id = std.atomic.Value(u64).init(1),
            .file_ids = .{},
            .trees = std.AutoHashMap(u64, *Tree).init(allocator),
            .ns_routes = std.AutoHashMap(u32, *RouteTable).init(allocator),
            .table_cache = TableCache.init(allocator, opts.dir),
            .mu = .{},
            .memtables = Memtables.init(allocator, mt),
        };
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

        self.ms.deinit();
        self.file_ids.mu.deinit();
        self.table_cache.deinit();
        self.mu.deinit();
        self.allocator.destroy(self);
    }

    /// Put a batch of key-value pairs into the current memtable.
    /// Callers can use shouldFreeze() afterward to decide whether to propose
    /// a memtable freeze through Raft.
    pub fn put(self: *Forest, items: []const Item) !void {
        for (items) |item| {
            try self.memtables.put(item.key, item.value);
        }
    }

    /// Create a new memtable, freezing the current one.
    /// Called when the newMemtable entry is committed via Raft.
    /// The new_id is assigned by the leader and included in the proposal so
    /// all nodes use the same ID. `log_index` is the apply index of that
    /// Raft entry — pins the snapshot horizon for the new memtable.
    pub fn newMemtable(self: *Forest, new_id: u64, log_index: u64) !void {
        try self.memtables.swapCurrentOut(new_id, log_index);
    }

    /// Ensure next_tree_id is past the given id. A follower that becomes leader
    /// must not reuse tree IDs assigned by the previous leader.
    pub fn advanceTreeId(self: *Forest, id: u64) void {
        while (true) {
            const cur = self.next_tree_id.load(.monotonic);
            if (cur > id) return;
            if (self.next_tree_id.cmpxchgWeak(cur, id + 1, .monotonic, .monotonic) == null) return;
        }
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
        while (iter.itemRef()) |item| {
            const tree_id = blk: {
                self.mu.lockSharedUncancelable(@src());
                defer self.mu.unlockShared();
                const rt = self.ns_routes.get(item.key.namespace) orelse {
                    try iter.next();
                    continue;
                };
                break :blk rt.lookup(item.key) orelse {
                    try iter.next();
                    continue;
                };
            };

            const bld = if (builders.get(tree_id)) |b| b else blk: {
                const b = try self.allocator.create(Builder);
                b.* = Builder.init(self.allocator, cfg);
                try builders.put(tree_id, b);
                break :blk b;
            };
            try bld.add(item.key, item.value);
            try iter.next();
        }

        // Flush all builders to SST files. writeAndSyncTable handles the full
        // write path (finish → create file → write → sync → close) and returns
        // the manifest TableChange directly.
        var flush_err: utils.ThreadSafeError = .{};
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

            const file_id = try self.file_ids.allocate();
            const path = try fi.sstPath(self.allocator, self.opts.dir, file_id);

            try handles.append(self.allocator, try zio.spawn(writeAndSyncTable, .{
                self.allocator, tb, path, file_id, tree_id, &flush_err,
            }));
        }

        // Join fibers and collect manifest changes.
        var changes: std.ArrayList(manifest.TableChange) = .{};
        for (handles.items) |*h| {
            const tc = h.join() orelse continue;
            try changes.append(self.allocator, tc);
        }
        handles.clearAndFree(self.allocator);
        if (flush_err.get()) |err| return err;

        // Best-effort directory sync.
        if (self.opts.dir.len > 0) {
            var dir_sync = try zio.spawn(vfs.VFS.syncDir, .{self.opts.dir});
            defer dir_sync.join();
        }

        return .{ .changes = changes, .memtable_id = mt.id };
    }

    /// Writes table bytes to a new SST file, syncs, and closes.
    /// Returns a manifest.TableChange (CREATE at level 0) on success.
    fn writeAndSyncTable(
        allocator: std.mem.Allocator,
        tb: table_mod.builder.TableBuffer,
        path: []const u8,
        file_id: u64,
        tree_id: u64,
        err_out: *utils.ThreadSafeError,
    ) ?manifest.TableChange {
        defer allocator.free(path);
        var mtb = tb;
        defer mtb.deinit();

        const wh = vfs.g().createFile(path, .{ .mode = .direct }) catch |err| {
            err_out.set(err);
            return null;
        };
        wh.write(mtb.bytes, 0, .p0) catch |err| {
            err_out.set(err);
            return null;
        };
        wh.sync() catch |err| {
            err_out.set(err);
            return null;
        };
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

        // Load all new L0 tables concurrently via the cache. writeFile may
        // have pre-opened these (follower path) → cache hit. On the leader,
        // flush writes bytes directly, so these will typically be misses.
        var needed: std.ArrayList(tree_mod.TableNeed) = .{};
        defer needed.deinit(self.allocator);
        for (changes) |c| {
            std.debug.assert(c.op == .create); // flush only creates
            try needed.append(self.allocator, .{ .id = c.table_id, .level = c.level });
        }
        try self.table_cache.loadTables(needed.items);

        // Install each loaded table into its tree's level. We zip changes and
        // needed in the same order they were built, so indices align.
        for (changes, needed.items) |c, n| {
            std.debug.assert(c.table_id == n.id);
            const tree = self.trees.get(c.tree_id) orelse continue;
            try tree.levels[c.level].addEntry(n.id, n.table.?);

            // Metrics: one per-tree flush output + bytes written to L0.
            const ns_id: u32 = if (self.ms.trees.get(c.tree_id)) |ts| ts.namespace_id else 0;
            try metrics.g().flushes.incr(.{ .namespace_id = ns_id, .tree_id = c.tree_id });
            try metrics.g().bytes_written.incrBy(.{
                .source = "flush",
                .namespace_id = ns_id,
                .tree_id = c.tree_id,
                .level = @intCast(c.level),
            }, n.table.?.fileSize());
        }

        // Remove the flushed memtable from local immutables IF it's there.
        // During recovery replay, the memtable's `newMemtable` proposal may
        // have been below snap.index and compacted; in that case immutables
        // either is empty or contains a *later* memtable. The SST install
        // above handles the data side; nothing local to remove.
        self.memtables.popOldestIfMatches(memtable_id);

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

        self.mu.lockSharedUncancelable(@src());
        defer self.mu.unlockShared();

        const rt = self.ns_routes.get(search_key.namespace) orelse return null;
        const tree_id = rt.lookup(search_key) orelse return null;
        const tree = self.trees.get(tree_id) orelse return null;
        return tree.get(search_key, priority);
    }

    pub const ScanCallback = *const fn (ctx: *anyopaque, item: Item) anyerror!bool;

    /// Forward scan over `[scan_range.left, scan_range.right)` within
    /// namespace `ns`, spanning every tree that overlaps the request.
    ///
    /// One tree at a time: look up the tree owning `current_left`,
    /// snapshot its range, open a cursor (with a fresh memtable iter
    /// snapshot), drain to the cursor's end (or max_count), then
    /// advance `current_left` past the snapshotted right boundary and
    /// re-look-up. Re-looking-up after each tree means the scan is
    /// safe against splits firing mid-scan: we always pick whichever
    /// tree currently covers the next key. Per-cursor memtable
    /// snapshots also pick up any newly-flushed data.
    pub fn scan(
        self: *Forest,
        allocator: std.mem.Allocator,
        ns: u32,
        scan_range: KeyRange,
        max_count: u64,
        priority: vfs.Priority,
        ctx: *anyopaque,
        cb: ScanCallback,
    ) !u64 {
        if (max_count == 0) return 0;

        // Per-call arena — used to dupe each visited tree's
        // right-boundary key so it stays alive across iterations
        // (we drop the forest read lock between cursor hops).
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();

        var emitted: u64 = 0;
        var current_left = scan_range.left;

        while (emitted < max_count) {
            // Find the tree owning current_left + snapshot its range
            // (tree.key_range stays in sync with the manifest via
            // applySplit). Re-doing the lookup per hop keeps the scan
            // safe against splits firing between hops — we always
            // pick whichever tree currently covers the next key.
            var tree: *Tree = undefined;
            var tree_range = KeyRange{};
            {
                self.mu.lockSharedUncancelable(@src());
                defer self.mu.unlockShared();
                const rt = self.ns_routes.get(ns) orelse return emitted;
                const tid = blk: {
                    if (current_left.primary.len == 0) {
                        if (rt.entries.items.len == 0) break :blk null;
                        break :blk rt.entries.items[0].tree_id;
                    }
                    break :blk rt.lookup(current_left) orelse null;
                };
                if (tid == null) return emitted;
                tree = self.trees.get(tid.?) orelse return emitted;
                tree_range = try tree.key_range.dupe(a);
            }

            // Restrict to the requested scan window. If the intersection
            // is empty the scan has run off the end → done.
            const remaining = KeyRange{ .left = current_left, .right = scan_range.right };
            const visit_range = remaining.intersect(tree_range);

            // Allocate memtable iters from the per-call arena so they
            // live exactly as long as the merge. (Memtables.getIterators
            // returns a fresh slice; freeing it through the arena at
            // end-of-scope is fine.)
            const mem_iters = try self.memtables.getIterators(a);
            var merge = try tree.openMerge(a, visit_range, priority, mem_iters);

            // Drain (or hit max_count). Use a labeled block so merge.deinit
            // fires on every exit path before we either return or hop to
            // the next tree.
            const stop_early = drain: {
                while (emitted < max_count) {
                    const item = merge.itemRef() orelse break :drain false;
                    const cont = try cb(ctx, item);
                    emitted += 1;
                    if (!cont) break :drain true;
                    try merge.next();
                }
                break :drain false;
            };
            merge.deinit();
            if (stop_early) return emitted;

            // Advance to next tree by jumping past the snapshotted
            // right edge. Empty primary = unbounded right (rightmost
            // tree) → done.
            if (tree_range.right.primary.len == 0) break;
            current_left = tree_range.right;
        }
        return emitted;
    }

    /// installTree opens an empty in-memory Tree for the given id and registers
    /// the namespace route. Caller must have already committed the TreeChange to
    /// manifest and must hold the forest write lock.
    fn installTree(self: *Forest, tree_id: u64, namespace_id: u32, range: KeyRange) !void {
        var tree = try Tree.open(self.allocator, self.opts, tree_id, &self.ms, &self.table_cache);
        errdefer tree.close();

        const tree_ptr = try self.allocator.create(Tree);
        errdefer self.allocator.destroy(tree_ptr);
        tree_ptr.* = tree;
        try self.trees.put(tree_id, tree_ptr);

        const rt = try self.getOrCreateRouteTable(namespace_id);
        try rt.addRoute(tree_id, range);
    }

    /// Walk every tree's level table list and append the file_ids of any SST
    /// not currently on disk. The returned slice is owned by the caller.
    pub fn collectMissingFileIds(self: *Forest, allocator: std.mem.Allocator) ![]u64 {
        var out: std.ArrayList(u64) = .empty;
        errdefer out.deinit(allocator);

        var it = self.ms.trees.iterator();
        while (it.next()) |kv| {
            for (kv.value_ptr.*.levels.items) |lm| {
                var tit = lm.tables.keyIterator();
                while (tit.next()) |fid| {
                    if (!self.hasFile(fid.*)) try out.append(allocator, fid.*);
                }
            }
        }
        return out.toOwnedSlice(allocator);
    }

    /// Walk `self.ms.trees` and install each tree (Tree + RouteTable entries),
    /// then advance `next_tree_id` past every id seen. Used after
    /// `ForestState.restoreFrom(snapshot.data)` at recovery — the manifest
    /// already names every tree and table, but the in-memory Tree objects
    /// and route table need to be rebuilt from it.
    ///
    /// File ids are NOT scanned here: each node's local counter is
    /// restored from HardState's `max_file_id` via `forest.file_ids.bind`,
    /// which is independent of (and immune to) ids in other nodes'
    /// namespaces that show up in the manifest.
    pub fn openTablesFromManifest(self: *Forest) !void {
        self.mu.lockUncancelable(@src());
        defer self.mu.unlock();

        var max_tree_id: u64 = 0;

        var it = self.ms.trees.iterator();
        while (it.next()) |kv| {
            const tree_id = kv.key_ptr.*;
            const tree = kv.value_ptr.*;

            // Route table holds non-owning refs into `self.ms.trees[tid]`.
            // Tree-key changes (split / update) flow through `applySplit`
            // under the forest write lock, which atomically rewrites both
            // `ms.trees[tid]` and the corresponding route entry — so the
            // ref never outlives the underlying allocation. Same lifetime
            // discipline as `createTree` / `applyNamespaceCreate`.
            const range: KeyRange = if (tree.left.len() == 0 and tree.right.len() == 0)
                .{ .inf = true }
            else
                .{ .left = tree.left, .right = tree.right };

            try self.installTree(tree_id, tree.namespace_id, range);

            if (tree_id > max_tree_id) max_tree_id = tree_id;
        }

        self.advanceTreeId(max_tree_id);
    }

    /// Create a new tree for the given namespace and key range.
    /// Single-node path (no Raft) — used by tests. Returns the new tree_id.
    pub fn createTree(self: *Forest, namespace_id: u32, range: KeyRange) !u64 {
        self.mu.lockUncancelable(@src());
        defer self.mu.unlock();

        const tree_id = self.next_tree_id.fetchAdd(1, .monotonic);

        const left_raw = try range.left.marshal(self.allocator);
        defer self.allocator.free(left_raw);
        const right_raw = try range.right.marshal(self.allocator);
        defer self.allocator.free(right_raw);

        try self.ms.commit(&.{}, &.{manifest.createTreeChange(tree_id, namespace_id, left_raw, right_raw)});
        try self.installTree(tree_id, namespace_id, range);
        return tree_id;
    }

    /// buildNamespaceCreate is Phase A of the Raft-replicated namespace create:
    /// allocates a new tree_id and returns a ManifestChange with one TreeChange
    /// carrying an infinite range (empty left+right). No state mutates beyond
    /// next_tree_id, so a failed propose only burns one id. Caller owns the
    /// returned slice.
    ///
    /// Errors with `NamespaceAlreadyExists` if a route table already exists
    /// for `namespace_id`.
    pub fn buildNamespaceCreate(self: *Forest, alloc: std.mem.Allocator, namespace_id: u32) !manifest.ManifestChange {
        std.debug.assert(namespace_id > 0);
        {
            self.mu.lockSharedUncancelable(@src());
            defer self.mu.unlockShared();
            if (self.ns_routes.contains(namespace_id)) return error.NamespaceAlreadyExists;
        }
        const tree_id = self.next_tree_id.fetchAdd(1, .monotonic);

        const tree_changes = try alloc.alloc(manifest.TreeChange, 1);
        tree_changes[0] = manifest.createTreeChange(tree_id, namespace_id, "", "");

        return .{
            .tree_changes = tree_changes,
            .table_changes = &.{},
            .cmd = 5, // ns_create
            .memtable_id = 0,
        };
    }

    /// applyNamespaceCreate is Phase B: commits each TreeChange to the manifest
    /// and installs the in-memory Tree + namespace route. Called on all nodes
    /// after Raft commit (cmd=5).
    ///
    /// Skips any TreeChange whose namespace already has a route table.
    pub fn applyNamespaceCreate(self: *Forest, mc: manifest.ManifestChange) !void {
        self.mu.lockUncancelable(@src());
        defer self.mu.unlock();

        for (mc.tree_changes) |tc| {
            if (tc.op != .create) continue; // only CREATE for ns_create
            if (self.ns_routes.contains(tc.namespace_id)) {
                std.log.warn("applyNamespaceCreate: ns={d} already exists, skipping tree_id={d}", .{ tc.namespace_id, tc.tree_id });
                continue;
            }
            self.advanceTreeId(tc.tree_id);
            try self.ms.commit(&.{}, &.{tc});
            // Empty left/right encodes the infinite range.
            try self.installTree(tc.tree_id, tc.namespace_id, .{ .inf = true });
        }
    }

    /// Get a tree by its id.
    pub fn getTree(self: *Forest, tree_id: u64) ?*Tree {
        self.mu.lockSharedUncancelable(@src());
        defer self.mu.unlockShared();
        return self.trees.get(tree_id);
    }

    /// True iff the namespace has been created (its route table exists).
    /// Used by the leader to reject mutations destined for unknown
    /// namespaces — those would otherwise pile up in immutable memtables
    /// since `buildFlush` silently drops items whose namespace has no
    /// route.
    pub fn hasNamespace(self: *Forest, namespace_id: u32) bool {
        self.mu.lockSharedUncancelable(@src());
        defer self.mu.unlockShared();
        return self.ns_routes.contains(namespace_id);
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
        return self.bestCompactionCandidateExcluding(null);
    }

    /// Same as `bestCompactionCandidate` but skips any tree whose id is
    /// in `skip` (used by parallel compactors so two fibers don't pick
    /// the same tree). `skip == null` means "no exclusions".
    pub fn bestCompactionCandidateExcluding(
        self: *Forest,
        skip: ?*const std.AutoHashMap(u64, void),
    ) ?CompactionTarget {
        self.mu.lockSharedUncancelable(@src());
        defer self.mu.unlockShared();

        var best: ?CompactionTarget = null;
        var it = self.trees.iterator();
        while (it.next()) |kv| {
            const tree_id = kv.key_ptr.*;
            if (skip) |s| if (s.contains(tree_id)) continue;
            const tree = kv.value_ptr.*;
            const candidate = tree.compact_candidate orelse continue;
            if (best == null or candidate.score > best.?.candidate.score) {
                best = .{ .tree = tree, .tree_id = tree_id, .candidate = candidate };
            }
        }
        return best;
    }

    /// Check if the current memtable should be frozen.
    /// Largest L0 table count among the trees that own data for this
    /// namespace. Used by the write-path backpressure in zdb-server so a
    /// hot namespace's bloated L0 throttles only its own writers, not
    /// every namespace on the node. After a tree split, a namespace can
    /// own multiple trees; we take the worst of them. Returns 0 if the
    /// namespace doesn't exist (caller already rejected the write).
    /// TODO: We should ideally do this based on the tree the keys are going to
    /// hit, instead of any tree in the namespace.
    pub fn maxL0TableCountForNs(self: *Forest, ns: u32) u32 {
        self.mu.lockSharedUncancelable(@src());
        defer self.mu.unlockShared();
        const route = self.ns_routes.get(ns) orelse return 0;
        var max_count: u32 = 0;
        for (route.entries.items) |e| {
            const tree = self.trees.get(e.tree_id) orelse continue;
            const n: u32 = @intCast(tree.levels[0].numTables());
            if (n > max_count) max_count = n;
        }
        return max_count;
    }

    pub fn shouldFreeze(self: *Forest) bool {
        return self.memtables.memoryUsage() >= self.opts.memtable_size;
    }

    /// Check if there are frozen memtables waiting to be flushed.
    pub fn hasFrozenMemtables(self: *Forest) bool {
        return self.memtables.peekOldest() != null;
    }

    // -----------------------------------------------------------------------
    // Stats dump
    // -----------------------------------------------------------------------

    /// What slice of the forest the caller wants to see. Default is a
    /// top-level summary that aggregates across all trees.
    pub const StatsScope = union(enum) {
        forest,
        tree: u64,
        namespace: u32,
    };

    pub const StatsOpts = struct {
        scope: StatsScope = .forest,
    };

    /// writeStats produces a hierarchical, human-readable text dump of
    /// current LSM state to `w`. Unlike the Prometheus metrics endpoint,
    /// this is for ops drill-down: per-tree shape, per-level sizes,
    /// pending compactions. Snapshot is best-effort — locks are taken
    /// briefly per section, so a concurrent flush/compaction may show
    /// itself half-applied. Acceptable for a debug endpoint.
    pub fn writeStats(self: *Forest, w: *std.Io.Writer, opts: StatsOpts) !void {
        switch (opts.scope) {
            .forest => try self.writeForestStats(w),
            .tree => |id| try self.writeTreeStats(w, id),
            .namespace => |ns| try self.writeNamespaceStats(w, ns),
        }
    }

    fn writeForestStats(self: *Forest, w: *std.Io.Writer) !void {
        const mt = self.memtables.stats();
        try w.print("forest:\n", .{});
        try w.print("  memtables: active=", .{});
        try writeBytes(w, mt.active_bytes);
        try w.print(" immutable={d} (", .{mt.immutable_count});
        try writeBytes(w, mt.immutable_bytes);
        try w.print(" pending flush)\n", .{});
        try w.print("  flush_pending: {d}\n", .{mt.immutable_count});

        // Per-level aggregate (sum + max-with-tree).
        const max_levels = self.opts.max_levels;
        const Agg = struct {
            tables_total: u64 = 0,
            tables_max: u64 = 0,
            tables_max_tree: u64 = 0,
            bytes_total: u64 = 0,
            bytes_max: u64 = 0,
            bytes_max_tree: u64 = 0,
        };
        const aggs = try self.allocator.alloc(Agg, max_levels);
        defer self.allocator.free(aggs);
        for (aggs) |*a| a.* = .{};

        // Per-level "compactions wanted" rollup.
        const Wanted = struct {
            count: u64 = 0,
            score_max: f64 = 0,
            score_max_tree: u64 = 0,
        };
        const wanted = try self.allocator.alloc(Wanted, max_levels);
        defer self.allocator.free(wanted);
        for (wanted) |*x| x.* = .{};

        // Largest tree (sum across levels).
        var largest_tree_id: u64 = 0;
        var largest_tree_size: i64 = 0;
        var tree_count: usize = 0;

        // Walk trees under the forest read lock, but only briefly: collect
        // the numbers we need, release. Each level access takes its own
        // lock inside numTables/getTotalSize.
        {
            self.mu.lockSharedUncancelable(@src());
            defer self.mu.unlockShared();
            tree_count = self.trees.count();
            var it = self.trees.iterator();
            while (it.next()) |kv| {
                const tree_id = kv.key_ptr.*;
                const tree = kv.value_ptr.*;

                var tree_total: i64 = 0;
                for (tree.levels, 0..) |*lv, idx| {
                    if (idx >= max_levels) break;
                    const tcount: u64 = @intCast(lv.numTables());
                    const tbytes: u64 = @intCast(lv.getTotalSize());
                    tree_total += @intCast(tbytes);

                    aggs[idx].tables_total += tcount;
                    if (tcount > aggs[idx].tables_max) {
                        aggs[idx].tables_max = tcount;
                        aggs[idx].tables_max_tree = tree_id;
                    }
                    aggs[idx].bytes_total += tbytes;
                    if (tbytes > aggs[idx].bytes_max) {
                        aggs[idx].bytes_max = tbytes;
                        aggs[idx].bytes_max_tree = tree_id;
                    }
                }
                if (tree_total > largest_tree_size) {
                    largest_tree_size = tree_total;
                    largest_tree_id = tree_id;
                }

                if (tree.compact_candidate) |c| {
                    if (c.level < max_levels) {
                        wanted[c.level].count += 1;
                        if (c.score > wanted[c.level].score_max) {
                            wanted[c.level].score_max = c.score;
                            wanted[c.level].score_max_tree = tree_id;
                        }
                    }
                }
            }
        }

        try w.print("  trees: {d}\n", .{tree_count});
        try w.print("  largest_tree: id={d} size=", .{largest_tree_id});
        try writeBytes(w, @intCast(largest_tree_size));
        try w.print("\n\n", .{});

        try w.print("  level summary (across all trees):\n", .{});
        for (aggs, 0..) |a, idx| {
            // Skip levels that have nothing — shows max_levels=7 dumps cleanly.
            if (a.tables_total == 0 and a.bytes_total == 0) continue;
            try w.print("    L{d}: tables={d} (max={d} in tree {d})  bytes=", .{
                idx, a.tables_total, a.tables_max, a.tables_max_tree,
            });
            try writeBytes(w, a.bytes_total);
            try w.print(" (max=", .{});
            try writeBytes(w, a.bytes_max);
            try w.print(" in tree {d})\n", .{a.bytes_max_tree});
        }

        var any_wanted = false;
        for (wanted) |x| if (x.count > 0) {
            any_wanted = true;
            break;
        };
        if (any_wanted) {
            try w.print("\n  compactions wanted:\n", .{});
            for (wanted, 0..) |x, idx| {
                if (x.count == 0) continue;
                try w.print("    L{d}: trees={d} score_max={d:.2} (tree {d})\n", .{
                    idx, x.count, x.score_max, x.score_max_tree,
                });
            }
        }
    }

    fn writeTreeStats(self: *Forest, w: *std.Io.Writer, tree_id: u64) !void {
        self.mu.lockSharedUncancelable(@src());
        defer self.mu.unlockShared();

        const tree = self.trees.get(tree_id) orelse {
            try w.print("tree id={d}: not found\n", .{tree_id});
            return;
        };
        const ns_id: u32 = if (self.ms.trees.get(tree_id)) |ts| ts.namespace_id else 0;
        const total = tree.totalSize();

        try w.print("tree id={d} namespace={d} size=", .{ tree_id, ns_id });
        try writeBytes(w, @intCast(total));
        try w.print("\n", .{});

        for (tree.levels, 0..) |*lv, idx| {
            const tcount = lv.numTables();
            const tbytes: u64 = @intCast(lv.getTotalSize());
            if (tcount == 0 and tbytes == 0) continue;
            try w.print("  L{d}: {d} tables, ", .{ idx, tcount });
            try writeBytes(w, tbytes);
            try w.print("\n", .{});
        }
        if (tree.compact_candidate) |c| {
            try w.print("  compact_candidate: L{d} score={d:.2}\n", .{ c.level, c.score });
        }
    }

    fn writeNamespaceStats(self: *Forest, w: *std.Io.Writer, ns_id: u32) !void {
        try w.print("namespace={d}:\n", .{ns_id});
        var any = false;
        // Collect tree ids in this namespace under the lock, then drop and
        // call writeTreeStats for each (which re-acquires its own lock).
        var ids: std.ArrayList(u64) = .{};
        defer ids.deinit(self.allocator);
        {
            self.mu.lockSharedUncancelable(@src());
            defer self.mu.unlockShared();
            var it = self.ms.trees.iterator();
            while (it.next()) |kv| {
                if (kv.value_ptr.namespace_id != ns_id) continue;
                try ids.append(self.allocator, kv.key_ptr.*);
            }
        }
        for (ids.items) |id| {
            any = true;
            try writeTreeStats(self, w, id);
        }
        if (!any) try w.print("  (no trees)\n", .{});
    }

    /// Pretty-print a byte count: KiB / MiB / GiB / TiB. Two decimal places
    /// of precision past the smallest meaningful unit.
    fn writeBytes(w: *std.Io.Writer, b: u64) !void {
        const kib: u64 = 1024;
        const mib: u64 = 1024 * kib;
        const gib: u64 = 1024 * mib;
        const tib: u64 = 1024 * gib;
        if (b >= tib) {
            try w.print("{d:.2}TiB", .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(tib))});
        } else if (b >= gib) {
            try w.print("{d:.2}GiB", .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(gib))});
        } else if (b >= mib) {
            try w.print("{d:.2}MiB", .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(mib))});
        } else if (b >= kib) {
            try w.print("{d:.2}KiB", .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(kib))});
        } else {
            try w.print("{d}B", .{b});
        }
    }

    /// Check if an SST file exists on disk.
    pub fn hasFile(self: *Forest, file_id: u64) bool {
        const path = fi.sstPath(self.allocator, self.opts.dir, file_id) catch return false;
        defer self.allocator.free(path);
        std.fs.cwd().access(path, .{}) catch return false;
        return true;
    }

    /// Write an SST file to disk and pre-open it into the table cache.
    /// Skips the write when the file is already cached, or already on disk with
    /// a matching size — file_id → content is deterministic in this system, and
    /// a size match is a strong signal the bytes are correct (no cross-ID
    /// collisions possible). If a size mismatch is detected, we log and rewrite.
    pub fn writeFile(self: *Forest, file_id: u64, data: []const u8) void {
        const fe = std.log.scoped(.file_event);
        if (self.table_cache.contains(file_id)) {
            fe.info("event=write_file file_id={d} path=cache_hit", .{file_id});
            return;
        }

        const path = fi.sstPath(self.allocator, self.opts.dir, file_id) catch return;
        defer self.allocator.free(path);

        // Probe existence + size via vfs (no allocation, just open + getEndPos).
        const existing_size: ?u64 = if (vfs.g().openFile(self.allocator, path, .{})) |fh| blk: {
            defer fh.close();
            break :blk fh.file_size;
        } else |_| null;

        const size_matches = existing_size != null and existing_size.? == data.len;
        if (existing_size != null and !size_matches) {
            log.warn("writeFile: size mismatch for {d} ({d} on disk vs {d}), rewriting", .{
                file_id, existing_size.?, data.len,
            });
        }

        if (!size_matches) {
            const fh = vfs.g().createFile(path, .{ .mode = .direct, .flags = .{ .truncate = true } }) catch |err| {
                log.err("writeFile: createFile failed for {d}: {s}", .{ file_id, @errorName(err) });
                return;
            };
            fh.write(data, 0, .p1) catch |err| {
                log.err("writeFile: write failed for {d}: {s}", .{ file_id, @errorName(err) });
                fh.close();
                return;
            };
            fh.sync() catch {};
            fh.close();
            fe.info("event=write_file file_id={d} path=wrote bytes={d}", .{ file_id, data.len });
        } else {
            fe.info("event=write_file file_id={d} path=disk_hit bytes={d}", .{ file_id, data.len });
        }

        // Pre-open the table into the cache so it's ready when compaction needs it.
        const t = Table.open(self.allocator, path, .p1) catch |err| {
            log.err("writeFile: Table.open failed for {d}: {s}", .{ file_id, @errorName(err) });
            return;
        };
        self.table_cache.own(file_id, t) catch |err| {
            log.err("writeFile: table_cache.own failed for {d}: {s}", .{ file_id, @errorName(err) });
        };
    }

    /// Read an SST file from disk. Caller owns the returned slice.
    pub fn readFile(self: *Forest, allocator: std.mem.Allocator, file_id: u64) ?[]const u8 {
        const path = fi.sstPath(self.allocator, self.opts.dir, file_id) catch return null;
        defer self.allocator.free(path);
        const handle = vfs.g().openFile(allocator, path, .{}) catch return null;
        defer handle.close();
        const result = handle.readAligned(allocator, 0, @intCast(handle.file_size), .p1) catch return null;
        return result.data;
    }

    /// splitTreeForTests splits the given tree at the median key of its
    /// bottommost level, locally, without going through Raft. **Test-only**
    /// — production must propose splits through the manifest pipeline so
    /// all nodes apply the same split deterministically; see
    /// `proposeSplit` in zdb-server. Caller must hold the write lock.
    pub fn splitTreeForTests(self: *Forest, tree_id: u64) !void {
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
            llv.mu.lockSharedUncancelable(@src());
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
            .cmd = 4, // tree_split
        };
    }

    /// applySplit is Phase B: commits the manifest changes, opens the new tree,
    /// updates the old tree's key range, removes migrated tables, and updates
    /// namespace routing. Called on all nodes after Raft commit.
    /// TODO(advance-tree-id): this path doesn't `advanceTreeId(new_tree_id)`,
    /// so a follower that becomes leader right after applying a split can
    /// re-issue the split's new id. Add the bump after the manifest commit
    /// (mirrors `applyNamespaceCreate`).
    pub fn applySplit(self: *Forest, mc: manifest.ManifestChange) !void {
        // Commit MANIFEST so we can recover from a crash.
        try self.ms.commit(mc.table_changes, mc.tree_changes);

        // Find the old tree_id (UPDATE) and new tree_id (CREATE) from tree_changes.
        var old_tree_id: u64 = 0;
        var new_tree_id: u64 = 0;
        var namespace_id: u32 = 0;
        for (mc.tree_changes) |tc| {
            if (tc.op == .update) { // UPDATE
                old_tree_id = tc.tree_id;
            } else if (tc.op == .create) { // CREATE
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
            if (c.op == .delete and c.tree_id == old_tree_id) { // DELETE from old tree
                left_tree.levels[c.level].deleteEntry(c.table_id);
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
