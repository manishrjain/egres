const std = @import("std");
const zio = @import("zio");
const utils = @import("utils");
const options = @import("options.zig");
const manifest = @import("manifest.zig");
const level_handler = @import("level.zig");
const table_mod = @import("table.zig");
const vfs = @import("vfs");
const metrics = @import("metrics.zig");

const Table = table_mod.Table;
const TableIterator = table_mod.TableIterator;
const Item = table_mod.table.Item;
const Level = level_handler.Level;
const TableEntry = level_handler.TableEntry;
const Key = @import("key.zig").Key;
const KeyRange = level_handler.KeyRange;
const MergeIterator = table_mod.merge_iterator.MergeIterator;
const ConcatIterator = table_mod.merge_iterator.ConcatIterator;
const Iterator = table_mod.merge_iterator.Iterator;
const Builder = table_mod.builder.Builder;

const log = std.log.scoped(.zdb_tree);

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

const Targets = struct {
    allocator: std.mem.Allocator,
    base_level: u8,
    target_sz: []i64,
    file_sz: []i64,

    fn deinit(self: *Targets) void {
        self.allocator.free(self.target_sz);
        self.allocator.free(self.file_sz);
    }
};

const LevelPriority = struct {
    level: usize,
    score: f64,
    adjusted: f64,
};

// ---------------------------------------------------------------------------
// TableCache
// ---------------------------------------------------------------------------

pub const TableNeed = struct { id: u64, level: usize, table: ?*Table = null };

/// TableCache is a Forest-owned registry of open *Table objects keyed by table_id.
/// Tree.open() calls loadTables() instead of opening SST files itself, so a table
/// opened for one tree is reused when the same file is needed by another tree
/// (e.g. shared tables after a split).
pub const TableCache = struct {
    allocator: std.mem.Allocator,
    dir: []const u8,
    tables: std.AutoHashMap(u64, *Table),
    mu: std.Thread.RwLock,

    pub fn init(allocator: std.mem.Allocator, dir: []const u8) TableCache {
        return .{
            .allocator = allocator,
            .dir = dir,
            .tables = std.AutoHashMap(u64, *Table).init(allocator),
            .mu = .{},
        };
    }

    pub fn deinit(self: *TableCache) void {
        var it = self.tables.iterator();
        while (it.next()) |kv| {
            const table = kv.value_ptr.*;
            // detach + decrRef mirrors Level.deinit: the path is cleared so
            // the file isn't deleted, and decrRef at zero frees the index /
            // binary_fuse / file handle (via Table.deinit). Without this,
            // those allocations leak — `destroy(table)` only frees the
            // *Table struct memory, not its owned fields.
            table.detach();
            table.decrRef();
            self.allocator.destroy(table);
        }
        self.tables.deinit();
    }

    /// contains reports whether a table with this id is currently cached.
    pub fn contains(self: *TableCache, id: u64) bool {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.tables.contains(id);
    }

    /// own hands ownership of a *Table to the cache. The cache incrRef's so
    /// the table stays alive until explicitly released (e.g. after a snapshot
    /// confirms no follower needs the backing SST file). Caller must not
    /// destroy t after this call.
    pub fn own(self: *TableCache, id: u64, t: *Table) !void {
        self.mu.lock();
        defer self.mu.unlock();
        std.debug.assert(!self.tables.contains(id));
        t.incrRef();
        try self.tables.put(id, t);
    }

    /// Evict tables whose ref has reached zero. Since own() holds a cache ref,
    /// a table only becomes evictable after its cache ref is explicitly released
    /// (e.g. after a snapshot confirms no follower needs the SST file).
    pub fn clearStale(self: *TableCache) void {
        self.mu.lock();
        defer self.mu.unlock();

        var to_evict: std.ArrayList(u64) = .{};
        defer to_evict.deinit(self.allocator);

        var it = self.tables.iterator();
        while (it.next()) |entry| {
            const table = entry.value_ptr.*;
            if (table.ref.load(.acquire) == 0) {
                to_evict.append(self.allocator, entry.key_ptr.*) catch continue;
            }
        }

        for (to_evict.items) |id| {
            if (self.tables.fetchRemove(id)) |entry| {
                self.allocator.destroy(entry.value);
            }
        }
    }

    /// loadTables serves each TableNeed from cache (read lock) or opens it
    /// concurrently via zio fibers (write lock per miss). On return, each
    /// need's .table field points to an open *Table owned by the cache.
    /// The caller is responsible for installing these tables into levels
    /// (e.g. via Level.addEntry) — this keeps loadTables free of any
    /// level-layout assumptions.
    pub fn loadTables(self: *TableCache, needed: []TableNeed) !void {
        // Phase 1: serve cache hits under read lock; collect pointers for misses.
        var misses: std.ArrayList(*TableNeed) = .{};
        defer misses.deinit(self.allocator);
        {
            self.mu.lockShared();
            defer self.mu.unlockShared();
            for (needed) |*n| {
                if (self.tables.get(n.id)) |t| {
                    std.debug.assert(t.ref.load(.acquire) > 0);
                    n.table = t;
                } else {
                    try misses.append(self.allocator, n);
                }
            }
        }
        if (misses.items.len == 0) return;

        // Phase 2: open misses concurrently. Each fiber writes to need.table.
        var tse: utils.ThreadSafeError = .{};
        var group = zio.group();
        defer group.cancel();
        for (misses.items) |n| {
            try group.spawn(openFiber, .{ self, n, &tse });
        }
        group.wait() catch {};
        if (tse.get()) |err| return err;
    }

    fn openFiber(
        self: *TableCache,
        need: *TableNeed,
        tse: *utils.ThreadSafeError,
    ) void {
        // Phase A: check under write lock (brief — no I/O).
        self.mu.lock();
        if (self.tables.get(need.id)) |t| {
            self.mu.unlock();
            need.table = t;
            return;
        }
        self.mu.unlock();

        // Phase B: open the SST file WITHOUT holding the lock.
        // Table.open may yield the fiber (async I/O); holding the lock here
        // would deadlock cooperative fibers on the same OS thread (EDEADLK).
        const path = options.sstPath(self.allocator, self.dir, need.id) catch |err| {
            tse.set(err);
            return;
        };
        defer self.allocator.free(path);
        const t = Table.open(self.allocator, path, .p0) catch |err| {
            tse.set(err);
            return;
        };
        defer t.decrRef(); // Drop the open-time ref; own() takes its own.

        // Phase C: re-acquire write lock to insert; handle race with another fiber.
        self.mu.lock();
        if (self.tables.get(need.id)) |existing| {
            // Another fiber opened the same table while we were in phase B.
            self.mu.unlock();
            need.table = existing;
            return;
        }
        // Cache takes its own ref; defer above drops the open-time ref.
        t.incrRef();
        self.tables.put(need.id, t) catch |err| {
            self.mu.unlock();
            tse.set(err);
            return;
        };
        self.mu.unlock();
        need.table = t;
    }
};

// ---------------------------------------------------------------------------
// Tree
// ---------------------------------------------------------------------------
//
// The terminology used here is that data starts flowing in Level 0 (top most)
// and goes to lower levels (Level 1, Level 2, onwards).
// Sometimes, people would call Level 3 as an "upper level", but in my
// terminology, it would be a "lower" level.
// I think of this as a pyramid, where flowing happens at top (L0) and then settles
// to lower levels (L1..n).
// If it helps, we can think of L1..n as basement levels, where i is the ith floor
// below ground. DB Infrastructure works way under the API surface, so this is apt.

/// A tree's pending work item, picked by `Forest.bestCompactionCandidate`.
/// `needs_split=true` means the tree has outgrown `tree_split_size` and
/// should be split instead of compacted; the `level` field then describes
/// the lowest non-empty level (informational, since split operates across
/// all levels at the median key, not on a single level). `score` is bumped
/// above the natural compaction range so a split preempts any pending
/// compaction in the forest.
pub const CompactCandidate = struct {
    level: u8,
    score: f64,
    needs_split: bool = false,
};

/// Score assigned to a tree that needs a split. Comfortably above any
/// natural compaction adjusted-score so splits always rank first.
pub const split_priority_score: f64 = 100.0;

pub const Tree = struct {
    allocator: std.mem.Allocator,
    opts: options.Options,
    tree_id: u64,
    ms: *manifest.ForestState,
    cache: *TableCache,
    levels: []Level,
    compact_candidate: ?CompactCandidate = null,
    key_range: KeyRange,

    /// open builds a Tree from an existing manifest state.
    /// Tables are loaded via cache, which opens SST files concurrently for misses
    /// and reuses already-open *Table objects for cache hits.
    pub fn open(allocator: std.mem.Allocator, opts: options.Options, tree_id: u64, ms: *manifest.ForestState, cache: *TableCache) !Tree {
        const levels = try allocator.alloc(Level, opts.max_levels);
        errdefer allocator.free(levels);
        for (levels, 0..) |*l, i| {
            l.* = Level.init(allocator, @intCast(i));
        }
        errdefer for (levels) |*l| l.deinit();

        var kr: KeyRange = .{};
        errdefer {
            kr.left.free(allocator);
            kr.right.free(allocator);
        }

        if (ms.trees.getPtr(tree_id)) |tree| {
            kr.left = try tree.left.dupe(allocator);
            kr.right = try tree.right.dupe(allocator);
            if (kr.left.len() == 0 and kr.right.len() == 0) {
                kr.inf = true;
            }

            var needed: std.ArrayList(TableNeed) = .{};
            defer needed.deinit(allocator);
            for (tree.levels.items, 0..) |lm, level_idx| {
                if (level_idx >= opts.max_levels) continue;
                var tit = lm.tables.keyIterator();
                while (tit.next()) |tid| {
                    try needed.append(allocator, .{ .id = tid.*, .level = level_idx });
                }
            }
            try cache.loadTables(needed.items);
            for (needed.items) |n| {
                try levels[n.level].addEntry(n.id, n.table.?);
            }
        }

        var tree: Tree = .{
            .allocator = allocator,
            .opts = opts,
            .tree_id = tree_id,
            .ms = ms,
            .cache = cache,
            .levels = levels,
            .key_range = kr,
        };
        try tree.updateCompactCandidate();
        return tree;
    }

    /// close tears down all level levels, freeing each table's memory without
    /// deleting SST files (which are managed by the manifest).
    pub fn close(self: *Tree) void {
        for (self.levels) |*h| h.deinit();
        self.allocator.free(self.levels);
        self.key_range.left.free(self.allocator);
        self.key_range.right.free(self.allocator);
    }

    /// get checks each level from L0 upward and returns the first matching item,
    /// or null if the key is absent. The returned item's key and value are
    /// duplicated with self.allocator; the caller must free them.
    pub fn get(self: *Tree, search_key: Key, priority: vfs.Priority) !?Item {
        for (self.levels) |*h| {
            if (try h.get(search_key, priority)) |item| return item;
        }
        return null;
    }

    pub fn totalSize(self: *Tree) i64 {
        var total: i64 = 0;
        for (self.levels) |*l| total += l.getTotalSize();
        return total;
    }

    pub fn lowestLevelSize(self: *Tree) struct {
        level: u32,
        size: i64,
    } {
        var idx: usize = self.levels.len;
        while (idx > 0) {
            idx -= 1;
            const sz = self.levels[idx].getTotalSize();
            if (sz > 0) return .{ .level = @intCast(idx), .size = sz };
        }
        return .{ .level = 0, .size = 0 };
    }

    pub fn findSplitKey(self: *Tree, allocator: std.mem.Allocator) !Key {
        const linfo = self.lowestLevelSize();
        if (linfo.level == 0) {
            return .{};
        }
        const level = &self.levels[linfo.level];

        level.mu.lockShared();
        defer level.mu.unlockShared();

        const items = level.tables.items;
        if (items.len < 2) {
            return .{};
        }
        const mid = items.len / 2;
        var key = try items[mid].table.smallest.dupe(allocator);
        std.debug.assert(key.len() > 0);

        // This is the smallest key in the table. We do not want that any primary key gets split
        // across trees. So, we'd take the absolutely max possible timestamp in the split key. That
        // way, all the timestamps would fall to the right of this key, and fully belong to the
        // newly split tree on the right.
        key.timestamp = std.math.maxInt(u64);
        return key;
    }

    // -----------------------------------------------------------------------
    // Level sizing
    // -----------------------------------------------------------------------

    fn levelTargets(self: *Tree) !Targets {
        const n = self.opts.max_levels;
        const target_sz = try self.allocator.alloc(i64, n);
        errdefer self.allocator.free(target_sz);
        const file_sz = try self.allocator.alloc(i64, n);
        errdefer self.allocator.free(file_sz);

        @memset(target_sz, 0);
        @memset(file_sz, 0);

        var base_level: u8 = 0;
        var db_size: i64 = self.levels[n - 1].getTotalSize();

        // Compute target sizes from the last level down to L0.
        var i: usize = n - 1;
        while (i > 0) : (i -= 1) {
            const ltarget = @max(db_size, self.opts.base_level_size);
            target_sz[i] = ltarget;
            // Set base_level on the first iteration where the level is at or
            // below the base size (mirrors Badger's sentinel-0 trick).
            if (base_level == 0 and ltarget <= self.opts.base_level_size) {
                base_level = @intCast(i);
            }
            db_size = @divTrunc(db_size, self.opts.level_size_multiplier);
        }

        // If any level between L1 and base_level-1 already has data, we must
        // set base_level to that level — L0 compaction can't skip over a non-empty level.
        if (base_level > 1) {
            for (1..base_level) |j| {
                if (self.levels[j].getTotalSize() > 0) {
                    base_level = @intCast(j);
                    break;
                }
            }
        }

        // Compute file sizes.
        var tsz: i64 = self.opts.base_table_size;
        file_sz[0] = @intCast(self.opts.memtable_size);
        for (1..n) |j| {
            if (j <= base_level) {
                file_sz[j] = tsz;
            } else {
                tsz = @min(tsz * self.opts.table_size_multiplier, self.opts.max_table_size);
                file_sz[j] = tsz;
            }
        }

        return .{
            .allocator = self.allocator,
            .base_level = base_level,
            .target_sz = target_sz,
            .file_sz = file_sz,
        };
    }

    // -----------------------------------------------------------------------
    // Compaction selection
    // -----------------------------------------------------------------------

    /// updateCompactCandidate recomputes which level most needs compaction and
    /// stores the result in self.compact_candidate. Called after open and after
    /// every doCompact so the candidate is always fresh.
    pub fn updateCompactCandidate(self: *Tree) !void {
        // Split check overrides compaction. If Lmax has outgrown the split
        // threshold, schedule a split ahead of any compaction work — the
        // split is manifest-only (cheap) and shrinks the tree before the
        // next compaction merges into an even larger Lmax. The score is
        // bumped to `split_priority_score` so this tree wins over normal
        // compaction candidates in the forest. Set `level` to the lowest
        // non-empty level for diagnostic legibility (`stats forest`).
        const linfo = self.lowestLevelSize();
        if (linfo.size > self.opts.tree_split_size) {
            // Log every split-trigger so we can spot mis-sized splits (e.g.
            // tree_split_size mistakenly compared against an upper-level
            // size instead of Lmax). Format: tree=<id> deepest_level=<L>
            // size=<bytes> > threshold=<bytes>.
            log.info("split-trigger tree={d} deepest_level={d} size={d} > threshold={d}", .{
                self.tree_id, linfo.level, linfo.size, self.opts.tree_split_size,
            });
            self.compact_candidate = .{
                .level = @intCast(linfo.level),
                .score = split_priority_score,
                .needs_split = true,
            };
            return;
        }

        var t = try self.levelTargets();
        defer t.deinit();

        const n = self.opts.max_levels;
        const prios = try self.allocator.alloc(LevelPriority, n);
        defer self.allocator.free(prios);

        // L0: count-based score.
        const l0_score = @as(f64, @floatFromInt(self.levels[0].numTables())) /
            @as(f64, @floatFromInt(self.opts.num_level_zero_tables));
        prios[0] = .{ .level = 0, .score = l0_score, .adjusted = l0_score };

        // If L0 needs compaction, execute it. We don't want to block writes because lower
        // levels are out of shape. Lower levels being out of shape is not as bad as blocking
        // user facing writes.
        if (l0_score >= 2.5) {
            self.compact_candidate = .{ .level = 0, .score = l0_score };
            return;
        }

        // L≥1: size-based score.
        for (1..n) |j| {
            const sz = self.levels[j].getTotalSize();
            const target = t.target_sz[j];
            const score = if (target > 0)
                @as(f64, @floatFromInt(sz)) / @as(f64, @floatFromInt(target))
            else
                0.0;
            prios[j] = .{ .level = j, .score = score, .adjusted = score };
        }

        // Adjusted scores: mirrors Badger/PebbleDB's priority dampening.
        // upperLevel starts at 0; for each level from base_level downward, if
        // prios[upperLevel].adjusted >= 1.0, divide it by the current level's
        // adjusted score (floored at 0.01).
        var upper_level: usize = 0;
        for (@as(usize, t.base_level)..n) |cur_level| {
            if (prios[upper_level].adjusted >= 1.0) {
                // If current level's adjusted score (= score, initially) is < 1.0, then it would
                // boost the uppper level's adjusted score AS. If it's > 1.0, then it would dampen
                // the upper level's AS. This helps because there's no point compacting something
                // at upper level when the level below it is already too big. This basically helps
                // make space in lower levels first before compacting upper levels.
                const denom = @max(prios[cur_level].adjusted, 0.01);
                prios[upper_level].adjusted /= denom;
            } else {
                // If adjusted score is < 1.0, it won't be compacted anyways,
                // so we can safely skip calculating the adjusted score.
            }
            upper_level = cur_level;
        }

        // Collect levels with score >= 1.0 (excluding the last level).
        var filtered = try std.ArrayList(LevelPriority).initCapacity(self.allocator, n);
        defer filtered.deinit(self.allocator);
        for (prios[0 .. n - 1]) |p| {
            if (p.score >= 1.0) try filtered.append(self.allocator, p);
        }
        if (filtered.items.len == 0) {
            self.compact_candidate = null;
            return;
        }

        // Sort descending by adjusted score.
        std.mem.sort(LevelPriority, filtered.items, {}, struct {
            fn gt(_: void, a: LevelPriority, b: LevelPriority) bool {
                return a.adjusted > b.adjusted;
            }
        }.gt);

        const winner = filtered.items[0];
        if (winner.adjusted <= 1.0) {
            self.compact_candidate = null;
        } else {
            self.compact_candidate = .{ .level = @intCast(winner.level), .score = winner.adjusted };
        }
    }

    // -----------------------------------------------------------------------
    // Compaction execution
    // -----------------------------------------------------------------------

    /// doCompact runs the full compaction pipeline: buildCompaction (Phase A)
    /// then applyCompaction (Phase B). For Raft integration, callers can use
    /// buildCompaction and applyCompaction separately.
    pub fn doCompact(self: *Tree, src_level: u8, next_file_id: *std.atomic.Value(u64)) !void {
        var changes = try self.buildCompaction(src_level, next_file_id);
        defer changes.deinit(self.allocator);
        try self.applyCompaction(changes.items);
    }

    /// buildCompaction is Phase A: merges src tables with overlapping dest tables,
    /// writes new SST files, and returns the manifest TableChange list describing
    /// the result. The caller is responsible for proposing these changes (e.g.
    /// through Raft) and then calling applyCompaction.
    pub fn buildCompaction(self: *Tree, src_level: u8, next_file_id: *std.atomic.Value(u64)) !std.ArrayList(manifest.TableChange) {
        const src: usize = src_level;
        const prio: vfs.Priority = if (src_level == 0) .p1 else .p2;

        var t = try self.levelTargets();
        defer t.deinit();

        const dest: usize = if (src_level == 0) t.base_level else @as(usize, src_level) + 1;
        if (dest >= self.opts.max_levels) return .{};

        // --- collect src entries (all of L0; first entry of L≥1) ---
        const src_entries_slice = blk: {
            self.levels[src].mu.lockShared();
            defer self.levels[src].mu.unlockShared();

            const items = self.levels[src].tables.items;
            if (items.len == 0) return .{};

            // TODO: This can be improved to consider which table to compact.
            const take = if (src_level == 0) items else items[0..1];
            break :blk try self.allocator.dupe(TableEntry, take);
        };
        defer self.allocator.free(src_entries_slice);

        // --- compute union key range of all src tables ---
        var kr = KeyRange{};
        for (src_entries_slice) |e| {
            const tkr = KeyRange{ .left = e.table.smallest, .right = e.table.biggest };
            kr.extend(tkr);
        }

        // --- collect overlapping dest (bot) entries ---
        const bot_entries_slice = try self.levels[dest].overlappingTables(self.allocator, kr);
        defer self.allocator.free(bot_entries_slice);

        // --- build single MergeIterator over src tables + one ConcatIterator for bot ---
        const bot_iters = try self.allocator.alloc(TableIterator, bot_entries_slice.len);
        defer self.allocator.free(bot_iters);

        const all_iters = try self.allocator.alloc(Iterator, src_entries_slice.len + 1);
        defer self.allocator.free(all_iters);

        for (src_entries_slice, 0..) |e, i|
            all_iters[i] = .{ .table = TableIterator.init(self.allocator, e.table, prio) };
        for (bot_entries_slice, 0..) |e, i|
            bot_iters[i] = TableIterator.init(self.allocator, e.table, prio);
        all_iters[src_entries_slice.len] = .{ .concat = ConcatIterator.init(bot_iters) };

        var merge = MergeIterator.init(self.allocator, all_iters);
        defer merge.deinit();
        try merge.seekRange(self.key_range.left, self.key_range.right);

        // --- output file IDs produced by this compaction ---
        var output_file_ids: std.ArrayList(u64) = .{};
        defer output_file_ids.deinit(self.allocator);

        // --- merge loop ---
        const cfg = table_mod.builder.Config{
            .target_block_size = self.opts.block_size,
            .codec = self.opts.compression,
            .priority = prio,
        };
        const file_sz: usize = @intCast(t.file_sz[dest]);

        var bld = Builder.init(self.allocator, cfg);
        defer bld.deinit();

        const FlushHandle = zio.JoinHandle(?FlushResult);
        var flush_handles: std.ArrayList(FlushHandle) = .empty;
        defer {
            for (flush_handles.items) |*h| _ = h.join();
            flush_handles.deinit(self.allocator);
        }

        while (merge.itemRef()) |item| {
            if (bld.table_bytes.items.len >= file_sz) {
                // We are not going to break up a primary key across tables. All the versions for
                // this key should live in the same table. This avoids a scenario where a table with
                // a higher version key ends up getting compacted and living in a lower level. We
                // expect that the flow of keys is from top to bottom levels, so we're expecting to
                // find higher version keys at higher levels. This expectation is used by get fn to
                // stop searching for key as soon as we're able to find a version >= search key.
                if (!item.key.equalPrimary(bld.lastKey())) {
                    try self.flushBuilderOutput(&bld, &flush_handles, next_file_id);
                    bld.reset();
                }
            }
            try bld.add(item.key, item.value);
            try merge.next();
        }

        // Flush any remaining data.
        try self.flushBuilderOutput(&bld, &flush_handles, next_file_id);

        // Start dir sync while we're waiting for file syncs to be done. We can do dir sync here
        // because files have already been created and written. So, dir should have everything it
        // needs.
        var dir_sync = try zio.spawn(vfs.VFS.syncDir, .{self.opts.dir});
        defer dir_sync.join();

        // Join all sync fibers and collect output file IDs.
        for (flush_handles.items) |*h| {
            const result = h.join() orelse return error.FlushFailed;
            try output_file_ids.append(self.allocator, result.file_id);
        }
        flush_handles.clearAndFree(self.allocator);

        // --- build manifest changes ---
        var changes = try std.ArrayList(manifest.TableChange).initCapacity(
            self.allocator,
            src_entries_slice.len + bot_entries_slice.len + output_file_ids.items.len,
        );

        for (src_entries_slice) |e| try changes.append(
            self.allocator,
            manifest.deleteTableChange(e.id, self.tree_id, @intCast(src)),
        );
        for (bot_entries_slice) |e| try changes.append(
            self.allocator,
            manifest.deleteTableChange(e.id, self.tree_id, @intCast(dest)),
        );
        for (output_file_ids.items) |fid| {
            try changes.append(self.allocator, manifest.createTableChange(fid, self.tree_id, @intCast(dest))); // usize → u32
        }

        return changes;
    }

    /// applyCompaction is Phase B: commits the manifest changes and updates the
    /// in-memory levels by opening tables from the SST directory.
    /// Called after Raft commit on all nodes (leader and followers).
    pub fn applyCompaction(self: *Tree, changes: []const manifest.TableChange) !void {
        if (changes.len == 0) return;

        // Commit to manifest (durable) before touching the levels.
        try self.ms.commit(changes, &.{});

        // Batch-load all CREATE tables via the cache in a single concurrent
        // pass. writeFile may have pre-opened them (follower path) → hit;
        // otherwise loadTables opens them.
        var needed: std.ArrayList(TableNeed) = .{};
        defer needed.deinit(self.allocator);
        for (changes) |c| {
            if (c.op == .create) {
                try needed.append(self.allocator, .{ .id = c.table_id, .level = c.level });
            }
        }
        try self.cache.loadTables(needed.items);

        // Apply level changes in original order: delete old entries, add new.
        // Track total bytes created and the destination level for metrics.
        var need_idx: usize = 0;
        var bytes_out: u64 = 0;
        var dest_level: ?u8 = null;
        for (changes) |c| {
            if (c.op == .delete) { // DELETE
                self.levels[c.level].deleteEntry(c.table_id);
            } else if (c.op == .create) { // CREATE
                const n = needed.items[need_idx];
                need_idx += 1;
                std.debug.assert(c.table_id == n.id);
                try self.levels[c.level].addEntry(n.id, n.table.?);
                bytes_out += n.table.?.fileSize();
                dest_level = @intCast(c.level);
            }
        }

        // Metrics: one compaction run, plus bytes written to the dest level.
        const ns_id: u32 = if (self.ms.trees.get(self.tree_id)) |ts| ts.namespace_id else 0;
        if (dest_level) |lvl| {
            try metrics.g().compactions.incr(.{
                .namespace_id = ns_id,
                .tree_id = self.tree_id,
                .level = lvl,
            });
            try metrics.g().bytes_written.incrBy(.{
                .source = "compact",
                .namespace_id = ns_id,
                .tree_id = self.tree_id,
                .level = lvl,
            }, bytes_out);
        }

        try self.updateCompactCandidate();
    }

    const FlushResult = struct { file_id: u64 };

    /// flushBuilderOutput finishes the builder, writes bytes to a new SST file,
    /// then spawns a fiber to sync it. The JoinHandle is appended to handles.
    fn flushBuilderOutput(
        self: *Tree,
        bld: *Builder,
        handles: *std.ArrayList(zio.JoinHandle(?FlushResult)),
        next_file_id: *std.atomic.Value(u64),
    ) !void {
        var tb = try bld.finish();
        defer tb.deinit();
        if (tb.bytes.len == 0) return;

        const id = next_file_id.fetchAdd(1, .monotonic);
        const path = try options.sstPath(self.allocator, self.opts.dir, id);
        errdefer self.allocator.free(path);

        const wh = try vfs.g().createFile(path, .{ .mode = .direct });
        try wh.write(tb.bytes, 0, bld.cfg.priority);

        // Sync in a fiber; data is in page cache after write.
        try handles.append(self.allocator, try zio.spawn(syncFile, .{
            self.allocator, wh, path, id,
        }));
    }

    fn syncFile(
        allocator: std.mem.Allocator,
        wh: vfs.FileHandle,
        path: []const u8,
        file_id: u64,
    ) ?FlushResult {
        defer allocator.free(path);
        wh.sync() catch return null;
        wh.close();
        return .{ .file_id = file_id };
    }
};
