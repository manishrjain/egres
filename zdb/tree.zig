const std = @import("std");
const zio = @import("zio");
const utils = @import("utils");
const config = @import("config.zig");
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

        // Phase 2: open misses concurrently. Each fiber writes to need.table
        // and bumps `completed` on return; the watchdog uses that to log
        // periodic progress if the group hangs.
        var tse: utils.ThreadSafeError = .{};
        var completed = std.atomic.Value(usize).init(0);
        const total = misses.items.len;
        var group = zio.group();
        defer group.cancel();
        group.spawn(loadTablesWatchdog, .{ total, &completed }) catch {};
        for (misses.items) |n| {
            try group.spawn(openFiber, .{ self, n, &tse, &completed });
        }
        group.wait() catch {};
        if (tse.get()) |err| return err;
    }

    /// Sibling of the `openFiber` spawns: poll the completed counter
    /// every 500 ms and log every 15 s while we're still short. Exits
    /// as soon as all opens are accounted for, so it doesn't hold the
    /// group's `wait()` open.
    fn loadTablesWatchdog(total: usize, completed: *std.atomic.Value(usize)) void {
        const start = std.time.milliTimestamp();
        var last_warn = start;
        while (completed.load(.acquire) < total) {
            zio.sleep(zio.Duration.fromMilliseconds(500)) catch return;
            const now = std.time.milliTimestamp();
            if (now - last_warn >= 15_000) {
                log.warn("loadTables watchdog: {d}/{d} opens completed after {d}ms", .{
                    completed.load(.acquire), total, now - start,
                });
                last_warn = now;
            }
        }
    }

    fn openFiber(
        self: *TableCache,
        need: *TableNeed,
        tse: *utils.ThreadSafeError,
        completed: *std.atomic.Value(usize),
    ) void {
        defer _ = completed.fetchAdd(1, .release);
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
        const path = config.sstPath(self.allocator, self.dir, need.id) catch |err| {
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
    opts: config.Config,
    tree_id: u64,
    ms: *manifest.ForestState,
    cache: *TableCache,
    levels: []Level,
    compact_candidate: ?CompactCandidate = null,
    key_range: KeyRange,

    /// open builds a Tree from an existing manifest state.
    /// Tables are loaded via cache, which opens SST files concurrently for misses
    /// and reuses already-open *Table objects for cache hits.
    pub fn open(allocator: std.mem.Allocator, opts: config.Config, tree_id: u64, ms: *manifest.ForestState, cache: *TableCache) !Tree {
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

    /// A live merge iterator over one tree's data, plus any extras the
    /// caller wired in (typically memtable iterators). Owns its arena
    /// and the underlying merge — caller drives via `itemRef`/`next`,
    /// frees via `deinit`. The walk loop lives in `Forest.scan`, which
    /// can compose multiple cursors back-to-back to span trees.
    /// Build a merge iterator over this tree's L≥1 data restricted to
    /// `range`, plus the caller-supplied `extras`.
    ///
    /// Sources fed into the merge:
    ///   - `extras` — passed through verbatim (typically memtable iters)
    ///   - L0    — one `TableIterator` per overlapping table (overlapping
    ///             ranges, so each needs its own merge slot)
    ///   - L≥1  — one `ConcatIterator` per level (tables are disjoint so
    ///             they can be walked sequentially)
    ///
    /// The cursor takes ownership of every iterator wired in (extras
    /// included). On success: `cursor.deinit()` releases everything.
    /// On error (before merge is built): we deinit `extras` ourselves
    /// so refs don't leak. All wiring memory (slices, sub-arrays) lives
    /// in the cursor's arena.
    /// `allocator` is the caller's per-call arena. Every allocation made
    /// here — overlapping-table slices, iter slots, the merge's internal
    /// heap, plus the iters' own scratch (key_buf etc.) — comes from it.
    /// The caller keeps the arena alive for the merge's lifetime and frees
    /// it at end-of-scope; `merge.deinit()` only releases table refs.
    pub fn openMerge(
        self: *Tree,
        allocator: std.mem.Allocator,
        range: KeyRange,
        priority: vfs.Priority,
        extras: []const Iterator,
    ) !MergeIterator {
        // Build iters into a list. Until we hand them off to merge,
        // each iter we created here owns a Table/SkipList ref — on
        // failure we must deinit them all to release those refs.
        var iters: std.ArrayList(Iterator) = .empty;
        var transferred = false;
        errdefer {
            if (!transferred) for (iters.items) |*it| it.deinit();
        }

        try iters.appendSlice(allocator, extras);

        for (self.levels, 0..) |*lv, level_idx| {
            const overlapping = try lv.overlappingTables(allocator, range);
            if (overlapping.len == 0) continue;

            if (level_idx == 0) {
                for (overlapping) |e| {
                    try iters.append(allocator, .{ .table = TableIterator.init(allocator, e.table, priority) });
                }
            } else {
                const subs = try allocator.alloc(TableIterator, overlapping.len);
                for (overlapping, 0..) |e, j| subs[j] = TableIterator.init(allocator, e.table, priority);
                try iters.append(allocator, .{ .concat = ConcatIterator.init(subs) });
            }
        }

        var merge = MergeIterator.init(allocator, iters.items);
        transferred = true; // merge.deinit now owns iter cleanup
        errdefer merge.deinit();
        try merge.seekRange(range.left, range.right);

        return merge;
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

    /// Minimum number of Lmax tables required before we'll consider a
    /// split. Below this, the keyspace coverage in Lmax is too coarse
    /// for an honest balance — the split would either leave one side
    /// nearly empty or land on a key that puts most of the upper-level
    /// data on one side. Wait for Lmax to fill out via compactions.
    const min_lmax_tables_for_split: usize = 10;

    /// Pick a split key that minimizes `|left_bytes − right_bytes|`
    /// across all L≥1 tables, drawn ONLY from the smallest-key
    /// boundaries of Lmax tables. This guarantees no L3 table gets
    /// split in half (which would force an L3→L3 compaction); the
    /// chosen key always cleanly partitions Lmax into "tables[0..i]"
    /// left and "tables[i..]" right.
    ///
    /// Candidates are only valid when Lmax has ≥ `min_lmax_tables_for_split`
    /// tables. Below that, return empty (no split this round; the
    /// caller refreshes the candidate and tries again later).
    ///
    /// For the balance score, we ignore tables that STRADDLE the
    /// candidate K — they go to BOTH children in `buildSplit`, so
    /// they cost the same on each side and don't differentiate
    /// candidates. Only strict-left and strict-right bytes count.
    pub fn findSplitKey(self: *Tree, allocator: std.mem.Allocator) !Key {
        // Use a per-call arena for keys we collect under each level's
        // read lock — once the lock drops, the source Table could be
        // freed by a concurrent delete, so we must not keep raw
        // pointers into table memory.
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const a = arena.allocator();

        // Step 1: snapshot Lmax candidate keys (smallest-of-table for
        // each table, skipping items[0] whose smallest is the level's
        // left boundary — using it would put nothing on the old side).
        // Bail if Lmax doesn't have enough tables for a meaningful split.
        const lmax_idx = self.opts.max_levels - 1;
        var cand_keys: std.ArrayList(Key) = .empty;
        {
            const lv = &self.levels[lmax_idx];
            lv.mu.lockShared();
            defer lv.mu.unlockShared();
            if (lv.tables.items.len < min_lmax_tables_for_split) return .{};
            for (lv.tables.items[1..]) |tbe| {
                try cand_keys.append(a, try tbe.table.smallest.dupe(a));
            }
        }

        // Step 2: snapshot all L≥1 tables (smallest, biggest, size)
        // across every level so we can score each candidate by its
        // L≥1-wide balance, not just within Lmax.
        const Entry = struct { smallest: Key, biggest: Key, size: u64 };
        var all_entries: std.ArrayList(Entry) = .empty;
        for (self.levels[1..]) |*lv| {
            lv.mu.lockShared();
            defer lv.mu.unlockShared();
            for (lv.tables.items) |tbe| {
                try all_entries.append(a, .{
                    .smallest = try tbe.table.smallest.dupe(a),
                    .biggest = try tbe.table.biggest.dupe(a),
                    .size = tbe.table.fileSize(),
                });
            }
        }

        // Step 3: for each candidate K, count strict-left and
        // strict-right bytes; pick the K with smallest difference.
        var best_key: ?Key = null;
        var best_score: u64 = std.math.maxInt(u64);
        for (cand_keys.items) |k| {
            var left: u64 = 0;
            var right: u64 = 0;
            for (all_entries.items) |e| {
                if (e.biggest.order(k) == .lt) {
                    left += e.size;
                } else if (e.smallest.order(k) != .lt) {
                    right += e.size;
                }
                // else: straddles K → goes to BOTH children → skip.
            }
            const score = if (left > right) left - right else right - left;
            if (score < best_score) {
                best_score = score;
                best_key = k;
            }
        }

        const chosen = best_key orelse return .{};
        // Re-dupe into the caller's allocator (arena dies on return).
        // Bump timestamp so every (primary, ts) version of the chosen
        // key sorts AFTER the split key — they all go to the right
        // tree, no primary gets split across trees.
        var key = try chosen.dupe(allocator);
        std.debug.assert(key.len() > 0);
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

        // Target sizes are FIXED per tree, derived from `tree_split_size`:
        // each tree splits when its bottom level outgrows that cap, so
        // the bottom level's target should be slightly under it (we use
        // 4/5 = 0.8x to leave headroom; once the actual bottom-level size
        // hits ~1.25x target it triggers a split, well within the cap).
        // Levels above cascade up by dividing by `level_size_multiplier`,
        // floored at 1 byte so the size/target score is well-defined.
        //
        // Why FIXED instead of derived from current data: dynamic targets
        // (RocksDB-style) have a chicken-and-egg problem — early in a
        // tree's life, Lmax is empty, so cascading from current sizes
        // produces big upper-level targets. Stepwise compaction then
        // can't drain those intermediates. Fixed targets sized to the
        // tree's eventual capacity sidestep this entirely. If Lmax
        // briefly exceeds its target before the split kicks in, that's
        // fine — we don't crash, the split will eventually fire.
        const lmax_idx: usize = n - 1;
        target_sz[lmax_idx] = @divTrunc(self.opts.tree_split_size * 4, 5);
        var t: i64 = target_sz[lmax_idx];
        {
            var i: usize = lmax_idx;
            while (i > 1) {
                i -= 1;
                t = @divTrunc(t, self.opts.level_size_multiplier);
                if (t < 1) t = 1;
                target_sz[i] = t;
            }
        }

        // Per-level file sizes: L0 inherits the memtable size; L1 uses
        // `l1_table_size`; each level below ramps by
        // `table_size_multiplier` up to `max_table_size`. Compaction is
        // strictly stepwise (L0→L1, L1→L2, …) so there's no separate
        // base-level concept — L1 is always the destination of L0.
        var tsz: i64 = self.opts.l1_table_size;
        file_sz[0] = @intCast(self.opts.memtable_size);
        for (1..n) |j| {
            if (j == 1) {
                file_sz[j] = tsz;
            } else {
                tsz = @min(tsz * self.opts.table_size_multiplier, self.opts.max_table_size);
                file_sz[j] = tsz;
            }
        }

        return .{
            .allocator = self.allocator,
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
        // Split check overrides compaction. We compare TOTAL tree size
        // (L0+L1+…+Lmax) against `tree_split_size`, not just Lmax. If
        // compactions can't keep up the data piles up across the upper
        // levels and Lmax stays under threshold even when the tree as a
        // whole is several times oversized — exactly the wedged shape
        // we just observed. Splitting on total size lets each tree get
        // its own future compactor and divides the catch-up work.
        // The score is bumped to `split_priority_score` so this tree
        // wins over normal compaction candidates in the forest. Set
        // `level` to the lowest non-empty level for diagnostic
        // legibility (`stats forest`).
        const linfo = self.lowestLevelSize();
        const total = self.totalSize();
        if (total > self.opts.tree_split_size) {
            log.debug("split-trigger tree={d} total={d} deepest_level={d} lmax_size={d} > threshold={d}", .{
                self.tree_id, total, linfo.level, linfo.size, self.opts.tree_split_size,
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

        // L0: count-based score, scaled so it competes on the same axis
        // as L1+ size scores. Score reaches 1.0 at HALF the backpressure
        // start count — early enough that compaction usually catches up
        // before backpressure kicks in. Without this scaling (or with
        // the old "if score >= 2.5, compact L0 immediately" shortcut),
        // L0 starves L1→L2/L3 work and lower levels go unmaintained.
        // Backpressure (`l0_backpressure_start_count`) is what protects
        // writes from a temporarily-out-of-shape L0; the scoring loop
        // is for steady-state level shape.
        const l0_tables = self.levels[0].numTables();
        const l0_score = if (self.opts.l0_backpressure_start_count == 0)
            @as(f64, @floatFromInt(l0_tables))
        else
            @as(f64, @floatFromInt(l0_tables * 2)) /
                @as(f64, @floatFromInt(self.opts.l0_backpressure_start_count));
        prios[0] = .{ .level = 0, .score = l0_score, .adjusted = l0_score };

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
        // upperLevel starts at 0; for each level from L1 downward, if
        // prios[upperLevel].adjusted >= 1.0, divide it by the current level's
        // adjusted score (floored at 0.01).
        var upper_level: usize = 0;
        for (1..n) |cur_level| {
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

        // Sort descending by adjusted score; pick the top.
        std.mem.sort(LevelPriority, filtered.items, {}, struct {
            fn gt(_: void, a: LevelPriority, b: LevelPriority) bool {
                return a.adjusted > b.adjusted;
            }
        }.gt);
        const winner: ?LevelPriority = if (filtered.items.len > 0 and filtered.items[0].adjusted > 1.0)
            filtered.items[0]
        else
            null;

        if (winner) |w| {
            self.compact_candidate = .{ .level = @intCast(w.level), .score = w.adjusted };
            return;
        }

        // Idle fallback: nothing scored above 1.0, but if L0 has any tables,
        // drain it. Without this, a few stragglers (e.g. 2 L0 tables, score
        // 0.5) would sit forever, paying read amplification until enough
        // flushes pushed L0 over the start_count threshold. Lower levels by
        // definition need no work when nothing scored ≥ 1.0, so L0 is always
        // the right idle pick.
        if (self.levels[0].numTables() > 0) {
            self.compact_candidate = .{ .level = 0, .score = prios[0].score };
        } else {
            self.compact_candidate = null;
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

        // Stepwise compaction: every level pushes to the level immediately
        // below it, including L0 → L1. Routing L0 directly to Lbase used to
        // produce huge cross-level compactions and made intermediate
        // levels effectively unused; routing one step at a time keeps
        // each compaction's input set bounded by the destination level's
        // overlapping range.
        const dest: usize = @as(usize, src_level) + 1;
        if (dest >= self.opts.max_levels) return .{};

        // --- collect src entries ---
        // L≥1: pick a RANDOM single table. Always picking items[0] (the
        // smallest-key table) starves higher key ranges and biases write
        // amplification toward the left edge of the keyspace.
        // L0: take the oldest-first prefix that brings total compaction
        // input bytes (selected L0 + their L1 overlap) up to
        // `l0_compaction_max_bytes`. The table that crosses the budget
        // is included; we stop afterwards. Newer L0 tables stay put and
        // continue to shadow the compacted versions on read.
        const src_entries_slice = blk: {
            // Snapshot the src table list under its own lock so the
            // per-iteration L1-overlap queries don't hold the src lock.
            const src_snapshot = ss: {
                self.levels[src].mu.lockShared();
                defer self.levels[src].mu.unlockShared();
                const items = self.levels[src].tables.items;
                if (items.len == 0) return .{};
                break :ss try self.allocator.dupe(TableEntry, items);
            };
            defer self.allocator.free(src_snapshot);

            if (src_level != 0) {
                // Pick any table at random.
                const idx = std.crypto.random.uintLessThan(usize, src_snapshot.len);
                break :blk try self.allocator.dupe(TableEntry, src_snapshot[idx .. idx + 1]);
            }

            const budget = self.opts.l0_compaction_max_bytes;
            var union_kr = KeyRange{};
            var l0_bytes: u64 = 0;
            var n: usize = 0;
            while (n < src_snapshot.len) {
                const e = src_snapshot[n];
                union_kr.extend(.{ .left = e.table.smallest, .right = e.table.biggest });
                l0_bytes += e.table.fileSize();
                n += 1;

                const overlap = try self.levels[dest].overlappingTables(self.allocator, union_kr);
                defer self.allocator.free(overlap);
                var bot_bytes: u64 = 0;
                for (overlap) |oe| bot_bytes += oe.table.fileSize();
                if (l0_bytes + bot_bytes >= budget) break;
            }
            break :blk try self.allocator.dupe(TableEntry, src_snapshot[0..n]);
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

        // Diagnostic: count items + bytes pulled from the merge iterator vs
        // items + bytes accepted by the builder. Pre-fix expectation (no
        // tombstones, no shadowing because the bench writes each primary at
        // most once): in/out should be EQUAL on both axes, AND keys should
        // equal the sum of `index.key_count` across all input tables.
        // `bytes_*` count user data only (key.primary + value), not the
        // 16-byte ns/tid/ts framing.
        var keys_in: u64 = 0;
        var keys_out: u64 = 0;
        var bytes_in: u64 = 0;
        var bytes_out: u64 = 0;

        while (merge.itemRef()) |item| {
            keys_in += 1;
            bytes_in += item.key.primary.len + item.value.len;
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
            keys_out += 1;
            bytes_out += item.key.primary.len + item.value.len;
            try merge.next();
        }

        // Flush any remaining data.
        try self.flushBuilderOutput(&bld, &flush_handles, next_file_id);

        // Sum up input key counts + on-disk byte sizes from every src + bot
        // SST's index for the "expected" baseline. If keys_in != expected,
        // the merge iterator is dropping items. If keys_out != keys_in, the
        // builder is dropping.
        var src_keys: u64 = 0;
        var src_bytes_disk: u64 = 0;
        var src_min_ts: u64 = std.math.maxInt(u64);
        var src_max_ts: u64 = 0;
        for (src_entries_slice) |e| {
            src_keys += @intCast(e.table.index.key_count);
            src_bytes_disk += e.table.fileSize();
            const mv = e.table.index.max_version;
            if (mv > src_max_ts) src_max_ts = mv;
            if (mv < src_min_ts) src_min_ts = mv;
        }
        var bot_keys: u64 = 0;
        var bot_bytes_disk: u64 = 0;
        for (bot_entries_slice) |e| {
            bot_keys += @intCast(e.table.index.key_count);
            bot_bytes_disk += e.table.fileSize();
        }
        const expected_in = src_keys + bot_keys;
        const expected_bytes_disk = src_bytes_disk + bot_bytes_disk;
        log.info(
            "compact tree={d} L{d}->L{d} src_tables={d} src_keys={d} src_bytes_disk={d} bot_tables={d} bot_keys={d} bot_bytes_disk={d} expected_in={d} expected_bytes_disk={d} keys_in={d} keys_out={d} bytes_in={d} bytes_out={d} dropped_in_iter={d} dropped_in_builder={d}",
            .{
                self.tree_id,           src,                 dest,
                src_entries_slice.len,  src_keys,            src_bytes_disk,
                bot_entries_slice.len,  bot_keys,            bot_bytes_disk,
                expected_in,            expected_bytes_disk, keys_in,
                keys_out,               bytes_in,            bytes_out,
                expected_in -| keys_in, keys_in -| keys_out,
            },
        );

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
        const path = try config.sstPath(self.allocator, self.opts.dir, id);
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
