// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const _config = @import("config.zig");
const _manifest = @import("manifest.zig");
const _level_handler = @import("level.zig");
const _table = @import("table.zig");
const vfs = @import("vfs");
const _metrics = @import("metrics.zig");
const _forest = @import("forest.zig");
const zroar = @import("zroar");
const _compaction = @import("compaction.zig");
const flags = @import("flags");

const Table = _table.Table;
const TableIterator = _table.TableIterator;
const Item = _table.table.Item;
const Level = _level_handler.Level;
const Lock = _level_handler.Lock;
const TableEntry = _level_handler.TableEntry;
const _kv = @import("kv.zig");
const Key = _kv.Key;
const KeyRange = _kv.KeyRange;
const MergeIterator = _table.merge_iterator.MergeIterator;
const ConcatIterator = _table.merge_iterator.ConcatIterator;
const Iterator = _table.merge_iterator.Iterator;

const log = std.log.scoped(.zdb_tree);

// Compaction lives in compaction.zig; these keep `_tree.X` and this
// file's own references to the moved declarations resolving.
pub const CompactCandidate = _compaction.CompactCandidate;
pub const split_priority_score = _compaction.split_priority_score;
const MultiGet = @import("read.zig").MultiGet;
const Lookup = @import("read.zig").Lookup;

// ---------------------------------------------------------------------------
// TableCache
// ---------------------------------------------------------------------------

/// One table to load: its id, the level it is wanted at, and the extent
/// the manifest recorded for it, which is all `Table.open` needs.
pub const TableNeed = struct {
    id: u64,
    level: usize,
    extent: Table.Extent,
    table: ?*Table = null,
};

/// TableCache is a Forest-owned registry of open *Table objects keyed by
/// table_id. Tree.open() calls loadTables() instead of opening SST files
/// itself, so a table opened for one tree is reused when the same file is
/// needed by another tree (e.g. shared tables after a split).
pub const TableCache = struct {
    allocator: std.mem.Allocator,
    store: *vfs.RemoteStore,
    tables: std.AutoHashMap(u64, *Table),
    mu: fiber.RWMutex(.tracked),

    pub fn init(
        allocator: std.mem.Allocator,
        store: *vfs.RemoteStore,
    ) TableCache {
        return .{
            .allocator = allocator,
            .store = store,
            .tables = std.AutoHashMap(u64, *Table).init(allocator),
            .mu = .{},
        };
    }

    pub fn deinit(self: *TableCache) void {
        // Release the cache's ref on each table; the last ref frees it.
        var it = self.tables.iterator();
        while (it.next()) |kv| kv.value_ptr.*.decrRef();
        self.tables.deinit();
        self.mu.deinit();
    }

    /// contains reports whether a table with this id is currently cached.
    pub fn contains(self: *TableCache, id: u64) bool {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.tables.contains(id);
    }

    /// own hands a *Table to the cache, which takes its own ref; the caller
    /// keeps its ref and drops it when done with `t`. Returns false when
    /// the id is already cached — the apply loop's `loadTables` can open
    /// the same file between a caller's `contains` check and this call,
    /// and Forest.writeFile does exactly that across seconds of I/O — in
    /// which case the cached one stays, nothing is taken, and the struct's
    /// disposal is the caller's too (a cache entry is what would otherwise
    /// own it).
    pub fn own(self: *TableCache, id: u64, t: *Table) !bool {
        self.mu.lock(@src());
        defer self.mu.unlock();
        const gop = try self.tables.getOrPut(id);
        if (gop.found_existing) return false;
        t.incrRef();
        gop.value_ptr.* = t;
        return true;
    }

    /// Let go of a table nobody published: the open-time ref was the only
    /// one, so this frees it.
    fn dropUnowned(t: *Table) void {
        std.debug.assert(t.ref.load(.acquire) == 1);
        t.decrRef();
    }

    /// Drop the cache's entry and its own ref on `id`, if it has one. The
    /// `Table`, its file descriptor and its cached blocks go away when the
    /// last ref goes: a reader or compaction that pinned the table keeps
    /// it usable for the rest of its work and frees it when it releases.
    /// For a file the manifest has just unlisted: no level points at it
    /// any more, so nothing will ask the cache for it again.
    pub fn evict(self: *TableCache, id: u64) void {
        const t = blk: {
            self.mu.lock(@src());
            defer self.mu.unlock();
            const entry = self.tables.fetchRemove(id) orelse return;
            break :blk entry.value;
        };
        // Outside the lock: the last ref purges the table's blocks, which
        // takes block cache shard locks.
        t.decrRef();
    }

    /// loadTables serves each TableNeed from cache (read lock) or opens it
    /// concurrently via fibers (write lock per miss). On return, each
    /// need's .table field points to an open *Table in the cache.
    /// The caller is responsible for installing these tables into levels
    /// (e.g. via Level.addEntry) — this keeps loadTables free of any
    /// level-layout assumptions.
    ///
    /// Each table comes with a ref for the caller, taken under the cache
    /// lock at the moment it was found or inserted: release them with
    /// `decrRefs` once the tables are installed (a level takes its
    /// own ref). Without it only the cache's ref would stand between the
    /// lookup and the install, and an `evict` of that id in between — the
    /// orphan scan can name a file whose entry is just being applied, not
    /// listed yet — would free the table under the caller. On error no
    /// ref is left with the caller.
    pub fn loadTables(self: *TableCache, needed: []TableNeed) !void {
        errdefer decrRefs(needed);
        // Phase 1: serve cache hits under read lock; collect pointers for
        // misses.
        var misses: std.ArrayList(*TableNeed) = .empty;
        defer misses.deinit(self.allocator);
        {
            self.mu.lockShared(@src());
            defer self.mu.unlockShared();
            for (needed) |*n| {
                if (self.tables.get(n.id)) |t| {
                    t.incrRef(); // the caller's, under the lock
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
        var tse: utils.SafeError = .{};
        var completed = std.atomic.Value(usize).init(0);

        // The watchdog waits on this, so it exits the moment the last
        // open lands rather than at its next 500 ms poll: the group's
        // wait() below holds the apply fiber, and a flush or compaction
        // apply used to cost a full 500 ms because of it.
        var done = fiber.Watermark.init(self.allocator);
        defer done.deinit();

        const total = misses.items.len;
        var g = try fiber.Group(void).initCapacity(self.allocator, 1 + total);
        defer g.deinit();
        g.spawn(.{}, loadTablesWatchdog, .{ total, &completed, &done }) catch {};
        for (misses.items) |n| {
            try g.spawn(.{}, openFiber, .{ self, n, &tse, &completed, &done });
        }
        g.wait();
        if (tse.get()) |err| return err;
    }

    /// Release the refs `loadTables` handed out, and forget the pointers.
    pub fn decrRefs(needed: []TableNeed) void {
        for (needed) |*n| {
            if (n.table) |t| t.decrRef();
            n.table = null;
        }
    }

    /// Sibling of the `openFiber` spawns: log every 15 s while opens
    /// are still short. Woken by `done` as each open lands, so it exits
    /// with the last one and never holds the group's `wait()` open.
    fn loadTablesWatchdog(
        total: usize,
        completed: *std.atomic.Value(usize),
        done: *fiber.Watermark,
    ) void {
        const start = utils.coarseMs();
        var last_warn = start;
        while (completed.load(.acquire) < total) {
            done.waitFor(total, 500) catch |err| switch (err) {
                error.Timeout => {},
                else => return,
            };
            const now = utils.coarseMs();
            if (now - last_warn >= 15_000) {
                log.warn(
                    "loadTables watchdog: {d}/{d} opens completed after {d}ms",
                    .{ completed.load(.acquire), total, now - start },
                );
                last_warn = now;
            }
        }
    }

    fn openFiber(
        self: *TableCache,
        need: *TableNeed,
        tse: *utils.SafeError,
        completed: *std.atomic.Value(usize),
        done: *fiber.Watermark,
    ) void {
        defer done.advance(completed.fetchAdd(1, .release) + 1);
        // Phase A: check under write lock (brief — no I/O).
        self.mu.lock(@src());
        if (self.tables.get(need.id)) |t| {
            t.incrRef(); // the caller's, under the lock
            self.mu.unlock();
            need.table = t;
            return;
        }
        self.mu.unlock();

        // Phase B: open the SST WITHOUT holding the lock. Table.open
        // yields the fiber (I/O); holding the lock here would deadlock
        // cooperative fibers on the same OS thread (EDEADLK).
        const t = Table.open(
            self.allocator,
            self.store,
            need.id,
            need.extent,
        ) catch |err| {
            tse.set(err);
            return;
        };

        // Phase C: re-acquire write lock to insert; handle race with another
        // fiber. The open-time ref becomes the cache's on success; on the
        // two other paths nothing owns `t`, so it is dropped whole.
        self.mu.lock(@src());
        if (self.tables.get(need.id)) |existing| {
            // Another fiber opened the same table while we were in phase B.
            existing.incrRef(); // the caller's, under the lock
            self.mu.unlock();
            need.table = existing;
            dropUnowned(t);
            return;
        }
        self.tables.put(need.id, t) catch |err| {
            self.mu.unlock();
            tse.set(err);
            dropUnowned(t);
            return;
        };
        t.incrRef(); // the caller's, under the lock
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
// I think of this as a pyramid, where flowing happens at top (L0) and then
// settles to lower levels (L1..n). If it helps, we can think of L1..n as
// basement levels, where i is the ith floor below ground. DB Infrastructure
// works way under the API surface, so this is apt.

pub const Tree = struct {
    allocator: std.mem.Allocator,
    /// The forest's options, read with `cur()` once per job, so an admin
    /// update reaches the next one; the fields in `Config.fixed` never
    /// change.
    live: *flags.Live(_config.Config),
    namespace: u64,
    tree_id: u64,
    manifest: *_manifest.Manifest,
    cache: *TableCache,
    levels: []Level,
    compact_candidate: ?CompactCandidate = null,
    /// Levels with a job running on them (`reserveJob`): the candidate
    /// never names one, so two jobs of this tree run at once only on
    /// disjoint levels.
    busy_levels: _compaction.LevelSet = _compaction.LevelSet.initEmpty(),
    key_range: KeyRange,
    /// What this tree holds (`buffers.TreeKind`): read off the manifest
    /// record at open, fixed for the tree's life. Picks the SST filter
    /// its tables carry, whether they may move down without a rewrite,
    /// and whether they can hold roaring chains to roll up.
    kind: _manifest.TreeKind,
    /// Holders of this object. One is the `TableTrees` entry that lists it;
    /// `TableTrees.getTree` hands out another per reader, released with
    /// `release` when the reader is done. The last release closes and frees
    /// the tree. This is what lets a split or a drop retire a tree without
    /// waiting: a split never mutates a live tree — it opens fresh objects
    /// for both halves from the manifest and swaps them in — so a reader
    /// that still holds the old one sees its full range and full table
    /// set until it lets go. Flush and compaction do install into a live
    /// tree's levels, under the level locks, with readers pinning the
    /// tables they select there.
    refs: std.atomic.Value(u32) = .init(1),
    /// The one lock over this tree's mutable state: every level's table
    /// list, `compact_candidate` and `busy_levels`. Shared to read — a
    /// point get, a merge open, the split-key and compaction pickers,
    /// stats — and exclusive to install: flush, compaction, backfill,
    /// and the candidate refresh.
    /// Levels have no lock of their own: every `Level` method takes the
    /// token this lock mints (`hold`), so a level cannot be touched without
    /// it, and so do the tree methods that read levels on a caller's behalf
    /// (`computeCompactCandidate`, `totalSize`, `lowestLevelSize`). Never
    /// held across I/O: readers pin the tables they select while they hold
    /// it and read after releasing (`Level.acquireTables`,
    /// `Level.overlappingTables`), so an install only ever waits on a
    /// selection, never on a block read. Not reentrant — nothing that runs
    /// under an exclusive hold may call something that takes it
    /// (`updateCompactCandidate` takes its own, so installs call it after
    /// releasing theirs). Order: `TableTrees.mu`, then this.
    mu: Lock = .{},

    // Compaction, from compaction.zig.
    pub const updateCompactCandidate = _compaction.updateCompactCandidate;
    pub const updateCompactCandidate_ = _compaction.updateCompactCandidate_;
    pub const clearCandidate = _compaction.clearCandidate;
    pub const reserveJob = _compaction.reserveJob;
    pub const releaseJob = _compaction.releaseJob;
    pub const reserveAll = _compaction.reserveAll;
    pub const releaseAll = _compaction.releaseAll;
    pub const Emitter = _compaction.Emitter;
    pub const buildCompaction = _compaction.buildCompaction;
    pub const mergeAndWrite = _compaction.mergeAndWrite;

    /// Whether `key` is this tree's to hold: of its table, and inside
    /// its range (a single tree's range is unbounded).
    pub fn covers(self: *const Tree, key: Key) bool {
        return key.namespace == self.namespace and
            key.table_id == self.tableId() and
            self.key_range.contains(key);
    }

    /// The SQL table this tree is part of, from the tree id.
    pub fn tableId(self: *const Tree) u32 {
        return _kv.treeTableId(self.tree_id);
    }

    /// `key` as a key of this tree: a table bound carries no namespace
    /// or table_id, and gets the tree's before it is used as one.
    pub fn stamp(self: *const Tree, key: Key) Key {
        var out = key;
        out.namespace = self.namespace;
        out.table_id = self.tableId();
        return out;
    }

    pub fn acquire(self: *Tree) void {
        const prev = self.refs.fetchAdd(1, .acquire);
        std.debug.assert(prev > 0);
    }

    /// Drop a ref; the last one closes the tree and frees it (it must
    /// have been created with `allocator`, as installTree/applySplit do).
    pub fn release(self: *Tree) void {
        const prev = self.refs.fetchSub(1, .release);
        std.debug.assert(prev > 0);
        if (prev == 1) {
            _ = self.refs.load(.acquire);
            const allocator = self.allocator;
            self.close();
            allocator.destroy(self);
        }
    }

    /// open builds a Tree from an existing manifest state, on the heap with
    /// one ref — `release` is its teardown, and a tree lives at one address
    /// for its whole life. Tables are loaded via cache, which opens SST
    /// files concurrently for misses and reuses already-open *Table objects
    /// for cache hits. Runs on the apply loop (or at startup), the only
    /// place the manifest map it reads is ever written.
    ///
    /// No lock here: nobody else can see this tree yet, and the caller may
    /// publish it (`TableTrees.addTree`) before the first lock is taken.
    pub fn open(
        allocator: std.mem.Allocator,
        live: *flags.Live(_config.Config),
        namespace: u64,
        tree_id: u64,
        manifest: *_manifest.Manifest,
        cache: *TableCache,
    ) !*Tree {
        const opts = live.cur();
        const self = try allocator.create(Tree);
        errdefer allocator.destroy(self);

        const levels = try allocator.alloc(Level, opts.max_levels);
        errdefer allocator.free(levels);
        for (levels, 0..) |*l, i| {
            l.* = Level.init(allocator, @intCast(i), &self.mu);
        }
        errdefer for (levels) |*l| l.deinit();

        var kr: KeyRange = .{};
        var kind: _manifest.TreeKind = .none;
        errdefer {
            kr.left.free(allocator);
            kr.right.free(allocator);
        }

        // The manifest record is read under the manifest lock and copied
        // out — range and table ids — so the loads below, file I/O, run
        // with it released.
        var needed: std.ArrayList(TableNeed) = .empty;
        defer needed.deinit(allocator);
        {
            const held = manifest.mu.hold(.shared, @src());
            defer held.release();
            if (manifest.treeState(held, .{
                .namespace = namespace,
                .tree_id = tree_id,
            })) |tree| {
                kr.left = try tree.left.dupe(allocator);
                kr.right = try tree.right.dupe(allocator);
                kind = tree.kind;
                for (tree.levels.items, 0..) |lm, level_idx| {
                    if (level_idx >= opts.max_levels) continue;
                    var tit = lm.tables.keyIterator();
                    while (tit.next()) |tid| {
                        try needed.append(allocator, .{
                            .id = tid.*,
                            .level = level_idx,
                            .extent = manifest.extent(held, tid.*).?,
                        });
                    }
                }
            }
        }
        try cache.loadTables(needed.items);
        defer TableCache.decrRefs(needed.items);

        self.* = .{
            .allocator = allocator,
            .live = live,
            .namespace = namespace,
            .tree_id = tree_id,
            .manifest = manifest,
            .cache = cache,
            .levels = levels,
            .key_range = kr,
            .kind = kind,
        };
        // Unpublished, so uncontended — but the levels take the token, and
        // the tree is at its final address, so the lock is simply taken.
        {
            const held = self.mu.hold(.exclusive, @src());
            defer held.release();
            for (needed.items) |n| {
                try levels[n.level].addEntry(
                    held,
                    n.id,
                    manifest.raftIndexOf(n.id),
                    n.table.?,
                );
            }
        }
        try self.updateCompactCandidate();
        return self;
    }

    /// close tears down all levels, freeing each table's memory without
    /// deleting SST files (which are managed by the manifest). The
    /// internal half of `release`, which is the teardown callers use.
    fn close(self: *Tree) void {
        for (self.levels) |*h| h.deinit();
        self.allocator.free(self.levels);
        self.clearCandidate();
        self.key_range.left.free(self.allocator);
        self.key_range.right.free(self.allocator);
        self.mu.deinit();
    }

    /// get checks each level from L0 upward and returns the newest
    /// version at or below `lookup.read_ts`, tombstone included, or null
    /// if the key is absent. A one-key row MultiGet over this tree alone
    /// (no memtables). The returned item's key and value are duplicated
    /// with self.allocator; the caller must free them.
    pub fn get(self: *Tree, lookup: Lookup) !?Item {
        const keys = [_]String{lookup.key.primary};
        var mg = try MultiGet.init(
            self.allocator,
            lookup.key.tableKey(),
            &keys,
            .{
                .read_ts = lookup.read_ts,
                .include_deleted = true,
            },
        );
        defer mg.deinit();
        try mg.walkTree(self, mg.pending.items, 0);
        const found = mg.item(lookup.key.primary) orelse return null;
        return try found.dupe(self.allocator);
    }

    /// Build a merge iterator over this tree's data overlapping `range`,
    /// plus the caller-supplied `extras` (typically memtable iterators).
    /// The walk loop lives in `Forest.Cursor`, which opens one of these
    /// per tree it visits.
    ///
    /// The caller holds a ref on this tree (`TableTrees.getTree`) and read
    /// `key_range` from this same object. That is enough for the range and
    /// the tables selected here to describe one instant: a split never
    /// mutates a live tree — it installs fresh objects for both halves
    /// and retires this one, which stays whole for whoever still holds it
    /// — so there is no gap in which the range says 1..100 while the
    /// levels hold only 1..50. Flush and compaction do install into the
    /// levels concurrently, under the level locks; the tables selected
    /// are pinned, so what was chosen stays readable.
    ///
    /// NOT positioned: `range` only picks the tables (and pins them —
    /// every iterator wired in holds its own table ref). The selection
    /// runs under one shared hold of `mu` here; the caller seeks with
    /// `merge.seekRange(range.left, range.right)` afterwards, so it can
    /// release the tree first and let the block reads a seek issues run
    /// holding nothing but table refs.
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
    /// `min_ts`: a table whose newest version (`TableIndex.max_version`)
    /// is at or below it holds nothing the scan wants and is not opened
    /// (Forest.ScanOptions.min_ts); 0 opens every overlapping table.
    /// `dir` is the way the merge and every source walk the range.
    pub fn openMerge(
        self: *Tree,
        allocator: std.mem.Allocator,
        range: KeyRange,
        min_ts: u64,
        extras: []const Iterator,
        dir: _kv.Direction,
    ) !MergeIterator {
        // Build iters into a list. Until we hand them off to merge,
        // each iter we created here owns a Table/SkipList ref — on
        // failure we must deinit them all to release those refs.
        var iters: std.ArrayList(Iterator) = .empty;
        var transferred = false;
        errdefer {
            if (!transferred) for (iters.items) |*it| it.deinit();
        }

        // On failure, extras still own refs but aren't in iters for
        // errdefer to release. This call consumes them even on error.
        iters.appendSlice(allocator, extras) catch |err| {
            for (extras) |source| {
                var it = source;
                it.deinit();
            }
            return err;
        };

        // The range's bounds are this tree's keys, or open: a table
        // edge (`Key.tableLeft`/`tableRight`) is a key of its table, and
        // the levels and iterators below compare primaries on that
        // basis.
        std.debug.assert(range.left.isInf() or
            (range.left.namespace == self.namespace and
                range.left.table_id == self.tableId()));
        std.debug.assert(range.right.isInf() or
            (range.right.namespace == self.namespace and
                range.right.table_id == self.tableId()));

        const held = self.mu.hold(.shared, @src());
        defer held.release();
        for (self.levels, 0..) |*lv, level_idx| {
            const overlapping = try lv.overlappingTables(
                held,
                allocator,
                range,
            );
            // The iterators take their own table refs; the selection
            // pins are only needed to bridge the gap between the
            // level lock releasing and the iterators existing.
            defer Level.releaseTables(allocator, overlapping);
            if (overlapping.len == 0) continue;

            if (level_idx == 0) {
                for (overlapping) |e| {
                    if (e.table.index.max_version <= min_ts) continue;
                    try iters.append(allocator, .{
                        .table = TableIterator.init(
                            allocator,
                            e.table,
                            .{
                                .namespace = self.namespace,
                                .table_id = self.tableId(),
                            },
                            dir,
                        ),
                    });
                }
            } else {
                var subs: std.ArrayList(TableIterator) = .empty;
                for (overlapping) |e| {
                    if (e.table.index.max_version <= min_ts) continue;
                    const tbl = e.table;
                    const sub = TableIterator.init(
                        allocator,
                        tbl,
                        .{
                            .namespace = self.namespace,
                            .table_id = self.tableId(),
                        },
                        dir,
                    );
                    try subs.append(allocator, sub);
                }
                if (subs.items.len == 0) continue;
                const concat = ConcatIterator.init(subs.items, dir);
                try iters.append(allocator, .{ .concat = concat });
            }
        }

        // merge.deinit now owns iter cleanup.
        const merge = try MergeIterator.init(allocator, iters.items, dir);
        transferred = true;
        return merge;
    }

    /// One level's table count, under a hold of its own — for stats and
    /// tests, where nothing else is read alongside it.
    pub fn numTablesAt(self: *Tree, level: usize) usize {
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        return self.levels[level].numTables(held);
    }

    pub fn totalSize(self: *Tree, held: Lock.Shared) i64 {
        self.mu.assertHeld(held);
        var total: i64 = 0;
        for (self.levels) |*l| total += l.getTotalSize(held);
        return total;
    }

    pub fn lowestLevelSize(self: *Tree, held: Lock.Shared) struct {
        level: u32,
        size: i64,
    } {
        self.mu.assertHeld(held);
        var idx: usize = self.levels.len;
        while (idx > 0) {
            idx -= 1;
            const sz = self.levels[idx].getTotalSize(held);
            if (sz > 0) return .{ .level = @intCast(idx), .size = sz };
        }
        return .{ .level = 0, .size = 0 };
    }

    /// Minimum number of Lmax tables required before we'll consider a
    /// split. Below this, the keyspace coverage in Lmax is too coarse
    /// for an honest balance — the split would either leave one side
    /// nearly empty or land on a key that puts most of the upper-level
    /// data on one side. Wait for Lmax to fill out via compactions.
    pub const min_lmax_tables_for_split: usize = 10;

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
    /// Every L>=1 table counts, by its block index (`Table.splitAround`):
    /// a table straddling the candidate puts the blocks below it left
    /// and the rest right, so a few large tables cannot hide most of the
    /// bytes from the score. Null when the tree is not worth splitting:
    /// Lmax too small, or no candidate.
    pub fn findSplitKey(self: *Tree, allocator: std.mem.Allocator) !?Key {
        // No I/O anywhere, so one shared hold covers the whole function;
        // it also keeps the tables alive while their indexes are read.
        const held = self.mu.hold(.shared, @src());
        defer held.release();

        // Candidates: each Lmax table's smallest key but the first, whose
        // smallest is the tree's left bound (nothing would go left).
        const lmax_idx = self.live.cur().max_levels - 1;
        const lmax = self.levels[lmax_idx].entries(held);
        if (lmax.len < min_lmax_tables_for_split) return null;

        var tables: std.ArrayList(*const Table) = .empty;
        defer tables.deinit(allocator);
        for (self.levels[1..]) |*lv| {
            for (lv.entries(held)) |tbe| {
                try tables.append(allocator, tbe.table);
            }
        }

        // The candidate with the smallest |left - right| over every
        // table's bytes.
        var best_key: ?Key = null;
        var best_score: u64 = std.math.maxInt(u64);
        for (lmax[1..]) |cand| {
            const k = cand.table.smallest;
            var sizes: Table.Sizes = .{};
            for (tables.items) |tbl| {
                const s = tbl.splitAround(k);
                sizes.left += s.left;
                sizes.right += s.right;
            }
            const score = if (sizes.left > sizes.right)
                sizes.left - sizes.right
            else
                sizes.right - sizes.left;
            if (score < best_score) {
                best_score = score;
                best_key = k;
            }
        }

        const chosen = best_key orelse return null;
        // Tree bounds name primaries, so every version of the split key
        // goes to the right tree: no primary is split across trees. The
        // table bound it came from carries no namespace or table_id; the
        // tree's go on, since the key becomes a tree bound.
        const key = self.stamp(try chosen.dupe(allocator));
        std.debug.assert(key.primary.len > 0);
        return key;
    }

    /// Fold one primary's roaring version chain into a bitmap by
    /// walking this tree's levels top-down from `from_level`, LAZILY:
    /// stopping at the first terminal without touching anything
    /// beneath it. A one-key posting MultiGet over the tree alone; the
    /// memtables are not read.
    ///
    /// `lookup.read_ts` bounds the walk: only versions at or below it
    /// participate. Returns null when no version exists in
    /// the walked window (the posting has no history here — folding
    /// from empty is the caller's call). The bitmap and the fold
    /// allocate from `allocator`.
    ///
    /// Compaction's read-below is the caller (from_level=dest+1,
    /// ts = oldest buffered ts − 1 so the walk starts strictly beneath
    /// the chain the compaction already buffered). Soundness of
    /// walking sources sequentially rests on
    /// the level-ordering invariant — for any primary, every version
    /// at level i is newer than every version at level j > i
    /// (compaction merges src ∪ overlapping-dest into dest and never
    /// splits a primary's run across output files) — plus the node's
    /// descending maxTs guard for mid-walk shuffles.
    ///
    /// CorruptPosting / NotRoaring mean the chain cannot be folded;
    /// compaction degrades to verbatim emission, reads surface it.
    pub fn rollupBitmap(
        self: *Tree,
        allocator: std.mem.Allocator,
        lookup: Lookup,
        from_level: usize,
    ) !?zroar.Bitmap {
        const keys = [_]String{lookup.key.primary};
        var mg = try MultiGet.init(
            allocator,
            lookup.key.tableKey(),
            &keys,
            .{ .read_ts = lookup.read_ts },
        );
        defer mg.deinit();
        try mg.walkTree(self, mg.pending.items, from_level);
        if (!mg.hasVersions(lookup.key.primary)) return null;
        return try mg.bitmap(allocator, lookup.key.primary);
    }

    /// applyCompaction is Phase B: commits the manifest changes and updates the
    /// in-memory levels by opening tables from the SST directory.
    /// Called after Raft commit on all nodes (leader and followers).
    pub fn applyCompaction(
        self: *Tree,
        index: u64,
        changes: []const _manifest.SSTableChange,
    ) !void {
        if (changes.len == 0) return;

        // Batch-load all CREATE tables via the cache in a single concurrent
        // pass. writeFile may have pre-opened them (follower path) → hit;
        // otherwise loadTables opens them. This is the step that can fail
        // (I/O), so it runs before any state changes: a retry of the
        // entry then starts from a clean manifest and clean levels.
        var needed: std.ArrayList(TableNeed) = .empty;
        defer needed.deinit(self.allocator);
        for (changes) |c| {
            if (c.op == .create) {
                try needed.append(self.allocator, .{
                    .id = c.sst_id,
                    .level = c.level,
                    .extent = .{
                        .size = c.size,
                        .index_offset = c.index_offset,
                    },
                });
            }
        }
        try self.cache.loadTables(needed.items);
        defer TableCache.decrRefs(needed.items);

        // Manifest and levels change together, both in memory; the entry
        // itself is already durable in the raft log.
        try self.manifest.commit(index, changes, &.{});

        // Apply level changes in original order: delete old entries, add new.
        // Track total bytes created and the destination level for metrics.
        // One exclusive hold for the whole batch, so no reader sees the
        // source gone before the output is in.
        var need_idx: usize = 0;
        var bytes_out: u64 = 0;
        var dest_level: ?u8 = null;
        {
            const held = self.mu.hold(.exclusive, @src());
            defer held.release();
            for (changes) |c| {
                if (c.op == .delete) { // DELETE
                    self.levels[c.level].deleteEntry(held, c.sst_id);
                } else if (c.op == .create) { // CREATE
                    const n = needed.items[need_idx];
                    need_idx += 1;
                    std.debug.assert(c.sst_id == n.id);
                    // A compaction output is a new file, listed under
                    // the applying entry's index, which is exactly what
                    // the manifest just stored for it. An L0→L0 merge's
                    // output carries its own instead: the newest
                    // input's, so it keeps their place in L0's order.
                    try self.levels[c.level].addEntry(
                        held,
                        n.id,
                        if (c.raft_index != 0) c.raft_index else index,
                        n.table.?,
                    );
                    // A moved file (deleted from another level in this
                    // entry) was written once already: no new bytes.
                    if (!deletedIn(changes, c.sst_id)) {
                        bytes_out += n.table.?.fileSize();
                    }
                    dest_level = @intCast(c.level);
                }
            }
        }

        // Metrics: one compaction run, plus bytes written to the dest level.
        const ns_id = self.namespace;
        if (dest_level) |lvl| {
            try _metrics.g().compactions.incr(.{
                .namespace_id = ns_id,
                .tree_id = self.tree_id,
                .level = lvl,
            });
            try _metrics.g().bytes_written.incrBy(.{
                .source = "compact",
                .namespace_id = ns_id,
                .tree_id = self.tree_id,
                .level = lvl,
            }, bytes_out);
        }

        try self.updateCompactCandidate();
    }

    /// Whether `changes` unlists `id` somewhere: a create of the same id
    /// in that entry is a trivial move, not a new file.
    fn deletedIn(changes: []const _manifest.SSTableChange, id: u64) bool {
        for (changes) |c| {
            if (c.op == .delete and c.sst_id == id) return true;
        }
        return false;
    }

    /// Install a CREATE INDEX backfill: bottom-level CREATEs only
    /// (Forest.buildBottomTable). Valid solely while the bottom level
    /// is still EMPTY — the backfill states are older than every
    /// delta, so anything already at the bottom (a compaction that
    /// raced past the engine's tree reservation) would sit ABOVE them
    /// in version order while sharing the level, breaking the sorted
    /// run — and a rolled-up terminal there would already be missing
    /// the backfilled rows. The check reads applied state, so every
    /// node skips (or installs) identically; the DDL session verifies
    /// via Forest.bottomLevelHas after the apply and fails CREATE
    /// INDEX loudly on a skip.
    pub fn applyBackfill(
        self: *Tree,
        index: u64,
        changes: []const _manifest.SSTableChange,
    ) !void {
        if (changes.len == 0) return;
        const bottom: u32 = @intCast(self.live.cur().max_levels - 1);
        for (changes) |c| {
            std.debug.assert(c.op == .create);
            std.debug.assert(c.level == bottom);
        }
        const bottom_tables = blk: {
            const held = self.mu.hold(.shared, @src());
            defer held.release();
            break :blk self.levels[bottom].tableCount(held);
        };
        if (bottom_tables > 0) {
            log.err(
                "backfill install skipped: bottom level not empty tree={d}",
                .{self.tree_id},
            );
            return;
        }
        try self.applyCompaction(index, changes);
    }
};
