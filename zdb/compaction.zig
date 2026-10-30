// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Compaction for a zdb Tree: the per-level size targets, the scoring
//! that picks which level to compact next, and Phase A of the pipeline,
//! which merges a level into the one below it and writes the output SST
//! files through the Emitter (emitter.zig, with the VersionTracker
//! that decides what of each primary is written). Every function here
//! takes `self: *Tree` and is aliased into `Tree` in tree.zig, so
//! callers keep using method syntax on a tree.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const _manifest = @import("manifest.zig");
const _level_handler = @import("level.zig");
const _table = @import("table.zig");
const vfs = @import("vfs");
const _metrics = @import("metrics.zig");
const _tree = @import("tree.zig");
const _config = @import("config.zig");
const _emitter = @import("emitter.zig");

const Tree = _tree.Tree;
const min_lmax_tables_for_split = Tree.min_lmax_tables_for_split;
const TableIterator = _table.TableIterator;
const Level = _level_handler.Level;
const Lock = _level_handler.Lock;
pub const TableEntry = _level_handler.TableEntry;
const Key = @import("kv.zig").Key;
const TableKey = @import("kv.zig").TableKey;
const KeyRange = @import("kv.zig").KeyRange;
const MergeIterator = _table.merge_iterator.MergeIterator;
const ConcatIterator = _table.merge_iterator.ConcatIterator;
const Iterator = _table.merge_iterator.Iterator;
pub const Emitter = _emitter.Emitter;
pub const VersionTracker = _emitter.VersionTracker;
const Metrics = _emitter.Metrics;

const log = std.log.scoped(.zdb_tree);

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// A tree's pending work item, picked by `Forest.bestCompactionCandidate`.
/// What a tree wants done next, decided by `updateCompactCandidate_`
/// from the levels as they are and kept fresh by every install. The
/// forest ranks trees by `score`; the builder runs `job` on `tables`
/// without picking again.
///
/// A `split` means the tree has outgrown `Config.splitBytes` and should
/// be split instead of compacted; `level` then describes the lowest
/// non-empty level (informational, since split operates across all
/// levels at the median key, not on a single level), and `score` is
/// bumped above the natural compaction range so a split preempts any
/// pending compaction in the forest.
pub const CompactCandidate = struct {
    /// The source level. L1 and below write to the level beneath; L0
    /// merges within itself or pushes into L1, as `job` says.
    level: u8,
    score: f64,
    /// Names the tables, nothing more: they are not pinned, so a table
    /// unlisted since the refresh may be freed. The builder looks each
    /// one up by id in its level and gives up if one is gone. Under the
    /// tree's lock, freed at the next refresh (`clearCandidate`).
    job: Job,

    /// The levels the job occupies while it runs: its source and
    /// destination; a split, every level. Two jobs of one tree run at
    /// once when these are disjoint (`Tree.busy_levels`).
    pub fn levelSet(self: CompactCandidate) LevelSet {
        var set = LevelSet.initEmpty();
        switch (self.job) {
            .split => set = LevelSet.initFull(),
            .merge_l0 => set.set(0),
            .move_down, .merge_down => {
                set.set(self.level);
                set.set(self.level + 1);
            },
        }
        return set;
    }

    /// A copy that owns its job (`Job.dupe`).
    pub fn dupe(
        self: CompactCandidate,
        allocator: std.mem.Allocator,
    ) !CompactCandidate {
        return .{
            .level = self.level,
            .score = self.score,
            .job = try self.job.dupe(allocator),
        };
    }
};

/// A set of a tree's levels (`Config.max_levels` is a u8).
pub const LevelSet = std.StaticBitSet(max_levels);
const max_levels = 1 + @as(usize, std.math.maxInt(u8));

/// What a level wants done (`pickJob`, `pickL0Merge`),
/// or a split of the whole tree.
/// Picked as views into the level's own array, valid under the hold it
/// was picked under; the candidate holds a copy.
pub const Job = union(enum) {
    /// One table that goes into the level beneath as it is.
    move_down: TableEntry,
    /// Sources to rewrite with what they overlap in the level beneath.
    merge_down: []const TableEntry,
    /// An L0 run to rewrite as one L0 table.
    merge_l0: []const TableEntry,
    /// The tree has outgrown `Config.splitBytes`: split it instead of
    /// compacting. Never picked for a level.
    split,

    fn tables(self: *const Job) []const TableEntry {
        return switch (self.*) {
            .move_down => |*e| e[0..1],
            .merge_down, .merge_l0 => |ts| ts,
            .split => &.{},
        };
    }

    /// A copy whose table list is the caller's, freed with `deinit`.
    pub fn dupe(self: Job, allocator: std.mem.Allocator) !Job {
        return switch (self) {
            .move_down => |e| .{ .move_down = e },
            .merge_down => |ts| .{
                .merge_down = try allocator.dupe(TableEntry, ts),
            },
            .merge_l0 => |ts| .{
                .merge_l0 = try allocator.dupe(TableEntry, ts),
            },
            .split => .split,
        };
    }

    pub fn deinit(self: Job, allocator: std.mem.Allocator) void {
        switch (self) {
            .merge_down, .merge_l0 => |ts| allocator.free(ts),
            .move_down, .split => {},
        }
    }
};

// -----------------------------------------------------------------------
// L0 job selection
// -----------------------------------------------------------------------

// L0 has two jobs of its own: a push into L1 (`pickJob`) and a
// merge within L0 (`pickL0Merge`). Both can be ready at once, and
// `updateCompactCandidate_` picks between them by score.

/// The run an L0 to L0 merge rewrites as one table, a sub-slice of
/// `tables` (L0 in its own order, oldest first), null when none is
/// ready. The run starts after the newest
/// finished table (one that has reached the table size,
/// `opts.table_bytes`), takes the tables that follow it in order, and
/// stops once it holds twice that size or runs out of tables. It is
/// ready once it has at least `opts.l0_merge_min_tables` tables. The
/// output is finished if the run reached the size.
///
/// A run is contiguous and its output is listed at the newest input's
/// raft index, so it stands where its inputs stood: a point read walks
/// L0 newest first and settles a key at the first table holding it,
/// which is only right while newer tables hold newer versions. A
/// finished table is never an input again, so a byte is rewritten
/// within L0 only until it sits in a finished table.
pub fn l0MergeRun(
    opts: _config.Config,
    tables: []const TableEntry,
) ?[]const TableEntry {
    const min_tables: usize = opts.l0_merge_min_tables;
    if (min_tables == 0) return null;

    const full: u64 = opts.table_bytes;
    var lo: usize = 0;
    for (tables, 0..) |t, i| {
        if (t.size >= full) lo = i + 1;
    }
    var hi: usize = lo;
    var total: u64 = 0;
    while (hi < tables.len and total < 2 * full) : (hi += 1) {
        total += tables[hi].size;
    }
    if (hi - lo < min_tables) return null;
    return tables[lo..hi];
}

/// Whether the tree's tables may ever move down without a rewrite:
/// rows and unique entries, whose rewrite above the bottom would only
/// recount stale bytes. A posting index's chains need the rewrite to
/// roll up; an unknown kind gets no assumption.
fn canMove(self: *const Tree) bool {
    return switch (self.kind) {
        .rows, .unique_index => true,
        .posting_index, .none, _ => false,
    };
}

/// Whether table `e` of the level above `dest` can go into `dest` as
/// it is (`moveDown`) instead of being rewritten. Sequential keys
/// (egids, timestamps) make this the common case: every flush covers
/// a new key range, so a push and every step down after it would
/// otherwise copy each byte into an identical file.
///
/// Three conditions, so that the rewrite a move skips would have
/// changed nothing:
///   - `e` meets no table of `dest`. A level below L0 is a sorted run.
///   - `dest` is not the bottom level. Only the bottom rewrite discards
///     versions below the read floor and drops tombstones (the tracker
///     is told so), so a moved table still gets both, later. It also
///     gives the bulk of the data the bottom codec; moved tables above
///     it keep their own (each records it).
///   - The tree's kind allows it (`canMove`).
///
/// One table moves at a time, and from L0 only the oldest: a version
/// in a lower level must be older than every version of its key above
/// it, and L0 tables can share keys, so a younger L0 table moved past
/// an older one would be shadowed by it. Below L0 a table shares no key
/// with its level, and every level above holds only newer versions.
fn movable(
    self: *Tree,
    held: Lock.Shared,
    dest: usize,
    e: TableEntry,
) bool {
    if (dest + 1 >= self.live.cur().max_levels) return false;
    if (!canMove(self)) return false;
    return !self.levels[dest].overlapsAny(held, tableRange(e));
}

fn tableRange(e: TableEntry) KeyRange {
    return .{ .left = e.table.smallest, .right = e.table.biggest };
}

/// Score of a level with a movable table (`Job.move_down`: it meets
/// nothing in the level below): above the scores a rewrite has, below
/// a split's. A move costs one manifest entry, so there is nothing to
/// wait for, and it empties the upper levels of sequential data at no
/// write cost. Measured at 32 load streams: 165k rows/s with moves
/// first against 153k on raw scores, whose L0 picks were 1 GiB rewrites.
pub const move_priority_score: f64 = 50.0;

/// Score of an L0 to L0 merge with tables to spare (`pickL0Merge`):
/// above the scores a rewrite has, below a move's.
pub const merge_priority_score: f64 = 49.0;

/// Score assigned to a tree that needs a split. Comfortably above any
/// natural compaction score so splits always rank first.
pub const split_priority_score: f64 = 100.0;

// -----------------------------------------------------------------------
// Level sizing
// -----------------------------------------------------------------------

/// The byte target of `level`, a function of the options alone. Fixed
/// per tree: the bottom level's is `lmax_target_bytes`, and the levels
/// above cascade up by dividing by `level_size_multiplier`, floored at
/// 1 byte so the size/target score is well-defined. L0's is L1's: it
/// pushes once it holds what L1 may hold. A tree splits once its total
/// passes `Config.splitBytes`, five fourths of the Lmax target: what
/// the whole tree weighs with every level at target.
///
/// Why FIXED instead of derived from current data: dynamic targets
/// (RocksDB-style) have a chicken-and-egg problem — early in a tree's
/// life, Lmax is empty, so cascading from current sizes produces big
/// upper-level targets. Stepwise compaction then can't drain those
/// intermediates. Fixed targets sized to the tree's eventual capacity
/// sidestep this entirely. If Lmax briefly exceeds its target before
/// the split kicks in, that's fine — the split will eventually fire.
pub fn levelTarget(opts: _config.Config, level: usize) i64 {
    if (level >= opts.max_levels) return 0;
    if (level == 0) return levelTarget(opts, 1);
    var target: i64 = opts.lmax_target_bytes;
    var i = opts.max_levels - 1;
    while (i > level) : (i -= 1) {
        target = @divTrunc(target, opts.level_size_multiplier);
        if (target < 1) return 1;
    }
    return target;
}

// -----------------------------------------------------------------------
// Compaction selection
// -----------------------------------------------------------------------

/// updateCompactCandidate recomputes which level most needs compaction and
/// stores the result in self.compact_candidate. Called after open and after
/// every compaction so the candidate is always fresh.
pub fn updateCompactCandidate(self: *Tree) !void {
    // Reads every level's size and writes the verdict, under one
    // exclusive hold. Installs call this after releasing theirs.
    const held = self.mu.hold(.exclusive, @src());
    defer held.release();
    try self.updateCompactCandidate_(held);
}

/// The verdict itself. Caller holds `mu` exclusively — `held` is the
/// proof; the level reads below use it as shared.
pub fn updateCompactCandidate_(self: *Tree, held: Lock.Exclusive) !void {
    self.mu.assertHeld(held);
    const rd = held.shared();
    // Split check overrides compaction. We compare TOTAL tree size
    // (L0+L1+…+Lmax) against `Config.splitBytes`, not just Lmax. If
    // compactions can't keep up the data piles up across the upper
    // levels and Lmax stays under threshold even when the tree as a
    // whole is several times oversized — exactly the wedged shape
    // we just observed. Splitting on total size lets each tree get
    // its own future compactor and divides the catch-up work.
    // The score is bumped to `split_priority_score` so this tree
    // wins over normal compaction candidates in the forest. Set
    // `level` to the lowest non-empty level for diagnostic
    // legibility (`stats forest`).
    const linfo = self.lowestLevelSize(rd);
    const total = self.totalSize(rd);
    // Gate the split on Lmax feasibility: findSplitKey needs Lmax to hold
    // at least min_lmax_tables_for_split tables to pick a boundary key. A
    // tree can be oversized in total while its data is still stacked in the
    // upper levels (Lmax near-empty) — triggering then spins forever:
    // the split verdict stays, buildSplit returns null, proposeSplit skips.
    // When infeasible, fall through to compaction, which drains data into
    // Lmax until a split becomes possible.
    const lmax_tables = self.levels[self.live.cur().max_levels - 1].numTables(rd);
    const split_bytes = self.live.cur().splitBytes();
    const busy = self.busy_levels;
    if (total > split_bytes and
        lmax_tables >= min_lmax_tables_for_split)
    {
        // A split remaps every table: it waits for the tree's running
        // jobs and lets no new one start meanwhile.
        if (busy.count() != 0) return self.clearCandidate();
        log.debug(
            "split-trigger tree={d} total={d} deepest_level={d} " ++
                "lmax_size={d} lmax_tables={d} > threshold={d}",
            .{
                self.tree_id,
                total,
                linfo.level,
                linfo.size,
                lmax_tables,
                split_bytes,
            },
        );
        try setCandidate(self, .{
            .level = @intCast(linfo.level),
            .score = split_priority_score,
            .job = .split,
        });
        return;
    }

    // What each level would do and its score, decided once here: the
    // winner is stored as the verdict and the builder runs its job as
    // it is. L0 offers a push and a merge; the bottom level nothing.
    //
    // The highest score above 1.0 wins, the upper level on a tie. For
    // rewrites that is the fullest level by ratio, as RocksDB does.
    // Badger's adjustment, dividing a level's score by the one below
    // it, was here once; it boosts a level over an under-full one, and
    // under a sustained load that let L1 win every pick over an L0 of
    // 65 tables, with the write brake throttling the load meanwhile.
    // A level with a job running on it, or on the level it writes to,
    // offers nothing until that job is done.
    var best: ?CompactCandidate = null;
    for (0..self.live.cur().max_levels) |level| {
        if (busy.isSet(level) or busy.isSet(level + 1)) continue;
        const c = pickJob(self, rd, level) orelse continue;
        if (best == null or c.score > best.?.score) best = c;
    }
    const merge = if (busy.isSet(0)) null else pickL0Merge(self, rd);
    if (merge) |c| {
        if (best == null or c.score > best.?.score) best = c;
    }
    if (best) |c| {
        if (c.score > 1.0) return setCandidate(self, c);
    }

    // Idle fallback: nothing scored above 1.0, so no level needs work.
    // A merge of exactly the fewest tables runs while the workers have
    // nothing better.
    if (merge) |c| return setCandidate(self, c);
    self.clearCandidate();
}

/// The verdict, with a copy of the job's tables in place of the level
/// views it was picked as.
fn setCandidate(self: *Tree, c: CompactCandidate) !void {
    const owned = try c.dupe(self.allocator);
    self.clearCandidate();
    self.compact_candidate = owned;
}

pub fn clearCandidate(self: *Tree) void {
    const c = self.compact_candidate orelse return;
    c.job.deinit(self.allocator);
    self.compact_candidate = null;
}

/// Takes the candidate to run it: the caller owns the returned copy
/// until `releaseJob`. Its levels are marked busy and the candidate
/// recomputed under the same hold, so the next pick sees the tree's
/// next runnable job, never this one. Null when the tree wants nothing.
pub fn reserveJob(self: *Tree) !?CompactCandidate {
    const held = self.mu.hold(.exclusive, @src());
    defer held.release();

    const c = self.compact_candidate orelse return null;
    const owned = try c.dupe(self.allocator);
    errdefer owned.job.deinit(self.allocator);

    self.busy_levels.setUnion(owned.levelSet());
    try self.updateCompactCandidate_(held);
    return owned;
}

/// Frees a job from `reserveJob`, done or abandoned, and recomputes the
/// candidate: a job its levels blocked can be offered now.
pub fn releaseJob(self: *Tree, c: CompactCandidate) void {
    defer c.job.deinit(self.allocator);

    const held = self.mu.hold(.exclusive, @src());
    defer held.release();

    var freed = c.levelSet();
    freed.toggleAll();
    self.busy_levels.setIntersection(freed);
    // The next install refreshes the candidate if this cannot.
    self.updateCompactCandidate_(held) catch {};
}

/// Marks every level busy, for a build outside compaction (an index
/// backfill): the tree offers no job until `releaseAll`. The tree is
/// new then and runs nothing, so no job's own levels are taken over.
pub fn reserveAll(self: *Tree) !void {
    const held = self.mu.hold(.exclusive, @src());
    defer held.release();
    self.busy_levels = LevelSet.initFull();
    try self.updateCompactCandidate_(held);
}

pub fn releaseAll(self: *Tree) !void {
    const held = self.mu.hold(.exclusive, @src());
    defer held.release();
    self.busy_levels = LevelSet.initEmpty();
    try self.updateCompactCandidate_(held);
}

/// `num / den` as a score.
fn ratio(num: u64, den: u64) f64 {
    return @as(f64, @floatFromInt(num)) / @as(f64, @floatFromInt(den));
}

/// A rewrite's score from its ratio `r`: `r` itself, kept below
/// `merge_priority_score`, so a merge with tables to spare and a move
/// outrank a rewrite however far over target its level is. The last
/// unit under the bound takes every larger ratio in order, so two
/// rewrites past it still rank by how far over they are.
fn rewriteScore(r: f64) f64 {
    const knee = merge_priority_score - 1.0;
    return if (r <= knee) r else merge_priority_score - knee / r;
}

/// L0's merge, null when no run is ready (`l0MergeRun`). Its score is
/// the run's length against the fewest tables a merge takes. Above
/// 1.0 that becomes `merge_priority_score`, ahead of the push and of
/// the levels below: the merge is cheap and leaves L1 alone, and every
/// table it removes is a run each read merges and the brake counts.
/// Scored against an L1 at six times its target, L0 waited to 99
/// tables. At exactly 1.0 it does not win the pick and waits for the
/// tree to be idle.
fn pickL0Merge(self: *Tree, held: Lock.Shared) ?CompactCandidate {
    const opts = self.live.cur();
    const run = l0MergeRun(opts, self.levels[0].entries(held)) orelse
        return null;
    const score = ratio(run.len, opts.l0_merge_min_tables);
    return .{
        .level = 0,
        .score = if (score > 1.0) merge_priority_score else score,
        .job = .{ .merge_l0 = run },
    };
}

/// The job level `src` wants now, null when it wants none. With
/// `pickL0Merge`, what decides which tables compact:
/// `updateCompactCandidate_` stores the winner and `buildCompaction`
/// runs it.
///
/// It moves ONE table down as it is when any can, else rewrites a run
/// of tables (`mergeDownRun`), scored by the level's bytes against its
/// target. Below L0 the moved table is picked at random among those
/// that can: always taking the smallest-key table starves higher key
/// ranges and biases write amplification to the keyspace's left edge.
///
/// L0 differs in two ways. Only its oldest table may move. Its
/// rewrite scores the larger of the bytes ratio (L0's target is L1's:
/// a push of that size costs about two bytes written per byte
/// drained) and its table count against HALF the backpressure start
/// count: full-size tables at the target are still too few to score
/// by count.
fn pickJob(self: *Tree, held: Lock.Shared, src: usize) ?CompactCandidate {
    const entries = self.levels[src].entries(held);
    if (entries.len == 0) return null;

    const opts = self.live.cur();
    const dest = src + 1;
    if (dest >= opts.max_levels) return null;

    // For move, L0 can only consider the oldest table. Otherwise, we'd
    // have issues with newer values in lower levels.
    const pool: []const TableEntry =
        if (src == 0) entries[0..1] else entries;
    var movables: std.ArrayList(TableEntry) = .empty;
    defer movables.deinit(self.allocator);
    for (pool) |e| {
        if (movable(self, held, dest, e))
            movables.append(self.allocator, e) catch unreachable;
    }

    if (movables.items.len > 0) {
        const i = randomIndex(movables.items.len);
        return .{
            .level = @intCast(src),
            .score = move_priority_score,
            .job = .{ .move_down = movables.items[i] },
        };
    }

    const size: u64 = @intCast(self.levels[src].getTotalSize(held));
    const target: u64 = @intCast(levelTarget(opts, src));
    var score = ratio(size, target);
    if (src == 0) {
        const start_count = opts.l0_backpressure_start_count;
        score = @max(score, ratio(entries.len * 2, start_count));
    }
    return .{
        .level = @intCast(src),
        .score = rewriteScore(score),
        .job = .{ .merge_down = mergeDownRun(self, held, src) },
    };
}

/// A random index below `n`.
fn randomIndex(n: usize) usize {
    const rng_impl: std.Random.IoSource = .{ .io = utils.context.g().io };
    return rng_impl.interface().uintLessThan(usize, n);
}

/// The tables a `merge_down` of level `src` takes: a first table and
/// its neighbours in the level's order, as many as `job_max_bytes`
/// allows. L0 starts at its oldest table, so the run is its oldest
/// tables in order. A lower level starts at a random table and grows
/// the run evenly to both sides, so neither edge of the level waits
/// longer than the other; the run is adjacent in key order, so each
/// table of the level below is rewritten once.
///
/// The budget bounds the input, the tables taken plus those they
/// overlap below. The table that reaches it is taken; after it, only
/// tables that widen no overlap below, while the run's own bytes are
/// under the budget: the job rewrites the same tables below either
/// way. Without that, an L1 larger than the budget let each push
/// drain a single L0 table.
fn mergeDownRun(self: *Tree, held: Lock.Shared, src: usize) []const TableEntry {
    const entries = self.levels[src].entries(held);
    const budget = self.live.cur().job_max_bytes;
    const start = if (src == 0) 0 else randomIndex(entries.len);

    // The run is entries[lo..hi], `kr` the union of its key ranges and
    // `bot` the bytes that union overlaps below.
    var lo = start;
    var hi = start + 1;
    var kr = tableRange(entries[start]);
    var taken: u64 = entries[start].size;
    var bot = overlapBytes(self, held, src + 1, kr);

    while (hi < entries.len or lo > 0) {
        // The side with fewer tables in the run, when both have one.
        const right = if (lo == 0)
            true
        else if (hi == entries.len)
            false
        else
            hi - start <= start - lo;
        const next = if (right) hi else lo - 1;

        var next_kr = kr;
        next_kr.extend(tableRange(entries[next]));
        const next_bot = overlapBytes(self, held, src + 1, next_kr);
        if (taken + bot >= budget and
            (next_bot > bot or taken >= budget)) break;

        kr = next_kr;
        bot = next_bot;
        taken += entries[next].size;
        if (right) hi += 1 else lo -= 1;
    }
    return entries[lo..hi];
}

/// The bytes of `level`'s tables that overlap `kr`.
fn overlapBytes(
    self: *Tree,
    held: Lock.Shared,
    level: usize,
    kr: KeyRange,
) u64 {
    var bytes: u64 = 0;
    for (self.levels[level].overlapping(held, kr)) |oe| bytes += oe.size;
    return bytes;
}

/// buildCompaction is Phase A: a job taken from the tree (`reserveJob`),
/// built into new SST files, as the manifest SSTableChange list the
/// caller proposes (through Raft) and then applies (applyCompaction).
/// Nothing is picked here: the job was computed from the levels as
/// they were. Its tables are looked up by id under one hold; one that
/// is gone means an install landed between the pick and this read,
/// and there is nothing to build for it.
///
/// A merge's output is listed under the newest input's `raft_index`,
/// not the applying entry's: it holds what its inputs held, so it stands
/// where they stood in L0's order. A flush that lands while this builds
/// is newer than every input and keeps sorting after the output.
///
/// `snapshot_ts` is the oldest timestamp a reader may still ask for
/// (`Forest.snapshot_ts`); anything strictly below it is unobservable
/// and eligible to be discarded. Pass 0 to discard nothing.
pub fn buildCompaction(
    self: *Tree,
    cand: CompactCandidate,
    snapshot_ts: u64,
) !std.ArrayList(_manifest.SSTableChange) {
    if (cand.job == .split) return .empty;
    // Pinned under the hold: a remote-built applyCompaction landing
    // mid-build (leadership flip) must not free tables we're about to
    // read. decrRef at zero frees memory only — the file stays — so a
    // pinned table remains readable. The hold covers the lookup only:
    // a build takes seconds to minutes, and the tree's exclusive holders
    // (a flush's apply, on the raft apply loop) must not wait on it.
    const srcs = blk: {
        const held = self.mu.hold(.shared, @src());
        defer held.release();

        // The listed entries, by id: the candidate's own may name a
        // table that has since been unlisted and freed.
        const entries = self.levels[cand.level].entries(held);
        const named = cand.job.tables();

        const srcs = try self.allocator.alloc(TableEntry, named.len);
        errdefer self.allocator.free(srcs);

        for (named, srcs) |n, *e| {
            e.* = findEntry(entries, n.id) orelse {
                log.info("compact tree={d} L{d} stale: sst={d} gone", .{
                    self.tree_id, cand.level, n.id,
                });
                self.allocator.free(srcs);
                return .empty;
            };
        }
        for (srcs) |e| e.table.incrRef();
        break :blk srcs;
    };
    defer Level.releaseTables(self.allocator, srcs);

    const src: usize = cand.level;
    switch (cand.job) {
        .move_down => return moveDown(self, src, src + 1, srcs[0]),
        .merge_down => return mergeDown(self, src, srcs, snapshot_ts),
        .merge_l0 => return self.mergeAndWrite(.{
            .src_level = 0,
            .dest_level = 0,
            .src = srcs,
            .bot = &.{},
            .out_raft_index = srcs[srcs.len - 1].raft_index,
        }, snapshot_ts),
        .split => unreachable,
    }
}

fn findEntry(entries: []const TableEntry, id: u64) ?TableEntry {
    for (entries) |e| {
        if (e.id == id) return e;
    }
    return null;
}

/// `srcs` of L`src` rewritten with the tables they overlap in the level
/// beneath. Stepwise: every level pushes to the level immediately below
/// it, including L0 → L1. Routing L0 directly to Lbase used to produce
/// huge cross-level compactions and made intermediate levels
/// effectively unused; routing one step at a time keeps each
/// compaction's input set bounded by the destination level's
/// overlapping range.
fn mergeDown(
    self: *Tree,
    src: usize,
    srcs: []const TableEntry,
    snapshot_ts: u64,
) !std.ArrayList(_manifest.SSTableChange) {
    const dest = src + 1;

    var kr: ?KeyRange = null;
    for (srcs) |e| {
        const tkr = tableRange(e);
        if (kr) |*r| r.extend(tkr) else kr = tkr;
    }

    const bot = blk: {
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        break :blk try self.levels[dest].overlappingTables(
            held,
            self.allocator,
            kr orelse return .empty,
        );
    };
    defer Level.releaseTables(self.allocator, bot);

    return self.mergeAndWrite(.{
        .src_level = src,
        .dest_level = dest,
        .src = srcs,
        .bot = bot,
    }, snapshot_ts);
}

/// A trivial move: list `e` in `dest` and unlist it from `src`, in one
/// entry, the file untouched. The manifest applies creates before
/// deletes, so a moved file's listing count goes 1, 2, 1 and it is never
/// retired. The create keeps the table's `raft_index`: the file's place
/// in L0's order is where it was first listed, and the manifest keeps
/// that record for a file it already knows.
fn moveDown(
    self: *Tree,
    src: usize,
    dest: usize,
    e: TableEntry,
) !std.ArrayList(_manifest.SSTableChange) {
    // The candidate said so when it was picked; nothing has changed
    // `dest` since without refreshing it.
    {
        const held = self.mu.hold(.shared, @src());
        defer held.release();
        std.debug.assert(movable(self, held, dest, e));
    }

    var changes = try std.ArrayList(_manifest.SSTableChange).initCapacity(
        self.allocator,
        2,
    );
    errdefer changes.deinit(self.allocator);
    changes.appendAssumeCapacity(_manifest.deleteSSTableChange(
        e.id,
        self.namespace,
        self.tree_id,
        @intCast(src),
    ));

    var create = _manifest.createSSTableChange(
        e.id,
        self.namespace,
        self.tree_id,
        @intCast(dest),
        e.table.extent,
    );
    create.raft_index = e.raft_index;
    changes.appendAssumeCapacity(create);

    log.info("move tree={d} L{d}->L{d} sst={d} bytes={d}", .{
        self.tree_id, src, dest, e.id, e.table.fileSize(),
    });
    return changes;
}

/// The I/O class a job from `src_level` runs at: foreground from L0,
/// since a full L0 stalls the flush and so the writes, like the flush
/// itself; background below, behind every user.
pub fn classFor(src_level: usize) fiber.IoClass {
    return if (src_level == 0) .fg else .bg;
}

/// The inputs and the shape of one compaction's output.
pub const MergeJob = struct {
    src_level: usize,
    dest_level: usize,
    /// Pinned by the caller for the duration of the build.
    src: []const TableEntry,
    /// The destination level's tables the sources overlap; rewritten
    /// with them. Empty for an L0→L0 merge.
    bot: []const TableEntry,
    /// The `raft_index` the outputs are listed under; 0 = the index of
    /// the entry that applies the job (every job but an L0→L0 merge).
    out_raft_index: u64 = 0,
};

/// One key-range slice of a job: the half-open range every source is
/// read in, and the bottom tables it rewrites, already opened as one
/// concat. A boundary is a bottom table's smallest key, so no version
/// of a primary is split across two slices and each slice's outputs
/// sit between its neighbours' in key order. The range bounds the
/// sources (L0's span the whole tree); the concat bounds only the
/// destination side.
const Partition = struct {
    range: KeyRange,
    bot: ConcatIterator,
};

/// Cut `job.bot` into up to `max` runs of consecutive tables, one
/// partition each. The first starts at the tree's left edge and the
/// last ends at its right, so the partitions cover the tree exactly;
/// with no bottom tables, or `max` of 1, there is one partition.
/// `bot_iters` is one iterator per bottom table, the caller's to free
/// once every partition's merge has released them.
fn partitions(
    self: *Tree,
    job: MergeJob,
    max: usize,
    bot_iters: []TableIterator,
    out: []Partition,
) []Partition {
    const bot = job.bot;
    std.debug.assert(bot_iters.len == bot.len);
    const table_key: TableKey = .{
        .namespace = self.namespace,
        .table_id = self.tableId(),
    };
    const window = self.live.cur().compaction_window_bytes;
    const tloc = fiber.allocator(.tenant);
    for (bot, bot_iters) |e, *it| {
        it.* = TableIterator.init(tloc, e.table, table_key);
        it.window_bytes = window;
    }
    const n = @min(@max(max, 1), @max(bot.len, 1), out.len);
    for (out[0..n], 0..) |*part, i| {
        const lo = bot.len * i / n;
        const hi = bot.len * (i + 1) / n;
        part.* = .{
            .range = .{
                .left = if (i == 0)
                    self.key_range.left
                else
                    buildTableBoundKey(table_key, bot[lo].table.smallest),
                .right = if (i == n - 1)
                    self.key_range.right
                else
                    buildTableBoundKey(table_key, bot[hi].table.smallest),
            },
            .bot = ConcatIterator.init(bot_iters[lo..hi]),
        };
    }
    return out[0..n];
}

/// A table's edge key as a seek key of the tree: a `Table` knows only
/// primaries (its `smallest`/`biggest` carry no namespace or table
/// id), and the iterators assert that a seek key is the tree's own.
fn buildTableBoundKey(table_key: TableKey, k: Key) Key {
    return .{
        .namespace = table_key.namespace,
        .table_id = table_key.table_id,
        .primary = k.primary,
        .bound = k.bound,
    };
}

/// Merge `job.src` with `job.bot`, write the output SSTs, and return the
/// manifest changes that swap the inputs for the outputs. The job is
/// split into `compaction_fibers` partitions along the destination's
/// table boundaries (`partitions`), each merged and written by its own
/// fiber: the sources are read once over, each fiber taking its own
/// range, and the outputs are installed by one manifest entry. An L0
/// to L0 merge has no bottom tables, so it stays one partition.
pub fn mergeAndWrite(
    self: *Tree,
    job: MergeJob,
    snapshot_ts: u64,
) !std.ArrayList(_manifest.SSTableChange) {
    const src = job.src_level;
    const dest = job.dest_level;
    const opts = self.live.cur();

    // The bottom iterators outlive the fibers: each partition's merge
    // releases its own at deinit, the slice is freed here after. The
    // job's buffers are held by the tenant this fiber runs as.
    const tloc = fiber.allocator(.tenant);
    const bot_iters = try tloc.alloc(TableIterator, job.bot.len);
    defer tloc.free(bot_iters);

    var part_buf: [max_compaction_fibers]Partition = undefined;
    const parts = partitions(
        self,
        job,
        opts.compaction_fibers,
        bot_iters,
        &part_buf,
    );

    var group = try fiber.Group(?Emitter.Result).initCapacity(
        self.allocator,
        parts.len,
    );
    defer group.deinit();

    // A partition not handed to a fiber still owns its concat.
    var spawned: usize = 0;
    errdefer for (parts[spawned..]) |*part| part.bot.deinit();

    for (parts) |part| {
        try group.spawn(.{}, runPartition, .{ self, job, part, snapshot_ts });
        spawned += 1;
    }
    const results = group.wait();

    // Every partition's puts are done; this makes their names durable.
    try self.cache.store.sync();

    // The manifest changes: every partition's CREATEs, in partition
    // order and listed at the job's raft index, then the DELETEs of
    // the inputs (the manifest applies creates first whatever the
    // order). The stats are summed.
    var changes: std.ArrayList(_manifest.SSTableChange) = .empty;
    errdefer changes.deinit(self.allocator);

    var stats: Metrics = .{};
    var failed = false;
    for (results) |r_opt| {
        var r = r_opt orelse {
            failed = true;
            continue;
        };
        defer r.changes.deinit(self.allocator);
        for (r.changes.items) |create| {
            var tc = create;
            tc.raft_index = job.out_raft_index;
            try changes.append(self.allocator, tc);
        }
        stats.merge(r.stats);
    }
    if (failed) return error.FlushFailed;
    try reportJob(self, job, parts.len, &stats, snapshot_ts);

    for (job.src) |e| try changes.append(
        self.allocator,
        _manifest.deleteSSTableChange(
            e.id,
            self.namespace,
            self.tree_id,
            @intCast(src),
        ),
    );
    for (job.bot) |e| try changes.append(
        self.allocator,
        _manifest.deleteSSTableChange(
            e.id,
            self.namespace,
            self.tree_id,
            @intCast(dest),
        ),
    );
    return changes;
}

/// A finished job's accounting: the input tables' own key counts and
/// on-disk sizes are added to `stats` as the "expected" baseline, the
/// job is logged, and its rollups counted in the metrics. If keys_in
/// != expected, the merge iterator is losing items and something is
/// wrong. keys_out < keys_in is normal: it's what this compaction
/// discarded.
fn reportJob(
    self: *Tree,
    job: MergeJob,
    num_fibers: usize,
    stats: *Metrics,
    snapshot_ts: u64,
) !void {
    const src = job.src_level;
    const dest = job.dest_level;
    for (job.src) |e| {
        stats.src_keys += @intCast(e.table.index.key_count);
        stats.src_bytes_disk += e.table.fileSize();
    }
    for (job.bot) |e| {
        stats.bot_keys += @intCast(e.table.index.key_count);
        stats.bot_bytes_disk += e.table.fileSize();
    }
    const expected_in = stats.src_keys + stats.bot_keys;
    const expected_bytes_disk =
        stats.src_bytes_disk + stats.bot_bytes_disk;
    const lost_in_iter = expected_in -| stats.keys_in;
    const discarded = stats.keys_in -| stats.keys_out;
    log.info(
        "compact tree={d} L{d}->L{d} fibers={d} src_tables={d} src_keys={d} " ++
            "src_bytes_disk={d} bot_tables={d} bot_keys={d} " ++
            "bot_bytes_disk={d} expected_in={d} expected_bytes_disk={d} " ++
            "keys_in={d} keys_out={d} bytes_in={d} bytes_out={d} " ++
            "lost_in_iter={d} discarded={d} discarded_bytes={d} " ++
            "stale_bytes={d} rollups={d} snapshot_ts={d}",
        .{
            self.tree_id,         src,               dest,
            num_fibers,           job.src.len,       stats.src_keys,
            stats.src_bytes_disk, job.bot.len,       stats.bot_keys,
            stats.bot_bytes_disk, expected_in,       expected_bytes_disk,
            stats.keys_in,        stats.keys_out,    stats.bytes_in,
            stats.bytes_out,      lost_in_iter,      discarded,
            stats.dropped_bytes,  stats.stale_bytes, stats.rollups,
            snapshot_ts,
        },
    );
    if (stats.rollups > 0) {
        try _metrics.g().rollups.incrBy(.{
            .namespace_id = self.namespace,
            .tree_id = self.tree_id,
            .level = @intCast(dest),
        }, stats.rollups);
    }
}

/// The bound on `compaction_fibers`: partitions live on the stack.
pub const max_compaction_fibers = 16;

/// One partition of a job on its own fiber: merge the sources over the
/// partition's range with its bottom tables, write the outputs, wait
/// for their syncs. Null on failure, logged here.
fn runPartition(
    self: *Tree,
    job: MergeJob,
    part: Partition,
    snapshot_ts: u64,
) ?Emitter.Result {
    return runPartition_(self, job, part, snapshot_ts) catch |err| {
        log.err("compact tree={d} L{d}->L{d} partition failed: {s}", .{
            self.tree_id, job.src_level, job.dest_level, @errorName(err),
        });
        return null;
    };
}

fn runPartition_(
    self: *Tree,
    job: MergeJob,
    part: Partition,
    snapshot_ts: u64,
) !Emitter.Result {
    const dest = job.dest_level;

    // The job's I/O class, and the caches bypassed: each input is read
    // once, and a hit must not count as reuse.
    const ctx = fiber.ctx();
    ctx.class = classFor(job.src_level);
    ctx.cache = .bypass;

    // --- one MergeIterator over the src tables + the partition's
    // concat over its bottom tables. The concat is this fiber's to
    // release until the merge takes it over. ---
    var part_bot = part.bot;
    const n_iters = job.src.len + 1;
    const tloc = fiber.allocator(.tenant);
    const all_iters = tloc.alloc(Iterator, n_iters) catch |err| {
        part_bot.deinit();
        return err;
    };
    defer tloc.free(all_iters);

    const table_key: TableKey = .{
        .namespace = self.namespace,
        .table_id = self.tableId(),
    };
    const window = self.live.cur().compaction_window_bytes;
    for (job.src, 0..) |e, i| {
        var it = TableIterator.init(tloc, e.table, table_key);
        it.window_bytes = window;
        all_iters[i] = .{ .table = it };
    }
    all_iters[job.src.len] = .{ .concat = part_bot };

    // The merge takes every iterator over, the concat included.
    var merge = MergeIterator.init(tloc, all_iters) catch |err| {
        for (all_iters) |*it| it.deinit();
        return err;
    };
    defer merge.deinit();
    try merge.seekRange(part.range.left, part.range.right);

    // --- merge loop ---
    var emitter = try Emitter.init(self, dest, snapshot_ts);
    defer emitter.deinit();
    while (merge.itemRef()) |item| {
        try emitter.push(item);
        try merge.next();
    }
    return emitter.finish();
}
