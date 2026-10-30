// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The compaction output stage: what of a primary's versions is
//! written (`VersionTracker`), and how the tables come out
//! (`Emitter`). One Emitter writes one level of one tree; the memtable
//! flush runs one per tree at L0, the index backfill one at the bottom
//! level, and a compaction job one per partition.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const _manifest = @import("manifest.zig");
const _table = @import("table.zig");
const vfs = @import("vfs");
const _read = @import("read.zig");
const zroar = @import("zroar");
const _tree = @import("tree.zig");
const _config = @import("config.zig");

const Tree = _tree.Tree;
const Item = _table.table.Item;
const Version = @import("kv.zig").Version;
const Builder = _table.builder.Builder;

const log = std.log.scoped(.zdb_tree);

/// Decides what of one primary's versions compaction writes. The merge
/// hands over one item per primary with every version of it, newest
/// first, so both jobs run over a slice within one call:
///
/// 1. Retention (`retain`): a terminal is a complete state or
///    tombstone. Versions after the newest terminal are shadowed and
///    count as stale. At the bottom level, the first terminal at or
///    below the read horizon establishes a discard boundary. A settled
///    tombstone is itself unreachable and is discarded.
///
/// 2. Roaring rollup (`track`): when the item's newest version is a
///    roaring delta, the chain down to its terminal (`Item.chain`) is
///    folded by `rollupRoaring` into a synthetic state (config triggers
///    permitting) or emitted verbatim. Only that head chain is ever
///    folded: everything below its terminal is shadowed history, and
///    re-rolling it would regenerate a synthetic state under the live
///    terminal on every compaction until GC collapses the chain.
///
/// `track` consumes and decides; the Emitter (passed per call) only
/// knows how to emit, and runs `retain` over what it is handed.
pub const VersionTracker = struct {
    /// Retention verdict for one version. `.stale` means kept but
    /// shadowed — the caller charges the version's own size to the
    /// stale accounting.
    const Update = enum { retain, stale, discard };

    allocator: std.mem.Allocator,
    can_discard_versions: bool,
    snapshot_ts_ts: u64,

    // Retention state, reset by `startItem`: one item is one primary.
    stale_below: u64 = 0,
    discard_below: u64 = 0,
    /// The versions of the item being emitted that retention keeps.
    kept: std.ArrayList(Version) = .empty,
    /// What a rollup emits: the synthetic state, then the item's
    /// versions below the delta it replaced.
    rolled: std.ArrayList(Version) = .empty,

    pub fn init(
        allocator: std.mem.Allocator,
        snapshot_ts_ts: u64,
        can_discard_versions: bool,
    ) VersionTracker {
        return .{
            .allocator = allocator,
            .snapshot_ts_ts = snapshot_ts_ts,
            .can_discard_versions = can_discard_versions,
        };
    }

    pub fn deinit(self: *VersionTracker) void {
        self.kept.deinit(self.allocator);
        self.rolled.deinit(self.allocator);
    }

    /// Open retention for a new item (a new primary).
    fn startItem(self: *VersionTracker) void {
        // stale_below is only used for tracking stale bytes in the
        // table. discard_below is for discarding versions.
        self.stale_below = 0;
        self.discard_below = 0;
    }

    /// Whether one version of the open item is retained, discarded, or
    /// retained but stale (eligible for discard later). Versions arrive
    /// newest first.
    fn retain(self: *VersionTracker, ver: Version) Update {
        if (ver.ts < self.discard_below) {
            return .discard;
        }
        const stale = ver.ts < self.stale_below;

        if (ver.meta.isState() or ver.meta.isDeleted()) {
            if (self.stale_below == 0) {
                self.stale_below = ver.ts;
            }

            // A terminal at or below the read floor: every read settles
            // on it or on something newer, so what it shadows is
            // unreachable at every level and goes now. The tombstone
            // itself waits for the bottom: dropped higher up, an older
            // version below would come back.
            if (ver.ts <= self.snapshot_ts_ts) {
                self.discard_below = ver.ts;
                if (ver.meta.isDeleted() and self.can_discard_versions)
                    return .discard;
            }
        }

        return if (stale) .stale else .retain;
    }

    /// Every consumed item enters here, in merge order. Its values are
    /// valid for the whole call: the merge holds their storage until
    /// its own `next`, which is what lets the rollup's peek below yield
    /// for I/O with the chain still borrowed.
    fn track(t: *VersionTracker, e: *Tree.Emitter, item: Item) !void {
        if (foldable(e, item)) |chain| return t.rollupRoaring(e, item, chain);
        try e.emitOne(item);
    }

    /// The chain a rollup may fold: the item's versions down to and
    /// including the first terminal, when rollup is on, the output goes
    /// below L0, the newest version is a roaring delta, and nothing
    /// foreign sits inside the chain — every version is a roaring
    /// delta, or the terminal is a tombstone (its encoding is not
    /// looked at) or a roaring state.
    /// Null otherwise: the item is emitted verbatim. A lone state is
    /// already rolled, raw rows are not ours, and everything below the
    /// terminal is shadowed history that is never folded.
    fn foldable(e: *const Tree.Emitter, item: Item) ?Item {
        if (!e.opts.rollup_enabled) return null;
        // An L0 to L0 merge folds nothing: a state it wrote would be
        // rewritten by the next push, and L0 would hold whole postings.
        if (e.dest == 0) return null;

        const newest = item.newest();
        if (!(newest.meta.isRoaring() and newest.meta.isDelta())) return null;

        const chain = item.chain();
        for (chain.versions.slice()) |ver| {
            if (ver.meta.isRoaring() and ver.meta.isDelta()) continue;
            if (ver.meta.isDeleted()) continue;
            if (ver.meta.isRoaring() and ver.meta.isState()) continue;
            return null;
        }
        return chain;
    }

    /// Whether `chain` ends in a terminal (a state or a tombstone), as
    /// against running out of versions on a delta.
    fn hasTerminal(chain: Item) bool {
        const vers = chain.versions.slice();
        return !vers[vers.len - 1].meta.isDelta();
    }

    /// Resolve `chain`, the foldable head of `item`: fold it into a
    /// synthetic state when policy allows, else emit the item verbatim.
    /// The synthetic state carries the newest delta's timestamp,
    /// REPLACING that delta: readers at read_ts >= t stop there (the
    /// point of rollup); readers below t skip it and fold the retained
    /// chain, emitted unchanged beneath — MVCC and GC semantics are
    /// untouched, the state is just a new terminal for `retain`. An
    /// empty fold emits a tombstone instead: 0 bytes vs zroar's 64-byte
    /// empty bitmap, and bottom-level GC can then drop the key outright.
    fn rollupRoaring(
        t: *VersionTracker,
        e: *Tree.Emitter,
        item: Item,
        chain: Item,
    ) !void {
        const opts = &e.opts;
        const alloc = e.allocator;
        const have_state = hasTerminal(chain);
        const n_deltas = chain.versions.len() - @intFromBool(have_state);

        // Policy: fold when the chain is long enough, and — when its
        // base is not among the inputs (no terminal in the chain) —
        // when a trigger justifies peeking below the destination for
        // it (see config.zig's rollup_* knobs).
        const roll = n_deltas >= opts.rollup_min_deltas and
            (have_state or
                e.dest >= opts.rollup_at_level or
                n_deltas >= opts.rollup_max_deltas);
        if (!roll) return e.emitOne(item);

        var external: ?zroar.Bitmap = null;
        defer if (external) |*eb| eb.deinit();

        if (!have_state) {
            // The oldest ts in the chain bounds the peek: the base must
            // sit strictly below the chain we hold. HLC timestamps are
            // never 0, so the -1 cannot wrap.
            const chain_vers = chain.versions.slice();
            const oldest = chain_vers[chain_vers.len - 1].ts;
            std.debug.assert(oldest > 0);

            external = e.tree.rollupBitmap(alloc, .{
                .key = item.key,
                .read_ts = oldest - 1,
            }, e.dest + 1) catch |err| switch (err) {
                // The chain below can't be folded (mixed or corrupt):
                // keep everything verbatim, reads keep folding.
                error.NotRoaring, error.CorruptPosting => {
                    log.warn(
                        "rollup: unfoldable chain below L{d} tree={d}; run " ++
                            "kept verbatim",
                        .{ e.dest, e.tree.tree_id },
                    );
                    return e.emitOne(item);
                },
                else => return err,
            };
        }

        var bm = foldChain(
            alloc,
            chain,
            if (external) |*eb| eb else null,
        ) catch |err| switch (err) {
            // Undecodable delta bytes (structure only — content was
            // validated at write time): keep the chain verbatim.
            error.CorruptPosting => {
                log.warn(
                    "rollup: undecodable roaring delta tree={d}; run kept " ++
                        "verbatim",
                    .{e.tree.tree_id},
                );
                return e.emitOne(item);
            },
            else => return err,
        };
        defer bm.deinit();
        // Canonical minimal buffer for persistence.
        try bm.compact();

        // The bitmap's buffer lives until this returns; emitOne copies
        // it into the builder during the call.
        const newest = chain.newest();
        const synthetic: Version = if (bm.isEmpty()) .{
            .ts = newest.ts,
            .meta = .{ .kind = .deleted, .encoding = .roaring },
            .value = "",
        } else .{
            .ts = newest.ts,
            .meta = .{ .kind = .state, .encoding = .roaring },
            .value = bm.toBuffer(),
        };
        utils.clearRetainCap(&t.rolled);
        try t.rolled.append(t.allocator, synthetic);
        try t.rolled.appendSlice(t.allocator, item.versions.slice()[1..]);
        try e.emitOne(.{
            .key = item.key,
            .versions = .{ .many = t.rolled.items },
        });
        e.stats.rollups += 1;
    }

    /// Fold `chain` into a bitmap — THE fold: compaction
    /// (`rollupRoaring`) and reads (MultiGet.bitmap through
    /// Tree.rollupBitmap) both resolve through `_read.baseBitmap` and
    /// `_read.applyDelta`, which is what keeps rollup read-invariant.
    /// Applies deltas oldest→newest (the chain is newest first),
    /// removes before adds within each. Base priority: a tombstone
    /// terminal folds from empty; a state terminal via fromBufferCopy
    /// (SST values sit at arbitrary offsets, so zroar's aligned
    /// zero-copy path is not usable); else `external` (compaction
    /// read-below) or the empty set. Delta bytes get an O(1) structural
    /// check — without it a truncated element region decodes as an
    /// empty list — but content contracts were enforced at write time
    /// (validateDelta).
    pub fn foldChain(
        alloc: std.mem.Allocator,
        chain: Item,
        external: ?*const zroar.Bitmap,
    ) !zroar.Bitmap {
        const vers = chain.versions.slice();
        const have_state = hasTerminal(chain);
        const terminal: ?Version = if (have_state) vers[vers.len - 1] else null;
        var bm = try _read.baseBitmap(alloc, terminal, external);
        errdefer bm.deinit();

        var i = vers.len - @intFromBool(have_state);
        while (i > 0) {
            i -= 1;
            try _read.applyDelta(&bm, vers[i].value);
        }
        return bm;
    }
};

/// Diagnostics. `keys_in - keys_out` is what this compaction discarded on
/// purpose (settled history below the horizon); 0 above the bottom level.
/// `stale_bytes` is kept-but-shadowed data, recorded per output table in
/// stale_data_size. A nonzero `expected_in - keys_in` is never expected —
/// it means the merge iterator lost rows. `bytes_*` count user data only
/// (key.primary + value), not framing. `rollups` counts roaring delta
/// chains folded into a synthetic state (so bytes_out > bytes_in is
/// expected when it's nonzero — the state is bigger than the delta it
/// replaces).
pub const Metrics = struct {
    keys_in: u64 = 0,
    keys_out: u64 = 0,
    bytes_in: u64 = 0,
    bytes_out: u64 = 0,
    dropped_bytes: u64 = 0,
    stale_bytes: u64 = 0,
    rollups: u64 = 0,
    src_keys: u64 = 0,
    src_bytes_disk: u64 = 0,
    bot_keys: u64 = 0,
    bot_bytes_disk: u64 = 0,

    /// Add `other`'s counts to this one's: every field is a sum, so a
    /// job's stats are its partitions' merged.
    pub fn merge(self: *Metrics, other: Metrics) void {
        inline for (@typeInfo(Metrics).@"struct".fields) |f| {
            @field(self, f.name) += @field(other, f.name);
        }
    }
};

/// The compaction output stage — emission mechanics only. Every
/// consumed item goes through `push` in merge order; `finish`
/// closes the stream and hands back what it wrote. Run awareness
/// (retention boundaries AND the roaring rollup) lives in the
/// VersionTracker, which calls back into `emitOne`: tracker update →
/// discard → file-split check → addItem → stale/stats accounting.
/// Consumption accounting (keys_in/bytes_in) happens in `push`. The
/// emitter owns the builder its tables come out of, the group their
/// syncs run in, and the stats of what it consumed and wrote.
pub const Emitter = struct {
    /// What `finish` hands back: a manifest CREATE per output table,
    /// at the emitter's level and in key order, and the stats of what
    /// it consumed and wrote. The changes are allocated from the
    /// tree's allocator.
    pub const Result = struct {
        changes: std.ArrayList(_manifest.SSTableChange),
        stats: Metrics,
    };

    tree: *Tree,
    /// The runtime's allocator counted on the tenant the job runs as:
    /// the tree's for a compaction, the shared one for a flush. Every
    /// buffer of the merge is on it; the result's changes are the
    /// tree's, since the caller frees them.
    allocator: std.mem.Allocator,
    /// The tunables when the job started: one snapshot for the whole
    /// merge, read per key by the rollup policy.
    opts: _config.Config,
    bld: Builder,
    /// One put fiber per table written; joined by `finish`.
    group: fiber.Group(?FlushResult),
    /// One seat per put in flight, `max_inflight` of them. A finished
    /// table sits in memory until its put returns, so `flush` takes a
    /// seat before spawning and parks when none is free instead of
    /// running ahead of a slow store; `putFile` gives it back.
    inflight: fiber.Channel(void),
    stats: Metrics = .{},
    /// An output table is cut once it reaches this. An L0 table has
    /// no limit: an L0→L0 merge writes one output, whatever it weighs,
    /// since a second would share its raft_index with the first.
    file_sz: usize,
    dest: usize,
    tracker: VersionTracker,

    pub fn init(
        tree: *Tree,
        dest: usize,
        snapshot_ts: u64,
    ) !Emitter {
        const opts = tree.live.cur();
        const cfg = _table.builder.Config{
            .block_bytes = opts.block_bytes,
            .codec = opts.codecFor(dest),
            .kind = tree.kind,
        };
        const allocator = fiber.allocator(.tenant);
        return .{
            .tree = tree,
            .allocator = allocator,
            .bld = Builder.init(allocator, cfg),
            .group = fiber.Group(?FlushResult).init(allocator),
            .inflight = try fiber.Channel(void).initAlloc(
                allocator,
                max_inflight,
            ),
            .file_sz = if (dest == 0)
                std.math.maxInt(usize)
            else
                opts.table_bytes,
            .dest = dest,
            .opts = opts,
            // Only the bottom level may discard history: lower
            // levels could still contain a version that an early
            // tombstone would otherwise resurrect.
            .tracker = VersionTracker.init(
                allocator,
                snapshot_ts,
                dest == @as(usize, opts.max_levels) - 1,
            ),
        };
    }

    /// Safe after `finish` and after an error: every put still
    /// running is joined before the builder goes.
    pub fn deinit(e: *Emitter) void {
        e.tracker.deinit();
        _ = e.group.wait();
        e.group.deinit();
        e.inflight.deinit();
        e.bld.deinit();
    }

    /// Finished tables an emitter holds while their puts run.
    const max_inflight = 3;

    /// Feed one item — one primary with its versions — in merge
    /// (consumption) order. Nothing is held across calls: the merge
    /// keeps the item's storage pinned until its own `next`.
    pub fn push(e: *Emitter, item: Item) !void {
        e.stats.keys_in += item.versions.len();
        for (item.versions.slice()) |ver| {
            e.stats.bytes_in += item.key.primary.len + ver.value.len;
        }
        try e.tracker.track(e, item);
    }

    /// Write the versions of `item` that retention keeps as one entry
    /// of the output.
    fn emitOne(e: *Emitter, item: Item) !void {
        const t = &e.tracker;
        t.startItem();
        utils.clearRetainCap(&t.kept);
        var stale_bytes: usize = 0;
        for (item.versions.slice()) |ver| {
            const nbytes = item.key.primary.len + ver.value.len;
            switch (t.retain(ver)) {
                .discard => {
                    e.stats.dropped_bytes += nbytes;
                    continue;
                },
                .stale => stale_bytes += nbytes,
                .retain => {},
            }
            try t.kept.append(t.allocator, ver);
            e.stats.keys_out += 1;
            e.stats.bytes_out += nbytes;
        }
        if (t.kept.items.len == 0) return;

        if (e.bld.table_buf.len >= e.file_sz) {
            // An item is a whole primary, so splitting the output here
            // never separates a primary's versions across tables. That
            // matters: a table holding a newer version of a key could
            // otherwise be compacted below a table holding an older
            // one, and the read path expects newer versions at higher
            // levels so it can stop at the first hit.
            try e.flush();
        }
        try e.bld.addItem(.{
            .key = item.key,
            .versions = .{ .many = t.kept.items },
        });
        if (stale_bytes > 0) {
            e.bld.addStaleBytes(stale_bytes);
            e.stats.stale_bytes += stale_bytes;
        }
    }

    /// The open table out: finished, its upload to the store spawned
    /// into the group; the builder is reset for the next. Nothing
    /// happens for an empty builder.
    pub fn flush(e: *Emitter) !void {
        var tb = try e.bld.finish();
        errdefer tb.deinit();
        defer e.bld.reset();
        if (tb.bytes.len == 0) {
            tb.deinit();
            return;
        }

        const tree = e.tree;
        const id = tree.manifest.newFileId();
        e.inflight.send({}) catch return error.FlushFailed;
        errdefer _ = e.inflight.tryReceive() catch {};
        // The fiber owns `tb` from here: the builder's next table is
        // built while this one uploads.
        try e.group.spawn(.{}, putFile, .{ e, tb, id });
    }

    /// Close the stream: the last table out, every sync joined, and
    /// what was written handed back as CREATEs in key order. A failed
    /// sync fails the whole job.
    pub fn finish(e: *Emitter) !Result {
        try e.flush();
        const tree = e.tree;
        var changes: std.ArrayList(_manifest.SSTableChange) = .empty;
        errdefer changes.deinit(tree.allocator);
        for (e.group.wait()) |r_opt| {
            const r = r_opt orelse return error.FlushFailed;
            try changes.append(tree.allocator, _manifest.createSSTableChange(
                r.file_id,
                tree.namespace,
                tree.tree_id,
                @intCast(e.dest),
                r.extent,
            ));
        }
        return .{ .changes = changes, .stats = e.stats };
    }
};

const FlushResult = struct { file_id: u64, extent: _table.Table.Extent };

/// Upload one finished table. put syncs the bytes; the caller's
/// `store.sync()` after the group covers the names.
fn putFile(
    e: *Emitter,
    tb_in: _table.builder.TableBuffer,
    file_id: u64,
) ?FlushResult {
    var tb = tb_in;
    defer tb.deinit();
    defer _ = e.inflight.tryReceive() catch {};
    const name = _table.table.sstName(tb.allocator, file_id) catch return null;
    defer tb.allocator.free(name);
    e.tree.cache.store.put(name, tb.bytes) catch |err| {
        log.err("put {s}: {s}", .{ name, @errorName(err) });
        return null;
    };
    return .{ .file_id = file_id, .extent = tb.extent() };
}
