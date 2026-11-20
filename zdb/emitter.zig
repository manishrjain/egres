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
const ItemRef = _table.table.ItemRef;
const Version = @import("kv.zig").Version;
const Builder = _table.builder.Builder;
const TreeKey = @import("kv.zig").TreeKey;
const Extent = _table.Table.Extent;

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
///    roaring delta, the chain down to its terminal (`Item.visibleAt`,
///    `.fold`) is folded by `rollupRoaring` into a synthetic state
///    (config triggers permitting) or emitted verbatim. Only that head
///    chain is ever folded: everything below its terminal is shadowed
///    history, and re-rolling it would regenerate a synthetic state
///    under the live terminal on every compaction until GC collapses
///    the chain.
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
    fn track(t: *VersionTracker, e: *Tree.Emitter, item: ItemRef) !void {
        if (foldable(e, item)) |chain| return t.rollupRoaring(e, item, &chain);
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
    fn foldable(e: *const Tree.Emitter, item: ItemRef) ?Item {
        if (!e.opts.rollup_enabled) return null;
        // An L0 to L0 merge folds nothing: a state it wrote would be
        // rewritten by the next push, and L0 would hold whole postings.
        if (e.dest == 0) return null;

        const newest = item.newest();
        if (!(newest.meta.isRoaring() and newest.meta.isDelta())) return null;

        // A rollup folds what is stored, under no snapshot: every
        // version is visible, so there is always a chain.
        const chain = item.visibleAt(std.math.maxInt(u64), .fold).?;
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
    fn hasTerminal(chain: ItemRef) bool {
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
        item: ItemRef,
        chain: ItemRef,
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
        try e.emitOne(&.{
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
        chain: ItemRef,
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
/// closes the stream and hands back the stats of what it consumed and
/// wrote. Run awareness (retention boundaries AND the roaring rollup)
/// lives in the VersionTracker, which calls back into `emitOne`:
/// tracker update → discard → table-split check → addItem →
/// stale/stats accounting. Consumption accounting (keys_in/bytes_in)
/// happens in `push`. The emitter owns the builder its tables come
/// out of; the tables go to its `PackObject`, which files them.
pub const Emitter = struct {
    tree: *Tree,
    /// The runtime's allocator counted on the tenant the job runs as:
    /// the tree's for a compaction, the shared one for a flush. Every
    /// buffer of the merge is on it.
    allocator: std.mem.Allocator,
    /// The tunables when the job started: one snapshot for the whole
    /// merge, read per key by the rollup policy.
    opts: _config.Config,
    bld: Builder,
    pack: *PackObject,
    stats: Metrics = .{},
    /// An output table is cut once it reaches this. An L0 table has
    /// no limit: an L0→L0 merge writes one output, whatever it weighs,
    /// since a second would share its raft_index with the first.
    file_sz: usize,
    dest: usize,
    tracker: VersionTracker,

    /// Writes `tree`'s tables at level `dest` into `pack`, which the
    /// caller keeps alive past `deinit`.
    pub fn init(
        tree: *Tree,
        dest: usize,
        snapshot_ts: u64,
        pack: *PackObject,
    ) !Emitter {
        const opts = tree.ground.live.cur();
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
            .pack = pack,
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

    pub fn deinit(e: *Emitter) void {
        e.tracker.deinit();
        e.bld.deinit();
    }

    /// Feed one item — one primary with its versions — in merge
    /// (consumption) order. Nothing is held across calls: the merge
    /// keeps the item's storage pinned until its own `next`.
    pub fn push(e: *Emitter, item: ItemRef) !void {
        e.stats.keys_in += item.versions.len();
        for (item.versions.slice()) |ver| {
            e.stats.bytes_in += item.key.primary.len + ver.value.len;
        }
        try e.tracker.track(e, item);
    }

    /// Write the versions of `item` that retention keeps as one entry
    /// of the output.
    fn emitOne(e: *Emitter, item: ItemRef) !void {
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

    /// The open table out: finished and handed to the pack; the
    /// builder is reset for the next. Nothing happens for an empty
    /// builder. The last call closes the stream; the pack's `finish`
    /// has the CREATEs once its puts are done, and `stats` says what
    /// was consumed and written.
    pub fn flush(e: *Emitter) !void {
        const tb = try e.bld.finish();
        e.bld.reset();
        try e.pack.add(.{
            .namespace = e.tree.namespace,
            .tree_id = e.tree.tree_id,
        }, tb);
    }
};

/// What a job hands back: the CREATEs of its pack and the stats of its
/// emitters.
pub const Result = struct {
    changes: std.ArrayList(_manifest.SSTableChange),
    stats: Metrics,
};

/// Where emitters' finished tables go: files in the store. Tables are
/// appended to the open file, each whole and page-aligned at its own
/// offset, until it holds `cut_bytes`; then it goes out on a fiber of
/// its own while the next fills, and `finish` waits for every put and
/// lists each table at its offset for its tree. The flush gives every
/// tree's emitter one pack, so a flush over thousands of trees is a few
/// files of the table size; a compaction's pack puts after every
/// table, so its files are its tables, as before.
pub const PackObject = struct {
    ground: *_tree.Ground,
    /// The job's allocator, as the emitters'.
    allocator: std.mem.Allocator,
    dest: usize,
    cut_bytes: usize,
    /// The open file: its tables as built, and whose each is.
    parts: std.ArrayList(_table.builder.TableBuffer) = .empty,
    entries: std.ArrayList(Entry) = .empty,
    len: u64 = 0,
    /// One put fiber per file written; joined by `finish`.
    group: fiber.Group(?Put),
    /// One seat per put in flight, `max_inflight` of them. A finished
    /// file sits in memory until its put returns, so `cutAndPut` takes a seat
    /// before spawning and parks when none is free instead of running
    /// ahead of a slow store; `putFile` gives it back.
    inflight: fiber.Channel(void),
    /// Set by `finish`, which takes every put's entries.
    finished: bool = false,

    /// One table of a file: the tree that lists it and where it sits.
    const Entry = struct { tree: TreeKey, extent: Extent };
    /// One file written, its tables in order.
    const Put = struct { file_id: u64, entries: []Entry };

    /// Finished files a pack holds while their puts run.
    const max_inflight = 3;

    /// A pack for level `dest` whose files are put once they hold
    /// `cut_bytes`; 0 puts each table as its own file.
    pub fn init(
        ground: *_tree.Ground,
        dest: usize,
        cut_bytes: usize,
    ) !PackObject {
        const allocator = fiber.allocator(.tenant);
        return .{
            .ground = ground,
            .allocator = allocator,
            .dest = dest,
            .cut_bytes = cut_bytes,
            .group = fiber.Group(?Put).init(allocator),
            .inflight = try fiber.Channel(void).initAlloc(
                allocator,
                max_inflight,
            ),
        };
    }

    /// Safe after `finish` and after an error: every put still
    /// running is joined before the pack goes, and the entries of puts
    /// `finish` did not take are freed here.
    pub fn deinit(p: *PackObject) void {
        const results = p.group.wait();
        if (!p.finished) for (results) |r_opt| {
            if (r_opt) |r| p.allocator.free(r.entries);
        };
        p.group.deinit();
        p.inflight.deinit();
        for (p.parts.items) |*tb| tb.deinit();
        p.parts.deinit(p.allocator);
        p.entries.deinit(p.allocator);
    }

    /// Takes `tb`, a table built for `tree`, into the open file; an
    /// empty one is dropped. The file goes out once it holds enough.
    pub fn add(
        p: *PackObject,
        tree: TreeKey,
        tb_in: _table.builder.TableBuffer,
    ) !void {
        var tb = tb_in;
        if (tb.bytes.len == 0) {
            tb.deinit();
            return;
        }
        const extent: Extent = .{
            .offset = p.len,
            .size = tb.bytes.len,
            .index_offset = tb.index_offset,
        };
        p.parts.append(p.allocator, tb) catch |err| {
            tb.deinit();
            return err;
        };
        try p.entries.append(p.allocator, .{ .tree = tree, .extent = extent });
        p.len += tb.bytes.len;
        if (p.len >= p.cut_bytes) try p.cutAndPut();
    }

    /// The open file ends here and goes to the store: its tables
    /// gathered into one buffer, a table alone taken as it is, and the
    /// put spawned into the group. Nothing happens for an empty file.
    pub fn cutAndPut(p: *PackObject) !void {
        if (p.parts.items.len == 0) return;
        defer {
            for (p.parts.items) |*tb| tb.deinit();
            p.parts.clearRetainingCapacity();
            p.len = 0;
        }

        var file = if (p.parts.items.len == 1)
            p.parts.pop().?
        else
            try p.gather();
        errdefer file.deinit();

        const entries = try p.entries.toOwnedSlice(p.allocator);
        errdefer p.allocator.free(entries);

        const id = p.ground.manifest.newFileId();
        p.inflight.send({}) catch return error.FlushFailed;
        errdefer _ = p.inflight.tryReceive() catch {};

        // The fiber owns `file` and `entries` from here: the next file
        // fills while this one uploads.
        try p.group.spawn(
            .{ .name = "put-file" },
            putFile,
            .{ p, file, id, entries },
        );
    }

    /// The open file's tables copied back to back into one
    /// page-aligned buffer, the address alignment O_DIRECT asks for.
    /// Each table is a page multiple, so each lands at its extent.
    fn gather(p: *PackObject) !_table.builder.TableBuffer {
        const bytes = try p.allocator.alignedAlloc(
            u8,
            vfs.page_alignment,
            @intCast(p.len),
        );
        var at: usize = 0;
        for (p.parts.items) |tb| {
            @memcpy(bytes[at..][0..tb.bytes.len], tb.bytes);
            at += tb.bytes.len;
        }
        return .{ .allocator = p.allocator, .bytes = bytes, .index_offset = 0 };
    }

    /// Close the pack: the open file out, every put joined, and what
    /// was written handed back as CREATEs, a table's for its tree at
    /// its extent, in write order. A failed put fails the whole job.
    pub fn finish(p: *PackObject) !std.ArrayList(_manifest.SSTableChange) {
        try p.cutAndPut();
        const arloc = p.ground.allocator;

        var changes: std.ArrayList(_manifest.SSTableChange) = .empty;
        errdefer changes.deinit(arloc);

        var failed = false;
        const results = p.group.wait();
        p.finished = true;
        for (results) |r_opt| {
            const r = r_opt orelse {
                failed = true;
                continue;
            };
            defer p.allocator.free(r.entries);
            for (r.entries) |en| {
                try changes.append(arloc, _manifest.createSSTableChange(
                    r.file_id,
                    en.tree.namespace,
                    en.tree.tree_id,
                    @intCast(p.dest),
                    en.extent,
                ));
            }
        }
        if (failed) return error.FlushFailed;
        return changes;
    }
};

/// Upload one finished file. put syncs the bytes; the caller's
/// `store.sync()` after the group covers the names. `entries` go back
/// in the result, or are freed on failure.
fn putFile(
    p: *PackObject,
    tb_in: _table.builder.TableBuffer,
    file_id: u64,
    entries: []PackObject.Entry,
) ?PackObject.Put {
    var tb = tb_in;
    defer tb.deinit();
    defer _ = p.inflight.tryReceive() catch {};
    const name = _table.table.sstName(tb.allocator, file_id) catch {
        p.allocator.free(entries);
        return null;
    };
    defer tb.allocator.free(name);
    const store = p.ground.cache.store;
    store.put(name, tb.bytes) catch |err| {
        log.err("put {s}: {s}", .{ name, @errorName(err) });
        p.allocator.free(entries);
        return null;
    };
    if (store.blockCache()) |cache| {
        for (entries) |en| admit(p, &tb, file_id, en.extent, cache);
    }
    return .{ .file_id = file_id, .entries = entries };
}

/// One written table's blocks into the block cache (`Table.admit`),
/// through a memory-backed Table over the file bytes just put: the
/// index and the decode are the reader's own, so what goes in is what
/// a read would have produced. Best effort: the file is durable either
/// way, and a table that does not fit is left to the reads.
fn admit(
    p: *PackObject,
    tb: *const _table.builder.TableBuffer,
    file_id: u64,
    extent: Extent,
    cache: *vfs.BlockCache,
) void {
    // No `buf_allocator`: the handle borrows `tb.bytes`, so its close
    // frees nothing and `tb.deinit` stays the owner.
    const handle: vfs.FileHandle = .{
        .file_size = tb.bytes.len,
        .owned_buf = tb.bytes,
        .vfs = vfs.g(),
        .bw = vfs.g().bw,
    };
    const obj = vfs.Object.init(p.ground.allocator, .{ .file = handle }) catch
        return;
    const t = _table.table.Table.openTable(
        p.ground.allocator,
        obj,
        extent,
    ) catch |err| {
        log.debug("admit {d}: open: {s}", .{ file_id, @errorName(err) });
        return;
    };
    defer t.decrRef();
    t.file_id = file_id;
    _ = t.admit(cache) catch |err| {
        log.debug("admit {d}: {s}", .{ file_id, @errorName(err) });
    };
}
