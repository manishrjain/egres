// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Forest's read paths: point lookups (`get`, `getRaw`, `getBitmap`),
//! the batch they are one-key calls of (`MultiGet`, `multiGet`), and
//! the range scan with its `Cursor` and `maxKey`. Every function here
//! takes a `*Forest` and leaves it unchanged; `Forest` aliases each
//! declaration back in, so callers keep using method syntax.

const std = @import("std");
const vfs = @import("vfs");
const zroar = @import("zroar");
const utils = @import("utils");
const String = utils.String;
const _level = @import("level.zig");
const _table = @import("table.zig");
const _tree = @import("tree.zig");
const _skl = @import("skl/skiplist.zig");
const DeltaReader = @import("buffers.gen.zig").RoaringDeltaReader;

const Forest = @import("forest.zig").Forest;
const kv = @import("kv.zig");
const Key = kv.Key;
const TableKey = kv.TableKey;
const KeyRange = kv.KeyRange;
const Level = _level.Level;
const Item = _table.table.Item;
const Table = _table.Table;
const TableIterator = _table.TableIterator;
const Tree = _tree.Tree;
const merge_iterator = _table.merge_iterator;
const Iterator = merge_iterator.Iterator;
const Pin = merge_iterator.Pin;

/// Many keys of one table at one read_ts, resolved together (see
/// `MultiGet`). The result owns its pins: read it through `item` /
/// `bitmap`, then `deinit`. `keys` need not be sorted; they are
/// borrowed until then. Rejects a read_ts below `snapshot_ts`,
/// whose versions may already have been compacted away.
pub fn multiGet(
    self: *Forest,
    allocator: std.mem.Allocator,
    table_key: TableKey,
    keys: []const String,
    opts: MultiGet.Options,
) !MultiGet {
    if (opts.read_ts < self.snapshotTs()) {
        return error.ReadBelowMinRead;
    }
    var mg = try MultiGet.init(allocator, table_key, keys, opts);
    errdefer mg.deinit();
    try fillMultiGet(self, &mg);
    return mg;
}

/// The walk: the memtables newest first (the txn's pending writes,
/// when given, then the active memtable, then the immutables), each
/// probed for the keys still open, then each tree's levels for the
/// keys still pending. No merge over the memtables: a newer source
/// settles a key before an older one is asked, and the node's maxTs
/// keeps an older source from re-supplying a version the newer one
/// already gave (the same rule the L0 walk uses). Memtable refs are
/// taken BEFORE the tree lookup: data flushed in between is then
/// visible in both, and maxTs skips the L0 duplicate. The reverse
/// order could miss it in both.
fn fillMultiGet(self: *Forest, mg: *MultiGet) !void {
    const alloc = mg.allocator;
    {
        // One usable version per key; deltas fold down to their state.
        const srcs = try self.memtables.getIterators(
            alloc,
            .{ .pending = mg.opts.pending },
            .{ .read_ts = mg.opts.read_ts, .want = 1 },
        );
        defer {
            for (srcs) |*it| it.deinit(); // releases each source's skiplist ref
            alloc.free(srcs);
        }
        for (srcs) |*it| {
            for (mg.pending.items) |node| {
                if (node.done()) continue;
                try mg.takeVersions(node, it);
            }
            MultiGet.removeDone(&mg.pending);
            if (mg.pending.items.len == 0) break;
        }
    }

    // Keys ascend, so a tree covers a contiguous run of the pending
    // nodes; the run ends at the first node another tree owns.
    const trees = self.getTable(mg.table_key) orelse return;
    const pend = mg.pending.items;
    var i: usize = 0;
    while (i < pend.len) {
        const first = mg.keyAt(pend[i]);
        const tree = trees.getTreeByKey(first) orelse {
            i += 1;
            continue;
        };
        defer tree.release();

        var j = i + 1;
        while (j < pend.len) : (j += 1) {
            const key = mg.keyAt(pend[j]);
            const owner = trees.getTreeByKey(key) orelse break;
            defer owner.release();

            if (owner.tree_id != tree.tree_id) break;
        }
        try mg.walkTree(tree, pend[i..j], 0);
        i = j;
    }
    MultiGet.removeDone(&mg.pending);
}

/// One point read: the row and the timestamp it is read as of.
pub const Lookup = struct {
    key: Key,
    /// Versions above this are not visible. The default reads the
    /// latest.
    read_ts: u64 = std.math.maxInt(u64),
};

/// Get a value by key: the newest version at or below `read_ts`, from
/// the memtables (current, then immutables newest-to-oldest) or the
/// owning tree. Returned Item is owned by the caller; call
/// item.free(allocator) when done.
///
/// A key deleted at or below `read_ts` reads as absent: the tombstone
/// wins the lookup and is reported as null. Rejects reads below
/// `snapshot_ts`, whose versions may already have been compacted away.
pub fn get(self: *Forest, allocator: std.mem.Allocator, lookup: Lookup) !?Item {
    if (lookup.read_ts < self.snapshotTs()) {
        return error.ReadBelowMinRead;
    }
    return getOne(self, allocator, lookup, false);
}

/// get without the tombstone filter or the horizon check: returns the
/// winning version verbatim, tombstone or not. For callers that need to
/// see deletions as data (compaction diagnostics, tests, repair tools)
/// rather than as absence.
pub fn getRaw(
    self: *Forest,
    allocator: std.mem.Allocator,
    lookup: Lookup,
) !?Item {
    return getOne(self, allocator, lookup, true);
}

/// A one-key row MultiGet, its item duped out with `allocator`.
fn getOne(
    self: *Forest,
    allocator: std.mem.Allocator,
    lookup: Lookup,
    include_deleted: bool,
) !?Item {
    const keys = [_]String{lookup.key.primary};
    var mg = try MultiGet.init(
        allocator,
        lookup.key.tableKey(),
        &keys,
        .{
            .read_ts = lookup.read_ts,
            .include_deleted = include_deleted,
        },
    );
    defer mg.deinit();
    try fillMultiGet(self, &mg);
    const found = mg.item(lookup.key.primary) orelse return null;
    return try found.dupe(allocator);
}

/// The posting bitmap for `lookup.key` as of `lookup.read_ts`.
/// A one-key posting MultiGet: the memtables, then the owning tree's
/// levels, stopping at the first terminal (a state or a tombstone).
/// After compaction rollup the newest version is usually already a
/// state, so the walk rarely descends past the level that holds it.
///
/// An absent key, a tombstoned key, and an empty posting all return
/// an EMPTY bitmap — an index reader cannot distinguish them and
/// should not try. The bitmap is owned by `allocator`; deinit it.
///
/// Fails with CorruptPosting on undecodable delta bytes and
/// NotRoaring if the chain contains a non-roaring version —
/// getBitmap is only meaningful on keys written as roaring postings.
pub fn getBitmap(
    self: *Forest,
    allocator: std.mem.Allocator,
    lookup: Lookup,
) !zroar.Bitmap {
    if (lookup.read_ts < self.snapshotTs()) {
        return error.ReadBelowMinRead;
    }
    const keys = [_]String{lookup.key.primary};
    var mg = try MultiGet.init(
        allocator,
        lookup.key.tableKey(),
        &keys,
        .{ .read_ts = lookup.read_ts },
    );
    defer mg.deinit();
    try fillMultiGet(self, &mg);
    return mg.bitmap(allocator, lookup.key.primary);
}

/// Many keys of one table, resolved together: for each, its versions
/// at or below read_ts, newest first, down to the first that is not
/// a delta (a state or a tombstone), which closes the chain. A row is
/// one version; a posting is its deltas over a state, folded into a
/// bitmap by `bitmap`. The keys are kept as nodes sorted by key (duplicates
/// collapse), so a table's share of them is one contiguous slice
/// found by binary search and consecutive keys land in the same
/// block. The sources are walked newest first — the txn's pending
/// writes, then each memtable (`fillMultiGet`), then each tree's
/// levels from the top (`walkTree`) — and at every level
/// only the tables whose range and bloom filter admit a key that is
/// still pending are probed. A key leaves `pending` as soon as its
/// versions settle it, so lower levels cost only what is still open.
/// Versions are borrowed under pins, never copied. The key slices are
/// borrowed from the caller for the MultiGet's lifetime; the nodes
/// and versions come from one arena, and `deinit` releases every pin.
pub const MultiGet = struct {
    /// The caller's allocator: iterators and the per-level table
    /// slices, which come and go during the walk.
    allocator: std.mem.Allocator,
    /// Nodes, versions and the pending list: freed together.
    arena: std.heap.ArenaAllocator,
    opts: Options,
    table_key: TableKey,
    nodes: []Node,
    /// The nodes not yet done, in key order. Rebuilt from itself
    /// after every source, so each source sees only what the ones
    /// above it left open; empty ends the walk.
    pending: std.ArrayList(*Node),
    /// Scratch for `probeTable`: the nodes one table is seeked for,
    /// and the blocks those seeks land in.
    accepted: std.ArrayList(*Node) = .empty,
    blocks: std.ArrayList(usize) = .empty,

    pub const Options = struct {
        /// Versions above this are not visible. Must be at or above
        /// `Forest.snapshotTs` for the entry points that check it.
        read_ts: u64 = std.math.maxInt(u64),
        /// Rows only: a txn's private writes, the first source (src 0
        /// of the memtable merge, as in ScanOptions.pending), so a
        /// pending write wins over every committed version and a
        /// pending tombstone reads as absent with no fallback. Not
        /// for postings: their pending ops are per-egid entries the
        /// sql layer overlays itself (Txn.overlayPostings).
        pending: ?*_skl.SkipList = null,
        /// Rows only: hand back a tombstone version as an item
        /// (getRaw).
        include_deleted: bool = false,
    };

    /// One key of the batch and the versions found for it so far,
    /// newest first: sources are walked newest first and each hands
    /// over a newest-first slice. The values are borrowed from the
    /// storage `pins` hold.
    pub const Node = struct {
        key: String,
        /// kv.bloomHash(key), once per key: every table probe reuses it.
        hash: u64,
        versions: std.ArrayList(kv.Version) = .empty,
        pins: std.ArrayList(Pin) = .empty,

        /// Versions above this are not wanted from the next source:
        /// the oldest taken ts - 1 once something was taken, so the
        /// next source down starts strictly below what was consumed (a
        /// version that re-surfaces at a lower source after a flush
        /// or compaction arrives above it and is skipped); read_ts
        /// before that.
        pub fn maxTs(self: *const Node, read_ts: u64) u64 {
            if (self.versions.items.len == 0) return read_ts;
            const oldest = self.versions.getLast();
            // Real HLC timestamps are never 0: no wrap.
            std.debug.assert(oldest.ts > 0);
            return oldest.ts - 1;
        }

        /// Nothing more is wanted: the oldest taken is a state or a
        /// tombstone, which closes the chain. A delta keeps it open.
        pub fn done(self: *const Node) bool {
            if (self.versions.items.len == 0) return false;
            return !self.versions.getLast().meta.isDelta();
        }

        fn lessByKey(_: void, a: Node, b: Node) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    };

    pub fn init(
        allocator: std.mem.Allocator,
        table_key: TableKey,
        keys: []const String,
        opts: Options,
    ) !MultiGet {
        var arena = std.heap.ArenaAllocator.init(allocator);
        errdefer arena.deinit();
        const arloc = arena.allocator();

        const all = try arloc.alloc(Node, keys.len);
        for (keys, all) |key, *node| {
            node.* = .{
                .key = key,
                .hash = kv.bloomHash(key),
            };
        }
        // Unstable is fine (equal keys are interchangeable) and, unlike
        // `mem.sort`, has no 512-element scratch cache: in a safe build
        // that cache is poison-filled on every call, which was a third of
        // the CPU on a point-read path that sorts one key. A single key
        // needs no sort at all.
        if (all.len > 1) std.mem.sortUnstable(Node, all, {}, Node.lessByKey);
        // Collapse duplicates: the sort made them adjacent.
        var kept: usize = 0;
        for (all) |node| {
            if (kept > 0 and std.mem.eql(u8, all[kept - 1].key, node.key)) {
                continue;
            }
            all[kept] = node;
            kept += 1;
        }
        const nodes = all[0..kept];

        var pending: std.ArrayList(*Node) = .empty;
        try pending.ensureTotalCapacityPrecise(arloc, nodes.len);
        for (nodes) |*node| pending.appendAssumeCapacity(node);

        return .{
            .allocator = allocator,
            .arena = arena,
            .opts = opts,
            .table_key = table_key,
            .nodes = nodes,
            .pending = pending,
        };
    }

    /// Drops every pin and every borrow handed out.
    pub fn deinit(self: *MultiGet) void {
        for (self.nodes) |node| {
            for (node.pins.items) |pin| pin.release();
        }
        self.arena.deinit();
    }

    /// Row result: `key`'s versions at or below read_ts, newest first,
    /// null if absent or deleted (unless include_deleted). The values
    /// are borrows valid until deinit. `key` must be one of the batch's
    /// keys. A row's chain is one version; a chain of deltas starts at
    /// the newest delta, the version `get` has always returned.
    pub fn item(self: *const MultiGet, key: String) ?Item {
        const node = self.find(key);
        const vers = node.versions.items;
        if (vers.len == 0) return null;
        if (vers[0].meta.isDeleted() and !self.opts.include_deleted) {
            return null;
        }
        return .{ .key = self.keyAt(node), .versions = .{ .many = vers } };
    }

    /// Posting result: the chain of `key` folded into a bitmap owned
    /// by `alloc`. Absent, tombstoned and empty all fold to an empty
    /// bitmap. `key` must be one of the batch's keys. Fails with
    /// NotRoaring when a version of the chain is not roaring (a
    /// tombstone's encoding is not looked at, as in compaction), and
    /// CorruptPosting on undecodable delta bytes.
    pub fn bitmap(
        self: *const MultiGet,
        alloc: std.mem.Allocator,
        key: String,
    ) !zroar.Bitmap {
        // Pending posting ops are the sql layer's to overlay.
        std.debug.assert(self.opts.pending == null);
        return foldBitmap(alloc, self.find(key).versions.items);
    }

    /// Whether `key`'s chain has any version at all (rollupBitmap's
    /// null result).
    pub fn hasVersions(self: *const MultiGet, key: String) bool {
        return self.find(key).versions.items.len > 0;
    }

    /// The node of a key that was passed to init.
    fn find(self: *const MultiGet, key: String) *Node {
        const idx = utils.search(Node, self.nodes, key, struct {
            fn f(k: String, node: Node) bool {
                return std.mem.order(u8, node.key, k) != .lt;
            }
        }.f);
        std.debug.assert(idx < self.nodes.len);
        std.debug.assert(std.mem.eql(u8, self.nodes[idx].key, key));
        return &self.nodes[idx];
    }

    /// `node`'s key as a full Key.
    pub fn keyAt(self: *const MultiGet, node: *const Node) Key {
        return .{
            .namespace = self.table_key.namespace,
            .table_id = self.table_key.table_id,
            .primary = node.key,
        };
    }

    /// Drop the nodes that are done from `pending`.
    pub fn removeDone(pending: *std.ArrayList(*Node)) void {
        var kept: usize = 0;
        for (pending.items) |node| {
            if (node.done()) continue;
            pending.items[kept] = node;
            kept += 1;
        }
        pending.items.len = kept;
    }

    /// The level loop over one tree for `run`, pending nodes in key
    /// order that this tree covers. Per level from `from_level`,
    /// under one shared hold of `tree.mu`, pin the tables that can
    /// hold the nodes still open: on L0 every table, since their
    /// ranges overlap, walked newest to oldest; deeper, the tables
    /// overlapping [first open, last open]. Then per table, with the
    /// hold released, probe its share (see `probeTable`). A level
    /// with nothing open ends the walk.
    pub fn walkTree(
        self: *MultiGet,
        tree: *Tree,
        run: []const *Node,
        from_level: usize,
    ) !void {
        const arloc = self.arena.allocator();
        var pending: std.ArrayList(*Node) = .empty;
        try pending.appendSlice(arloc, run);

        const max_levels = tree.live.cur().max_levels;
        var lvl = from_level;
        while (lvl < max_levels) : (lvl += 1) {
            removeDone(&pending);
            const open = pending.items;
            if (open.len == 0) return;

            const range = KeyRange{
                .left = self.keyAt(open[0]),
                .right = self.keyAt(open[open.len - 1]),
            };
            const tables = blk: {
                const held = tree.mu.hold(.shared, @src());
                defer held.release();
                break :blk try tree.levels[lvl].overlappingTables(
                    held,
                    self.allocator,
                    range,
                );
            };
            defer Level.releaseTables(self.allocator, tables);

            if (lvl == 0) {
                // L0 sorts by raft_index ascending (newest = highest
                // raft_index, last in the list): walk in reverse so the
                // newest table settles a key first.
                var ri = tables.len;
                while (ri > 0) {
                    ri -= 1;
                    try self.probeTable(tables[ri].table, open);
                }
            } else {
                for (tables) |e| try self.probeTable(e.table, open);
            }
        }
    }

    /// One table's share of the open nodes: those inside
    /// [smallest, biggest] (a binary search on the list) whose hash
    /// the table's filter accepts, skipping any a newer L0 table
    /// settled this level. None → the table is not opened. Else the
    /// blocks their seeks land in are prefetched, and one iterator is
    /// seeked per node in key order, so a run of keys in one block is
    /// one in-block search each.
    fn probeTable(
        self: *MultiGet,
        tbl: *Table,
        nodes: []const *Node,
    ) !void {
        const lo = utils.search(*Node, nodes, tbl.smallest.primary, struct {
            fn f(smallest: String, node: *Node) bool {
                return std.mem.order(u8, node.key, smallest) != .lt;
            }
        }.f);
        const hi = utils.search(*Node, nodes, tbl.biggest.primary, struct {
            fn f(biggest: String, node: *Node) bool {
                return std.mem.order(u8, node.key, biggest) == .gt;
            }
        }.f);

        const arloc = self.arena.allocator();
        utils.clearRetainCap(&self.accepted);
        for (nodes[lo..hi]) |node| {
            if (node.done()) continue;
            if (!tbl.containsPrimaryHash(node.key, node.hash)) continue;
            try self.accepted.append(arloc, node);
        }
        if (self.accepted.items.len == 0) return;
        try self.prefetch(tbl);

        var it: Iterator = .{ .table = TableIterator.init(
            self.allocator,
            tbl,
            self.table_key,
        ) };
        defer it.deinit();
        for (self.accepted.items) |node| {
            try self.takeVersions(node, &it);
        }
    }

    /// Before a table's seeks: the blocks they will land in, read in
    /// one batch into the block cache when there is one, so each seek
    /// then hits. Keys ascend, so the block indices do too and a repeat
    /// is the previous entry.
    fn prefetch(self: *MultiGet, tbl: *Table) !void {
        const cache = _table.table.block_cache orelse return;
        const arloc = self.arena.allocator();
        utils.clearRetainCap(&self.blocks);
        for (self.accepted.items) |node| {
            const idx = tbl.blockFor(self.keyAt(node)) orelse return;
            if (self.blocks.items.len > 0 and self.blocks.getLast() == idx) {
                continue;
            }
            try self.blocks.append(arloc, idx);
        }
        _ = try tbl.prefetchBlocks(
            cache,
            self.allocator,
            self.blocks.items,
        );
    }

    /// Take `node`'s versions from one positioned source, newest first,
    /// until the node is done or the source leaves the key. The seek is
    /// unbounded and lands on the key's newest version; of each item
    /// the versions at or below what the node still wants are taken,
    /// down to the first terminal. The key check ends the walk, and it
    /// compares whole keys because the memtable sources span
    /// namespaces and tables. A source that yields one version per
    /// item (an SST until it packs) is walked item by item; a merged
    /// source hands the whole key over in one item.
    fn takeVersions(self: *MultiGet, node: *Node, it: *Iterator) !void {
        const read_ts = self.opts.read_ts;
        const start = self.keyAt(node);

        // Bound the seek to this one key. An unbounded seek that misses
        // lands on the next key in order, and the memtable iterator
        // copies that key's whole version chain before the key compare
        // below can reject it; after a table's last row that neighbour
        // is a hot row of the next table with thousands of versions.
        try it.seekRange(start, start.after());
        while (it.itemRef()) |found| {
            if (found.key.order(start) != .eq) return;
            if (found.visibleAt(node.maxTs(read_ts))) |vis| {
                var buf: [1]Pin = undefined;
                try self.take(node, vis.chain(), it.currentPins(&buf));
                if (node.done()) return;
            }
            try it.next();
        }
    }

    /// Append `wanted`'s versions, holding the storage behind them.
    fn take(
        self: *MultiGet,
        node: *Node,
        wanted: Item,
        pins: []const Pin,
    ) !void {
        const arloc = self.arena.allocator();
        try node.versions.appendSlice(arloc, wanted.versions.slice());
        for (pins) |pin| try node.pins.append(arloc, pin.acquire());
    }
};

/// The bitmap a roaring chain folds from: empty for a tombstone
/// terminal, the state's bitmap for a state terminal (copied: SST
/// values sit at arbitrary offsets, so zroar's aligned zero-copy path
/// is not usable), else `external` cloned (compaction's read-below),
/// else empty. Reads (`MultiGet.bitmap`) and compaction
/// (`VersionTracker.foldChain`) both fold through this
/// and `applyDelta`, which is what keeps rollup read-invariant.
/// A posting chain, newest first, folded into a bitmap owned by
/// `alloc`: the oldest version is the base when it is a state or a
/// tombstone, else the chain folds from empty; the deltas apply oldest
/// first. NotRoaring when a version is not roaring (a tombstone's
/// encoding is not looked at, as in compaction).
pub fn foldBitmap(
    alloc: std.mem.Allocator,
    vers: []const kv.Version,
) !zroar.Bitmap {
    var terminal: ?kv.Version = null;
    var deltas = vers;
    if (vers.len > 0) {
        const oldest = vers[vers.len - 1];
        if (!oldest.meta.isDeleted() and !oldest.meta.isRoaring()) {
            return error.NotRoaring;
        }
        if (!oldest.meta.isDelta()) {
            terminal = oldest;
            deltas = vers[0 .. vers.len - 1];
        }
    }
    var bm = try baseBitmap(alloc, terminal, null);
    errdefer bm.deinit();
    var i = deltas.len;
    while (i > 0) {
        i -= 1;
        if (!deltas[i].meta.isRoaring()) return error.NotRoaring;
        try applyDelta(&bm, deltas[i].value);
    }
    return bm;
}

pub fn baseBitmap(
    alloc: std.mem.Allocator,
    terminal: ?kv.Version,
    external: ?*const zroar.Bitmap,
) !zroar.Bitmap {
    if (terminal) |term| {
        if (term.meta.isDeleted()) return zroar.Bitmap.init(alloc);
        return zroar.Bitmap.fromBufferCopy(alloc, term.value);
    }
    if (external) |eb| return eb.clone();
    return zroar.Bitmap.init(alloc);
}

/// Apply one roaring delta: removes, then adds. The bytes get an O(1)
/// structural check — without it a truncated element region decodes
/// as an empty list — but content contracts were enforced at write
/// time (validateDelta). Deltas are applied oldest to newest.
pub fn applyDelta(bm: *zroar.Bitmap, delta: String) !void {
    const r = DeltaReader.init(delta, 0) catch return error.CorruptPosting;
    r.verify() catch return error.CorruptPosting;
    var j: u32 = 0;
    while (j < r.removesLen()) : (j += 1)
        _ = bm.remove(r.removesAt(j) catch unreachable);
    j = 0;
    while (j < r.addsLen()) : (j += 1)
        _ = try bm.set(r.addsAt(j) catch unreachable);
}

/// How much of a key's version history a scan should surface.
///
/// The merge iterator hands us one item per primary with every version
/// of it, newest first; these decide which of them reach the callback.
/// The defaults make a scan agree with `get`: the current value of each
/// live key, and nothing for a deleted one.
pub const ScanOptions = struct {
    /// Stop after handing this many items — one per key — to the
    /// callback. Bounds what the caller RECEIVES, not what the scan walks
    /// past: keys the filters below skip are free. 0 returns nothing. The
    /// default is unbounded — the whole range.
    max_rows: u64 = std.math.maxInt(u64),
    /// Max versions in each emitted item, newest first. 1 (the default)
    /// is "just the current value". Clamped up to 1 — there's no use for
    /// a scan that returns zero versions of every key, and `max_rows = 0`
    /// already says "return nothing".
    num_versions: u32 = 1,
    /// When false, a primary stops emitting at its first tombstone: the
    /// tombstone is not emitted, and neither is anything older than it,
    /// regardless of `num_versions`. That is what makes a deleted key look
    /// absent.
    ///
    /// When true, tombstones are emitted like any other version and older
    /// versions behind them keep coming, up to `num_versions`. For callers
    /// that need deletions as data — replication, repair, debugging.
    include_deleted: bool = false,
    /// Report the world as of this timestamp: versions above it are not
    /// visible and are skipped as though they had not been written. The
    /// default, maxInt, means "latest".
    ///
    /// This is the same view `get` gives for one key, so a scan at
    /// `read_ts` and a `get` at `read_ts` agree — including on deletion: a
    /// tombstone ABOVE read_ts hasn't happened yet from here, so it does
    /// not hide the key, and the live version under it is what surfaces.
    ///
    /// Must be at or above `snapshot_ts`, like any other read.
    read_ts: u64 = std.math.maxInt(u64),
    /// Only what changed after this timestamp: versions at or below it
    /// are skipped, so a key is emitted only when its newest visible
    /// version is above it, and an SST whose every version is at or
    /// below it (`TableIndex.max_version`) is not opened at all. The
    /// default, 0, skips nothing. With `include_deleted`, a key deleted
    /// after `min_ts` is emitted as its tombstone — the shape of a
    /// "what changed since" pass (CREATE INDEX's merge of the rows
    /// written during its backfill). Memtables are always opened.
    min_ts: u64 = 0,
    /// A transaction's private pending-write skiplist, merged in as
    /// the highest-priority source (src 0) so the txn sees its own
    /// uncommitted writes. Its entries carry ts = read_ts, which the
    /// visibility filter admits and which wins equal-key ties against
    /// committed versions (lower src wins in the merge). null = none.
    pending: ?*_skl.SkipList = null,
    /// Cursor only. Off: an item's value is a borrow valid until the
    /// following `next`. On: the cursor holds the storage behind
    /// every value it hands out until `Cursor.releasePins`, so a
    /// caller can keep a whole batch of borrows instead of copying
    /// them. Costs one refcount pair per block or memtable arena
    /// walked, and keeps those blocks resident until released.
    pin_values: bool = false,
};

/// Forward scan over `range` within the table `table_key`. The bounds
/// name primaries of that table, or are `Key.inf` for no bound on that
/// side (`KeyRange.all` for the whole table). The cursor confines the
/// range to the table (`KeyRange.limitToTable`), which keeps the scan
/// from spilling across the namespace-wide memtable into an adjacent
/// table.
///
/// `opts` carries everything else: how many items to return, the view
/// (as-of timestamp, versions per key, deletions as data or absence), and
/// see `ScanOptions`. The defaults emit every live
/// key's current value once and hide deleted keys, matching `get`.
///
/// `scan_range`'s own timestamps only position the seek — `opts.read_ts`
/// is what decides visibility.
///
/// A loop over a `Cursor`: the callback sees each item the cursor hands
/// out, in order, and stops the scan by returning false. Returns how many
/// items reached the callback (the one it said false to included). The
/// item is a borrow — see `Cursor.next`. Instantiated per ctx type, but
/// the walk itself lives in `Cursor` and compiles once.
pub fn scan(
    self: *Forest,
    allocator: std.mem.Allocator,
    table_key: TableKey,
    range: KeyRange,
    opts: ScanOptions,
    ctx: anytype,
    comptime cb: fn (@TypeOf(ctx), Item) anyerror!bool,
) !u64 {
    var cur = try self.openCursor(allocator, table_key, range, opts);
    defer cur.deinit();
    while (try cur.next()) |item| {
        if (!try cb(ctx, item)) break;
    }
    return cur.emitted;
}

/// Open a `Cursor` over `scan_range` — the walk `scan` does, handed out
/// one item per `next` so the caller sets the pace. Same arguments and
/// guards as `scan`: a `read_ts` below `snapshotTs` is refused here,
/// except when `max_rows` is 0, which opens an already-exhausted cursor
/// without looking. Nothing is read or pinned until the first `next`.
pub fn openCursor(
    self: *Forest,
    allocator: std.mem.Allocator,
    table_key: TableKey,
    range: KeyRange,
    opts: ScanOptions,
) !Cursor {
    if (opts.max_rows != 0 and opts.read_ts < self.snapshotTs()) {
        return error.ReadBelowMinRead;
    }
    return .{
        .forest = self,
        .allocator = allocator,
        .arena = std.heap.ArenaAllocator.init(allocator),
        // Confined to the table here, so the memtable walk, which holds
        // every table's keys, never sees an unbounded side.
        .key_range = range.limitToTable(table_key),
        .opts = opts,
        .want_versions = @max(opts.num_versions, 1),
        .table_key = table_key,
    };
}

/// A scan as a pull handle: `next` returns the next visible item, null
/// once the range is exhausted. Same walk, filters, and options as
/// `scan` — that function is a loop over one of these. What the cursor
/// adds is pacing: a caller can take some items, return to whoever
/// called it, and come back later, with the open tree's merge iterator
/// and its refs (SST tables, memtable arenas, the pending skiplist)
/// held in between. Holding across a wait is safe — the refs keep that
/// storage alive even if a flush or compaction has since replaced it in
/// the manifest — so the cost is residency, not correctness.
///
/// One tree at a time: look up the tree owning `key_range.left`,
/// snapshot its range, open a merge over the intersection (with a fresh
/// memtable iter snapshot), drain it, then move `key_range.left` past
/// the snapshotted right boundary and re-look-up. Re-looking-up per hop
/// keeps the scan safe against splits firing mid-scan. Per-hop memtable
/// snapshots also pick up newly-flushed data.
///
/// One fiber drives a cursor. `deinit` on every path — an abandoned
/// cursor pins its tables until then.
pub const Cursor = struct {
    forest: *Forest,
    allocator: std.mem.Allocator,
    /// Per-cursor arena: memtable iterator slices and each visited
    /// tree's duped range live exactly as long as the cursor (the tree
    /// ref is released once the hop's merge is open, and a split
    /// retires the tree, so the range cannot borrow from it).
    arena: std.heap.ArenaAllocator,
    /// The window still to visit: the scan's range at open, confined
    /// to the table, its left bound advanced past each drained tree's
    /// snapshotted right edge.
    key_range: KeyRange,
    opts: ScanOptions,
    /// `opts.num_versions` clamped up to 1 (see ScanOptions).
    want_versions: u32,
    table_key: TableKey,
    /// The open tree's merge; null between trees (a hop is pending)
    /// and once `done`.
    merge: ?merge_iterator.MergeIterator = null,
    /// The open tree's range as snapshotted at its hop (tree.key_range
    /// stays in sync with the manifest via applySplit).
    tree_range: KeyRange = .{},
    /// The merge is parked on an item the caller already has. Advancing
    /// right after the hand-out would overwrite the key buffer and could
    /// drop the block the value lives in, so the step past it is owed to
    /// the following `next`. False only before the first hand-out from a
    /// freshly opened tree.
    next_pending: bool = false,
    /// With `opts.pin_values`: a hold on the storage behind every
    /// value handed out since the last `releasePins` — one entry per
    /// block or memtable arena, since consecutive items share one —
    /// so those borrows stay valid past the following `next`.
    pins: std.ArrayList(Pin) = .empty,
    /// No tree left to visit.
    done: bool = false,
    /// Items handed out so far — what `scan` returns. Bounded by
    /// `opts.max_rows`; keys the filters skip are free.
    emitted: u64 = 0,

    /// Releases the open merge (table + skiplist refs) and the cursor's
    /// memory. Safe after exhaustion, and after an error from `next`.
    pub fn deinit(self: *Cursor) void {
        for (self.pins.items) |pin| pin.release();
        self.pins.deinit(self.allocator);
        if (self.merge) |*m| m.deinit();
        self.merge = null;
        self.arena.deinit();
    }

    /// The next item the options admit, or null when the scan is done
    /// (`max_rows` reached, or the range walked to its end). Null is
    /// sticky.
    ///
    /// The returned Item is a BORROW valid until the following `next`
    /// or `deinit`: its key and versions point into the merge's
    /// buffers, its values into blocks or arenas the merge holds refs
    /// on, and the advance overwrites the ones and may release the
    /// others. Copy what must outlive the call — except the values
    /// under `opts.pin_values`, which stay valid until `releasePins`.
    pub fn next(self: *Cursor) !?Item {
        while (self.emitted < self.opts.max_rows) {
            if (self.merge == null) {
                if (self.done) return null;

                try self.openNextTree();
                // nothing covers key_range.left
                if (self.merge == null) return null;
            }
            const merge = &self.merge.?;
            if (self.next_pending) {
                self.next_pending = false;
                try merge.next();
            }
            if (try self.nextVisible(merge)) |item| {
                // The merge stays parked on `item` so the borrow holds;
                // the step past it is owed to the next call.
                self.next_pending = true;
                if (self.opts.pin_values) try self.pinCurrent(merge);
                return item;
            }

            // This tree is drained: close it and move the window past
            // its snapshotted right edge. An unbounded right is the
            // rightmost tree → done.
            merge.deinit();
            self.merge = null;
            if (self.tree_range.right.isInf()) {
                self.done = true;
                return null;
            }
            self.key_range.left = self.tree_range.right;
        }
        return null;
    }

    /// Hold the storage behind the parked item's values, one pin per
    /// source it drew from, unless the last pin already covers it
    /// (Pin's doc has the stability guarantee a hold gives).
    fn pinCurrent(
        self: *Cursor,
        merge: *merge_iterator.MergeIterator,
    ) !void {
        for (merge.currentPins()) |pin| {
            const covered = self.pins.items.len > 0 and
                std.meta.eql(self.pins.getLast(), pin);
            if (!covered) try self.pins.append(self.allocator, pin.acquire());
        }
    }

    /// The caller is done with the values handed out so far: drop
    /// their holds — except the item most recently handed out, which
    /// the merge is still parked on and the caller may still be
    /// holding; it stays pinned. No-op unless `opts.pin_values`.
    pub fn releasePins(self: *Cursor) !void {
        for (self.pins.items) |pin| pin.release();
        utils.clearRetainCap(&self.pins);
        if (self.opts.pin_values and self.next_pending) {
            for (self.merge.?.currentPins()) |pin| {
                try self.pins.append(self.allocator, pin.acquire());
            }
        }
    }

    /// The item `next` just handed out, read as a posting: its chain
    /// folded by `foldBitmap`. The cursor must carry the whole chain,
    /// `num_versions` at its maximum, and no `pending`: a txn's pending
    /// posting ops have their own encoding and are the sql layer's to
    /// overlay.
    pub fn bitmap(
        self: *const Cursor,
        alloc: std.mem.Allocator,
        item: *const Item,
    ) !zroar.Bitmap {
        std.debug.assert(self.want_versions == std.math.maxInt(u32));
        std.debug.assert(self.opts.pending == null);
        return foldBitmap(alloc, item.versions.slice());
    }

    /// Look up the tree owning `key_range.left`, snapshot its range, and
    /// open a merge over its intersection with the window. Redoing the
    /// lookup per hop keeps the scan safe against splits firing between
    /// hops — we always take whichever tree currently covers the next
    /// key. Leaves `merge` null and sets `done` when no tree does. An
    /// empty intersection just opens an empty merge; `next` then hops
    /// or finishes as usual.
    ///
    /// The tree is held by ref (`TableTrees.getTree`) across the range
    /// snapshot and the table selection. A split never mutates a live
    /// tree — it installs fresh objects and retires this one, which
    /// stays whole for us — so the range copied and the tables pinned
    /// describe the same instant, and a later split just moves tables
    /// we already hold. The ref is released before the seek, the part
    /// that reads blocks.
    fn openNextTree(self: *Cursor) !void {
        const a = self.arena.allocator();

        const visit_range = blk: {
            const trees = self.forest.getTable(self.table_key) orelse {
                self.done = true;
                return;
            };
            const tree = trees.getTreeByKey(self.key_range.left) orelse {
                self.done = true;
                return;
            };
            defer tree.release();
            self.tree_range = try tree.key_range.dupe(a);
            const visit_range = self.key_range.intersect(self.tree_range);

            // Memtable iters come from the cursor arena so they live
            // exactly as long as the merge. (Memtables.getIterators
            // returns a fresh slice; freeing it through the arena at
            // deinit is fine.) The txn's pending skiplist rides in as
            // src 0 — the merge's equal-key tie-break.
            const srcs = try self.forest.memtables.getIterators(
                a,
                .{ .pending = self.opts.pending },
                .{
                    .read_ts = self.opts.read_ts,
                    .want = self.want_versions,
                },
            );
            self.merge = try tree.openMerge(
                a,
                visit_range,
                self.opts.min_ts,
                srcs,
            );
            break :blk visit_range;
        };

        // Position the sources. On error the merge stays set, so
        // `deinit` still releases everything it pinned.
        try self.merge.?.seekRange(visit_range.left, visit_range.right);
        // A hop only happens after the previous merge was drained, and
        // draining begins by paying the owed advance — so none is owed.
        std.debug.assert(!self.next_pending);
    }

    /// Walk the open merge to the next item the options admit and
    /// return the part of it they admit, leaving the merge positioned
    /// ON it. Null when this tree is drained.
    fn nextVisible(self: *Cursor, merge: *merge_iterator.MergeIterator) !?Item {
        while (merge.itemRef()) |item| {
            if (self.window(item)) |vis| {
                self.emitted += 1;
                return vis;
            }
            try merge.next();
        }
        return null;
    }

    /// The versions of `item` this scan hands out, newest first, or
    /// null when none is.
    fn window(self: *const Cursor, item: Item) ?Item {
        // Not written yet, as far as this scan is concerned: versions
        // above read_ts go first, before the tombstone rule (a delete
        // above read_ts must not hide a key still live at read_ts) and
        // before the version cap (invisible versions must not consume
        // it).
        const vis = item.visibleAt(self.opts.read_ts) orelse return null;
        // Unchanged since min_ts, as far as this scan is concerned: a
        // key whose newest visible version is at or below it is not
        // emitted at all, and one that changed emits only what changed.
        if (vis.newest().ts <= self.opts.min_ts) return null;

        const vers = switch (vis.versions) {
            // One version, and it passed the checks above; a tombstone
            // is hidden unless asked for.
            .one => |ver| {
                if (ver.meta.isDeleted() and !self.opts.include_deleted) {
                    return null;
                }
                return vis;
            },
            .many => |vers| vers,
        };
        var n: usize = 0;
        while (n < vers.len and n < self.want_versions) : (n += 1) {
            const ver = vers[n];
            if (ver.ts <= self.opts.min_ts) break;
            // Deleted as of this version: the tombstone and everything
            // older behind it are suppressed.
            if (ver.meta.isDeleted() and !self.opts.include_deleted) break;
        }
        if (n == 0) return null;
        return .{ .key = vis.key, .versions = .{ .many = vers[0..n] } };
    }
};

/// The largest stored key for (namespace, table_id), or null if the table
/// has no keys. Assumes a monotonically-increasing row id (SQL's egid), so
/// the max lives in the rightmost tree and, within it, the rightmost SST of
/// each level: last in L0 (newest flush = highest raft_index) or last-by-
/// smallest in L≥1 (highest range). Those SSTs' in-memory `biggest` keys
/// give a floor — O(#levels), no I/O — and every key above it is an
/// unflushed memtable row, so the memtables are the only thing seeked:
/// each one from the floor to the table end, keeping the last key. No SST
/// is opened.
///
/// Order matters: the memtables are pinned BEFORE the tree is read. A
/// flush completing between the two moves rows from a memtable into a
/// new L0 file; pinned first, the popped memtable is still ours to read,
/// so the row is seen either way. Tree first, the row would be in a file
/// the floor never saw and gone from the memtables by the time they are
/// read.
///
/// The returned Key's `primary` is owned by `allocator` (free it).
/// Callers derive a table's next id from it without persisting a
/// counter — so the max is the largest STORED key, deleted or not: the
/// memtable walk counts tombstones (the skiplist iterator hides
/// nothing) and the SST `biggest` is physical, so a deleted current-max
/// still seeds the next id past itself and an id is never re-minted onto
/// a tombstoned row.
pub fn maxKey(
    self: *Forest,
    allocator: std.mem.Allocator,
    table_key: TableKey,
) !?Key {
    // Step 1: pin every memtable (active + immutables) — see the doc on
    // why this precedes the tree. `want = 1`: the walk needs keys, not
    // version chains.
    const iters = try self.memtables.getIterators(allocator, .{}, .{ .want = 1 });
    defer {
        for (iters) |*it| it.deinit();
        allocator.free(iters);
    }

    // Step 2: floor = max `biggest` over the rightmost SST of each level in
    // the rightmost tree. Owned by `allocator`; null if no SSTs (or no
    // tree yet — the memtables may still hold rows).
    var floor: ?Key = null;
    errdefer if (floor) |f| f.free(allocator);
    if (self.getTable(table_key)) |trees| {
        if (trees.getRightmostTree()) |tree| {
            defer tree.release();
            var best_sst: ?Key = null;
            const held = tree.mu.hold(.shared, @src());
            defer held.release();
            for (tree.levels) |*lv| {
                const items = lv.entries(held);
                if (items.len == 0) continue;

                const b = items[items.len - 1].table.biggest; // rightmost
                if (b.primary.len == 0) continue;

                if (best_sst == null or b.orderPrimary(best_sst.?) == .gt) {
                    best_sst = b;
                }
            }
            // Dupe out from under the tables — they may close once locks
            // drop — and stamp the tree's ids: a table bound has none.
            if (best_sst) |b| floor = tree.stamp(try b.dupe(allocator));
        }
    }

    // Step 3: walk each memtable over [floor, table end) — floor (if any)
    // is an in-table key we keep; no floor limits to the table start — and
    // keep the largest key. `best` borrows a skiplist node; the pins from
    // step 1 keep it alive until it is duped below.
    const range = (KeyRange{
        .left = floor orelse Key.inf,
    }).limitToTable(table_key);
    var best: ?Key = null;
    for (iters) |*it| {
        try it.seekRange(range.left, range.right);
        while (it.skiplist.itemRef()) |item| : (try it.skiplist.next()) {
            if (best == null or item.key.order(best.?) == .gt) best = item.key;
        }
    }

    // A memtable row above floor wins; otherwise floor stands (or null if
    // the table had no SSTs and no rows).
    if (best) |b| {
        const k = try b.dupe(allocator);
        if (floor) |f| f.free(allocator);
        return k;
    }
    return floor;
}
