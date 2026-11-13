// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const _table = @import("table.zig");
const _cache = @import("../cache.zig");
const Key = @import("../kv.zig").Key;
const Direction = @import("../kv.zig").Direction;
const KeyRange = @import("../kv.zig").KeyRange;
const Version = @import("../kv.zig").Version;
const _skl = @import("../skl/skiplist.zig");
const utils = @import("utils");
const fiber = @import("zfiber");

pub const Error = _table.Error;

/// A refcounted hold on the storage backing an iterator item's values:
/// a decompressed block (SST sources) or a memtable's skiplist arena.
/// Every positioned source is pinnable — `Iterator.currentPins` is
/// empty only when there is no item, and a merged item names one pin
/// per source it drew from. `acquire` takes the ref and returns the pin
/// for append-style use; `release` drops it (the skiplist arena
/// outlives its Memtables slot while held; a block outlives cache
/// eviction while held).
///
/// A held slice is stable AND immutable, even against the active
/// memtable: skiplist arenas are append-only — an overwrite dupes new
/// bytes and publishes a new version node, never touching old bytes (see
/// SkipList.putItem) — and blocks are immutable by construction. So
/// borrowing until a fold completes carries exactly the guarantees
/// copying at capture time had.
pub const Pin = union(enum) {
    block: *_cache.Value,
    skiplist: *_skl.SkipList,

    /// A pinned block is memory the tenant holds, counted as the
    /// batch's own buffer is, on the tenant and not the statement:
    /// `SELECT b FROM rec` stalled on its client holds one batch of
    /// blocks, and the meter sees them. Every pinner pays, so two
    /// statements on one block count it twice. A skiplist arena lives
    /// as long as its memtable, pinned or not, so it is not charged.
    pub fn acquire(self: Pin) Pin {
        switch (self) {
            .block => |b| {
                _ = b.incrRef();
                fiber.ctx().tenant.holdMem(@intCast(b.byteSize()));
            },
            .skiplist => |sl| sl.acquire(),
        }
        return self;
    }

    pub fn release(self: Pin) void {
        switch (self) {
            .block => |b| {
                fiber.ctx().tenant.releaseMem(@intCast(b.byteSize()));
                b.decrRef();
            },
            .skiplist => |sl| sl.release(),
        }
    }
};

/// Iterator is a tagged union over every source shape: a TableIterator
/// (single SST), a ConcatIterator (multiple non-overlapping SSTs), a
/// memtable scan (`skl.Iterator`), or a MergeIterator over other
/// sources. All expose the same `seekRange` / `itemRef` / `next` /
/// `currentPins` / `deinit` shape, so a reader can take one `*Iterator`
/// whatever is behind it. Every source walks in one direction, fixed
/// at its init; a merge takes only sources built for its own.
pub const Iterator = union(enum) {
    table: _table.TableIterator,
    concat: ConcatIterator,
    skiplist: _skl.Iterator,
    merge: MergeIterator,

    // `Error`, not inferred: a merge's seek calls back into this one.
    pub fn seekRange(self: *Iterator, start: Key, end: Key) Error!void {
        switch (self.*) {
            inline else => |*it| try it.seekRange(start, end),
        }
    }

    pub fn direction(self: *const Iterator) Direction {
        return switch (self.*) {
            inline else => |*it| it.dir,
        };
    }

    /// The pins on the storage backing itemRef()'s values; empty only
    /// when the source holds no item. A leaf source has one pin and
    /// returns it through `buf`; a merge returns its own list, one pin
    /// per source the item drew from.
    pub fn currentPins(self: *Iterator, buf: *[1]Pin) []const Pin {
        switch (self.*) {
            .table => |*t| {
                const b = t.currentBlock() orelse return &.{};
                buf[0] = .{ .block = b };
                return buf[0..1];
            },
            .concat => |*c| {
                const b = c.currentBlock() orelse return &.{};
                buf[0] = .{ .block = b };
                return buf[0..1];
            },
            .skiplist => |*s| {
                if (s.node == null) return &.{};
                buf[0] = .{ .skiplist = s.skiplist };
                return buf[0..1];
            },
            .merge => |*m| return m.currentPins(),
        }
    }

    /// The source's own item, read in place: valid until the source
    /// moves. A pointer and not a copy because a scan looks at every
    /// item and keeps few, and a copy taken right after the item was
    /// written waits for those writes to land.
    pub fn itemRef(self: *const Iterator) ?_table.ItemRef {
        return switch (self.*) {
            inline else => |*it| it.itemRef(),
        };
    }

    pub fn next(self: *Iterator) Error!void {
        switch (self.*) {
            inline else => |*it| try it.next(),
        }
    }

    pub fn deinit(self: *Iterator) void {
        switch (self.*) {
            inline else => |*it| it.deinit(),
        }
    }
};

/// One item per key, drawn from every source that holds it.
///
/// `live` is the sources that still hold an item, sorted by that item's
/// key in the walk's direction; sources on one key are in no order.
/// The sources holding the next key are therefore the first `n_cur`
/// entries, and the item is built from them: one source's versions as
/// they are, or several sources' versions sorted by timestamp
/// descending. Source order is not timestamp order (L0 tables arrive
/// oldest first), which is why the versions are sorted.
///
/// A timestamp names one version of a key, but two sources can hold
/// that version at once: a reader that took the memtables before a
/// flush and the tree after it holds a memtable and its L0 table.
/// The copies are the same, so such a version is taken once, from
/// whichever source comes up first. A txn's pending writes never tie
/// with a committed version: they sit at kv.pending_ts, above all of
/// them.
///
/// `next` takes the current sources out, advances them, and puts each
/// back at its place with a scan from the front: a source in the
/// middle of a run of keys costs one compare, one that jumped past the
/// others costs as many as it passed.
///
/// Nothing is held: the item's key and values point into the sources,
/// which keep their blocks and arenas pinned until they are advanced,
/// so the item is valid until the following `next`, `seekRange` or
/// `deinit`. `currentPins` names those pins for a caller that keeps a
/// value longer.
pub const MergeIterator = struct {
    /// One current source's versions, newest first, as `build` takes
    /// them.
    const Run = struct {
        versions: []const Version,

        fn top(self: Run) ?Version {
            return if (self.versions.len == 0) null else self.versions[0];
        }

        /// The head, taken.
        fn next(self: *Run) Version {
            const v = self.versions[0];
            self.versions = self.versions[1..];
            return v;
        }

        /// Drop the head for as long as it is `kept` again, this
        /// source's copy of it.
        fn skip(self: *Run, kept: Version) void {
            while (self.top()) |v| {
                if (v.ts != kept.ts) break;
                // One timestamp, one version: a copy that differs would
                // make the item depend on which source came up first.
                std.debug.assert(v.meta.toByte() == kept.meta.toByte());
                std.debug.assert(std.mem.eql(u8, v.value, kept.value));
                self.versions = self.versions[1..];
            }
        }
    };

    /// One input: its iterator and its current item (the iterator's
    /// own, taken again on every advance, null once exhausted).
    const Source = struct {
        it: Iterator,
        item: ?_table.ItemRef = null,
    };

    allocator: std.mem.Allocator,
    dir: Direction,
    /// Every input, for the merge's lifetime: a source that ran out
    /// leaves `live` but comes back on the next `seekRange`.
    srcs: []*Source,
    /// The sources holding an item, sorted by key.
    live: std.ArrayList(*Source) = .empty,
    /// How many sources at the front of `live` hold the current key;
    /// 0 when there is no item. With one, the item is that source's
    /// own, read in place; with more it is `merged`.
    n_cur: usize = 0,
    /// The shared key over `vers`.
    merged: _table.Item = undefined,
    vers: std.ArrayList(Version) = .empty,
    /// Scratch for `build`: what is left of each current source's run.
    runs: std.ArrayList(Run) = .empty,
    /// Scratch for `currentPins`.
    pins: std.ArrayList(Pin) = .empty,

    /// Takes the iterators over: `deinit` releases them. Every one
    /// must have been built to walk in `dir`. `iters` itself stays the
    /// caller's; on error nothing was taken.
    pub fn init(
        allocator: std.mem.Allocator,
        iters: []const Iterator,
        dir: Direction,
    ) !MergeIterator {
        const srcs = try allocator.alloc(*Source, iters.len);
        errdefer allocator.free(srcs);
        var made: usize = 0;
        errdefer for (srcs[0..made]) |src| allocator.destroy(src);
        for (srcs, iters) |*slot, it| {
            const src = try allocator.create(Source);
            std.debug.assert(it.direction() == dir);
            src.* = .{ .it = it };
            slot.* = src;
            made += 1;
        }
        var self: MergeIterator = .{
            .allocator = allocator,
            .dir = dir,
            .srcs = srcs,
        };
        errdefer self.live.deinit(allocator);
        try self.live.ensureTotalCapacity(allocator, iters.len);
        // One pin per source at most.
        try self.pins.ensureTotalCapacity(allocator, iters.len);
        return self;
    }

    pub fn deinit(self: *MergeIterator) void {
        for (self.srcs) |src| {
            src.it.deinit();
            self.allocator.destroy(src);
        }
        self.allocator.free(self.srcs);
        self.live.deinit(self.allocator);
        self.vers.deinit(self.allocator);
        self.runs.deinit(self.allocator);
        self.pins.deinit(self.allocator);
    }

    /// The order of `live`: `a`'s key comes strictly before `b`'s in
    /// the walk's direction, so the two do not share it. The full key
    /// is compared, since a MultiGet seek over the memtables has no end
    /// bound and its sources can settle on keys of different tables or
    /// namespaces. The direction is the context std.sort passes.
    fn ahead(dir: Direction, a: *Source, b: *Source) bool {
        return switch (a.item.?.key.order(b.item.?.key)) {
            .lt => dir == .forward,
            .gt => dir == .reverse,
            .eq => false,
        };
    }

    pub fn seekRange(self: *MergeIterator, start: Key, end: Key) Error!void {
        utils.clearRetainCap(&self.live);
        for (self.srcs) |src| {
            try src.it.seekRange(start, end);
            src.item = src.it.itemRef();
            if (src.item != null) self.live.appendAssumeCapacity(src);
        }
        std.mem.sortUnstable(*Source, self.live.items, self.dir, ahead);
        try self.build();
    }

    /// The pins on the storage backing itemRef()'s values, one per
    /// source the item was built from; empty when the merge is
    /// exhausted. The pins stay borrowed: the sources hold them.
    pub fn currentPins(self: *MergeIterator) []const Pin {
        utils.clearRetainCap(&self.pins);
        for (self.live.items[0..self.n_cur]) |src| {
            var buf: [1]Pin = undefined;
            for (src.it.currentPins(&buf)) |pin| {
                const covered = self.pins.items.len > 0 and
                    std.meta.eql(self.pins.getLast(), pin);
                if (!covered) self.pins.appendAssumeCapacity(pin);
            }
        }
        return self.pins.items;
    }

    pub fn itemRef(self: *const MergeIterator) ?_table.ItemRef {
        return switch (self.n_cur) {
            0 => null,
            1 => self.live.items[0].item.?,
            else => &self.merged,
        };
    }

    /// Step to the next key: advance the sources the item was built
    /// from, move each to its place, and build again. The sources are
    /// the first `n_cur` entries, and each pass handles the one at the
    /// front: the ones not yet advanced still hold the smallest key, so
    /// the advanced one lands behind them, and the next pass finds the
    /// next of them at the front.
    pub fn next(self: *MergeIterator) Error!void {
        if (self.n_cur == 0) return;
        const alone = self.n_cur == 1;
        for (0..self.n_cur) |_| {
            const src = self.live.items[0];
            try src.it.next();
            src.item = src.it.itemRef();
            if (src.item == null) {
                _ = self.live.orderedRemove(0);
                continue;
            }
            // A run of keys from one source, most of any scan: its
            // next key is still ahead of every other source's, so it
            // is the item as it stands, at one compare.
            if (alone) {
                const items = self.live.items;
                if (items.len == 1 or ahead(self.dir, src, items[1])) return;
            }
            // Its place among the rest: past every source whose key
            // comes before its own. One compare and no move when it
            // still holds the smallest key.
            var pos: usize = 0;
            for (self.live.items[1..]) |other| {
                if (!ahead(self.dir, other, src)) break;
                pos += 1;
            }
            const items = self.live.items;
            std.mem.copyForwards(*Source, items[0..pos], items[1 .. pos + 1]);
            items[pos] = src;
        }
        try self.build();
    }

    /// Build the item from the sources at the front of `live`, the ones
    /// holding the next key in the walk's direction.
    fn build(self: *MergeIterator) Error!void {
        self.n_cur = 0;
        if (self.live.items.len == 0) return;
        // No item if the versions cannot be gathered.
        errdefer self.n_cur = 0;

        const first = self.live.items[0].item.?;
        self.n_cur = 1;
        while (self.n_cur < self.live.items.len) : (self.n_cur += 1) {
            const item = self.live.items[self.n_cur].item.?;
            if (!item.key.eq(first.key)) break;
        }
        if (self.n_cur == 1) return;

        // Each source's run is already newest first, so the runs are
        // merged, linear in the versions: a hot key can hold hundreds
        // of thousands, and sorting their concatenation was quadratic
        // (L0 sources arrive oldest first, the worst case). Each pass
        // finds the lead, the run with the newest head, and the newest
        // head among the others. The lead's versions newer than that
        // come in a row, without another scan: one source usually holds
        // a key's whole recent history. A head shared with another run
        // is one version held twice (a flushed memtable and its L0
        // table): the lead's copy is taken and the others' dropped.
        utils.clearRetainCap(&self.vers);
        try self.runs.resize(self.allocator, self.n_cur);
        for (self.live.items[0..self.n_cur], self.runs.items) |src, *run|
            run.* = .{ .versions = src.item.?.versions.slice() };

        while (true) {
            // One pass over the heads: `lead` is the run whose head is
            // the newest, `next_ts` the newest head among the rest.
            var lead: ?usize = null;
            var lead_ts: u64 = 0;
            var next_ts: ?u64 = null;
            for (self.runs.items, 0..) |run, i| {
                // A run with nothing left takes no part.
                const ts = (run.top() orelse continue).ts;
                if (lead == null or ts > lead_ts) {
                    // A newer head: this run leads, and the one it
                    // displaced is the newest of the rest so far.
                    if (lead != null) next_ts = lead_ts;
                    lead = i;
                    lead_ts = ts;
                } else if (next_ts == null or ts > next_ts.?) {
                    // Not newer than the lead, so it competes for
                    // second. A head equal to the lead's lands here
                    // too, which is how a duplicate shows up below.
                    next_ts = ts;
                }
            }
            // No run has a version left: the item is complete.
            const run = &self.runs.items[lead orelse break];

            if (next_ts != null and lead_ts == next_ts.?) {
                // We found a duplicate version. Pick the lead. Advance all
                // the rest. And re-calculate lead and next.
                const kept = run.next();
                try self.vers.append(self.allocator, kept);
                for (self.runs.items) |*r| r.skip(kept);
                continue;
            }

            while (run.top()) |v| {
                if (next_ts != null and v.ts <= next_ts.?) break;
                try self.vers.append(self.allocator, run.next());
            }
        }
        self.merged = .{
            .key = first.key,
            .versions = .{ .many = self.vers.items },
        };
    }
};

pub const ConcatIterator = struct {
    iters: []_table.TableIterator, // caller-owned slice; sorted by smallest key
    current: ?usize, // null before a seek and once exhausted
    dir: Direction,
    /// Carried over from `seekRange` for the lazy seek of each table
    /// reached.
    range: KeyRange = .all,

    /// `iters` were each built to walk in `dir`.
    pub fn init(
        iters: []_table.TableIterator,
        dir: Direction,
    ) ConcatIterator {
        for (iters) |*it| std.debug.assert(it.dir == dir);
        return .{ .iters = iters, .current = null, .dir = dir };
    }

    pub fn deinit(self: *ConcatIterator) void {
        for (self.iters) |*it| it.deinit();
    }

    /// Seek the FIRST relevant iter only — subsequent iters get seeked
    /// lazily by `next()` as it advances `current`. Avoids paying the
    /// per-table `setBlock` + decompress cost for tables we may never
    /// reach (e.g., when the merge upstream stops early). The first in
    /// reverse is the last table holding a key below `end`.
    pub fn seekRange(self: *ConcatIterator, start: Key, end: Key) !void {
        self.range = .{ .left = start, .right = end };
        if (self.iters.len == 0) return;

        switch (self.dir) {
            .forward => if (start.isInf()) {
                self.current = 0;
            } else {
                // First table whose biggest key >= start.
                const Pred = struct {
                    fn f(sk: Key, it: _table.TableIterator) bool {
                        return it.table.biggest.gePrimary(sk);
                    }
                };
                const idx = utils.search(
                    _table.TableIterator,
                    self.iters,
                    start,
                    Pred.f,
                );
                self.current = if (idx < self.iters.len) idx else null;
            },
            .reverse => if (end.isInf()) {
                self.current = self.iters.len - 1;
            } else {
                // First table whose smallest key >= end: it and every
                // one after hold nothing below `end`.
                const Pred = struct {
                    fn f(ek: Key, it: _table.TableIterator) bool {
                        return it.table.smallest.gePrimary(ek);
                    }
                };
                const above = utils.search(
                    _table.TableIterator,
                    self.iters,
                    end,
                    Pred.f,
                );
                self.current = if (above > 0) above - 1 else null;
            },
        }
        const cur = self.current orelse return;
        try self.iters[cur].seekRange(start, end);
    }

    pub fn itemRef(self: *const ConcatIterator) ?_table.ItemRef {
        const cur = self.current orelse return null;
        return self.iters[cur].itemRef();
    }

    pub fn currentBlock(self: *const ConcatIterator) ?*_cache.Value {
        const cur = self.current orelse return null;
        return self.iters[cur].currentBlock();
    }

    /// When the current iter exhausts, move to the neighbouring table
    /// and lazy-seek it. The handoff side is unbounded: every table
    /// ahead is by definition entirely past the current one's range,
    /// so only the range's far bound still applies.
    pub fn next(self: *ConcatIterator) Error!void {
        var cur = self.current orelse return;
        try self.iters[cur].next();
        while (self.iters[cur].itemRef() == null) {
            switch (self.dir) {
                .forward => {
                    cur += 1;
                    if (cur == self.iters.len) {
                        self.current = null;
                        return;
                    }
                    try self.iters[cur].seekRange(Key.inf, self.range.right);
                },
                .reverse => {
                    if (cur == 0) {
                        self.current = null;
                        return;
                    }
                    cur -= 1;
                    try self.iters[cur].seekRange(self.range.left, Key.inf);
                },
            }
        }
        self.current = cur;
    }
};
