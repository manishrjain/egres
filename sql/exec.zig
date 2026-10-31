// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The statement executor: a pull pipeline of RowNodes exchanging ONE
//! Batch — rows the source copied once, plus a bit set saying which of
//! them survive — and BitmapNodes that turn index reads into egid sets.
//! The source copies; operators after it only clear bits; sinks
//! (Materialize, the wire emitter) are the only ones to touch bytes
//! again. plan.zig builds the trees; dml.zig drives them.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const zdb = @import("zdb");

const _key = @import("key.zig");
const _pred = @import("pred.zig");
const _table = @import("table.zig");
const _txn = @import("txn.zig");
const String = @import("value.zig").String;

const log = std.log.scoped(.sql);
const Forest = zdb.forest.Forest;
const Item = zdb.kv.Item;
const Key = zdb.kv.Key;
const KeyRange = zdb.kv.KeyRange;
const Pred = _pred.Pred;
const Txn = _txn.Txn;
const zroar = zdb.zroar;

/// Everything a node needs from the statement it runs in.
pub const ExecCtx = struct {
    /// Statement arena: captured rows (Materialize), sorted key lists.
    arloc: std.mem.Allocator,
    /// read_ts, pending writes, the forest, the locks; its allocator
    /// backs bitmaps and the scan cursor.
    tx: *Txn,
    tbl: *const _table.Table,
    table_id: u32,
    /// Take row locks — on the OUTPUT: Materialize gathers every row
    /// and locks them after its drain, so a row a filter discarded is
    /// never locked and sources never lock at all. lockRows may raise
    /// Restart, so nothing reaches the wire before the last lock.
    /// Without this Materialize streams.
    lock: bool,
};

pub const row_cap = 1024;
pub const byte_cap: usize = 256 << 10;
/// Bit i set = row i of the batch survives. std.StaticBitSet, not
/// zroar: 16 words inline, no allocation — zroar stays for u64 id sets
/// (postings, BitmapNode.egids), where the universe really is u64.
pub const Selection = std.StaticBitSet(row_cap);

/// The rows in flight between operators. `append` copies a row's key
/// into `bytes` — capacity-pinned while non-empty, growing only to
/// admit ONE oversized row into an EMPTY batch — and its value per
/// `ValueMode`: copied next to the key, borrowed against a pin the
/// source holds for the batch's lifetime, or dropped. `appendRefs`
/// places whole rows that live elsewhere (the statement arena), the
/// caller's promise. Sources fill it and set the bits; filters clear
/// bits; consumers walk `selected()` (ascending).
pub const Batch = struct {
    allocator: std.mem.Allocator,
    /// Reserved to row_cap once; never grows past it.
    rows: std.ArrayList(Item),
    sel: Selection = Selection.initEmpty(),
    bytes: std.ArrayList(u8),
    /// How many rows a fill may place. The plan root (Materialize)
    /// sets it before every fill: the cap, or what a LIMIT has left.
    /// `reset` leaves it alone; sources read it through `full`.
    row_limit: usize = row_cap,
    /// Set when `append` turned a row away for lack of bytes: the fill
    /// is over even though rows remain below the limit. Sources stop at
    /// the first refusal and stash that row (rows are sequential, so a
    /// smaller one is never offered in its place). Cleared by `reset`.
    no_space: bool = false,
    /// Rows a count-only source counted instead of appending (see
    /// plan.Options.count_only). Cleared by `reset`.
    counted: u64 = 0,

    pub fn init(allocator: std.mem.Allocator) !Batch {
        var rows: std.ArrayList(Item) = .empty;
        try rows.ensureTotalCapacityPrecise(allocator, row_cap);
        errdefer rows.deinit(allocator);

        var bytes: std.ArrayList(u8) = .empty;
        try bytes.ensureTotalCapacityPrecise(allocator, byte_cap);
        return .{ .allocator = allocator, .rows = rows, .bytes = bytes };
    }

    pub fn deinit(self: *Batch) void {
        self.rows.deinit(self.allocator);
        self.bytes.deinit(self.allocator);
    }

    /// Empty the batch; capacity stays (it never shrinks).
    pub fn reset(self: *Batch) void {
        utils.clearRetainCap(&self.rows);
        utils.clearRetainCap(&self.bytes);
        self.sel = Selection.initEmpty();
        self.no_space = false;
        self.counted = 0;
    }

    pub fn numRows(self: *const Batch) usize {
        return self.rows.items.len;
    }

    /// This fill accepts no more rows: the row limit is reached, or a
    /// row was already refused for lack of bytes (`no_space`).
    pub fn full(self: *const Batch) bool {
        return self.no_space or self.numRows() >= self.row_limit;
    }

    /// What `append` does with a row's value. The key is always copied:
    /// a cursor rebuilds it in its own buffer on every advance.
    pub const ValueMode = enum {
        /// Copy it into `bytes` next to the key.
        copy,
        /// Keep the slice as is: the source holds a pin on the storage
        /// behind it for as long as the batch holds the row.
        borrow,
        /// Drop it — a key-only consumer.
        none,
    };

    /// Place `item` — its key copied, its value per `mode` — and select
    /// it. false = no room — the caller keeps the row for the next
    /// fill, and `full` is true from here on. A row larger than the
    /// whole byte capacity is admitted only into an EMPTY batch: nothing
    /// slices into `bytes` then, so growing it is safe.
    pub fn append(self: *Batch, item: Item, mode: ValueMode) !bool {
        if (self.full()) return false;

        const src = item.newest();
        const vlen = if (mode == .copy) src.value.len else 0;
        const need = item.key.primary.len + vlen;
        if (self.bytes.items.len + need > self.bytes.capacity) {
            if (self.numRows() > 0) {
                self.no_space = true;
                return false;
            }
            std.debug.assert(self.bytes.items.len == 0);
            try self.bytes.ensureTotalCapacity(self.allocator, need);
        }

        const kstart = self.bytes.items.len;
        self.bytes.appendSliceAssumeCapacity(item.key.primary);
        const primary = self.bytes.items[kstart..][0..item.key.primary.len];

        var value: String = "";
        switch (mode) {
            .copy => {
                const vstart = self.bytes.items.len;
                self.bytes.appendSliceAssumeCapacity(src.value);
                value = self.bytes.items[vstart..][0..src.value.len];
            },
            .borrow => value = src.value,
            .none => {},
        }

        self.sel.set(self.numRows());
        self.rows.appendAssumeCapacity(.{
            .key = .{
                .namespace = item.key.namespace,
                .table_id = item.key.table_id,
                .primary = primary,
            },
            .versions = .{ .one = .{
                .ts = src.ts,
                .meta = src.meta,
                .value = value,
            } },
        });
        return true;
    }

    /// Place rows that live elsewhere — no copy, `bytes` untouched —
    /// and select them. Only the row limit bounds them. Returns how
    /// many were placed; the caller offers the rest on the next fill.
    pub fn appendRefs(self: *Batch, rows: []const Item) usize {
        const start = self.numRows();
        const n = @min(rows.len, self.row_limit -| start);
        self.rows.appendSliceAssumeCapacity(rows[0..n]);
        self.sel.setRangeValue(.{ .start = start, .end = start + n }, true);
        return n;
    }

    /// The selected rows, ascending. The batch must not move or be
    /// refilled while an iterator is live.
    pub const Selected = struct {
        batch: *const Batch,
        it: Selection.Iterator(.{}),

        pub fn next(self: *Selected) ?Item {
            const i = self.it.next() orelse return null;
            return self.batch.rows.items[i];
        }
    };

    pub fn selected(self: *const Batch) Selected {
        return .{ .batch = self, .it = self.sel.iterator(.{}) };
    }
};

/// A point read of an index tree: which tree + the memcomparable key.
/// Unique: the entry's value IS the one row key. Non-unique: the entry
/// is a roaring bitmap — every matching egid.
pub const IndexLookup = struct {
    index_table_id: u32,
    key: String,
    unique: bool,
};

/// A range read of an index tree: every entry whose key is in
/// [lo, hi), the memcomparable bounds the planner built from `col op
/// literal` (plan.rangeLookup).
pub const IndexRange = struct {
    index_table_id: u32,
    lo: String,
    hi: String,
    unique: bool,
};

/// Bitmap nodes produce a WHOLE egid set — a bitmap already is the
/// batch, and every variant evaluates to one (an egid list touches no
/// index and and_/or_ combine bitmaps, hence not "IndexNode").
pub const BitmapNode = union(enum) {
    index_lookup: IndexLookup,
    index_range: IndexRange,
    /// Literal egids (`egid = N`; IN lists later), built by the planner
    /// and owned by the plan; eval hands out a clone.
    egids: zroar.Bitmap,
    and_: []BitmapNode,
    or_: []BitmapNode,

    /// Frees this node's own bitmaps, recursively.
    pub fn deinit(self: *BitmapNode) void {
        switch (self.*) {
            .egids => |*bm| bm.deinit(),
            .and_, .or_ => |kids| for (kids) |*kid| kid.deinit(),
            .index_lookup, .index_range => {},
        }
    }

    /// Whether any egid in the result came from an index — a missing
    /// row is then corruption (rows and index entries commit
    /// atomically), not an absent literal id.
    pub fn fromIndex(self: *const BitmapNode) bool {
        return switch (self.*) {
            .index_lookup, .index_range => true,
            .egids => false,
            .and_, .or_ => |kids| for (kids) |*kid| {
                if (kid.fromIndex()) break true;
            } else false,
        };
    }

    /// The egid set this node names, OWNED by the caller (tx.allocator).
    /// Pure reads, recorded on the txn: no locks are taken here — not
    /// even on an absent unique key. A reader that finds no row locks
    /// nothing (Postgres's rule too); a concurrent INSERT of that key
    /// lands, and the reader's own later write of it sees the stale
    /// read and gets a serialization failure to retry.
    /// AND folds in place and stops at the first empty result; OR
    /// evaluates every child and unions them in one pass.
    pub fn eval(self: *const BitmapNode, ctx: *ExecCtx) !zroar.Bitmap {
        // One yield point per node: a node's own work is bounded by one
        // index key, but an OR over a long IN list is many nodes.
        try fiber.ctx().maybeYield();

        const alloc = ctx.tx.allocator;
        switch (self.*) {
            .egids => |*bm| return bm.clone(),
            .index_lookup => |lu| return if (lu.unique)
                uniqueLookup(ctx, lu)
            else
                postingLookup(ctx, lu),
            .index_range => |r| return if (r.unique)
                uniqueRange(ctx, r)
            else
                postingRange(ctx, r),
            .and_ => |kids| {
                std.debug.assert(kids.len > 0);
                var acc = try kids[0].eval(ctx);
                errdefer acc.deinit();

                for (kids[1..]) |*kid| {
                    if (acc.isEmpty()) break;
                    var other = try kid.eval(ctx);
                    defer other.deinit();
                    acc.andInPlace(&other);
                }
                return acc;
            },
            .or_ => |kids| {
                std.debug.assert(kids.len > 0);
                var parts: std.ArrayList(zroar.Bitmap) = .empty;
                defer {
                    for (parts.items) |*p| p.deinit();
                    parts.deinit(alloc);
                }

                try parts.ensureTotalCapacity(alloc, kids.len);
                for (kids) |*kid| parts.appendAssumeCapacity(try kid.eval(ctx));
                // Pointers only once the list is complete — appends
                // may have moved it.
                const ptrs = try alloc.alloc(
                    *const zroar.Bitmap,
                    parts.items.len,
                );
                defer alloc.free(ptrs);
                for (parts.items, ptrs) |*p, *q| q.* = p;
                return zroar.Bitmap.fastOr(alloc, ptrs);
            },
        }
    }

    /// `WHERE <unique col> = v`: the entry under the key names the one
    /// row. Absent → empty. The value is the 8 BE egid bytes INSERT
    /// wrote — anything else is corruption, failed loudly rather than
    /// read as "0 rows".
    fn uniqueLookup(ctx: *ExecCtx, lu: IndexLookup) !zroar.Bitmap {
        var bm = try zroar.Bitmap.init(ctx.tx.allocator);
        errdefer bm.deinit();

        var res = try ctx.tx.multiGet(lu.index_table_id, &.{lu.key});
        defer res.deinit();
        const entry = res.item(lu.key) orelse return bm;

        const egid = _key.decodeEgid(entry.newest().value) orelse
            return error.CorruptIndexEntry;
        _ = try bm.set(egid);
        return bm;
    }

    /// `WHERE <indexed col> = v` through a posting bitmap. Recorded as a
    /// point read of the INDEX key, not a scan: any commit changing
    /// this key's membership writes a posting delta under this exact
    /// key, so one identity hash revalidates the whole result on a
    /// later read_ts bump — phantom-free. Own pending posting ops are
    /// overlaid. A corrupt posting fails the statement loudly.
    fn postingLookup(ctx: *ExecCtx, lu: IndexLookup) !zroar.Bitmap {
        const pk = Key{
            .namespace = ctx.tx.ns,
            .table_id = lu.index_table_id,
            .primary = lu.key,
        };
        try ctx.tx.recordRead(pk);
        var bm = try ctx.tx.forest.getBitmap(ctx.tx.allocator, .{
            .key = pk,
            .read_ts = ctx.tx.read_ts,
        });
        errdefer bm.deinit();

        try ctx.tx.overlayPostings(lu.index_table_id, lu.key, &bm);
        return bm;
    }

    /// `WHERE <unique col> op v`: every entry between the bounds, each
    /// naming one row; the cursor merges this txn's pending entries in,
    /// since a unique index's are row-shaped. A range is recorded as a
    /// scan: a key committed into it later is a phantom no point read
    /// would catch on a read_ts bump.
    fn uniqueRange(ctx: *ExecCtx, ir: IndexRange) !zroar.Bitmap {
        const alloc = ctx.tx.allocator;
        var bm = try zroar.Bitmap.init(alloc);
        errdefer bm.deinit();

        ctx.tx.recordScan();
        var cursor = try ctx.tx.forest.openCursor(
            alloc,
            .{ .namespace = ctx.tx.ns, .table_id = ir.index_table_id },
            rangeOf(ctx, ir),
            .{ .read_ts = ctx.tx.read_ts, .pending = ctx.tx.writes },
        );
        defer cursor.deinit();
        while (try cursor.next()) |entry| {
            const egid = _key.decodeEgid(entry.newest().value) orelse
                return error.CorruptIndexEntry;
            _ = try bm.set(egid);
        }
        return bm;
    }

    /// `WHERE <indexed col> op v` through posting bitmaps: one cursor
    /// over the committed index between the bounds, each key's chain
    /// folded and unioned, then this txn's pending ops on the range
    /// overlaid in one pass. Cheaper than a table scan while the range
    /// is selective, which is the planner's bet for now.
    fn postingRange(ctx: *ExecCtx, r: IndexRange) !zroar.Bitmap {
        const alloc = ctx.tx.allocator;
        ctx.tx.recordScan();
        var acc = try zroar.Bitmap.init(alloc);
        errdefer acc.deinit();
        var cursor = try ctx.tx.forest.openCursor(
            alloc,
            .{ .namespace = ctx.tx.ns, .table_id = r.index_table_id },
            rangeOf(ctx, r),
            .{
                .read_ts = ctx.tx.read_ts,
                .num_versions = std.math.maxInt(u32),
            },
        );
        defer cursor.deinit();
        var seen: usize = 0;
        while (try cursor.next()) |entry| : (seen += 1) {
            // One yield point per hundred keys: a key is one fold.
            if (seen % 100 == 0) try fiber.ctx().maybeYield();
            var part = try cursor.bitmap(alloc, &entry);
            defer part.deinit();
            try acc.orInPlace(&part);
        }
        try ctx.tx.overlayPostingsInRange(r.index_table_id, r.lo, r.hi, &acc);
        return acc;
    }

    fn rangeOf(ctx: *ExecCtx, r: IndexRange) KeyRange {
        return .{
            .left = .{
                .namespace = ctx.tx.ns,
                .table_id = r.index_table_id,
                .primary = r.lo,
            },
            .right = .{
                .namespace = ctx.tx.ns,
                .table_id = r.index_table_id,
                .primary = r.hi,
            },
        };
    }
};

/// One operator. `next` either fills `b` (sources) or narrows `b.sel`
/// (filters). Its bool says whether THIS batch is valid: true = selected
/// rows (or a count) are in `b`, a partial batch only when the source
/// ran dry; false = `b` is empty, the node is exhausted, and every later
/// call is false too. A false never carries rows, so a caller may stop
/// on it without looking at `b`. ONE Batch per pipeline, owned by the
/// plan root and passed down. `deinit` frees this node's own resources
/// only — the plan deinits every node.
pub const RowNode = union(enum) {
    seq_scan: SeqScan,
    fetch: Fetch,
    filter: Filter,
    materialize: Materialize,

    // Explicit error set: Filter and Materialize recurse into RowNode.next,
    // which an inferred set cannot express (dependency loop).
    pub fn next(self: *RowNode, ctx: *ExecCtx, b: *Batch) anyerror!bool {
        return switch (self.*) {
            .seq_scan => |*n| n.next(ctx, b),
            .fetch => |*n| n.next(ctx, b),
            .filter => |*n| n.next(ctx, b),
            .materialize => |*n| n.next(ctx, b),
        };
    }

    /// Frees what the subtree holds — cursors, bitmaps, held rows —
    /// children included: a node owns its child, the plan owns the
    /// root. Node memory itself belongs to the statement arena.
    pub fn deinit(self: *RowNode) void {
        switch (self.*) {
            .seq_scan => |*n| n.deinit(),
            .fetch => |*n| n.deinit(),
            .filter => |*n| n.child.deinit(),
            .materialize => |*n| n.child.deinit(),
        }
    }
};

/// Snapshot scan of the whole table at read_ts, own pending writes
/// merged in. Recorded on the txn as a scan (SI: a later read_ts bump
/// cannot revalidate it). The residual predicate runs on the cursor's
/// BORROW, so a non-matching row costs nothing — and a matching row's
/// value is never copied either: the cursor is opened pinning, so the
/// storage behind every value it hands out stays held until the next
/// fill releases it (Cursor.releasePins).
pub const SeqScan = struct {
    pred: ?*const Pred = null,
    /// false = keys only (a DELETE that never reads the row back).
    keep_value: bool = true,
    /// count(*): count the rows that pass `pred` into `Batch.counted`
    /// and hand nothing over.
    count_only: bool = false,
    cursor: ?Forest.Cursor = null,
    /// A pulled row that did not fit: the borrow stays valid until the
    /// next cursor.next, which happens only after it is appended.
    stash: ?Item = null,
    done: bool = false,

    pub fn next(self: *SeqScan, ctx: *ExecCtx, b: *Batch) !bool {
        // The previous batch is consumed: let its values go.
        if (self.cursor) |*c| try c.releasePins();
        b.reset();
        if (self.done) return false;
        // One yield point per batch: a scan is as long as the table.
        try fiber.ctx().maybeYield();

        if (self.cursor == null) {
            ctx.tx.recordScan();
            self.cursor = try ctx.tx.forest.openCursor(
                ctx.tx.allocator,
                .{ .namespace = ctx.tx.ns, .table_id = ctx.table_id },
                .all,
                .{
                    .read_ts = ctx.tx.read_ts,
                    .pending = ctx.tx.writes,
                    .pin_values = self.keep_value,
                },
            );
        }
        const cursor = &self.cursor.?;
        if (self.count_only) {
            // Nothing is handed over: the whole range is counted in one
            // call, and the batch carries only the count.
            while (try cursor.next()) |item| {
                if (self.pred) |pred| {
                    if (!pred.matches(ctx.tbl, item)) continue;
                }
                b.counted += 1;
            }
            self.done = true;
            return b.counted > 0;
        }
        const mode: Batch.ValueMode = if (self.keep_value) .borrow else .none;
        if (self.stash) |item| {
            // An empty batch admits any single row.
            const ok = try b.append(item, mode);
            std.debug.assert(ok);
            self.stash = null;
        }
        while (!b.full()) {
            const item = (try cursor.next()) orelse {
                self.done = true;
                break;
            };
            if (self.pred) |pred| {
                if (!pred.matches(ctx.tbl, item)) continue;
            }

            if (!try b.append(item, mode)) {
                self.stash = item;
                break;
            }
        }
        return b.numRows() > 0;
    }

    pub fn deinit(self: *SeqScan) void {
        if (self.cursor) |*c| c.deinit();
        self.cursor = null;
    }
};

/// The rows of every egid a BitmapNode names, egid-ascending — the
/// same order a table scan yields — read a chunk at a time through
/// Txn.multiGet: up to one batch fill of egids per call, so a run of
/// egids in one block costs one block visit. A chunk's rows are
/// borrows under the batch result's pins, copied into the batch as
/// they fit; a chunk's unconsumed tail stays pinned until the next
/// fill. Takes no locks: row locks are Materialize's, on the output.
///
/// TODO: For select count(*) queries, we don't need to fetch the rows. We can
/// just return based on cardinality of the bitmaps.
pub const Fetch = struct {
    bnode: *BitmapNode,
    pred: ?*const Pred = null,
    keep_value: bool = true,
    /// The evaluated set and a bookmark into it (the bitmap is
    /// unbounded and rows go out a batch at a time; zroar has no seek).
    /// The iterator points at `bm`: the node is NOT movable after its
    /// first next.
    bm: ?zroar.Bitmap = null,
    it: ?zroar.Iterator = null,
    /// The chunk in hand: its keys, the batch result, and the next key
    /// to place. `key_bytes` is the storage, 8 bytes per egid key;
    /// `key_slices` the table over it that multiGet takes, one slice per
    /// slot; `keys` the prefix of that table in use. Both are sized once
    /// to the chunk capacity — the bitmap's cardinality, capped at one
    /// batch fill — and overwritten per chunk. Sized to the set rather
    /// than the cap: a point read pulls one egid, and a cap-sized pair
    /// of buffers was a 24 KiB allocation (poison-filled in a safe
    /// build) per statement.
    keys: []String = &.{},
    key_slices: []String = &.{},
    key_bytes: []u8 = &.{},
    res: ?Forest.MultiGet = null,
    chunk_pos: usize = 0,
    from_index: bool = false,
    done: bool = false,

    pub fn next(self: *Fetch, ctx: *ExecCtx, batch: *Batch) !bool {
        batch.reset();
        if (self.done) return false;
        try fiber.ctx().maybeYield();

        if (self.bm == null) {
            self.bm = try self.bnode.eval(ctx);

            self.it = self.bm.?.iterator();
            self.from_index = self.bnode.fromIndex(); // For debug assert
            const cap: usize = @intCast(@max(1, @min(
                row_cap,
                self.bm.?.getCardinality(),
            )));
            self.key_bytes = try ctx.arloc.alloc(u8, cap * 8);
            self.key_slices = try ctx.arloc.alloc(String, cap);
        }

        while (!batch.full()) {
            if (self.res == null) {
                if (!try self.fetchKeys(ctx, batch.row_limit)) {
                    self.done = true;
                    break;
                }
            }
            if (self.chunk_pos == self.keys.len) {
                self.res.?.deinit();
                self.res = null;
                continue;
            }
            const key = self.keys[self.chunk_pos];
            const row = self.res.?.item(key) orelse {
                self.missing(ctx, key);
                self.chunk_pos += 1;
                continue;
            };
            if (self.pred) |p| if (!p.matches(ctx.tbl, row)) {
                self.chunk_pos += 1;
                continue;
            };
            // A row that does not fit ends the fill and is offered
            // again by the next one (an empty batch admits any row).
            if (!try batch.append(row, self.valueMode())) break;
            self.chunk_pos += 1;
        }
        return batch.numRows() > 0;
    }

    /// Pull up to `limit` egids from the bitmap into the chunk buffers
    /// and read them in one batch. false = the bitmap is exhausted.
    fn fetchKeys(self: *Fetch, ctx: *ExecCtx, limit: usize) !bool {
        var n: usize = 0;
        while (n < @min(limit, self.key_slices.len)) : (n += 1) {
            const egid = self.it.?.next() orelse break;
            const key = self.key_bytes[n * 8 ..][0..8];
            key.* = _key.encodeEgid(egid);
            self.key_slices[n] = key;
        }
        if (n == 0) return false;
        self.keys = self.key_slices[0..n];
        self.res = try ctx.tx.multiGet(ctx.table_id, self.keys);
        self.chunk_pos = 0;
        return true;
    }

    /// An absent row is a legit empty result when a literal named it,
    /// but CORRUPTION when an index did (rows and entries commit
    /// atomically) — logged loudly, then still skipped so the
    /// statement answers from the rows that do exist.
    fn missing(self: *const Fetch, ctx: *ExecCtx, key: String) void {
        if (!self.from_index) return;
        log.err(
            "corrupt index entry: an index names egid={d}, " ++
                "no such row in table={d}",
            .{ _key.decodeEgid(key).?, ctx.table_id },
        );
    }

    /// The batch copies each row once, out of the pinned block or
    /// arena the result borrows it from.
    fn valueMode(self: *const Fetch) Batch.ValueMode {
        return if (self.keep_value) .copy else .none;
    }

    /// Also frees the bitmap tree: the planner built it for this node.
    pub fn deinit(self: *Fetch) void {
        if (self.res) |*res| res.deinit();
        self.res = null;
        if (self.bm) |*bm| bm.deinit();
        self.bm = null;
        self.it = null;
        self.bnode.deinit();
    }
};

/// Narrows a batch to the rows that satisfy `pred` — clears bits, copies
/// nothing. For a predicate that cannot sit on a source (post-join);
/// the planner pushes the residual into the source otherwise.
pub const Filter = struct {
    child: *RowNode,
    pred: *const Pred,

    pub fn next(self: *Filter, ctx: *ExecCtx, batch: *Batch) anyerror!bool {
        // A batch whose every row fails yields nothing — pull the next
        // rather than hand back an empty selection.
        while (try self.child.next(ctx, batch)) {
            // Clearing bit i mid-walk is safe: the iterator has already
            // copied the word it is handing bits out of.
            var it = batch.sel.iterator(.{});
            while (it.next()) |i| {
                const row = batch.rows.items[i];
                if (!self.pred.matches(ctx.tbl, row)) batch.sel.unset(i);
            }
            if (batch.sel.count() > 0) return true;
        }
        return false;
    }
};

/// The root of every plan. Bounds the result (LIMIT) and, when the
/// statement locks (ctx.lock), gathers and locks every row before the
/// first one goes out: the DML write bodies take that slice through
/// gatherAndLockRows before their first tx.put, and FOR UPDATE's
/// emission drives `next` over it. Without the lock a `next` is one
/// bounded fill of the child, passed straight through — no copy, no
/// gather.
pub const Materialize = struct {
    child: *RowNode,
    /// Rows still to produce; null = unbounded.
    remaining: ?usize = null,
    rows: ?[]const Item = null,
    pos: usize = 0,

    /// One fill of the child. This root is the one writer of the
    /// batch's row_limit, set before every fill: the cap, or under a
    /// LIMIT what remains, so a source places no more than that. A
    /// filter above a source can only clear bits, so selected ≤ placed
    /// ≤ remaining and the count stays exact either way (the filter
    /// merely underfills). Nothing left → false without touching the
    /// child: LIMIT 0 opens no cursor and records no read.
    fn fill(self: *Materialize, ctx: *ExecCtx, batch: *Batch) anyerror!bool {
        const remaining = self.remaining orelse {
            batch.row_limit = row_cap;
            return self.child.next(ctx, batch);
        };
        if (remaining == 0) {
            batch.reset();
            return false;
        }
        batch.row_limit = @min(row_cap, remaining);
        const valid = try self.child.next(ctx, batch);
        self.remaining = remaining - batch.sel.count();
        return valid;
    }

    /// Every selected row of the child, copied into the statement arena
    /// (keys and whatever values the source kept), in the order the
    /// child yielded them — an ORDER BY below survives. With ctx.lock
    /// every row is then locked — the OUTPUT rows, so nothing a filter
    /// discarded is ever locked — in (table, key) order, the order that
    /// keeps concurrent statements from cycling, taken from a separate
    /// sorted key list so the rows keep their order; and this returns
    /// only once EVERY lock is held (lockRows may Restart the statement,
    /// which is safe exactly because nothing was written or sent yet).
    /// `batch` is scratch to drive the child.
    pub fn gatherAndLockRows(
        self: *Materialize,
        ctx: *ExecCtx,
        batch: *Batch,
    ) anyerror![]const Item {
        if (self.rows) |rows| return rows;

        var lrows: std.ArrayList(Item) = .empty;
        while (try self.fill(ctx, batch)) {
            var it = batch.selected();
            while (it.next()) |row| {
                try lrows.append(ctx.arloc, try row.dupe(ctx.arloc));
            }
        }
        const rows = try lrows.toOwnedSlice(ctx.arloc);
        if (ctx.lock) try lockInKeyOrder(ctx, rows);
        self.rows = rows;
        return rows;
    }

    /// Lock every row ascending by (table, key), whatever order `rows`
    /// is in. Both sources already yield ascending keys, so the sort is
    /// one pass in practice; it makes the lock order this node's own
    /// property rather than the child's. Rows carry their table in the
    /// key, so a multi-table output (a join, later) needs nothing more.
    fn lockInKeyOrder(ctx: *ExecCtx, rows: []const Item) !void {
        const keys = try ctx.arloc.alloc(Key, rows.len);
        for (rows, keys) |row, *key| key.* = row.key;

        // Keys are distinct rows, so stability is moot; the unstable
        // sort has no scratch cache to poison-fill per statement.
        std.mem.sortUnstable(Key, keys, {}, lessThanKey);
        const hashes = try ctx.arloc.alloc(u64, keys.len);
        for (keys, hashes) |key, *h| {
            h.* = ctx.tx.rowHash(key.table_id, key.primary);
        }
        try ctx.tx.lockRows(hashes);
    }

    fn lessThanKey(_: void, a: Key, b: Key) bool {
        if (a.table_id != b.table_id) return a.table_id < b.table_id;
        return std.mem.order(u8, a.primary, b.primary) == .lt;
    }

    /// Streaming: the child's next fill, bounded. Locking: the gathered
    /// rows a batch at a time, by reference — they already live in the
    /// arena, so nothing is copied; the first call does the gather.
    pub fn next(
        self: *Materialize,
        ctx: *ExecCtx,
        batch: *Batch,
    ) anyerror!bool {
        if (!ctx.lock) return self.fill(ctx, batch);

        const rows = try self.gatherAndLockRows(ctx, batch);
        batch.reset();

        const n = batch.appendRefs(rows[self.pos..]);
        self.pos += n;
        return n > 0;
    }
};
