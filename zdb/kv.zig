// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const utils = @import("utils");
const String = utils.String;

/// How a value's bytes are encoded. Lives in Meta's high nibble.
///
/// Non-exhaustive on purpose: Meta is @bitCast from bytes we read off disk, so
/// every u4 must be a legal value. An exhaustive enum would make an unknown tag
/// illegal behavior at the bitCast — a panic in Debug, UB in ReleaseFast — on
/// data some future writer produced. Switches on this must carry an `else`.
pub const Encoding = enum(u4) {
    raw = 0,
    roaring = 1,
    _,
};

/// What a row version is, relative to the older versions of the same key.
/// Compaction consults this to decide how far down a key's history it must keep
/// (see Tree.buildCompaction).
///
/// Non-exhaustive for the same reason as Encoding: Meta is @bitCast from disk
/// bytes, so the fourth (unused) u2 value must stay legal. `state` is 0 so a
/// zeroed Meta is a self-contained row — the safe default. Switches must carry
/// an `else`.
pub const Kind = enum(u2) {
    /// A complete row: reading it needs nothing older. Once the read horizon
    /// passes it, every version beneath it is dead history.
    state = 0,
    /// A partial row layered on the versions below it. The nearest `state`
    /// (and any deltas between) must survive for this to be reconstructable, so
    /// compaction never seals a key on a delta.
    delta = 1,
    /// A tombstone: the key reads as absent from this version onward.
    deleted = 2,
    _,
};

/// One byte of metadata about a row, carried verbatim end-to-end: wire →
/// memtable → SST → compaction → read. Low nibble holds the row `kind`, high
/// nibble the value `encoding`.
///
/// `kind` is interpreted: `deleted` makes a key read as absent (Forest.get) and
/// lets bottom-level compaction forget it, while `state`/`delta` decide how much
/// version history that compaction must retain (Tree.buildCompaction). The
/// `encoding` nibble selects the value's format: `roaring` marks posting
/// values (zdb/validate.zig) — a zroar bitmap when kind=state, a RoaringDelta
/// message when kind=delta — folded at read (Forest.getBitmap) and rolled up
/// into synthetic states by compaction (Tree.zig Emitter).
///
/// Meta belongs to a version of a row (a Version), never to a Key. A Key
/// doubles as a bare position in keyspace — tree boundaries, iterator
/// bounds, block base keys — where "is this a tombstone" has no meaning.
/// Keeping meta off Key is what stops e.g. Tree.findSplitKey from minting a
/// boundary that claims to be deleted.
///
/// Zig packs LSB-first (u/hlc.zig relies on the same), so the bit positions
/// below are the on-disk positions. They are frozen: zdb/kv_test.zig asserts
/// them, and SSTs on disk depend on them.
pub const Meta = packed struct(u8) {
    kind: Kind = .state, // bits 0-1
    _reserved: u2 = 0, // bits 2-3
    encoding: Encoding = .raw, // bits 4-7

    /// A plain state row, raw value. The default for anything that doesn't care.
    pub const raw: Meta = .{};

    pub inline fn isState(self: Meta) bool {
        return self.kind == .state;
    }

    pub inline fn isDelta(self: Meta) bool {
        return self.kind == .delta;
    }

    pub inline fn isDeleted(self: Meta) bool {
        return self.kind == .deleted;
    }

    pub inline fn isRoaring(self: Meta) bool {
        return self.encoding == .roaring;
    }

    pub inline fn toByte(self: Meta) u8 {
        return @bitCast(self);
    }

    pub inline fn fromByte(b: u8) Meta {
        return @bitCast(b);
    }
};

/// The stamp of a transaction's own pending writes, above every
/// committed timestamp: they sort newest in any merge and shadow
/// nothing by accident, since a committed version can never carry it.
/// `Item.visibleAt` admits it at any read_ts. Only a txn's private
/// write set holds versions at it; the commit stamps them with the
/// real HLC.
pub const pending_ts: u64 = std.math.maxInt(u64);

/// One version of a row: when it was committed (ts), what it holds
/// (value), and what's true about it (meta).
pub const Version = struct {
    ts: u64,
    // No default. Every construction site is a place where silently defaulting
    // to .raw would drop a row's metadata, so let the compiler catch a new one.
    meta: Meta,
    value: String,
};

/// A row's versions, newest first. One version sits inline, so a
/// writer's item needs no array behind it; several are a slice into
/// storage the producer owns (an iterator's buffer, or a `dupe`).
pub const Versions = union(enum) {
    one: Version,
    many: []const Version,

    /// Every version, newest first; never empty. Borrows `self`, so
    /// the item must outlive the slice: call it on a named item, not
    /// on a temporary.
    pub fn slice(self: *const Versions) []const Version {
        return switch (self.*) {
            .one => |*ver| @as(*const [1]Version, ver)[0..],
            .many => |vers| vers,
        };
    }

    pub fn len(self: Versions) usize {
        return switch (self) {
            .one => 1,
            .many => |vers| vers.len,
        };
    }
};

/// One primary and its versions, newest first. `key` says where the
/// row lives (namespace, table_id, primary); `versions` is never
/// empty. A version's stamp is `versions[i].ts`.
///
/// An item from an iterator is a borrow: the versions live in the
/// iterator's buffer and the values in a block or arena it pins, both
/// valid until the following `next`. `dupe` for anything kept longer.
pub const Item = struct {
    key: Key,
    versions: Versions,

    pub fn newest(self: Item) Version {
        return switch (self.versions) {
            .one => |ver| ver,
            .many => |vers| vers[0],
        };
    }

    /// The versions at or below `ts`: the suffix from the first version
    /// with `ts <=` it. Null when every version is newer. A walk from
    /// the newest, not a binary search: reads are almost always at or
    /// near the newest version, so the first compare usually decides.
    /// The versions at or below `ts`, newest first — a pending one
    /// (`pending_ts`) always among them — or null when there is none.
    pub fn visibleAt(self: Item, ts: u64) ?Item {
        switch (self.versions) {
            .one => |ver| return if (visible(ver, ts)) self else null,
            .many => |vers| {
                for (vers, 0..) |ver, i| {
                    if (visible(ver, ts)) {
                        return .{
                            .key = self.key,
                            .versions = .{ .many = vers[i..] },
                        };
                    }
                }
                return null;
            },
        }
    }

    fn visible(ver: Version, ts: u64) bool {
        return ver.ts <= ts or ver.ts == pending_ts;
    }

    /// The chain a read folds: the versions down to and including the
    /// first that is not a delta (a state or a tombstone), or every
    /// version when none is.
    pub fn chain(self: Item) Item {
        const vers = switch (self.versions) {
            .one => return self,
            .many => |vers| vers,
        };
        for (vers, 0..) |ver, i| {
            if (!ver.meta.isDelta()) {
                return .{
                    .key = self.key,
                    .versions = .{ .many = vers[0 .. i + 1] },
                };
            }
        }
        return self;
    }

    /// A copy owned by `allocator`: the primary, every value, and the
    /// versions slice when there are several. Release with `free`.
    pub fn dupe(self: Item, allocator: std.mem.Allocator) !Item {
        const primary = try allocator.dupe(u8, self.key.primary);
        errdefer allocator.free(primary);
        const key: Key = .{
            .namespace = self.key.namespace,
            .table_id = self.key.table_id,
            .primary = primary,
        };

        switch (self.versions) {
            .one => |ver| {
                const value = try allocator.dupe(u8, ver.value);
                return .{ .key = key, .versions = .{ .one = .{
                    .ts = ver.ts,
                    .meta = ver.meta,
                    .value = value,
                } } };
            },
            .many => |src| {
                const vers = try allocator.alloc(Version, src.len);
                var duped: usize = 0;
                errdefer {
                    for (vers[0..duped]) |ver| allocator.free(ver.value);
                    allocator.free(vers);
                }
                for (src, vers) |from, *to| {
                    to.* = .{
                        .ts = from.ts,
                        .meta = from.meta,
                        .value = try allocator.dupe(u8, from.value),
                    };
                    duped += 1;
                }
                return .{ .key = key, .versions = .{ .many = vers } };
            },
        }
    }

    pub fn free(self: Item, allocator: std.mem.Allocator) void {
        allocator.free(self.key.primary);
        switch (self.versions) {
            .one => |ver| allocator.free(ver.value),
            .many => |vers| {
                for (vers) |ver| allocator.free(ver.value);
                allocator.free(vers);
            },
        }
    }
};

/// Global identity of an LSM tree: a namespace (u64, global) plus a
/// namespace-local tree_id. The tree_id packs `(table_id << 32) | split_id`
/// (see makeTreeId), so a tree's SQL table is derivable from it and is
/// never stored separately. Two namespaces can reuse the same tree_id, so the
/// namespace must always travel with it — this struct is the map key for both
/// `Forest.trees` and `Manifest.trees`.
pub const TreeKey = packed struct(u128) {
    namespace: u64,
    tree_id: u64,
};

/// Global identity of a table (one routing entry): a namespace (u64) plus a
/// namespace-local table_id (u32), muxed into a single u96. The map key for
/// `Forest.tables`. table_id 0 is the namespace's default (raw-KV)
/// table, so a namespace "exists" iff its `{namespace, 0}` key is present.
pub const TableKey = packed struct(u96) {
    namespace: u64,
    table_id: u32,

    /// Identity hash for lock-table (and future conflict-oracle)
    /// keys. Seed 1 keeps table keys disjoint from row identities
    /// (Key.identityHash, seed 0), so a table's lock can never collide
    /// with a row's.
    pub fn identityHash(self: TableKey) u64 {
        const v: u128 = @as(u96, @bitCast(self));
        return std.hash.XxHash3.hash(1, std.mem.asBytes(&v));
    }
};

/// Identity hash of a whole namespace — the database-level intent-lock
/// key. Seed 2 keeps it disjoint from row identities (Key.identityHash,
/// seed 0) and table identities (TableKey.identityHash, seed 1).
pub fn namespaceHash(namespace: u64) u64 {
    return std.hash.XxHash3.hash(2, std.mem.asBytes(&namespace));
}

/// Compose a namespace-local tree_id from its table (high 32 bits) and
/// split index (low 32 bits). split_id 0 is the table's first/only tree; it
/// grows by one on every split.
pub inline fn makeTreeId(table_id: u32, split_id: u32) u64 {
    return (@as(u64, table_id) << 32) | @as(u64, split_id);
}

/// The SQL table a tree belongs to (high 32 bits of its tree_id).
pub inline fn treeTableId(tree_id: u64) u32 {
    return @intCast(tree_id >> 32);
}

/// The split index of a tree within its table (low 32 bits of its tree_id).
pub inline fn treeSplitId(tree_id: u64) u32 {
    return @truncate(tree_id);
}

pub const Key = struct {
    /// Where a key sits relative to its primary. Stored keys are
    /// `exact`; the other bounds are query-side only, never stored or
    /// marshaled. `orderPrimary` places them; declaration order does
    /// not matter.
    pub const Bound = enum {
        /// An ordinary key, compared by its primary bytes.
        exact,
        /// Just past `primary`: sorts after every stored entry of that
        /// primary and before any longer one, so `[k, k.after()]` is
        /// exactly the entries of `k` with nothing copied.
        after,
        /// The two edges of one table, no primary: `table_left` sorts
        /// before every key of the same namespace and table_id and
        /// after any key of a lower one; `table_right` after every key
        /// of the table and before any key of a higher one. A
        /// whole-table range runs between them (`KeyRange.limitToTable`):
        /// a memtable holds every table's keys in one order and stops
        /// at them, while a tree, whose keys are all one table's, reads
        /// each as no bound on its side. Neither is ever stored nor
        /// equal to a stored key, so inclusive and exclusive do not
        /// apply to them, and they mean the same walking a range in
        /// either direction.
        table_left,
        table_right,
        /// No bound at all, on whichever side of a range it sits. Never
        /// compared: the range helpers check for it first. The stored
        /// form of an unbounded range side is empty bytes (`init`).
        inf,
    };

    namespace: u64 = 0,
    table_id: u32 = 0,
    primary: String = "",
    bound: Bound = .exact,

    /// The unbounded range side.
    pub const inf: Key = .{ .bound = .inf };

    pub inline fn isInf(self: Key) bool {
        return self.bound == .inf;
    }

    /// The left edge of `table_key`: below every key of the table.
    pub fn tableLeft(table_key: TableKey) Key {
        return .{
            .namespace = table_key.namespace,
            .table_id = table_key.table_id,
            .bound = .table_left,
        };
    }

    /// The right edge of `table_key`: past every key of the table.
    pub fn tableRight(table_key: TableKey) Key {
        return .{
            .namespace = table_key.namespace,
            .table_id = table_key.table_id,
            .bound = .table_right,
        };
    }

    /// Whether this is one of the table edges, which carry no primary.
    pub inline fn isTableEdge(self: Key) bool {
        return self.bound == .table_left or self.bound == .table_right;
    }

    /// The range end that admits only this key (`Bound.after`). Borrows
    /// `primary`; nothing is copied.
    pub fn after(self: Key) Key {
        std.debug.assert(self.bound == .exact);
        var k = self;
        k.bound = .after;
        return k;
    }

    /// The table this key belongs to.
    pub inline fn tableKey(self: Key) TableKey {
        return .{ .namespace = self.namespace, .table_id = self.table_id };
    }

    /// A key may be compared only if it is not `inf`. A table edge
    /// carries no primary and an exact or after key always has one:
    /// a zero key (`Key{}`) is uninitialized, not a bound.
    inline fn assertComparable(self: Key) void {
        switch (self.bound) {
            .exact, .after => std.debug.assert(self.primary.len > 0),
            .table_left, .table_right => {
                std.debug.assert(self.primary.len == 0);
            },
            .inf => unreachable,
        }
    }

    /// Where a key sits among its table's keys before primaries are
    /// looked at: the left edge, then every stored key, then the right
    /// edge.
    inline fn edgeRank(self: Key) u8 {
        return switch (self.bound) {
            .table_left => 0,
            .exact, .after => 1,
            .table_right => 2,
            .inf => unreachable,
        };
    }

    /// Parse a marshaled key: [ns:u64 BE][tid:u32 BE][primary]. A key
    /// names a row, not a version: a version's timestamp lives in
    /// Version.ts, and reads take their as-of timestamp separately.
    /// For keys without the prefix, construct Key directly via
    /// Key{ .primary = ... }.
    pub fn init(raw: String) Key {
        // Empty bytes are how a stored range writes an unbounded side.
        if (raw.len == 0) {
            return inf;
        }
        return .{
            .namespace = std.mem.readInt(u64, raw[0..8], .big),
            .table_id = std.mem.readInt(u32, raw[8..12], .big),
            .primary = raw[12..],
        };
    }

    pub fn initAlloc(allocator: std.mem.Allocator, raw: String) !Key {
        var k = init(raw);
        k.primary = try allocator.dupe(u8, k.primary);
        return k;
    }

    pub fn dupe(self: Key, allocator: std.mem.Allocator) !Key {
        var out = self;
        out.primary = try allocator.dupe(u8, self.primary);
        return out;
    }

    pub fn free(self: Key, allocator: std.mem.Allocator) void {
        if (self.primary.len > 0) {
            allocator.free(self.primary);
        }
    }

    /// The stored form: `[ns][tid][primary]` for an exact key, empty
    /// The longest primary an SST entry holds: the block entry stores
    /// key lengths as u16 (table/builder.zig). The SQL layer refuses a
    /// longer index key at the write; one that reaches the builder
    /// anyway is logged and dropped.
    pub const max_primary_len: usize = std.math.maxInt(u16);

    /// for `inf` (how a stored range writes an unbounded side). The
    /// other bounds are never stored.
    pub fn marshal(self: Key, allocator: std.mem.Allocator) !String {
        std.debug.assert(self.bound == .exact or self.bound == .inf);
        if (self.bound == .inf) {
            return allocator.alloc(u8, 0);
        }
        const buf = try allocator.alloc(u8, self.len());
        self.writeTo(buf);
        return buf;
    }

    pub fn marshalTo(
        self: Key,
        allocator: std.mem.Allocator,
        data: *std.ArrayList(u8),
    ) !String {
        std.debug.assert(self.bound == .exact or self.bound == .inf);
        if (self.bound == .inf) {
            return allocator.alloc(u8, 0);
        }
        const start = data.items.len;
        try data.appendNTimes(allocator, 0, self.len());
        const buf = data.items[start..];
        self.writeTo(buf);
        return buf;
    }

    /// Identity hash over (namespace, table_id, primary): all versions
    /// of a row share one identity. Row-lock key for fiber.SemaphoreMap
    /// (and the future conflict oracle). Seed 0; TableKey.identityHash
    /// uses seed 1 so the two key shapes can never collide.
    pub fn identityHash(self: Key) u64 {
        var h = std.hash.XxHash3.init(0);
        h.update(std.mem.asBytes(&self.namespace));
        h.update(std.mem.asBytes(&self.table_id));
        h.update(self.primary);
        return h.final();
    }

    fn writeTo(self: Key, buf: []u8) void {
        std.mem.writeInt(u64, buf[0..8], self.namespace, .big);
        std.mem.writeInt(u32, buf[8..12], self.table_id, .big);
        @memcpy(buf[12 .. 12 + self.primary.len], self.primary);
    }

    /// Namespace, then table_id, then primary bytes. ns and tid are
    /// fixed-width big-endian, so this is also the bytewise order of
    /// two marshaled keys.
    pub fn order(self: Key, other: Key) std.math.Order {
        const ns_ord = std.math.order(self.namespace, other.namespace);
        if (ns_ord != .eq) return ns_ord;
        const tid_ord = std.math.order(self.table_id, other.table_id);
        if (tid_ord != .eq) return tid_ord;
        return self.orderPrimary(other);
    }

    /// Primary bytes, then `after`, ignoring namespace and table_id: the
    /// order of two keys of one tree. An SST stores primaries only (its
    /// tree owns the prefix), so its bounds and block base keys are
    /// compared this way against a search key, which by contract is a
    /// key of that tree (`Tree.openMerge` asserts it).
    pub fn orderPrimary(self: Key, other: Key) std.math.Order {
        self.assertComparable();
        other.assertComparable();
        // The table edges sort outside every primary; among equal
        // primaries, `after` sorts past `exact`.
        const edge_ord = std.math.order(self.edgeRank(), other.edgeRank());
        if (edge_ord != .eq) return edge_ord;
        const p_ord = std.mem.order(u8, self.primary, other.primary);
        if (p_ord != .eq) return p_ord;
        return std.math.order(
            @intFromBool(self.bound == .after),
            @intFromBool(other.bound == .after),
        );
    }

    /// `order` as a predicate, so a bound check reads as the compare
    /// it means: `a.le(b)`, not `a.order(b) != .gt`.
    pub fn lt(self: Key, other: Key) bool {
        return self.order(other) == .lt;
    }
    pub fn le(self: Key, other: Key) bool {
        return self.order(other) != .gt;
    }
    pub fn eq(self: Key, other: Key) bool {
        return self.order(other) == .eq;
    }
    pub fn ge(self: Key, other: Key) bool {
        return self.order(other) != .lt;
    }
    pub fn gt(self: Key, other: Key) bool {
        return self.order(other) == .gt;
    }

    /// `orderPrimary` as a predicate, as `lt`..`gt` are for `order`.
    pub fn ltPrimary(self: Key, other: Key) bool {
        return self.orderPrimary(other) == .lt;
    }
    pub fn lePrimary(self: Key, other: Key) bool {
        return self.orderPrimary(other) != .gt;
    }
    pub fn eqPrimary(self: Key, other: Key) bool {
        return self.orderPrimary(other) == .eq;
    }
    pub fn gePrimary(self: Key, other: Key) bool {
        return self.orderPrimary(other) != .lt;
    }
    pub fn gtPrimary(self: Key, other: Key) bool {
        return self.orderPrimary(other) == .gt;
    }

    /// Length of the marshaled key: 0 for `inf`, the prefix plus the
    /// primary otherwise.
    pub inline fn len(self: Key) usize {
        if (self.bound == .inf) return 0;
        return 12 + self.primary.len;
    }
};

/// Which way an iterator walks a range `[left, right)`: `forward`
/// positions at the first key at or past `left` and steps up;
/// `reverse` positions at the last key below `right` and steps down.
/// The bounds mean the same either way, and so does a key's version
/// chain, which stays newest first.
pub const Direction = enum { forward, reverse };

/// A range of keys, `[left, right)`. A bound of `Key.inf` is no bound
/// on that side, so the default range is everything (`all`). There is
/// no empty range: a caller that accumulates one starts from null.
pub const KeyRange = struct {
    left: Key = Key.inf,
    right: Key = Key.inf,

    pub const all: KeyRange = .{};

    /// Whether `key` is inside `[left, right)`.
    pub fn contains(self: KeyRange, key: Key) bool {
        if (!self.left.isInf() and key.lt(self.left)) return false;
        if (!self.right.isInf() and key.ge(self.right)) return false;
        return true;
    }

    /// This range CLAMPED to one table's key window: intersected with
    /// the table's edges (`Key.tableLeft`, `Key.tableRight`). A left
    /// below the table (a lower namespace/table, or unbounded) is raised
    /// to the left edge; a right past the table (or unbounded) is
    /// lowered to the right edge; a left/right genuinely inside the
    /// table is kept. So a scan over the namespace-wide memtable stays
    /// within the table, and a tree of that table reads each edge as no
    /// bound on its side.
    pub fn limitToTable(self: KeyRange, table_key: TableKey) KeyRange {
        return self.intersect(.{
            .left = Key.tableLeft(table_key),
            .right = Key.tableRight(table_key),
        });
    }

    /// Deep copy: dupes the underlying primary bytes for both bounds
    /// using `allocator`. The original Keys may borrow from
    /// short-lived storage (e.g. manifest entries protected by a
    /// lock); duping into an allocator the caller controls keeps the
    /// range usable after that storage is released.
    pub fn dupe(self: KeyRange, allocator: std.mem.Allocator) !KeyRange {
        return .{
            .left = try self.left.dupe(allocator),
            .right = try self.right.dupe(allocator),
        };
    }

    pub fn overlapsWith(self: KeyRange, other: KeyRange) bool {
        // [other.left, other.right] ... [self.left, self.right]
        if (!other.right.isInf() and !self.left.isInf() and
            other.right.lt(self.left)) return false;
        // [self.left, self.right] ... [other.left, other.right]
        if (!self.right.isInf() and !other.left.isInf() and
            self.right.lt(other.left)) return false;
        return true;
    }

    /// Widen `self` to cover `other` too. An unbounded side absorbs.
    pub fn extend(self: *KeyRange, other: KeyRange) void {
        self.left = looser_left(self.left, other.left);
        self.right = looser_right(self.right, other.right);
    }

    /// Tighten `self` against `other` and return the overlap. An
    /// unbounded side is no constraint from that side, so this composes
    /// naturally with caller-supplied scan bounds and tree-stored ranges.
    pub fn intersect(self: KeyRange, other: KeyRange) KeyRange {
        return .{
            .left = tighter_left(self.left, other.left),
            .right = tighter_right(self.right, other.right),
        };
    }

    /// The "leftmost" lower bound (= min); an unbounded one wins.
    fn looser_left(a: Key, b: Key) Key {
        if (a.isInf() or b.isInf()) return Key.inf;
        return if (a.lt(b)) a else b;
    }

    /// The "rightmost" upper bound (= max); an unbounded one wins.
    fn looser_right(a: Key, b: Key) Key {
        if (a.isInf() or b.isInf()) return Key.inf;
        return if (a.gt(b)) a else b;
    }

    /// Pick the "rightmost" lower bound (= max). An unbounded one means
    /// no constraint — the OTHER bound wins.
    fn tighter_left(a: Key, b: Key) Key {
        if (a.isInf()) return b;
        if (b.isInf()) return a;
        return if (a.gt(b)) a else b;
    }

    /// Pick the "leftmost" upper bound (= min). An unbounded one means
    /// no constraint — the OTHER bound wins.
    fn tighter_right(a: Key, b: Key) Key {
        if (a.isInf()) return b;
        if (b.isInf()) return a;
        return if (a.lt(b)) a else b;
    }
};

/// Hash of a primary key alone: what a `bytes` table's SST filter is
/// built on and probed with (namespace and table are implied by the
/// table). NOT a row identity: use Key.identityHash for locks and
/// conflict tracking.
pub fn bloomHash(primary: String) u64 {
    return std.hash.Wyhash.hash(0, primary);
}
