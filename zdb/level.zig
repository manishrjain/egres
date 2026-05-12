const std = @import("std");
const Key = @import("key.zig").Key;
const table = @import("table.zig");
const vfs = @import("vfs");
const utils = @import("utils");

const Table = table.Table;
const TableIterator = table.TableIterator;
const Item = table.table.Item;

pub const Error = table.table.Error;

// ---------------------------------------------------------------------------
// KeyRange
// ---------------------------------------------------------------------------

pub const KeyRange = struct {
    left: Key = .{},
    right: Key = .{},
    inf: bool = false,

    pub fn isEmpty(self: KeyRange) bool {
        return self.left.primary.len == 0 and self.right.primary.len == 0 and !self.inf;
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
            .inf = self.inf,
        };
    }

    pub fn overlapsWith(self: KeyRange, other: KeyRange) bool {
        if (self.isEmpty()) return true;
        if (other.isEmpty()) return false;
        if (self.inf or other.inf) return true;
        // [other.left, other.right] ... [self.left, self.right]
        if (other.right.order(self.left) == .lt) return false;
        // [self.left, self.right] ... [other.left, other.right]
        if (self.right.order(other.left) == .lt) return false;
        return true;
    }

    pub fn extend(self: *KeyRange, other: KeyRange) void {
        if (other.isEmpty()) return;
        if (self.isEmpty()) {
            self.* = other;
            return;
        }
        if (self.left.primary.len == 0 or other.left.order(self.left) == .lt) {
            self.left = other.left;
        }
        if (self.right.primary.len == 0 or other.right.order(self.right) == .gt) {
            self.right = other.right;
        }
        if (other.inf) self.inf = true;
    }

    /// Tighten `self` against `other` and return the overlap. Empty
    /// primary on either bound is treated as "open" (no constraint
    /// from that side), so this composes naturally with caller-supplied
    /// open scan bounds and tree-stored ranges. `inf` survives only
    /// when BOTH sides are unbounded after tightening.
    pub fn intersect(self: KeyRange, other: KeyRange) KeyRange {
        const lhs = tighter_left(self.left, other.left);
        const rhs = tighter_right(self.right, other.right);
        return .{
            .left = lhs,
            .right = rhs,
            .inf = lhs.primary.len == 0 and rhs.primary.len == 0,
        };
    }

    /// Pick the "rightmost" lower bound (= max). An empty primary
    /// means "unbounded on the left" — the OTHER bound wins.
    fn tighter_left(a: Key, b: Key) Key {
        if (a.primary.len == 0) return b;
        if (b.primary.len == 0) return a;
        return if (a.order(b) == .gt) a else b;
    }

    /// Pick the "leftmost" upper bound (= min). An empty primary means
    /// "unbounded on the right" — the OTHER bound wins.
    fn tighter_right(a: Key, b: Key) Key {
        if (a.primary.len == 0) return b;
        if (b.primary.len == 0) return a;
        return if (a.order(b) == .lt) a else b;
    }
};

// ---------------------------------------------------------------------------
// TableEntry
// ---------------------------------------------------------------------------

pub const TableEntry = struct {
    id: u64,
    table: *Table,
};

// ---------------------------------------------------------------------------
// Level
// ---------------------------------------------------------------------------

pub const Level = struct {
    mu: std.Thread.RwLock,
    tables: std.ArrayList(TableEntry),
    total_size: i64, // sum of each table's buf.len
    level: u8,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, level: u8) Level {
        return .{
            .mu = .{},
            .tables = .{},
            .total_size = 0,
            .level = level,
            .allocator = allocator,
        };
    }

    /// deinit releases each table's reference without deleting SST files.
    /// Used by Tree.close(); detaches the path so decrRef at
    /// zero only frees memory.
    pub fn deinit(self: *Level) void {
        for (self.tables.items) |e| {
            e.table.detach(); // clear path → decrRef won't delete the file
            e.table.decrRef();
        }
        self.tables.deinit(self.allocator);
    }

    /// initEntries bulk-sets entries, sorts them, and recomputes total_size.
    pub fn initEntries(self: *Level, new_entries: []const TableEntry) !void {
        self.mu.lock();
        defer self.mu.unlock();

        self.tables.clearRetainingCapacity();
        try self.tables.appendSlice(self.allocator, new_entries);
        self.sortEntries();

        self.total_size = 0;
        for (self.tables.items) |e| {
            self.total_size += @intCast(e.table.fileSize());
        }
    }

    /// sortEntries: L0 → by id ascending; L≥1 → by table.smallest key.
    /// L0 tables would overlap. Newer tables would have higher ids and would be at the end.
    /// L1 and onwards would have non-overlapping tables.
    fn sortEntries(self: *Level) void {
        if (self.level == 0) {
            std.mem.sort(TableEntry, self.tables.items, {}, struct {
                fn lt(_: void, a: TableEntry, b: TableEntry) bool {
                    return a.id < b.id;
                }
            }.lt);
        } else {
            std.mem.sort(TableEntry, self.tables.items, {}, struct {
                fn lt(_: void, a: TableEntry, b: TableEntry) bool {
                    return a.table.smallest.order(b.table.smallest) == .lt;
                }
            }.lt);
        }
    }

    /// addEntry appends an entry, sorts, and increments the table's ref count.
    pub fn addEntry(self: *Level, id: u64, t: *Table) !void {
        self.mu.lock();
        defer self.mu.unlock();

        try self.tables.append(self.allocator, .{ .id = id, .table = t });
        t.incrRef();
        self.total_size += @intCast(t.fileSize());
        self.sortEntries();
    }

    /// deleteEntry removes a single entry by id and decrements the removed table's ref.
    pub fn deleteEntry(self: *Level, id: u64) void {
        self.mu.lock();
        defer self.mu.unlock();

        return self.deleteEntriesLocked(&.{.{ .id = id, .table = undefined }});
    }

    fn deleteEntriesLocked(self: *Level, to_del: []const TableEntry) void {
        // Iterate over the full list of tables we own, and see for each table whether
        // it's id is present in the ids list provided.
        var i: usize = self.tables.items.len;
        while (i > 0) {
            i -= 1;
            const e = self.tables.items[i];
            // I think looping here is fine, given we expect ids to be small.
            // We can consider storing ids in a map as a micro-optimization.
            const found = for (to_del) |entry| {
                if (e.id == entry.id) break true;
            } else false;

            if (found) {
                // TableEntry e is on the chopping board.
                self.total_size -= @intCast(e.table.fileSize());
                e.table.decrRef();
                _ = self.tables.orderedRemove(i);
            }
        }
    }

    /// replaceEntries is write-locked: deletes del_ids, appends to_add, then sorts.
    pub fn replaceEntries(self: *Level, to_del: []const TableEntry, to_add: []const TableEntry) !void {
        self.mu.lock();
        defer self.mu.unlock();

        self.deleteEntriesLocked(to_del);

        // Add new entries.
        for (to_add) |e| {
            try self.tables.append(self.allocator, e);
            e.table.incrRef();
            self.total_size += @intCast(e.table.fileSize());
        }

        self.sortEntries();
    }

    /// overlappingTables returns a heap-allocated slice of TableEntry whose key
    /// range overlaps kr. The caller must free the slice when done.
    /// L0: always all tables; L≥1: binary search on both ends.
    pub fn overlappingTables(self: *Level, allocator: std.mem.Allocator, kr: KeyRange) ![]TableEntry {
        self.mu.lockShared();
        defer self.mu.unlockShared();

        const items = self.tables.items;
        const slice: []TableEntry = if (self.level == 0)
            items
        else if (kr.isEmpty())
            items[0..0]
        else blk: {
            // First table where kr.left <= biggest (could overlap on the left).
            const left = utils.search(TableEntry, items, kr.left, struct {
                fn f(kl: Key, e: TableEntry) bool {
                    return kl.order(e.table.biggest) != .gt;
                }
            }.f);
            // First table where kr.right < smallest (no longer overlapping).
            // An empty kr.right means "no upper bound" — sweep to end.
            // Without this, the binary search would treat the empty primary
            // as smaller than every table's smallest key and return 0,
            // making the slice empty even when the left bound is well-formed.
            const right = if (kr.right.primary.len == 0)
                items.len
            else
                utils.search(TableEntry, items, kr.right, struct {
                    fn f(right_key: Key, e: TableEntry) bool {
                        return right_key.order(e.table.smallest) == .lt;
                    }
                }.f);
            break :blk items[left..right];
        };

        return allocator.dupe(TableEntry, slice);
    }

    pub fn getTotalSize(self: *Level) i64 {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.total_size;
    }

    pub fn numTables(self: *Level) usize {
        self.mu.lockShared();
        defer self.mu.unlockShared();
        return self.tables.items.len;
    }

    /// get seeks search_key within this level's tables.
    /// Returns an Item with owned primary + value on hit, or null on miss.
    /// Caller must free item with self.allocator.
    ///
    /// L0: back-to-front (highest id = newest); first match wins.
    /// L≥1: binary search for the one table that could contain the key.
    pub fn get(self: *Level, search_key: Key, priority: vfs.Priority) Error!?Item {
        if (self.level == 0) {
            self.mu.lockShared();
            defer self.mu.unlockShared();

            var ri: usize = self.tables.items.len;
            while (ri > 0) {
                ri -= 1;
                const e = self.tables.items[ri];
                if (!e.table.containsKey(search_key)) continue;
                var it = TableIterator.init(self.allocator, e.table, priority);
                defer it.deinit();

                try it.seekRange(search_key, .{});
                const found = it.itemRef() orelse continue;
                if (!search_key.equalPrimary(found.key)) continue;

                return try found.dupe(self.allocator);
            }
            return null;
        } else {
            self.mu.lockShared();
            defer self.mu.unlockShared();

            // First table whose biggest key >= search_key.
            const lo = utils.search(TableEntry, self.tables.items, search_key, struct {
                fn f(sk: Key, e: TableEntry) bool {
                    return e.table.biggest.order(sk) != .lt;
                }
            }.f);
            if (lo >= self.tables.items.len) return null;

            const e = self.tables.items[lo];

            // Verify the key can actually be in this table. Compare without considering timestamp.
            // A table whose smallest primary is greater than the search primary cannot contain the
            // key at any timestamp. Note that full Key.order includes reversed timestamp which
            // would give wrong results when the search key uses timestamp = maxInt as a "find any
            // version" sentinel. So, we don't use "order". Instead use "orderWithoutTs".
            if (e.table.smallest.orderWithoutTs(search_key) == .gt) return null;
            if (!e.table.containsKey(search_key)) return null;

            var it = TableIterator.init(self.allocator, e.table, priority);
            defer it.deinit();

            try it.seekRange(search_key, .{});
            const found = it.itemRef() orelse return null;
            if (!search_key.equalPrimary(found.key)) return null;

            return try found.dupe(self.allocator);
        }
    }
};
