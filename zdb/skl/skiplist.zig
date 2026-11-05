// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const Atomic = std.atomic.Value;
const Allocator = std.mem.Allocator;
const utils = @import("utils");
const String = utils.String;
const Key = @import("../kv.zig").Key;
const Item = @import("../kv.zig").Item;
const Version = @import("../kv.zig").Version;
const Direction = @import("../kv.zig").Direction;
const KeyRange = @import("../kv.zig").KeyRange;

pub const Error = error{
    OutOfMemory,
    ArenaFull,
};

pub const SkipList = struct {
    const Self = @This();
    const max_height = 16;
    const branch_probability = 4;
    const max_arena_size = 64 * 1024 * 1024;

    /// One version of a row, immutable once published. A node's
    /// version nodes form a chain newest first; `next` is the only
    /// field a writer ever stores after publication, and only to
    /// splice a new version node in behind this one.
    pub const VersionNode = struct {
        ver: Version,
        next: Atomic(?*VersionNode),
    };

    /// One row: the marshaled key without a timestamp ([ns][tid]
    /// [primary]) and its versions, a chain of version nodes from
    /// `vhead`, newest first. Nodes sort by key. `vhead` is never null
    /// once the node is linked in: a node is built with its first
    /// version node. `prev` is the level-0 neighbour below, the head
    /// for the first node and null for the head itself: a reverse walk
    /// steps through it as a forward one steps through `next[0]`.
    pub const Node = struct {
        key: String,
        vhead: Atomic(?*VersionNode),
        next: []Atomic(?*Node),
        prev: Atomic(?*Node),
    };

    // Concurrency contract:
    // - single writer calls `putItem`
    // - many readers may call `getVersion`/iterate
    // - ownership is refcounted via `acquire`/`release`
    //
    // Every pointer a reader follows (a node's tower slot, a node's
    // `vhead`, a version node's `next`) is stored with release ordering
    // after the target is fully written, and loaded with acquire, so a
    // reader never sees a half-built node or version node. Nothing is
    // mutated in place: an overwrite publishes a new version node and
    // leaves the old one where a reader parked on it can still walk on.
    /// Log index of the Raft `new_memtable` entry that created this
    /// memtable; also its identity (0 for the placeholder Forest.open
    /// creates before any replay). Flush proposals name the memtable by
    /// it.
    raft_index: u64,
    /// The lowest log index whose replay rebuilds this memtable: its
    /// `raft_index`, or lower once a branch copied versions written
    /// before it was created (Memtables.fork). The copies have no log
    /// entry of their own; replay remakes them only if the entries that
    /// wrote their sources replay too. The snapshot horizon and the
    /// recovery skip floor are the lowest over in-memory memtables
    /// (Memtables.oldestInMemoryLogIndex).
    replay_from: u64,
    allocator: Allocator,
    arena: std.heap.ArenaAllocator,
    head: *Node,
    max_height_seen: Atomic(u8),
    rng: std.Random.DefaultPrng,
    ref_count: Atomic(u32),
    node_count: Atomic(usize),
    /// Bytes written into this memtable: keys, nodes and their towers,
    /// version nodes and values. Used by `memoryUsage`/`shouldFreeze` instead
    /// of arena capacity: the arena reserves `max_arena_size` up front, so
    /// `queryCapacity()` reports the full reservation even when empty —
    /// which would make an empty memtable look "full" and trip the
    /// freeze threshold immediately.
    used_bytes: Atomic(usize),

    pub fn init(allocator: Allocator) !*Self {
        return initCapacity(allocator, max_arena_size);
    }

    /// `init` with a caller-chosen starting arena capacity. Pass 0 to
    /// skip the reservation and grow on demand — right for small private
    /// skiplists like a transaction's pending writes.
    pub fn initCapacity(allocator: Allocator, capacity: usize) !*Self {
        var arena = std.heap.ArenaAllocator.init(allocator);
        if (capacity > 0) {
            // Reserve the first chunk at `capacity` bytes without
            // touching it. Through the raw entry point: `Allocator.alloc`
            // fills what it returns with `undefined`, which a safe build
            // writes out as 64 MiB of 0xAA, faulting in every page while
            // the swap holds the tree lock. Untouched, the pages fault in
            // as the entries arrive instead.
            _ = arena.allocator().rawAlloc(
                capacity,
                std.mem.Alignment.fromByteUnits(1),
                @returnAddress(),
            ) orelse return error.OutOfMemory;
            _ = arena.reset(.retain_capacity);
        }

        const next_arr = try arena.allocator().alloc(Atomic(?*Node), max_height);
        for (next_arr) |*p| p.* = Atomic(?*Node).init(null);
        const head = try arena.allocator().create(Node);
        head.* = .{
            .key = &[_]u8{},
            .vhead = Atomic(?*VersionNode).init(null),
            .next = next_arr,
            .prev = Atomic(?*Node).init(null),
        };

        const self = try allocator.create(Self);
        self.* = .{
            .raft_index = 0,
            .replay_from = 0,
            .allocator = allocator,
            .arena = arena,
            .head = head,
            .max_height_seen = Atomic(u8).init(1),
            // Fixed seed: Heights only shape lookup cost, so
            // every memtable drawing the same sequence costs nothing.
            .rng = std.Random.DefaultPrng.init(0x5eed_5eed_5eed_5eed),
            .ref_count = Atomic(u32).init(1),
            .node_count = Atomic(usize).init(0),
            .used_bytes = Atomic(usize).init(0),
        };
        return self;
    }

    pub fn acquire(self: *Self) void {
        const prev = self.ref_count.fetchAdd(1, .acquire);
        std.debug.assert(prev > 0);
    }

    pub fn release(self: *Self) void {
        const old_count = self.ref_count.fetchSub(1, .release);
        std.debug.assert(old_count > 0);
        if (old_count == 1) {
            const allocator = self.allocator;
            self.arena.deinit();
            allocator.destroy(self);
        }
    }

    fn randomHeight(self: *Self) u8 {
        var height: u8 = 1;
        const rand = self.rng.random();
        while (height < max_height and rand.uintLessThan(u32, branch_probability) == 0) {
            height += 1;
        }
        return height;
    }

    const FindResult = struct {
        node: ?*Node,
        exact: bool,
    };

    fn findNode(
        self: *Self,
        search_key: Key,
        lefties: ?*[max_height]?*Node,
    ) FindResult {
        var current = self.head;
        var level: i32 = @intCast(self.max_height_seen.load(.acquire) - 1);

        while (level >= 0) {
            const level_idx: usize = @intCast(level);
            // This loop always stops at the last node in the level, where
            // next.key > given key and current.key < given key. If ever
            // next.key == given key, we'd return the node and mark it as
            // an exact match.
            // The lefties are useful when we want to add the node to the
            // list. But if we find an exact match, we don't care that
            // lefties isn't being fully filled because we're returning
            // earlier (cmp == .eq).
            while (current.next[level_idx].load(.acquire)) |next| {
                const cmp = Key.init(next.key).order(search_key);
                if (cmp == .gt) break;
                if (cmp == .eq) return .{ .node = next, .exact = true };
                current = next;
            }

            if (lefties) |u| u[level_idx] = current;
            level -= 1;
        }

        return .{
            .node = current.next[0].load(.acquire),
            .exact = false,
        };
    }

    /// The node holding the largest key, or null for an empty list:
    /// right along each level from the top, then down.
    fn lastNode(self: *Self) ?*Node {
        var current = self.head;
        var level: usize = self.max_height_seen.load(.acquire) - 1;
        while (true) {
            while (current.next[level].load(.acquire)) |next| current = next;
            if (level == 0) break;
            level -= 1;
        }
        return if (current == self.head) null else current;
    }

    /// The newest version of `key` at or below `ts`, or null when the key
    /// has no such version. The returned value points into arena memory:
    /// the caller must keep the skiplist alive (via acquire/release) while
    /// using it, or dupe it.
    pub fn getVersion(self: *Self, key: Key, ts: u64) ?Version {
        // An exact match compares the whole key: the memtable is global
        // across namespaces and tables, and a miss lands on the next key.
        const result = self.findNode(key, null);
        if (!result.exact) return null;
        var cur = result.node.?.vhead.load(.acquire);
        while (cur) |vnode| : (cur = vnode.next.load(.acquire)) {
            if (vnode.ver.ts <= ts) return vnode.ver;
        }
        return null;
    }

    /// Convenience for callers with nothing to say about the row: raw
    /// encoding, no flags. The write path uses putItem.
    pub fn put(self: *Self, key: Key, ts: u64, value: String) Error!void {
        return self.putItem(.{
            .key = key,
            .versions = .{ .one = .{ .ts = ts, .meta = .{}, .value = value } },
        });
    }

    /// Write every version of `item`. A version at a timestamp the key
    /// already holds replaces that one; any other goes into the chain at
    /// its timestamp, whatever order versions arrive in.
    pub fn putItem(self: *Self, item: Item) Error!void {
        var lefties = [_]?*Node{null} ** max_height;
        const found = self.findNode(item.key, &lefties);
        var vers = item.versions.slice();
        var node: *Node = undefined;
        if (found.exact) {
            node = found.node.?;
        } else {
            node = try self.newNode(item.key, vers[0], &lefties);
            vers = vers[1..];
        }
        for (vers) |ver| try self.insertVersionNode(node, ver);
    }

    /// A version node for `ver`, its value copied into the arena,
    /// unpublished.
    fn newVersionNode(self: *Self, ver: Version) Error!*VersionNode {
        const alloc = self.arena.allocator();
        const val_own = alloc.dupe(u8, ver.value) catch return Error.ArenaFull;
        const vnode = try alloc.create(VersionNode);
        vnode.* = .{
            .ver = .{ .ts = ver.ts, .meta = ver.meta, .value = val_own },
            .next = Atomic(?*VersionNode).init(null),
        };
        const vnode_bytes = @sizeOf(VersionNode) + val_own.len;
        _ = self.used_bytes.fetchAdd(vnode_bytes, .monotonic);
        return vnode;
    }

    /// Splice a version node for `ver` into `node`'s chain at its
    /// timestamp. The chain is walked to the first version node at or
    /// below `ver.ts`: an equal one is replaced (the new version node
    /// takes its `next`, the old one stays in the arena for any reader
    /// parked on it), else the new version node goes in front of it.
    /// One release store publishes it, at the predecessor's `next` or
    /// at `vhead`.
    fn insertVersionNode(self: *Self, node: *Node, ver: Version) Error!void {
        const new_vnode = try self.newVersionNode(ver);

        var prev: ?*VersionNode = null;
        var cur = node.vhead.load(.acquire);
        while (cur) |vnode| : (cur = vnode.next.load(.acquire)) {
            if (vnode.ver.ts <= ver.ts) break;
            prev = vnode;
        }
        const next: ?*VersionNode = if (cur) |vnode|
            (if (vnode.ver.ts == ver.ts) vnode.next.load(.acquire) else vnode)
        else
            null;
        new_vnode.next = Atomic(?*VersionNode).init(next);

        if (prev) |p| {
            p.next.store(new_vnode, .release);
        } else {
            node.vhead.store(new_vnode, .release);
        }
    }

    /// A node for `key` holding `first` as its only version node,
    /// linked into the tower between `lefties` and their successors.
    /// The version node is in place before the node is published, so a
    /// reader never sees a node without a version.
    fn newNode(
        self: *Self,
        key: Key,
        first: Version,
        lefties: *[max_height]?*Node,
    ) Error!*Node {
        const alloc = self.arena.allocator();
        const vnode = try self.newVersionNode(first);
        const key_own = key.marshal(alloc) catch return Error.ArenaFull;

        const height = self.randomHeight();
        const next_arr = try alloc.alloc(Atomic(?*Node), height);
        for (next_arr) |*p| p.* = Atomic(?*Node).init(null);
        const new_node = try alloc.create(Node);
        new_node.* = .{
            .key = key_own,
            .vhead = Atomic(?*VersionNode).init(vnode),
            .next = next_arr,
            .prev = Atomic(?*Node).init(lefties[0] orelse self.head),
        };

        // We expect multiple readers, but only expect one writer. So, we
        // can just do a load and store, without worrying about the value
        // changing since we loaded it.
        if (height > self.max_height_seen.load(.acquire)) {
            self.max_height_seen.store(height, .release);
        }

        // Here we're inserting new_node between lefties[i] and it's next.
        // If lefties[i] is null, then we're using head[i] as the lefties[i].
        for (0..height) |level| {
            const next = if (lefties[level]) |u| u.next[level].load(.acquire) else null;
            new_node.next[level].store(next, .release);

            if (lefties[level]) |left| {
                left.next[level].store(new_node, .release);
            } else {
                self.head.next[level].store(new_node, .release);
            }
        }
        // The successor's `prev` is swung after the node is published:
        // a reverse reader on the successor sees the chain before or
        // after the insert, both complete, as a forward reader does.
        if (new_node.next[0].load(.acquire)) |next| {
            next.prev.store(new_node, .release);
        }

        _ = self.node_count.fetchAdd(1, .monotonic);
        const tower = height * @sizeOf(Atomic(?*Node));
        const node_bytes = key_own.len + @sizeOf(Node) + tower;
        _ = self.used_bytes.fetchAdd(node_bytes, .monotonic);
        return new_node;
    }

    pub fn contains(self: *Self, key: Key, ts: u64) bool {
        return self.getVersion(key, ts) != null;
    }

    /// The number of keys, whatever their version count.
    pub fn numNodes(self: *Self) usize {
        return self.node_count.load(.monotonic);
    }

    pub fn memoryUsage(self: *Self) usize {
        // Bytes written, tracked on every put. We deliberately do NOT use
        // `arena.queryCapacity()`: the arena reserves `max_arena_size`
        // up front, so an empty memtable would report the full
        // 64 MiB reservation and trip `shouldFreeze` immediately, spinning
        // the newMemtable loop forever.
        return self.used_bytes.load(.monotonic);
    }

    pub fn memoryCapacity(self: *Self) usize {
        return self.arena.queryCapacity();
    }

    pub fn iterator(self: *Self, allocator: Allocator) !Iterator {
        return Iterator.init(allocator, self, .{}, .forward);
    }
};

/// Iterator over a SkipList with a peek/advance API matching the merge
/// iterator (see zdb/table/merge_iterator.zig): one item per key, its
/// versions newest first in a buffer the iterator owns, refilled at
/// every position and valid until the next `next`. The values stay in
/// the arena. `acquire`s the underlying skiplist on init; `release`s on
/// deinit. Safe to hand across the lifetime of a flush completion: as
/// long as the iterator holds a ref, the arena stays mapped even after
/// the immutable's owning slot in `Memtables` is popped. `dir` is
/// fixed at init: which way `seekRange` positions and `next` steps;
/// the range bounds mean the same either way.
pub const Iterator = struct {
    allocator: Allocator,
    skiplist: *SkipList,
    node: ?*SkipList.Node,
    vers: std.ArrayList(Version) = .empty,
    dir: Direction,
    /// Kept from `seekRange`: the walk ends at the bound ahead of it.
    range: KeyRange = .all,
    limit: Limit,

    /// How much of a key's chain a reader needs. `fill` walks the
    /// chain newest first, skips versions above `read_ts`, and stops
    /// after the version that both completes a fold (a state or a
    /// tombstone, not a delta) and brings the count to `want`. The
    /// default copies everything: what a flush or a compaction needs.
    /// A hot row carries thousands of versions per memtable lifetime;
    /// a read wants one or two of them.
    pub const Limit = struct {
        read_ts: u64 = std.math.maxInt(u64),
        want: u32 = std.math.maxInt(u32),
    };

    pub fn init(
        allocator: Allocator,
        skiplist: *SkipList,
        limit: Limit,
        dir: Direction,
    ) !Iterator {
        skiplist.acquire();
        var it: Iterator = .{
            .allocator = allocator,
            .skiplist = skiplist,
            .node = switch (dir) {
                .forward => skiplist.head.next[0].load(.acquire),
                .reverse => skiplist.lastNode(),
            },
            .dir = dir,
            .limit = limit,
        };
        errdefer it.deinit();
        try it.fill();
        return it;
    }

    pub fn deinit(self: *Iterator) void {
        self.vers.deinit(self.allocator);
        self.skiplist.release();
    }

    /// Position at the first key of `[start, end)` in the iterator's
    /// direction: the smallest at or past `start` forward, the largest
    /// below `end` in reverse. `Key.inf` on a side is no bound there.
    pub fn seekRange(self: *Iterator, start: Key, end: Key) !void {
        self.range = .{ .left = start, .right = end };
        switch (self.dir) {
            .forward => if (start.isInf()) {
                self.node = self.skiplist.head.next[0].load(.acquire);
            } else {
                self.node = self.skiplist.findNode(start, null).node;
            },
            .reverse => if (end.isInf()) {
                self.node = self.skiplist.lastNode();
            } else {
                // An exact hit is `end` itself, excluded: its `prev` is
                // the last key below. A miss leaves the level-0
                // predecessor in `lefties`, the head when there is none.
                var lefties = [_]?*SkipList.Node{null} ** SkipList.max_height;
                const found = self.skiplist.findNode(end, &lefties);
                self.node = if (found.exact)
                    found.node.?.prev.load(.acquire)
                else
                    lefties[0];
                self.dropHead();
            },
        }
        self.checkBound();
        try self.fill();
    }

    pub fn itemRef(self: *const Iterator) ?Item {
        const node = self.node orelse return null;
        return .{
            .key = Key.init(node.key),
            .versions = .{ .many = self.vers.items },
        };
    }

    pub fn next(self: *Iterator) !void {
        const node = self.node orelse return;
        switch (self.dir) {
            .forward => self.node = node.next[0].load(.acquire),
            .reverse => {
                self.node = node.prev.load(.acquire);
                self.dropHead();
            },
        }
        self.checkBound();
        try self.fill();
    }

    /// The head is the first node's `prev`: reaching it is the end.
    fn dropHead(self: *Iterator) void {
        if (self.node == self.skiplist.head) self.node = null;
    }

    /// The walk is over once it leaves the range. Only the bound ahead
    /// can fail: the seek landed inside the one behind.
    fn checkBound(self: *Iterator) void {
        const node = self.node orelse return;
        if (!self.range.contains(Key.init(node.key))) self.node = null;
    }

    /// Copy the current node's chain into `vers`. A writer may publish
    /// a version node while this walks; either the chain before or the
    /// chain after is seen, both complete.
    fn fill(self: *Iterator) !void {
        utils.clearRetainCap(&self.vers);
        const node = self.node orelse return;
        const lim = self.limit;
        var cur = node.vhead.load(.acquire);
        while (cur) |vnode| : (cur = vnode.next.load(.acquire)) {
            const ver = vnode.ver;
            if (ver.ts > lim.read_ts) continue;
            try self.vers.append(self.allocator, ver);
            if (self.vers.items.len >= lim.want and !ver.meta.isDelta()) break;
        }
    }
};
