//! S3-FIFO block cache for ZDB.
//!
//! Three FIFO queues: S (small, 10%), M (main, 90%), G (ghost, fixed count).
//!
//!   S  — admission filter; byte budget = 10% of total capacity.
//!   M  — proven-reuse entries; byte budget = 90% of total capacity.
//!   G  — key hashes of recently evicted S one-hit-wonders;
//!          capacity = @max(16, m_cap_bytes / 512) slots.
//!
//! Eviction triggers when s_bytes + entry_bytes > s_cap_bytes (for S) or
//! m_bytes + entry_bytes > m_cap_bytes (for M), where entry_bytes =
//! keySize(key) + val.len.  Ring buffers grow dynamically (doubling) when
//! all slots are occupied.
//!
//! - Cache(K)        — single shard, not thread-safe.
//! - ShardedCache(K) — N shards, each guarded by a Mutex; thread-safe.
//!
//! Values are ref-counted `*Value` structs. The cache incrRefs on put/get and
//! decrRefs on eviction/deinit. Callers must decrRef values returned by get.
//! For []const u8 keys, the cache takes ownership of the key on put.

const std = @import("std");
const utils = @import("utils");
const Allocator = std.mem.Allocator;

// Re-export the canonical refcounted buffer from utils. Kept under the old
// name so existing cache call sites (`cache_mod.Value.init`, `incrRef`, etc.)
// keep compiling without churn.
pub const Value = utils.RefValue;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn isSliceOfU8(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .pointer => |p| p.size == .slice and p.child == u8,
        else => false,
    };
}

fn hashKey(comptime K: type, key: K) u64 {
    if (comptime isSliceOfU8(K)) {
        return std.hash.Wyhash.hash(0, key);
    } else {
        var h = std.hash.Wyhash.init(0);
        std.hash.autoHash(&h, key);
        return h.final();
    }
}

/// Returns the byte footprint of a key: slice length for []const u8,
/// or @sizeOf(K) for scalar types.
fn keySize(comptime K: type, key: K) usize {
    if (comptime isSliceOfU8(K)) return key.len;
    return @sizeOf(K);
}

// ---------------------------------------------------------------------------
// Cache(K) — single shard, not thread-safe
// ---------------------------------------------------------------------------

pub fn Cache(comptime K: type) type {
    return struct {
        const Self = @This();

        /// Which queue holds an entry.
        const Tag = enum(u1) { s_queue, m_queue };

        /// Payload stored in the main hash map: queue tag, ring slot, value,
        /// and frequency bit.  Keeping val + freq in the map means `get` never
        /// touches the ring buffers.
        const Entry = struct {
            val: *Value,
            slot: usize,
            freq: std.atomic.Value(bool),
            tag: Tag,
        };

        /// HashMap specialised for string keys when K = []const u8.
        const Map = if (isSliceOfU8(K))
            std.StringHashMap(Entry)
        else
            std.AutoHashMap(K, Entry);

        // ----------------------------------------------------------------
        // Fields
        // ----------------------------------------------------------------

        allocator: Allocator,
        map: Map,
        ghost_set: std.AutoHashMap(u64, void),

        /// S ring: stores only keys; val + freq live in the map Entry.
        s_ring: std.ArrayList(K),
        s_head: usize, // absolute head counter
        s_tail: usize, // absolute tail counter
        s_len: usize, // live entry count in S
        s_cap_bytes: usize, // byte budget for S (10% of total capacity)
        s_bytes: usize, // current live bytes in S

        /// M ring: stores only keys; val + freq live in the map Entry.
        m_ring: std.ArrayList(K),
        m_head: usize,
        m_tail: usize,
        m_len: usize,
        m_cap_bytes: usize, // byte budget for M (90% of total capacity)
        m_bytes: usize, // current live bytes in M

        /// G ring (fixed size; stores only u64 hashes).
        g_ring: []u64,
        g_head: usize,
        g_tail: usize,
        g_len: usize,
        g_cap: usize,

        // ----------------------------------------------------------------
        // Public API
        // ----------------------------------------------------------------

        pub fn init(alloc: Allocator, capacity: usize) !Self {
            const s_cap_bytes = @max(1, capacity / 10);
            const m_cap_bytes = capacity - s_cap_bytes;
            const g_cap = @max(16, m_cap_bytes / 512);

            const s_ring_len = @max(16, s_cap_bytes / 4096);
            var s_ring = try std.ArrayList(K).initCapacity(alloc, s_ring_len);
            errdefer s_ring.deinit(alloc);
            s_ring.expandToCapacity();

            var m_ring = try std.ArrayList(K).initCapacity(alloc, 8 * s_ring_len);
            errdefer m_ring.deinit(alloc);
            m_ring.expandToCapacity();

            const g_ring = try alloc.alloc(u64, g_cap);
            errdefer alloc.free(g_ring);

            var map = Map.init(alloc);
            errdefer map.deinit();

            const ghost_set = std.AutoHashMap(u64, void).init(alloc);

            return Self{
                .allocator = alloc,
                .map = map,
                .ghost_set = ghost_set,
                .s_ring = s_ring,
                .s_head = 0,
                .s_tail = 0,
                .s_len = 0,
                .s_cap_bytes = s_cap_bytes,
                .s_bytes = 0,
                .m_ring = m_ring,
                .m_head = 0,
                .m_tail = 0,
                .m_len = 0,
                .m_cap_bytes = m_cap_bytes,
                .m_bytes = 0,
                .g_ring = g_ring,
                .g_head = 0,
                .g_tail = 0,
                .g_len = 0,
                .g_cap = g_cap,
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.map.iterator();
            while (it.next()) |kv| {
                self.freeKey(kv.key_ptr.*);
                kv.value_ptr.val.decrRef();
            }
            self.map.deinit();
            self.ghost_set.deinit();
            self.s_ring.deinit(self.allocator);
            self.m_ring.deinit(self.allocator);
            self.allocator.free(self.g_ring);
        }

        /// Returns the cached value for `key`, marking it as recently used.
        /// The returned *Value has been incrRef'd; the caller must decrRef
        /// when done.  Only touches the map; no ring access needed.
        pub fn get(self: *Self, key: K) ?*Value {
            const entry = self.map.getPtr(key) orelse return null;
            entry.freq.store(true, .release);
            return entry.val.incrRef();
        }

        /// Inserts `key` → `val` into the cache. The cache takes its own
        /// reference on `val` (incrRef); the caller retains their existing ref.
        /// Key ownership is transferred for []const u8 keys.
        /// On error, the key is freed (for []const u8) but val is NOT decrRef'd.
        pub fn put(self: *Self, key: K, val: *Value) !void {
            // Key already present: update value in-place, adjusting byte counts.
            if (self.map.getPtr(key)) |entry| {
                self.freeKey(key);

                const old_len = entry.val.data.len;
                entry.val.decrRef(); // release old
                _ = val.incrRef(); // cache holds new
                entry.val = val;
                switch (entry.tag) {
                    .s_queue => self.s_bytes = self.s_bytes - old_len + val.data.len,
                    .m_queue => self.m_bytes = self.m_bytes - old_len + val.data.len,
                }
                return;
            }

            const h = hashKey(K, key);
            if (self.ghost_set.contains(h)) {
                // Ghost hit: skip S, go directly to M.
                // pushMain takes ownership of a ref, so incrRef here.
                _ = val.incrRef();
                self.pushMain(key, val) catch |err| {
                    val.decrRef(); // undo incrRef on error
                    return err;
                };
                return;
            }

            // Regular S insertion with byte-based eviction.
            const entry_bytes = keySize(K, key) + val.data.len;
            while (self.s_bytes + entry_bytes > self.s_cap_bytes and self.s_len > 0) {
                self.evictSmall() catch |err| {
                    self.freeKey(key);
                    return err;
                };
            }
            if (self.s_len == self.s_ring.items.len) {
                self.growRing(.s_queue) catch |err| {
                    self.freeKey(key);
                    return err;
                };
            }
            const slot = self.s_head;
            _ = val.incrRef(); // cache's ref
            self.map.put(key, .{
                .tag = .s_queue,
                .slot = slot,
                .val = val,
                .freq = std.atomic.Value(bool).init(false),
            }) catch |err| {
                val.decrRef(); // undo cache's ref
                self.freeKey(key);
                return err;
            };
            self.s_ring.items[slot] = key;
            self.s_head = (self.s_head + 1) % self.s_ring.items.len;
            self.s_len += 1;
            self.s_bytes += entry_bytes;
        }

        /// Total live bytes in S + M.
        pub fn byteSize(self: *const Self) usize {
            return self.s_bytes + self.m_bytes;
        }

        /// Total live entry count (S + M).
        pub fn count(self: *const Self) usize {
            return self.s_len + self.m_len;
        }

        // ----------------------------------------------------------------
        // Internal helpers
        // ----------------------------------------------------------------

        /// Free a key if K = []const u8; no-op for scalar K.
        fn freeKey(self: *Self, key: K) void {
            if (comptime isSliceOfU8(K)) self.allocator.free(key);
        }

        /// Double the ring capacity. Entries at [tail..old_cap) stay in place.
        /// Only the wrapped portion [0..tail) must move to [old_cap..old_cap+tail)
        /// so the ring remains contiguous from tail → head:
        ///
        ///   Before (cap=8, tail=3):  [e5 e6 e7 | e0 e1 e2 e3 e4]
        ///                             0  1  2    3  4  5  6  7
        ///                                        ^tail
        ///
        ///   After  (cap=16):         [__ __ __ | e0 e1 e2 e3 e4 | e5 e6 e7 | __ __ __ __ __]
        ///                             0  1  2    3  4  5  6  7     8  9  10   11 .. 15
        ///                                        ^tail                        ^head=11
        ///
        /// Slots [tail..old_cap) are untouched. Only the `tail` relocated entries
        /// need map fixup via direct getPtr lookups (no full map iteration).
        fn growRing(self: *Self, comptime tag: Tag) !void {
            const ring = if (tag == .s_queue) &self.s_ring else &self.m_ring;
            const tail = if (tag == .s_queue) self.s_tail else self.m_tail;
            const old_cap = ring.items.len;

            // ensureTotalCapacity would grow by at least the new_capacity. I'm seeing it
            // grow beyond that because it grows super linearly.
            try ring.ensureTotalCapacity(self.allocator, old_cap * 2);
            ring.expandToCapacity();

            if (tail > 0) {
                @memcpy(ring.items[old_cap .. old_cap + tail], ring.items[0..tail]);
                for (old_cap..old_cap + tail) |new_slot| {
                    self.map.getPtr(ring.items[new_slot]).?.slot = new_slot;
                }
            }
            if (tag == .s_queue) {
                self.s_head = old_cap + tail;
            } else {
                self.m_head = old_cap + tail;
            }
        }

        fn pushGhost(self: *Self, h: u64) !void {
            if (self.g_cap == 0) return;
            if (self.g_len == self.g_cap) {
                const old = self.g_ring[self.g_tail];
                _ = self.ghost_set.remove(old);
                self.g_tail = (self.g_tail + 1) % self.g_cap;
                self.g_len -= 1;
            }
            self.g_ring[self.g_head] = h;
            try self.ghost_set.put(h, {});
            self.g_head = (self.g_head + 1) % self.g_cap;
            self.g_len += 1;
        }

        /// Push (key, val) into M, evicting first if M is over byte budget.
        /// Takes ownership of one ref on val AND key (for []const u8).
        /// On error, val is decrRef'd and key is freed.
        fn pushMain(self: *Self, key: K, val: *Value) !void {
            const entry_bytes = keySize(K, key) + val.data.len;
            while (self.m_bytes + entry_bytes > self.m_cap_bytes and self.m_len > 0)
                self.evictMain();

            if (self.m_len == self.m_ring.items.len) {
                self.growRing(.m_queue) catch |err| {
                    self.freeKey(key);
                    val.decrRef();
                    return err;
                };
            }
            const slot = self.m_head;
            self.map.put(key, .{
                .tag = .m_queue,
                .slot = slot,
                .val = val,
                .freq = std.atomic.Value(bool).init(false),
            }) catch |err| {
                val.decrRef();
                self.freeKey(key);
                return err;
            };
            self.m_ring.items[slot] = key;
            self.m_head = (self.m_head + 1) % self.m_ring.items.len;
            self.m_len += 1;
            self.m_bytes += entry_bytes;
        }

        /// Evict the oldest S entry.  freq=true → promote to M (transferring
        /// key ownership); freq=false → record hash in G and free key + value.
        fn evictSmall(self: *Self) !void {
            std.debug.assert(self.s_len > 0);
            const key = self.s_ring.items[self.s_tail];
            const entry = self.map.get(key).?;
            _ = self.map.remove(key);

            self.s_tail = (self.s_tail + 1) % self.s_ring.items.len;
            self.s_len -= 1;
            self.s_bytes -= keySize(K, key) + entry.val.data.len;
            if (entry.freq.load(.acquire)) {
                return self.pushMain(key, entry.val);
            } else {
                self.pushGhost(hashKey(K, key)) catch {}; // best-effort
                self.freeKey(key);
                entry.val.decrRef();
            }
        }

        /// Evict one entry from M.  Entries with freq=true are reinserted at
        /// the M head with freq reset to false; the loop continues until a
        /// freq=false entry is found and freed.
        fn evictMain(self: *Self) void {
            while (true) {
                std.debug.assert(self.m_len > 0);
                const key = self.m_ring.items[self.m_tail];
                const entry = self.map.getPtr(key).?;

                // Advance tail as we already considered this slot.
                self.m_tail = (self.m_tail + 1) % self.m_ring.items.len;
                self.m_len -= 1;
                if (entry.freq.load(.acquire)) {
                    // Reinsert at head with freq=false (no net byte change).
                    const new_slot = self.m_head;
                    self.m_ring.items[new_slot] = key;
                    entry.slot = new_slot;
                    entry.freq.store(false, .release);

                    self.m_head = (self.m_head + 1) % self.m_ring.items.len;
                    self.m_len += 1;
                    // m_bytes unchanged — try the next tail.
                } else {
                    self.m_bytes -= keySize(K, key) + entry.val.data.len;
                    entry.val.decrRef();
                    _ = self.map.remove(key);
                    self.freeKey(key);
                    return;
                }
            }
        }
    };
}

// ---------------------------------------------------------------------------
// ShardedCache(K) — N shards, thread-safe
// ---------------------------------------------------------------------------

pub fn ShardedCache(comptime K: type) type {
    return struct {
        const Self = @This();

        const Shard = struct {
            cache: Cache(K),
            lock: std.Thread.RwLock,
        };

        allocator: Allocator,
        shards: []Shard,

        pub fn init(alloc: Allocator, total_capacity: usize, shard_count: usize) !Self {
            std.debug.assert(shard_count > 0);
            const per = @max(total_capacity / shard_count, 2);
            const shards = try alloc.alloc(Shard, shard_count);
            errdefer alloc.free(shards);
            var n: usize = 0;
            errdefer for (shards[0..n]) |*s| s.cache.deinit();
            for (shards) |*s| {
                s.cache = try Cache(K).init(alloc, per);
                s.lock = .{};
                n += 1;
            }
            return Self{ .allocator = alloc, .shards = shards };
        }

        pub fn deinit(self: *Self) void {
            for (self.shards) |*s| s.cache.deinit();
            self.allocator.free(self.shards);
        }

        fn pickShard(self: *const Self, key: K) *Shard {
            return &self.shards[hashKey(K, key) % self.shards.len];
        }

        pub fn get(self: *Self, key: K) ?*Value {
            const s = self.pickShard(key);
            s.lock.lockShared();
            defer s.lock.unlockShared();
            return s.cache.get(key);
        }

        /// See `Cache.put` for ownership semantics.
        pub fn put(self: *Self, key: K, val: *Value) !void {
            const s = self.pickShard(key);
            s.lock.lock();
            defer s.lock.unlock();
            return s.cache.put(key, val);
        }

        /// Approximate total count across shards.
        pub fn count(self: *Self) usize {
            var total: usize = 0;
            for (self.shards) |*s| {
                s.lock.lockShared();
                defer s.lock.unlockShared();
                total += s.cache.count();
            }
            return total;
        }

        /// Approximate total byte size across shards.
        pub fn byteSize(self: *Self) usize {
            var total: usize = 0;
            for (self.shards) |*s| {
                s.lock.lockShared();
                defer s.lock.unlockShared();
                total += s.cache.byteSize();
            }
            return total;
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "Cache basic put/get round-trip" {
    var c = try Cache([]const u8).init(std.testing.allocator, 100);
    defer c.deinit();

    const val = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "hello"));
    defer val.decrRef();
    try c.put(
        try std.testing.allocator.dupe(u8, "key"),
        val,
    );
    const got = c.get("key") orelse return error.Missing;
    defer got.decrRef();
    try std.testing.expectEqualStrings("hello", got.data);
    try std.testing.expectEqual(@as(usize, 1), c.count());
}

test "Cache put/get round-trip with u64 key" {
    var c = try Cache(u64).init(std.testing.allocator, 100);
    defer c.deinit();

    const val = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "forty-two"));
    defer val.decrRef();
    try c.put(42, val);
    const got = c.get(42) orelse return error.Missing;
    defer got.decrRef();
    try std.testing.expectEqualStrings("forty-two", got.data);
    try std.testing.expectEqual(@as(?*Value, null), c.get(99));
}

test "Cache one-hit-wonder: evicted to ghost, not M" {
    // capacity=10 → s_cap_bytes=1, m_cap_bytes=9.
    // "kN"/"vN" entries are 4 bytes each.  S holds 1 entry before the byte
    // budget is exceeded and the next insert evicts it.
    var c = try Cache([]const u8).init(std.testing.allocator, 10);
    defer c.deinit();

    // k0 inserted, no get → freq stays false.
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v0"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k0"), v);
    }
    // k1 triggers eviction of k0 from S; freq=false → k0 goes to ghost.
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v1"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k1"), v);
    }

    try std.testing.expectEqual(@as(?*Value, null), c.get("k0")); // evicted
    if (c.get("k1")) |v| v.decrRef(); // still present
    try std.testing.expectEqual(@as(usize, 0), c.m_len); // k0 did NOT go to M
    const h0 = hashKey([]const u8, "k0");
    try std.testing.expect(c.ghost_set.contains(h0)); // hash in ghost
}

test "Cache reused key promoted S → M" {
    // capacity=10 → s_cap_bytes=1, m_cap_bytes=9.
    var c = try Cache([]const u8).init(std.testing.allocator, 10);
    defer c.deinit();

    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v0"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k0"), v);
    }
    if (c.get("k0")) |v| v.decrRef(); // sets freq=true

    // k1 evicts k0 from S; freq=true → k0 goes to M.
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v1"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k1"), v);
    }

    const got = c.get("k0") orelse return error.Missing; // now in M
    defer got.decrRef();
    try std.testing.expectEqualStrings("v0", got.data);
    try std.testing.expectEqual(@as(usize, 1), c.m_len);
}

test "Cache ghost re-entry goes directly to M" {
    // capacity=10 → s_cap_bytes=1, m_cap_bytes=9.
    var c = try Cache([]const u8).init(std.testing.allocator, 10);
    defer c.deinit();

    // Evict k0 to ghost (freq=false).
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v0"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k0"), v);
    }
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v1"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k1"), v); // evicts k0 → ghost
    }

    // Re-insert k0: ghost hit → should land in M directly.
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v0b"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k0"), v);
    }

    const got = c.get("k0") orelse return error.Missing;
    defer got.decrRef();
    try std.testing.expectEqualStrings("v0b", got.data);
    try std.testing.expectEqual(@as(usize, 1), c.m_len);
}

test "Cache S FIFO eviction order" {
    // capacity=100 → s_cap_bytes=10; "kN"/"vN" entries are 4 bytes each.
    // S holds 2 entries (2×4=8 ≤ 10) but evicts when a 3rd arrives (3×4=12 > 10).
    var c = try Cache([]const u8).init(std.testing.allocator, 100);
    defer c.deinit();

    // Insert four keys without any gets (freq stays false throughout).
    inline for (.{ "k0", "k1", "k2", "k3" }, .{ "v0", "v1", "v2", "v3" }) |k, vdata| {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, vdata));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, k), v);
    }

    // k0 and k1 were evicted (freq=false → ghost); k2 and k3 remain.
    try std.testing.expectEqual(@as(?*Value, null), c.get("k0"));
    try std.testing.expectEqual(@as(?*Value, null), c.get("k1"));
    if (c.get("k2")) |v| v.decrRef();
    if (c.get("k3")) |v| v.decrRef();
    try std.testing.expectEqual(@as(usize, 0), c.m_len); // no promotions
}

test "Cache M freq reset on reinsertion" {
    // capacity=10 → s_cap_bytes=1, m_cap_bytes=9.
    // "kN"/"vN" entries are 4 bytes each; M holds 2 entries (2×4=8 ≤ 9, 3×4=12 > 9).
    var c = try Cache([]const u8).init(std.testing.allocator, 10);
    defer c.deinit();

    // Promote k0 and k1 into M.
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v0"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k0"), v);
    }
    if (c.get("k0")) |v| v.decrRef(); // freq=true
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v1"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k1"), v); // k0 → M
    }
    if (c.get("k1")) |v| v.decrRef(); // freq=true in S
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v2"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k2"), v); // k1 → M; M has k0, k1
    }

    // Mark both M entries as freq=true.
    if (c.get("k0")) |v| v.decrRef();
    if (c.get("k1")) |v| v.decrRef();

    // Promote k2 into M: M full by bytes → evictM cycles past freq=true entries,
    // reinserting with freq=false, until one is evicted (k0 comes around first).
    if (c.get("k2")) |v| v.decrRef(); // freq=true in S
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v3"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k3"), v); // k2 → M; evicts k0
    }

    try std.testing.expectEqual(@as(?*Value, null), c.get("k0")); // evicted from M
    if (c.get("k1")) |v| v.decrRef();
    if (c.get("k2")) |v| v.decrRef();
}

test "Cache update existing key frees old value" {
    var c = try Cache([]const u8).init(std.testing.allocator, 100);
    defer c.deinit();

    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "old"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "key"), v);
    }
    // Second put: owned key is freed immediately (entry exists); only val is replaced.
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "new"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "key"), v);
    }

    const got = c.get("key") orelse return error.Missing;
    defer got.decrRef();
    try std.testing.expectEqualStrings("new", got.data);
    try std.testing.expectEqual(@as(usize, 1), c.count()); // still one entry
}

test "Cache byteSize reflects inserted bytes and respects capacity" {
    // capacity=1000 → s_cap_bytes=100, m_cap_bytes=900.
    var c = try Cache(u64).init(std.testing.allocator, 1000);
    defer c.deinit();

    // u64 key = 8 bytes; val "hi" = 2 bytes → entry_bytes = 10.
    {
        const v1 = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "hi"));
        defer v1.decrRef();
        try c.put(1, v1);
    }
    {
        const v2 = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "hi"));
        defer v2.decrRef();
        try c.put(2, v2);
    }
    try std.testing.expectEqual(@as(usize, 20), c.byteSize());

    // Insert 100 more entries, each 18 bytes (8 + 10).
    // 102 total × 18 = 1836 > 1000 → evictions must fire to stay under budget.
    for (3..103) |i| {
        const val = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "aaaaaaaaaa"));
        defer val.decrRef();
        try c.put(@intCast(i), val);
    }
    try std.testing.expect(c.byteSize() <= 1000);
}

test "ShardedCache basic get/put" {
    var sc = try ShardedCache([]const u8).init(std.testing.allocator, 200, 4);
    defer sc.deinit();

    const val = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "xval"));
    defer val.decrRef();
    try sc.put(
        try std.testing.allocator.dupe(u8, "x"),
        val,
    );
    const got = sc.get("x") orelse return error.Missing;
    defer got.decrRef();
    try std.testing.expectEqualStrings("xval", got.data);
}

test "ShardedCache concurrent puts and gets" {
    var gpa = std.heap.GeneralPurposeAllocator(.{ .thread_safe = true }){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const alloc = gpa.allocator();

    var sc = try ShardedCache([]const u8).init(alloc, 200, 4);
    defer sc.deinit();

    const Ctx = struct {
        sc: *ShardedCache([]const u8),
        alloc: Allocator,
    };

    // String literals have static storage; safe to use as cache keys across threads.
    const static_keys = [_][]const u8{ "aa", "bb", "cc", "dd", "ee", "ff" };

    const work = struct {
        fn run(ctx: Ctx) void {
            for (0..64) |i| {
                const key_lit = static_keys[i % static_keys.len];
                const key = ctx.alloc.dupe(u8, key_lit) catch continue;
                const data = ctx.alloc.dupe(u8, key_lit) catch {
                    ctx.alloc.free(key);
                    continue;
                };
                const val = Value.init(ctx.alloc, data) catch {
                    ctx.alloc.free(key);
                    continue;
                };
                defer val.decrRef();
                ctx.sc.put(key, val) catch {};
                if (ctx.sc.get(key_lit)) |v| v.decrRef();
            }
        }
    };

    var threads: [4]std.Thread = undefined;
    const ctx = Ctx{ .sc = &sc, .alloc = alloc };
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, work.run, .{ctx});
    for (threads) |t| t.join();
}

test "Cache ring growth preserves all entries" {
    // capacity=20000 → s_cap_bytes=2000, m_cap_bytes=18000.
    // u64 key (8 bytes) + ~6-byte val ≈ 14 bytes per entry.
    // S ring starts at 16 slots; 100 inserts trigger growth at 16, 32, 64.
    // Phase 2 inserts cause byte eviction of freq=true entries → promotion
    // to M, triggering M ring growth at 16, 32.
    const alloc = std.testing.allocator;
    var c = try Cache(u64).init(alloc, 20000);
    defer c.deinit();

    // HACK: Shrink their capacity to trigger the test condition.
    c.s_ring.shrinkRetainingCapacity(16);
    c.m_ring.shrinkRetainingCapacity(16 * 8);

    const s_ring_orig = c.s_ring.items.len;
    const m_ring_orig = c.m_ring.items.len;

    var next_key: u64 = 1;
    // Phase 1: fill S past 64 slots → S ring grows 16→32→64→128.
    for (0..1000) |_| {
        for (0..64) |_| {
            var buf: [20]u8 = undefined;
            const s = std.fmt.bufPrint(&buf, "val-{d}", .{next_key}) catch unreachable;
            const val = try Value.init(alloc, try alloc.dupe(u8, s));
            defer val.decrRef();
            try c.put(next_key, val);
            next_key += 1;
        }

        for (1..next_key) |key_i| {
            const got = c.get(@intCast(key_i)) orelse return error.Missing;
            defer got.decrRef();
            var buf: [20]u8 = undefined;
            const expected = std.fmt.bufPrint(&buf, "val-{d}", .{key_i}) catch unreachable;
            try std.testing.expectEqualStrings(expected, got.data);
        }

        std.debug.print("s_ring {d} -> {d} . m_ring {d} -> {d}\n", .{
            s_ring_orig,
            c.s_ring.items.len,
            m_ring_orig,
            c.m_ring.items.len,
        });
        if (c.s_ring.items.len > s_ring_orig and c.m_ring.items.len > m_ring_orig) {
            // pass
            return;
        }
    }
    try std.testing.expect(false); // fail -- the rings didn't increase in size
}
