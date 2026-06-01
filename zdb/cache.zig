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
const fiber = @import("fiber");
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

pub fn hashKey(comptime K: type, key: K) u64 {
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
            lock: fiber.RWMutex,
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

// Tests live in zdb/cache_test.zig. They access the cache module by name
// (`@import("zdb").cache`) so cache.zig stays purely in the zdb module's
// file tree without being claimed by the unified test binary.

