// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const zdb = @import("zdb");
const utils = @import("utils");
const String = utils.String;
const Cache = zdb.cache.Cache;
const ShardedCache = zdb.cache.ShardedCache;
const Value = zdb.cache.Value;
const hashKey = zdb.cache.hashKey;
const Allocator = std.mem.Allocator;

/// Fills M to its budget with one entry under `key` (owned, as for
/// `put`). An unreused entry leaving S is promoted while M has room,
/// so tests of eviction to the ghost fill M first.
fn fillMain(comptime K: type, c: *Cache(K), key: K) !void {
    const alloc = std.testing.allocator;
    const v = try Value.init(alloc, try alloc.alloc(u8, c.m.cap));
    defer v.decrRef();
    try c.restore(key, v);
}

/// Budgets S for `s_entries` and M for `m_entries` entries like the
/// probe: `body` bytes under a key the size of `probe`. An entry's
/// charge is the value's header and the cache's bookkeeping besides
/// the bytes, so it is read off a put rather than computed. The probe
/// is removed again; its dead slot stays at the head of S's ring, and
/// eviction skips it.
fn budgetFor(
    comptime K: type,
    c: *Cache(K),
    probe: K,
    body: String,
    s_entries: usize,
    m_entries: usize,
) !void {
    const alloc = std.testing.allocator;
    const key = if (K == String) try alloc.dupe(u8, probe) else probe;
    const v = try Value.init(alloc, try alloc.dupe(u8, body));
    defer v.decrRef();
    try c.put(key, v);
    const per = c.s.cost;
    try std.testing.expect(c.remove(probe));
    c.s.cap = per * s_entries;
    c.m.cap = per * m_entries;
}

test "Cache basic put/get round-trip" {
    var c = try Cache(String).init(std.testing.allocator, 100);
    defer c.deinit();

    const val = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "hello"));
    defer val.decrRef();
    try c.put(
        try std.testing.allocator.dupe(u8, "key"),
        val,
    );
    const got = c.get("key", .reuse) orelse return error.Missing;
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
    const got = c.get(42, .reuse) orelse return error.Missing;
    defer got.decrRef();
    try std.testing.expectEqualStrings("forty-two", got.data);
    try std.testing.expectEqual(@as(?*Value, null), c.get(99, .reuse));
}

test "Cache one-hit-wonder: evicted to ghost, not M" {
    // capacity=10 → s_cap=1, m_cap=9: S holds one entry, whatever it
    // costs, and the next insert evicts it. M is full once the filler
    // is in, so the evicted entry cannot be promoted.
    var c = try Cache(String).init(std.testing.allocator, 10);
    defer c.deinit();
    try fillMain(String, &c, try std.testing.allocator.dupe(u8, "m"));

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

    // evicted
    try std.testing.expectEqual(@as(?*Value, null), c.get("k0", .reuse));
    if (c.get("k1", .reuse)) |v| v.decrRef(); // still present
    // k0 did NOT go to M, which holds only the filler.
    try std.testing.expectEqual(@as(usize, 1), c.m.len);
    const h0 = hashKey(String, "k0");
    try std.testing.expect(c.ghost.contains(h0)); // hash in ghost
}

test "Cache reused key promoted S → M" {
    // capacity=10 → s_cap=1: S holds one entry.
    var c = try Cache(String).init(std.testing.allocator, 10);
    defer c.deinit();

    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v0"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k0"), v);
    }
    if (c.get("k0", .reuse)) |v| v.decrRef(); // sets freq=true

    // k1 evicts k0 from S; freq=true → k0 goes to M.
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v1"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k1"), v);
    }

    const got = c.get("k0", .reuse) orelse return error.Missing; // now in M
    defer got.decrRef();
    try std.testing.expectEqualStrings("v0", got.data);
    try std.testing.expectEqual(@as(usize, 1), c.m.len);
}

test "Cache ghost re-entry goes directly to M" {
    // capacity=10 → s_cap=1: S holds one entry.
    var c = try Cache(String).init(std.testing.allocator, 10);
    defer c.deinit();
    // M full: k0 leaving S goes to the ghost, not to M.
    try fillMain(String, &c, try std.testing.allocator.dupe(u8, "m"));

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

    // Re-insert k0: ghost hit → should land in M directly, evicting
    // the filler to make room.
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v0b"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k0"), v);
    }

    try std.testing.expectEqual(@as(u64, 1), c.ghost_hits);
    const got = c.get("k0", .reuse) orelse return error.Missing;
    defer got.decrRef();
    try std.testing.expectEqualStrings("v0b", got.data);
    try std.testing.expectEqual(@as(usize, 1), c.m.len);
    try std.testing.expect(!c.contains("m"));
}

test "Cache S FIFO eviction order" {
    // S holds 2 entries and evicts when a 3rd arrives. M is full, so
    // what S evicts goes to the ghost.
    var c = try Cache(String).init(std.testing.allocator, 100);
    defer c.deinit();
    try budgetFor(String, &c, "p0", "v0", 2, 1);
    try fillMain(String, &c, try std.testing.allocator.dupe(u8, "m"));

    // Insert four keys without any gets (freq stays false throughout).
    inline for (.{ "k0", "k1", "k2", "k3" }, .{ "v0", "v1", "v2", "v3" }) |k, vdata| {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, vdata));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, k), v);
    }

    // k0 and k1 were evicted (freq=false → ghost); k2 and k3 remain.
    try std.testing.expectEqual(@as(?*Value, null), c.get("k0", .reuse));
    try std.testing.expectEqual(@as(?*Value, null), c.get("k1", .reuse));
    if (c.get("k2", .reuse)) |v| v.decrRef();
    if (c.get("k3", .reuse)) |v| v.decrRef();
    try std.testing.expect(c.ghost.contains(hashKey(String, "k0")));
    try std.testing.expect(c.ghost.contains(hashKey(String, "k1")));
    // No promotions: M holds only the filler.
    try std.testing.expectEqual(@as(usize, 1), c.m.len);
}

test "Cache M freq reset on reinsertion" {
    // S holds 1 entry, M holds 2.
    var c = try Cache(String).init(std.testing.allocator, 100);
    defer c.deinit();
    try budgetFor(String, &c, "p0", "v0", 1, 2);

    // Promote k0 and k1 into M.
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v0"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k0"), v);
    }
    if (c.get("k0", .reuse)) |v| v.decrRef(); // freq=true
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v1"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k1"), v); // k0 → M
    }
    if (c.get("k1", .reuse)) |v| v.decrRef(); // freq=true in S
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v2"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k2"), v); // k1 → M; M has k0, k1
    }

    // Mark both M entries as freq=true.
    if (c.get("k0", .reuse)) |v| v.decrRef();
    if (c.get("k1", .reuse)) |v| v.decrRef();

    // Promote k2 into M: M full by bytes → evictM cycles past freq=true entries,
    // reinserting with freq=false, until one is evicted (k0 comes around first).
    if (c.get("k2", .reuse)) |v| v.decrRef(); // freq=true in S
    {
        const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "v3"));
        defer v.decrRef();
        try c.put(try std.testing.allocator.dupe(u8, "k3"), v); // k2 → M; evicts k0
    }

    // evicted from M
    try std.testing.expectEqual(@as(?*Value, null), c.get("k0", .reuse));
    if (c.get("k1", .reuse)) |v| v.decrRef();
    if (c.get("k2", .reuse)) |v| v.decrRef();
}

test "Cache update existing key frees old value" {
    var c = try Cache(String).init(std.testing.allocator, 100);
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

    const got = c.get("key", .reuse) orelse return error.Missing;
    defer got.decrRef();
    try std.testing.expectEqualStrings("new", got.data);
    try std.testing.expectEqual(@as(usize, 1), c.count()); // still one entry
}

test "Cache totalCost reflects inserted bytes and respects capacity" {
    // capacity=1000 → s_cap=100, m_cap=900.
    var c = try Cache(u64).init(std.testing.allocator, 1000);
    defer c.deinit();

    // An entry costs its value's length, "hi" = 2, plus the value's
    // header and the cache's bookkeeping for it.
    {
        const v1 = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "hi"));
        defer v1.decrRef();
        try c.put(1, v1);
    }
    const per = c.totalCost();
    try std.testing.expect(per > 2 + @sizeOf(Value));
    {
        const v2 = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "hi"));
        defer v2.decrRef();
        try c.put(2, v2);
    }
    try std.testing.expectEqual(2 * per, c.totalCost());

    // Insert 100 more entries, each ~100 bytes charged: far over 1000,
    // so evictions must fire to stay in budget.
    for (3..103) |i| {
        const val = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "aaaaaaaaaa"));
        defer val.decrRef();
        try c.put(@intCast(i), val);
    }
    try std.testing.expect(c.totalCost() <= 1000);
}

test "ShardedCache basic get/put" {
    var sc = try ShardedCache(String).init(std.testing.allocator, 200, 4);
    defer sc.deinit();

    const val = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "xval"));
    defer val.decrRef();
    try sc.put(
        try std.testing.allocator.dupe(u8, "x"),
        val,
    );
    const got = sc.get("x", .reuse) orelse return error.Missing;
    defer got.decrRef();
    try std.testing.expectEqualStrings("xval", got.data);
}

test "ShardedCache concurrent puts and gets" {
    var gpa = std.heap.DebugAllocator(.{ .thread_safe = true }){};
    defer std.debug.assert(gpa.deinit() == .ok);
    const alloc = gpa.allocator();

    var sc = try ShardedCache(String).init(alloc, 200, 4);
    defer sc.deinit();

    const Ctx = struct {
        sc: *ShardedCache(String),
        alloc: Allocator,
    };

    // String literals have static storage; safe to use as cache keys across threads.
    const static_keys = [_]String{ "aa", "bb", "cc", "dd", "ee", "ff" };

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
                if (ctx.sc.get(key_lit, .reuse)) |v| v.decrRef();
            }
        }
    };

    var handles: [4]fiber.JoinHandle(void) = undefined;
    const ctx = Ctx{ .sc = &sc, .alloc = alloc };
    for (&handles) |*h| h.* = try fiber.spawn(.{}, work.run, .{ctx});
    for (&handles) |*h| h.join();
}

test "Cache ring growth preserves all entries" {
    // capacity=100000 → s_cap=10000, m_cap=90000.
    // An entry costs its ~6-byte val plus headers, about 100 bytes:
    // S holds ~100, M ~900, so every key put stays present.
    // S ring starts at 16 slots; 100 inserts trigger growth at 16, 32, 64.
    // Phase 2 inserts cause byte eviction of freq=true entries → promotion
    // to M, triggering M ring growth at 16, 32.
    const alloc = std.testing.allocator;
    var c = try Cache(u64).init(alloc, 100000);
    defer c.deinit();

    // HACK: Shrink their capacity to trigger the test condition.
    c.s.ring.shrinkRetainingCapacity(16);
    c.m.ring.shrinkRetainingCapacity(16 * 8);

    const s_ring_orig = c.s.ring.items.len;
    const m_ring_orig = c.m.ring.items.len;

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
            const got = c.get(@intCast(key_i), .reuse) orelse
                return error.Missing;
            defer got.decrRef();
            var buf: [20]u8 = undefined;
            const expected = std.fmt.bufPrint(&buf, "val-{d}", .{key_i}) catch unreachable;
            try std.testing.expectEqualStrings(expected, got.data);
        }

        std.log.scoped(.cache_test).info(
            "s_ring {d} -> {d} . m_ring {d} -> {d}",
            .{ s_ring_orig, c.s.ring.items.len, m_ring_orig, c.m.ring.items.len },
        );
        if (c.s.ring.items.len > s_ring_orig and c.m.ring.items.len > m_ring_orig) {
            // pass
            return;
        }
    }
    try std.testing.expect(false); // fail -- the rings didn't increase in size
}

test "Cache remove frees the entry and eviction skips its dead slot" {
    // S holds exactly three entries. M is full, so S's evictions leave
    // the cache; counts below are S's plus M's one filler, costs S's
    // alone.
    var c = try Cache(u64).init(std.testing.allocator, 1000);
    defer c.deinit();
    const body = "x" ** 20;
    try budgetFor(u64, &c, 999, body, 3, 1);
    try fillMain(u64, &c, 1000);
    const putOne = struct {
        fn f(cache: *Cache(u64), k: u64) !void {
            const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, body));
            defer v.decrRef();
            try cache.put(k, v);
        }
    }.f;
    try putOne(&c, 1);
    const per = c.s.cost;
    try std.testing.expect(3 * per <= c.s.cap and 4 * per > c.s.cap);
    try putOne(&c, 2);
    try putOne(&c, 3);
    try std.testing.expectEqual(@as(usize, 1 + 3), c.count());
    try std.testing.expectEqual(3 * per, c.s.cost);

    try std.testing.expect(c.remove(2));
    try std.testing.expect(!c.remove(2));
    try std.testing.expectEqual(@as(?*Value, null), c.get(2, .reuse));
    try std.testing.expectEqual(@as(usize, 1 + 2), c.count());
    try std.testing.expectEqual(2 * per, c.s.cost);

    // 4 fits beside 1 and 3. 5 pushes S over budget: the tail evicts 1,
    // which is live. S's ring is now [dead 2, 3, 4, 5]. Presence is
    // checked through `count` and by absence only: a `get` would mark
    // its entry as reused and change what the next eviction does.
    try putOne(&c, 4);
    try putOne(&c, 5);
    try std.testing.expectEqual(@as(?*Value, null), c.get(1, .reuse));
    try std.testing.expectEqual(@as(usize, 1 + 3), c.count());
    try std.testing.expectEqual(3 * per, c.s.cost);

    // 6 pushes S over budget again: the tail is 2's dead slot, which is
    // skipped, and 3 is the one evicted.
    try putOne(&c, 6);
    try std.testing.expectEqual(@as(?*Value, null), c.get(3, .reuse));
    try std.testing.expectEqual(@as(usize, 1 + 3), c.count());
    try std.testing.expectEqual(3 * per, c.s.cost);
    for ([_]u64{ 4, 5, 6 }) |k| {
        const got = c.get(k, .reuse) orelse return error.Missing;
        got.decrRef();
    }
}

test "Cache remove of a String key: its dead slot frees it when reclaimed" {
    var c = try Cache(String).init(std.testing.allocator, 100);
    defer c.deinit();
    try budgetFor(String, &c, "p0", "v0", 1, 9);
    const putOne = struct {
        fn f(cache: *Cache(String), k: String, body: String) !void {
            const alloc = std.testing.allocator;
            const v = try Value.init(alloc, try alloc.dupe(u8, body));
            defer v.decrRef();
            try cache.put(try alloc.dupe(u8, k), v);
        }
    }.f;
    // Removed, k0's slot is dead but still holds its key: the eviction
    // pass hashes it to see the slot is dead, so it must stay allocated
    // until then.
    try putOne(&c, "k0", "v0");
    try std.testing.expect(c.remove("k0"));
    try putOne(&c, "k1", "v1");
    try putOne(&c, "k2", "v2"); // S over budget: reclaims k0's slot
    try std.testing.expectEqual(@as(?*Value, null), c.get("k0", .reuse));
    for ([_]String{ "k1", "k2" }) |k| {
        const got = c.get(k, .reuse) orelse return error.Missing;
        got.decrRef();
    }
    // A dead slot still in the ring at deinit: its key is freed there
    // (the test allocator reports a leak otherwise).
    try std.testing.expect(c.remove("k2"));
}

test "Cache remove then re-put of the same key leaves a stale slot that eviction skips" {
    var c = try Cache(u64).init(std.testing.allocator, 1000);
    defer c.deinit();
    const body = "y" ** 20;
    try budgetFor(u64, &c, 999, body, 3, 1);
    // M full: S's evictions leave the cache; count adds M's filler.
    try fillMain(u64, &c, 1000);
    const putOne = struct {
        fn f(cache: *Cache(u64), k: u64) !void {
            const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, body));
            defer v.decrRef();
            try cache.put(k, v);
        }
    }.f;
    try putOne(&c, 1);
    const per = c.s.cost;
    try std.testing.expect(3 * per <= c.s.cap and 4 * per > c.s.cap);
    try std.testing.expect(c.remove(1));
    // Re-put: the map now points at a newer slot; the old slot is stale.
    try putOne(&c, 1);
    try putOne(&c, 2);
    try putOne(&c, 3);
    // 4 pushes S over budget: the tail meets 1's stale slot first, must
    // skip it, then evicts 1's live slot — not 2 or 3.
    try putOne(&c, 4);
    try std.testing.expectEqual(@as(?*Value, null), c.get(1, .reuse));
    for ([_]u64{ 2, 3, 4 }) |k| {
        const got = c.get(k, .reuse) orelse return error.Missing;
        got.decrRef();
    }
    try std.testing.expectEqual(@as(usize, 1 + 3), c.count());
}

test "ShardedCache remove" {
    var c = try ShardedCache(u64).init(std.testing.allocator, 4096, 4);
    defer c.deinit();
    const v = try Value.init(std.testing.allocator, try std.testing.allocator.dupe(u8, "blob"));
    defer v.decrRef();
    try c.put(7, v);
    try std.testing.expect(c.remove(7));
    try std.testing.expect(!c.remove(7));
    try std.testing.expectEqual(@as(?*Value, null), c.get(7, .reuse));
}

test "ShardedCache counts a put of a key its ghost ring remembers" {
    const alloc = std.testing.allocator;
    // One shard; S holds one 4 KiB block, so each put evicts the last.
    // M is full, so that eviction goes to the ghost.
    var c = try ShardedCache(u128).init(alloc, 5 * 4096, 1);
    defer c.deinit();
    try fillMain(u128, &c.shards[0].cache, 1000);
    for (1..4) |i| {
        const v = try Value.init(alloc, try alloc.alloc(u8, 4096));
        defer v.decrRef();
        try c.put(i, v);
    }
    try std.testing.expectEqual(@as(u64, 0), c.ghostHits());

    // Back after its eviction from S: the ghost ring remembers it.
    const v = try Value.init(alloc, try alloc.alloc(u8, 4096));
    defer v.decrRef();
    try c.put(1, v);
    try std.testing.expectEqual(@as(u64, 1), c.ghostHits());
}

test "ShardedCache setCapacity: a shrink evicts to the new budget" {
    const alloc = std.testing.allocator;
    // One shard, room for about eight 4 KiB blocks.
    var c = try ShardedCache(u128).init(alloc, 8 * 4096, 1);
    defer c.deinit();
    for (1..9) |i| {
        const v = try Value.init(alloc, try alloc.alloc(u8, 4096));
        defer v.decrRef();
        try c.put(i, v);
        if (c.get(i, .reuse)) |got| got.decrRef(); // into M at the next put
    }
    try std.testing.expect(c.totalCost() > 4 * 4096);

    c.setCapacity(4 * 4096);
    try std.testing.expectEqual(@as(usize, 4 * 4096), c.capacity());
    try std.testing.expect(c.totalCost() <= 4 * 4096);
    try std.testing.expect(c.count() <= 4);

    // Grown again, it admits more without evicting what it kept.
    const kept = c.count();
    c.setCapacity(16 * 4096);
    for (20..24) |i| {
        const v = try Value.init(alloc, try alloc.alloc(u8, 4096));
        defer v.decrRef();
        try c.put(i, v);
    }
    try std.testing.expect(c.count() >= kept + 1);
}

test "Cache sizes its ghost by M's entry count at turnover" {
    const Ghost = zdb.cache.Ghost;
    const alloc = std.testing.allocator;
    // Values on an arena, declared before the cache so it outlives the
    // cache's deinit: the test allocator traces every alloc and free.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arloc = arena.allocator();
    // 1 MiB: an entry of 64 bytes is charged ~150; S takes ~700, M
    // ~6200.
    var c = try Cache(u64).init(alloc, 1 << 20);
    defer c.deinit();
    try std.testing.expectEqual(Ghost.min_quota, c.ghost.quota);

    // Put and get: S promotes these to M, past what M holds, so M is
    // full when the one-hit wonders arrive.
    for (0..20_000) |i| {
        const v = try Value.init(arloc, try arloc.alloc(u8, 64));
        defer v.decrRef();
        try c.put(i, v);
        const got = c.get(i, .reuse) orelse return error.Missing;
        got.decrRef();
    }
    // Full: one more entry, charged as S's are, would not fit.
    const per = c.s.cost / c.s.len;
    try std.testing.expect(c.m.cost + per > c.m.cap);
    // One-hit wonders: once S holds only these, each put evicts one
    // to the ghost (M has no room), past the first generation's quota.
    for (0..4000) |i| {
        const v = try Value.init(arloc, try arloc.alloc(u8, 64));
        defer v.decrRef();
        try c.put(1_000_000 + i, v);
    }
    // The quota is M's count at the turnover; a ghost false positive
    // puts the odd one-hit wonder into M after it, so M may have grown
    // by a few since.
    try std.testing.expect(c.ghost.quota >= 4000);
    try std.testing.expect(c.ghost.quota <= c.m.len);
    try std.testing.expect(c.m.len - c.ghost.quota < 10);
    const bits = try std.math.ceilPowerOfTwo(
        usize,
        c.ghost.quota * Ghost.bits_per_key,
    );
    const f = c.ghost.filters[c.ghost.active];
    try std.testing.expectEqual(bits, f.bit_length);
}

test "Ghost remembers a hash for one to two generations" {
    const Ghost = zdb.cache.Ghost;
    const alloc = std.testing.allocator;
    var g = try Ghost.init(alloc);
    defer g.deinit(alloc);
    // M empty: the floor, 1024 a generation, sizes it.
    const quota = Ghost.min_quota;

    // Hashes are uniform: a splitmix of the index.
    const H = struct {
        fn of(i: u64) u64 {
            var z = i +% 0x9e3779b97f4a7c15;
            z = (z ^ (z >> 30)) *% 0xbf58476d1ce4e5b9;
            z = (z ^ (z >> 27)) *% 0x94d049bb133111eb;
            return z ^ (z >> 31);
        }
    };

    g.insert(alloc, H.of(0), 0);
    try std.testing.expect(g.contains(H.of(0)));
    // Through the rest of this generation and all of the next: kept.
    for (1..2 * quota) |i| g.insert(alloc, H.of(i), 0);
    try std.testing.expect(g.contains(H.of(0)));
    // The third generation clears the first: forgotten.
    g.insert(alloc, H.of(2 * quota), 0);
    try std.testing.expect(!g.contains(H.of(0)));
    try std.testing.expect(g.contains(H.of(1500)));
    try std.testing.expectEqual(quota, g.quota);

    // Both filters full: never-inserted hashes read as present at
    // the bloom's rate, about 0.1%; under 0.3% leaves room.
    for (2 * quota + 1..3 * quota) |i| g.insert(alloc, H.of(i), 0);
    var false_hits: usize = 0;
    for (1_000_000..1_100_000) |i| {
        if (g.contains(H.of(i))) false_hits += 1;
    }
    try std.testing.expect(false_hits < 300);

    // The next turnover takes M's count: 5000 keys at 16 bits each
    // is 80000 bits, a 128 Ki bit filter; the other keeps its size.
    const before = g.filters[g.active].bit_length;
    g.insert(alloc, H.of(3 * quota), 5000);
    try std.testing.expectEqual(@as(usize, 5000), g.quota);
    const grown = g.filters[g.active].bit_length;
    try std.testing.expectEqual(@as(usize, 128 << 10), grown);
    try std.testing.expectEqual(before, g.filters[g.active +% 1].bit_length);
}

test "Cache lookups of missing keys stay fast through put and evict churn" {
    // A tombstone-deleting map fills with tombstones under this churn
    // until each missing key probes the whole table; the array map's
    // index shifts entries back on delete, so its probes stay short.
    const alloc = std.testing.allocator;
    // Values on an arena: the test allocator traces every alloc and
    // free, and 300k values through it take tens of seconds. A
    // value's decrRef frees through the arena, a no-op. Declared
    // before the cache, so it outlives the cache's deinit.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const arloc = arena.allocator();
    // About 7k entries of 64 bytes each, ~150 charged.
    var c = try Cache(u64).init(alloc, 1 << 20);
    defer c.deinit();
    for (0..300_000) |i| {
        const v = try Value.init(arloc, try arloc.alloc(u8, 64));
        defer v.decrRef();
        try c.put(i, v);
        // Every third key gets a hit, so some reach M and churn it too.
        if (i % 3 == 0) if (c.get(i, .reuse)) |got| got.decrRef();
    }
    try std.testing.expect(c.totalCost() <= 1 << 20);
    try std.testing.expect(c.count() > 1000);

    const t0 = utils.timestampNow(.monotonic, .nanos);
    for (0..100_000) |i| {
        try std.testing.expectEqual(
            @as(?*Value, null),
            c.get(1_000_000 + i, .reuse),
        );
    }
    const t1 = utils.timestampNow(.monotonic, .nanos);
    const ms = @divTrunc(t1 - t0, 1_000_000);
    // Microseconds of work in all; a full-table probe per miss would
    // take seconds. Generous for a loaded test box.
    try std.testing.expect(ms < 2000);
}
