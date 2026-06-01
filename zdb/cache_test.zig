const std = @import("std");
const fiber = @import("fiber");
const zdb = @import("zdb");
const Cache = zdb.cache.Cache;
const ShardedCache = zdb.cache.ShardedCache;
const Value = zdb.cache.Value;
const hashKey = zdb.cache.hashKey;
const Allocator = std.mem.Allocator;

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
    var gpa = std.heap.DebugAllocator(.{ .thread_safe = true }){};
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

    var handles: [4]fiber.JoinHandle(void) = undefined;
    const ctx = Ctx{ .sc = &sc, .alloc = alloc };
    for (&handles) |*h| h.* = try fiber.spawn(.{}, work.run, .{ctx});
    for (&handles) |*h| h.join();
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

        std.log.scoped(.cache_test).info(
            "s_ring {d} -> {d} . m_ring {d} -> {d}",
            .{ s_ring_orig, c.s_ring.items.len, m_ring_orig, c.m_ring.items.len },
        );
        if (c.s_ring.items.len > s_ring_orig and c.m_ring.items.len > m_ring_orig) {
            // pass
            return;
        }
    }
    try std.testing.expect(false); // fail -- the rings didn't increase in size
}
