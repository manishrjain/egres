// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");

const SemaphoreMap = fiber.SemaphoreMap;
const gpa = std.testing.allocator;

/// One key through `acquireMany`: true when this call took the hold,
/// false when `owner_id` already held it (re-entrant no-op).
fn acquire(
    sm: *SemaphoreMap,
    key: u64,
    owner_id: u64,
    mode: SemaphoreMap.Mode,
) !bool {
    // A fixed one-slot list: no allocation per call, which the big
    // test makes millions of times through the testing allocator.
    var slot: [1]u64 = undefined;
    var newly: std.ArrayList(u64) = .initBuffer(&slot);
    try sm.acquireMany(&.{key}, owner_id, mode, &newly);
    return newly.items.len == 1;
}

/// One key through `releaseMany`.
fn release(sm: *SemaphoreMap, key: u64, owner_id: u64) void {
    sm.releaseMany(&.{key}, owner_id);
}

fn waiterCount(sm: *SemaphoreMap, key: u64) usize {
    sm.mu.lock();
    defer sm.mu.unlock();
    const e = sm.entries.get(key) orelse return 0;
    return e.waiters.items.len;
}

/// Park the test until `key` has at least `n` registered waiters — the
/// point where a contender's wait edge is published and it is about to
/// (or already did) park in the entry's RWMutex.
fn awaitWaiters(sm: *SemaphoreMap, key: u64, n: usize) void {
    while (waiterCount(sm, key) < n) fiber.yield();
}

test "SemaphoreMap: uncontended acquire/release reclaims the entry" {
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    try std.testing.expect(try acquire(&sm, 1, 1, .exclusive));
    try std.testing.expect(try acquire(&sm, 2, 1, .shared));
    try std.testing.expectEqual(2, sm.entries.count());

    release(&sm, 1, 1);
    release(&sm, 2, 1);
    // Drained entries leave the map for the reuse pool.
    try std.testing.expectEqual(0, sm.entries.count());
    try std.testing.expectEqual(2, sm.pool.items.len);

    // The next acquire reuses a pooled entry instead of allocating.
    try std.testing.expect(try acquire(&sm, 3, 1, .exclusive));
    try std.testing.expectEqual(1, sm.pool.items.len);
    release(&sm, 3, 1);
    try std.testing.expectEqual(2, sm.pool.items.len);
}

/// One statement's worth of row keys, taken and released.
fn burst(sm: *SemaphoreMap, keys: []const u64, owner_id: u64) !void {
    var newly: std.ArrayList(u64) = .empty;
    defer newly.deinit(gpa);
    try newly.ensureUnusedCapacity(gpa, keys.len);
    try sm.acquireMany(keys, owner_id, .exclusive, &newly);
    sm.releaseMany(newly.items, owner_id);
}

test "SemaphoreMap: a sweep frees a burst after two quiet periods" {
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    const keys = try gpa.alloc(u64, 1000);
    defer gpa.free(keys);
    for (keys, 1..) |*k, i| k.* = i;

    // After an INSERT of 1,000 rows the map holds nothing and keeps
    // what the rows grew.
    try burst(&sm, keys, 1);
    try std.testing.expectEqual(0, sm.entries.count());
    try std.testing.expect(sm.entries.capacity() >= keys.len);
    const pooled = sm.pool.items.len;
    try std.testing.expect(pooled > 0);

    // The period of the burst and the one after keep it: the next
    // statement may be the same size.
    sm.sweep();
    sm.sweep();
    try std.testing.expect(sm.entries.capacity() >= keys.len);
    try std.testing.expectEqual(pooled, sm.pool.items.len);

    // Another burst in between starts the wait over.
    try burst(&sm, keys, 1);
    sm.sweep();
    sm.sweep();
    try std.testing.expect(sm.entries.capacity() >= keys.len);

    // Two quiet periods: an idle database holds nothing.
    sm.sweep();
    try std.testing.expectEqual(0, sm.entries.capacity());
    try std.testing.expectEqual(0, sm.pool.items.len);
    try std.testing.expectEqual(0, sm.pool.capacity);

    // The emptied map still serves the next statement.
    try burst(&sm, keys[0..8], 1);
    try std.testing.expectEqual(0, sm.entries.count());
    try std.testing.expectEqual(8, sm.pool.items.len);
}

test "SemaphoreMap: a sweep sizes the map for the statements that followed" {
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    const keys = try gpa.alloc(u64, 1000);
    defer gpa.free(keys);
    for (keys, 1..) |*k, i| k.* = i;
    const small = keys[0..40];

    // A burst, then a small statement every period while owner 2 holds
    // one key throughout: the map never drains, and its use is now
    // the small statements.
    try std.testing.expect(try acquire(&sm, 0, 2, .exclusive));
    try burst(&sm, keys, 1);
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        sm.sweep();
        try burst(&sm, small, 1);
    }
    try std.testing.expect(sm.entries.capacity() >= small.len + 1);
    try std.testing.expect(sm.entries.capacity() < keys.len / 2);
    try std.testing.expect(sm.pool.items.len <= small.len + 1);
    // The held key came through the move.
    try std.testing.expect(!try acquire(&sm, 0, 2, .exclusive));
    try std.testing.expect(!sm.entries.get(0).?.rw.tryLock(@src()));
    release(&sm, 0, 2);
}

test "SemaphoreMap: re-entrant acquire by the holder is a no-op" {
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    try std.testing.expect(try acquire(&sm, 10, 7, .exclusive));
    // Same or weaker mode by the same owner: no park, no new hold.
    try std.testing.expect(!try acquire(&sm, 10, 7, .exclusive));
    try std.testing.expect(!try acquire(&sm, 10, 7, .shared));
    release(&sm, 10, 7);
    try std.testing.expectEqual(0, sm.entries.count());

    // Shared self re-acquire is likewise a no-op.
    try std.testing.expect(try acquire(&sm, 10, 7, .shared));
    try std.testing.expect(!try acquire(&sm, 10, 7, .shared));
    // But a shared holder asking for exclusive is refused, not upgraded.
    try std.testing.expectError(
        error.Upgrade,
        acquire(&sm, 10, 7, .exclusive),
    );
    release(&sm, 10, 7);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: acquireMany takes a set, skips held keys, releaseMany" {
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    // Key 3 is already held by the owner: not taken twice, not listed.
    try std.testing.expect(try acquire(&sm, 3, 1, .exclusive));
    var newly: std.ArrayList(u64) = .empty;
    defer newly.deinit(gpa);
    try newly.ensureUnusedCapacity(gpa, 5);
    try sm.acquireMany(&.{ 1, 2, 3, 4, 5 }, 1, .exclusive, &newly);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 4, 5 }, newly.items);
    try std.testing.expectEqual(5, sm.entries.count());

    // Another owner sees every key held.
    for ([_]u64{ 1, 2, 3, 4, 5 }) |k| {
        try std.testing.expect(!sm.entries.get(k).?.rw.tryLockShared(@src()));
    }
    // A shared ask on a key held shared by the owner is a no-op too;
    // an exclusive ask on it is refused as an upgrade.
    try std.testing.expect(try acquire(&sm, 6, 1, .shared));
    var more: std.ArrayList(u64) = .empty;
    defer more.deinit(gpa);
    try more.ensureUnusedCapacity(gpa, 1);
    try sm.acquireMany(&.{6}, 1, .shared, &more);
    try std.testing.expectEqual(0, more.items.len);
    try std.testing.expectError(
        error.Upgrade,
        sm.acquireMany(&.{6}, 1, .exclusive, &more),
    );

    sm.releaseMany(newly.items, 1);
    release(&sm, 3, 1);
    release(&sm, 6, 1);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: acquireMany parks on a contended key and resumes" {
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    // Owner 100 holds key 2 of the set; the batch takes 1, waits for 2,
    // then takes 3 once 2 is handed off.
    try std.testing.expect(try acquire(&sm, 2, 100, .exclusive));
    var newly: std.ArrayList(u64) = .empty;
    defer newly.deinit(gpa);
    try newly.ensureUnusedCapacity(gpa, 3);
    const Body = struct {
        fn run(m: *SemaphoreMap, out: *std.ArrayList(u64)) !void {
            try m.acquireMany(&.{ 1, 2, 3 }, 1, .exclusive, out);
        }
    };
    var h = try fiber.spawn(.{}, Body.run, .{ &sm, &newly });
    awaitWaiters(&sm, 2, 1);
    // Key 1 is held already; 3 is not touched until 2 is granted.
    try std.testing.expectEqualSlices(u64, &.{1}, newly.items);
    try std.testing.expect(sm.entries.get(3) == null);

    release(&sm, 2, 100);
    try h.join();
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3 }, newly.items);
    sm.releaseMany(newly.items, 1);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: FIFO hand-off under exclusive contention" {
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    try std.testing.expect(try acquire(&sm, 10, 100, .exclusive));

    // Three contenders queue behind us; each records its grant slot.
    var order: [3]u8 = .{ 0, 0, 0 };
    var next: std.atomic.Value(u8) = .init(0);
    const Body = struct {
        fn run(
            m: *SemaphoreMap,
            id: u8,
            slot_src: *std.atomic.Value(u8),
            ord: []u8,
        ) !void {
            try std.testing.expect(try acquire(m, 10, id, .exclusive));
            ord[slot_src.fetchAdd(1, .monotonic)] = id;
            release(m, 10, id);
        }
    };
    // Wait until each contender is queued before spawning the next, so
    // the FIFO order is deterministic.
    var h1 = try fiber.spawn(.{}, Body.run, .{ &sm, 1, &next, &order });
    awaitWaiters(&sm, 10, 1);
    var h2 = try fiber.spawn(.{}, Body.run, .{ &sm, 2, &next, &order });
    awaitWaiters(&sm, 10, 2);
    var h3 = try fiber.spawn(.{}, Body.run, .{ &sm, 3, &next, &order });
    awaitWaiters(&sm, 10, 3);

    release(&sm, 10, 100);
    try h1.join();
    try h2.join();
    try h3.join();

    try std.testing.expectEqual([3]u8{ 1, 2, 3 }, order);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: shared holders coexist; writer preference holds" {
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    // Three shared owners hold concurrently — none blocks.
    try std.testing.expect(try acquire(&sm, 10, 1, .shared));
    try std.testing.expect(try acquire(&sm, 10, 2, .shared));
    try std.testing.expect(try acquire(&sm, 10, 3, .shared));

    // A writer queues behind them...
    var writer_done: std.atomic.Value(bool) = .init(false);
    const Writer = struct {
        fn run(m: *SemaphoreMap, done: *std.atomic.Value(bool)) !void {
            try std.testing.expect(try acquire(m, 10, 4, .exclusive));
            done.store(true, .release);
            release(m, 10, 4);
        }
    };
    var hw = try fiber.spawn(.{}, Writer.run, .{ &sm, &writer_done });
    awaitWaiters(&sm, 10, 1);

    // ...and while it is pending, a NEW reader must queue too (writer
    // preference), not sneak past.
    var reader_done: std.atomic.Value(bool) = .init(false);
    const Reader = struct {
        fn run(m: *SemaphoreMap, done: *std.atomic.Value(bool)) !void {
            try std.testing.expect(try acquire(m, 10, 5, .shared));
            done.store(true, .release);
            release(m, 10, 5);
        }
    };
    var hr = try fiber.spawn(.{}, Reader.run, .{ &sm, &reader_done });
    awaitWaiters(&sm, 10, 2);
    try std.testing.expect(!reader_done.load(.acquire));

    // Writer runs first once the shared holders drain, then the reader.
    release(&sm, 10, 1);
    release(&sm, 10, 2);
    try std.testing.expect(!writer_done.load(.acquire));
    release(&sm, 10, 3);
    try hw.join();
    try hr.join();
    try std.testing.expect(writer_done.load(.acquire));
    try std.testing.expect(reader_done.load(.acquire));
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: two-party deadlock refused at the closing edge" {
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    // Owners are tokens, not fibers: the test holds A as owner 1 and B
    // as owner 2 from the main fiber, then parks a helper as owner 1.
    try std.testing.expect(try acquire(&sm, 0xA, 1, .exclusive));
    try std.testing.expect(try acquire(&sm, 0xB, 2, .exclusive));

    const Body = struct {
        fn run(m: *SemaphoreMap) !void {
            // Owner 1 blocks on B (held by 2) — edge 1→2 published.
            try std.testing.expect(try acquire(m, 0xB, 1, .exclusive));
            release(m, 0xB, 1);
        }
    };
    var h = try fiber.spawn(.{}, Body.run, .{&sm});
    awaitWaiters(&sm, 0xB, 1);

    // Owner 2 requesting A would close 2→1→2: refused, never parks.
    try std.testing.expectError(
        error.Deadlock,
        acquire(&sm, 0xA, 2, .exclusive),
    );

    // Owner 2 backs off; owner 1's blocked acquire proceeds.
    release(&sm, 0xB, 2);
    try h.join();
    release(&sm, 0xA, 1);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: deadlock through a shared holder is detected" {
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    try std.testing.expect(try acquire(&sm, 0xA, 1, .shared));
    try std.testing.expect(try acquire(&sm, 0xB, 2, .exclusive));

    const Body = struct {
        fn run(m: *SemaphoreMap) !void {
            // Owner 1 (shared holder of A) blocks on B exclusively.
            try std.testing.expect(try acquire(m, 0xB, 1, .exclusive));
            release(m, 0xB, 1);
        }
    };
    var h = try fiber.spawn(.{}, Body.run, .{&sm});
    awaitWaiters(&sm, 0xB, 1);

    // Owner 2's exclusive request on A waits on A's SHARED holder 1,
    // who waits on B held by 2 — a cycle through a shared edge.
    try std.testing.expectError(
        error.Deadlock,
        acquire(&sm, 0xA, 2, .exclusive),
    );

    release(&sm, 0xB, 2);
    try h.join();
    release(&sm, 0xA, 1);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "SemaphoreMap: three-party cycle is detected" {
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    try std.testing.expect(try acquire(&sm, 0xA, 1, .exclusive));
    try std.testing.expect(try acquire(&sm, 0xB, 2, .exclusive));
    try std.testing.expect(try acquire(&sm, 0xC, 3, .exclusive));

    const Blocked = struct {
        fn run(m: *SemaphoreMap, key: u64, owner: u64) !void {
            try std.testing.expect(try acquire(m, key, owner, .exclusive));
            release(m, key, owner);
        }
    };
    // 1 waits on B (held by 2); 2 waits on C (held by 3).
    var h1 = try fiber.spawn(.{}, Blocked.run, .{ &sm, 0xB, 1 });
    awaitWaiters(&sm, 0xB, 1);
    var h2 = try fiber.spawn(.{}, Blocked.run, .{ &sm, 0xC, 2 });
    awaitWaiters(&sm, 0xC, 1);

    // 3 requesting A closes 3→1→2→3 — DFS beyond depth two.
    try std.testing.expectError(
        error.Deadlock,
        acquire(&sm, 0xA, 3, .exclusive),
    );

    // Unwind: 3 releases C → 2 completes; 2's main-held B frees → 1
    // completes; drain A.
    release(&sm, 0xC, 3);
    try h2.join();
    release(&sm, 0xB, 2);
    try h1.join();
    release(&sm, 0xA, 1);
    try std.testing.expectEqual(0, sm.entries.count());
}

test "BIG: SemaphoreMap: 64 owners cycling shared holds free drained entries" {
    // Stress for the drain-and-recycle path under real concurrency: with
    // more than `max_kept_capacity` holders at once, drained entries are
    // freed rather than pooled. The release once unlocked the entry's rw
    // AFTER dropping the map lock, so a concurrent release could recycle (or
    // free) the entry first and the late unlock landed on a reset lock
    // or freed memory. That race showed under pgbench's I/O-bound load
    // and NOT here — three runs of this test passed on the broken
    // ordering — so this is a stress exercise of the path, not the
    // tripwire; the rw's underflow assert in a ReleaseSafe server is.
    // Opt-in (`--bigtest`): a few seconds in Debug.
    var sm = SemaphoreMap.init(gpa);
    defer sm.deinit();

    const owners = 64;
    const rounds = 10_000;
    const Body = struct {
        fn run(map: *SemaphoreMap, owner: u64) void {
            var i: usize = 0;
            while (i < rounds) : (i += 1) {
                _ = acquire(map, 1, owner, .shared) catch unreachable;
                _ = acquire(map, 2, owner, .shared) catch unreachable;
                if (i % 7 == 0) fiber.yield();
                release(map, 2, owner);
                release(map, 1, owner);
            }
        }
    };
    var handles: [owners]fiber.JoinHandle(void) = undefined;
    for (&handles, 0..) |*h, i| {
        h.* = try fiber.spawn(.{}, Body.run, .{ &sm, @as(u64, i + 1) });
    }
    for (&handles) |*h| h.join();
    try std.testing.expectEqual(0, sm.entries.count());
}
