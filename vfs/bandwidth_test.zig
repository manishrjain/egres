const std = @import("std");
const testing = std.testing;
const zio = @import("zio");
const vfs = @import("vfs");
const bandwidth = vfs.bandwidth;
const Limiter = bandwidth.Limiter;
const Spinner = bandwidth.Spinner;
const page_size = vfs.page_size;

// Helper: create a spinner, acquire all, return total granted.
fn acquireAll(limiter: *Limiter, priority: vfs.Priority, bytes: usize, kind: vfs.IOKind) usize {
    var s = limiter.spinner(priority, bytes, kind);
    defer s.deinit();
    var total: usize = 0;
    while (true) {
        const chunk = s.acquireBytes();
        if (chunk == 0) break;
        total += chunk;
    }
    return total;
}

// ── 1. P0 never blocks ──────────────────────────────────────────────

test "P0 never blocks" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 1024 * 1024 });
    const granted = acquireAll(&limiter, .p0, 8 * page_size, .read);
    try testing.expectEqual(@as(usize, 8 * page_size), granted);
}

// ── 2. P0 drains bucket ─────────────────────────────────────────────

test "P0 drains bucket" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 64 * page_size });
    const granted = acquireAll(&limiter, .p0, 128 * page_size, .read);
    try testing.expectEqual(@as(usize, 128 * page_size), granted);
    try testing.expectEqual(@as(u64, 0), limiter.pages.load(.monotonic));
}

// ── 3. Bucket never negative ────────────────────────────────────────

test "bucket never negative" {
    var limiter = Limiter.init(.{ .bytes_per_sec = page_size });
    _ = acquireAll(&limiter, .p0, 256 * page_size, .read);
    try testing.expectEqual(@as(u64, 0), limiter.pages.load(.monotonic));
}

// ── 4. Token refill ─────────────────────────────────────────────────

test "token refill" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 100_000_000 }); // 100 MB/s
    _ = acquireAll(&limiter, .p0, 200_000_000, .read);
    try testing.expectEqual(@as(u64, 0), limiter.pages.load(.monotonic));

    try zio.sleep(zio.Duration.fromMilliseconds(50));

    // After 50ms at 100 MB/s, ~5 MB refilled.
    var s = limiter.spinner(.p1, 100_000_000, .read);
    defer s.deinit();
    const granted = s.acquireBytes();
    try testing.expect(granted > 0);
    try testing.expect(granted % page_size == 0);
}

// ── 5. Write cost multiplier ────────────────────────────────────────

test "write cost multiplier" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 64 * page_size, .write_cost = 8 });

    // Bucket starts at 64 pages. Write of 4 pages costs 4*8=32 pages of tokens.
    const granted = acquireAll(&limiter, .p0, 4 * page_size, .write);
    try testing.expectEqual(@as(usize, 4 * page_size), granted);
    // 64 - 32 = 32 pages left.
    try testing.expectEqual(@as(u64, 32), limiter.pages.load(.monotonic));
}

// ── 6. Partial grant ────────────────────────────────────────────────

test "partial grant" {
    // 128 pages of bandwidth.
    var limiter = Limiter.init(.{ .bytes_per_sec = 128 * page_size });
    // Drain most of the bucket with P0 — leaves 8 pages.
    _ = acquireAll(&limiter, .p0, 120 * page_size, .read);

    // P1 spinner requesting 64 pages — first acquire gets only what's left (~8 pages).
    var s = limiter.spinner(.p1, 64 * page_size, .read);
    defer s.deinit();
    const granted = s.acquireBytes();
    try testing.expect(granted > 0);
    try testing.expect(granted <= 64 * page_size);
    try testing.expect(granted % page_size == 0);
}

// ── 7. Burst cap ────────────────────────────────────────────────────

test "burst cap" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 32 * page_size });

    // Simulate long idle: push last_refill 60s back, drain tokens.
    limiter.last_refill.store(limiter.last_refill.load(.monotonic) - 60_000, .monotonic);
    limiter.pages.store(0, .monotonic);

    // After long idle, refill caps at bytes_per_sec (32 pages), not 60x that.
    var s = limiter.spinner(.p1, 256 * page_size, .read);
    defer s.deinit();
    const granted = s.acquireBytes();
    try testing.expect(granted <= 32 * page_size);
    try testing.expect(granted % page_size == 0);
}

// ── 8. P1 demand blocks P2 ──────────────────────────────────────────

test "P1 demand blocks P2" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 1_000_000, .min_alloc_bytes = page_size });

    // Create a P1 spinner with large demand — don't acquire yet.
    var p1 = limiter.spinner(.p1, 10_000_000, .read);
    defer p1.deinit();

    // P2 sees demand_p1 > available tokens, hits timeout, gets minimum grant.
    var p2 = limiter.spinner(.p2, 64 * page_size, .read);
    defer p2.deinit();
    const granted = p2.acquireBytes();
    try testing.expectEqual(@as(usize, page_size), granted);
}

// ── 9. Grants are always page-aligned ──────────────────────────────

test "exact byte fulfillment for non-aligned requests" {
    // Non-page-multiple request sizes — total granted must equal exact bytes requested.
    var limiter = Limiter.init(.{ .bytes_per_sec = 50_000 });

    const g1 = acquireAll(&limiter, .p0, 5_000, .read);
    try testing.expectEqual(@as(usize, 5_000), g1);

    const g2 = acquireAll(&limiter, .p1, 13_000, .read);
    try testing.expectEqual(@as(usize, 13_000), g2);

    const g3 = acquireAll(&limiter, .p0, 7_777, .write);
    try testing.expectEqual(@as(usize, 7_777), g3);
}

// Regression: a write spinner whose limiter has fewer cost-units than
// `write_cost` left in the bucket must NOT return 0 from acquireBytes
// while bytes are still pending. Pre-fix bug: acquireFromLimiter could
// return 7 cost-units (with write_cost=8), the integer divide by wm
// flooring to 0 actual pages, and the spinner returned 0 — vfs.write
// interpreted that as "done" and silently dropped the unwritten tail.
// Symptom in production: SST files truncated to a multiple of page_size,
// no write error, manifest references the bad file, apply later trips
// CorruptTable on Table.open.
//
// The fix makes `acquireFromLimiter` quantize its grant to a multiple of
// `factor` (== write_cost for writes), so divide-by-factor is exact AND
// we don't drain the bucket beyond what we can actually use.
test "write spinner under bucket pressure never returns 0 while bytes remain" {
    // Bucket holds 11 cost-units total (intentionally NOT a multiple of
    // write_cost=8). After a P0 write of 1 page (8 cost-units), the
    // bucket has 3 cost-units left — less than write_cost — which is
    // the regression-trigger condition for a P1 follower.
    var limiter = Limiter.init(.{ .bytes_per_sec = 11 * page_size, .write_cost = 8, .timeout_ms = 50 });

    // Drain to the trigger condition: 1 P0 page-write costs 8 of 11 -> 3 left.
    _ = acquireAll(&limiter, .p0, page_size, .write);
    try testing.expectEqual(@as(u64, 3), limiter.pages.load(.monotonic));

    // Now a P1 write that wants 4 pages. Pre-fix this would short-write
    // and the for-loop in vfs.write would treat the 0 return as success.
    // Post-fix: spinner waits for refill (or timeout fallback) until the
    // bucket can grant a multiple of write_cost, then returns the full
    // 4 pages eventually. Each chunk MUST be page-aligned (cost-unit
    // grants quantized to a multiple of write_cost ⇒ pages quantized to
    // whole pages).
    var s = limiter.spinner(.p1, 4 * page_size, .write);
    defer s.deinit();
    var total: usize = 0;
    var chunks: u32 = 0;
    while (true) {
        const chunk = s.acquireBytes();
        if (chunk == 0) break;
        try testing.expect(chunk % page_size == 0); // factor invariant
        total += chunk;
        chunks += 1;
    }
    try testing.expectEqual(@as(usize, 4 * page_size), total);
    try testing.expect(chunks >= 1);
}

// ── 10. Spinner returns 0 when done ─────────────────────────────────

test "spinner returns 0 when done" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 1024 * 1024 });
    var s = limiter.spinner(.p0, 2 * page_size, .read);
    defer s.deinit();

    const first = s.acquireBytes();
    try testing.expectEqual(@as(usize, 2 * page_size), first);

    // Already fully granted — next acquire returns 0.
    const second = s.acquireBytes();
    try testing.expectEqual(@as(usize, 0), second);
}

// ── 11. P1 demand decreases as spinner acquires ─────────────────────

test "P1 demand tracking" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 1024 * 1024 });

    var s = limiter.spinner(.p1, 8 * page_size, .read);
    try testing.expectEqual(@as(u64, 8), limiter.demand_p1.load(.monotonic));

    _ = s.acquireBytes();
    // demand_p1 should have decreased by the granted amount.
    try testing.expect(limiter.demand_p1.load(.monotonic) < 8);

    s.deinit();
    // After deinit, demand_p1 should be 0.
    try testing.expectEqual(@as(u64, 0), limiter.demand_p1.load(.monotonic));
}

// ── 12. Non-page-aligned bytes are fulfilled exactly ────────────────

test "non-page-aligned bytes fulfilled exactly" {
    var limiter = Limiter.init(.{ .bytes_per_sec = 1024 * 1024 });

    // Request 5000 bytes — not a multiple of page_size (4096).
    const requested: usize = 5000;
    var s = limiter.spinner(.p0, requested, .read);
    defer s.deinit();

    var total: usize = 0;
    while (true) {
        const chunk = s.acquireBytes();
        if (chunk == 0) break;
        total += chunk;
    }

    // Should get exactly 5000 bytes, not 4096 or 8192.
    try testing.expectEqual(requested, total);
}

// ── 13. Page-aligned requests yield page-aligned chunks ─────────────

test "page-aligned requests yield page-aligned chunks" {
    // Small bucket so P1/P2 get partial grants across multiple acquires.
    var limiter = Limiter.init(.{ .bytes_per_sec = 4 * page_size });

    // Drain the bucket.
    _ = acquireAll(&limiter, .p0, 4 * page_size, .read);

    // Wait for some refill so P1 gets partial grants.
    try zio.sleep(zio.Duration.fromMilliseconds(50));

    const requested: usize = 16 * page_size;
    var s = limiter.spinner(.p1, requested, .read);
    defer s.deinit();

    var total: usize = 0;
    var chunks: usize = 0;
    while (true) {
        const chunk = s.acquireBytes();
        if (chunk == 0) break;
        // Each individual chunk must be page-aligned.
        try testing.expect(chunk % page_size == 0);
        total += chunk;
        chunks += 1;
    }
    try testing.expectEqual(requested, total);
    // Must have taken multiple chunks (not a single grant).
    try testing.expect(chunks >= 2);
}
