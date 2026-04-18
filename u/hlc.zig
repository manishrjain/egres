const std = @import("std");

// Hybrid Logical Clock.
//
// Packed u64 layout (low-bit to high-bit):
//   logical:  32 bits — counter within the current second, starts at 1
//   seconds:  32 bits — seconds since Unix epoch (rolls over in 2106)
//
// Integer comparison on the u64 bit-cast (seconds in high bits) gives
// "newer > older" ordering — no custom comparator needed. The all-zero
// value `{seconds=0, logical=0}` is reserved as a sentinel "no HLC yet" —
// every real HLC emitted by `Clock.next()` has logical >= 1.
//
// `Clock.next()` advances strictly monotonically on every call:
//   - If wall-clock seconds have moved past `last.seconds`, emit (wall_sec, 1).
//   - Otherwise, emit (last.seconds, last.logical + 1).
// CAS ensures monotonicity under concurrent callers.
//
// `Clock.next()` is called inside the Raft propose critical section so HLC
// order on the leader matches log-index order. That invariant is what makes
// `applied_hlc` on a follower a safe bound for consistent reads.

pub const HLC = packed struct(u64) {
    logical: u32,
    seconds: u32,

    pub fn asU64(self: HLC) u64 {
        return @bitCast(self);
    }

    pub fn fromU64(v: u64) HLC {
        return @bitCast(v);
    }

    pub fn order(a: HLC, b: HLC) std.math.Order {
        return std.math.order(a.asU64(), b.asU64());
    }
};

pub const Clock = struct {
    last: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Returns a fresh HLC strictly greater than any previously returned.
    /// If the logical counter would overflow u32 (4B+ events within a second),
    /// advances the physical seconds by 1 instead. In practice overflow never
    /// happens on real workloads, but the branch keeps monotonicity airtight.
    pub fn next(self: *Clock) HLC {
        const wall_sec: u32 = @intCast(std.time.timestamp());
        while (true) {
            const cur_u = self.last.load(.monotonic);
            const cur = HLC.fromU64(cur_u);

            const nx: HLC = blk: {
                if (wall_sec > cur.seconds) {
                    break :blk .{ .seconds = wall_sec, .logical = 1 };
                }
                if (cur.logical == std.math.maxInt(u32)) {
                    // Logical overflow: carry into seconds.
                    break :blk .{ .seconds = cur.seconds + 1, .logical = 1 };
                }
                break :blk .{ .seconds = cur.seconds, .logical = cur.logical + 1 };
            };

            if (self.last.cmpxchgWeak(cur_u, nx.asU64(), .release, .monotonic) == null) {
                return nx;
            }
        }
    }

    /// Raise `last` to at least `floor`. Any subsequent `next()` returns a
    /// value strictly greater than `floor`. Called on startup after scanning
    /// the WAL so a newly-elected leader's HLC is past every committed entry.
    pub fn seed(self: *Clock, floor: HLC) void {
        const floor_u = floor.asU64();
        while (true) {
            const cur = self.last.load(.monotonic);
            if (cur >= floor_u) return;
            if (self.last.cmpxchgWeak(cur, floor_u, .release, .monotonic) == null) return;
        }
    }

    /// Returns the current HLC without advancing. Use for read-only inspection
    /// (e.g. the tick fiber reading the leader's latest stamp).
    pub fn peek(self: *const Clock) HLC {
        return HLC.fromU64(self.last.load(.monotonic));
    }
};

// Global singleton — mirrors the `vfs.g()` / `metrics.g()` pattern.
var g_clock: Clock = .{};

pub fn g() *Clock {
    return &g_clock;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "packed layout: seconds in high bits, logical in low bits" {
    const t = std.testing;
    const h = HLC{ .seconds = 0x12345678, .logical = 0x9abcdef0 };
    try t.expectEqual(@as(u64, 0x123456789abcdef0), h.asU64());
    try t.expectEqual(@as(u32, 0x12345678), h.seconds);
    try t.expectEqual(@as(u32, 0x9abcdef0), h.logical);
}

test "next is strictly monotonic" {
    const t = std.testing;
    var clock = Clock{};
    const a = clock.next();
    const b = clock.next();
    const c = clock.next();
    try t.expect(b.asU64() > a.asU64());
    try t.expect(c.asU64() > b.asU64());
}

test "first next from fresh clock has logical >= 1" {
    const t = std.testing;
    var clock = Clock{};
    const h = clock.next();
    try t.expect(h.logical >= 1);
}

test "logical counter increments within same second" {
    const t = std.testing;
    var clock = Clock{};
    // Pin to a far-future second so wall < last.seconds — forces logical bump.
    clock.seed(.{ .seconds = 0xF0000000, .logical = 0 });
    const a = clock.next();
    const b = clock.next();
    try t.expectEqual(a.seconds, b.seconds);
    try t.expectEqual(@as(u32, 1), a.logical);
    try t.expectEqual(@as(u32, 2), b.logical);
}

test "seed raises but never lowers" {
    const t = std.testing;
    var clock = Clock{};
    const high: HLC = .{ .seconds = 100, .logical = 50 };
    clock.seed(high);
    try t.expectEqual(high.asU64(), clock.peek().asU64());

    // Lower seed is a no-op.
    clock.seed(.{ .seconds = 50, .logical = 0 });
    try t.expectEqual(high.asU64(), clock.peek().asU64());

    // Higher seed raises.
    const higher: HLC = .{ .seconds = 200, .logical = 0 };
    clock.seed(higher);
    try t.expectEqual(higher.asU64(), clock.peek().asU64());
}

test "logical overflow carries into seconds" {
    const t = std.testing;
    var clock = Clock{};
    // Seed at max logical in a far-future second (wall clock can't push us forward).
    clock.seed(.{ .seconds = 0xF0000000, .logical = std.math.maxInt(u32) });
    const h = clock.next();
    try t.expectEqual(@as(u32, 0xF0000001), h.seconds);
    try t.expectEqual(@as(u32, 1), h.logical);
}

test "seed floor respected on next() call" {
    const t = std.testing;
    var clock = Clock{};
    clock.seed(.{ .seconds = 0xF0000000, .logical = 42 });
    const h = clock.next();
    try t.expectEqual(@as(u32, 0xF0000000), h.seconds);
    try t.expectEqual(@as(u32, 43), h.logical);
}

test "concurrent next yields distinct monotonic HLCs" {
    const t = std.testing;
    var clock = Clock{};
    const Ctx = struct {
        clock: *Clock,
        results: [256]HLC = undefined,
        fn run(ctx: *@This()) void {
            for (0..ctx.results.len) |i| ctx.results[i] = ctx.clock.next();
        }
    };
    var a = Ctx{ .clock = &clock };
    var b = Ctx{ .clock = &clock };
    const ta = try std.Thread.spawn(.{}, Ctx.run, .{&a});
    const tb = try std.Thread.spawn(.{}, Ctx.run, .{&b});
    ta.join();
    tb.join();

    // Every HLC emitted must be unique (CAS + strict increment guarantees it).
    var all: [512]u64 = undefined;
    for (a.results, 0..) |h, i| all[i] = h.asU64();
    for (b.results, 0..) |h, i| all[256 + i] = h.asU64();
    std.mem.sort(u64, &all, {}, comptime std.sort.asc(u64));
    for (1..all.len) |i| try t.expect(all[i] > all[i - 1]);
}
