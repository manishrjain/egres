const std = @import("std");
const fiber = @import("fiber");

// Serialize the two `.exclusive`-touching tests so they don't taint
// each other's `live_exclusive` reads — Phase B fans tests out as a
// Group, and two parallel exclusives would double-count. .shared
// tests don't need this and can stay parallel.
var placement_mu: fiber.Mutex = .{};

fn recordWorker(slot: *u8) void {
    slot.* = fiber.currentWorker().id;
}

fn recordWorkerAndPark(slot: *u8) void {
    slot.* = fiber.currentWorker().id;
    fiber.park();
}

/// Spawn `n` exclusive fibers, each parking after stamping its
/// home-worker id into `workers[i]`. Once every fiber has parked,
/// the placement decisions are frozen and we can inspect them.
/// `release()` wakes + joins them, returning live_exclusive to 0.
const Squad = struct {
    alloc: std.mem.Allocator,
    workers: []u8,
    handles: []fiber.JoinHandle(void),

    fn spawnExclusive(alloc: std.mem.Allocator, n: usize) !Squad {
        const workers = try alloc.alloc(u8, n);
        errdefer alloc.free(workers);
        @memset(workers, 0);
        const handles = try alloc.alloc(fiber.JoinHandle(void), n);
        errdefer alloc.free(handles);

        for (0..n) |i| {
            handles[i] = try fiber.spawn(
                .{ .sharing = .exclusive },
                recordWorkerAndPark,
                .{&workers[i]},
            );
        }
        for (handles) |h| while (h.fiber.loadState().tag != .waiting) fiber.yield();
        return .{ .alloc = alloc, .workers = workers, .handles = handles };
    }

    fn release(self: Squad) void {
        for (self.handles) |h| fiber.wake(h.fiber);
        for (self.handles) |h| h.join();
        self.alloc.free(self.workers);
        self.alloc.free(self.handles);
    }
};

/// Count how many of the recorded fibers landed on each worker.
/// Result is a per-worker histogram (index = worker id).
fn countsByWorker(alloc: std.mem.Allocator, observed: []const u8, num_workers: usize) ![]u32 {
    const counts = try alloc.alloc(u32, num_workers);
    @memset(counts, 0);
    for (observed) |w| counts[w] += 1;
    return counts;
}

test "Placement: .default lane RR spreads across multiple workers" {
    const n = fiber.currentRuntime().workers.len;
    const N = 2 * n;

    const observed = try std.testing.allocator.alloc(u8, N);
    defer std.testing.allocator.free(observed);
    @memset(observed, 0);

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N);
    for (observed) |*slot| try g.spawn(.{}, recordWorker, .{slot});
    g.wait();

    const counts = try countsByWorker(std.testing.allocator, observed, n);
    defer std.testing.allocator.free(counts);

    var workers_hit: u32 = 0;
    for (counts) |c| if (c > 0) {
        workers_hit += 1;
    };
    // With > 1 worker, RR must use at least 2 of them. We can't
    // demand "all of them" because concurrent tests share the same
    // lane_rr counter and shift our spawn offsets unpredictably.
    if (n > 1) try std.testing.expect(workers_hit >= 2);
}

test "Placement: N exclusives each get a distinct worker (N = num_workers)" {
    placement_mu.lock();
    defer placement_mu.unlock();

    const n = fiber.currentRuntime().workers.len;
    const squad = try Squad.spawnExclusive(std.testing.allocator, n);
    defer squad.release();

    const counts = try countsByWorker(std.testing.allocator, squad.workers, n);
    defer std.testing.allocator.free(counts);

    // Every worker holds exactly one exclusive — the min-pick policy
    // is finding the zero-count worker each time.
    for (counts) |c| try std.testing.expectEqual(@as(u32, 1), c);
}

test "Placement: extra exclusive doubles on one worker; the rest stay at 1" {
    placement_mu.lock();
    defer placement_mu.unlock();

    const n = fiber.currentRuntime().workers.len;
    if (n < 2) return error.SkipZigTest;

    const squad = try Squad.spawnExclusive(std.testing.allocator, n + 1);
    defer squad.release();

    const counts = try countsByWorker(std.testing.allocator, squad.workers, n);
    defer std.testing.allocator.free(counts);

    // After n picks every worker is at 1. The (n+1)-th picks one of
    // them (all tied at min), doubling it. Invariant: min == 1
    // (no worker skipped), max == 2 (no triple-stack).
    try std.testing.expectEqual(@as(u32, 1), std.mem.min(u32, counts));
    try std.testing.expectEqual(@as(u32, 2), std.mem.max(u32, counts));
}

test "Placement: .raft lane with zero workers falls back to .default" {
    var observed: u8 = 0;
    const h = try fiber.spawn(.{ .lane = .raft }, recordWorker, .{&observed});
    h.join();

    // Tests run with LaneConfig{} (raft = 0). A .raft spawn must
    // land in the .default range instead of crashing on width=0.
    const default_range = fiber.currentRuntime().lane_ranges[@intFromEnum(fiber.Lane.default)];
    try std.testing.expect(observed >= default_range.start);
    try std.testing.expect(observed < default_range.end);
}
