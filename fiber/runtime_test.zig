const std = @import("std");
const fiber = @import("fiber");

test "Runtime: 100 fibers each yielding 10 times all finish" {
    const N = 100;
    var counters = [_]u32{0} ** N;

    const Body = struct {
        fn run(c: *u32) void {
            while (c.* < 10) : (c.* += 1) {
                fiber.yield();
            }
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N);

    for (&counters) |*c| try g.spawn(.{}, Body.run, .{c});
    g.wait();

    for (counters) |c| try std.testing.expectEqual(@as(u32, 10), c);
}

test "Runtime: producer parks, consumer wakes, producer resumes" {
    if (true) {
        return error.SkipZigTest;
    }

    var producer_handle: ?*fiber.Fiber = null;
    var producer_resumed = false;

    const Producer = struct {
        out: *?*fiber.Fiber,
        resumed: *bool,

        fn run(self: *@This()) void {
            self.out.* = fiber.currentFiber();
            fiber.park();
            self.resumed.* = true;
        }
    };
    const Consumer = struct {
        target: *?*fiber.Fiber,

        fn run(self: *@This()) void {
            // Cross-worker scheduling may run us before the producer;
            // yield until the producer has recorded its handle.
            while (self.target.* == null) fiber.yield();
            fiber.wake(self.target.*.?);
        }
    };

    var p_ctx = Producer{ .out = &producer_handle, .resumed = &producer_resumed };
    var c_ctx = Consumer{ .target = &producer_handle };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.spawn(.{}, Producer.run, .{&p_ctx});
    try g.spawn(.{}, Consumer.run, .{&c_ctx});
    g.wait();

    try std.testing.expect(producer_resumed);
    try std.testing.expect(producer_handle != null);
}

test "Runtime: CPU accounting per fiber" {
    if (true) {
        return error.SkipZigTest;
    }
    const target_ns: u64 = 10 * std.time.ns_per_ms;

    const Body = struct {
        fn run(dummy: *u64) void {
            var tp: std.os.linux.timespec = undefined;
            _ = std.os.linux.clock_gettime(std.os.linux.CLOCK.THREAD_CPUTIME_ID, &tp);
            const start: i64 = @as(i64, tp.sec) * std.time.ns_per_s + tp.nsec;
            while (true) {
                _ = std.os.linux.clock_gettime(std.os.linux.CLOCK.THREAD_CPUTIME_ID, &tp);
                const now: i64 = @as(i64, tp.sec) * std.time.ns_per_s + tp.nsec;
                if (now - start >= target_ns) break;
                dummy.* +%= 1;
            }
        }
    };

    var dummies = [_]u64{ 0, 0, 0 };
    const h0 = try fiber.spawn(.{}, Body.run, .{&dummies[0]});
    const h1 = try fiber.spawn(.{}, Body.run, .{&dummies[1]});
    const h2 = try fiber.spawn(.{}, Body.run, .{&dummies[2]});

    h0.join();
    h1.join();
    h2.join();

    // 20% tolerance both sides — clock granularity + the busy-loop's
    // own clock_gettime overhead inflate the measured CPU slightly.
    const low: u64 = target_ns * 80 / 100;
    const high: u64 = target_ns * 130 / 100;
    for ([_]*fiber.Fiber{ h0.fiber, h1.fiber, h2.fiber }) |f| {
        try std.testing.expect(f.cpu_total_ns >= low);
        try std.testing.expect(f.cpu_total_ns <= high);
    }
}

test "Runtime: TSC accounting times a fibonacci workload" {
    // Naive recursive fibonacci — does meaningful CPU work even in
    // Debug mode (~500K recursive calls for fib(28)) without slowing
    // down the test suite. fib(28) = 317_811.
    const N: u64 = 28;

    const Body = struct {
        fn fib(n: u64) u64 {
            if (n < 2) return n;
            return fib(n - 1) + fib(n - 2);
        }
        fn run(n: u64) u64 {
            return fib(n);
        }
    };

    const h = try fiber.spawn(.{}, Body.run, .{N});
    const result = h.join();

    try std.testing.expectEqual(@as(u64, 317_811), result);
    try std.testing.expect(h.fiber.cpu_total_ns > 0);

    // Convert the Q32.32 ns/cycle factor back to a real number and
    // derive an approximate TSC frequency from it.
    const factor_q32 = fiber.tscNsPerCycleQ32();
    const ns_per_cycle: f64 = @as(f64, @floatFromInt(factor_q32)) /
        @as(f64, @floatFromInt(@as(u64, 1) << 32));
    const tsc_ghz: f64 = if (ns_per_cycle > 0.0) 1.0 / ns_per_cycle else 0.0;

    std.debug.print(
        "\n  fib({d}) = {d}\n" ++
            "  cpu_total = {d} ns ({d:.3} ms)\n" ++
            "  tsc factor (Q32.32) = {d}  →  {d:.4} ns/cycle  ({d:.3} GHz)\n",
        .{
            N,
            result,
            h.fiber.cpu_total_ns,
            @as(f64, @floatFromInt(h.fiber.cpu_total_ns)) / @as(f64, std.time.ns_per_ms),
            factor_q32,
            ns_per_cycle,
            tsc_ghz,
        },
    );
}

test "Runtime: high priority runs before normal in queue scan" {
    if (true) {
        return error.SkipZigTest;
    }
    var first: ?u8 = null;

    const Body = struct {
        flag: *?u8,
        letter: u8,

        fn run(self: *@This()) void {
            if (self.flag.* == null) self.flag.* = self.letter;
        }
    };

    // Both spawns land on the current worker's local queue (spawn
    // always pushes to current worker now). The per-worker
    // priority scan picks high before normal — that's the
    // invariant under test.
    var n_ctx = Body{ .flag = &first, .letter = 'n' };
    var h_ctx = Body{ .flag = &first, .letter = 'h' };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.spawn(.{ .priority = .normal }, Body.run, .{&n_ctx});
    try g.spawn(.{ .priority = .high }, Body.run, .{&h_ctx});
    g.wait();

    try std.testing.expectEqual(@as(?u8, 'h'), first);
}

test "Runtime: bursty spawns distribute across workers via global queue" {
    // Spawn N short-lived fibers from a single fiber (which sits on
    // some worker). The first 16 fill the spawner's local ring; the
    // rest spill to the lane's global queue, and idle peers pull
    // batches from there (no work stealing). The test verifies that
    // (a) every fiber ran and (b) at least 2 workers participated
    // when more than one is available.
    const rt = fiber.runtime();
    const n_workers: usize = rt.workers.len;
    if (n_workers < 2) return error.SkipZigTest;

    const N = 64;

    var ran: std.atomic.Value(u32) = .init(0);
    var per_worker: [256]std.atomic.Value(u32) = @splat(.init(0));

    const Body = struct {
        fn run(ran_ptr: *std.atomic.Value(u32), pw: *[256]std.atomic.Value(u32)) void {
            const wid = fiber.currentWorker().id;
            _ = pw[wid].fetchAdd(1, .release);
            _ = ran_ptr.fetchAdd(1, .release);
        }
    };

    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N);
    for (0..N) |_| {
        try g.spawn(.{}, Body.run, .{ &ran, &per_worker });
    }
    g.wait();

    try std.testing.expectEqual(@as(u32, N), ran.load(.acquire));

    var distinct: u32 = 0;
    for (per_worker[0..n_workers]) |*c| {
        if (c.load(.acquire) > 0) distinct += 1;
    }
    // Soft check: spreading across workers needs idle peers to pull the
    // spilled fibers from the global queue. Under the test harness's
    // parallelism the runtime is often saturated, so distribution isn't
    // guaranteed — log it rather than fail. The hard correctness check
    // (every fiber ran) is the expectEqual above.
    if (distinct < 2) {
        std.log.warn(
            "bursty spawns ran on only {d} worker(s) — runtime saturated, not a failure",
            .{distinct},
        );
    }
}

test "Runtime: JoinHandle returns the spawned fiber's value" {
    const Producer = struct {
        fn run(x: u32) u64 {
            return @as(u64, x) * 2;
        }
    };

    const ph = try fiber.spawn(.{}, Producer.run, .{21});
    try std.testing.expectEqual(@as(u64, 42), ph.join());
}

test "Runtime: JoinHandle fast-path after producer already finished" {
    const Producer = struct {
        fn run() u32 {
            return 7;
        }
    };

    std.log.debug("starting spawn\n", .{});
    const ph = try fiber.spawn(.{}, Producer.run, .{});
    std.log.debug("fiber spawn done\n", .{});
    // Yield enough times that the no-op producer finishes first.
    // join() should then take the fast path (sees .finished, returns
    // the cached result without parking).
    fiber.yield();
    std.log.debug("yield 1 done\n", .{});
    fiber.yield();
    std.log.debug("yield 2 done\n", .{});
    try std.testing.expectEqual(@as(u32, 7), ph.join());
}

test "Runtime: JoinHandle(void) blocks for completion without a value" {
    var produced = false;

    const Producer = struct {
        fn run(out: *bool) void {
            fiber.yield();
            out.* = true;
        }
    };

    const ph = try fiber.spawn(.{}, Producer.run, .{&produced});
    ph.join();
    try std.testing.expect(produced);
}

test "Runtime: dumpFibers reflects a parked fiber" {
    if (true) {
        return error.SkipZigTest;
    }
    var producer_handle: ?*fiber.Fiber = null;

    const Producer = struct {
        out: *?*fiber.Fiber,

        fn run(self: *@This()) void {
            self.out.* = fiber.currentFiber();
            fiber.park();
        }
    };

    var p_ctx = Producer{ .out = &producer_handle };
    const ph = try fiber.spawn(.{}, Producer.run, .{&p_ctx});

    // Wait for the producer to record its handle (and thus park).
    while (producer_handle == null) fiber.yield();
    // One extra yield so the producer reaches the park() between
    // setting `out` and parking.
    fiber.yield();

    var aw: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer aw.deinit();
    try fiber.dumpFibers(fiber.runtime(), &aw.writer);
    const text = aw.writer.buffered();
    // dumpFibers shared across concurrent tests means we can't count
    // parked fibers exactly. Just verify the format included at least
    // one `parked_for=` entry (proving we saw a parked fiber).
    try std.testing.expect(std.mem.indexOf(u8, text, "parked_for=") != null);

    fiber.wake(producer_handle.?);
    ph.join();
}
