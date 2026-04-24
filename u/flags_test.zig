const std = @import("std");
const flags = @import("flags.zig");

const RaftCfg = struct {
    name: []const u8 = "default",
    retries: u32 = 3,
    ratio: f64 = 0.5,
    tracing: bool = false,
    tick_interval_ms: u32 = 10,

    pub const docs = .{
        .self = "Raft consensus tunables. Hot-reloadable.",
        .name = "Human-readable identifier for this raft node.",
        .retries = "Number of attempts before giving up on a proposal.",
        .ratio = "Sampling ratio in [0,1].",
        .tracing = "Emit per-event trace logs (very chatty).",
        .tick_interval_ms = "Wall-clock tick period for the raft loop.",
    };
};

const NetCfg = struct {
    listen_addr: []const u8 = ":9000",
    enable_tls: bool = false,

    pub const docs = .{
        .listen_addr = "Address the server listens on.",
        .enable_tls = "Whether incoming connections require TLS.",
    };
};

// ---------- Live(T) tests ----------

test "init with empty input uses defaults" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "");
    defer live.deinit();
    const c = live.snapshot();
    try std.testing.expectEqualStrings("default", c.name);
    try std.testing.expectEqual(@as(u32, 3), c.retries);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), c.ratio, 1e-9);
    try std.testing.expect(!c.tracing);
    try std.testing.expectEqual(@as(u32, 10), c.tick_interval_ms);
}

test "init applies partial overrides" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "retries=5;");
    defer live.deinit();
    const c = live.snapshot();
    try std.testing.expectEqual(@as(u32, 5), c.retries);
    try std.testing.expectApproxEqAbs(@as(f64, 0.5), c.ratio, 1e-9);
}

test "update merges, leaves unspecified fields alone" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "retries=5; ratio=0.9;");
    defer live.deinit();
    try live.update("tracing=true;");
    const c = live.snapshot();
    try std.testing.expectEqual(@as(u32, 5), c.retries);
    try std.testing.expectApproxEqAbs(@as(f64, 0.9), c.ratio, 1e-9);
    try std.testing.expect(c.tracing);
}

test "update rejects unknown key, leaves state intact" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "retries=5;");
    defer live.deinit();
    const v_before = live.version();
    try std.testing.expectError(error.UnknownOption, live.update("mystery=1;"));
    const c = live.snapshot();
    try std.testing.expectEqual(@as(u32, 5), c.retries);
    try std.testing.expectEqual(v_before, live.version());
}

test "update rejects malformed value, leaves state intact" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "retries=5;");
    defer live.deinit();
    try std.testing.expectError(error.InvalidInt, live.update("retries=abc;"));
    try std.testing.expectEqual(@as(u32, 5), live.snapshot().retries);
}

test "string field arena lifetime survives later updates" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "name=first;");
    defer live.deinit();
    const snap1 = live.snapshot();
    try std.testing.expectEqualStrings("first", snap1.name);

    try live.update("name=second;");
    try live.update("name=third;");
    // snap1 still references the original arena slice; it must still equal "first".
    try std.testing.expectEqualStrings("first", snap1.name);
    try std.testing.expectEqualStrings("third", live.snapshot().name);
}

test "_ms duration field accepts unit notation" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "tick-interval-ms=500ms;");
    defer live.deinit();
    try std.testing.expectEqual(@as(u32, 500), live.snapshot().tick_interval_ms);

    try live.update("tick-interval-ms=2s;");
    try std.testing.expectEqual(@as(u32, 2000), live.snapshot().tick_interval_ms);

    try live.update("tick-interval-ms=1h;");
    try std.testing.expectEqual(@as(u32, 3_600_000), live.snapshot().tick_interval_ms);

    try live.update("tick-interval-ms=42;");
    try std.testing.expectEqual(@as(u32, 42), live.snapshot().tick_interval_ms);
}

test "bool accepts short and long forms (case-insensitive)" {
    const alloc = std.testing.allocator;
    const trues = [_][]const u8{ "true", "TRUE", "t", "T", "1", "yes", "y", "on", "ON" };
    const falses = [_][]const u8{ "false", "f", "F", "0", "no", "n", "off", "OFF" };
    for (trues) |v| {
        const opt = try std.fmt.allocPrint(alloc, "tracing={s};", .{v});
        defer alloc.free(opt);
        var live = try flags.Live(RaftCfg).init(alloc, opt);
        defer live.deinit();
        try std.testing.expect(live.snapshot().tracing);
    }
    for (falses) |v| {
        const opt = try std.fmt.allocPrint(alloc, "tracing={s};", .{v});
        defer alloc.free(opt);
        var live = try flags.Live(RaftCfg).init(alloc, opt);
        defer live.deinit();
        try std.testing.expect(!live.snapshot().tracing);
    }
    var bad = try flags.Live(RaftCfg).init(alloc, "");
    defer bad.deinit();
    try std.testing.expectError(error.InvalidBool, bad.update("tracing=garbage;"));
}

test "key name mapping accepts all reasonable variants" {
    const alloc = std.testing.allocator;
    const variants = [_][]const u8{
        "tick-interval-ms=42;",
        "tick_interval_ms=42;",
        "Tick-Interval-MS=42;",
        "TICK_INTERVAL_MS=42;",
    };
    for (variants) |v| {
        var live = try flags.Live(RaftCfg).init(alloc, v);
        defer live.deinit();
        try std.testing.expectEqual(@as(u32, 42), live.snapshot().tick_interval_ms);
    }
}

test "version monotonic on success, unchanged on failure" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "");
    defer live.deinit();
    try std.testing.expectEqual(@as(u64, 0), live.version());
    try live.update("retries=1;");
    try std.testing.expectEqual(@as(u64, 1), live.version());
    try live.update("retries=2;");
    try std.testing.expectEqual(@as(u64, 2), live.version());
    try std.testing.expectError(error.UnknownOption, live.update("nope=1;"));
    try std.testing.expectEqual(@as(u64, 2), live.version());
}

const ConcurrentCtx = struct {
    live: *flags.Live(RaftCfg),
    stop: *std.atomic.Value(bool),
    err: *std.atomic.Value(u16),
};

fn readerLoop(ctx: ConcurrentCtx) void {
    while (!ctx.stop.load(.acquire)) {
        const c = ctx.live.snapshot();
        // Touch every field so any torn state would surface.
        std.mem.doNotOptimizeAway(c.retries);
        std.mem.doNotOptimizeAway(c.tracing);
        std.mem.doNotOptimizeAway(c.tick_interval_ms);
        std.mem.doNotOptimizeAway(c.ratio);
        if (c.name.len == 0) {
            _ = ctx.err.cmpxchgStrong(0, 1, .acq_rel, .monotonic);
            return;
        }
    }
}

test "concurrent readers do not see torn state" {
    const alloc = std.testing.allocator;
    var live = try flags.Live(RaftCfg).init(alloc, "name=initial;");
    defer live.deinit();

    var stop = std.atomic.Value(bool).init(false);
    var err_flag = std.atomic.Value(u16).init(0);
    const ctx = ConcurrentCtx{ .live = &live, .stop = &stop, .err = &err_flag };

    var threads: [4]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, readerLoop, .{ctx});

    // Hammer with updates.
    var i: usize = 0;
    while (i < 200) : (i += 1) {
        const buf = try std.fmt.allocPrint(alloc, "name=v{d}; retries={d}; tracing={s};", .{
            i, @as(u32, @intCast(i % 10)), if (i % 2 == 0) "true" else "false",
        });
        defer alloc.free(buf);
        try live.update(buf);
    }

    stop.store(true, .release);
    for (threads) |t| t.join();
    try std.testing.expectEqual(@as(u16, 0), err_flag.load(.acquire));
}

// ---------- Registry tests ----------

test "registry: register returns Live pointer; dispatch routes to it" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);
    const net = try reg.register("net", NetCfg);

    try reg.update("raft", "tracing=true; retries=7;");
    try reg.update("net", "enable-tls=true;");

    try std.testing.expect(raft.snapshot().tracing);
    try std.testing.expectEqual(@as(u32, 7), raft.snapshot().retries);
    try std.testing.expect(net.snapshot().enable_tls);
    try std.testing.expectEqualStrings(":9000", net.snapshot().listen_addr);
}

test "registry: duplicate name rejected" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    _ = try reg.register("raft", RaftCfg);
    try std.testing.expectError(error.DuplicateModule, reg.register("raft", RaftCfg));
}

test "registry: unknown module returns error" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);
    try std.testing.expectError(error.UnknownModule, reg.update("ghost", "x=1;"));
    try std.testing.expectEqual(@as(u32, 3), raft.snapshot().retries);
}

test "registry: module name normalization" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("My_Module", RaftCfg);

    try reg.update("my-module", "retries=1;");
    try std.testing.expectEqual(@as(u32, 1), raft.snapshot().retries);
    try reg.update("MY_MODULE", "retries=2;");
    try std.testing.expectEqual(@as(u32, 2), raft.snapshot().retries);
    try reg.update("My-Module", "retries=3;");
    try std.testing.expectEqual(@as(u32, 3), raft.snapshot().retries);
}

test "registry: parseArgv happy path" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);
    const net = try reg.register("net", NetCfg);

    var leftover: std.ArrayList([]const u8) = .empty;
    defer leftover.deinit(alloc);
    const argv = [_][]const u8{
        "--raft=retries=7;",
        "--net",
        "listen-addr=:8080;",
        "data.db",
    };
    try reg.parseArgv(&argv, &leftover);
    try std.testing.expectEqual(@as(u32, 7), raft.snapshot().retries);
    try std.testing.expectEqualStrings(":8080", net.snapshot().listen_addr);
    try std.testing.expectEqual(@as(usize, 1), leftover.items.len);
    try std.testing.expectEqualStrings("data.db", leftover.items[0]);
}

test "registry: parseArgv strips surviving outer quotes from payload" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);
    const net = try reg.register("net", NetCfg);

    var leftover: std.ArrayList([]const u8) = .empty;
    defer leftover.deinit(alloc);
    const argv = [_][]const u8{
        "--raft=\"retries=7; ratio=0.6\"",
        "--net",
        "'listen-addr=:8080; enable-tls=true'",
    };
    try reg.parseArgv(&argv, &leftover);
    try std.testing.expectEqual(@as(u32, 7), raft.snapshot().retries);
    try std.testing.expectApproxEqAbs(@as(f64, 0.6), raft.snapshot().ratio, 1e-9);
    try std.testing.expectEqualStrings(":8080", net.snapshot().listen_addr);
    try std.testing.expect(net.snapshot().enable_tls);
}

test "registry: parseArgv unknown module" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);

    var leftover: std.ArrayList([]const u8) = .empty;
    defer leftover.deinit(alloc);
    const argv = [_][]const u8{ "--ghost=x=1;", "--raft=retries=2;" };
    try std.testing.expectError(error.UnknownModule, reg.parseArgv(&argv, &leftover));
    try std.testing.expectEqual(@as(u32, 3), raft.snapshot().retries);
}

test "registry: writeHelpAll renders both modules with their docs" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    _ = try reg.register("raft", RaftCfg);
    _ = try reg.register("net", NetCfg);

    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try reg.writeHelpAll(&out);
    std.debug.print("{s}\n", .{out.items});

    // raft has a self-doc; rendered next to the module name.
    try std.testing.expect(std.mem.indexOf(u8, out.items, "Raft consensus tunables. Hot-reloadable.\n") != null);
    // net has no self-doc; module name appears bare on its own line.
    try std.testing.expect(std.mem.indexOf(u8, out.items, "  net\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "tick-interval-ms=") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "listen-addr=") != null);
    // Field doc strings get rendered.
    try std.testing.expect(std.mem.indexOf(u8, out.items, "Wall-clock tick period for the raft loop.") != null);
    try std.testing.expect(std.mem.indexOf(u8, out.items, "Whether incoming connections require TLS.") != null);
}

test "global registry: initGlobal / g() / deinitGlobal" {
    const alloc = std.testing.allocator;
    flags.initGlobal(alloc);
    defer flags.deinitGlobal();

    const raft = try flags.g().register("raft", RaftCfg);
    try flags.g().update("raft", "retries=42;");
    try std.testing.expectEqual(@as(u32, 42), raft.snapshot().retries);
}

const MultiUpdaterCtx = struct {
    live: *flags.Live(RaftCfg),
    iters: usize,
    err: *std.atomic.Value(u16),
};

fn updaterLoop(ctx: MultiUpdaterCtx) void {
    var i: usize = 0;
    while (i < ctx.iters) : (i += 1) {
        var buf: [64]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, "retries={d};", .{i % 100}) catch {
            _ = ctx.err.cmpxchgStrong(0, 1, .acq_rel, .monotonic);
            return;
        };
        ctx.live.update(s) catch {
            _ = ctx.err.cmpxchgStrong(0, 2, .acq_rel, .monotonic);
            return;
        };
    }
}

test "registry: concurrent updates same module are serialised" {
    const alloc = std.testing.allocator;
    var reg = flags.Registry.init(alloc);
    defer reg.deinit();
    const raft = try reg.register("raft", RaftCfg);

    var err_flag = std.atomic.Value(u16).init(0);
    const ctx = MultiUpdaterCtx{ .live = raft, .iters = 100, .err = &err_flag };

    var threads: [4]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, updaterLoop, .{ctx});
    for (threads) |t| t.join();

    try std.testing.expectEqual(@as(u16, 0), err_flag.load(.acquire));
    try std.testing.expectEqual(@as(u64, 4 * 100), raft.version());
}
