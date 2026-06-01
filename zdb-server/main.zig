const std = @import("std");
const raft = @import("raft");
const zdb = @import("zdb");
const fiber = @import("fiber");
const zdb_server = @import("zdb_server");
const flags = @import("flags");
const utils = @import("utils");

pub const std_options = std.Options{
    .log_level = .debug,
};

/// `--listen` — where the server binds. Actual addresses are
/// `<host>:<port+offset>` for both raft and client. `offset` makes it
/// trivial to bring up N servers on one host without spelling out
/// per-node port assignments.
const ListenCfg = struct {
    host: []const u8 = "127.0.0.1",
    raft_port: u16 = 9000,
    client_port: u16 = 8000,
    offset: u16 = 0,

    pub const docs = .{
        .self = "Where the server listens. Both raft and client ports get +offset.",
        .host = "Hostname/IP to bind both the raft and client listeners on.",
        .raft_port = "Base port for raft peer connections. Actual port = raft_port + offset.",
        .client_port = "Base port for client/stats connections. Actual port = client_port + offset.",
        .offset = "Added to both raft_port and client_port. Convenient when running several servers on one host (e.g. offset=2 ⇒ raft=9002, client=8002).",
    };
};

/// `--raft` — Raft node identity and cluster wiring. `id` is required (must
/// be > 0). `seeds` is comma-separated; if empty, the node bootstraps as a
/// single-node cluster.
const RaftCfg = struct {
    id: u64 = 0,
    cluster_id: u64 = 1,
    seeds: []const u8 = "",
    tick_ms: u32 = 20,

    pub const docs = .{
        .self = "Raft identity and cluster wiring.",
        .id = "Raft node ID (required, must be > 0).",
        .cluster_id = "Cluster ID for handshake gating.",
        .seeds = "Comma-separated raft addresses of existing peers. Empty = bootstrap as a single-node cluster.",
        .tick_ms = "Raft tick interval in milliseconds.",
    };
};

/// `--data` — on-disk locations. `dir` is required; `wal_dir` defaults to
/// `dir` if empty (single-disk setup) but can be pointed at a faster
/// device (e.g. NVMe) for write-ahead log throughput.
const DataCfg = struct {
    dir: []const u8 = "",
    wal_dir: []const u8 = "",

    pub const docs = .{
        .self = "Storage paths. WAL can live on a faster device than the LSM forest.",
        .dir = "Directory holding the LSM forest. Required.",
        .wal_dir = "Directory holding the raft WAL. Empty = same as `dir`.",
    };
};

fn parseSeedList(text: []const u8, allocator: std.mem.Allocator) ![]const []const u8 {
    var list = std.ArrayList([]const u8).empty;
    errdefer list.deinit(allocator);
    var it = std.mem.splitScalar(u8, text, ',');
    while (it.next()) |s| {
        if (s.len > 0) try list.append(allocator, s);
    }
    return list.toOwnedSlice(allocator);
}

/// Dump `--help` from the flag registry. flags.zig renders one line per
/// field with default + docstring, plus a header per registered module.
fn usage(alloc: std.mem.Allocator) void {
    var help: std.ArrayList(u8) = .empty;
    defer help.deinit(alloc);
    flags.g().writeHelpAll(&help) catch {};
    const io = utils.context.g().io;
    const stderr = std.Io.File.stderr();
    stderr.writeStreamingAll(io, help.items) catch {};
    stderr.writeStreamingAll(io, "\n") catch {};
}

const log = std.log.scoped(.zdb_main);

fn deinitWithLog(comptime msg: []const u8, ptr: anytype) void {
    ptr.deinit();
    log.info("deinit: " ++ msg ++ " done", .{});
}

/// Bundle passed to appMain — the parsed-flags snapshots plus derived
/// fields. `raft_addr` / `client_addr` are computed from
/// `host:port+offset` once at startup and own their backing buffers.
/// `seeds` is the comma-split version of `raft.seeds`. `wal_dir` is
/// `data.wal_dir` if set, else `data.dir`.
const Args = struct {
    listen: ListenCfg,
    raft: RaftCfg,
    data: DataCfg,
    raft_addr: []const u8,
    client_addr: []const u8,
    seeds: []const []const u8,
    wal_dir: []const u8,
};

/// Set by the SIGINT/SIGTERM handler; appMain polls it to begin shutdown.
/// A signal handler must be async-signal-safe, so it does nothing but store
/// this flag — it never touches the fiber runtime.
var g_shutdown_requested: std.atomic.Value(bool) = .init(false);

fn shutdownSignalHandler(_: std.posix.SIG) callconv(.c) void {
    g_shutdown_requested.store(true, .release);
}

/// Install SIGINT/SIGTERM handlers that flag shutdown. POSIX-only for now;
/// Windows console-ctrl handling is a TODO.
fn installShutdownSignals() void {
    if (@import("builtin").os.tag == .windows) return;
    const posix = std.posix;
    var sa = posix.Sigaction{
        .handler = .{ .handler = shutdownSignalHandler },
        .mask = posix.sigemptyset(),
        .flags = posix.SA.RESTART,
    };
    posix.sigaction(posix.SIG.INT, &sa, null);
    posix.sigaction(posix.SIG.TERM, &sa, null);
}

pub fn main(init: std.process.Init) !void {
    defer log.info("shutdown complete", .{});
    utils.context.initGlobal(init);
    const allocator = utils.context.g().gpa;
    const argv = try utils.context.g().argv();

    // The in-tree fiber runtime. One worker per core; worker id 0 is the
    // dedicated `.raft` lane so raft loops get priority isolation, the rest
    // form the `.default` lane (client / compaction / snapshot work).
    // `run()` blocks this thread (worker 0) until `fiber.stop()`, which
    // `bootstrapAndStop` calls when bootstrap returns (i.e. on shutdown).
    const cpus = std.Thread.getCpuCount() catch 4;
    var fiber_rt: fiber.Runtime = undefined;
    try fiber_rt.init(allocator, .{ .num_workers = cpus, .raft_workers = 1 });
    defer fiber_rt.deinit();
    log.info("runtime initialized: {d} workers", .{cpus});

    _ = try fiber.spawn(.{}, bootstrapAndStop, .{ allocator, argv });
    fiber_rt.run();
}

/// Spawned on the default lane to drive process bootstrap. Stops the
/// runtime (unblocks `fiber_rt.run()` in `main`) whenever bootstrap
/// returns — normal shutdown or an early error.
fn bootstrapAndStop(allocator: std.mem.Allocator, argv: []const []const u8) void {
    defer fiber.stop();
    bootstrap(allocator, argv) catch |err| {
        log.err("bootstrap failed: {s}", .{@errorName(err)});
    };
}

fn bootstrap(allocator: std.mem.Allocator, argv: []const []const u8) !void {
    flags.initGlobal(allocator);
    defer flags.deinitGlobal();
    const live_listen = try flags.g().register("listen", ListenCfg);
    const live_raft = try flags.g().register("raft", RaftCfg);
    const live_data = try flags.g().register("data", DataCfg);
    const live_snapshot = try flags.g().register("snapshot", zdb_server.SnapshotCfg);

    // --help is terminal; handle after registration so writeHelpAll has
    // every module's fields populated.
    for (argv[1..]) |a| {
        if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            usage(allocator);
            return;
        }
    }

    var leftover: std.ArrayList([]const u8) = .empty;
    defer leftover.deinit(allocator);
    flags.g().parseArgv(argv[1..], &leftover) catch |err| {
        std.log.err("flag parse failed: {s}", .{@errorName(err)});
        usage(allocator);
        std.process.exit(1);
    };
    if (leftover.items.len != 0) {
        std.log.err("unexpected positional '{s}' — config goes through --listen/--raft/--data flags", .{leftover.items[0]});
        usage(allocator);
        std.process.exit(1);
    }

    const listen = live_listen.snapshot();
    const raft_cfg = live_raft.snapshot();
    const data_cfg = live_data.snapshot();
    if (raft_cfg.id == 0 or data_cfg.dir.len == 0) {
        std.log.err("--raft requires id (>0); --data requires dir", .{});
        usage(allocator);
        std.process.exit(1);
    }

    // Resolve listen addresses from host + base port + offset.
    const raft_addr = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ listen.host, listen.raft_port + listen.offset });
    defer allocator.free(raft_addr);
    const client_addr = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ listen.host, listen.client_port + listen.offset });
    defer allocator.free(client_addr);

    // wal_dir defaults to data dir when empty — the common single-disk
    // case. Setting it lets you put WAL on a faster device than the LSM.
    const wal_dir = if (data_cfg.wal_dir.len > 0) data_cfg.wal_dir else data_cfg.dir;

    const seeds = try parseSeedList(raft_cfg.seeds, allocator);
    defer allocator.free(seeds);
    const args: Args = .{
        .listen = listen,
        .raft = raft_cfg,
        .data = data_cfg,
        .raft_addr = raft_addr,
        .client_addr = client_addr,
        .seeds = seeds,
        .wal_dir = wal_dir,
    };

    log.info("starting zdb-server id={d} cluster={d} raft={s} client={s} dir={s} wal={s} tick={d}ms seeds={d}", .{
        raft_cfg.id, raft_cfg.cluster_id, raft_addr, client_addr, data_cfg.dir, wal_dir, raft_cfg.tick_ms, seeds.len,
    });

    // Initialize VFS with bandwidth limit.
    zdb.vfs.VFS.initGlobal(.{ .bytes_per_sec = 1024 * 1024 * 1024 });

    // Initialize global metrics before anything that might increment them.
    // zdb owns storage-layer metrics; raft owns its own propose-phase
    // histograms — they live in separate Metrics structs but both surface
    // through /stats once the server's handleStats walks both globals.
    try zdb.metrics.initGlobal(allocator);
    defer zdb.metrics.deinitGlobal();
    try raft.metrics.initGlobal(allocator);
    defer raft.metrics.deinitGlobal();

    // Open raft WAL.
    log.info("opening raft WAL at {s}", .{wal_dir});
    var wal = try raft.DiskWAL.openPath(wal_dir, allocator, 1024);
    defer deinitWithLog("wal", &wal);

    const node = try raft.Node.init(
        allocator,
        &.{},
        &wal,
        .{
            .id = raft_cfg.id,
            .cluster_id = raft_cfg.cluster_id,
            .listen_addr = raft_addr,
            .tick_interval_ms = raft_cfg.tick_ms,
        },
    );
    defer deinitWithLog("node", node);

    log.info("raft node initialized id={d}", .{raft_cfg.id});

    // Open zdb forest.
    log.info("opening forest at {s}", .{data_cfg.dir});
    var forest = try zdb.forest.Forest.open(allocator, .{
        .dir = data_cfg.dir,
    });
    defer {
        forest.close();
        log.info("deinit: forest done", .{});
    }
    log.info("forest opened", .{});

    // Wire the file_id allocator: node_id = raft node id; initial local
    // counter restored from HardState's persisted max_file_id (any id we
    // could have written to disk before the crash is < that value, so we
    // resume past every possibly-orphaned SST without a disk scan).
    forest.file_ids.bind(
        @intCast(raft_cfg.id),
        node.log.hardState().max_file_id,
        @ptrCast(&wal),
        raft.DiskWAL.persistMaxFileId,
    );

    // Storage interface bridges raft commits to forest.
    var storage_impl = zdb_server.StorageImpl{
        .forest = forest,
        .allocator = allocator,
        .node = node,
    };
    var sm = storage_impl.asStateMachine();
    node.state_machine = &sm;

    var raft_rt = raft.RaftRuntime.init(allocator, node);
    defer deinitWithLog("raft_rt", &raft_rt);

    // ZDB server (client-facing).
    var zdb_srv = try zdb_server.ZdbServer.init(
        allocator,
        forest,
        node,
        &storage_impl,
        live_snapshot,
        args.client_addr,
    );
    defer deinitWithLog("zdb_srv", &zdb_srv);

    log.info("all components initialized", .{});

    // appMain(&raft_rt, &zdb_srv, node, &args);
    // // Entry point: spawn appMain on the raft pool. It orchestrates everything.
    var handle = try fiber.spawn(.{ .lane = .raft }, appMain, .{ &raft_rt, &zdb_srv, node, &args });
    handle.join();

    log.info("shutting down...", .{});
}

/// Runs on the shared runtime (.raft group). Spawns raft runtime, client
/// server (.query), compaction + snapshot loops (.background) — all via
/// `runtime.spawnOn` — then waits for SIGINT or SIGTERM to shut down.
fn appMain(raft_rt: *raft.RaftRuntime, zdb_srv: *zdb_server.ZdbServer, node: *raft.Node, args: *const Args) void {
    log.info("inside appMain", .{});
    // The four shim fibers (runRaft, runServer, runCompaction, runSnapshot)
    // run alongside appMain on the .raft runtime. When the signal handler
    // fires below, `defer g.cancel()` propagates cancellation into each
    // shim's `handle.join()`, which in turn cancels the inner loop running
    // on its actual target runtime (.query / .background).
    var g = fiber.Group(void).init(node.allocator);
    defer g.deinit();

    // Initialize transport (needed for both join and raft loops).
    node.initTransport() catch |err| {
        log.err("failed to init transport: {s}", .{@errorName(err)});
        return;
    };

    // If we have seeds, join the cluster before starting raft loops.
    // This ensures the node receives its add_learner entry and doesn't
    // accidentally bootstrap as a single-node cluster.
    if (args.seeds.len > 0) {
        log.info("appMain: joining cluster via seeds", .{});
        runJoin(args, raft_rt);
    }

    // Sequenced startup, before any raft / compaction / snapshot loop runs:
    //   1. Install the snapshot manifest. Forest is now in a valid state.
    //   2. Synchronously apply every committed WAL entry on disk
    //      (apply_index → commit_index).
    // This eliminates the race where the compaction loop would propose
    // new SST ids that collide with ids replay is about to install
    // (PathAlreadyExists).
    log.info("appMain: synchronous recovery", .{});
    zdb_srv.recoverFromSnapshot() catch |err| {
        log.err("recoverFromSnapshot err: {}", .{err});
        return;
    };

    // It's safe to apply all the commits before joining Raft group, because if
    // a log entry was acknowledged by this node, it would already have
    // downloaded the SSTable files, because we consider them to be part of the
    // WAL log. We have everything we need to replay the committed log.
    log.info("appMain: synchronous applyCommits", .{});
    node.applyCommits();

    log.info("appMain: spawning raft runtime on raft pool", .{});
    g.spawn(.{}, runRaft, .{raft_rt}) catch |err| {
        log.err("appMain: spawn runRaft failed: {s}", .{@errorName(err)});
        return;
    };

    log.info("appMain: spawning client server on query pool", .{});
    g.spawn(.{}, runServer, .{zdb_srv}) catch |err| {
        log.err("appMain: spawn runServer failed: {s}", .{@errorName(err)});
        return;
    };

    log.info("appMain: spawning compaction loop on background pool", .{});
    g.spawn(.{}, runCompaction, .{zdb_srv}) catch |err| {
        log.err("appMain: spawn runCompaction failed: {s}", .{@errorName(err)});
        return;
    };

    log.info("appMain: spawning snapshot loop on background pool", .{});
    g.spawn(.{}, runSnapshot, .{zdb_srv}) catch |err| {
        log.err("appMain: spawn runSnapshot failed: {s}", .{@errorName(err)});
        return;
    };

    installShutdownSignals();
    log.info("appMain: waiting for SIGINT/SIGTERM to shutdown", .{});
    // Poll the async-signal-safe flag (the handler can't touch the runtime).
    // 100 ms is plenty responsive for Ctrl-C; `isShuttingDown()` also covers
    // shutdown requested from elsewhere (e.g. a fatal loop error).
    while (!g_shutdown_requested.load(.acquire) and !node.isShuttingDown()) {
        fiber.sleep(100);
    }
    log.info("received shutdown signal, shutting down...", .{});
    node.startShutdown();
    g.wait();
}

fn runRaft(r: *raft.RaftRuntime) void {
    r.run() catch |err| {
        if (err == error.Canceled) return;
        std.log.err("raft runtime error: {s}", .{@errorName(err)});
        if (@errorReturnTrace()) |bt| {
            // Zig 0.16 split StackTrace: `@errorReturnTrace` returns
            // `builtin.StackTrace`, but `dumpStackTrace` expects
            // `debug.StackTrace`. Bridge by repacking the fields.
            const dbg_trace: std.debug.StackTrace = .{
                .return_addresses = bt.instruction_addresses[0..bt.index],
                .skipped = .none,
            };
            std.debug.dumpStackTrace(&dbg_trace);
        }
    };
}

/// Spawn the client TCP accept loop on the query runtime pool.
/// Must be called from a zio coroutine. Blocks until cancelled.
fn runServer(s: *zdb_server.ZdbServer) void {
    var handle = fiber.spawn(.{}, zdb_server.ZdbServer.runClientLoop, .{s}) catch |err| {
        log.err("runServer err: {}", .{err});
        return;
    };
    handle.join();
}

/// Spawn the snapshot loop on the background runtime pool. Leader-only loop
/// that polls WAL size/age and proposes snapshots through Raft.
/// Must be called from a zio coroutine. Blocks until cancelled.
fn runSnapshot(s: *zdb_server.ZdbServer) void {
    var handle = fiber.spawn(.{}, zdb_server.ZdbServer.runSnapshotLoop, .{s}) catch |err| {
        log.err("runSnapshot err: {}", .{err});
        return;
    };
    handle.join();
}

/// Spawn the compaction loop on the background runtime pool.
/// Must be called from a zio coroutine. Blocks until cancelled.
fn runCompaction(s: *zdb_server.ZdbServer) void {
    var handle = fiber.spawn(.{}, zdb_server.ZdbServer.runCompactionLoop, .{s}) catch |err| {
        log.err("runCompaction err: {}", .{err});
        return;
    };
    handle.join();
}

fn runJoin(args: *const Args, raft_rt: *raft.RaftRuntime) void {
    raft.joinCluster(
        raft_rt.allocator,
        args.seeds,
        args.raft.cluster_id,
        args.raft.id,
        args.raft_addr,
    ) catch |err| {
        std.log.err("cluster join failed: {s}", .{@errorName(err)});
    };
}
