const std = @import("std");
const zio = @import("zio");
const raft = @import("raft");
const zdb = @import("zdb");
const runtime = @import("runtime");
const zdb_server = @import("zdb_server");

pub const std_options = std.Options{
    .log_level = .debug,
};

const ParsedArgs = struct {
    id: raft.NodeId,
    cluster_id: u64,
    raft_addr: []const u8,
    client_addr: []const u8,
    data_dir: []const u8,
    seeds: []const []const u8,
    tick_ms: u32,
};

fn usage() void {
    std.debug.print(
        "usage: zdb-server --id <n> --cluster-id <n> --raft-addr <ip:port> " ++
            "--client-addr <ip:port> --data-dir <path> [--seeds <addr1,addr2,...>] [--tick-ms <n>]\n",
        .{},
    );
}

fn parseSeeds(text: []const u8, allocator: std.mem.Allocator) ![]const []const u8 {
    var seeds = std.ArrayList([]const u8).empty;
    errdefer seeds.deinit(allocator);
    var it = std.mem.splitScalar(u8, text, ',');
    while (it.next()) |s| {
        if (s.len > 0) try seeds.append(allocator, s);
    }
    return seeds.toOwnedSlice(allocator);
}

fn parseArgs(allocator: std.mem.Allocator, argv: []const []const u8) !ParsedArgs {
    var id: ?raft.NodeId = null;
    var cluster_id: u64 = 1;
    var raft_addr: ?[]const u8 = null;
    var client_addr: ?[]const u8 = null;
    var data_dir: ?[]const u8 = null;
    var seeds: []const []const u8 = &.{};
    var tick_ms: u32 = 20;

    var i: usize = 1;
    while (i < argv.len) {
        const arg = argv[i];
        if (std.mem.eql(u8, arg, "--id")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            id = try std.fmt.parseInt(raft.NodeId, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--cluster-id")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            cluster_id = try std.fmt.parseInt(u64, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--raft-addr")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            raft_addr = argv[i];
        } else if (std.mem.eql(u8, arg, "--client-addr")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            client_addr = argv[i];
        } else if (std.mem.eql(u8, arg, "--data-dir")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            data_dir = argv[i];
        } else if (std.mem.eql(u8, arg, "--seeds")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            seeds = try parseSeeds(argv[i], allocator);
        } else if (std.mem.eql(u8, arg, "--tick-ms")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            tick_ms = try std.fmt.parseInt(u32, argv[i], 10);
        } else {
            return error.InvalidArgs;
        }
        i += 1;
    }

    return .{
        .id = id orelse return error.InvalidArgs,
        .cluster_id = cluster_id,
        .raft_addr = raft_addr orelse return error.InvalidArgs,
        .client_addr = client_addr orelse return error.InvalidArgs,
        .data_dir = data_dir orelse return error.InvalidArgs,
        .seeds = seeds,
        .tick_ms = tick_ms,
    };
}

const log = std.log.scoped(.zdb_main);

fn deinitWithLog(comptime msg: []const u8, ptr: anytype) void {
    ptr.deinit();
    log.info("deinit: " ++ msg ++ " done", .{});
}

pub fn main() !void {
    defer log.info("shutdown complete", .{});
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    const args = parseArgs(allocator, argv) catch |err| {
        usage();
        return err;
    };
    defer allocator.free(args.seeds);

    log.info("starting zdb-server id={d} cluster={d} raft={s} client={s} data={s} tick={d}ms seeds={d}", .{
        args.id, args.cluster_id, args.raft_addr, args.client_addr, args.data_dir, args.tick_ms, args.seeds.len,
    });

    // Initialize VFS with 500 MB/s bandwidth limit.
    zdb.vfs.VFS.initGlobal(.{ .bytes_per_sec = 500 * 1024 * 1024 });

    // Runtime pool singleton — created first so all runtimes are available.
    const pool = try runtime.Pool.init(allocator, .{});
    defer deinitWithLog("pool", pool);
    log.info("runtime pool initialized", .{});

    // Open raft WAL.
    log.info("opening raft WAL at {s}", .{args.data_dir});
    var wal = try raft.DiskWAL.openPath(args.data_dir, allocator, 1024);
    defer deinitWithLog("wal", &wal);

    const wal_iface = wal.asRaftWAL();

    const node = try raft.Node.init(
        allocator,
        &.{},
        wal_iface,
        .{
            .id = args.id,
            .cluster_id = args.cluster_id,
            .listen_addr = args.raft_addr,
            .tick_interval_ms = args.tick_ms,
        },
    );
    defer deinitWithLog("node", node);

    log.info("raft node initialized id={d}", .{args.id});

    // Open zdb forest.
    log.info("opening forest at {s}", .{args.data_dir});
    var forest = try zdb.forest.Forest.open(allocator, .{
        .dir = args.data_dir,
    });
    defer {
        forest.close();
        log.info("deinit: forest done", .{});
    }
    log.info("forest opened", .{});

    // Storage interface bridges raft commits to forest.
    var storage_impl = zdb_server.StorageImpl{
        .forest = forest,
        .allocator = allocator,
    };
    var sm = storage_impl.asStateMachine();
    node.state_machine = &sm;

    // zio runtime must be created before raft_rt so its defer (deinit) runs last,
    // after all coroutines are cleaned up.
    const rt = try zio.Runtime.init(
        allocator,
        .{ .executors = .exact(1) },
    );
    defer deinitWithLog("zio_rt", rt);

    var raft_rt = raft.RaftRuntime.init(allocator, node);
    defer deinitWithLog("raft_rt", &raft_rt);

    // ZDB server (client-facing).
    var zdb_srv = try zdb_server.ZdbServer.init(
        allocator,
        forest,
        node,
        args.client_addr,
    );
    defer deinitWithLog("zdb_srv", &zdb_srv);

    log.info("all components initialized", .{});

    // appMain(&raft_rt, &zdb_srv, node, &args);
    // // Entry point: spawn appMain on the raft pool. It orchestrates everything.
    var handle = try pool.get(.raft).spawn(appMain, .{ &raft_rt, &zdb_srv, node, &args });
    handle.join();

    log.info("shutting down...", .{});
}

/// Runs on the raft pool. Spawns raft runtime, client server (on query pool
/// via runtime.pool() singleton), compaction loop (on background pool),
/// and waits for SIGINT to shut down.
fn appMain(raft_rt: *raft.RaftRuntime, zdb_srv: *zdb_server.ZdbServer, node: *raft.Node, args: *const ParsedArgs) void {
    log.info("inside appMain", .{});
    var group = zio.group();
    defer group.cancel();

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

    log.info("appMain: spawning raft runtime on raft pool", .{});
    group.spawn(runRaft, .{raft_rt}) catch return;

    log.info("appMain: spawning client server on query pool", .{});
    group.spawn(runServer, .{zdb_srv}) catch return;

    log.info("appMain: spawning compaction loop on background pool", .{});
    group.spawn(runCompaction, .{zdb_srv}) catch return;

    log.info("appMain: waiting for SIGINT (ctrl+c to shutdown)", .{});
    var sig = zio.Signal.init(.interrupt) catch return;
    defer sig.deinit();
    sig.wait() catch return;

    log.info("received SIGINT, shutting down...", .{});
    node.startShutdown();
}

fn runRaft(r: *raft.RaftRuntime) void {
    r.run() catch |err| {
        if (err == error.Canceled) return;
        std.log.err("raft runtime error: {s}", .{@errorName(err)});
        if (@errorReturnTrace()) |trace| {
            std.debug.dumpStackTrace(trace.*);
        }
    };
}

/// Spawn the client TCP accept loop on the query runtime pool.
/// Must be called from a zio coroutine. Blocks until cancelled.
fn runServer(s: *zdb_server.ZdbServer) void {
    var handle = runtime.pool().get(.query).spawn(
        zdb_server.ZdbServer.runClientLoop,
        .{s},
    ) catch |err| {
        log.err("runServer err: {}", .{err});
        return;
    };
    handle.join();
}

/// Spawn the compaction loop on the background runtime pool.
/// Must be called from a zio coroutine. Blocks until cancelled.
fn runCompaction(s: *zdb_server.ZdbServer) void {
    var handle = runtime.pool().get(.background).spawn(
        zdb_server.ZdbServer.runCompactionLoop,
        .{s},
    ) catch |err| {
        log.err("runCompaction err: {}", .{err});
        return;
    };
    handle.join();
}

fn runJoin(args: *const ParsedArgs, raft_rt: *raft.RaftRuntime) void {
    raft.joinCluster(
        raft_rt.allocator,
        args.seeds,
        args.cluster_id,
        args.id,
        args.raft_addr,
    ) catch |err| {
        std.log.err("cluster join failed: {s}", .{@errorName(err)});
    };
}
