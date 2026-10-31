// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");
const raft = @import("raft");
const zdb = @import("zdb");
const fiber = @import("zfiber");
const pgwire = @import("pgwire");
const engine = @import("engine");
const sql = @import("sql");
const flags = @import("flags");
const utils = @import("utils");
const String = utils.String;
// The build-selected allocator backend (see `-Dallocator` in
// build.zig): jemalloc or mimalloc behind one adapter API, exactly one
// linked per binary. `galloc.kind` names it at comptime.
const galloc = @import("galloc");

pub const std_options = std.Options{
    // Debug lines (raft prints three per apply batch) are compiled out
    // of release builds: std.log filters at comptime.
    .log_level = if (builtin.mode == .Debug) .debug else .info,
    // `2026-10-22T14:05:31.123456Z INFO  raft: ...`; see u/log.zig.
    .logFn = utils.log.logFn,
};

/// `--listen` — where the server binds. Actual addresses are
/// `<host>:<port+offset>` for both raft and client. `offset` makes it
/// trivial to bring up N servers on one host without spelling out
/// per-node port assignments.
const ListenCfg = struct {
    host: String = "127.0.0.1",
    raft_port: u16 = 4210,
    client_port: u16 = 5210,
    sql_port: u16 = 3210,
    offset: u16 = 0,

    pub const docs = .{
        .self = "Where the server listens. raft, admin (client), and SQL" ++
            " ports each get +offset.",
        .host = "Hostname/IP to bind the raft, admin, and SQL listeners on.",
        .raft_port = "Base port for raft peer connections. Actual port =" ++
            " raft_port + offset.",
        .client_port = "Base port for the admin protocol. Actual port =" ++
            " client_port + offset.",
        .sql_port = "Base port for the SQL listener (pgwire protocol)." ++
            " Actual port = sql_port + offset.",
        .offset = "Added to every port. Convenient when running several" ++
            " servers on one host: offset=1 moves every port up by one.",
    };
};

/// `--shutdown` — the drain between the first signal and the stop (see
/// fiber.Shutdown and appMain).
const ShutdownCfg = struct {
    grace_ms: u32 = 10_000,

    pub const docs = .{
        .self = "How a SIGINT/SIGTERM ends the process: a drain, then" ++
            " the stop. A second signal stops at once.",
        .grace_ms = "Milliseconds the drain waits for the statements in" ++
            " flight to finish before the loops stop and the rest are" ++
            " failed.",
    };
};

/// `--raft` — Raft node identity and cluster wiring. `id` is required (must
/// be > 0). `seeds` is comma-separated; if empty, the node bootstraps as a
/// single-node cluster.
const RaftCfg = struct {
    id: u64 = 1,
    cluster_id: u64 = 1,
    seeds: String = "",
    tick_ms: u32 = 20,

    pub const docs = .{
        .self = "Raft identity and cluster wiring.",
        .id = "Raft node ID, above 0. A single node is 1.",
        .cluster_id = "Cluster ID for handshake gating.",
        .seeds = "Comma-separated raft addresses of existing peers. Empty =" ++
            " bootstrap as a single-node cluster.",
        .tick_ms = "Raft tick interval in milliseconds.",
    };
};

/// `--disk-cache` — the local-disk chunk cache for an s3 remote.
const DiskCacheCfg = struct {
    path: String = "",
    capacity_bytes: u64 = 0,
    chunk_bytes: u64 = 0,

    pub const docs = .{
        .self = "Pages of SSTs fetched from an s3 remote, kept on local" ++
            " disk.",
        .path = "The slot file. Empty = no disk cache.",
        .capacity_bytes = "What the slot file holds at most.",
        .chunk_bytes = "Chunk size: what a miss fetches from the remote" ++
            " and the unit of eviction, a power of two from 8KiB to" ++
            " 8MiB, fixed when the slot file is created. 0 = 128KiB.",
    };
};

/// `--store` — the object store that holds every SST: a compaction or
/// flush puts its output there before proposing the manifest change,
/// and every node reads SSTs from it. Required. A cluster's nodes must
/// share one (an s3 bucket, or a dir on a shared mount); a plain local
/// dir serves a single node. The dir is write-once blobs only — never
/// the WAL dir. Exactly one of `dir_path` and `s3_endpoint` is set.
const StoreCfg = struct {
    dir_path: String = "egres-data/store",
    s3_endpoint: String = "",
    s3_bucket: String = "",
    s3_prefix: String = "",
    s3_region: String = "",

    pub const docs = .{
        .self = "Object store holding every SST: dir_path, or the" ++
            " s3_* fields, which win when s3_endpoint is set.",
        .dir_path = "Directory the SSTs live in: local, or a shared mount" ++
            " such as NFS for a cluster. Under the working directory" ++
            " unless set.",
        .s3_endpoint = "S3-compatible endpoint URL: http://host[:port]" ++
            " or https://host[:port]. The scheme decides TLS; the host" ++
            " may be a DNS name or IP literal. E.g." ++
            " https://s3.us-west-2.amazonaws.com or" ++
            " http://127.0.0.1:8333. Creds via AWS_ACCESS_KEY_ID /" ++
            " AWS_SECRET_ACCESS_KEY, plus AWS_SESSION_TOKEN for" ++
            " temporary ones.",
        .s3_bucket = "Bucket name. Required with s3_endpoint.",
        .s3_prefix = "Optional key prefix inside the bucket (unreserved URI" ++
            " characters only).",
        .s3_region = "SigV4 signing region. Empty = taken from the" ++
            " endpoint host (s3.<region>.<domain>, else us-east-1)." ++
            " Set it for a host that carries none, such as R2 or a" ++
            " VPC endpoint.",
    };
};

fn parseSeedList(text: String, allocator: std.mem.Allocator) ![]const String {
    var list = std.ArrayList(String).empty;
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
    stderr.writeStreamingAll(io, "\n" ++ probe_help ++ "\n") catch {};
}

const probe_help =
    \\Modes:
    \\  --help   this text
    \\  --probe  run the probes and exit: the disk calibration of every
    \\           data directory, and the store's ideal readahead (the
    \\           read size past which a request costs its transfer rather
    \\           than its round trip, for sizing a cache's chunks). The
    \\           numbers go to stdout; takes the same --store/--wal flags.
;

const log = std.log.scoped(.egres);

/// The default database, present from a node's very first boot — created by
/// the WAL's genesis entry (see seedGenesis), never at runtime. The SQL
/// layer owns the name (sessions with no database fall back to it).
const default_database = sql.default_database;

/// Seed a fresh WAL with the deterministic genesis prefix: entry 1 creates
/// the default database at the first user namespace id ('egres' is ns 16 on
/// every cluster). Every node writes the same bytes — fixed ids, term 0,
/// fixed timestamp — so raft's log matching holds across fresh nodes, and
/// the entry is committed by construction (HardState.commit_index covers
/// it): engine recovery applies it before any election or client traffic.
/// A restarted or joining node with a non-empty WAL skips this.
fn seedGenesis(allocator: std.mem.Allocator, wal: *raft.DiskWAL) !void {
    if (wal.lastIndex() != 0) return;

    // Same trees as forest.buildNsCreate — one per system table,
    // posting indexes as there; tree 0 (the raw-KV default table)
    // carries the database name.
    const sys = zdb.manifest.namespace_system_table_ids;
    var tc: [sys.len]zdb.manifest.TreeChange = undefined;
    for (sys, &tc) |table_id, *c|
        c.* = zdb.manifest.createTreeChange(
            zdb.kv.makeTreeId(table_id, 0),
            zdb.manifest.first_namespace,
            "",
            "",
            .posting_index,
        );
    tc[0].name = @constCast(default_database);
    const payload = try zdb.buffers.marshalRoot(allocator, zdb.manifest.ManifestChange{
        .cmd = .create_ns,
        .tree_changes = &tc,
        .sstable_changes = &.{},
        .memtable_id = 0,
    });
    defer allocator.free(payload);

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);
    var writer = try raft.buffers.LogEntryWriter.initWithBuf(allocator, &buf);
    defer writer.deinit();
    try writer.set(.{
        .index = 1,
        .term = 0,
        .timestamp = 1, // fixed; real HLC values are far larger
        .data = payload,
        .entry_type = .manifest,
    });
    _ = try writer.finishWithTail();
    const data = try buf.toOwnedSlice(allocator);
    var entry = try raft.buffers.LogEntryBuffer.initWithTail(allocator, data, .owned);
    defer entry.deinit();

    try wal.addEntries(&.{entry});
    try wal.sync();
    try wal.setHardState(.{ .commit_index = 1 });
    log.info("seeded genesis: database '{s}' -> ns={d}", .{ default_database, zdb.manifest.first_namespace });
}

fn deinitWithLog(comptime msg: String, ptr: anytype) void {
    ptr.deinit();
    log.info("deinit: " ++ msg ++ " done", .{});
}

/// Bundle passed to appMain — the parsed-flags snapshots plus derived
/// fields. `raft_addr` / `client_addr` are computed from
/// `host:port+offset` once at startup and own their backing buffers.
/// `seeds` is the comma-split version of `raft.seeds`.
const Args = struct {
    listen: ListenCfg,
    raft: RaftCfg,
    shutdown: ShutdownCfg,
    data: zdb.config.Config,
    raft_addr: String,
    client_addr: String,
    seeds: []const String,
    wal_dir: String,
};

pub fn main(init: std.process.Init) !void {
    defer log.info("shutdown complete", .{});
    // The process allocator, counted (`mem`, the cache governor).
    utils.context.initGlobalWithAllocator(init, galloc.allocator());
    const allocator = utils.context.g().gpa;
    // Fail fast if the gpa above ever stops routing to the build-selected
    // allocator: the embedded allocator policy, memory accounting, and
    // reclamation model all assume it.
    try galloc.verifyInUse(allocator);
    log.info("{s} in use: OK", .{galloc.kind});
    const argv = try utils.context.g().argv();

    // The in-tree fiber runtime. One worker per core; worker id 0 is the
    // dedicated `.raft` lane so raft loops get priority isolation, the rest
    // form the `.default` lane (client / compaction / snapshot work).
    // `run()` blocks this thread (worker 0) until `fiber.stop()`, which
    // `bootstrapAndStop` calls when bootstrap returns (i.e. on shutdown).
    var fiber_rt: fiber.Runtime = undefined;
    // num_workers/raft_workers omitted → auto: workers = CPU core count,
    // raft_workers ≈ 1/4 capped at 4 (see fiber.runtime.raftWorkerCount). The
    // coarse clock reads time via `context.g().io`, already initialized above.
    try fiber_rt.init(allocator, .{});
    defer fiber_rt.deinit();
    log.info("runtime initialized: {d} workers", .{fiber_rt.workers.len});

    // Without allocator background threads (mimalloc has none; jemalloc
    // has them on Linux only, see jemalloc/root.zig), freed memory
    // returns to the OS only when something enters the allocator after
    // a purge deadline passes, and a quiet arena has nobody to do that.
    // This fiber is that somebody; it isn't even compiled in where the
    // allocator reclaims on its own.
    if (comptime !galloc.has_background_reclaim) {
        try fiber.spawnDetached(.{}, allocMaintenance, .{});
    }

    try fiber.spawnDetached(.{}, bootstrapAndStop, .{ allocator, argv });
    fiber_rt.run();
}

/// Detached and never joined: parked in sleepMs at shutdown, freed by
/// Runtime.deinit like any other leftover fiber. The decay step runs on
/// an fsops pool thread, not inline: it purges with the arena lock held
/// and a big post-burst purge is many madvise calls, which would wedge
/// a worker for their whole duration. Under load it finds little to do
/// (the allocation path keeps active arenas decayed; ~5 µs for mimalloc
/// per the zmem measurement) and returns a freed burst to the OS within
/// a couple of ticks.
fn allocMaintenance() void {
    while (true) {
        fiber.sleepMs(1000);
        fiber.fsops.run(decayStep, .{});
    }
}

/// Runs on a pool thread, so it must not touch fiber state.
fn decayStep() void {
    galloc.decay() catch |err| {
        log.warn("allocator decay failed: {}", .{err});
    };
}

/// Spawned on the default lane to drive process bootstrap. Stops the
/// runtime (unblocks `fiber_rt.run()` in `main`) whenever bootstrap
/// returns — normal shutdown or an early error.
fn bootstrapAndStop(allocator: std.mem.Allocator, argv: []const String) void {
    defer fiber.stop();
    bootstrap(allocator, argv) catch |err| {
        log.err("bootstrap failed: {s}", .{@errorName(err)});
    };
}

/// Scheduler knobs for A/B runs; the defaults are the measured-best
/// settings. See zfiber.Runtime.steal_lone / prepark_poll_us.
/// `--wal` — where the raft WAL lives and how its log files are laid
/// out. The WAL (with the raft snapshot and hard state) is the one thing
/// Egres keeps on local disk; SSTs live in `--store`. The layout
/// defaults suit a local SSD on any filesystem: `prezero=auto`
/// zero-fills log files on ext4 and xfs and skips it on ZFS, btrfs and
/// tmpfs.
const WalCfg = struct {
    dir: String = "egres-data/wal",
    log_file_bytes: u64 = 256 << 20,
    prezero: raft.diskwal.Prezero = .auto,
    max_spare_log_files: usize = 64,

    pub const docs = .{
        .self = "Raft WAL location and log files (see raft/diskwal.zig" ++
            " OpenOptions).",
        .dir = "Directory holding the raft WAL. Under the working" ++
            " directory unless set.",
        .log_file_bytes = "Log file size; a record that would cross it" ++
            " rotates to a pre-created log file.",
        .prezero = "Zero-fill a log file when it is created so appends never" ++
            " grow the file: true for ext4/xfs, false for ZFS/btrfs/tmpfs," ++
            " auto decides by statfs.",
        .max_spare_log_files = "The most log files kept for reuse after a" ++
            " snapshot drops them. Fewer are kept when fewer were needed" ++
            " since the last snapshot, none on an idle node. 0 = never" ++
            " reuse. Only with prezero.",
    };
};

fn bootstrap(allocator: std.mem.Allocator, argv: []const String) !void {
    flags.initGlobal(allocator);
    defer flags.deinitGlobal();
    const live_listen = try flags.g().register("listen", ListenCfg);
    const live_raft = try flags.g().register("raft", RaftCfg);
    const live_shutdown = try flags.g().register("shutdown", ShutdownCfg);
    const live_zdb = try flags.g().register("zdb", zdb.config.Config);
    const live_store = try flags.g().register("store", StoreCfg);
    const live_memory = try flags.g().register(
        "memory",
        engine.governor.Cfg,
    );
    const live_disk_cache = try flags.g().register(
        "disk-cache",
        DiskCacheCfg,
    );
    const live_snapshot = try flags.g().register("snapshot", engine.SnapshotCfg);
    const live_runtime = try flags.g().register(
        "runtime",
        fiber.Runtime.Cfg,
    );
    const live_wal = try flags.g().register("wal", WalCfg);
    const live_io = try flags.g().register("io", zdb.vfs.calibrate.Cfg);

    // --help and --probe are modes, not settings: --help is terminal,
    // handled after registration so writeHelpAll has every module's
    // fields; --probe is taken off argv before the groups parse it.
    var probe = false;
    var settings: std.ArrayList(String) = .empty;
    defer settings.deinit(allocator);
    for (argv[1..]) |a| {
        if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            usage(allocator);
            return;
        }
        if (std.mem.eql(u8, a, "--probe")) {
            probe = true;
            continue;
        }
        try settings.append(allocator, a);
    }

    var leftover: std.ArrayList(String) = .empty;
    defer leftover.deinit(allocator);
    flags.g().parseArgv(settings.items, &leftover) catch |err| {
        log.err("flag parse failed: {s}", .{@errorName(err)});
        usage(allocator);
        std.process.exit(1);
    };
    if (leftover.items.len != 0) {
        log.err(
            "unexpected positional '{s}' — config goes through" ++
                " --listen/--raft/--zdb flags",
            .{leftover.items[0]},
        );
        usage(allocator);
        std.process.exit(1);
    }

    const listen = live_listen.cur();
    const raft_cfg = live_raft.cur();
    const shutdown_cfg = live_shutdown.cur();
    const zdb_cfg = live_zdb.cur();
    const store_cfg = live_store.cur();
    const memory_cfg = live_memory.cur();
    const disk_cache_cfg = live_disk_cache.cur();
    const wal_cfg = live_wal.cur();
    const io_cfg = live_io.cur();
    // The runtime is already up (main): its flags go on it directly,
    // before any listener spawns a connection fiber. The governor
    // re-applies them every tick.
    fiber.runtime().apply(live_runtime.cur());
    if (raft_cfg.id == 0 or wal_cfg.dir.len == 0) {
        log.err("--raft requires id (>0); --wal requires dir", .{});
        usage(allocator);
        std.process.exit(1);
    }
    if (!(memory_cfg.query_ratio > 0 and memory_cfg.query_ratio <= 1)) {
        log.err("--memory query_ratio must be in (0, 1]", .{});
        usage(allocator);
        std.process.exit(1);
    }
    // An endpoint selects S3; dir_path has a default, so it cannot.
    const store_dir = store_cfg.s3_endpoint.len == 0;
    if (store_dir and store_cfg.dir_path.len == 0) {
        log.err("--store needs dir_path or s3_endpoint", .{});
        std.process.exit(1);
    }
    if (!store_dir and store_cfg.s3_bucket.len == 0) {
        log.err("--store s3_endpoint requires s3_bucket", .{});
        std.process.exit(1);
    }
    if (store_dir and std.mem.eql(u8, store_cfg.dir_path, wal_cfg.dir)) {
        log.err("--store dir_path and --wal dir must differ", .{});
        std.process.exit(1);
    }

    // Resolve listen addresses from host + base port + offset.
    const raft_addr = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ listen.host, listen.raft_port + listen.offset });
    defer allocator.free(raft_addr);
    const client_addr = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ listen.host, listen.client_port + listen.offset });
    defer allocator.free(client_addr);
    // SQL (pgwire protocol) listen address.
    const sql_addr = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ listen.host, listen.sql_port + listen.offset });
    defer allocator.free(sql_addr);

    const wal_dir = wal_cfg.dir;

    const seeds = try parseSeedList(raft_cfg.seeds, allocator);
    defer allocator.free(seeds);
    // This release runs a single node: no joining, and no membership
    // changes over the admin socket (AdminServer.handleConf).
    if (seeds.len > 0) {
        log.err("--raft seeds: joining a cluster is not available in" ++
            " this release; start without seeds", .{});
        return error.ClusteringDisabled;
    }
    const args: Args = .{
        .listen = listen,
        .raft = raft_cfg,
        .shutdown = shutdown_cfg,
        .data = zdb_cfg,
        .raft_addr = raft_addr,
        .client_addr = client_addr,
        .seeds = seeds,
        .wal_dir = wal_dir,
    };

    // The I/O limiters: one per device egres writes, each from its
    // disk's measured profile unless a flag pins it, held by the VFS
    // the files on that device open through. The probes run before
    // anything else touches the devices. An s3 remote has no SST
    // device; its chunk cache's is probed instead.
    const calibrate = zdb.vfs.calibrate;
    try calibrate.initGlobal(io_cfg);
    const wal_vfs = try calibrate.deviceVfs(allocator, io_cfg, wal_dir);
    const data_vfs = if (store_dir)
        try calibrate.deviceVfs(allocator, io_cfg, store_cfg.dir_path)
    else
        null;
    const cache_vfs = if (disk_cache_cfg.path.len == 0)
        null
    else
        try calibrate.deviceVfs(
            allocator,
            io_cfg,
            std.fs.path.dirname(disk_cache_cfg.path) orelse ".",
        );
    // The meter is for the store: the tenants split the device their
    // data is on, the store's or the chunk cache's under s3, and only
    // that device charges them. A WAL disk of its own is not metered.
    const tenant_vfs = data_vfs orelse cache_vfs orelse wal_vfs;
    tenant_vfs.bw.metered = true;
    fiber.meter.capacity.io_pages_per_s = tenant_vfs.bw.pages_per_sec;

    log.info(
        "starting egres id={d} cluster={d} raft={s} admin={s} sql={s} " ++
            "wal={s} tick={d}ms seeds={d}",
        .{
            raft_cfg.id,      raft_cfg.cluster_id, raft_addr,
            client_addr,      sql_addr,            wal_dir,
            raft_cfg.tick_ms, seeds.len,
        },
    );

    // The remote store every SST lives in.
    var store: zdb.vfs.RemoteStore = if (store_dir)
        try zdb.vfs.RemoteStore.initFile(
            allocator,
            store_cfg.dir_path,
            data_vfs.?,
        )
    else
        try zdb.vfs.RemoteStore.initS3(allocator, .{
            .endpoint = store_cfg.s3_endpoint,
            .bucket = store_cfg.s3_bucket,
            .prefix = store_cfg.s3_prefix,
            .region = store_cfg.s3_region,
            .cache = .{
                .path = disk_cache_cfg.path,
                .capacity_bytes = disk_cache_cfg.capacity_bytes,
                .chunk_bytes = disk_cache_cfg.chunk_bytes,
                .vfs = cache_vfs,
            },
        });
    defer store.deinit();
    if (store_dir) {
        log.info("store: dir {s}", .{store_cfg.dir_path});
    } else {
        log.info("store: s3 {s}/{s}/{s}", .{
            store_cfg.s3_endpoint, store_cfg.s3_bucket,
            store_cfg.s3_prefix,
        });
    }
    // `--probe`: the probes' numbers instead of the server. The disks
    // were probed by deviceVfs above; the store's readahead runs here.
    // Returning is the shutdown path, so the store above is closed on
    // the way out.
    if (probe) {
        var out: std.ArrayList(u8) = .empty;
        defer out.deinit(allocator);
        for (calibrate.devices()) |d| {
            try out.print(allocator, "disk {s}: bandwidth_bytes={d}MiB " ++
                "write_cost={d}\n", .{
                d.path, d.cfg.bytes_per_sec >> 20, d.cfg.write_cost,
            });
        }
        const la = try store.calibrateLookahead(zdb_cfg.block_bytes);
        try out.print(allocator, "store lookahead: {d} bytes ({d} KiB)\n", .{
            la, la >> 10,
        });
        const io = utils.context.g().io;
        try std.Io.File.stdout().writeStreamingAll(io, out.items);
        return;
    }
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
    var wal = try raft.DiskWAL.openPath(wal_dir, allocator, .{
        .vfs = wal_vfs,
        .log_file_size = wal_cfg.log_file_bytes,
        .prezero = wal_cfg.prezero,
        .max_spare_log_files = wal_cfg.max_spare_log_files,
    });
    defer deinitWithLog("wal", &wal);
    // Genesis before the node reads the log: a fresh WAL gets its
    // deterministic bootstrap prefix (default database), pre-committed.
    try seedGenesis(allocator, &wal);

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

    // The SST block cache, created before the forest so it outlives every
    // open table. With blocks cached decompressed here, tables open
    // O_DIRECT: the page cache would otherwise hold the same data twice.
    const mem = utils.host.memory();
    const total = engine.governor.resolveTotal(memory_cfg.total_bytes);
    // Starts at the cap the governor's first tick would set.
    const start_cap = engine.governor.startCap(total);
    var block_cache = try zdb.cache.ShardedCache(u128).init(
        allocator,
        @intCast(start_cap),
        16,
    );
    defer block_cache.deinit();
    zdb.table.table.block_cache = &block_cache;
    zdb.table.table.read_mode = .direct;
    defer {
        zdb.table.table.block_cache = null;
        zdb.table.table.read_mode = .default;
    }
    engine.governor.init(total, &block_cache, live_memory);
    log.info(
        "block cache governed under total {d} MiB resident (host " ++
            "memory {d} MiB via {s}); sst reads direct",
        .{ total >> 20, mem.bytes >> 20, mem.source },
    );

    // Open zdb forest.
    log.info("opening forest", .{});
    zdb_cfg.validate() catch |err| {
        log.err("--zdb: invalid storage options: {s}", .{@errorName(err)});
        std.process.exit(1);
    };
    var forest = try zdb.forest.Forest.open(allocator, live_zdb, &store);
    defer {
        // Closing the forest frees every table, and a table drops its
        // blocks from the block cache as it goes. The whole cache is
        // freed right after, so skip that per-block work.
        zdb.table.table.block_cache = null;
        forest.close();
        log.info("deinit: forest done", .{});
    }
    log.info("forest opened", .{});

    // The replicated storage engine: builds the apply state machine + raft
    // runtime and installs the SM on the node. Owns all that node/forest
    // wiring.
    const eng = try engine.Engine.open(
        allocator,
        node,
        forest,
        live_snapshot,
        live_runtime,
    );
    defer deinitWithLog("engine", eng);

    // The admin protocol front-end over the engine.
    var admin_srv = try engine.AdminServer.init(eng, args.client_addr);
    defer deinitWithLog("admin_srv", &admin_srv);

    // The SQL front-end (pgwire protocol) over the same engine. Database
    // and table names live in the zdb manifest, so there is no separate SQL
    // store — DDL resolves names via the forest and proposes manifest
    // changes (see sql/server.zig).
    var sql_srv = try sql.Server.init(eng, sql_addr);
    defer deinitWithLog("sql_srv", &sql_srv);

    log.info("all components initialized", .{});

    var handle = try fiber.spawn(.{}, appMain, .{ eng, &admin_srv, &sql_srv, node, &args });
    handle.join();

    log.info("shutting down...", .{});
}

/// Drives the node lifecycle: bring up transport, (optionally) join the
/// cluster, then hand off to the engine (recovery + loops) and register the
/// admin front-end, finally blocking until a shutdown signal.
fn appMain(eng: *engine.Engine, admin_srv: *engine.AdminServer, sql_srv: *sql.Server, node: *raft.Node, args: *const Args) void {
    log.debug("inside appMain", .{});

    // Transport must be up before the cluster join (seeded nodes) and before
    // recovery fetches missing SSTs from the leader.
    node.initTransport() catch |err| {
        log.err("failed to init transport: {s}", .{@errorName(err)});
        return;
    };

    // Seeded nodes join before any loop runs, so they receive their
    // add_learner entry instead of bootstrapping as a single-node cluster.
    if (args.seeds.len > 0) {
        log.info("appMain: joining cluster via seeds", .{});
        runJoin(args, eng.allocator);
    }

    // Feed the commit stream to the SQL server's per-table caches (egid
    // counters). Register before start so no applied entry is missed; the
    // apply loop stops (eng.wait) before sql_srv is torn down.
    eng.setApplyObserver(sql_srv.applyObserver());

    // The engine owns recovery + applyCommits + the raft/compaction/snapshot
    // loops; register the admin front-end into its loop group.
    eng.start() catch |err| {
        log.err("engine.start failed: {s}", .{@errorName(err)});
        return;
    };
    eng.spawnListener(engine.AdminServer.runListener, .{admin_srv}) catch |err| {
        log.err("appMain: spawn admin server failed: {s}", .{@errorName(err)});
        return;
    };
    eng.spawnListener(sql.Server.runListener, .{sql_srv}) catch |err| {
        log.err("appMain: spawn sql server failed: {s}", .{@errorName(err)});
        return;
    };
    eng.spawnListener(sql.Server.runOracleRotation, .{sql_srv}) catch |err| {
        log.err("appMain: spawn oracle rotation failed: {s}", .{@errorName(err)});
        return;
    };

    node.shutdown.install() catch |err| {
        log.err("appMain: installing signal handlers failed: {s}", .{@errorName(err)});
        return;
    };
    log.info("appMain: waiting for SIGINT/SIGTERM to shutdown", .{});
    // Blocks until SIGINT/SIGTERM starts the drain, or an internal fatal
    // path starts the stop outright. In the drain the SQL listener has
    // stopped accepting and no statement starts (Ctx.attach refuses),
    // while the loops keep running so the statements in flight commit.
    _ = node.shutdown.waitForStart(.drain, null);
    log.info(
        "received shutdown signal: draining {d} statements in flight",
        .{node.shutdown.activeCtxs()},
    );
    if (!node.shutdown.waitForActiveCtxs(args.shutdown.grace_ms)) {
        if (node.isShuttingDown())
            log.info("second signal: stopping now", .{})
        else
            log.warn("grace over: {d} statements still in flight", .{
                node.shutdown.activeCtxs(),
            });
    }

    // The stop: the node reacts on its own (`shutdownWatchLoop` closes
    // everything raft fibers park on) and the statements the grace cut
    // off fail at their next yield point (Ctx.maybeYield), so there is
    // nothing to invoke here beyond starting it — then wait for the
    // fibers to drain.
    node.shutdown.startShutdown();
    log.info("shutting down...", .{});
    fiber.spawnDetached(.{}, shutdownWatchdog, .{eng.allocator}) catch {};
    eng.wait();
    stop_done.store(true, .release);
    // The loops are gone, so nothing will resolve a proposal still in
    // flight: fail them now, then let every statement the grace cut off
    // run its error path and exit while the databases it holds pointers
    // into exist. The deinits in main run after this returns.
    node.abortPending();
    if (!node.shutdown.waitForActiveCtxs(args.shutdown.grace_ms))
        log.warn("{d} statements still in flight at exit", .{
            node.shutdown.activeCtxs(),
        });
}

/// Set once every engine fiber has exited; `shutdownWatchdog` reads it.
var stop_done = std.atomic.Value(bool).init(false);

/// The loops and listeners exit within a second of the stop. When they
/// have not after ten, the parked fibers go to the log with their
/// traces: the admin listener, the usual way to that dump, is gone by
/// then, and a stop that hangs is otherwise opaque.
fn shutdownWatchdog(allocator: std.mem.Allocator) void {
    fiber.sleepMs(10_000);
    if (stop_done.load(.acquire)) return;

    var aw: std.Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();
    fiber.dumpFibers(fiber.runtime(), &aw.writer) catch |err| {
        log.err("shutdown stalled; fiber dump failed: {s}", .{
            @errorName(err),
        });
        return;
    };
    log.err("shutdown stalled for 10 s; parked fibers:\n{s}", .{
        aw.writer.buffered(),
    });
}

fn runJoin(args: *const Args, allocator: std.mem.Allocator) void {
    raft.joinCluster(
        allocator,
        args.seeds,
        args.raft.cluster_id,
        args.raft.id,
        args.raft_addr,
    ) catch |err| {
        log.err("cluster join failed: {s}", .{@errorName(err)});
    };
}
