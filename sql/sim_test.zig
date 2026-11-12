// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! SQL under the simulation: the engine and the SQL server, opened and
//! started the way `egres/main.zig` does it, on the simulated disk and
//! network, clients that speak pgwire to the listener, and a model of
//! what every acknowledged statement must read as.
//!
//! Each writer owns its rows of one table, so a read of one has one
//! right answer: the last write the writer saw acknowledged. A crash
//! is a power cut under the data directory (`SimDisk.crash`): it shows
//! what was durable at that instant, and every op on it fails from
//! then on. The statements in flight fail or lose their connection,
//! the server is stopped on its dead disk, the disk works again and a
//! new server opens on the same directory. There each writer's rows
//! must read as its acknowledged statements, with or without the
//! whole transaction it had in flight: an acknowledged write gone, or
//! half a transaction, fails the run. The rows read through the
//! secondary index must be the same rows.
//!
//! No two writers touch one row, so beside them run tellers, after
//! Jepsen's bank tests: they move money between accounts that any of
//! them may touch, and their transactions wait on each other, conflict
//! and deadlock. An account at zero has no row, so a transfer also
//! inserts and deletes. Every read of all accounts must add up to the
//! same total, at any time and after any crash.
//!
//! At the end the WAL's disk stops taking writes: the statement then
//! in flight must fail, the node must stop by itself once the log
//! gives up, and a restart must have every acknowledged row.
//!
//! A run with faults makes the WAL's directory and the store's each a
//! `Resource`. Each draws its own kinds of error, their rates and its
//! outages from the seed, so the seeds between them run the kinds
//! alone and together, on one resource and on both at once. A
//! statement may then be answered with an error. What it had in flight
//! is read back: all of it applied or none. The log must not give up,
//! and once every resource works again no statement may fail.
//!
//! One sim seed is one workload and one schedule:
//! `zig build test -- --sim-seed N --filter "sim: sql"`.

const std = @import("std");
const testing = std.testing;
const engine = @import("engine");
const fiber = @import("zfiber");
const flags = @import("flags");
const pgwire = @import("pgwire");
const raft = @import("raft");
const sql = @import("sql");
const utils = @import("utils");
const zdb = @import("zdb");

const String = utils.String;
const Faults = fiber.simdisk.Faults;
const vfs = zdb.vfs;
const log = std.log.scoped(.sim_test);

const num_writers = 4;
const rows_per_writer = 48;

const num_tellers = 3;
const num_accounts = 8;
/// Account `a` is a row of table `a % bank_tables`, so most transfers
/// span two tables.
const bank_tables = 2;
/// What all accounts hold between them, for the whole run. It starts
/// in account 0.
const bank_total = 300;
/// The most one transfer moves.
const max_transfer = 10;
/// What a transfer that backs out writes first. No account ever
/// holds a balance this far under zero.
const poison = 1000;
/// What one run of the test does. The values here are the test every
/// run of the suite includes: seconds per seed.
const Shape = struct {
    /// Crashes, each followed by a restart and a check.
    crashes: u32 = 4,
    /// The writers run this long before a crash, and up to
    /// `stretch_more_ms` longer.
    stretch_ms: u64 = 300,
    stretch_more_ms: u64 = 2500,
    /// The most bytes a row's body is the hex of.
    body_max: usize = 450,
    /// The resources have faults while the writers run, drawn from
    /// the seed (`Resources.drawDirs`).
    faults: bool = false,
};

/// Every kind of error, each off one time in two: a seed runs some of
/// them together and the others not at all.
fn drawFaults() Faults {
    var faults: Faults = .{};
    inline for (std.meta.fields(Faults)) |field|
        @field(faults, field.name) = drawRate();
    return faults;
}

/// Per thousand ops: 0.2%, 1% or 5%. At more than that a table, which
/// is many writes, is rarely written whole.
fn drawRate() u16 {
    const rates = [_]u16{ 0, 0, 0, 2, 10, 50 };
    return rates[fiber.random().uintLessThan(usize, rates.len)];
}

/// `Shape.body_max` of the run, for `bodyOf`.
var body_max: usize = 450;
/// Writer `w` has the ids and the seqs in [w * span, (w + 1) * span).
const span = 1_000_000;

/// Ports on the simulated network, which has one host.
const sql_addr = "127.0.0.1:5432";
const raft_addr = "127.0.0.1:7000";

/// Sizes that make a few virtual seconds of writes reach every loop:
/// memtables freeze, flush, compact and split, the WAL rotates and a
/// snapshot trims it.
const zdb_cfg: zdb.config.Config = .{
    .table_bytes = 64 << 10,
    .block_bytes = 4 << 10,
    .flush_min_memtables = 2,
    .flush_max_age_seconds = 1,
    .lmax_target_bytes = 1 << 20,
};
const snapshot_cfg: engine.SnapshotCfg = .{
    .min_bytes = 256 << 10,
    .min_age_seconds = 2,
    .poll_interval_seconds = 1,
};
/// A log file is a 16 MiB header and then entries: 2 MiB of them here.
const wal_opts: raft.diskwal.OpenOptions = .{
    .log_file_size = raft.diskwal.log_file_offset + (2 << 20),
    .max_spare_log_files = 2,
    .write_retry_ms = retry_ms,
};
const retry_ms = 2000;

/// A disk that fails every op, on a file or by path.
const dead_disk: Faults = .{
    .read = 1000,
    .write = 1000,
    .sync = 1000,
    .set_size = 1000,
    .path_ops = 1000,
};

/// Virtual milliseconds, `min` up to `max` and not `max` itself.
const Range = struct {
    min: u64,
    max: u64,

    fn draw(self: Range) u64 {
        const width = self.max - self.min;
        return self.min + fiber.random().uintLessThan(u64, width);
    }
};

/// One thing the server depends on that fails by itself. It has its
/// own error rates and moves between working, having its errors and
/// an outage at times it draws: two resources are out together when
/// their times cross.
const Resource = struct {
    name: String,
    target: Target,
    /// `outage` is one of its states: every op on it fails.
    outages: bool,
    /// How long it stays in `works` or in `errors`.
    period_ms: Range = .{ .min = 300, .max = 1500 },
    state: State = .works,
    /// When `state` next changes, in `Resources.elapsed_ms`.
    change_at: u64 = never,

    const State = enum { works, errors, outage };
    const never = std.math.maxInt(u64);
    /// An outage lasts a period over this: much shorter than the log's
    /// retry bound, and the period after it takes several retries, so
    /// the log must not give up.
    const outage_fraction = 4;

    /// Where the faults are applied: what it takes to apply them and
    /// the rates this run drew.
    const Target = union(enum) {
        dir: struct {
            sim: *fiber.simdisk.SimDisk,
            path: String,
            rates: Faults,
        },
    };

    fn set(self: *Resource, state: State) !void {
        switch (self.target) {
            .dir => |dir| try dir.sim.setFaults(dir.path, switch (state) {
                .works => .{},
                .errors => dir.rates,
                .outage => dead_disk,
            }),
        }
        self.state = state;
    }

    /// Goes to `state` at `change_at` and draws how long it stays.
    fn enter(self: *Resource, state: State) !void {
        try self.set(state);
        log.debug("{s}: {s} from {d} ms", .{
            self.name, @tagName(state), self.change_at,
        });
        const period = self.period_ms.draw();
        self.change_at += switch (state) {
            .works, .errors => period,
            .outage => period / outage_fraction,
        };
    }

    /// The next state, one of the other two.
    fn change(self: *Resource) !void {
        const first = fiber.random().boolean();
        const out = self.outages and first;
        try self.enter(switch (self.state) {
            .works => if (out) .outage else .errors,
            .errors => if (out) .outage else .works,
            .outage => if (first) .works else .errors,
        });
    }

    /// Whether its faults may have a statement answered with an error.
    /// The server retries a failed write. A failed read, sync, resize
    /// or path op may fail the statement that needed it, as the sync
    /// of the WAL's directory at a rotation does, and so may an outage.
    fn refuses(self: Resource) bool {
        if (self.outages) return true;
        switch (self.target) {
            .dir => |dir| {
                const rates = dir.rates;
                return rates.read != 0 or rates.sync != 0 or
                    rates.sync_drop != 0 or rates.set_size != 0 or
                    rates.path_ops != 0 or rates.shutdown != 0;
            },
        }
    }
};

/// What the server runs on, each with its own faults, and the power
/// cut, which takes the whole data directory at once.
const Resources = struct {
    sim: *fiber.simdisk.SimDisk,
    data_dir: String,
    list: std.ArrayList(Resource) = .empty,
    /// Virtual milliseconds of `injectFaults` since `start`.
    elapsed_ms: u64 = 0,
    /// True from `start` until `clear`. A statement may be answered
    /// with an error meanwhile, also when the resource that failed it
    /// works again by then.
    active: bool = false,
    /// True from a power cut until `clear`.
    cut: bool = false,

    /// The WAL's directory and the store's, each a resource.
    fn drawDirs(
        self: *Resources,
        alloc: std.mem.Allocator,
        wal_dir: String,
        store_dir: String,
    ) !void {
        try self.addDir(alloc, "wal", wal_dir, drawFaults());
        // The store's filesystem never shuts: a flush that fails is
        // tried again for ever (#45), so the node would not stop.
        var store_rates = drawFaults();
        store_rates.shutdown = 0;
        try self.addDir(alloc, "store", store_dir, store_rates);
    }

    fn addDir(
        self: *Resources,
        alloc: std.mem.Allocator,
        name: String,
        path: String,
        rates: Faults,
    ) !void {
        const outages = fiber.random().boolean();
        log.info("{s}: {any}, outages {}", .{ name, rates, outages });
        try self.list.append(alloc, .{
            .name = name,
            .target = .{
                .dir = .{ .sim = self.sim, .path = path, .rates = rates },
            },
            .outages = outages,
        });
    }

    /// Every resource starts out working or with its errors.
    fn start(self: *Resources) !void {
        self.elapsed_ms = 0;
        self.active = true;
        for (self.list.items) |*res| {
            res.change_at = 0;
            const works = fiber.random().boolean();
            try res.enter(if (works) .works else .errors);
        }
    }

    /// `ms` virtual milliseconds of the writers' run. Each resource
    /// changes state at the times it drew.
    fn injectFaults(self: *Resources, ms: u64) !void {
        const end = self.elapsed_ms + ms;
        while (self.earliest()) |res| {
            if (res.change_at >= end) break;
            fiber.sleepMs(res.change_at - self.elapsed_ms);
            self.elapsed_ms = res.change_at;
            try res.change();
        }
        fiber.sleepMs(end - self.elapsed_ms);
        self.elapsed_ms = end;
    }

    /// The resource whose state changes first.
    fn earliest(self: *Resources) ?*Resource {
        var first: ?*Resource = null;
        for (self.list.items) |*res| {
            if (first == null or res.change_at < first.?.change_at)
                first = res;
        }
        return first;
    }

    /// An outage in progress ends, and no state changes from here.
    fn endOutages(self: *Resources) !void {
        for (self.list.items) |*res| {
            if (res.state == .outage) try res.set(.errors);
            res.change_at = Resource.never;
        }
    }

    /// Everything works again.
    fn clear(self: *Resources) !void {
        for (self.list.items) |*res| {
            try res.set(.works);
            res.change_at = Resource.never;
        }
        try self.sim.setFaults(self.data_dir, .{});
        self.active = false;
        self.cut = false;
    }

    /// The power cut. Every op under the data directory fails from
    /// then on for the server still running, so nothing that server
    /// does is there after a restart.
    fn powerCut(self: *Resources, mode: fiber.simdisk.Crash) !void {
        self.sim.crash(self.data_dir, mode);
        try self.sim.setFaults(self.data_dir, dead_disk);
        for (self.list.items) |*res| switch (res.target) {
            .dir => try res.set(.outage),
        };
        self.cut = true;
    }

    /// Whether a resource's filesystem has shut. It takes no write
    /// until the restart, so the log gives up and the node stops.
    fn shut(self: *const Resources) bool {
        for (self.list.items) |res| switch (res.target) {
            .dir => |dir| if (dir.sim.shutAt(dir.path) != null) return true,
        };
        return false;
    }

    /// Whether the server is on its way to a restart: after a power
    /// cut, or with a filesystem shut. A statement that fails
    /// meanwhile is that event's, not a failed run.
    fn down(self: *const Resources) bool {
        return self.cut or self.shut();
    }

    /// No filesystem shuts from here: the stretch that follows ends
    /// without the restart a shut one needs.
    fn endShutdowns(self: *Resources) void {
        for (self.list.items) |*res| switch (res.target) {
            .dir => |*dir| dir.rates.shutdown = 0,
        };
    }

    fn mayRefuse(self: *const Resources) bool {
        if (!self.active) return false;
        for (self.list.items) |res| if (res.refuses()) return true;
        return false;
    }
};

/// The run's resources, where `ask` and the writers read whether a
/// statement that failed is the faults' or a failed run.
var resources: Resources = undefined;

/// One boot of the server on a data directory: the store and the flags
/// `egres/main.zig` hands to `Engine.open`, then its calls in its
/// order, without the admin listener.
const Instance = struct {
    alloc: std.mem.Allocator,
    wal_dir: []u8,
    store_dir: []u8,
    store: vfs.Store,
    /// Null when this boot drew none.
    block_cache: ?zdb.cache.ShardedCache(u128),
    zdb_live: *flags.Live(zdb.config.Config),
    snapshot_live: *flags.Live(engine.SnapshotCfg),
    runtime_live: *flags.Live(fiber.Runtime.Cfg),
    eng: *engine.Engine,
    sqlsrv: *sql.Server,

    /// Open on `dir`, fresh or left by an earlier boot, and return once
    /// the listener takes a connection.
    fn open(alloc: std.mem.Allocator, dir: String) !*Instance {
        const self = try alloc.create(Instance);
        errdefer alloc.destroy(self);
        self.alloc = alloc;

        self.wal_dir = try std.fmt.allocPrint(alloc, "{s}/wal", .{dir});
        errdefer alloc.free(self.wal_dir);

        self.store_dir = try std.fmt.allocPrint(alloc, "{s}/store", .{dir});
        errdefer alloc.free(self.store_dir);

        try vfs.createDirPath(self.store_dir);

        self.store = try vfs.Store.initFile(alloc, self.store_dir, vfs.g());
        errdefer self.store.deinit();
        self.zdb_live = try flags.Live(zdb.config.Config).create(
            alloc,
            zdb_cfg,
        );
        errdefer self.zdb_live.destroy();
        self.snapshot_live = try flags.Live(engine.SnapshotCfg).create(
            alloc,
            snapshot_cfg,
        );
        errdefer self.snapshot_live.destroy();
        self.runtime_live = try flags.Live(fiber.Runtime.Cfg).create(
            alloc,
            .{},
        );
        errdefer self.runtime_live.destroy();

        // A restart has an empty cache. Each boot draws one that
        // evicts within the run, one that holds every block or none,
        // and direct or buffered reads.
        const cache_sizes = [_]usize{ 0, 256 << 10, 16 << 20 };
        const cache_bytes =
            cache_sizes[fiber.random().uintLessThan(usize, cache_sizes.len)];
        const mode: vfs.Mode =
            if (fiber.random().boolean()) .direct else .default;
        log.info("block cache {d} KiB, {s} reads", .{
            cache_bytes >> 10, @tagName(mode),
        });
        self.block_cache = null;
        if (cache_bytes > 0) self.block_cache =
            try zdb.cache.ShardedCache(u128).init(alloc, cache_bytes, 4);
        errdefer if (self.block_cache) |*cache| cache.deinit();
        self.store.setReadMode(mode);
        self.store.setBlockCache(
            if (self.block_cache) |*cache| cache else null,
        );

        self.eng = try engine.Engine.open(alloc, .{
            .wal_dir = self.wal_dir,
            .wal = wal_opts,
            .raft = .{ .id = 1, .listen_addr = raft_addr },
            .store = &self.store,
            .database = sql.default_database,
            .zdb_cfg = self.zdb_live,
            .snapshot_cfg = self.snapshot_live,
            .runtime_cfg = self.runtime_live,
        });
        errdefer self.eng.deinit();
        self.sqlsrv = try sql.Server.create(self.eng, sql_addr);
        errdefer self.sqlsrv.deinit();

        try self.eng.start();
        errdefer self.eng.stop();
        try self.sqlsrv.start();

        // A statement before the election is answered "not the
        // leader", which a client retries; the test waits instead. An
        // election takes at most twenty 100 ms ticks.
        var waited_ms: u32 = 0;
        while (!self.eng.node.isLeader()) : (waited_ms += 10) {
            if (waited_ms > 30_000) return error.NoLeader;
            fiber.sleepMs(10);
        }
        var first = try connect(alloc);
        first.close();
        return self;
    }

    /// Every connection is one of the test's and closed by now. The
    /// sleep lets the server's side of each see that and end before
    /// what it uses is freed: virtual time moves only once nothing is
    /// runnable. The engine may have been stopped already.
    fn close(self: *Instance) void {
        fiber.sleepMs(1);
        self.eng.stop();
        self.sqlsrv.deinit();
        self.eng.deinit();
        if (self.block_cache) |*cache| {
            log.info("block cache: {d} hits, {d} misses", .{
                cache.hits.load(.monotonic), cache.misses.load(.monotonic),
            });
            cache.deinit();
        }
        self.store.deinit();
        self.runtime_live.destroy();
        self.snapshot_live.destroy();
        self.zdb_live.destroy();
        self.alloc.free(self.store_dir);
        self.alloc.free(self.wal_dir);
        self.alloc.destroy(self);
    }
};

/// A session on the server. The listener opens once the node has
/// applied what its WAL held at boot, and refuses until then, so the
/// first connection of a boot is retried, within a bound: a boot that
/// never gets there is a failed run, not a hang.
fn connect(alloc: std.mem.Allocator) !pgwire.Client {
    const addr = try fiber.net.IpAddress.parseIpAndPort(sql_addr);
    var waited_ms: u32 = 0;
    while (waited_ms <= 60_000) : (waited_ms += 10) {
        return pgwire.Client.connect(
            alloc,
            addr,
            sql.default_database,
        ) catch |err| {
            if (err != error.ConnectionRefused) return err;
            fiber.sleepMs(10);
            continue;
        };
    }
    return error.ServerNeverListened;
}

/// One statement and its reply. No statement here is one a server
/// on a working disk may refuse: a refusal there fails the run.
fn ask(
    client: *pgwire.Client,
    arloc: std.mem.Allocator,
    text: String,
) !pgwire.client.Reply {
    const reply = try client.query(arloc, text);
    return refusal(reply.err orelse return reply, text);
}

fn refusal(
    refused: pgwire.client.ServerError,
    text: String,
) error{StatementRefused} {
    const faults = resources.down() or resources.mayRefuse();
    if (!faults) log.err("{s} {s}: {s}", .{
        &refused.code,
        refused.message,
        text[0..@min(text.len, 100)],
    });
    return error.StatementRefused;
}

/// A statement that returns no rows, answered with `tag`.
fn exec(
    client: *pgwire.Client,
    arloc: std.mem.Allocator,
    text: String,
    tag: String,
) !void {
    try expectTag(try ask(client, arloc, text), tag, text);
}

fn expectTag(reply: pgwire.client.Reply, tag: String, text: String) !void {
    if (std.mem.eql(u8, reply.tag, tag)) return;
    log.err("want {s}, answered {s}: {s}", .{
        tag,
        reply.tag,
        text[0..@min(text.len, 100)],
    });
    return error.WrongCommandTag;
}

/// The table every writer has its rows in, and an index that moves
/// with every write of one.
fn createTable(alloc: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    var client = try connect(alloc);
    defer client.close();
    try exec(
        &client,
        arena.allocator(),
        "CREATE TABLE kv (id int8 PRIMARY KEY, seq int8, body text)",
        "CREATE TABLE",
    );
    try exec(
        &client,
        arena.allocator(),
        "CREATE INDEX kv_seq ON kv (seq)",
        "CREATE INDEX",
    );
    try createBank(&client, arena.allocator());
}

/// The tellers' tables, each with an index that moves with every
/// transfer, and all the money in account 0.
fn createBank(client: *pgwire.Client, arloc: std.mem.Allocator) !void {
    for (0..bank_tables) |tbl| {
        try exec(client, arloc, try std.fmt.allocPrint(
            arloc,
            "CREATE TABLE bank{d} (id int8 PRIMARY KEY, balance int8)",
            .{tbl},
        ), "CREATE TABLE");
        try exec(client, arloc, try std.fmt.allocPrint(
            arloc,
            "CREATE INDEX bank{d}_balance ON bank{d} (balance)",
            .{ tbl, tbl },
        ), "CREATE INDEX");
    }
    try exec(client, arloc, try std.fmt.allocPrint(
        arloc,
        "INSERT INTO bank0 (id, balance) VALUES (0, {d})",
        .{bank_total},
    ), "INSERT 0 1");
}

/// One row's write in a statement; `seq` null is a delete.
const Write = struct { row: u32, seq: ?u64 };

/// What each of a writer's rows holds: its seq, null for no row.
const Rows = [rows_per_writer]?u64;

const Statement = struct { text: String, tag: String };

const Writer = struct {
    id: u32,
    alloc: std.mem.Allocator,
    prng: std.Random.DefaultPrng,
    /// Open during a session and a check, not between them.
    client: pgwire.Client = undefined,
    /// Every write of this writer takes the next one, so a row's seq
    /// names the write that made it.
    next_seq: u64 = 1,
    /// What the server acknowledged.
    acked: Rows = @splat(null),
    /// The statement or transaction sent and not yet acknowledged.
    /// Left as it is by a step that failed.
    pending: std.ArrayList(Write) = .empty,
    ops: u64 = 0,
    /// Crashes that kept what was in flight.
    kept_in_flight: u32 = 0,
    /// Steps the server answered with an error under disk errors.
    refused: u64 = 0,
    failed: ?anyerror = null,

    fn deinit(self: *Writer) void {
        self.pending.deinit(self.alloc);
    }

    /// The writer's fiber: sessions of statements until `stop`.
    fn run(self: *Writer, stop: *const bool) void {
        self.sessions(stop) catch |err| {
            // The crash or the shut filesystem took the step in
            // flight: an error, or the connection gone. `recover`
            // decides what became of it.
            if (!resources.down()) self.failed = err;
        };
    }

    /// One session until a statement is refused under disk errors, then
    /// the next: closing the first ends the block it may have had open.
    fn sessions(self: *Writer, stop: *const bool) !void {
        while (!stop.*) {
            self.client = try connect(self.alloc);
            defer self.client.close();
            self.session(stop) catch |err| {
                if (err != error.StatementRefused) return err;
                if (resources.down() or !resources.mayRefuse()) return err;
                self.refused += 1;
                log.debug("writer {d}: refused, in flight {any}", .{
                    self.id, self.pending.items,
                });
            };
        }
    }

    fn session(self: *Writer, stop: *const bool) !void {
        try self.settle(stop);
        while (!stop.*) {
            try self.step();
            self.ops += 1;
        }
    }

    /// What became of the writes a refused statement left in flight:
    /// all of them are there or none is. A read the server refuses is
    /// asked again, and at `stop` they stay in flight for the next
    /// session or for `recover`.
    fn settle(self: *Writer, stop: *const bool) !void {
        if (self.pending.items.len == 0) return;
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        var applied: usize = 0;
        for (self.pending.items) |write| {
            const row = write.row;
            const found = try self.getAnswered(arena.allocator(), row, stop);
            if (std.meta.eql(found, write.seq)) {
                applied += 1;
                continue;
            }
            try expectSeq(self.id, row, self.acked[row], found);
        }
        log.debug("writer {d}: {d} of the {d} in flight are there", .{
            self.id, applied, self.pending.items.len,
        });
        if (applied == self.pending.items.len) return self.acknowledged();
        if (applied != 0) {
            log.err("writer {d}: {d} of {d} writes of a refused block", .{
                self.id, applied, self.pending.items.len,
            });
            return error.RefusedBlockHalfApplied;
        }
        self.pending.clearRetainingCapacity();
    }

    /// `get`, asked again while the disk's errors fail it.
    fn getAnswered(
        self: *Writer,
        arloc: std.mem.Allocator,
        row: u32,
        stop: *const bool,
    ) !?u64 {
        while (true) {
            return self.get(arloc, row) catch |err| {
                if (err != error.StatementRefused) return err;
                if (stop.* or resources.down()) return err;
                if (!resources.mayRefuse()) return err;
                fiber.sleepMs(5);
                continue;
            };
        }
    }

    fn step(self: *Writer) !void {
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const arloc = arena.allocator();
        const rand = self.prng.random();
        switch (rand.uintLessThan(u32, 10)) {
            0...3 => try self.writeOne(arloc),
            4...6 => try self.writeBlock(arloc),
            7...8 => {
                const row = rand.uintLessThan(u32, rows_per_writer);
                const found = try self.get(arloc, row);
                try expectSeq(self.id, row, self.acked[row], found);
            },
            else => {
                const found = try self.scan(arloc);
                try expectRows(self.id, "scan", self.acked, found);
                const by_index = try self.scanIndex(arloc);
                try expectRows(self.id, "index", self.acked, by_index);
            },
        }
    }

    /// The next write of `row`: one time in five the delete of a row
    /// that is there, else the next seq.
    fn draw(self: *Writer, row: u32) Write {
        const rand = self.prng.random();
        if (self.acked[row] != null and rand.uintLessThan(u32, 5) == 0)
            return .{ .row = row, .seq = null };
        const seq = idOf(self.id, 0) + self.next_seq;
        self.next_seq += 1;
        return .{ .row = row, .seq = seq };
    }

    /// The statement that makes `write` of a row that holds what was
    /// acknowledged, and the tag the server answers it with.
    fn statement(
        self: *Writer,
        arloc: std.mem.Allocator,
        write: Write,
    ) !Statement {
        const id = idOf(self.id, write.row);
        const seq = write.seq orelse return .{
            .text = try std.fmt.allocPrint(
                arloc,
                "DELETE FROM kv WHERE id = {d}",
                .{id},
            ),
            .tag = "DELETE 1",
        };
        const body = try bodyOf(arloc, id, seq);
        if (self.acked[write.row] == null) return .{
            .text = try std.fmt.allocPrint(
                arloc,
                "INSERT INTO kv (id, seq, body) VALUES ({d}, {d}, '{s}')",
                .{ id, seq, body },
            ),
            .tag = "INSERT 0 1",
        };
        return .{
            .text = try std.fmt.allocPrint(
                arloc,
                "UPDATE kv SET seq = {d}, body = '{s}' WHERE id = {d}",
                .{ seq, body, id },
            ),
            .tag = "UPDATE 1",
        };
    }

    /// One statement outside a block: it commits by itself.
    fn writeOne(self: *Writer, arloc: std.mem.Allocator) !void {
        const rand = self.prng.random();
        const write = self.draw(rand.uintLessThan(u32, rows_per_writer));
        const st = try self.statement(arloc, write);
        try self.pending.append(self.alloc, write);
        try exec(&self.client, arloc, st.text, st.tag);
        self.acknowledged();
    }

    /// BEGIN, one to four writes of distinct rows, a read of one of
    /// them, which must see the block's own write, then COMMIT: all of
    /// it applies or none. One block in eight ends in ROLLBACK and
    /// leaves the rows as they were.
    fn writeBlock(self: *Writer, arloc: std.mem.Allocator) !void {
        const rand = self.prng.random();
        const count = 1 + rand.uintLessThan(u32, 4);
        const first = rand.uintLessThan(u32, rows_per_writer);
        const commit = rand.uintLessThan(u32, 8) != 0;
        const writes = try arloc.alloc(Write, count);
        for (writes, 0..) |*write, nth| {
            const row = (first + @as(u32, @intCast(nth))) % rows_per_writer;
            write.* = self.draw(row);
        }
        // In flight from here, when it is to commit: a crash before the
        // COMMIT keeps none of it, one after it may keep all of it.
        if (commit) try self.pending.appendSlice(self.alloc, writes);

        try exec(&self.client, arloc, "BEGIN", "BEGIN");
        for (writes) |write| {
            const st = try self.statement(arloc, write);
            try exec(&self.client, arloc, st.text, st.tag);
        }
        const probe = writes[rand.uintLessThan(u32, count)];
        const in_block = try self.get(arloc, probe.row);
        try expectSeq(self.id, probe.row, probe.seq, in_block);
        if (!commit)
            return exec(&self.client, arloc, "ROLLBACK", "ROLLBACK");
        try exec(&self.client, arloc, "COMMIT", "COMMIT");
        self.acknowledged();
    }

    /// The server answered what was in flight.
    fn acknowledged(self: *Writer) void {
        log.debug("writer {d}: acknowledged {any}", .{
            self.id, self.pending.items,
        });
        apply(&self.acked, self.pending.items);
        self.pending.clearRetainingCapacity();
    }

    /// The seq `row` holds, read by its primary key.
    fn get(self: *Writer, arloc: std.mem.Allocator, row: u32) !?u64 {
        const id = idOf(self.id, row);
        const reply = try ask(&self.client, arloc, try std.fmt.allocPrint(
            arloc,
            "SELECT seq, body FROM kv WHERE id = {d}",
            .{id},
        ));
        if (reply.rows.len == 0) return null;
        if (reply.rows.len > 1) return error.RowReadTwice;
        const cols = reply.rows[0];
        if (cols.len != 2) return error.WrongColumnCount;
        return try seqOf(arloc, id, cols[0], cols[1]);
    }

    /// Every row of this writer, by a range of the primary key.
    fn scan(self: *Writer, arloc: std.mem.Allocator) !Rows {
        const lo = idOf(self.id, 0);
        const reply = try ask(&self.client, arloc, try std.fmt.allocPrint(
            arloc,
            "SELECT id, seq, body FROM kv WHERE id >= {d} AND id < {d}",
            .{ lo, lo + span },
        ));
        var found: Rows = @splat(null);
        for (reply.rows) |cols| {
            if (cols.len != 3) return error.WrongColumnCount;
            const id = try intOf(cols[0]);
            const row = try self.rowOf(id);
            if (found[row] != null) return error.RowReadTwice;
            found[row] = try seqOf(arloc, id, cols[1], cols[2]);
        }
        return found;
    }

    /// The same rows by a range of the index on seq: this writer's
    /// seqs are the range its ids are.
    fn scanIndex(self: *Writer, arloc: std.mem.Allocator) !Rows {
        const lo = idOf(self.id, 0);
        const reply = try ask(&self.client, arloc, try std.fmt.allocPrint(
            arloc,
            "SELECT id, seq FROM kv WHERE seq >= {d} AND seq < {d}",
            .{ lo, lo + span },
        ));
        var found: Rows = @splat(null);
        for (reply.rows) |cols| {
            if (cols.len != 2) return error.WrongColumnCount;
            const row = try self.rowOf(try intOf(cols[0]));
            if (found[row] != null) return error.RowReadTwice;
            found[row] = try intOf(cols[1]);
        }
        return found;
    }

    /// Which of this writer's rows `id` is.
    fn rowOf(self: *Writer, id: u64) !u32 {
        const lo = idOf(self.id, 0);
        if (id < lo or id >= lo + rows_per_writer)
            return error.UnknownRow;
        return @intCast(id - lo);
    }

    /// What the rows hold, read three ways that must agree: the range
    /// of the primary key, each row by its key, the range of the index.
    fn readAll(self: *Writer, arloc: std.mem.Allocator) !Rows {
        const found = try self.scan(arloc);
        for (found, 0..) |by_scan, nth| {
            const row: u32 = @intCast(nth);
            try expectSeq(self.id, row, by_scan, try self.get(arloc, row));
        }
        try expectRows(self.id, "index", found, try self.scanIndex(arloc));
        return found;
    }

    /// After a restart on the crashed disk: the rows must read as
    /// they were acknowledged, or as that plus the whole of what the
    /// crash failed in flight. An acknowledgement read after the crash
    /// counts as any other: the dead disk took no commit, so the
    /// server made that one durable before it. What is found is what
    /// the writer carries on from.
    fn recover(self: *Writer) !void {
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        self.client = try connect(self.alloc);
        defer self.client.close();

        const found = try self.readAll(arena.allocator());
        var with_pending = self.acked;
        apply(&with_pending, self.pending.items);

        const moved = !std.meta.eql(found, self.acked);
        if (moved and !std.meta.eql(found, with_pending)) {
            for (found, self.acked, with_pending, 0..) |got, a, b, row| {
                if (std.meta.eql(got, a) and std.meta.eql(got, b)) continue;
                log.err(
                    "writer {d} row {d}: found {?d}, acked {?d}," ++
                        " with what was in flight {?d}",
                    .{ self.id, row, got, a, b },
                );
            }
            return error.CrashLostOrTornWrites;
        }
        if (moved) self.kept_in_flight += 1;
        log.debug("writer {d}: after the crash, in flight {any} kept: {}", .{
            self.id, self.pending.items, moved,
        });
        self.acked = found;
        self.pending.clearRetainingCapacity();
    }

    /// One write to a server whose log takes no write: it must come
    /// back failed, as an error or a closed connection, and the rows
    /// stay as they were acknowledged.
    fn refusedWrite(self: *Writer) !void {
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        const arloc = arena.allocator();
        self.client = try connect(self.alloc);
        defer self.client.close();

        const st = try self.statement(arloc, self.draw(0));
        const reply = self.client.query(arloc, st.text) catch return;
        if (reply.err == null) return error.AcknowledgedWithoutLog;
    }

    /// After a clean stop and a restart: every acknowledged write.
    fn expectAcked(self: *Writer) !void {
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        self.client = try connect(self.alloc);
        defer self.client.close();
        const found = try self.readAll(arena.allocator());
        try expectRows(self.id, "restart", self.acked, found);
    }
};

/// What each account holds: null for no row.
const Balances = [num_accounts]?i64;

/// The SQLSTATEs the server ends a transaction with for meeting
/// another one: a serialization failure, a deadlock, a key the other
/// inserted first, a snapshot grown too old. Nothing of it applied.
const conflict_codes = [_]String{ "40001", "40P01", "23505", "72000" };

/// Which of `conflict_codes` it is, or null for any other error.
fn conflictOf(code: [5]u8) ?usize {
    for (conflict_codes, 0..) |conflict, nth| {
        if (std.mem.eql(u8, &code, conflict)) return nth;
    }
    return null;
}

/// A bank client. Its transfers read two accounts and write both, in
/// either order, so two tellers lock the same rows and in opposite
/// orders. What a crash or a refusal left in flight needs no
/// settling: the accounts add up with the transfer or without it.
const Teller = struct {
    id: u32,
    alloc: std.mem.Allocator,
    prng: std.Random.DefaultPrng,
    /// Open during a session and an audit, not between them.
    client: pgwire.Client = undefined,
    transfers: u64 = 0,
    /// Transfers that found their account empty and backed out.
    short: u64 = 0,
    /// Transactions that another one ended, by `conflict_codes`.
    conflicts: [conflict_codes.len]u64 = @splat(0),
    reads: u64 = 0,
    /// Sessions the disk's errors ended.
    refused: u64 = 0,
    /// A read of the accounts was wrong.
    wrong: bool = false,
    failed: ?anyerror = null,

    /// The teller's fiber: sessions of transfers and reads until `stop`.
    fn run(self: *Teller, stop: *const bool) void {
        self.sessions(stop) catch |err| {
            // A statement the crash or a shut filesystem failed is not
            // a failed run. Accounts read wrong are, whenever read.
            if (self.wrong or !resources.down()) self.failed = err;
        };
    }

    /// One session until the disk's errors fail a statement, then the
    /// next: closing the first ends the block it had open.
    fn sessions(self: *Teller, stop: *const bool) !void {
        while (!stop.*) {
            self.client = try connect(self.alloc);
            defer self.client.close();
            self.session(stop) catch |err| {
                if (err != error.StatementRefused) return err;
                if (resources.down() or !resources.mayRefuse()) return err;
                self.refused += 1;
            };
        }
    }

    fn session(self: *Teller, stop: *const bool) !void {
        while (!stop.*) {
            var arena = std.heap.ArenaAllocator.init(self.alloc);
            defer arena.deinit();
            const read = self.prng.random().uintLessThan(u32, 4) == 0;
            const step = if (read)
                self.readAll(arena.allocator())
            else
                self.transfer(arena.allocator());
            step catch |err| {
                if (err != error.Conflict) return err;
            };
        }
    }

    /// After a restart: the accounts add up, whatever was in flight.
    fn audit(self: *Teller) !void {
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        defer arena.deinit();
        self.client = try connect(self.alloc);
        defer self.client.close();
        try self.readAll(arena.allocator());
    }

    /// One statement of a transaction that another may end. The block
    /// then takes only its ROLLBACK.
    fn query(
        self: *Teller,
        arloc: std.mem.Allocator,
        text: String,
    ) !pgwire.client.Reply {
        const reply = try self.client.query(arloc, text);
        const refused = reply.err orelse return reply;
        const conflict = conflictOf(refused.code) orelse
            return refusal(refused, text);
        self.conflicts[conflict] += 1;
        if (reply.status == .failed_tx)
            try exec(&self.client, arloc, "ROLLBACK", "ROLLBACK");
        return error.Conflict;
    }

    fn execute(
        self: *Teller,
        arloc: std.mem.Allocator,
        text: String,
        tag: String,
    ) !void {
        try expectTag(try self.query(arloc, text), tag, text);
    }

    /// BEGIN, read both accounts, write both, COMMIT. It moves up to
    /// `max_transfer`, or all the account has when that is less, which
    /// deletes its row. From an account with no row it is `backOut`.
    fn transfer(self: *Teller, arloc: std.mem.Allocator) !void {
        const rand = self.prng.random();
        const from = rand.uintLessThan(u32, num_accounts);
        const others = rand.uintLessThan(u32, num_accounts - 1);
        const to = (from + 1 + others) % num_accounts;

        try self.execute(arloc, "BEGIN", "BEGIN");
        var from_has: i64 = undefined;
        var to_has: i64 = undefined;
        if (rand.boolean()) {
            from_has = try self.balance(arloc, from);
            to_has = try self.balance(arloc, to);
        } else {
            to_has = try self.balance(arloc, to);
            from_has = try self.balance(arloc, from);
        }

        if (from_has == 0) return self.backOut(arloc, from, to, to_has);
        const drawn: i64 = 1 + rand.uintLessThan(u32, max_transfer);
        const amount = @min(drawn, from_has);

        if (rand.boolean()) {
            try self.set(arloc, from, from_has, from_has - amount);
            try self.set(arloc, to, to_has, to_has + amount);
        } else {
            try self.set(arloc, to, to_has, to_has + amount);
            try self.set(arloc, from, from_has, from_has - amount);
        }
        try self.execute(arloc, "COMMIT", "COMMIT");
        self.transfers += 1;
    }

    /// `from` has nothing, so the transfer does not happen. One time
    /// in two it first writes balances no account may show, so a
    /// ROLLBACK that leaves any of its writes behind is seen by the
    /// next read.
    fn backOut(
        self: *Teller,
        arloc: std.mem.Allocator,
        from: u32,
        to: u32,
        to_has: i64,
    ) !void {
        self.short += 1;
        if (self.prng.random().boolean()) {
            try self.set(arloc, from, 0, -poison);
            try self.set(arloc, to, to_has, to_has - poison);
        }
        try self.execute(arloc, "ROLLBACK", "ROLLBACK");
    }

    /// What account `id` holds: zero when it has no row.
    fn balance(self: *Teller, arloc: std.mem.Allocator, id: u32) !i64 {
        const reply = try self.query(arloc, try std.fmt.allocPrint(
            arloc,
            "SELECT balance FROM bank{d} WHERE id = {d}",
            .{ id % bank_tables, id },
        ));
        if (reply.rows.len == 0) return 0;
        if (reply.rows.len > 1) return error.RowReadTwice;
        const cols = reply.rows[0];
        if (cols.len != 1) return error.WrongColumnCount;
        return balanceOf(cols[0]);
    }

    /// Takes account `id` from `has` to `now`. A balance of zero is no
    /// row, so this inserts or deletes at that end. An UPDATE has the
    /// server do the sum one time in two.
    fn set(
        self: *Teller,
        arloc: std.mem.Allocator,
        id: u32,
        has: i64,
        now: i64,
    ) !void {
        const tbl = id % bank_tables;
        if (now == 0) return self.execute(arloc, try std.fmt.allocPrint(
            arloc,
            "DELETE FROM bank{d} WHERE id = {d}",
            .{ tbl, id },
        ), "DELETE 1");
        if (has == 0) return self.execute(arloc, try std.fmt.allocPrint(
            arloc,
            "INSERT INTO bank{d} (id, balance) VALUES ({d}, {d})",
            .{ tbl, id, now },
        ), "INSERT 0 1");

        const text = if (self.prng.random().boolean())
            try std.fmt.allocPrint(
                arloc,
                "UPDATE bank{d} SET balance = {d} WHERE id = {d}",
                .{ tbl, now, id },
            )
        else
            try std.fmt.allocPrint(
                arloc,
                "UPDATE bank{d} SET balance = balance {s} {d} WHERE id = {d}",
                .{ tbl, if (now < has) "-" else "+", @abs(now - has), id },
            );
        try self.execute(arloc, text, "UPDATE 1");
    }

    /// Every account in one transaction, each table by its primary
    /// key and by its index on the balance. The two must show the same
    /// rows, each with a balance over zero, and `bank_total` in all.
    fn readAll(self: *Teller, arloc: std.mem.Allocator) !void {
        var by_key: Balances = @splat(null);
        var by_index: Balances = @splat(null);
        try self.execute(arloc, "BEGIN", "BEGIN");
        for (0..bank_tables) |tbl| {
            try self.readTable(arloc, &by_key, try std.fmt.allocPrint(
                arloc,
                "SELECT id, balance FROM bank{d}",
                .{tbl},
            ));
            // Wide enough for a balance `backOut` wrote, if one leaks.
            try self.readTable(arloc, &by_index, try std.fmt.allocPrint(
                arloc,
                "SELECT id, balance FROM bank{d}" ++
                    " WHERE balance >= {d} AND balance <= {d}",
                .{ tbl, -2 * poison, bank_total },
            ));
        }
        try self.execute(arloc, "COMMIT", "COMMIT");
        self.reads += 1;

        expectBank(by_key, by_index) catch |err| {
            self.wrong = true;
            log.err("teller {d}: by key {any}, by index {any}", .{
                self.id, by_key, by_index,
            });
            return err;
        };
    }

    fn readTable(
        self: *Teller,
        arloc: std.mem.Allocator,
        found: *Balances,
        text: String,
    ) !void {
        const reply = try self.query(arloc, text);
        for (reply.rows) |cols| {
            if (cols.len != 2) return error.WrongColumnCount;
            const id = try intOf(cols[0]);
            if (id >= num_accounts) return error.UnknownAccount;
            if (found[id] != null) return error.RowReadTwice;
            found[id] = try balanceOf(cols[1]);
        }
    }
};

fn balanceOf(text: ?String) !i64 {
    return std.fmt.parseInt(i64, text orelse return error.NullColumn, 10);
}

/// One read of all accounts, by key and by index.
fn expectBank(by_key: Balances, by_index: Balances) !void {
    if (!std.meta.eql(by_key, by_index)) return error.BankIndexDiffers;
    var total: i64 = 0;
    for (by_key) |held| {
        const amount = held orelse continue;
        if (amount <= 0) return error.BankBalanceNotPositive;
        total += amount;
    }
    if (total != bank_total) return error.BankTotalWrong;
}

fn apply(rows: *Rows, writes: []const Write) void {
    for (writes) |write| rows[write.row] = write.seq;
}

fn expectSeq(writer: u32, row: u32, want: ?u64, got: ?u64) !void {
    if (std.meta.eql(want, got)) return;
    log.err("writer {d} row {d}: want seq {?d}, read {?d}", .{
        writer, row, want, got,
    });
    return error.ReadMismatch;
}

fn expectRows(writer: u32, how: String, want: Rows, got: Rows) !void {
    if (std.meta.eql(want, got)) return;
    for (want, got, 0..) |w, g, row| {
        if (std.meta.eql(w, g)) continue;
        log.err("writer {d} row {d} by {s}: want seq {?d}, read {?d}", .{
            writer, row, how, w, g,
        });
    }
    return error.ReadMismatch;
}

fn idOf(writer: u32, row: u32) u64 {
    return @as(u64, writer) * span + row;
}

/// The body write `seq` puts in row `id`: a run of hex digits whose
/// length and content come from the two, so a body from another write
/// or a damaged one does not pass for it.
fn bodyOf(arloc: std.mem.Allocator, id: u64, seq: u64) !String {
    var prng = std.Random.DefaultPrng.init(seq *% 0x9e3779b97f4a7c15 +% id);
    const rand = prng.random();
    const raw = try arloc.alloc(u8, 1 + rand.uintLessThan(usize, body_max));
    rand.bytes(raw);
    return std.fmt.allocPrint(arloc, "{x}", .{raw});
}

fn intOf(text: ?String) !u64 {
    return std.fmt.parseInt(u64, text orelse return error.NullColumn, 10);
}

/// The seq a row holds, once its body is what that write put there.
fn seqOf(
    arloc: std.mem.Allocator,
    id: u64,
    seq_text: ?String,
    body: ?String,
) !u64 {
    const seq = try intOf(seq_text);
    const want = try bodyOf(arloc, id, seq);
    const got = body orelse return error.NullColumn;
    if (!std.mem.eql(u8, want, got)) return error.DamagedRow;
    return seq;
}

/// Run every writer and teller for `ms` virtual milliseconds, then
/// tell them to stop and call `at_end`. Returns once each has had the
/// statement it was in answered.
fn runClients(
    alloc: std.mem.Allocator,
    writers: []Writer,
    tellers: []Teller,
    ms: u64,
    ctx: anytype,
    comptime at_end: fn (@TypeOf(ctx)) anyerror!void,
) !void {
    var stop = false;
    const clients = writers.len + tellers.len;
    var group = try fiber.Group(void).initCapacity(alloc, clients);
    defer group.deinit();
    defer group.wait();
    defer stop = true;

    for (writers) |*w| try group.spawn(.{}, Writer.run, .{ w, &stop });
    for (tellers) |*t| try group.spawn(.{}, Teller.run, .{ t, &stop });
    try resources.injectFaults(ms);
    // Before `at_end`, which may park: no writer starts a statement
    // after the first thing it does.
    stop = true;
    try at_end(ctx);
}

const CrashAt = struct {
    inst: *Instance,
    mode: fiber.simdisk.Crash,

    /// The power cut, which no statement starts after. The server
    /// runs on for a while on its dead disk, long enough one time in
    /// two for the log to give up, and is then stopped with statements
    /// still waiting. An apply that reads the store fails for as long
    /// as the disk is dead, and only the stop answers a COMMIT queued
    /// behind it.
    fn take(self: *const CrashAt) anyerror!void {
        // With a filesystem shut the node is right to have stopped,
        // and the cut is the restart that mounts it again.
        if (resources.shut()) {
            const gave_up = self.inst.eng.node.shutdown.hasFailed();
            log.info("a filesystem shut, node gave up: {}", .{gave_up});
        } else try stillUp(self.inst);
        try resources.powerCut(self.mode);
        fiber.sleepMs(fiber.random().uintLessThan(u64, 2 * retry_ms));
        self.inst.eng.stop();
    }
};

/// The node has not stopped by itself: no error the writers ran under
/// lasts long enough for the log to give up.
fn stillUp(inst: *Instance) anyerror!void {
    if (inst.eng.node.shutdown.hasFailed()) return error.NodeGaveUp;
}

/// The end of a stretch that no crash ends. An outage left on would
/// last until the writers have stopped, which they do only once the
/// log has answered them: longer than its retry bound.
fn endWithoutCrash(inst: *Instance) anyerror!void {
    try stillUp(inst);
    try resources.endOutages();
}

fn firstFailure(writers: []const Writer, tellers: []const Teller) !void {
    for (writers) |w| if (w.failed) |err| return err;
    for (tellers) |t| if (t.failed) |err| return err;
}

test "sim: sql keeps acknowledged statements across crashes" {
    try run(.{});
}

// Minutes per seed, so opt-in (`--bigtest`): bodies larger than a
// block, enough of them to outgrow a table, and a WAL that rotates
// many times.
test "BIG: sim: sql keeps acknowledged statements across many crashes" {
    try run(.{
        .crashes = 40,
        .stretch_ms = 500,
        .stretch_more_ms = 4000,
        .body_max = 6000,
        .faults = true,
    });
}

// Opt-in: the disk has the errors the seed draws.
test "BIG: sim: sql rides through disk errors" {
    try run(.{ .faults = true });
}

fn run(shape: Shape) !void {
    const sim = fiber.simDisk() orelse return error.SkipZigTest;
    const alloc = testing.allocator;
    body_max = shape.body_max;

    // Four times what the waits of this shape add up to at their
    // longest. A run past it is stuck and ends with its fibers
    // printed. The runner's `--sim-until` is used when given.
    const run_state = fiber.runtime().simState().?;
    const own_bound = run_state.opts.until_ms == null;
    const stretch_max_ms = shape.stretch_ms + shape.stretch_more_ms;
    if (own_bound) run_state.opts.until_ms = run_state.elapsedMs() +
        4 * (shape.crashes + 2) * (stretch_max_ms + 2 * retry_ms);
    defer if (own_bound) {
        run_state.opts.until_ms = null;
    };

    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();

    var writers: [num_writers]Writer = undefined;
    for (&writers, 0..) |*w, id| w.* = .{
        .id = @intCast(id),
        .alloc = alloc,
        .prng = std.Random.DefaultPrng.init(fiber.random().int(u64)),
    };
    defer for (&writers) |*w| w.deinit();
    var tellers: [num_tellers]Teller = undefined;
    for (&tellers, 0..) |*t, id| t.* = .{
        .id = @intCast(id),
        .alloc = alloc,
        .prng = std.Random.DefaultPrng.init(fiber.random().int(u64)),
    };

    const dir = try std.fmt.allocPrint(alloc, "{s}/data", .{tmp.path});
    defer alloc.free(dir);
    const wal_dir = try std.fmt.allocPrint(alloc, "{s}/wal", .{dir});
    defer alloc.free(wal_dir);
    const store_dir = try std.fmt.allocPrint(alloc, "{s}/store", .{dir});
    defer alloc.free(store_dir);

    resources = .{ .sim = sim, .data_dir = dir };
    defer resources.list.deinit(alloc);
    if (shape.faults) try resources.drawDirs(alloc, wal_dir, store_dir);

    // Null between a close and the open that follows it.
    var booted: ?*Instance = try Instance.open(alloc, dir);
    defer if (booted) |inst| inst.close();

    try createTable(alloc);

    for (0..shape.crashes) |_| {
        const crash: CrashAt = .{
            .inst = booted.?,
            .mode = if (fiber.random().boolean()) .lose_all else .lose_some,
        };
        const ms = shape.stretch_ms +
            fiber.random().uintLessThan(u64, shape.stretch_more_ms);
        try resources.start();
        try runClients(alloc, &writers, &tellers, ms, &crash, CrashAt.take);
        try firstFailure(&writers, &tellers);

        // The next boot is on a disk that works again.
        booted.?.close();
        booted = null;
        try resources.clear();
        booted = try Instance.open(alloc, dir);
        for (&writers) |*w| try w.recover();
        try tellers[0].audit();
    }

    // A last stretch, a clean stop, and a restart that must lose nothing.
    resources.endShutdowns();
    try resources.start();
    try runClients(
        alloc,
        &writers,
        &tellers,
        1000,
        booted.?,
        endWithoutCrash,
    );
    try firstFailure(&writers, &tellers);
    try stillUp(booted.?);
    try resources.clear();
    if (shape.faults) {
        // The faults are over: every statement is answered again, the
        // first of each writer the read of what it had in flight.
        try runClients(alloc, &writers, &tellers, 500, booted.?, stillUp);
        try firstFailure(&writers, &tellers);
    }
    booted.?.close();
    booted = null;
    booted = try Instance.open(alloc, dir);
    for (&writers) |*w| try w.expectAcked();
    try tellers[0].audit();

    // The WAL's disk stops taking writes. The statement in flight
    // fails once the log gives up, the node stops by itself, and a
    // restart on a disk that works has every acknowledged row.
    try sim.setFaults(wal_dir, .{ .write = 1000 });
    const started = utils.coarseMs();
    try writers[0].refusedWrite();
    const waited = utils.coarseMs() - started;
    try testing.expect(waited < 2 * retry_ms);
    try testing.expect(booted.?.eng.node.shutdown.hasFailed());
    booted.?.close();
    booted = null;
    try sim.setFaults(wal_dir, .{});
    booted = try Instance.open(alloc, dir);
    for (&writers) |*w| try w.expectAcked();
    try tellers[0].audit();

    var ops: u64 = 0;
    var kept: u32 = 0;
    var refused: u64 = 0;
    for (writers) |w| {
        ops += w.ops;
        kept += w.kept_in_flight;
        refused += w.refused;
    }
    log.info(
        "{d} statements and blocks across {d} crashes," ++
            " {d} of them in flight at a crash and kept," ++
            " {d} refused under disk errors",
        .{ ops, shape.crashes, kept, refused },
    );

    var transfers: u64 = 0;
    var short: u64 = 0;
    var reads: u64 = 0;
    var conflicts: [conflict_codes.len]u64 = @splat(0);
    for (tellers) |t| {
        transfers += t.transfers;
        short += t.short;
        reads += t.reads;
        for (&conflicts, t.conflicts) |*sum, count| sum.* += count;
    }
    log.info(
        "bank: {d} transfers, {d} more from an empty account, {d} reads" ++
            " of all accounts, ended by another transaction: {d} with 40001," ++
            " {d} with 40P01, {d} with 23505, {d} with 72000",
        .{
            transfers,    short,        reads,        conflicts[0],
            conflicts[1], conflicts[2], conflicts[3],
        },
    );
}
