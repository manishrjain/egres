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
//! At the end the WAL's disk stops taking writes: the statement then
//! in flight must fail, the node must stop by itself once the log
//! gives up, and a restart must have every acknowledged row.
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
const vfs = zdb.vfs;
const log = std.log.scoped(.sim_test);

const num_writers = 4;
const rows_per_writer = 48;
/// Crashes in one run; each is followed by a restart and a check.
const crashes = 4;
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
const dead_disk: fiber.simdisk.Faults = .{
    .read = 1000,
    .write = 1000,
    .sync = 1000,
    .set_size = 1000,
    .path_ops = 1000,
};

/// True from a crash until the disk works again. A statement that
/// fails meanwhile is the crash's, not a failed run.
var disk_down = false;

/// One boot of the server on a data directory: the store and the flags
/// `egres/main.zig` hands to `Engine.open`, then its calls in its
/// order, without the admin listener.
const Instance = struct {
    alloc: std.mem.Allocator,
    wal_dir: []u8,
    store_dir: []u8,
    store: vfs.Store,
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

    /// Every connection is one of the test's and closed by now, which
    /// the server's side of each has seen (`pgwire.Client.close`).
    fn close(self: *Instance) void {
        self.eng.stop();
        self.sqlsrv.deinit();
        self.eng.deinit();
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
    const refused = reply.err orelse return reply;
    if (!disk_down) log.err("{s} {s}: {s}", .{
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
    const reply = try ask(client, arloc, text);
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
    failed: ?anyerror = null,

    fn deinit(self: *Writer) void {
        self.pending.deinit(self.alloc);
    }

    /// The writer's fiber: one session of statements until `stop`.
    fn run(self: *Writer, stop: *const bool) void {
        self.session(stop) catch |err| {
            self.failed = err;
        };
    }

    fn session(self: *Writer, stop: *const bool) !void {
        self.client = try connect(self.alloc);
        defer self.client.close();
        while (!stop.*) {
            self.step() catch |err| {
                // The crash took the step in flight: an error, or the
                // connection gone. `recover` decides what became of it.
                if (disk_down) return;
                return err;
            };
            self.ops += 1;
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
    const raw = try arloc.alloc(u8, 1 + rand.uintLessThan(usize, 450));
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

/// Run every writer for `ms` virtual milliseconds, then, with them
/// still writing, call `at_end` and stop them.
fn runWriters(
    alloc: std.mem.Allocator,
    writers: []Writer,
    ms: u64,
    ctx: anytype,
    comptime at_end: fn (@TypeOf(ctx)) anyerror!void,
) !void {
    var stop = false;
    var group = try fiber.Group(void).initCapacity(alloc, writers.len);
    defer group.deinit();
    defer group.wait();
    defer stop = true;

    for (writers) |*w| try group.spawn(.{}, Writer.run, .{ w, &stop });
    fiber.sleepMs(ms);
    try at_end(ctx);
}

const CrashAt = struct {
    disk: *fiber.simdisk.SimDisk,
    dir: String,
    mode: fiber.simdisk.Crash,

    /// The power cut. The disk stays dead for the server still
    /// running on it, so nothing that server does from here is there
    /// after a restart. The caller stops the writers in the same step
    /// of its fiber: none starts a statement the crash did not see.
    fn take(self: *const CrashAt) anyerror!void {
        self.disk.crash(self.dir, self.mode);
        try self.disk.setFaults(self.dir, dead_disk);
        disk_down = true;
    }
};

fn nothing(_: void) anyerror!void {}

fn firstFailure(writers: []const Writer) !void {
    for (writers) |w| if (w.failed) |err| return err;
}

test "sim: sql keeps acknowledged statements across crashes" {
    const disk = fiber.simDisk() orelse return error.SkipZigTest;
    const alloc = testing.allocator;

    var tmp = try vfs.TmpDir.init(alloc);
    defer tmp.deinit();

    var writers: [num_writers]Writer = undefined;
    for (&writers, 0..) |*w, id| w.* = .{
        .id = @intCast(id),
        .alloc = alloc,
        .prng = std.Random.DefaultPrng.init(fiber.random().int(u64)),
    };
    defer for (&writers) |*w| w.deinit();

    const dir = try std.fmt.allocPrint(alloc, "{s}/data", .{tmp.path});
    defer alloc.free(dir);
    // Null between a close and the open that follows it.
    var booted: ?*Instance = try Instance.open(alloc, dir);
    defer if (booted) |inst| inst.close();

    try createTable(alloc);

    for (0..crashes) |_| {
        const crash: CrashAt = .{
            .disk = disk,
            .dir = dir,
            .mode = if (fiber.random().boolean()) .lose_all else .lose_some,
        };
        const ms = 300 + fiber.random().uintLessThan(u64, 2500);
        try runWriters(alloc, &writers, ms, &crash, CrashAt.take);
        try firstFailure(&writers);

        // The server runs on for a while on its dead disk, long enough
        // one time in two for the log to give up, and is stopped
        // there. Then the disk works again.
        fiber.sleepMs(fiber.random().uintLessThan(u64, 2 * retry_ms));
        booted.?.close();
        booted = null;
        try disk.setFaults(dir, .{});
        disk_down = false;
        booted = try Instance.open(alloc, dir);
        for (&writers) |*w| try w.recover();
    }

    // A last stretch, a clean stop, and a restart that must lose nothing.
    try runWriters(alloc, &writers, 1000, {}, nothing);
    try firstFailure(&writers);
    booted.?.close();
    booted = null;
    booted = try Instance.open(alloc, dir);
    for (&writers) |*w| try w.expectAcked();

    // The WAL's disk stops taking writes. The statement in flight
    // fails once the log gives up, the node stops by itself, and a
    // restart on a disk that works has every acknowledged row.
    const wal_dir = try std.fmt.allocPrint(alloc, "{s}/wal", .{dir});
    defer alloc.free(wal_dir);
    try disk.setFaults(wal_dir, .{ .write = 1000 });
    const started = utils.coarseMs();
    try writers[0].refusedWrite();
    const waited = utils.coarseMs() - started;
    try testing.expect(waited < 2 * retry_ms);
    try testing.expect(booted.?.eng.node.shutdown.hasFailed());
    booted.?.close();
    booted = null;
    try disk.setFaults(wal_dir, .{});
    booted = try Instance.open(alloc, dir);
    for (&writers) |*w| try w.expectAcked();

    var ops: u64 = 0;
    var kept: u32 = 0;
    for (writers) |w| {
        ops += w.ops;
        kept += w.kept_in_flight;
    }
    log.info(
        "{d} statements and blocks across {d} crashes," ++
            " {d} of them in flight at a crash and kept",
        .{ ops, crashes, kept },
    );
}
