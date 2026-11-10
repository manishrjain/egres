// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The engine under the simulation: a real WAL, raft node, forest and
//! the engine's loops on the simulated disk, a seeded workload against
//! them, and a model of what every acknowledged write must read as.
//!
//! Each writer owns its keys, so a read of one has one right answer:
//! the last write the writer saw acknowledged. A crash is a copy of
//! the data directory as a power cut at that instant would leave it
//! (`SimDisk.crashCopy`); the engine that was running is stopped and a
//! new one opens on the copy. There each writer's keys must read as
//! its acknowledged writes, with or without the whole batch it had in
//! flight: an acknowledged write gone, or half a batch, fails the run.
//!
//! One sim seed is one workload and one schedule:
//! `zig build test -- --sim-seed N --filter "sim: engine"`.

const std = @import("std");
const testing = std.testing;
const engine = @import("engine");
const fiber = @import("zfiber");
const flags = @import("flags");
const raft = @import("raft");
const utils = @import("utils");
const zdb = @import("zdb");

const String = utils.String;
const Key = zdb.kv.Key;
const Meta = zdb.kv.Meta;
const vfs = zdb.vfs;
const zen = zdb.buffers;
const log = std.log.scoped(.sim_test);

const ns: u64 = zdb.manifest.first_namespace;
const num_writers = 4;
const keys_per_writer = 48;
/// Crashes in one run; each is followed by a restart and a check.
const crashes = 4;

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
};

/// One boot of the engine on a data directory: the store and the flags
/// `egres/main.zig` hands to `Engine.open`, without its listeners.
const Instance = struct {
    alloc: std.mem.Allocator,
    wal_dir: []u8,
    store_dir: []u8,
    store: vfs.Store,
    zdb_live: *flags.Live(zdb.config.Config),
    snapshot_live: *flags.Live(engine.SnapshotCfg),
    runtime_live: *flags.Live(fiber.Runtime.Cfg),
    eng: *engine.Engine,

    /// Open on `dir`, fresh or left by an earlier boot, and return once
    /// the node leads and has applied everything its WAL held.
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

        // The open, start and stop the binary runs. No listen address:
        // a single node with no transport.
        self.eng = try engine.Engine.open(alloc, .{
            .wal_dir = self.wal_dir,
            .wal = wal_opts,
            .raft = .{ .id = 1 },
            .store = &self.store,
            .database = "sim",
            .zdb_cfg = self.zdb_live,
            .snapshot_cfg = self.snapshot_live,
            .runtime_cfg = self.runtime_live,
        });
        errdefer self.eng.deinit();

        try self.eng.start();
        errdefer self.eng.stop();

        const node = self.eng.node;
        // An election takes at most twenty 100 ms ticks.
        var waited_ms: u32 = 0;
        while (!node.isLeader()) : (waited_ms += 10) {
            if (waited_ms > 30_000) return error.NoLeader;
            fiber.sleepMs(10);
        }
        // `Engine.waitForBootCatchUp` with a bound: an apply that
        // retries a damaged entry for ever is a failed run, not a hang.
        const boot_tail = node.log.lastTimestamp();
        node.waitForApplied(boot_tail, 60_000) catch
            return error.BootNeverCaughtUp;
        return self;
    }

    fn close(self: *Instance) void {
        self.eng.stop();
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

/// One key's write in a batch; `seq` null is a delete.
const Write = struct { key: u32, seq: ?u64 };

/// What each of a writer's keys holds: the seq of its value, null for
/// absent.
const Keys = [keys_per_writer]?u64;

const Writer = struct {
    id: u32,
    prng: std.Random.DefaultPrng,
    /// Every write of this writer takes the next one, so a value names
    /// the write that made it.
    next_seq: u64 = 1,
    /// What the engine acknowledged.
    acked: Keys = @splat(null),
    /// The batch proposed and not yet acknowledged.
    pending: std.ArrayList(Write) = .empty,
    /// `acked` and `pending` at the instant of the crash copy.
    crash_acked: Keys = @splat(null),
    crash_pending: std.ArrayList(Write) = .empty,
    ops: u64 = 0,
    failed: ?anyerror = null,

    fn deinit(self: *Writer, alloc: std.mem.Allocator) void {
        self.pending.deinit(alloc);
        self.crash_pending.deinit(alloc);
    }

    /// The writer's fiber: writes and checked reads until `stop`.
    fn run(self: *Writer, inst: *Instance, stop: *const bool) void {
        while (!stop.*) {
            self.step(inst) catch |err| {
                self.failed = err;
                return;
            };
            self.ops += 1;
        }
    }

    fn step(self: *Writer, inst: *Instance) !void {
        const rand = self.prng.random();
        switch (rand.uintLessThan(u32, 10)) {
            0...6 => try self.writeBatch(inst),
            7...8 => {
                const key = rand.uintLessThan(u32, keys_per_writer);
                const found = try self.get(inst, key);
                try expectSeq(self.id, key, self.acked[key], found);
            },
            else => {
                const found = try self.scan(inst);
                for (found, self.acked, 0..) |got, want, key|
                    try expectSeq(self.id, @intCast(key), want, got);
            },
        }
    }

    /// One proposal of one to four puts and deletes on distinct keys:
    /// one raft entry, so all of it applies or none.
    fn writeBatch(self: *Writer, inst: *Instance) !void {
        const alloc = inst.alloc;
        const rand = self.prng.random();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const arloc = arena.allocator();

        const count = 1 + rand.uintLessThan(usize, 4);
        const first = rand.uintLessThan(u32, keys_per_writer);
        const kvs = try arloc.alloc(zen.KeyValueStruct, count);
        self.pending.clearRetainingCapacity();
        for (kvs, 0..) |*kv, i| {
            const key = (first + @as(u32, @intCast(i))) % keys_per_writer;
            const wire_key = try keyOf(self.id, key).marshal(arloc);
            if (rand.uintLessThan(u32, 5) == 0) {
                try self.pending.append(alloc, .{ .key = key, .seq = null });
                kv.* = .{
                    .key = wire_key,
                    .value = "",
                    .meta = (Meta{ .kind = .deleted }).toByte(),
                };
                continue;
            }
            const seq = self.next_seq;
            self.next_seq += 1;
            try self.pending.append(alloc, .{ .key = key, .seq = seq });
            kv.* = .{
                .key = wire_key,
                .value = try valueOf(arloc, self.id, key, seq),
            };
        }
        try inst.eng.proposeWrites(ns, kvs, .apply);
        apply(&self.acked, self.pending.items);
        self.pending.clearRetainingCapacity();
    }

    fn get(self: *Writer, inst: *Instance, key: u32) !?u64 {
        const item = try inst.eng.forest.get(inst.alloc, .{
            .key = keyOf(self.id, key),
        }) orelse return null;
        defer item.free(inst.alloc);
        return try seqOf(inst.alloc, self.id, key, item.newest().value);
    }

    /// Every key of this writer the engine holds, by one range scan.
    fn scan(self: *Writer, inst: *Instance) !Keys {
        var lo_buf: [16]u8 = undefined;
        var hi_buf: [16]u8 = undefined;
        const lo = try std.fmt.bufPrint(&lo_buf, "w{d:0>2}/", .{self.id});
        const hi = try std.fmt.bufPrint(&hi_buf, "w{d:0>2}0", .{self.id});
        const Ctx = struct {
            alloc: std.mem.Allocator,
            writer: u32,
            found: Keys = @splat(null),

            fn emit(c: *@This(), item: zdb.kv.Item) anyerror!bool {
                const primary = item.key.primary;
                const key = try std.fmt.parseInt(u32, primary[5..], 10);
                c.found[key] = try seqOf(
                    c.alloc,
                    c.writer,
                    key,
                    item.newest().value,
                );
                return true;
            }
        };
        var ctx: Ctx = .{ .alloc = inst.alloc, .writer = self.id };
        _ = try inst.eng.forest.scan(
            inst.alloc,
            .{ .namespace = ns, .table_id = 0 },
            .{
                .left = .{ .namespace = ns, .primary = lo },
                .right = .{ .namespace = ns, .primary = hi },
            },
            .{},
            &ctx,
            Ctx.emit,
        );
        return ctx.found;
    }

    /// After a restart on a crash copy: the keys must read as they
    /// were acknowledged at the crash, or as that plus the whole batch
    /// then in flight. What is found is what the writer carries on from.
    fn recover(self: *Writer, inst: *Instance) !void {
        const found = try self.scan(inst);
        for (found, 0..) |by_scan, key|
            try expectSeq(
                self.id,
                @intCast(key),
                by_scan,
                try self.get(inst, @intCast(key)),
            );
        var with_pending = self.crash_acked;
        apply(&with_pending, self.crash_pending.items);
        const lost = !std.meta.eql(found, self.crash_acked);
        if (lost and !std.meta.eql(found, with_pending)) {
            for (found, self.crash_acked, with_pending, 0..) |got, a, b, key| {
                if (std.meta.eql(got, a) and std.meta.eql(got, b)) continue;
                log.err(
                    "writer {d} key {d}: found {?d}, acked {?d}," ++
                        " with the batch in flight {?d}",
                    .{ self.id, key, got, a, b },
                );
            }
            return error.CrashLostOrTornWrites;
        }
        self.acked = found;
        self.pending.clearRetainingCapacity();
    }
};

fn apply(keys: *Keys, batch: []const Write) void {
    for (batch) |w| keys[w.key] = w.seq;
}

fn expectSeq(writer: u32, key: u32, want: ?u64, got: ?u64) !void {
    if (std.meta.eql(want, got)) return;
    log.err("writer {d} key {d}: want seq {?d}, read {?d}", .{
        writer, key, want, got,
    });
    return error.ReadMismatch;
}

/// "wNN/kNNNN" in the raw-KV table of the genesis namespace.
fn keyOf(writer: u32, key: u32) Key {
    const names = struct {
        var bufs: [num_writers][keys_per_writer][9]u8 = undefined;
    };
    const buf = &names.bufs[writer][key];
    _ = std.fmt.bufPrint(buf, "w{d:0>2}/k{d:0>4}", .{ writer, key }) catch
        unreachable;
    return .{ .namespace = ns, .primary = buf };
}

/// The value write `seq` of `writer` puts at `key`: the seq, then a
/// run of bytes and a length both drawn from the three, so a value
/// from another write or a damaged one does not pass for it.
fn valueOf(
    alloc: std.mem.Allocator,
    writer: u32,
    key: u32,
    seq: u64,
) ![]u8 {
    var prng = std.Random.DefaultPrng.init(
        seq *% 0x9e3779b97f4a7c15 +% (@as(u64, writer) << 32 | key),
    );
    const rand = prng.random();
    const value = try alloc.alloc(u8, 8 + rand.uintLessThan(usize, 900));
    std.mem.writeInt(u64, value[0..8], seq, .little);
    rand.bytes(value[8..]);
    return value;
}

/// The seq `value` carries, once the whole value is what that write
/// put there.
fn seqOf(
    alloc: std.mem.Allocator,
    writer: u32,
    key: u32,
    value: String,
) !u64 {
    if (value.len < 8) return error.DamagedValue;
    const seq = std.mem.readInt(u64, value[0..8], .little);
    const want = try valueOf(alloc, writer, key, seq);
    defer alloc.free(want);
    if (!std.mem.eql(u8, want, value)) return error.DamagedValue;
    return seq;
}

/// Run every writer against `inst` for `ms` virtual milliseconds, then,
/// with them still writing, call `at_end` and stop them.
fn runWriters(
    alloc: std.mem.Allocator,
    inst: *Instance,
    writers: []Writer,
    ms: u64,
    ctx: anytype,
    comptime at_end: fn (@TypeOf(ctx)) anyerror!void,
) !void {
    var stop = false;
    var handles: std.ArrayList(fiber.JoinHandle(void)) = .empty;
    defer handles.deinit(alloc);
    defer for (handles.items) |*h| h.join();
    defer stop = true;
    for (writers) |*w| {
        const h = try fiber.spawn(.{}, Writer.run, .{ w, inst, &stop });
        try handles.append(alloc, h);
    }
    fiber.sleepMs(ms);
    try at_end(ctx);
}

const CrashAt = struct {
    alloc: std.mem.Allocator,
    disk: *fiber.simdisk.SimDisk,
    writers: []Writer,
    from: String,
    to: String,
    mode: fiber.simdisk.Crash,

    /// The copy and what each writer had acknowledged and in flight,
    /// taken in one step of this fiber: no write lands between them.
    fn take(self: *const CrashAt) anyerror!void {
        try self.disk.crashCopy(self.from, self.to, self.mode);
        for (self.writers) |*w| {
            w.crash_acked = w.acked;
            w.crash_pending.clearRetainingCapacity();
            try w.crash_pending.appendSlice(self.alloc, w.pending.items);
        }
    }
};

fn nothing(_: void) anyerror!void {}

fn firstFailure(writers: []const Writer) !void {
    for (writers) |w| if (w.failed) |err| return err;
}

test "sim: engine keeps acknowledged writes across crashes" {
    const disk = fiber.simDisk() orelse return error.SkipZigTest;
    const alloc = testing.allocator;
    const rand = fiber.random();

    const base = try std.fmt.allocPrint(alloc, "/tmp/egres-sim-{x}", .{
        rand.int(u64),
    });
    defer alloc.free(base);
    try vfs.createDirPath(base);
    defer vfs.deleteTree(base) catch {};

    var writers: [num_writers]Writer = undefined;
    for (&writers, 0..) |*w, id| w.* = .{
        .id = @intCast(id),
        .prng = std.Random.DefaultPrng.init(rand.int(u64)),
    };
    defer for (&writers) |*w| w.deinit(alloc);

    // One data directory per boot; each crash copies the last into
    // the next.
    var dirs: std.ArrayList([]u8) = .empty;
    defer {
        for (dirs.items) |d| alloc.free(d);
        dirs.deinit(alloc);
    }
    try dirs.ensureTotalCapacity(alloc, crashes + 1);
    for (0..crashes + 1) |boot| dirs.appendAssumeCapacity(
        try std.fmt.allocPrint(alloc, "{s}/boot{d}", .{ base, boot }),
    );
    var dir: String = dirs.items[0];
    // Null between a close and the open that follows it.
    var booted: ?*Instance = try Instance.open(alloc, dir);
    defer if (booted) |inst| inst.close();

    for (1..crashes + 1) |boot| {
        const next: String = dirs.items[boot];
        const crash: CrashAt = .{
            .alloc = alloc,
            .disk = disk,
            .writers = &writers,
            .from = dir,
            .to = next,
            .mode = if (rand.boolean()) .lose_all else .lose_some,
        };
        const ms = 300 + rand.uintLessThan(u64, 2500);
        try runWriters(alloc, booted.?, &writers, ms, &crash, CrashAt.take);
        try firstFailure(&writers);

        // The engine that was running stops in its own time; nothing it
        // does from here is in the copy.
        booted.?.close();
        booted = null;
        try vfs.deleteTree(dir);
        dir = next;
        booted = try Instance.open(alloc, dir);
        for (&writers) |*w| try w.recover(booted.?);
    }

    // A last stretch, a clean stop, and a restart that must lose nothing.
    try runWriters(alloc, booted.?, &writers, 1000, {}, nothing);
    try firstFailure(&writers);
    booted.?.close();
    booted = null;
    booted = try Instance.open(alloc, dir);
    for (&writers) |*w| {
        const found = try w.scan(booted.?);
        for (found, w.acked, 0..) |got, want, key|
            try expectSeq(w.id, @intCast(key), want, got);
    }
    var ops: u64 = 0;
    for (writers) |w| ops += w.ops;
    log.info("{d} ops across {d} crashes", .{ ops, crashes });
}
