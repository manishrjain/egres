// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Wire-format tests for the .data Mutations payload: a flat kv list
//! with full self-describing keys, one namespace per entry.

const std = @import("std");
const testing = std.testing;
const engine = @import("engine");
const zdb = @import("zdb");

const zen = zdb.buffers;
const Key = zdb.kv.Key;
const Meta = zdb.kv.Meta;

test "mutations: flat multi-table round-trip" {
    const alloc = testing.allocator;

    // Two tables in one namespace. Keys go on the wire marshaled and
    // without a timestamp: apply stamps every write with the entry's
    // commit HLC.
    const ka = try (Key{
        .namespace = 7,
        .table_id = 16,
        .primary = "row-a",
    }).marshal(alloc);
    defer alloc.free(ka);
    const kb = try (Key{
        .namespace = 7,
        .table_id = 17,
        .primary = "row-b",
    }).marshal(alloc);
    defer alloc.free(kb);

    var kvs = [_]zen.KeyValueStruct{
        .{ .key = ka, .value = "va" },
        .{ .key = kb, .meta = (Meta{ .kind = .deleted }).toByte() },
    };
    const payload = try engine.Engine.encodeMutations(alloc, 7, &kvs);
    defer alloc.free(payload);

    const ms = switch (try zen.parseMessageWithTail(payload)) {
        .Mutations => |r| r,
        else => return error.TestUnexpectedResult,
    };
    try testing.expectEqual(7, ms.namespace());
    try testing.expectEqual(2, ms.kvsLen());

    // kv 0: a plain put — the meta zero-default decodes as a raw state
    // row. Parsing the wire key recovers the full routing identity.
    {
        const kv = try ms.kvsAt(0);
        try testing.expectEqualSlices(u8, ka, kv.key());
        try testing.expectEqualSlices(u8, "va", kv.value());
        try testing.expect(!Meta.fromByte(kv.meta()).isDeleted());
        const key = Key.init(kv.key());
        try testing.expectEqual(7, key.namespace);
        try testing.expectEqual(16, key.table_id);
        try testing.expectEqualSlices(u8, "row-a", key.primary);
    }
    // kv 1: a tombstone in the other table — empty value, kind deleted.
    {
        const kv = try ms.kvsAt(1);
        try testing.expectEqualSlices(u8, kb, kv.key());
        try testing.expectEqual(0, kv.value().len);
        try testing.expect(Meta.fromByte(kv.meta()).isDeleted());
        try testing.expectEqual(17, Key.init(kv.key()).table_id);
    }
}

const raft = @import("raft");
const utils = @import("utils");
const flags = @import("flags");

const Forest = zdb.forest.Forest;
const String = []const u8;
const ns: u64 = 7;

const ApplyFixture = struct {
    tmp: std.testing.TmpDir,
    dir_path: [:0]u8,
    live: *flags.Live(zdb.config.Config),
    store: *zdb.vfs.RemoteStore,
    forest: *Forest,
    impl: engine.StorageImpl,
    arena: std.heap.ArenaAllocator,
    /// Every entry applied, in order: what the WAL holds, for a test
    /// that restarts and replays it.
    wal: std.ArrayList(Logged),

    const Logged = struct { index: u64, ts: u64, payload: String };

    /// A forest with a real memtable (entry 1) and namespace `ns`
    /// (entry 2), recovered with `oldest_memtable_index`.
    fn setup(self: *ApplyFixture, oldest_memtable_index: u64) !void {
        const alloc = testing.allocator;
        self.wal = .empty;
        self.tmp = std.testing.tmpDir(.{});
        errdefer self.tmp.cleanup();
        self.dir_path = try self.tmp.dir.realPathFileAlloc(
            utils.context.g().io,
            ".",
            alloc,
        );
        errdefer alloc.free(self.dir_path);
        self.store = try alloc.create(zdb.vfs.RemoteStore);
        errdefer alloc.destroy(self.store);
        self.store.* = try zdb.vfs.RemoteStore.initFile(
            alloc,
            self.dir_path,
            zdb.vfs.g(),
        );
        errdefer self.store.deinit();
        self.live = try flags.Live(zdb.config.Config).create(alloc, .{});
        errdefer self.live.destroy();
        self.forest = try Forest.open(alloc, self.live, self.store);
        errdefer self.forest.close();
        self.arena = std.heap.ArenaAllocator.init(alloc);
        self.impl = .{
            .forest = self.forest,
            .allocator = alloc,
            .node = undefined,
            .snapshot_cfg = undefined,
        };
        self.impl.markRecovered(oldest_memtable_index);

        try self.apply(1, 10, .{
            .tree_changes = &.{},
            .sstable_changes = &.{},
            .cmd = .new_memtable,
            .memtable_id = 0,
        });
        const created = try self.forest.buildNsCreate(
            self.arena.allocator(),
            .{ .given = ns },
            "",
        );
        try self.apply(2, 20, created.change);
    }

    fn teardown(self: *ApplyFixture) void {
        const alloc = testing.allocator;
        self.arena.deinit();
        self.forest.close();
        self.live.destroy();
        self.store.deinit();
        alloc.destroy(self.store);
        alloc.free(self.dir_path);
        self.tmp.cleanup();
    }

    fn apply(
        self: *ApplyFixture,
        index: u64,
        ts: u64,
        mc: zdb.manifest.ManifestChange,
    ) !void {
        const payload = try zen.marshalRoot(self.arena.allocator(), mc);
        try self.applyLogged(index, ts, payload);
    }

    /// A .data entry writing `kvs` in namespace `in_ns`.
    fn applyData(
        self: *ApplyFixture,
        index: u64,
        ts: u64,
        in_ns: u64,
        kvs: []zen.KeyValueStruct,
    ) !void {
        const payload = try engine.Engine.encodeMutations(
            self.arena.allocator(),
            in_ns,
            kvs,
        );
        try self.applyLogged(index, ts, payload);
    }

    /// A flush entry for the frozen memtables, built as the leader
    /// builds it.
    fn applyFlush(self: *ApplyFixture, index: u64, ts: u64) !void {
        var flush = (try self.forest.buildFlush()) orelse
            return error.TestExpectedFlush;
        defer flush.changes.deinit(testing.allocator);
        try self.apply(index, ts, .{
            .tree_changes = &.{},
            .sstable_changes = flush.changes.items,
            .cmd = .flush,
            .memtable_id = flush.newest_memtable_id,
        });
    }

    fn applyLogged(
        self: *ApplyFixture,
        index: u64,
        ts: u64,
        payload: String,
    ) !void {
        try self.wal.append(self.arena.allocator(), .{
            .index = index,
            .ts = ts,
            .payload = payload,
        });
        try self.applyRaw(index, ts, payload);
    }

    fn applyRaw(
        self: *ApplyFixture,
        index: u64,
        ts: u64,
        payload: String,
    ) !void {
        const sm = self.impl.asStateMachine();
        try sm.applyFn(sm.ctx, .{
            .term = 1,
            .index = index,
            .timestamp = ts,
        }, payload);
    }

    /// The snapshot `Engine.maybeProposeSnapshot` would take at
    /// `snap_index`: a draft manifest replaying every manifest entry up
    /// to it, serialized. Caller frees.
    fn snapshotAt(self: *ApplyFixture, snap_index: u64) !String {
        var draft = zdb.manifest.Manifest.init(testing.allocator);
        defer draft.deinit();
        for (self.wal.items) |logged| {
            if (logged.index > snap_index) break;
            const mc = switch (try zen.parseMessageWithTail(logged.payload)) {
                .ManifestChange => |reader| reader,
                else => continue,
            };
            const mcs = try mc.toStruct(testing.allocator);
            defer {
                mcs.deinit(testing.allocator);
                testing.allocator.destroy(mcs);
            }
            try draft.apply(logged.index, mcs.*);
        }
        return draft.serialize(testing.allocator);
    }

    /// Crash and restart as `Engine.recoverFromSnapshot` and the apply
    /// loop do: a fresh forest over the same files, the manifest from
    /// the snapshot, then the entries the WAL kept after it, with rows
    /// below `oldest_memtable_index` skipped.
    fn restart(
        self: *ApplyFixture,
        snap_index: u64,
        snap_bytes: String,
        snap_ts: u64,
        oldest_memtable_index: u64,
    ) !void {
        const alloc = testing.allocator;
        self.forest.close();
        self.forest = try Forest.open(alloc, self.live, self.store);
        try self.forest.manifest.restoreFrom(snap_bytes);
        _ = self.forest.setSnapshotTs(snap_ts);
        try self.forest.openTablesFromManifest();
        self.impl = .{
            .forest = self.forest,
            .allocator = alloc,
            .node = undefined,
            .snapshot_cfg = undefined,
        };
        self.impl.markRecovered(oldest_memtable_index);
        for (self.wal.items) |logged| {
            if (logged.index <= snap_index) continue;
            try self.applyRaw(logged.index, logged.ts, logged.payload);
        }
    }

    /// The value of `primary` in `tk` at `read_ts`, or null.
    fn readIn(
        self: *ApplyFixture,
        tk: zdb.kv.TableKey,
        primary: String,
        read_ts: u64,
    ) !?[]u8 {
        const found = try self.forest.get(testing.allocator, .{
            .key = .{
                .namespace = tk.namespace,
                .table_id = tk.table_id,
                .primary = primary,
            },
            .read_ts = read_ts,
        }) orelse return null;
        defer found.free(testing.allocator);
        return try self.arena.allocator().dupe(u8, found.newest().value);
    }

    /// A catalog-table key in `in_ns`, marshaled for the wire.
    fn key(self: *ApplyFixture, in_ns: u64, primary: String) !String {
        return (Key{
            .namespace = in_ns,
            .table_id = 1,
            .primary = primary,
        }).marshal(self.arena.allocator());
    }
};

test "branch: a restart keeps the rows a namespace branch copied" {
    var fx: ApplyFixture = undefined;
    try fx.setup(0);
    defer fx.teardown();

    // 3 to 5: a row in memtable 1, which freezes and flushes, so the
    // snapshot below has something to cover.
    var warm = [_]zen.KeyValueStruct{
        .{ .key = try fx.key(ns, "warm"), .value = "w" },
    };
    try fx.applyData(3, 30, ns, &warm);
    try fx.apply(4, 40, newMemtable());
    try fx.applyFlush(5, 50);

    const catalog: zdb.kv.TableKey = .{ .namespace = ns, .table_id = 1 };
    // 6: a catalog row lands in memtable 4.
    var row = [_]zen.KeyValueStruct{
        .{ .key = try fx.key(ns, "users"), .value = "cols" },
    };
    try fx.applyData(6, 60, ns, &row);

    // 7: memtable 4 freezes; memtable 7 is current.
    try fx.apply(7, 70, newMemtable());

    // 8: CREATE DATABASE ... TEMPLATE: the row is copied from frozen
    // memtable 4 into memtable 7, under the new namespace.
    const branched = try fx.forest.buildNsBranch(
        fx.arena.allocator(),
        ns,
        "",
    );
    try fx.apply(8, 80, branched.change);
    const copy: zdb.kv.TableKey = .{
        .namespace = branched.namespace,
        .table_id = catalog.table_id,
    };
    try testing.expectEqualStrings(
        "cols",
        (try fx.readIn(copy, "users", 80)).?,
    );

    // 9: memtable 4 flushes; its SST holds the source's row only, and
    // the copy stays in memtable 7.
    try fx.applyFlush(9, 90);

    // The snapshot maybeProposeSnapshot may take now: its index is
    // capped one below the oldest index replay needs, which is also the
    // skip floor. That must keep the source's write (6), though the
    // memtable it went into has flushed.
    const oldest = fx.forest.memtables.oldestInMemoryLogIndex();
    try testing.expect(oldest <= 6);
    const snap_index = oldest - 1;
    try testing.expect(snap_index > 0);
    const snap_bytes = try fx.snapshotAt(snap_index);
    defer testing.allocator.free(snap_bytes);

    // Crash before memtable 7 flushes; restart and replay what the WAL
    // kept.
    try fx.restart(snap_index, snap_bytes, 30, oldest);

    try testing.expectEqualStrings(
        "cols",
        (try fx.readIn(catalog, "users", 90)).?,
    );
    try testing.expectEqualStrings(
        "cols",
        (try fx.readIn(copy, "users", 90)) orelse
            return error.BranchCopyLostOnRestart,
    );
}

fn newMemtable() zdb.manifest.ManifestChange {
    return .{
        .tree_changes = &.{},
        .sstable_changes = &.{},
        .cmd = .new_memtable,
        .memtable_id = 0,
    };
}
