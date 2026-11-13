// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Benchmarks for the scan path a statement drives: `Forest.Cursor`
//! over a whole table, every layer included (cursor, merge, level
//! iterators, block decode). table_bench.zig has each layer alone.
//!
//! Run with `zig build bench -Drelease -- --filter "cursor scan"`.

const std = @import("std");
const utils = @import("utils");
const String = utils.String;
const zdb = @import("zdb");
const createTree = @import("testing.zig").createTree;
const buildCompaction = @import("testing.zig").buildCompaction;
const flags = @import("flags");
const fiber = @import("zfiber");
const bench = fiber.bench;
const Bench = bench.Bench;
const vfs = zdb.vfs;

const Item = zdb.kv.Item;
const Forest = zdb.forest.Forest;
const Config = zdb.config.Config;

const ns: u64 = 1;
const table_key: zdb.kv.TableKey = .{ .namespace = ns, .table_id = 0 };
const num_rows: u64 = 100_000;
/// Rows put between flushes while loading.
const rows_per_flush: u64 = 20_000;
/// What a narrow SQL row takes: a bigint key and a 32-byte text.
const value: String = "0123456789abcdef0123456789abcdef01234567";

/// A forest with one table of `num_rows` rows, keyed by a big-endian
/// u64 as a SQL row is. The first `in_l1` are in L1; the rest stay in
/// the live memtable, as the tail of a fresh load does.
const Fixture = struct {
    alloc: std.mem.Allocator,
    tmp: vfs.TmpDir,
    live: *flags.Live(Config),
    store: vfs.Store,
    f: *Forest,
    tree_id: u64,
    next_index: u64 = 0,

    fn setup(self: *Fixture, alloc: std.mem.Allocator, in_l1: u64) !void {
        self.* = .{
            .alloc = alloc,
            .tmp = try vfs.TmpDir.init(alloc),
            .live = undefined,
            .store = undefined,
            .f = undefined,
            .tree_id = undefined,
        };
        // A bottom target of one byte makes a push L0's job at any
        // size, so the compaction below lands everything in L1.
        self.live = try flags.Live(Config).create(alloc, .{
            .lmax_target_bytes = 1,
        });
        self.store = try vfs.Store.initFile(alloc, self.tmp.path, vfs.g());
        self.f = try Forest.open(alloc, self.live, &self.store);
        self.tree_id = try createTree(self.f, table_key, .{}, .posting_index);
        try self.f.newMemtable(self.nextIndex());

        var id: u64 = 0;
        while (id < in_l1) : (id += 1) {
            try self.put(id);
            if ((id + 1) % rows_per_flush == 0) try self.flush();
        }
        if (in_l1 % rows_per_flush != 0) try self.flush();
        if (in_l1 > 0) try self.compactL0();
        while (id < num_rows) : (id += 1) try self.put(id);
    }

    fn teardown(self: *Fixture) void {
        self.f.close();
        self.store.deinit();
        self.live.destroy();
        self.tmp.deinit();
    }

    fn nextIndex(self: *Fixture) u64 {
        self.next_index += 1;
        return self.next_index;
    }

    fn put(self: *Fixture, id: u64) !void {
        var primary: [8]u8 = undefined;
        std.mem.writeInt(u64, &primary, id, .big);
        const items = [_]Item{.{
            .key = .{ .namespace = ns, .table_id = 0, .primary = &primary },
            .versions = .{ .one = .{ .ts = 10, .meta = .{}, .value = value } },
        }};
        try self.f.put(&items);
    }

    fn flush(self: *Fixture) !void {
        try self.f.newMemtable(self.nextIndex());
        var fl = (try self.f.buildFlush()) orelse return error.NoFlush;
        defer fl.changes.deinit(self.alloc);
        try self.f.applyFlush(
            self.nextIndex(),
            fl.changes.items,
            fl.newest_memtable_id,
        );
    }

    fn compactL0(self: *Fixture) !void {
        const tree = self.f.getTree(ns, self.tree_id) orelse
            return error.NoTree;
        defer tree.release();
        var comp = try buildCompaction(tree, 0);
        defer comp.deinit(self.alloc);
        try tree.applyCompaction(self.nextIndex(), comp.items);
    }
};

/// One op is a whole-table scan as count(*) drives it: keys and
/// versions handed out, no value read or pinned.
fn benchCursorScan(b: *Bench, in_l1: u64) !void {
    const alloc = std.heap.smp_allocator;
    var fx: Fixture = undefined;
    try fx.setup(alloc, in_l1);
    defer fx.teardown();

    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        var cur = try fx.f.openCursor(alloc, table_key, .all, .{});
        defer cur.deinit();
        var count: u64 = 0;
        while (try cur.next()) |item| : (count += 1) {
            b.addBytes(item.key.primary.len);
        }
        if (count != num_rows) return error.ShortScan;
    }
}

test "BENCH: cursor scan 100k rows, all in L1" {
    try benchCursorScan(bench.current(), num_rows);
}
test "BENCH: cursor scan 100k rows, 90% in L1, 10% in memtable" {
    try benchCursorScan(bench.current(), num_rows * 9 / 10);
}
test "BENCH: cursor scan 100k rows, all in memtable" {
    try benchCursorScan(bench.current(), 0);
}
