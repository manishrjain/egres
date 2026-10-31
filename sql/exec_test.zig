// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const fiber = @import("zfiber");
const sql = @import("sql");
const String = sql.value.String;
const utils = @import("utils");
const zdb = @import("zdb");
const createTree = @import("../zdb/testing.zig").createTree;
const flags = @import("flags");

const exec = sql.exec;
const Batch = exec.Batch;
const BitmapNode = exec.BitmapNode;
const RowNode = exec.RowNode;
const Pred = sql.pred.Pred;
const Table = sql.table.Table;
const Txn = sql.txn.Txn;
const Forest = zdb.forest.Forest;
const Config = zdb.config.Config;
const Item = zdb.kv.Item;
const Value = sql.value.Value;
const zroar = zdb.zroar;

const ns: u64 = 16;
const tid: u32 = 16;
const read_ts: u64 = 500;

/// Builds a fixture's committed rows under the table's layout.
const RowBuilder = *const fn (
    a: std.mem.Allocator,
    tbl: *const Table,
) anyerror![]const Item;

/// A Forest in a tmpdir with one tree for (ns, tid) and the built rows,
/// the lock map and oracle a Txn needs, and a hand-built table
/// (a int4, b text) whose layout the rows were marshalled under.
const Fixture = struct {
    tmp: std.testing.TmpDir,
    dir_path: [:0]u8,
    forest: *Forest,
    /// The forest reads its options through this; freed after it.
    live: *flags.Live(Config),
    /// The forest's SSTs; heap-held so its address survives the move out
    /// of setup. Freed after the forest.
    store: *zdb.vfs.RemoteStore,
    db: sql.database.Database,
    arena: std.heap.ArenaAllocator,
    tbl: Table,

    fn setup(build: RowBuilder) !Fixture {
        return setupWith(build, false);
    }

    /// `setup`, with the rows flushed to an L0 SST rather than left in
    /// the memtable.
    fn setupOnDisk(build: RowBuilder) !Fixture {
        return setupWith(build, true);
    }

    fn setupWith(build: RowBuilder, on_disk: bool) !Fixture {
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        errdefer arena.deinit();
        const a = arena.allocator();

        const logical = [_]sql.schema.LogicalColumn{
            .{ .column_id = 1, .name = "a", .ty = .int4 },
            .{ .column_id = 2, .name = "b", .ty = .text },
        };
        const ps = try sql.schema.compile(
            a,
            .{ .columns = &logical, .current_version = 1 },
            1,
        );
        const columns = try a.alloc(sql.table.Column, 2);
        columns[0] = .{
            .column_id = 1,
            .pos = 0,
            .name = "a",
            .ty = .int4,
            .pkey_position = null,
        };
        columns[1] = .{
            .column_id = 2,
            .pos = 1,
            .name = "b",
            .ty = .text,
            .pkey_position = null,
        };
        const tbl = Table{
            .columns = columns,
            .indexes = try a.alloc(sql.table.Index, 0),
            .ps = ps,
            .version = 1,
            .arena = std.heap.ArenaAllocator.init(testing.allocator),
        };

        var tmp = std.testing.tmpDir(.{});
        errdefer tmp.cleanup();
        const dir_path = try tmp.dir.realPathFileAlloc(
            utils.context.g().io,
            ".",
            testing.allocator,
        );
        errdefer testing.allocator.free(dir_path);
        const store = try testing.allocator.create(zdb.vfs.RemoteStore);
        errdefer testing.allocator.destroy(store);
        store.* = try zdb.vfs.RemoteStore.initFile(
            testing.allocator,
            dir_path,
            zdb.vfs.g(),
        );
        errdefer store.deinit();

        const f_live = try flags.Live(Config).create(
            testing.allocator,
            .{},
        );
        errdefer f_live.destroy();
        const f = try Forest.open(testing.allocator, f_live, store);
        errdefer f.close();
        _ = try createTree(f, .{
            .namespace = ns,
            .table_id = tid,
        }, .{}, .posting_index);
        if (on_disk) {
            // The id-0 placeholder memtable is released, never flushed:
            // write into a real one.
            try f.newMemtable(1);
        }
        try f.put(try build(a, &tbl));
        if (on_disk) {
            try f.newMemtable(2);
            const fl = (try f.buildFlush()) orelse
                return error.TestExpectedFlush;
            var changes = fl.changes;
            defer changes.deinit(testing.allocator);
            try f.applyFlush(3, changes.items, fl.newest_memtable_id);
        }

        return .{
            .tmp = tmp,
            .dir_path = dir_path,
            .forest = f,
            .live = f_live,
            .store = store,
            .db = try sql.database.Database.init(testing.allocator, ns),
            .arena = arena,
            .tbl = tbl,
        };
    }

    fn teardown(self: *Fixture) void {
        self.tbl.deinit();
        self.db.deinit();
        self.forest.close();
        self.live.destroy();
        self.store.deinit();
        testing.allocator.destroy(self.store);
        testing.allocator.free(self.dir_path);
        self.tmp.cleanup();
        self.arena.deinit();
    }

    fn txn(self: *Fixture) Txn {
        return Txn.init(
            testing.allocator,
            1,
            read_ts,
            1,
            &self.db,
            self.forest,
        );
    }

    fn ctx(self: *Fixture, tx: *Txn, lock: bool) exec.ExecCtx {
        return .{
            .arloc = self.arena.allocator(),
            .tx = tx,
            .tbl = &self.tbl,
            .table_id = tid,
            .lock = lock,
        };
    }

    fn col(self: *const Fixture, name: String) *const sql.table.Column {
        return self.tbl.column(name).?;
    }
};

/// A stored row: the egid in the key, (a, b) marshalled under `tbl`.
fn row(
    a: std.mem.Allocator,
    tbl: *const Table,
    egid: u64,
    a_val: i32,
    b_val: String,
) !Item {
    var buf: std.ArrayList(u8) = .empty;
    const cells = [_]Value{ .{ .int4 = a_val }, .{ .text = b_val } };
    const bytes = try sql.row.marshalRow(a, &tbl.ps, &cells, &buf);
    return .{
        .key = .{
            .namespace = ns,
            .table_id = tid,
            .primary = try a.dupe(u8, &sql.key.encodeEgid(egid)),
        },
        .versions = .{ .one = .{ .ts = 100, .meta = .{}, .value = bytes } },
    };
}

fn noRows(_: std.mem.Allocator, _: *const Table) anyerror![]const Item {
    return &.{};
}

/// 2500 rows: a = egid, b alternates "x" (even egid) / "y".
fn alternatingRows(
    a: std.mem.Allocator,
    tbl: *const Table,
) anyerror![]const Item {
    const items = try a.alloc(Item, 2500);
    for (items, 1..) |*it, egid| {
        const b_val: String = if (egid % 2 == 0) "x" else "y";
        it.* = try row(a, tbl, egid, @intCast(egid), b_val);
    }
    return items;
}

/// 400 rows with 1 KiB text — a batch's byte cap fills before its row cap.
fn wideRows(a: std.mem.Allocator, tbl: *const Table) anyerror![]const Item {
    const wide = try a.alloc(u8, 1024);
    @memset(wide, 'w');
    const items = try a.alloc(Item, 400);
    for (items, 1..) |*it, egid| {
        it.* = try row(a, tbl, egid, @intCast(egid), wide);
    }
    return items;
}

/// A one-version row whose value is the caller's storage, pinned.
fn tinyItem(primary: *const [8]u8, value: String) Item {
    return .{
        .key = .{
            .namespace = ns,
            .table_id = tid,
            .primary = primary,
        },
        .versions = .{ .one = .{ .ts = 1, .meta = .{}, .value = value } },
    };
}

fn firstEgid(b: *const Batch) u64 {
    return sql.key.decodeEgid(b.rows.items[0].key.primary).?;
}

/// Does `slice` lie inside the batch's own byte buffer?
fn withinBytes(b: *const Batch, slice: String) bool {
    const start = @intFromPtr(b.bytes.items.ptr);
    const end = start + b.bytes.capacity;
    const p = @intFromPtr(slice.ptr);
    return p >= start and p < end;
}

test "sql.exec: Batch row cap, byte cap, growth only when empty" {
    var b = try Batch.init(testing.allocator);
    defer b.deinit();
    const cap0 = b.bytes.capacity;
    try testing.expectEqual(exec.byte_cap, cap0);

    // Row cap: 1024 tiny rows in, the 1025th refused, every slice resolves.
    var keys: [exec.row_cap][8]u8 = undefined;
    for (&keys, 1..) |*k, egid| k.* = sql.key.encodeEgid(egid);
    for (&keys) |*k| {
        try testing.expect(try b.append(tinyItem(k, "v"), .copy));
    }
    try testing.expect(b.full());
    try testing.expect(!try b.append(tinyItem(&keys[0], "v"), .copy));
    try testing.expectEqual(@as(usize, exec.row_cap), b.sel.count());
    try testing.expectEqual(cap0, b.bytes.capacity);
    var n: usize = 0;
    var it = b.selected();
    while (it.next()) |r| : (n += 1) {
        try testing.expectEqual(n + 1, sql.key.decodeEgid(r.key.primary).?);
        try testing.expectEqualStrings("v", r.newest().value);
    }
    try testing.expectEqual(@as(usize, exec.row_cap), n);

    // Byte cap: 1 KiB values fill the bytes long before 1024 rows; the
    // first row that does not fit is refused and the capacity is untouched.
    b.reset();
    try testing.expectEqual(@as(usize, 0), b.sel.count());
    const big = [_]u8{0xAB} ** 1024;
    n = 0;
    try testing.expect(!b.full());
    while (try b.append(tinyItem(&keys[n], &big), .copy)) n += 1;
    try testing.expectEqual(exec.byte_cap / (8 + big.len), n);
    try testing.expectEqual(n, b.numRows());
    try testing.expectEqual(cap0, b.bytes.capacity);
    // A refusal for bytes closes the fill: full() agrees with append()
    // even though the row limit is far away, and a tiny row that would
    // fit is refused too. reset() reopens.
    try testing.expect(b.full());
    try testing.expect(!try b.append(tinyItem(&keys[0], "v"), .copy));
    b.reset();
    try testing.expect(!b.full());

    // .none copies the key only; .borrow copies the key only too and
    // keeps the value slice as given — the caller's storage, pinned.
    try testing.expect(try b.append(tinyItem(&keys[0], &big), .none));
    try testing.expectEqual(@as(usize, 8), b.bytes.items.len);
    try testing.expectEqualStrings("", b.rows.items[0].newest().value);
    b.reset();
    try testing.expect(try b.append(tinyItem(&keys[0], &big), .borrow));
    try testing.expectEqual(@as(usize, 8), b.bytes.items.len);
    const kept = b.rows.items[0].newest().value;
    try testing.expectEqual(@as([*]const u8, &big), kept.ptr);
    try testing.expectEqual(big.len, kept.len);

    // One oversized row is admitted into an EMPTY batch by growing.
    b.reset();
    const huge = try testing.allocator.alloc(u8, exec.byte_cap + 1);
    defer testing.allocator.free(huge);
    @memset(huge, 1);
    try testing.expect(try b.append(tinyItem(&keys[0], huge), .copy));
    try testing.expectEqual(@as(usize, 1), b.numRows());
    try testing.expect(b.bytes.capacity > cap0);
    try testing.expectEqualSlices(u8, huge, b.rows.items[0].newest().value);
}

test "sql.exec: BitmapNode and_/or_/egids algebra" {
    var fx = try Fixture.setup(&noRows);
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var ctx = fx.ctx(&tx, false);

    const leaf = struct {
        fn make(vals: []const u64) !BitmapNode {
            var bm = try zroar.Bitmap.init(testing.allocator);
            errdefer bm.deinit();
            for (vals) |v| _ = try bm.set(v);
            return .{ .egids = bm };
        }
    }.make;

    var and_kids = [_]BitmapNode{
        try leaf(&.{ 1, 2, 3 }),
        try leaf(&.{ 2, 3, 4 }),
    };
    var and_node = BitmapNode{ .and_ = &and_kids };
    defer and_node.deinit();
    try testing.expect(!and_node.fromIndex());
    var got = try and_node.eval(&ctx);
    defer got.deinit();
    try testing.expectEqual(@as(u64, 2), got.getCardinality());
    try testing.expect(got.contains(2) and got.contains(3));

    var or_kids = [_]BitmapNode{
        try leaf(&.{ 1, 2, 3 }),
        try leaf(&.{ 2, 3, 4 }),
        try leaf(&.{9}),
    };
    var or_node = BitmapNode{ .or_ = &or_kids };
    defer or_node.deinit();
    var got_or = try or_node.eval(&ctx);
    defer got_or.deinit();
    try testing.expectEqual(@as(u64, 5), got_or.getCardinality());
    try testing.expect(got_or.contains(1));
    try testing.expect(got_or.contains(4) and got_or.contains(9));

    // AND with an empty leaf is empty; the leaf's own bitmap is untouched
    // (eval hands out clones).
    var empty_kids = [_]BitmapNode{ try leaf(&.{}), try leaf(&.{ 1, 2 }) };
    var empty_and = BitmapNode{ .and_ = &empty_kids };
    defer empty_and.deinit();
    var got_empty = try empty_and.eval(&ctx);
    defer got_empty.deinit();
    try testing.expect(got_empty.isEmpty());
    try testing.expectEqual(@as(u64, 2), empty_kids[1].egids.getCardinality());
}

test "sql.exec: seq_scan: full ascending batches; pred skips without copying" {
    var fx = try Fixture.setup(&alternatingRows);
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var ctx = fx.ctx(&tx, false);
    var b = try Batch.init(testing.allocator);
    defer b.deinit();

    // No predicate: 2500 rows in 3 batches, all full but the last.
    {
        var node = RowNode{ .seq_scan = .{} };
        defer node.deinit();
        var batches: usize = 0;
        var total: usize = 0;
        var last: u64 = 0;
        while (try node.next(&ctx, &b)) : (batches += 1) {
            if (batches < 2) try testing.expect(b.full());
            try testing.expectEqual(b.numRows(), b.sel.count());
            var it = b.selected();
            while (it.next()) |r| : (total += 1) {
                const egid = sql.key.decodeEgid(r.key.primary).?;
                try testing.expect(egid > last);
                last = egid;
                const a_val = fx.tbl.parseCol(r, fx.col("a"));
                try testing.expectEqual(Value{ .int4 = @intCast(egid) }, a_val);
            }
        }
        try testing.expectEqual(@as(usize, 3), batches);
        try testing.expectEqual(@as(usize, 2500), total);
        try testing.expect(!try node.next(&ctx, &b)); // sticky
    }

    // b = 'x': half the rows. The batch holds the KEYS of matches only;
    // values are borrows into storage the cursor pins, never copied.
    {
        const pred = Pred{
            .cmp = .{ .col = fx.col("b"), .val = .{ .text = "x" } },
        };
        var node = RowNode{ .seq_scan = .{ .pred = &pred } };
        defer node.deinit();
        var total: usize = 0;
        while (try node.next(&ctx, &b)) {
            try testing.expectEqual(b.numRows() * 8, b.bytes.items.len);
            var it = b.selected();
            while (it.next()) |r| : (total += 1) {
                try testing.expect(!withinBytes(&b, r.newest().value));
                const b_val = fx.tbl.parseCol(r, fx.col("b"));
                try testing.expectEqualStrings("x", b_val.text);
            }
        }
        try testing.expectEqual(@as(usize, 1250), total);
    }
}

test "sql.exec: seq_scan: borrowed values outlive the blocks the walk left" {
    var fx = try Fixture.setupOnDisk(&wideRows);
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var ctx = fx.ctx(&tx, false);
    var b = try Batch.init(testing.allocator);
    defer b.deinit();

    // 400 rows of 1 KiB across several 16 KiB SST blocks, all in ONE
    // batch: only keys land in `bytes`; values are borrows into blocks
    // the cursor pinned — and every one still reads after the walk
    // moved past its block.
    var node = RowNode{ .seq_scan = .{} };
    defer node.deinit();
    try testing.expect(try node.next(&ctx, &b));
    try testing.expectEqual(@as(usize, 400), b.numRows());
    try testing.expectEqual(@as(usize, 400 * 8), b.bytes.items.len);
    const pins = &node.seq_scan.cursor.?.pins;
    try testing.expect(pins.items.len >= 2);
    var it = b.selected();
    while (it.next()) |r| {
        try testing.expect(!withinBytes(&b, r.newest().value));
        const egid = sql.key.decodeEgid(r.key.primary).?;
        const a_val = fx.tbl.parseCol(r, fx.col("a"));
        try testing.expectEqual(Value{ .int4 = @intCast(egid) }, a_val);
        const b_val = fx.tbl.parseCol(r, fx.col("b"));
        try testing.expectEqual(@as(usize, 1024), b_val.text.len);
        try testing.expectEqual(@as(u8, 'w'), b_val.text[1023]);
    }
    // The next fill lets them go; the walk is over.
    try testing.expect(!try node.next(&ctx, &b));
    try testing.expectEqual(@as(usize, 0), pins.items.len);
}

test "sql.exec: row_limit bounds a fill — a LIMIT pulls only what it needs" {
    var fx = try Fixture.setup(&alternatingRows);
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var ctx = fx.ctx(&tx, false);
    var b = try Batch.init(testing.allocator);
    defer b.deinit();
    b.row_limit = 10;

    var node = RowNode{ .seq_scan = .{} };
    defer node.deinit();
    try testing.expect(try node.next(&ctx, &b));
    try testing.expectEqual(@as(usize, 10), b.numRows());
    try testing.expect(b.full());
    // The scan stopped AT the limit — nothing was pulled past row 10.
    try testing.expect(try node.next(&ctx, &b));
    try testing.expectEqual(@as(u64, 11), firstEgid(&b));
    // Raising the limit takes effect on the next fill.
    b.row_limit = exec.row_cap;
    try testing.expect(try node.next(&ctx, &b));
    try testing.expectEqual(@as(usize, exec.row_cap), b.numRows());
    try testing.expectEqual(@as(u64, 21), firstEgid(&b));
}

test "sql.exec: fetch: no row locks of its own, held row across batches" {
    var fx = try Fixture.setup(&wideRows);
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var ctx = fx.ctx(&tx, true);
    var b = try Batch.init(testing.allocator);
    defer b.deinit();

    // egids 1..300 plus one that does not exist — a literal id, so it
    // is skipped quietly.
    var bm = try zroar.Bitmap.init(testing.allocator);
    for (1..301) |egid| _ = try bm.set(egid);
    _ = try bm.set(9_999);
    var leaf = BitmapNode{ .egids = bm };

    // The fetch owns its bitmap tree: one deinit frees the leaf too.
    var node = RowNode{ .fetch = .{ .bnode = &leaf } };
    defer node.deinit();
    try testing.expectEqual(@as(usize, 0), tx.held_rows.items.len);
    var batches: usize = 0;
    var total: usize = 0;
    var last: u64 = 0;
    while (try node.next(&ctx, &b)) : (batches += 1) {
        // 1 KiB rows, copied: the byte cap ends a batch well before
        // 300 rows.
        try testing.expect(b.numRows() < 300);
        var it = b.selected();
        while (it.next()) |r| : (total += 1) {
            const egid = sql.key.decodeEgid(r.key.primary).?;
            try testing.expect(egid > last);
            last = egid;
        }
    }
    try testing.expectEqual(@as(usize, 300), total);
    try testing.expect(batches >= 2);
    // Row locks are Materialize's, on the output: a bare fetch under a
    // locking ctx takes none.
    try testing.expectEqual(@as(usize, 0), tx.held_rows.items.len);
}

test "sql.exec: fetch: a bitmap wider than one chunk, with a residual" {
    var fx = try Fixture.setup(&alternatingRows);
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var ctx = fx.ctx(&tx, false);
    var b = try Batch.init(testing.allocator);
    defer b.deinit();

    // Every egid, so the fetch reads three chunks (1024 + 1024 + 452),
    // and a residual that keeps the odd ones.
    var bm = try zroar.Bitmap.init(testing.allocator);
    for (1..2501) |egid| _ = try bm.set(egid);
    var leaf = BitmapNode{ .egids = bm };
    const pred = Pred{
        .cmp = .{ .col = fx.col("b"), .val = .{ .text = "y" } },
    };
    var node = RowNode{ .fetch = .{ .bnode = &leaf, .pred = &pred } };
    defer node.deinit();

    var total: usize = 0;
    var last: u64 = 0;
    while (try node.next(&ctx, &b)) {
        var it = b.selected();
        while (it.next()) |r| : (total += 1) {
            const egid = sql.key.decodeEgid(r.key.primary).?;
            try testing.expect(egid > last);
            try testing.expect(egid % 2 == 1);
            last = egid;
        }
    }
    try testing.expectEqual(@as(usize, 1250), total);
    try testing.expectEqual(@as(u64, 2499), last);
    // Every key of the bitmap was recorded as a read.
    try testing.expectEqual(@as(usize, 2500), tx.cur.reads.items.len);
}

test "sql.exec: filter clears bits and copies nothing" {
    var fx = try Fixture.setup(&alternatingRows);
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var ctx = fx.ctx(&tx, false);
    var b = try Batch.init(testing.allocator);
    defer b.deinit();

    const pred = Pred{
        .cmp = .{ .col = fx.col("b"), .val = .{ .text = "y" } },
    };
    var scan = RowNode{ .seq_scan = .{} };
    var node = RowNode{ .filter = .{ .child = &scan, .pred = &pred } };
    defer node.deinit();
    var total: usize = 0;
    while (try node.next(&ctx, &b)) {
        // Every scanned row is still in the batch; only the bits changed.
        try testing.expectEqual(b.numRows() * 8, b.bytes.items.len);
        try testing.expect(b.sel.count() < b.numRows());
        var it = b.selected();
        while (it.next()) |r| : (total += 1) {
            const b_val = fx.tbl.parseCol(r, fx.col("b"));
            try testing.expectEqualStrings("y", b_val.text);
        }
    }
    try testing.expectEqual(@as(usize, 1250), total);
}

test "sql.exec: materialize: ascending arena copies, locks held, streams back" {
    var fx = try Fixture.setup(&alternatingRows);
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var ctx = fx.ctx(&tx, true);
    var b = try Batch.init(testing.allocator);
    defer b.deinit();

    const pred = Pred{
        .cmp = .{ .col = fx.col("b"), .val = .{ .text = "x" } },
    };
    var scan = RowNode{ .seq_scan = .{ .pred = &pred } };
    var node = RowNode{ .materialize = .{ .child = &scan } };
    defer node.deinit();

    const rows = try node.materialize.gatherAndLockRows(&ctx, &b);
    try testing.expectEqual(@as(usize, 1250), rows.len);
    try testing.expectEqual(@as(usize, 1250), tx.held_rows.items.len);
    var last: u64 = 0;
    for (rows) |r| {
        const egid = sql.key.decodeEgid(r.key.primary).?;
        try testing.expect(egid > last);
        last = egid;
        const a_val = fx.tbl.parseCol(r, fx.col("a"));
        try testing.expectEqual(Value{ .int4 = @intCast(egid) }, a_val);
    }
    // The copies outlive the batch: refill it and the rows still read.
    b.reset();
    const first = fx.tbl.parseCol(rows[0], fx.col("a"));
    try testing.expectEqual(Value{ .int4 = 2 }, first);

    // Second call is idempotent; next() streams the capture in batches
    // by reference: the batch's bytes stay empty, rows point into the
    // arena.
    const again = try node.materialize.gatherAndLockRows(&ctx, &b);
    try testing.expectEqual(rows.ptr, again.ptr);
    var total: usize = 0;
    var batches: usize = 0;
    while (try node.next(&ctx, &b)) : (batches += 1) {
        try testing.expectEqual(@as(usize, 0), b.bytes.items.len);
        var it = b.selected();
        while (it.next()) |r| : (total += 1) {
            const want = rows[total].key.primary.ptr;
            try testing.expectEqual(want, r.key.primary.ptr);
        }
    }
    try testing.expectEqual(@as(usize, 1250), total);
    try testing.expectEqual(@as(usize, 2), batches);
}

test "sql.exec: materialize over a fetch locks the output rows only" {
    var fx = try Fixture.setup(&alternatingRows);
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var ctx = fx.ctx(&tx, true);
    var b = try Batch.init(testing.allocator);
    defer b.deinit();

    // egids 1..300 plus an absent one; the residual keeps the even
    // ones. Locks land on the 150 rows that come OUT — not on the odd
    // rows the residual dropped, not on the absent id.
    var bm = try zroar.Bitmap.init(testing.allocator);
    for (1..301) |egid| _ = try bm.set(egid);
    _ = try bm.set(9_999);
    var leaf = BitmapNode{ .egids = bm };
    const pred = Pred{
        .cmp = .{ .col = fx.col("b"), .val = .{ .text = "x" } },
    };
    var fetch = RowNode{ .fetch = .{ .bnode = &leaf, .pred = &pred } };
    var node = RowNode{ .materialize = .{ .child = &fetch } };
    defer node.deinit();

    const rows = try node.materialize.gatherAndLockRows(&ctx, &b);
    try testing.expectEqual(@as(usize, 150), rows.len);
    try testing.expectEqual(@as(usize, 150), tx.held_rows.items.len);
    for (rows) |r| {
        const egid = sql.key.decodeEgid(r.key.primary).?;
        try testing.expect(egid % 2 == 0);
    }
}

test "sql.exec: materialize without lock streams the child, bounded" {
    var fx = try Fixture.setup(&alternatingRows);
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var ctx = fx.ctx(&tx, false);
    var b = try Batch.init(testing.allocator);
    defer b.deinit();

    // LIMIT 1500 across two fills: a full batch, then exactly the rest
    // — the source is never asked past row 1500.
    var scan = RowNode{ .seq_scan = .{} };
    var capped = RowNode{ .materialize = .{
        .child = &scan,
        .remaining = 1500,
    } };
    defer capped.deinit();
    try testing.expect(try capped.next(&ctx, &b));
    try testing.expectEqual(@as(usize, exec.row_cap), b.numRows());
    try testing.expect(try capped.next(&ctx, &b));
    try testing.expectEqual(@as(usize, 476), b.numRows());
    try testing.expectEqual(@as(u64, 1025), firstEgid(&b));
    try testing.expect(!try capped.next(&ctx, &b));
    try testing.expectEqual(@as(usize, 0), b.numRows());

    // LIMIT 0 never touches the child: no cursor, no scan recorded.
    var scan2 = RowNode{ .seq_scan = .{} };
    var none = RowNode{ .materialize = .{ .child = &scan2, .remaining = 0 } };
    defer none.deinit();
    try testing.expect(!try none.next(&ctx, &b));
    try testing.expect(scan2.seq_scan.cursor == null);

    // Unbounded, on the same batch the LIMIT left at 476: the root
    // sets the bound before every fill, so the batches are full again
    // and the scan's rows pass straight through — keys in the batch's
    // bytes, nothing gathered, nothing locked.
    var scan3 = RowNode{ .seq_scan = .{} };
    var node = RowNode{ .materialize = .{ .child = &scan3 } };
    defer node.deinit();
    var total: usize = 0;
    var batches: usize = 0;
    while (try node.next(&ctx, &b)) : (batches += 1) {
        try testing.expect(b.bytes.items.len > 0);
        if (batches < 2)
            try testing.expectEqual(@as(usize, exec.row_cap), b.numRows());
        total += b.sel.count();
    }
    try testing.expectEqual(@as(usize, 2500), total);
    try testing.expectEqual(@as(usize, 3), batches);
    try testing.expectEqual(@as(usize, 0), tx.held_rows.items.len);
    try testing.expect(node.materialize.rows == null);
}

test "sql.exec: materialize with lock and a limit locks the limited rows" {
    var fx = try Fixture.setup(&alternatingRows);
    defer fx.teardown();
    var tx = fx.txn();
    defer tx.deinit();
    var ctx = fx.ctx(&tx, true);
    var b = try Batch.init(testing.allocator);
    defer b.deinit();

    // FOR UPDATE ... LIMIT 100 over the 1250 matches: the gather stops
    // at 100, so 100 rows are locked — the first 100 even egids — and
    // next() streams exactly those back.
    const pred = Pred{
        .cmp = .{ .col = fx.col("b"), .val = .{ .text = "x" } },
    };
    var scan = RowNode{ .seq_scan = .{ .pred = &pred } };
    var node = RowNode{ .materialize = .{ .child = &scan, .remaining = 100 } };
    defer node.deinit();

    const rows = try node.materialize.gatherAndLockRows(&ctx, &b);
    try testing.expectEqual(@as(usize, 100), rows.len);
    try testing.expectEqual(@as(usize, 100), tx.held_rows.items.len);
    for (rows, 1..) |r, i| {
        const egid = sql.key.decodeEgid(r.key.primary).?;
        try testing.expectEqual(@as(u64, 2 * i), egid);
    }
    try testing.expect(try node.next(&ctx, &b));
    try testing.expectEqual(@as(usize, 100), b.sel.count());
    try testing.expect(!try node.next(&ctx, &b));
}
