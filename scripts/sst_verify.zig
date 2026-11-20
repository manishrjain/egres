// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const zdb = @import("zdb");
const utils = @import("utils");
const String = utils.String;
const vfs = zdb.vfs;
const Table = zdb.table.Table;
const TableIterator = zdb.table.TableIterator;
const Key = zdb.kv.Key;

/// Returns true on success, false on any failure. Prints the result line
/// directly so we don't need to retain pointers into the table after it's
/// dropped (smallest/biggest live in the table's index memory).
/// The file id an SST's name encodes (`<id:016x>.sst`, see
/// `zdb.sstName`). 0 when the name does not follow that layout — this
/// tool only reports the id, so a stray name is not an error.
fn idFromName(basename: String) u64 {
    if (!std.mem.endsWith(u8, basename, ".sst")) return 0;
    const stem = basename[0 .. basename.len - 4];
    if (stem.len != 16) return 0;
    return std.fmt.parseInt(u64, stem, 16) catch 0;
}
/// The extent of the table whose last byte is at `end` of the file.
/// A table ends in its trailer ([u32 index_len][u64 hash]), its index
/// has every block's offset and length, and its size is its blocks,
/// index and trailer padded to a page (`Builder.finish`), so its start
/// is `end` less that. A flush packs many trees' tables into one file,
/// each whole, so a walk from the file's end down to 0 visits them
/// all. This tool has no manifest to ask.
fn probeExtent(
    allocator: std.mem.Allocator,
    obj: *vfs.Object,
    end: u64,
) !Table.Extent {
    const trailer_len = zdb.table.index.trailer_len;
    if (end < trailer_len) return error.CorruptTable;
    const trailer_at = end - trailer_len;
    const res = try obj.readAligned(allocator, trailer_at, trailer_len);
    defer res.free();
    if (res.data.len < trailer_len) return error.CorruptTable;
    const index_len = std.mem.readInt(u32, res.data[0..4], .little);
    if (index_len > trailer_at) return error.CorruptTable;

    const index_at = trailer_at - index_len;
    const idx = try obj.readAligned(allocator, index_at, index_len);
    defer idx.free();
    if (idx.data.len < index_len) return error.CorruptTable;
    const reader = try zdb.buffers.TableIndexReader.init(
        idx.data[0..index_len],
        0,
    );
    var index = try reader.toStruct(allocator);
    defer index.destroy(allocator);
    var blocks_end: u64 = 0;
    for (index.blocks) |b| {
        blocks_end = @max(blocks_end, @as(u64, b.offset) + b.len);
    }
    const page = vfs.page_size;
    const unpadded = blocks_end + index_len + trailer_len;
    const size = (unpadded + page - 1) / page * page;
    if (size > end) return error.CorruptTable;
    return .{
        .offset = end - size,
        .size = size,
        .index_offset = size - index_len - trailer_len,
    };
}

/// Every table of one file, from its end down: true when all verify.
fn verifyOne(
    allocator: std.mem.Allocator,
    store: *vfs.Store,
    base: String,
) bool {
    const fid = idFromName(base);
    const file_size = (store.stat(base) catch |err| {
        std.debug.print("FAIL {s} stat={s}\n", .{ base, @errorName(err) });
        return false;
    }).size;
    const file = Table.openObject(allocator, store, fid) catch |err| {
        std.debug.print("FAIL {s} open={s}\n", .{ base, @errorName(err) });
        return false;
    };
    defer file.decrRef();

    var tables: u64 = 0;
    var keys: u64 = 0;
    var end = file_size;
    while (end > 0) : (tables += 1) {
        const extent = probeExtent(allocator, file, end) catch |err| {
            std.debug.print("FAIL {s} end={d} probe={s}\n", .{
                base, end, @errorName(err),
            });
            return false;
        };
        file.incrRef();
        const t = Table.openTable(allocator, file, extent) catch |err| {
            std.debug.print("FAIL {s} offset={d} open={s}\n", .{
                base, extent.offset, @errorName(err),
            });
            return false;
        };
        defer t.decrRef();
        t.file_id = fid;
        if (!verifyTable(allocator, base, file_size, fid, t, &keys)) {
            return false;
        }
        end = extent.offset;
    }
    std.debug.print("OK   {s} size={d} fid={d} tables={d} keys={d}\n", .{
        base, file_size, fid, tables, keys,
    });
    return true;
}

/// One table: keys ascend strictly, one entry each, versions descend.
/// Adds its keys to `total`.
fn verifyTable(
    allocator: std.mem.Allocator,
    base: String,
    file_size: u64,
    fid: u64,
    t: *Table,
    total: *u64,
) bool {
    var prev_buf: std.ArrayList(u8) = .empty;
    defer prev_buf.deinit(allocator);
    var keys: u64 = 0;
    var have_prev = false;

    // The file carries no namespace or table_id; a standalone check
    // stamps 0 and 0, and only the primaries are looked at.
    var it = TableIterator.init(
        allocator,
        t,
        .{ .namespace = 0, .table_id = 0 },
        .forward,
    );
    defer it.deinit();
    it.seekRange(Key.inf, Key.inf) catch |err| {
        std.debug.print("FAIL {s} size={d} fid={d} seek={s}\n", .{ base, file_size, fid, @errorName(err) });
        return false;
    };

    while (it.itemRef()) |item| {
        // Entries ascend strictly by key, one entry per key.
        if (have_prev) {
            const prev = Key.init(prev_buf.items);
            if (item.key.order(prev) != .gt) {
                std.debug.print("FAIL {s} fid={d} bad_order_at={d}\n", .{ base, fid, keys });
                return false;
            }
        }
        // An entry's versions descend strictly in timestamp.
        const vers = item.versions.slice();
        var vi: usize = 1;
        while (vi < vers.len) : (vi += 1) {
            if (vers[vi].ts >= vers[vi - 1].ts) {
                std.debug.print(
                    "FAIL {s} fid={d} bad_version_order_at={d}\n",
                    .{ base, fid, keys },
                );
                return false;
            }
        }
        prev_buf.clearRetainingCapacity();
        _ = item.key.marshalTo(allocator, &prev_buf) catch |err| {
            std.debug.print("FAIL {s} fid={d} marshal={s}\n", .{ base, fid, @errorName(err) });
            return false;
        };
        have_prev = true;
        keys += 1;
        it.next() catch |err| {
            std.debug.print("FAIL {s} fid={d} keys={d} next={s}\n", .{ base, fid, keys, @errorName(err) });
            return false;
        };
    }
    total.* += keys;
    return true;
}

pub fn main(init: std.process.Init) !void {
    utils.context.initGlobal(init);
    const allocator = utils.context.g().gpa;
    const io = utils.context.g().io;
    const argv = try utils.context.g().argv();

    const dir_path = if (argv.len >= 2) argv[1] else {
        std.debug.print("usage: sst-verify <dir>\n", .{});
        std.process.exit(2);
    };

    // Same defaults the server uses; the verifier only reads, but the global
    // VFS still needs a bandwidth controller.
    vfs.VFS.initGlobal(.{ .bytes_per_sec = 500 * 1024 * 1024 });
    // The dir as a store, so tables open the way the server opens them.
    var store = try vfs.Store.initFile(allocator, dir_path, vfs.g());
    defer store.deinit();

    var dir = try std.Io.Dir.cwd().openDir(io, dir_path, .{ .iterate = true });
    defer dir.close(io);

    var names: std.ArrayList(String) = .empty;
    defer {
        for (names.items) |n| allocator.free(n);
        names.deinit(allocator);
    }
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".sst")) continue;
        try names.append(allocator, try allocator.dupe(u8, entry.name));
    }
    std.mem.sort(String, names.items, {}, struct {
        fn lt(_: void, a: String, b: String) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);

    var ok: u64 = 0;
    var bad: u64 = 0;
    for (names.items) |n| {
        if (verifyOne(allocator, &store, n)) ok += 1 else bad += 1;
    }
    std.debug.print("\nsummary: {d} OK, {d} FAIL\n", .{ ok, bad });
    if (bad > 0) std.process.exit(1);
}
