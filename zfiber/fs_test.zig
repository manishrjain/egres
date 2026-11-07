// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");

const fs = fiber.fs;
const String = utils.String;

// ── helpers: the ops as this file uses them ──

fn create(path: String) !fiber.File {
    return fs.submit(.open, .{
        .path = path,
        .flags = .{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true },
        .mode = 0o644,
    });
}

fn openRead(path: String) !fiber.File {
    return fs.submit(.open, .{
        .path = path,
        .flags = .{ .ACCMODE = .RDONLY },
        .mode = 0,
    });
}

fn close(file: fiber.File) void {
    fs.submit(.close, file);
}

fn del(path: String) void {
    fs.submit(.delete_file, path) catch {};
}

fn sync(file: fiber.File) !void {
    return fs.submit(.sync, .{ .file = file, .data_only = false });
}

fn size(file: fiber.File) !u64 {
    return (try fs.submit(.stat, file)).size;
}

fn write(file: fiber.File, data: String, offset: u64) !usize {
    return fs.submit(.write, .{ .file = file, .data = data, .offset = offset });
}

fn read(file: fiber.File, buf: []u8, offset: u64) !usize {
    return fs.submit(.read, .{ .file = file, .buf = buf, .offset = offset });
}

/// Single ops can be short: loop to the end.
fn writeAllAt(file: fiber.File, data: String, offset: u64) !void {
    var off: usize = 0;
    while (off < data.len) {
        const n = try write(file, data[off..], offset + off);
        if (n == 0) return error.WriteFailed;
        off += n;
    }
}

fn readAllAt(file: fiber.File, buf: []u8, offset: u64) !usize {
    var off: usize = 0;
    while (off < buf.len) {
        const n = try read(file, buf[off..], offset + off);
        if (n == 0) break; // EOF
        off += n;
    }
    return off;
}

test "fs: write, sync, stat, read back at offsets" {
    const path = "/tmp/fiber_fs_test_rw";
    del(path);
    const file = try create(path);
    defer {
        close(file);
        del(path);
    }

    const data = "hello fiber file io";
    const written = try write(file, data, 0);
    try std.testing.expectEqual(@as(usize, data.len), written);

    try sync(file);

    const st = try fs.submit(.stat, file);
    try std.testing.expectEqual(@as(u64, data.len), st.size);

    var buf: [64]u8 = undefined;
    const n = try read(file, buf[0..data.len], 0);
    try std.testing.expectEqual(@as(usize, data.len), n);
    try std.testing.expectEqualSlices(u8, data, buf[0..n]);

    // Positional read mid-file: bytes 6..11 are "fiber".
    const m = try read(file, buf[0..5], 6);
    try std.testing.expectEqualStrings("fiber", buf[0..m]);
}

test "fs: set_size truncates" {
    const path = "/tmp/fiber_fs_test_trunc";
    del(path);
    const file = try create(path);
    defer {
        close(file);
        del(path);
    }

    _ = try write(file, "0123456789", 0);
    try fs.submit(.set_size, .{ .file = file, .length = 4 });
    try std.testing.expectEqual(@as(u64, 4), try size(file));
}

test "fs: read_batch reads offsets across two files in one park" {
    const path = "/tmp/fiber_fs_test_batch";
    const path2 = "/tmp/fiber_fs_test_batch2";
    del(path);
    del(path2);
    const file = try create(path);
    const file2 = try create(path2);
    defer {
        close(file);
        close(file2);
        del(path);
        del(path2);
    }

    // 256 bytes where byte[i] == i; the second file holds 255 - i.
    var content: [256]u8 = undefined;
    for (&content, 0..) |*b, i| b.* = @intCast(i);
    _ = try write(file, &content, 0);
    try sync(file);
    for (&content, 0..) |*b, i| b.* = @intCast(255 - i);
    _ = try write(file2, &content, 0);
    try sync(file2);

    var b0: [16]u8 = undefined;
    var b1: [16]u8 = undefined;
    var b2: [16]u8 = undefined;
    var b3: [16]u8 = undefined;
    var ops = [_]fs.BatchOp{
        .{ .file = file, .buf = &b0, .offset = 0 },
        .{ .file = file, .buf = &b1, .offset = 100 },
        .{ .file = file2, .buf = &b2, .offset = 100 },
        .{ .file = file, .buf = &b3, .offset = 240 },
    };
    try fs.submit(.read_batch, .{
        .alloc = std.testing.allocator,
        .ops = &ops,
    });

    for (ops) |op| try std.testing.expectEqual(@as(usize, 16), try op.result);
    try std.testing.expectEqual(@as(u8, 0), b0[0]);
    try std.testing.expectEqual(@as(u8, 100), b1[0]);
    try std.testing.expectEqual(@as(u8, 155), b2[0]);
    try std.testing.expectEqual(@as(u8, 240), b3[0]);
}

test "fs: open read-only, access, delete_file" {
    const path = "/tmp/fiber_fs_test_dir";
    del(path);

    {
        const w = try create(path);
        _ = try write(w, "payload", 0);
        try sync(w);
        close(w);
    }

    try fs.submit(.access, path);

    const r = try openRead(path);
    var buf: [16]u8 = undefined;
    const n = try read(r, buf[0..7], 0);
    try std.testing.expectEqualStrings("payload", buf[0..n]);
    close(r);

    try fs.submit(.delete_file, path);
    try std.testing.expectError(
        error.FileNotFound,
        fs.submit(.access, path),
    );
}

test "fs: large patterned write/read round-trip (256 KiB)" {
    const path = "/tmp/fiber_fs_test_large";
    del(path);
    const file = try create(path);
    defer {
        close(file);
        del(path);
    }

    const N = 256 * 1024;
    const data = try std.testing.allocator.alloc(u8, N);
    defer std.testing.allocator.free(data);
    for (data, 0..) |*b, i| b.* = @truncate(i *% 2654435761);

    try writeAllAt(file, data, 0);
    try sync(file);
    try std.testing.expectEqual(@as(u64, N), try size(file));

    const back = try std.testing.allocator.alloc(u8, N);
    defer std.testing.allocator.free(back);
    const got = try readAllAt(file, back, 0);
    try std.testing.expectEqual(@as(usize, N), got);
    try std.testing.expect(std.mem.eql(u8, data, back));
}

test "fs: data survives sync + close + reopen" {
    const path = "/tmp/fiber_fs_test_durable";
    del(path);
    defer del(path);

    {
        const w = try create(path);
        try writeAllAt(w, "durable-data", 0);
        try sync(w);
        close(w);
    }

    const r = try openRead(path);
    defer close(r);
    var buf: [12]u8 = undefined;
    const n = try readAllAt(r, &buf, 0);
    try std.testing.expectEqualStrings("durable-data", buf[0..n]);
}

test "fs: set_size extends with zero fill" {
    const path = "/tmp/fiber_fs_test_extend";
    del(path);
    const file = try create(path);
    defer {
        close(file);
        del(path);
    }

    try writeAllAt(file, "abc", 0);
    try fs.submit(.set_size, .{ .file = file, .length = 10 });
    try std.testing.expectEqual(@as(u64, 10), try size(file));

    var buf: [10]u8 = undefined;
    const n = try readAllAt(file, &buf, 0);
    try std.testing.expectEqual(@as(usize, 10), n);
    try std.testing.expectEqualStrings("abc", buf[0..3]);
    for (buf[3..10]) |b| try std.testing.expectEqual(@as(u8, 0), b);
}

test "fs: rename moves a file" {
    const old = "/tmp/fiber_fs_test_rn_old";
    const new = "/tmp/fiber_fs_test_rn_new";
    del(old);
    del(new);
    defer {
        del(old);
        del(new);
    }

    {
        const w = try create(old);
        try writeAllAt(w, "moved", 0);
        try sync(w);
        close(w);
    }

    try fs.submit(.rename, .{ .old_path = old, .new_path = new });
    try std.testing.expectError(error.FileNotFound, fs.submit(.access, old));
    try fs.submit(.access, new);

    const r = try openRead(new);
    defer close(r);
    var buf: [5]u8 = undefined;
    const n = try readAllAt(r, &buf, 0);
    try std.testing.expectEqualStrings("moved", buf[0..n]);
}

test "fs: open on a nonexistent path returns FileNotFound" {
    const path = "/tmp/fiber_fs_test_missing_xyz";
    del(path);
    try std.testing.expectError(error.FileNotFound, openRead(path));
}

test "fs: many fibers run full file lifecycles concurrently" {
    const N = 16;
    const Body = struct {
        fn run(id: usize, ok: *bool) void {
            var pbuf: [64]u8 = undefined;
            const path = std.fmt.bufPrint(
                &pbuf,
                "/tmp/fiber_fs_conc_{d}",
                .{id},
            ) catch return;
            del(path);

            const file = create(path) catch return;
            var data: [1024]u8 = undefined;
            for (&data, 0..) |*b, i| b.* = @truncate(i +% id);

            writeAllAt(file, &data, 0) catch {
                close(file);
                return;
            };
            sync(file) catch {
                close(file);
                return;
            };
            var back: [1024]u8 = undefined;
            const n = readAllAt(file, &back, 0) catch {
                close(file);
                return;
            };
            close(file);
            del(path);

            ok.* = (n == data.len and std.mem.eql(u8, &data, &back));
        }
    };

    var oks = [_]bool{false} ** N;
    var g: fiber.Group(void) = .init(std.testing.allocator);
    defer g.deinit();
    try g.ensureTotalCapacity(N);
    for (&oks, 0..) |*ok, i| try g.spawn(.{}, Body.run, .{ i, ok });
    g.wait();

    for (oks) |ok| try std.testing.expect(ok);
}

test "fs: list reports every entry with its kind" {
    const alloc = std.testing.allocator;
    const root = "/tmp/fiber_fs_test_list";
    fs.submit(.delete_tree, root) catch {};
    try fs.submit(.create_dir_path, root ++ "/sub");
    defer fs.submit(.delete_tree, root) catch {};

    for ([_]String{ root ++ "/a.txt", root ++ "/b.txt" }) |p| {
        const file = try create(p);
        _ = try write(file, "x", 0);
        close(file);
    }

    const entries = try fs.submit(.list, .{ .alloc = alloc, .path = root });
    defer fs.freeEntries(alloc, entries);
    try std.testing.expectEqual(@as(usize, 3), entries.len);

    var files: usize = 0;
    var dirs: usize = 0;
    for (entries) |e| switch (e.kind) {
        .file => {
            files += 1;
            try std.testing.expect(std.mem.endsWith(u8, e.name, ".txt"));
        },
        .directory => {
            dirs += 1;
            try std.testing.expectEqualStrings("sub", e.name);
        },
        else => return error.TestUnexpectedKind,
    };
    try std.testing.expectEqual(@as(usize, 2), files);
    try std.testing.expectEqual(@as(usize, 1), dirs);
}

test "fs: stat reports a fresh file's mtime as now" {
    // Real time on the real disk, virtual time on the simulated one;
    // `now` reads the same clock as the disk stamps with.
    const path = "/tmp/fiber_fs_test_mtime";
    del(path);
    const file = try create(path);
    defer {
        close(file);
        del(path);
    }
    _ = try write(file, "now", 0);

    const now = utils.timestampNow(.realtime, .secs);
    const st = try fs.submit(.stat, file);
    const skew = @abs(now - st.mtime_s);
    try std.testing.expect(skew < 10);
}

test "fs: create_dir_path, directory sync, delete_tree" {
    const root = "/tmp/fiber_fs_test_tree";
    fs.submit(.delete_tree, root) catch {};
    try fs.submit(.create_dir_path, root ++ "/a/b");
    try fs.submit(.access, root ++ "/a/b");

    const file = try create(root ++ "/a/b/f");
    _ = try write(file, "x", 0);
    close(file);

    // The directory fsync that makes the new name durable.
    const dir = try fs.submit(.open, .{
        .path = root ++ "/a/b",
        .flags = .{ .ACCMODE = .RDONLY, .DIRECTORY = true },
        .mode = 0,
    });
    try sync(dir);
    _ = try fs.submit(.device, dir);
    close(dir);

    try fs.submit(.delete_tree, root);
    try std.testing.expectError(
        error.FileNotFound,
        fs.submit(.access, root),
    );
}
