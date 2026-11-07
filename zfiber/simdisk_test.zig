// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The simulated disk's model, driven directly: these run on the real
//! runtime too, since `SimDisk` is a plain value; its waits are then
//! real milliseconds.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");

const fs = fiber.fs;
const SimDisk = fiber.simdisk.SimDisk;
const String = utils.String;

const rdwr_creat: std.posix.O = .{
    .ACCMODE = .RDWR,
    .CREAT = true,
    .TRUNC = true,
};
const rdonly: std.posix.O = .{ .ACCMODE = .RDONLY };

fn open(d: *SimDisk, path: String, flags: std.posix.O) !fiber.File {
    return d.apply(.open, .{ .path = path, .flags = flags, .mode = 0o644 });
}

fn write(d: *SimDisk, file: fiber.File, data: String, off: u64) !usize {
    return d.apply(.write, .{ .file = file, .data = data, .offset = off });
}

fn read(d: *SimDisk, file: fiber.File, buf: []u8, off: u64) !usize {
    return d.apply(.read, .{ .file = file, .buf = buf, .offset = off });
}

test "simdisk: a create needs its directory, then round-trips bytes" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();

    try std.testing.expectError(
        error.FileNotFound,
        open(&d, "/data/a", rdwr_creat),
    );
    try d.apply(.create_dir_path, "/data/sub");
    const f = try open(&d, "/data/a", rdwr_creat);
    try std.testing.expectEqual(@as(usize, 5), try write(&d, f, "hello", 0));
    // A write past the end zero-fills the gap.
    _ = try write(&d, f, "!", 8);
    try std.testing.expectEqual(@as(u64, 9), (try d.apply(.stat, f)).size);

    var buf: [16]u8 = undefined;
    const n = try read(&d, f, &buf, 0);
    try std.testing.expectEqualSlices(u8, "hello\x00\x00\x00!", buf[0..n]);
    // At the end: libxev's EOF, not zero bytes.
    try std.testing.expectError(error.EOF, read(&d, f, &buf, 9));
    d.apply(.close, f);
}

test "simdisk: open flags: EXCL, TRUNC, read-only, DIRECTORY, IsDir" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    const f = try open(&d, "/tmp/x", rdwr_creat);
    _ = try write(&d, f, "0123456789", 0);
    d.apply(.close, f);

    var excl = rdwr_creat;
    excl.TRUNC = false;
    excl.EXCL = true;
    try std.testing.expectError(
        error.PathAlreadyExists,
        open(&d, "/tmp/x", excl),
    );
    // TRUNC on a reopen empties it.
    const t = try open(&d, "/tmp/x", rdwr_creat);
    try std.testing.expectEqual(@as(u64, 0), (try d.apply(.stat, t)).size);
    d.apply(.close, t);
    // A read-only handle cannot write.
    const r = try open(&d, "/tmp/x", rdonly);
    try std.testing.expectError(
        error.NotOpenForWriting,
        write(&d, r, "a", 0),
    );
    d.apply(.close, r);
    // DIRECTORY wants a directory; writing to one is IsDir.
    var dir_flags = rdonly;
    dir_flags.DIRECTORY = true;
    try std.testing.expectError(error.NotDir, open(&d, "/tmp/x", dir_flags));
    const dir = try open(&d, "/tmp", dir_flags);
    try d.apply(.sync, .{ .file = dir, .data_only = false });
    d.apply(.close, dir);
    try std.testing.expectError(
        error.IsDir,
        open(&d, "/tmp", .{ .ACCMODE = .RDWR }),
    );
    try std.testing.expectError(
        error.NotDir,
        open(&d, "/tmp/x/under", rdwr_creat),
    );
}

test "simdisk: an unlinked file stays readable through its handle" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    const f = try open(&d, "/tmp/gone", rdwr_creat);
    _ = try write(&d, f, "still here", 0);
    try d.apply(.delete_file, "/tmp/gone");
    try std.testing.expectError(
        error.FileNotFound,
        d.apply(.access, "/tmp/gone"),
    );
    var buf: [10]u8 = undefined;
    _ = try read(&d, f, &buf, 0);
    try std.testing.expectEqualStrings("still here", &buf);
    d.apply(.close, f);
    try std.testing.expectError(
        error.FileNotFound,
        d.apply(.delete_file, "/tmp/gone"),
    );
}

test "simdisk: rename replaces a file and refuses a full directory" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    for ([_]String{ "/tmp/a", "/tmp/b" }) |p| {
        const f = try open(&d, p, rdwr_creat);
        _ = try write(&d, f, p, 0);
        d.apply(.close, f);
    }
    try d.apply(.rename, .{ .old_path = "/tmp/a", .new_path = "/tmp/b" });
    try expectGone(&d, "/tmp/a");
    const f = try open(&d, "/tmp/b", rdonly);
    var buf: [6]u8 = undefined;
    _ = try read(&d, f, &buf, 0);
    try std.testing.expectEqualStrings("/tmp/a", &buf);
    d.apply(.close, f);

    try d.apply(.create_dir_path, "/tmp/d1/inner");
    try d.apply(.create_dir_path, "/tmp/d2");
    try std.testing.expectError(
        error.DirNotEmpty,
        d.apply(.rename, .{ .old_path = "/tmp/d2", .new_path = "/tmp/d1" }),
    );
    try std.testing.expectError(
        error.IsDir,
        d.apply(.rename, .{ .old_path = "/tmp/b", .new_path = "/tmp/d2" }),
    );
    try d.apply(.rename, .{ .old_path = "/tmp/d1", .new_path = "/tmp/d2" });
    try d.apply(.access, "/tmp/d2/inner");
}

test "simdisk: list is sorted with kinds, delete_tree takes the subtree" {
    const alloc = std.testing.allocator;
    var d = try SimDisk.init(alloc, 7);
    defer d.deinit();
    try d.apply(.create_dir_path, "/tmp/t/sub");
    for ([_]String{ "/tmp/t/b.txt", "/tmp/t/a.txt" }) |p| {
        d.apply(.close, try open(&d, p, rdwr_creat));
    }
    const entries = try d.apply(.list, .{ .alloc = alloc, .path = "/tmp/t" });
    defer fs.freeEntries(alloc, entries);
    try std.testing.expectEqual(@as(usize, 3), entries.len);
    try std.testing.expectEqualStrings("a.txt", entries[0].name);
    try std.testing.expectEqualStrings("b.txt", entries[1].name);
    try std.testing.expectEqualStrings("sub", entries[2].name);
    try std.testing.expectEqual(fs.Kind.directory, entries[2].kind);
    try std.testing.expectError(
        error.NotDir,
        d.apply(.list, .{ .alloc = alloc, .path = "/tmp/t/a.txt" }),
    );

    // The tree goes while a handle into it stays usable.
    const held = try open(&d, "/tmp/t/a.txt", rdonly);
    try d.apply(.delete_tree, "/tmp/t");
    try d.apply(.delete_tree, "/tmp/t"); // nothing there is fine
    try std.testing.expectError(error.FileNotFound, d.apply(.access, "/tmp/t"));
    _ = try d.apply(.stat, held);
    d.apply(.close, held);
    try d.apply(.access, "/tmp");
}

test "simdisk: set_size, stat mtime, device and fs_magic" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    const f = try open(&d, "/tmp/sz", rdwr_creat);
    _ = try write(&d, f, "abc", 0);
    try d.apply(.set_size, .{ .file = f, .length = 6 });
    var buf: [6]u8 = undefined;
    _ = try read(&d, f, &buf, 0);
    try std.testing.expectEqualSlices(u8, "abc\x00\x00\x00", &buf);
    try d.apply(.set_size, .{ .file = f, .length = 1 });
    try std.testing.expectEqual(@as(u64, 1), (try d.apply(.stat, f)).size);

    const now_s = utils.timestampNow(.realtime, .secs);
    try std.testing.expect(@abs((try d.apply(.stat, f)).mtime_s - now_s) <= 1);
    try std.testing.expectEqual(@as(u64, 1), try d.apply(.device, f));
    d.apply(.close, f);
    try std.testing.expectEqual(@as(?i64, 0xef53), d.apply(.fs_magic, "/tmp"));
    try std.testing.expectEqual(@as(?i64, null), d.apply(.fs_magic, "/nope"));
}

test "simdisk: paths resolve . and .. and relative from the root" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    try d.apply(.create_dir_path, "a/b");
    try d.apply(.access, "/a/./b");
    try d.apply(.access, "/a/b/../b");
    try d.apply(.access, "../../a");
    try std.testing.expectError(
        error.FileNotFound,
        d.apply(.access, "/a/c/.."),
    );
}

// ── crash ──

fn syncDir(d: *SimDisk, path: String) !void {
    var flags = rdonly;
    flags.DIRECTORY = true;
    const dir = try open(d, path, flags);
    defer d.apply(.close, dir);
    try d.apply(.sync, .{ .file = dir, .data_only = false });
}

fn sync(d: *SimDisk, file: fiber.File) !void {
    try d.apply(.sync, .{ .file = file, .data_only = false });
}

fn expectFile(d: *SimDisk, path: String, want: String) !void {
    var buf: [64]u8 = undefined;
    try std.testing.expectEqualStrings(want, try contents(d, path, &buf));
}

fn expectGone(d: *SimDisk, path: String) !void {
    try std.testing.expectError(error.FileNotFound, d.apply(.access, path));
}

/// The whole file, read fresh after a crash.
fn contents(d: *SimDisk, path: String, buf: []u8) ![]u8 {
    const f = try open(d, path, rdonly);
    defer d.apply(.close, f);
    const size = (try d.apply(.stat, f)).size;
    if (size == 0) return buf[0..0];
    const n = try read(d, f, buf[0..@intCast(size)], 0);
    return buf[0..n];
}

test "simdisk: a crash keeps synced bytes and drops the rest" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    const f = try open(&d, "/tmp/f", rdwr_creat);
    try syncDir(&d, "/tmp");
    _ = try write(&d, f, "hello", 0);
    try sync(&d, f);
    _ = try write(&d, f, " world", 5);
    d.crash("/tmp", .lose_all);

    var buf: [32]u8 = undefined;
    try expectFile(&d, "/tmp/f", "hello");
    // The old handle belongs to a dead process.
    try std.testing.expectError(error.InputOutput, write(&d, f, "x", 0));
    try std.testing.expectError(error.InputOutput, read(&d, f, &buf, 0));
    try std.testing.expectError(error.InputOutput, sync(&d, f));
    d.apply(.close, f);
}

test "simdisk: a name is durable only after a sync of its directory" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    const f = try open(&d, "/tmp/new", rdwr_creat);
    _ = try write(&d, f, "data", 0);
    d.apply(.close, f);
    d.crash("/tmp", .lose_all);
    // Nothing was synced: the file is gone, name and bytes.
    try expectGone(&d, "/tmp/new");

    // The file's own sync commits its bytes, not its name.
    const g = try open(&d, "/tmp/new", rdwr_creat);
    _ = try write(&d, g, "data", 0);
    try sync(&d, g);
    d.apply(.close, g);
    d.crash("/tmp", .lose_all);
    try expectGone(&d, "/tmp/new");
    // Neither does an O_DSYNC write.
    var dsync = rdwr_creat;
    dsync.DSYNC = true;
    const h = try open(&d, "/tmp/ds", dsync);
    _ = try write(&d, h, "x", 0);
    d.apply(.close, h);
    d.crash("/tmp", .lose_all);
    try expectGone(&d, "/tmp/ds");

    // The directory's sync alone keeps the name and none of the bytes.
    const k = try open(&d, "/tmp/new", rdwr_creat);
    _ = try write(&d, k, "data", 0);
    try syncDir(&d, "/tmp");
    d.crash("/tmp", .lose_all);
    try expectFile(&d, "/tmp/new", "");
    d.apply(.close, k);
    // Both syncs keep both.
    const m = try open(&d, "/tmp/new", .{ .ACCMODE = .RDWR });
    _ = try write(&d, m, "data", 0);
    try sync(&d, m);
    d.apply(.close, m);
    d.crash("/tmp", .lose_all);
    try expectFile(&d, "/tmp/new", "data");
    // Directories made with create_dir_path are durable at once.
    try d.apply(.create_dir_path, "/tmp/d/e");
    d.crash("/tmp", .lose_all);
    try d.apply(.access, "/tmp/d/e");
}

test "simdisk: rename and delete wait for a sync of the directory" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    for ([_]String{ "/tmp/a", "/tmp/b" }) |p| {
        const f = try open(&d, p, rdwr_creat);
        _ = try write(&d, f, p, 0);
        try sync(&d, f);
        d.apply(.close, f);
    }
    try syncDir(&d, "/tmp");

    try d.apply(.rename, .{ .old_path = "/tmp/a", .new_path = "/tmp/b" });
    d.crash("/tmp", .lose_all);
    try expectFile(&d, "/tmp/a", "/tmp/a");
    try expectFile(&d, "/tmp/b", "/tmp/b");

    // The renamed file's own sync does not commit the rename.
    try d.apply(.rename, .{ .old_path = "/tmp/a", .new_path = "/tmp/b" });
    const b = try open(&d, "/tmp/b", rdonly);
    try sync(&d, b);
    d.apply(.close, b);
    d.crash("/tmp", .lose_all);
    try expectFile(&d, "/tmp/a", "/tmp/a");
    try expectFile(&d, "/tmp/b", "/tmp/b");

    try d.apply(.rename, .{ .old_path = "/tmp/a", .new_path = "/tmp/b" });
    try syncDir(&d, "/tmp");
    d.crash("/tmp", .lose_all);
    try expectGone(&d, "/tmp/a");
    try expectFile(&d, "/tmp/b", "/tmp/a");

    try d.apply(.delete_file, "/tmp/b");
    d.crash("/tmp", .lose_all);
    try d.apply(.access, "/tmp/b");
    try d.apply(.delete_file, "/tmp/b");
    try syncDir(&d, "/tmp");
    d.crash("/tmp", .lose_all);
    try expectGone(&d, "/tmp/b");
}

test "simdisk: a deleted tree is back after a crash until a sync" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    try d.apply(.create_dir_path, "/tmp/t/sub");
    for ([_]String{ "/tmp/t/f", "/tmp/t/sub/g" }) |p| {
        const f = try open(&d, p, rdwr_creat);
        _ = try write(&d, f, p, 0);
        try sync(&d, f);
        d.apply(.close, f);
    }
    try syncDir(&d, "/tmp/t");
    try syncDir(&d, "/tmp/t/sub");

    try d.apply(.delete_tree, "/tmp/t");
    try expectGone(&d, "/tmp/t/sub/g");
    d.crash("/tmp", .lose_all);
    try expectFile(&d, "/tmp/t/f", "/tmp/t/f");
    try expectFile(&d, "/tmp/t/sub/g", "/tmp/t/sub/g");

    // The sync of its directory takes the tree off the platter.
    try d.apply(.delete_tree, "/tmp/t");
    try syncDir(&d, "/tmp");
    d.crash("/tmp", .lose_all);
    try expectGone(&d, "/tmp/t");

    // A directory made again starts empty on the platter too.
    try d.apply(.create_dir_path, "/tmp/u");
    d.apply(.close, try open(&d, "/tmp/u/h", rdwr_creat));
    try syncDir(&d, "/tmp/u");
    try d.apply(.delete_tree, "/tmp/u");
    try d.apply(.create_dir_path, "/tmp/u");
    d.crash("/tmp", .lose_all);
    try d.apply(.access, "/tmp/u");
    try expectGone(&d, "/tmp/u/h");
}

test "simdisk: O_DSYNC writes survive, an unsynced truncate does not" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    var dsync = rdwr_creat;
    dsync.DSYNC = true;
    const f = try open(&d, "/tmp/s", dsync);
    try syncDir(&d, "/tmp");
    _ = try write(&d, f, "0123456789", 0);
    try d.apply(.set_size, .{ .file = f, .length = 4 });
    d.crash("/tmp", .lose_all);
    var buf: [16]u8 = undefined;
    // The write was durable on return; the shrink waited for a sync.
    try std.testing.expectEqualStrings(
        "0123456789",
        try contents(&d, "/tmp/s", &buf),
    );
    d.apply(.close, f);

    const g = try open(&d, "/tmp/s", .{ .ACCMODE = .RDWR });
    try d.apply(.set_size, .{ .file = g, .length = 4 });
    try sync(&d, g);
    d.crash("/tmp", .lose_all);
    try expectFile(&d, "/tmp/s", "0123");
    d.apply(.close, g);
}

test "simdisk: lose_some keeps or drops each dirty page whole" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    const f = try open(&d, "/tmp/p", rdwr_creat);
    try syncDir(&d, "/tmp");
    // Four pages of 0xaa on the platter, then one write across all four
    // with a page's index in each.
    var pages: [4 * 4096]u8 = undefined;
    @memset(&pages, 0xaa);
    _ = try write(&d, f, &pages, 0);
    try sync(&d, f);
    for (0..4) |i| @memset(pages[i * 4096 ..][0..4096], @intCast(i + 1));
    _ = try write(&d, f, &pages, 0);
    // Past the synced size: cut whatever the coins say.
    _ = try write(&d, f, "tail", pages.len);
    d.crash("/tmp", .lose_some);

    const kept = try contents(&d, "/tmp/p", &pages);
    try std.testing.expectEqual(4 * 4096, kept.len);
    var old: usize = 0;
    for (0..4) |i| {
        const page = kept[i * 4096 ..][0..4096];
        try std.testing.expect(std.mem.allEqual(u8, page, page[0]));
        if (page[0] == 0xaa) old += 1 else try std.testing.expectEqual(
            @as(u8, @intCast(i + 1)),
            page[0],
        );
    }
    // Seed 7 lands some of the four, not all: the write is torn.
    try std.testing.expect(old >= 1 and old <= 3);
    d.apply(.close, f);
}

test "simdisk: a crash brings back the pages an unsynced shrink cut" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    const f = try open(&d, "/tmp/t", rdwr_creat);
    try syncDir(&d, "/tmp");
    _ = try write(&d, f, "0123456789", 0);
    try sync(&d, f);
    try d.apply(.set_size, .{ .file = f, .length = 4 });
    _ = try write(&d, f, "ab", 4);
    try std.testing.expectEqual(6, (try d.apply(.stat, f)).size);
    d.crash("/tmp", .lose_all);
    try expectFile(&d, "/tmp/t", "0123456789");
    d.apply(.close, f);
}

// ── faults ──

test "simdisk: each fault surfaces as the kernel's error would" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    const f = try open(&d, "/tmp/flt", rdwr_creat);
    defer d.apply(.close, f);
    try syncDir(&d, "/tmp");
    _ = try write(&d, f, "0123456789", 0);
    try sync(&d, f);

    try d.setFaults("/tmp", .{ .read = 1000 });
    var buf: [16]u8 = undefined;
    try std.testing.expectError(error.InputOutput, read(&d, f, &buf, 0));

    // A failed write leaves the file as it was.
    try d.setFaults("/tmp", .{ .write = 1000 });
    try std.testing.expectError(error.InputOutput, write(&d, f, "xx", 0));
    try d.setFaults("/tmp", .{});
    try expectFile(&d, "/tmp/flt", "0123456789");

    // A short write lands a prefix; the caller issues the rest.
    try d.setFaults("/tmp", .{ .short_write = 1000 });
    const n = try write(&d, f, "abcdefgh", 10);
    try std.testing.expect(n >= 1 and n < 8);
    try d.setFaults("/tmp", .{});
    try std.testing.expectEqual(@as(u64, 10 + n), (try d.apply(.stat, f)).size);
    _ = try read(&d, f, buf[0..n], 10);
    try std.testing.expectEqualStrings("abcdefgh"[0..n], buf[0..n]);

    // A failed sync leaves the writes pending: a crash takes them.
    try d.setFaults("/tmp", .{ .sync = 1000 });
    try std.testing.expectError(error.InputOutput, sync(&d, f));
    try d.setFaults("/tmp", .{});
    d.crash("/tmp", .lose_all);
    try expectFile(&d, "/tmp/flt", "0123456789");

    const g = try open(&d, "/tmp/flt", .{ .ACCMODE = .RDWR });
    defer d.apply(.close, g);
    try d.setFaults("/tmp", .{ .set_size = 1000 });
    try std.testing.expectError(
        error.InputOutput,
        d.apply(.set_size, .{ .file = g, .length = 4 }),
    );
    try d.setFaults("/tmp", .{});
    try std.testing.expectEqual(@as(u64, 10), (try d.apply(.stat, g)).size);
}

test "simdisk: a short write on an O_DIRECT handle is whole pages" {
    var d = try SimDisk.init(std.testing.allocator, 7);
    defer d.deinit();
    var flags = rdwr_creat;
    if (@hasField(std.posix.O, "DIRECT")) flags.DIRECT = true;
    const f = try open(&d, "/tmp/dio", flags);
    defer d.apply(.close, f);
    var page_buf: [4 * 4096]u8 align(4096) = undefined;
    @memset(&page_buf, 0xab);
    try d.setFaults("/tmp", .{ .short_write = 1000 });
    const n = try write(&d, f, &page_buf, 0);
    try std.testing.expect(n % 4096 == 0);
    try std.testing.expect(n >= 4096 and n < page_buf.len);
}
