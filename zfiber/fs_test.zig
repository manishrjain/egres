// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");

test "fs.File: write, sync, stat, read back at offsets" {
    const path = "/tmp/fiber_fs_test_rw";
    fiber.Dir.cwd().deleteFile(path) catch {};
    var file = try fiber.Dir.cwd().createFile(path, .{});
    defer {
        file.close();
        fiber.Dir.cwd().deleteFile(path) catch {};
    }

    const data = "hello fiber file io";
    const written = try file.write(data, 0);
    try std.testing.expectEqual(@as(usize, data.len), written);

    try file.sync(.{});

    const st = try file.stat();
    try std.testing.expectEqual(@as(u64, data.len), st.size);

    var buf: [64]u8 = undefined;
    const n = try file.read(buf[0..data.len], 0);
    try std.testing.expectEqual(@as(usize, data.len), n);
    try std.testing.expectEqualSlices(u8, data, buf[0..n]);

    // Positional read mid-file: bytes 6..11 are "fiber".
    const m = try file.read(buf[0..5], 6);
    try std.testing.expectEqualStrings("fiber", buf[0..m]);
}

test "fs.File: setSize truncates" {
    const path = "/tmp/fiber_fs_test_trunc";
    fiber.Dir.cwd().deleteFile(path) catch {};
    var file = try fiber.Dir.cwd().createFile(path, .{});
    defer {
        file.close();
        fiber.Dir.cwd().deleteFile(path) catch {};
    }

    _ = try file.write("0123456789", 0);
    try file.setSize(4);
    try std.testing.expectEqual(@as(u64, 4), try file.size());
}

test "fs.preadBatch reads offsets across two files in one park" {
    const path = "/tmp/fiber_fs_test_batch";
    const path2 = "/tmp/fiber_fs_test_batch2";
    fiber.Dir.cwd().deleteFile(path) catch {};
    fiber.Dir.cwd().deleteFile(path2) catch {};
    var file = try fiber.Dir.cwd().createFile(path, .{});
    var file2 = try fiber.Dir.cwd().createFile(path2, .{});
    defer {
        file.close();
        file2.close();
        fiber.Dir.cwd().deleteFile(path) catch {};
        fiber.Dir.cwd().deleteFile(path2) catch {};
    }

    // 256 bytes where byte[i] == i; the second file holds 255 - i.
    var content: [256]u8 = undefined;
    for (&content, 0..) |*b, i| b.* = @intCast(i);
    _ = try file.write(&content, 0);
    try file.sync(.{});
    for (&content, 0..) |*b, i| b.* = @intCast(255 - i);
    _ = try file2.write(&content, 0);
    try file2.sync(.{});

    var b0: [16]u8 = undefined;
    var b1: [16]u8 = undefined;
    var b2: [16]u8 = undefined;
    var b3: [16]u8 = undefined;
    var ops = [_]fiber.File.BatchOp{
        .{ .file = file, .buf = &b0, .offset = 0 },
        .{ .file = file, .buf = &b1, .offset = 100 },
        .{ .file = file2, .buf = &b2, .offset = 100 },
        .{ .file = file, .buf = &b3, .offset = 240 },
    };
    try fiber.fs.preadBatch(std.testing.allocator, &ops);

    for (ops) |op| try std.testing.expectEqual(@as(usize, 16), try op.result);
    try std.testing.expectEqual(@as(u8, 0), b0[0]);
    try std.testing.expectEqual(@as(u8, 100), b1[0]);
    try std.testing.expectEqual(@as(u8, 155), b2[0]);
    try std.testing.expectEqual(@as(u8, 240), b3[0]);
}

test "fs.Dir: openFile, access, deleteFile" {
    const path = "/tmp/fiber_fs_test_dir";
    fiber.Dir.cwd().deleteFile(path) catch {};

    {
        var w = try fiber.Dir.cwd().createFile(path, .{});
        _ = try w.write("payload", 0);
        try w.sync(.{});
        w.close();
    }

    try fiber.Dir.cwd().access(path, .{});

    var r = try fiber.Dir.cwd().openFile(path, .{});
    var buf: [16]u8 = undefined;
    const n = try r.read(buf[0..7], 0);
    try std.testing.expectEqualStrings("payload", buf[0..n]);
    r.close();

    try fiber.Dir.cwd().deleteFile(path);
    try std.testing.expectError(error.FileNotFound, fiber.Dir.cwd().access(path, .{}));
}

// ── helpers: loop fiber.File.read/write (single ops can be short) ──
fn writeAllAt(file: fiber.File, data: []const u8, offset: u64) !void {
    var off: usize = 0;
    while (off < data.len) {
        const n = try file.write(data[off..], offset + off);
        if (n == 0) return error.WriteFailed;
        off += n;
    }
}
fn readAllAt(file: fiber.File, buf: []u8, offset: u64) !usize {
    var off: usize = 0;
    while (off < buf.len) {
        const n = try file.read(buf[off..], offset + off);
        if (n == 0) break; // EOF
        off += n;
    }
    return off;
}

test "fs.File: large patterned write/read round-trip (256 KiB)" {
    const path = "/tmp/fiber_fs_test_large";
    fiber.Dir.cwd().deleteFile(path) catch {};
    var file = try fiber.Dir.cwd().createFile(path, .{});
    defer {
        file.close();
        fiber.Dir.cwd().deleteFile(path) catch {};
    }

    const N = 256 * 1024;
    const data = try std.testing.allocator.alloc(u8, N);
    defer std.testing.allocator.free(data);
    for (data, 0..) |*b, i| b.* = @truncate(i *% 2654435761);

    try writeAllAt(file, data, 0);
    try file.sync(.{});
    try std.testing.expectEqual(@as(u64, N), try file.size());

    const back = try std.testing.allocator.alloc(u8, N);
    defer std.testing.allocator.free(back);
    const got = try readAllAt(file, back, 0);
    try std.testing.expectEqual(@as(usize, N), got);
    try std.testing.expect(std.mem.eql(u8, data, back));
}

test "fs.File: data survives sync + close + reopen" {
    const path = "/tmp/fiber_fs_test_durable";
    fiber.Dir.cwd().deleteFile(path) catch {};
    defer fiber.Dir.cwd().deleteFile(path) catch {};

    {
        var w = try fiber.Dir.cwd().createFile(path, .{});
        try writeAllAt(w, "durable-data", 0);
        try w.sync(.{});
        w.close();
    }

    var r = try fiber.Dir.cwd().openFile(path, .{});
    defer r.close();
    var buf: [12]u8 = undefined;
    const n = try readAllAt(r, &buf, 0);
    try std.testing.expectEqualStrings("durable-data", buf[0..n]);
}

test "fs.File: setSize extends with zero fill" {
    const path = "/tmp/fiber_fs_test_extend";
    fiber.Dir.cwd().deleteFile(path) catch {};
    var file = try fiber.Dir.cwd().createFile(path, .{});
    defer {
        file.close();
        fiber.Dir.cwd().deleteFile(path) catch {};
    }

    try writeAllAt(file, "abc", 0);
    try file.setSize(10);
    try std.testing.expectEqual(@as(u64, 10), try file.size());

    var buf: [10]u8 = undefined;
    const n = try readAllAt(file, &buf, 0);
    try std.testing.expectEqual(@as(usize, 10), n);
    try std.testing.expectEqualStrings("abc", buf[0..3]);
    for (buf[3..10]) |b| try std.testing.expectEqual(@as(u8, 0), b);
}

test "fs.Dir: rename moves a file" {
    const old = "/tmp/fiber_fs_test_rn_old";
    const new = "/tmp/fiber_fs_test_rn_new";
    fiber.Dir.cwd().deleteFile(old) catch {};
    fiber.Dir.cwd().deleteFile(new) catch {};
    defer {
        fiber.Dir.cwd().deleteFile(old) catch {};
        fiber.Dir.cwd().deleteFile(new) catch {};
    }

    {
        var w = try fiber.Dir.cwd().createFile(old, .{});
        try writeAllAt(w, "moved", 0);
        try w.sync(.{});
        w.close();
    }

    try fiber.Dir.cwd().rename(old, fiber.Dir.cwd(), new);
    try std.testing.expectError(error.FileNotFound, fiber.Dir.cwd().access(old, .{}));
    try fiber.Dir.cwd().access(new, .{});

    var r = try fiber.Dir.cwd().openFile(new, .{});
    defer r.close();
    var buf: [5]u8 = undefined;
    const n = try readAllAt(r, &buf, 0);
    try std.testing.expectEqualStrings("moved", buf[0..n]);
}

test "fs.Dir: openFile on a nonexistent path returns FileNotFound" {
    const path = "/tmp/fiber_fs_test_missing_xyz";
    fiber.Dir.cwd().deleteFile(path) catch {};
    try std.testing.expectError(error.FileNotFound, fiber.Dir.cwd().openFile(path, .{}));
}

test "fs: many fibers run full file lifecycles concurrently" {
    const N = 16;
    const Body = struct {
        fn run(id: usize, ok: *bool) void {
            var pbuf: [64]u8 = undefined;
            const path = std.fmt.bufPrint(&pbuf, "/tmp/fiber_fs_conc_{d}", .{id}) catch return;
            fiber.Dir.cwd().deleteFile(path) catch {};

            var file = fiber.Dir.cwd().createFile(path, .{}) catch return;
            var data: [1024]u8 = undefined;
            for (&data, 0..) |*b, i| b.* = @truncate(i +% id);

            writeAllAt(file, &data, 0) catch {
                file.close();
                return;
            };
            file.sync(.{}) catch {
                file.close();
                return;
            };
            var back: [1024]u8 = undefined;
            const n = readAllAt(file, &back, 0) catch {
                file.close();
                return;
            };
            file.close();
            fiber.Dir.cwd().deleteFile(path) catch {};

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

test "fs.Dir: list reports every entry with its kind" {
    const alloc = std.testing.allocator;
    const root = "/tmp/fiber_fs_test_list";
    std.Io.Dir.cwd().deleteTree(utils.context.g().io, root) catch {};
    try std.Io.Dir.cwd().createDirPath(utils.context.g().io, root ++ "/sub");
    defer std.Io.Dir.cwd().deleteTree(utils.context.g().io, root) catch {};

    for ([_][]const u8{ root ++ "/a.txt", root ++ "/b.txt" }) |p| {
        const f = try fiber.Dir.cwd().createFile(p, .{});
        _ = try f.write("x", 0);
        f.close();
    }

    const entries = try fiber.Dir.cwd().list(alloc, root);
    defer fiber.freeDirEntries(alloc, entries);
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

test "fs.File: stat reports a fresh file's mtime as now" {
    // The real disk stamps real time; under the simulation `now` is
    // virtual, so the two agree only once the sim disk stamps files.
    if (fiber.isSim()) return error.SkipZigTest;
    const path = "/tmp/fiber_fs_test_mtime";
    fiber.Dir.cwd().deleteFile(path) catch {};
    var file = try fiber.Dir.cwd().createFile(path, .{});
    defer {
        file.close();
        fiber.Dir.cwd().deleteFile(path) catch {};
    }
    _ = try file.write("now", 0);

    const now = utils.timestampNow(.realtime, .secs);
    const st = try file.stat();
    const skew = @abs(now - st.mtime_s);
    try std.testing.expect(skew < 10);
}
