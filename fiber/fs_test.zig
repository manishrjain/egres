const std = @import("std");
const fiber = @import("fiber");

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

test "fs.File: preadBatch reads independent offsets in one park" {
    const path = "/tmp/fiber_fs_test_batch";
    fiber.Dir.cwd().deleteFile(path) catch {};
    var file = try fiber.Dir.cwd().createFile(path, .{});
    defer {
        file.close();
        fiber.Dir.cwd().deleteFile(path) catch {};
    }

    // 256 bytes where byte[i] == i.
    var content: [256]u8 = undefined;
    for (&content, 0..) |*b, i| b.* = @intCast(i);
    _ = try file.write(&content, 0);
    try file.sync(.{});

    var b0: [16]u8 = undefined;
    var b1: [16]u8 = undefined;
    var b2: [16]u8 = undefined;
    var ops = [_]fiber.File.BatchOp{
        .{ .buf = &b0, .offset = 0 },
        .{ .buf = &b1, .offset = 100 },
        .{ .buf = &b2, .offset = 240 },
    };
    try file.preadBatch(std.testing.allocator, &ops);

    try std.testing.expectEqual(@as(usize, 16), try ops[0].result);
    try std.testing.expectEqual(@as(usize, 16), try ops[1].result);
    try std.testing.expectEqual(@as(usize, 16), try ops[2].result);
    try std.testing.expectEqual(@as(u8, 0), b0[0]);
    try std.testing.expectEqual(@as(u8, 100), b1[0]);
    try std.testing.expectEqual(@as(u8, 240), b2[0]);
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

test "fs.File: syncAndClose persists data and releases the fd in one round-trip" {
    const path = "/tmp/fiber_fs_test_synclose";
    fiber.Dir.cwd().deleteFile(path) catch {};
    defer fiber.Dir.cwd().deleteFile(path) catch {};

    {
        var w = try fiber.Dir.cwd().createFile(path, .{});
        try writeAllAt(w, "sync-and-close", 0);
        try w.syncAndClose(); // single offload: fsync + close, one park/wake
    }

    // Reopen: data is durable (sync ran) and the file is usable (closed cleanly).
    var r = try fiber.Dir.cwd().openFile(path, .{});
    defer r.close();
    var buf: [14]u8 = undefined;
    const n = try readAllAt(r, &buf, 0);
    try std.testing.expectEqualStrings("sync-and-close", buf[0..n]);
}
