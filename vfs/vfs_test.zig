// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const fiber = @import("zfiber");
const vfs = @import("vfs");
const utils = @import("utils");
const String = utils.String;
const bandwidth = vfs.bandwidth;
const VFS = vfs.VFS;
const page_size = vfs.page_size;

// 0.16 helpers: thin wrappers around std.Io.Dir that inject the
// process-wide `io` from `utils.context`. Keeps the per-test call sites
// readable (no `io` parameter to thread).
inline fn ioOf() std.Io {
    return utils.context.g().io;
}
fn createFileAbs(path: String) !std.Io.File {
    return std.Io.Dir.createFileAbsolute(ioOf(), path, .{});
}
fn openFileAbs(path: String) !std.Io.File {
    return std.Io.Dir.openFileAbsolute(ioOf(), path, .{});
}
fn deleteFileAbs(path: String) !void {
    return std.Io.Dir.deleteFileAbsolute(ioOf(), path);
}
fn accessAbs(path: String) !void {
    return std.Io.Dir.accessAbsolute(ioOf(), path, .{});
}
inline fn nowMs() i64 {
    return std.Io.Timestamp.now(ioOf(), .real).toMilliseconds();
}

// ── Helpers ──────────────────────────────────────────────────────────

fn writeTempFile(path: String, data: String) !void {
    const io = ioOf();
    const file = try createFileAbs(path);
    defer file.close(io);
    try file.writeStreamingAll(io, data);
}

// ── 1. Open / close round-trip ───────────────────────────────────────

test "open and close" {
    const path = "/tmp/vfs_test_open_close";
    try writeTempFile(path, "hello world");
    defer deleteFileAbs(path) catch {};

    const v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    try testing.expectEqual(@as(u64, 11), handle.file_size);
}

// ── 2. readAligned at aligned offset ─────────────────────────────────

test "readAligned at aligned offset" {
    const path = "/tmp/vfs_test_aligned";

    // Write exactly 8192 bytes (two pages).
    var data: [8192]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i);
    try writeTempFile(path, &data);
    defer deleteFileAbs(path) catch {};

    const v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    // Read 100 bytes at offset 0 (aligned).
    const r = try handle.readAligned(testing.allocator, 0, 100);
    defer r.free();

    try testing.expectEqual(@as(usize, 100), r.data.len);
    try testing.expectEqualSlices(u8, data[0..100], r.data);
}

// ── 3. readAligned at unaligned offset ───────────────────────────────

test "readAligned at unaligned offset" {
    const path = "/tmp/vfs_test_unaligned";

    var data: [8192]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i);
    try writeTempFile(path, &data);
    defer deleteFileAbs(path) catch {};

    const v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    // Read 12 bytes at offset 5000 (unaligned).
    const r = try handle.readAligned(testing.allocator, 5000, 12);
    defer r.free();

    try testing.expectEqual(@as(usize, 12), r.data.len);
    try testing.expectEqualSlices(u8, data[5000..5012], r.data);
}

// ── 4. readAligned alignment math ────────────────────────────────────

test "readAligned alignment math" {
    const path = "/tmp/vfs_test_math";

    var data: [8192]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i);
    try writeTempFile(path, &data);
    defer deleteFileAbs(path) catch {};

    const v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    // Read 12 bytes at offset 5000.
    // aligned_offset = 4096, prefix = 904, aligned_size = 4096.
    const r = try handle.readAligned(testing.allocator, 5000, 12);
    defer r.free();

    // Buffer must be 4KB-aligned.
    const buf = r.buf.?;
    try testing.expect(@intFromPtr(buf.ptr) % vfs.page_size == 0);
    // Buffer size is a multiple of page_size.
    try testing.expect(buf.len % vfs.page_size == 0);
    // Data slice is correct.
    try testing.expectEqual(@as(usize, 12), r.data.len);
}

// ── 5a. O_DIRECT page-aligned write round-trips intact (regression guard) ──
//
// O_DIRECT requires the buffer ADDRESS, LENGTH, and OFFSET to be page-aligned;
// `vfs.FileHandle.write` asserts that contract and every in-tree direct writer
// satisfies it at the source (the SST builder page-aligns + pads its output;
// the snapshot-install path copies network data into an aligned buffer). This
// test exercises that supported path on a ~16 MB SST-sized buffer and verifies
// every byte lands on disk intact — it plants a checksum in the last 4 bytes
// (which would be the casualty of any tail truncation / zero-padding) and
// checks BOTH the file size AND that the checksum still matches the content
// after a read-back through a plain (non-O_DIRECT) handle.
//
// Needs a real disk-backed filesystem (tmpfs falls back to non-direct).
// Uses /data/egres/ which the project pins to ext4.
test "O_DIRECT page-aligned write round-trips intact (content verified)" {
    const dir = "/data/egres";
    accessAbs(dir) catch return error.SkipZigTest;

    const path = dir ++ "/vfs_test_direct_aligned";
    defer deleteFileAbs(path) catch {};
    deleteFileAbs(path) catch {};

    // 16 MB + 1 page — page-aligned length, the size class where SST-write
    // corruption was once suspected. The buffer is page-aligned via
    // alignedAlloc and written at offset 0, matching the direct-write contract.
    const len: usize = 16 * 1024 * 1024 + vfs.page_size;
    const data = try testing.allocator.alignedAlloc(u8, vfs.page_alignment, len);
    defer testing.allocator.free(data);

    // Fill body with a deterministic pattern, then plant a u32 checksum of the
    // body in the LAST 4 bytes — the tail that any truncation would drop.
    var prng = std.Random.DefaultPrng.init(0xc0ffee);
    prng.fill(data[0 .. len - 4]);
    const expected_checksum = std.hash.XxHash32.hash(0, data[0 .. len - 4]);
    std.mem.writeInt(u32, data[len - 4 ..][0..4], expected_checksum, .little);

    const v = vfs.g().*;
    const wh = try v.createFile(path, .{ .mode = .direct });
    try wh.write(data, 0);
    wh.close();

    // Read the file back through a plain (non-O_DIRECT) handle so we
    // observe what's actually on disk, not whatever caches we skipped.
    const io = ioOf();
    const file = try openFileAbs(path);
    defer file.close(io);
    try testing.expectEqual(@as(u64, len), (try file.stat(io)).size);

    const back = try testing.allocator.alloc(u8, len);
    defer testing.allocator.free(back);
    const n = try file.readPositionalAll(io, back, 0);
    try testing.expectEqual(len, n);

    // Tail must survive: checksum bytes intact AND match the body.
    const stored_checksum = std.mem.readInt(u32, back[len - 4 ..][0..4], .little);
    try testing.expectEqual(expected_checksum, stored_checksum);
    const actual_checksum = std.hash.XxHash32.hash(0, back[0 .. len - 4]);
    try testing.expectEqual(expected_checksum, actual_checksum);
}

// ── 5. O_DIRECT fallback on tmpfs ────────────────────────────────────

test "O_DIRECT fallback on tmpfs" {
    // /tmp is typically tmpfs on Linux; O_DIRECT is unsupported there.
    // VFS.openFile must succeed via the fallback path.
    const path = "/tmp/vfs_test_direct_fallback";
    try writeTempFile(path, "fallback test");
    defer deleteFileAbs(path) catch {};

    const v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    try testing.expectEqual(@as(u64, 13), handle.file_size);

    // Also verify we can read through the fallback fd.
    const r = try handle.readAligned(testing.allocator, 0, 13);
    defer r.free();

    try testing.expectEqualSlices(u8, "fallback test", r.data);
}

// ── 6. Bandwidth limiter integration ─────────────────────────────────

test "bandwidth limiter deducts on read" {
    const path = "/tmp/vfs_test_bw";

    var data: [8192]u8 = undefined;
    @memset(&data, 0xFF);
    try writeTempFile(path, &data);
    defer deleteFileAbs(path) catch {};

    var limiter = bandwidth.Limiter.init(.{ .bytes_per_sec = 1_000_000 });
    var v = VFS.new(&limiter);
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    const r = try handle.readAligned(testing.allocator, 0, 100);
    defer r.free();

    // A read of 100 bytes is one page on the device's count.
    try testing.expectEqual(1, limiter.total_read_pages.load(.monotonic));
}

// ── 7. mmap open/close ──────────────────────────────────────────────

test "mmap open and close" {
    const path = "/tmp/vfs_test_mmap_open";
    try writeTempFile(path, "mmap test data");
    defer deleteFileAbs(path) catch {};

    var v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .mmap });
    defer handle.close();

    try testing.expectEqual(@as(u64, 14), handle.file_size);
    try testing.expect(handle.mapped != null);
}

// ── 8. mmap readAligned ─────────────────────────────────────────────

test "mmap readAligned" {
    const path = "/tmp/vfs_test_mmap_read";

    var data: [8192]u8 = undefined;
    for (&data, 0..) |*b, i| b.* = @truncate(i);
    try writeTempFile(path, &data);
    defer deleteFileAbs(path) catch {};

    var v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .mmap });
    defer handle.close();

    // Aligned offset.
    const r1 = try handle.readAligned(testing.allocator, 0, 100);
    defer r1.free();
    try testing.expectEqualSlices(u8, data[0..100], r1.data);

    // Unaligned offset.
    const r2 = try handle.readAligned(testing.allocator, 5000, 12);
    defer r2.free();
    try testing.expectEqualSlices(u8, data[5000..5012], r2.data);
}

// ── 9. mmap readAligned returns no buf ──────────────────────────────

test "mmap readAligned no allocation" {
    const path = "/tmp/vfs_test_mmap_nobuf";
    try writeTempFile(path, "no alloc test!");
    defer deleteFileAbs(path) catch {};

    var v = vfs.g().*;
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .mmap });
    defer handle.close();

    const r = try handle.readAligned(testing.allocator, 0, 14);
    defer r.free();

    try testing.expect(r.buf == null);
    try testing.expectEqualSlices(u8, "no alloc test!", r.data);
}

// ── 10. mmap bandwidth ─────────────────────────────────────────────

test "mmap enforces bandwidth limiter" {
    const path = "/tmp/vfs_test_mmap_bw";

    var data: [8192]u8 = undefined;
    @memset(&data, 0xFF);
    try writeTempFile(path, &data);
    defer deleteFileAbs(path) catch {};

    var limiter = bandwidth.Limiter.init(.{ .bytes_per_sec = 1_000_000 });
    var v = VFS.new(&limiter);
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .mmap });
    defer handle.close();

    const r = try handle.readAligned(testing.allocator, 0, 100);
    defer r.free();

    // mmap reads go through the limiter too.
    try testing.expectEqual(1, limiter.total_read_pages.load(.monotonic));
}

/// A tenant at `pages_per_s` on this test fiber's context, so the
/// metered limiter below charges it rather than the system, which
/// never waits. The caller resets the context.
fn tenantAt(id: u64, pages_per_s: i64) fiber.Tenant {
    return fiber.Tenant.init(id, .{
        .cpu_milli = 1000,
        .io_pages_per_s = pages_per_s,
        .mem_bytes = 1,
    }, utils.coarseMs());
}

// ── 11. Read bandwidth enforcement ──────────────────────────────────

test "read bandwidth enforcement" {
    const path = "/tmp/vfs_test_bw_read_rate";
    const file_size = 1024 * page_size; // 4 MB
    const data = try testing.allocator.alloc(u8, file_size);
    defer testing.allocator.free(data);
    @memset(data, 0xAB);
    try writeTempFile(path, data);
    defer deleteFileAbs(path) catch {};

    // The file is one second of the tenant's rate: the first read is
    // granted at once and owed, the second waits the debt out.
    var t = tenantAt(21, 1024);
    const ctx = fiber.ctx();
    ctx.tenant = &t;
    defer ctx.tenant = &fiber.meter.shared;
    ctx.class = .bg;
    defer ctx.class = .fg;
    var limiter = bandwidth.Limiter.init(.{
        .bytes_per_sec = 1024 * page_size,
    });
    limiter.metered = true;

    var v = VFS.new(&limiter);
    const handle = try v.openFile(testing.allocator, path, .{ .mode = .direct });
    defer handle.close();

    const first = try handle.readAligned(testing.allocator, 0, file_size);
    defer first.free();
    try testing.expectEqual(file_size, first.data.len);

    const start = nowMs();
    const second = try handle.readAligned(testing.allocator, 0, file_size);
    defer second.free();
    const elapsed_ms = nowMs() - start;
    try testing.expectEqual(file_size, second.data.len);
    // About 900 ms of debt at the rate. Wide bounds for a loaded box.
    try testing.expect(elapsed_ms >= 500);
    try testing.expect(elapsed_ms <= 10000);
}

// ── 12. Write bandwidth enforcement ─────────────────────────────────

test "write bandwidth enforcement" {
    const path = "/tmp/vfs_test_bw_write_rate";

    // Create the file first.
    const file_size = 512 * page_size; // 2 MB
    const data = try testing.allocator.alignedAlloc(u8, vfs.page_alignment, file_size);
    defer testing.allocator.free(data);
    @memset(data, 0xCD);
    try writeTempFile(path, data);
    defer deleteFileAbs(path) catch {};

    // 8192 pages/s at a write cost of 8: the 512-page write costs
    // 4096, half a second of the tenant's rate. The first is granted
    // at once and owed, the second waits the debt out.
    var t = tenantAt(22, 8192);
    const ctx = fiber.ctx();
    ctx.tenant = &t;
    defer ctx.tenant = &fiber.meter.shared;
    ctx.class = .bg;
    defer ctx.class = .fg;
    var limiter = bandwidth.Limiter.init(.{
        .bytes_per_sec = 8192 * page_size,
        .write_cost = 8,
    });
    limiter.metered = true;

    // Open for writing (need a real fd, not O_DIRECT for writes on tmpfs).
    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{ .ACCMODE = .WRONLY }, 0);
    defer (std.Io.File{ .handle = fd, .flags = .{ .nonblocking = false } }).close(ioOf());

    var v = VFS.new(&limiter);
    const fh = vfs.FileHandle{
        .file = fiber.File.fromFd(fd),
        .file_size = file_size,
        .vfs = &v,
        .bw = &limiter,
    };

    try fh.write(data, 0);
    const start = nowMs();
    try fh.write(data, 0);
    const elapsed_ms = nowMs() - start;

    // About 400 ms of debt at the rate. Wide bounds for a loaded box.
    try testing.expect(elapsed_ms >= 250);
    try testing.expect(elapsed_ms <= 5000);
}

// ── batchRead tests ─────────────────────────────────────────────────

test "batchRead 512 requests" {
    const alloc = testing.allocator;
    const path = "/tmp/vfs_test_batch_read";
    defer deleteFileAbs(path) catch {};

    // Write a 2 MiB file. Each page filled with a byte derived from page index.
    const num_pages = 512;
    {
        const io = ioOf();
        const file = try createFileAbs(path);
        defer file.close(io);
        var buf: [page_size]u8 = undefined;
        for (0..num_pages) |i| {
            @memset(&buf, @intCast(i % 251)); // distinct fill per page (mod 251 to stay in u8)
            try file.writeStreamingAll(io, &buf);
        }
    }

    const v = vfs.g().*;
    const handle = try v.openFile(alloc, path, .{ .mode = .default });
    defer handle.close();

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();

    // Build 512 read requests — exceeds io_uring SQ depth (256), tests chunking.
    const num_requests = 512;
    const read_len = 100;
    const intra_page_offset = 10;
    var requests = try alloc.alloc(vfs.FileHandle.ReadRequest, num_requests);
    defer alloc.free(requests);
    for (0..num_requests) |i| {
        requests[i] = .{
            .offset = @as(u64, i) * page_size + intra_page_offset,
            .len = read_len,
        };
    }

    try handle.batchRead(&arena, requests);

    // Verify each read got the correct fill byte.
    for (0..num_requests) |i| {
        try testing.expectEqual(@as(usize, read_len), requests[i].data.len);
        const expected_fill: u8 = @intCast(i % 251);
        for (requests[i].data) |b| {
            try testing.expectEqual(expected_fill, b);
        }
    }
}

test "openFile: a file charges the limiter of the VFS it opened through" {
    const path = "/tmp/vfs_test_handle_bw";
    try writeTempFile(path, "x");
    defer deleteFileAbs(path) catch {};
    var limiter = bandwidth.Limiter.init(.{ .bytes_per_sec = 1_000_000 });
    var v = VFS.new(&limiter);
    const handle = try v.openFile(testing.allocator, path, .{});
    defer handle.close();
    try testing.expectEqual(&limiter, handle.bw);
    const global = try vfs.g().openFile(testing.allocator, path, .{});
    defer global.close();
    try testing.expectEqual(vfs.g().bw, global.bw);
}

test "deviceOfPath: a directory and one under it are on one device" {
    const dir = "/tmp/vfs_test_device_of_path";
    try vfs.createDirPath(dir);
    defer vfs.deleteTree(dir) catch {};
    try testing.expectEqual(
        try vfs.deviceOfPath("/tmp"),
        try vfs.deviceOfPath(dir),
    );
}
