// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const raft = @import("raft");
const vfs = @import("vfs");
const zbuf = @import("zbuf").zbuf;
const zen = raft.buffers;
const u = raft.utils;

fn makeTempWalDir(alloc: std.mem.Allocator) ![]u8 {
    var path_buf: [160]u8 = undefined;
    const ts = std.Io.Timestamp.now(utils.context.g().io, .real).nanoseconds;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/egres-diskwal-{d}", .{ts});
    try vfs.createDirPath(path);
    return alloc.dupe(u8, path);
}

fn makeLogEntry(
    alloc: std.mem.Allocator,
    index: u64,
    term: u64,
    data: String,
) !zen.LogEntryBuffer {
    var writer = try zen.LogEntryWriter.init(alloc);
    defer writer.deinit();
    try writer.set(.{
        .index = index,
        .term = term,
        .data = data,
        .entry_type = zen.EntryTypeEnum.data,
        .conf_change = zen.NullOffset,
    });
    return writer.finishWithTail();
}

fn makeTypedEntry(
    alloc: std.mem.Allocator,
    index: u64,
    ty: zen.EntryTypeEnum,
    data: String,
) !zen.LogEntryBuffer {
    var writer = try zen.LogEntryWriter.init(alloc);
    defer writer.deinit();
    try writer.set(.{
        .index = index,
        .term = 1,
        .data = data,
        .entry_type = ty,
        .conf_change = zen.NullOffset,
    });
    return writer.finishWithTail();
}

fn appendEntry(
    alloc: std.mem.Allocator,
    log: *raft.DiskWAL,
    index: u64,
    term: u64,
    data: String,
) !void {
    var entry = try makeLogEntry(alloc, index, term, data);
    defer entry.deinit();
    try log.addEntries(&[_]zen.LogEntryBuffer{entry});
}

fn appendRange(
    alloc: std.mem.Allocator,
    log: *raft.DiskWAL,
    start: u64,
    end: u64,
    term: u64,
) !void {
    var idx = start;
    var appended: u64 = 0;
    while (idx <= end) {
        var batch = std.ArrayList(zen.LogEntryBuffer).empty;
        defer {
            for (batch.items) |*entry| entry.deinit();
            batch.deinit(alloc);
        }

        var n: usize = 0;
        while (idx <= end and n < 256) : (n += 1) {
            const payload = if ((idx & 1) == 0) "p0" else "p1";
            const entry = try makeLogEntry(alloc, idx, term, payload);
            try batch.append(alloc, entry);
            idx += 1;
            appended += 1;
        }
        if (appended % 100 == 0 or idx > end) {
            u.testPrint(
                "[diskwal_test] appendRange progress start={d} end={d} next_idx={d} appended={d}\n",
                .{ start, end, idx, appended },
            );
        }
        try log.addEntries(batch.items);
    }
}

fn checkRange(log: *raft.DiskWAL, lo: u64, hi: u64) !void {
    var result = log.entriesRange(.{ .lo = lo, .hi = hi }) catch |err| {
        std.debug.print("checkRange({d},{d}) entriesRange failed: {}\n", .{ lo, hi, err });
        return err;
    };
    defer result.deinit();
    if (result.entries.len != @as(usize, @intCast(hi - lo))) {
        std.debug.print("checkRange({d},{d}) expected {d} entries, got {d}\n", .{ lo, hi, hi - lo, result.entries.len });
        return error.TestExpectedEqual;
    }
    for (result.entries, 0..) |stored, i| {
        const idx = lo + @as(u64, @intCast(i));
        const reader = try zen.LogEntryReader.parseRoot(stored.bytes);
        try std.testing.expectEqual(idx, reader.index());
        const expected = if ((idx & 1) == 0) "p0" else "p1";
        try std.testing.expectEqualSlices(u8, expected, reader.data());
    }
}

fn fillLargePayload(buf: []u8, index: u64) void {
    // Use a deterministic pattern so we can validate payload integrity
    // after rotation and reopen without storing all expected entries.
    @memset(buf, @intCast(index % 251));
    const le = std.mem.toBytes(index);
    @memcpy(buf[0..8], &le);
}

fn checkLargePayload(reader: zen.LogEntryReader, index: u64, expected_len: usize) !void {
    const data = reader.data();
    try std.testing.expectEqual(expected_len, data.len);

    const expected_fill: u8 = @intCast(index % 251);
    // Spot-check beginning/middle/end, and verify the encoded index prefix.
    try std.testing.expectEqual(expected_fill, data[data.len / 2]);
    try std.testing.expectEqual(expected_fill, data[data.len - 1]);

    const got_index = std.mem.readInt(u64, data[0..8], .little);
    try std.testing.expectEqual(index, got_index);
}

fn countWalFiles(path: String) !usize {
    const alloc = std.testing.allocator;
    const entries = try fiber.Dir.cwd().list(alloc, path);
    defer fiber.freeDirEntries(alloc, entries);
    var count: usize = 0;
    for (entries) |e| {
        if (e.kind != .file) continue;
        if (std.mem.endsWith(u8, e.name, ".wal")) count += 1;
    }
    return count;
}

test "diskwal term and index basics" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
    defer ds.deinit();
    const log = &ds;

    try std.testing.expectEqual(@as(u64, 1), log.firstIndex());
    try std.testing.expectEqual(@as(u64, 0), log.lastIndex());
    try std.testing.expectEqual(@as(?u64, null), try log.term(1));

    try appendEntry(alloc, log, 1, 1, "a");
    try appendEntry(alloc, log, 2, 2, "bb");
    try appendEntry(alloc, log, 3, 2, "ccc");
    try log.sync();

    try std.testing.expectEqual(@as(u64, 1), log.firstIndex());
    try std.testing.expectEqual(@as(u64, 3), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 2), log.lastTerm());
    try std.testing.expectEqual(@as(?u64, 2), try log.term(3));
}

test "diskwal entriesRange max_bytes keeps at least one" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
    defer ds.deinit();
    const log = &ds;

    try appendEntry(alloc, log, 1, 1, "1111");
    try appendEntry(alloc, log, 2, 1, "2222");
    try appendEntry(alloc, log, 3, 1, "3333");
    try log.sync();

    var all = try log.entriesRange(.{ .lo = 1, .hi = 4 });
    defer all.deinit();
    try std.testing.expectEqual(@as(usize, 3), all.entries.len);

    // Even when max_bytes is tiny, API should return at least one entry.
    var limited = try log.entriesRange(.{ .lo = 1, .hi = 4, .max_bytes = 1 });
    defer limited.deinit();
    try std.testing.expectEqual(@as(usize, 1), limited.entries.len);
    const reader = try zen.LogEntryReader.parseRoot(limited.entries[0].bytes);
    try std.testing.expectEqualSlices(u8, "1111", reader.data());
}

test "diskwal entriesRange type filter reads only that type" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    // A cache of 8 entries: most of the log is read from disk, which
    // is the path the filter has to get right.
    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{
        .cache_capacity = 8,
    });
    defer ds.deinit();

    // Every 37th entry is a manifest; the rest are 4 KiB data entries.
    const n: u64 = 300;
    const big = "d" ** 4096;
    var idx: u64 = 1;
    while (idx <= n) : (idx += 1) {
        var name_buf: [16]u8 = undefined;
        const is_manifest = idx % 37 == 0;
        const data = if (is_manifest)
            try std.fmt.bufPrint(&name_buf, "m{d}", .{idx})
        else
            big;
        const ty: zen.EntryTypeEnum = if (is_manifest) .manifest else .data;
        var e = try makeTypedEntry(alloc, idx, ty, data);
        defer e.deinit();
        try ds.addEntries(&[_]zen.LogEntryBuffer{e});
    }
    try ds.sync();

    var result = try ds.entriesRange(.{
        .lo = 1,
        .hi = n + 1,
        .ty = .manifest,
    });
    defer result.deinit();
    try std.testing.expect(result.eof);
    try std.testing.expectEqual(n / 37, result.entries.len);
    for (result.entries, 1..) |stored, i| {
        try std.testing.expectEqual(i * 37, stored.index);
        const reader = try zen.LogEntryReader.parseRoot(stored.bytes);
        try std.testing.expectEqual(
            zen.EntryTypeEnum.manifest,
            reader.entryType(),
        );
        var want_buf: [16]u8 = undefined;
        const want = try std.fmt.bufPrint(&want_buf, "m{d}", .{i * 37});
        try std.testing.expectEqualStrings(want, reader.data());
    }

    // The unfiltered read still returns everything, cached or not.
    var all = try ds.entriesRange(.{ .lo = 1, .hi = n + 1, .max_bytes = 0 });
    defer all.deinit();
    try std.testing.expectEqual(n, all.entries.len);
    const last = try zen.LogEntryReader.parseRoot(all.entries[n - 1].bytes);
    try std.testing.expectEqualStrings(big, last.data());
}

test "diskwal append overwrites conflicting suffix" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
    defer ds.deinit();
    const log = &ds;

    try appendEntry(alloc, log, 1, 1, "1");
    try appendEntry(alloc, log, 2, 1, "2");
    try appendEntry(alloc, log, 3, 1, "3");

    try appendEntry(alloc, log, 2, 9, "2x");
    try appendEntry(alloc, log, 3, 9, "3x");
    try log.sync();

    try std.testing.expectEqual(@as(u64, 3), log.lastIndex());
    try std.testing.expectEqual(@as(?u64, 1), try log.term(1));
    try std.testing.expectEqual(@as(?u64, 9), try log.term(2));
    try std.testing.expectEqual(@as(?u64, 9), try log.term(3));

    var result = try log.entriesRange(.{ .lo = 1, .hi = 4 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 3), result.entries.len);
    const reader = try zen.LogEntryReader.parseRoot(result.entries[2].bytes);
    try std.testing.expectEqualSlices(u8, "3x", reader.data());
}

test "diskwal truncateFrom drops suffix" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
    defer ds.deinit();
    const log = &ds;

    try appendEntry(alloc, log, 1, 1, "a");
    try appendEntry(alloc, log, 2, 1, "b");
    try appendEntry(alloc, log, 3, 1, "c");
    try appendEntry(alloc, log, 4, 1, "d");
    try log.truncateFrom(3);

    try std.testing.expectEqual(@as(u64, 2), log.lastIndex());
    try std.testing.expectEqual(@as(?u64, null), try log.term(4));
}

test "diskwal persists hardstate and entries across reopen" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds.deinit();
        const log = &ds;
        try log.setHardState(.{
            .current_term = 7,
            .voted_for = 2,
        });
        try appendEntry(alloc, log, 1, 7, "a1");
        try appendEntry(alloc, log, 2, 7, "a2");
        try log.sync();
    }

    {
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds2.deinit();
        const log2 = &ds2;
        try std.testing.expectEqual(@as(u64, 2), log2.lastIndex());
        try std.testing.expectEqual(@as(?u64, 7), try log2.term(2));
        const hs = log2.hardState();
        try std.testing.expectEqual(@as(u64, 7), hs.current_term);
        try std.testing.expectEqual(@as(u64, 2), hs.voted_for);
    }
}

test "diskwal drops malformed trailing wal files on reopen" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    const payload_size: usize = 4 * 1024 * 1024;
    const payload = try alloc.alloc(u8, payload_size);
    defer alloc.free(payload);

    // Write enough entries to trigger a rotation, producing at least 2 WAL files.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds.deinit();
        const log = &ds;
        for (1..70) |i| {
            fillLargePayload(payload, @intCast(i));
            try appendEntry(alloc, log, @intCast(i), 1, payload);
        }
        try log.sync();
        try std.testing.expect(try countWalFiles(wal_dir) >= 2);
    }

    // Corrupt the header of the last (current) file by flipping bytes in the magic.
    {
        const entries = try fiber.Dir.cwd().list(alloc, wal_dir);
        defer fiber.freeDirEntries(alloc, entries);

        // Find the highest fid file.
        var max_fid: u64 = 0;
        for (entries) |e| {
            if (e.kind != .file) continue;
            const fid = raft.diskwal.wal.parseWalFid(e.name) orelse continue;
            if (fid > max_fid) max_fid = fid;
        }
        try std.testing.expect(max_fid > 1);

        // Corrupt the highest file's magic bytes.
        const bad_path = try std.fmt.allocPrint(alloc, "{s}/{d:0>5}.wal", .{ wal_dir, max_fid });
        defer alloc.free(bad_path);
        const io = utils.context.g().io;
        var file = try std.Io.Dir.cwd().openFile(io, bad_path, .{ .mode = .read_write });
        defer file.close(io);
        var garbage = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF };
        _ = try file.writePositionalAll(io, &garbage, 0);
    }

    // Reopen — the malformed current file should be dropped. The previous
    // rotated file becomes current and its entries should survive.
    {
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds2.deinit();
        const log = &ds2;
        // Should have entries from the rotated file (at least the first log file's worth).
        try std.testing.expect(log.lastIndex() > 0);
    }
}

test "diskwal tolerates WAL firstIndex below snapshot+1 on reopen" {
    // Crash recovery scenario: meta-store records a snapshot at index N,
    // but WAL compaction (which deletes pre-snapshot files) hadn't
    // completed before the kill. On reopen we should accept the WAL —
    // the extra pre-snapshot entries are harmless because apply_index
    // initializes to snap.index, skipping them. Only a true gap
    // (firstIndex > snap.index+1, missing entries) is data loss.
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds.deinit();
        const log = &ds;
        try log.setSnapshot(.{ .index = 5, .term = 2, .data = "snap" });
        try log.sync();
    }

    // Used to error.CorruptLogFile; now succeeds.
    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
    defer ds.deinit();
}

test "BIG: diskwal storage" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    // Keep this large enough to cross a log file boundary.
    const N: u64 = 62_000;

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds.deinit();
        const log = &ds;

        try appendRange(alloc, log, 1, N, 1);
        try std.testing.expectEqual(N, log.lastIndex());

        try checkRange(log, 1, 2);
        try checkRange(log, 1, N + 1);

        // Jump back by 2001 to cross the 60k boundary, then append 100 entries.
        const jump_start: u64 = N - 2001; // 59999 when N=62000
        const jump_end: u64 = jump_start + 100; // 60099
        try appendRange(alloc, log, jump_start, jump_start, 3);
        try std.testing.expectEqual(jump_start, log.lastIndex());

        try appendRange(alloc, log, jump_start + 1, jump_end, 3);
        try std.testing.expectEqual(jump_end, log.lastIndex());

        try checkRange(log, jump_start, jump_end + 1);
        try std.testing.expectEqual(@as(?u64, 3), try log.term(jump_start));
        try std.testing.expectEqual(@as(?u64, 3), try log.term(jump_end));

        // Jump back again to 60000 and overwrite the same 100-entry window.
        const overwrite_start: u64 = 60_000;
        const overwrite_end: u64 = overwrite_start + 99;
        try appendRange(alloc, log, overwrite_start, overwrite_end, 4);
        try std.testing.expectEqual(overwrite_end, log.lastIndex());
        try checkRange(log, overwrite_start, overwrite_end + 1);
        try std.testing.expectEqual(@as(?u64, 4), try log.term(overwrite_start));
        try std.testing.expectEqual(@as(?u64, 4), try log.term(overwrite_end));
        try log.sync();
    }

    {
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds2.deinit();
        const log2 = &ds2;
        try std.testing.expectEqual(@as(u64, 60_099), log2.lastIndex());
        try checkRange(log2, 1, 60_100);
        try std.testing.expectEqual(@as(?u64, 4), try log2.term(60_000));
        try std.testing.expectEqual(@as(?u64, 4), try log2.term(60_099));
    }
}

test "BIG: diskwal truncate storage" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    // 17 MiB log files (1 MiB past the 16 MiB header): about 16k of
    // these ~64-byte records each.
    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{
        .log_file_size = 17 << 20,
    });
    defer ds.deinit();
    const log = &ds;

    const per_log_file: u64 = 20_000;
    const num_entries: u64 = (per_log_file * 3) / 2;
    const compact_to: u64 = per_log_file + 10;

    // Fill across log file boundaries.
    try appendRange(alloc, log, 1, num_entries, 1);
    try std.testing.expectEqual(num_entries, log.lastIndex());
    try checkRange(log, 1, num_entries + 1);

    // Compaction here is log file-granular. Compact beyond first log file so
    // early indices become unavailable.
    try log.compactUntil(compact_to);
    try std.testing.expectEqual(@as(?u64, null), try log.term(1));
    const first_after_compact = log.firstIndex();
    try checkRange(log, first_after_compact, num_entries + 1);

    // Append one more full-log file worth and verify full retained range.
    try appendRange(alloc, log, num_entries + 1, num_entries + per_log_file, 2);
    const new_last = num_entries + per_log_file;
    try std.testing.expectEqual(new_last, log.lastIndex());
    try std.testing.expectEqual(@as(?u64, 2), try log.term(new_last));
    try checkRange(log, first_after_compact, new_last + 1);
    try checkRange(log, new_last - 100, new_last + 1);
}

test "diskwal rotated file with a torn header is rebuilt from its records" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    // 32 MiB files (16 MiB of records past the header region) and
    // 1 MiB entries: 16 per file, so 40 entries make three files.
    const opts: raft.diskwal.OpenOptions = .{ .log_file_size = 32 << 20 };
    const payload = try alloc.alloc(u8, 1 << 20);
    defer alloc.free(payload);
    const total: u64 = 40;
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
        defer ds.deinit();
        var idx: u64 = 1;
        while (idx <= total) : (idx += 1) {
            fillLargePayload(payload, idx);
            try appendEntry(alloc, &ds, idx, 1, payload);
        }
        try ds.sync();
        // At least: a fill that loses to the appends rotates once more.
        try std.testing.expect(try countWalFiles(wal_dir) >= 3);
    }

    // A crash between the rotation and the writer's finalization of the
    // outgoing file leaves its header unwritten: zero the first file's.
    const first_path = try std.fmt.allocPrint(alloc, "{s}/00001.wal", .{wal_dir});
    defer alloc.free(first_path);
    const io = utils.context.g().io;
    {
        var file = try std.Io.Dir.cwd().openFile(io, first_path, .{
            .mode = .read_write,
        });
        defer file.close(io);
        const zeros = [_]u8{0} ** 4096;
        try file.writePositionalAll(io, &zeros, 0);
    }

    // Reopen: every entry is there, the first file's from a scan.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
        defer ds.deinit();
        try std.testing.expectEqual(total, ds.lastIndex());
        var cross = try ds.entriesRange(.{ .lo = 1, .hi = total + 1 });
        defer cross.deinit();
        try std.testing.expectEqual(@as(usize, total), cross.entries.len);
        for (cross.entries, 1..) |stored, i| {
            try std.testing.expectEqual(i, stored.index);
            const reader = try zen.LogEntryReader.parseRoot(stored.bytes);
            fillLargePayload(payload, i);
            try std.testing.expectEqualSlices(u8, payload, reader.data());
        }
    }

    // The scan wrote the header back, so the next open reads it.
    {
        var file = try std.Io.Dir.cwd().openFile(io, first_path, .{});
        defer file.close(io);
        var prefix: [zbuf.Frame.hdr_size]u8 = undefined;
        _ = try file.readPositionalAll(io, &prefix, 0);
        try std.testing.expect(!std.mem.allEqual(u8, &prefix, 0));
    }
}

test "diskwal rotated file left with its empty header is rebuilt too" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    // As above: 40 entries of 1 MiB in three 32 MiB files.
    const opts: raft.diskwal.OpenOptions = .{ .log_file_size = 32 << 20 };
    const payload = try alloc.alloc(u8, 1 << 20);
    defer alloc.free(payload);
    const total: u64 = 40;
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
        defer ds.deinit();
        var idx: u64 = 1;
        while (idx <= total) : (idx += 1) {
            fillLargePayload(payload, idx);
            try appendEntry(alloc, &ds, idx, 1, payload);
        }
        try ds.sync();
    }

    // What that crash really leaves at the head of the outgoing file:
    // not zeros, but the header it was created with, a valid frame
    // that lists no record. Put that back over the first file's.
    const first_path = try std.fmt.allocPrint(
        alloc,
        "{s}/00001.wal",
        .{wal_dir},
    );
    defer alloc.free(first_path);
    const io = utils.context.g().io;
    {
        const created = raft.diskwal.LogFile{
            .fid = 1,
            .handle = undefined,
            .allocator = alloc,
            .slots = .empty,
            .write_offset = raft.diskwal.log_file_offset,
            .is_current = true,
        };
        const frame = try created.encodeHeader();
        defer alloc.free(frame);
        var page = [_]u8{0} ** 4096;
        @memcpy(page[0..frame.len], frame);

        var file = try std.Io.Dir.cwd().openFile(io, first_path, .{
            .mode = .read_write,
        });
        defer file.close(io);
        try file.writePositionalAll(io, &page, 0);
    }

    // Reopen: every entry is there, the first file's from a scan.
    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
    defer ds.deinit();
    try std.testing.expectEqual(total, ds.lastIndex());
    var all = try ds.entriesRange(.{ .lo = 1, .hi = total + 1 });
    defer all.deinit();
    try std.testing.expectEqual(@as(usize, total), all.entries.len);
    for (all.entries, 1..) |stored, i| {
        try std.testing.expectEqual(i, stored.index);
        const reader = try zen.LogEntryReader.parseRoot(stored.bytes);
        fillLargePayload(payload, i);
        try std.testing.expectEqualSlices(u8, payload, reader.data());
    }
}

test "diskwal rotated file with no record at all is dropped at open" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    // 12 entries of 300 KiB in 17 MiB files (1 MiB past the header,
    // the minimum): 3 per file. The next file's 1 MiB zero-fill runs
    // at p2 through a 1 MiB/s limiter whose one-second bucket the
    // open's own fill empties, so it takes about a second and the
    // first rotation, a few ms in, finds it unready: an unfilled file
    // under a skipped fid, as on a slow disk. The layout is read
    // back, not assumed.
    var limiter = vfs.bandwidth.Limiter.init(.{
        .bytes_per_sec = 1 << 20,
        .write_cost = 1,
    });
    const wal_vfs = vfs.VFS.new(&limiter);
    const opts: raft.diskwal.OpenOptions = .{
        .log_file_size = 17 << 20,
        .prezero = .true,
        .vfs = &wal_vfs,
    };
    const payload = try alloc.alloc(u8, 300 << 10);
    defer alloc.free(payload);
    var first_last: u64 = 0;
    var second_fid: u64 = 0;
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
        defer ds.deinit();
        var idx: u64 = 1;
        while (idx <= 12) : (idx += 1) {
            fillLargePayload(payload, idx);
            try appendEntry(alloc, &ds, idx, 1, payload);
        }
        try ds.sync();
        const files = ds.wal_store.files.items;
        first_last = files[0].lastEntry().?.index;
        second_fid = files[1].fid;
    }

    // The second file as a crash can leave one whose records never
    // reached the disk: the empty header, and no record after it.
    var name_buf: raft.diskwal.LogFile.NameBuf = undefined;
    const second_name = try raft.diskwal.LogFile.fname(
        second_fid,
        &name_buf,
    );
    const second_path = try raft.diskwal.LogFile.fullPath(
        alloc,
        wal_dir,
        second_name,
    );
    defer alloc.free(second_path);
    const io = utils.context.g().io;
    {
        const created = raft.diskwal.LogFile{
            .fid = second_fid,
            .handle = undefined,
            .allocator = alloc,
            .slots = .empty,
            .write_offset = raft.diskwal.log_file_offset,
            .is_current = true,
        };
        const frame = try created.encodeHeader();
        defer alloc.free(frame);
        var page = [_]u8{0} ** 4096;
        @memcpy(page[0..frame.len], frame);
        const zeros = [_]u8{0} ** 4096;

        var file = try std.Io.Dir.cwd().openFile(io, second_path, .{
            .mode = .read_write,
        });
        defer file.close(io);
        try file.writePositionalAll(io, &page, 0);
        try file.writePositionalAll(
            io,
            &zeros,
            raft.diskwal.log_file_offset,
        );
    }

    // Reopen: the empty file is gone, and the log ends where the gap
    // it left begins, at the first file's last entry.
    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
    defer ds.deinit();
    try std.testing.expect(!try dirHas(wal_dir, second_name));
    try std.testing.expectEqual(first_last, ds.lastIndex());
    try std.testing.expectEqual(@as(?u64, 1), try ds.term(first_last));
    try std.testing.expectEqual(
        @as(?u64, null),
        try ds.term(first_last + 1),
    );
}

test "BIG: diskwal large payload rotation" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    const payload_size = 4 * 1024 * 1024;
    const total_entries: u64 = 150;
    const payload = try alloc.alloc(u8, payload_size);
    defer alloc.free(payload);

    {
        // No prezero: a fill that falls behind on a slow disk makes
        // the wal rotate onto an unfilled file and again when the
        // prepared one is ready, and the file count below is exact.
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{
            .prezero = .false,
        });
        defer ds.deinit();
        const log = &ds;

        var idx: u64 = 1;
        while (idx <= total_entries) : (idx += 1) {
            fillLargePayload(payload, idx);
            // Use term=index to exercise varying term persistence across log files.
            try appendEntry(alloc, log, idx, idx, payload);
        }
        try log.sync();

        try std.testing.expectEqual(total_entries, log.lastIndex());

        // 150 x 4 MiB fill 3 log files; a fill that loses to the appends
        // rotates once more, so the count is a floor.
        try std.testing.expect(try countWalFiles(wal_dir) >= 3);

        // Validate reads that cross file boundaries.
        var cross_1 = try log.entriesRange(.{ .lo = 60, .hi = 67 });
        defer cross_1.deinit();
        try std.testing.expectEqual(@as(usize, 7), cross_1.entries.len);
        for (cross_1.entries, 0..) |stored, i| {
            const expected_index = @as(u64, 60) + @as(u64, @intCast(i));
            const reader = try zen.LogEntryReader.parseRoot(stored.bytes);
            try std.testing.expectEqual(expected_index, reader.index());
            try std.testing.expectEqual(expected_index, reader.term());
            try checkLargePayload(reader, expected_index, payload_size);
        }

        var cross_2 = try log.entriesRange(.{ .lo = 124, .hi = 131 });
        defer cross_2.deinit();
        try std.testing.expectEqual(@as(usize, 7), cross_2.entries.len);
        for (cross_2.entries, 0..) |stored, i| {
            const expected_index = @as(u64, 124) + @as(u64, @intCast(i));
            const reader = try zen.LogEntryReader.parseRoot(stored.bytes);
            try std.testing.expectEqual(expected_index, reader.index());
            try std.testing.expectEqual(expected_index, reader.term());
            try checkLargePayload(reader, expected_index, payload_size);
        }

        // Even with a very small max_bytes, API should return at least one entry.
        var limited = try log.entriesRange(.{ .lo = 1, .hi = 10, .max_bytes = 1024 * 1024 });
        defer limited.deinit();
        try std.testing.expectEqual(@as(usize, 1), limited.entries.len);
        const limited_reader = try zen.LogEntryReader.parseRoot(limited.entries[0].bytes);
        try std.testing.expectEqual(@as(u64, 1), limited_reader.index());
        try checkLargePayload(limited_reader, 1, payload_size);
    }

    // Reopen and verify recovery paths over the same boundary ranges.
    {
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds2.deinit();
        const log2 = &ds2;

        try std.testing.expectEqual(total_entries, log2.lastIndex());
        try std.testing.expectEqual(@as(?u64, 1), try log2.term(1));
        try std.testing.expectEqual(@as(?u64, 63), try log2.term(63));
        try std.testing.expectEqual(@as(?u64, 64), try log2.term(64));
        try std.testing.expectEqual(@as(?u64, 126), try log2.term(126));
        try std.testing.expectEqual(@as(?u64, 127), try log2.term(127));
        try std.testing.expectEqual(@as(?u64, 150), try log2.term(150));

        var cross = try log2.entriesRange(.{ .lo = 124, .hi = 131 });
        defer cross.deinit();
        try std.testing.expectEqual(@as(usize, 7), cross.entries.len);
        for (cross.entries, 0..) |stored, i| {
            const expected_index = @as(u64, 124) + @as(u64, @intCast(i));
            const reader = try zen.LogEntryReader.parseRoot(stored.bytes);
            try std.testing.expectEqual(expected_index, reader.index());
            try std.testing.expectEqual(expected_index, reader.term());
            try checkLargePayload(reader, expected_index, payload_size);
        }

        // Overwrite from an older index after reopen. This should truncate the
        // suffix and drop the highest log file, leaving two WAL files.
        fillLargePayload(payload, 100);
        try appendEntry(alloc, log2, 100, 100, payload);
        try log2.sync();

        try std.testing.expectEqual(@as(u64, 100), log2.lastIndex());
        try std.testing.expectEqual(@as(?u64, null), try log2.term(150));
        try std.testing.expectEqual(@as(usize, 2), try countWalFiles(wal_dir));
    }
}

// =========================================================================
// EntryCache ring buffer tests
// =========================================================================

const EntryCache = raft.diskwal.EntryCache;

test "entry cache basic append and lookup" {
    const alloc = std.testing.allocator;
    var cache = try EntryCache.init(alloc, 8);
    defer cache.deinit();

    try cache.append(1, 1, "hello");
    try cache.append(2, 1, "world");
    try cache.append(3, 2, "foo");

    // Hits.
    const e1 = cache.lookup(1).?;
    try std.testing.expectEqual(@as(u64, 1), e1.index);
    try std.testing.expectEqualSlices(u8, "hello", e1.bytes);

    const e3 = cache.lookup(3).?;
    try std.testing.expectEqual(@as(u64, 3), e3.index);
    try std.testing.expectEqual(@as(u64, 2), e3.term);

    // Misses.
    try std.testing.expect(cache.lookup(0) == null);
    try std.testing.expect(cache.lookup(4) == null);
}

test "entry cache ring wraparound evicts oldest" {
    const alloc = std.testing.allocator;
    var cache = try EntryCache.init(alloc, 4);
    defer cache.deinit();

    // Fill to capacity.
    try cache.append(10, 1, "a");
    try cache.append(11, 1, "b");
    try cache.append(12, 1, "c");
    try cache.append(13, 1, "d");
    try std.testing.expectEqual(@as(usize, 4), cache.len);

    // Append beyond capacity — oldest (index 10) should be evicted.
    try cache.append(14, 1, "e");
    try std.testing.expectEqual(@as(usize, 4), cache.len);
    try std.testing.expect(cache.lookup(10) == null);
    try std.testing.expectEqualSlices(u8, "b", cache.lookup(11).?.bytes);
    try std.testing.expectEqualSlices(u8, "e", cache.lookup(14).?.bytes);

    // Two more — evicts 11 and 12.
    try cache.append(15, 1, "f");
    try cache.append(16, 1, "g");
    try std.testing.expect(cache.lookup(11) == null);
    try std.testing.expect(cache.lookup(12) == null);
    try std.testing.expectEqualSlices(u8, "d", cache.lookup(13).?.bytes);
    try std.testing.expectEqualSlices(u8, "g", cache.lookup(16).?.bytes);
}

test "entry cache truncateFrom drops tail" {
    const alloc = std.testing.allocator;
    var cache = try EntryCache.init(alloc, 8);
    defer cache.deinit();

    try cache.append(1, 1, "a");
    try cache.append(2, 1, "b");
    try cache.append(3, 1, "c");
    try cache.append(4, 1, "d");

    // Truncate from index 3 — keep 1 and 2.
    cache.truncateFrom(3);
    try std.testing.expectEqual(@as(usize, 2), cache.len);
    try std.testing.expectEqualSlices(u8, "a", cache.lookup(1).?.bytes);
    try std.testing.expectEqualSlices(u8, "b", cache.lookup(2).?.bytes);
    try std.testing.expect(cache.lookup(3) == null);
    try std.testing.expect(cache.lookup(4) == null);

    // Truncate from index 1 — clears everything.
    cache.truncateFrom(1);
    try std.testing.expectEqual(@as(usize, 0), cache.len);
}

test "entry cache evictBefore drops head" {
    const alloc = std.testing.allocator;
    var cache = try EntryCache.init(alloc, 8);
    defer cache.deinit();

    try cache.append(5, 1, "a");
    try cache.append(6, 1, "b");
    try cache.append(7, 1, "c");
    try cache.append(8, 1, "d");

    // Evict entries with index < 7.
    cache.evictBefore(7);
    try std.testing.expectEqual(@as(usize, 2), cache.len);
    try std.testing.expect(cache.lookup(5) == null);
    try std.testing.expect(cache.lookup(6) == null);
    try std.testing.expectEqualSlices(u8, "c", cache.lookup(7).?.bytes);
    try std.testing.expectEqualSlices(u8, "d", cache.lookup(8).?.bytes);

    // Evict beyond what exists — clears all.
    cache.evictBefore(100);
    try std.testing.expectEqual(@as(usize, 0), cache.len);
}

test "entry cache clear frees all entries" {
    const alloc = std.testing.allocator;
    var cache = try EntryCache.init(alloc, 4);
    defer cache.deinit();

    try cache.append(1, 1, "abc");
    try cache.append(2, 1, "def");
    try std.testing.expectEqual(@as(usize, 2), cache.len);

    cache.clear();
    try std.testing.expectEqual(@as(usize, 0), cache.len);
    try std.testing.expect(cache.lookup(1) == null);
}

test "entry cache wraparound with truncateFrom and evictBefore" {
    const alloc = std.testing.allocator;
    var cache = try EntryCache.init(alloc, 4);
    defer cache.deinit();

    // Fill and wrap around so head != 0.
    try cache.append(1, 1, "a");
    try cache.append(2, 1, "b");
    try cache.append(3, 1, "c");
    try cache.append(4, 1, "d");
    try cache.append(5, 1, "e"); // evicts index 1, head wraps
    try cache.append(6, 1, "f"); // evicts index 2

    // Now cache has [3,4,5,6] with head wrapped.
    try std.testing.expectEqual(@as(usize, 4), cache.len);
    try std.testing.expect(cache.lookup(1) == null);
    try std.testing.expect(cache.lookup(2) == null);
    try std.testing.expectEqualSlices(u8, "c", cache.lookup(3).?.bytes);
    try std.testing.expectEqualSlices(u8, "f", cache.lookup(6).?.bytes);

    // truncateFrom in wrapped state.
    cache.truncateFrom(5);
    try std.testing.expectEqual(@as(usize, 2), cache.len);
    try std.testing.expectEqualSlices(u8, "c", cache.lookup(3).?.bytes);
    try std.testing.expectEqualSlices(u8, "d", cache.lookup(4).?.bytes);
    try std.testing.expect(cache.lookup(5) == null);

    // evictBefore in wrapped state.
    cache.evictBefore(4);
    try std.testing.expectEqual(@as(usize, 1), cache.len);
    try std.testing.expectEqualSlices(u8, "d", cache.lookup(4).?.bytes);
}

// =========================================================================
// CRC recovery, CURRENT.wal, and rotation tests
// =========================================================================

const diskwal = raft.diskwal;

test "diskwal CURRENT.wal recovery after clean close" {
    // Append entries without rotating, close, reopen — all entries should be
    // recovered by scanning CRC-framed records from CURRENT.wal.
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    // Write 5 entries and close.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds.deinit();
        const log = &ds;
        try appendRange(alloc, log, 1, 5, 1);
        try log.sync();
    }

    // Reopen — CURRENT.wal should be recovered via CRC scan.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds.deinit();
        const log = &ds;

        try std.testing.expectEqual(@as(u64, 5), log.lastIndex());
        try std.testing.expectEqual(@as(?u64, 1), try log.term(1));
        try std.testing.expectEqual(@as(?u64, 1), try log.term(5));
        try checkRange(log, 1, 6);
    }
}

test "diskwal CRC corruption truncates at corrupt entry" {
    // Write entries, corrupt a byte in the payload region of CURRENT.wal,
    // reopen — recovery should stop at the corrupted entry.
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    // Write 5 entries.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds.deinit();
        const log = &ds;
        try appendRange(alloc, log, 1, 5, 1);
        try log.sync();
    }

    // Corrupt the 3rd entry's payload by flipping a byte.
    // Entries start at log_file_offset. Each entry is framed as
    // `[u64 hash | u32 len | payload]` — see zbuf.Frame.hdr_size.
    {
        // The current file is 00001.wal (no rotation happened).
        const current_path = try std.fmt.allocPrint(alloc, "{s}/00001.wal", .{wal_dir});
        defer alloc.free(current_path);
        const io = utils.context.g().io;
        var file = try std.Io.Dir.cwd().openFile(io, current_path, .{ .mode = .read_write });
        defer file.close(io);

        // Read past the first two entries to find the 3rd entry's payload.
        const hdr_size: usize = zbuf.Frame.hdr_size;
        var offset: u64 = diskwal.log_file_offset;
        var hdr_buf: [zbuf.Frame.hdr_size]u8 = undefined;
        for (0..2) |_| {
            _ = try file.readPositionalAll(utils.context.g().io, &hdr_buf, offset);
            // u32 length lives at byte 8 of the prefix (after the u64 hash).
            const payload_len = std.mem.readInt(u32, hdr_buf[8..12], .little);
            offset += hdr_size + payload_len;
        }
        // Now offset points at the 3rd entry. Corrupt a byte in its payload.
        const corrupt_offset = offset + hdr_size + 1; // +1 to hit inside the payload
        var byte: [1]u8 = undefined;
        _ = try file.readPositionalAll(utils.context.g().io, &byte, corrupt_offset);
        byte[0] ^= 0xFF; // flip all bits
        _ = try file.writePositionalAll(utils.context.g().io, &byte, corrupt_offset);
    }

    // Reopen — should recover only entries 1 and 2.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds.deinit();
        const log = &ds;

        try std.testing.expectEqual(@as(u64, 2), log.lastIndex());
        try checkRange(log, 1, 3);
        // Entry 3 and beyond should be gone.
        try std.testing.expectEqual(@as(?u64, null), try log.term(3));
    }
}

test "diskwal writeHeader/readHeader round-trip" {
    // Drive writeHeader and readHeader directly (no DiskWAL wrapper, no
    // data-region records). Verifies that a LogFile's in-memory slots
    // survive a serialize/deserialize cycle through the on-disk WalHeader,
    // covering both the empty-slots case (freshly created) and the
    // populated case (multiple appended data slots).
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    const vfs_inst = vfs.g();

    // Log file functions take the setting already resolved by the wal.
    const lf_opts: raft.diskwal.OpenOptions = .{ .log_file_size = 20 << 20, .prezero = .false };
    var lf = try raft.diskwal.LogFile.createCurrent(
        vfs_inst,
        alloc,
        wal_dir,
        1,
        lf_opts,
    );
    defer lf.deinit();

    // Empty case: createCurrent writes a WalHeader with no data slots.
    {
        var slots = try raft.diskwal.LogFile.readHeader(alloc, lf.handle);
        defer slots.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), slots.items.len);
    }

    // Populated case: append slot metadata and re-persist via writeHeader.
    // Test slots can carry arbitrary data_offset/data_len/entry_type
    // values — the data region is never touched by either function.
    try lf.slots.append(alloc, .{
        .term = 5,
        .index = 101,
        .timestamp = 0xDEAD_BEF0,
        .data_offset = diskwal.log_file_offset,
        .data_len = 42,
        .entry_type = zen.EntryTypeEnum.data,
    });
    try lf.slots.append(alloc, .{
        .term = 6,
        .index = 102,
        .timestamp = 0xDEAD_BEF1,
        .data_offset = diskwal.log_file_offset + 50,
        .data_len = 100,
        .entry_type = zen.EntryTypeEnum.conf,
    });
    try lf.writeHeader();

    {
        var slots = try raft.diskwal.LogFile.readHeader(alloc, lf.handle);
        defer slots.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 2), slots.items.len);

        try std.testing.expectEqual(@as(u64, 5), slots.items[0].term);
        try std.testing.expectEqual(@as(u64, 101), slots.items[0].index);
        try std.testing.expectEqual(@as(u64, 0xDEAD_BEF0), slots.items[0].timestamp);
        const first_offset: u32 = diskwal.log_file_offset;
        try std.testing.expectEqual(first_offset, slots.items[0].data_offset);
        try std.testing.expectEqual(@as(u32, 42), slots.items[0].data_len);
        try std.testing.expectEqual(zen.EntryTypeEnum.data, slots.items[0].entry_type);

        try std.testing.expectEqual(@as(u64, 6), slots.items[1].term);
        try std.testing.expectEqual(@as(u64, 102), slots.items[1].index);
        try std.testing.expectEqual(@as(u64, 0xDEAD_BEF1), slots.items[1].timestamp);
        const second_offset: u32 = diskwal.log_file_offset + 50;
        try std.testing.expectEqual(second_offset, slots.items[1].data_offset);
        try std.testing.expectEqual(@as(u32, 100), slots.items[1].data_len);
        try std.testing.expectEqual(zen.EntryTypeEnum.conf, slots.items[1].entry_type);
    }
}

fn makeLogEntryWithTimestamp(
    alloc: std.mem.Allocator,
    index: u64,
    term: u64,
    timestamp: u64,
    data: String,
) !zen.LogEntryBuffer {
    var writer = try zen.LogEntryWriter.init(alloc);
    defer writer.deinit();
    try writer.set(.{
        .index = index,
        .term = term,
        .timestamp = timestamp,
        .data = data,
        .entry_type = zen.EntryTypeEnum.data,
        .conf_change = zen.NullOffset,
    });
    return writer.finishWithTail();
}

// A log entry's data, stored as `Node.propose` stores it and read back
// as its readers do: compressed when `maybePack` chose to, and equal to
// what was proposed either way. An entry with no codec byte, from a
// writer that never sets it, reads as it is.
test "log entry: data round-trips through its codec byte" {
    const alloc = std.testing.allocator;
    const compression = utils.compression;
    const rule: compression.PackRule = .{
        .codec = .lz4,
        .min_bytes = 512,
        .min_saving = 1.0 / 8.0,
    };
    const cases = [_]struct { data: String, codec: compression.Codec }{
        .{ .data = "the quick brown fox. " ** 200, .codec = .lz4 },
        .{ .data = "small", .codec = .none },
        .{ .data = "", .codec = .none },
    };
    var pack_scratch: std.ArrayList(u8) = .empty;
    defer pack_scratch.deinit(alloc);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(alloc);
    for (cases) |case| {
        const stored = try compression.maybePack(
            alloc,
            case.data,
            rule,
            &pack_scratch,
        );
        try std.testing.expectEqual(case.codec, stored.codec);

        var writer = try zen.LogEntryWriter.init(alloc);
        defer writer.deinit();
        try writer.set(.{
            .index = 1,
            .term = 1,
            .timestamp = 1,
            .data = stored.data,
            .entry_type = zen.EntryTypeEnum.data,
            .conf_change = zen.NullOffset,
            .codec = @intFromEnum(stored.codec),
        });
        var entry = try writer.finishWithTail();
        defer entry.deinit();
        const reader = try entry.initReader();
        const got = try compression.maybeUnpack(
            alloc,
            reader.codec(),
            reader.data(),
            &scratch,
        );
        try std.testing.expectEqualStrings(case.data, got);
    }

    var plain = try makeLogEntry(alloc, 1, 1, "plain");
    defer plain.deinit();
    const reader = try plain.initReader();
    try std.testing.expectEqual(@as(u8, 0), reader.codec());
    const got = try compression.maybeUnpack(
        alloc,
        reader.codec(),
        reader.data(),
        &scratch,
    );
    try std.testing.expectEqualStrings("plain", got);
}

// A snapshot takes its term and timestamp from the slot of the entry
// at its index, which is nearly always a data entry: the lookup must
// answer for any type and say so when the index is not in the log.
test "diskwal slotAt: term and timestamp of any entry" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
    defer ds.deinit();

    for (1..6) |i| {
        var entry = try makeLogEntryWithTimestamp(
            alloc,
            @intCast(i),
            7,
            1000 + i,
            "payload",
        );
        defer entry.deinit();
        try ds.addEntries(&[_]zen.LogEntryBuffer{entry});
    }

    for ([_]u64{ 1, 4, 5 }) |idx| {
        const slot = (try ds.slotAt(idx)).?;
        try std.testing.expectEqual(idx, slot.index);
        try std.testing.expectEqual(@as(u64, 7), slot.term);
        try std.testing.expectEqual(1000 + idx, slot.timestamp);
        try std.testing.expectEqual(zen.EntryTypeEnum.data, slot.entry_type);
    }
    // Not in the log: index 0 is no entry, 6 is past the head.
    try std.testing.expect((try ds.slotAt(0)) == null);
    try std.testing.expect((try ds.slotAt(6)) == null);
}

test "diskwal findIndexOlderThan bytes-only branch" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
    defer ds.deinit();

    // 10 entries with 1 KiB payload each = 10 KiB total tail.
    // Timestamps left at 0 so age check is bypassed (HLC sentinel).
    const payload = try alloc.alloc(u8, 1024);
    defer alloc.free(payload);
    @memset(payload, 0xab);
    for (1..11) |i| {
        var entry = try makeLogEntryWithTimestamp(alloc, @intCast(i), 1, 0, payload);
        defer entry.deinit();
        try ds.addEntries(&[_]zen.LogEntryBuffer{entry});
    }

    // Threshold larger than the whole tail → null.
    try std.testing.expectEqual(@as(?u64, null), ds.findIndexOlderThan(1 << 20, std.math.maxInt(u64)));

    // Need ≥ 3 KiB from head — slot 10 (1 KiB), 9 (2 KiB), 8 (3 KiB) → returns 8.
    try std.testing.expectEqual(@as(?u64, 8), ds.findIndexOlderThan(3 * 1024, std.math.maxInt(u64)));

    // Need ≥ 1 byte → first slot walked (slot 10) wins.
    try std.testing.expectEqual(@as(?u64, 10), ds.findIndexOlderThan(1, std.math.maxInt(u64)));
}

test "diskwal findIndexOlderThan age-only branch" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
    defer ds.deinit();

    const now_secs: u32 = @intCast(std.Io.Timestamp.now(utils.context.g().io, .real).toSeconds());
    // Slot 1: 100 s ago. Slot 2: 50 s ago. Slot 3: 10 s ago.
    const ages = [_]u32{ 100, 50, 10 };
    for (ages, 1..) |age, i| {
        const ts: u64 = (raft.utils.hlc.HLC{ .seconds = now_secs - age, .logical = 1 }).asU64();
        var entry = try makeLogEntryWithTimestamp(alloc, @intCast(i), 1, ts, "x");
        defer entry.deinit();
        try ds.addEntries(&[_]zen.LogEntryBuffer{entry});
    }

    // Need entries ≥ 60 s old. Walking newest-first: slot 3 (10s) no, slot 2
    // (50s) no, slot 1 (100s) yes → returns 1.
    try std.testing.expectEqual(@as(?u64, 1), ds.findIndexOlderThan(std.math.maxInt(u64), 60));

    // Need ≥ 30 s old. Slot 3 (10s) no, slot 2 (50s) yes → returns 2.
    try std.testing.expectEqual(@as(?u64, 2), ds.findIndexOlderThan(std.math.maxInt(u64), 30));

    // Need ≥ 5 s old. Slot 3 (10s) yes → returns 3.
    try std.testing.expectEqual(@as(?u64, 3), ds.findIndexOlderThan(std.math.maxInt(u64), 5));

    // Need ≥ 1000 s old. Nothing qualifies → null.
    try std.testing.expectEqual(@as(?u64, null), ds.findIndexOlderThan(std.math.maxInt(u64), 1000));
}

test "diskwal findIndexOlderThan returns larger of bytes and age" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
    defer ds.deinit();

    const now_secs: u32 = @intCast(std.Io.Timestamp.now(utils.context.g().io, .real).toSeconds());
    // 5 entries, 1 KiB each. Slot 1 is the oldest (100 s). Slots 2–5 are fresh.
    const payload = try alloc.alloc(u8, 1024);
    defer alloc.free(payload);
    @memset(payload, 0xcd);
    for (1..6) |i| {
        const age: u32 = if (i == 1) 100 else 0;
        const ts: u64 = (raft.utils.hlc.HLC{ .seconds = now_secs - age, .logical = 1 }).asU64();
        var entry = try makeLogEntryWithTimestamp(alloc, @intCast(i), 1, ts, payload);
        defer entry.deinit();
        try ds.addEntries(&[_]zen.LogEntryBuffer{entry});
    }

    // Bytes: need ≥ 3 KiB. Walking back: slot 5 (1KiB), 4 (2KiB), 3 (3KiB) — fires at slot 3.
    // Age: 60 s, only slot 1 fires.
    // Either condition triggers — slot 3 fires first while walking back, so returns 3.
    try std.testing.expectEqual(@as(?u64, 3), ds.findIndexOlderThan(3 * 1024, 60));
}

test "diskwal findIndexOlderThan returns null on empty WAL" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
    defer ds.deinit();
    try std.testing.expectEqual(@as(?u64, null), ds.findIndexOlderThan(1, 1));
}

test "diskwal rotation creates numbered file and new CURRENT.wal" {
    // Fill entries until rotation is triggered, then verify:
    // - A numbered .wal file exists (rotated log file)
    // - CURRENT.wal exists (new active log file)
    // - All entries are readable across the boundary
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    // Write enough large entries to trigger rotation.
    // Each entry ~4 MiB; with 32 MiB log files a handful fills one.
    const payload_size: usize = 4 * 1024 * 1024;
    const num_entries: u64 = 70; // enough to trigger at least one rotation

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds.deinit();
        const log = &ds;

        const payload = try alloc.alloc(u8, payload_size);
        defer alloc.free(payload);

        for (1..num_entries + 1) |i| {
            fillLargePayload(payload, @intCast(i));
            try appendEntry(alloc, log, @intCast(i), @intCast(i), payload);
        }
        try log.sync();

        try std.testing.expectEqual(num_entries, log.lastIndex());

        // Should have at least 2 WAL files (1 rotated + current).
        try std.testing.expect(try countWalFiles(wal_dir) >= 2);
    }

    // Reopen and verify all entries survived rotation + recovery.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, .{});
        defer ds.deinit();
        const log = &ds;

        try std.testing.expectEqual(num_entries, log.lastIndex());
        // Check first, last, and a mid-rotation entry.
        try std.testing.expectEqual(@as(?u64, 1), try log.term(1));
        try std.testing.expectEqual(@as(?u64, num_entries), try log.term(num_entries));
        try std.testing.expectEqual(@as(?u64, 35), try log.term(35));
    }
}

test "diskwal: no prezero (copy-on-write filesystem setting)" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    const opts: raft.diskwal.OpenOptions = .{
        .log_file_size = 17 << 20,
        .prezero = .false,
    };
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
        defer ds.deinit();
        try appendRange(alloc, &ds, 1, 40_000, 1);
        try ds.sync();
        try std.testing.expect(try countWalFiles(wal_dir) >= 2);
        // Nothing is prepared ahead: with no fill to run, the next log
        // file is created inline at rotation.
        try std.testing.expect(ds.wal_store.next.creating == null);
        try std.testing.expect(!try dirHas(wal_dir, "00002.wal.tmp"));
        try std.testing.expect(!try dirHas(wal_dir, "00003.wal.tmp"));
    }
    // Log files were not zero-filled: the rotated one holds its data
    // and nothing past it; the current one is only as big as its data.
    var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
    defer ds2.deinit();
    try std.testing.expectEqual(@as(u64, 40_000), ds2.lastIndex());
    try checkRange(&ds2, 1, 40_001);
}

fn dirHas(path: String, name: String) !bool {
    const alloc = std.testing.allocator;
    const entries = try fiber.Dir.cwd().list(alloc, path);
    defer fiber.freeDirEntries(alloc, entries);
    for (entries) |e| {
        if (std.mem.eql(u8, e.name, name)) return true;
    }
    return false;
}

test "diskwal prepared log file: renamed in at rotation, a spare at open" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    // 17 MiB log files: 1 MiB of records past the header. Zero-filled
    // even on tmpfs, so the fill path runs wherever the tests do.
    const opts: raft.diskwal.OpenOptions = .{
        .log_file_size = 17 << 20,
        .prezero = .true,
    };
    const payload = try alloc.alloc(u8, 300 * 1024);
    defer alloc.free(payload);

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
        defer ds.deinit();
        // Prepare log file 2 as the sync fiber would, and let it finish.
        ds.wal_store.next.prepare();
        while (!ds.wal_store.next.isReady()) fiber.yield();
        try std.testing.expect(try dirHas(wal_dir, "00002.wal.tmp"));
        try std.testing.expect(!try dirHas(wal_dir, "00002.wal"));

        // Four 300 KiB records cross 1 MiB: the fourth rotates.
        for (1..5) |i| {
            fillLargePayload(payload, @intCast(i));
            try appendEntry(alloc, &ds, @intCast(i), 1, payload);
        }
        try ds.sync();
        try std.testing.expect(try dirHas(wal_dir, "00002.wal"));
        try std.testing.expect(!try dirHas(wal_dir, "00002.wal.tmp"));
        try std.testing.expectEqual(@as(usize, 2), try countWalFiles(wal_dir));
    }
    // A prepared log file left behind by a crash is a spare at the
    // next open; one whose fill was cut short is removed. The log
    // files around them are untouched.
    {
        var stray = try raft.diskwal.LogFile.createNamed(vfs.g(), alloc, wal_dir, "00003.wal.tmp", 3, opts);
        stray.deinit();
        const short = try std.fmt.allocPrint(
            alloc,
            "{s}/00007.wal.tmp",
            .{wal_dir},
        );
        defer alloc.free(short);
        try vfs.writeFile(short, "a fill cut short");
    }
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
        defer ds.deinit();
        try std.testing.expect(try dirHas(wal_dir, "00003.wal.tmp"));
        try std.testing.expect(!try dirHas(wal_dir, "00007.wal.tmp"));
        try std.testing.expectEqual(@as(u64, 4), ds.lastIndex());
        for (1..5) |i| try std.testing.expectEqual(@as(?u64, 1), try ds.term(@intCast(i)));
        fillLargePayload(payload, 5);
        try appendEntry(alloc, &ds, 5, 1, payload);
        try ds.sync();
    }
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
        defer ds.deinit();
        try std.testing.expectEqual(@as(u64, 5), ds.lastIndex());
        for (1..6) |i| try std.testing.expectEqual(@as(?u64, 1), try ds.term(@intCast(i)));
    }
}

test "diskwal Prezero: true and false are fixed, auto reads the filesystem" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    const Prezero = raft.diskwal.Prezero;
    try std.testing.expectEqual(Prezero.true, Prezero.true.resolve(wal_dir));
    try std.testing.expectEqual(Prezero.false, Prezero.false.resolve(wal_dir));
    // fsMagic answers only on Linux. Elsewhere auto settles on `false`
    // without it (macOS's APFS is copy-on-write), which is the contract
    // to pin there.
    const magic = vfs.fsMagic(wal_dir) orelse {
        try std.testing.expect(builtin.os.tag != .linux);
        try std.testing.expectEqual(
            Prezero.false,
            Prezero.auto.resolve(wal_dir),
        );
        return;
    };
    const want: Prezero = switch (magic) {
        vfs.fs_magic.zfs, vfs.fs_magic.btrfs, vfs.fs_magic.tmpfs => .false,
        else => .true,
    };
    try std.testing.expectEqual(want, Prezero.auto.resolve(wal_dir));
}

test "diskwal prepared log file: a late prepare never delays rotation" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    const opts: raft.diskwal.OpenOptions = .{
        .log_file_size = 17 << 20,
        .prezero = .true,
    };
    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
    defer ds.deinit();
    const next = &ds.wal_store.next;

    // Prepare fid 2 and ask for the next file before its fill has
    // run. If the fiber already finished (a fast disk beat us), the
    // case cannot be built here.
    next.prepare();
    if (next.isReady()) return error.SkipZigTest;
    var early = try next.swapIn();
    defer early.deinit();
    // An unfilled file under the next fid, at once; the prepare keeps
    // running.
    try std.testing.expectEqual(@as(u64, 3), early.fid);
    try std.testing.expect(!early.filled);
    try std.testing.expect(early.handle.file_size < opts.log_file_size);
    try std.testing.expect(try dirHas(wal_dir, "00003.wal"));
    try std.testing.expect(next.creating != null);

    // The prepared file, once ready, comes in under a fresh fid so
    // file order stays time order, and is filled.
    while (!next.isReady()) fiber.yield();
    var late = try next.swapIn();
    defer late.deinit();
    try std.testing.expectEqual(@as(u64, 4), late.fid);
    try std.testing.expect(late.filled);
    try std.testing.expect(try dirHas(wal_dir, "00004.wal"));
    try std.testing.expect(!try dirHas(wal_dir, "00002.wal.tmp"));
    try std.testing.expect(!try dirHas(wal_dir, "00002.wal"));
}

test "diskwal: an unfilled log file rotates once the prepared one is ready" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    const opts: raft.diskwal.OpenOptions = .{
        .log_file_size = 17 << 20,
        .prezero = .true,
    };
    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
    defer ds.deinit();

    // Mark the current file as one handed out unfilled, with the
    // next one prepared and ready: the next append rotates.
    try appendRange(alloc, &ds, 1, 3, 1);
    try std.testing.expectEqual(@as(usize, 1), try countWalFiles(wal_dir));
    ds.wal_store.current.filled = false;
    ds.wal_store.next.prepare();
    while (!ds.wal_store.next.isReady()) fiber.yield();
    try appendRange(alloc, &ds, 4, 4, 1);
    try ds.sync();
    try std.testing.expectEqual(@as(usize, 2), try countWalFiles(wal_dir));
    try std.testing.expect(ds.wal_store.current.filled);
    try checkRange(&ds, 1, 5);
}

// One 300 KiB record, appended with the next log file already
// prepared: a rotation then always finds it, and no file is handed
// out unfilled.
fn appendPrepared(
    alloc: std.mem.Allocator,
    ds: *raft.DiskWAL,
    payload: []u8,
    index: u64,
) !void {
    ds.wal_store.next.prepare();
    while (!ds.wal_store.next.isReady()) fiber.yield();
    fillLargePayload(payload, index);
    try appendEntry(alloc, ds, index, 1, payload);
}

// 17 MiB log files, zero-filled: three 300 KiB records each.
const reuse_opts: raft.diskwal.OpenOptions = .{
    .log_file_size = 17 << 20,
    .prezero = .true,
};

test "diskwal: a trimmed log file is reused, its old records unread" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    const payload = try alloc.alloc(u8, 300 * 1024);
    defer alloc.free(payload);

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, reuse_opts);
        defer ds.deinit();
        const spares = &ds.wal_store.next.spares;
        // Files 1 to 4. File 5 is prepared after record 12, new.
        for (1..13) |i| {
            try appendPrepared(alloc, &ds, payload, @intCast(i));
        }
        try ds.sync();
        try std.testing.expectEqual(@as(usize, 4), try countWalFiles(wal_dir));

        // A snapshot at 8 drops files 1 and 2. Three rotations since
        // the start, so the pool keeps both, as spares.
        try ds.setSnapshot(.{ .index = 8, .term = 1, .data = "snap" });
        try ds.compactUntil(8);
        try std.testing.expectEqual(@as(usize, 2), try countWalFiles(wal_dir));
        try std.testing.expectEqual(@as(usize, 2), spares.items.len);
        try std.testing.expect(try dirHas(wal_dir, "00001.wal.tmp"));
        try std.testing.expect(try dirHas(wal_dir, "00002.wal.tmp"));

        // 13 rotates into file 5. The prepare after it takes the old
        // file 2, which becomes file 6 when 16 rotates in.
        for (13..17) |i| {
            try appendPrepared(alloc, &ds, payload, @intCast(i));
        }
        try ds.sync();
        // The prepare after 16 takes the other pooled file.
        while (!ds.wal_store.next.isReady()) fiber.yield();
        try std.testing.expect(try dirHas(wal_dir, "00006.wal"));
        try std.testing.expect(!try dirHas(wal_dir, "00002.wal.tmp"));
        try std.testing.expectEqual(@as(usize, 0), spares.items.len);
        try std.testing.expectEqual(@as(u64, 16), ds.lastIndex());

        // Right after 16 in file 6 sits the old file's second record,
        // 5: an intact frame, which only its index gives away.
        const slot = (try ds.slotAt(16)).?;
        const frame_len: u64 = zbuf.Frame.hdr_size + slot.data_len;
        const stale_at: u64 = slot.data_offset + frame_len;
        const path = try std.fmt.allocPrint(alloc, "{s}/00006.wal", .{wal_dir});
        defer alloc.free(path);
        const io = utils.context.g().io;
        var file = try std.Io.Dir.cwd().openFile(io, path, .{});
        defer file.close(io);
        const frame = try alloc.alloc(u8, @intCast(frame_len));
        defer alloc.free(frame);
        _ = try file.readPositionalAll(io, frame, stale_at);
        const stale = try zen.LogEntryReader.parseRoot(
            try zbuf.Frame.read(frame),
        );
        try std.testing.expectEqual(@as(u64, 5), stale.index());
    }

    // Reopened, file 6 is indexed from its records and ends at 16.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, reuse_opts);
        defer ds.deinit();
        try std.testing.expectEqual(@as(u64, 16), ds.lastIndex());
        try std.testing.expectEqual(@as(?u64, 1), try ds.term(16));
        try std.testing.expectEqual(@as(?u64, null), try ds.term(17));
        try appendPrepared(alloc, &ds, payload, 17);
        try ds.sync();
        try std.testing.expectEqual(@as(u64, 17), ds.lastIndex());
    }
}

test "diskwal: a pooled log file nobody took goes at the next trim" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    const payload = try alloc.alloc(u8, 300 * 1024);
    defer alloc.free(payload);

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, reuse_opts);
    defer ds.deinit();
    const spares = &ds.wal_store.next.spares;
    for (1..13) |i| {
        try appendPrepared(alloc, &ds, payload, @intCast(i));
    }
    try ds.sync();
    try ds.setSnapshot(.{ .index = 8, .term = 1, .data = "snap" });
    try ds.compactUntil(8);
    try std.testing.expectEqual(@as(usize, 2), spares.items.len);

    // No rotation since that trim: the next one keeps none.
    try ds.compactUntil(8);
    try std.testing.expectEqual(@as(usize, 0), spares.items.len);
    try std.testing.expect(!try dirHas(wal_dir, "00001.wal.tmp"));
    try std.testing.expect(!try dirHas(wal_dir, "00002.wal.tmp"));
    try std.testing.expectEqual(@as(usize, 2), try countWalFiles(wal_dir));
}

test "diskwal: the pool keeps at most max_spare_log_files" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    const payload = try alloc.alloc(u8, 300 * 1024);
    defer alloc.free(payload);

    var opts = reuse_opts;
    opts.max_spare_log_files = 1;
    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, opts);
    defer ds.deinit();
    for (1..13) |i| {
        try appendPrepared(alloc, &ds, payload, @intCast(i));
    }
    try ds.sync();
    // Files 1 and 2 are dropped after three rotations: one is kept.
    try ds.setSnapshot(.{ .index = 8, .term = 1, .data = "snap" });
    try ds.compactUntil(8);
    try std.testing.expectEqual(
        @as(usize, 1),
        ds.wal_store.next.spares.items.len,
    );
    try std.testing.expect(try dirHas(wal_dir, "00001.wal.tmp"));
    try std.testing.expect(!try dirHas(wal_dir, "00002.wal.tmp"));
    try std.testing.expect(!try dirHas(wal_dir, "00002.wal"));
}

test "diskwal: spare log files are taken up again after a restart" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    const payload = try alloc.alloc(u8, 300 * 1024);
    defer alloc.free(payload);

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, reuse_opts);
        defer ds.deinit();
        for (1..13) |i| {
            try appendPrepared(alloc, &ds, payload, @intCast(i));
        }
        try ds.sync();
        try ds.setSnapshot(.{ .index = 8, .term = 1, .data = "snap" });
        try ds.compactUntil(8);
    }
    // Left on disk: the two pooled files and the one that was prepared.
    try std.testing.expect(try dirHas(wal_dir, "00001.wal.tmp"));
    try std.testing.expect(try dirHas(wal_dir, "00002.wal.tmp"));
    try std.testing.expect(try dirHas(wal_dir, "00005.wal.tmp"));

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, reuse_opts);
    defer ds.deinit();
    const next = &ds.wal_store.next;
    try std.testing.expectEqual(@as(usize, 3), next.spares.items.len);
    // No new file takes a fid a spare holds.
    try std.testing.expectEqual(@as(u64, 6), next.next_fid);

    // 13 rotates into one of the spares; the prepare after it takes
    // another.
    try appendPrepared(alloc, &ds, payload, 13);
    try ds.sync();
    while (!next.isReady()) fiber.yield();
    try std.testing.expectEqual(@as(usize, 3), try countWalFiles(wal_dir));
    try std.testing.expectEqual(@as(usize, 1), next.spares.items.len);
    try std.testing.expectEqual(@as(u64, 13), ds.lastIndex());
}

test "diskwal: a reused log file opens empty" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        vfs.deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }
    const payload = try alloc.alloc(u8, 300 * 1024);
    defer alloc.free(payload);

    // File 1 with three records, then made a spare by hand.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, reuse_opts);
        defer ds.deinit();
        for (1..4) |i| {
            try appendPrepared(alloc, &ds, payload, @intCast(i));
        }
        try ds.sync();
    }
    const live = try std.fmt.allocPrint(alloc, "{s}/00001.wal", .{wal_dir});
    defer alloc.free(live);
    const tmp = try std.fmt.allocPrint(alloc, "{s}/00001.wal.tmp", .{wal_dir});
    defer alloc.free(tmp);
    try vfs.renameFile(live, tmp);

    const LogFile = raft.diskwal.LogFile;
    {
        var next = raft.diskwal.NextLog{
            .allocator = alloc,
            .vfs = vfs.g(),
            .dir_path = wal_dir,
            .opts = reuse_opts,
            .next_fid = 9,
        };
        var lf = try next.reuse(1);
        lf.deinit();
    }
    // Rotation would rename it in; here by hand, as file 9.
    const cur = try std.fmt.allocPrint(alloc, "{s}/00009.wal", .{wal_dir});
    defer alloc.free(cur);
    try vfs.renameFile(tmp, cur);

    // Scanned as a current file: no record, though the old second and
    // third are still in it.
    var lf = try LogFile.openCurrent(vfs.g(), alloc, wal_dir, 9, reuse_opts);
    defer lf.deinit();
    try std.testing.expectEqual(@as(usize, 0), lf.slots.items.len);
    try std.testing.expectEqual(
        @as(u64, raft.diskwal.log_file_offset),
        lf.write_offset,
    );
}
