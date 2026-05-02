const std = @import("std");
const build_options = @import("build_options");
const raft = @import("raft");
const zio = @import("zio");
const zbuf = @import("zbuf").zbuf;
const zen = raft.buffers;
const u = raft.utils;

fn makeTempWalDir(alloc: std.mem.Allocator) ![]u8 {
    var path_buf: [160]u8 = undefined;
    const ts = std.time.nanoTimestamp();
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/egres-diskwal-{d}", .{ts});
    try std.fs.cwd().makePath(path);
    return alloc.dupe(u8, path);
}

fn makeLogEntry(
    alloc: std.mem.Allocator,
    index: u64,
    term: u64,
    data: []const u8,
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

fn appendEntry(
    alloc: std.mem.Allocator,
    log: *raft.DiskWAL,
    index: u64,
    term: u64,
    data: []const u8,
) !void {
    var entry = try makeLogEntry(alloc, index, term, data);
    defer entry.deinit();
    try log.append(&[_]zen.LogEntryBuffer{entry});
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
        try log.append(batch.items);
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

fn countWalFiles(path: []const u8) !usize {
    var dir = try std.fs.cwd().openDir(path, .{ .iterate = true });
    defer dir.close();

    var it = dir.iterate();
    var count: usize = 0;
    while (try it.next()) |entry_dir| {
        if (entry_dir.kind != .file) continue;
        if (std.mem.endsWith(u8, entry_dir.name, ".wal")) count += 1;
    }
    return count;
}

test "diskwal term and index basics" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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

test "diskwal append overwrites conflicting suffix" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    const payload_size: usize = 4 * 1024 * 1024;
    const payload = try alloc.alloc(u8, payload_size);
    defer alloc.free(payload);

    // Write enough entries to trigger a rotation, producing at least 2 WAL files.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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
        var dir = try std.fs.cwd().openDir(wal_dir, .{ .iterate = true });
        defer dir.close();

        // Find the highest fid file.
        var max_fid: u64 = 0;
        var iter = dir.iterate();
        while (try iter.next()) |e| {
            if (e.kind != .file) continue;
            const fid = raft.diskwal.wal.parseWalFid(e.name) orelse continue;
            if (fid > max_fid) max_fid = fid;
        }
        try std.testing.expect(max_fid > 1);

        // Corrupt the highest file's magic bytes.
        const bad_path = try std.fmt.allocPrint(alloc, "{s}/{d:0>5}.wal", .{ wal_dir, max_fid });
        defer alloc.free(bad_path);
        var file = try std.fs.cwd().openFile(bad_path, .{ .mode = .read_write });
        defer file.close();
        var garbage = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF };
        _ = try file.pwriteAll(&garbage, 0);
    }

    // Reopen — the malformed current file should be dropped. The previous
    // rotated file becomes current and its entries should survive.
    {
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
        defer ds2.deinit();
        const log = &ds2;
        // Should have entries from the rotated file (at least the first segment's worth).
        try std.testing.expect(log.lastIndex() > 0);
    }
}

test "diskwal rejects snapshot-log firstIndex mismatch on reopen" {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
        defer ds.deinit();
        const log = &ds;
        try log.setSnapshot(.{ .index = 5, .term = 2, .data = "snap" });
        try log.sync();
    }

    try std.testing.expectError(
        error.CorruptLogFile,
        raft.DiskWAL.openPath(wal_dir, alloc, 1024),
    );
}

fn bigTestStorageImpl() !void {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    // Keep this large enough to cross a segment boundary.
    const N: u64 = 62_000;

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
        defer ds2.deinit();
        const log2 = &ds2;
        try std.testing.expectEqual(@as(u64, 60_099), log2.lastIndex());
        try checkRange(log2, 1, 60_100);
        try std.testing.expectEqual(@as(?u64, 4), try log2.term(60_000));
        try std.testing.expectEqual(@as(?u64, 4), try log2.term(60_099));
    }
}

test "BIGTEST diskwal storage" {
    if (!build_options.enable_bigtests) return error.SkipZigTest;
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var handle = try rt.spawn(bigTestStorageImpl, .{});
    try handle.join();
}

test "BIGTEST diskwal truncate storage" {
    if (!build_options.enable_bigtests) return error.SkipZigTest;
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var handle = try rt.spawn(bigTestTruncateStorageImpl, .{});
    try handle.join();
}

test "BIGTEST diskwal large payload rotation" {
    if (!build_options.enable_bigtests) return error.SkipZigTest;
    const rt = try zio.Runtime.init(std.testing.allocator, .{ .executors = .exact(2) });
    defer rt.deinit();
    var handle = try rt.spawn(bigTestLargePayloadRotationImpl, .{});
    try handle.join();
}

fn bigTestTruncateStorageImpl() !void {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
    defer ds.deinit();
    const log = &ds;

    const num_entries: u64 = (raft.diskwal.max_num_entries * 3) / 2;
    const compact_to: u64 = raft.diskwal.max_num_entries + 10;

    // Fill across segment boundaries.
    try appendRange(alloc, log, 1, num_entries, 1);
    try std.testing.expectEqual(num_entries, log.lastIndex());
    try checkRange(log, 1, num_entries + 1);

    // Compaction here is segment-granular. Compact beyond first segment so
    // early indices become unavailable.
    try log.compactUntil(compact_to);
    try std.testing.expectEqual(@as(?u64, null), try log.term(1));
    const first_after_compact = log.firstIndex();
    try checkRange(log, first_after_compact, num_entries + 1);

    // Append one more full-segment worth and verify full retained range.
    try appendRange(alloc, log, num_entries + 1, num_entries + raft.diskwal.max_num_entries, 2);
    const new_last = num_entries + raft.diskwal.max_num_entries;
    try std.testing.expectEqual(new_last, log.lastIndex());
    try std.testing.expectEqual(@as(?u64, 2), try log.term(new_last));
    try checkRange(log, first_after_compact, new_last + 1);
    try checkRange(log, new_last - 100, new_last + 1);
}

fn bigTestLargePayloadRotationImpl() !void {
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    const payload_size = 4 * 1024 * 1024;
    const total_entries: u64 = 150;
    const payload = try alloc.alloc(u8, payload_size);
    defer alloc.free(payload);

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
        defer ds.deinit();
        const log = &ds;

        var idx: u64 = 1;
        while (idx <= total_entries) : (idx += 1) {
            fillLargePayload(payload, idx);
            // Use term=index to exercise varying term persistence across segments.
            try appendEntry(alloc, log, idx, idx, payload);
        }
        try log.sync();

        try std.testing.expectEqual(total_entries, log.lastIndex());

        // 150 x 4MiB should span exactly 3 WAL segment files with current limits.
        try std.testing.expectEqual(@as(usize, 3), try countWalFiles(wal_dir));

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
        var ds2 = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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
        // suffix and drop the highest segment file, leaving two WAL files.
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
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    // Write 5 entries and close.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
        defer ds.deinit();
        const log = &ds;
        try appendRange(alloc, log, 1, 5, 1);
        try log.sync();
    }

    // Reopen — CURRENT.wal should be recovered via CRC scan.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    // Write 5 entries.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
        defer ds.deinit();
        const log = &ds;
        try appendRange(alloc, log, 1, 5, 1);
        try log.sync();
    }

    // Corrupt the 3rd entry's payload by flipping a byte.
    // Entries start at log_file_offset (1 MiB). Each entry is framed as
    // `[u64 hash | u32 len | payload]` — see zbuf.Frame.hdr_size.
    {
        // The current file is 00001.wal (no rotation happened).
        const current_path = try std.fmt.allocPrint(alloc, "{s}/00001.wal", .{wal_dir});
        defer alloc.free(current_path);
        var file = try std.fs.cwd().openFile(current_path, .{ .mode = .read_write });
        defer file.close();

        // Read past the first two entries to find the 3rd entry's payload.
        const hdr_size: usize = zbuf.Frame.hdr_size;
        var offset: u64 = diskwal.log_file_offset;
        var hdr_buf: [zbuf.Frame.hdr_size]u8 = undefined;
        for (0..2) |_| {
            _ = try file.preadAll(&hdr_buf, offset);
            // u32 length lives at byte 8 of the prefix (after the u64 hash).
            const payload_len = std.mem.readInt(u32, hdr_buf[8..12], .little);
            offset += hdr_size + payload_len;
        }
        // Now offset points at the 3rd entry. Corrupt a byte in its payload.
        const corrupt_offset = offset + hdr_size + 1; // +1 to hit inside the payload
        var byte: [1]u8 = undefined;
        _ = try file.preadAll(&byte, corrupt_offset);
        byte[0] ^= 0xFF; // flip all bits
        _ = try file.pwriteAll(&byte, corrupt_offset);
    }

    // Reopen — should recover only entries 1 and 2.
    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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
    // data-region records). Verifies that a logFile's in-memory slots
    // survive a serialize/deserialize cycle through the on-disk WalHeader,
    // covering both the empty-slots case (just base) and the populated
    // case (base + multiple data slots).
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    const vfs = @import("vfs");
    const vfs_inst = vfs.g();

    const base = raft.diskwal.entry{
        .term = 5,
        .index = 100,
        .timestamp = 0xDEAD_BEEF,
    };
    var lf = try raft.diskwal.logFile.createCurrent(vfs_inst, alloc, wal_dir, 1, base);
    defer lf.deinit();

    // Empty case: createCurrent writes a WalHeader with just the base slot.
    {
        var slots = try raft.diskwal.logFile.readHeader(alloc, lf.handle);
        defer slots.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), slots.items.len);
        try std.testing.expectEqual(@as(u64, 5), slots.items[0].term);
        try std.testing.expectEqual(@as(u64, 100), slots.items[0].index);
        try std.testing.expectEqual(@as(u64, 0xDEAD_BEEF), slots.items[0].timestamp);
    }

    // Populated case: append slot metadata and re-persist via writeHeader.
    // Test slots can carry arbitrary data_offset/data_len/entry_type
    // values — the data region is never touched by either function.
    try lf.slots.append(alloc, .{
        .term = 5,
        .index = 101,
        .timestamp = 0xDEAD_BEF0,
        .data_offset = 1 << 20,
        .data_len = 42,
        .entry_type = zen.EntryTypeEnum.data,
    });
    try lf.slots.append(alloc, .{
        .term = 6,
        .index = 102,
        .timestamp = 0xDEAD_BEF1,
        .data_offset = (1 << 20) + 50,
        .data_len = 100,
        .entry_type = zen.EntryTypeEnum.conf,
    });
    try lf.writeHeader();

    {
        var slots = try raft.diskwal.logFile.readHeader(alloc, lf.handle);
        defer slots.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 3), slots.items.len);

        try std.testing.expectEqual(@as(u64, 5), slots.items[0].term);
        try std.testing.expectEqual(@as(u64, 100), slots.items[0].index);
        try std.testing.expectEqual(@as(u64, 0xDEAD_BEEF), slots.items[0].timestamp);

        try std.testing.expectEqual(@as(u64, 5), slots.items[1].term);
        try std.testing.expectEqual(@as(u64, 101), slots.items[1].index);
        try std.testing.expectEqual(@as(u64, 0xDEAD_BEF0), slots.items[1].timestamp);
        try std.testing.expectEqual(@as(u32, 1 << 20), slots.items[1].data_offset);
        try std.testing.expectEqual(@as(u32, 42), slots.items[1].data_len);
        try std.testing.expectEqual(zen.EntryTypeEnum.data, slots.items[1].entry_type);

        try std.testing.expectEqual(@as(u64, 6), slots.items[2].term);
        try std.testing.expectEqual(@as(u64, 102), slots.items[2].index);
        try std.testing.expectEqual(@as(u64, 0xDEAD_BEF1), slots.items[2].timestamp);
        try std.testing.expectEqual(@as(u32, (1 << 20) + 50), slots.items[2].data_offset);
        try std.testing.expectEqual(@as(u32, 100), slots.items[2].data_len);
        try std.testing.expectEqual(zen.EntryTypeEnum.conf, slots.items[2].entry_type);
    }
}

test "diskwal rotation creates numbered file and new CURRENT.wal" {
    // Fill entries until rotation is triggered, then verify:
    // - A numbered .wal file exists (rotated segment)
    // - CURRENT.wal exists (new active segment)
    // - All entries are readable across the boundary
    const alloc = std.testing.allocator;
    const wal_dir = try makeTempWalDir(alloc);
    defer {
        std.fs.cwd().deleteTree(wal_dir) catch {};
        alloc.free(wal_dir);
    }

    // Write enough large entries to trigger rotation.
    // Each entry ~4 MiB, log_file_size is 256 MiB, so ~64 entries should fill a segment.
    const payload_size: usize = 4 * 1024 * 1024;
    const num_entries: u64 = 70; // enough to trigger at least one rotation

    {
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
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
        var ds = try raft.DiskWAL.openPath(wal_dir, alloc, 1024);
        defer ds.deinit();
        const log = &ds;

        try std.testing.expectEqual(num_entries, log.lastIndex());
        // Check first, last, and a mid-rotation entry.
        try std.testing.expectEqual(@as(?u64, 1), try log.term(1));
        try std.testing.expectEqual(@as(?u64, num_entries), try log.term(num_entries));
        try std.testing.expectEqual(@as(?u64, 35), try log.term(35));
    }
}
