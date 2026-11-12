// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// Tests for raft/meta.zig::MetaStore.
//
// Each test creates a fresh /tmp/egres-meta-<ts> directory, exercises a
// scenario, and tears it down. We use std.testing.allocator so leak
// detection catches any forgotten frees.

const std = @import("std");
const fiber = @import("zfiber");
const raft = @import("raft");
const vfs = @import("vfs");
const zen = raft.buffers;
const meta = raft.meta;

/// Named from the seed's random bytes, not the clock: under the
/// simulation every test in one virtual ms reads the same time.
fn makeTempMetaDir(alloc: std.mem.Allocator) ![]u8 {
    var path_buf: [160]u8 = undefined;
    const path = try std.fmt.bufPrint(
        &path_buf,
        "/tmp/egres-meta-{x:0>16}",
        .{fiber.random().int(u64)},
    );
    return alloc.dupe(u8, path);
}

/// Flip one byte of the file at `offset`, through vfs: the test reaches
/// the same disk MetaStore wrote, simulated or real.
fn flipByte(alloc: std.mem.Allocator, path: []const u8, offset: u64) !void {
    const fh = try vfs.g().openFile(alloc, path, .{
        .flags = .{ .writable = true },
    });
    defer fh.close();
    const res = try fh.readAligned(alloc, offset, 1);
    defer res.free();
    const b = [_]u8{res.data[0] ^ 0xFF};
    try fh.write(&b, offset);
}

/// Flip the first byte of the slot's CRC (offset = slot_idx * 4096).
/// A single bit flip is enough to fail the CRC check on read.
fn corruptSlotCrc(alloc: std.mem.Allocator, dir: []const u8, slot_idx: u32) !void {
    const path = try std.fs.path.join(alloc, &.{ dir, "state.bin" });
    defer alloc.free(path);
    try flipByte(alloc, path, @as(u64, slot_idx) * 4096);
}

test "meta: fresh open creates two valid slots with default HardState" {
    const alloc = std.testing.allocator;
    const dir = try makeTempMetaDir(alloc);
    defer {
        vfs.deleteTree(dir) catch {};
        alloc.free(dir);
    }

    var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
    defer ms.deinit();

    const hs = ms.hardState();
    try std.testing.expectEqual(@as(u64, 0), hs.current_term);
    try std.testing.expectEqual(@as(u64, 0), hs.voted_for);
    try std.testing.expectEqual(@as(u64, 0), hs.raft_id);
    try std.testing.expectEqual(@as(u64, 0), hs.checkpoint_index);

    // Slot B (the higher seq from initialisation) should be active.
    try std.testing.expectEqual(@as(u8, 1), ms.active_slot);
}

test "meta: a crash keeps the last hardstate and snapshot" {
    // Only the simulated disk can lose power.
    const disk = fiber.simDisk() orelse return error.SkipZigTest;
    const alloc = std.testing.allocator;
    const dir = try makeTempMetaDir(alloc);
    defer {
        vfs.deleteTree(dir) catch {};
        alloc.free(dir);
    }

    {
        var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
        defer ms.deinit();
        try ms.setHardState(.{ .current_term = 9, .voted_for = 3 });
        try ms.setSnapshot(.{
            .index = 5,
            .term = 1,
            .data = "abc",
            .conf_state = null,
        });
        disk.crash(dir, .lose_all);
    }

    var ms2 = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
    defer ms2.deinit();
    try std.testing.expectEqual(@as(u64, 9), ms2.hardState().current_term);
    try std.testing.expectEqual(@as(u64, 3), ms2.hardState().voted_for);
    try std.testing.expectEqual(@as(u64, 5), ms2.snapshotState().index);
}

test "meta: a failed write leaves the last hardstate in place" {
    // Only the simulated disk fails on purpose.
    const disk = fiber.simDisk() orelse return error.SkipZigTest;
    const alloc = std.testing.allocator;
    const dir = try makeTempMetaDir(alloc);
    defer {
        vfs.deleteTree(dir) catch {};
        alloc.free(dir);
    }

    var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
    defer ms.deinit();
    try ms.setHardState(.{ .current_term = 4, .voted_for = 1 });

    try disk.setFaults(dir, .{ .write = 1000 });
    defer disk.setFaults(dir, .{}) catch {};
    try std.testing.expectError(
        error.InputOutput,
        ms.setHardState(.{ .current_term = 5, .voted_for = 1 }),
    );
    try disk.setFaults(dir, .{});
    try std.testing.expectEqual(@as(u64, 4), ms.hardState().current_term);

    var ms2 = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
    defer ms2.deinit();
    try std.testing.expectEqual(@as(u64, 4), ms2.hardState().current_term);
}

test "meta: a snapshot whose directory sync fails is not installed" {
    // Only the simulated disk fails on purpose.
    const disk = fiber.simDisk() orelse return error.SkipZigTest;
    const alloc = std.testing.allocator;
    const dir = try makeTempMetaDir(alloc);
    defer {
        vfs.deleteTree(dir) catch {};
        alloc.free(dir);
    }

    {
        var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
        defer ms.deinit();
        try ms.setSnapshot(.{
            .index = 5,
            .term = 1,
            .data = "abc",
            .conf_state = null,
        });

        // The tmp file is O_DSYNC, so the one sync here is the
        // directory's, after the rename.
        try disk.setFaults(dir, .{ .sync = 1000 });
        defer disk.setFaults(dir, .{}) catch {};
        try std.testing.expectError(error.InputOutput, ms.setSnapshot(.{
            .index = 9,
            .term = 1,
            .data = "defg",
            .conf_state = null,
        }));
        try disk.setFaults(dir, .{});
        // The store still has the snapshot a power cut brings back, so
        // nothing that one needs is deleted for the new one.
        try std.testing.expectEqual(@as(u64, 5), ms.snapshotState().index);
        disk.crash(dir, .lose_all);
    }

    var ms2 = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
    defer ms2.deinit();
    try std.testing.expectEqual(@as(u64, 5), ms2.snapshotState().index);
    try std.testing.expectEqualSlices(u8, "abc", ms2.snapshotState().data);
}

test "meta: setHardState round-trip survives reopen" {
    const alloc = std.testing.allocator;
    const dir = try makeTempMetaDir(alloc);
    defer {
        vfs.deleteTree(dir) catch {};
        alloc.free(dir);
    }

    {
        var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
        defer ms.deinit();
        try ms.setHardState(.{
            .current_term = 5,
            .voted_for = 3,
            .raft_id = 1,
            .checkpoint_index = 42,
        });
    }

    var ms2 = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
    defer ms2.deinit();
    const hs = ms2.hardState();
    try std.testing.expectEqual(@as(u64, 5), hs.current_term);
    try std.testing.expectEqual(@as(u64, 3), hs.voted_for);
    try std.testing.expectEqual(@as(u64, 1), hs.raft_id);
    try std.testing.expectEqual(@as(u64, 42), hs.checkpoint_index);
}

test "meta: setHardState alternates slots" {
    const alloc = std.testing.allocator;
    const dir = try makeTempMetaDir(alloc);
    defer {
        vfs.deleteTree(dir) catch {};
        alloc.free(dir);
    }

    {
        var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
        defer ms.deinit();
        // After fresh open, slot B (idx=1) is active. The next 5 writes
        // should toggle 0, 1, 0, 1, 0.
        const expected_active = [_]u8{ 0, 1, 0, 1, 0 };
        for (expected_active, 0..) |want, i| {
            try ms.setHardState(.{
                .current_term = @intCast(i + 1),
                .voted_for = 0,
                .raft_id = 7,
                .checkpoint_index = 0,
            });
            try std.testing.expectEqual(want, ms.active_slot);
        }
        // The fresh open wrote seqs 0 and 1; five writes took 2 to 6.
        try std.testing.expectEqual(@as(u64, 6), ms.hardState().seq);
    }

    var ms2 = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
    defer ms2.deinit();
    try std.testing.expectEqual(@as(u64, 6), ms2.hardState().seq);
    try std.testing.expectEqual(@as(u8, 0), ms2.active_slot);
}

test "meta: torn write to newer slot recovers older value" {
    const alloc = std.testing.allocator;
    const dir = try makeTempMetaDir(alloc);
    defer {
        vfs.deleteTree(dir) catch {};
        alloc.free(dir);
    }

    // Write two distinct values; the second is the "newer" durable state.
    {
        var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
        defer ms.deinit();
        try ms.setHardState(.{ .current_term = 100, .voted_for = 0, .raft_id = 0, .checkpoint_index = 0 });
        try ms.setHardState(.{ .current_term = 200, .voted_for = 0, .raft_id = 0, .checkpoint_index = 0 });
        // active_slot now holds the newer (term=200) value. The OLDER
        // value (term=100) is in the OTHER slot — survival path.
    }

    // Corrupt the slot that holds the newer value. We snapshot the active
    // slot index, then close the store so the file isn't held open while
    // we go around the API and pwrite directly.
    var newer: u8 = undefined;
    {
        var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
        newer = ms.active_slot;
        ms.deinit();
    }
    try corruptSlotCrc(alloc, dir, newer);

    // Reopen — recovery should fall back to the older valid slot (term=100).
    var ms3 = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
    defer ms3.deinit();
    try std.testing.expectEqual(@as(u64, 100), ms3.hardState().current_term);
}

test "meta: both slots corrupt returns MetaCorrupt" {
    const alloc = std.testing.allocator;
    const dir = try makeTempMetaDir(alloc);
    defer {
        vfs.deleteTree(dir) catch {};
        alloc.free(dir);
    }

    {
        var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
        ms.deinit();
    }
    try corruptSlotCrc(alloc, dir, 0);
    try corruptSlotCrc(alloc, dir, 1);

    try std.testing.expectError(
        meta.Error.MetaCorrupt,
        meta.MetaStore.openPath(vfs.g(), dir, alloc),
    );
}

test "meta: setSnapshot round-trip survives reopen" {
    const alloc = std.testing.allocator;
    const dir = try makeTempMetaDir(alloc);
    defer {
        vfs.deleteTree(dir) catch {};
        alloc.free(dir);
    }

    // Build a non-trivial Snapshot: 1 KiB data + a populated conf_state.
    const data = try alloc.alloc(u8, 1024);
    defer alloc.free(data);
    for (data, 0..) |*b, i| b.* = @truncate(i);

    const peers = try alloc.alloc(zen.ConfStatePeerStruct, 2);
    defer alloc.free(peers);
    peers[0] = .{ .id = 1, .role = .unknown };
    peers[1] = .{ .id = 2, .role = .voter };
    const conf: zen.ConfStateStruct = .{ .peers = peers };

    {
        var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
        defer ms.deinit();
        try ms.setSnapshot(.{
            .index = 100,
            .term = 7,
            .data = data,
            .conf_state = conf,
        });
    }

    var ms2 = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
    defer ms2.deinit();
    const recovered = ms2.snapshotState();
    try std.testing.expectEqual(@as(u64, 100), recovered.index);
    try std.testing.expectEqual(@as(u64, 7), recovered.term);
    try std.testing.expectEqualSlices(u8, data, recovered.data);
    try std.testing.expect(recovered.conf_state != null);
    try std.testing.expectEqual(@as(usize, 2), recovered.conf_state.?.peers.len);
    try std.testing.expectEqual(@as(u64, 1), recovered.conf_state.?.peers[0].id);
}

test "meta: stale snapshot.tmp is cleaned on reopen" {
    const alloc = std.testing.allocator;
    const dir = try makeTempMetaDir(alloc);
    defer {
        vfs.deleteTree(dir) catch {};
        alloc.free(dir);
    }

    // First open creates the directory + state.bin.
    {
        var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
        ms.deinit();
    }

    // Plant a garbage snapshot.tmp simulating a crashed setSnapshot.
    const tmp_path = try std.fs.path.join(alloc, &.{ dir, "snapshot.tmp" });
    defer alloc.free(tmp_path);
    try vfs.writeFile(
        tmp_path,
        "garbage that doesn't parse as a valid Snapshot",
    );

    // Reopen — should silently delete the stale tmp.
    {
        var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
        defer ms.deinit();
    }

    // tmp should now be gone.
    try std.testing.expectError(error.FileNotFound, vfs.access(tmp_path));
}

test "meta: corrupt snapshot.bin returns SnapshotCorrupt" {
    const alloc = std.testing.allocator;
    const dir = try makeTempMetaDir(alloc);
    defer {
        vfs.deleteTree(dir) catch {};
        alloc.free(dir);
    }

    // Write a real snapshot first so snapshot.bin exists.
    {
        var ms = try meta.MetaStore.openPath(vfs.g(), dir, alloc);
        defer ms.deinit();
        try ms.setSnapshot(.{
            .index = 5,
            .term = 1,
            .data = "abc",
            .conf_state = null,
        });
    }

    // Flip a byte in the CRC at offset 0 of snapshot.bin.
    const snap_path = try std.fs.path.join(alloc, &.{ dir, "snapshot.bin" });
    defer alloc.free(snap_path);
    try flipByte(alloc, snap_path, 0);

    try std.testing.expectError(
        meta.Error.SnapshotCorrupt,
        meta.MetaStore.openPath(vfs.g(), dir, alloc),
    );
}
