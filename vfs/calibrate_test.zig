// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const vfs = @import("vfs");
const calibrate = vfs.calibrate;

// The plumbing, not the numbers: a tmp dir on whatever filesystem the
// tests run on yields both bandwidths, and the scratch dir is gone.
test "calibrate: runCalibration profiles a directory and cleans up" {
    const alloc = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try vfs.tmpDirPath(alloc, &tmp);
    defer alloc.free(root);

    const p = try calibrate.runCalibration(alloc, root);
    try testing.expect(p.read_bps > 0);
    try testing.expect(p.write_bps > 0);

    const scratch = try std.fmt.allocPrint(alloc, "{s}/calibrate", .{root});
    defer alloc.free(scratch);
    try testing.expectError(error.FileNotFound, vfs.fileSize(scratch));
}

test "calibrate: bwConfigFrom prices a write by the bandwidth ratio" {
    // 7 GB/s reads, 5 GB/s writes: the ratio 1.4 rounds to one, and
    // the budget refills at the read rate; 7 to 4 rounds to two.
    const c = calibrate.bwConfigFrom(.{
        .read_bps = 7000 << 20,
        .write_bps = 5000 << 20,
    });
    try testing.expectEqual(@as(u64, 7000 << 20), c.bytes_per_sec);
    try testing.expectEqual(@as(u8, 1), c.write_cost);
    const two = calibrate.bwConfigFrom(.{
        .read_bps = 7000 << 20,
        .write_bps = 4000 << 20,
    });
    try testing.expectEqual(@as(u8, 2), two.write_cost);

    // Writes faster than reads cost one; a huge ratio clamps at 255.
    const fast = calibrate.bwConfigFrom(.{
        .read_bps = 100,
        .write_bps = 900,
    });
    try testing.expectEqual(@as(u8, 1), fast.write_cost);
    const slow = calibrate.bwConfigFrom(.{
        .read_bps = 1 << 40,
        .write_bps = 1,
    });
    try testing.expectEqual(@as(u8, 255), slow.write_cost);
}
