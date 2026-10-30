// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const zdb = @import("zdb");

const compaction = zdb.compaction;
const TableEntry = compaction.TableEntry;
const Config = zdb.config.Config;

const kib: u64 = 1024;
const mib: u64 = 1024 * 1024;
const gib: u64 = 1024 * mib;

/// L0 entries of the given sizes, oldest first, each with its own id
/// (100, 101, ...) and raft_index (10, 20, ...). The picker reads sizes
/// and indexes only, so no table stands behind them.
fn tablesOf(comptime sizes: []const u64) [sizes.len]TableEntry {
    var out: [sizes.len]TableEntry = undefined;
    for (sizes, 0..) |sz, i| out[i] = .{
        .id = 100 + i,
        .raft_index = (i + 1) * 10,
        .size = sz,
        .table = undefined,
    };
    return out;
}

/// The run is exactly the entries with these ids, in this order.
fn expectIds(run: []const TableEntry, want: []const u64) !void {
    try std.testing.expectEqual(want.len, run.len);
    for (want, run) |id, e| try std.testing.expectEqual(id, e.id);
}

const no_run: ?[]const TableEntry = null;
const l0MergeRun = compaction.l0MergeRun;

test "l0MergeRun: needs l0_merge_min_tables unfinished tables" {
    const opts: Config = .{ .l0_merge_min_tables = 4 };
    const three = tablesOf(&.{ 30 * kib, 30 * kib, 30 * kib });
    try std.testing.expectEqual(no_run, l0MergeRun(opts, &three));

    const four = tablesOf(&.{ 30 * kib, 30 * kib, 30 * kib, 30 * kib });
    const run = l0MergeRun(opts, &four) orelse return error.TestExpectedRun;
    try expectIds(run, &.{ 100, 101, 102, 103 });
}

test "l0MergeRun: an unfinished output merges again with later flushes" {
    const opts: Config = .{ .l0_merge_min_tables = 4 };
    // Oldest first: an earlier merge's 3 MiB output, then four flushes.
    const tables = tablesOf(&.{ 3 * mib, 30 * kib, 50 * kib, 20 * kib, 45 * kib });
    const run = l0MergeRun(opts, &tables) orelse return error.TestExpectedRun;
    try expectIds(run, &.{ 100, 101, 102, 103, 104 });
}

test "l0MergeRun: the run starts after the newest finished table" {
    const opts: Config = .{ .l0_merge_min_tables = 4 };
    try std.testing.expectEqual(@as(usize, 64 * mib), opts.table_bytes);

    // A finished table between small ones: only the newer ones are
    // taken, whatever sits before it.
    const split = tablesOf(&.{ 30 * kib, 30 * kib, 64 * mib, 30 * kib, 30 * kib, 30 * kib, 30 * kib });
    const newer = l0MergeRun(opts, &split) orelse return error.TestExpectedRun;
    try expectIds(newer, &.{ 103, 104, 105, 106 });

    // Too few after it: nothing, although there are enough before it.
    const short = tablesOf(&.{ 30 * kib, 30 * kib, 30 * kib, 64 * mib, 30 * kib, 30 * kib, 30 * kib });
    try std.testing.expectEqual(no_run, l0MergeRun(opts, &short));

    // Finished tables only: nothing to merge.
    const done = tablesOf(&.{ 64 * mib, 70 * mib, 64 * mib, 80 * mib, 64 * mib });
    try std.testing.expectEqual(no_run, l0MergeRun(opts, &done));

    // The output itself may pass the L0 file size: four 20 MiB tables
    // make one 80 MiB table. The 80 MiB output of an earlier merge, next
    // to them, stays out.
    const tiers = tablesOf(&.{ 80 * mib, 20 * mib, 20 * mib, 20 * mib, 20 * mib });
    const run = l0MergeRun(opts, &tiers) orelse return error.TestExpectedRun;
    try expectIds(run, &.{ 101, 102, 103, 104 });
}

test "l0MergeRun: the run stops once it holds twice the L0 file size" {
    const opts: Config = .{ .l0_merge_min_tables = 4 };
    // Eight 20 MiB tables: the seventh brings the run to 140 MiB, past
    // 128 MiB, and ends it; the eighth waits for the next merge.
    const bulk = tablesOf(&(.{20 * mib} ** 8));
    const run = l0MergeRun(opts, &bulk) orelse return error.TestExpectedRun;
    try expectIds(run, &.{ 100, 101, 102, 103, 104, 105, 106 });

    // Six 30 MiB tables: the fifth brings the run to 150 MiB and ends
    // it. A run is picked the same whether or not L0 is due a push.
    const six = tablesOf(&(.{30 * mib} ** 6));
    const five = l0MergeRun(opts, &six) orelse return error.TestExpectedRun;
    try expectIds(five, &.{ 100, 101, 102, 103, 104 });
}

test "l0MergeRun: 0 turns L0 to L0 compaction off" {
    const opts: Config = .{ .l0_merge_min_tables = 0 };
    const tables = tablesOf(&.{ 30 * kib, 30 * kib, 30 * kib, 30 * kib, 30 * kib });
    try std.testing.expectEqual(no_run, l0MergeRun(opts, &tables));
}

test "classFor: a job from L0 is foreground, below it background" {
    try std.testing.expectEqual(.fg, compaction.classFor(0));
    try std.testing.expectEqual(.bg, compaction.classFor(1));
    try std.testing.expectEqual(.bg, compaction.classFor(5));
}

test "config: a tree needs at least two levels" {
    const two: Config = .{ .max_levels = 2 };
    try two.validate();
    const one: Config = .{ .max_levels = 1 };
    try std.testing.expectError(error.InvalidMaxLevels, one.validate());
    const none: Config = .{ .max_levels = 0 };
    try std.testing.expectError(error.InvalidMaxLevels, none.validate());
}

test "config: a merge needs at least two tables" {
    const ok: Config = .{};
    try ok.validate();
    const off: Config = .{ .l0_merge_min_tables = 0 };
    try off.validate();
    const one: Config = .{ .l0_merge_min_tables = 1 };
    try std.testing.expectError(error.InvalidL0MergeMinTables, one.validate());
}

test "config: the backpressure start count cannot be zero" {
    const zero: Config = .{ .l0_backpressure_start_count = 0 };
    try std.testing.expectError(error.InvalidL0BackpressureStartCount, zero.validate());
}

test "levelTarget: fixed targets cascade up from the Lmax target" {
    const opts: Config = .{};
    const lmax: i64 = opts.lmax_target_bytes;
    try std.testing.expectEqual(@as(i64, 10 * gib), lmax);
    // The split point is a tree with every level at target.
    try std.testing.expectEqual(@as(i64, 12800 * mib), opts.splitBytes());
    try std.testing.expectEqual(lmax, compaction.levelTarget(opts, 3));
    try std.testing.expectEqual(@divTrunc(lmax, 5), compaction.levelTarget(opts, 2));
    try std.testing.expectEqual(@divTrunc(lmax, 25), compaction.levelTarget(opts, 1));
    // L0's target is L1's; a level past Lmax has none.
    const l1 = @divTrunc(lmax, 25);
    try std.testing.expectEqual(l1, compaction.levelTarget(opts, 0));
    try std.testing.expectEqual(@as(i64, 0), compaction.levelTarget(opts, 4));
}
