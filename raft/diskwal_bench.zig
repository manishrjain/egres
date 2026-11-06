// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! DiskWAL on the real disk: what one durable batch costs, what the
//! append alone costs, and how the latency spreads across a run long
//! enough to cross a log file rotation.
//! The unit tests use /tmp, which is tmpfs here; this runs under /data,
//! the disk the WAL lives on.
//!
//!     zig build bench -- --filter diskwal

const std = @import("std");
const utils = @import("utils");
const fiber = @import("zfiber");
const String = utils.String;
const raft = @import("raft");
const vfs = @import("vfs");
const zen = raft.buffers;
const bench = fiber.bench;
const Bench = bench.Bench;

const dir_root = "/data";
/// A real allocator, as the server has (mimalloc); the harness's
/// page_allocator would charge an mmap to every entry.
const gpa = std.heap.smp_allocator;

/// A fresh WAL directory on the real disk, removed on close.
const Wal = struct {
    dir: []u8,
    log: raft.DiskWAL,
    next_index: u64 = 1,
    alloc: std.mem.Allocator,

    fn open(alloc: std.mem.Allocator) !Wal {
        var path_buf: [96]u8 = undefined;
        const ts = utils.timestampNow(.realtime, .nanos);
        const path = try std.fmt.bufPrint(
            &path_buf,
            dir_root ++ "/egres-diskwal-bench-{d}",
            .{ts},
        );
        try vfs.createDirPath(path);
        const dir = try alloc.dupe(u8, path);
        errdefer alloc.free(dir);
        return .{
            .dir = dir,
            .log = try raft.DiskWAL.openPath(dir, alloc, .{ .log_file_size = 32 << 20 }),
            .alloc = alloc,
        };
    }

    fn close(self: *Wal) void {
        self.log.deinit();
        vfs.deleteTree(self.dir) catch {};
        self.alloc.free(self.dir);
    }

    /// Build `n` batches of `count` entries carrying `payload`, with
    /// consecutive indices, so the timed region holds only the WAL.
    fn buildBatches(
        self: *Wal,
        n: u64,
        count: usize,
        payload: String,
    ) ![]Batch {
        const out = try self.alloc.alloc(Batch, @intCast(n));
        for (out) |*batch| {
            batch.* = try self.alloc.alloc(zen.LogEntryBuffer, count);
            for (batch.*) |*e| {
                var w = try zen.LogEntryWriter.init(self.alloc);
                defer w.deinit();
                try w.set(.{
                    .index = self.next_index,
                    .term = 1,
                    .data = payload,
                    .entry_type = zen.EntryTypeEnum.data,
                    .conf_change = zen.NullOffset,
                });
                e.* = try w.finishWithTail();
                self.next_index += 1;
            }
        }
        return out;
    }

    fn freeBatches(self: *Wal, batches: []Batch) void {
        for (batches) |batch| {
            for (batch) |*e| e.deinit();
            self.alloc.free(batch);
        }
        self.alloc.free(batches);
    }
};

const Batch = []zen.LogEntryBuffer;

fn payloadOf(a: std.mem.Allocator, len: usize) ![]u8 {
    const p = try a.alloc(u8, len);
    @memset(p, 0x5a);
    return p;
}

/// One op = append a pre-built batch of `count` entries and make it
/// durable: the raft loop's per-iteration WAL cost with `count`
/// proposals queued.
fn benchDurable(b: *Bench, count: usize, payload_len: usize) !void {
    var w = try Wal.open(gpa);
    defer w.close();
    const payload = try payloadOf(gpa, payload_len);
    defer gpa.free(payload);
    const batches = try w.buildBatches(b.n, count, payload);
    defer w.freeBatches(batches);
    b.resetTimer();
    for (batches) |batch| {
        try w.log.addEntries(batch);
        try w.log.sync();
    }
    b.stopTimer();
}

/// One op = append a pre-built batch, no sync: the WAL's CPU side
/// (framing, CRC, slots, entry cache). Synced once at the end, untimed.
fn benchAppendOnly(b: *Bench, count: usize, payload_len: usize) !void {
    var w = try Wal.open(gpa);
    defer w.close();
    const payload = try payloadOf(gpa, payload_len);
    defer gpa.free(payload);
    const batches = try w.buildBatches(b.n, count, payload);
    defer w.freeBatches(batches);
    b.resetTimer();
    for (batches) |batch| try w.log.addEntries(batch);
    b.stopTimer();
    try w.log.sync();
}

/// One op = build one batch of `count` entries (LogEntryWriter per
/// entry): what the raft loop pays before the WAL sees a proposal.
fn benchBuild(b: *Bench, count: usize, payload_len: usize) !void {
    var w = try Wal.open(gpa);
    defer w.close();
    const payload = try payloadOf(gpa, payload_len);
    defer gpa.free(payload);
    b.resetTimer();
    const batches = try w.buildBatches(b.n, count, payload);
    b.stopTimer();
    w.freeBatches(batches);
}

test "BENCH: diskwal build batch, 21 x 400 B" {
    try benchBuild(bench.current(), 21, 400);
}
test "BENCH: diskwal durable batch, 1 x 400 B" {
    try benchDurable(bench.current(), 1, 400);
}
test "BENCH: diskwal durable batch, 21 x 400 B" {
    try benchDurable(bench.current(), 21, 400);
}
test "BENCH: diskwal durable batch, 1 x 8 KiB" {
    try benchDurable(bench.current(), 1, 8192);
}
test "BENCH: diskwal append only, 21 x 400 B" {
    try benchAppendOnly(bench.current(), 21, 400);
}

// A sustained run of 21 x 400 B durable batches long enough to cross
// a log file rotation (the files are pre-zeroed, so rotation is the
// only stall left to find): prints the latency spread, since the mean
// hides the stalls.
test "BENCH: diskwal sustained 21 x 400 B across a rotation" {
    const b = bench.current();
    var w = try Wal.open(gpa);
    defer w.close();
    const payload = try payloadOf(gpa, 400);
    defer gpa.free(payload);
    // 21 x ~470 B per batch: enough to cross one 32 MiB log file.
    const batches: usize = (32 << 20) / (21 * 470) + 200;
    const lat = try gpa.alloc(u64, batches);
    defer gpa.free(lat);
    const built = try w.buildBatches(batches, 21, payload);
    defer w.freeBatches(built);
    b.resetTimer();
    for (lat, built) |*l, batch| {
        const t0 = utils.timestampNow(.monotonic, .nanos);
        try w.log.addEntries(batch);
        try w.log.sync();
        l.* = @intCast(utils.timestampNow(.monotonic, .nanos) - t0);
    }
    b.stopTimer();
    std.mem.sort(u64, lat, {}, std.sort.asc(u64));
    const p = struct {
        fn at(s: []const u64, q: f64) u64 {
            const i: usize = @intFromFloat(q * @as(f64, @floatFromInt(s.len - 1)));
            return s[i];
        }
    };
    std.debug.print(
        "\n  {d} batches: p50 {d} us  p99 {d} us  p99.9 {d} us  max {d} us\n",
        .{
            lat.len,
            p.at(lat, 0.5) / 1000,
            p.at(lat, 0.99) / 1000,
            p.at(lat, 0.999) / 1000,
            lat[lat.len - 1] / 1000,
        },
    );
}
