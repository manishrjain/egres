// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

// DiskWAL micro-benchmarks: append + (optionally) fsync records through the
// raw `raft.diskwal` layer, bypassing Raft itself. Isolates the WAL from
// the propose queue / quorum / apply loop so we can tell whether the
// end-to-end ceiling we see in the egres load test is the WAL or
// something upstream.
//
// Run via `zig build bench`. Each cell auto-tunes its iteration count;
// each iteration appends one batch (and fsyncs per `fsync_mode`).
//
// What this file no longer does (vs. the previous standalone executable):
//   - No 3D matrix sweep (payload × batch × fsync). We pick representative
//     cells; if the matrix view is needed for a one-off perf investigation,
//     run cells via `zig build bench -- --filter <name>`.
//   - No p50/p99/max latency reporting. The framework reports `ns/op` (which
//     is the per-batch latency for these benches). Add latency percentiles
//     to the framework if you need them — see u/bench.zig + test_runner.zig.

const std = @import("std");
const raft = @import("raft");
const vfs = @import("vfs");
const utils = @import("utils");
const fiber = @import("zfiber");
const bench = fiber.bench;
const Bench = bench.Bench;

const zen = raft.buffers;
const diskwal = raft.diskwal;

const FsyncMode = enum {
    /// Append only, no fsync. Measures the OS write-cache ceiling.
    never,
    /// One fsync per batch. Models normal Raft batched commit.
    per_batch,
    /// One fsync per entry. Worst case for fsync-bound workloads.
    per_append,
};

/// Run one bench cell. The framework drives `b.n` outer iterations; each
/// iteration appends `batch_size` entries (plus optional fsync). Setup —
/// dir creation, payload allocation, ArrayList prep — is excluded from
/// the measurement via `b.resetTimer()`.
fn benchOne(
    b: *Bench,
    payload_size: usize,
    batch_size: u32,
    fsync_mode: FsyncMode,
) !void {
    // VFS uses a global bandwidth limiter; raise it so the bench measures
    // the WAL, not the throttle. Idempotent if already initialized; safe
    // to call across samples.
    vfs.VFS.initGlobal(.{ .bytes_per_sec = 10 * 1024 * 1024 * 1024 });

    // Fresh dir per sample. `std.testing.tmpDir` puts each one under
    // `.zig-cache/tmp/<random_hex>/` — real-disk path (NOT tmpfs, so
    // fsync actually waits for the disk), gitignored, unique per call so
    // concurrent `zig build bench` invocations don't collide.
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = utils.context.g().io;
    const dir = try tmp.dir.realPathFileAlloc(io, ".", b.alloc);
    defer b.alloc.free(dir);

    var dwal = try diskwal.DiskWAL.openPath(dir, b.alloc, .{ .cache_len = 0 });
    defer dwal.deinit();

    const payload = try b.alloc.alloc(u8, payload_size);
    defer b.alloc.free(payload);
    for (payload, 0..) |*c, idx| c.* = @truncate(idx ^ 0xA5);

    // Reusable batch scratch — entries get rebuilt each iter (index must
    // be monotonic) but the ArrayList stays put.
    var batch: std.ArrayList(zen.LogEntryBuffer) = .empty;
    defer {
        for (batch.items) |*e| e.deinit();
        batch.deinit(b.alloc);
    }
    try batch.ensureTotalCapacity(b.alloc, batch_size);

    b.resetTimer();

    const bytes_per_iter: u64 = @as(u64, batch_size) * @as(u64, payload_size);
    var next_index: u64 = 1;
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        for (batch.items) |*e| e.deinit();
        batch.clearRetainingCapacity();
        var k: u32 = 0;
        while (k < batch_size) : (k += 1) {
            var writer = try zen.LogEntryWriter.init(b.alloc);
            defer writer.deinit();
            try writer.set(.{
                .index = next_index,
                .term = 1,
                .timestamp = 0,
                .data = payload,
                .entry_type = zen.EntryTypeEnum.data,
                .conf_change = zen.NullOffset,
            });
            batch.appendAssumeCapacity(try writer.finishWithTail());
            next_index += 1;
        }

        switch (fsync_mode) {
            .per_append => for (batch.items) |*e| {
                try dwal.wal_store.addEntries(&.{e.*});
                try dwal.wal_store.syncLocked();
            },
            .per_batch => {
                try dwal.wal_store.addEntries(batch.items);
                try dwal.wal_store.syncLocked();
            },
            .never => try dwal.wal_store.addEntries(batch.items),
        }
        b.addBytes(bytes_per_iter);
    }
}

// ── Benchmarks ───────────────────────────────────────────────────────────

test "BENCH: diskwal 1KiB batch=1 per-batch-fsync" {
    try benchOne(bench.current(), 1024, 1, .per_batch);
}

test "BENCH: diskwal 1KiB batch=16 per-batch-fsync" {
    try benchOne(bench.current(), 1024, 16, .per_batch);
}

test "BENCH: diskwal 1KiB batch=256 per-batch-fsync" {
    try benchOne(bench.current(), 1024, 256, .per_batch);
}

test "BENCH: diskwal 1KiB batch=1 no-fsync" {
    try benchOne(bench.current(), 1024, 1, .never);
}

test "BENCH: diskwal 1KiB batch=16 no-fsync" {
    try benchOne(bench.current(), 1024, 16, .never);
}

test "BENCH: diskwal 1KiB batch=256 no-fsync" {
    try benchOne(bench.current(), 1024, 256, .never);
}

// ── The load benchmark's shape, with and without lz4 ─────────────────────
//
// What the WAL sees while benchmarks/run loads a table: every entry is
// one multi-row INSERT, 200 rows of about 1 KiB, and a durable write
// carries one entry from each load stream. On ext4 the WAL writes
// about 1.75 bytes per payload byte (each log file is zero-filled
// before use), which at 200k rows/s is most of what one SATA SSD takes.
// These cells ask what compressing each entry's payload with lz4 would
// change, on the disk the WAL would be on:
//
//     EGRES_BENCH_DIR=/wal zig build bench -- --filter "diskwal load"
//
// `none` writes the payloads as they are. `lz4` compresses each one
// inside the timed loop, on this one thread, where a server would do it
// in the proposing fibers, in parallel. `lz4-untimed` compresses them
// before the timer starts: what the disk alone gains. MiB/s counts the
// uncompressed payload in every cell, so the cells compare directly.
//
// One op here is `op_bytes` of payload, not one durable write: enough
// to cross several log files, so the zero-fill of each next file, half
// of what the disk writes, is in the measurement. Each cell also
// prints the latency of its durable writes.

/// Payload written before the timer starts: past the first log file,
/// so the one-off cost of preparing files at startup is not measured.
const warm_bytes: u64 = 320 << 20;
/// Payload per op: four log files' worth.
const op_bytes: u64 = 1 << 30;

const Compress = enum { none, lz4, lz4_untimed };

/// Rows shaped like the load benchmark's (`blended` in
/// benchmarks/run/workloads.py): per row, 240 random base64 characters
/// and 760 characters of words drawn from a 20,000-word vocabulary,
/// the common ones far more often. About 1.5x under lz4.
const Blend = struct {
    const row_chars = 1000;
    const random_chars = 240;
    const vocabulary = 20_000;
    const text_bytes = 2 << 20;
    const alphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    text: []u8,

    fn init(alloc: std.mem.Allocator) !Blend {
        var prng = std.Random.DefaultPrng.init(7);
        const rand = prng.random();
        const cons = "bcdfghjklmnprstvwz";
        const vowels = "aeiou";

        // The words: one to four syllables of consonant and vowel.
        const words = try alloc.alloc([12]u8, vocabulary);
        defer alloc.free(words);
        const lens = try alloc.alloc(u8, vocabulary);
        defer alloc.free(lens);
        for (words, lens) |*word, *len| {
            var n: u8 = 0;
            const syllables = rand.intRangeAtMost(u8, 1, 4);
            for (0..syllables) |_| {
                word[n] = cons[rand.uintLessThan(usize, cons.len)];
                word[n + 1] = vowels[rand.uintLessThan(usize, vowels.len)];
                n += 2;
                if (rand.boolean()) {
                    word[n] = "nrst"[rand.uintLessThan(usize, 4)];
                    n += 1;
                }
            }
            len.* = n;
        }

        // Frequency 1/rank: `cum[i]` is the weight of words 0..i.
        const cum = try alloc.alloc(f64, vocabulary);
        defer alloc.free(cum);
        var total: f64 = 0;
        for (cum, 1..) |*c, rank| {
            total += 1.0 / @as(f64, @floatFromInt(rank));
            c.* = total;
        }

        const text = try alloc.alloc(u8, text_bytes);
        var at: usize = 0;
        while (at < text.len) {
            const want = rand.float(f64) * total;
            var lo: usize = 0;
            var hi: usize = vocabulary - 1;
            while (lo < hi) {
                const mid = (lo + hi) / 2;
                if (cum[mid] < want) lo = mid + 1 else hi = mid;
            }
            const word = words[lo][0..lens[lo]];
            const n = @min(word.len, text.len - at);
            @memcpy(text[at .. at + n], word[0..n]);
            at += n;
            if (at < text.len) {
                text[at] = ' ';
                at += 1;
            }
        }
        return .{ .text = text };
    }

    fn deinit(self: Blend, alloc: std.mem.Allocator) void {
        alloc.free(self.text);
    }

    /// One statement's worth of rows, as bytes.
    fn rows(
        self: Blend,
        alloc: std.mem.Allocator,
        rand: std.Random,
        count: usize,
    ) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(alloc);
        for (0..count) |_| {
            try out.print(alloc, "({d}, 0, {d}, '", .{
                rand.int(u32),
                rand.intRangeAtMost(u16, 1, 1000),
            });
            for (0..random_chars) |_| {
                const pick = rand.uintLessThan(usize, alphabet.len);
                try out.append(alloc, alphabet[pick]);
            }
            const words_chars = row_chars - random_chars;
            const off = rand.uintLessThan(usize, self.text.len - words_chars);
            try out.appendSlice(alloc, self.text[off .. off + words_chars]);
            try out.appendSlice(alloc, "'),\n");
        }
        return out.toOwnedSlice(alloc);
    }
};

fn benchLoad(b: *Bench, batch_size: u32, mode: Compress) !void {
    vfs.VFS.initGlobal(.{ .bytes_per_sec = 10 * 1024 * 1024 * 1024 });

    // The WAL goes under EGRES_BENCH_DIR when it is set, so the cells
    // can be pointed at the disk in question; else a temp dir here.
    const io = utils.context.g().io;
    var tmp: ?std.testing.TmpDir = null;
    defer if (tmp) |*t| t.cleanup();
    const dir = if (utils.context.g().env("EGRES_BENCH_DIR")) |root| blk: {
        const ts = utils.timestampNow(.realtime, .nanos);
        const path = try std.fmt.allocPrint(
            b.alloc,
            "{s}/diskwal-bench-{d}",
            .{ root, ts },
        );
        try vfs.createDirPath(path);
        break :blk path;
    } else blk: {
        tmp = std.testing.tmpDir(.{});
        break :blk try tmp.?.dir.realPathFileAlloc(io, ".", b.alloc);
    };
    defer b.alloc.free(dir);
    defer if (tmp == null) vfs.deleteTree(dir) catch {};

    var dwal = try diskwal.DiskWAL.openPath(dir, b.alloc, .{
        .cache_len = 0,
    });
    defer dwal.deinit();

    // One distinct statement per stream, reused every iteration, and
    // its lz4 form for the untimed cell.
    const blend = try Blend.init(b.alloc);
    defer blend.deinit(b.alloc);
    var prng = std.Random.DefaultPrng.init(11);
    const payloads = try b.alloc.alloc([]u8, batch_size);
    defer b.alloc.free(payloads);
    const packed_payloads = try b.alloc.alloc([]u8, batch_size);
    defer b.alloc.free(packed_payloads);
    var raw_bytes: u64 = 0;
    var packed_bytes: u64 = 0;
    for (payloads, packed_payloads) |*payload, *packed_payload| {
        payload.* = try blend.rows(b.alloc, prng.random(), 200);
        var out: std.ArrayList(u8) = .empty;
        _ = try utils.compression.compress(b.alloc, .lz4, payload.*, &out);
        packed_payload.* = try out.toOwnedSlice(b.alloc);
        raw_bytes += payload.len;
        packed_bytes += packed_payload.len;
    }
    defer for (payloads, packed_payloads) |payload, packed_payload| {
        b.alloc.free(payload);
        b.alloc.free(packed_payload);
    };
    var load = Load{
        .alloc = b.alloc,
        .dwal = &dwal,
        .payloads = payloads,
        .packed_payloads = packed_payloads,
        .mode = mode,
    };
    defer load.deinit();
    try load.batch.ensureTotalCapacity(b.alloc, batch_size);

    var written: u64 = 0;
    while (written < warm_bytes) : (written += raw_bytes) try load.write();

    const writes_per_op = (op_bytes + raw_bytes - 1) / raw_bytes;
    var lat_ns: std.ArrayList(u64) = .empty;
    defer lat_ns.deinit(b.alloc);
    try lat_ns.ensureTotalCapacity(b.alloc, b.n * writes_per_op);

    b.resetTimer();

    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        for (0..writes_per_op) |_| {
            const t0 = utils.timestampNow(.monotonic, .nanos);
            try load.write();
            const t1 = utils.timestampNow(.monotonic, .nanos);
            lat_ns.appendAssumeCapacity(@intCast(t1 - t0));
            b.addBytes(raw_bytes);
        }
    }
    b.stopTimer();

    std.mem.sort(u64, lat_ns.items, {}, std.sort.asc(u64));
    const lats = lat_ns.items;
    std.debug.print(
        "diskwal load: batch={d} mode={t} payload={d} bytes lz4 {d:.2}x " ++
            "writes={d} write_ms p50={d:.2} p99={d:.2} max={d:.2}\n",
        .{
            batch_size,
            mode,
            raw_bytes / batch_size,
            @as(f64, @floatFromInt(raw_bytes)) /
                @as(f64, @floatFromInt(packed_bytes)),
            lats.len,
            @as(f64, @floatFromInt(lats[lats.len / 2])) / 1e6,
            @as(f64, @floatFromInt(lats[lats.len * 99 / 100])) / 1e6,
            @as(f64, @floatFromInt(lats[lats.len - 1])) / 1e6,
        },
    );
}

/// One durable write of the load: an entry per stream, appended and
/// synced.
const Load = struct {
    alloc: std.mem.Allocator,
    dwal: *diskwal.DiskWAL,
    payloads: []const []u8,
    packed_payloads: []const []u8,
    mode: Compress,
    batch: std.ArrayList(zen.LogEntryBuffer) = .empty,
    scratch: std.ArrayList(u8) = .empty,
    next_index: u64 = 1,

    fn deinit(self: *Load) void {
        for (self.batch.items) |*e| e.deinit();
        self.batch.deinit(self.alloc);
        self.scratch.deinit(self.alloc);
    }

    fn write(self: *Load) !void {
        for (self.batch.items) |*e| e.deinit();
        self.batch.clearRetainingCapacity();
        for (self.payloads, self.packed_payloads) |payload, packed_payload| {
            const data: utils.String = switch (self.mode) {
                .none => payload,
                .lz4_untimed => packed_payload,
                .lz4 => blk: {
                    self.scratch.clearRetainingCapacity();
                    _ = try utils.compression.compress(
                        self.alloc,
                        .lz4,
                        payload,
                        &self.scratch,
                    );
                    break :blk self.scratch.items;
                },
            };
            var writer = try zen.LogEntryWriter.init(self.alloc);
            defer writer.deinit();
            try writer.set(.{
                .index = self.next_index,
                .term = 1,
                .timestamp = 0,
                .data = data,
                .entry_type = zen.EntryTypeEnum.data,
                .conf_change = zen.NullOffset,
            });
            self.batch.appendAssumeCapacity(try writer.finishWithTail());
            self.next_index += 1;
        }
        try self.dwal.wal_store.addEntries(self.batch.items);
        try self.dwal.wal_store.syncLocked();
    }
};

test "BENCH: diskwal load 200rows batch=16 none" {
    try benchLoad(bench.current(), 16, .none);
}

test "BENCH: diskwal load 200rows batch=16 lz4" {
    try benchLoad(bench.current(), 16, .lz4);
}

test "BENCH: diskwal load 200rows batch=16 lz4-untimed" {
    try benchLoad(bench.current(), 16, .lz4_untimed);
}

test "BENCH: diskwal load 200rows batch=1 none" {
    try benchLoad(bench.current(), 1, .none);
}

test "BENCH: diskwal load 200rows batch=1 lz4" {
    try benchLoad(bench.current(), 1, .lz4);
}
