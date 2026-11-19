// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const m = @import("metrics");
const utils = @import("utils");
const String = utils.String;

// Storage-layer metrics. The `Metrics` struct is owned by the application
// (e.g. egres), not by Forest — so multiple Forest instances (tests,
// future tenants) can share or swap the metric sink.
//
// Library code uses these via a `*Metrics` pointer. The `noop` variant of
// every metric means code paths not wired to a real Metrics instance are still
// safe to call — the ops become no-ops.

// No metric carries a tree or a namespace: a tenant is a namespace,
// and 200,000 of them made a series per tree, a label lookup under
// the counter's lock on every table a flush installed and a scrape
// of 400,000 lines. What a tenant used is the meter's, not a metric.

/// Labels for `compactions` and `rollups`: the destination level.
pub const CompactionLabels = struct {
    level: u8,
};

/// Labels for `bytes_written`: write source (user|flush|compact) and
/// the destination level, 0 for user writes, which land in a memtable.
/// Write amp = sum(bytes_written) / sum(bytes_written{source="user"}).
pub const BytesWrittenLabels = struct {
    source: String,
    level: u8 = 0,
};

// Millisecond buckets for the L0 write brake: the curve runs from one
// step (a few ms) to `backpressure_max_ms`, and the tail says how
// often writes hit the cap.
const backpressure_buckets: []const u64 = &.{
    1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000,
};
pub const BackpressureHist = m.Histogram(u64, backpressure_buckets);

/// Labels for `compaction_ms`: the level pair a job merged, with no
/// tree, so the series stay a handful (L0->L0, L0->L1, L1->L2, ...).
pub const CompactionJobLabels = struct {
    src: u8,
    dest: u8,
};

// Millisecond buckets for a compaction's build, merge and write: an
// L0 merge takes about a second, an L2 merge minutes.
const compaction_buckets: []const u64 = &.{
    100,   250,   500,   1000,   2500,   5000,
    10000, 30000, 60000, 120000, 300000, 600000,
};
pub const CompactionHist =
    m.HistogramVec(u64, CompactionJobLabels, compaction_buckets);

/// Top-level handle. Construct once in main, pass to Forest/Tree/server.
/// Adding a new metric is a one-line field here plus a call-site update.
pub const Metrics = struct {
    /// Tables a memtable flush produced, one per tree it touched.
    flushes: m.Counter(u64),
    /// Completed compaction runs, per destination level.
    compactions: m.CounterVec(u64, CompactionLabels),
    /// Roaring delta chains folded into a synthetic state during
    /// compaction (leader-only; incremented at build time).
    rollups: m.CounterVec(u64, CompactionLabels),
    /// Bytes written, by source and destination level.
    bytes_written: m.CounterVec(u64, BytesWrittenLabels),
    /// How long each write batch slept in the L0 brake
    /// (`Engine.applyWriteBackpressure`); batches that did not sleep
    /// are not observed, so the count is the number of braked batches.
    write_backpressure_ms: BackpressureHist,
    /// How long each merge's build took (merge and write, not the
    /// propose and apply), per level pair (`Engine.runCompaction`).
    /// Moves and splits are not merges and are not observed.
    compaction_ms: CompactionHist,

    pub fn init(allocator: std.mem.Allocator) !Metrics {
        const io = utils.context.g().io;
        return .{
            // A comptime counter: no allocator, nothing to deinit.
            .flushes = m.Counter(u64).init("zdb_flushes_total", .{}, .{}),
            .compactions = try m.CounterVec(u64, CompactionLabels).init(
                allocator,
                io,
                "zdb_compactions_total",
                .{},
                .{},
            ),
            .rollups = try m.CounterVec(u64, CompactionLabels).init(
                allocator,
                io,
                "zdb_rollups_total",
                .{},
                .{},
            ),
            .bytes_written = try m.CounterVec(u64, BytesWrittenLabels).init(
                allocator,
                io,
                "zdb_bytes_written_total",
                .{},
                .{},
            ),
            // A comptime histogram: no allocator, nothing to deinit.
            .write_backpressure_ms = BackpressureHist.init(
                "zdb_write_backpressure_ms",
                .{},
                .{},
            ),
            .compaction_ms = try CompactionHist.init(
                allocator,
                io,
                "zdb_compaction_ms",
                .{},
                .{},
            ),
        };
    }

    pub fn deinit(self: *Metrics) void {
        self.compactions.deinit();
        self.rollups.deinit();
        self.bytes_written.deinit();
        self.compaction_ms.deinit();
    }

    /// noop returns a Metrics instance where every metric is a no-op. Useful
    /// for tests and single-node embedders that don't want metrics observed.
    pub fn noop() Metrics {
        return m.initializeNoop(Metrics);
    }
};

// Global singleton — mirrors the `vfs.g()` pattern used elsewhere in the repo.
// Starts as noop so library code can `metrics.g().flushes.incr(...)`
// unconditionally. The application calls `initGlobal(allocator)` at startup to
// swap in a real Metrics; tests can skip the init and observe no-ops.

var g_metrics: Metrics = Metrics.noop();

pub fn g() *Metrics {
    return &g_metrics;
}

/// Swap the global to a real Metrics. Call once at startup, before any spawn.
pub fn initGlobal(allocator: std.mem.Allocator) !void {
    g_metrics = try Metrics.init(allocator);
}

pub fn deinitGlobal() void {
    g_metrics.deinit();
    g_metrics = Metrics.noop();
}
