const std = @import("std");
const m = @import("metrics");

// Storage-layer metrics. The `Metrics` struct is owned by the application
// (e.g. zdb-server), not by Forest — so multiple Forest instances (tests,
// future tenants) can share or swap the metric sink.
//
// Library code uses these via a `*Metrics` pointer. The `noop` variant of
// every metric means code paths not wired to a real Metrics instance are still
// safe to call — the ops become no-ops.

/// Labels for `flushes`: which tree (in which namespace) this flush targeted.
pub const FlushLabels = struct {
    namespace_id: u32,
    tree_id: u64,
};

/// Labels for `compactions`: which tree, at which destination level.
pub const CompactionLabels = struct {
    namespace_id: u32,
    tree_id: u64,
    level: u8,
};

/// Labels for `bytes_written`: write source (user|flush|compact) plus the
/// coordinate fields that are known at emit time. User writes don't know the
/// destination tree/level yet, so those default to 0 (sentinel "not applicable").
/// Write amp = sum(bytes_written) / sum(bytes_written{source="user"}).
pub const BytesWrittenLabels = struct {
    source: []const u8,
    namespace_id: u32 = 0,
    tree_id: u64 = 0,
    level: u8 = 0,
};

/// Top-level handle. Construct once in main, pass to Forest/Tree/server.
/// Adding a new metric is a one-line field here plus a call-site update.
pub const Metrics = struct {
    /// Per-tree flush outputs — incr once per SST produced by a memtable flush.
    flushes: m.CounterVec(u64, FlushLabels),
    /// Per-tree-per-level compactions — incr once per completed compaction run.
    compactions: m.CounterVec(u64, CompactionLabels),
    /// Bytes written, sliced by source + tree coordinates.
    bytes_written: m.CounterVec(u64, BytesWrittenLabels),

    pub fn init(allocator: std.mem.Allocator) !Metrics {
        return .{
            .flushes = try m.CounterVec(u64, FlushLabels).init(
                allocator,
                "zdb_flushes_total",
                .{},
                .{},
            ),
            .compactions = try m.CounterVec(u64, CompactionLabels).init(
                allocator,
                "zdb_compactions_total",
                .{},
                .{},
            ),
            .bytes_written = try m.CounterVec(u64, BytesWrittenLabels).init(
                allocator,
                "zdb_bytes_written_total",
                .{},
                .{},
            ),
        };
    }

    pub fn deinit(self: *Metrics) void {
        self.flushes.deinit();
        self.compactions.deinit();
        self.bytes_written.deinit();
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
