const std = @import("std");
const m = @import("metrics");

// Raft-layer metrics. Same lifecycle pattern as zdb/metrics.zig: an
// application creates a `Metrics` once, library code reaches it via the
// `g()` global. Default is noop so unit tests + embedders that don't call
// initGlobal still compile and run safely.

pub const PhaseLabels = struct {
    phase: []const u8,
};

// Microsecond bucket boundaries — leader phases on a non-contended node
// should live in the 1-500µs band, so a tail above 1ms instantly screams
// "fsync stall" or "scheduling hazard". Last bucket = 50ms.
const phase_buckets: []const u64 = &.{ 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 50000 };

// Powers-of-two bucket boundaries for proposal-batch sizes. The propose
// pipeline drains the entire pending queue per call, so this histogram
// shows the actual group-commit width — the key driver of WAL amortization.
const batch_buckets: []const u64 = &.{ 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 4096 };

pub const PhaseHist = m.HistogramVec(u64, PhaseLabels, phase_buckets);
pub const BatchHist = m.Histogram(u64, batch_buckets);

pub const Metrics = struct {
    /// Wall-clock duration of each phase of the leader's mainLoop iteration.
    /// Phase taxonomy:
    ///   inbound          drain inbound_ch (handle AE replies, etc.)
    ///   propose          processProposalRequests total (queue + build + persist)
    ///   propose_build    sub-phase: walk requests, build LogEntry buffers
    ///   propose_persist  sub-phase: log.append + log.sync
    ///   advance          advanceCommitIndex + applyConfChanges + notify
    ///   broadcast        sendAppendEntries to all peers
    propose_phase_us: PhaseHist,
    /// How many proposals were drained out of the queue per
    /// `processProposalRequests` call. Higher = more group-commit
    /// amortization across the WAL fsync. Recorded only when the call had
    /// at least one proposal to handle (no zero samples — the propose
    /// phase histogram already encodes idle iterations).
    propose_batch_size: BatchHist,

    pub fn init(allocator: std.mem.Allocator) !Metrics {
        return .{
            .propose_phase_us = try PhaseHist.init(
                allocator,
                "raft_propose_phase_us",
                .{},
                .{},
            ),
            // Histogram (no labels) is a comptime construction — no allocator,
            // no error union. Storage is a fixed [N]u64 array of bucket counts.
            .propose_batch_size = BatchHist.init(
                "raft_propose_batch_size",
                .{},
                .{},
            ),
        };
    }

    pub fn deinit(self: *Metrics) void {
        self.propose_phase_us.deinit();
        // propose_batch_size has no dynamic state, no deinit needed.
    }

    /// noop returns a Metrics where every metric is a no-op. Library code can
    /// always call observe() unconditionally without checking for init.
    pub fn noop() Metrics {
        return m.initializeNoop(Metrics);
    }

    /// Observe `microTimestamp() - start_us` on the named phase. Errors from
    /// the underlying allocator (only possible the first time a new label is
    /// seen) are swallowed — losing one sample beats taking down the leader.
    pub fn observePhase(self: *Metrics, phase: []const u8, start_us: i64) void {
        const elapsed_us: u64 = @intCast(std.time.microTimestamp() - start_us);
        self.propose_phase_us.observe(.{ .phase = phase }, elapsed_us) catch {};
    }

    /// Record the drain depth — number of proposal requests pulled out of
    /// the queue in one `processProposalRequests` call. Skip the call
    /// entirely if `count == 0` so the histogram only describes active
    /// drains (matches `propose_build`'s sample count).
    pub fn observeBatchSize(self: *Metrics, count: usize) void {
        if (count == 0) return;
        self.propose_batch_size.observe(@intCast(count));
    }
};

var g_metrics: Metrics = Metrics.noop();

pub fn g() *Metrics {
    return &g_metrics;
}

pub fn initGlobal(allocator: std.mem.Allocator) !void {
    g_metrics = try Metrics.init(allocator);
}

pub fn deinitGlobal() void {
    g_metrics.deinit();
    g_metrics = Metrics.noop();
}
