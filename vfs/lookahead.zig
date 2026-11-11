// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Ideal-readahead calibration: how large a read is still (half) "free"
//! on a store endpoint? Timed ranged GETs at doubling sizes feed a fit of
//! `latency(s) = ttfb + s/bandwidth`; the answer is `ttfb × bandwidth` —
//! the bytes the link moves in one round trip's worth of waiting. A short
//! discovery ladder bounds the sizes; interleaved sweeps add sample
//! depth; offsets rotate so no cache serves repeats.
//!
//! The result reflects the link AS MEASURED AT STARTUP: a run during a
//! congestion window adopts that window's (honest) number until the next
//! restart. Deliberate — nodes must never write their conclusions back
//! into the shared bucket.
//!
//! `calibrate` drives any store (duck-typed) that provides:
//!   probeRead(offset, len) !Probe    — one timed ranged read of the
//!                                      probe object
//!   head / put                       — check + upload the probe object
//! Today that is vfs.s3.S3Store (fixed cost = network round trip) and
//! the file store (fixed cost = syscall + device latency, read O_DIRECT
//! so the page cache can't stand in for the device).

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;

const log = std.log.scoped(.lookahead);

pub const probe_object = "_lookahead_probe";
pub const probe_max: u64 = 32 << 20; // probe object size = largest size tried
const probe_repeats = 3; // Repeat each probe this times per size

/// One timed ranged read: `size` bytes requested, first byte after
/// `ttfb_ns`, last after `total_ns`.
pub const Probe = struct {
    size: u64,
    ttfb_ns: u64,
    total_ns: u64,
};

/// Adaptive duration for logs ({f}): microseconds below 10 ms, else ms.
const Dur = struct {
    ns: u64,
    pub fn format(self: Dur, w: *std.Io.Writer) std.Io.Writer.Error!void {
        if (self.ns < 10 * std.time.ns_per_ms) {
            try w.print("{d}us", .{self.ns / std.time.ns_per_us});
        } else {
            try w.print("{d}ms", .{self.ns / std.time.ns_per_ms});
        }
    }
};

fn dur(ns: u64) Dur {
    return .{ .ns = ns };
}

/// Adaptive rate for logs ({f}): MB/s (one decimal) at MB scale, else KB/s.
const Rate = struct {
    bps: u64, // bytes per second
    pub fn format(self: Rate, w: *std.Io.Writer) std.Io.Writer.Error!void {
        if (self.bps >= 1_000_000) {
            try w.print("{d}.{d}MB/s", .{ self.bps / 1_000_000, (self.bps / 100_000) % 10 });
        } else {
            try w.print("{d}KB/s", .{self.bps / 1000});
        }
    }
};

fn rate(bps: u64) Rate {
    return .{ .bps = bps };
}

/// Measure the endpoint's ideal readahead and return it. Run at startup;
/// callers treat failure as non-fatal.
pub fn calibrate(a: std.mem.Allocator, store: anytype, block_size: u64) !u64 {
    // Upload the probe object if it's missing or too small (e.g. one
    // uploaded before probe_max grew); a larger one serves fine.
    const have = store.head(probe_object) catch 0;
    if (have < probe_max) try uploadProbe(a, store);

    // Every trial reads a random region of the probe object, different
    // in every run, so no CDN / range cache can serve a repeat of the
    // same slice and flatter the numbers (`randomOffset`).

    // A few throwaway fetches pay the cold-start cost — connection setup
    // on a network store, power-state exit on an idle device (NVMe APST
    // wake-up runs hundreds of us and would taint the first reads) — and
    // double as a content check: a probe object some older version
    // uploaded with different bytes fails verification and is replaced.
    const first_size = @max(block_size, 1);
    for (0..probe_repeats) |_| {
        const off = randomOffset(first_size);
        _ = store.probeRead(off, first_size) catch |err| switch (err) {
            error.BadProbeData => try uploadProbe(a, store),
            else => {},
        };
    }

    var probes: std.ArrayList(Probe) = .empty;
    defer probes.deinit(a);
    var ladder: std.ArrayList(u64) = .empty;
    defer ladder.deinit(a);
    // The transfer-dominated rungs — the only sizes that feed the fit's
    // bandwidth estimate, and so the only ones worth re-probing in
    // phase 2. (Smaller rungs matter for DISCOVERY — finding where
    // "transfer-dominated" starts without hammering a slow link — and
    // their TTFB samples come along for free.)
    var heavy: std.ArrayList(u64) = .empty;
    defer heavy.deinit(a);

    // Phase 1 — discovery: climb doubling sizes, a few trials each, until
    // consecutive sizes are transfer-dominated. Cheap, and it bounds
    // phase 2 so a slow link never hammers 16 MiB.
    var size_totals: std.ArrayList(u64) = .empty;
    defer size_totals.deinit(a);
    var rising: usize = 0;
    var size = first_size;
    while (size <= probe_max) : (size *= 2) {
        try ladder.append(a, size);

        size_totals.clearRetainingCapacity();
        for (0..probe_repeats) |_| {
            // A failed trial is skipped, never fatal; the minimum-sample
            // guard below catches a dead endpoint.
            const s = store.probeRead(randomOffset(size), size) catch |err| {
                log.warn("lookahead probe: size={d} trial failed: {s}", .{ size, @errorName(err) });
                continue;
            };
            try probes.append(a, s);
            try size_totals.append(a, s.total_ns);
        }
        if (size_totals.items.len == 0) continue; // every trial failed
        // Median, like every other decision here: a single spiked trial
        // (a TTFB burst on a tiny read) must not mark the size as risen —
        // stopping the climb early caps the answer, which is the
        // expensive error; climbing a rung too far only costs a few reads.
        const med_total_ns = median(size_totals.items);
        const med_ttfb_ns = try ttfbMedian(a, probes.items);
        log.debug("lookahead probe: size={d} total_med={f} ttfb_pooled={f}", .{
            size, dur(med_total_ns), dur(med_ttfb_ns),
        });
        // Only consecutive rising sizes end the climb — a single
        // transient shouldn't truncate it.
        if (med_total_ns > 2 * med_ttfb_ns) {
            rising += 1;
            try heavy.append(a, size);
        } else rising = 0;
        if (rising >= 3) break;
    }

    // Fast link: nothing was transfer-dominated (the fit will take the
    // largest probed size) — still sweep the top rungs so that verdict
    // rests on more than the climb's samples.
    if (heavy.items.len == 0) {
        const tail = @min(ladder.items.len, 3);
        try heavy.appendSlice(a, ladder.items[ladder.items.len - tail ..]);
    }

    // Phase 2 — interleaved sweeps over the transfer-dominated rungs: the
    // remaining trials run round-robin, spaced out in time, so a
    // transient dip degrades a few trials of EVERY size (which the fit's
    // medians shed) instead of every trial of the sizes measured during
    // the dip.
    for (0..2 * probe_repeats) |sweep| {
        fiber.sleepMs(300);
        const start_ms = utils.coarseMs();
        const before = probes.items.len;
        for (heavy.items) |sz| {
            const s = store.probeRead(randomOffset(sz), sz) catch |err| {
                log.warn("lookahead probe: size={d} trial failed: {s}", .{ sz, @errorName(err) });
                continue;
            };
            try probes.append(a, s);
        }
        log.debug("lookahead sweep {d}/{d}: {d} probes across {d} sizes " ++
            "in {d} ms ({d} total)", .{
            sweep + 1,       2 * probe_repeats,           probes.items.len - before,
            heavy.items.len, utils.coarseMs() - start_ms, probes.items.len,
        });
    }

    // Too few survivors means the endpoint is effectively down; 0 from
    // the fit means the measurements were degenerate. Fail rather than
    // report an answer-shaped guess.
    if (probes.items.len < 3 * probe_repeats) return error.CalibrationFailed;
    const measured = try pickLookahead(a, probes.items);
    if (measured == 0) return error.CalibrationFailed;

    // Confirmation reads at 0.5x / 1x / 2x of the recommendation: the fit
    // interpolated it between ladder rungs, so measure directly what
    // half-knee, knee, and double-knee reads actually cost (the model
    // says 1.5x / 2x / 3x the TTFB — the c multiplier a consumer picks
    // maps straight to total latency (1+c)x). Report-only — failures
    // here never affect the recommendation.
    const confirm_sizes = [3]u64{ measured / 2, measured, 2 * measured };
    var confirm_meds = [3]u64{ 0, 0, 0 };
    for (confirm_sizes, &confirm_meds) |sz, *med| {
        if (sz == 0 or sz > probe_max) continue;
        var totals: std.ArrayList(u64) = .empty;
        defer totals.deinit(a);
        for (0..probe_repeats) |_| {
            const s = store.probeRead(randomOffset(sz), sz) catch continue;
            try totals.append(a, s.total_ns);
        }
        med.* = median(totals.items);
    }
    log.info("lookahead calibrated: {d} bytes in median {f} (0.5x={d} in {f}, 2x={d} in {f}, block_size={d} ttfb_med={f})", .{
        measured,         dur(confirm_meds[1]), confirm_sizes[0], dur(confirm_meds[0]),
        confirm_sizes[2], dur(confirm_meds[2]), block_size,       dur(try ttfbMedian(a, probes.items)),
    });
    return measured;
}

/// A random offset for a read of `size` — anywhere in the probe object.
/// A stray near-collision at most cache-warms one trial, which the fit's
/// medians shed.
fn randomOffset(size: u64) u64 {
    // A fresh handle per draw: it is the worker's, and the caller parks
    // on every read.
    return fiber.random().uintAtMost(u64, probe_max - size);
}

/// The probe object's byte at absolute position `i`: a cheap
/// position-dependent pattern (multiply-shift), so any ranged read
/// verifies byte-for-byte — and, unlike zeros, it is incompressible, so
/// no layer can shrink the transfer and flatter the bandwidth estimate.
pub fn probeByte(i: u64) u8 {
    return @truncate((i *% 0x9E3779B97F4A7C15) >> 56);
}

/// Verify a ranged read of the probe object against the pattern.
pub fn verifyProbeData(off: u64, bytes: String) bool {
    var failed: u32 = 0;
    for (bytes, 0..) |b, i| {
        failed += @intFromBool(b != probeByte(off + i));
    }
    return failed == 0;
}

fn uploadProbe(a: std.mem.Allocator, store: anytype) !void {
    // Page-aligned: the file store writes puts O_DIRECT and rejects
    // unaligned buffers (probe_max is a page multiple already).
    const data = try a.alignedAlloc(u8, .fromByteUnits(4096), @intCast(probe_max));
    defer a.free(data);
    for (data, 0..) |*b, i| b.* = probeByte(i);
    try store.put(probe_object, data);
}

/// Fit `latency(s) = a + s/b` over every trial and return `a × b` — the
/// size whose transfer time equals the fixed per-request cost, i.e. the
/// largest read that is still (half) "free" — rounded to 64 KiB
/// granularity and clamped to the probed range. Fitting beats
/// threshold-crossing per size: near the knee a threshold rule is binary
/// (noise flips the answer 2x per run), while `a` (pooled TTFB median)
/// and `b` (median bandwidth over transfer-dominated trials) each pool
/// dozens of samples.
pub fn pickLookahead(a: std.mem.Allocator, trials: []const Probe) !u64 {
    if (trials.len == 0) return 0;

    // The probed size range bounds the final answer.
    var floor: u64 = std.math.maxInt(u64);
    var cap: u64 = 0;
    for (trials) |t| {
        floor = @min(floor, t.size);
        cap = @max(cap, t.size);
    }

    // The model's fixed per-request cost — TTFB is size-independent, so
    // every trial contributes one sample. A zero median means the
    // measurements are degenerate (broken clock): report "uncalibrated",
    // don't guess.
    const med_ttfb_ns = try ttfbMedian(a, trials);
    if (med_ttfb_ns == 0) return 0;

    // Bandwidth, from trials where the transfer carries real signal
    // (transfer time ≥ half the fixed cost — small sizes are all noise).
    var rates: std.ArrayList(u64) = .empty;
    defer rates.deinit(a);
    for (trials) |t| {
        if (t.total_ns <= t.ttfb_ns) continue;
        const xfer_ns = t.total_ns - t.ttfb_ns;
        if (2 * xfer_ns < med_ttfb_ns) continue;
        try rates.append(a, @intCast(@as(u128, t.size) * std.time.ns_per_s / xfer_ns));
    }
    // No transfer-dominated trial at all -- NOT "no information": even the
    // largest probed size transferred in under half the fixed cost, so the
    // true knee lies beyond cap. Return cap, the largest size we have
    // direct evidence is still free; a smaller default would under-read on
    // exactly the fast links that reach this branch, and extrapolating
    // past cap would claim sizes we never measured.
    if (rates.items.len == 0) {
        log.info("lookahead fit: no transfer-dominated probe out of {d} — even {d} is fixed-cost-dominated; taking it", .{
            trials.len, cap,
        });
        return cap;
    }
    const bw = median(rates.items); // bytes/sec

    // Based on the bw above, how many bytes can we move in a median time
    // to first byte.
    const moved_bytes: u64 = @intCast(@as(u128, med_ttfb_ns) * bw / std.time.ns_per_s);

    // Round to 64 KiB granularity, NOT powers of two: pow2 bins flap when
    // the estimate sits near a boundary (2 MiB one run, 4 MiB the next);
    // fine granularity lets the answer move smoothly with the link. Below
    // 64 KiB (local disks: NVMe knees sit in the tens of KB) drop to page
    // granularity so the answer doesn't round to zero.
    const granule: u64 = if (moved_bytes < 64 << 10) 4 << 10 else 64 << 10;
    const rounded = ((moved_bytes + granule / 2) / granule) * granule;
    log.info("lookahead fit: ttfb_med={f} x bw={f} ({d}/{d} transfer-dominated probes) = {d} bytes; rounded to {d}, clamped to [{d}, {d}]", .{
        dur(med_ttfb_ns), rate(bw), rates.items.len, trials.len,
        moved_bytes,      rounded,  floor,           cap,
    });
    return @max(floor, @min(cap, rounded));
}

/// Pooled fixed-cost estimate over collected trials: TTFB is
/// size-independent, so every trial contributes one sample and the
/// estimate sharpens as trials accumulate.
fn ttfbMedian(a: std.mem.Allocator, trials: []const Probe) !u64 {
    var ttfbs: std.ArrayList(u64) = .empty;
    defer ttfbs.deinit(a);
    for (trials) |t| try ttfbs.append(a, t.ttfb_ns);
    return median(ttfbs.items);
}

fn median(vals: []u64) u64 {
    if (vals.len == 0) return 0;
    std.mem.sort(u64, vals, {}, std.sort.asc(u64));
    return vals[vals.len / 2];
}
