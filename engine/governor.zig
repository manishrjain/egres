// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The block cache's governor: sizes the cache from what the rest of
//! the process holds, so the cache takes the memory nothing else needs
//! and gives it back when something does.
//!
//! `total` (`--memory total_bytes`) is the memory the process may hold as
//! the allocator holds it: resident, what the cgroup charges. The
//! program counts its own live bytes (the counting allocator); the
//! allocator holds more on top (size-class rounding, freed pages not
//! yet returned), measured as `ratio`, resident bytes per live byte. The
//! live bytes allowed are `total / ratio`, and the cache gets those less
//! a margin kept for everything else. Ratio and margin both rise at once
//! and fall back within a minute, so the cache shrinks for a burst at once
//! and regrows slowly, and recurring bursts (a compaction's buffers) do
//! not swing it.
const std = @import("std");
const zdb = @import("zdb");
const galloc = @import("galloc");
const fiber = @import("zfiber");
const utils = @import("raft").utils;
const flags = @import("flags");
const log = std.log.scoped(.governor);

const BlockCache = zdb.cache.ShardedCache(u128);

/// The cache's cap: the value in force, and the cache it is applied to.
pub const Knob = struct {
    cache: *BlockCache,
    now: u64,

    pub fn init(cache: *BlockCache) Knob {
        return .{ .cache = cache, .now = cache.capacity() };
    }

    pub fn set(self: *Knob, next: u64) void {
        self.now = next;
        self.cache.setCapacity(next);
    }
};

/// Resident bytes per live byte, the allocator's overhead (size-class
/// rounding, freed pages not yet returned). `cur` is the recent peak
/// of what `measure` read: up at once, down a step per `update`.
pub const Ratio = struct {
    cur: f64 = default,
    measured: f64 = default,

    /// Until one is measured, and for an allocator that reports no
    /// resident bytes: the high end of what the orders runs saw (1.06
    /// to 1.11 under O_DIRECT).
    pub const default: f64 = 1.10;

    /// Takes the allocator's resident bytes against `live`, never under
    /// 1. Skipped while live is under `min_live`: on a small heap the
    /// allocator's fixed costs overstate it (1.69 at 1 GiB in a 64 GiB
    /// run).
    pub fn measure(self: *Ratio, live: u64, min_live: u64) void {
        if (live < min_live) return;
        const resident = galloc.snapshot().resident orelse return;
        const r = @as(f64, @floatFromInt(resident)) /
            @as(f64, @floatFromInt(live));
        self.measured = @max(1.0, r);
    }

    /// Moves `cur` toward `measured`: up at once, down
    /// `Governor.decay_ticks`th of the gap, as the margin moves.
    pub fn update(self: *Ratio) void {
        if (self.measured >= self.cur)
            self.cur = self.measured
        else
            self.cur -= (self.cur - self.measured) / Governor.decay_ticks;
    }

    /// The live bytes `total` resident bytes allow: `total / cur`.
    pub fn liveFor(self: Ratio, total: u64) u64 {
        return @intFromFloat(@as(f64, @floatFromInt(total)) / self.cur);
    }
};

/// `--memory`: the budget and what statements may take of it. Every
/// field is live: `Governor.apply` takes the current value before each
/// tick, so a change moves the cap on the next one.
pub const Cfg = struct {
    total_bytes: u64 = 0,
    query_ratio: f64 = 0.5,
    query_bytes: i64 = 0,

    pub const docs = .{
        .self = "The memory budget and what statements may take of it." ++
            " The budget is a cap, not a reservation: memory is used" ++
            " as blocks are read.",
        .total_bytes = "The overall memory Egres aims to hold, resident." ++
            " 0 = 90% of host RAM or of the cgroup limit. The" ++
            " block cache is auto-resized based on what the rest of the" ++
            " process uses and the allocator's measured overhead. Set it" ++
            " on a host shared with other processes.",
        .query_ratio = "The fraction of the memory budget statements may" ++
            " hold in all; past it a database over its share waits for" ++
            " room, then its statement is refused. The cache takes" ++
            " what statements leave.",
        .query_bytes = "Bytes one statement may hold before it is" ++
            " refused, any database. 0 = no cap.",
    };
};

/// `total_bytes` as a budget: itself, or 90% of what the host or the
/// cgroup allows when zero. The tenth is for the WAL's page cache, the
/// kernel and thread stacks.
pub fn resolveTotal(total_bytes: u64) u64 {
    if (total_bytes > 0) return total_bytes;
    return utils.host.memory().bytes / 10 * 9;
}

pub const Governor = struct {
    /// The memory the process may hold, resident (`--memory total_bytes`).
    total: u64,
    /// Kept for what the process holds outside the cache: the recent
    /// peak of it, never under `min_margin`.
    margin: u64 = min_margin,
    ratio: Ratio = .{},
    /// What statements may hold in all, as a fraction of the live
    /// target (`--memory query_ratio`).
    query_share: f64 = 0.5,
    /// The `--memory` flags, applied when their version moves; null
    /// in a test, which sets the fields itself.
    live: ?*flags.Live(Cfg),
    /// The version of `live` last applied; none before the first tick.
    applied: ?u64 = null,
    ticks: u32 = 0,
    knob: Knob,

    pub const min_cap: u64 = 64 << 20;
    pub const min_margin: u64 = 256 << 20;
    /// Each tick closes this fraction of the margin's and the ratio's
    /// gaps to what they follow.
    pub const decay_ticks = 120;
    /// Ticks between measures of the ratio: reading the allocator's
    /// stats refreshes them, cheap once a second, not every tick.
    const ratio_every_ticks = 10;

    pub fn init(
        total: u64,
        cache: *BlockCache,
        live: ?*flags.Live(Cfg),
    ) Governor {
        const gov: Governor = .{
            .total = total,
            .live = live,
            .knob = Knob.init(cache),
        };
        gov.storeLimit();
        return gov;
    }

    /// Takes the `--memory` flags: the total, the statements' share
    /// and the per-statement cap. A ratio outside (0, 1] cannot be
    /// refused where it was set, so it is left unapplied and said.
    fn apply(self: *Governor, cfg: Cfg) void {
        const total = resolveTotal(cfg.total_bytes);
        if (total != self.total) {
            log.info("total {d} MiB, was {d} MiB", .{
                total >> 20, self.total >> 20,
            });
            self.total = total;
        }
        if (cfg.query_ratio > 0 and cfg.query_ratio <= 1)
            self.query_share = cfg.query_ratio
        else
            utils.logEvery(.warn, 60, "--memory query_ratio {d} is not in" ++
                " (0, 1]: keeping {d}", .{ cfg.query_ratio, self.query_share });
        fiber.meter.query_mem_bytes.store(cfg.query_bytes, .monotonic);
    }

    /// The live bytes the process may hold: `total` over the ratio.
    pub fn liveTarget(self: *const Governor) u64 {
        return self.ratio.liveFor(self.total);
    }

    /// The statement pool: `query_share` of the live target.
    pub fn queryPool(self: *const Governor) u64 {
        const live: f64 = @floatFromInt(self.liveTarget());
        return @intFromFloat(live * self.query_share);
    }

    /// Allocations past the live target are counted, for `mem`.
    fn storeLimit(self: *const Governor) void {
        const limit = &utils.context.counting().limit;
        limit.store(self.liveTarget(), .monotonic);
    }

    /// Moves the margin for `others` live bytes held outside the cache
    /// and returns the cap, the live target less the margin. The margin
    /// rises to `others` at once and falls toward it a step per tick.
    pub fn nextCap(self: *Governor, others: u64) u64 {
        const held = @max(others, min_margin);
        if (held >= self.margin)
            self.margin = held
        else
            self.margin -= (self.margin - held) / decay_ticks;
        return @max(min_cap, self.liveTarget() -| self.margin);
    }

    /// Reads the counters, moves the ratio and the margin, and applies
    /// the cap. Then the tenants' tick: the statement pool is their
    /// memory capacity, and a tenant over its share waits while the
    /// pool is full or the cache is at its floor with nothing to give.
    pub fn tick(self: *Governor) void {
        if (self.live) |l| {
            const version = l.version();
            if (self.applied != version) {
                self.apply(l.cur());
                self.applied = version;
            }
        }
        const live = utils.context.counting().live_bytes.load(.monotonic);
        if (self.ticks % ratio_every_ticks == 0)
            self.ratio.measure(live, self.total / 4);
        self.ticks +%= 1;
        self.ratio.update();
        self.storeLimit();

        // What the cap governs is the cache's block bytes. Blocks it
        // evicted that readers still hold come from its allocator too
        // but not under the cap, so they count with the rest.
        const others = live -| self.knob.cache.totalCost();
        const cap = self.nextCap(others);
        if (cap != self.knob.now) {
            self.knob.set(cap);
            // The cap moves a little on most ticks: sample it every 5 s.
            utils.logEvery(.debug, 5, "governor: cache cap {d} MiB (margin " ++
                "{d} MiB, ratio {d:.3})", .{
                cap >> 20, self.margin >> 20, self.ratio.cur,
            });
        }

        const meter = fiber.meter;
        const held: u64 = @intCast(
            @max(meter.mem_held_total.load(.monotonic), 0),
        );
        const pool = self.queryPool();
        meter.capacity.mem_bytes = @intCast(pool);
        meter.mem_pressure.store(
            held >= pool or cap <= min_cap,
            .monotonic,
        );
        meter.tenants.tick(utils.coarseMs());
    }
};

/// The cache's cap before the first tick, for its creation.
pub fn startCap(total: u64) u64 {
    const unmeasured: Ratio = .{};
    return @max(
        Governor.min_cap,
        unmeasured.liveFor(total) -| Governor.min_margin,
    );
}

var global: Governor = undefined;
var installed: bool = false;

/// Installs the process's governor over `cache`: the engine runs its
/// ticks once started and `mem` prints it. The cache must outlive the
/// engine (`main` waits on it before the cache is freed).
pub fn init(total: u64, cache: *BlockCache, live: *flags.Live(Cfg)) void {
    global = Governor.init(total, cache, live);
    installed = true;
}

/// The process's governor, or null when none is installed (tests and
/// tools that start an engine without `main`).
pub fn g() ?*Governor {
    return if (installed) &global else null;
}
