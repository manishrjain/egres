// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The meter: how a shared rate is split by class and by tenant, for
//! cpu and io alike.
//!
//! A `Resource` is one shared rate's buckets, one per class in class order,
//! filled in that order: `rate` units arrive per second, the fg bucket
//! takes them up to `burst`, and only the overflow reaches bg. So bg
//! holds what fg left, and "foreground first" is arithmetic, not a
//! rank; a third class would be one more enum value.
//! A request of either class waits until its bucket is out of debt
//! (`pay`), then takes its whole cost, as far below zero as the cost
//! says (`charge`): a debt is at most one request and is paid before
//! the next. A balance below zero is debt, there is no other word for
//! it here. No floor anywhere: a 1 GB read on a 100 MB/s share holds
//! that tenant for ten seconds.
//!
//! A `Tenant` is what one database's statements are charged to: a
//! resource per kind (cpu in the machine's ticks, io in pages), a
//! balance for memory, the lifetime totals, and a `Limit`, its size in
//! the units a plan is sold in. The policy looks at the tenant's own
//! buckets and nothing else: `pay` waits on them, `charge` and `take`
//! draw on them. Work no session resolved, the WAL, the manifest,
//! raft, runs as the `system` tenant, which never waits and only
//! counts what it spent.
//!
//! The `Tenants` set turns the limits into rates once a tick. What the
//! system spent since the last tick comes off `capacity` first, and
//! the active tenants split the rest by the fair-share rule:
//!
//!     rate_i = left × limit_i / Σ limit_j over the active tenants
//!
//! so the limit is the share a tenant is guaranteed when the box is
//! sold out and its weight when it is not. A tenant alone gets the
//! box; idle tenants are not in the sum. A tenant never goes beyond its
//! rate: unused share moves between tenants at the tick, not through
//! the buckets.
//!
//! Memory is held, not spent, so it has no bucket: the tick gives
//! each active tenant a share of `capacity.mem_bytes`, the statement
//! pool, and a tenant holding more than its share waits
//! (`payForMem`) only while the pool is full or the cache can give
//! nothing back (`mem_pressure`, the governor's word). Any other time
//! it just runs and the cache shrinks. The wait is bounded, since a
//! waiting statement can hold a lock a running one needs, and ends
//! in a refusal. A single statement past `query_mem_bytes` is refused
//! at its next yield.
//!
//! Time is the caller's: every call takes `now_ms`, a monotonic
//! millisecond clock, so all of it is testable and the runtime passes
//! its coarse clock. A resource is lock free: a CAS on `last_ms` picks one
//! filler per window. Where a charge comes from (the worker loop, the
//! bandwidth grant, a statement arena) and where a fiber parks
//! (`Ctx.maybeYield`, the grant) is the callers' business; see
//! notes/egres-multi-tenancy-plan.md.

const std = @import("std");
const _rt = @import("runtime.zig");
const _clock = @import("utils").coarse_clock;
const sleepMs = @import("deadlines.zig").sleepMs;

/// The class of a piece of work: foreground is what a user waits on,
/// queries, the WAL, flushes; background is compactions, log file
/// prezero, index builds. Nothing ranks one background job over
/// another.
pub const IoClass = enum(u1) { fg, bg };

/// The two resources a tenant is metered on.
pub const Kind = enum { cpu, io };

const classes = @typeInfo(IoClass).@"enum".fields.len;
const kinds = @typeInfo(Kind).@"enum".fields.len;

/// One balance: filled by its resource, taken by the policy.
pub const Bucket = struct {
    tokens: std.atomic.Value(i64) = .init(0),

    /// Negative is debt.
    pub fn balance(self: *const Bucket) i64 {
        return self.tokens.load(.monotonic);
    }

    /// All of `cost`, as far below zero as it goes. Never waits.
    pub fn take(self: *Bucket, cost: i64) void {
        _ = self.tokens.fetchSub(cost, .monotonic);
    }

    /// Adds up to `amount`, stopping at `cap`; returns what went in,
    /// so the resource can pass the rest down the class order.
    pub fn fill(self: *Bucket, amount: i64, cap: i64) i64 {
        while (true) {
            const old = self.tokens.load(.monotonic);
            const new = @min(old +| amount, cap);
            if (new <= old) return 0;
            if (self.tokens.cmpxchgWeak(
                old,
                new,
                .monotonic,
                .monotonic,
            ) == null) return new - old;
        }
    }
};

/// One shared rate's buckets, one per class, filled in class order.
pub const Resource = struct {
    /// Units per second. Never negative; signed so a balance, a debt
    /// and a rate are one arithmetic with no casts between them.
    rate: std.atomic.Value(i64),
    /// The most any bucket holds: what an idle stretch saves up.
    burst: std.atomic.Value(i64),
    last_ms: std.atomic.Value(i64),
    /// Indexed by `IoClass`, in the order the refill fills them.
    buckets: [classes]Bucket,

    /// The wait when the rate is zero: nothing refills, so a waiter
    /// re-checks on the next tick.
    pub const idle_wait_ms = 100;
    pub const max_wait_ms = 1000;

    /// Every bucket full.
    pub fn init(rate: i64, burst: i64, now_ms: i64) Resource {
        return .{
            .rate = .init(rate),
            .burst = .init(burst),
            .last_ms = .init(now_ms),
            .buckets = @splat(.{ .tokens = .init(burst) }),
        };
    }

    /// A new rate and burst, from the next refill on. The balances
    /// stay: a debt is still owed, a saving still spends.
    pub fn setRate(self: *Resource, rate: i64, burst: i64) void {
        self.rate.store(rate, .monotonic);
        self.burst.store(burst, .monotonic);
    }

    pub fn bucket(self: *Resource, class: IoClass) *Bucket {
        return &self.buckets[@intFromEnum(class)];
    }

    /// Adds what the rate earned since the last refill to the buckets
    /// in class order: each takes its debt and then room up to
    /// `burst`, and the next gets what is left. A window too short to
    /// earn one unit is left for the next call, so a low rate still
    /// accrues.
    pub fn refill(self: *Resource, now_ms: i64) void {
        const last = self.last_ms.load(.monotonic);
        const elapsed = now_ms - last;
        if (elapsed <= 0) return;

        const rate = self.rate.load(.monotonic);
        if (rate <= 0) return;

        // The system's rate is 2^62: the product needs the width, and
        // the clamp is what `fill` saturates on.
        const wide = @divTrunc(@as(i128, elapsed) * rate, 1000);
        const earned: i64 = @intCast(@min(wide, std.math.maxInt(i64)));
        if (earned == 0) return;

        if (self.last_ms.cmpxchgStrong(
            last,
            now_ms,
            .monotonic,
            .monotonic,
        ) != null) return;

        const cap = self.burst.load(.monotonic);
        var rest = earned;
        for (&self.buckets) |*b| rest -= b.fill(rest, cap);
    }

    pub fn take(self: *Resource, class: IoClass, cost: i64, now_ms: i64) void {
        self.refill(now_ms);
        self.bucket(class).take(cost);
    }

    /// The class's balance after the refill; negative is debt.
    pub fn balance(self: *Resource, class: IoClass, now_ms: i64) i64 {
        self.refill(now_ms);
        return self.bucket(class).balance();
    }

    /// Milliseconds until the rate has paid the class's debt, at least
    /// 1 and at most `max_wait_ms`; 0 when there is none. The rate
    /// fills the buckets before this one first, so their room is in
    /// the sum: bg sleeps once for fg to fill, not once per unit.
    pub fn waitMs(self: *Resource, class: IoClass) u64 {
        const own = self.bucket(class).tokens.load(.monotonic);
        if (own >= 0) return 0;

        const rate = self.rate.load(.monotonic);
        if (rate <= 0) return idle_wait_ms;

        const cap = self.burst.load(.monotonic);
        var need: i128 = -own;
        for (self.buckets[0..@intFromEnum(class)]) |*ahead| {
            need += @max(cap - ahead.tokens.load(.monotonic), 0);
        }
        // `need` units at `rate` per second, in whole ms, rounded up.
        const ms = std.math.divCeil(i128, need * 1000, rate) catch unreachable;
        return @intCast(@min(ms, max_wait_ms));
    }
};

pub const Tenant = struct {
    id: u64,
    limit: Limit,
    /// Ticks of the machine's counter, what the worker loop reads.
    cpu: Resource,
    /// Pages; a write costs the device's write cost in pages.
    io: Resource,
    /// Live bytes held by the tenant's contexts, and the bytes the last
    /// tick gave it: over the share under pressure, a statement waits.
    mem_held: std.atomic.Value(i64) = .init(0),
    mem_share: std.atomic.Value(i64) = .init(0),
    last_active_ms: std.atomic.Value(i64),
    totals: Totals = .{},

    /// A tenant's size, in the units a plan is sold in: cpu in
    /// millicores (1000 per core, so 500 is half a core), io in pages
    /// per second, memory in bytes. The three are independent, so a
    /// plan can be cpu-heavy or memory-heavy. Each is positive: a
    /// zero would be a tenant with none of that resource, whose rate
    /// never refills and whose park never ends. Signed like the rates
    /// they become.
    pub const Limit = struct {
        cpu_milli: i64,
        io_pages_per_s: i64,
        mem_bytes: i64,

        pub fn assertPositive(self: Limit) void {
            std.debug.assert(self.cpu_milli > 0);
            std.debug.assert(self.io_pages_per_s > 0);
            std.debug.assert(self.mem_bytes > 0);
        }

        /// The cpu limit in ticks per second on a machine whose tick
        /// counter runs at `hz`: what the tenant gets when the box is
        /// exactly sold out.
        pub fn cpuTicks(self: Limit, hz: u64) i64 {
            return @intCast(@divTrunc(@as(i128, hz) * self.cpu_milli, 1000));
        }
    };

    /// Lifetime counters, for the admin `tenants` line and metrics.
    pub const Totals = struct {
        cpu_ticks: std.atomic.Value(i64) = .init(0),
        io_pages: std.atomic.Value(i64) = .init(0),
        mem_peak: std.atomic.Value(i64) = .init(0),
        parked_ms: std.atomic.Value(u64) = .init(0),
        statements: std.atomic.Value(u64) = .init(0),
        failed: std.atomic.Value(u64) = .init(0),
    };

    /// A tenant with anything to its name in this window is in the
    /// share split. Longer than a tick so a tenant with a statement
    /// every few hundred ms does not flap in and out.
    pub const active_window_ms = 1000;

    /// Starts with its limit as its rate, the sold-out box's answer,
    /// and full buckets; the next tick sets the real rate.
    pub fn init(id: u64, limit: Limit, now_ms: i64) Tenant {
        limit.assertPositive();
        const cpu = limit.cpuTicks(_rt.ticksPerSecond());
        const io = limit.io_pages_per_s;
        return .{
            .id = id,
            .limit = limit,
            .cpu = Resource.init(cpu, burstFor(cpu), now_ms),
            .io = Resource.init(io, burstFor(io), now_ms),
            .last_active_ms = .init(now_ms),
        };
    }

    pub fn resource(self: *Tenant, kind: Kind) *Resource {
        return switch (kind) {
            .cpu => &self.cpu,
            .io => &self.io,
        };
    }

    pub fn total(self: *Tenant, kind: Kind) *std.atomic.Value(i64) {
        return switch (kind) {
            .cpu => &self.totals.cpu_ticks,
            .io => &self.totals.io_pages,
        };
    }

    pub fn isActive(self: *const Tenant, now_ms: i64) bool {
        const last = self.last_active_ms.load(.monotonic);
        return now_ms - last < active_window_ms;
    }

    /// A statement started: in the split from now.
    pub fn touch(self: *Tenant, now_ms: i64) void {
        self.last_active_ms.store(now_ms, .monotonic);
        _ = self.totals.statements.fetchAdd(1, .monotonic);
    }

    /// A statement arena grew or shrank. The peak is a statistic: two
    /// racing updates may lose one, never the balance.
    pub fn holdMem(self: *Tenant, n: i64) void {
        const held = self.mem_held.fetchAdd(n, .monotonic) + n;
        if (held > self.totals.mem_peak.load(.monotonic))
            self.totals.mem_peak.store(held, .monotonic);
    }

    pub fn releaseMem(self: *Tenant, n: i64) void {
        _ = self.mem_held.fetchSub(n, .monotonic);
    }

    /// Over the memory share, the tick's number, while the cache can
    /// give nothing back.
    pub fn overMem(self: *const Tenant) bool {
        return mem_pressure.load(.monotonic) and
            self.mem_held.load(.monotonic) > self.mem_share.load(.monotonic);
    }

    /// Room for a statement: waits while the tenant is over its share
    /// under pressure, at most `mem_wait_ms`, then refuses. Polls: a
    /// waiter exists only under pressure, and a wake on every release
    /// would cost more than a sleep this short.
    pub fn payForMem(self: *Tenant) error{MemShare}!void {
        var waited: u64 = 0;
        while (self.overMem()) {
            if (waited >= mem_wait_ms) return error.MemShare;
            sleepMs(mem_poll_ms);
            waited += mem_poll_ms;
            _ = self.totals.parked_ms.fetchAdd(mem_poll_ms, .monotonic);
        }
    }

    pub fn failed(self: *Tenant) void {
        _ = self.totals.failed.fetchAdd(1, .monotonic);
    }

    /// The policy, for cpu and io alike, over the tenant's own buckets
    /// alone: the tick made them the tenant's share of the box.
    ///
    /// Waits until the class's bucket is out of debt. The ms slept go
    /// to `stats` and the tenant's `parked_ms`.
    pub fn pay(self: *Tenant, kind: Kind, class: IoClass) void {
        const own = self.resource(kind);
        while (own.balance(class, _clock.ms()) < 0) {
            self.sleep(kind, class, own.waitMs(class));
        }
    }

    /// A sleep is a wait, charged at what was asked for: an oversleep
    /// under load goes uncounted.
    fn sleep(self: *Tenant, kind: Kind, class: IoClass, ms: u64) void {
        sleepMs(ms);
        _ = statsFor(kind, class).wait_ms.fetchAdd(ms, .monotonic);
        _ = self.totals.parked_ms.fetchAdd(ms, .monotonic);
    }

    /// A grant before the work: pay, then charge the whole cost. Cpu,
    /// charged after the fact, never asks.
    pub fn acquire(
        self: *Tenant,
        kind: Kind,
        class: IoClass,
        cost: i64,
    ) void {
        std.debug.assert(cost > 0);
        _ = statsFor(kind, class).grants.fetchAdd(1, .monotonic);
        self.pay(kind, class);
        self.charge(kind, class, cost, _clock.ms());
    }

    /// A cost that is not refused: taken whole and counted in the
    /// totals.
    pub fn charge(
        self: *Tenant,
        kind: Kind,
        class: IoClass,
        cost: i64,
        now_ms: i64,
    ) void {
        self.resource(kind).take(class, cost, now_ms);
        _ = self.total(kind).fetchAdd(cost, .monotonic);
        self.last_active_ms.store(now_ms, .monotonic);
    }
};

/// What every grant paid, process wide, by kind and class: grants
/// asked for and the ms slept waiting in all. A tenant's own waits
/// are in its `totals`.
pub const Stats = struct {
    grants: std.atomic.Value(u64) = .init(0),
    wait_ms: std.atomic.Value(u64) = .init(0),
};

pub var stats: [kinds][classes]Stats = @splat(@splat(.{}));

pub fn statsFor(kind: Kind, class: IoClass) *Stats {
    return &stats[@intFromEnum(kind)][@intFromEnum(class)];
}

/// The system's tenant, id 0: the default on every Ctx, and what the
/// WAL, the manifest, raft and anything no session resolved runs as.
/// Its buckets are never short: the rate is as large as a balance
/// allows, and it is not in the `Tenants` map. Its totals are what
/// the tick takes off the box before the tenants split it.
pub var system: Tenant = .{
    .id = 0,
    .limit = .{ .cpu_milli = 1, .io_pages_per_s = 1, .mem_bytes = 1 },
    .cpu = Resource.init(never_short, never_short, 0),
    .io = Resource.init(never_short, never_short, 0),
    .mem_share = .init(never_short),
    .last_active_ms = .init(0),
};
const never_short: i64 = 1 << 62;

/// The box, in a limit's units: the default lane's workers in
/// millicores, the pages per second of the device the tenants' data
/// is on, and the statement pool, `--memory query_ratio` of the live
/// target. Main sets cpu and io at startup; the governor sets memory
/// every tick. Zero until then, a sold-out box.
pub var capacity: Tenant.Limit = .{
    .cpu_milli = 0,
    .io_pages_per_s = 0,
    .mem_bytes = 0,
};

/// The pool is full, or the cache is at its floor: a tenant over its
/// share waits. The governor's, each tick.
pub var mem_pressure: std.atomic.Value(bool) = .init(false);

/// Live bytes held by every `Ctx`, all tenants: against the pool,
/// for the governor.
pub var mem_held_total: std.atomic.Value(i64) = .init(0);

/// A statement's live bytes past which its next yield refuses it;
/// 0 = no cap. `--memory query_bytes`, written by the governor's
/// fiber and read at every statement's yield.
pub var query_mem_bytes: std.atomic.Value(i64) = .init(0);

/// How long `payForMem` waits for room before the statement is
/// refused.
pub var mem_wait_ms: u64 = 5000;
const mem_poll_ms: u64 = 2;

/// Every tenant's limit until one is set for it, so they weigh the
/// same; the units matter once a limit can differ.
pub const default_limit: Tenant.Limit = .{
    .cpu_milli = 1000,
    .io_pages_per_s = 25_000,
    .mem_bytes = 1 << 30,
};

/// The process's tenants, ticked by the cache governor, on the
/// runtime's allocator from the first session on.
pub var tenants: Tenants = .{ .default_limit = default_limit };

/// One tick's worth of a rate: what a bucket holds at most.
pub fn burstFor(rate: i64) i64 {
    return @divTrunc(rate * Tenants.tick_ms, 1000);
}

/// Every tenant the process has seen, by id, and the tick that sets
/// their rates. Each is made on first sight with the default limit.
pub const Tenants = struct {
    /// Null until the first `get`, which takes the runtime's: a global
    /// has no allocator to name at comptime. A test gives its own.
    alloc: ?std.mem.Allocator = null,
    map: std.AutoHashMapUnmanaged(u64, *Tenant) = .empty,
    /// Readers share `map`: a lookup, and the tick, which writes only
    /// the tenants' atomics. A writer holds it alone: the insert of a
    /// new tenant, and a new limit, a plain field the tick reads.
    lock: RWMutex = .{},
    default_limit: Limit,
    /// The tick's active set, decided once per tick: the stamp a
    /// tenant becomes active by is written without the lock, so a
    /// second pass over the map could find one the sum left out.
    /// Scratch of the one fiber that ticks; keeps its capacity.
    active: std.ArrayList(*Tenant) = .empty,
    /// The system's totals at the last tick, by `Kind`, and when it
    /// was: what it spent since is off the box this tick.
    system_at_tick: [kinds]i64 = @splat(0),
    last_tick_ms: i64 = 0,

    const Limit = Tenant.Limit;
    const RWMutex = @import("sync.zig").RWMutex(.untracked);

    /// How often `tick` runs, the governor's cadence.
    pub const tick_ms: i64 = 100;

    pub fn init(alloc: std.mem.Allocator, limit: Limit) Tenants {
        limit.assertPositive();
        return .{ .alloc = alloc, .default_limit = limit };
    }

    pub fn deinit(self: *Tenants) void {
        const alloc = self.alloc orelse return;
        var it = self.map.valueIterator();
        while (it.next()) |t| alloc.destroy(t.*);
        self.map.deinit(alloc);
        self.active.deinit(alloc);
    }

    /// The tenant for `id`, made with the default limit on first sight.
    pub fn get(self: *Tenants, id: u64, now_ms: i64) !*Tenant {
        self.lock.lockShared(@src());
        const known = self.map.get(id);
        self.lock.unlockShared();
        if (known) |t| return t;

        // First sight, or a race with another first sight: getOrPut
        // under the write lock settles it.
        self.lock.lock(@src());
        defer self.lock.unlock();
        if (self.alloc == null) self.alloc = _rt.runtime().allocator;
        const alloc = self.alloc.?;
        const gop = try self.map.getOrPut(alloc, id);
        if (!gop.found_existing) {
            const t = try alloc.create(Tenant);
            t.* = Tenant.init(id, self.default_limit, now_ms);
            gop.value_ptr.* = t;
        }
        return gop.value_ptr.*;
    }

    /// A new size for a tenant; its rates follow at the next tick.
    pub fn setLimit(self: *Tenants, id: u64, limit: Limit, now_ms: i64) !void {
        limit.assertPositive();
        const t = try self.get(id, now_ms);
        self.lock.lock(@src());
        defer self.lock.unlock();
        t.limit = limit;
    }

    pub fn count(self: *Tenants) usize {
        self.lock.lockShared(@src());
        defer self.lock.unlockShared();
        return self.map.count();
    }

    /// Splits `capacity`, less what the system spent since the last
    /// tick, among the active tenants by their limits, and sets each
    /// one's rates and memory share. An idle tenant keeps its last
    /// rates: it is not in the sum, and it has nothing to spend them
    /// on.
    pub fn tick(self: *Tenants, now_ms: i64) void {
        self.lock.lockShared(@src());
        defer self.lock.unlockShared();

        const elapsed = @max(now_ms - self.last_tick_ms, 1);
        self.last_tick_ms = now_ms;
        // The box's cores in ticks once, so the shares round once.
        const box_ticks = capacity.cpuTicks(_rt.ticksPerSecond());
        const left_cpu = self.left(.cpu, box_ticks, elapsed);
        const left_io = self.left(.io, capacity.io_pages_per_s, elapsed);
        var sum: Limit = .{
            .cpu_milli = 0,
            .io_pages_per_s = 0,
            .mem_bytes = 0,
        };
        self.active.clearRetainingCapacity();
        var it = self.map.valueIterator();
        while (it.next()) |t| {
            if (!t.*.isActive(now_ms)) continue;
            // Out of memory: the rates stay what the last tick set.
            self.active.append(self.alloc.?, t.*) catch return;

            sum.cpu_milli += t.*.limit.cpu_milli;
            sum.io_pages_per_s += t.*.limit.io_pages_per_s;
            sum.mem_bytes += t.*.limit.mem_bytes;
        }

        for (self.active.items) |tenant| {
            const lim = tenant.limit;
            const cpu = share(left_cpu, lim.cpu_milli, sum.cpu_milli);
            const io = share(left_io, lim.io_pages_per_s, sum.io_pages_per_s);
            tenant.cpu.setRate(cpu, burstFor(cpu));
            tenant.io.setRate(io, burstFor(io));
            tenant.mem_share.store(
                share(capacity.mem_bytes, lim.mem_bytes, sum.mem_bytes),
                .monotonic,
            );
        }
    }

    /// `cap` less the system's spend since the last tick as a rate:
    /// what the tenants split, zero when the system took it all.
    fn left(self: *Tenants, kind: Kind, cap: i64, elapsed_ms: i64) i64 {
        const at_tick = &self.system_at_tick[@intFromEnum(kind)];
        const total = system.total(kind).load(.monotonic);
        const used = total - at_tick.*;
        at_tick.* = total;
        const rate = @divTrunc(@as(i128, used) * 1000, elapsed_ms);
        return cap - @as(i64, @intCast(@min(rate, cap)));
    }
};

/// `cap × limit / sum`, exact in i128. Called for an active tenant,
/// whose positive limit is in the sum, so the sum is never zero.
fn share(cap: i64, limit: i64, sum: i64) i64 {
    std.debug.assert(sum >= limit and limit > 0 and cap >= 0);
    return @intCast(@divTrunc(@as(i128, cap) * limit, sum));
}
