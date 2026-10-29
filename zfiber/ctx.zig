// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Every fiber's context: the I/O class its reads and writes run as,
//! and, while a statement runs, the record of the statement's path
//! through the system, kept as latency-bucketed traces for the admin
//! port.
//!
//! A fiber owns its `Ctx` (`Fiber.ctx`), reached with `current()`, and
//! sets its own I/O class there; a spawn copies the class into the
//! child. A statement runs `start` and `finish` on its fiber's
//! context, and any code it calls stamps the moment it was reached
//! with `fiber.ctx().record("loaded {d} tables", .{n})`. The records
//! are a flat timeline, not nested spans: read top to bottom they
//! show where the time went. A joinable helper fiber's context points
//! at the statement's (`parent`), so its records go there.
//! `current()` is never null: off any fiber, or on a fiber with no
//! statement, `record` does nothing, so library code never branches
//! on it.
//!
//! When the statement finishes, `finish` offers the context to the
//! sampler: one ring of the newest few traces per latency bucket (at
//! least 1 ms, 10 ms, ...), the shape of Go's net/trace. A statement
//! goes into the highest bucket it qualifies for, so the slow ones are
//! not pushed out by the merely slow. `writeTraces` prints them for
//! the admin command `traces [min_ms]`.
//!
//! Memory: a statement's arena over the runtime allocator holds the
//! name, the formatted labels and the record list. Keeping a trace
//! moves the arena into the ring, so nothing is copied at the end of a
//! statement, and evicting a trace frees it whole.
//!
//! A context is also where a statement is interruptible: its loops call
//! `maybeYield` once per batch, and every policy that stops or paces a
//! statement (the shutdown's stop stage, the tenant's cpu share and
//! the statement's memory cap today; cancel later) delivers through
//! that one call, so the loops never learn about any of them. The
//! statement's memory is what its arenas hold from `allocator`,
//! counted here and on the tenant.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const _rt = @import("runtime.zig");
const _shutdown = @import("shutdown.zig");
const _clock = @import("utils").coarse_clock;
const _date = @import("utils").date;
const Spinlock = @import("spinlock.zig").Spinlock;
const _meter = @import("meter.zig");
const Tenant = _meter.Tenant;

const String = @import("utils").String;

/// Statement text kept per context. A bulk insert can carry megabytes
/// of values; the head is what identifies it in a trace.
const name_max = 512;

/// Bucket thresholds, in microseconds, and how many traces each keeps.
const thresholds_us = [_]u64{
    1_000,   10_000,  20_000,    50_000,
    100_000, 500_000, 1_000_000, 10_000_000,
};
const per_bucket = 8;

pub const Record = struct {
    label: String,
    /// Microseconds since the context started.
    at_us: u64,
};

/// The class of the work under a context, foreground or background:
/// what every shared rate is split by (meter.zig).
pub const IoClass = @import("meter.zig").IoClass;

/// Whether the block and chunk caches serve and keep the reads made
/// under a context. A compaction streams each block once: keeping
/// them would evict what queries reuse, and the hits it does get
/// must not count as reuse either.
pub const CachePolicy = enum { use, bypass };

pub const Ctx = struct {
    /// A statement runs on this fiber: the arena, live from `start` to
    /// `finish`, holds the name, the labels and the record list, and
    /// is moved to the sampler or freed at the end. Null otherwise,
    /// and then records are dropped.
    arena: ?std.heap.ArenaAllocator = null,
    name: String = "",
    /// Realtime, read once, so a trace can be placed on an absolute
    /// timeline; durations use the monotonic clock.
    start_unix_us: i64 = 0,
    start_mono_us: i64 = 0,
    records: std.ArrayList(Record) = .empty,
    /// Set by `start` when the installed Shutdown (`shutdown.zig`)
    /// counted the statement in, so `finish` counts it out on the
    /// same instance and one started before `install` is never
    /// counted out.
    counted: bool = false,
    /// What every read and write on the fiber runs as; vfs and the
    /// caches look them up with `current()`, nothing passes them. A
    /// fiber sets its own; a spawn copies the spawner's.
    class: IoClass = .fg,
    cache: CachePolicy = .use,
    /// Who the fiber's cpu, io and memory are charged to: the session
    /// sets it from its database at connect (`Handler.onStartup`), a
    /// spawn copies it, and until then it is the system's, which is
    /// never short.
    tenant: *Tenant = &_meter.system,
    /// Live bytes in the statement's arenas, on the root: a helper's
    /// allocations count here too. Checked against the cap at a yield.
    mem_held: std.atomic.Value(i64) = .init(0),
    /// The statement this fiber helps, set at spawn for a joinable
    /// fiber: `record` forwards to it. The spawner's frame joins the
    /// helper, so the root outlives it. Null on a root and on a fiber
    /// with no statement behind it.
    parent: ?*Ctx = null,
    /// Joined helper fibers record into the same list from other
    /// workers; the arena is not thread safe either, so formatting a
    /// label happens under this lock too. A few dozen records per
    /// statement, so the lock is never contended for long.
    lock: Spinlock = .{},

    /// The context of code off any fiber: `record` does nothing.
    /// Never written.
    pub var bg: Ctx = .{};

    /// Begins a statement on this context, the current fiber's: waits
    /// for the tenant's memory (`Tenant.payForMem`) unless the session is
    /// inside a transaction block, whose statements release what it
    /// holds; copies the head of `name` (the statement text) into a
    /// fresh arena, resets the I/O class, counts the statement as in
    /// flight on the installed Shutdown (`Shutdown.enterCtx`) and on
    /// its tenant, which is active in the tick's split from here.
    /// Refused once the process is draining. Needs a running runtime,
    /// whose allocator backs the arena.
    pub fn start(self: *Ctx, name: String, in_txn: bool) YieldError!void {
        std.debug.assert(self.arena == null and self.parent == null);
        if (!in_txn) try self.tenant.payForMem();
        if (_shutdown.installed()) |sd| {
            if (!sd.enterCtx()) return error.Shutdown;
            self.counted = true;
        }

        self.tenant.touch(_clock.ms());
        self.arena = std.heap.ArenaAllocator.init(_rt.runtime().allocator);
        const head = name[0..@min(name.len, name_max)];
        self.name = self.arena.?.allocator().dupe(u8, head) catch "";
        self.start_unix_us = _clock.timestampNow(.realtime, .micros);
        self.start_mono_us = _clock.timestampNow(.monotonic, .micros);
        self.records = .empty;
        self.class = .fg;
        self.cache = .use;
    }

    /// Ends the statement: either hands the name and records to the
    /// sampler or frees them, and leaves the context as a fresh
    /// fiber's but for the tenant, which is the connection's. Must run
    /// on the fiber that started it, after its helpers are joined.
    pub fn finish(self: *Ctx) void {
        var arena = self.arena.?;
        if (self.counted) _shutdown.installed().?.leaveCtx();
        if (!offer(self, arena, self.elapsedUs())) arena.deinit();
        const tenant = self.tenant;
        self.* = .{ .tenant = tenant };
    }

    /// The context the records go to: the statement's, through
    /// `parent` on a helper.
    fn root(self: *Ctx) *Ctx {
        return self.parent orelse self;
    }

    /// Stamps the moment with a label. A label without args is a
    /// comptime string and is stored as is; with args it is printed
    /// into the arena. Out of memory drops the record; a statement
    /// never fails over its trace.
    pub fn record(self: *Ctx, comptime fmt: String, args: anytype) void {
        const r = self.root();
        const arena = if (r.arena) |*a| a else return;
        const at_us = r.elapsedUs();

        r.lock.lock();
        defer r.lock.unlock();

        const arloc = arena.allocator();
        const label = if (args.len == 0)
            fmt
        else
            std.fmt.allocPrint(arloc, fmt, args) catch return;

        r.records.append(
            arloc,
            .{ .label = label, .at_us = at_us },
        ) catch return;
    }

    /// `n` bytes an arena of the statement took from, or gave back
    /// to, `allocator`: on the statement, its tenant and the process.
    pub fn holdMem(self: *Ctx, n: i64) void {
        _ = self.root().mem_held.fetchAdd(n, .monotonic);
        self.tenant.holdMem(n);
        _ = _meter.mem_held_total.fetchAdd(n, .monotonic);
    }

    pub fn releaseMem(self: *Ctx, n: i64) void {
        _ = self.root().mem_held.fetchSub(n, .monotonic);
        self.tenant.releaseMem(n);
        _ = _meter.mem_held_total.fetchSub(n, .monotonic);
    }

    /// The runtime's allocator, counted on this context: what every
    /// statement arena is made on. What it hands out is held by the
    /// statement and its tenant until given back, which the arena does
    /// before the statement ends. An arena asks per chunk, so the
    /// count is cheap and a little coarse.
    pub fn allocator(self: *Ctx) Allocator {
        return .{ .ptr = self, .vtable = &mem_vtable };
    }

    /// A statement arena on `allocator`: the caller's to deinit.
    pub fn newArena(self: *Ctx) std.heap.ArenaAllocator {
        return .init(self.allocator());
    }

    pub fn elapsedUs(self: *const Ctx) u64 {
        const now = _clock.timestampNow(.monotonic, .micros);
        return @intCast(@max(0, now - self.start_mono_us));
    }

    /// `MemShare`: the tenant waited its bound for memory and is still
    /// over its share. `MemLimit`: the statement is past the cap. Both
    /// are the statement's, answered as out of memory.
    pub const YieldError = error{ Shutdown, MemShare, MemLimit };

    /// The statement's yield point: a loop calls this once per batch or
    /// block of work, and the runtime may pause the statement here or
    /// decline to resume it, which comes back as the error. It asks
    /// whether the process is stopping (`Shutdown.startShutdown`) and
    /// whether the statement holds more than `query_mem_bytes`, then
    /// charges the cpu since the last settle to the tenant and pays
    /// its debt: a bucket below zero parks the fiber until the rate
    /// has covered it. Cancel will come through here too. The drain
    /// stage before the stop is not an interrupt: a
    /// running statement gets the grace period to finish on its own,
    /// only `start` refuses new ones. Callers must treat this as a
    /// blocking call: no spinlock held across it.
    pub fn maybeYield(self: *Ctx) YieldError!void {
        if (_shutdown.installed()) |sd| {
            if (sd.isStopping()) return error.Shutdown;
        }
        const cap = _meter.query_mem_bytes.load(.monotonic);
        if (cap > 0 and self.root().mem_held.load(.monotonic) > cap)
            return error.MemLimit;
        if (_rt.currentWorkerOrNull()) |w| {
            if (w.current_fiber) |f| _ = w.chargeCpu(f);
        }
        self.tenant.pay(.cpu, self.class);
    }
};

/// The current fiber's own context, or `Ctx.bg` off any fiber. Never
/// null.
pub fn current() *Ctx {
    const f = _rt.currentFiberOrNull() orelse return &Ctx.bg;
    return &f.ctx;
}

/// The allocator's data pointer is the context, so a chunk is counted
/// on the context that made the arena wherever it is taken; `Ctx.bg`
/// counts nothing.
const mem_vtable: Allocator.VTable = .{
    .alloc = memAlloc,
    .resize = memResize,
    .remap = memRemap,
    .free = memFree,
};

fn memCtx(ptr: *anyopaque) *Ctx {
    return @ptrCast(@alignCast(ptr));
}

fn memHeld(ptr: *anyopaque, old_len: usize, new_len: usize) void {
    const ctx = memCtx(ptr);
    if (ctx == &Ctx.bg) return;
    if (new_len >= old_len)
        ctx.holdMem(@intCast(new_len - old_len))
    else
        ctx.releaseMem(@intCast(old_len - new_len));
}

fn memAlloc(ptr: *anyopaque, len: usize, al: Alignment, ra: usize) ?[*]u8 {
    const p = _rt.runtime().allocator.rawAlloc(len, al, ra) orelse return null;
    memHeld(ptr, 0, len);
    return p;
}

fn memResize(
    ptr: *anyopaque,
    memory: []u8,
    al: Alignment,
    new_len: usize,
    ra: usize,
) bool {
    if (!_rt.runtime().allocator.rawResize(memory, al, new_len, ra))
        return false;
    memHeld(ptr, memory.len, new_len);
    return true;
}

fn memRemap(
    ptr: *anyopaque,
    memory: []u8,
    al: Alignment,
    new_len: usize,
    ra: usize,
) ?[*]u8 {
    const p = _rt.runtime().allocator.rawRemap(memory, al, new_len, ra) orelse
        return null;
    memHeld(ptr, memory.len, new_len);
    return p;
}

fn memFree(ptr: *anyopaque, memory: []u8, al: Alignment, ra: usize) void {
    _rt.runtime().allocator.rawFree(memory, al, ra);
    memHeld(ptr, memory.len, 0);
}

/// A finished statement kept by the sampler. Owns its arena, which
/// holds the name, the labels and the records.
pub const Trace = struct {
    arena: std.heap.ArenaAllocator,
    name: String,
    start_unix_us: i64,
    elapsed_us: u64,
    records: []const Record,
};

/// A ring of the newest traces at or above one threshold.
const Bucket = struct {
    traces: [per_bucket]?Trace = .{null} ** per_bucket,
    /// The slot the next trace overwrites: the oldest once full.
    next: usize = 0,
};

var buckets: [thresholds_us.len]Bucket = @splat(.{});
var buckets_lock: Spinlock = .{};

/// The highest bucket `elapsed_us` qualifies for, or null below the
/// lowest threshold.
fn bucketFor(elapsed_us: u64) ?usize {
    var i = thresholds_us.len;
    while (i > 0) {
        i -= 1;
        if (elapsed_us >= thresholds_us[i]) return i;
    }
    return null;
}

/// Moves a finished statement into its bucket's ring, taking over the
/// arena. Returns false, without locking, when the statement is too
/// fast to keep: the common path, and the caller frees the context.
fn offer(ctx: *Ctx, arena: std.heap.ArenaAllocator, elapsed_us: u64) bool {
    const b = bucketFor(elapsed_us) orelse return false;
    const trace: Trace = .{
        .arena = arena,
        .name = ctx.name,
        .start_unix_us = ctx.start_unix_us,
        .elapsed_us = elapsed_us,
        .records = ctx.records.items,
    };

    var evicted: ?Trace = null;
    {
        buckets_lock.lock();
        defer buckets_lock.unlock();

        const bucket = &buckets[b];
        evicted = bucket.traces[bucket.next];
        bucket.traces[bucket.next] = trace;
        bucket.next = (bucket.next + 1) % per_bucket;
    }
    if (evicted) |*old| old.arena.deinit();
    return true;
}

/// Prints every kept trace that took at least `min_ms`, slowest bucket
/// first, in no particular order within a bucket: the statement line,
/// then its records indented with their offsets from the start.
pub fn writeTraces(w: *std.Io.Writer, min_ms: u64) !void {
    buckets_lock.lock();
    defer buckets_lock.unlock();

    var b = thresholds_us.len;
    while (b > 0) {
        b -= 1;
        const bucket = &buckets[b];
        var shown: usize = 0;
        for (bucket.traces) |maybe| {
            const trace = maybe orelse continue;
            if (trace.elapsed_us < min_ms * 1000) continue;
            if (shown == 0) {
                try w.print(">= {d}ms\n", .{thresholds_us[b] / 1000});
            }
            shown += 1;
            try writeTrace(w, &trace);
        }
    }
}

fn writeTrace(w: *std.Io.Writer, trace: *const Trace) !void {
    try w.writeAll("  ");
    try _date.formatTimestampUtc(w, trace.start_unix_us);
    try w.print("  {d}.{d:0>3}ms  {s}\n", .{
        trace.elapsed_us / 1000, trace.elapsed_us % 1000, trace.name,
    });
    for (trace.records) |r| {
        try w.print("      +{d}us  {s}\n", .{ r.at_us, r.label });
    }
}
