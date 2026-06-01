//! Debug wrappers around `zio.Mutex` and `zio.RwLock` that capture the
//! acquiring call site via `@src()` so a stuck server can be asked
//! "which locks are held, by what code, and how many callers are
//! waiting?" without attaching a debugger. Triage tool, not an
//! automated detector — no cycle detection, no owner identity.
//!
//! Each tracked lock is small: just an `inner` zio primitive plus an
//! atomic pointer to a heap-allocated `RegEntry`. The entry is
//! allocated from a fixed-size static pool on the lock's first acquire
//! and lives in the pool until the lock's optional `deinit()` is
//! called (or forever).
//!
//! Lifetime contract:
//!   - **Long-lived locks** (init-time, parent struct lives until process
//!     exit): just use them. Their entries persist for the program's life.
//!   - **Short-lived locks**: call `defer mu.deinit()` to return the pool
//!     slot. If you forget, the entry leaks into the pool — bounded at
//!     `MAX_ENTRIES` for the whole process (see its decl for sizing).
//!   - If the pool is exhausted, further locks silently degrade to
//!     untracked. lock/unlock still work; they just don't appear in dumps.
const std = @import("std");
const fiber = @import("fiber");
const context = @import("context.zig");

/// Cap on simultaneously-tracked locks. Sized to a ~512 KiB static
/// storage budget so we don't have to revisit the constant when
/// `RegEntry` gains or loses a field.
pub const MAX_ENTRIES: usize = (512 * 1024) / @sizeOf(RegEntry);

/// Per-lock dump state. Lives in the static `entry_pool` below. Once
/// allocated, the address is stable for the lifetime of the program (or
/// until `deinit()` returns it to the free list for reuse). The dump
/// walker only ever reads through these; it never touches the owning
/// tracked lock, so the lock can be destroyed without `deinit()` and
/// the walker stays safe.
pub const RegEntry = struct {
    /// Source location of the most recent acquirer, in either shared or
    /// exclusive mode. Cleared when the last holder releases. Reads and
    /// writes are racy by design (see file header).
    holder_src: ?*const std.builtin.SourceLocation = null,
    /// Most recent waiter (any mode).
    last_waiter_src: ?*const std.builtin.SourceLocation = null,
    /// Timestamp of the most recent acquire. 0 when fully released.
    /// Under a shared lock with concurrent readers, each new acquire
    /// overwrites this — so the derived `held_for_ms` is a staleness
    /// gauge ("how long since the last reader joined"), not a
    /// per-holder stopwatch.
    last_held_since_ms: i64 = 0,
    /// Callers currently blocked on this lock (any mode).
    waiter_count: std.atomic.Value(u32) = .init(0),
    /// Current holders: 0/1 for an exclusive lock, 0..N for a shared
    /// (RwLock) acquire. Drives the dump's "is held" check.
    holder_count: std.atomic.Value(u32) = .init(0),
    /// Intrusive list link. Used by the live list when registered,
    /// reused by the free list after `deinit()`.
    next: ?*RegEntry = null,
};

pub const TrackedMutex = struct {
    inner: fiber.Mutex = .{},
    entry: std.atomic.Value(?*RegEntry) = .init(null),

    pub fn tryLock(self: *TrackedMutex, comptime src: std.builtin.SourceLocation) bool {
        const e = ensureEntry(&self.entry);
        if (!self.inner.tryLock()) return false;
        recordAcquire(e, src);
        return true;
    }

    pub fn lock(self: *TrackedMutex, comptime src: std.builtin.SourceLocation) void {
        const e = ensureEntry(&self.entry);
        if (self.inner.tryLock()) {
            recordAcquire(e, src);
            return;
        }
        if (e) |ee| {
            _ = ee.waiter_count.fetchAdd(1, .monotonic);
            ee.last_waiter_src = staticSrc(src);
        }
        self.inner.lock();
        if (e) |ee| _ = ee.waiter_count.fetchSub(1, .monotonic);
        recordAcquire(e, src);
    }

    /// Retained for source compatibility — fiber locks never cancel, so
    /// this is identical to `lock`.
    pub fn lockUncancelable(self: *TrackedMutex, comptime src: std.builtin.SourceLocation) void {
        self.lock(src);
    }

    pub fn unlock(self: *TrackedMutex) void {
        recordRelease(self.entry.load(.monotonic));
        self.inner.unlock();
    }

    pub fn deinit(self: *TrackedMutex) void {
        const e = self.entry.swap(null, .acquire) orelse return;
        unregister(e);
    }
};

pub const TrackedRwLock = struct {
    inner: fiber.RWMutex = .{},
    entry: std.atomic.Value(?*RegEntry) = .init(null),

    pub fn tryLock(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) bool {
        const e = ensureEntry(&self.entry);
        if (!self.inner.tryLock()) return false;
        recordAcquire(e, src);
        return true;
    }

    pub fn tryLockShared(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) bool {
        const e = ensureEntry(&self.entry);
        if (!self.inner.tryLockShared()) return false;
        recordAcquire(e, src);
        return true;
    }

    pub fn lock(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) void {
        self.acquire(src, false);
    }

    pub fn lockShared(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) void {
        self.acquire(src, true);
    }

    /// Retained for source compatibility — fiber locks never cancel.
    pub fn lockUncancelable(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) void {
        self.acquire(src, false);
    }

    pub fn lockSharedUncancelable(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) void {
        self.acquire(src, true);
    }

    /// Shared body. `shared` is comptime so each public entrypoint
    /// collapses to a single fast path.
    fn acquire(
        self: *TrackedRwLock,
        comptime src: std.builtin.SourceLocation,
        comptime shared: bool,
    ) void {
        const e = ensureEntry(&self.entry);
        // Fast path: uncontended → skip waiter accounting.
        const got = if (shared) self.inner.tryLockShared() else self.inner.tryLock();
        if (got) {
            recordAcquire(e, src);
            return;
        }
        // Slow path.
        if (e) |ee| {
            _ = ee.waiter_count.fetchAdd(1, .monotonic);
            ee.last_waiter_src = staticSrc(src);
        }
        if (shared) self.inner.lockShared() else self.inner.lock();
        if (e) |ee| _ = ee.waiter_count.fetchSub(1, .monotonic);
        recordAcquire(e, src);
    }

    pub fn unlock(self: *TrackedRwLock) void {
        recordRelease(self.entry.load(.monotonic));
        self.inner.unlock();
    }

    pub fn unlockShared(self: *TrackedRwLock) void {
        recordRelease(self.entry.load(.monotonic));
        self.inner.unlockShared();
    }

    pub fn deinit(self: *TrackedRwLock) void {
        const e = self.entry.swap(null, .acquire) orelse return;
        unregister(e);
    }
};

// ---------------------------------------------------------------------------
// Internal helpers shared by both Tracked* types.
// ---------------------------------------------------------------------------

fn recordAcquire(e: ?*RegEntry, comptime src: std.builtin.SourceLocation) void {
    const ee = e orelse return;
    // Write src/ts before the counter bump so a dump reader that sees
    // `holder_count > 0` is more likely to find a populated holder_src.
    // Per the file header, the entry fields are racy by design, so the
    // ordering is best-effort, not a hard invariant.
    ee.holder_src = staticSrc(src);
    ee.last_held_since_ms = fiber.coarseMs();
    _ = ee.holder_count.fetchAdd(1, .monotonic);
}

fn recordRelease(e: ?*RegEntry) void {
    const ee = e orelse return;
    // Clear the holder breadcrumbs only on the final release so a
    // single shared-mode reader leaving doesn't reset the entry while
    // other readers still hold it.
    if (ee.holder_count.fetchSub(1, .monotonic) == 1) {
        ee.holder_src = null;
        ee.last_held_since_ms = 0;
    }
}

fn ensureEntry(atomic_entry: *std.atomic.Value(?*RegEntry)) ?*RegEntry {
    if (atomic_entry.load(.acquire)) |e| return e;
    const e = allocate() orelse return null;
    e.* = .{};
    if (atomic_entry.cmpxchgStrong(null, e, .release, .acquire)) |existing| {
        release(e);
        return existing;
    }
    link(e);
    return e;
}

/// Lifts a `comptime SourceLocation` to a pointer with static storage
/// duration. See full rationale in the file header (anonymous-struct
/// trick parks the value in `.rodata`).
fn staticSrc(comptime src: std.builtin.SourceLocation) *const std.builtin.SourceLocation {
    const Static = struct {
        const value: std.builtin.SourceLocation = src;
    };
    return &Static.value;
}

// ---------------------------------------------------------------------------
// Registry storage: static entry pool + live/free intrusive lists.
//
// Per-call writes on entries (holder_src, waiter_count, reader_count, etc.)
// are racy by design — the system is wedged when we look, so a slightly-
// stale snapshot is acceptable.
// ---------------------------------------------------------------------------

var entry_pool: [MAX_ENTRIES]RegEntry = undefined;
var pool_used: usize = 0;
var free_head: ?*RegEntry = null;
var live_head: ?*RegEntry = null;
// MUST stay std.Thread.Mutex (not TrackedMutex / zio.Mutex):
//   1. Circular — TrackedMutex protecting itself would be infinite recursion.
//   2. Post-zio shutdown path reaches mu.deinit() from main thread after
//      the runtime is torn down. A zio.Mutex would have no task context.
// Safety invariant: critical sections holding reg_mu MUST NOT yield to
// the zio runtime. All ops below (allocate / release / link / unregister
// / dump's walk) are pure CPU work — no zio.sleep, no I/O, no
// channel send/recv. Adding any yield point inside reg_mu would risk
// the "all executor threads parked on this lock" deadlock pattern.
// std.Thread.Mutex → std.Io.Mutex in Zig 0.16. The Io-backed mutex behaves
// like Thread.Mutex when used with std.Io.Threaded (which std.process.Init
// gives us via context.g().io), so this is the closest drop-in.
var reg_mu: std.Io.Mutex = .init;

inline fn regLock() void {
    reg_mu.lockUncancelable(context.g().io);
}
inline fn regUnlock() void {
    reg_mu.unlock(context.g().io);
}

fn allocate() ?*RegEntry {
    regLock();
    defer regUnlock();
    if (free_head) |e| {
        free_head = e.next;
        e.next = null;
        return e;
    }
    if (pool_used >= MAX_ENTRIES) return null;
    const e = &entry_pool[pool_used];
    pool_used += 1;
    return e;
}

fn release(e: *RegEntry) void {
    regLock();
    defer regUnlock();
    e.next = free_head;
    free_head = e;
}

fn link(e: *RegEntry) void {
    regLock();
    defer regUnlock();
    e.next = live_head;
    live_head = e;
}

fn unregister(e: *RegEntry) void {
    regLock();
    defer regUnlock();
    if (live_head == e) {
        live_head = e.next;
    } else {
        var prev = live_head;
        while (prev) |p| : (prev = p.next) {
            if (p.next == e) {
                p.next = e.next;
                break;
            }
        }
    }
    // Clear all per-lock fields so a reused slot can't carry state into
    // the next lock that grabs it.
    e.holder_src = null;
    e.last_waiter_src = null;
    e.last_held_since_ms = 0;
    e.waiter_count.store(0, .monotonic);
    e.holder_count.store(0, .monotonic);
    e.next = free_head;
    free_head = e;
}

/// Walk every live entry and write a one-line summary per *currently
/// held* lock, followed by a totals line. Idle locks are skipped to
/// keep the output focused on what matters when a server is wedged.
/// Held under `reg_mu` for the duration of the walk, so concurrent
/// deinit/first-touch waits behind. The dump call site is rare (admin
/// command) and the writer is always in-memory, so the lock window is
/// microseconds even at the full pool cap.
///
/// Output (one held lock per line, then a summary):
///   held_by=<file:line> held_for_ms=<n> count=<n> waiters=<n> last_waiter=<file:line>
///   ...
///   summary: <total> tracked locks, <held> currently held
///
/// `count` is the number of current holders: 0/1 for exclusive locks
/// (mutex / RwLock write) and 0..N for shared (RwLock read). Held locks
/// always have `count >= 1`; the dump skips entries where it's 0.
pub fn dump(w: *std.Io.Writer) !void {
    regLock();
    defer regUnlock();
    var node = live_head;
    var total: usize = 0;
    var held: usize = 0;
    while (node) |n| : (node = n.next) {
        total += 1;
        const count = n.holder_count.load(.monotonic);
        if (count == 0) continue;
        held += 1;
        const since = n.last_held_since_ms;
        const held_for: i64 = if (since == 0) 0 else fiber.coarseMs() - since;
        if (n.holder_src) |sl| {
            try w.print("held_by={s}:{d} held_for_ms={d}", .{ sl.file, sl.line, held_for });
        } else {
            try w.print("held_by=- held_for_ms={d}", .{held_for});
        }
        try w.print(" count={d}", .{count});
        try w.print(" waiters={d}", .{n.waiter_count.load(.monotonic)});
        if (n.last_waiter_src) |sl| {
            try w.print(" last_waiter={s}:{d}", .{ sl.file, sl.line });
        } else {
            try w.print(" last_waiter=-", .{});
        }
        try w.writeByte('\n');
    }
    try w.print("LOCKS: {d} tracked locks, {d} currently held\n", .{ total, held });
}
