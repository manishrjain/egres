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
//!     `MAX_ENTRIES` (1024) for the whole process.
//!   - If the pool is exhausted, further locks silently degrade to
//!     untracked. lock/unlock still work; they just don't appear in dumps.
const std = @import("std");
const zio = @import("zio");

/// Cap on simultaneously-tracked locks. ~80 KiB of static storage at
/// the current `RegEntry` size.
pub const MAX_ENTRIES: usize = 1024;

/// Per-lock dump state. Lives in the static `entry_pool` below. Once
/// allocated, the address is stable for the lifetime of the program (or
/// until `deinit()` returns it to the free list for reuse). The dump
/// walker only ever reads through these; it never touches the owning
/// tracked lock, so the lock can be destroyed without `deinit()` and
/// the walker stays safe.
pub const RegEntry = struct {
    /// Auto-derived from the first acquire's `@src().file`.
    name: []const u8,
    /// Set on exclusive-mode acquire; null when no exclusive holder.
    holder_src: ?*const std.builtin.SourceLocation = null,
    /// Most recent waiter (any mode).
    last_waiter_src: ?*const std.builtin.SourceLocation = null,
    /// Most recent shared-mode acquirer. RwLock only; mutex leaves null.
    last_reader_src: ?*const std.builtin.SourceLocation = null,
    /// Time the current exclusive holder acquired. 0 when not exclusively
    /// held. Has no meaning for shared-only state (multiple holders).
    held_since_ms: i64 = 0,
    /// Callers currently blocked on this lock (any mode).
    waiter_count: std.atomic.Value(u32) = .{ .raw = 0 },
    /// Current shared holders. RwLock only; mutex stays 0.
    reader_count: std.atomic.Value(u32) = .{ .raw = 0 },
    /// Intrusive list link. Used by the live list when registered,
    /// reused by the free list after `deinit()`.
    next: ?*RegEntry = null,
};

pub const Stats = struct {
    name: []const u8,
    holder_src: ?*const std.builtin.SourceLocation,
    held_for_ms: i64,
    waiter_count: u32,
    last_waiter_src: ?*const std.builtin.SourceLocation,
    reader_count: u32,
    last_reader_src: ?*const std.builtin.SourceLocation,
};

pub const TrackedMutex = struct {
    inner: zio.Mutex = .init,
    entry: std.atomic.Value(?*RegEntry) = .{ .raw = null },

    pub fn tryLock(self: *TrackedMutex, comptime src: std.builtin.SourceLocation) bool {
        const e = ensureEntry(&self.entry, src);
        if (!self.inner.tryLock()) return false;
        if (e) |ee| {
            ee.holder_src = staticSrc(src);
            ee.held_since_ms = std.time.milliTimestamp();
        }
        return true;
    }

    pub fn lock(self: *TrackedMutex, comptime src: std.builtin.SourceLocation) zio.Cancelable!void {
        return self.lockImpl(src, true);
    }

    pub fn lockUncancelable(self: *TrackedMutex, comptime src: std.builtin.SourceLocation) void {
        self.lockImpl(src, false);
    }

    fn lockImpl(
        self: *TrackedMutex,
        comptime src: std.builtin.SourceLocation,
        comptime cancelable: bool,
    ) (if (cancelable) zio.Cancelable!void else void) {
        const e = ensureEntry(&self.entry, src);
        if (self.inner.tryLock()) {
            if (e) |ee| {
                ee.holder_src = staticSrc(src);
                ee.held_since_ms = std.time.milliTimestamp();
            }
            return;
        }
        if (e) |ee| {
            _ = ee.waiter_count.fetchAdd(1, .monotonic);
            ee.last_waiter_src = staticSrc(src);
        }
        if (cancelable) {
            self.inner.lock() catch |err| {
                if (e) |ee| _ = ee.waiter_count.fetchSub(1, .monotonic);
                return err;
            };
        } else {
            self.inner.lockUncancelable();
        }
        if (e) |ee| {
            _ = ee.waiter_count.fetchSub(1, .monotonic);
            ee.holder_src = staticSrc(src);
            ee.held_since_ms = std.time.milliTimestamp();
        }
    }

    pub fn unlock(self: *TrackedMutex) void {
        if (self.entry.load(.monotonic)) |e| {
            e.holder_src = null;
            e.held_since_ms = 0;
        }
        self.inner.unlock();
    }

    pub fn deinit(self: *TrackedMutex) void {
        const e = self.entry.swap(null, .acquire) orelse return;
        unregister(e);
    }

    pub fn stats(self: *const TrackedMutex) ?Stats {
        const e = self.entry.load(.acquire) orelse return null;
        return statsOf(e);
    }
};

pub const TrackedRwLock = struct {
    inner: zio.RwLock = .init,
    entry: std.atomic.Value(?*RegEntry) = .{ .raw = null },

    pub fn tryLock(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) bool {
        const e = ensureEntry(&self.entry, src);
        if (!self.inner.tryLock()) return false;
        if (e) |ee| {
            ee.holder_src = staticSrc(src);
            ee.held_since_ms = std.time.milliTimestamp();
        }
        return true;
    }

    pub fn tryLockShared(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) bool {
        const e = ensureEntry(&self.entry, src);
        if (!self.inner.tryLockShared()) return false;
        if (e) |ee| {
            _ = ee.reader_count.fetchAdd(1, .monotonic);
            ee.last_reader_src = staticSrc(src);
        }
        return true;
    }

    pub fn lock(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) zio.Cancelable!void {
        return self.acquire(src, false, true);
    }

    pub fn lockUncancelable(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) void {
        self.acquire(src, false, false);
    }

    pub fn lockShared(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) zio.Cancelable!void {
        return self.acquire(src, true, true);
    }

    pub fn lockSharedUncancelable(self: *TrackedRwLock, comptime src: std.builtin.SourceLocation) void {
        self.acquire(src, true, false);
    }

    /// Shared body. `shared` and `cancelable` are both comptime so each
    /// of the four public entrypoints collapses to a single fast path.
    fn acquire(
        self: *TrackedRwLock,
        comptime src: std.builtin.SourceLocation,
        comptime shared: bool,
        comptime cancelable: bool,
    ) (if (cancelable) zio.Cancelable!void else void) {
        const e = ensureEntry(&self.entry, src);
        // Fast path: uncontended → skip waiter accounting.
        const got = if (shared) self.inner.tryLockShared() else self.inner.tryLock();
        if (got) {
            recordAcquire(e, src, shared);
            return;
        }
        // Slow path.
        if (e) |ee| {
            _ = ee.waiter_count.fetchAdd(1, .monotonic);
            ee.last_waiter_src = staticSrc(src);
        }
        if (cancelable) {
            const r = if (shared) self.inner.lockShared() else self.inner.lock();
            r catch |err| {
                if (e) |ee| _ = ee.waiter_count.fetchSub(1, .monotonic);
                return err;
            };
        } else {
            if (shared) self.inner.lockSharedUncancelable() else self.inner.lockUncancelable();
        }
        if (e) |ee| _ = ee.waiter_count.fetchSub(1, .monotonic);
        recordAcquire(e, src, shared);
    }

    pub fn unlock(self: *TrackedRwLock) void {
        if (self.entry.load(.monotonic)) |e| {
            e.holder_src = null;
            e.held_since_ms = 0;
        }
        self.inner.unlock();
    }

    pub fn unlockShared(self: *TrackedRwLock) void {
        if (self.entry.load(.monotonic)) |e| {
            _ = e.reader_count.fetchSub(1, .monotonic);
        }
        self.inner.unlockShared();
    }

    pub fn deinit(self: *TrackedRwLock) void {
        const e = self.entry.swap(null, .acquire) orelse return;
        unregister(e);
    }

    pub fn stats(self: *const TrackedRwLock) ?Stats {
        const e = self.entry.load(.acquire) orelse return null;
        return statsOf(e);
    }
};

// ---------------------------------------------------------------------------
// Internal helpers shared by both Tracked* types.
// ---------------------------------------------------------------------------

fn recordAcquire(
    e: ?*RegEntry,
    comptime src: std.builtin.SourceLocation,
    comptime shared: bool,
) void {
    const ee = e orelse return;
    if (shared) {
        _ = ee.reader_count.fetchAdd(1, .monotonic);
        ee.last_reader_src = staticSrc(src);
    } else {
        ee.holder_src = staticSrc(src);
        ee.held_since_ms = std.time.milliTimestamp();
    }
}

fn statsOf(e: *const RegEntry) Stats {
    const since = e.held_since_ms;
    const held_for: i64 = if (since == 0) 0 else std.time.milliTimestamp() - since;
    return .{
        .name = e.name,
        .holder_src = e.holder_src,
        .held_for_ms = held_for,
        .waiter_count = e.waiter_count.load(.monotonic),
        .last_waiter_src = e.last_waiter_src,
        .reader_count = e.reader_count.load(.monotonic),
        .last_reader_src = e.last_reader_src,
    };
}

fn ensureEntry(
    atomic_entry: *std.atomic.Value(?*RegEntry),
    comptime src: std.builtin.SourceLocation,
) ?*RegEntry {
    if (atomic_entry.load(.acquire)) |e| return e;
    const e = allocate() orelse return null;
    e.* = .{ .name = src.file };
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
var reg_mu: std.Thread.Mutex = .{};

fn allocate() ?*RegEntry {
    reg_mu.lock();
    defer reg_mu.unlock();
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
    reg_mu.lock();
    defer reg_mu.unlock();
    e.next = free_head;
    free_head = e;
}

fn link(e: *RegEntry) void {
    reg_mu.lock();
    defer reg_mu.unlock();
    e.next = live_head;
    live_head = e;
}

fn unregister(e: *RegEntry) void {
    reg_mu.lock();
    defer reg_mu.unlock();
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
    e.last_reader_src = null;
    e.held_since_ms = 0;
    e.waiter_count.store(0, .monotonic);
    e.reader_count.store(0, .monotonic);
    e.next = free_head;
    free_head = e;
}

/// Walk every live entry and write a one-line summary per lock. Held
/// under `reg_mu` for the duration of the walk, so concurrent
/// deinit/first-touch waits behind. The dump call site is rare (admin
/// command) and the writer is always in-memory, so the lock window is
/// microseconds even at the full 1024-entry cap.
///
/// Output (one lock per line):
///   lock=<name> held_by=<file:line> held_for_ms=<n> readers=<n> last_reader=<file:line> waiters=<n> last_waiter=<file:line>
pub fn dump(w: *std.Io.Writer) !void {
    reg_mu.lock();
    defer reg_mu.unlock();
    var node = live_head;
    var count: usize = 0;
    while (node) |n| : (node = n.next) {
        const since = n.held_since_ms;
        const held_for: i64 = if (since == 0) 0 else std.time.milliTimestamp() - since;
        try w.print("lock={s}", .{n.name});
        if (n.holder_src) |sl| {
            try w.print(" held_by={s}:{d} held_for_ms={d}", .{ sl.file, sl.line, held_for });
        } else {
            try w.print(" held_by=- held_for_ms=0", .{});
        }
        try w.print(" readers={d}", .{n.reader_count.load(.monotonic)});
        if (n.last_reader_src) |sl| {
            try w.print(" last_reader={s}:{d}", .{ sl.file, sl.line });
        } else {
            try w.print(" last_reader=-", .{});
        }
        try w.print(" waiters={d}", .{n.waiter_count.load(.monotonic)});
        if (n.last_waiter_src) |sl| {
            try w.print(" last_waiter={s}:{d}", .{ sl.file, sl.line });
        } else {
            try w.print(" last_waiter=-", .{});
        }
        try w.writeByte('\n');
        count += 1;
    }
    if (count == 0) try w.print("(no tracked mutexes)\n", .{});
}
