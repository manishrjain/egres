// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Holder tracking for the *tracked* `Mutex`/`RWMutex` modes in sync.zig.
//! A triage tool for a wedged server: `dumpLocks` answers "which locks are
//! held, by what code, and for how long?" without a debugger.
//!
//! Why this is tiny compared to the old `u/tracked_mutex.zig`:
//!   - **Waiters are not tracked.** A fiber blocked on a lock is parked in the
//!     lock's own wait queue, so `dumpFibers` already prints its full stack —
//!     strictly more than the old per-lock `last_waiter` `@src`. Gone.
//!   - **Holder/waiter counts are derived, not stored.** They live in the
//!     lock's own fields (`state`/`waiters`; `readers`/`writer`/queues), read
//!     racily at dump time via a per-type `snapshot` fn. Racy is fine — the
//!     system is wedged when we look.
//!   - **No "record release".** "Held" is derived from the lock's state, so a
//!     released lock simply reads `held == 0` and the dump skips it; there's no
//!     counter to decrement and no breadcrumb to clear. `unlock` stays
//!     untouched. Tracking is acquire-only.
//!
//! So a tracked lock embeds only `Tracking`: the holder's acquire `@src`, the
//! acquire timestamp, and the intrusive registry link (+ a once-flag and the
//! type-erasure plumbing to read the lock's live state back at dump time).
const std = @import("std");
const _context = @import("utils").context;
const _coarse = @import("utils").coarse_clock;

const SourceLocation = std.builtin.SourceLocation;

/// A lock's live state, derived from its OWN fields at dump time. `held` is the
/// holder count: 0/1 for a Mutex or an RW writer, 0..N for RW readers.
pub const Snap = struct {
    held: u32,
    has_waiters: bool,
    /// Lifetime contention gauges (see sync.Mutex); 0 for lock types
    /// that don't keep them.
    contended: u64 = 0,
    wakes: u64 = 0,
};

/// Embedded in a *tracked* lock (`void`, zero-width, when untracked). Carries
/// ONLY what the lock itself lacks. `owner`+`snapshot` are how the dump reaches
/// the lock's own `state`/`waiters` generically across the two lock types —
/// plumbing, not duplicated state.
pub const Tracking = struct {
    holder_src: ?*const SourceLocation = null,
    held_since_ms: i64 = 0,
    next: ?*Tracking = null,
    prev: ?*Tracking = null,
    owner: *anyopaque = undefined,
    snapshot: *const fn (*anyopaque) Snap = undefined,
    /// 0 = unregistered, 2 = linked. Lets the common case skip `reg_mu`.
    registered: std.atomic.Value(u8) = .init(0),
};

// Intrusive live list of every tracked lock's embedded `Tracking`, guarded by a
// std.Io.Mutex. MUST stay a thread mutex (not a fiber lock): guarding fiber-lock
// tracking with a fiber lock would recurse, and the dump/unregister paths run
// from the main thread during shutdown after the runtime is torn down. Critical
// sections are pure CPU (link/unlink/walk) — no yields inside. Doubly
// linked so an unlink takes no walk: shutdown unlinks every lock, and
// 26,000 tenants hold about 230,000 of them.
var reg_mu: std.Io.Mutex = .init;
var live_head: ?*Tracking = null;
/// Count of currently-registered tracked locks (registers − unregisters).
/// Static atomic, so a teardown check can read it safely even if a leaked
/// lock's embedded node memory is already gone — which is exactly why we
/// *count* rather than *walk* the list to detect leaks. Returns to 0 once
/// every tracked lock has been `deinit`'d.
var registered_count: std.atomic.Value(usize) = .init(0);

inline fn regLock() void {
    reg_mu.lockUncancelable(_context.g().io);
}
inline fn regUnlock() void {
    reg_mu.unlock(_context.g().io);
}

/// Stamp the acquire site + time. Called from inside `lock`/`lockShared`/
/// `tryLock*` once ownership is held, on the acquirer's own stack. `@src` is a
/// comptime constant, so "capturing" it is just storing a `*rodata` pointer —
/// near-zero cost even on a hot tracked lock.
pub fn recordAcquire(
    self: *Tracking,
    owner: *anyopaque,
    snapshot: *const fn (*anyopaque) Snap,
    comptime src: SourceLocation,
) void {
    // A registered node whose owner is not the lock acquiring through it
    // was copied after its first acquire: the registry still links the
    // old address, and the next dump or unregister walks freed memory.
    // Catch it here, at the copy's first use, where `src` names it.
    if (self.registered.load(.acquire) == 2 and self.owner != owner)
        std.debug.panic(
            "tracked lock moved after its first acquire: {s}:{d}",
            .{ src.file, src.line },
        );
    ensureRegistered(self, owner, snapshot);
    self.holder_src = staticSrc(src);
    self.held_since_ms = _coarse.ms();
}

/// Lift a comptime `SourceLocation` to a `*rodata` pointer with static storage
/// duration (the anonymous-struct trick parks the value in `.rodata`).
fn staticSrc(comptime src: SourceLocation) *const SourceLocation {
    const S = struct {
        const value: SourceLocation = src;
    };
    return &S.value;
}

fn ensureRegistered(self: *Tracking, owner: *anyopaque, snapshot: *const fn (*anyopaque) Snap) void {
    if (self.registered.load(.acquire) == 2) return; // fast path: already linked
    regLock();
    defer regUnlock();
    if (self.registered.load(.acquire) == 2) return; // double-checked under lock
    self.owner = owner;
    self.snapshot = snapshot;
    self.prev = null;
    self.next = live_head;
    if (live_head) |head| head.prev = self;
    live_head = self;
    self.registered.store(2, .release);
    _ = registered_count.fetchAdd(1, .monotonic);
}

/// Unlink a tracked lock before it is destroyed. Mandatory for non-static
/// tracked locks — a freed lock left in the list would dangle the next dump.
/// No-op if never registered (the lock was never acquired).
pub fn unregister(self: *Tracking) void {
    if (self.registered.load(.acquire) != 2) return;
    regLock();
    defer regUnlock();
    if (self.prev) |prev| prev.next = self.next else live_head = self.next;
    if (self.next) |next| next.prev = self.prev;
    self.next = null;
    self.prev = null;
    self.registered.store(0, .release);
    _ = registered_count.fetchSub(1, .monotonic);
}

/// Walk the live list and panic if it is inconsistent: a node whose
/// `registered` flag is not set (its lock was freed without `deinit`;
/// the testing allocator poisons freed memory), or more nodes than the
/// count (a lock copied after its first acquire, whose copy `deinit`'d
/// and decremented the count while the original stays linked). For the
/// test runner, after each test, so the culprit is named while its
/// frame is fresh; the same defect would otherwise surface as a
/// segfault in an unrelated test's teardown.
pub fn checkLockRegistryForTests() void {
    regLock();
    defer regUnlock();

    var walked: usize = 0;
    var node = live_head;
    while (node) |n| : (node = n.next) {
        walked += 1;
        if (n.registered.load(.acquire) != 2 or
            walked > registered_count.load(.monotonic))
            @panic("lock registry: a lock was freed without deinit " ++
                "or copied after its first acquire");
    }
}

/// Number of tracked locks currently registered (acquired at least once and
/// not yet `deinit`'d). A teardown check expecting 0 — e.g. at the end of the
/// test suite, where every tracked lock is transient — catches a `.tracked`
/// lock that was never `deinit`'d, which would otherwise dangle the registry.
pub fn trackedLockCount() usize {
    return registered_count.load(.monotonic);
}

/// Walk the registry and print one line per *currently held* lock: where it
/// was acquired (`@src`), how long held, the derived holder count, and whether
/// anyone is waiting (the actual waiters, with full stacks, are in
/// `dumpFibers`). Idle locks are skipped. Held under `reg_mu` for the whole
/// walk — a rare admin command writing to an in-memory buffer, so microseconds.
pub fn dumpLocks(w: *std.Io.Writer) !void {
    regLock();
    defer regUnlock();
    const now = _coarse.ms();
    var total: usize = 0;
    var held: usize = 0;
    var node = live_head;
    while (node) |n| : (node = n.next) {
        total += 1;
        const snap = n.snapshot(n.owner);
        // Lifetime gauges for every lock that ever contended, named by
        // its last holder site — the rate between two dumps is what a
        // hot lock looks like.
        if (snap.contended > 0) {
            if (n.holder_src) |sl| {
                try w.print("lock {s}:{d}", .{ sl.file, sl.line });
            } else {
                try w.writeAll("lock -");
            }
            try w.print(" contended={d} wakes={d}\n", .{
                snap.contended, snap.wakes,
            });
        }
        if (snap.held == 0) continue;
        held += 1;
        const held_for: i64 = if (n.held_since_ms == 0) 0 else now - n.held_since_ms;
        if (n.holder_src) |sl| {
            try w.print("held_by={s}:{d}", .{ sl.file, sl.line });
        } else {
            try w.writeAll("held_by=-");
        }
        try w.print(" held_for_ms={d} count={d} waiters={s}\n", .{
            held_for, snap.held, if (snap.has_waiters) "yes" else "no",
        });
    }
    try w.print("LOCKS: {d} tracked locks, {d} currently held\n", .{ total, held });
}
