// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! SemaphoreMap: keyed reader/writer locks with owner tokens and
//! block-time deadlock detection.
//!
//! `acquireMany(keys, owner, mode)` blocks — FIFO, writer-preferred, all
//! delegated to a per-entry `sync.RWMutex` — while conflicting owners
//! hold `key`; it is a no-op for an owner re-acquiring at the same or
//! weaker mode, and returns `error.Deadlock` when granting would close
//! a wait cycle. `owner_id` is an opaque caller token (a transaction
//! id), not fiber identity, so a logical owner can span fibers and any
//! of them may release.
//!
//! There are NO timeouts. Every wait-for cycle has a last edge, and the
//! requester inserting it runs a DFS over the graph under `mu` BEFORE
//! parking — so deadlocks are refused at the door (victim = the
//! edge-closer), no cycle can ever form among parked fibers, and no
//! parked fiber ever needs interrupting.
//!
//! This file never touches fibers, parks, or deadlines: blocking is the
//! entry RWMutex's job. `mu` guards the entry table and the wait-for
//! graph (holders + registration-ordered waiters + `waiting`), which
//! mirror the RWMutex state for detection but never replace it.
//!
//! Keys are bare u64s — typically a hash of the caller's logical key so the map
//! never touches key bytes and the hot path allocates nothing once the entry
//! pool is warm. For hashing callers, a 64-bit collision silently MERGES two
//! keys' locks (false contention, or a false re-entrant no-op for one owner) —
//! never corruption — and with a handful of live entries the odds are ~2^-64;
//! we accept that.

const std = @import("std");
const _sync = @import("sync.zig");
const Spinlock = @import("spinlock.zig").Spinlock;

pub const SemaphoreMap = struct {
    allocator: std.mem.Allocator,
    /// Guards the entry table, the wait-for graph and the pool.
    /// A spinlock, not a fiber mutex: every hold is a hash probe, a
    /// list edit, the deadlock DFS or a walk over one statement's keys,
    /// microseconds, and no hold parks (`waitForLock` lets go before
    /// it parks on the key). A mutex here convoyed: its unlock handed
    /// the lock to a parked waiter queued on the unlocker's worker, so
    /// the waiter owned the lock while waiting behind that worker's
    /// statement, and a 16-stream load spent 4 ms per statement in
    /// this walk (1.14M hand-offs in 191k statements). A spinner
    /// burns a few microseconds of its core instead and never owns
    /// the lock while queued.
    mu: Spinlock = .{},
    /// Held-or-contended keys only; an entry is created on first
    /// acquire and recycled by whichever release/abort leaves it with
    /// no holders and no waiters. An ArrayHashMap, not a HashMap: a
    /// statement inserts its keys and removes them at commit, so the
    /// map churns at the write rate, and HashMap's tombstone deletes
    /// leave it with no free slot to end a miss on; a `getOrPut` then
    /// probes the whole table, 6 us each with every other connection
    /// spinning on `mu` meanwhile (a third of the server's CPU at
    /// 100k rows/s). ArrayHashMap removes by shifting, no tombstones.
    entries: std.AutoArrayHashMapUnmanaged(u64, *Entry) = .empty,
    /// The wait-for graph's edge source: which entry each BLOCKED owner
    /// is queued on (an owner waits on one key at a time), and in what
    /// mode. Registered before the fiber ever parks. Same churn, same
    /// map type.
    waiting: std.AutoArrayHashMapUnmanaged(u64, Waiting) = .empty,
    /// Drained entries kept for reuse with their list capacity intact,
    /// so acquire/release under load does no allocator work. Bounded by
    /// `max_pooled`, and an entry whose lists a spike ballooned is
    /// freed instead; `sweep` cuts it to what the map has been using.
    pool: std.ArrayList(*Entry) = .empty,
    /// wouldDeadlock's walker, reused across runs. Only touched under
    /// `mu`, which serializes detection.
    dfs: Dfs,
    /// The most keys held at once since the last `sweep`, and in the
    /// window before that: what the map has been using, which a sweep
    /// sizes it for. Between statements a map holds nothing, so the
    /// count at a sweep says nothing about its use.
    peak: usize = 0,
    last_peak: usize = 0,

    const max_pooled = 128;
    /// A pooled entry keeps its holders/waiters capacity; anything that
    /// grew past this many slots during a spike is freed, not pooled.
    const max_kept_capacity = 32;

    pub const Mode = enum { shared, exclusive };

    const Waiting = struct { entry: *Entry, mode: Mode };
    const OwnerMode = struct { owner_id: u64, mode: Mode };

    const Entry = struct {
        /// Does ALL the blocking; writer preference means a pending
        /// exclusive blocks new shared acquirers (see sync.RWMutex).
        rw: _sync.RWMutex(.untracked) = .{},
        /// One exclusive holder, or any number of shared ones.
        holders: std.ArrayList(OwnerMode) = .empty,
        /// Blocked acquirers, registration order.
        waiters: std.ArrayList(OwnerMode) = .empty,

        // Mode-dispatched RWMutex ops. `rw` is `.untracked`, so the
        // `@src()` these pass is comptime-elided — no source location is
        // lost by routing through a helper.
        fn tryLock(e: *Entry, mode: Mode) bool {
            return switch (mode) {
                .exclusive => e.rw.tryLock(@src()),
                .shared => e.rw.tryLockShared(@src()),
            };
        }
        fn lock(e: *Entry, mode: Mode) void {
            switch (mode) {
                .exclusive => e.rw.lock(@src()),
                .shared => e.rw.lockShared(@src()),
            }
        }
        fn unlock(e: *Entry, mode: Mode) void {
            switch (mode) {
                .exclusive => e.rw.unlock(),
                .shared => e.rw.unlockShared(),
            }
        }
    };

    /// One walk of the wait-for graph: owners still to expand, plus the
    /// ones already expanded so a diamond costs a single visit. Kept
    /// (with its capacity) between walks, so steady-state detection
    /// does no allocator work.
    const Dfs = struct {
        allocator: std.mem.Allocator,
        stack: std.ArrayList(u64) = .empty,
        visited: std.AutoHashMapUnmanaged(u64, void) = .empty,

        fn deinit(self: *Dfs) void {
            self.stack.deinit(self.allocator);
            self.visited.deinit(self.allocator);
        }

        /// Drop the previous walk, keep its capacity.
        fn restart(self: *Dfs) void {
            // Not clearRetainingCapacity (see utils.clearRetainCap):
            // it memsets the retained items in safe builds.
            self.stack.items.len = 0;
            self.visited.clearRetainingCapacity();
        }

        /// Queue `owner` for expansion. Duplicates are fine: `next`
        /// hands out each owner at most once.
        fn push(self: *Dfs, owner: u64) error{OutOfMemory}!void {
            try self.stack.append(self.allocator, owner);
        }

        /// The next owner to expand, or null once the walk is done.
        fn next(self: *Dfs) error{OutOfMemory}!?u64 {
            while (self.stack.pop()) |owner| {
                const gop = try self.visited.getOrPut(self.allocator, owner);
                if (!gop.found_existing) return owner;
            }
            return null;
        }
    };

    pub fn init(allocator: std.mem.Allocator) SemaphoreMap {
        return .{
            .allocator = allocator,
            .dfs = .{ .allocator = allocator },
        };
    }

    pub fn deinit(self: *SemaphoreMap) void {
        // Callers release everything they hold before tearing the map
        // down; anything left here is an unclean shutdown.
        for (self.entries.values()) |e| self.destroyEntry(e);
        self.entries.deinit(self.allocator);
        for (self.pool.items) |e| self.destroyEntry(e);
        self.pool.deinit(self.allocator);
        self.dfs.deinit();
        self.waiting.deinit(self.allocator);
    }

    fn destroyEntry(self: *SemaphoreMap, e: *Entry) void {
        e.holders.deinit(self.allocator);
        e.waiters.deinit(self.allocator);
        self.allocator.destroy(e);
    }

    /// The contended path of `acquireMany`, entered with `mu` held and
    /// `e` just seen refusing `owner_mode.mode`: publish the wait edge,
    /// refuse if blocking would close a wait cycle, else park until the
    /// hand-off and move waiter → holder. Returns with `mu` held again
    /// so the caller's walk goes on; an error releases it. The edge
    /// must be visible before we park so any later requester's DFS can
    /// see it — that ordering is the completeness argument. `key` is
    /// only for the rollback on OOM after the wake.
    fn waitForLock(
        self: *SemaphoreMap,
        key: u64,
        e: *Entry,
        owner_mode: OwnerMode,
    ) error{ Deadlock, OutOfMemory }!void {
        const owner_id = owner_mode.owner_id;
        const mode = owner_mode.mode;
        e.waiters.append(self.allocator, owner_mode) catch {
            self.mu.unlock();
            return error.OutOfMemory;
        };
        // An owner can only be waiting on exactly one key at any time.
        self.waiting.put(self.allocator, owner_id, .{
            .entry = e,
            .mode = mode,
        }) catch {
            _ = removeOwner(&e.waiters, owner_id);
            self.mu.unlock();
            return error.OutOfMemory;
        };

        const cyclic = self.wouldDeadlock(owner_id, e, mode) catch {
            self.deregisterWaiter(e, owner_id);
            self.mu.unlock();
            return error.OutOfMemory;
        };
        if (cyclic) {
            self.deregisterWaiter(e, owner_id);
            self.mu.unlock();
            return error.Deadlock;
        }
        self.mu.unlock();

        e.lock(mode); // Block here waiting for lock.

        // Granted. Move waiter → holder; until this runs we are still
        // in `waiters`, which is what keeps the entry alive across the
        // wake (release only frees an entry with no holders AND no
        // waiters).
        self.mu.lock();
        self.deregisterWaiter(e, owner_id);
        e.holders.append(self.allocator, owner_mode) catch {
            self.rollbackHold(key, e, mode);
            self.mu.unlock();
            return error.OutOfMemory;
        };
    }

    /// Whether `owner_id` already holds `e` in a mode that covers
    /// `mode`: an exclusive hold covers both, a shared hold covers a
    /// shared ask. `error.Upgrade` for shared held and exclusive asked:
    /// an upgrade in place would wait for the owner's own shared hold
    /// to drain, a self-deadlock, and two owners upgrading in lockstep
    /// deadlock classically.
    fn alreadyHeld(
        e: *const Entry,
        owner_id: u64,
        req_mode: Mode,
    ) error{Upgrade}!bool {
        for (e.holders.items) |hold| {
            if (hold.owner_id != owner_id) continue;
            if (req_mode == .shared or hold.mode == .exclusive) return true;
            return error.Upgrade;
        }
        return false;
    }

    /// Block until `owner_id` holds every key in `mode` — FIFO,
    /// writer-preferred, re-entrant. Every uncontended key is taken under
    /// one hold of `mu`: a statement locking hundreds of rows costs one
    /// round on the map lock instead of one per key, which is what 16
    /// connections doing that at once spent most of their CPU on. A
    /// contended key goes to `waitForLock` with `mu` still held, which
    /// parks, detects deadlocks and comes back with `mu` re-taken; the
    /// walk resumes after it. Keys `owner_id` already holds are
    /// skipped. Each key newly held is appended to `newly`, whose
    /// capacity the caller has reserved for `keys.len`, so the caller
    /// releases exactly what it got.
    /// `error.Deadlock` if a wait would close a cycle, `error.Upgrade`
    /// if a key is held shared and asked exclusive (the map does not
    /// upgrade in place). On an error the keys taken so far stay held
    /// and are in `newly`.
    pub fn acquireMany(
        self: *SemaphoreMap,
        keys: []const u64,
        owner_id: u64,
        mode: Mode,
        newly: *std.ArrayList(u64),
    ) error{ Deadlock, Upgrade, OutOfMemory }!void {
        const owner_mode = OwnerMode{ .owner_id = owner_id, .mode = mode };

        // One hold of `mu` for the whole walk; only a park on a
        // contended key lets go of it, and waitForLock re-takes it
        // before returning, so the walk never re-locks itself.
        self.mu.lock();
        for (keys) |key| {
            const e = self.entryFor(key) catch {
                self.mu.unlock();
                return error.OutOfMemory;
            };
            const held = alreadyHeld(e, owner_id, mode) catch |err| {
                self.mu.unlock();
                return err;
            };
            if (held) continue;
            if (!e.tryLock(mode)) {
                // Contended: the entry was just probed, so no second
                // lookup or hold check on the way in.
                try self.waitForLock(key, e, owner_mode);
                newly.appendAssumeCapacity(key);
                continue;
            }
            e.holders.append(self.allocator, owner_mode) catch {
                self.rollbackHold(key, e, mode);
                self.mu.unlock();
                return error.OutOfMemory;
            };
            newly.appendAssumeCapacity(key);
        }
        self.mu.unlock();
    }

    /// Undo a granted-but-unrecorded rw hold (OOM recording it): unlock
    /// the rw — waking any parked contender; they re-take `mu`, which we
    /// hold, so they simply queue behind us — and reclaim the entry if
    /// that left it unused. Caller holds `mu`.
    fn rollbackHold(
        self: *SemaphoreMap,
        key: u64,
        e: *Entry,
        mode: Mode,
    ) void {
        e.unlock(mode);
        self.maybeRecycle(key, e);
    }

    /// Release `owner_id`'s holds on `keys` under one hold of `mu`; the
    /// pair of `acquireMany`. Each entry is freed when nothing holds or
    /// waits on it any more; otherwise its RWMutex hands off (writer
    /// first, else all readers). Releasing a key the owner does not
    /// hold is a caller bookkeeping bug (a txn releases exactly the
    /// keys `acquireMany` gave it), so it panics.
    pub fn releaseMany(
        self: *SemaphoreMap,
        keys: []const u64,
        owner_id: u64,
    ) void {
        self.mu.lock();
        defer self.mu.unlock();
        for (keys) |key| {
            if (!self.releaseLocked(key, owner_id))
                std.debug.panic(
                    "SemaphoreMap.releaseMany: owner {d} does not hold key {x}",
                    .{ owner_id, key },
                );
        }
    }

    /// The release itself; caller holds `mu`. False when `owner_id`
    /// does not hold `key`.
    fn releaseLocked(self: *SemaphoreMap, key: u64, owner_id: u64) bool {
        const kv = self.entries.getEntry(key) orelse return false;
        const e = kv.value_ptr.*;
        const mode = removeOwner(&e.holders, owner_id) orelse return false;
        if (e.holders.items.len == 0 and e.waiters.items.len == 0) {
            // We hold `mu` and just proved the entry drained, so drop it
            // straight from the map — no need for reclaimLocked's
            // defensive re-lookup (that guards the error paths, which
            // don't keep a `key_ptr`). The still-locked rw needs no
            // unlock before recycling — `recycle` resets it.
            _ = self.entries.swapRemove(key);
            self.recycle(e);
            return true;
        }
        // Unlock the rw while still holding `mu`. Once `mu` is dropped a
        // concurrent release can drain and recycle `e` — reset its rw, or
        // free it outright when a contention spike grew its lists — and
        // an rw unlock after that lands on a reset lock or freed memory
        // (the first word of a freed Entry is its rw: a cross-thread
        // double free under a 64-client pgbench). The wake this may
        // issue costs a peer at most a short spin.
        e.unlock(mode);
        return true;
    }

    /// The entry for `key`, pulled from the pool (or created) on first
    /// use. Caller holds `mu`.
    fn entryFor(self: *SemaphoreMap, key: u64) !*Entry {
        const gop = try self.entries.getOrPut(self.allocator, key);
        if (gop.found_existing) return gop.value_ptr.*;
        self.peak = @max(self.peak, self.entries.count());

        if (self.pool.pop()) |e| {
            gop.value_ptr.* = e;
            return e;
        }
        const e = self.allocator.create(Entry) catch |err| {
            _ = self.entries.swapRemove(key);
            return err;
        };
        e.* = .{};
        gop.value_ptr.* = e;
        return e;
    }

    /// Drop an entry with no holders and no waiters. Caller holds `mu`.
    /// Looks the entry up by key and compares pointers BEFORE any
    /// deref: callers on error paths may hold an `e` that a concurrent
    /// release already recycled (and the key may even map to a fresh
    /// entry).
    fn maybeRecycle(self: *SemaphoreMap, key: u64, e: *Entry) void {
        const kv = self.entries.getEntry(key) orelse return;
        if (kv.value_ptr.* != e) return;
        if (e.holders.items.len > 0 or e.waiters.items.len > 0) return;

        // No one else is holding this key-entry. Remove.
        _ = self.entries.swapRemove(key);
        self.recycle(e);
    }

    /// Reset a drained entry and pool it for reuse — or free it when
    /// the pool is full or a contention spike ballooned its lists.
    /// The rw may still be locked (release skips the unlock on the
    /// reclaim path); nothing references it anymore, so a plain reset
    /// is sound. Caller holds `mu`.
    fn recycle(self: *SemaphoreMap, e: *Entry) void {
        if (self.pool.items.len >= max_pooled or
            e.holders.capacity > max_kept_capacity or
            e.waiters.capacity > max_kept_capacity)
        {
            self.destroyEntry(e);
        } else {
            e.rw = .{};
            // As above: keep the buffers without the safe-build memset.
            e.holders.items.len = 0;
            e.waiters.items.len = 0;
            // Grown on demand, not reserved at init: a reserve costs
            // every database the same, whether it ever locks a row.
            self.pool.append(self.allocator, e) catch self.destroyEntry(e);
        }
    }

    /// One tick of the owner's sweeper, on a fixed period: size the map
    /// for what it used over the last two periods, when that is under
    /// a quarter of what it has. A burst's memory so lives two to
    /// three periods, long enough for the next statement to reuse it,
    /// a database that went on to run small statements comes down to
    /// their size, not to nothing and back, and one that went quiet
    /// holds nothing: a map is one database's, and keeping even a
    /// small statement's entries cost each idle database kilobytes.
    /// Nothing points into the tables across a release of `mu`, so
    /// moving them under it is sound; the entries themselves stay
    /// where parked fibers hold pointers to them.
    pub fn sweep(self: *SemaphoreMap) void {
        self.mu.lock();
        defer self.mu.unlock();

        const want = @max(self.last_peak, self.peak);
        self.last_peak = self.peak;
        // The keys still held open the next window.
        self.peak = self.entries.count();

        if (want * 4 < self.entries.capacity()) {
            std.debug.assert(self.entries.count() <= want);
            shrinkTable(*Entry, &self.entries, self.allocator, want);
        }
        // Blocked owners only, so sized to its own use.
        const blocked = self.waiting.count();
        if (blocked * 4 < self.waiting.capacity())
            shrinkTable(Waiting, &self.waiting, self.allocator, blocked);

        while (self.pool.items.len > want)
            self.destroyEntry(self.pool.pop().?);
        if (self.pool.capacity > want)
            self.pool.shrinkAndFree(self.allocator, self.pool.items.len);
    }

    /// Move `table` to one of exactly `want` entries: an ArrayHashMap
    /// keeps its arrays at their peak through removes, and its index
    /// through `shrinkAndFree`. On OOM the table stays as it is.
    fn shrinkTable(
        comptime V: type,
        table: *std.AutoArrayHashMapUnmanaged(u64, V),
        allocator: std.mem.Allocator,
        want: usize,
    ) void {
        var fresh: std.AutoArrayHashMapUnmanaged(u64, V) = .empty;
        // Nothing to size for: the table was empty to get here.
        if (want == 0) {
            table.deinit(allocator);
            table.* = fresh;
            return;
        }
        // The arrays first, at `want` exactly: `ensureTotalCapacity`
        // would grow them with a list's slack, and only adds the index
        // once they are large enough.
        fresh.entries.setCapacity(allocator, want) catch return;
        fresh.ensureTotalCapacity(allocator, want) catch {
            fresh.deinit(allocator);
            return;
        };
        for (table.keys(), table.values()) |k, v|
            fresh.putAssumeCapacityNoClobber(k, v);
        table.deinit(allocator);
        table.* = fresh;
    }

    /// Pull `owner_id`'s published wait edge: its slot in the entry's
    /// waiter list and its entry in the `waiting` graph. Caller holds
    /// `mu`.
    fn deregisterWaiter(self: *SemaphoreMap, e: *Entry, owner_id: u64) void {
        _ = removeOwner(&e.waiters, owner_id);
        _ = self.waiting.swapRemove(owner_id);
    }

    fn removeOwner(list: *std.ArrayList(OwnerMode), owner_id: u64) ?Mode {
        for (list.items, 0..) |om, i| {
            if (om.owner_id == owner_id) {
                _ = list.orderedRemove(i);
                return om.mode;
            }
        }
        return null;
    }

    /// Would `start` blocking on `entry` in `mode` close a wait cycle
    /// through `start` itself? Walks the wait-for graph from start's
    /// own blockers outward (iteratively — fiber stacks are small): if
    /// following "waits for" edges leads back to `start`, granting the
    /// wait would deadlock. Runs under `mu`. A cycle NOT through
    /// `start` is impossible here — its own closing edge was refused
    /// when it was requested.
    fn wouldDeadlock(
        self: *SemaphoreMap,
        start: u64,
        entry: *Entry,
        mode: Mode,
    ) error{OutOfMemory}!bool {
        self.dfs.restart();
        try self.pushBlockers(entry, start, mode);
        while (try self.dfs.next()) |owner| {
            if (owner == start) return true;
            // Absent from `waiting` = runnable, so it blocks no one
            // else: a dead end for the walk.
            const w = self.waiting.get(owner) orelse continue;
            try self.pushBlockers(w.entry, owner, w.mode);
        }
        return false;
    }

    /// `mario` is blocked on `entry` wanting `mode`. Push everyone he
    /// has to wait for. Two kinds of blocker:
    ///   - holders: they have the lock right now.
    ///   - queued exclusive waiters: they hold nothing yet, but the
    ///     RWMutex can still grant them before mario.
    ///
    /// THE ONE ASSUMPTION: `waiters` is in `sync.RWMutex`'s grant order
    /// and that lock is writer-preferring FIFO. Every rule below follows
    /// from that single fact — it is not extra policy, it is a shadow of
    /// the lock's. Detection cannot be made order-agnostic: writer
    /// preference is what CREATES the reader-behind-writer deadlocks
    /// these waiter edges catch (drop them and such cycles hang, with no
    /// timeout backstop), and the "ahead of mario" cut is what stops two
    /// writers queued on one key from looking like a cycle. If
    /// sync.RWMutex's discipline ever changes, the "FIFO hand-off" and
    /// "writer preference holds" tests fail here rather than hanging in
    /// production — they pin this contract.
    fn pushBlockers(
        self: *SemaphoreMap,
        entry: *Entry,
        mario: u64,
        want_mode: Mode,
    ) error{OutOfMemory}!void {
        // Holders:
        //   - mario wants exclusive: he waits for all of them.
        //   - mario wants shared: he can share with shared holders, so
        //     only an exclusive holder blocks him.
        for (entry.holders.items) |holder| {
            if (want_mode == .shared and holder.mode == .shared) continue;
            try self.dfs.push(holder.owner_id);
        }
        // Waiters, in registration order, with mario somewhere in the
        // list:
        //   - queued shared waiters never block him — they are granted
        //     alongside him, or stuck behind the same writer he is.
        //   - mario wants exclusive: writers are FIFO among
        //     themselves, so only the exclusives AHEAD of him run
        //     first. Stop at his own slot.
        //   - mario wants shared: mutex writer preference parks readers
        //     while any writer is queued, whenever it arrived, so every
        //     queued exclusive runs first. Scan the whole list.
        for (entry.waiters.items) |waiter| {
            if (waiter.owner_id == mario) {
                if (want_mode == .exclusive) break; // out of for loop
                continue;
            }
            if (waiter.mode == .exclusive) try self.dfs.push(waiter.owner_id);
        }
    }
};
