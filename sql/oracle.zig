// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Oracle: which keys committed recently, and at what timestamp.
//!
//! One map for every namespace, keyed by row/table identity hash
//! (zdb `Key.identityHash` / `TableKey.identityHash`, which hash the
//! namespace in), holding the newest known `commit_ts` per key. Two
//! consumers, one question — "has this key changed since a
//! transaction's read_ts?":
//!   - write path: after taking a row lock, `commitTs(h) > read_ts`
//!     means a conflicting commit landed after the txn began — bump
//!     read_ts to that value (or abort).
//!   - commit validation: a bumped txn re-asks the same question for
//!     every key it read.
//! `updateItems` runs on the apply path, BEFORE the committing txn's
//! locks release — so a later acquirer of any written key always sees
//! its commit.
//!
//! Memory is bounded by TWO GENERATIONS: updates go into `current`;
//! lookups check `current` then `previous`; a rotation retires
//! `previous`, `current` takes its place, and updates start over. A
//! generation ends at `max_keys` entries, or on the caller's clock
//! (`rotate`, every few minutes) when writes are slow. An entry
//! therefore lives one to two generations. A horizon per namespace
//! makes eviction safe:
//!
//!   INVARIANT: every commit of namespace `ns` with
//!   `commit_ts > horizon(ns)` is present in current or previous.
//!
//! Rotation maintains it without any wall-clock reasoning: each
//! generation tracks, per namespace, the max commit_ts written into
//! it, and the namespace's horizon max-folds the dying generation's.
//! An absent key's last commit is thus provably <= its namespace's
//! horizon, and `commitTs` answers that horizon for it — conservative
//! (can demand an unneeded bump, for transactions older than a
//! rotation period) but never wrong (a conflict can never hide).
//! Nothing here needs to survive a leader change: a new leader's
//! transactions begin with read_ts at the applied frontier, at or
//! above every commit that ever registered.
//!
//! The horizon is per namespace and the maps are not, because the
//! maps are what cost memory and the horizon is what a neighbour
//! could harm. A map pair per database was 32 MiB resident at its
//! first statement and up to 540 MiB once written to, whatever the
//! database held. Shared, a database that writes fast ends every
//! generation sooner, so the others' entries are retired sooner too;
//! but each of those answers with its own namespace's horizon, the
//! newest of its own retired commits, never with the fast writer's.
//!
//! The two maps are sized for `max_keys` once and swapped, never
//! freed or grown: a generation's map is cleared and reused as the
//! next `current`. Under a 160k rows/s load a time-based generation
//! reached 64M keys, and each doubling of the map rehashed it under
//! the lock on the apply fiber, stalling every commit for seconds.
const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const zdb = @import("zdb");

pub const Oracle = struct {
    allocator: std.mem.Allocator,
    mu: fiber.RWMutex(.tracked) = .{},
    cur_gen: Generation = .{},
    prev_gen: Generation = .{},
    /// Namespace -> its horizon (module invariant). A namespace gets
    /// its entry at its first recorded commit and keeps it; one that
    /// has none reads as 0. A value only ever rises.
    horizons: HorizonMap = .empty,
    /// Coarse clock at the last rotation of either kind. `rotate`
    /// skips a tick that would retire a generation younger than the
    /// period, so an entry always lives at least one period.
    last_rotation_ms: i64,
    /// Entries per generation; the maps hold this many without a
    /// grow. At 100k writes/s the default is a window of about two
    /// minutes, which a transaction may outlive at the cost of a
    /// conservative bump.
    max_keys: u32,

    /// What a map of 16M slots holds: about 270 MB per map, two maps
    /// for the whole server. A round 8M took the same slots and left
    /// a third of this unused; one key more would double them.
    pub const default_max_keys: u32 = keysFor(16 << 20);

    /// The most keys a map of `slots` slots, a power of two, takes
    /// before it grows.
    pub fn keysFor(slots: u32) u32 {
        const percent = std.hash_map.default_max_load_percentage;
        return @intCast(@as(u64, slots) * percent / 100);
    }

    const HorizonMap = std.AutoHashMapUnmanaged(u64, u64);

    const Generation = struct {
        /// key identity hash -> newest commit_ts this window.
        map: std.AutoHashMapUnmanaged(u64, u64) = .empty,
        /// Namespace -> max commit_ts recorded in this generation;
        /// folded into the namespace's horizon when the generation is
        /// dropped.
        horizon: HorizonMap = .empty,

        /// The caller has made room (`updateItems`).
        fn raiseHorizon(self: *Generation, ns: u64, ts: u64) void {
            const gop = self.horizon.getOrPutAssumeCapacity(ns);
            if (!gop.found_existing or gop.value_ptr.* < ts)
                gop.value_ptr.* = ts;
        }
    };

    pub fn init(allocator: std.mem.Allocator, max_keys: u32) !Oracle {
        var self: Oracle = .{
            .allocator = allocator,
            .max_keys = max_keys,
            .last_rotation_ms = utils.coarseMs(),
        };
        errdefer self.deinit();
        try self.cur_gen.map.ensureTotalCapacity(allocator, max_keys);
        try self.prev_gen.map.ensureTotalCapacity(allocator, max_keys);
        return self;
    }

    pub fn deinit(self: *Oracle) void {
        for ([_]*Generation{ &self.cur_gen, &self.prev_gen }) |gen| {
            gen.map.deinit(self.allocator);
            gen.horizon.deinit(self.allocator);
        }
        self.horizons.deinit(self.allocator);
        self.mu.deinit();
    }

    /// Record a run of committed items of namespace `ns` — one mu hold
    /// for the whole write set (the apply fiber records a commit at
    /// once). Each item's version timestamp is its commit ts (apply
    /// stamps it with the entry's HLC); apply order makes per-key
    /// timestamps non-decreasing, and the max-fold keeps a same-window
    /// re-commit from regressing.
    pub fn updateItems(
        self: *Oracle,
        ns: u64,
        items: []const zdb.kv.Item,
    ) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        // Every slot this call or a rotation inside it writes for
        // `ns`, taken before the first key goes in: a failure after
        // that would leave keys in a map that no horizon learns of
        // when they are retired.
        const horizon = try self.horizons.getOrPut(self.allocator, ns);
        if (!horizon.found_existing) horizon.value_ptr.* = 0;
        try self.cur_gen.horizon.ensureUnusedCapacity(self.allocator, 1);
        try self.prev_gen.horizon.ensureUnusedCapacity(self.allocator, 1);

        var newest: u64 = 0;
        for (items) |item| {
            const h = item.key.identityHash();
            const ts = item.newest().ts;
            // A full generation ends here, on the apply fiber, so the
            // insert below always has room and never grows the map.
            if (self.cur_gen.map.count() >= self.max_keys) {
                self.cur_gen.raiseHorizon(ns, newest);
                newest = 0;
                self.rotateLocked();
            }
            const gop = self.cur_gen.map.getOrPutAssumeCapacity(h);
            if (!gop.found_existing or gop.value_ptr.* < ts)
                gop.value_ptr.* = ts;
            newest = @max(newest, ts);
        }
        self.cur_gen.raiseHorizon(ns, newest);
    }

    /// The newest commit_ts known for key-hash `h` of namespace `ns`.
    /// For a key with no live entry this is the namespace's horizon —
    /// an upper bound on whatever its true last commit was (module
    /// invariant), so callers comparing `commitTs > read_ts` err
    /// toward seeing a conflict, never miss one.
    pub fn commitTs(self: *Oracle, ns: u64, h: u64) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.lookupLocked(h) orelse self.horizonLocked(ns);
    }

    /// The newest commit_ts across a whole set of key-hashes of
    /// namespace `ns` — one mu hold instead of a call per key (commit
    /// validation checks a txn's full read set at once). Absent keys
    /// contribute the horizon, as in `commitTs`; an empty set answers
    /// 0.
    pub fn maxCommitTs(self: *Oracle, ns: u64, hashes: []const u64) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();

        var max: u64 = 0;
        var absent = false;
        for (hashes) |h| {
            if (self.lookupLocked(h)) |ts|
                max = @max(max, ts)
            else
                absent = true;
        }
        if (absent) max = @max(max, self.horizonLocked(ns));
        return max;
    }

    /// The horizon of `ns` (module invariant).
    pub fn horizonTs(self: *Oracle, ns: u64) u64 {
        self.mu.lockShared(@src());
        defer self.mu.unlockShared();
        return self.horizonLocked(ns);
    }

    fn horizonLocked(self: *Oracle, ns: u64) u64 {
        return self.horizons.get(ns) orelse 0;
    }

    fn lookupLocked(self: *Oracle, h: u64) ?u64 {
        // A re-committed key can sit in both generations; `current`
        // holds the newer value.
        return self.cur_gen.map.get(h) orelse self.prev_gen.map.get(h);
    }

    /// Age out the older generation on the caller's clock: `previous`
    /// is retired (its maxes fold into the horizons first), `current`
    /// becomes `previous`, and updates start over. A generation also
    /// ends on its own when it reaches `max_keys` (see `updateItems`);
    /// a tick within `period_ms` of any rotation is skipped, or it
    /// would retire entries written seconds ago and force needless
    /// bumps. Pass 0 to rotate unconditionally.
    pub fn rotate(self: *Oracle, period_ms: u64) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        const age: u64 = @intCast(utils.coarseMs() - self.last_rotation_ms);
        if (age < period_ms) return;
        self.rotateLocked();
    }

    /// The retired maps are cleared and reused as the new `current`,
    /// so they keep their capacity for the life of the oracle.
    fn rotateLocked(self: *Oracle) void {
        var it = self.prev_gen.horizon.iterator();
        while (it.next()) |retired| {
            // Present since the namespace's first commit was recorded.
            const horizon = self.horizons.getPtr(retired.key_ptr.*).?;
            horizon.* = @max(horizon.*, retired.value_ptr.*);
        }
        std.mem.swap(Generation, &self.prev_gen, &self.cur_gen);
        self.cur_gen.map.clearRetainingCapacity();
        self.cur_gen.horizon.clearRetainingCapacity();
        self.last_rotation_ms = utils.coarseMs();
    }
};
