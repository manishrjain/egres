// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const raft = @import("raft");
const zdb = @import("zdb");
const _governor = @import("governor.zig");
const fiber = @import("zfiber");
const m = @import("metrics");
const flags = @import("flags");
const utils = @import("raft").utils;
const String = utils.String;

/// `--snapshot` — Raft snapshot triggers, polling cadence, and the SST
/// file reclamation that hangs off snapshots. The snapshot loop reads
/// these every iteration so operators can tune at runtime via the live
/// flags channel.
pub const SnapshotCfg = struct {
    min_bytes: u64 = 1 << 30, // 1 GiB
    min_age_seconds: u64 = 10 * 60, // 10 minutes
    poll_interval_seconds: u64 = 60,
    orphan_min_age_seconds: u64 = 30 * 60,
    orphan_scan_interval_seconds: u64 = 30 * 60,
    max_txn_age_seconds: u64 = 60 * 60,

    pub const docs = .{
        .self = "Raft snapshot triggers. Leader proposes a snapshot when WAL" ++
            " tail bytes from head reach min_bytes OR an entry crosses" ++
            " min_age_seconds.",
        .min_bytes = "Snapshot triggers once WAL tail-bytes-from-head reach" ++
            " this.",
        .min_age_seconds = "Snapshot triggers when an entry is at least this" ++
            " many seconds older than wall clock.",
        .poll_interval_seconds = "How often the snapshot loop checks the" ++
            " criteria.",
        .orphan_min_age_seconds = "SST files the manifest does not know are" ++
            " removed only once older than this and not written after the" ++
            " snapshot; must exceed one compaction's window from first" ++
            " output write to proposal.",
        .orphan_scan_interval_seconds = "How often the remote store is" ++
            " scanned for orphan SST files.",
        .max_txn_age_seconds = "How long an open transaction on any" ++
            " voting node holds back the read floor, and with it the" ++
            " versions it can see. One open for longer is refused as" ++
            " snapshot too old on its next statement. At most 12 hours.",
    };
};

const zdb_bufs = zdb.buffers;
const _manifest = zdb.manifest;
const Forest = zdb.forest.Forest;
const ReadMarks = zdb.forest.ReadMarks;
const Item = zdb.kv.Item;
const Key = zdb.kv.Key;
const Meta = zdb.kv.Meta;
const TreeKey = zdb.kv.TreeKey;
const metrics = zdb.metrics;
const log = std.log.scoped(.engine);
/// A manifest apply past this holds every entry behind it: a warning.
const slow_manifest_apply_us: i64 = 100_000;

/// A front-end's hook into the commit stream. `onData` fires on the apply
/// fiber right after a committed Mutations entry is written to the forest,
/// with the entry's namespace and the rows written — a Mutations entry is
/// single-namespace (apply skips any key disagreeing with it), so `ns`
/// names every item's home. Lets a front-end (SQL) maintain caches (e.g.
/// a per-table max row id) from the commit stream on EVERY node, and a
/// node that becomes leader inherits them without re-scanning. MUST NOT
/// block, propose, or re-enter the engine.
pub const ApplyObserver = struct {
    ctx: *anyopaque,
    onData: *const fn (
        ctx: *anyopaque,
        ns: u64,
        items: []const zdb.kv.Item,
    ) void,
};

pub const StorageImpl = struct {
    forest: *Forest,
    allocator: std.mem.Allocator,
    node: *raft.Node,
    /// For `max_txn_age_seconds`, read when this node reports its
    /// oldest read to the leader (`minReadTs`).
    snapshot_cfg: *flags.Live(SnapshotCfg),
    /// Optional commit-stream hook (see ApplyObserver). Set once at startup
    /// via Engine.setApplyObserver, before the apply loop runs; the loop stops
    /// (eng.wait) before the observer's owner is torn down, so no unregister.
    apply_observer: ?ApplyObserver = null,
    /// Gate the apply path until recovery finishes. Until then trees, route
    /// tables, and SST files referenced by the snapshot may not be in
    /// place, and apply would error-retry-spin. Flipped (along with
    /// `oldest_memtable_index`) by `markRecovered`, which the Engine's
    /// recovery routine calls — even on the no-snapshot fast path so the
    /// gate doesn't wedge the apply loop forever.
    recovery_done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// Skip Mutations whose log index is strictly less than this during
    /// recovery replay — the corresponding data is in SSTs that the replay
    /// will install. Set from the recovery snapshot's
    /// `oldest_memtable_index` field (0 if no snapshot, equivalent to "no
    /// skipping"). Read-only after `recovery_done` flips, so no atomic.
    oldest_memtable_index: u64 = 0,

    pub fn asStateMachine(self: *StorageImpl) raft.Node.StateMachine {
        return .{
            .ctx = @ptrCast(self),
            .applyFn = &applyEntry,
            .ensureFilesFn = &prefetchFiles,
            .isReadyFn = &isReady,
            .snapshotInstallFn = &snapshotInstall,
            .minReadTsFn = &minReadTs,
        };
    }

    /// Reported to the leader in each append reply: the oldest read
    /// open here, or the applied timestamp when none is.
    fn minReadTs(ctx: *anyopaque) u64 {
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));
        return self.forest.minReadTs(
            self.node.appliedTs(),
            self.snapshot_cfg.cur().max_txn_age_seconds,
            ReadMarks.no_peers,
        );
    }

    /// A snapshot entry applied: its timestamp becomes
    /// `Forest.snapshot_ts`, on every node at this point in the log.
    fn snapshotInstall(ctx: *anyopaque, snap: *const raft.buffers.SnapshotStruct) void {
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));
        const floor = self.forest.setSnapshotTs(snap.timestamp);
        log.info("read floor: snapshot index={d} timestamp={d} floor={d}", .{ snap.index, snap.timestamp, floor });
    }

    /// Cheap check: SM is ready for a data proposal once `recoverFromSnapshot`
    /// has finished AND a real memtable (raft_index != 0) is current. The leader
    /// polls this in `Node.propose`; without the memtable gate, a
    /// write that races startup would land in the placeholder and be dropped
    /// by the first `swapCurrentOut`.
    fn isReady(ctx: *anyopaque) bool {
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));
        return self.recovery_done.load(.acquire) and self.forest.memtables.currentId() != 0;
    }

    fn applyEntry(ctx: *anyopaque, entry: raft.Entry, data: String) anyerror!void {
        // Empty-data entries (clock ticks) have no state machine work — the
        // raft node still advances `apply_ts` after we return, so the tick's
        // timestamp becomes visible to blocked readers.
        if (data.len == 0) return;
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));

        // Recovery barrier: if `runRecovery` is still fetching SSTs and
        // installing the snapshot manifest, applies would fail (missing
        // trees/files). Surface as `error.RecoveryInProgress` so the raft
        // apply loop retries (with shutdown polling) instead of us busy-
        // looping here.
        if (!self.recovery_done.load(.acquire)) return error.RecoveryInProgress;

        return self.applyEntryInner(entry, data);
    }

    fn applyEntryInner(self: *StorageImpl, entry: raft.Entry, data: String) !void {
        const parsed = zdb_bufs.parseMessageWithTail(data) catch {
            log.err("on_apply: unknown entry type at index={d}", .{entry.index});
            return;
        };
        switch (parsed) {
            .Mutations => |ms| {
                // Recovery shortcut: mutations below `oldest_memtable_index`
                // are durable in SSTs that the replay will install via
                // upcoming flush proposals. Putting them into the
                // placeholder memtable here would just waste CPU — the
                // placeholder is discarded by the first replayed
                // `newMemtable` anyway.
                if (entry.index < self.oldest_memtable_index) return;

                const ns = ms.namespace();
                const count = ms.kvsLen();

                var items: std.ArrayList(Item) = .empty;
                defer items.deinit(self.allocator);
                try items.ensureTotalCapacity(self.allocator, count);

                var total_bytes: u64 = 0;
                for (0..count) |i| {
                    const kv = try ms.kvsAt(@intCast(i));
                    // Wire keys are marshaled keys (self-describing
                    // ns/tid), with no timestamp: the commit timestamp is
                    // the entry's HLC. Minimum: 12-byte prefix + 1
                    // primary byte. A short key — or a key whose
                    // namespace disagrees with the entry's — means an
                    // encoder bug; skipping keeps apply deterministic
                    // across nodes without wedging the loop on a poison
                    // entry.
                    if (kv.key().len < 13) {
                        log.err("on_apply: short wire key at index={d} kv={d} len={d}", .{ entry.index, i, kv.key().len });
                        continue;
                    }
                    const key = Key.init(kv.key());
                    if (key.namespace != ns) {
                        log.err("on_apply: key ns={d} != entry ns={d} at index={d} kv={d}", .{ key.namespace, ns, entry.index, i });
                        continue;
                    }
                    items.appendAssumeCapacity(.{
                        .key = key,
                        .versions = .{ .one = .{
                            .ts = entry.timestamp,
                            .meta = Meta.fromByte(kv.meta()),
                            .value = kv.value(),
                        } },
                    });
                    total_bytes += kv.value().len;
                }

                // Write this to Forest.
                try self.forest.put(items.items);

                // Feed the committed rows to any front-end cache (SQL's
                // per-table max row id) — on every node, so a new leader
                // inherits it without a re-scan.
                if (self.apply_observer) |obs|
                    obs.onData(obs.ctx, ns, items.items);

                try metrics.g().bytes_written.incrBy(.{
                    .source = "user",
                    .namespace_id = ns,
                }, total_bytes);
            },
            .ManifestChange => |mc| {
                try self.applyManifestChange(entry.index, entry.timestamp, mc);
            },
            else => {
                log.err("on_apply: unhandled message type at index={d}", .{entry.index});
            },
        }
    }

    /// `ts` is the entry's commit timestamp: what a branch takes as its
    /// as-of point.
    fn applyManifestChange(
        self: *StorageImpl,
        index: u64,
        ts: u64,
        mc: zdb_bufs.ManifestChangeReader,
    ) !void {
        const mcs = try mc.toStruct(self.allocator);
        defer {
            mcs.deinit(self.allocator);
            self.allocator.destroy(mcs);
        }

        // The last "enter" without an "exit" names the entry that wedged
        // the apply path; debug only, two lines per manifest change.
        log.debug("applyManifestChange enter idx={d} cmd={t} mt={d} tcs={d}", .{
            index, mcs.cmd, mcs.memtable_id, mcs.sstable_changes.len,
        });
        // The apply fiber applies everything in order: a slow manifest
        // apply holds every data entry behind it, so a slow one is a
        // warning.
        const t0 = utils.timestampNow(.monotonic, .micros);
        defer {
            const took = utils.timestampNow(.monotonic, .micros) - t0;
            log.debug("applyManifestChange exit  idx={d} cmd={t} took={d}us", .{
                index, mcs.cmd, took,
            });
            if (took > slow_manifest_apply_us) {
                log.info(
                    "applyManifestChange slow idx={d} cmd={t} took={d}us",
                    .{ index, mcs.cmd, took },
                );
            }
        }

        switch (mcs.cmd) {
            .new_memtable => try self.forest.newMemtable(index),
            .flush => try self.forest.applyFlush(index, mcs.sstable_changes, mcs.memtable_id),
            .compaction => {
                if (mcs.sstable_changes.len > 0) {
                    const sc = mcs.sstable_changes[0];
                    if (self.forest.getTree(sc.namespace_id, sc.tree_id)) |tree| {
                        defer tree.release();
                        try tree.applyCompaction(index, mcs.sstable_changes);
                    }
                }
            },
            .index_backfill => {
                if (mcs.sstable_changes.len > 0) {
                    const sc = mcs.sstable_changes[0];
                    if (self.forest.getTree(sc.namespace_id, sc.tree_id)) |tree| {
                        defer tree.release();
                        try tree.applyBackfill(index, mcs.sstable_changes);
                    }
                }
            },
            .tree_split => try self.forest.applySplit(index, mcs.*),
            .create_ns, .drop_ns, .create_tree, .drop_tree => {
                try self.forest.applyTrees(index, mcs.*);
            },
            .branch_ns => try self.forest.applyNsBranch(
                index,
                ts,
                mcs.*,
            ),
            .branch_table => try self.forest.applyTableBranch(
                index,
                ts,
                mcs.*,
            ),
            else => log.err("unknown ManifestChange cmd={d}", .{@intFromEnum(mcs.cmd)}),
        }
        // Whatever this change unlisted has no level pointing at it;
        // drop its blocks and its table cache entry now rather than
        // when the file goes.
        try self.forest.evictTables();
    }

    /// StateMachine hook behind the announce prefetch: a peer announced
    /// these files are in the store; open them into the table cache so
    /// the manifest entry that follows applies without waiting on the
    /// store. One batch open, retried until it succeeds — the files that
    /// opened are cache hits on the retry — with error.ShuttingDown the
    /// only exit short of success.
    fn prefetchFiles(
        ctx: *anyopaque,
        files: []const raft.buffers.FileRefStruct,
    ) anyerror!void {
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));
        if (files.len == 0) return;
        const needed = try self.allocator.alloc(zdb.tree.TableNeed, files.len);
        defer self.allocator.free(needed);
        for (needed, files) |*n, f| n.* = .{
            .id = f.id,
            .level = 0,
            .extent = .{ .size = f.size, .index_offset = f.index_offset },
        };

        var attempt: u64 = 1;
        while (true) : (attempt += 1) {
            if (self.node.isShuttingDown()) return error.ShuttingDown;
            self.forest.prefetch(needed) catch |err| {
                log.warn(
                    "prefetch: {d} files attempt={d} failed: {s}; retrying",
                    .{ files.len, attempt, @errorName(err) },
                );
                fiber.sleepMs(300);
                continue;
            };
            return;
        }
    }

    /// Publish the result of recovery to the apply path: install the
    /// snapshot's `oldest_memtable_index` floor and flip the apply-loop
    /// barrier. Must be called exactly once, even on no-snapshot/error
    /// paths, so applies don't stall forever. The release-store pairs
    /// with `applyEntry`'s acquire-load so `oldest_memtable_index` is
    /// visible by the time apply reads it.
    pub fn markRecovered(self: *StorageImpl, oldest_memtable_index: u64) void {
        self.oldest_memtable_index = oldest_memtable_index;
        self.recovery_done.store(true, .release);
    }
};

/// Snapshot of work the compaction/flush loops are currently building.
/// Lives only between the moment we start writing SSTs / building changes
/// and the moment the resulting manifest entry is appended via
/// `proposeAndWaitForApply`. Surfaced through `zdb` so an operator
/// can tell whether a "compactions wanted" entry is genuinely starved or
/// just waiting on an in-flight build to finish.
pub const InFlight = struct {
    pub const Kind = enum { flush, compaction, split, new_memtable };
    /// A flush or a new memtable works on no one tree.
    pub const no_tree: TreeKey = .{ .namespace = 0, .tree_id = 0 };
    kind: Kind,
    tree: TreeKey = no_tree,
    src_level: u8 = 0,
    src_tables: u32 = 0,
    /// Compaction id (`cid`) for `kind == .compaction`; 0 otherwise. Lets
    /// the watchdog log line cross-reference the compaction's own line
    /// (`compact cid=N done ...`, and its phases at debug).
    cid: u64 = 0,
    started_at_ms: i64,
};

/// How far a propose blocks: `commit` = replicated to a quorum (durable +
/// ordered); `apply` = also applied to this node's state (locally readable).
pub const Wait = enum { commit, apply };

/// The glue binding `zdb` (the forest) and `raft` (the node) into one
/// replicated storage unit: writes flow through raft and apply into the forest,
/// reads come from the forest, and the background loops keep the LSM healthy via
/// replicated manifest changes. See `root.zig` for the full contract.
pub const Engine = struct {
    allocator: std.mem.Allocator,
    forest: *Forest,
    node: *raft.Node,
    /// The apply state machine (owned). Loops/handlers gate on
    /// `storage_impl.recovery_done` (and, for data mutations, on the forest
    /// also having a real memtable) so the SM is valid before any proposal.
    storage_impl: StorageImpl,
    /// The StateMachine view of `storage_impl`, installed as `node.state_machine`
    /// — must stay at a stable address, hence the heap-allocated Engine.
    sm: raft.Node.StateMachine,
    /// The raft event-loop runtime (owned).
    raft_rt: raft.RaftRuntime,
    snapshot_cfg: *flags.Live(SnapshotCfg),
    in_flight_mu: fiber.Mutex(.tracked) = .{},
    in_flight: std.ArrayList(InFlight) = .empty,
    /// Monotonic per-call ID for proposeCompaction so we can grep its
    /// per-phase log lines (start/build_done/distribute_done/done) out
    /// of a noisy log without picking up unrelated compactions. Wraps
    /// at u64 — we'll never wrap in practice.
    next_compaction_id: std.atomic.Value(u64) = std.atomic.Value(u64).init(1),
    /// Last term in which `catchUp` barriered on a caught-up leader. 0 =
    /// never; a leadership change bumps the term and re-arms the barrier.
    catchup_term: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Serializes `proposeFlush` and `proposeSplit` end-to-end (build → propose
    /// → wait-for-apply). Without this, a flush whose `buildFlush` ran before a
    /// split's `applySplit` routes every key to the pre-split tree (`ns_routes`
    /// only updates at apply time); when applyFlush later installs the L0 SST,
    /// keys ≥ split_key sit in the wrong tree's L0 and reads (which route to
    /// the new tree) miss them. Holding the lock guarantees the next acquirer
    /// reads post-apply state — fresh routes for a flush after a split, fresh
    /// L0 contents for a split after a flush. Compactions don't take this lock:
    /// they don't touch routes, and a split runs only when no job of its
    /// tree does (`Tree.reserveJob`).
    flush_split_mu: fiber.Mutex(.tracked) = .{},

    /// Background fibers: the raft runtime + compaction + snapshot loops, plus
    /// any front-end listeners the binary registers via `spawnListener`.
    /// Spawned by `start()`, joined by `wait()`.
    bg: fiber.Group(void),

    /// Construct the engine and do all the node/forest wiring: build the apply
    /// state machine and install it on the node, and create the raft runtime.
    /// Heap-allocated so `storage_impl`/`sm`/`raft_rt` keep stable addresses.
    pub fn open(
        allocator: std.mem.Allocator,
        node: *raft.Node,
        forest: *Forest,
        snapshot_cfg: *flags.Live(SnapshotCfg),
    ) !*Engine {
        const self = try allocator.create(Engine);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .forest = forest,
            .node = node,
            .storage_impl = .{
                .forest = forest,
                .allocator = allocator,
                .node = node,
                .snapshot_cfg = snapshot_cfg,
            },
            .sm = undefined,
            .raft_rt = raft.RaftRuntime.init(allocator, node),
            .snapshot_cfg = snapshot_cfg,
            .bg = fiber.Group(void).init(allocator),
        };
        self.sm = self.storage_impl.asStateMachine();
        self.node.state_machine = &self.sm;
        return self;
    }

    pub fn deinit(self: *Engine) void {
        self.in_flight_mu.lock(@src());
        self.in_flight.deinit(self.allocator);
        self.in_flight_mu.unlock();
        self.in_flight_mu.deinit();
        self.flush_split_mu.deinit();
        self.bg.deinit();
        self.raft_rt.deinit();
        self.allocator.destroy(self);
    }

    /// Recover from the snapshot, replay committed entries, then spawn the
    /// engine's background loops (raft runtime + compaction + snapshot) into
    /// `bg`. Non-blocking: the binary registers front-end listeners via
    /// `spawnListener` and blocks on `wait()`. `node.initTransport()` and any
    /// cluster join must run before this.
    pub fn start(self: *Engine) !void {
        // Sequenced startup before any loop runs: install the snapshot manifest
        // and synchronously apply every committed WAL entry, so the compaction
        // loop can't propose SST ids that replay is about to install.
        try self.recoverFromSnapshot();
        self.node.applyCommits();

        try self.bg.spawn(.{}, runRaft, .{&self.raft_rt});
        try self.bg.spawn(.{}, runCompactions, .{self});
        try self.bg.spawn(.{}, runSnapshot, .{self});
        // High priority, so a shrink for a burst does not wait behind
        // a busy worker's queue; high fibers are pinned, to worker 0.
        if (_governor.g() != null)
            try self.bg.spawn(
                .{ .priority = .high, .worker_id = 0 },
                runGovernor,
                .{self},
            );
    }

    /// Sizes the block cache from the rest of the process's memory
    /// (`governor.Governor.tick`), ten times a second: live bytes pass
    /// the total by at most what 100 ms allocates past the margin.
    fn runGovernor(self: *Engine) void {
        const gov = _governor.g().?;
        while (!self.node.isShuttingDown()) {
            gov.tick();
            if (self.node.shutdown.waitForStart(.shutdown, 100)) return;
        }
    }

    /// The raft runtime loop. Runs on the .raft lane directly (no re-spawn).
    fn runRaft(r: *raft.RaftRuntime) void {
        r.run() catch |err| {
            if (err == error.Canceled) return;
            log.err("raft runtime error: {s}", .{@errorName(err)});
            if (@errorReturnTrace()) |bt| {
                // Zig 0.16 split StackTrace: `@errorReturnTrace` returns
                // `builtin.StackTrace`; `dumpStackTrace` wants `debug.StackTrace`.
                const dbg_trace: std.debug.StackTrace = .{
                    .return_addresses = bt.instruction_addresses[0..bt.index],
                    .skipped = .none,
                };
                std.debug.dumpStackTrace(&dbg_trace);
            }
        };
    }

    /// Re-spawn the compaction loop off the .raft lane (onto .default) and join,
    /// so heavy compaction work never competes with raft on its dedicated lane.
    fn runCompactions(s: *Engine) void {
        var handle = fiber.spawn(.{}, Engine.runCompactionLoop, .{s}) catch |err| {
            log.err("runCompaction err: {}", .{err});
            return;
        };
        handle.join();
    }

    /// Re-spawn the snapshot loop off the .raft lane and join.
    fn runSnapshot(s: *Engine) void {
        var handle = fiber.spawn(.{}, Engine.runSnapshotLoop, .{s}) catch |err| {
            log.err("runSnapshot err: {}", .{err});
            return;
        };
        handle.join();
    }
    /// Register a front-end listener loop (admin, sql) into the engine's
    /// background group so it is cancelled and joined with the engine.
    pub fn spawnListener(self: *Engine, comptime f: anytype, args: anytype) !void {
        try self.bg.spawn(.{}, f, args);
    }

    /// Watcher fiber — one per front-end listener, spawned detached by each
    /// `runListener`. Blocks until the node's shutdown reaches `at` (the
    /// SQL listener stops taking connections at the drain, the admin one
    /// stays up to answer `traces` and `locks` until the stop), then
    /// wakes the listener's parked `accept()` by opening a throwaway connection
    /// to its own address. `accept` has no per-op cancellation in the fiber
    /// runtime, so a sibling fiber has to break it from the outside — and a
    /// real self-connection makes `accept()` return a live stream *cleanly*,
    /// after which the loop sees the stage and exits. We connect
    /// rather than `shutdown()` the listen socket because `shutdown()` wakes
    /// accept but completes it with EINVAL (Linux returns EINVAL for accept on
    /// a shut-down listener) — a noisy `unexpectedErrno` dump in debug builds
    /// plus a spurious "accept failed" on every shutdown.
    pub fn stopListenerOnStart(
        self: *Engine,
        listen_addr: fiber.net.IpAddress,
        at: fiber.Shutdown.Start,
    ) void {
        _ = self.node.shutdown.waitForStart(at, null);
        var stream = listen_addr.connect(.{}) catch |err| {
            log.warn("shutdown self-connect failed: {s}", .{@errorName(err)});
            return;
        };
        stream.close();
    }

    /// Block until every background fiber (loops + front-ends) has exited.
    pub fn wait(self: *Engine) void {
        self.bg.wait();
    }

    /// Reserve a tree against compaction/split for a long-running
    /// external build (CREATE INDEX's backfill): every level is marked
    /// busy, so the tree offers no job until releaseTree. Keeps the
    /// index tree's bottom level empty while the backfill runs — the
    /// condition its install depends on (Tree.applyBackfill).
    pub fn reserveTree(self: *Engine, key: TreeKey) !void {
        const tree = self.forest.getTree(key.namespace, key.tree_id) orelse
            return error.TreeNotFound;
        defer tree.release();
        try tree.reserveAll();
    }

    pub fn releaseTree(self: *Engine, key: TreeKey) void {
        const tree = self.forest.getTree(key.namespace, key.tree_id) orelse
            return;
        defer tree.release();
        tree.releaseAll() catch |err| {
            log.err("releaseTree tree={d}: {s}", .{
                key.tree_id, @errorName(err),
            });
        };
    }

    /// Add an in-flight record. Returns the slice index for `removeInFlight`.
    /// Caller is responsible for pairing exactly one `remove` per `add`,
    /// typically via `defer`.
    fn addInFlight(self: *Engine, entry: InFlight) void {
        self.in_flight_mu.lock(@src());
        defer self.in_flight_mu.unlock();
        self.in_flight.append(self.allocator, entry) catch {};
    }

    fn removeInFlight(
        self: *Engine,
        kind: InFlight.Kind,
        tree: TreeKey,
        src_level: u8,
    ) void {
        self.in_flight_mu.lock(@src());
        defer self.in_flight_mu.unlock();
        // Match on (kind, tree, src_level): a tree runs at most one job
        // per level (`Tree.reserveJob`).
        for (self.in_flight.items, 0..) |it, i| {
            if (it.kind == kind and std.meta.eql(it.tree, tree) and
                it.src_level == src_level)
            {
                _ = self.in_flight.orderedRemove(i);
                return;
            }
        }
    }

    pub fn runCompactionLoop(self: *Engine) void {
        // Block until we're leader and have proposed + applied a no-op
        // entry. That commits the prior-term WAL tail (Raft's prior-term
        // commitment rule), so by the time the compactor fibers spawn
        // and start fetchAdd-ing `next_file_id` for new SSTs, every
        // flush/compaction id in the log has already been observed by
        // the apply path. Without this, a compactor could claim an id
        // that an unapplied tail entry already used (PathAlreadyExists
        // when both try to create the same SST file).
        self.node.waitForQuorum() catch |err| {
            log.err("runCompactionLoop: waitForQuorum: {s}", .{@errorName(err)});
            return;
        };

        const n_compactors = self.forest.live.cur().num_compactors;
        log.info("compaction loops started: num_compactors={d}", .{n_compactors});

        // All compaction loops self-exit via isShuttingDown() — no collective
        // cancel needed; `wait()` is enough. Background fibers don't need
        // `.exclusive` placement: they're allocator-bound and I/O-bound with
        // ample sleep windows, so co-scheduling on shared threads is fine.
        var g = fiber.Group(void).init(self.allocator);
        defer g.deinit();
        g.ensureTotalCapacity(3 + n_compactors) catch |err| {
            log.err("runCompactionLoop: ensureTotalCapacity failed: {s}", .{@errorName(err)});
            return;
        };

        g.spawn(.{}, runMemtableLoop, .{self}) catch |err| {
            log.err("runCompactionLoop: spawn runMemtableLoop failed: {s}", .{@errorName(err)});
            return;
        };
        g.spawn(.{}, runFlushLoop, .{self}) catch |err| {
            log.err("runCompactionLoop: spawn runFlushLoop failed: {s}", .{@errorName(err)});
            return;
        };
        // Each levels-loop fiber runs one job at a time, taken from the
        // best tree (`Tree.reserveJob`); with N fibers, N jobs run at
        // once. A tree runs several only on disjoint levels, so an L0
        // merge proceeds while a long rewrite below holds the tree.
        for (0..n_compactors) |_| {
            g.spawn(.{}, runLevelsLoop, .{self}) catch |err| {
                log.err("runCompactionLoop: spawn runLevelsLoop failed: {s}", .{@errorName(err)});
                return;
            };
        }
        g.spawn(.{}, runInFlightWatchdog, .{self}) catch |err| {
            log.err("runCompactionLoop: spawn runInFlightWatchdog failed: {s}", .{@errorName(err)});
            return;
        };

        g.wait();
        log.info("compaction loops exiting (shutdown)", .{});
    }

    /// Diagnostic loop: every 15s, scan the in_flight table and warn
    /// about any entry older than the configured threshold. Catches
    /// stuck compactions, flushes, splits, and newMemtable proposals
    /// generically — wherever the wedge is, the entry stops aging out.
    fn runInFlightWatchdog(self: *Engine) void {
        const poll: u64 = 15000;
        const stuck_threshold_ms: i64 = 30_000;
        while (!self.node.isShuttingDown()) {
            if (self.node.shutdown.waitForStart(.shutdown, poll)) return;
            const now_ms = utils.coarseMs();
            self.in_flight_mu.lock(@src());
            defer self.in_flight_mu.unlock();

            for (self.in_flight.items) |it| {
                const age_ms = now_ms - it.started_at_ms;
                if (age_ms < stuck_threshold_ms) continue;
                log.warn("in-flight stuck: kind={s} cid={d} tree={d}:{d} " ++
                    "src_level={d} age_ms={d}", .{
                    @tagName(it.kind), it.cid,       it.tree.namespace,
                    it.tree.tree_id,   it.src_level, age_ms,
                });
            }
        }
    }

    /// Restore the manifest from `wal.snapshot().data` and bring the
    /// in-memory trees online, opening every SST it lists from the
    /// store. No-op when there is no snapshot on disk.
    ///
    /// The apply loop tolerates a brief window where this hasn't
    /// completed: `applyEntry` errors retry until the manifest is in
    /// place.
    pub fn recoverFromSnapshot(self: *Engine) !void {
        // Always publish a result — even on the no-snapshot fast path or
        // mid-recovery failure — so the apply gate doesn't wedge forever.
        var oldest_idx: u64 = 0;
        defer self.storage_impl.markRecovered(oldest_idx);

        const snap = self.node.log.snapshot();
        if (snap.index == 0 or snap.data.len == 0) return;

        log.info("recovery: restoring manifest from snapshot index={d} bytes={d}", .{ snap.index, snap.data.len });
        try self.forest.manifest.restoreFrom(snap.data);
        oldest_idx = snap.oldest_memtable_index;
        _ = self.forest.setSnapshotTs(snap.timestamp);

        try self.forest.openTablesFromManifest();
        log.info("recovery: snapshot manifest installed", .{});
    }

    /// Periodic background loop. Polls per `snapshot_cfg.poll_interval_seconds`
    /// and invokes `maybeProposeSnapshot`, which is a no-op until the WAL
    /// crosses the size or age threshold. Runs concurrently with `applyLoop` —
    /// DiskWAL/Forest are designed for concurrent reads.
    ///
    /// The loop also does file GC, on the leader only, like the proposal:
    /// `waitForQuorum` below holds a node here until it is leader, and
    /// every tick is skipped while it is not. Whenever the installed
    /// snapshot index has advanced since the last tick the leader unlinks
    /// the SSTs queued for deletion at or below it (`Forest.deleteFiles`),
    /// and every `orphan_scan_interval_seconds` it rescans the data
    /// directory for files the manifest does not know
    /// (`Forest.removeOrphans`); the first tick as leader runs a scan.
    ///
    /// Leader-only because the store is shared: one node must own
    /// deletion. A follower's `to_delete` records accumulate until it
    /// becomes leader.
    pub fn runSnapshotLoop(self: *Engine) void {
        // Same barrier as runCompactionLoop — snapshot proposal would
        // race the WAL-tail replay otherwise.
        self.node.waitForQuorum() catch |err| {
            log.err("runSnapshotLoop: waitForQuorum: {s}", .{@errorName(err)});
            return;
        };

        // The barrier above means replay has reached the commit index, so
        // the manifest now knows every file this node needs.
        var last_scan_ms: i64 = 0;

        // Sweep only when a snapshot has been installed since the last
        // look: a retire becomes deletable only by the snapshot index
        // passing it, so nothing changes in between.
        var swept_upto: u64 = 0;

        while (!self.node.isShuttingDown()) {
            const cfg = self.snapshot_cfg.cur();
            const poll_ms = cfg.poll_interval_seconds * 1000;
            if (self.node.shutdown.waitForStart(.shutdown, poll_ms)) return;
            if (!self.node.isLeader()) continue;

            const snap_index = self.node.log.snapshot().index;
            if (snap_index > swept_upto) {
                self.forest.deleteFiles(snap_index) catch |err| {
                    log.warn("deleteFiles: {s}", .{@errorName(err)});
                };
                swept_upto = snap_index;
            }

            const now_ms = utils.coarseMs();
            if (now_ms - last_scan_ms >= cfg.orphan_scan_interval_seconds * 1000) {
                last_scan_ms = now_ms;
                const min_age_secs = cfg.orphan_min_age_seconds;
                self.forest.removeOrphans(min_age_secs) catch |err| {
                    log.warn("removeOrphans: {s}", .{@errorName(err)});
                };
            }

            self.maybeProposeSnapshot() catch |err| {
                log.warn("maybeProposeSnapshot: {s}", .{@errorName(err)});
            };
        }
    }

    /// Leader-only. Pick a target snapshot index from WAL size/age (capped by
    /// the oldest in-memory memtable so replay can reconstruct memtable state),
    /// build snapshot bytes by replaying manifest entries onto a draft seeded
    /// from the previous snapshot, then propose the marshalled SnapshotStruct
    /// through Raft. Every node (including the leader) installs via
    /// `Node.applySnapshotEntry` when the proposal is committed and applied.
    ///
    /// === Replay-safety invariant ===
    ///
    /// After `setSnapshot(I) + compactUntil(I)`, log entries < I are gone.
    /// For recovery to reconstruct correct state, every committed mutation
    /// must have AT LEAST ONE surviving representation:
    ///    (a) a log entry at index >= I (preserved by `compactUntil`), OR
    ///    (b) an SST referenced either by `snap.data`'s manifest, or by a
    ///        `flush` proposal whose log index >= I (so replay installs it).
    ///
    /// The cap below — `min(oldest_in_memory - 1, apply_index)` — is the
    /// precise bound that makes (a) || (b) hold for every mutation:
    ///
    ///   - Mutations in still-in-memory memtables (no SST yet):
    ///       Mutations into memtable `Mt` are at log indices > Mt.created_at.
    ///       So if `Mt` is in-memory, `oldest_in_memory <= Mt.created_at`,
    ///       and capping `I <= oldest_in_memory - 1` keeps every mutation
    ///       index strictly above I → (a) holds.
    ///
    ///   - Mutations in already-flushed memtables (SST on disk):
    ///       The flush proposal at log index F either survives compaction
    ///       (F >= I) — replay reaches F and installs the SST → (b) — or
    ///       was already folded into `snap.data` by the build-replay pass
    ///       (F < I) → (b) via the manifest itself.
    ///
    /// === Worked example ===
    ///
    ///   100  newMemtable(M_0)        placeholder discarded; current=M_0
    ///   120  newMemtable(M_1)        M_0->immutable; current=M_1
    ///   125  flush(M_0)->SST_a       pops M_0
    ///   131,132   mutations into M_1 (in-memory only, no SST yet)
    ///   140  newMemtable(M_2)        M_1->immutable; current=M_2
    ///   160  flush(M_1)->SST_b       pops M_1; SST_b captures 121..139
    ///
    /// At leader head ~170: oldest = M_2.created_at = 140 → cap = 139.
    /// We propose snap.index = I = 135 (between the two flushes, valid).
    ///
    /// On follower recovery from this snapshot:
    ///   - `snap.data` lists SST_a (flush at 125 <= I) but not SST_b (160 > I).
    ///   - `compactUntil(135)` drops entries 121..134, including 131, 132.
    ///   - Log preserves entries 136..head (including flush at 160).
    ///   - Replay from 136..139 puts mutations into the id=0 placeholder.
    ///     (At this point, queries would not see them yet.)
    ///   - At 140, `swapCurrentOut` discards the placeholder. The id=0
    ///     branch frees its in-memory contents — fine, because:
    ///   - At 160, `applyFlush` installs SST_b. SST_b on the leader was
    ///     built from M_1's full history (121..139), so it captures the
    ///     "lost" mutations 131, 132 AND 136..139. Queries against those
    ///     keys hit SST_b. No data loss.
    ///
    /// Brief inconsistency window: between replay of 140 (placeholder
    /// dropped) and 160 (SST_b installed), keys mutated at 136..139 are
    /// momentarily unreachable on the follower. After 160, all good.
    /// This is normal Raft follower-catchup eventual consistency, not
    /// data loss.
    fn maybeProposeSnapshot(self: *Engine) !void {
        const oldest = self.forest.memtables.oldestInMemoryLogIndex();
        if (oldest == 0) return; // no real memtable yet — nothing pinned

        // See the replay-safety invariant above. Two caps, both required:
        //   1. `oldest - 1`: every in-memory memtable's mutation history
        //      stays in the log for replay reconstruction.
        //   2. `apply_index`: never snapshot past entries we haven't applied
        //      to the manifest yet — the draft replay would skip them, and
        //      after restart `apply_index = snap.index` would silently drop
        //      their effects from the state machine.
        const memtable_cap = oldest - 1;
        const apply_cap = self.node.apply_index.load(.acquire);

        const wal = self.node.log;
        const cfg = self.snapshot_cfg.cur();
        const found = wal.findIndexOlderThan(cfg.min_bytes, cfg.min_age_seconds) orelse return;
        const target = @min(@min(found, memtable_cap), apply_cap);

        const prev_snap = wal.snapshot();
        if (target <= prev_snap.index) return;

        // Asked of the log by index: the range read below returns
        // manifest entries only, and the target is rarely one.
        const target_slot = (try wal.slotAt(target)) orelse {
            log.warn("maybeProposeSnapshot: no slot for index={d}", .{target});
            return;
        };
        const target_term = target_slot.term;
        // `Snapshot.timestamp`: `Forest.snapshot_ts` once this applies.
        const target_ts = self.forest.minReadTs(
            target_slot.timestamp,
            cfg.max_txn_age_seconds,
            self.node.peersMinReadTs(),
        );

        // Seed draft from previous snapshot's manifest, then replay every
        // manifest-bearing log entry in (prev_snap.index, target].
        var draft = _manifest.Manifest.init(self.allocator);
        defer draft.deinit();
        if (prev_snap.data.len > 0) {
            try draft.restoreFrom(prev_snap.data);
        }

        // Only the manifest entries are read: the slot table knows each
        // entry's type, so the data entries between them, the bulk of
        // the log, are never touched.
        var result = try wal.entriesRange(.{
            .lo = prev_snap.index + 1,
            .hi = target + 1,
            .ty = .manifest,
            .max_bytes = std.math.maxInt(u64),
        });
        defer result.deinit();
        if (!result.eof) {
            log.warn("maybeProposeSnapshot: entries truncated by max_bytes — aborting", .{});
            return;
        }

        var scratch: std.ArrayList(u8) = .empty;
        defer scratch.deinit(self.allocator);
        for (result.entries) |stored| {
            const reader = raft.buffers.LogEntryReader.parseRoot(stored.bytes) catch |err| {
                log.err("maybeProposeSnapshot: parseRoot at index={d}: {s}", .{
                    stored.index, @errorName(err),
                });
                continue;
            };
            // An entry's data may be stored compressed.
            const payload = try utils.compression.maybeUnpack(
                self.allocator,
                reader.codec(),
                reader.data(),
                &scratch,
            );
            if (payload.len == 0) continue;

            const parsed = zdb_bufs.parseMessageWithTail(payload) catch |err| {
                log.err("maybeProposeSnapshot: parseMessageWithTail at " ++
                    "index={d}: {s}", .{ stored.index, @errorName(err) });
                continue;
            };
            switch (parsed) {
                .ManifestChange => |mc| {
                    const mcs = try mc.toStruct(self.allocator);
                    defer {
                        mcs.deinit(self.allocator);
                        self.allocator.destroy(mcs);
                    }
                    // apply, not commit: the draft must also pick up the
                    // change's name and counter effects. The WAL below the
                    // snapshot is gone after a restart, so a name missing
                    // from the snapshot is lost and its database or table
                    // becomes unreachable even though its rows are there.
                    try draft.apply(stored.index, mcs.*);
                },
                else => continue,
            }
        }

        const data = try draft.serialize(self.allocator);
        defer self.allocator.free(data);

        // Pick the latest conf state. It doesn't need to be as of the snapshot index.
        var conf_state = try self.node.confState();
        defer conf_state.deinit(self.allocator);

        try self.node.proposeSnapshot(.{
            .index = target,
            .term = target_term,
            .data = data,
            .conf_state = conf_state,
            // Tells recovering nodes to skip mutation replay below this
            // index — those mutations are captured in SSTs the replay
            // will install. See `applyEntryInner` and the worked example
            // above. (= `oldest` from above; recompute in case it changed
            // between the cap calc and now is unnecessary — `oldest` is
            // already pinned by the cap.)
            .oldest_memtable_index = oldest,
            .timestamp = target_ts,
        });
        log.info("snapshot proposed: index={d} term={d} bytes={d}", .{ target, target_term, data.len });
    }

    /// Single-purpose loop: when leader, propose a `newMemtable` whenever
    /// either (a) the placeholder is still current (id == 0) or (b) the
    /// active memtable has hit `table_bytes`. Bootstrap and steady-state
    /// freeze fall out of the same condition, so there's no separate
    /// startup phase.
    fn runMemtableLoop(self: *Engine) void {
        const interval: u64 = 100;
        while (!self.node.isShuttingDown()) {
            const needs = self.node.isLeader() and
                (self.forest.memtables.currentId() == 0 or self.forest.shouldFreeze());
            if (!needs) {
                if (self.node.shutdown.waitForStart(.shutdown, interval))
                    return;
                continue;
            }
            self.proposeNewMemtable() catch |err| {
                log.err("proposeNewMemtable failed: {s}", .{@errorName(err)});
                if (self.node.shutdown.waitForStart(.shutdown, interval))
                    return;
            };
        }
    }

    /// Leader-only. A flush covers every frozen memtable waiting at that
    /// moment (`Forest.buildFlush`), so holding off lets them accumulate:
    /// each tree gets one larger L0 table in place of several small ones.
    /// Flush when `flush_min_memtables` are waiting, or when
    /// `flush_max_age_seconds` have passed since the last flush and at
    /// least one is. The first flush after start never waits.
    fn runFlushLoop(self: *Engine) void {
        // Flushes and their uploads run as foreground: a stalled flush
        // stalls writes.
        fiber.ctx().class = .fg;
        const interval: u64 = 200;
        var last_flush_ms: ?i64 = null;
        while (!self.node.isShuttingDown()) {
            if (!self.node.isLeader() or !self.flushDue(last_flush_ms)) {
                if (self.node.shutdown.waitForStart(.shutdown, interval))
                    return;
                continue;
            }
            self.proposeFlush() catch |err| {
                log.err("proposeFlush failed: {s}", .{@errorName(err)});
            };
            last_flush_ms = utils.coarseMs();
        }
    }

    fn flushDue(self: *Engine, last_flush_ms: ?i64) bool {
        const opts = self.forest.live.cur();
        const waiting = self.forest.memtables.immutableCount();
        if (waiting == 0) return false;
        if (waiting >= opts.flush_min_memtables) return true;

        const last = last_flush_ms orelse return true;
        const max_age_ms: i64 = @as(i64, opts.flush_max_age_seconds) * 1000;
        return utils.coarseMs() - last >= max_age_ms;
    }

    // TODO: This work can be done by a follower instead of the leader. Leader
    // should figure out the candidates and hand them over to anyone who's caught up.
    // Given that we're only doing one compaction per tree, no one else is working on
    // that tree, so the follower's results would be valid. The leader would check
    // results before proposing, so we should be fine.
    // For now, leader is the one doing the compaction.
    //
    // Multiple instances of this loop run in parallel — one per
    // `num_compactors`. `Tree.reserveJob` hands each job out once and
    // marks its levels busy, so two fibers never run the same job or
    // overlapping levels of one tree.
    fn runLevelsLoop(self: *Engine) void {
        const interval: u64 = 500;
        while (!self.node.isShuttingDown()) {
            if (!self.node.isLeader()) {
                if (self.node.shutdown.waitForStart(.shutdown, interval))
                    return;
                continue;
            }
            const job = self.takeJob() orelse {
                if (self.node.shutdown.waitForStart(.shutdown, interval))
                    return;
                continue;
            };
            defer job.tree.release();
            defer job.tree.releaseJob(job.cand);
            // The candidate's job is a split when the tree has outgrown
            // `Config.splitBytes`, its Lmax target plus the levels above
            // it. updateCompactCandidate sets this in the apply path
            // (after every compaction/flush), so the dispatch here
            // always reflects the latest applied state — no separate
            // polling for splits is needed.
            if (job.cand.job == .split) {
                self.proposeSplit(job.key) catch |err| {
                    log.err("proposeSplit failed for tree {d}: {s}", .{
                        job.key.tree_id, @errorName(err),
                    });
                };
            } else {
                self.proposeCompaction(job) catch |err| {
                    // A job that outlives the proposal queue is redone
                    // after the restart; only a live node has failed.
                    if (self.node.isShuttingDown()) {
                        log.info("compaction result dropped at shutdown", .{});
                    } else {
                        log.err("proposeCompaction failed: {s}", .{
                            @errorName(err),
                        });
                    }
                };
            }
        }
    }

    /// A job taken from a tree, held with a reference to the tree until
    /// `Tree.releaseJob` and `release`.
    const TakenJob = struct {
        key: TreeKey,
        tree: *zdb.tree.Tree,
        cand: zdb.compaction.CompactCandidate,
    };

    /// The best tree's job, reserved. Null when no tree wants anything,
    /// or the best one was retired or satisfied between the ranking and
    /// the reservation.
    fn takeJob(self: *Engine) ?TakenJob {
        const key = self.forest.bestTreeToCompact() orelse return null;
        const tree = self.forest.getTree(key.namespace, key.tree_id) orelse
            return null;
        errdefer tree.release();

        const cand = tree.reserveJob() catch |err| {
            log.err("reserveJob tree={d}: {s}", .{
                key.tree_id, @errorName(err),
            });
            tree.release();
            return null;
        } orelse {
            tree.release();
            return null;
        };
        return .{ .key = key, .tree = tree, .cand = cand };
    }

    fn proposeNewMemtable(self: *Engine) !void {
        self.addInFlight(.{
            .kind = .new_memtable,
            .started_at_ms = utils.coarseMs(),
        });
        defer self.removeInFlight(.new_memtable, InFlight.no_tree, 0);
        // memtable_id stays 0 in the proposal: the id is assigned at apply
        // as the entry's own log index, identically on every node.
        const payload = try zdb_bufs.marshalRoot(self.allocator, _manifest.ManifestChange{ .cmd = .new_memtable, .memtable_id = 0 });
        defer self.allocator.free(payload);
        try self.node.proposeAndWaitForApply(payload, .manifest);
    }

    fn proposeFlush(self: *Engine) !void {
        // Block while a split is in-flight.
        self.flush_split_mu.lock(@src());
        defer self.flush_split_mu.unlock();

        // Track the flush BUILD as in-flight (writing SSTs is the slow
        // part). buildFlush iterates the oldest immutable memtable; we
        // don't know the target tree(s) until changes return, so the
        // record names no tree.
        self.addInFlight(.{
            .kind = .flush,
            .started_at_ms = utils.coarseMs(),
        });
        defer self.removeInFlight(.flush, InFlight.no_tree, 0);

        const result = try self.forest.buildFlush() orelse return;

        var changes = result.changes;
        defer changes.deinit(self.allocator);
        if (changes.items.len == 0) return;

        try self.announceNewFiles(changes.items);

        const payload = try encodeManifestChange(
            self.allocator,
            .flush,
            changes.items,
            &.{},
            result.newest_memtable_id,
        );
        defer self.allocator.free(payload);
        try self.node.proposeAndWaitForApply(payload, .manifest);
    }

    fn proposeCompaction(self: *Engine, job: TakenJob) !void {
        // Per-call ID so we can grep all phase-lines for one compaction
        // out of a noisy multi-loop log: every line below carries
        // `cid=<id>`. The `cid_` prefix is unique enough to tail+grep.
        // Assigned BEFORE addInFlight so the watchdog warning carries it.
        const cid = self.next_compaction_id.fetchAdd(1, .monotonic);

        // Track the compaction so `zdb` can show whether a
        // "compactions wanted" entry is genuinely starved or just waiting
        // on a build/sync that hasn't proposed yet.
        const tree = job.tree;
        self.addInFlight(.{
            .kind = .compaction,
            .tree = job.key,
            .src_level = job.cand.level,
            .cid = cid,
            .started_at_ms = utils.coarseMs(),
        });
        defer self.removeInFlight(.compaction, job.key, job.cand.level);

        const src = job.cand.level;
        const t0 = utils.coarseMs();
        log.debug("compact cid={d} start tree={d} L{d}", .{
            cid, job.key.tree_id, src,
        });

        // What this build may discard below: the read floor, the
        // installed snapshot's timestamp. Every node raised it when that
        // snapshot entry applied, before this compaction's entry.
        const snapshot_ts = self.forest.snapshotTs();
        var changes = try tree.buildCompaction(job.cand, snapshot_ts);
        defer changes.deinit(self.allocator);

        if (changes.items.len == 0) {
            // The job went stale between the pick and the build (an
            // install unlisted one of its tables). `releaseJob` refreshes
            // the candidate, so the next pick sees what the tree needs.
            log.debug("compact cid={d} build_done changes=0 (no-op)", .{cid});
            return;
        }
        const t1 = utils.coarseMs();
        log.debug("compact cid={d} build_done changes={d} build_ms={d}", .{
            cid, changes.items.len, t1 - t0,
        });

        // The build, merge and write, of merges only: a move renames one
        // file and would pull its level pair's times toward zero.
        const dest: ?u8 = switch (job.cand.job) {
            .merge_l0 => 0,
            .merge_down => src + 1,
            .move_down, .split => null,
        };
        if (dest) |d| metrics.g().compaction_ms.observe(
            .{ .src = src, .dest = d },
            @intCast(@max(t1 - t0, 0)),
        ) catch {};

        try self.announceNewFiles(changes.items);
        const t2 = utils.coarseMs();
        log.debug("compact cid={d} distribute_done distribute_ms={d}", .{
            cid, t2 - t1,
        });

        const payload = try encodeManifestChange(self.allocator, .compaction, changes.items, &.{}, 0);
        defer self.allocator.free(payload);

        try self.node.proposeAndWaitForApply(payload, .manifest);
        const t3 = utils.coarseMs();
        // The one line per compaction a release log keeps: every phase's
        // time is in it.
        log.info(
            "compact cid={d} done tree={d} L{d} changes={d} build_ms={d} " ++
                "distribute_ms={d} propose_ms={d} total_ms={d}",
            .{
                cid,     job.key.tree_id, src,     changes.items.len,
                t1 - t0, t2 - t1,         t3 - t2, t3 - t0,
            },
        );
    }

    fn proposeSplit(self: *Engine, key: TreeKey) !void {
        // Block while a flush is in-flight.
        self.flush_split_mu.lock(@src());
        defer self.flush_split_mu.unlock();

        self.addInFlight(.{
            .kind = .split,
            .tree = key,
            .started_at_ms = utils.coarseMs(),
        });
        defer self.removeInFlight(.split, key, 0);
        // buildSplit allocates sstable_changes / tree_changes on the arena;
        // encodeManifestChange copies the bytes into a self-owned payload,
        // so the arena can be torn down right after encode. No new SST
        // files are produced (split just remaps existing tables between
        // trees), so announceNewFiles is unnecessary here. tree_split routes
        // through applyManifestChange to applySplit on every node.
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const mc = try self.forest.buildSplit(
            arena.allocator(),
            key.namespace,
            key.tree_id,
        ) orelse {
            // No split key (Lmax has < 2 tables): the candidate that
            // sent us here was stale. `releaseJob` recomputes it.
            log.debug("proposeSplit tree={d} skipped: no split key", .{
                key.tree_id,
            });
            return;
        };

        // Diagnostic: dump the split's SST-change layout so we can correlate
        // the trigger size (logged by Tree.updateCompactCandidate) with what
        // the split actually proposes — table count, new tree id, etc.
        var n_create: usize = 0;
        var n_delete: usize = 0;
        for (mc.sstable_changes) |tc| switch (tc.op) {
            .create => n_create += 1,
            .delete => n_delete += 1,
            else => {},
        };
        log.info("proposeSplit tree={d} sstable_changes={d} (create={d} delete={d}) tree_changes={d}", .{
            key.tree_id, mc.sstable_changes.len, n_create, n_delete, mc.tree_changes.len,
        });

        const payload = try encodeManifestChange(
            self.allocator,
            mc.cmd,
            mc.sstable_changes,
            mc.tree_changes,
            0,
        );
        defer self.allocator.free(payload);
        try self.node.proposeAndWaitForApply(payload, .manifest);
    }

    /// Announce new SST files, BEFORE their manifest change is proposed:
    /// the builder put each `.create` in the store already; tell every
    /// peer the ids and wait until a quorum has opened them (batch acks),
    /// so the entry usually lands on followers whose table cache already
    /// holds the tables — apply never stalls. The apply-time open remains
    /// the backstop. Pub: SST creators outside the engine (sql index
    /// backfill) call it too.
    pub fn announceNewFiles(
        self: *Engine,
        changes: []const _manifest.SSTableChange,
    ) !void {
        var files: std.ArrayList(raft.buffers.FileRefStruct) = .empty;
        defer files.deinit(self.allocator);
        for (changes) |c| {
            if (c.op != .create) continue;
            try files.append(self.allocator, .{
                .id = c.sst_id,
                .size = c.size,
                .index_offset = c.index_offset,
            });
        }
        if (files.items.len == 0) return;
        try self.node.announceAndWaitForFileQuorum(files.items);
    }

    /// Write backpressure: sleep proportional to how many L0 tables the
    /// trees these writes land in currently have, the worst of them, or
    /// to how many frozen memtables wait for a flush, whichever is
    /// further past its start count. Bounds writes from out-running
    /// flush + compaction so L0 doesn't grow into a 10s-of-GiB
    /// monolithic compaction target. The L0 count is per tree, so a
    /// backlog behind one table's tail does not stall writes to
    /// anything else on the node. See `Options.backpressure_step_ms`
    /// for the curve. `kvs` are the batch's marshaled keys.
    pub fn applyWriteBackpressure(self: *Engine, kvs: []const zdb_bufs.KeyValueStruct) void {
        const opts = self.forest.live.cur();
        // Every key: a batch is in key order, a table's rows before
        // its index entries, so a prefix never reaches the index trees.
        var l0: u32 = 0;
        var keys_buf: [64]String = undefined;
        var rest = kvs;
        while (rest.len > 0) {
            const n = @min(rest.len, keys_buf.len);
            for (rest[0..n], keys_buf[0..n]) |kv, *k| k.* = kv.key;
            const count = self.forest.maxL0TableCountForKeys(keys_buf[0..n]);
            l0 = @max(l0, count);
            rest = rest[n..];
        }

        // Memtables waiting for a flush brake every write the same way;
        // one curve, so the larger excess over its start count decides.
        const frozen = self.forest.memtables.immutableCount();
        const excess = @max(
            l0 -| opts.l0_backpressure_start_count,
            frozen -| opts.memtable_backpressure_start_count,
        );
        if (excess == 0) return;

        const delay_ms = @min(
            @as(u64, excess) * opts.backpressure_step_ms,
            @as(u64, opts.backpressure_max_ms),
        );
        // The sleep itself is the cost; record it so a slow write path
        // can be told apart from a braked one.
        metrics.g().write_backpressure_ms.observe(delay_ms);
        fiber.sleepMs(delay_ms);
    }

    /// Append a section listing in-flight flush/compaction/split builds —
    /// work that's started writing SSTs or computing manifest changes but
    /// hasn't yet appended the resulting Raft entry. A `compactions wanted`
    /// row in the forest stats with no matching in-flight row means the
    /// compaction loop is starved (not picking up the candidate); a row
    /// that's been "in flight" for many seconds means a build is slow.
    pub fn writeInFlight(self: *Engine, w: *std.Io.Writer) !void {
        self.in_flight_mu.lock(@src());
        defer self.in_flight_mu.unlock();
        if (self.in_flight.items.len == 0) {
            try w.print("\n  in-flight: (none)\n", .{});
            return;
        }
        const now = utils.coarseMs();
        try w.print("\n  in-flight:\n", .{});
        for (self.in_flight.items) |it| {
            const age_ms = now - it.started_at_ms;
            const ns = it.tree.namespace;
            const id = it.tree.tree_id;
            switch (it.kind) {
                .compaction => try w.print(
                    "    compaction tree={d}:{d} L{d} age_ms={d}\n",
                    .{ ns, id, it.src_level, age_ms },
                ),
                .flush => try w.print("    flush age_ms={d}\n", .{age_ms}),
                .split => try w.print(
                    "    split tree={d}:{d} age_ms={d}\n",
                    .{ ns, id, age_ms },
                ),
                .new_memtable => try w.print("    new_memtable age_ms={d}\n", .{age_ms}),
            }
        }
    }

    /// Ensure this node is a leader that has APPLIED every entry committed
    /// before now, erroring NotLeader instead of waiting for leadership.
    ///
    /// NOT a serializer — competing DDLs are serialized by sql.Server's
    /// ddl_mu (held across check → propose → apply). This barrier fixes the
    /// one bug that funnel can't see: a freshly-elected leader reading
    /// STALE applied state. A crashed node restarts with a lagging
    /// persisted commit_index, so recovery applies only a prefix; the tail
    /// re-commits only after election, racing the first DDL's name check
    /// and id allocation. waitForQuorum proposes an empty entry and waits
    /// for its apply — applies are ordered, so every surviving prior-term
    /// entry (necessarily already in this leader's log) applies first.
    /// After that no foreign entry can enter this term's log, so once per
    /// term suffices (cached; concurrent callers may double-barrier,
    /// harmless).
    pub fn catchUp(self: *Engine) !void {
        if (!self.node.isLeader()) return error.NotLeader;
        // Read the term BEFORE the barrier: if leadership changes mid-wait,
        // we cache the old term and the next DDL re-barriers — safe side.
        const term = self.node.current_term;
        if (self.catchup_term.load(.acquire) == term) return;
        try self.node.waitForQuorum();
        self.catchup_term.store(term, .release);
    }

    /// Register a front-end's commit-stream hook (see ApplyObserver). Call once
    /// at startup, before eng.start(), so no applied entry is missed.
    pub fn setApplyObserver(self: *Engine, obs: ApplyObserver) void {
        self.storage_impl.apply_observer = obs;
    }

    /// Propose an already-encoded payload through Raft and block for `wait`
    /// (commit = quorum; apply = also applied here). The one propose+wait path
    /// the encode helpers below funnel through.
    pub fn proposeAndWait(self: *Engine, payload: String, proposal_type: raft.Node.ProposalType, wait_for: Wait) !void {
        switch (wait_for) {
            .apply => try self.node.proposeAndWaitForApply(payload, proposal_type),
            .commit => {
                var w: raft.ProposalWaiter = .{};
                try self.node.propose(payload, proposal_type, &w);
                switch (w.wait()) {
                    .committed => {},
                    .not_leader => return error.NotLeader,
                    else => return error.NotCommitted,
                }
            },
        }
    }

    /// Encode a forest manifest change (ns/tree create or drop, …) and propose
    /// it. Pairs with the forest `build*` constructors: the caller builds the
    /// change (leader side), this makes it real everywhere. Control plane uses
    /// `.apply` — the change must be locally visible when the caller returns.
    pub fn proposeMC(self: *Engine, mc: _manifest.ManifestChange, wait_for: Wait) !void {
        // The whole change: a branch names its source in `branch_from`.
        const payload = try zdb_bufs.marshalRoot(self.allocator, mc);
        defer self.allocator.free(payload);
        try self.proposeAndWait(payload, .manifest, wait_for);
    }

    /// Encode key-value writes and propose them as one .data entry — one
    /// raft entry, one commit HLC, atomic across every key/table the keys
    /// touch. Each kv's key is a FULL marshaled zdb key (Key.marshal);
    /// its wire timestamp is ignored and rewritten to the commit HLC at
    /// apply, and its namespace must match `namespace` (the batch
    /// authority apply validates against). Callers pick `wait_for`:
    /// `.apply` when the writes must be locally readable on return
    /// (catalog rows), `.commit` when durable + ordered is enough.
    pub fn proposeWrites(self: *Engine, namespace: u64, kvs: []zdb_bufs.KeyValueStruct, wait_for: Wait) !void {
        // Every data proposal enters here: row commits, catalog rows,
        // index builds, the admin set and set_batch. The brake is applied
        // once, for all of them.
        self.applyWriteBackpressure(kvs);
        const payload = try encodeMutations(self.allocator, namespace, kvs);
        defer self.allocator.free(payload);
        fiber.ctx().record("mutations encoded, {d} bytes", .{payload.len});
        try self.proposeAndWait(payload, .data, wait_for);
    }

    /// Encode key-value writes as one `Mutations` payload for a .data
    /// proposal. `kvs` is borrowed — marshalRoot copies the bytes out.
    pub fn encodeMutations(allocator: std.mem.Allocator, namespace: u64, kvs: []zdb_bufs.KeyValueStruct) ![]u8 {
        return zdb_bufs.marshalRoot(allocator, zdb_bufs.MutationsStruct{
            .namespace = namespace,
            .kvs = kvs,
        });
    }
};

/// Encode a ManifestChange into a zbuf-serialized payload to propose.
fn encodeManifestChange(
    allocator: std.mem.Allocator,
    cmd: _manifest.Command,
    sstable_changes: []_manifest.SSTableChange,
    tree_changes: []_manifest.TreeChange,
    memtable_id: u64,
) ![]u8 {
    // marshalRoot serializes every field of the nested SSTableChange /
    // TreeChange (namespace_id, checksum, size, …); a hand-written writer
    // that copies only a subset would silently drop the new id-model fields.
    return zdb_bufs.marshalRoot(allocator, _manifest.ManifestChange{
        .cmd = cmd,
        .sstable_changes = sstable_changes,
        .tree_changes = tree_changes,
        .memtable_id = memtable_id,
    });
}

// --- background loop shims (spawned by start) -------------------------------
