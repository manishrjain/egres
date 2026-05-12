const std = @import("std");
const zio = @import("zio");
const raft = @import("raft");
const zdb = @import("zdb");
const runtime = @import("runtime");
const proto = @import("protocol.zig");
const m = @import("metrics");
const flags = @import("flags");
const utils = @import("raft").utils;
const WaitGroup = utils.WaitGroup;

/// `--snapshot` — Raft snapshot triggers and polling cadence. The leader's
/// snapshot loop reads these every iteration so operators can tune at
/// runtime via the live flags channel.
pub const SnapshotCfg = struct {
    min_bytes: u64 = 1 << 30, // 1 GiB
    min_age_seconds: u64 = 10 * 60, // 10 minutes
    poll_interval_seconds: u64 = 30,

    pub const docs = .{
        .self = "Raft snapshot triggers. Leader proposes a snapshot when WAL tail bytes from head reach min_bytes OR an entry crosses min_age_seconds.",
        .min_bytes = "Snapshot triggers once WAL tail-bytes-from-head reach this. Default 1 GiB.",
        .min_age_seconds = "Snapshot triggers when an entry is at least this many seconds older than wall clock. Default 600 (10 min).",
        .poll_interval_seconds = "How often the snapshot loop checks the criteria. Default 30s.",
    };
};

const zdb_bufs = zdb.buffers;
const manifest = zdb.manifest;
const Forest = zdb.forest.Forest;
const Item = zdb.key.Item;
const Key = zdb.key.Key;
const vfs = zdb.vfs;
const metrics = zdb.metrics;
const log = std.log.scoped(.zdb_server);

pub const StorageImpl = struct {
    forest: *Forest,
    allocator: std.mem.Allocator,
    node: *raft.Node,
    /// Gate the apply path until recovery finishes. Until then trees, route
    /// tables, and SST files referenced by the snapshot may not be in
    /// place, and apply would error-retry-spin. Flipped (along with
    /// `oldest_memtable_index`) by `markRecovered`, which the ZdbServer's
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
            .writeFileFn = &writeFile,
            .readFileFn = &readFile,
            .hasFileFn = &hasFile,
            .preAppendFn = &preAppend,
            .isReadyFn = &isReady,
        };
    }

    /// Cheap check: SM is ready for a data proposal once `recoverFromSnapshot`
    /// has finished AND a real memtable (id != 0) is current. The leader
    /// polls this in `Node.proposeDataChange`; without the memtable gate, a
    /// write that races startup would land in the placeholder and be dropped
    /// by the first `swapCurrentOut`.
    fn isReady(ctx: *anyopaque) bool {
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));
        return self.recovery_done.load(.acquire) and self.forest.memtables.currentId() != 0;
    }

    fn applyEntry(ctx: *anyopaque, entry: raft.Entry, data: []const u8) anyerror!void {
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

    fn applyEntryInner(self: *StorageImpl, entry: raft.Entry, data: []const u8) !void {
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
                    const val = kv.value();
                    items.appendAssumeCapacity(.{
                        .key = .{
                            .namespace = ns,
                            .primary = kv.key(),
                            // Timestamp comes from the raft Entry (one per batch).
                            .timestamp = entry.timestamp,
                        },
                        .value = val,
                    });
                    total_bytes += val.len;
                }

                // Write this to Forest.
                try self.forest.put(items.items);

                try metrics.g().bytes_written.incrBy(.{
                    .source = "user",
                    .namespace_id = ns,
                }, total_bytes);
            },
            .ManifestChange => |mc| {
                try self.applyManifestChange(entry.index, mc);
            },
            else => {
                log.err("on_apply: unhandled message type at index={d}", .{entry.index});
            },
        }
    }

    fn applyManifestChange(self: *StorageImpl, index: u64, mc: zdb_bufs.ManifestChangeReader) !void {
        const mcs = try mc.toStruct(self.allocator);
        defer {
            mcs.deinit(self.allocator);
            self.allocator.destroy(mcs);
        }

        // Advance file ID counter so a follower that becomes leader
        // won't reuse IDs assigned by the old leader.
        self.forest.advanceFileId(mcs.memtable_id);
        for (mcs.table_changes) |tc| {
            self.forest.advanceFileId(tc.table_id);
        }

        switch (mcs.cmd) {
            1 => try self.forest.newMemtable(mcs.memtable_id, index),
            2 => try self.forest.applyFlush(mcs.table_changes, mcs.memtable_id),
            3 => {
                if (mcs.table_changes.len > 0) {
                    const tree_id = mcs.table_changes[0].tree_id;
                    if (self.forest.trees.get(tree_id)) |tree| {
                        try tree.applyCompaction(mcs.table_changes);
                    }
                }
            },
            4 => try self.forest.applySplit(mcs.*),
            5 => try self.forest.applyNamespaceCreate(mcs.*),
            else => log.err("unknown ManifestChange cmd={d}", .{mcs.cmd}),
        }
    }

    fn writeFile(ctx: *anyopaque, file_id: u64, data: []const u8) void {
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));
        self.forest.writeFile(file_id, data);
    }

    fn readFile(ctx: *anyopaque, allocator: std.mem.Allocator, file_id: u64) ?[]const u8 {
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));
        return self.forest.readFile(allocator, file_id);
    }

    fn hasFile(ctx: *anyopaque, file_id: u64) bool {
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));
        return self.forest.hasFile(file_id);
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

    /// Ensure all SST files referenced by ManifestChange entries are on disk
    /// before the WAL append. Fetches missing files from the leader.
    fn preAppend(ctx: *anyopaque, node: *raft.Node, entries: []const raft.buffers.LogEntryBuffer) void {
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));

        // Collect missing file IDs across all entries.
        var missing = std.ArrayList(u64).empty;
        defer missing.deinit(self.allocator);

        for (entries) |entry| {
            const reader = entry.initReader() catch continue;
            if (reader.entryType() != .manifest) continue;
            const data = reader.data();
            if (data.len == 0) continue;

            const mc = zdb_bufs.ManifestChangeReader.parseRoot(data) catch continue;
            const tc_len = mc.tableChangesLen();
            for (0..tc_len) |i| {
                const tc = mc.tableChangesAt(@intCast(i)) catch continue;
                if (tc.op() == .create and !self.forest.hasFile(tc.tableId())) {
                    missing.append(self.allocator, tc.tableId()) catch continue;
                }
            }
        }

        if (missing.items.len == 0) return;

        log.info("preAppend: fetching {d} missing files from leader", .{missing.items.len});
        while (true) {
            node.fetchFiles(missing.items) catch |err| {
                log.err("preAppend: fetchFiles failed: {s}, retrying", .{@errorName(err)});
                zio.sleep(zio.Duration.fromMilliseconds(100)) catch return;
                continue;
            };
            break;
        }
        log.info("preAppend: received {d} files from leader", .{missing.items.len});
    }
};

/// Snapshot of work the compaction/flush loops are currently building.
/// Lives only between the moment we start writing SSTs / building changes
/// and the moment the resulting manifest entry is appended via
/// `proposeAndWaitForApply`. Surfaced through `zdb forest` so an operator
/// can tell whether a "compactions wanted" entry is genuinely starved or
/// just waiting on an in-flight build to finish.
pub const InFlight = struct {
    pub const Kind = enum { flush, compaction, split, new_memtable };
    kind: Kind,
    tree_id: u64,
    src_level: u8 = 0,
    dest_level: u8 = 0,
    src_tables: u32 = 0,
    started_at_us: i64,
};

pub const ZdbServer = struct {
    allocator: std.mem.Allocator,
    forest: *Forest,
    node: *raft.Node,
    /// Shared with the apply path. Loops/handlers gate on
    /// `storage.recovery_done` (and, for data mutations, on the forest also
    /// having a real memtable) so the SM is in a valid shape before any
    /// proposal goes through Raft.
    storage: *StorageImpl,
    snapshot_cfg: *flags.Live(SnapshotCfg),
    listen_addr: zio.net.IpAddress,
    in_flight_mu: std.Thread.Mutex = .{},
    in_flight: std.ArrayList(InFlight) = .empty,
    /// Monotonic per-call ID for proposeCompaction so we can grep its
    /// per-phase log lines (start/build_done/distribute_done/done) out
    /// of a noisy log without picking up unrelated compactions. Wraps
    /// at u64 — we'll never wrap in practice.
    next_compaction_id: std.atomic.Value(u64) = std.atomic.Value(u64).init(1),

    /// Set of trees currently being compacted/split by some compactor
    /// fiber. Each fiber reserves its tree before starting work so
    /// two fibers never pick the same tree. Allocated lazily on init;
    /// the deinit code below frees it.
    compacting_mu: std.Thread.Mutex = .{},
    compacting_trees: std.AutoHashMap(u64, void) = undefined,

    pub fn init(
        allocator: std.mem.Allocator,
        forest: *Forest,
        node: *raft.Node,
        storage: *StorageImpl,
        snapshot_cfg: *flags.Live(SnapshotCfg),
        client_addr: []const u8,
    ) !ZdbServer {
        const addr = try zio.net.IpAddress.parseIpAndPort(client_addr);
        return .{
            .allocator = allocator,
            .forest = forest,
            .node = node,
            .storage = storage,
            .snapshot_cfg = snapshot_cfg,
            .listen_addr = addr,
            .compacting_trees = std.AutoHashMap(u64, void).init(allocator),
        };
    }

    pub fn deinit(self: *ZdbServer) void {
        self.in_flight_mu.lock();
        self.in_flight.deinit(self.allocator);
        self.in_flight_mu.unlock();
        self.compacting_mu.lock();
        self.compacting_trees.deinit();
        self.compacting_mu.unlock();
    }

    /// Try to claim a compaction target; returns null if every eligible
    /// tree is already being compacted by another fiber. The reservation
    /// stays in place until `releaseCompactingTree` runs (typically via
    /// `defer` in the caller). Race-safe because the candidate selection
    /// and the reservation insert happen under the same mutex.
    fn pickAndReserveCompactionTarget(self: *ZdbServer) ?Forest.CompactionTarget {
        self.compacting_mu.lock();
        defer self.compacting_mu.unlock();
        const target = self.forest.bestCompactionCandidateExcluding(&self.compacting_trees) orelse return null;
        self.compacting_trees.put(target.tree_id, {}) catch return null;
        return target;
    }

    fn releaseCompactingTree(self: *ZdbServer, tree_id: u64) void {
        self.compacting_mu.lock();
        defer self.compacting_mu.unlock();
        _ = self.compacting_trees.remove(tree_id);
    }

    /// Add an in-flight record. Returns the slice index for `removeInFlight`.
    /// Caller is responsible for pairing exactly one `remove` per `add`,
    /// typically via `defer`.
    fn addInFlight(self: *ZdbServer, entry: InFlight) void {
        self.in_flight_mu.lock();
        defer self.in_flight_mu.unlock();
        self.in_flight.append(self.allocator, entry) catch {};
    }

    fn removeInFlight(self: *ZdbServer, kind: InFlight.Kind, tree_id: u64, src_level: u8) void {
        self.in_flight_mu.lock();
        defer self.in_flight_mu.unlock();
        // Match on (kind, tree_id, src_level) — multiple in-flight entries
        // with the same key would mean two concurrent compactions on the
        // same level of the same tree, which we don't currently spawn.
        for (self.in_flight.items, 0..) |it, i| {
            if (it.kind == kind and it.tree_id == tree_id and it.src_level == src_level) {
                _ = self.in_flight.orderedRemove(i);
                return;
            }
        }
    }

    pub fn runClientLoop(self: *ZdbServer) void {
        log.info("inside runClientLoop", .{});
        defer log.info("client loop exiting (shutdown)", .{});

        const listener = self.listen_addr.listen(.{ .reuse_address = true }) catch |err| {
            log.err("failed to listen on client addr: {}", .{err});
            return;
        };
        defer listener.close();

        log.info("client TCP listener started, accepting connections", .{});

        var group = zio.group();
        defer group.cancel();

        var conn_id: u64 = 0;
        while (!self.node.isShuttingDown()) {
            const stream = listener.accept() catch |err| {
                log.err("runClientLoop: accept failed: {}", .{err});
                return;
            };
            stream.socket.setNoDelay(true) catch {};
            conn_id += 1;
            log.info("accepted client connection #{d}", .{conn_id});
            group.spawn(handleClient, .{ self, stream }) catch {
                log.err("failed to spawn handler for connection #{d}", .{conn_id});
                stream.close();
            };
        }
    }

    pub fn runCompactionLoop(self: *ZdbServer) void {
        const n_compactors = self.forest.opts.num_compactors;
        log.info("compaction loops started: num_compactors={d}", .{n_compactors});
        var group = zio.group();
        group.spawn(runMemtableLoop, .{self}) catch return;
        group.spawn(runFlushLoop, .{self}) catch return;
        // Each levels-loop fiber claims one tree at a time via
        // `pickAndReserveCompactionTarget`; with N fibers we get N
        // tree-parallel compactions. Single-tree workloads degenerate
        // to one effective compactor (others spin idle), which is
        // exactly the right behavior — multiple compactors on one
        // tree would race on level mutexes.
        for (0..n_compactors) |_| {
            group.spawn(runLevelsLoop, .{self}) catch return;
        }
        group.spawn(runInFlightWatchdog, .{self}) catch return;
        group.wait() catch {};
        log.info("compaction loops exiting (shutdown)", .{});
    }

    /// Diagnostic loop: every 15s, scan the in_flight table and warn
    /// about any entry older than the configured threshold. Catches
    /// stuck compactions, flushes, splits, and newMemtable proposals
    /// generically — wherever the wedge is, the entry stops aging out.
    fn runInFlightWatchdog(self: *ZdbServer) void {
        const poll = zio.Duration.fromSeconds(15);
        const stuck_threshold_ms: i64 = 30_000;
        while (!self.node.isShuttingDown()) {
            zio.sleep(poll) catch return;
            const now_us = std.time.microTimestamp();
            self.in_flight_mu.lock();
            defer self.in_flight_mu.unlock();
            for (self.in_flight.items) |it| {
                const age_ms = @divTrunc(now_us - it.started_at_us, 1000);
                if (age_ms < stuck_threshold_ms) continue;
                log.warn("in-flight stuck: kind={s} tree={d} src_level={d} dest_level={d} age_ms={d}", .{
                    @tagName(it.kind), it.tree_id, it.src_level, it.dest_level, age_ms,
                });
            }
        }
    }

    /// Block until `recoverFromSnapshot` has finished. Background loops
    /// gate on this so they don't try to propose against a half-installed
    /// state machine — applies would just error with `RecoveryInProgress`
    /// and the loop would spin in raft's retry path.
    fn waitForRecovery(self: *ZdbServer) void {
        const interval = zio.Duration.fromMilliseconds(50);
        while (!self.node.isShuttingDown()) {
            if (self.storage.recovery_done.load(.acquire)) return;
            zio.sleep(interval) catch return;
        }
    }

    /// Restore the manifest from `wal.snapshot().data` and bring the in-memory
    /// trees online. If any SST files referenced by the manifest are missing
    /// from disk, fetch them from the leader (retry until success or shutdown).
    /// No-op when there is no snapshot on disk.
    ///
    /// Must run AFTER `node.initTransport()` so file fetches can use the
    /// transport, and AFTER raft has started so a leader is reachable
    /// (followers learn `leader_id` via AppendEntries; nodes that joined via
    /// seeds learn it via the join response). The apply loop tolerates a brief
    /// window where this hasn't completed: `applyEntry` errors retry until
    /// the manifest is in place.
    pub fn recoverFromSnapshot(self: *ZdbServer) !void {
        // Always publish a result — even on the no-snapshot fast path or
        // mid-recovery failure — so the apply gate doesn't wedge forever.
        var oldest_idx: u64 = 0;
        defer self.storage.markRecovered(oldest_idx);

        const snap = self.node.log.snapshot();
        if (snap.index == 0 or snap.data.len == 0) return;

        log.info("recovery: restoring manifest from snapshot index={d} bytes={d}", .{ snap.index, snap.data.len });
        try self.forest.ms.restoreFrom(snap.data);
        oldest_idx = snap.oldest_memtable_index;

        // Fetch any missing SSTs from the leader. Retry until empty or
        // shutdown — fetchFiles errors when leader_id is unknown, which
        // resolves once raft elects/learns one.
        while (!self.node.isShuttingDown()) {
            const missing = try self.forest.collectMissingFileIds(self.allocator);
            defer self.allocator.free(missing);
            if (missing.len == 0) break;
            log.info("recovery: fetching {d} missing SST files from leader", .{missing.len});
            self.node.fetchFiles(missing) catch |err| {
                log.warn("recovery: fetchFiles failed ({s}); retrying in 200 ms", .{@errorName(err)});
                zio.sleep(zio.Duration.fromMilliseconds(200)) catch return;
                continue;
            };
        }

        try self.forest.openTablesFromManifest();
        log.info("recovery: snapshot manifest installed", .{});
    }

    /// Periodic background loop. Polls per `snapshot_cfg.poll_interval_seconds`
    /// and invokes `maybeProposeSnapshot`, which is a no-op until the WAL
    /// crosses the size or age threshold. Runs concurrently with `applyLoop` —
    /// DiskWAL/Forest are designed for concurrent reads.
    pub fn runSnapshotLoop(self: *ZdbServer) void {
        self.waitForRecovery();
        while (!self.node.isShuttingDown()) {
            const cfg = self.snapshot_cfg.snapshot();
            zio.sleep(zio.Duration.fromSeconds(cfg.poll_interval_seconds)) catch return;
            if (!self.node.isLeader()) continue;
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
    fn maybeProposeSnapshot(self: *ZdbServer) !void {
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
        const cfg = self.snapshot_cfg.snapshot();
        const found = wal.findIndexOlderThan(cfg.min_bytes, cfg.min_age_seconds) orelse return;
        const target = @min(@min(found, memtable_cap), apply_cap);

        const prev_snap = wal.snapshot();
        if (target <= prev_snap.index) return;

        const target_term = (try wal.term(target)) orelse {
            log.warn("maybeProposeSnapshot: no term for index={d}", .{target});
            return;
        };

        // Seed draft from previous snapshot's manifest, then replay every
        // manifest-bearing log entry in (prev_snap.index, target].
        var draft = manifest.ForestState.init(self.allocator);
        defer draft.deinit();
        if (prev_snap.data.len > 0) {
            try draft.restoreFrom(prev_snap.data);
        }

        // TODO: fix the bulk-read path in diskwal.zig:entriesRange so that
        // type-filtered queries (.ty != .any) correctly slice each matching
        // entry out of the shared bulk buffer when there are gaps from
        // skipped non-matching entries. Currently passing .ty=.manifest
        // here causes ~10-20% of returned entries to fail `parseRoot` with
        // OutOfBounds — likely a `disk_first_offset` math mismatch. As a
        // workaround we ask for ALL entries in the range and filter by
        // `reader.entryType()` in user space; correct, just heavier I/O.
        var result = try wal.entriesRange(.{
            .lo = prev_snap.index + 1,
            .hi = target + 1,
            .ty = .any,
            .max_bytes = std.math.maxInt(u64),
        });
        defer result.deinit();
        if (!result.eof) {
            log.warn("maybeProposeSnapshot: entries truncated by max_bytes — aborting", .{});
            return;
        }

        for (result.entries) |stored| {
            const reader = raft.buffers.LogEntryReader.parseRoot(stored.bytes) catch |err| {
                log.warn("maybeProposeSnapshot: parseRoot at index={d}: {s}", .{ stored.index, @errorName(err) });
                continue;
            };
            if (reader.entryType() != .manifest) continue;
            const payload = reader.data();
            if (payload.len == 0) continue;
            const parsed = zdb_bufs.parseMessageWithTail(payload) catch |err| {
                log.warn("maybeProposeSnapshot: parseMessageWithTail at index={d}: {s}", .{ stored.index, @errorName(err) });
                continue;
            };
            switch (parsed) {
                .ManifestChange => |mc| {
                    const mcs = try mc.toStruct(self.allocator);
                    defer {
                        mcs.deinit(self.allocator);
                        self.allocator.destroy(mcs);
                    }
                    try draft.commit(mcs.table_changes, mcs.tree_changes);
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
        });
        log.info("snapshot proposed: index={d} term={d} bytes={d}", .{ target, target_term, data.len });
    }

    /// Single-purpose loop: when leader, propose a `newMemtable` whenever
    /// either (a) the placeholder is still current (id == 0) or (b) the
    /// active memtable has hit `memtable_size`. Bootstrap and steady-state
    /// freeze fall out of the same condition, so there's no separate
    /// startup phase.
    fn runMemtableLoop(self: *ZdbServer) void {
        self.waitForRecovery();

        const interval = zio.Duration.fromMilliseconds(100);
        while (!self.node.isShuttingDown()) {
            const needs = self.node.isLeader() and
                (self.forest.memtables.currentId() == 0 or self.forest.shouldFreeze());
            if (!needs) {
                zio.sleep(interval) catch return;
                continue;
            }
            self.proposeNewMemtable() catch |err| {
                log.err("proposeNewMemtable failed: {s}", .{@errorName(err)});
                zio.sleep(interval) catch return;
            };
        }
    }

    fn runFlushLoop(self: *ZdbServer) void {
        self.waitForRecovery();

        const interval = zio.Duration.fromMilliseconds(200);
        while (!self.node.isShuttingDown()) {
            if (!self.node.isLeader() or !self.forest.hasFrozenMemtables()) {
                zio.sleep(interval) catch return;
                continue;
            }
            self.proposeFlush() catch |err| {
                log.err("proposeFlush failed: {s}", .{@errorName(err)});
            };
        }
    }

    // TODO: This work can be done by a follower instead of the leader. Leader
    // should figure out the candidates and hand them over to anyone who's caught up.
    // Given that we're only doing one compaction per tree, no one else is working on
    // that tree, so the follower's results would be valid. The leader would check
    // results before proposing, so we should be fine.
    // For now, leader is the one doing the compaction.
    //
    // Multiple instances of this loop run in parallel — one per
    // `num_compactors`. `pickAndReserveCompactionTarget` ensures no
    // two fibers ever pick the same tree concurrently (each tree
    // gets at most one in-flight compaction/split).
    fn runLevelsLoop(self: *ZdbServer) void {
        self.waitForRecovery();

        const interval = zio.Duration.fromMilliseconds(500);
        while (!self.node.isShuttingDown()) {
            if (!self.node.isLeader()) {
                zio.sleep(interval) catch return;
                continue;
            }
            const target = self.pickAndReserveCompactionTarget() orelse {
                zio.sleep(interval) catch return;
                continue;
            };
            defer self.releaseCompactingTree(target.tree_id);
            // The candidate carries `needs_split=true` when the tree's
            // Lmax has outgrown the split threshold. updateCompactCandidate
            // sets this in the apply path (after every compaction/flush),
            // so the dispatch here always reflects the latest applied
            // state — no separate polling for splits is needed.
            if (target.candidate.needs_split) {
                self.proposeSplit(target.tree_id) catch |err| {
                    log.err("proposeSplit failed for tree {d}: {s}", .{ target.tree_id, @errorName(err) });
                };
            } else {
                self.proposeCompaction(target) catch |err| {
                    log.err("proposeCompaction failed: {s}", .{@errorName(err)});
                };
            }
        }
    }

    fn proposeNewMemtable(self: *ZdbServer) !void {
        self.addInFlight(.{ .kind = .new_memtable, .tree_id = 0, .started_at_us = std.time.microTimestamp() });
        defer self.removeInFlight(.new_memtable, 0, 0);
        const new_id = self.forest.next_file_id.fetchAdd(1, .monotonic);
        const payload = try encodeManifestChange(self.allocator, 1, &.{}, &.{}, new_id);
        defer self.allocator.free(payload);
        try self.node.proposeAndWaitForApply(payload, .manifest);
    }

    fn proposeFlush(self: *ZdbServer) !void {
        // Track the flush BUILD as in-flight (writing SSTs is the slow
        // part). buildFlush iterates the oldest immutable memtable; we
        // don't know the target tree(s) until changes return, so use
        // tree_id=0 as a "global" marker.
        self.addInFlight(.{ .kind = .flush, .tree_id = 0, .started_at_us = std.time.microTimestamp() });
        defer self.removeInFlight(.flush, 0, 0);
        const result = try self.forest.buildFlush() orelse return;
        var changes = result.changes;
        defer changes.deinit(self.allocator);
        if (changes.items.len == 0) return;

        try self.distributeNewFiles(changes.items);

        const payload = try encodeManifestChange(self.allocator, 2, changes.items, &.{}, result.memtable_id);
        defer self.allocator.free(payload);
        try self.node.proposeAndWaitForApply(payload, .manifest);
    }

    fn proposeCompaction(self: *ZdbServer, target: Forest.CompactionTarget) !void {
        // Track the compaction so `zdb forest` can show whether a
        // "compactions wanted" entry is genuinely starved or just waiting
        // on a build/sync that hasn't proposed yet.
        self.addInFlight(.{
            .kind = .compaction,
            .tree_id = target.tree_id,
            .src_level = @intCast(target.candidate.level),
            .dest_level = @intCast(@as(usize, target.candidate.level) + 1),
            .started_at_us = std.time.microTimestamp(),
        });
        defer self.removeInFlight(.compaction, target.tree_id, @intCast(target.candidate.level));

        // Per-call ID so we can grep all phase-lines for one compaction
        // out of a noisy multi-loop log: every line below carries
        // `cid=<id>`. The `cid_` prefix is unique enough to tail+grep.
        const cid = self.next_compaction_id.fetchAdd(1, .monotonic);
        const src = target.candidate.level;
        const dst: usize = @as(usize, src) + 1;
        const t0 = std.time.milliTimestamp();
        log.info("compact cid={d} start tree={d} L{d}->L{d}", .{ cid, target.tree_id, src, dst });

        var changes = try target.tree.buildCompaction(src, &self.forest.next_file_id);
        defer changes.deinit(self.allocator);
        if (changes.items.len == 0) {
            log.info("compact cid={d} build_done changes=0 (no-op)", .{cid});
            return;
        }
        const t1 = std.time.milliTimestamp();
        log.info("compact cid={d} build_done changes={d} build_ms={d}", .{ cid, changes.items.len, t1 - t0 });

        try self.distributeNewFiles(changes.items);
        const t2 = std.time.milliTimestamp();
        log.info("compact cid={d} distribute_done distribute_ms={d}", .{ cid, t2 - t1 });

        const payload = try encodeManifestChange(self.allocator, 3, changes.items, &.{}, 0);
        defer self.allocator.free(payload);
        try self.node.proposeAndWaitForApply(payload, .manifest);
        const t3 = std.time.milliTimestamp();
        log.info("compact cid={d} done propose_ms={d} total_ms={d}", .{ cid, t3 - t2, t3 - t0 });
    }

    fn proposeSplit(self: *ZdbServer, tree_id: u64) !void {
        self.addInFlight(.{ .kind = .split, .tree_id = tree_id, .started_at_us = std.time.microTimestamp() });
        defer self.removeInFlight(.split, tree_id, 0);
        // buildSplit allocates table_changes / tree_changes on the arena;
        // encodeManifestChange copies the bytes into a self-owned payload,
        // so the arena can be torn down right after encode. No new SST
        // files are produced (split just remaps existing tables between
        // trees), so distributeNewFiles is unnecessary here. cmd=4 routes
        // through applyManifestChange to applySplit on every node.
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const mc = try self.forest.buildSplit(arena.allocator(), tree_id) orelse {
            // No split key (Lmax has < 2 tables). The candidate that sent
            // us here was stale — refresh it so the next levels-loop
            // iteration picks a different (real) candidate instead of
            // hammering this tree forever. Without this, the loop spins
            // on the same `needs_split=true` tree and starves every
            // other tree's pending compactions.
            if (self.forest.trees.get(tree_id)) |t| t.updateCompactCandidate() catch {};
            log.warn("proposeSplit tree={d} skipped: no split key (refreshed candidate)", .{tree_id});
            return;
        };

        // Diagnostic: dump the split's TC layout so we can correlate the
        // trigger size (logged by Tree.updateCompactCandidate) with what
        // the split actually proposes — table count, new tree id, etc.
        var n_create: usize = 0;
        var n_delete: usize = 0;
        for (mc.table_changes) |tc| switch (tc.op) {
            .create => n_create += 1,
            .delete => n_delete += 1,
            else => {},
        };
        log.info("proposeSplit tree={d} table_changes={d} (create={d} delete={d}) tree_changes={d}", .{
            tree_id, mc.table_changes.len, n_create, n_delete, mc.tree_changes.len,
        });

        const payload = try encodeManifestChange(
            self.allocator,
            mc.cmd,
            mc.table_changes,
            mc.tree_changes,
            0,
        );
        defer self.allocator.free(payload);
        try self.node.proposeAndWaitForApply(payload, .manifest);
    }

    /// Send new SST files to peers and wait for quorum acks.
    fn distributeNewFiles(self: *ZdbServer, changes: []const manifest.TableChange) !void {
        var file_ids = std.ArrayList(u64).empty;
        defer file_ids.deinit(self.allocator);
        for (changes) |c| {
            if (c.op == .create) try file_ids.append(self.allocator, c.table_id);
        }
        if (file_ids.items.len == 0) return;
        try self.node.sendAndWaitForFileQuorum(file_ids.items);
    }

    fn handleClient(self: *ZdbServer, stream: zio.net.Stream) void {
        defer stream.close();
        var ops: u64 = 0;
        // One buffered reader per connection — protocol reads (command
        // headers, batch tokens) come from the 256 KiB buffer instead of
        // hitting io_uring per byte. Buffers heap-allocated because zio
        // fibers have small stacks.
        const reader_buf = self.allocator.alloc(u8, 256 * 1024) catch return;
        defer self.allocator.free(reader_buf);
        const line_buf = self.allocator.alloc(u8, 64 * 1024) catch return;
        defer self.allocator.free(line_buf);
        var reader = stream.reader(reader_buf);
        const r = &reader.interface;

        while (!self.node.isShuttingDown()) {
            // Step 1: pull a line. Stream-level errors (LineTooLong, I/O)
            // close the connection — byte stream is at an unknown offset.
            const line = proto.readLine(r, line_buf) catch |err| switch (err) {
                error.EndOfStream => {
                    log.info("client disconnected after {d} ops (EOF)", .{ops});
                    return;
                },
                else => {
                    log.warn("client stream error after {d} ops: {s} — closing", .{ ops, @errorName(err) });
                    proto.writeErr(stream, @errorName(err)) catch {};
                    return;
                },
            };
            if (line.len == 0) continue; // blank line — common from interactive clients
            // Step 2: parse the line. Parse errors are RECOVERABLE — the
            // line is fully consumed, the stream is at a clean boundary,
            // so we send ERR and keep the connection open. Common in
            // interactive use (typo a command, fix and retry).
            const cmd = proto.parseLine(line) catch |err| {
                proto.writeErr(stream, @errorName(err)) catch return;
                continue;
            };
            ops += 1;
            switch (cmd) {
                .set => |req| {
                    log.debug("SET ns={d} key={s} value_len={d}", .{
                        req.ns, req.key, req.value.len,
                    });
                    self.handleSet(stream, req) catch return;
                },
                .set_batch => |req| {
                    log.debug("SET_BATCH ns={d} count={d}", .{ req.ns, req.count });
                    var batch = proto.readSetBatchBody(r, req.ns, req.count, self.allocator) catch |err| {
                        proto.writeErr(stream, @errorName(err)) catch {};
                        return;
                    };
                    defer batch.deinit(self.allocator);
                    self.handleSetBatch(stream, batch) catch return;
                },
                .get => |req| {
                    log.debug("GET ns={d} key={s} read_ts={d}", .{ req.ns, req.key, req.read_ts });
                    self.handleGet(stream, req) catch return;
                },
                .scan => |req| {
                    log.debug("SCAN ns={d} start_key={s} max={d} mode={s}", .{
                        req.ns, req.start_key, req.max_count, @tagName(req.mode),
                    });
                    self.handleScan(stream, req) catch return;
                },
                .conf => |req| {
                    log.info("CONF {s} node={d}", .{ @tagName(req.action), req.node_id });
                    self.handleConf(stream, req) catch return;
                },
                .ns_create => |req| {
                    log.info("NS CREATE ns={d}", .{req.ns});
                    self.handleNsCreate(stream, req) catch return;
                },
                .zdb => |req| {
                    self.handleZdb(stream, req) catch return;
                },
                .raft => |req| {
                    self.handleRaft(stream, req) catch return;
                },
                .vfs => |_| {
                    self.handleVfs(stream) catch return;
                },
                .help => |_| {
                    proto.writeHelp(stream) catch return;
                },
            }
        }
        log.info("client handler exiting after {d} ops (shutdown)", .{ops});
    }

    /// L0 backpressure: sleep proportional to how many L0 tables the
    /// destination namespace's worst tree currently has. Bound writes from
    /// out-running flush + compaction so L0 doesn't grow into a 10s-of-GiB
    /// monolithic compaction target. Per-namespace (not global) so a hot
    /// namespace doesn't stall writes to other namespaces on the same node.
    /// See `Options.l0_backpressure_start_count` etc. for the curve.
    fn applyWriteBackpressure(self: *ZdbServer, ns: u32) void {
        const opts = self.forest.opts;
        if (opts.l0_backpressure_start_count == 0) return;
        const l0 = self.forest.maxL0TableCountForNs(ns);
        if (l0 <= opts.l0_backpressure_start_count) return;
        const excess: u64 = l0 - opts.l0_backpressure_start_count;
        const delay_us: u64 = @min(
            excess * opts.l0_backpressure_step_us,
            @as(u64, opts.l0_backpressure_max_us),
        );
        if (delay_us == 0) return;
        zio.sleep(zio.Duration.fromMicroseconds(delay_us)) catch {};
    }

    fn handleSet(self: *ZdbServer, stream: zio.net.Stream, req: proto.SetCmd) !void {
        // Reject up-front if the destination namespace has no route table.
        // Otherwise the mutation is durably committed through Raft but
        // `buildFlush` will silently drop it (no tree to route to), and the
        // immutable memtable never gets popped — RAM grows without bound.
        if (!self.forest.hasNamespace(req.ns)) {
            try writeUnknownNamespace(stream, req.ns);
            return;
        }
        self.applyWriteBackpressure(req.ns);
        // Encode as a single-item Mutations batch — the wire format is the
        // same regardless of batch size, so the apply path doesn't need to
        // know whether this write came from a `set` or a `set_batch`.
        const proposal_bytes = try encodeMutations(self.allocator, req.ns, &.{
            .{ .key = req.key, .value = req.value },
        });
        defer self.allocator.free(proposal_bytes);

        // TODO: Fire callback at commit time (quorum achieved) instead of after apply.
        // Currently the client blocks until both commit + apply are done. To fix:
        // 1. Move callback fire before on_apply in applyCommits
        // 2. For reads, track applied_timestamp in StorageApplier and block reads
        //    until applied_timestamp >= read timestamp for linearizability.
        // Set up the commit context with WaitGroup.
        var ctx = CommitCtx{
            .status = .committed,
        };
        ctx.wg.init();
        ctx.wg.add(1);

        self.node.proposeDataChange(proposal_bytes, &onCommit, @ptrCast(&ctx)) catch |err| {
            log.err("propose failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, "propose failed");
            return;
        };

        // Block until the commit callback fires.
        ctx.wg.wait() catch {
            log.err("wg.wait failed for set", .{});
            try proto.writeErr(stream, "commit wait failed");
            return;
        };

        switch (ctx.status) {
            .committed => {
                log.debug("SET committed", .{});
                try proto.writeOk(stream);
            },
            .not_leader => {
                const leader = self.node.getLeaderId() orelse 0;
                log.info("SET rejected: not leader (leader={d})", .{leader});
                try proto.writeNotLeader(stream, leader);
            },
            else => {
                log.err("SET failed: status={s}", .{@tagName(ctx.status)});
                try proto.writeErr(stream, @tagName(ctx.status));
            },
        }
    }

    /// Tokenize `count * 2` whitespace-delimited tokens from the buffered
    /// reader into (key, value) pairs, encode as a single Mutations,
    /// propose once. Each token is hex-decoded if prefixed with `0x`,
    /// literal otherwise.
    fn handleSetBatch(self: *ZdbServer, stream: zio.net.Stream, batch: proto.ParsedBatch) !void {
        if (!self.forest.hasNamespace(batch.ns)) {
            try writeUnknownNamespace(stream, batch.ns);
            return;
        }
        self.applyWriteBackpressure(batch.ns);
        const proposal_bytes = try encodeMutations(self.allocator, batch.ns, batch.kvs);
        defer self.allocator.free(proposal_bytes);

        var ctx = CommitCtx{ .status = .committed };
        ctx.wg.init();
        ctx.wg.add(1);
        self.node.proposeDataChange(proposal_bytes, &onCommit, @ptrCast(&ctx)) catch |err| {
            log.err("batch propose failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, "propose failed");
            return;
        };
        ctx.wg.wait() catch {
            try proto.writeErr(stream, "commit wait failed");
            return;
        };
        switch (ctx.status) {
            .committed => try proto.writeOk(stream),
            .not_leader => {
                const leader = self.node.getLeaderId() orelse 0;
                try proto.writeNotLeader(stream, leader);
            },
            else => try proto.writeErr(stream, @tagName(ctx.status)),
        }
    }

    fn handleGet(self: *ZdbServer, stream: zio.net.Stream, req: proto.GetCmd) !void {
        // If the client sent a read_ts, wait for this node's apply_ts to reach
        // it (5s timeout). No read_ts → serve local applied state immediately.
        if (req.read_ts > 0) {
            self.node.waitUntilApplied(req.read_ts, zio.Duration.fromSeconds(5)) catch {
                try proto.writeErr(stream, "read_stale");
                return;
            };
        }
        // MVCC lookup at the requested timestamp. 0 → latest (maxInt).
        const search_ts: u64 = if (req.read_ts == 0) std.math.maxInt(u64) else req.read_ts;
        const search_key = Key{
            .namespace = req.ns,
            .primary = req.key,
            .timestamp = search_ts,
        };
        const result = self.forest.get(self.allocator, search_key, .p0) catch |err| {
            log.err("forest.get failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, "get failed");
            return;
        };
        if (result) |item| {
            defer item.free(self.allocator);
            log.debug("GET found: ts={d} value_len={d}", .{ item.key.timestamp, item.value.len });
            try proto.writeFound(stream, item.key.timestamp, item.value);
        } else {
            log.debug("GET miss", .{});
            try proto.writeNotFound(stream);
        }
    }

    fn handleScan(self: *ZdbServer, stream: zio.net.Stream, req: proto.ScanCmd) !void {
        // Scan range = [start, end). Both bounds use ts=maxInt so the
        // primary-key compare comes first and same-primary versions
        // sort newest-first inside the bound. An empty primary on
        // either side means "open."
        const scan_start = Key{
            .namespace = req.ns,
            .primary = req.start_key,
            .timestamp = std.math.maxInt(u64),
        };
        const scan_end: Key = if (req.end_key.len == 0) .{} else .{
            .namespace = req.ns,
            .primary = req.end_key,
            .timestamp = std.math.maxInt(u64),
        };
        const scan_range = zdb.level.KeyRange{
            .left = scan_start,
            .right = scan_end,
            .inf = req.start_key.len == 0 and req.end_key.len == 0,
        };

        // 4 KiB buffered writer — 1-2 items fit per drain at the bench's
        // 4 KiB value size; smaller modes pack many items per drain.
        var buf: [4096]u8 = undefined;
        var sw = stream.writer(&buf);
        const w = &sw.interface;

        const Ctx = struct {
            w: *std.Io.Writer,
            mode: proto.ScanCmd.Mode,
            ns: u32,

            fn emit(ctx_raw: *anyopaque, item: zdb.key.Item) anyerror!bool {
                const c: *@This() = @ptrCast(@alignCast(ctx_raw));
                // Memtables are forest-wide; an item with a different
                // namespace means we've walked off the end of ours.
                if (item.key.namespace != c.ns) return false;
                try proto.writeScanItem(
                    c.w,
                    c.mode,
                    item.key.timestamp,
                    item.key.primary,
                    item.value,
                );
                return true;
            }
        };
        var cctx = Ctx{ .w = w, .mode = req.mode, .ns = req.ns };

        const emitted = self.forest.scan(
            self.allocator,
            req.ns,
            scan_range,
            req.max_count,
            .p0,
            @ptrCast(&cctx),
            &Ctx.emit,
        ) catch |err| {
            log.err("forest.scan failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, "scan failed");
            return;
        };
        try proto.writeScanEnd(w, emitted);
        try w.flush();
    }

    fn handleVfs(self: *ZdbServer, stream: zio.net.Stream) !void {
        _ = self;
        // Pretty-printed snapshot of the global VFS bandwidth limiter,
        // counted in pages (multiply by VFS page size for bytes). P0
        // always wins (saturating subtract — never blocks); P1 sees the
        // full bucket; P2 sees `bucket - demand_p1`. If `available_p2`
        // is 0 while compaction work is pending, P1 demand is starving
        // P2 — a likely culprit for L≥1 compaction wedges.
        var buf: [4096]u8 = undefined;
        var sw = stream.writer(&buf);
        const w = &sw.interface;
        const s = vfs.g().bw.status();
        try w.print("vfs bandwidth (pages):\n", .{});
        try w.print("  rate:          {d} pages/s\n", .{s.pages_per_sec});
        try w.print("  bucket:        {d}\n", .{s.bucket});
        try w.print("  demand_p1:     {d}\n", .{s.demand_p1});
        try w.print("  available_p1:  {d}\n", .{s.available_p1});
        try w.print("  available_p2:  {d}   (bucket - demand_p1)\n", .{s.available_p2});
        try w.print("  total_read:    {d}\n", .{s.total_read_pages});
        try w.print("  total_write:   {d}\n", .{s.total_write_pages});
        try w.print("  note: p0 is unbounded (saturating subtract)\n", .{});
        try w.flush();
    }

    fn handleConf(self: *ZdbServer, stream: zio.net.Stream, req: proto.ConfCmd) !void {
        const change_type: raft.ConfChangeType = switch (req.action) {
            .add => .add_learner,
            .to_voter => .to_voter,
            .to_learner => .to_learner,
            .remove => .remove_node,
            .addr => .update_addr,
        };

        // Validate address for commands that require it.
        if (req.action == .add or req.action == .addr) {
            _ = zio.net.IpAddress.parseIpAndPort(req.addr) catch {
                try proto.writeErr(stream, "invalid address");
                return;
            };
        }

        var ctx = CommitCtx{ .status = .committed };
        ctx.wg.init();
        ctx.wg.add(1);

        self.node.proposeConfChange(.{
            .change_type = change_type,
            .node_id = req.node_id,
            .addr = req.addr,
        }, &onCommit, @ptrCast(&ctx)) catch |err| {
            log.err("conf change failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, @errorName(err));
            return;
        };

        ctx.wg.wait() catch {
            try proto.writeErr(stream, "cancelled");
            return;
        };

        switch (ctx.status) {
            .committed => try proto.writeOk(stream),
            .not_leader => {
                const leader = self.node.getLeaderId() orelse 0;
                try proto.writeNotLeader(stream, leader);
            },
            else => try proto.writeErr(stream, @tagName(ctx.status)),
        }
    }

    fn handleRaft(self: *ZdbServer, stream: zio.net.Stream, req: proto.RaftCmd) !void {
        if (req.sub == .entry) return self.handleRaftEntry(stream, req.lo, req.hi);
        // Best-effort snapshot of node + peer state. Races against an
        // in-flight conf change are possible (peers map mutated by
        // mainLoop's applyConfChange); accepted because this is an admin
        // endpoint, not a hot path. Output is one `node` line per cluster
        // member with the same column shape, so progress is visually
        // alignable. The local node's line carries extra cluster-wide
        // fields (term/leader/quorum/commit/apply/first) at the end.
        // Blank line terminates — same shape as `stats` so simple clients
        // can read until `\n\n`.
        var body: std.Io.Writer.Allocating = .init(self.allocator);
        defer body.deinit();
        const w = &body.writer;

        const node = self.node;
        const leader_id = node.getLeaderId() orelse 0;
        const last_idx = node.log.lastIndex();

        // Self row — `last` = local last_index (Raft's `lastLogIndex`),
        // `next` = last+1 (the index the next append would write).
        // Role is left-padded to 9 chars so the column aligns with peer
        // rows regardless of "leader" / "follower" / "voter" / etc.
        // Cluster-wide extras (term/leader/quorum/commit/apply/log range)
        // come at the end of the line.
        try w.print(
            "node id={d} role={s:<9} last={d} next={d} addr={s}  term={d} leader={d} quorum={d} commit={d} apply={d} log=[{d},{d}]\n",
            .{
                node.id,
                @tagName(node.state),
                last_idx,
                last_idx + 1,
                node.config.listen_addr,
                node.current_term,
                leader_id,
                node.quorum_size,
                node.commit_index.load(.acquire),
                node.apply_index.load(.acquire),
                node.log.firstIndex(),
                last_idx,
            },
        );

        // Peer rows. Only the leader has authoritative match/next per
        // peer (Raft's `matchIndex[]` is leader-side state); followers
        // don't track each other's replication progress. So followers
        // show just role + addr — the operator should query the leader
        // (route via `leader=` in the self-row above) for real progress.
        const is_leader = node.state == .leader;
        var it = node.peers.iterator();
        while (it.next()) |kv| {
            const peer = kv.value_ptr.*;
            if (is_leader) {
                try w.print("node id={d} role={s:<9} last={d} next={d} addr={s}\n", .{
                    kv.key_ptr.*,
                    @tagName(peer.role),
                    peer.last_index,
                    peer.next_index,
                    peer.addr,
                });
            } else {
                try w.print("node id={d} role={s:<9} addr={s}\n", .{
                    kv.key_ptr.*,
                    @tagName(peer.role),
                    peer.addr,
                });
            }
        }
        try w.writeByte('\n');
        try stream.writeAll(body.writer.buffered(), .none);
    }

    /// Dump WAL entries [lo, hi] (inclusive) as one human-readable line each.
    /// Used for poking at the raft log when an apply error wedges progress
    /// at a known index. Out-of-range or missing entries become an error
    /// line so the caller can tell missing-vs-empty apart.
    fn handleRaftEntry(self: *ZdbServer, stream: zio.net.Stream, lo: u64, hi: u64) !void {
        var body: std.Io.Writer.Allocating = .init(self.allocator);
        defer body.deinit();
        const w = &body.writer;

        var i: u64 = lo;
        while (i <= hi) : (i += 1) {
            var result = self.node.log.entriesRange(.{ .lo = i, .hi = i + 1 }) catch |err| {
                try w.print("idx={d} err={s}\n", .{ i, @errorName(err) });
                continue;
            };
            defer result.deinit();
            if (result.entries.len == 0) {
                try w.print("idx={d} <missing>\n", .{i});
                continue;
            }
            const stored = result.entries[0];
            try formatEntry(self.allocator, w, stored);
        }
        try w.writeByte('\n');
        try stream.writeAll(body.writer.buffered(), .none);
    }

    fn handleZdb(self: *ZdbServer, stream: zio.net.Stream, req: proto.ZdbCmd) !void {
        // Snapshot into a buffer so we write in a single syscall. Clients
        // read until '\n\n' so we always terminate the body with a blank line.
        var body: std.Io.Writer.Allocating = .init(self.allocator);
        defer body.deinit();
        switch (req.filter) {
            .prom => {
                // Both metric namespaces (zdb storage + raft propose-phase)
                // walk through the same writer so they appear in one blob.
                try m.write(zdb.metrics.g(), &body.writer);
                try m.write(raft.metrics.g(), &body.writer);
            },
            .forest => {
                try self.forest.writeStats(&body.writer, .{ .scope = .forest });
                try self.writeInFlight(&body.writer);
            },
            .tree => try self.forest.writeStats(&body.writer, .{ .scope = .{ .tree = req.tree_id } }),
            .namespace => try self.forest.writeStats(&body.writer, .{ .scope = .{ .namespace = req.ns_id } }),
        }
        try body.writer.writeByte('\n');
        try stream.writeAll(body.writer.buffered(), .none);
    }

    /// Append a section listing in-flight flush/compaction/split builds —
    /// work that's started writing SSTs or computing manifest changes but
    /// hasn't yet appended the resulting Raft entry. A `compactions wanted`
    /// row in the forest stats with no matching in-flight row means the
    /// compaction loop is starved (not picking up the candidate); a row
    /// that's been "in flight" for many seconds means a build is slow.
    fn writeInFlight(self: *ZdbServer, w: *std.Io.Writer) !void {
        self.in_flight_mu.lock();
        defer self.in_flight_mu.unlock();
        if (self.in_flight.items.len == 0) {
            try w.print("\n  in-flight: (none)\n", .{});
            return;
        }
        const now = std.time.microTimestamp();
        try w.print("\n  in-flight:\n", .{});
        for (self.in_flight.items) |it| {
            const age_ms = @divTrunc(now - it.started_at_us, 1000);
            switch (it.kind) {
                .compaction => try w.print(
                    "    compaction tree={d} L{d}->L{d} age_ms={d}\n",
                    .{ it.tree_id, it.src_level, it.dest_level, age_ms },
                ),
                .flush => try w.print("    flush age_ms={d}\n", .{age_ms}),
                .split => try w.print("    split tree={d} age_ms={d}\n", .{ it.tree_id, age_ms }),
                .new_memtable => try w.print("    new_memtable age_ms={d}\n", .{age_ms}),
            }
        }
    }

    fn handleNsCreate(self: *ZdbServer, stream: zio.net.Stream, req: proto.NsCreateCmd) !void {
        if (!self.node.isLeader()) {
            const leader = self.node.getLeaderId() orelse 0;
            try proto.writeNotLeader(stream, leader);
            return;
        }

        // Build the ManifestChange on a temporary arena so we don't track each
        // inner allocation; the proposal is serialized before the arena drops.
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const mc = self.forest.buildNamespaceCreate(arena.allocator(), req.ns) catch |err| {
            log.err("buildNamespaceCreate failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, @errorName(err));
            return;
        };

        const payload = encodeManifestChange(
            self.allocator,
            mc.cmd,
            mc.table_changes,
            mc.tree_changes,
            mc.memtable_id,
        ) catch |err| {
            log.err("encodeManifestChange failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, @errorName(err));
            return;
        };
        defer self.allocator.free(payload);

        self.node.proposeAndWaitForApply(payload, .manifest) catch |err| {
            log.err("ns create propose failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, @errorName(err));
            return;
        };
        try proto.writeOk(stream);
    }
};

const CommitCtx = struct {
    wg: WaitGroup = .{},
    status: raft.CommitStatus,
    /// Timestamp the leader assigned to the committed entry (0 on failure).
    commit_ts: u64 = 0,
};

fn onCommit(ctx_raw: *anyopaque, entry: raft.Entry, status: raft.CommitStatus) void {
    const ctx: *CommitCtx = @ptrCast(@alignCast(ctx_raw));
    ctx.status = status;
    ctx.commit_ts = entry.timestamp;
    ctx.wg.done();
}

fn writeUnknownNamespace(stream: zio.net.Stream, ns: u32) !void {
    var buf: [64]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "unknown namespace {d}", .{ns}) catch unreachable;
    try proto.writeErr(stream, msg);
}

/// Render one StoredEntry as a single-line human-readable summary.
/// Format: `idx=I term=T ts=SECS.LOGICAL type=X bytes=B detail=...`
/// HLC is split into <unix_seconds>.<logical> so log times are visually
/// scannable instead of a giant u64.
fn formatEntry(alloc: std.mem.Allocator, w: *std.Io.Writer, stored: raft.StoredEntry) !void {
    const reader = raft.buffers.LogEntryReader.parseRoot(stored.bytes) catch |err| {
        try w.print("idx={d} term={d} bytes={d} parse_err={s}\n", .{ stored.index, stored.term, stored.bytes.len, @errorName(err) });
        return;
    };
    const ety = reader.entryType();
    const hlc = utils.hlc.HLC.fromU64(reader.timestamp());
    const data = reader.data();
    try w.print("idx={d} term={d} ts={d}.{d} type={s} data_bytes={d}", .{
        stored.index, stored.term, hlc.seconds, hlc.logical, @tagName(ety), data.len,
    });
    if (data.len == 0) {
        try w.print(" detail=tick\n", .{});
        return;
    }
    switch (ety) {
        .data, .manifest => {
            const parsed = zdb_bufs.parseMessageWithTail(data) catch |err| {
                try w.print(" detail=parse_err({s})\n", .{@errorName(err)});
                return;
            };
            switch (parsed) {
                .Mutations => |ms| {
                    const count = ms.kvsLen();
                    try w.print(" detail=Mutations ns={d} count={d}", .{ ms.namespace(), count });
                    if (count > 0) {
                        const kv = ms.kvsAt(0) catch {
                            try w.writeByte('\n');
                            return;
                        };
                        try w.print(" first_key={x}", .{kv.key()});
                    }
                    try w.writeByte('\n');
                },
                .ManifestChange => |mc| {
                    const mcs = mc.toStruct(alloc) catch |err| {
                        try w.print(" detail=ManifestChange err={s}\n", .{@errorName(err)});
                        return;
                    };
                    defer {
                        mcs.deinit(alloc);
                        alloc.destroy(mcs);
                    }
                    try w.print(" detail=ManifestChange cmd={d} memtable_id={d} table_changes={d} tree_changes={d}", .{
                        mcs.cmd, mcs.memtable_id, mcs.table_changes.len, mcs.tree_changes.len,
                    });
                    try w.writeByte('\n');
                    for (mcs.table_changes, 0..) |tc, idx| {
                        try w.print("  tc[{d}]=(op={s} fid={d} tree={d} lvl={d})\n", .{
                            idx, @tagName(tc.op), tc.table_id, tc.tree_id, tc.level,
                        });
                    }
                },
                else => try w.print(" detail=Other({s})\n", .{@tagName(parsed)}),
            }
        },
        .conf => try w.print(" detail=ConfChange\n", .{}),
        .snapshot => try w.print(" detail=Snapshot\n", .{}),
        else => try w.print(" detail=unknown\n", .{}),
    }
}

/// Encode a ManifestChange into a zbuf-serialized payload for proposeDataChange.
fn encodeManifestChange(
    allocator: std.mem.Allocator,
    cmd: u32,
    table_changes: []const manifest.TableChange,
    tree_changes: []const manifest.TreeChange,
    memtable_id: u64,
) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    var mw = try zdb_bufs.ManifestChangeWriter.initWithBuf(allocator, &buf);

    if (table_changes.len > 0) {
        const offsets = try allocator.alloc(u32, table_changes.len);
        defer allocator.free(offsets);
        for (table_changes, 0..) |tc, i| {
            var tw = try zdb_bufs.TableChangeWriter.initWithBuf(allocator, &buf);
            try tw.set(.{ .op = tc.op, .level = tc.level, .table_id = tc.table_id, .tree_id = tc.tree_id });
            offsets[i] = (try tw.finish()).offset;
        }
        try mw.setTableChanges(offsets);
    }

    if (tree_changes.len > 0) {
        const offsets = try allocator.alloc(u32, tree_changes.len);
        defer allocator.free(offsets);
        for (tree_changes, 0..) |tc, i| {
            var tw = try zdb_bufs.TreeChangeWriter.initWithBuf(allocator, &buf);
            try tw.set(.{
                .op = tc.op,
                .tree_id = tc.tree_id,
                .namespace_id = tc.namespace_id,
                .left_key = tc.left_key,
                .right_key = tc.right_key,
            });
            offsets[i] = (try tw.finish()).offset;
        }
        try mw.setTreeChanges(offsets);
    }

    try mw.setCmd(cmd);
    if (memtable_id != 0) try mw.setMemtableId(memtable_id);
    _ = try mw.finishWithTail();
    return buf.toOwnedSlice(allocator);
}

/// Encode one or more key-value writes as a single `Mutations` payload for
/// proposeDataChange. All kvs in a batch share a namespace (keeps tree
/// routing + apply simple) and land as one Raft log entry (one commit HLC).
fn encodeMutations(allocator: std.mem.Allocator, namespace: u32, kvs: []const proto.KV) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(allocator);

    var mw = try zdb_bufs.MutationsWriter.initWithBuf(allocator, &buf);

    const offsets = try allocator.alloc(u32, kvs.len);
    defer allocator.free(offsets);
    for (kvs, 0..) |kv, i| {
        var kvw = try zdb_bufs.KeyValueWriter.initWithBuf(allocator, &buf);
        try kvw.set(.{ .key = kv.key, .value = kv.value });
        offsets[i] = (try kvw.finish()).offset;
    }
    try mw.setNamespace(namespace);
    try mw.setKvs(offsets);
    _ = try mw.finishWithTail();
    return buf.toOwnedSlice(allocator);
}
