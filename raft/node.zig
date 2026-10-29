// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const _diskwal = @import("diskwal.zig");
const zen = @import("buffers_gen");
const utils = @import("utils");
const compression = utils.compression;
const String = utils.String;
const _transport = @import("transport");
const Transport = _transport.Transport;
const _serial = @import("serial");
const Serial = _serial.Serial;
const _metrics = @import("metrics.zig");
const raft_log = std.log.scoped(.raft);

pub const NodeId = _diskwal.NodeId;
pub const State = enum {
    follower,
    candidate,
    leader,
    learner,
};
pub const ConfChange = _diskwal.ConfChange;
pub const ConfChangeType = _diskwal.ConfChangeType;

pub const PeerRole = zen.RoleEnum;

pub const Config = struct {
    id: NodeId,
    cluster_id: u64 = 0,
    listen_addr: String = "",
    tick_interval_ms: u32 = 100,
    heartbeat_ticks: u32 = 2,
    election_timeout_min_ticks: u32 = 10,
    election_timeout_max_ticks: u32 = 20,
    max_entries_per_msg: u32 = 64,
    max_msg_size: u32 = 1_048_576,
    /// Peers within `lockstep_cutoff` of the slowest in-window peer share a
    /// single AppendEntries (built once, fan-out via RefValue). Followers
    /// already at the higher index walk past the duplicate prefix entries
    /// (term-match check) and only WAL-append the new tail. Outliers beyond
    /// this cutoff get their own AE to avoid shipping huge duplicate prefixes.
    lockstep_cutoff: u32 = 32,
    enable_prevote: bool = true,

    pub fn validate(self: Config) !void {
        if (self.election_timeout_min_ticks == 0) return error.InvalidConfig;
        if (self.election_timeout_max_ticks < self.election_timeout_min_ticks) return error.InvalidConfig;
        if (self.heartbeat_ticks == 0) return error.InvalidConfig;
        if (self.heartbeat_ticks >= self.election_timeout_min_ticks) return error.InvalidConfig;
    }
};

pub const ConfigError = error{
    InvalidConfig,
};

/// Monotonically-advancing u64 with wait/signal. Used for raft progress
/// markers (applied HLC, commit index, etc.) where many tasks block until
/// the value crosses a target. Re-exported from `fiber.Watermark`.
pub const Watermark = fiber.Watermark;

pub const DiskWAL = @import("diskwal.zig").DiskWAL;
pub const RaftLogError = _diskwal.RaftLogError;

pub const RaftError = error{
    NotLeader,
    InvalidMessage,
    ConfChangeInProgress,
    LastVoter,
    InvalidBytes,
    QueueClosed,
    QueueFull,
    CommitFailed,
    NotCommitted,
    NotReady,
} || RaftLogError || zen.Error;

pub const Peer = struct {
    id: NodeId,
    next_index: u64,
    // Highest log index known to be replicated on this peer (leader view).
    // Raft's `matchIndex[i]` — kept here under the more readable name.
    last_index: u64,
    role: PeerRole,
    addr: String = "",
    /// What this peer last reported in an AppendEntries reply as the
    /// oldest timestamp it still reads at (`StateMachine.minReadTs`),
    /// and the tick the reply arrived on. 0: nothing reported yet.
    min_read_ts: u64 = 0,
    min_read_tick: u64 = 0,
};

pub const msg_type_raft = _serial.msg_type_raft;
pub const msg_type_join_request = _serial.msg_type_join_request;

pub const InboundMsg = struct {
    message_type: u16,
    payload: []u8, // owned copy
};

pub const Inbound = String;
pub const Entry = struct {
    term: u64,
    index: u64,
    timestamp: u64 = 0,
};

pub const CommitStatus = enum {
    committed,
    not_leader,
    step_down,
    shutdown,
    pending_conf_change,
    last_voter,
    unknown_node,
    existing_node,
    invalid_addr,
    invalid_req,
};

/// What a proposer holds while its proposal is in flight, and where the
/// outcome lands. The node fills `entry` when the proposal is appended
/// (index, term, HLC timestamp), then sets `status` exactly once, at
/// commit or once this node has applied the entry (`after`), or with a
/// failure status when the proposal cannot get there, and notifies. The
/// proposer parks in `wait`, and keeps the struct alive until then.
pub const ProposalWaiter = struct {
    /// When the waiter is finished: at commit (durable on a quorum), or
    /// once this node has applied the entry. The apply fiber finishes an
    /// apply waiter right after the entry, so the proposer parks once
    /// instead of waking at commit and parking again for apply.
    pub const WaitFor = enum { commit, apply };

    wait_for: WaitFor = .commit,
    entry: Entry = .{ .term = 0, .index = 0 },
    status: CommitStatus = .shutdown,
    done: fiber.Notify = .{},

    pub fn wait(self: *ProposalWaiter) CommitStatus {
        self.done.wait();
        if (self.entry.index != 0) {
            const woken_us = utils.timestampNow(.monotonic, .micros);
            _metrics.timeline().record(self.entry.index, .woken, woken_us);
        }
        return self.status;
    }

    /// Node side: the outcome, exactly once.
    fn finish(self: *ProposalWaiter, status: CommitStatus) void {
        self.status = status;
        self.done.notify();
    }
};

/// A worker's id is a u8.
const max_workers = std.math.maxInt(u8) + 1;

pub const Node = struct {
    const TimeoutResetReason = enum {
        init,
        grant_vote,
        start_prevote,
        start_election,
        append_entries,
        step_down,
    };

    fn dbg(comptime format: String, args: anytype) void {
        raft_log.debug(format, args);
    }

    /// The replicated state machine interface. The application implements
    /// this (e.g. with forest/applier) and sets node.state_machine.
    pub const StateMachine = struct {
        ctx: *anyopaque,
        /// Apply an entry to the state machine. Returning an error parks the
        /// raft apply loop on this entry: `applyCommits` will sleep briefly
        /// (still polling shutdown) and retry until apply succeeds. The
        /// state machine must NOT loop on transient errors itself — it would
        /// block the raft fiber from observing shutdown.
        applyFn: *const fn (*anyopaque, Entry, String) anyerror!void,
        /// Open the given file ids from the store into the table cache.
        /// Behind the announce prefetch: raft only relays ids, never file
        /// bytes. Single attempt — the caller reports failure (announce
        /// fail-ack).
        ensureFilesFn: ?*const fn (
            *anyopaque,
            []const zen.FileRefStruct,
        ) anyerror!void = null,
        /// Cheap predicate the leader checks in `propose` before
        /// queuing a `.data` proposal — typically "recovery done AND a real
        /// memtable exists". Must be non-blocking. When null, the gate is
        /// treated as always-ready.
        isReadyFn: ?*const fn (*anyopaque) bool = null,
        /// A snapshot entry was just installed on this node, in log order
        /// with the applies around it. The state machine reads what it
        /// needs off the snapshot (its `timestamp` is the read floor).
        snapshotInstallFn: ?*const fn (*anyopaque, *const zen.SnapshotStruct) void = null,
        /// The oldest timestamp this node still reads at, or may yet
        /// begin a read at. Raft carries it to the leader in every
        /// AppendEntries reply, and the leader gives the minimum over
        /// the voters (`Node.peersMinReadTs`) to the state machine to
        /// pick a snapshot's read floor with. 0, or null here: nothing to
        /// report. Called on the raft fiber: must not block.
        minReadTsFn: ?*const fn (*anyopaque) u64 = null,

        pub fn apply(self: *StateMachine, entry: Entry, data: String) anyerror!void {
            try self.applyFn(self.ctx, entry, data);
        }
        pub fn ensureFiles(
            self: *StateMachine,
            files: []const zen.FileRefStruct,
        ) anyerror!void {
            const f = self.ensureFilesFn orelse return error.NoEnsureFiles;
            try f(self.ctx, files);
        }
        pub fn isReady(self: *StateMachine) bool {
            if (self.isReadyFn) |f| return f(self.ctx);
            return true;
        }
        pub fn snapshotInstall(self: *StateMachine, snap: *const zen.SnapshotStruct) void {
            if (self.snapshotInstallFn) |f| f(self.ctx, snap);
        }
        pub fn minReadTs(self: *StateMachine) u64 {
            if (self.minReadTsFn) |f| return f(self.ctx);
            return 0;
        }
    };

    id: NodeId,
    peers: std.AutoHashMap(NodeId, Peer),

    current_term: u64,
    voted_for: ?NodeId,
    log: *DiskWAL,

    state: State,
    /// Commit index. Lockless reads on hot replication paths via `commit_index.load()`;
    /// applyLoop blocks on `commit_index.waitForUntimed(target)`.
    /// Initialized in `init` (the fiber Watermark needs an allocator).
    commit_index: Watermark,
    apply_index: std.atomic.Value(u64),
    /// HLC timestamp of the most-recently applied entry. Progress marker
    /// parallel to `apply_index`. Readers bounded by `read_ts` block until
    /// `applied_ts.load() >= read_ts` via `waitForApplied`.
    /// Initialized in `init` (the fiber Watermark needs an allocator).
    applied_ts: Watermark,
    conf_applied_index: u64,
    leader_id: ?NodeId,
    pending_conf_change: bool,
    quorum_size: usize,

    votes_received: std.AutoHashMap(NodeId, void),
    prevotes_received: std.AutoHashMap(NodeId, void),
    // peers map stores both membership role and replication state.

    elapsed_ticks: u32,
    election_timeout: u32,
    tick_count: u64,
    /// Where the apply loop decodes a compressed entry's data
    /// (`compression.maybeUnpack`): one buffer, reused entry after
    /// entry.
    apply_scratch: std.ArrayList(u8) = .empty,
    /// Where `propose` compresses: one buffer for each fiber worker,
    /// by its id, so that proposers on different threads compress at
    /// the same time with no lock, and none allocates once its buffer
    /// has grown to the largest payload it has seen.
    pack_scratch: [max_workers]std.ArrayList(u8) =
        [_]std.ArrayList(u8){.empty} ** max_workers,
    config: Config,

    allocator: std.mem.Allocator,

    prng: std.Random.DefaultPrng,
    proposal_ch: fiber.Channel(ProposalRequest) = undefined,
    /// Proposals waiting for commit, in index order. `advanceCommitIndex`
    /// finishes them on the main loop, which is also the only appender,
    /// so no lock.
    commit_waiters: std.ArrayList(*ProposalWaiter) = .empty,
    /// Proposals waiting for apply, in index order; `fireApplied`
    /// drains them on the apply fiber. `applied_held` is the one
    /// taken out but not yet due.
    apply_waiters: fiber.Channel(*ProposalWaiter) = undefined,
    // applied_held is cached when we receive a waiter but haven't reached its
    // index yet.
    applied_held: ?*ProposalWaiter = null,

    event_notify: fiber.Notify = .{},
    inbound_ch: fiber.Channel(InboundMsg) = undefined,
    /// Two-stage shutdown signal: fire via `shutdown.startShutdown()`
    /// (POSIX signal, fatal path, or the binary). Loops poll it via
    /// `isShuttingDown()`;
    /// fibers that must block until shutdown (the engine's listener
    /// watchers) park on `shutdown.waitForStart(.shutdown, null)`;
    /// `shutdownWatchLoop` reacts by
    /// tearing down everything a raft fiber can park on.
    shutdown: fiber.Shutdown = undefined,

    transport: ?*Transport = null,

    on_conf_applied: ?*const fn (*anyopaque, ConfChange) void = null,
    on_conf_applied_ctx: ?*anyopaque = null,

    state_machine: ?*StateMachine = null,

    // Announce tracks a fileset that's been announced by the leader, and which
    // nodes have acknowledged it and which have failed — so we know if we have
    // reached quorum or not. Each peer starts at .sent; its batch response
    // flips it to .ok ("I have every announced file") or .fail.
    pending_announce: fiber.SafeHashMap(u64, *Announce),
    /// Monotonic within a boot, seeded randomly at init (see the seed
    /// derivation there) so ids never repeat across restarts.
    next_announce_id: std.atomic.Value(u64) = std.atomic.Value(u64).init(1),

    pub const Status = enum(u8) {
        sent = 0,
        ok = 1,
        fail = 2,
    };
    const Announce = struct {
        nodes: fiber.SafeHashMap(u64, Status),
    };

    /// Proposal type is the same enum used in the WAL frame's entry_type.
    /// At propose time, callers pass `.data`, `.conf`, `.manifest`, etc. `.any`
    /// is the wildcard filter and never appears as a real proposal.
    pub const ProposalType = zen.EntryTypeEnum;
    const proposal_queue_capacity: usize = 4096;
    const ProposalRequest = struct {
        /// The payload as the entry will store it, and the codec it
        /// is in (`compression.maybePack` in `propose`).
        data: []u8,
        codec: compression.Codec = .none,
        proposal_type: ProposalType = .data,
        /// Null: nobody waits (the bootstrap self-add); the entry goes
        /// into the log and that is all.
        waiter: ?*ProposalWaiter,
        /// When it was queued; the timeline gets it once there is an index.
        proposed_us: i64,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        peers: []const Peer,
        log: *DiskWAL,
        config: Config,
    ) !*Node {
        try config.validate();
        const seed = @as(u64, @intCast(std.Io.Timestamp.now(utils.context.g().io, .real).nanoseconds)) ^ config.id;
        var prng = std.Random.DefaultPrng.init(seed);
        // Random per-boot base for announce ids: a restarted announcer must
        // not mint ids colliding with its pre-restart announces still in
        // flight (a follower's slow download finishing after the restart
        // would tally its stale response into the new announce of the same
        // id). Responses are routed by sender_id, so only self-across-
        // restart collisions exist; within a boot fetchAdd keeps ids unique.
        const announce_base = prng.random().int(u64);
        const node = try allocator.create(Node);
        node.* = .{
            .id = config.id,
            .peers = std.AutoHashMap(NodeId, Peer).init(allocator),
            .current_term = 0,
            .voted_for = null,
            .log = log,
            .state = .follower,
            .apply_index = std.atomic.Value(u64).init(0),
            .conf_applied_index = 0,
            .leader_id = null,
            .pending_conf_change = false,
            .quorum_size = 1,
            .votes_received = std.AutoHashMap(NodeId, void).init(allocator),
            .prevotes_received = std.AutoHashMap(NodeId, void).init(allocator),
            .elapsed_ticks = 0,
            .election_timeout = config.election_timeout_min_ticks,
            .tick_count = 0,
            .config = config,
            .allocator = allocator,
            .prng = prng,
            .pending_announce = fiber.SafeHashMap(u64, *Announce)
                .init(allocator),
            .next_announce_id = std.atomic.Value(u64).init(announce_base),
            .commit_index = Watermark.init(allocator),
            .applied_ts = Watermark.init(allocator),
        };
        node.proposal_ch = try fiber.Channel(ProposalRequest).initAlloc(allocator, proposal_queue_capacity);
        node.apply_waiters = try fiber.Channel(*ProposalWaiter).initAlloc(allocator, 1024);
        node.log.on_written = .{ .ctx = node, .call = onWritten };
        node.inbound_ch = try fiber.Channel(InboundMsg).initAlloc(allocator, 1024);
        node.shutdown = fiber.Shutdown.init(allocator);
        errdefer allocator.destroy(node);
        for (peers) |peer| {
            try node.peers.put(peer.id, peer);
        }
        node.recomputeQuorum();
        node.resetElectionTimeout(.init);

        // Seed the HLC clock with the current wall-clock second so peek()
        // returns a sensible value even before the first `next()` call.
        utils.hlc.g().seed(.{ .seconds = @intCast(std.Io.Timestamp.now(utils.context.g().io, .real).toSeconds()), .logical = 0 });

        // Restore persisted state from WAL if present.
        if (node.log.lastIndex() > 0) {
            raft_log.info("node {d}: restoring state from WAL", .{node.id});
            node.restoreState();
            node.restoreConfState();
            raft_log.info("node {d}: state restored, term={d} peers={d} quorum={d}", .{
                node.id, node.current_term, node.peers.count(), node.quorum_size,
            });
            // Raise the HLC clock past the most recent log entry's timestamp,
            // so a newly-elected leader's future `next()` values are strictly
            // past every previously-committed one.
            const seed_ts = node.log.lastTimestamp();
            if (seed_ts != 0) {
                utils.hlc.g().seed(utils.hlc.HLC.fromU64(seed_ts));
            }
        }

        // If a snapshot is on disk, resume apply from snapshot.index+1.
        // The storage layer's `recoverFromSnapshot` (in the engine) handles
        // installing the snapshot's manifest + fetching missing SSTs before
        // the apply gate opens. commit_index is safe to seed here too: the
        // snapshot was built strictly from committed entries.
        const snap_idx = node.log.snapshot().index;
        if (snap_idx > 0) {
            node.apply_index.store(snap_idx, .release);
            raft_log.info("node {d}: post-snapshot startup, apply_index={d}", .{ node.id, snap_idx });
        }

        // commit_index = max(snap.index, min(persisted_ci, lastIndex)).
        //
        // - persisted_ci is written periodically by `commitPersistLoop`, so
        //   restart can resume apply without first waiting for a new
        //   current-term commit (Raft's prior-term commitment rule). Safe
        //   per the Leader Completeness Property — committed entries stay
        //   committed across restarts.
        // - Cap by lastIndex: if the WAL was truncated during opening
        //   (e.g. tail discarded by chain check), persisted_ci could
        //   reference entries that no longer exist on disk; never advance
        //   commit_index past what we actually have.
        // - Floor by snap.index: snapshots are built strictly from
        //   committed entries, so commit_index is at least snap.index.
        const persisted_ci = node.log.hardState().commit_index;
        const last_idx = node.log.lastIndex();
        const initial_ci = @max(snap_idx, @min(persisted_ci, last_idx));
        if (initial_ci > 0) {
            node.commit_index.advance(initial_ci);
            if (persisted_ci > snap_idx) {
                raft_log.info("node {d}: restored commit_index={d} (snap={d} persisted={d} last={d})", .{
                    node.id, initial_ci, snap_idx, persisted_ci, last_idx,
                });
            }
        }

        dbg(
            "init node={d} state={s} term={d} election_timeout={d} quorum={d} seed=0x{x}",
            .{
                node.id,
                @tagName(node.state),
                node.current_term,
                node.election_timeout,
                node.quorum_size,
                seed,
            },
        );
        return node;
    }

    /// Reject every proposal still queued and fail every waiter parked
    /// on a commit or apply with `.shutdown`. For after the raft loops
    /// have exited: nothing resolves them any more, and the fibers that
    /// own them (SQL statements mid-commit) must run their error paths
    /// while the state they touch is still alive. Idempotent; `deinit`
    /// calls it too.
    pub fn abortPending(self: *Node) void {
        self.proposal_ch.close(.graceful);
        while (self.proposal_ch.tryReceive()) |req| {
            self.allocator.free(req.data);
            self.reject(req, .shutdown);
        } else |_| {}
        self.failAllPendingCallbacks(.shutdown);
    }

    pub fn deinit(self: *Node) void {
        self.apply_scratch.deinit(self.allocator);
        for (&self.pack_scratch) |*scratch| scratch.deinit(self.allocator);

        self.abortPending();
        self.proposal_ch.deinit();
        self.commit_waiters.deinit(self.allocator);
        self.apply_waiters.close(.immediate);
        self.apply_waiters.deinit();
        // Fires startShutdown() (idempotent) then joins the signal-watcher
        // fiber (if install() was used) before freeing — nothing stays
        // parked.
        self.shutdown.deinit();

        self.inbound_ch.close(.graceful);
        while (self.inbound_ch.tryReceive()) |msg| {
            self.allocator.free(msg.payload);
        } else |_| {}
        self.inbound_ch.deinit();
        self.votes_received.deinit();
        self.prevotes_received.deinit();
        var pit = self.peers.valueIterator();
        while (pit.next()) |p| {
            if (p.addr.len > 0) self.allocator.free(p.addr);
        }
        self.peers.deinit();
        // Free the watermark waiter tables. `shutdownWatchLoop` already
        // `close()`d both, so no fiber is still parked on them here.
        self.commit_index.deinit();
        self.applied_ts.deinit();
        // Empty here: announceAndWaitForFileQuorum removes its own entry
        // via defer before returning.
        self.pending_announce.deinit();
        self.allocator.destroy(self);
    }

    /// Create and start the transport. Must be called inside a fiber coroutine.
    pub fn initTransport(self: *Node) !void {
        const tr = try self.allocator.create(Transport);
        tr.* = try Transport.init(self.allocator, .{
            .self_id = self.id,
            .cluster_id = self.config.cluster_id,
            .listen_addr = self.config.listen_addr,
            .ctx = @ptrCast(self),
            .on_message_p0 = &handleP0Message,
            .on_message_p1 = &handleP1Message,
        });
        self.transport = tr;

        // run() starts accept/dial loops. addPeer after run() so dial loops
        // are only spawned once (run() spawns for existing peers, addPeer
        // spawns for new ones).
        try tr.run();

        // Register existing peers so dial loops start.
        var peer_it = self.peers.iterator();
        while (peer_it.next()) |kv| {
            const peer = kv.value_ptr.*;
            if (peer.addr.len > 0) {
                tr.addPeer(peer.id, peer.addr) catch |err| {
                    raft_log.err("failed to add restored peer {d}: {s}", .{ peer.id, @errorName(err) });
                };
            }
        }
    }

    pub fn deinitTransport(self: *Node) void {
        if (self.transport) |tr| {
            tr.deinit();
            self.allocator.destroy(tr);
            self.transport = null;
        }
    }

    // handleP0Message would be called concurrently via multiple Serial listener coroutines.
    fn handleP0Message(ctx: *anyopaque, conn: *Serial, message_type: u16, payload: String) void {
        const self: *Node = @ptrCast(@alignCast(ctx));
        switch (message_type) {
            _serial.msg_type_raft => {
                const owned = self.allocator.dupe(u8, payload) catch return;
                self.inbound_ch.send(.{
                    .message_type = message_type,
                    .payload = owned,
                }) catch {
                    self.allocator.free(owned);
                    return;
                };
                self.event_notify.notify();
            },
            _serial.msg_type_join_request => {
                // Handle join requests in the Serial reader's coroutine directly.
                // This blocks the reader until the join is processed, which is fine —
                // the joiner waits for a response anyway.
                self.handleJoinRequest(conn, payload);
            },
            else => {},
        }
    }

    // -- join request handling (server side) --

    pub const JoinReason = struct {
        pub const ok: u32 = 0;
        pub const not_leader: u32 = 1;
        pub const cluster_mismatch: u32 = 2;
        pub const conf_change_rejected: u32 = 3;
        pub const invalid_addr: u32 = 4;
    };

    fn handleJoinRequest(self: *Node, conn: *Serial, payload: String) void {
        const parsed = zen.parseMessageWithTail(payload) catch return;
        switch (parsed) {
            .JoinRequest => |req| {
                if (req.clusterId() != self.config.cluster_id) {
                    self.sendJoinResponse(conn, false, self.getLeaderId(), JoinReason.cluster_mismatch);
                    return;
                }
                if (!self.isLeader()) {
                    self.sendJoinResponse(conn, false, self.getLeaderId(), JoinReason.not_leader);
                    return;
                }
                const peer_id: NodeId = @intCast(req.nodeId());
                if (peer_id == self.id or self.peers.contains(peer_id)) {
                    dbg("handleJoinRequest: peer {d} already part of the cluster", .{peer_id});
                    self.sendJoinResponse(conn, true, self.id, JoinReason.ok);
                    return;
                }

                var w: ProposalWaiter = .{};
                self.proposeConfChange(.{
                    .change_type = .add_learner,
                    .node_id = @intCast(req.nodeId()),
                    .addr = req.addr(),
                }, &w) catch |err| {
                    raft_log.err("join proposeConfChange failed: {s}", .{@errorName(err)});
                    self.sendJoinResponse(conn, false, self.id, JoinReason.conf_change_rejected);
                    return;
                };
                const status = w.wait();
                const accepted = status == .committed or status == .existing_node;
                self.sendJoinResponse(conn, accepted, self.id, if (accepted) JoinReason.ok else JoinReason.conf_change_rejected);
            },
            else => {},
        }
    }

    fn sendJoinResponse(self: *Node, conn: *Serial, accepted: bool, leader_id: ?NodeId, reason_code: u32) void {
        const leader_addr: String = if (leader_id) |lid| blk: {
            if (lid == self.id) break :blk self.config.listen_addr;
            if (self.peers.get(lid)) |peer| break :blk peer.addr;
            break :blk "";
        } else "";
        var writer = zen.JoinResponseWriter.init(self.allocator) catch return;
        defer writer.deinit();
        writer.set(.{
            .accepted = accepted,
            .leader_id = if (leader_id) |lid| lid else 0,
            .reason_code = reason_code,
            .leader_addr = leader_addr,
        }) catch return;
        var reply_buf = writer.finishWithTail() catch return;
        defer reply_buf.deinit();
        const bytes = reply_buf.bytes orelse return;
        conn.send(_serial.msg_type_join_request, bytes) catch |err| {
            raft_log.err("sendJoinResponse failed: {s}", .{@errorName(err)});
        };
    }

    // --- join handling end

    fn handleP1Message(ctx: *anyopaque, conn: *Serial, message_type: u16, payload: String) void {
        _ = conn; // dispatch only relays ids; responses go via the transport
        const self: *Node = @ptrCast(@alignCast(ctx));
        switch (message_type) {
            _serial.msg_type_announce => {
                const parsed = zen.parseMessageWithTail(payload) catch {
                    raft_log.err("announce: invalid message", .{});
                    return;
                };
                // Ensure-have runs in a detached fiber: store downloads can
                // take seconds and must not block this conn's reader loop
                // (it carries every other message from this peer). The
                // fiber outlives `payload`, so hand it an owned Struct copy.
                const ann = switch (parsed) {
                    .AnnounceFiles => |r| r.toStruct(self.allocator) catch |err| {
                        raft_log.err("announce: toStruct failed: {s}", .{@errorName(err)});
                        return;
                    },
                    else => return,
                };
                fiber.spawnDetached(
                    .{ .lane = .raft },
                    handleFileAnnounce,
                    .{ self, ann },
                ) catch |err| {
                    ann.destroy(self.allocator);
                    raft_log.err("announce: spawn failed: {s}", .{@errorName(err)});
                };
            },
            _serial.msg_type_announce_resp => {
                const parsed = zen.parseMessageWithTail(payload) catch {
                    raft_log.err("announce_resp: invalid message", .{});
                    return;
                };
                switch (parsed) {
                    .AnnounceFilesResp => |resp| self.onAnnounceResp(resp),
                    else => {},
                }
            },
            else => {},
        }
    }

    /// Announce a batch of file ids (already in the store, put by the
    /// caller) to all peers, and wait until a quorum of nodes (self
    /// included) has every one open. A prefetch optimization: followers
    /// open the tables from the store on the announce and batch-ack, so
    /// the manifest entry lands on nodes whose table cache already holds
    /// them and their apply loops never stall. The apply-time open
    /// remains the backstop.
    pub fn announceAndWaitForFileQuorum(
        self: *Node,
        files: []const zen.FileRefStruct,
    ) !void {
        const tr = self.transport orelse return error.NoTransport;

        const announce_id = self.next_announce_id.fetchAdd(1, .monotonic);
        const ann = try self.allocator.create(Announce);
        ann.* = .{
            .nodes = fiber.SafeHashMap(u64, Status).init(self.allocator),
        };
        {
            var peer_it = self.peers.iterator();
            while (peer_it.next()) |kv| try ann.nodes.put(kv.key_ptr.*, .sent);
        }
        self.pending_announce.put(announce_id, ann) catch |err| {
            ann.nodes.deinit();
            self.allocator.destroy(ann);
            return err;
        };
        defer if (self.pending_announce.fetchRemove(announce_id)) |kv| {
            kv.value.nodes.deinit();
            self.allocator.destroy(kv.value);
        };

        const payload = try zen.marshalRoot(self.allocator, zen.AnnounceFilesStruct{
            .announce_id = announce_id,
            .sender_id = self.id,
            // marshalTo only reads; the Struct field is mutable because a
            // parsed Struct owns its slice.
            .files = @constCast(files),
        });
        // Wrap once; sendMsg incrRefs per peer enqueued, no per-peer copy.
        const value = utils.RefValue.init(self.allocator, payload) catch |err| {
            self.allocator.free(payload);
            return err;
        };
        defer value.decrRef();
        var peer_it = self.peers.iterator();
        while (peer_it.next()) |kv| {
            tr.sendMsg(kv.key_ptr.*, .p1, _serial.msg_type_announce, value) catch {};
        }

        // Poll the tallies. Enough oks (self included) → quorum holds the
        // files. Enough fails that ok can never reach quorum → error out
        // rather than spin forever (written addition-only: usize).
        while (true) {
            if (!self.isLeader()) return error.NotLeader;
            if (ann.nodes.countValue(.ok) + 1 >= self.quorum_size) return;
            if (ann.nodes.countValue(.fail) + self.quorum_size > ann.nodes.count() + 1) {
                return error.FileQuorumFailed;
            }
            fiber.sleepMs(10);
        }
    }

    fn onAnnounceResp(self: *Node, resp: zen.AnnounceFilesRespReader) void {
        const ann = self.pending_announce.get(resp.announceId()) orelse return;
        const status: Status = switch (resp.status()) {
            .ok => .ok,
            .fail => .fail,
            else => return,
        };
        if (status == .fail) {
            raft_log.warn("announce {d}: node {d} failed: {s}", .{
                resp.announceId(), resp.nodeId(), resp.failError(),
            });
        }
        ann.nodes.put(resp.nodeId(), status) catch return;
    }

    /// Detached fiber: open every announced file (the state machine
    /// opens them from the store into its table cache), then send the
    /// announcer one batch response. Must not run inline in
    /// the conn reader loop — see the msg_type_announce arm. Owns +
    /// destroys `ann`.
    fn handleFileAnnounce(self: *Node, ann: *zen.AnnounceFilesStruct) void {
        defer ann.destroy(self.allocator);

        var status: zen.AnnounceStatusEnum = .ok;
        var fail_error: String = "";
        if (self.state_machine) |sm| {
            sm.ensureFiles(ann.files) catch |err| {
                status = .fail;
                fail_error = @errorName(err);
            };
        } else {
            status = .fail;
            fail_error = "NoStateMachine";
        }

        const payload = zen.marshalRoot(self.allocator, zen.AnnounceFilesRespStruct{
            .announce_id = ann.announce_id,
            .node_id = self.id,
            .status = status,
            .fail_error = fail_error,
        }) catch |err| {
            raft_log.err("announce {d}: marshal resp failed: {s}", .{ ann.announce_id, @errorName(err) });
            return;
        };
        const tr = self.transport orelse {
            self.allocator.free(payload);
            return;
        };
        const value = utils.RefValue.init(self.allocator, payload) catch {
            self.allocator.free(payload);
            return;
        };
        defer value.decrRef();
        // Via the transport, not the announce's conn: the conn can be torn
        // down and replaced while we were fetching.
        tr.sendMsg(ann.sender_id, .p1, _serial.msg_type_announce_resp, value) catch |err| {
            raft_log.err("announce {d}: resp send failed: {s}", .{ ann.announce_id, @errorName(err) });
        };
    }

    /// Restore hard state (term, voted_for) from WAL on restart.
    /// commit_index is not persisted — the leader re-sends it via AppendEntries.
    pub fn restoreState(self: *Node) void {
        const hs = self.log.hardState();
        self.current_term = hs.current_term;
        self.voted_for = if (hs.voted_for == 0) null else hs.voted_for;
        dbg("restoreState node={d} term={d} voted_for={?d}", .{
            self.id, self.current_term, self.voted_for,
        });
    }

    /// Restore cluster membership from snapshot conf_state + replay conf change entries.
    /// Updates the node's peers map in place. After this, the caller can iterate
    /// node.peers to hand addresses to the transport.
    pub fn restoreConfState(self: *Node) void {
        const snap = self.log.snapshot();
        if (snap.conf_state) |cs| {
            for (cs.peers) |p| {
                const role: PeerRole = p.role;
                if (p.id == self.id) {
                    // Restore our own role from conf state.
                    self.state = if (role == .learner) .learner else .follower;
                } else {
                    self.addPeer(p.id, role, p.addr);
                }
            }
        }

        // Replay all conf changes from snapshot to lastIndex. All entries in
        // our log were received from the leader and represent correct state.
        const last = self.log.lastIndex();
        const start = if (snap.index > 0) snap.index + 1 else self.log.firstIndex();
        if (start > last) {
            self.conf_applied_index = last;
            self.recomputeQuorum();
            return;
        }
        var lo = start;
        while (lo <= last) {
            var result = while (true) {
                break self.log.entriesRange(.{ .lo = lo, .hi = last + 1, .ty = .conf }) catch |err| {
                    utils.logEvery(.err, 5, "restoreConfState: " ++
                        "entriesRange failed: {}, retrying", .{err});
                    fiber.sleepMs(1000);
                    continue;
                };
            };
            defer result.deinit();
            for (result.entries) |entry| {
                const reader = zen.LogEntryReader.parseRoot(entry.bytes) catch continue;
                if (reader.confChange()) |cc_reader| {
                    const cc = self.decodeConfChange(cc_reader) catch continue;
                    self.applyConfChange(cc);
                }
                lo = entry.index + 1;
            }
            if (result.eof) break;
        }
        self.conf_applied_index = last;
        self.recomputeQuorum();
        dbg("restoreConfState node={d} peers={d} quorum={d}", .{
            self.id, self.peers.count(), self.quorum_size,
        });
        var peer_it = self.peers.iterator();
        while (peer_it.next()) |kv| {
            const p = kv.value_ptr.*;
            dbg("  peer id={d} role={s} addr={s} match={d} next={d}", .{
                p.id, @tagName(p.role), p.addr, p.last_index, p.next_index,
            });
        }
    }

    /// Build the current conf state from the peers map (including self).
    /// Caller must deinit the returned ConfState.
    /// Apply a `.snapshot` log entry: parse the marshalled SnapshotStruct,
    /// install via `setSnapshot`, then trim log files below it. Idempotent —
    /// skips if the snapshot index is at or below the current snapshot.
    /// The state machine's data is not touched (the Forest is updated by
    /// the normal `.manifest` applies; this only affects WAL state), but
    /// it is told (`snapshotInstalled`), in log order.
    fn applySnapshotEntry(self: *Node, payload: String) !void {
        const new_snap = try zen.SnapshotReader.parseRoot(payload);
        const cur = self.log.snapshot();
        if (new_snap.index() <= cur.index) return;

        const snap_ptr = try new_snap.toStruct(self.allocator);
        defer {
            snap_ptr.deinit(self.allocator);
            self.allocator.destroy(snap_ptr);
        }
        try self.log.setSnapshot(snap_ptr.*);
        self.log.compactUntil(snap_ptr.index) catch |err| {
            raft_log.warn("compactUntil({d}): {s}", .{ snap_ptr.index, @errorName(err) });
        };
        raft_log.info("snapshot installed: index={d} term={d} bytes={d}", .{
            snap_ptr.index, snap_ptr.term, snap_ptr.data.len,
        });
        if (self.state_machine) |sm| sm.snapshotInstall(snap_ptr);
    }

    pub fn confState(self: *Node) !_diskwal.ConfState {
        const total = self.peers.count() + 1; // +1 for self
        var cs_peers = try self.allocator.alloc(_diskwal.ConfStatePeer, total);
        errdefer self.allocator.free(cs_peers);
        @memset(cs_peers, .{});

        // Add self.
        cs_peers[0] = .{
            .id = self.id,
            .role = if (self.state != .learner) .voter else .learner,
        };

        var i: usize = 1;
        var it = self.peers.valueIterator();
        while (it.next()) |peer| {
            cs_peers[i] = .{
                .id = peer.id,
                .addr = try self.allocator.dupe(u8, peer.addr),
                .role = peer.role,
            };
            i += 1;
        }
        return .{ .peers = cs_peers };
    }

    /// Parks until the shutdown signal fires — `shutdown.startShutdown()`
    /// from any source (a POSIX signal via `shutdown.install()`, an
    /// internal fatal path, or the binary) — then closes everything a
    /// raft loop can park on
    /// so all of them unwind. Runs as one of the loop-group fibers
    /// (`blockingRunAllLoops`, test cluster harness), so the teardown is
    /// joined with the loops it wakes. `wait()` returns immediately if
    /// shutdown already started, so spawn order doesn't matter.
    pub fn shutdownWatchLoop(self: *Node) void {
        _ = self.shutdown.waitForStart(.shutdown, null);
        self.proposal_ch.close(.graceful);
        // Wake mainLoop's blocked wait() so it observes shutdown and returns
        // (Notify has no close — a notify is the shutdown poke).
        self.event_notify.notify();
        // Wake applyLoop + any pending proposeAndWaitForApply / waitForApplied
        // callers with error.Closed.
        self.commit_index.close();
        self.applied_ts.close();
        // Graceful, not immediate: buffered callbacks stay drainable for
        // deinit's failAllPendingCallbacks to fail with .shutdown
        // (immediate would clear them and wedge their waiters).
        self.apply_waiters.close(.graceful);
    }

    pub fn isShuttingDown(self: *const Node) bool {
        return self.shutdown.isStopping();
    }

    /// The oldest timestamp a voter other than this node still reads
    /// at, from what each last reported in an AppendEntries reply
    /// (`Peer.min_read_ts`); maxInt when none constrains: this node is
    /// not the leader, has no voting peer, or has heard from none
    /// lately. For the state machine, which asks once per snapshot it
    /// proposes, so it is worked out here and not kept.
    ///
    /// A voter not heard from for an election timeout is left out: it
    /// is down or cut off, and holding the floor for it would stop
    /// garbage collection for the group. A read it still has open is
    /// refused, not misread, once it catches up to a floor above it.
    pub fn peersMinReadTs(self: *const Node) u64 {
        var oldest: u64 = std.math.maxInt(u64);
        if (self.state != .leader) return oldest;

        const stale = self.config.election_timeout_max_ticks;
        var it = self.peers.valueIterator();
        while (it.next()) |peer| {
            if (peer.role != .voter or peer.min_read_ts == 0) continue;
            if (self.tick_count - peer.min_read_tick > stale) continue;
            oldest = @min(oldest, peer.min_read_ts);
        }
        return oldest;
    }

    /// Returns true if a heartbeat is due (leader only).
    pub fn tick(self: *Node) RaftError!bool {
        self.tick_count += 1;
        self.elapsed_ticks += 1;
        switch (self.state) {
            .leader => if (self.elapsed_ticks >= self.config.heartbeat_ticks) {
                self.elapsed_ticks = 0;
                return true;
            },
            .follower, .candidate => if (self.elapsed_ticks >= self.election_timeout) {
                dbg(
                    "election-timeout tick={d} node={d} state={s} term={d} elapsed={d} timeout={d}",
                    .{
                        self.tick_count,
                        self.id,
                        @tagName(self.state),
                        self.current_term,
                        self.elapsed_ticks,
                        self.election_timeout,
                    },
                );
                if (self.config.enable_prevote) {
                    try self.startPreVote();
                } else {
                    try self.startElection();
                }
            },
            .learner => {},
        }
        return false;
    }

    // handleInbound would be called serially by mainLoop.
    pub fn handleInbound(self: *Node, buf: Inbound) RaftError!void {
        switch (zen.parseMessageWithTail(buf) catch return RaftError.InvalidMessage) {
            .RequestVote => |rv| try self.handleRequestVote(rv),
            .RequestVoteReply => |rvr| try self.handleRequestVoteReply(rvr),
            .AppendEntries => |ae| try self.handleAppendEntries(ae),
            .AppendEntriesReply => |aer| try self.handleAppendEntriesReply(aer),
            else => return RaftError.InvalidMessage,
        }
    }

    pub fn startPreVote(self: *Node) RaftError!void {
        if (self.state == .learner) return;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout(.start_prevote);
        dbg(
            "start-prevote tick={d} node={d} term={d} last_idx={d} last_term={d}",
            .{ self.tick_count, self.id, self.current_term, self.log.lastIndex(), self.log.lastTerm() },
        );

        self.prevotes_received.clearRetainingCapacity();
        try self.prevotes_received.put(self.id, {});

        // Single-node cluster: self-vote is already quorum.
        if (self.prevotes_received.count() >= self.quorumSize()) {
            return try self.startElection();
        }

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            try self.sendRequestVote(kv.key_ptr.*, true);
        }
    }

    pub fn startElection(self: *Node) RaftError!void {
        if (self.state == .learner) return;
        self.state = .candidate;
        self.current_term += 1;
        self.voted_for = self.id;
        self.leader_id = null;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout(.start_election);
        dbg(
            "start-election tick={d} node={d} term={d} timeout={d}",
            .{ self.tick_count, self.id, self.current_term, self.election_timeout },
        );

        self.votes_received.clearRetainingCapacity();
        try self.votes_received.put(self.id, {});

        try self.persistHardState();

        // Single-node cluster: self-vote is already quorum.
        if (self.votes_received.count() >= self.quorumSize()) {
            return try self.becomeLeader();
        }

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            try self.sendRequestVote(kv.key_ptr.*, false);
        }
    }

    pub fn becomeLeader(self: *Node) RaftError!void {
        if (self.state == .learner) return;
        self.state = .leader;
        self.leader_id = self.id;
        self.elapsed_ticks = 0;
        // Info, not debug: one line per election, and the e2e scripts
        // and benchmarks/run wait for it as the ready signal.
        raft_log.info(
            "become-leader tick={d} node={d} term={d}",
            .{ self.tick_count, self.id, self.current_term },
        );

        var it = self.peers.iterator();
        while (it.next()) |kv| {
            kv.value_ptr.next_index = 1;
            kv.value_ptr.last_index = 0;
        }

        // The term's first entry, queued for the loop to append. On
        // bootstrap it is the self-add conf change, so joiners learn
        // our address from the log; after that an empty entry, as the
        // paper's no-op: a leader commits prior-term entries only
        // behind one of its own, and nothing else is sure to propose.
        if (self.log.lastIndex() == 0) {
            raft_log.info("node {d}: bootstrap self-add as voter with addr={s}", .{ self.id, self.config.listen_addr });
            self.proposeConfChange(.{
                .change_type = .add_voter,
                .node_id = self.id,
                .addr = self.config.listen_addr,
            }, null) catch |err| {
                raft_log.err("node {d}: bootstrap self-add failed: {s}", .{ self.id, @errorName(err) });
            };
        } else {
            self.propose(&.{}, .manifest, null) catch |err| {
                raft_log.err("node {d}: term no-op failed: {s}", .{
                    self.id, @errorName(err),
                });
            };
        }

        self.maybeBroadcast(true);
    }

    /// Send AppendEntries only to peers that are behind (need entries or commit update).
    pub fn maybeBroadcast(self: *Node, do_send: bool) void {
        if (!do_send) return;
        if (self.state != .leader) return;
        const tr = self.transport orelse return;
        const cutoff = self.config.lockstep_cutoff;

        // Pass 1: highest next_index across peers — the "leading" position.
        var top: u64 = 0;
        {
            var it = self.peers.iterator();
            while (it.next()) |kv| {
                if (kv.value_ptr.*.next_index > top) top = kv.value_ptr.*.next_index;
            }
        }
        if (top == 0) return; // no peers at all
        const window_lo = if (top > cutoff) top - cutoff else 0;

        // Pass 2: min next_index of peers in [window_lo, top]. That cluster
        // shares one AE built at cluster_min — followers ahead walk past
        // duplicate prefix entries (term-match check) and only WAL-append
        // the new tail. Stragglers (below window_lo) get their own AE.
        var cluster_min: u64 = top;
        {
            var it = self.peers.iterator();
            while (it.next()) |kv| {
                const p = kv.value_ptr.*;
                if (p.next_index >= window_lo and p.next_index < cluster_min) {
                    cluster_min = p.next_index;
                }
            }
        }

        // Build the cluster AE once. Even single-peer clusters take this
        // path — building once + RefValue overhead is sub-µs, and the
        // common steady-state has all peers in the same cluster.
        const cluster_value = self.buildAppendEntries(cluster_min) catch return;
        defer cluster_value.decrRef();

        // Pass 3: send. In-window peers share the cluster AE; stragglers
        // build & send their own.
        var it = self.peers.iterator();
        while (it.next()) |kv| {
            const p = kv.value_ptr.*;
            if (p.next_index >= window_lo) {
                tr.sendMsg(kv.key_ptr.*, .p0, _serial.msg_type_raft, cluster_value) catch {};
            } else {
                self.sendAppendEntries(kv.key_ptr.*) catch {};
            }
        }
    }

    /// Build an AppendEntries starting from `from_index`, wrapped in a
    /// refcounted `RefValue` (caller owns one ref). Used both by the per-peer
    /// send path and the grouped fan-out path in `maybeBroadcast` — the
    /// latter shares the same RefValue across multiple peers.
    fn buildAppendEntries(self: *Node, from_index: u64) RaftError!*utils.RefValue {
        const prev_index = if (from_index == 0) 0 else from_index - 1;
        const prev_term: u64 = if (prev_index == 0) 0 else (try self.log.term(prev_index)) orelse 0;
        var result = try self.log.entriesRange(.{
            .lo = from_index,
            .hi = from_index + self.config.max_entries_per_msg,
            .max_bytes = 32 * 1024 * 1024, // 32 MiB
        });
        defer result.deinit();

        var buf = std.ArrayList(u8).empty;
        errdefer buf.deinit(self.allocator);

        var ae_writer = try zen.AppendEntriesWriter.initWithBuf(self.allocator, &buf);
        defer ae_writer.deinit();

        var entry_offsets = std.ArrayList(u32).empty;
        defer entry_offsets.deinit(self.allocator);
        try entry_offsets.ensureTotalCapacity(self.allocator, result.entries.len);

        for (result.entries) |entry_ref| {
            const entry_reader = try zen.LogEntryReader.parseRoot(entry_ref.bytes);
            const entry_off = try entry_reader.cloneInto(self.allocator, &buf);
            try entry_offsets.append(self.allocator, entry_off);
        }

        try ae_writer.set(.{
            .term = self.current_term,
            .leader_id = self.id,
            .prev_log_index = prev_index,
            .prev_log_term = prev_term,
            .entries = entry_offsets.items,
            .leader_commit = self.commit_index.load(),
        });
        _ = try ae_writer.finishWithTail();
        const bytes = try buf.toOwnedSlice(self.allocator);
        return utils.RefValue.init(self.allocator, bytes) catch |err| {
            self.allocator.free(bytes);
            return err;
        };
    }

    pub fn sendAppendEntries(self: *Node, peer: NodeId) RaftError!void {
        if (self.state != .leader) return;
        const state = self.peers.getPtr(peer) orelse return;
        const tr = self.transport orelse return;
        const value = try self.buildAppendEntries(state.next_index);
        defer value.decrRef();
        tr.sendMsg(peer, .p0, _serial.msg_type_raft, value) catch |err| {
            utils.logEvery(.debug, 5, "sendMsg to {d} failed: {s}", .{
                peer, @errorName(err),
            });
        };
    }

    fn handleRequestVote(self: *Node, rv: zen.RequestVoteReader) RaftError!void {
        const rv_term = rv.term();
        const is_prevote = rv.isPrevote();
        const candidate_id = rv.candidateId();
        const last_log_index = rv.lastLogIndex();
        const last_log_term = rv.lastLogTerm();
        if (!is_prevote and rv_term > self.current_term) {
            try self.stepDown(rv_term);
        }

        if (rv_term < self.current_term) {
            dbg(
                "vote-reject tick={d} node={d} candidate={d} reason=stale_term rv_term={d} cur_term={d}",
                .{ self.tick_count, self.id, candidate_id, rv_term, self.current_term },
            );
            try self.sendRequestVoteReply(candidate_id, false, is_prevote);
            return;
        }

        if (self.state == .learner) {
            dbg(
                "vote-reject tick={d} node={d} candidate={d} reason=self_is_learner",
                .{ self.tick_count, self.id, candidate_id },
            );
            try self.sendRequestVoteReply(candidate_id, false, is_prevote);
            return;
        }

        // Only vote for known voter peers.
        if (self.peers.get(candidate_id)) |peer| {
            if (peer.role == .learner) {
                dbg(
                    "vote-reject tick={d} node={d} candidate={d} reason=candidate_is_learner",
                    .{ self.tick_count, self.id, candidate_id },
                );
                try self.sendRequestVoteReply(candidate_id, false, is_prevote);
                return;
            }
        } else {
            dbg(
                "vote-reject tick={d} node={d} candidate={d} reason=unknown_peer",
                .{ self.tick_count, self.id, candidate_id },
            );
            try self.sendRequestVoteReply(candidate_id, false, is_prevote);
            return;
        }

        const up_to_date = self.isLogUpToDate(last_log_term, last_log_index);
        const can_vote = self.voted_for == null or self.voted_for.? == candidate_id;

        var granted = false;
        if (is_prevote) {
            granted = up_to_date;
        } else if (can_vote and up_to_date) {
            self.voted_for = candidate_id;
            self.elapsed_ticks = 0;
            self.resetElectionTimeout(.grant_vote);
            try self.persistHardState();
            granted = true;
        }
        if (is_prevote) {
            dbg(
                "prevote tick={d} node={d} candidate={d} term={d} granted={} up_to_date={} cand_last=({d},{d}) self_last=({d},{d})",
                .{
                    self.tick_count,
                    self.id,
                    candidate_id,
                    rv_term,
                    granted,
                    up_to_date,
                    last_log_term,
                    last_log_index,
                    self.log.lastTerm(),
                    self.log.lastIndex(),
                },
            );
        } else {
            dbg(
                "vote tick={d} node={d} candidate={d} term={d} granted={} can_vote={} up_to_date={} voted_for={any} cand_last=({d},{d}) self_last=({d},{d})",
                .{
                    self.tick_count,
                    self.id,
                    candidate_id,
                    rv_term,
                    granted,
                    can_vote,
                    up_to_date,
                    self.voted_for,
                    last_log_term,
                    last_log_index,
                    self.log.lastTerm(),
                    self.log.lastIndex(),
                },
            );
        }

        try self.sendRequestVoteReply(candidate_id, granted, is_prevote);
    }

    fn handleRequestVoteReply(self: *Node, rvr: zen.RequestVoteReplyReader) RaftError!void {
        const is_prevote = rvr.isPrevote();
        const term = rvr.term();
        if (!is_prevote and term > self.current_term) {
            try self.stepDown(term);
            return;
        }
        if (term < self.current_term) return;
        if (!rvr.voteGranted()) return;

        const from = rvr.from();
        if (is_prevote) {
            if (self.state == .learner) return;
            _ = try self.prevotes_received.put(from, {});
            dbg(
                "prevote-reply tick={d} node={d} from={d} term={d} count={d} quorum={d}",
                .{
                    self.tick_count,
                    self.id,
                    from,
                    self.current_term,
                    self.prevotes_received.count(),
                    self.quorumSize(),
                },
            );
            if (self.prevotes_received.count() >= self.quorumSize()) {
                try self.startElection();
            }
            return;
        }

        if (self.state != .candidate) return;
        _ = try self.votes_received.put(from, {});
        dbg(
            "vote-reply tick={d} node={d} from={d} term={d} count={d} quorum={d}",
            .{
                self.tick_count,
                self.id,
                from,
                self.current_term,
                self.votes_received.count(),
                self.quorumSize(),
            },
        );
        if (self.votes_received.count() >= self.quorumSize()) {
            try self.becomeLeader();
        }
    }

    pub fn handleAppendEntries(self: *Node, ae: zen.AppendEntriesReader) RaftError!void {
        const ae_term = ae.term();
        const leader_id = ae.leaderId();
        if (ae_term > self.current_term) {
            try self.stepDown(ae_term);
        }
        if (ae_term < self.current_term) {
            try self.sendAppendEntriesReply(leader_id, .{
                .success = false,
                .last_index = 0,
                .conflict_term = 0,
                .conflict_index = self.log.lastIndex() + 1,
            });
            return;
        }

        if (self.state != .follower and self.state != .learner) {
            self.state = .follower;
        }
        self.leader_id = leader_id;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout(.append_entries);

        if (ae.prevLogIndex() > 0) {
            const prev_term_opt = try self.log.term(ae.prevLogIndex());
            if (prev_term_opt == null or prev_term_opt.? != ae.prevLogTerm()) {
                try self.sendAppendEntriesReply(leader_id, .{
                    .success = false,
                    .last_index = 0,
                    .conflict_term = prev_term_opt orelse 0,
                    .conflict_index = if (prev_term_opt) |t| self.log.firstIndexOfTerm(t) else self.log.lastIndex() + 1,
                });
                return;
            }
        }

        var append_from: ?usize = null;
        const ae_entries_len = ae.entriesLen();
        if (ae_entries_len > 0) {
            var idx: u32 = 0;
            while (idx < ae_entries_len) : (idx += 1) {
                const log_entry = try ae.entriesAt(idx);
                const term_opt = try self.log.term(log_entry.index());
                if (term_opt == null or term_opt.? != log_entry.term()) {
                    append_from = @intCast(idx);
                    break;
                }
            }

            var entries_to_append = std.ArrayList(zen.LogEntryBuffer).empty;
            defer {
                for (entries_to_append.items) |*entry| {
                    entry.deinit();
                }
                entries_to_append.deinit(self.allocator);
            }

            const start_idx: usize = if (append_from) |v| v else ae_entries_len;
            for (start_idx..ae_entries_len) |append_idx| {
                const entry_reader = try ae.entriesAt(@intCast(append_idx));
                var entry_bytes: std.ArrayList(u8) = .empty;
                errdefer entry_bytes.deinit(self.allocator);

                const entry_off =
                    try entry_reader.cloneInto(self.allocator, &entry_bytes);
                try zen.addTail(self.allocator, &entry_bytes, zen.LogEntryReader, entry_off);
                const owned = try entry_bytes.toOwnedSlice(self.allocator);

                const entry = try zen.LogEntryBuffer.initWithTail(self.allocator, owned, .owned);
                try entries_to_append.append(self.allocator, entry);
            }
            if (entries_to_append.items.len > 0) {
                dbg("node {d}: appending {d} entries to log", .{ self.id, entries_to_append.items.len });
                try self.log.addEntries(entries_to_append.items);
                // TODO: Follower can be optimized to not run sync, and reply
                // back with synced index. In fact, as synced index advances, it
                // can reply back to leader without a trigger.
                try self.log.sync();
            }
        }

        // utils.logEvery(.debug, 10, "node {d}: handleAE entries={d} " ++
        //     "leaderCommit={d} ci={d} lastIndex={d}", .{
        //     self.id, ae_entries_len, ae.leaderCommit(), self.commit_index.load(), self.log.lastIndex(),
        // });

        if (ae.leaderCommit() > self.commit_index.load()) {
            const last_index = self.log.lastIndex();
            const new_ci = if (ae.leaderCommit() < last_index) ae.leaderCommit() else last_index;
            dbg("node {d}: advancing commit_index {d} -> {d}", .{ self.id, self.commit_index.load(), new_ci });
            self.commit_index.advance(new_ci);
        }
        try self.sendAppendEntriesReply(leader_id, .{
            .success = true,
            .last_index = self.log.lastIndex(),
            .conflict_term = 0,
            .conflict_index = 0,
        });
    }

    pub fn handleAppendEntriesReply(self: *Node, aer: zen.AppendEntriesReplyReader) RaftError!void {
        if (self.state != .leader) return;

        const term = aer.term();
        if (term < self.current_term) return;
        if (term > self.current_term) {
            try self.stepDown(term);
            return;
        }

        const from = aer.from();
        // The peer's oldest read rides on every reply, accepted or not.
        if (self.peers.getPtr(from)) |state| {
            state.min_read_ts = aer.minReadTs();
            state.min_read_tick = self.tick_count;
        }
        if (!aer.success()) {
            if (self.peers.getPtr(from)) |state| {
                const conflict_index = aer.conflictIndex();
                if (conflict_index > 0) {
                    state.next_index = conflict_index;
                } else if (state.next_index > 1) {
                    state.next_index -= 1;
                }
            }
            return;
        }

        if (self.peers.getPtr(from)) |state| {
            const last_idx = aer.lastIndex();
            state.last_index = last_idx;
            state.next_index = last_idx + 1;
        }
    }

    /// Encode a ConfChange and propose it through the normal proposal queue.
    /// For fire-and-forget (e.g., join requests), use a no-op callback.
    pub fn proposeConfChange(self: *Node, cc: ConfChange, w: ?*ProposalWaiter) RaftError!void {
        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(self.allocator);

        dbg("proposeConfChange: node_id={d} type={s} addr_len={d} addr={s}", .{
            cc.node_id, @tagName(cc.change_type), cc.addr.len, cc.addr,
        });

        var cc_writer = try zen.ConfChangeWriter.initWithBuf(self.allocator, &buf);
        defer cc_writer.deinit();
        try cc_writer.set(.{
            .change_type = cc.change_type,
            .node_id = cc.node_id,
            .addr = cc.addr,
        });
        _ = try cc_writer.finishWithTail();
        try self.propose(buf.items, .conf, w);
    }

    /// Marshal a SnapshotStruct as the proposal payload and propose it
    /// through the raft queue with `entry_type=.snapshot`. Followers receive
    /// the byte-identical proposal via AppendEntries; on apply, every node
    /// (leader included) installs via `Node.applySnapshotEntry`.
    pub fn proposeSnapshot(self: *Node, snap: _diskwal.Snapshot) RaftError!void {
        const buf = try zen.marshalRoot(self.allocator, snap);
        defer self.allocator.free(buf);

        try self.proposeAndWaitForApply(buf, .snapshot);
    }

    /// Propose a data change (normal mutation) through the raft proposal
    /// queue; `w` wakes at commit.
    /// Synchronous propose-and-block-until-applied wrapper. The wait doubles
    /// as a rate-limit: the caller fiber stays parked until commit + apply,
    /// so it can't pile up more than one in-flight proposal. `proposal_type`
    /// becomes the wire-level `entry_type`, which lets the snapshot builder
    /// filter manifest entries via `entriesRange(.ty = .manifest)`.
    ///
    /// Apply-not-just-commit matters for control-plane proposals: e.g. a
    /// memtable flush proposal's apply pops the immutable from the queue,
    /// and the next iteration of the flush loop must observe that. Returning
    /// at commit instead let the loop see the same memtable still queued and
    /// re-flush it, producing two L0 SSTs with identical (primary, ts) data
    /// → MergeIterator yields duplicates → builder rejects → KeyOrderViolation.
    /// User-facing writes go through `Engine.proposeWrites` (waits on
    /// commit) and are unaffected.
    pub fn proposeAndWaitForApply(self: *Node, data: String, proposal_type: ProposalType) RaftError!void {
        std.debug.assert(proposal_type != .any);
        const start_ms = utils.coarseMs();

        var w: ProposalWaiter = .{ .wait_for = .apply };
        try self.propose(data, proposal_type, &w);

        fiber.ctx().record("proposed", .{});
        const status = w.wait();
        fiber.ctx().record("committed and applied", .{});

        const took_ms = utils.coarseMs() - start_ms;
        if (took_ms > 5_000) {
            // The timeline's slot, if the ring still has it, says where
            // the time went; `woken` was recorded by `wait`.
            if (_metrics.timeline().lookup(w.entry.index)) |slot| {
                raft_log.warn("proposeAndWaitForApply: commit+apply slow type={s} ms={d} index={d}: {f}", .{
                    @tagName(proposal_type), took_ms, w.entry.index, slot,
                });
            } else {
                raft_log.warn("proposeAndWaitForApply: commit+apply slow type={s} ms={d} index={d}", .{
                    @tagName(proposal_type), took_ms, w.entry.index,
                });
            }
        }
        if (status != .committed) return error.NotCommitted;
        // Woken after this node applied the entry (and after applied_ts
        // moved past it), so there is no second wait.
        std.debug.assert(self.applied_ts.load() >= w.entry.timestamp);
    }

    /// How a proposal's data is stored in its log entry. Every byte of
    /// a proposal is written to the WAL, on each node, and shipped to
    /// each follower, so a payload that shrinks is cheaper everywhere
    /// at once. The entry's `codec` byte says which form it is in, and
    /// an entry decodes on its own, with no other entry's bytes.
    ///
    /// lz4, not zstd: entries are applied by one fiber, and lz4 decodes
    /// several times faster for somewhat less saved. Below 512 bytes a
    /// proposal is not tried: the durable write of a small entry costs
    /// the same whatever its size. Above, it is kept compressed only
    /// when that saves an eighth.
    const data_pack_rule: compression.PackRule = .{
        .codec = .lz4,
        .min_bytes = 1024,
        .min_saving = 1.0 / 8.0,
    };
    /// Queue a proposal for the main loop; `w` gets the outcome (see
    /// `ProposalWaiter`), or null when nobody wants it.
    pub fn propose(self: *Node, data: String, proposal_type: ProposalType, w: ?*ProposalWaiter) RaftError!void {
        if (self.isShuttingDown()) return error.QueueClosed;

        // How long propose polls `StateMachine.isReady` before
        // returning `error.NotReady`. Long enough to absorb startup (recovery +
        // the leader's bootstrap `newMemtable` round-trip), short enough that a
        // client doesn't sit forever if the SM is genuinely stuck.
        if (self.state_machine) |sm| {
            if (proposal_type == .data and !sm.isReady()) {
                const deadline = utils.coarseMs() + 1000; // 1s
                while (!sm.isReady()) {
                    if (utils.coarseMs() >= deadline) return error.NotReady;
                    fiber.sleepMs(10);
                }
            }
        }

        // Compressed here, in the proposer's fiber, where every kind of
        // proposal passes: the log, the WAL and the followers only ever
        // see the stored form (see `data_pack_rule`).
        // The worker is read here and used at once: nothing from this
        // line to the copy below yields, so the fiber stays on it.
        const scratch = &self.pack_scratch[fiber.currentWorker().id];
        const stored = try compression.maybePack(
            self.allocator,
            data,
            data_pack_rule,
            scratch,
        );
        // The one allocation a proposal costs: its stored bytes, exact
        // size, owned by the request from here on.
        const owned = try self.allocator.dupe(u8, stored.data);
        errdefer self.allocator.free(owned);
        self.proposal_ch.trySend(.{
            .data = owned,
            .codec = stored.codec,
            .proposal_type = proposal_type,
            .waiter = w,
            .proposed_us = nowUs(),
        }) catch |err| switch (err) {
            error.ChannelClosed => return error.QueueClosed,
            error.ChannelFull => return error.QueueFull,
        };
        self.event_notify.notify();
    }

    /// Append the successful subset of `requests` to the leader's log,
    /// assigning each a fresh HLC timestamp under the propose lock so log
    /// order equals timestamp order. Each successfully-appended request's
    /// waiter is queued (`commit_waiters` or `apply_waiters`);
    /// rejected conf changes are finished inline and are NOT queued. If
    /// the queueing fails (channel closed during shutdown), the waiter
    /// is finished inline with `.shutdown`.
    fn appendProposalsToLeaderLog(
        self: *Node,
        requests: []const ProposalRequest,
    ) RaftError!void {
        if (self.state != .leader) return RaftError.NotLeader;
        if (requests.len == 0) return;

        // Sub-phase: propose_build — walk requests, validate + serialize each
        // into a LogEntryBuffer, register the apply callback. Pure CPU work
        // (no I/O); anything > 1 ms/batch points at zbuf encoding cost.
        const t_build = utils.coarseMs();

        var entries = std.ArrayList(zen.LogEntryBuffer).empty;
        defer {
            for (entries.items) |*entry| entry.deinit();
            entries.deinit(self.allocator);
        }
        try entries.ensureTotalCapacity(self.allocator, requests.len);

        var index = self.log.lastIndex() + 1;
        var has_conf = false;
        for (requests) |req| {
            const is_conf = req.proposal_type == .conf;
            if (is_conf) {
                if (self.pending_conf_change or has_conf) {
                    self.reject(req, .pending_conf_change);
                    continue;
                }
                // Its bytes are parsed below as they are. A conf change
                // is a few dozen bytes, far under what `propose`
                // compresses; one that arrives compressed is not one.
                if (req.codec != .none) {
                    self.reject(req, .invalid_req);
                    continue;
                }

                const cc = try zen.ConfChangeReader.init(req.data, 0);
                const ct = cc.changeType();
                // Reject the uninitialized default and any unknown variant from
                // a future schema (forward-compat catch-all).
                switch (ct) {
                    .add_voter, .add_learner, .to_voter, .to_learner, .remove_node, .update_addr => {},
                    .unknown, _ => {
                        self.reject(req, .invalid_req);
                        continue;
                    },
                }
                // Reject add if addr is missing or unparseable.
                if (ct == .add_voter or ct == .add_learner) {
                    _ = fiber.net.IpAddress.parseIpAndPort(cc.addr()) catch {
                        self.reject(req, .invalid_addr);
                        continue;
                    };
                    // Allow self-add (bootstrap), reject if peer already exists.
                    // Use update_addr to change an existing peer's address.
                    if (cc.nodeId() != self.id and self.peers.contains(cc.nodeId())) {
                        self.reject(req, .existing_node);
                        continue;
                    }
                }

                if (ct == .update_addr) {
                    _ = fiber.net.IpAddress.parseIpAndPort(cc.addr()) catch {
                        self.reject(req, .invalid_addr);
                        continue;
                    };
                    if (cc.nodeId() != self.id and !self.peers.contains(cc.nodeId())) {
                        self.reject(req, .unknown_node);
                        continue;
                    }
                }

                // Reject if the target peer doesn't exist for changes that require it.
                if (ct == .to_voter or ct == .to_learner or ct == .remove_node) {
                    if (cc.nodeId() != self.id and !self.peers.contains(cc.nodeId())) {
                        self.reject(req, .unknown_node);
                        continue;
                    }
                }

                // Reject if this would remove the last voter.
                if (self.quorum_size <= 1 and (ct == .to_learner or ct == .remove_node)) {
                    self.reject(req, .last_voter);
                    continue;
                }
                has_conf = true;
            }

            var buf = std.ArrayList(u8).empty;
            defer buf.deinit(self.allocator);

            var writer = try zen.LogEntryWriter.initWithBuf(self.allocator, &buf);
            defer writer.deinit();

            var cc_offset: u32 = zen.NullOffset;
            if (is_conf) {
                // req.data is a serialized ConfChange. Clone it into buf and get the offset.
                const cc_reader = try zen.ConfChangeReader.parseRoot(req.data);
                cc_offset = try cc_reader.cloneInto(self.allocator, &buf);
            }

            // Assign the HLC inside this critical section so log order matches
            // timestamp order — the invariant that makes applied_timestamp a
            // safe bound for consistent follower reads.
            const ts = utils.hlc.g().next().asU64();
            try writer.set(.{
                .index = index,
                .term = self.current_term,
                .timestamp = ts,
                .data = if (is_conf) &[_]u8{} else req.data,
                .entry_type = req.proposal_type,
                .conf_change = cc_offset,
                .codec = @intFromEnum(req.codec),
            });
            _ = try writer.finishWithTail();

            const data = try buf.toOwnedSlice(self.allocator);
            var entry = try zen.LogEntryBuffer.initWithTail(self.allocator, data, .owned);
            errdefer entry.deinit();

            try entries.append(self.allocator, entry);

            // Enqueue the pending apply callback. send() blocks on full channel
            // (backpressure if state-machine apply is lagging) and fails only
            // on channel closed (shutdown) or fiber cancellation — in either
            // case we fire the callback inline with .shutdown so the client
            // isn't left hanging.
            const tl = _metrics.timeline();
            tl.record(index, .proposed, req.proposed_us);
            tl.record(index, .buffered, nowUs());
            if (req.waiter) |w| {
                w.entry = .{ .index = index, .term = self.current_term, .timestamp = ts };
                switch (w.wait_for) {
                    .commit => self.commit_waiters.append(self.allocator, w) catch w.finish(.shutdown),
                    .apply => self.apply_waiters.send(w) catch w.finish(.shutdown),
                }
            }

            index += 1;
        }

        _metrics.g().observePhase("propose_build", t_build);

        // Sub-phase: propose_persist — DiskWAL.append batches the framed
        // records into one pwrite. The fdatasync is no longer issued here;
        // mainLoop spawns it in a fiber so it can overlap with broadcast.
        const t_persist = utils.coarseMs();
        try self.log.addEntries(entries.items);
        _metrics.g().observePhase("log_append", t_persist);

        if (has_conf) self.pending_conf_change = true;
    }

    // processProposalRequests is just checking for proposals in proposal_requests queue, and
    // proposing them. This is only relevant to the leader node. Followers won't have this.
    pub fn processProposalRequests(self: *Node) RaftError!usize {
        if (self.isShuttingDown()) {
            while (self.proposal_ch.tryReceive()) |req| {
                self.allocator.free(req.data);
                self.reject(req, .shutdown);
            } else |_| {}
            return 0;
        }

        var batch = std.ArrayList(ProposalRequest).empty;
        defer batch.deinit(self.allocator);

        while (true) {
            const req = self.proposal_ch.tryReceive() catch break;
            try batch.append(self.allocator, req);
        }
        // Drain depth — emitted before the early-out so a separate "queue was
        // empty" sample isn't accidentally generated (observeBatchSize skips
        // on count==0 anyway, but the early return below would skip it too).
        _metrics.g().observeBatchSize(batch.items.len);
        if (batch.items.len == 0) return 0;

        defer for (batch.items) |req| self.allocator.free(req.data);

        // appendProposalsToLeaderLog queues each successful entry's
        // waiter with the assigned HLC timestamp. Rejected conf changes
        // were finished inline from within the function, so there's no
        // per-request loop here.
        self.appendProposalsToLeaderLog(batch.items) catch |err| switch (err) {
            error.NotLeader => {
                for (batch.items) |req| {
                    self.reject(req, .not_leader);
                }
                return batch.items.len;
            },
            else => return err,
        };

        return batch.items.len;
    }

    pub fn blockingRunAllLoops(self: *Node) !void {
        // Every loop self-exits when `isShuttingDown()` flips, so we don't
        // need `cancel()` — `wait()` suffices.
        //
        // Pin the three hot Raft loops to distinct raft-lane workers via
        // high-priority `worker_id` placement — mainLoop (consensus) on worker
        // 0, applyLoop (state-machine apply) on worker 1, tickLoop (election/
        // heartbeat ticks) on worker 2.`worker_id` is
        // lane-relative and wraps if the raft lane has < 3 workers.
        var g = fiber.Group(void).init(self.allocator);
        defer g.deinit();
        try g.ensureTotalCapacity(6);

        try g.spawn(.{ .lane = .raft, .priority = .high, .worker_id = 0 }, mainLoop, .{ self, self.config.tick_interval_ms });
        try g.spawn(.{ .lane = .raft, .priority = .high, .worker_id = 1 }, applyLoop, .{self});
        try g.spawn(.{ .lane = .raft, .priority = .high, .worker_id = 2 }, tickLoop, .{ self, self.config.tick_interval_ms });
        try g.spawn(.{ .lane = .raft }, clockTickLoop, .{self});
        try g.spawn(.{ .lane = .raft }, commitPersistLoop, .{self});
        // Reacts to the shutdown signal (from wherever it fires) by closing
        // everything the loops above can park on, then exits with them.
        try g.spawn(.{ .lane = .raft }, shutdownWatchLoop, .{self});

        g.wait();
    }

    pub fn applyLoop(self: *Node) void {
        // TODO: Use this loop to replay all the logs from last snapshot.
        // Actually, if we just set the apply index to the last snapshot index, then this would
        // automatically apply all the pending entries anyway. We just need to trigger it.
        while (!self.isShuttingDown()) {
            self.applyCommits();
            const target = self.apply_index.load(.acquire) + 1;
            self.commit_index.waitForUntimed(target) catch |err| {
                if (err != error.Closed) {
                    raft_log.err("applyLoop: waitFor commit >= {d}: {s}", .{ target, @errorName(err) });
                }
                return;
            };
        }
    }

    const max_apply_bytes: u64 = 16 * 1024 * 1024; // 16 MiB per batch

    pub fn applyCommits(self: *Node) void {
        var ai = self.apply_index.load(.acquire);
        const ci = self.commit_index.load();
        if (ai >= ci) {
            return;
        }
        dbg(
            "applyCommits.1 ticks: {d} node {d}: applyCommits commit_index={d} apply_index={d}",
            .{ self.tick_count, self.id, ci, ai },
        );

        while (ai < ci) {
            utils.logEvery(.debug, 1, "applyCommits progress: ai={d} " ++
                "ci={d} remaining={d}", .{ ai, ci, ci - ai });
            const now = nowUs();
            var result = self.log.entriesRange(.{ .lo = ai + 1, .hi = ci + 1, .max_bytes = max_apply_bytes }) catch return;
            _metrics.g().observeStage("entries_range", nowUs() - now);
            defer result.deinit();
            if (result.entries.len == 0) return;

            for (result.entries) |entry| {
                const reader = zen.LogEntryReader.parseRoot(entry.bytes) catch continue;

                const ety = reader.entryType();
                // The data as proposed: decoded here, once, for whichever
                // branch below takes it. Failing is retried like a failed
                // apply, so the fiber stays responsive to shutdown.
                const data = while (true) {
                    break compression.maybeUnpack(
                        self.allocator,
                        reader.codec(),
                        reader.data(),
                        &self.apply_scratch,
                    ) catch |err| {
                        utils.logEvery(.err, 5, "entry idx={d}: decode " ++
                            "data: {s}, retrying", .{
                            entry.index, @errorName(err),
                        });
                        if (self.isShuttingDown()) return;
                        fiber.sleepMs(1);
                        continue;
                    };
                };
                if ((ety == .data or ety == .manifest) and data.len > 0) {
                    if (self.state_machine) |s| {
                        // One observation per state-machine apply call —
                        // each carries one Raft entry's worth of work
                        // (e.g. a batched forest.put). Lets us see whether
                        // the apply path is the bottleneck when commit
                        // outpaces apply.
                        const t_apply = utils.coarseMs();
                        // Timeline: queueing behind earlier entries, and the
                        // apply call itself, split.
                        const picked_us = nowUs();
                        _metrics.timeline().record(entry.index, .picked, picked_us);
                        const sm_entry = Entry{
                            .index = entry.index,
                            .term = entry.term,
                            .timestamp = reader.timestamp(),
                        };
                        // Retry on apply failure with a 1 ms backoff so the
                        // raft fiber stays responsive to shutdown. The state
                        // machine must NOT do its own retry loop — it would
                        // wedge shutdown.
                        //
                        // A 1 ms retry would print a thousand lines a
                        // second: both lines are rate-limited, the
                        // expected `RecoveryInProgress` of startup at
                        // debug, a real failure at err.
                        while (true) {
                            s.apply(sm_entry, data) catch |err| {
                                if (err == error.RecoveryInProgress) {
                                    utils.logEvery(.debug, 5, "apply " ++
                                        "idx={d}: recovery in progress, " ++
                                        "retrying", .{entry.index});
                                } else {
                                    utils.logEvery(.err, 5, "apply idx={d}: " ++
                                        "{s}, retrying", .{
                                        entry.index, @errorName(err),
                                    });
                                }
                                if (self.isShuttingDown()) return;
                                fiber.sleepMs(1);
                                continue;
                            };
                            break;
                        }
                        _metrics.g().observePhase("apply", t_apply);
                        _metrics.g().observeStage(
                            if (ety == .manifest) "apply_manifest" else "apply_data",
                            nowUs() - picked_us,
                        );
                    }
                } else if (ety == .snapshot and data.len > 0) {
                    self.applySnapshotEntry(data) catch |err| {
                        raft_log.err("applySnapshotEntry at index={d}: {s}", .{ entry.index, @errorName(err) });
                    };
                }
                ai = entry.index;
                _metrics.timeline().record(ai, .apply, nowUs());
                self.apply_index.store(ai, .release);
                // Advance applied AFTER the state machine has applied the
                // entry (or is a no-op for empty-data ticks). Matches the
                // read contract: "applied.ts >= T implies every entry with
                // timestamp <= T is visible." advance() broadcasts under
                // its own lock, so no reader can be mid-(check, suspend).
                self.applied_ts.advance(reader.timestamp());
                // Apply waiters wake here, after the entry is applied and
                // `applied_ts` has passed it; commit waiters woke in
                // `advanceCommitIndex`, without waiting for apply.
                self.processApplyWaiters(ai);
            }
        }
        dbg(
            "applyCommits.2 ticks: {d} node {d}: applyCommits ai={d} ci={d}",
            .{ self.tick_count, self.id, ai, ci },
        );
    }

    /// Fire the apply-time callbacks of every entry at or below `ai`,
    /// in index order (they were enqueued in it). Apply fiber only.
    fn processApplyWaiters(self: *Node, ai: u64) void {
        while (true) {
            const w = self.applied_held orelse
                (self.apply_waiters.tryReceive() catch return);
            if (w.entry.index > ai) {
                self.applied_held = w;
                return;
            }
            self.applied_held = null;
            w.finish(.committed);
        }
    }

    /// Tick loop: periodically notifies the event notifier to trigger raft processing.
    pub fn tickLoop(self: *Node, tick_interval_ms: u32) void {
        const tick_dur: u64 = tick_interval_ms;
        while (!self.isShuttingDown()) {
            fiber.sleepMs(tick_dur);
            self.event_notify.notify();
        }
    }

    /// Adaptive HLC tick loop. On the leader only: wakes every 400 ms and
    /// checks whether wall-clock seconds have advanced past the last log
    /// entry's HLC second. If yes, proposes an empty-data LogEntry so
    /// `apply_ts` advances on followers even during write-quiet periods.
    /// During active writes, each user mutation naturally advances the
    /// last log entry's second, so no tick fires. Followers are passive —
    /// they receive the leader's empty entries via normal AppendEntries
    /// replication and advance their own `apply_ts` on apply.
    ///
    /// The wait-for-commit doubles as a rate-limit: only one tick can be
    /// in flight at a time, so burst-piling under apply lag is impossible.
    pub fn clockTickLoop(self: *Node) void {
        const poll: u64 = 400;
        while (!self.isShuttingDown()) {
            fiber.sleepMs(poll);
            if (!self.isLeader()) continue;

            const last_ts = utils.hlc.HLC.fromU64(self.log.lastTimestamp());
            const wall_sec: u32 = @intCast(std.Io.Timestamp.now(utils.context.g().io, .real).toSeconds());
            if (wall_sec <= last_ts.seconds) continue;

            self.proposeAndWaitForApply(&.{}, .data) catch |err| {
                // The queue closes under a tick during shutdown.
                if (self.isShuttingDown()) continue;
                raft_log.warn("clock tick propose failed: {s}", .{@errorName(err)});
            };
        }
    }

    /// The WAL writer fiber's report after each batch: mark the
    /// timeline and poke mainLoop, whose advance() commits through
    /// `log.syncedIndex()`. The leader appends and broadcasts while the
    /// disk is busy (Raft paper §7 pipelining).
    fn onWritten(ctx: *anyopaque, written: DiskWAL.Written) void {
        const self: *Node = @ptrCast(@alignCast(ctx));
        const tl = _metrics.timeline();
        tl.recordRange(written.first_index, written.last_index, .write_start, written.started_us);
        tl.recordRange(written.first_index, written.last_index, .write_end, written.done_us);
        self.event_notify.notify();
    }

    /// The timeline's clock: monotonic microseconds, since it only
    /// takes differences.
    fn nowUs() i64 {
        return utils.timestampNow(.monotonic, .micros);
    }

    /// Main processing loop. Blocks on the event notifier, drains inbound messages,
    /// processes proposals, advances state, and broadcasts to peers.
    pub fn mainLoop(self: *Node, tick_interval_ms: u32) void {
        const tick_ms: i64 = @as(i64, tick_interval_ms);
        var last_tick: i64 = utils.coarseMs();

        while (!self.isShuttingDown()) {
            self.event_notify.wait();
            if (self.isShuttingDown()) return;

            // Phase: inbound — drain inbound_ch (handle AE, AER, RV replies).
            // We observe even when the queue was empty: the histogram includes
            // a 0-bucket sample, which is information about how often the loop
            // ran without inbound work to do.
            var t = utils.coarseMs();
            while (self.inbound_ch.tryReceive()) |msg| {
                defer self.allocator.free(msg.payload);
                switch (msg.message_type) {
                    msg_type_raft => {
                        self.handleInbound(msg.payload) catch {};
                    },
                    else => {},
                }
            } else |_| {}
            _metrics.g().observePhase("inbound", t);

            const ci_before = self.commit_index.load();
            const last_before = self.log.lastIndex();

            // Phase: propose — drain proposal queue, build entries, persist,
            // register apply callbacks. Sub-phases ("propose_build" and
            // "propose_persist") are observed inside appendProposalsToLeaderLog.
            t = utils.coarseMs();
            _ = self.processProposalRequests() catch {};
            _metrics.g().observePhase("propose", t);
            const log_grew = self.log.lastIndex() > last_before;

            // The appends above went to the WAL's write buffer and asked
            // its writer fiber for a round; `onWritten` pokes this loop
            // after each batch, and advance() below commits only through
            // `log.syncedIndex()`. mainLoop never waits on the disk.

            // Phase: advance — advanceCommitIndex + applyConfChanges + notify.
            t = utils.coarseMs();
            self.advance();
            _metrics.g().observePhase("advance", t);

            const ci_advanced = self.commit_index.load() > ci_before;

            var heartbeat_due = false;
            const now = utils.coarseMs();
            if (now - last_tick >= tick_ms) {
                heartbeat_due = self.tick() catch false;
                last_tick = now;
            }

            // Phase: broadcast — for multi-voter clusters, the WAL fsync
            // runs concurrently with these network sends.
            t = utils.coarseMs();
            self.maybeBroadcast(heartbeat_due or ci_advanced or log_grew);
            _metrics.g().observePhase("broadcast", t);
        }
    }

    pub fn isLeader(self: *const Node) bool {
        return self.state == .leader;
    }

    pub fn getLeaderId(self: *const Node) ?NodeId {
        return self.leader_id;
    }

    pub fn clusterSize(self: *const Node) u32 {
        return @intCast(self.peers.count() + 1);
    }

    /// Block until we're the leader, then propose an empty .manifest
    /// entry and wait for it to apply. It is appended behind the
    /// term's no-op from `becomeLeader`, so by Raft's prior-term
    /// commitment rule every prior entry in the WAL is committed and
    /// applied before it. Callers that need a fully-applied state
    /// machine before they start working — e.g. compactors that
    /// fetchAdd `next_file_id` for new SSTs — call this first to avoid
    /// claiming an id that an unapplied WAL-tail entry already used.
    ///
    /// Empty payload (`data.len == 0`) is recognised by the apply path
    /// as a no-op (no state-machine call), so this barrier doesn't do
    /// any redundant manifest work itself.
    pub fn waitForQuorum(self: *Node) RaftError!void {
        const interval: u64 = 50;
        while (!self.isShuttingDown()) {
            if (self.isLeader()) break;
            fiber.sleepMs(interval);
        }
        if (self.isShuttingDown()) return;
        try self.proposeAndWaitForApply(&.{}, .manifest);
    }

    pub fn persistHardState(self: *Node) RaftError!void {
        // Only the fields the raft mainLoop owns. commit_index is updated
        // by `commitPersistLoop`; raft_id by no one after init. The merge
        // happens atomically inside DiskWAL.setHardState, so concurrent
        // commit_index persists won't be clobbered.
        try self.log.setHardState(.{
            .current_term = self.current_term,
            .voted_for = self.voted_for orelse 0,
            // raft_id pins the metadata to this node so a misplaced data
            // dir can't be silently picked up by a different node id.
            .raft_id = self.id,
        });
        try self.log.sync();
    }

    /// Periodic background loop. Every 1 s, persists `commit_index` to
    /// HardState if it has advanced by at least `min_advance` entries
    /// since the last persisted value. Lets a clean restart skip the
    /// "wait for first current-term commit" delay before the WAL tail
    /// can replay (Raft's prior-term commitment rule). On crash, the
    /// staleness is bounded by ~1 s of entries; the rest is re-derived.
    pub fn commitPersistLoop(self: *Node) void {
        const interval: u64 = 1000;
        const min_advance: u64 = 100;
        while (!self.isShuttingDown()) {
            if (self.shutdown.waitForStart(.shutdown, interval)) break;

            const ci = self.commit_index.load();
            const persisted = self.log.hardState().commit_index;
            if (ci < persisted + min_advance) continue;
            self.log.setHardState(.{ .commit_index = ci }) catch |err| {
                raft_log.err("commitPersistLoop: setHardState failed: {s}", .{
                    @errorName(err),
                });
                continue;
            };
        }

        // Final persist on the way out: a graceful restart then applies the
        // full committed tail during recovery, BEFORE any listener serves —
        // otherwise a short-lived node (< min_advance entries) restarts at
        // the stale index and clients race the post-election re-commit.
        const ci = self.commit_index.load();
        if (ci > self.log.hardState().commit_index) {
            self.log.setHardState(.{ .commit_index = ci }) catch |err| {
                raft_log.err("commitPersistLoop: final setHardState " ++
                    "failed: {s}", .{@errorName(err)});
            };
        }
    }

    pub fn persistLog(self: *Node, entries: []const zen.LogEntryBuffer) RaftError!void {
        try self.log.addEntries(entries);
        try self.log.sync();
    }

    pub fn isLogUpToDate(self: *const Node, candidate_term: u64, candidate_index: u64) bool {
        const last_term = self.log.lastTerm();
        if (candidate_term > last_term) return true;
        if (candidate_term < last_term) return false;
        return candidate_index >= self.log.lastIndex();
    }

    fn sendEncoded(self: *Node, to: NodeId, payload: []u8) void {
        const tr = self.transport orelse {
            self.allocator.free(payload);
            return;
        };
        const value = utils.RefValue.init(self.allocator, payload) catch {
            self.allocator.free(payload);
            return;
        };
        defer value.decrRef();
        tr.sendMsg(to, .p0, _serial.msg_type_raft, value) catch |err| {
            utils.logEvery(.debug, 5, "sendMsg to {d} failed: {s}", .{
                to, @errorName(err),
            });
        };
    }

    fn decodeConfChange(self: *Node, reader: zen.ConfChangeReader) RaftError!ConfChange {
        _ = self;
        const change_type = reader.changeType();
        if (change_type == .unknown) return RaftError.InvalidMessage;
        return .{
            .change_type = change_type,
            .node_id = reader.nodeId(),
            .addr = reader.addr(),
        };
    }

    fn sendRequestVote(self: *Node, to: NodeId, is_prevote: bool) !void {
        var writer = try zen.RequestVoteWriter.init(self.allocator);
        defer writer.deinit();

        try writer.set(.{
            .term = self.current_term,
            .candidate_id = self.id,
            .last_log_index = self.log.lastIndex(),
            .last_log_term = self.log.lastTerm(),
            .is_prevote = is_prevote,
        });
        const out = try writer.finishWithTail();
        const bytes = out.bytes orelse return zen.Error.Invalid;
        self.sendEncoded(to, bytes);
    }

    fn sendRequestVoteReply(self: *Node, to: NodeId, vote_granted: bool, is_prevote: bool) !void {
        var writer = try zen.RequestVoteReplyWriter.init(self.allocator);
        defer writer.deinit();

        try writer.set(.{
            .term = self.current_term,
            .vote_granted = vote_granted,
            .from = self.id,
            .is_prevote = is_prevote,
        });
        const buf = try writer.finishWithTail();
        const bytes = buf.bytes orelse return zen.Error.Invalid;
        self.sendEncoded(to, bytes);
    }

    fn sendAppendEntriesReply(self: *Node, to: NodeId, opts: struct {
        success: bool,
        last_index: u64,
        conflict_term: u64,
        conflict_index: u64,
    }) !void {
        var writer = try zen.AppendEntriesReplyWriter.init(self.allocator);
        defer writer.deinit();

        try writer.set(.{
            .term = self.current_term,
            .success = opts.success,
            .from = self.id,
            .last_index = opts.last_index,
            .conflict_term = opts.conflict_term,
            .conflict_index = opts.conflict_index,
            .min_read_ts = if (self.state_machine) |sm| sm.minReadTs() else 0,
        });
        const buf = try writer.finishWithTail();
        const bytes = buf.bytes orelse return zen.Error.Invalid;
        self.sendEncoded(to, bytes);
    }

    pub fn quorumSize(self: *const Node) usize {
        return self.quorum_size;
    }

    fn advanceCommitIndex(self: *Node) void {
        if (self.state != .leader) return;

        var matches = std.ArrayList(u64).empty;
        defer matches.deinit(self.allocator);

        // Leader always has its own log up to lastIndex.
        // The leader's own vote: what is durable, not what is appended.
        matches.append(self.allocator, self.log.syncedIndex()) catch return;

        var it = self.peers.valueIterator();
        while (it.next()) |peer| {
            if (peer.role != .voter) continue;
            matches.append(self.allocator, peer.last_index) catch return;
        }

        if (matches.items.len == 0) return;

        std.mem.sort(u64, matches.items, {}, comptime std.sort.asc(u64));

        const quorum_pos = matches.items.len - self.quorum_size;
        const candidate = matches.items[quorum_pos];
        if (candidate <= self.commit_index.load()) return;

        const term = self.log.term(candidate) catch return orelse return;
        if (term != self.current_term) return;

        const prev = self.commit_index.load();
        self.commit_index.advance(candidate);
        _metrics.timeline().recordRange(prev + 1, candidate, .commit, nowUs());

        self.processCommitWaiters(candidate);
    }

    /// Fire the commit callbacks of every pending proposal at or below
    /// `ci`, in index order. Main loop only. Everything queued is this
    /// leader's: the leader commits only entries of its own term, and a
    /// step-down fails the queue before the term changes.
    fn processCommitWaiters(self: *Node, ci: u64) void {
        var n: usize = 0;
        for (self.commit_waiters.items) |w| {
            if (w.entry.index > ci) break;
            w.finish(.committed);
            n += 1;
        }
        if (n > 0) self.commit_waiters.replaceRangeAssumeCapacity(0, n, &.{});
    }

    // commit_index advances when leader sees a quorum match.
    // apply_index advances when entries are applied to the state machine (via callbacks).
    // conf_applied_index advances when conf changes are applied to raft membership.
    //
    // `commit_index.advance()` broadcasts internally on a real advance, so
    // applyLoop (state-machine apply) is woken without a separate notify
    // call; proposal commit callbacks fire right there in advanceCommitIndex.
    pub fn advance(self: *Node) void {
        self.advanceCommitIndex();
        self.applyConfChanges();
    }

    fn applyConfChanges(self: *Node) void {
        const ci = self.commit_index.load();
        if (self.conf_applied_index >= ci) return;
        dbg("applyConfChanges node={d} conf_applied={d} ci={d}", .{ self.id, self.conf_applied_index, ci });

        while (self.conf_applied_index < ci) {
            var result = self.log.entriesRange(.{
                .lo = self.conf_applied_index + 1,
                .hi = ci + 1,
                .ty = .conf,
            }) catch return;
            defer result.deinit();

            for (result.entries) |entry| {
                self.conf_applied_index = entry.index;
                const reader = zen.LogEntryReader.parseRoot(entry.bytes) catch continue;
                if (reader.confChange()) |cc_reader| {
                    const cc = self.decodeConfChange(cc_reader) catch continue;
                    self.applyConfChange(cc);
                    self.pending_conf_change = false;
                }
            }

            // All conf entries in the range were returned — we're caught up.
            if (result.eof) {
                self.conf_applied_index = ci;
                return;
            }
        }
    }

    fn applyConfChange(self: *Node, cc: ConfChange) void {
        raft_log.info("node {d}: applyConfChange type={s} target={d} addr={s}", .{
            self.id, @tagName(cc.change_type), cc.node_id, cc.addr,
        });
        if (cc.node_id == self.id) {
            // Update our own role.
            switch (cc.change_type) {
                .add_voter, .to_voter => {
                    if (self.state == .learner) self.state = .follower;
                },
                .add_learner, .to_learner => self.state = .learner,
                .remove_node => {
                    // We've been removed from the cluster. Shut down.
                    dbg("node {d} removed from cluster, shutting down", .{self.id});
                    self.shutdown.startShutdown();
                },
                .update_addr => {}, // Our own address — nothing to do locally.
                .unknown, _ => {},
            }
        } else {
            switch (cc.change_type) {
                .add_voter => self.addPeer(cc.node_id, .voter, cc.addr),
                .add_learner => self.addPeer(cc.node_id, .learner, cc.addr),
                .to_voter => self.setPeerRole(cc.node_id, .voter),
                .to_learner => self.setPeerRole(cc.node_id, .learner),
                .remove_node => self.removePeer(cc.node_id),
                .update_addr => self.updatePeerAddr(cc.node_id, cc.addr),
                .unknown, _ => {},
            }
        }
        // Manage transport peers directly.
        if (self.transport) |tr| {
            switch (cc.change_type) {
                .add_voter, .add_learner => {
                    if (cc.addr.len > 0 and cc.node_id != self.id) {
                        tr.addPeer(cc.node_id, cc.addr) catch |err| {
                            raft_log.err("addPeer for {d} failed: {}", .{ cc.node_id, err });
                        };
                    }
                },
                .remove_node => {
                    if (cc.node_id != self.id) tr.removePeer(cc.node_id);
                },
                else => {},
            }
        }
        if (self.on_conf_applied) |cb| {
            cb(self.on_conf_applied_ctx.?, cc);
        }
    }

    fn hasPeer(self: *Node, id: NodeId) bool {
        return self.peers.contains(id);
    }

    fn addPeer(self: *Node, id: NodeId, role: PeerRole, addr: String) void {
        if (self.hasPeer(id)) return;
        const owned_addr = self.allocator.dupe(u8, addr) catch {
            @panic("raft: addPeer out of memory");
        };
        _ = self.peers.put(id, .{
            .id = id,
            .next_index = self.log.lastIndex() + 1,
            .last_index = 0,
            .role = role,
            .addr = owned_addr,
        }) catch {
            self.allocator.free(owned_addr);
            @panic("raft: addPeer out of memory");
        };
        self.recomputeQuorum();
    }

    fn removePeer(self: *Node, id: NodeId) void {
        if (self.peers.fetchRemove(id)) |kv| {
            if (kv.value.addr.len > 0) self.allocator.free(kv.value.addr);
        }
        self.recomputeQuorum();
        if (id == self.id) {
            self.state = .learner;
        }
    }

    fn updatePeerAddr(self: *Node, id: NodeId, addr: String) void {
        const peer = self.peers.getPtr(id) orelse return;
        if (peer.addr.len > 0) self.allocator.free(peer.addr);
        peer.addr = self.allocator.dupe(u8, addr) catch return;
        // Update transport connection if running.
        if (self.transport) |tr| {
            tr.addPeer(id, addr) catch |err| {
                raft_log.err("updatePeerAddr: transport addPeer for {d} failed: {s}", .{ id, @errorName(err) });
            };
        }
    }

    fn setPeerRole(self: *Node, id: NodeId, role: PeerRole) void {
        if (self.peers.getPtr(id)) |peer| {
            peer.role = role;
            self.recomputeQuorum();
        } else {
            // If it's not a peer, treat as add with desired role.
            self.addPeer(id, role, "");
            return;
        }
        if (id == self.id) {
            if (role == .learner) {
                self.state = .learner;
            } else if (self.state == .learner) {
                self.state = .follower;
            }
        }
    }

    fn recomputeQuorum(self: *Node) void {
        var voters: usize = 0;
        if (self.state != .learner) voters += 1;
        var it = self.peers.valueIterator();
        while (it.next()) |peer| {
            if (peer.role == .voter) voters += 1;
        }
        self.quorum_size = (voters / 2) + 1;
        dbg("node={d} quorum_size={d}", .{ self.id, self.quorum_size });
    }

    fn resetElectionTimeout(self: *Node, reason: TimeoutResetReason) void {
        const old_timeout = self.election_timeout;
        const rand = self.prng.random();
        const draw = rand.int(u32);
        const min = self.config.election_timeout_min_ticks;
        const max = self.config.election_timeout_max_ticks;
        const span = (max - min) + 1;
        self.election_timeout = min + (draw % span);

        _ = old_timeout;
        _ = reason;
        // utils.logEvery(.debug, 5, "timeout-reset tick={d} node={d} " ++
        //     "reason={s} old={d} new={d} range=[{d},{d}] elapsed={d}", .{
        //     self.tick_count,
        //     self.id,
        //     @tagName(reason),
        //     old_timeout,
        //     self.election_timeout,
        //     min,
        //     max,
        //     self.elapsed_ticks,
        // });
    }

    fn stepDown(self: *Node, new_term: u64) RaftError!void {
        const prev_state = self.state;
        const prev_term = self.current_term;
        if (self.state == .learner) {
            self.state = .learner;
        } else {
            self.state = .follower;
        }
        self.current_term = new_term;
        self.voted_for = null;
        self.leader_id = null;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout(.step_down);
        self.failAllPendingCallbacks(.step_down);
        dbg(
            "step-down tick={d} node={d} {s}/term={d} -> {s}/term={d}",
            .{ self.tick_count, self.id, @tagName(prev_state), prev_term, @tagName(self.state), self.current_term },
        );
        try self.persistHardState();
    }

    /// HLC timestamp of the most-recently applied entry on this node.
    /// Reads bounded by `read_ts` must wait until `appliedTs() >= read_ts`.
    pub fn appliedTs(self: *Node) u64 {
        return self.applied_ts.load();
    }

    /// Block the caller until this node's applied timestamp >= `target_ts`
    /// or the timeout expires. Returns `error.Timeout` if the deadline
    /// passes before catch-up.
    pub fn waitForApplied(self: *Node, target_ts: u64, timeout_ms: u64) !void {
        return self.applied_ts.waitFor(target_ts, timeout_ms);
    }

    fn failAllPendingCallbacks(self: *Node, status: CommitStatus) void {
        for (self.commit_waiters.items) |w| w.finish(status);
        utils.clearRetainCap(&self.commit_waiters);
        if (self.applied_held) |w| {
            self.applied_held = null;
            w.finish(status);
        }
        while (true) {
            const head = self.apply_waiters.tryReceive() catch break;
            head.finish(status);
        }
    }

    /// A proposal that never reached the log: no index, this term.
    fn reject(self: *Node, req: ProposalRequest, status: CommitStatus) void {
        const w = req.waiter orelse return;
        w.entry = .{ .term = self.current_term, .index = 0 };
        w.finish(status);
    }

    // TESTING ONLY
    // _propose is and should only being used by tests.
    pub fn _propose(self: *Node, data: String) RaftError!u64 {
        if (self.state != .leader) return RaftError.NotLeader;

        const new_index = self.log.lastIndex() + 1;
        var writer = try zen.LogEntryWriter.init(self.allocator);
        defer writer.deinit();
        try writer.set(.{
            .index = new_index,
            .term = self.current_term,
            .timestamp = utils.hlc.g().next().asU64(),
            .data = data,
            .entry_type = zen.EntryTypeEnum.data,
            .conf_change = zen.NullOffset,
        });
        var entry = try writer.finishWithTail();
        defer entry.deinit();

        try self.log.addEntries(&[_]zen.LogEntryBuffer{entry});
        try self.log.sync();
        self.advance();
        self.maybeBroadcast(true);
        return new_index;
    }
};
