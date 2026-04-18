const std = @import("std");
const raft = @import("raft");
const zio = @import("zio");
const cluster_mod = @import("cluster.zig");
const zen = raft.buffers;

const TestCluster = cluster_mod.TestCluster;
const TestLog = cluster_mod.TestLog;
const Config = raft.Config;
const NodeId = raft.NodeId;

test "all" {
    try raft.utils.runTestsInZioRuntime(@This());
}

fn noopCb(_: *anyopaque, _: raft.Entry, _: raft.CommitStatus) void {}

fn defaultConfig() Config {
    return .{
        .id = 1,
        .heartbeat_ticks = 2,
        .election_timeout_min_ticks = 5,
        .election_timeout_max_ticks = 15,
        .max_entries_per_msg = 64,
        .max_msg_size = 1_048_576,
    };
}

fn findLeader(cluster: *TestCluster) ?NodeId {
    var it = cluster.nodes.valueIterator();
    while (it.next()) |entry| {
        const node = entry.*.node;
        if (node.isLeader()) return node.id;
    }
    return null;
}

fn countLeaders(cluster: *TestCluster) usize {
    var count: usize = 0;
    var it = cluster.nodes.valueIterator();
    while (it.next()) |entry| {
        const node = entry.*.node;
        if (node.isLeader()) count += 1;
    }
    return count;
}

pub fn testLeaderElection() !void {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    const cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    cluster.waitForTicks(50);

    const leader = findLeader(cluster) orelse return error.NoLeader;
    _ = leader;
    try std.testing.expectEqual(@as(usize, 1), countLeaders(cluster));
}

pub fn testHeartbeatsKeepFollowersStable() !void {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    const cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    cluster.waitForTicks(50);
    const leader = findLeader(cluster) orelse return error.NoLeader;

    cluster.waitForTicks(50);
    const leader2 = findLeader(cluster) orelse return error.NoLeader;
    try std.testing.expectEqual(leader, leader2);
    try std.testing.expectEqual(@as(usize, 1), countLeaders(cluster));
}

pub fn testLeaderReElectionAfterIsolation() !void {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    const cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    cluster.waitForTicks(50);
    const leader1 = findLeader(cluster) orelse return error.NoLeader;

    cluster.setDropFrom(leader1, true);
    cluster.waitForTicks(100);

    const leader2 = findLeader(cluster) orelse return error.NoLeader;
    try std.testing.expect(leader2 != leader1);
    try std.testing.expectEqual(@as(usize, 1), countLeaders(cluster));
}

pub fn testLearnerNeverVotesOrBecomesLeader() !void {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3, 4 };
    var learners = [_]NodeId{4};

    const cluster = try TestCluster.init(std.testing.allocator, &ids, &learners, cfg);
    defer cluster.deinit();

    cluster.waitForTicks(50);

    const leader = findLeader(cluster) orelse return error.NoLeader;
    try std.testing.expect(leader != 4);

    if (cluster.getNode(4)) |node| {
        try std.testing.expect(!node.isLeader());
        try std.testing.expect(node.state == .learner);
    } else {
        return error.MissingNode;
    }
}

pub fn testPreVoteAvoidsTermBumpWithoutQuorum() !void {
    var ids = [_]NodeId{ 1, 2, 3 };

    var cfg_pre = defaultConfig();
    cfg_pre.enable_prevote = true;
    const cluster_pre = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg_pre);
    defer cluster_pre.deinit();

    var cfg_no = defaultConfig();
    cfg_no.enable_prevote = false;
    const cluster_no = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg_no);
    defer cluster_no.deinit();

    inline for ([_]NodeId{ 1, 2, 3 }) |id| {
        cluster_pre.setDropFrom(id, true);
        cluster_no.setDropFrom(id, true);
    }

    const node1_pre = cluster_pre.getNode(1) orelse return error.MissingNode;
    const node1_no = cluster_no.getNode(1) orelse return error.MissingNode;
    const term_before = node1_pre.current_term;

    cluster_pre.waitForTicks(50);
    cluster_no.waitForTicks(50);

    try std.testing.expectEqual(term_before, node1_pre.current_term);
    try std.testing.expect(node1_no.current_term > term_before);
}

pub fn testConvertToVoterAndLearnerAdjustQuorum() !void {
    var cfg = defaultConfig();
    cfg.enable_prevote = false;
    var ids = [_]NodeId{ 1, 2, 3 };

    const cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    cluster.waitForTicks(100);
    const leader_id = findLeader(cluster) orelse return error.NoLeader;
    const leader = cluster.getNode(leader_id) orelse return error.MissingNode;

    const quorum_before = leader.quorum_size;

    try leader.proposeConfChange(.{ .change_type = .add_learner, .node_id = 4, .addr = "127.0.0.1:9004" }, &noopCb, undefined);
    cluster.waitForTicks(20);
    try std.testing.expectEqual(quorum_before, leader.quorum_size);

    try leader.proposeConfChange(.{ .change_type = .to_voter, .node_id = 4 }, &noopCb, undefined);
    cluster.waitForTicks(20);
    try std.testing.expect(leader.quorum_size > quorum_before);

    try leader.proposeConfChange(.{ .change_type = .to_learner, .node_id = 4 }, &noopCb, undefined);
    cluster.waitForTicks(20);
    try std.testing.expectEqual(quorum_before, leader.quorum_size);
}

pub fn testCandidateStepsDownOnHigherTerm() !void {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    const cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    cluster.waitForTicks(200);

    const leader = findLeader(cluster) orelse return error.NoLeader;
    const follower_id: NodeId = if (leader == 1) 2 else 1;

    const follower = cluster.getNode(follower_id) orelse return error.MissingNode;
    const higher_term = follower.current_term + 5;

    var writer = try zen.AppendEntriesWriter.init(std.testing.allocator);
    defer writer.deinit();
    try writer.set(.{
        .term = higher_term,
        .leader_id = follower_id,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &[_]u32{},
        .leader_commit = 0,
    });
    var buf = try writer.finishWithTail();
    defer buf.deinit();
    const encoded = buf.bytes orelse return error.Invalid;

    // Send via inbound_ch so mainLoop processes it.
    const leader_node = cluster.getNode(leader) orelse return error.MissingNode;
    const owned = try std.testing.allocator.dupe(u8, encoded);
    leader_node.inbound_ch.send(.{
        .message_type = raft.msg_type_raft,
        .payload = owned,
    }) catch {
        std.testing.allocator.free(owned);
        return error.ChannelSendFailed;
    };
    leader_node.event_ch.trySend(1) catch {};

    cluster.waitForTicks(5);

    // The leader must have stepped down and adopted the higher term.
    // It may have won re-election, so we check term, not leadership.
    try std.testing.expect(leader_node.current_term >= higher_term);
}

pub fn testProposalCallbackFiresOnCommit() !void {
    var cfg = defaultConfig();
    cfg.enable_prevote = false;

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    var ncfg = cfg;
    ncfg.id = 1;
    const node = try raft.Node.init(
        std.testing.allocator,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        ncfg,
    );
    defer node.deinit();
    try node.becomeLeader();

    const Ctx = struct {
        called: bool = false,
        entry: ?raft.Entry = null,
        status: ?raft.CommitStatus = null,
    };
    const callback = struct {
        fn run(raw: *anyopaque, entry: raft.Entry, status: raft.CommitStatus) void {
            const ctx: *Ctx = @ptrCast(@alignCast(raw));
            ctx.called = true;
            ctx.entry = entry;
            ctx.status = status;
        }
    };

    var ctx = Ctx{};
    try node.proposeDataChange("hello", callback.run, &ctx);
    _ = try node.processProposalRequests(16);
    node.advance();
    node.applyCommits();

    try std.testing.expect(ctx.called);
    try std.testing.expectEqual(@as(u64, 1), (ctx.entry orelse return error.Invalid).index);
    try std.testing.expectEqual(raft.CommitStatus.committed, ctx.status orelse return error.Invalid);
}

// ---- Transport helpers ----

fn runRaftRuntime(t: *raft.RaftRuntime) void {
    t.run() catch {};
}

// ---- Transport tests (zio-based) ----

pub fn testTransportStartsAndStopsCleanly() !void {
    var cfg = defaultConfig();
    cfg.enable_prevote = false;
    cfg.cluster_id = 1001;
    cfg.listen_addr = "127.0.0.1:0";
    cfg.tick_interval_ms = 1;

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    const node = try raft.Node.init(
        std.testing.allocator,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        cfg,
    );
    defer node.deinit();
    try node.becomeLeader();

    var rt = raft.RaftRuntime.init(std.testing.allocator, node);
    defer rt.deinit();

    var group = zio.group();
    defer group.wait() catch {};
    try group.spawn(runRaftRuntime, .{&rt});

    zio.sleep(zio.Duration.fromMilliseconds(50)) catch return;
    node.startShutdown();
}

pub fn testTransportProcessesProposal() !void {
    var cfg = defaultConfig();
    cfg.enable_prevote = false;
    cfg.id = 1;
    cfg.cluster_id = 1002;
    cfg.listen_addr = "127.0.0.1:0";
    cfg.tick_interval_ms = 1;

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    const node = try raft.Node.init(
        std.testing.allocator,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        cfg,
    );
    defer node.deinit();
    try node.becomeLeader();

    var rt = raft.RaftRuntime.init(std.testing.allocator, node);
    defer rt.deinit();

    var group = zio.group();
    defer group.wait() catch {};
    try group.spawn(runRaftRuntime, .{&rt});

    const Ctx = struct {
        done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    };
    const callback = struct {
        fn run(raw: *anyopaque, _: raft.Entry, _: raft.CommitStatus) void {
            const ctx: *Ctx = @ptrCast(@alignCast(raw));
            ctx.done.store(true, .seq_cst);
        }
    };

    var ctx = Ctx{};
    try node.proposeDataChange("transport-cb", callback.run, &ctx);

    var observed = false;
    for (0..500) |_| {
        if (ctx.done.load(.seq_cst)) {
            observed = true;
            break;
        }
        zio.sleep(zio.Duration.fromMilliseconds(10)) catch break;
    }

    try std.testing.expect(observed);
    node.startShutdown();
}

pub fn testTransportStopIsClean() !void {
    var cfg = defaultConfig();
    cfg.id = 1;
    cfg.cluster_id = 1003;
    cfg.listen_addr = "127.0.0.1:0";
    cfg.tick_interval_ms = 1;

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    const node = try raft.Node.init(
        std.testing.allocator,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        cfg,
    );
    defer node.deinit();

    var rt = raft.RaftRuntime.init(std.testing.allocator, node);
    defer rt.deinit();

    var group = zio.group();
    defer group.wait() catch {};
    try group.spawn(runRaftRuntime, .{&rt});

    zio.sleep(zio.Duration.fromMilliseconds(50)) catch return;
    node.startShutdown();
}

pub fn testProposeAfterShutdownReturnsQueueClosed() !void {
    const cfg = defaultConfig();

    var test_wal = try TestLog.init(std.testing.allocator, 1);
    defer test_wal.deinit(std.testing.allocator);
    var ncfg = cfg;
    ncfg.id = 1;
    const node = try raft.Node.init(
        std.testing.allocator,
        &[_]raft.Peer{},
        test_wal.asRaftWAL(),
        ncfg,
    );
    defer node.deinit();

    node.startShutdown();
    const callback = struct {
        fn run(_: *anyopaque, _: raft.Entry, _: raft.CommitStatus) void {}
    };
    try std.testing.expectError(
        raft.RaftError.QueueClosed,
        node.proposeDataChange("x", callback.run, undefined),
    );
}

pub fn testLogReplication() !void {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    const cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    cluster.waitForTicks(50);
    const leader = findLeader(cluster) orelse return error.NoLeader;
    const leader_node = cluster.getNode(leader) orelse return error.MissingNode;

    var prng = std.Random.DefaultPrng.init(1234567);
    const rand = prng.random();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var expected = std.ArrayList([]const u8).empty;
    defer expected.deinit(alloc);

    var i: usize = 0;
    while (i < 10) : (i += 1) {
        const len = rand.intRangeAtMost(u8, 1, 16);
        const data_buf = try alloc.alloc(u8, len);
        rand.bytes(data_buf);
        const dup = try alloc.dupe(u8, data_buf);
        try expected.append(alloc, dup);
        _ = try leader_node._propose(data_buf);
        cluster.waitForTicks(5);
    }

    // Wait for replication to complete.
    cluster.waitForTicks(50);

    var it = cluster.nodes.valueIterator();
    while (it.next()) |entry| {
        const node = entry.*.node;
        if (node.id == leader) continue;
        try std.testing.expectEqual(@as(u64, @intCast(expected.items.len)), node.log.lastIndex());
        var log_result = try node.log.entriesRange(.{ .lo = 1, .hi = node.log.lastIndex() + 1 });
        defer log_result.deinit();
        for (expected.items, 0..) |data, idx| {
            if (idx >= log_result.entries.len) return error.MissingEntry;
            const reader = try zen.LogEntryReader.parseRoot(log_result.entries[idx].bytes);
            try std.testing.expectEqualSlices(u8, data, reader.data());
        }
    }
}

pub fn testFollowerCatchesUpAfterDisconnectAndReconnect() !void {
    var cfg = defaultConfig();
    cfg.enable_prevote = false;

    var ids = [_]NodeId{ 1, 2, 3 };
    const cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    cluster.waitForTicks(100);
    const leader_id = findLeader(cluster) orelse return error.NoLeader;
    const lagging_id: NodeId = if (leader_id != 2) 2 else 3;
    const lagging = cluster.getNode(lagging_id) orelse return error.MissingNode;

    const disconnect_start: usize = 10;
    const reconnect_at: usize = 40;
    const total_props: usize = 80;

    var proposed_count: usize = 0;
    while (proposed_count < total_props) {
        if (proposed_count == disconnect_start) {
            cluster.setDropFrom(lagging_id, true);
            cluster.setDropTo(lagging_id, true);
        }
        if (proposed_count == reconnect_at) {
            cluster.setDropFrom(lagging_id, false);
            cluster.setDropTo(lagging_id, false);
        }

        const current_leader_id = findLeader(cluster) orelse {
            cluster.waitForTicks(5);
            continue;
        };
        const current_leader = cluster.getNode(current_leader_id) orelse return error.MissingNode;

        const payload = try std.fmt.allocPrint(
            std.testing.allocator,
            "{d}",
            .{proposed_count + 1},
        );
        defer std.testing.allocator.free(payload);
        _ = current_leader._propose(payload) catch |err| switch (err) {
            raft.RaftError.NotLeader => {
                cluster.waitForTicks(5);
                continue;
            },
            else => return err,
        };
        proposed_count += 1;
        cluster.waitForTicks(5);
    }

    // Wait for lagging node to catch up after reconnection.
    cluster.waitForTicks(200);
    const final_leader = cluster.getNode(findLeader(cluster) orelse return error.NoLeader) orelse
        return error.MissingNode;
    try std.testing.expectEqual(final_leader.log.lastIndex(), lagging.log.lastIndex());

    const last = final_leader.log.lastIndex();
    var leader_result = try final_leader.log.entriesRange(.{ .lo = 1, .hi = last + 1 });
    defer leader_result.deinit();
    var lagging_result = try lagging.log.entriesRange(.{ .lo = 1, .hi = last + 1 });
    defer lagging_result.deinit();

    if (leader_result.entries.len != lagging_result.entries.len) return error.MissingEntry;
    for (leader_result.entries, lagging_result.entries) |le, lag| {
        const leader_reader = try zen.LogEntryReader.parseRoot(le.bytes);
        const lagging_reader = try zen.LogEntryReader.parseRoot(lag.bytes);
        try std.testing.expectEqual(leader_reader.term(), lagging_reader.term());
        try std.testing.expectEqualSlices(u8, leader_reader.data(), lagging_reader.data());
    }
}
