const std = @import("std");
const raft = @import("raft");
const cluster_mod = @import("cluster.zig");

const TestCluster = cluster_mod.TestCluster;
const Config = raft.Config;
const NodeId = raft.NodeId;

fn defaultConfig() Config {
    return .{
        .heartbeat_ticks = 2,
        .election_timeout_min_ticks = 5,
        .election_timeout_max_ticks = 8,
        .tick_interval_ms = 10,
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

test "leader election" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);

    const leader = findLeader(&cluster) orelse return error.NoLeader;
    _ = leader;
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));
}

test "heartbeats keep followers stable" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);
    const leader = findLeader(&cluster) orelse return error.NoLeader;

    try cluster.runTicks(20);
    const leader2 = findLeader(&cluster) orelse return error.NoLeader;
    try std.testing.expectEqual(leader, leader2);
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));
}

test "leader re-election after isolation" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);
    const leader1 = findLeader(&cluster) orelse return error.NoLeader;

    cluster.setDropFrom(leader1, true);
    try cluster.runTicks(30);

    const leader2 = findLeader(&cluster) orelse return error.NoLeader;
    try std.testing.expect(leader2 != leader1);
    try std.testing.expectEqual(@as(usize, 1), countLeaders(&cluster));
}

test "learner never votes or becomes leader" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3, 4 };
    var learners = [_]NodeId{4};

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &learners, cfg);
    defer cluster.deinit();

    try cluster.runTicks(30);

    const leader = findLeader(&cluster) orelse return error.NoLeader;
    try std.testing.expect(leader != 4);

    if (cluster.getNode(4)) |node| {
        try std.testing.expect(!node.isLeader());
        try std.testing.expect(node.state == .learner);
    } else {
        return error.MissingNode;
    }
}

test "pre-vote avoids term bump without quorum" {
    var ids = [_]NodeId{ 1, 2, 3 };

    var cfg_pre = defaultConfig();
    cfg_pre.enable_prevote = true;
    var cluster_pre = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg_pre);
    defer cluster_pre.deinit();

    var cfg_no = defaultConfig();
    cfg_no.enable_prevote = false;
    var cluster_no = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg_no);
    defer cluster_no.deinit();

    // Drop all outbound messages so no quorum can be reached.
    inline for ([_]NodeId{ 1, 2, 3 }) |id| {
        cluster_pre.setDropFrom(id, true);
        cluster_no.setDropFrom(id, true);
    }

    const node1_pre = cluster_pre.getNode(1) orelse return error.MissingNode;
    const node1_no = cluster_no.getNode(1) orelse return error.MissingNode;
    const term_before = node1_pre.current_term;

    try cluster_pre.runTicks(30);
    try cluster_no.runTicks(30);

    // With pre-vote enabled, isolated node should not bump term.
    try std.testing.expectEqual(term_before, node1_pre.current_term);
    // Without pre-vote, the node will start elections and bump term.
    try std.testing.expect(node1_no.current_term > term_before);
}

test "convert to voter and learner adjust quorum" {
    var cfg = defaultConfig();
    cfg.enable_prevote = false;
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);
    const leader_id = findLeader(&cluster) orelse return error.NoLeader;
    const leader = cluster.getNode(leader_id) orelse return error.MissingNode;

    const quorum_before = leader.quorum_size;

    _ = try leader.proposeConfChange(.{ .change_type = .add_learner, .node_id = 4 });
    leader.commit_index = leader.log.lastIndex();
    leader.applyCommitted();
    try std.testing.expectEqual(quorum_before, leader.quorum_size);

    _ = try leader.proposeConfChange(.{ .change_type = .to_voter, .node_id = 4 });
    leader.commit_index = leader.log.lastIndex();
    leader.applyCommitted();
    try std.testing.expect(leader.quorum_size > quorum_before);

    _ = try leader.proposeConfChange(.{ .change_type = .to_learner, .node_id = 4 });
    leader.commit_index = leader.log.lastIndex();
    leader.applyCommitted();
    try std.testing.expectEqual(quorum_before, leader.quorum_size);
}

test "candidate steps down on higher term" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);

    const leader = findLeader(&cluster) orelse return error.NoLeader;
    const follower_id: NodeId = if (leader == 1) 2 else 1;

    const follower = cluster.getNode(follower_id) orelse return error.MissingNode;
    const higher_term = follower.current_term + 5;

    const msg = raft.Message{ .append_entries = .{
        .term = higher_term,
        .leader_id = follower_id,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &[_]raft.LogEntry{},
        .leader_commit = 0,
    } };

    const leader_node = cluster.getNode(leader) orelse return error.MissingNode;
    try leader_node.handleMessage(msg);

    try std.testing.expect(!leader_node.isLeader());
    try std.testing.expect(leader_node.current_term == higher_term);
}

test "log replication" {
    const cfg = defaultConfig();
    var ids = [_]NodeId{ 1, 2, 3 };

    var cluster = try TestCluster.init(std.testing.allocator, &ids, &[_]NodeId{}, cfg);
    defer cluster.deinit();

    try cluster.runTicks(20);
    const leader = findLeader(&cluster) orelse return error.NoLeader;
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
        const buf = try alloc.alloc(u8, len);
        rand.bytes(buf);
        const dup = try alloc.dupe(u8, buf);
        try expected.append(alloc, dup);
        _ = try leader_node.propose(buf);
        try cluster.runTicks(1);
    }

    var it = cluster.nodes.valueIterator();
    while (it.next()) |entry| {
        const node = entry.*.node;
        if (node.id == leader) continue;
        try std.testing.expectEqual(@as(u64, @intCast(expected.items.len)), node.log.lastIndex());
        for (expected.items, 0..) |data, idx| {
            const entry_log = node.log.entry(@intCast(idx + 1)) orelse return error.MissingEntry;
            try std.testing.expectEqualSlices(u8, data, entry_log.data);
        }
    }
}
