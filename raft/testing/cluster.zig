const std = @import("std");
const zio = @import("zio");
const raft = @import("raft");
pub const Node = raft.Node;
pub const NodeId = raft.NodeId;
pub const Config = raft.Config;

pub const TestLog = struct {
    disk: raft.DiskWAL,
    wal_dir: []u8,

    pub fn init(allocator: std.mem.Allocator, id: NodeId) !TestLog {
        const wal_dir = try makeWalDir(allocator, id);
        errdefer {
            std.fs.cwd().deleteTree(wal_dir) catch {};
            allocator.free(wal_dir);
        }
        const disk = try raft.DiskWAL.openPath(wal_dir, allocator, 1024);
        return .{ .disk = disk, .wal_dir = wal_dir };
    }

    pub fn deinit(self: *TestLog, allocator: std.mem.Allocator) void {
        self.disk.deinit();
        std.fs.cwd().deleteTree(self.wal_dir) catch {};
        allocator.free(self.wal_dir);
    }

    fn makeWalDir(allocator: std.mem.Allocator, id: NodeId) ![]u8 {
        var buf: [192]u8 = undefined;
        const ts = std.time.nanoTimestamp();
        const path = try std.fmt.bufPrint(&buf, "/tmp/egres-test-cluster-{d}-{d}", .{ ts, id });
        try std.fs.cwd().makePath(path);
        return allocator.dupe(u8, path);
    }
};

pub const TestCluster = struct {
    allocator: std.mem.Allocator,
    nodes: std.AutoHashMap(NodeId, *MetaNode),
    drop_from: std.AutoHashMap(NodeId, void),
    drop_to: std.AutoHashMap(NodeId, void),

    pub const MetaNode = struct {
        node: *Node,
        wal: TestLog,
        peers: []const raft.Peer,
        group: zio.Group,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        ids: []const NodeId,
        learners: []const NodeId,
        config: Config,
    ) !*TestCluster {
        var nodes = std.AutoHashMap(NodeId, *MetaNode).init(allocator);
        const drop_from = std.AutoHashMap(NodeId, void).init(allocator);
        const drop_to = std.AutoHashMap(NodeId, void).init(allocator);
        try nodes.ensureTotalCapacity(@intCast(ids.len));

        // Phase 1: Create all nodes with localhost transport.
        for (ids) |id| {
            const peers = try buildPeers(allocator, ids, learners, id);
            var entry = try allocator.create(MetaNode);
            entry.* = .{
                .node = undefined,
                .wal = try TestLog.init(allocator, id),
                .peers = peers,
                .group = zio.group(),
            };
            var ncfg = config;
            ncfg.id = id;
            ncfg.listen_addr = "127.0.0.1:0";
            ncfg.tick_interval_ms = 1; // Fast ticks for tests.
            entry.node = try Node.init(
                allocator,
                peers,
                &entry.wal.disk,
                ncfg,
            );
            if (containsNode(learners, id)) {
                entry.node.state = .learner;
            }
            try entry.node.initTransport();
            try nodes.put(id, entry);
        }

        // Phase 2: Exchange actual bound addresses between nodes.
        var addr_it = nodes.iterator();
        while (addr_it.next()) |kv| {
            const node_id = kv.key_ptr.*;
            const port = kv.value_ptr.*.node.transport.?.getListenPort();
            const addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
            defer allocator.free(addr);

            var peer_it = nodes.iterator();
            while (peer_it.next()) |pkv| {
                if (pkv.key_ptr.* == node_id) continue;
                pkv.value_ptr.*.node.transport.?.addPeer(node_id, addr) catch {};
            }
        }

        const cluster = try allocator.create(TestCluster);
        cluster.* = .{
            .allocator = allocator,
            .nodes = nodes,
            .drop_from = drop_from,
            .drop_to = drop_to,
        };

        // Phase 3: Start coroutines for each node.
        var node_it = cluster.nodes.valueIterator();
        while (node_it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            entry.group.spawn(Node.tickLoop, .{ entry.node, entry.node.config.tick_interval_ms }) catch {};
            entry.group.spawn(Node.mainLoop, .{ entry.node, entry.node.config.tick_interval_ms }) catch {};
            entry.group.spawn(Node.applyLoop, .{entry.node}) catch {};
        }
        std.debug.print("TestCluster.init: done\n", .{});
        return cluster;
    }

    pub fn deinit(self: *TestCluster) void {
        // Shut down all nodes — this makes mainLoop/tickLoop/applyLoop exit.
        var shutdown_it = self.nodes.valueIterator();
        while (shutdown_it.next()) |entry_ptr| {
            entry_ptr.*.node.startShutdown();
        }
        // Wait for ALL coroutines to finish before any cleanup.
        var wait_it = self.nodes.valueIterator();
        while (wait_it.next()) |entry_ptr| {
            entry_ptr.*.group.wait() catch {};
        }
        // Now safe to clean up — no coroutines are running.
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            entry.node.deinitTransport();
            entry.node.deinit();
            entry.wal.deinit(self.allocator);
            self.allocator.free(entry.peers);
            self.allocator.destroy(entry);
        }
        self.nodes.deinit();
        self.drop_from.deinit();
        self.drop_to.deinit();
        self.allocator.destroy(self);
    }

    /// Wait until every node has advanced by at least `n` ticks.
    pub fn waitForTicks(self: *TestCluster, n: u64) void {
        std.debug.print("waitForTicks: waiting for {d} ticks\n", .{n});
        // Record starting tick counts.
        var starts: [16]u64 = undefined;
        var count: usize = 0;
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            starts[count] = entry_ptr.*.node.tick_count;
            count += 1;
        }

        // Poll until all nodes have advanced by n ticks.
        while (true) {
            var all_done = true;
            var i: usize = 0;
            var it2 = self.nodes.valueIterator();
            while (it2.next()) |entry_ptr| {
                if (entry_ptr.*.node.tick_count < starts[i] + n) {
                    all_done = false;
                    break;
                }
                i += 1;
            }
            if (all_done) return;
            zio.sleep(zio.Duration.fromMilliseconds(1)) catch return;
        }
    }

    pub fn getNode(self: *TestCluster, id: NodeId) ?*Node {
        const entry = self.nodes.get(id) orelse return null;
        return entry.node;
    }

    /// Drop all messages sent by this node.
    pub fn setDropFrom(self: *TestCluster, id: NodeId, drop: bool) void {
        const entry = self.nodes.get(id) orelse return;
        if (entry.node.transport) |tr| tr.setDropAll(drop);
    }

    /// Drop all messages destined to this node (from every other node's perspective).
    pub fn setDropTo(self: *TestCluster, id: NodeId, drop: bool) void {
        var it = self.nodes.iterator();
        while (it.next()) |kv| {
            if (kv.key_ptr.* == id) continue;
            if (kv.value_ptr.*.node.transport) |tr| tr.setDropForPeer(id, drop);
        }
    }

    fn buildPeers(
        allocator: std.mem.Allocator,
        ids: []const NodeId,
        learners: []const NodeId,
        self_id: NodeId,
    ) ![]const raft.Peer {
        var peers = std.ArrayList(raft.Peer).empty;
        for (ids) |id| {
            if (id == self_id) continue;
            const role: raft.PeerRole = if (containsNode(learners, id)) .learner else .voter;
            try peers.append(allocator, .{
                .id = id,
                .next_index = 1,
                .match_index = 0,
                .role = role,
            });
        }
        return peers.toOwnedSlice(allocator);
    }

    fn containsNode(list: []const NodeId, id: NodeId) bool {
        for (list) |item| if (item == id) return true;
        return false;
    }
};
