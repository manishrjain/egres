const std = @import("std");
const raft = @import("raft");
pub const Node = raft.Node;
pub const NodeId = raft.NodeId;
pub const Message = raft.Message;
pub const Config = raft.Config;

pub const TestCluster = struct {
    allocator: std.mem.Allocator,
    nodes: std.AutoHashMap(NodeId, *MetaNode),
    drop_from: std.AutoHashMap(NodeId, void),

    pub const MetaNode = struct {
        node: Node,
        log: raft.MemRaftLog,
        inbox: std.ArrayList(Message),
        peers: []const NodeId,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        ids: []const NodeId,
        learners: []const NodeId,
        config: Config,
    ) !TestCluster {
        var nodes = std.AutoHashMap(NodeId, *MetaNode).init(allocator);
        const drop_from = std.AutoHashMap(NodeId, void).init(allocator);
        try nodes.ensureTotalCapacity(@intCast(ids.len));

        for (ids) |id| {
            const peers = try buildPeers(allocator, ids, id);
            const is_learner = containsNode(learners, id);

            var entry = try allocator.create(MetaNode);
            entry.* = .{
                .node = undefined,
                .log = raft.MemRaftLog.init(allocator),
                .inbox = std.ArrayList(Message).empty,
                .peers = peers,
            };
            entry.node = try Node.init(
                allocator,
                id,
                peers,
                entry.log.asRaftLog(),
                config,
                is_learner,
            );

            try nodes.put(id, entry);
        }

        return .{
            .allocator = allocator,
            .nodes = nodes,
            .drop_from = drop_from,
        };
    }

    pub fn deinit(self: *TestCluster) void {
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            entry.node.deinit();
            entry.log.deinit();
            entry.inbox.deinit(self.allocator);
            self.allocator.free(entry.peers);
            self.allocator.destroy(entry);
        }
        self.nodes.deinit();
        self.drop_from.deinit();
    }

    pub fn tickAll(self: *TestCluster) !void {
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            try entry_ptr.*.node.tick();
        }
        self.deliverOutbox();
    }

    pub fn deliverOutbox(self: *TestCluster) void {
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            if (self.drop_from.contains(entry.node.id)) {
                entry.node.clearOutbox();
                continue;
            }
            const outbox = entry.node.takeOutbox();
            if (outbox.len == 0) continue;
            for (outbox) |msg| {
                if (self.nodes.get(msg.to)) |dest| {
                    _ = dest.inbox.append(self.allocator, msg.msg) catch {
                        @panic("raft: inbox append failed");
                    };
                }
            }
            entry.node.clearOutbox();
        }
    }

    pub fn deliverInbox(self: *TestCluster) !void {
        var it = self.nodes.valueIterator();
        while (it.next()) |entry_ptr| {
            const entry = entry_ptr.*;
            if (entry.inbox.items.len == 0) continue;
            for (entry.inbox.items) |msg| {
                try entry.node.handleMessage(msg);
            }
            entry.inbox.clearRetainingCapacity();
        }
        self.deliverOutbox();
    }

    pub fn runTicks(self: *TestCluster, ticks: u32) !void {
        var i: u32 = 0;
        while (i < ticks) : (i += 1) {
            try self.tickAll();
            try self.deliverInbox();
        }
    }

    pub fn getNode(self: *TestCluster, id: NodeId) ?*Node {
        const entry = self.nodes.get(id) orelse return null;
        return &entry.node;
    }

    pub fn setDropFrom(self: *TestCluster, id: NodeId, drop: bool) void {
        if (drop) {
            _ = self.drop_from.put(id, {}) catch {
                @panic("raft: drop_from insert failed");
            };
        } else {
            _ = self.drop_from.remove(id);
        }
    }

    fn buildPeers(allocator: std.mem.Allocator, ids: []const NodeId, self_id: NodeId) ![]const NodeId {
        var peers = std.ArrayList(NodeId).empty;
        for (ids) |id| {
            if (id == self_id) continue;
            try peers.append(allocator, id);
        }
        return peers.toOwnedSlice(allocator);
    }

    fn containsNode(list: []const NodeId, id: NodeId) bool {
        for (list) |item| if (item == id) return true;
        return false;
    }
};
