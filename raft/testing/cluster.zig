const std = @import("std");
const raft = @import("raft");
const mem_wal = @import("mem_wal.zig");

pub const Node = raft.Node;
pub const NodeId = raft.NodeId;
pub const Message = raft.Message;
pub const Config = raft.Config;

pub const TestCluster = struct {
    allocator: std.mem.Allocator,
    nodes: std.AutoHashMap(NodeId, MetaNode),

    pub const MetaNode = struct {
        node: Node,
        wal: mem_wal.MemWal,
        inbox: std.ArrayList(Message),
        peers: []const NodeId,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        ids: []const NodeId,
        spectators: []const NodeId,
        config: Config,
    ) !TestCluster {
        var nodes = std.AutoHashMap(NodeId, MetaNode).init(allocator);
        try nodes.ensureTotalCapacity(@intCast(ids.len));

        for (ids) |id| {
            var wal = mem_wal.MemWal.init(allocator);
            const inbox = std.ArrayList(Message).empty;
            const peers = try buildPeers(allocator, ids, id);
            const is_spectator = containsNode(spectators, id);

            const node = try Node.init(
                allocator,
                id,
                peers,
                wal.asWal(),
                config,
                is_spectator,
            );

            try nodes.put(id, .{
                .node = node,
                .wal = wal,
                .inbox = inbox,
                .peers = peers,
            });
        }

        return .{
            .allocator = allocator,
            .nodes = nodes,
        };
    }

    pub fn deinit(self: *TestCluster) void {
        var it = self.nodes.valueIterator();
        while (it.next()) |entry| {
            entry.node.deinit();
            entry.wal.deinit();
            entry.inbox.deinit(self.allocator);
            self.allocator.free(entry.peers);
        }
        self.nodes.deinit();
    }

    pub fn tickAll(self: *TestCluster) !void {
        var it = self.nodes.valueIterator();
        while (it.next()) |entry| {
            try entry.node.tick();
        }
        self.deliverOutbox();
    }

    pub fn deliverOutbox(self: *TestCluster) void {
        var it = self.nodes.valueIterator();
        while (it.next()) |entry| {
            const outbox = entry.node.takeOutbox();
            if (outbox.len == 0) continue;
            for (outbox) |msg| {
                // Can't use this because we're not using outbox. Only inbox.
                // entry.node.send(msg.to, msg.msg);
                if (self.nodes.getPtr(msg.to)) |dest| {
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
        while (it.next()) |entry| {
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
        const entry = self.nodes.getPtr(id) orelse return null;
        return &entry.node;
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
