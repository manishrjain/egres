const std = @import("std");
const zio = @import("zio");
const node_mod = @import("node.zig");
const serial_mod = @import("serial");
const Serial = serial_mod.Serial;
const transport_mod = @import("transport");
const Transport = transport_mod.Transport;
const zen = @import("buffers_gen");
const log = std.log.scoped(.raft_runtime);

pub const NodeId = node_mod.NodeId;

pub const msg_type_join_request = node_mod.msg_type_join_request;

pub const RaftRuntime = struct {
    allocator: std.mem.Allocator,
    node: *node_mod.Node,

    pub fn init(allocator: std.mem.Allocator, node: *node_mod.Node) RaftRuntime {
        return .{
            .allocator = allocator,
            .node = node,
        };
    }

    pub fn deinit(self: *RaftRuntime) void {
        self.node.deinitTransport();
    }

    /// Main entry point. Must be called inside a zio coroutine.
    /// Initializes transport, starts peer connections, and runs concurrent
    /// coroutines for ticking, raft processing, and commit callbacks.
    /// Returns when node.isShuttingDown() or cancelled.
    /// Run the raft tick, main, and apply loops. Transport must already be
    /// initialized via node.initTransport(). Blocks until shutdown.
    pub fn run(self: *RaftRuntime) !void {
        if (self.node.transport == null) try self.node.initTransport();

        try self.node.blockingRunAllLoops();
    }
};

// -- requestJoin: client-side join protocol over mux --

/// Join a cluster by connecting to the leader. Must be called from
/// within a zio coroutine.
pub fn requestJoin(
    allocator: std.mem.Allocator,
    leader_addr: []const u8,
    cluster_id: u64,
    node_id: NodeId,
    self_addr: []const u8,
) !zen.JoinResponseBuffer {
    const addr = try zio.net.IpAddress.parseIpAndPort(leader_addr);
    const conn = try addr.connect(.{});
    conn.socket.setNoDelay(true) catch {};

    // Transport-level binary handshake.
    try Transport.sendHandshake(conn, cluster_id, node_id, .p0, .join);
    const hs = try Transport.recvHandshake(conn);
    if (hs.cluster_id != cluster_id) return error.ClusterMismatch;

    // Temporary Serial connection for the join exchange.
    const s = try Serial.init(allocator, conn);
    defer s.deinit();

    // Send join request.
    var writer = try zen.JoinRequestWriter.init(allocator);
    defer writer.deinit();
    try writer.set(.{
        .cluster_id = cluster_id,
        .node_id = node_id,
        .addr = self_addr,
    });
    var req_buf = try writer.finishWithTail();
    defer req_buf.deinit();
    const req_bytes = req_buf.bytes orelse return error.Invalid;
    try s.send(msg_type_join_request, req_bytes);

    // Read response.
    const reply = try s.recvOne();
    return zen.JoinResponseBuffer.initWithTail(allocator, reply.payload, .owned) catch {
        if (reply.payload.len > 0) allocator.free(reply.payload);
        return error.InvalidJoinResponse;
    };
}

/// Try seeds in order, follow NOT_LEADER redirects, retry with backoff.
/// Must be called from a zio coroutine.
pub fn joinCluster(
    allocator: std.mem.Allocator,
    seeds: []const []const u8,
    cluster_id: u64,
    node_id: NodeId,
    self_addr: []const u8,
) !void {
    const max_attempts: u32 = 30;
    var backoff_ms: u32 = 250;

    var attempt: u32 = 0;
    while (attempt < max_attempts) : (attempt += 1) {
        log.info("joinCluster: attempt {d}/{d}", .{ attempt + 1, max_attempts });
        for (seeds) |seed| {
            const target = seed;
            log.info("joinCluster: trying seed {s}", .{target});
            var resp = requestJoin(allocator, target, cluster_id, node_id, self_addr) catch |err| {
                log.err("joinCluster: requestJoin to {s} failed: {s}", .{ target, @errorName(err) });
                continue;
            };
            defer resp.deinit();

            const reader = resp.initReader() catch |err| {
                log.err("joinCluster: initReader failed: {s}", .{@errorName(err)});
                continue;
            };
            if (reader.accepted()) {
                log.info("join accepted by {s}", .{target});
                return;
            }

            const reason = reader.reasonCode();
            log.info("joinCluster: rejected by {s}, reason={d}", .{ target, reason });
            if (reason == node_mod.Node.JoinReason.not_leader) {
                const leader_addr = reader.leaderAddr();
                if (leader_addr.len > 0) {
                    log.info("joinCluster: redirecting to leader at {s}", .{leader_addr});
                    var resp2 = requestJoin(allocator, leader_addr, cluster_id, node_id, self_addr) catch |err| {
                        log.err("joinCluster: redirect requestJoin failed: {s}", .{@errorName(err)});
                        continue;
                    };
                    defer resp2.deinit();
                    const r2 = resp2.initReader() catch continue;
                    if (r2.accepted()) {
                        log.info("join accepted by leader at {s}", .{leader_addr});
                        return;
                    }
                    log.info("joinCluster: leader redirect also rejected, reason={d}", .{r2.reasonCode()});
                }
            }
        }

        log.info("joinCluster: backing off {d}ms", .{backoff_ms});
        zio.sleep(zio.Duration.fromMilliseconds(backoff_ms)) catch return error.Cancelled;
        backoff_ms = @min(backoff_ms * 2, 5_000);
    }
    return error.JoinFailed;
}
