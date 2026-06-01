const std = @import("std");
const fiber = @import("fiber");
const serial_mod = @import("serial");
const utils = @import("utils");
const Serial = serial_mod.Serial;
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.transport);

const builtin = @import("builtin");

const handshake_size: usize = 18;

fn configureSocket(conn: fiber.net.Stream) void {
    // NODELAY is applied at socket creation by fiber.net. Here we add raft's
    // aggressive keepalive policy (peer-death detection) on top.
    conn.setKeepAlive(true) catch {};

    // Set keepalive parameters: 5s idle, 1s interval, 3 probes → ~8s detection.
    const os_tag = comptime builtin.os.tag;
    if (os_tag == .windows) return;

    const is_macos = os_tag == .macos;
    const TCP = if (is_macos) std.c.TCP else std.os.linux.TCP;
    const IDLE = if (is_macos) TCP.KEEPALIVE else TCP.KEEPIDLE;

    const fd = conn.handle();
    const tcp_proto = std.posix.IPPROTO.TCP;
    std.posix.setsockopt(fd, tcp_proto, IDLE, &std.mem.toBytes(@as(c_int, 5))) catch {};
    std.posix.setsockopt(fd, tcp_proto, TCP.KEEPINTVL, &std.mem.toBytes(@as(c_int, 1))) catch {};
    std.posix.setsockopt(fd, tcp_proto, TCP.KEEPCNT, &std.mem.toBytes(@as(c_int, 3))) catch {};
}

/// Transport holds 2 TCP connections per peer (P0 and P1) using simple serial framing.
pub const Transport = struct {
    pub const Priority = enum(u8) { p0 = 0, p1 = 1 };

    pub const Config = struct {
        self_id: u64,
        cluster_id: u64,
        listen_addr: []const u8, // "ip:port"
        ctx: *anyopaque,
        on_message_p0: serial_mod.Serial.OnMessageFn,
        on_message_p1: serial_mod.Serial.OnMessageFn,
    };

    const PeerSlot = struct {
        addr: ?[]const u8, // "ip:port" string, null for inbound-only peers
        conn: [2]?*Serial, // indexed by @intFromEnum(Priority)
        dial_loops: std.atomic.Value(u8) = std.atomic.Value(u8).init(0),

        fn deinit(self: *PeerSlot, allocator: Allocator) void {
            for (&self.conn) |*c| {
                if (c.*) |s| {
                    s.deinit();
                    c.* = null;
                }
            }
            if (self.addr) |a| allocator.free(a);
            allocator.destroy(self);
        }
    };

    allocator: Allocator,
    cfg: Config,
    listen_addr: fiber.net.IpAddress,
    mu: utils.TrackedMutex,
    peers: std.AutoHashMap(u64, *PeerSlot),
    group: fiber.Group(void),
    listener: ?fiber.net.Server,
    shutting_down: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn init(allocator: Allocator, cfg: Config) !Transport {
        const listen_addr = try fiber.net.IpAddress.parseIpAndPort(cfg.listen_addr);
        const peers = std.AutoHashMap(u64, *PeerSlot).init(allocator);
        log.info("node {d}: initialized, cluster={d}", .{ cfg.self_id, cfg.cluster_id });
        return .{
            .allocator = allocator,
            .cfg = cfg,
            .listen_addr = listen_addr,
            .mu = .{},
            .peers = peers,
            .group = fiber.Group(void).init(allocator),
            .listener = null,
        };
    }

    pub fn deinit(self: *Transport) void {
        log.info("node {d}: Transport shutting down", .{self.cfg.self_id});
        self.shutting_down.store(true, .release);

        // Close all connections so reader/writer loops exit.
        var num_closed: u32 = 0;
        {
            self.mu.lock(@src());
            defer self.mu.unlock();
            var it = self.peers.valueIterator();
            while (it.next()) |slot_ptr| {
                for (&slot_ptr.*.conn) |*c| {
                    if (c.*) |s| {
                        s.closeConn();
                        num_closed += 1;
                    }
                }
            }
        }
        log.info("Transport close Conns are done {d}", .{num_closed});

        if (self.listener) |*l| l.shutdown(.both) catch {};
        log.info("Transport l.close is done", .{});
        self.group.wait();
        self.group.deinit();
        log.debug("node {d}: Transport group stopped", .{self.cfg.self_id});

        var it = self.peers.valueIterator();
        while (it.next()) |slot_ptr| {
            slot_ptr.*.deinit(self.allocator);
        }
        self.peers.deinit();
        self.mu.deinit();
    }

    /// Add a peer dynamically and start dialing it.
    /// Safe to call while the server is running.
    pub fn addPeer(self: *Transport, id: u64, addr: []const u8) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        log.debug("addPeer for {d} {s}\n", .{ id, addr });

        if (self.peers.get(id)) |slot| {
            if (slot.addr) |existing| {
                if (std.mem.eql(u8, existing, addr)) return; // Same address.
                // Address changed — close existing connections so dial loop reconnects.
                log.info("node {d}: addr changed for peer {d} to {s}", .{
                    self.cfg.self_id, id, addr,
                });
                for (&slot.conn) |*c| {
                    if (c.*) |s| {
                        s.closeConn();
                        c.* = null;
                    }
                }
                self.allocator.free(existing);
                slot.addr = try self.allocator.dupe(u8, addr);
                // Dial loops already running — they'll pick up the new address.
                return;
            }
            // First time we have an address — set it and start dial loops.
            log.debug("addPeer got address {s} for peer {d}\n", .{ addr, id });
            slot.addr = try self.allocator.dupe(u8, addr);
            for ([_]Priority{ .p0, .p1 }) |pri| {
                self.group.spawn(.{ .lane = .raft }, dialLoop, .{ self, id, pri }) catch |err| {
                    log.err("failed to spawn dial loop for peer {d}: {s}", .{ id, @errorName(err) });
                };
            }
            return;
        }

        const slot = try self.allocator.create(PeerSlot);
        slot.* = .{ .addr = try self.allocator.dupe(u8, addr), .conn = .{ null, null } };
        try self.peers.put(id, slot);

        log.info("node {d}: added peer {d} addr={s}", .{ self.cfg.self_id, id, addr });

        // Start dial loops for this peer.
        for ([_]Priority{ .p0, .p1 }) |pri| {
            self.group.spawn(.{ .lane = .raft }, dialLoop, .{ self, id, pri }) catch {};
        }
    }

    pub fn removePeer(self: *Transport, id: u64) void {
        self.mu.lock(@src());
        defer self.mu.unlock();

        const slot = self.peers.get(id) orelse return;
        slot.deinit(self.allocator);
        _ = self.peers.remove(id);
        log.info("node {d}: removed peer {d}", .{ self.cfg.self_id, id });
    }

    /// Start the server. Spawns accept and dial coroutines.
    /// Non-blocking — returns immediately. Must be called inside zio runtime.
    pub fn run(self: *Transport) !void {
        if (self.listener == null) {
            self.listener = self.listen_addr.listen(.{ .reuse_address = true }) catch |err| {
                log.err("node {d}: listen failed on {s}: {}", .{ self.cfg.self_id, self.cfg.listen_addr, err });
                return error.ListenFailed;
            };
        }
        log.info("node {d}: listening on {s}", .{ self.cfg.self_id, self.cfg.listen_addr });

        try self.group.spawn(.{ .lane = .raft }, acceptLoop, .{self});
        try self.group.spawn(.{ .lane = .raft }, livenessLoop, .{self});

        // Spawn one dial coroutine per (peer, priority) pair.
        var it = self.peers.iterator();
        while (it.next()) |kv| {
            const peer_id = kv.key_ptr.*;
            for ([_]Priority{ .p0, .p1 }) |pri| {
                self.group.spawn(.{ .lane = .raft }, dialLoop, .{ self, peer_id, pri }) catch {};
            }
        }
    }

    /// Get Serial connection for a peer at given priority. Returns null if not connected.
    pub fn getPeerConn(self: *Transport, peer_id: u64, priority: Priority) ?*Serial {
        self.mu.lock(@src());
        defer self.mu.unlock();
        const slot = self.peers.get(peer_id) orelse return null;
        return slot.conn[@intFromEnum(priority)];
    }

    /// Set drop_messages on all connections for a specific peer.
    pub fn setDropForPeer(self: *Transport, peer_id: u64, drop: bool) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        const slot = self.peers.get(peer_id) orelse return;
        for (&slot.conn) |c| {
            if (c) |s| s.drop_messages.store(drop, .release);
        }
    }

    /// Set drop_messages on all connections for all peers.
    pub fn setDropAll(self: *Transport, drop: bool) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        var it = self.peers.valueIterator();
        while (it.next()) |slot_ptr| {
            for (&slot_ptr.*.conn) |c| {
                if (c) |s| s.drop_messages.store(drop, .release);
            }
        }
    }

    /// Return the actual bound port (useful when listening on port 0).
    pub fn getListenPort(self: *Transport) u16 {
        const listener = self.listener orelse return 0;
        return listener.port();
    }

    /// Queue a refcounted payload for sending to a peer. Non-blocking.
    /// Borrows `value`: `queueMsg` incrRefs on successful enqueue (writer
    /// decrRefs after sending). Caller retains its own ref and is responsible
    /// for decrRef'ing when done. Fan-out is just calling this once per peer.
    pub fn sendMsg(self: *Transport, peer_id: u64, priority: Priority, message_type: u16, value: *utils.RefValue) !void {
        const s = self.getPeerConn(peer_id, priority) orelse return error.PeerNotConnected;
        s.queueMsg(message_type, value);
    }

    fn acceptLoop(self: *Transport) void {
        log.debug("acceptLoop started", .{});
        defer log.debug("node {d}: accept loop exiting", .{self.cfg.self_id});
        while (true) {
            const conn = self.listener.?.accept(.{}) catch return;
            configureSocket(conn);
            self.group.spawn(.{ .lane = .raft }, handleInbound, .{ self, conn }) catch {
                conn.close();
            };
        }
    }

    fn handleInbound(self: *Transport, conn: fiber.net.Stream) void {
        log.debug("handleInbound for Transport", .{});
        defer log.debug("handleInbound DONE", .{});

        const hs = recvHandshake(conn) catch |err| {
            log.warn(
                "node {d}: inbound handshake failed. err: {}",
                .{ self.cfg.self_id, err },
            );
            conn.close();
            return;
        };
        log.debug("node {d}: inbound handshake from node {d} pri={s} purpose={s}", .{
            self.cfg.self_id, hs.node_id, @tagName(hs.priority), @tagName(hs.purpose),
        });

        if (hs.cluster_id != self.cfg.cluster_id) {
            log.warn("node {d}: rejected inbound from node {d}: cluster mismatch (got {d}, want {d})", .{
                self.cfg.self_id, hs.node_id, hs.cluster_id, self.cfg.cluster_id,
            });
            conn.close();
            return;
        }

        sendHandshake(conn, self.cfg.cluster_id, self.cfg.self_id, hs.priority, hs.purpose) catch {
            log.warn("node {d}: failed to send reply handshake to node {d}", .{ self.cfg.self_id, hs.node_id });
            conn.close();
            return;
        };

        const s = Serial.init(self.allocator, conn) catch |err| {
            log.err("node {d}: failed to init Serial: {s}", .{ self.cfg.self_id, @errorName(err) });
            conn.close();
            return;
        };

        if (hs.purpose == .join) {
            // Temporary join connection — handle inline, don't install.
            defer s.deinit();
            const on_message = self.cfg.on_message_p0;
            log.info("node {d}: join conn from node {d}", .{ self.cfg.self_id, hs.node_id });
            // Read and handle the single join request message.
            const msg = s.recvOne() catch |err| {
                log.err("node {d}: join recvOne failed: {s}", .{ self.cfg.self_id, @errorName(err) });
                return;
            };
            defer if (msg.payload.len > 0) self.allocator.free(msg.payload);
            on_message(self.cfg.ctx, s, msg.message_type, msg.payload);
            return;
        }

        // Permanent connection.
        self.getOrCreateSlot(hs.node_id) catch {
            s.deinit();
            return;
        };

        const installed = self.installConn(hs.node_id, hs.priority, s);
        if (installed != s) {
            s.deinit();
            return;
        }

        const on_message = if (hs.priority == .p0) self.cfg.on_message_p0 else self.cfg.on_message_p1;
        log.info("node {d}: inbound conn established with node {d} pri={s}", .{
            self.cfg.self_id, hs.node_id, @tagName(hs.priority),
        });
        s.run(self.cfg.ctx, on_message);

        log.info("node {d}: inbound conn disconnected from node {d} pri={s}", .{
            self.cfg.self_id, hs.node_id, @tagName(hs.priority),
        });
        self.removeConn(hs.node_id, hs.priority, s);
    }

    fn dialLoop(self: *Transport, peer_id: u64, priority: Priority) void {
        const pri_idx = @intFromEnum(priority);

        {
            self.mu.lock(@src());
            defer self.mu.unlock();
            if (self.peers.get(peer_id)) |slot| {
                _ = slot.dial_loops.fetchAdd(1, .acq_rel);
            }
        }
        defer {
            self.mu.lock(@src());
            defer self.mu.unlock();
            if (self.peers.get(peer_id)) |slot| {
                _ = slot.dial_loops.fetchSub(1, .acq_rel);
            }
        }

        log.debug("node {d}: starting dial loop for peer {d} pri={s}", .{
            self.cfg.self_id, peer_id, @tagName(priority),
        });

        var backoff_ms: u64 = 250;
        while (!self.shutting_down.load(.acquire)) {
            const state = blk: {
                self.mu.lock(@src());
                defer self.mu.unlock();
                const slot = self.peers.get(peer_id) orelse return;
                break :blk .{
                    .addr = slot.addr orelse return,
                    .connected = slot.conn[pri_idx] != null,
                };
            };
            if (state.connected) {
                fiber.sleep(backoff_ms);
                continue;
            }

            const addr = fiber.net.IpAddress.parseIpAndPort(state.addr) catch |err| {
                log.err("node {d}: failed to parse addr for peer {d}: {s}", .{ self.cfg.self_id, peer_id, @errorName(err) });
                fiber.sleep(backoff_ms);
                backoff_ms = @min(backoff_ms * 2, 5000);
                continue;
            };
            const conn = addr.connect(.{}) catch {
                fiber.sleep(backoff_ms);
                backoff_ms = @min(backoff_ms * 2, 5000);
                continue;
            };
            configureSocket(conn);

            sendHandshake(conn, self.cfg.cluster_id, self.cfg.self_id, priority, .permanent) catch {
                conn.close();
                fiber.sleep(backoff_ms);
                backoff_ms = @min(backoff_ms * 2, 5000);
                continue;
            };

            const hs = recvHandshake(conn) catch {
                conn.close();
                fiber.sleep(backoff_ms);
                backoff_ms = @min(backoff_ms * 2, 5000);
                continue;
            };

            if (hs.cluster_id != self.cfg.cluster_id) {
                log.warn("node {d}: dial to peer {d}: cluster mismatch (got {d})", .{
                    self.cfg.self_id, peer_id, hs.cluster_id,
                });
                conn.close();
                fiber.sleep(backoff_ms);
                backoff_ms = @min(backoff_ms * 2, 5000);
                continue;
            }

            const s = Serial.init(self.allocator, conn) catch {
                conn.close();
                fiber.sleep(backoff_ms);
                continue;
            };

            const installed = self.installConn(peer_id, priority, s);
            if (installed != s) {
                s.deinit();
                fiber.sleep(backoff_ms);
                continue;
            }

            backoff_ms = 250;
            const on_message = if (priority == .p0) self.cfg.on_message_p0 else self.cfg.on_message_p1;
            log.info("node {d}: outbound conn established with peer {d} pri={s}", .{
                self.cfg.self_id, peer_id, @tagName(priority),
            });

            s.run(self.cfg.ctx, on_message);
            log.info("node {d}: outbound conn disconnected from peer {d} pri={s}", .{
                self.cfg.self_id, peer_id, @tagName(priority),
            });
            self.removeConn(peer_id, priority, s);
        }
    }

    const probe_interval_ms: i64 = 2_000;
    const dead_threshold_ms: i64 = 10_000;

    fn livenessLoop(self: *Transport) void {
        defer log.debug("exiting liveness loop", .{});

        var to_probe = std.ArrayList(*Serial).empty;
        defer to_probe.deinit(self.allocator);

        while (!self.shutting_down.load(.acquire)) {
            to_probe.clearRetainingCapacity();
            fiber.sleep(1000);
            if (self.shutting_down.load(.acquire)) {
                return;
            }

            {
                self.mu.lock(@src());
                defer self.mu.unlock();

                const now = fiber.coarseMs();
                var peer_it = self.peers.iterator();
                while (peer_it.next()) |kv| {
                    const slot = kv.value_ptr.*;
                    // Check dial loop health.
                    const loops = slot.dial_loops.load(.acquire);
                    if (loops != 2) {
                        log.err("liveness: peer {d} has {d} dial loops (expected 2)", .{ kv.key_ptr.*, loops });
                    }
                    for (&slot.conn) |*c| {
                        const s = c.* orelse continue;
                        const elapsed = now - s.last_recv_ms.load(.acquire);
                        if (elapsed > dead_threshold_ms) {
                            log.info("liveness: closing dead conn (no recv in {d}ms)", .{elapsed});
                            s.closeConn();
                            c.* = null;
                        } else if (elapsed > probe_interval_ms) {
                            to_probe.append(self.allocator, s) catch {};
                        }
                    }
                }
            }

            for (to_probe.items) |s| {
                s.send(serial_mod.msg_type_ping, &.{}) catch |err| {
                    log.err("liveness: ping failed: {s}", .{@errorName(err)});
                };
            }
        }
    }

    /// Get or create a PeerSlot for a peer (no address, inbound-only).
    fn getOrCreateSlot(self: *Transport, peer_id: u64) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();

        if (self.peers.contains(peer_id)) return;

        const slot = try self.allocator.create(PeerSlot);
        slot.* = .{ .addr = null, .conn = .{ null, null } };
        try self.peers.put(peer_id, slot);
    }

    fn installConn(self: *Transport, peer_id: u64, priority: Priority, s: *Serial) *Serial {
        const pri_idx = @intFromEnum(priority);

        self.mu.lock(@src());
        defer self.mu.unlock();

        const slot = self.peers.get(peer_id) orelse return s;

        if (slot.conn[pri_idx]) |existing| {
            log.debug("node {d}: found existing conn for peer {d} pri={d}", .{
                self.cfg.self_id, peer_id, pri_idx,
            });
            // The caller can close this new conn, we don't need to do it here.
            return existing;
        }
        log.debug("node {d}: installed conn to peer {d} pri={d}", .{
            self.cfg.self_id, peer_id, pri_idx,
        });

        slot.conn[pri_idx] = s;
        return s;
    }

    fn removeConn(self: *Transport, peer_id: u64, priority: Priority, s: *Serial) void {
        const pri_idx = @intFromEnum(priority);

        self.mu.lock(@src());
        defer self.mu.unlock();

        const slot = self.peers.get(peer_id) orelse {
            s.deinit();
            return;
        };

        if (slot.conn[pri_idx] == s) {
            slot.conn[pri_idx] = null;
        }
        s.deinit();
    }

    pub const Purpose = enum(u8) { permanent = 0, join = 1 };

    pub const Handshake = struct {
        cluster_id: u64,
        node_id: u64,
        priority: Priority,
        purpose: Purpose,
    };

    pub fn sendHandshake(conn: fiber.net.Stream, cluster_id: u64, node_id: u64, priority: Priority, purpose: Purpose) !void {
        var buf: [handshake_size]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], cluster_id, .little);
        std.mem.writeInt(u64, buf[8..16], node_id, .little);
        buf[16] = @intFromEnum(priority);
        buf[17] = @intFromEnum(purpose);
        conn.writeAll(&buf) catch return error.HandshakeFailed;
    }

    pub fn recvHandshake(conn: fiber.net.Stream) !Handshake {
        var buf: [handshake_size]u8 = undefined;
        var off: usize = 0;
        while (off < buf.len) {
            const n = conn.read(buf[off..]) catch return error.HandshakeFailed;
            if (n == 0) return error.HandshakeFailed;
            off += n;
        }
        const priority: Priority = switch (buf[16]) {
            0 => .p0,
            1 => .p1,
            else => return error.InvalidHandshake,
        };
        const purpose: Purpose = switch (buf[17]) {
            0 => .permanent,
            1 => .join,
            else => return error.InvalidHandshake,
        };
        return .{
            .cluster_id = std.mem.readInt(u64, buf[0..8], .little),
            .node_id = std.mem.readInt(u64, buf[8..16], .little),
            .priority = priority,
            .purpose = purpose,
        };
    }
};
