const std = @import("std");
const zio = @import("zio");
const raft = @import("raft");
const zdb = @import("zdb");
const runtime = @import("runtime");
const proto = @import("protocol.zig");
const m = @import("metrics");
const WaitGroup = @import("raft").utils.WaitGroup;

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

    pub fn asStateMachine(self: *StorageImpl) raft.Node.StateMachine {
        return .{
            .ctx = @ptrCast(self),
            .applyFn = &applyEntry,
            .writeFileFn = &writeFile,
            .readFileFn = &readFile,
            .hasFileFn = &hasFile,
            .preAppendFn = &preAppend,
        };
    }

    fn applyEntry(ctx: *anyopaque, entry: raft.Entry, data: []const u8) void {
        // Empty-data entries (clock ticks) have no state machine work — the
        // raft node still advances `apply_ts` after we return, so the tick's
        // timestamp becomes visible to blocked readers.
        if (data.len == 0) return;
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));
        while (true) {
            self.applyEntryInner(entry, data) catch |err| {
                log.err("on_apply: failed at index={d}: {s}, retrying", .{ entry.index, @errorName(err) });
                zio.sleep(zio.Duration.fromMilliseconds(100)) catch return;
                continue;
            };
            return;
        }
    }

    fn applyEntryInner(self: *StorageImpl, entry: raft.Entry, data: []const u8) !void {
        const parsed = zdb_bufs.parseMessageWithTail(data) catch {
            log.err("on_apply: unknown entry type at index={d}", .{entry.index});
            return;
        };
        switch (parsed) {
            .Mutations => |ms| {
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
        _ = index;
        const mcs = try zdb_bufs.ManifestChangeStruct.fromReader(self.allocator, mc);
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
            1 => try self.forest.newMemtable(mcs.memtable_id),
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

    /// Ensure all SST files referenced by ManifestChange entries are on disk
    /// before the WAL append. Fetches missing files from the leader.
    fn preAppend(ctx: *anyopaque, node: *raft.Node, entries: []const raft.buffers.LogEntryBuffer) void {
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));

        // Collect missing file IDs across all entries.
        var missing = std.ArrayList(u64).empty;
        defer missing.deinit(self.allocator);

        for (entries) |entry| {
            const reader = entry.initReader() catch continue;
            if (!reader.isNormal()) continue;
            const data = reader.data();
            if (data.len == 0) continue;

            // Try parsing as ManifestChange.
            const parsed = zdb_bufs.parseMessageWithTail(data) catch continue;
            switch (parsed) {
                .ManifestChange => |mc| {
                    const tc_len = mc.tableChangesLen();
                    for (0..tc_len) |i| {
                        const tc = mc.tableChangesAt(@intCast(i)) catch continue;
                        if (tc.op() == 0 and !self.forest.hasFile(tc.tableId())) {
                            missing.append(self.allocator, tc.tableId()) catch continue;
                        }
                    }
                },
                else => {},
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

pub const ZdbServer = struct {
    allocator: std.mem.Allocator,
    forest: *Forest,
    node: *raft.Node,
    listen_addr: zio.net.IpAddress,

    pub fn init(
        allocator: std.mem.Allocator,
        forest: *Forest,
        node: *raft.Node,
        client_addr: []const u8,
    ) !ZdbServer {
        const addr = try zio.net.IpAddress.parseIpAndPort(client_addr);
        return .{
            .allocator = allocator,
            .forest = forest,
            .node = node,
            .listen_addr = addr,
        };
    }

    pub fn deinit(self: *ZdbServer) void {
        _ = self;
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
        log.info("compaction loops started", .{});
        var group = zio.group();
        group.spawn(runMemtableLoop, .{self}) catch return;
        group.spawn(runFlushLoop, .{self}) catch return;
        group.spawn(runLevelsLoop, .{self}) catch return;
        group.wait() catch {};
        log.info("compaction loops exiting (shutdown)", .{});
    }

    fn runMemtableLoop(self: *ZdbServer) void {
        const interval = zio.Duration.fromMilliseconds(100);
        while (!self.node.isShuttingDown()) {
            if (!self.node.isLeader() or !self.forest.shouldFreeze()) {
                zio.sleep(interval) catch return;
                continue;
            }
            self.proposeNewMemtable() catch |err| {
                log.err("proposeNewMemtable failed: {s}", .{@errorName(err)});
            };
        }
    }

    fn runFlushLoop(self: *ZdbServer) void {
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
    fn runLevelsLoop(self: *ZdbServer) void {
        const interval = zio.Duration.fromMilliseconds(500);
        while (!self.node.isShuttingDown()) {
            if (!self.node.isLeader()) {
                zio.sleep(interval) catch return;
                continue;
            }
            if (self.forest.bestCompactionCandidate()) |target| {
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
            } else {
                zio.sleep(interval) catch return;
            }
        }
    }

    fn proposeNewMemtable(self: *ZdbServer) !void {
        const new_id = self.forest.next_file_id.fetchAdd(1, .monotonic);
        const payload = try encodeManifestChange(self.allocator, 1, &.{}, &.{}, new_id);
        defer self.allocator.free(payload);
        try self.node.proposeAndWait(payload);
    }

    fn proposeFlush(self: *ZdbServer) !void {
        const result = try self.forest.buildFlush() orelse return;
        var changes = result.changes;
        defer changes.deinit(self.allocator);
        if (changes.items.len == 0) return;

        try self.distributeNewFiles(changes.items);

        const payload = try encodeManifestChange(self.allocator, 2, changes.items, &.{}, result.memtable_id);
        defer self.allocator.free(payload);
        try self.node.proposeAndWait(payload);
    }

    fn proposeCompaction(self: *ZdbServer, target: Forest.CompactionTarget) !void {
        var changes = try target.tree.buildCompaction(target.candidate.level, &self.forest.next_file_id);
        defer changes.deinit(self.allocator);
        if (changes.items.len == 0) return;

        try self.distributeNewFiles(changes.items);

        const payload = try encodeManifestChange(self.allocator, 3, changes.items, &.{}, 0);
        defer self.allocator.free(payload);
        try self.node.proposeAndWait(payload);
    }

    fn proposeSplit(self: *ZdbServer, tree_id: u64) !void {
        // buildSplit allocates table_changes / tree_changes on the arena;
        // encodeManifestChange copies the bytes into a self-owned payload,
        // so the arena can be torn down right after encode. No new SST
        // files are produced (split just remaps existing tables between
        // trees), so distributeNewFiles is unnecessary here. cmd=4 routes
        // through applyManifestChange to applySplit on every node.
        var arena = std.heap.ArenaAllocator.init(self.allocator);
        defer arena.deinit();
        const mc = try self.forest.buildSplit(arena.allocator(), tree_id) orelse return;

        const payload = try encodeManifestChange(
            self.allocator,
            mc.cmd,
            mc.table_changes,
            mc.tree_changes,
            0,
        );
        defer self.allocator.free(payload);
        try self.node.proposeAndWait(payload);
    }

    /// Send new SST files to peers and wait for quorum acks.
    fn distributeNewFiles(self: *ZdbServer, changes: []const manifest.TableChange) !void {
        var file_ids = std.ArrayList(u64).empty;
        defer file_ids.deinit(self.allocator);
        for (changes) |c| {
            if (c.op == 0) try file_ids.append(self.allocator, c.table_id);
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
            const cmd = proto.readCommand(r, line_buf) catch |err| {
                // A parse error leaves the byte stream in an unknown state
                // (mid-line, oversized line, etc.) — continuing the loop
                // here would spam ERR until the client gives up. Close
                // instead; the client can reconnect with a fresh stream.
                log.warn("client parse error after {d} ops: {s} — closing", .{ ops, @errorName(err) });
                proto.writeErr(stream, @errorName(err)) catch {};
                return;
            } orelse {
                log.info("client disconnected after {d} ops (EOF)", .{ops});
                return;
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
                .raft => |_| {
                    self.handleRaft(stream) catch return;
                },
                .help => |_| {
                    proto.writeHelp(stream) catch return;
                },
            }
        }
        log.info("client handler exiting after {d} ops (shutdown)", .{ops});
    }

    fn handleSet(self: *ZdbServer, stream: zio.net.Stream, req: proto.SetCmd) !void {
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

    fn handleRaft(self: *ZdbServer, stream: zio.net.Stream) !void {
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
            .forest => try self.forest.writeStats(&body.writer, .{ .scope = .forest }),
            .tree => try self.forest.writeStats(&body.writer, .{ .scope = .{ .tree = req.tree_id } }),
            .namespace => try self.forest.writeStats(&body.writer, .{ .scope = .{ .namespace = req.ns_id } }),
        }
        try body.writer.writeByte('\n');
        try stream.writeAll(body.writer.buffered(), .none);
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

        self.node.proposeAndWait(payload) catch |err| {
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
