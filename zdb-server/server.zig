const std = @import("std");
const zio = @import("zio");
const raft = @import("raft");
const zdb = @import("zdb");
const runtime = @import("runtime");
const proto = @import("protocol.zig");
const WaitGroup = @import("raft").utils.WaitGroup;

const zdb_bufs = zdb.buffers;
const manifest = zdb.manifest;
const Forest = zdb.forest.Forest;
const Item = zdb.key.Item;
const Key = zdb.key.Key;
const vfs = zdb.vfs;
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

    fn applyEntry(ctx: *anyopaque, index: u64, _term: u64, data: []const u8) void {
        _ = _term;
        const self: *StorageImpl = @ptrCast(@alignCast(ctx));
        while (true) {
            self.applyEntryInner(index, data) catch |err| {
                log.err("on_apply: failed at index={d}: {s}, retrying", .{ index, @errorName(err) });
                zio.sleep(zio.Duration.fromMilliseconds(100)) catch return;
                continue;
            };
            return;
        }
    }

    fn applyEntryInner(self: *StorageImpl, index: u64, data: []const u8) !void {
        const parsed = zdb_bufs.parseMessageWithTail(data) catch {
            log.err("on_apply: unknown entry type at index={d}", .{index});
            return;
        };
        switch (parsed) {
            .Mutation => |mutation| {
                const items = [_]Item{.{
                    .key = .{
                        .namespace = mutation.namespace(),
                        .primary = mutation.key(),
                        .timestamp = mutation.timestamp(),
                    },
                    .value = mutation.value(),
                }};
                try self.forest.put(&items);
            },
            .ManifestChange => |mc| {
                try self.applyManifestChange(index, mc);
            },
            else => {
                log.err("on_apply: unhandled message type at index={d}", .{index});
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
                self.proposeCompaction(target) catch |err| {
                    log.err("proposeCompaction failed: {s}", .{@errorName(err)});
                };
            } else {
                zio.sleep(interval) catch return;
            }
        }
    }

    fn proposeNewMemtable(self: *ZdbServer) !void {
        const new_id = self.forest.next_file_id.fetchAdd(1, .monotonic);
        const payload = try encodeManifestChange(self.allocator, 1, &.{}, &.{}, new_id);
        defer self.allocator.free(payload);
        try self.proposeAndWait(payload);
    }

    fn proposeFlush(self: *ZdbServer) !void {
        const result = try self.forest.buildFlush() orelse return;
        var changes = result.changes;
        defer changes.deinit(self.allocator);
        if (changes.items.len == 0) return;

        try self.distributeNewFiles(changes.items);

        const payload = try encodeManifestChange(self.allocator, 2, changes.items, &.{}, result.memtable_id);
        defer self.allocator.free(payload);
        try self.proposeAndWait(payload);
    }

    fn proposeCompaction(self: *ZdbServer, target: Forest.CompactionTarget) !void {
        var changes = try target.tree.buildCompaction(target.candidate.level, &self.forest.next_file_id);
        defer changes.deinit(self.allocator);
        if (changes.items.len == 0) return;

        try self.distributeNewFiles(changes.items);

        const payload = try encodeManifestChange(self.allocator, 3, changes.items, &.{}, 0);
        defer self.allocator.free(payload);
        try self.proposeAndWait(payload);
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

    fn proposeAndWait(self: *ZdbServer, payload: []const u8) !void {
        var ctx = CommitCtx{ .status = .committed };
        ctx.wg.init();
        ctx.wg.add(1);
        try self.node.proposeDataChange(payload, &onCommit, @ptrCast(&ctx));
        ctx.wg.wait() catch return error.CommitFailed;
        if (ctx.status != .committed) return error.NotCommitted;
    }

    fn handleClient(self: *ZdbServer, stream: zio.net.Stream) void {
        defer stream.close();
        var ops: u64 = 0;
        var buf: [64 * 1024]u8 = undefined;

        while (!self.node.isShuttingDown()) {
            const cmd = proto.readCommand(stream, &buf) catch |err| {
                if (ops > 0) {
                    log.info("client disconnected after {d} ops: {s}", .{ ops, @errorName(err) });
                } else {
                    log.debug("client error: {s}", .{@errorName(err)});
                }
                // Send error back for parse errors, then continue.
                proto.writeErr(stream, @errorName(err)) catch return;
                continue;
            } orelse {
                log.info("client disconnected after {d} ops (EOF)", .{ops});
                return;
            };
            ops += 1;
            switch (cmd) {
                .set => |req| {
                    log.debug("SET ns={d} key={s} ts={d} value_len={d}", .{
                        req.ns, req.key, req.ts, req.value.len,
                    });
                    self.handleSet(stream, req) catch return;
                },
                .get => |req| {
                    log.debug("GET ns={d} key={s} ts={d}", .{ req.ns, req.key, req.ts });
                    self.handleGet(stream, req) catch return;
                },
                .conf => |req| {
                    log.info("CONF {s} node={d}", .{ @tagName(req.action), req.node_id });
                    self.handleConf(stream, req) catch return;
                },
            }
        }
        log.info("client handler exiting after {d} ops (shutdown)", .{ops});
    }

    fn handleSet(self: *ZdbServer, stream: zio.net.Stream, req: proto.SetCmd) !void {
        // Encode mutation as zbuf for the raft proposal.
        var mutation_writer = try zdb_bufs.MutationWriter.init(self.allocator);
        defer mutation_writer.deinit();
        try mutation_writer.set(.{
            .namespace = req.ns,
            .key = req.key,
            .value = req.value,
            .timestamp = req.ts,
        });
        var mutation_buf = try mutation_writer.finishWithTail();
        defer mutation_buf.deinit();
        const proposal_bytes = mutation_buf.bytes orelse return error.EncodeFailed;

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

    fn handleGet(self: *ZdbServer, stream: zio.net.Stream, req: proto.GetCmd) !void {
        const search_key = Key{
            .namespace = req.ns,
            .primary = req.key,
            .timestamp = req.ts,
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
};

const CommitCtx = struct {
    wg: WaitGroup = .{},
    status: raft.CommitStatus,
};

fn onCommit(ctx_raw: *anyopaque, _entry: raft.ProposalEntry, status: raft.CommitStatus) void {
    _ = _entry;
    const ctx: *CommitCtx = @ptrCast(@alignCast(ctx_raw));
    ctx.status = status;
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
