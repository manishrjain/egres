const std = @import("std");
const msg = @import("message.zig");
const log_mod = @import("log.zig");
const wal_mod = @import("wal.zig");
const config_mod = @import("config.zig");

pub const NodeId = msg.NodeId;
pub const Message = msg.Message;
pub const MessageType = msg.MessageType;
pub const RequestVote = msg.RequestVote;
pub const RequestVoteReply = msg.RequestVoteReply;
pub const AppendEntries = msg.AppendEntries;
pub const AppendEntriesReply = msg.AppendEntriesReply;
pub const LogEntry = msg.LogEntry;
pub const State = msg.State;

pub const Config = config_mod.Config;
pub const Log = log_mod.Log;
pub const Wal = wal_mod.Wal;
pub const WalError = wal_mod.WalError;

pub const RaftError = error{
    NotLeader,
    InvalidMessage,
} || WalError;

pub const PeerState = struct {
    next_index: u64,
    match_index: u64,
};

pub const Outbound = struct {
    to: NodeId,
    msg: Message,
};

pub const Node = struct {
    id: NodeId,
    peers: []const NodeId,

    current_term: u64,
    voted_for: ?NodeId,
    log: Log,

    state: State,
    commit_index: u64,
    last_applied: u64,
    leader_id: ?NodeId,

    votes_received: std.AutoHashMap(NodeId, void),
    peer_states: std.AutoHashMap(NodeId, PeerState),

    elapsed_ticks: u32,
    election_timeout: u32,
    election_timeout_min: u32,
    election_timeout_max: u32,
    heartbeat_interval: u32,

    wal: Wal,
    allocator: std.mem.Allocator,

    outbox: std.ArrayList(Outbound),
    prng: std.rand.DefaultPrng,

    pub fn init(
        allocator: std.mem.Allocator,
        id: NodeId,
        peers: []const NodeId,
        wal: Wal,
        config: Config,
        is_spectator: bool,
    ) !Node {
        try config.validate();
        const seed = @as(u64, @intCast(std.time.nanoTimestamp()));
        const prng = std.rand.DefaultPrng.init(seed ^ id);
        var node = Node{
            .id = id,
            .peers = peers,
            .current_term = 0,
            .voted_for = null,
            .log = Log.init(allocator),
            .state = .follower,
            .commit_index = 0,
            .last_applied = 0,
            .leader_id = null,
            .votes_received = std.AutoHashMap(NodeId, void).init(allocator),
            .peer_states = std.AutoHashMap(NodeId, PeerState).init(allocator),
            .elapsed_ticks = 0,
            .election_timeout = config.election_timeout_min,
            .election_timeout_min = config.election_timeout_min,
            .election_timeout_max = config.election_timeout_max,
            .heartbeat_interval = config.heartbeat_interval,
            .wal = wal,
            .allocator = allocator,
            .outbox = std.ArrayList(Outbound).init(allocator),
            .prng = prng,
        };
        if (is_spectator) {
            node.state = .spectator;
        }
        node.resetElectionTimeout();
        return node;
    }

    pub fn deinit(self: *Node) void {
        self.log.deinit();
        self.votes_received.deinit();
        self.peer_states.deinit();
        self.outbox.deinit();
    }

    pub fn tick(self: *Node) RaftError!void {
        self.elapsed_ticks += 1;
        switch (self.state) {
            .leader => if (self.elapsed_ticks >= self.heartbeat_interval) {
                self.elapsed_ticks = 0;
                try self.broadcastHeartbeat();
            },
            .follower, .candidate => if (self.elapsed_ticks >= self.election_timeout) {
                try self.startElection();
            },
            .spectator => {},
        }
    }

    pub fn handleMessage(self: *Node, message: Message) RaftError!void {
        const msg_term = message.term();
        if (msg_term > self.current_term) {
            try self.stepDown(msg_term);
        }

        switch (message) {
            .request_vote => |rv| try self.handleRequestVote(rv),
            .request_vote_reply => |rvr| try self.handleRequestVoteReply(rvr),
            .append_entries => |ae| try self.handleAppendEntries(ae),
            .append_entries_reply => |aer| try self.handleAppendEntriesReply(aer),
        }
    }

    pub fn startElection(self: *Node) RaftError!void {
        if (self.state == .spectator) return;
        self.state = .candidate;
        self.current_term += 1;
        self.voted_for = self.id;
        self.leader_id = null;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout();

        self.votes_received.clearRetainingCapacity();
        try self.votes_received.put(self.id, {});

        try self.persistHardState();

        const rv = RequestVote{
            .term = self.current_term,
            .candidate_id = self.id,
            .last_log_index = self.log.lastIndex(),
            .last_log_term = self.log.lastTerm(),
        };

        for (self.peers) |peer| {
            self.send(peer, Message{ .request_vote = rv });
        }
    }

    pub fn becomeLeader(self: *Node) RaftError!void {
        if (self.state == .spectator) return;
        self.state = .leader;
        self.leader_id = self.id;
        self.elapsed_ticks = 0;

        self.peer_states.clearRetainingCapacity();
        const next_index = self.log.lastIndex() + 1;
        for (self.peers) |peer| {
            try self.peer_states.put(peer, .{ .next_index = next_index, .match_index = 0 });
        }

        try self.broadcastHeartbeat();
    }

    pub fn broadcastHeartbeat(self: *Node) RaftError!void {
        for (self.peers) |peer| {
            try self.sendAppendEntries(peer);
        }
    }

    pub fn sendAppendEntries(self: *Node, peer: NodeId) RaftError!void {
        const prev_index = self.log.lastIndex();
        const prev_term = self.log.lastTerm();
        const entries = &[_]LogEntry{};
        const ae = AppendEntries{
            .term = self.current_term,
            .leader_id = self.id,
            .prev_log_index = prev_index,
            .prev_log_term = prev_term,
            .entries = entries,
            .leader_commit = self.commit_index,
        };
        self.send(peer, Message{ .append_entries = ae });
    }

    pub fn handleRequestVote(self: *Node, rv: RequestVote) RaftError!void {
        if (rv.term < self.current_term) {
            self.send(rv.candidate_id, Message{ .request_vote_reply = .{
                .term = self.current_term,
                .vote_granted = false,
                .from = self.id,
            } });
            return;
        }

        if (self.state == .spectator) {
            self.send(rv.candidate_id, Message{ .request_vote_reply = .{
                .term = self.current_term,
                .vote_granted = false,
                .from = self.id,
            } });
            return;
        }

        const can_vote = self.voted_for == null or self.voted_for.? == rv.candidate_id;
        const up_to_date = self.isLogUpToDate(rv.last_log_term, rv.last_log_index);

        var granted = false;
        if (can_vote and up_to_date) {
            self.voted_for = rv.candidate_id;
            self.elapsed_ticks = 0;
            self.resetElectionTimeout();
            try self.persistHardState();
            granted = true;
        }

        self.send(rv.candidate_id, Message{ .request_vote_reply = .{
            .term = self.current_term,
            .vote_granted = granted,
            .from = self.id,
        } });
    }

    pub fn handleRequestVoteReply(self: *Node, rvr: RequestVoteReply) RaftError!void {
        if (self.state != .candidate) return;
        if (rvr.term < self.current_term) return;
        if (!rvr.vote_granted) return;

        _ = try self.votes_received.put(rvr.from, {});
        if (self.votes_received.count() >= self.quorumSize()) {
            try self.becomeLeader();
        }
    }

    pub fn handleAppendEntries(self: *Node, ae: AppendEntries) RaftError!void {
        if (ae.term < self.current_term) {
            self.send(ae.leader_id, Message{ .append_entries_reply = .{
                .term = self.current_term,
                .success = false,
                .from = self.id,
                .match_index = 0,
                .conflict_term = 0,
                .conflict_index = self.log.lastIndex() + 1,
            } });
            return;
        }

        if (self.state != .follower and self.state != .spectator) {
            self.state = .follower;
        }
        self.leader_id = ae.leader_id;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout();

        if (ae.prev_log_index > 0) {
            const prev = self.log.getEntry(ae.prev_log_index);
            if (prev == null or prev.?.term != ae.prev_log_term) {
                self.send(ae.leader_id, Message{ .append_entries_reply = .{
                    .term = self.current_term,
                    .success = false,
                    .from = self.id,
                    .match_index = 0,
                    .conflict_term = if (prev) |p| p.term else 0,
                    .conflict_index = if (prev) |p| self.log.firstIndexOfTerm(p.term) else self.log.lastIndex() + 1,
                } });
                return;
            }
        }

        if (ae.entries.len > 0) {
            const first_new_index = ae.prev_log_index + 1;
            if (self.log.getEntry(first_new_index)) |existing| {
                if (existing.term != ae.entries[0].term) {
                    self.log.truncateFrom(first_new_index);
                }
            }
            try self.log.appendSlice(ae.entries);
            try self.persistLog(ae.entries);
        }

        if (ae.leader_commit > self.commit_index) {
            const last_index = self.log.lastIndex();
            self.commit_index = if (ae.leader_commit < last_index) ae.leader_commit else last_index;
        }

        self.send(ae.leader_id, Message{ .append_entries_reply = .{
            .term = self.current_term,
            .success = true,
            .from = self.id,
            .match_index = self.log.lastIndex(),
            .conflict_term = 0,
            .conflict_index = 0,
        } });
    }

    pub fn handleAppendEntriesReply(self: *Node, aer: AppendEntriesReply) RaftError!void {
        if (self.state != .leader) return;
        if (aer.term < self.current_term) return;

        if (!aer.success) {
            if (self.peer_states.getPtr(aer.from)) |state| {
                if (aer.conflict_index > 0) {
                    state.next_index = aer.conflict_index;
                } else if (state.next_index > 1) {
                    state.next_index -= 1;
                }
            }
            return;
        }

        if (self.peer_states.getPtr(aer.from)) |state| {
            state.match_index = aer.match_index;
            state.next_index = aer.match_index + 1;
        }
        self.advanceCommitIndex();
    }

    pub fn propose(self: *Node, data: []const u8) RaftError!u64 {
        if (self.state != .leader) return RaftError.NotLeader;
        const new_index = self.log.lastIndex() + 1;
        const entry = LogEntry{ .index = new_index, .term = self.current_term, .data = data };
        try self.log.append(entry);
        try self.persistLog(&[_]LogEntry{entry});
        for (self.peers) |peer| {
            try self.sendAppendEntries(peer);
        }
        return new_index;
    }

    pub fn isLeader(self: *const Node) bool {
        return self.state == .leader;
    }

    pub fn getLeaderId(self: *const Node) ?NodeId {
        return self.leader_id;
    }

    pub fn clusterSize(self: *const Node) u32 {
        return @intCast(self.peers.len + 1);
    }

    pub fn persistHardState(self: *Node) RaftError!void {
        try self.wal.writeHardState(self.current_term, self.voted_for);
        try self.wal.sync();
    }

    pub fn persistLog(self: *Node, entries: []const LogEntry) RaftError!void {
        try self.wal.appendEntries(entries);
        try self.wal.sync();
    }

    pub fn isLogUpToDate(self: *const Node, candidate_term: u64, candidate_index: u64) bool {
        const last_term = self.log.lastTerm();
        if (candidate_term > last_term) return true;
        if (candidate_term < last_term) return false;
        return candidate_index >= self.log.lastIndex();
    }

    pub fn send(self: *Node, to: NodeId, message: Message) void {
        _ = self.outbox.append(.{ .to = to, .msg = message }) catch {
            @panic("raft: outbox append failed");
        };
    }

    pub fn takeOutbox(self: *Node) []Outbound {
        return self.outbox.items;
    }

    pub fn clearOutbox(self: *Node) void {
        self.outbox.clearRetainingCapacity();
    }

    pub fn quorumSize(self: *const Node) usize {
        const size: usize = self.peers.len + 1;
        return (size / 2) + 1;
    }

    fn resetElectionTimeout(self: *Node) void {
        const rand = self.prng.random();
        self.election_timeout = rand.intRangeAtMost(u32, self.election_timeout_min, self.election_timeout_max);
    }

    fn stepDown(self: *Node, new_term: u64) RaftError!void {
        if (self.state == .spectator) {
            self.state = .spectator;
        } else {
            self.state = .follower;
        }
        self.current_term = new_term;
        self.voted_for = null;
        self.leader_id = null;
        self.elapsed_ticks = 0;
        self.resetElectionTimeout();
        try self.persistHardState();
    }

    fn advanceCommitIndex(self: *Node) void {
        _ = self;
        // TODO: compute majority match index and advance commit_index safely.
    }
};
