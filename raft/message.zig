pub const NodeId = u64;

pub const State = enum {
    follower,
    candidate,
    leader,
    spectator,
};

pub const MessageType = enum {
    request_vote,
    request_vote_reply,
    append_entries,
    append_entries_reply,
};

pub const LogEntry = struct {
    index: u64,
    term: u64,
    data: []const u8,
};

pub const RequestVote = struct {
    term: u64,
    candidate_id: NodeId,
    last_log_index: u64,
    last_log_term: u64,
};

pub const RequestVoteReply = struct {
    term: u64,
    vote_granted: bool,
    from: NodeId,
};

pub const AppendEntries = struct {
    term: u64,
    leader_id: NodeId,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: []const LogEntry,
    leader_commit: u64,
};

pub const AppendEntriesReply = struct {
    term: u64,
    success: bool,
    from: NodeId,
    match_index: u64,
    conflict_term: u64,
    conflict_index: u64,
};

pub const Message = union(MessageType) {
    request_vote: RequestVote,
    request_vote_reply: RequestVoteReply,
    append_entries: AppendEntries,
    append_entries_reply: AppendEntriesReply,

    pub fn msgType(self: Message) MessageType {
        return @as(MessageType, self);
    }

    pub fn term(self: Message) u64 {
        return switch (self) {
            .request_vote => |m| m.term,
            .request_vote_reply => |m| m.term,
            .append_entries => |m| m.term,
            .append_entries_reply => |m| m.term,
        };
    }
};
