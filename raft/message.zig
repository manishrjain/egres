pub const NodeId = u64;

pub const State = enum {
    follower,
    candidate,
    leader,
    learner,
};

pub const MessageType = enum {
    request_vote,
    request_vote_reply,
    append_entries,
    append_entries_reply,
    pre_vote,
    pre_vote_reply,
};

pub const EntryType = enum(u8) {
    normal,
    conf_change,
};

pub const ConfChangeType = enum(u8) {
    add_voter,
    add_learner,
    to_voter,
    to_learner,
    remove_node,
};

pub const ConfChange = struct {
    change_type: ConfChangeType,
    node_id: NodeId,
};

pub const LogEntry = struct {
    index: u64,
    term: u64,
    data: []const u8,
    ty: EntryType = .normal,
    conf_change: ?ConfChange = null,
};

pub const RequestVote = struct {
    term: u64,
    candidate_id: NodeId,
    last_log_index: u64,
    last_log_term: u64,
};

pub const PreVote = struct {
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

pub const PreVoteReply = struct {
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
    pre_vote: PreVote,
    pre_vote_reply: PreVoteReply,

    pub fn msgType(self: Message) MessageType {
        return @as(MessageType, self);
    }

    pub fn term(self: Message) u64 {
        return switch (self) {
            .request_vote => |m| m.term,
            .request_vote_reply => |m| m.term,
            .append_entries => |m| m.term,
            .append_entries_reply => |m| m.term,
            .pre_vote => |m| m.term,
            .pre_vote_reply => |m| m.term,
        };
    }
};
