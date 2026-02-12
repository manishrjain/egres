const schema = @import("schema");

// Enums from message.zig are encoded as u32 for now.
pub const ConfChange = schema.Table(.{
    .{ .id = 1, .name = "change_type", .ty = schema.B64 },
    .{ .name = "add_voter", .ty = schema.Bool(.{ .field_id = 1, .bit = 0 }) },
    .{ .name = "add_learner", .ty = schema.Bool(.{ .field_id = 1, .bit = 1 }) },
    .{ .name = "to_voter", .ty = schema.Bool(.{ .field_id = 1, .bit = 2 }) },
    .{ .name = "to_learner", .ty = schema.Bool(.{ .field_id = 1, .bit = 3 }) },
    .{ .name = "remove_node", .ty = schema.Bool(.{ .field_id = 1, .bit = 4 }) },
    .{ .id = 2, .name = "node_id", .ty = schema.U64 },
});

pub const LogEntry = schema.Table(.{
    .{ .id = 1, .name = "index", .ty = schema.U64 },
    .{ .id = 2, .name = "term", .ty = schema.U64 },
    .{ .id = 3, .name = "data", .ty = schema.Bytes },
    .{ .id = 4, .name = "entry_type", .ty = schema.B64 },
    .{ .name = "normal", .ty = schema.Bool(.{ .field_id = 4, .bit = 0 }) },
    .{ .name = "conf_change", .ty = schema.Bool(.{ .field_id = 4, .bit = 1 }) },
    .{ .id = 5, .name = "conf_change_payload", .ty = ConfChange },
});

pub const RequestVote = schema.Table(.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "candidate_id", .ty = schema.U64 },
    .{ .id = 3, .name = "last_log_index", .ty = schema.U64 },
    .{ .id = 4, .name = "last_log_term", .ty = schema.U64 },
});

pub const RequestVoteReply = schema.Table(.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "status", .ty = schema.B64 },
    .{ .name = "vote_granted", .ty = schema.Bool(.{ .field_id = 2, .bit = 0 }) },
    .{ .id = 3, .name = "from", .ty = schema.U64 },
});

pub const PreVote = schema.Table(.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "candidate_id", .ty = schema.U64 },
    .{ .id = 3, .name = "last_log_index", .ty = schema.U64 },
    .{ .id = 4, .name = "last_log_term", .ty = schema.U64 },
});

pub const PreVoteReply = schema.Table(.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "status", .ty = schema.B64 },
    .{ .name = "vote_granted", .ty = schema.Bool(.{ .field_id = 2, .bit = 0 }) },
    .{ .id = 3, .name = "from", .ty = schema.U64 },
});

pub const AppendEntries = schema.Table(.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "leader_id", .ty = schema.U64 },
    .{ .id = 3, .name = "prev_log_index", .ty = schema.U64 },
    .{ .id = 4, .name = "prev_log_term", .ty = schema.U64 },
    .{ .id = 5, .name = "entries", .ty = schema.Array(LogEntry) },
    .{ .id = 6, .name = "leader_commit", .ty = schema.U64 },
});

pub const AppendEntriesReply = schema.Table(.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "status", .ty = schema.B64 },
    .{ .name = "success", .ty = schema.Bool(.{ .field_id = 2, .bit = 0 }) },
    .{ .id = 3, .name = "from", .ty = schema.U64 },
    .{ .id = 4, .name = "match_index", .ty = schema.U64 },
    .{ .id = 5, .name = "conflict_term", .ty = schema.U64 },
    .{ .id = 6, .name = "conflict_index", .ty = schema.U64 },
});

// zbuf tail currently stores table_id as u16, so we reserve high bits in u16
// for module-level namespacing and OR local ids into the low bits.
const RAFT_TABLE_NS: u16 = 0xA500;

fn tableId(comptime local_id: u16) u16 {
    return RAFT_TABLE_NS | local_id;
}

pub const tables = .{
    .{ .name = "ConfChange", .ty = ConfChange, .id = tableId(1) },
    .{ .name = "LogEntry", .ty = LogEntry, .id = tableId(2) },
    .{ .name = "RequestVote", .ty = RequestVote, .id = tableId(3) },
    .{ .name = "RequestVoteReply", .ty = RequestVoteReply, .id = tableId(4) },
    .{ .name = "PreVote", .ty = PreVote, .id = tableId(5) },
    .{ .name = "PreVoteReply", .ty = PreVoteReply, .id = tableId(6) },
    .{ .name = "AppendEntries", .ty = AppendEntries, .id = tableId(7) },
    .{ .name = "AppendEntriesReply", .ty = AppendEntriesReply, .id = tableId(8) },
};
