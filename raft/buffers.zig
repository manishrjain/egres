const schema = @import("schema");

// tag 0 reserved for undefined — a freshly-zeroed byte never decodes as a real change.
pub const ChangeType = schema.EnumDef(.{
    .{ .name = "unknown", .tag = 0 },
    .{ .name = "add_voter", .tag = 1 },
    .{ .name = "add_learner", .tag = 2 },
    .{ .name = "to_voter", .tag = 3 },
    .{ .name = "to_learner", .tag = 4 },
    .{ .name = "remove_node", .tag = 5 },
    .{ .name = "update_addr", .tag = 6 },
});

pub const ConfChange = schema.Table(.{
    .{ .id = 1, .name = "change_type", .ty = schema.Enum(ChangeType) },
    .{ .id = 2, .name = "node_id", .ty = schema.U64 },
    .{ .id = 3, .name = "addr", .ty = schema.Bytes },
});

pub const Role = schema.EnumDef(.{
    .{ .name = "unknown", .tag = 0 },
    .{ .name = "voter", .tag = 1 },
    .{ .name = "learner", .tag = 2 },
});

pub const ConfStatePeer = schema.Table(.{
    .{ .id = 1, .name = "id", .ty = schema.U64 },
    .{ .id = 2, .name = "addr", .ty = schema.Bytes },
    .{ .id = 3, .name = "role", .ty = schema.Enum(Role) },
});

pub const ConfState = schema.Table(.{
    .{ .id = 1, .name = "peers", .ty = schema.Array(ConfStatePeer) },
});

// Persisted Raft "small frequently-updated" state. All fields fit easily into
// a single 4 KiB metadata page (see raft/meta.zig::MetaStore). zbuf forward-
// compat means new fields can be added without breaking older on-disk data.
pub const HardState = schema.Table(.{
    // Storage-internal monotonic counter. MetaStore writes alternating
    // slots; on recovery the slot with the higher `seq` wins (tie-break:
    // slot B). Callers outside MetaStore should treat this as opaque —
    // it's not part of raft's protocol-level semantics.
    .{ .id = 1, .name = "seq", .ty = schema.U64 },

    .{ .id = 2, .name = "current_term", .ty = schema.U64 },
    .{ .id = 3, .name = "voted_for", .ty = schema.U64 },
    // raft_id of the node that owns this metadata file. Persisted so a
    // misplaced data directory can't be silently picked up by a different
    // node id and confused for that node's state.
    .{ .id = 4, .name = "raft_id", .ty = schema.U64 },
    // Highest log index covered by the most recent durable snapshot. Lets
    // recovery short-circuit replay below this index. 0 = no snapshot yet.
    .{ .id = 5, .name = "checkpoint_index", .ty = schema.U64 },
    // Persisted Raft commit_index. Per the Raft paper this is volatile and
    // re-derived via replication, but persisting it (lazily — see
    // `commitPersistLoop`) lets restart skip the "wait for first
    // current-term commit before old entries can apply" delay. Safe due to
    // the Leader Completeness Property: once committed, an entry stays
    // committed across restarts and leader changes. Staleness is bounded
    // by the persist cadence; on crash recovery we just re-derive the
    // tail through normal raft.
    .{ .id = 6, .name = "commit_index", .ty = schema.U64 },
});

pub const Snapshot = schema.Table(.{
    .{ .id = 1, .name = "index", .ty = schema.U64 },
    .{ .id = 2, .name = "term", .ty = schema.U64 },
    .{ .id = 3, .name = "data", .ty = schema.Bytes },
    .{ .id = 4, .name = "conf_state", .ty = ConfState },
    // Recovery hint for the state machine: log index of the oldest in-memory
    // memtable on the leader at snapshot-creation time. Mutations at indices
    // strictly less than this are durable in SSTs that the post-snapshot
    // replay will install via flush proposals, so the state machine can skip
    // putting them into the placeholder memtable (where they'd be discarded
    // anyway). 0 means "skip nothing" — backward-compat default.
    .{ .id = 5, .name = "oldest_memtable_index", .ty = schema.U64 },
});

// tag 0 = .any: serves both as the safe default for an uninitialized byte and
// as the wildcard filter in `EntriesRangeOpts.ty` (no real entry on the wire
// is ever .any, so it never matches an actual entry).
//
// `manifest` is a peer of `data` (not a flag on top of it): its payload is an
// LSM ManifestChange zbuf rather than a Mutations zbuf. Splitting it out lets
// the snapshot builder filter manifest-only entries via `entriesRange(.ty = .manifest)`
// without parsing each frame's payload.
pub const EntryType = schema.EnumDef(.{
    .{ .name = "any", .tag = 0 },
    .{ .name = "data", .tag = 1 },
    .{ .name = "conf", .tag = 2 },
    .{ .name = "manifest", .tag = 3 },
    // Carries a marshalled SnapshotStruct as its payload. Leader-only at
    // propose time; the apply path inside Node parses + installs (bypasses
    // the state machine, like .conf).
    .{ .name = "snapshot", .tag = 4 },
});

// One row of the WAL's slot index. Slots live in the file header region;
// each slot points at a CRC-framed record in the data region. `timestamp`
// is the HLC timestamp from the LogEntry — copying it into the slot lets
// snapshot policy ("retain at least N minutes of activity") read the slot
// region without parsing payload bytes. Wire format is fixed-size; the
// reader treats an all-zero slot as the empty sentinel via index() == 0.
pub const WalSlot = schema.Table(.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "index", .ty = schema.U64 },
    .{ .id = 3, .name = "timestamp", .ty = schema.U64 },
    .{ .id = 4, .name = "data_offset", .ty = schema.U32 },
    .{ .id = 5, .name = "data_len", .ty = schema.U32 },
    .{ .id = 6, .name = "flags", .ty = schema.U16 },
    .{ .id = 7, .name = "entry_type", .ty = schema.Enum(EntryType) },
});

pub const WalHeader = schema.Table(.{
    .{ .id = 1, .name = "slots", .ty = schema.Array(WalSlot) },
});

pub const LogEntry = schema.Table(.{
    .{ .id = 1, .name = "index", .ty = schema.U64 },
    .{ .id = 2, .name = "term", .ty = schema.U64 },
    // HLC-derived monotonic timestamp, assigned by the leader inside the
    // propose critical section (so log order == timestamp order).
    .{ .id = 3, .name = "timestamp", .ty = schema.U64 },
    .{ .id = 4, .name = "data", .ty = schema.Bytes },
    .{ .id = 5, .name = "entry_type", .ty = schema.Enum(EntryType) },
    .{ .id = 6, .name = "conf_change", .ty = ConfChange },
});

pub const RequestVote = schema.Table(.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "candidate_id", .ty = schema.U64 },
    .{ .id = 3, .name = "last_log_index", .ty = schema.U64 },
    .{ .id = 4, .name = "last_log_term", .ty = schema.U64 },

    .{ .id = 5, .name = "status", .ty = schema.U16 },
    .{ .name = "is_prevote", .ty = schema.Bool(.{ .field_id = 5, .bit = 0 }) },
});

pub const RequestVoteReply = schema.Table(.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "from", .ty = schema.U64 },

    .{ .id = 3, .name = "status", .ty = schema.U16 },
    .{ .name = "is_prevote", .ty = schema.Bool(.{ .field_id = 3, .bit = 0 }) },
    .{ .name = "vote_granted", .ty = schema.Bool(.{ .field_id = 3, .bit = 1 }) },
});

pub const AppendEntries = schema.Table(.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "leader_id", .ty = schema.U64 },
    .{ .id = 3, .name = "leader_commit", .ty = schema.U64 },
    .{ .id = 4, .name = "prev_log_index", .ty = schema.U64 },
    .{ .id = 5, .name = "prev_log_term", .ty = schema.U64 },
    .{ .id = 6, .name = "entries", .ty = schema.Array(LogEntry) },
});

pub const AppendEntriesReply = schema.Table(.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "status", .ty = schema.U16 },
    .{ .name = "success", .ty = schema.Bool(.{ .field_id = 2, .bit = 0 }) },
    .{ .id = 3, .name = "from", .ty = schema.U64 },
    .{ .id = 4, .name = "last_index", .ty = schema.U64 },
    .{ .id = 5, .name = "conflict_term", .ty = schema.U64 },
    .{ .id = 6, .name = "conflict_index", .ty = schema.U64 },
});

pub const TransportHandshake = schema.Table(.{
    .{ .id = 1, .name = "cluster_id", .ty = schema.U64 },
    .{ .id = 2, .name = "node_id", .ty = schema.U64 },
});

pub const JoinRequest = schema.Table(.{
    .{ .id = 1, .name = "cluster_id", .ty = schema.U64 },
    .{ .id = 2, .name = "node_id", .ty = schema.U64 },
    .{ .id = 3, .name = "addr", .ty = schema.Bytes },
});

pub const JoinResponse = schema.Table(.{
    .{ .id = 1, .name = "status", .ty = schema.U16 },
    .{ .name = "accepted", .ty = schema.Bool(.{ .field_id = 1, .bit = 0 }) },
    .{ .id = 2, .name = "leader_id", .ty = schema.U64 },
    .{ .id = 3, .name = "reason_code", .ty = schema.U32 },
    .{ .id = 4, .name = "leader_addr", .ty = schema.Bytes },
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
    .{ .name = "AppendEntries", .ty = AppendEntries, .id = tableId(5) },
    .{ .name = "AppendEntriesReply", .ty = AppendEntriesReply, .id = tableId(6) },
    .{ .name = "TransportHandshake", .ty = TransportHandshake, .id = tableId(7) },
    .{ .name = "JoinRequest", .ty = JoinRequest, .id = tableId(8) },
    .{ .name = "JoinResponse", .ty = JoinResponse, .id = tableId(9) },
    .{ .name = "ConfStatePeer", .ty = ConfStatePeer, .id = tableId(10) },
    .{ .name = "ConfState", .ty = ConfState, .id = tableId(11) },
    .{ .name = "HardState", .ty = HardState, .id = tableId(12) },
    .{ .name = "Snapshot", .ty = Snapshot, .id = tableId(13) },
    .{ .name = "WalSlot", .ty = WalSlot, .id = tableId(14) },
    .{ .name = "WalHeader", .ty = WalHeader, .id = tableId(15) },
};
