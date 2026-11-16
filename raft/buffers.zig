// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const schema = @import("schema");

// tag 0 reserved for undefined — a freshly-zeroed byte never decodes as a real change.
pub const ChangeType = schema.EnumDef(&.{
    .{ .name = "unknown", .tag = 0 },
    .{ .name = "add_voter", .tag = 1 },
    .{ .name = "add_learner", .tag = 2 },
    .{ .name = "to_voter", .tag = 3 },
    .{ .name = "to_learner", .tag = 4 },
    .{ .name = "remove_node", .tag = 5 },
    .{ .name = "update_addr", .tag = 6 },
});

pub const ConfChange = schema.Table(&.{
    .{ .id = 1, .name = "change_type", .ty = ChangeType },
    .{ .id = 2, .name = "node_id", .ty = schema.U64 },
    .{ .id = 3, .name = "addr", .ty = schema.Bytes },
});

pub const Role = schema.EnumDef(&.{
    .{ .name = "unknown", .tag = 0 },
    .{ .name = "voter", .tag = 1 },
    .{ .name = "learner", .tag = 2 },
});

pub const ConfStatePeer = schema.Table(&.{
    .{ .id = 1, .name = "id", .ty = schema.U64 },
    .{ .id = 2, .name = "addr", .ty = schema.Bytes },
    .{ .id = 3, .name = "role", .ty = Role },
});

pub const ConfState = schema.Table(&.{
    .{ .id = 1, .name = "peers", .ty = schema.Array(ConfStatePeer) },
});

// Persisted Raft "small frequently-updated" state. All fields fit easily into
// a single 4 KiB metadata page (see raft/meta.zig::MetaStore). zbuf forward-
// compat means new fields can be added without breaking older on-disk data.
pub const HardState = schema.Table(&.{
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
    // The log index of the `.snapshot` entry last installed, 0 for none:
    // that entry is the durable snapshot, read back from the log at open
    // (DiskWAL.setSnapshot). Set once the entry is on disk, and before
    // the log is cut below the snapshot.
    .{ .id = 5, .name = "snapshot_entry_index", .ty = schema.U64 },
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

pub const Snapshot = schema.Table(&.{
    .{ .id = 1, .name = "index", .ty = schema.U64 },
    .{ .id = 2, .name = "term", .ty = schema.U64 },
    // The state machine's, opaque here: where it keeps the state as of
    // `index`, not the state itself, so the entry stays small. The
    // engine's is a zdb `SnapshotObject`.
    .{ .id = 3, .name = "data", .ty = schema.Bytes },
    .{ .id = 4, .name = "conf_state", .ty = ConfState },
    // Recovery hint for the state machine: log index of the oldest in-memory
    // memtable on the leader at snapshot-creation time. Mutations at indices
    // strictly less than this are durable in SSTs that the post-snapshot
    // replay will install via flush proposals, so the state machine can skip
    // putting them into the placeholder memtable (where they'd be discarded
    // anyway). 0 means "skip nothing" — backward-compat default.
    .{ .id = 5, .name = "oldest_memtable_index", .ty = schema.U64 },
    // The oldest timestamp a reader may ask for once this snapshot is
    // installed: the HLC timestamp of the log entry at `index`, or older
    // when a read still open on a voting node was older (the state
    // machine picks it). 0 when unknown. The orphan scan reads it too: a file
    // written after this moment may belong to an entry the snapshot does
    // not cover, and an older moment only spares more files.
    .{ .id = 6, .name = "timestamp", .ty = schema.U64 },
});

// tag 0 = .any: serves both as the safe default for an uninitialized byte and
// as the wildcard filter in `EntriesRangeOpts.ty` (no real entry on the wire
// is ever .any, so it never matches an actual entry).
//
// `manifest` is a peer of `data` (not a flag on top of it): its payload is an
// LSM ManifestChange zbuf rather than a Mutations zbuf. Splitting it out lets
// the snapshot builder filter manifest-only entries via `entriesRange(.ty = .manifest)`
// without parsing each frame's payload.
pub const EntryType = schema.EnumDef(&.{
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
pub const WalSlot = schema.Table(&.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "index", .ty = schema.U64 },
    .{ .id = 3, .name = "timestamp", .ty = schema.U64 },
    .{ .id = 4, .name = "data_offset", .ty = schema.U32 },
    .{ .id = 5, .name = "data_len", .ty = schema.U32 },
    .{ .id = 6, .name = "flags", .ty = schema.U16 },
    .{ .id = 7, .name = "entry_type", .ty = EntryType },
});

pub const WalHeader = schema.Table(&.{
    .{ .id = 1, .name = "slots", .ty = schema.Array(WalSlot) },
});

pub const LogEntry = schema.Table(&.{
    .{ .id = 1, .name = "index", .ty = schema.U64 },
    .{ .id = 2, .name = "term", .ty = schema.U64 },
    // HLC-derived monotonic timestamp, assigned by the leader inside the
    // propose critical section (so log order == timestamp order).
    .{ .id = 3, .name = "timestamp", .ty = schema.U64 },
    .{ .id = 4, .name = "data", .ty = schema.Bytes },
    .{ .id = 5, .name = "entry_type", .ty = EntryType },
    .{ .id = 6, .name = "conf_change", .ty = ConfChange },
    // How `data` is stored: a `compress.Codec` tag, 0 for as proposed.
    // `Node.propose` compresses; readers go through
    // `compression.maybeUnpack`.
    .{ .id = 7, .name = "codec", .ty = schema.U8 },
});

pub const RequestVote = schema.Table(&.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "candidate_id", .ty = schema.U64 },
    .{ .id = 3, .name = "last_log_index", .ty = schema.U64 },
    .{ .id = 4, .name = "last_log_term", .ty = schema.U64 },

    .{ .id = 5, .name = "status", .ty = schema.U16 },
    .{ .name = "is_prevote", .ty = schema.Bool(.{ .field_id = 5, .bit = 0 }) },
});

pub const RequestVoteReply = schema.Table(&.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "from", .ty = schema.U64 },

    .{ .id = 3, .name = "status", .ty = schema.U16 },
    .{ .name = "is_prevote", .ty = schema.Bool(.{ .field_id = 3, .bit = 0 }) },
    .{ .name = "vote_granted", .ty = schema.Bool(.{ .field_id = 3, .bit = 1 }) },
});

pub const AppendEntries = schema.Table(&.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "leader_id", .ty = schema.U64 },
    .{ .id = 3, .name = "leader_commit", .ty = schema.U64 },
    .{ .id = 4, .name = "prev_log_index", .ty = schema.U64 },
    .{ .id = 5, .name = "prev_log_term", .ty = schema.U64 },
    .{ .id = 6, .name = "entries", .ty = schema.Array(LogEntry) },
});

pub const AppendEntriesReply = schema.Table(&.{
    .{ .id = 1, .name = "term", .ty = schema.U64 },
    .{ .id = 2, .name = "status", .ty = schema.U16 },
    .{ .name = "success", .ty = schema.Bool(.{ .field_id = 2, .bit = 0 }) },
    .{ .id = 3, .name = "from", .ty = schema.U64 },
    .{ .id = 4, .name = "last_index", .ty = schema.U64 },
    .{ .id = 5, .name = "conflict_term", .ty = schema.U64 },
    .{ .id = 6, .name = "conflict_index", .ty = schema.U64 },
    // The oldest timestamp the sender still reads at, or may yet begin
    // a read at (`StateMachine.minReadTs`): the leader keeps the next
    // snapshot's timestamp, the read floor, at or below what every
    // voter reports. 0: nothing to report.
    .{ .id = 7, .name = "min_read_ts", .ty = schema.U64 },
});

pub const TransportHandshake = schema.Table(&.{
    .{ .id = 1, .name = "cluster_id", .ty = schema.U64 },
    .{ .id = 2, .name = "node_id", .ty = schema.U64 },
});

pub const JoinRequest = schema.Table(&.{
    .{ .id = 1, .name = "cluster_id", .ty = schema.U64 },
    .{ .id = 2, .name = "node_id", .ty = schema.U64 },
    .{ .id = 3, .name = "addr", .ty = schema.Bytes },
});

pub const JoinResponse = schema.Table(&.{
    .{ .id = 1, .name = "status", .ty = schema.U16 },
    .{ .name = "accepted", .ty = schema.Bool(.{ .field_id = 1, .bit = 0 }) },
    .{ .id = 2, .name = "leader_id", .ty = schema.U64 },
    .{ .id = 3, .name = "reason_code", .ty = schema.U32 },
    .{ .id = 4, .name = "leader_addr", .ty = schema.Bytes },
});

// File-distribution control plane: after uploading a batch of new SSTs to
// the shared remote store, the creator announces them; each peer opens
// them from the store into its table cache, then replies with one batch
// AnnounceFilesResp. A prefetch optimization — the apply-time open is the
// backstop — so raft relays file references, never file bytes.

// One announced SST: its id and what the manifest will say about it, so
// the peer's open is the same single read the apply's would be.
pub const FileRef = schema.Table(&.{
    .{ .id = 1, .name = "id", .ty = schema.U64 },
    .{ .id = 2, .name = "size", .ty = schema.U64 },
    .{ .id = 3, .name = "index_offset", .ty = schema.U64 },
});

pub const AnnounceFiles = schema.Table(&.{
    .{ .id = 1, .name = "announce_id", .ty = schema.U64 },
    // sender_id in case the files are available on a non-leader peer.
    .{ .id = 2, .name = "sender_id", .ty = schema.U64 },
    .{ .id = 3, .name = "files", .ty = schema.Array(FileRef) },
});

// tag 0 reserved — a freshly-zeroed byte never decodes as a real status.
pub const AnnounceStatus = schema.EnumDef(&.{
    .{ .name = "unknown", .tag = 0 },
    .{ .name = "ok", .tag = 1 },
    .{ .name = "fail", .tag = 2 },
});

pub const AnnounceFilesResp = schema.Table(&.{
    .{ .id = 1, .name = "announce_id", .ty = schema.U64 },
    .{ .id = 2, .name = "node_id", .ty = schema.U64 },
    .{ .id = 3, .name = "status", .ty = AnnounceStatus },
    .{ .id = 4, .name = "fail_error", .ty = schema.Bytes },
});

// zbuf tail currently stores table_id as u16, so we reserve high bits in u16
// for module-level namespacing and OR local ids into the low bits.
const RAFT_TABLE_NS: u16 = 0xA500;

fn tableId(comptime local_id: u16) u16 {
    return RAFT_TABLE_NS | local_id;
}

pub const records: []const schema.Named = &.{
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
    .{ .name = "AnnounceFiles", .ty = AnnounceFiles, .id = tableId(16) },
    .{ .name = "AnnounceFilesResp", .ty = AnnounceFilesResp, .id = tableId(17) },
    .{ .name = "FileRef", .ty = FileRef, .id = tableId(18) },
};
