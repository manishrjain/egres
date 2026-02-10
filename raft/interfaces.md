# Raft Interfaces -- Type Definitions

All types described in a Zig-like pseudocode notation. These are the structs,
enums, and function signatures that make up the public and internal surface
of the Raft module.

---

## Enums

```zig
const State = enum {
    follower,
    candidate,
    leader,
};

const MessageType = enum {
    request_vote,
    request_vote_reply,
    append_entries,
    append_entries_reply,
};
```

---

## Core Structs

### NodeId

```zig
/// Unique identifier for a node in the cluster.
/// Could be a simple integer or a hash. Must be comparable and hashable.
const NodeId = u64;
```

### LogEntry

```zig
const LogEntry = struct {
    index: u64,         // 1-indexed, monotonically increasing
    term: u64,          // the term when this entry was created
    data: []const u8,   // opaque payload; NO_OP if data.len == 0
};
```

### HardState (persisted)

```zig
/// The subset of state that MUST be persisted to stable storage
/// before responding to any RPC.
const HardState = struct {
    current_term: u64,
    voted_for: ?NodeId,     // null if not voted in this term
};
```

### PeerState (leader only, volatile)

```zig
/// Per-peer replication tracking, maintained only by the leader.
const PeerState = struct {
    next_index: u64,    // next log index to send to this peer
    match_index: u64,   // highest log index known to be replicated on this peer
};
```

---

## Message Types

### RequestVote

```zig
const RequestVote = struct {
    term: u64,              // candidate's term
    candidate_id: NodeId,   // candidate requesting vote
    last_log_index: u64,    // index of candidate's last log entry
    last_log_term: u64,     // term of candidate's last log entry
};
```

### RequestVoteReply

```zig
const RequestVoteReply = struct {
    term: u64,          // currentTerm of the voter, for candidate to update itself
    vote_granted: bool, // true = candidate received vote
    from: NodeId,       // who sent this reply
};
```

### AppendEntries

```zig
const AppendEntries = struct {
    term: u64,              // leader's term
    leader_id: NodeId,      // so follower can redirect clients
    prev_log_index: u64,    // index of log entry immediately preceding new ones
    prev_log_term: u64,     // term of prevLogIndex entry
    entries: []LogEntry,    // log entries to store (empty for heartbeat)
    leader_commit: u64,     // leader's commitIndex
};
```

### AppendEntriesReply

```zig
const AppendEntriesReply = struct {
    term: u64,              // currentTerm, for leader to update itself
    success: bool,          // true if follower contained entry matching prevLog*
    from: NodeId,           // who sent this reply
    match_index: u64,       // if success: highest index replicated. If failure: unused.
    conflict_term: u64,     // if failure: term of the conflicting entry (0 if none)
    conflict_index: u64,    // if failure: first index of conflict_term, or
                            //             last_log_index + 1 if log is too short
};
```

### Message (tagged union)

```zig
const Message = struct {
    type: MessageType,
    payload: union(MessageType) {
        request_vote: RequestVote,
        request_vote_reply: RequestVoteReply,
        append_entries: AppendEntries,
        append_entries_reply: AppendEntriesReply,
    },
};
```

---

## RaftNode

The main struct. This holds all state for one Raft node.

```zig
const RaftNode = struct {
    // -- Identity --
    id: NodeId,
    peers: []NodeId,            // all other nodes in the cluster (fixed for core)

    // -- Persistent state (backed by WAL) --
    current_term: u64,
    voted_for: ?NodeId,
    log: Log,                   // see Log interface below

    // -- Volatile state (all nodes) --
    state: State,               // follower | candidate | leader
    commit_index: u64,          // highest log entry known to be committed
    last_applied: u64,          // highest log entry applied to state machine
    leader_id: ?NodeId,         // current known leader (for client redirection)

    // -- Volatile state (candidate) --
    votes_received: Set(NodeId),

    // -- Volatile state (leader) --
    peer_states: Map(NodeId, PeerState),

    // -- Timing --
    elapsed_ticks: u32,         // ticks since last event (election or heartbeat)
    election_timeout: u32,      // randomized, in ticks
    heartbeat_interval: u32,    // fixed, in ticks (e.g., 1-2)

    // -- Infrastructure --
    wal: Wal,                   // write-ahead log handle
    running: bool,

    // -- Allocator --
    allocator: Allocator,
};
```

---

## Log

In-memory representation of the replicated log, backed by WAL on disk.

```zig
const Log = struct {
    entries: ArrayList(LogEntry),

    // Returns the index of the last entry, or 0 if empty.
    fn lastIndex(self) -> u64

    // Returns the term of the last entry, or 0 if empty.
    fn lastTerm(self) -> u64

    // Returns the entry at the given index, or null if out of range.
    fn getEntry(self, index: u64) -> ?LogEntry

    // Appends a single entry. Caller must ensure index == lastIndex() + 1.
    fn append(self, entry: LogEntry) -> void

    // Appends multiple entries.
    fn appendSlice(self, entries: []LogEntry) -> void

    // Removes all entries from `fromIndex` onward (inclusive).
    // Used when a conflict is detected with the leader's log.
    fn truncateFrom(self, fromIndex: u64) -> void

    // Returns entries from startIndex up to maxCount.
    fn entriesFrom(self, startIndex: u64, maxCount: u32) -> []LogEntry

    // Returns the first index in the log with the given term.
    // Used for the conflict backtracking optimization.
    fn firstIndexOfTerm(self, term: u64) -> u64

    // Returns the last index in the log with the given term.
    fn lastIndexOfTerm(self, term: u64) -> u64

    // Rebuilds the in-memory log from WAL entries (used during recovery).
    fn rebuild(self, entries: []LogEntry) -> void
};
```

---

## WAL (Write-Ahead Log)

Interface for durable storage. This is where the integration with your
existing storage layer goes.

```zig
const Wal = struct {
    // Writes a hard state record (currentTerm, votedFor).
    fn writeHardState(self, term: u64, voted_for: ?NodeId) -> !void

    // Appends log entries to the WAL.
    fn appendEntries(self, entries: []LogEntry) -> !void

    // Truncates the WAL from the given index onward.
    fn truncateFrom(self, fromIndex: u64) -> !void

    // Ensures all buffered data is flushed to stable storage.
    fn sync(self) -> !void

    // Replays the entire WAL from the beginning.
    // Returns (hard_state, log_entries).
    fn replay(self) -> !(HardState, []LogEntry)
};
```

The `!void` return types indicate these can fail (Zig error unions). Disk
failures are surfaced as errors. The caller decides whether to retry or panic.

---

## RaftNode Functions

### Lifecycle

```zig
fn init(allocator, id, peers, wal) -> !RaftNode
fn deinit(self) -> void
fn recover(self) -> !void     // replay WAL, reconstruct state
fn run(self) -> !void         // main event loop (blocking)
```

### Core Protocol

```zig
fn tick(self) -> void                           // advance logical clock
fn handleMessage(self, msg: Message) -> void    // process incoming RPC

fn startElection(self) -> void
fn becomeLeader(self) -> void

fn handleRequestVote(self, rv: RequestVote) -> void
fn handleRequestVoteReply(self, rvr: RequestVoteReply) -> void
fn handleAppendEntries(self, ae: AppendEntries) -> void
fn handleAppendEntriesReply(self, aer: AppendEntriesReply) -> void

fn advanceCommitIndex(self) -> void
fn applyCommitted(self) -> void
```

### Replication (leader only)

```zig
fn sendAppendEntries(self, peer: NodeId) -> void
fn broadcastHeartbeat(self) -> void
```

### Client API

```zig
fn propose(self, data: []const u8) -> !u64   // returns entry index
fn isLeader(self) -> bool
fn getLeaderId(self) -> ?NodeId
fn clusterSize(self) -> u32
```

### Persistence

```zig
fn persistHardState(self) -> !void
fn persistLog(self) -> !void
```

### Helpers

```zig
fn isLogUpToDate(self, candidate_term: u64, candidate_index: u64) -> bool
fn send(self, to: NodeId, msg: Message) -> void
fn quorumSize(self) -> u32   // (clusterSize / 2) + 1
```

---

## Error Types

```zig
const RaftError = error {
    NotLeader,          // client proposal sent to non-leader
    WalWriteFailed,     // disk write failure
    WalSyncFailed,      // fsync failure
    WalCorrupted,       // WAL integrity check failed on replay
    InvalidMessage,     // malformed or unexpected message
    ConnectionFailed,   // network send failure (transient)
};
```

---

## Constants / Configuration

```zig
const Config = struct {
    heartbeat_interval: u32 = 2,        // ticks (e.g., 2 * 100ms = 200ms)
    election_timeout_min: u32 = 10,     // ticks (e.g., 1000ms)
    election_timeout_max: u32 = 20,     // ticks (e.g., 2000ms)
    tick_interval_ms: u32 = 100,        // how often tick() is called
    max_entries_per_msg: u32 = 64,      // max log entries in one AppendEntries
    max_msg_size: u32 = 1_048_576,      // 1 MiB max serialized message
};
```

Timing relationship (must hold):
```
tick_interval_ms * heartbeat_interval  <<  tick_interval_ms * election_timeout_min
```
i.e., multiple heartbeats fit within one election timeout.
