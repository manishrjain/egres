# Raft Architecture -- Core

**Zig version: master branch (tracking v0.16 release)**

## Design Philosophy

This is **not** a standalone library. The Raft module is a tightly integrated
component of egres. It directly owns its storage (WAL) and network (TCP)
layers. There are no abstract `Transport` or `Storage` interfaces -- the Raft
module knows exactly how it persists state and talks to peers.

This is closer to how Dgraph integrated etcd/raft with Badger than to how
etcd/raft itself is designed as a generic library.

## Scope (Core Only)

- Leader election (Section 5.2 of the paper)
- Log replication (Section 5.3)
- Safety guarantees (Section 5.4)

Not covered (yet): membership changes, log compaction/snapshots, read-only
queries, pre-vote protocol, learner nodes.

---

## Component Overview

```
┌─────────────────────────────────────────────────────┐
│                     RaftNode                        │
│                                                     │
│  ┌────────────┐   ┌────────────────────────────┐   │
│  │   State     │   │         Log                │   │
│  │  Machine    │   │  ┌──────┐ ┌─────────────┐ │   │
│  │            │   │  │In-Mem │ │  WAL (disk) │ │   │
│  │ {follower,  │   │  │Cache  │ │             │ │   │
│  │  candidate, │   │  └──────┘ └─────────────┘ │   │
│  │  leader}    │   └────────────────────────────┘   │
│  └────────────┘                                     │
│                                                     │
│  ┌────────────┐   ┌────────────────────────────┐   │
│  │  Election   │   │   Replication State        │   │
│  │  Timer      │   │  (nextIndex, matchIndex    │   │
│  │             │   │   per peer -- leader only) │   │
│  └────────────┘   └────────────────────────────┘   │
│                                                     │
│  ┌──────────────────────────────────────────────┐  │
│  │              Network (TCP)                    │  │
│  │  Persistent connections to each peer.         │  │
│  │  Sends/receives serialized Messages.          │  │
│  └──────────────────────────────────────────────┘  │
│                                                     │
│  ┌──────────────────────────────────────────────┐  │
│  │              Storage (WAL)                    │  │
│  │  Persists: currentTerm, votedFor, log[]       │  │
│  │  Sync-on-write before responding to RPCs.     │  │
│  └──────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────┘
```

## State Machine

Three states, with strictly defined transitions:

```
                    ┌──────────────────────────┐
                    │                          │
          timeout   │    receives higher term   │
         ┌──────┐   │   ┌──────────────────┐   │
         │      v   │   v                  │   │
         │  ┌───────────────┐              │   │
         │  │   Candidate   │──────────────┘   │
         │  └───────────────┘                  │
         │      │       ^                      │
         │      │       │ election timeout,    │
         │      │       │ no winner            │
         │      │       └──────────┐           │
         │      │ wins majority    │           │
         │      v                  │           │
    ┌────────────────┐        ┌────────────────┐
    │     Leader     │───────>│   Follower     │
    └────────────────┘        └────────────────┘
      discovers higher term     starts here
                                (or: receives valid
                                 AppendEntries/
                                 RequestVote with
                                 higher term)
```

### Transition Rules

| From      | To        | Trigger                                        |
|-----------|-----------|-------------------------------------------------|
| Follower  | Candidate | Election timeout expires without hearing from a leader or granting a vote |
| Candidate | Leader    | Receives votes from a majority of the cluster   |
| Candidate | Follower  | Receives AppendEntries from a valid leader (term >= currentTerm) |
| Candidate | Candidate | Election timeout expires (split vote, restart)  |
| Leader    | Follower  | Discovers a message with a higher term           |
| Any       | Follower  | Receives any RPC with term > currentTerm         |

### Universal Rule

**If any RPC request or response contains `term > currentTerm`**: set
`currentTerm = term`, transition to Follower, reset `votedFor = null`. This
rule is checked **before** any other processing on every incoming message.

---

## Data Flow: Leader Election

```
1. Follower's election timer fires
2. Follower increments currentTerm
3. Transitions to Candidate
4. Votes for self (votedFor = self)
5. Persists (currentTerm, votedFor) to WAL
6. Resets election timer (randomized)
7. Sends RequestVote to all peers:
     { term, candidateId, lastLogIndex, lastLogTerm }

8. Each peer receives RequestVote:
   a. If term < currentTerm → reject
   b. If already voted for someone else this term → reject
   c. If candidate's log is not at least as up-to-date → reject
   d. Otherwise → grant vote, persist votedFor, reset election timer

9. Candidate tallies votes:
   a. If majority → become Leader, send empty AppendEntries (heartbeat)
   b. If election timer fires again → increment term, restart election
   c. If receives AppendEntries with term >= currentTerm → become Follower
```

### "At least as up-to-date" check (Section 5.4.1):
```
candidate is at least as up-to-date as voter if:
  candidate.lastLogTerm > voter.lastLogTerm
  OR (candidate.lastLogTerm == voter.lastLogTerm
      AND candidate.lastLogIndex >= voter.lastLogIndex)
```

---

## Data Flow: Log Replication

```
1. Client sends a proposal to the Leader
2. Leader appends entry to its local log (term = currentTerm)
3. Leader persists entry to WAL
4. Leader sends AppendEntries to all peers:
     { term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit }

5. Each peer receives AppendEntries:
   a. If term < currentTerm → reject
   b. If log doesn't contain entry at prevLogIndex with prevLogTerm → reject
   c. If existing entries conflict with new ones → delete from conflict point onward
   d. Append new entries
   e. Persist to WAL
   f. Update commitIndex = min(leaderCommit, index of last new entry)
   g. Reply with { term, success: true, matchIndex }

6. Leader receives AppendEntriesReply:
   a. If success → update matchIndex[peer] and nextIndex[peer]
   b. If failure → decrement nextIndex[peer], retry
      (The log consistency check failed; back up until logs match)

7. Leader advances commitIndex:
   - Find the highest N such that:
     - matchIndex[i] >= N for a majority
     - log[N].term == currentTerm   ← CRITICAL safety requirement
   - Set commitIndex = N

8. All nodes apply committed entries:
   - While lastApplied < commitIndex:
     - lastApplied++
     - Apply log[lastApplied] to the state machine
```

### Why `log[N].term == currentTerm`?

A leader can only commit entries from its **own** term. Entries from previous
terms get committed indirectly once a current-term entry is committed on top of
them. This prevents the scenario in Figure 8 of the paper where a leader could
overwrite a committed entry.

---

## Persistence Model

### What must be persisted (before responding to any RPC):

| Field       | When persisted                                       |
|-------------|------------------------------------------------------|
| currentTerm | Before sending/responding to any RPC                 |
| votedFor    | Before responding to RequestVote (grant)             |
| log[]       | Before responding to AppendEntries (success)         |

These three fields constitute the **HardState**. Everything else (commitIndex,
lastApplied, nextIndex, matchIndex, state) can be reconstructed after a restart.

### WAL Strategy

- All writes go through a write-ahead log.
- Each WAL entry is `(index, term, data)` for log entries, or a special
  metadata record for `(currentTerm, votedFor)`.
- Fsync before acknowledging any RPC. This is the critical durability
  guarantee. Without it, a node could "forget" a vote it granted and vote
  for a different candidate in the same term.
- On restart, replay the WAL to reconstruct in-memory state.

### Batching

Disk writes can be batched: if multiple AppendEntries arrive in quick
succession, batch the WAL writes and fsync once. But never respond to a peer
until the relevant data is durable.

---

## Network Model

### Connection Management

- Maintain a persistent TCP connection to each peer.
- Reconnect with exponential backoff on failure.
- Each connection runs a send loop and a receive loop.

### Message Framing

Simple length-prefixed messages:
```
[4 bytes: message length][message bytes]
```

Serialization can be straightforward: Zig packed structs for fixed-size
headers, followed by variable-length log entry data.

### Heartbeats

- The leader sends empty AppendEntries (no log entries) as heartbeats.
- Heartbeat interval: typically 100-200ms.
- Election timeout: typically 1000-2000ms (randomized per node, re-randomized
  on each election timeout reset).
- Rule of thumb: `broadcastTime << electionTimeout << MTBF`

---

## Concurrency Model

Zig doesn't have goroutines. Two reasonable approaches:

### Option A: Single-threaded event loop (recommended)

```
while running:
    events = poll(network_fds + timer_fd, timeout)
    for event in events:
        if event is network_readable:
            msg = deserialize(read(fd))
            handleMessage(msg)
        if event is timer_expired:
            tick()
    applyCommitted()
```

- All Raft logic runs on a single thread.
- No locks needed for Raft state.
- Network I/O uses epoll (or io_uring).
- Timer is a timerfd or a computed poll timeout.
- Storage writes (WAL fsync) block the event loop, but that's intentional:
  we must wait for durability before responding anyway.

### Option B: Dedicated Raft thread + channel

- Raft runs on its own thread with a message inbox.
- Network threads receive messages and push them into the inbox.
- Raft thread processes inbox, produces outgoing messages + WAL writes.
- More complex, but allows network I/O to proceed while WAL syncs.

**Recommendation**: Start with Option A. It's simpler, easier to reason about
correctness, and the fsync latency is inherent to the protocol anyway.

---

## Commit Safety Illustration

This is the subtle part of Raft. Here's the scenario from Figure 8:

```
Time →
         T1    T2    T3    T4    T5

S1(L)    1     2                       S1 is leader in T2, replicates to S2
S2       1     2
S3       1                             S1 crashes
S4       1
S5       1                 3           S5 elected leader in T3, gets one entry

S1 comes back, elected leader in T4.
S1 continues replicating term-2 entry to S3 (now on majority: S1, S2, S3).

CAN S1 commit the term-2 entry? NO!

If S1 commits it and then crashes, S5 could be elected (its log is as
up-to-date) and would overwrite the committed entry. Violation!

Solution: S1 must first replicate a term-4 entry. Once that term-4 entry
is committed (on a majority), the term-2 entry is indirectly committed.
```

This is why `advanceCommitIndex()` requires `log[N].term == currentTerm`.

---

## Error Handling Strategy

Errors fall into two categories:

1. **Transient / recoverable**: Network failures, temporary disk errors.
   - Retry with backoff. Don't crash.
   - Log the error, continue the event loop.

2. **Fatal / unrecoverable**: WAL corruption, impossible state transitions.
   - Panic. It's better to stop than to violate safety.
   - In Zig: `@panic("raft: invariant violated: ...")`.

The Raft protocol is designed to tolerate node crashes, so crashing on
corruption is the right thing to do. The cluster continues with the remaining
nodes.
