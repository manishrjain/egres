# Raft Paper → Implementation Mapping

Reference: "In Search of an Understandable Consensus Algorithm"
by Diego Ongaro and John Ousterhout (2014).

This document maps each relevant section and figure of the Raft paper to the
specific module, function, or struct in this implementation.

---

## Figure 2: State (Condensed Summary of Raft)

This is the most important figure. Every field and RPC from Figure 2 has a
direct counterpart.

### Persistent State on All Servers

| Paper Field | Implementation | File |
|-------------|---------------|------|
| `currentTerm` | `RaftNode.current_term` | node.zig |
| `votedFor` | `RaftNode.voted_for: ?NodeId` | node.zig |
| `log[]` | `RaftNode.log: Log` (backed by `Wal`) | node.zig, log.zig, wal.zig |

### Volatile State on All Servers

| Paper Field | Implementation | File |
|-------------|---------------|------|
| `commitIndex` | `RaftNode.commit_index` | node.zig |
| `lastApplied` | `RaftNode.last_applied` | node.zig |

### Volatile State on Leaders

| Paper Field | Implementation | File |
|-------------|---------------|------|
| `nextIndex[]` | `PeerState.next_index` in `RaftNode.peer_states` | node.zig |
| `matchIndex[]` | `PeerState.match_index` in `RaftNode.peer_states` | node.zig |

### AppendEntries RPC

| Paper | Implementation | File |
|-------|---------------|------|
| Arguments | `AppendEntries` struct | message.zig |
| Results | `AppendEntriesReply` struct | message.zig |
| Receiver impl | `RaftNode.handleAppendEntries()` | node.zig |

### RequestVote RPC

| Paper | Implementation | File |
|-------|---------------|------|
| Arguments | `RequestVote` struct | message.zig |
| Results | `RequestVoteReply` struct | message.zig |
| Receiver impl | `RaftNode.handleRequestVote()` | node.zig |

### Rules for Servers

| Paper Rule | Implementation | File |
|-----------|---------------|------|
| All Servers: apply committed entries | `RaftNode.applyCommitted()` | node.zig |
| All Servers: step down on higher term | Top of `RaftNode.handleMessage()` | node.zig |
| Followers: reset timer on AppendEntries | `handleAppendEntries()` sets `elapsed_ticks = 0` | node.zig |
| Followers: redirect to leader | `RaftNode.leader_id` + `propose()` returns NotLeader | node.zig |
| Candidates: start election on timeout | `tick()` → `startElection()` | node.zig |
| Candidates: become leader on majority | `handleRequestVoteReply()` → `becomeLeader()` | node.zig |
| Candidates: step down on valid AppendEntries | `handleAppendEntries()` sets `state = .follower` | node.zig |
| Leaders: send heartbeats | `tick()` → `broadcastHeartbeat()` | node.zig |
| Leaders: respond to client proposals | `propose()` | node.zig |
| Leaders: replicate to followers | `sendAppendEntries()` | node.zig |
| Leaders: advance commitIndex | `advanceCommitIndex()` | node.zig |

---

## Section 5.1: Raft Basics

| Concept | Implementation |
|---------|---------------|
| Terms | `current_term` field, incremented on election start |
| Three states | `State` enum: `follower`, `candidate`, `leader` |
| State transitions | See state machine diagram in architecture.md |
| RPC types | `MessageType` enum, `Message` tagged union |

---

## Section 5.2: Leader Election

| Concept | Implementation | Location |
|---------|---------------|----------|
| Election timeout (randomized) | `election_timeout = randomInRange(min, max)` | `startElection()` in node.zig |
| Candidate increments term | `current_term += 1` | `startElection()` in node.zig |
| Candidate votes for self | `voted_for = self.id` | `startElection()` in node.zig |
| RequestVote sent to all peers | Loop over `self.peers` | `startElection()` in node.zig |
| Vote granted if: not yet voted + log is up-to-date | Two conditions checked | `handleRequestVote()` in node.zig |
| Candidate wins with majority | `votes_received.count() > clusterSize() / 2` | `handleRequestVoteReply()` in node.zig |
| Split vote → new election | Election timeout fires while candidate | `tick()` in node.zig |
| Randomized timeout prevents split votes | Random range for `election_timeout` | `startElection()` in node.zig |

---

## Section 5.3: Log Replication

| Concept | Implementation | Location |
|---------|---------------|----------|
| Leader appends entry to local log | `log.append(entry)` | `propose()` in node.zig |
| Leader sends AppendEntries | `sendAppendEntries()` | node.zig |
| Log Matching Property | Checked via `prevLogIndex`/`prevLogTerm` | `handleAppendEntries()` in node.zig |
| Follower rejects if prevLog doesn't match | Return `success = false` | `handleAppendEntries()` in node.zig |
| Leader decrements nextIndex on rejection | `nextIndex[peer] = ...` | `handleAppendEntriesReply()` in node.zig |
| Follower deletes conflicting entries | `log.truncateFrom()` | `handleAppendEntries()` in node.zig |
| Committed = replicated on majority | `advanceCommitIndex()` | node.zig |
| Leader never overwrites its own log | Entries only appended, never modified | `propose()`, invariant check in testing |

### Backtracking Optimization (end of 5.3)

| Paper | Implementation | Location |
|-------|---------------|----------|
| "the protocol can be optimized to reduce the number of rejected AppendEntries RPCs" | `conflict_term` + `conflict_index` in reply, skip-by-term logic | `handleAppendEntries()` and `handleAppendEntriesReply()` in node.zig |

---

## Section 5.4: Safety

### Section 5.4.1: Election Restriction

| Concept | Implementation | Location |
|---------|---------------|----------|
| "voter denies vote if its own log is more up-to-date" | `isLogUpToDate()` | node.zig |
| Comparison: last term wins, then last index | `candidateTerm > myLastTerm \|\| (== && candidateIndex >= myLastIndex)` | `isLogUpToDate()` in node.zig |

### Section 5.4.2: Committing from Previous Terms

| Concept | Implementation | Location |
|---------|---------------|----------|
| "a leader cannot determine if an entry from a previous term is committed" | `advanceCommitIndex()` checks `log[N].term == currentTerm` | node.zig |
| No-op on new leader | `becomeLeader()` calls `propose(NO_OP)` | node.zig |

### Section 5.4.3: Safety Argument

The safety argument from the paper is not implemented in code -- it's the
**proof** that the protocol is correct. However, the invariants derived
from this argument are checked in tests:

| Safety Property | Test Invariant | Location |
|----------------|----------------|----------|
| Election Safety | `checkElectionSafety()` | testing/invariants.zig |
| Leader Append-Only | `checkLeaderAppendOnly()` | testing/invariants.zig |
| Log Matching | `checkLogMatching()` | testing/invariants.zig |
| Leader Completeness | `checkLeaderCompleteness()` | testing/invariants.zig |
| State Machine Safety | `checkStateMachineSafety()` | testing/invariants.zig |

---

## Figure 8: Committing from Previous Terms

This figure is the motivation for the `log[N].term == currentTerm` rule.

| Paper | Implementation | Location |
|-------|---------------|----------|
| The scenario | Reproduced as test S3 ("Figure 8 scenario") | testing/cluster.zig |
| The solution | `advanceCommitIndex()` only commits current-term entries | node.zig |
| The no-op trick | `becomeLeader()` immediately proposes a no-op | node.zig |
| Detailed walkthrough | See architecture.md "Commit Safety Illustration" | architecture.md |

---

## Sections NOT Covered (Out of Scope for Core)

| Section | Topic | Status |
|---------|-------|--------|
| 5.5 | Follower and candidate crashes | Handled implicitly: RPCs are idempotent, retried |
| 5.6 | Timing and availability | Addressed via Config (heartbeat << election timeout) |
| 6 | Cluster membership changes | Future work |
| 7 | Log compaction / snapshots | Future work |
| 8 | Client interaction | Partially covered by `propose()`, full linearizability is future work |

---

## Quick Reference: Where to Find Each Function

| Function | Paper Reference | File:Function |
|----------|----------------|---------------|
| Start election | Fig 2, Candidates | node.zig:`startElection()` |
| Handle RequestVote | Fig 2, RequestVote RPC | node.zig:`handleRequestVote()` |
| Handle AppendEntries | Fig 2, AppendEntries RPC | node.zig:`handleAppendEntries()` |
| Advance commit | Fig 2, Leaders rule 4 | node.zig:`advanceCommitIndex()` |
| Apply entries | Fig 2, All Servers rule 1 | node.zig:`applyCommitted()` |
| Log up-to-date check | §5.4.1 | node.zig:`isLogUpToDate()` |
| Commit safety check | §5.4.2, Fig 8 | node.zig:`advanceCommitIndex()` |
| Conflict backtracking | §5.3 optimization | node.zig:`handleAppendEntriesReply()` |
| No-op on election | §5.4.2 | node.zig:`becomeLeader()` |
