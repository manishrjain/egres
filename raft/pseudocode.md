# Raft Pseudocode -- Core

All pseudocode uses a Zig-ish style but is meant to be readable, not
compilable. `self` refers to the RaftNode instance.

---

## Main Loop

```
fn run(self):
    while self.running:
        timeout = self.nextTickDeadline - now()
        events = poll(self.fds, timeout)

        for event in events:
            if event.fd is a peer connection:
                msg = self.recvMessage(event.fd)
                self.handleMessage(msg)

        if now() >= self.nextTickDeadline:
            self.tick()

        self.applyCommitted()
```

---

## tick()

Called periodically (e.g., every 100ms). Drives election timeouts and
heartbeats based on elapsed ticks, not wall-clock time.

```
fn tick(self):
    self.elapsedTicks += 1

    switch self.state:
        .follower, .candidate =>
            if self.elapsedTicks >= self.electionTimeout:
                self.startElection()

        .leader =>
            if self.elapsedTicks >= self.heartbeatInterval:
                self.broadcastHeartbeat()
                self.elapsedTicks = 0
```

---

## startElection()

```
fn startElection(self):
    self.currentTerm += 1
    self.state = .candidate
    self.votedFor = self.id
    self.votesReceived = { self.id }     // vote for self
    self.elapsedTicks = 0
    self.electionTimeout = randomInRange(10, 20)  // in ticks

    self.persistHardState()

    lastIndex = self.log.lastIndex()
    lastTerm = self.log.lastTerm()

    for peer in self.peers:
        self.send(peer, RequestVote {
            .term = self.currentTerm,
            .candidateId = self.id,
            .lastLogIndex = lastIndex,
            .lastLogTerm = lastTerm,
        })
```

---

## handleMessage()

Top-level dispatcher. Enforces the universal term rule first.

```
fn handleMessage(self, msg):
    // Universal rule: step down if we see a higher term
    if msg.term > self.currentTerm:
        self.currentTerm = msg.term
        self.state = .follower
        self.votedFor = null
        self.leaderId = null
        self.persistHardState()

    // Reject stale messages (except: RequestVoteReply and
    // AppendEntriesReply which we just handled above)
    if msg.term < self.currentTerm:
        // For AppendEntries: reply false so sender updates its term
        // For RequestVote: reply false
        // For replies: ignore
        self.rejectStaleMessage(msg)
        return

    switch msg.type:
        .RequestVote        => self.handleRequestVote(msg)
        .RequestVoteReply   => self.handleRequestVoteReply(msg)
        .AppendEntries      => self.handleAppendEntries(msg)
        .AppendEntriesReply => self.handleAppendEntriesReply(msg)
```

---

## handleRequestVote()

```
fn handleRequestVote(self, msg):
    grant = false

    // Can we vote for this candidate?
    if self.votedFor == null or self.votedFor == msg.candidateId:
        // Is the candidate's log at least as up-to-date as ours?
        if self.isLogUpToDate(msg.lastLogTerm, msg.lastLogIndex):
            grant = true
            self.votedFor = msg.candidateId
            self.elapsedTicks = 0           // reset election timer
            self.persistHardState()

    self.send(msg.candidateId, RequestVoteReply {
        .term = self.currentTerm,
        .voteGranted = grant,
        .from = self.id,
    })

fn isLogUpToDate(self, candidateTerm, candidateIndex):
    myLastTerm = self.log.lastTerm()
    myLastIndex = self.log.lastIndex()

    if candidateTerm != myLastTerm:
        return candidateTerm > myLastTerm
    return candidateIndex >= myLastIndex
```

---

## handleRequestVoteReply()

```
fn handleRequestVoteReply(self, msg):
    if self.state != .candidate:
        return      // stale reply, we already moved on

    if msg.term != self.currentTerm:
        return      // reply from an old election

    if msg.voteGranted:
        self.votesReceived.insert(msg.from)

        if self.votesReceived.count() > self.clusterSize() / 2:
            self.becomeLeader()
```

---

## becomeLeader()

```
fn becomeLeader(self):
    self.state = .leader
    self.leaderId = self.id
    self.elapsedTicks = 0

    // Initialize replication state for each peer
    for peer in self.peers:
        self.nextIndex[peer] = self.log.lastIndex() + 1
        self.matchIndex[peer] = 0

    // Send initial empty AppendEntries (heartbeat) to assert leadership
    // and prevent new elections
    self.broadcastHeartbeat()

    // Append a no-op entry for the new term.
    // This is necessary to commit entries from previous terms.
    // Without this, a leader can never advance commitIndex for
    // entries it didn't create (see architecture.md, commit safety).
    self.propose(NO_OP)
```

**Why the no-op?** A leader cannot commit entries from prior terms directly
(the `log[N].term == currentTerm` requirement). By appending a no-op in its
own term and committing it, all prior entries get committed indirectly. Without
this, a leader that receives no new client proposals would never advance
commitIndex.

---

## handleAppendEntries()

```
fn handleAppendEntries(self, msg):
    // At this point, msg.term >= self.currentTerm (checked in handleMessage)

    self.state = .follower
    self.leaderId = msg.leaderId
    self.elapsedTicks = 0           // heard from leader, reset timer

    // Check log consistency
    if msg.prevLogIndex > 0:
        prevEntry = self.log.getEntry(msg.prevLogIndex)

        if prevEntry == null or prevEntry.term != msg.prevLogTerm:
            // Log doesn't match at prevLogIndex
            // Help the leader find the right point faster by sending
            // a hint about where our log diverges.

            hintIndex = 0
            hintTerm = 0

            if prevEntry != null:
                // We have an entry but wrong term. Send back the
                // first index of the conflicting term so leader can
                // skip the entire term.
                hintTerm = prevEntry.term
                hintIndex = self.log.firstIndexOfTerm(prevEntry.term)
            else:
                // We don't have this index at all.
                hintIndex = self.log.lastIndex() + 1

            self.send(msg.leaderId, AppendEntriesReply {
                .term = self.currentTerm,
                .success = false,
                .from = self.id,
                .conflictTerm = hintTerm,
                .conflictIndex = hintIndex,
            })
            return

    // Process entries
    if msg.entries.len > 0:
        // Find the point where our log diverges from the leader's
        insertFrom = 0
        for i, entry in msg.entries:
            existing = self.log.getEntry(entry.index)
            if existing == null or existing.term != entry.term:
                insertFrom = i
                break
            insertFrom = i + 1  // all matched so far

        if insertFrom < msg.entries.len:
            // Truncate conflicting entries and append new ones
            self.log.truncateFrom(msg.entries[insertFrom].index)
            self.log.appendEntries(msg.entries[insertFrom..])
            self.persistLog()

    // Update commit index
    if msg.leaderCommit > self.commitIndex:
        lastNewIndex = if msg.entries.len > 0
                         msg.entries[msg.entries.len - 1].index
                       else
                         msg.prevLogIndex
        self.commitIndex = min(msg.leaderCommit, lastNewIndex)

    self.send(msg.leaderId, AppendEntriesReply {
        .term = self.currentTerm,
        .success = true,
        .from = self.id,
        .matchIndex = self.log.lastIndex(),
    })
```

---

## handleAppendEntriesReply()

```
fn handleAppendEntriesReply(self, msg):
    if self.state != .leader:
        return

    peer = msg.from

    if msg.success:
        // Update peer's replication progress
        self.nextIndex[peer] = msg.matchIndex + 1
        self.matchIndex[peer] = msg.matchIndex

        // Try to advance commit index
        self.advanceCommitIndex()

    else:
        // Log inconsistency. Use the conflict hint to skip back faster.
        if msg.conflictTerm > 0:
            // Look for the last entry of conflictTerm in our log
            lastOfTerm = self.log.lastIndexOfTerm(msg.conflictTerm)
            if lastOfTerm > 0:
                // We have entries in this term. Send from after them.
                self.nextIndex[peer] = lastOfTerm + 1
            else:
                // We don't have this term at all. Send from where
                // the follower says the conflict starts.
                self.nextIndex[peer] = msg.conflictIndex
        else:
            // Follower's log is shorter than prevLogIndex.
            self.nextIndex[peer] = msg.conflictIndex

        // Clamp to at least 1
        self.nextIndex[peer] = max(self.nextIndex[peer], 1)

        // Retry with updated nextIndex
        self.sendAppendEntries(peer)
```

---

## advanceCommitIndex()

This is where the critical safety check lives.

```
fn advanceCommitIndex(self):
    // We need to find the highest N such that:
    //   1. log[N].term == currentTerm  (SAFETY: can only commit own term's entries)
    //   2. A majority of matchIndex[i] >= N
    //
    // We include ourselves in the majority count (our matchIndex is
    // implicitly our last log index).

    // Collect all matchIndex values (including self)
    matches = []
    matches.append(self.log.lastIndex())    // self
    for peer in self.peers:
        matches.append(self.matchIndex[peer])

    // Sort descending
    sort(matches, descending)

    // The value at index (clusterSize / 2) is the highest index
    // replicated on a majority.
    majorityMatch = matches[self.clusterSize() / 2]

    // Only commit if it's from our term
    if majorityMatch > self.commitIndex:
        entry = self.log.getEntry(majorityMatch)
        if entry.term == self.currentTerm:
            self.commitIndex = majorityMatch
```

---

## sendAppendEntries() (to a specific peer)

```
fn sendAppendEntries(self, peer):
    prevLogIndex = self.nextIndex[peer] - 1
    prevLogTerm = 0
    if prevLogIndex > 0:
        prevLogTerm = self.log.getEntry(prevLogIndex).term

    // Gather entries from nextIndex[peer] onward
    // Cap at some max batch size to avoid huge messages
    entries = self.log.entriesFrom(self.nextIndex[peer], maxBatchSize)

    self.send(peer, AppendEntries {
        .term = self.currentTerm,
        .leaderId = self.id,
        .prevLogIndex = prevLogIndex,
        .prevLogTerm = prevLogTerm,
        .entries = entries,
        .leaderCommit = self.commitIndex,
    })
```

---

## broadcastHeartbeat()

```
fn broadcastHeartbeat(self):
    for peer in self.peers:
        self.sendAppendEntries(peer)
```

Note: heartbeats are just AppendEntries with `entries = []`. They still
carry `leaderCommit` so followers can advance their commit index, and they
still carry `prevLogIndex/prevLogTerm` for consistency checking.

---

## propose() -- Client Proposals

```
fn propose(self, data):
    if self.state != .leader:
        return error.NotLeader

    entry = LogEntry {
        .index = self.log.lastIndex() + 1,
        .term = self.currentTerm,
        .data = data,
    }

    self.log.append(entry)
    self.persistLog()

    // Immediately try to replicate to all peers
    for peer in self.peers:
        self.sendAppendEntries(peer)

    // The entry will be committed once a majority acknowledges it.
    // Return the entry index so the caller can track it.
    return entry.index
```

---

## applyCommitted()

```
fn applyCommitted(self):
    while self.lastApplied < self.commitIndex:
        self.lastApplied += 1
        entry = self.log.getEntry(self.lastApplied)

        if entry.data != NO_OP:
            self.applyToStateMachine(entry)
```

`applyToStateMachine` is the integration point with egres. This is where
committed Raft entries become database mutations.

---

## Persistence Helpers

```
fn persistHardState(self):
    // Write currentTerm and votedFor to WAL as a metadata record.
    // Must fsync before returning.
    self.wal.writeHardState(self.currentTerm, self.votedFor)
    self.wal.sync()

fn persistLog(self):
    // Append new log entries to WAL.
    // Must fsync before sending replies.
    self.wal.sync()
```

---

## Recovery After Restart

```
fn recover(self):
    // Replay WAL to reconstruct state
    (term, votedFor, logEntries) = self.wal.replay()

    self.currentTerm = term
    self.votedFor = votedFor
    self.log.rebuild(logEntries)

    // Volatile state starts fresh
    self.commitIndex = 0
    self.lastApplied = 0
    self.state = .follower
    self.leaderId = null
    self.elapsedTicks = 0
    self.electionTimeout = randomInRange(10, 20)

    // Note: commitIndex = 0 means we might re-apply some entries.
    // The state machine must handle idempotent application, OR
    // we persist lastApplied (but that's an optimization, not required
    // for correctness if the state machine is idempotent).
```

---

## Conflict Backtracking Optimization

The basic Raft paper suggests decrementing `nextIndex` by 1 on each
AppendEntries failure. This is O(n) in the number of conflicting entries.
The pseudocode above uses the term-based optimization from Section 5.3:

```
Follower rejects AppendEntries:
  → sends back (conflictTerm, conflictIndex)

Leader receives rejection:
  → if leader has entries in conflictTerm:
       nextIndex = leader's last entry of conflictTerm + 1
    else:
       nextIndex = conflictIndex from follower

This skips entire terms at a time, making backtracking O(terms) instead
of O(entries).
```

In practice, conflicts are rare and short, so this optimization matters
most during recovery after a long partition.
