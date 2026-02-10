# Raft Test Strategy

## Principles

1. **Deterministic testing.** No real timers, no real network, no real disk.
   Everything is simulated so tests are reproducible.
2. **Test invariants, not implementations.** Assert the safety properties
   from the paper, not the internal structure of the code.
3. **Adversarial.** Actively try to break things: partitions, reorders, drops,
   duplicates, restarts.

---

## Test Infrastructure

### Simulated Cluster

Build a `TestCluster` that holds N `RaftNode` instances in-process. Instead
of real TCP and real WAL, use:

- **In-memory network**: Messages go into per-node queues. The test controls
  delivery: it can delay, drop, reorder, duplicate, or partition.
- **In-memory WAL**: A simple list-based WAL. Can inject write failures. Can
  be "wiped" to simulate a crash (lose everything not explicitly persisted).
- **Manual tick control**: The test calls `tick()` on each node explicitly.
  No real timers. The test decides when time passes.

```
TestCluster:
    nodes: [N]RaftNode (all using in-memory network + WAL)
    network: SimulatedNetwork

    fn tickAll()             // tick every node once
    fn tickNode(id)          // tick a specific node
    fn deliverAll()          // deliver all pending messages
    fn deliverTo(id)         // deliver only messages for one node
    fn partition(a, b)       // drop all messages between sets a and b
    fn heal()                // remove all partitions
    fn crash(id)             // stop a node, wipe volatile state
    fn restart(id)           // recover a crashed node from WAL
    fn propose(id, data)     // submit a proposal to a specific node
    fn waitForCommit(index)  // tick + deliver until index is committed on majority
```

### SimulatedNetwork

```
SimulatedNetwork:
    inboxes: Map(NodeId, Queue(Message))
    partitions: Set((NodeId, NodeId))   // bidirectional
    drop_rate: f64                       // 0.0 to 1.0

    fn send(from, to, msg):
        if (from, to) in partitions: drop
        if random() < drop_rate: drop
        inboxes[to].push(msg)

    fn deliver(to) -> []Message:
        return inboxes[to].drainAll()
```

---

## Invariant Checks

Run these after every test step (every tick + deliver cycle). If any
invariant is violated, the test fails immediately with a detailed state dump.

### 1. Election Safety (Section 5.2)

> At most one leader per term.

```
fn checkElectionSafety(cluster):
    leaders_by_term = Map(u64, Set(NodeId))
    for node in cluster.nodes:
        if node.state == .leader:
            leaders_by_term[node.current_term].insert(node.id)
    for term, leaders in leaders_by_term:
        assert leaders.count() <= 1,
            "Multiple leaders in term {}: {}", term, leaders
```

### 2. Leader Append-Only (Section 5.3)

> A leader never overwrites or deletes entries in its log.

```
fn checkLeaderAppendOnly(node, prevLog):
    if node.state == .leader:
        // prevLog is a snapshot of the leader's log from last check
        for i in 1..prevLog.len:
            assert node.log[i] == prevLog[i],
                "Leader modified entry at index {}", i
        assert node.log.len >= prevLog.len,
            "Leader's log shrank"
```

### 3. Log Matching (Section 5.3)

> If two logs contain an entry with the same index and term, then the logs
> are identical in all entries up through that index.

```
fn checkLogMatching(cluster):
    for (a, b) in allPairs(cluster.nodes):
        for i in 1..min(a.log.len, b.log.len):
            if a.log[i].term == b.log[i].term:
                // All entries 1..i must match
                for j in 1..i:
                    assert a.log[j] == b.log[j],
                        "Log matching violated at index {} between {} and {}",
                        j, a.id, b.id
```

### 4. State Machine Safety (Section 5.4.3)

> If a server has applied a log entry at a given index, no other server will
> ever apply a different entry for that index.

```
fn checkStateMachineSafety(cluster):
    applied = Map(u64, (NodeId, LogEntry))  // index → (first applier, entry)
    for node in cluster.nodes:
        for i in 1..node.last_applied:
            entry = node.log[i]
            if i in applied:
                (prevNode, prevEntry) = applied[i]
                assert entry.data == prevEntry.data,
                    "Nodes {} and {} applied different entries at index {}",
                    prevNode, node.id, i
            else:
                applied[i] = (node.id, entry)
```

### 5. Leader Completeness (Section 5.4)

> If a log entry is committed in a given term, that entry will be present in
> the logs of all leaders for all higher-numbered terms.

```
fn checkLeaderCompleteness(cluster, committedEntries):
    // committedEntries: entries known to be committed (applied by any node)
    for node in cluster.nodes:
        if node.state == .leader:
            for (index, entry) in committedEntries:
                if index <= node.log.lastIndex():
                    assert node.log[index].term == entry.term
                        and node.log[index].data == entry.data,
                        "Leader {} missing committed entry at index {}",
                        node.id, index
```

---

## Test Scenarios

### Election Tests

| # | Scenario | Setup | Expected |
|---|----------|-------|----------|
| E1 | Basic election | 3 nodes, let election timeout fire | One leader elected within 2 election timeouts |
| E2 | Leader heartbeat prevents election | 3 nodes, leader sends heartbeats | No new election as long as heartbeats arrive |
| E3 | Leader failure triggers election | 3 nodes, crash leader | New leader elected |
| E4 | Split vote | 5 nodes, 2 candidates start simultaneously | Eventually one wins (may take multiple rounds) |
| E5 | Stale candidate | Candidate with old term tries to get votes | Rejected, learns new term, becomes follower |
| E6 | Candidate with stale log | Candidate's log is behind | Rejected by nodes with longer/newer logs |
| E7 | Two partitions | 5 nodes split into (2, 3) | Group of 3 elects leader, group of 2 cannot |
| E8 | Rejoin after partition | Heal partition from E7 | Old leader in minority steps down |

### Replication Tests

| # | Scenario | Setup | Expected |
|---|----------|-------|----------|
| R1 | Basic replication | Propose to leader, deliver messages | Entry committed on majority, applied |
| R2 | Follower crash and recovery | Propose, crash 1 follower, propose again, restart follower | Follower catches up after restart |
| R3 | Multiple proposals | Propose 100 entries rapidly | All committed and applied in order |
| R4 | Leader crash mid-replication | Leader crashes after sending to 1/2 followers | New leader either has or doesn't have the entry (both are safe since it wasn't committed) |
| R5 | Log conflict resolution | Force a follower to have wrong entries, then replicate | Leader detects conflict, follower truncates and catches up |
| R6 | Replication after partition heal | Partition leader from 2/4, propose to new leader, heal | Old leader catches up with new entries |

### Safety Tests

| # | Scenario | Setup | Expected |
|---|----------|-------|----------|
| S1 | Committed entry survives leader change | Commit entry, crash leader, elect new leader | New leader has the committed entry |
| S2 | Uncommitted entry may be lost | Propose to leader, crash before majority ack | Entry may or may not appear in new leader's log (both ok) |
| S3 | Figure 8 scenario | Reproduce the exact scenario from Figure 8 | No committed entry is ever overwritten |
| S4 | No commit from previous term | Leader has entries from prior terms | commitIndex only advances when a current-term entry is committed |
| S5 | Apply order consistency | Commit entries on all nodes | All nodes apply entries in the same order |

### Chaos / Stress Tests

| # | Scenario | Setup | Expected |
|---|----------|-------|----------|
| C1 | Random partitions | Randomly partition and heal every N ticks | All invariants hold, cluster makes progress when a majority is connected |
| C2 | Message drops | Set 20% drop rate | Cluster still makes progress (slower) |
| C3 | Random restarts | Randomly crash and restart nodes | Cluster recovers, no data loss for committed entries |
| C4 | Everything at once | Drops + partitions + restarts + proposals | All invariants hold |

---

## Figure 8 Test (Detailed)

This is the most important safety test. It validates the commit rule.

```
Setup: 5 nodes (S1-S5)

Step 1: S1 is leader in term 1. Replicate entry (index=2, term=1) to S1, S2 only.
Step 2: Crash S1.
Step 3: S5 is elected leader in term 2 (votes from S3, S4, S5).
         S5 appends entry (index=2, term=2) but doesn't replicate.
Step 4: Crash S5.
Step 5: S1 restarts. S1 is elected leader in term 3 (votes from S1, S2, S3).
         S1 replicates (index=2, term=1) to S3. Now on S1, S2, S3.
Step 6: S1 must NOT commit (index=2, term=1) yet!
         It's on a majority, but term != currentTerm.
Step 7: S1 appends (index=3, term=3) and replicates to majority.
Step 8: Now (index=3, term=3) is committed → (index=2, term=1) is indirectly committed.

Verify: After step 8, no future leader can lack entries at index 2 and 3.
```

---

## Test Utilities

```
fn assertLeaderExists(cluster) -> NodeId
    // Exactly one leader in the cluster, return its id

fn assertNoLeader(cluster)
    // No node thinks it's a leader

fn assertAllLogsMatch(cluster)
    // All committed portions of logs are identical

fn assertEntryCommitted(cluster, index, data)
    // The entry at `index` with `data` is committed on a majority

fn dumpClusterState(cluster)
    // Print: each node's state, term, votedFor, log, commitIndex
    // Invaluable for debugging failing tests
```

---

## Test Execution Strategy

1. **Unit tests first**: Test each function in isolation (isLogUpToDate,
   advanceCommitIndex, handleRequestVote with various inputs).

2. **Integration tests**: Use TestCluster for multi-node scenarios.

3. **Fuzz/chaos tests**: Run C1-C4 for extended periods (thousands of ticks)
   with invariant checking after every step.

4. **Regression tests**: Every bug found becomes a test case with the exact
   sequence of events that triggered it.
