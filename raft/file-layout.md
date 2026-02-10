# Raft File Layout

## Directory Structure

```
raft/
├── build.zig               # Build configuration
│
├── raft.zig                # Public entry point. Re-exports RaftNode, Config,
│                           # Message types, and errors. This is what the
│                           # rest of egres imports: @import("raft/raft.zig")
│
├── node.zig                # RaftNode struct and all core protocol logic:
│                           #   - init, deinit, recover
│                           #   - tick, handleMessage
│                           #   - startElection, becomeLeader
│                           #   - handleRequestVote, handleRequestVoteReply
│                           #   - handleAppendEntries, handleAppendEntriesReply
│                           #   - advanceCommitIndex, applyCommitted
│                           #   - propose
│                           # This is the big file. ~600-800 lines.
│
├── log.zig                 # Log struct: in-memory log with index/term
│                           # operations. append, truncateFrom, getEntry,
│                           # entriesFrom, firstIndexOfTerm, lastIndexOfTerm.
│                           # ~150-200 lines.
│
├── message.zig             # All message types: RequestVote, AppendEntries,
│                           # their replies, the Message tagged union.
│                           # Serialization and deserialization.
│                           # ~200-250 lines.
│
├── wal.zig                 # Write-ahead log: writeHardState, appendEntries,
│                           # truncateFrom, sync, replay.
│                           # Integration point with egres storage.
│                           # ~200-300 lines.
│
├── config.zig              # Config struct, defaults, validation.
│                           # ~30-50 lines.
│
└── testing/                # Test infrastructure (only compiled in test builds)
    ├── cluster.zig         # TestCluster: N in-process nodes with simulated
    │                       # network and WAL. tick/deliver/partition/crash/restart.
    │                       # ~200-300 lines.
    │
    ├── network.zig         # SimulatedNetwork: message queues, partitions,
    │                       # drop rate. ~100-150 lines.
    │
    ├── mem_wal.zig         # In-memory WAL for testing. Same interface as wal.zig
    │                       # but backed by ArrayList. Can inject failures.
    │                       # ~100-150 lines.
    │
    └── invariants.zig      # Invariant checkers: election safety, log matching,
                            # leader append-only, state machine safety,
                            # leader completeness. ~150-200 lines.
```

## Module Dependency Graph

```
raft.zig (public API)
  └── node.zig (core logic)
        ├── log.zig (in-memory log)
        ├── message.zig (types + serialization)
        ├── wal.zig (persistence)
        └── config.zig (configuration)

testing/cluster.zig (test harness)
  ├── node.zig
  ├── testing/network.zig
  ├── testing/mem_wal.zig
  └── testing/invariants.zig
```

## File Responsibilities

### raft.zig

The public entry point. Minimal code -- just re-exports:

```zig
pub const RaftNode = @import("node.zig").RaftNode;
pub const Config = @import("config.zig").Config;
pub const Message = @import("message.zig").Message;
pub const LogEntry = @import("message.zig").LogEntry;
pub const RaftError = @import("node.zig").RaftError;
```

### node.zig

The heart of the implementation. Contains the `RaftNode` struct and all
protocol logic. This file should be self-contained in terms of Raft
correctness -- someone should be able to read this one file and understand
the complete protocol.

All functions from pseudocode.md live here.

### log.zig

The in-memory log is a separate module because it has enough operations to
warrant its own file, and it can be tested independently.

Key design choice: the Log does NOT know about persistence. It's a pure
in-memory data structure. `node.zig` coordinates between Log and WAL.

### message.zig

Defines all message types and handles serialization. Serialization format:

```
[1 byte: message type tag]
[fixed-size header fields, packed]
[variable-length entries, if AppendEntries]
```

Each entry in AppendEntries:
```
[8 bytes: index][8 bytes: term][4 bytes: data length][data bytes]
```

### wal.zig

This is where you plug in your storage layer. The interface is simple
(see interfaces.md). The implementation depends on what egres uses for
durable storage.

### config.zig

Small file with compile-time and runtime configuration. Separated so it
can be imported without pulling in all of node.zig.

### testing/

All test infrastructure. Uses Zig's built-in test framework (`test "..." {}`).
The testing/ directory is only compiled when running `zig build test`.

The key architectural decision: tests use the **same** `RaftNode` code as
production, but with swapped WAL and network implementations. This is achieved
by making `node.zig` accept the WAL as a parameter (dependency injection
through struct fields, not interfaces/vtables).

---

## Build Configuration (build.zig)

```
raft module:
  - source: raft.zig
  - dependencies: none external (pure Zig + std)

tests:
  - source: testing/cluster.zig (imports everything)
  - run: zig build test
```

## Zig Conventions Followed

- One struct per file (primary struct matches filename)
- `pub` only for things used outside the module
- Error sets defined close to where they're used
- `std.mem.Allocator` passed explicitly
- No global state
- `comptime` for configuration that can be resolved at compile time
- Tests in the same file for unit tests, testing/ for integration tests
