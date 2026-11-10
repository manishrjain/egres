// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! engine — the glue binding `zdb` (storage) and `raft` (consensus) into one
//! replicated storage unit, behind a single API.
//!
//! Alone, neither half is enough: `zdb` is a local LSM forest with no
//! replication; `raft` is a replicated log that doesn't know what its entries
//! mean. The engine wires them together and enforces the contract that fuses
//! them into one system:
//!
//!   * All writes go through raft. A mutation is proposed
//!     (`proposeAndWaitForApply`), replicated, committed, then *applied* — and
//!     apply (`StorageImpl`, the raft state machine) is the ONLY path that
//!     writes the forest. So every replica's storage converges to the same
//!     state; nothing writes the forest directly.
//!   * Reads serve local applied state straight from the forest (`get`/`scan`)
//!     — no raft round-trip.
//!   * LSM housekeeping is replicated too: the flush/compaction/snapshot/split
//!     loops propose manifest changes so every node installs the same SSTs, and
//!     snapshots prune the raft WAL.
//!
//! `Engine.open` opens one node's parts (raft WAL, raft node, forest) and
//! wires them (state machine ↔ node, file-id allocator ↔ node+WAL, raft
//! runtime); `start` recovers + runs the loops, `stop` ends them; the
//! data/control API (`proposeAndWaitForApply`, `get`, `scan`,
//! `createNamespace`) lets callers treat replicated storage as one unit.
//!
//! It also serves the admin protocol (a line protocol: set/get/scan +
//! locks/workers/vfs/raft/zdb diagnostics) and exposes `spawnListener` for
//! attaching front-ends. The `egres` binary wraps this module and adds a
//! SQL/pgwire front-end; the engine itself never imports `sql`/`pgwire` —
//! dependencies point down only (front-ends → engine → zdb/raft).

const _engine = @import("engine.zig");

/// The admin protocol + its front-end server (wire format + the listener).
pub const admin = @import("admin.zig");
pub const governor = @import("governor.zig");

/// The replicated storage engine (forest + raft node + apply + loops).
pub const Engine = _engine.Engine;
pub const StorageImpl = _engine.StorageImpl;
pub const SnapshotCfg = _engine.SnapshotCfg;
pub const InFlight = _engine.InFlight;
pub const ApplyObserver = _engine.ApplyObserver;

/// The admin protocol front-end: a line-protocol listener over an Engine.
pub const AdminServer = admin.AdminServer;
