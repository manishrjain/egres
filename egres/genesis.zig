// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The genesis prefix every fresh WAL starts with: the state a node has
//! before its first election, written the same, byte for byte, on every
//! node.

const std = @import("std");
const log = std.log.scoped(.genesis);
const raft = @import("raft");
const zdb = @import("zdb");
const sql = @import("sql");

const manifest = zdb.manifest;
const Forest = zdb.forest.Forest;

/// The default database, present from a node's very first boot. The SQL
/// layer owns the name (sessions with no database fall back to it).
pub const default_database = sql.default_database;

/// The genesis entries, in log order from index 1:
///   1. namespace 1, the auth tables' trees (no name, no tenant);
///   2. tenant `egres` (id 1);
///   3. the default database at the first user namespace id ('egres' is
///      ns 16 on every cluster), owned by tenant 1.
/// The tenant comes before its database because apply skips a user
/// namespace whose tenant does not exist. All allocations go on `alloc`
/// (an arena).
pub fn changes(alloc: std.mem.Allocator) ![3]manifest.ManifestChange {
    return .{
        try Forest.authNsCreateChange(alloc),
        try Forest.tenantCreateChange(
            alloc,
            manifest.default_tenant_id,
            manifest.default_tenant_name,
        ),
        try Forest.nsCreateChange(
            alloc,
            manifest.first_namespace,
            manifest.default_tenant_id,
            default_database,
        ),
    };
}

/// Seed a fresh WAL with the genesis entries. Every node writes the same
/// bytes — fixed ids, term 0, each entry's index as its timestamp — so
/// raft's log matching holds across fresh nodes, and the entries are
/// committed by construction (HardState.commit_index covers them):
/// engine recovery applies them before any election or client traffic.
/// A restarted or joining node with a non-empty WAL skips this.
pub fn seed(allocator: std.mem.Allocator, wal: *raft.DiskWAL) !void {
    if (wal.lastIndex() != 0) return;

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const mcs = try changes(arena.allocator());

    var entries: [mcs.len]raft.buffers.LogEntryBuffer = undefined;
    var made: usize = 0;
    defer for (entries[0..made]) |*e| e.deinit();
    for (mcs, &entries, 1..) |mc, *entry, index| {
        entry.* = try entryAt(allocator, index, mc);
        made += 1;
    }

    try wal.addEntries(&entries);
    try wal.sync();
    try wal.setHardState(.{ .commit_index = mcs.len });
    log.info("seeded genesis: tenant '{s}' (id {d}), database '{s}' -> " ++
        "ns={d}", .{
        manifest.default_tenant_name,
        manifest.default_tenant_id,
        default_database,
        manifest.first_namespace,
    });
}

/// One genesis manifest entry at `index`: term 0, and the index as its
/// fixed timestamp (real HLC values are far larger).
fn entryAt(
    allocator: std.mem.Allocator,
    index: u64,
    mc: manifest.ManifestChange,
) !raft.buffers.LogEntryBuffer {
    const payload = try zdb.buffers.marshalRoot(allocator, mc);
    defer allocator.free(payload);

    var buf = std.ArrayList(u8).empty;
    defer buf.deinit(allocator);
    var writer = try raft.buffers.LogEntryWriter.initWithBuf(allocator, &buf);
    defer writer.deinit();
    try writer.set(.{
        .index = index,
        .term = 0,
        .timestamp = index,
        .data = payload,
        .entry_type = .manifest,
    });
    _ = try writer.finishWithTail();
    const data = try buf.toOwnedSlice(allocator);
    return raft.buffers.LogEntryBuffer.initWithTail(allocator, data, .owned);
}
