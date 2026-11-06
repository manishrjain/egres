// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const raft = @import("raft");
const zdb = @import("zdb");
const vfs = @import("vfs");
const utils = @import("utils");
const _genesis = @import("genesis.zig");

const manifest = zdb.manifest;

/// A fresh, empty WAL directory under /tmp. Caller frees and deletes it.
fn tempWalDir(alloc: std.mem.Allocator, tag: []const u8) ![]u8 {
    var path_buf: [160]u8 = undefined;
    const ts = std.Io.Timestamp.now(utils.context.g().io, .real).nanoseconds;
    const path = try std.fmt.bufPrint(
        &path_buf,
        "/tmp/egres-genesis-{s}-{d}",
        .{ tag, ts },
    );
    try vfs.createDirPath(path);
    return alloc.dupe(u8, path);
}

test "genesis: two fresh WALs get the same three committed entries" {
    const alloc = std.testing.allocator;
    const dir_a = try tempWalDir(alloc, "a");
    defer {
        vfs.deleteTree(dir_a) catch {};
        alloc.free(dir_a);
    }
    const dir_b = try tempWalDir(alloc, "b");
    defer {
        vfs.deleteTree(dir_b) catch {};
        alloc.free(dir_b);
    }

    var wal_a = try raft.DiskWAL.openPath(dir_a, alloc, .{});
    defer wal_a.deinit();
    var wal_b = try raft.DiskWAL.openPath(dir_b, alloc, .{});
    defer wal_b.deinit();
    try _genesis.seed(alloc, &wal_a);
    try _genesis.seed(alloc, &wal_b);
    // A second seed of a non-empty WAL changes nothing.
    try _genesis.seed(alloc, &wal_a);

    try std.testing.expectEqual(@as(u64, 3), wal_a.lastIndex());
    try std.testing.expectEqual(@as(u64, 3), wal_a.hardState().commit_index);

    var got_a = try wal_a.entriesRange(.{ .lo = 1 });
    defer got_a.deinit();
    var got_b = try wal_b.entriesRange(.{ .lo = 1 });
    defer got_b.deinit();
    try std.testing.expectEqual(@as(usize, 3), got_a.entries.len);
    for (got_a.entries, got_b.entries) |ea, eb| {
        try std.testing.expectEqual(ea.index, eb.index);
        try std.testing.expectEqual(@as(u64, 0), ea.term);
        try std.testing.expectEqualSlices(u8, ea.bytes, eb.bytes);
    }

    // Applied in order, the entries leave namespace 1's auth trees, tenant
    // `egres` and its database.
    var m = manifest.Manifest.init(alloc);
    defer m.deinit();
    for (got_a.entries) |stored| {
        const entry = try raft.buffers.LogEntryReader.parseRoot(stored.bytes);
        const parsed = try zdb.buffers.parseMessageWithTail(entry.data());
        const mcs = try parsed.ManifestChange.toStruct(alloc);
        defer {
            mcs.deinit(alloc);
            alloc.destroy(mcs);
        }
        try std.testing.expect(try m.apply(stored.index, mcs.*));
    }
    for (manifest.auth_table_ids) |table_id|
        try std.testing.expect(m.trees.contains(.{
            .namespace = manifest.auth_namespace,
            .tree_id = zdb.kv.makeTreeId(table_id, 0),
        }));
    try std.testing.expect(m.tenantOf(manifest.auth_namespace) == null);
    try std.testing.expectEqual(
        @as(?u64, manifest.default_tenant_id),
        m.lookupTenant(manifest.default_tenant_name),
    );
    try std.testing.expectEqual(
        @as(?u64, manifest.first_namespace),
        m.lookupNamespace(
            manifest.default_tenant_id,
            _genesis.default_database,
        ),
    );
    try std.testing.expectEqual(
        @as(?u64, manifest.default_tenant_id),
        m.tenantOf(manifest.first_namespace),
    );
}
