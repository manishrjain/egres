// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const zdb = @import("zdb");
const vfs = zdb.vfs;

const _manifest = zdb.manifest;
const ManifestError = _manifest.ManifestError;
const TreeKey = zdb.kv.TreeKey;
const createSSTableChange = _manifest.createSSTableChange;
const deleteSSTableChange = _manifest.deleteSSTableChange;
const createTreeChange = _manifest.createTreeChange;
const deleteTreeChange = _manifest.deleteTreeChange;
const updateTreeChange = _manifest.updateTreeChange;

const tree_id: u64 = 1;
// CREATEs here name ids with no file behind them.
const no_extent: _manifest.Extent = .{ .size = 0, .index_offset = 0 };

/// Shorthand for a tree's global identity (namespace, namespace-local tree_id).
fn tk(namespace: u64, id: u64) TreeKey {
    return .{ .namespace = namespace, .tree_id = id };
}

// -------------------------------------------------------------------------
// In-memory commit + apply
// -------------------------------------------------------------------------

test "manifest commit applies tree + table changes" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(tree_id, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(2, &.{
        createSSTableChange(1, 0, tree_id, 0, no_extent),
        createSSTableChange(2, 0, tree_id, 1, no_extent),
        createSSTableChange(3, 0, tree_id, 2, no_extent),
    }, &.{});

    try std.testing.expectEqual(@as(u32, 3), manifest.tableCount());
    const tree = manifest.trees.get(tk(0, tree_id)).?;
    try std.testing.expect(tree.levels.items[0].tables.contains(1));
    try std.testing.expect(tree.levels.items[1].tables.contains(2));
    try std.testing.expect(tree.levels.items[2].tables.contains(3));
}

// -------------------------------------------------------------------------
// Tree create and delete (DELETE cascades through tables)
// -------------------------------------------------------------------------

test "manifest tree create and delete" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
        createTreeChange(2, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(2, &.{
        createSSTableChange(10, 0, 1, 0, no_extent),
        createSSTableChange(20, 0, 2, 0, no_extent),
    }, &.{});

    // Delete tree 2 — its tables are automatically cleaned up.
    try manifest.commit(3, &.{}, &.{deleteTreeChange(2, 0)});

    try std.testing.expectEqual(@as(u32, 1), manifest.trees.count());
    try std.testing.expect(manifest.trees.contains(tk(0, 1)));
    try std.testing.expect(!manifest.trees.contains(tk(0, 2)));
    try std.testing.expectEqual(@as(u32, 1), manifest.tableCount());
    try std.testing.expect(
        manifest.trees.get(tk(0, 1)).?.levels.items[0].tables.contains(10),
    );
}

// -------------------------------------------------------------------------
// Multi-tree table ownership (same table_id, different trees)
// -------------------------------------------------------------------------

test "manifest multi-tree table" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
        createTreeChange(2, 0, &.{}, &.{}, .posting_index),
    });

    // Same table_id=100 belongs to both trees (post-split shared SST scenario).
    try manifest.commit(2, &.{
        createSSTableChange(100, 0, 1, 0, no_extent),
        createSSTableChange(100, 0, 2, 0, no_extent),
    }, &.{});

    // Delete from tree 1 — table should still exist in tree 2.
    try manifest.commit(3, &.{deleteSSTableChange(100, 0, 1, 0)}, &.{});
    try std.testing.expect(
        !manifest.trees.get(tk(0, 1)).?.levels.items[0].tables.contains(100),
    );
    try std.testing.expect(
        manifest.trees.get(tk(0, 2)).?.levels.items[0].tables.contains(100),
    );

    // Delete from tree 2 — now table should be gone from all trees.
    try manifest.commit(4, &.{deleteSSTableChange(100, 0, 2, 0)}, &.{});
    try std.testing.expectEqual(@as(u32, 0), manifest.tableCount());
}

// -------------------------------------------------------------------------
// Tree key range update
// -------------------------------------------------------------------------

test "manifest tree key range update" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    const Key = zdb.kv.Key;

    const left = Key{ .primary = "aaa" };
    const mid = Key{ .primary = "mmm" };
    const right = Key{ .primary = "zzz" };

    const left_raw = try left.marshal(alloc);
    defer alloc.free(left_raw);
    const mid_raw = try mid.marshal(alloc);
    defer alloc.free(mid_raw);
    const right_raw = try right.marshal(alloc);
    defer alloc.free(right_raw);

    try manifest.commit(1, &.{}, &.{
        createTreeChange(1, 0, left_raw, mid_raw, .posting_index),
    });
    {
        const tree = manifest.trees.get(tk(0, 1)).?;
        try std.testing.expectEqualStrings("aaa", tree.left.primary);
        try std.testing.expectEqualStrings("mmm", tree.right.primary);
    }

    try manifest.commit(
        2,
        &.{},
        &.{updateTreeChange(1, 0, left_raw, right_raw)},
    );
    {
        const tree = manifest.trees.get(tk(0, 1)).?;
        try std.testing.expectEqualStrings("aaa", tree.left.primary);
        try std.testing.expectEqualStrings("zzz", tree.right.primary);
    }
}

// -------------------------------------------------------------------------
// Tree-not-found errors
// -------------------------------------------------------------------------

test "manifest applySSTableChange on missing tree errors" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try std.testing.expectError(
        ManifestError.TreeNotFound,
        manifest.commit(1, &.{
            createSSTableChange(1, 0, 999, 0, no_extent),
        }, &.{}),
    );
}

// -------------------------------------------------------------------------
// serialize → restoreFrom round-trip
// -------------------------------------------------------------------------

test "manifest serialize/restoreFrom round-trip" {
    const alloc = std.testing.allocator;

    // Build a non-trivial state: 2 trees with key ranges + namespaces, tables
    // spread across L0–L3, including the "shared table across trees" case.
    var src = _manifest.Manifest.init(alloc);
    defer src.deinit();

    const Key = zdb.kv.Key;
    const a_raw = try (Key{ .primary = "aaa" }).marshal(alloc);
    defer alloc.free(a_raw);
    const m_raw = try (Key{ .primary = "mmm" }).marshal(alloc);
    defer alloc.free(m_raw);
    const z_raw = try (Key{ .primary = "zzz" }).marshal(alloc);
    defer alloc.free(z_raw);

    // Tree 2 is keyed by egids; the kind must survive the round-trip,
    // or a restored rows tree writes fuse filters and never moves.
    const egid_tree = createTreeChange(2, 107, m_raw, z_raw, .rows);
    try src.commit(1, &.{}, &.{
        createTreeChange(1, 107, a_raw, m_raw, .posting_index),
        egid_tree,
        // A named user table, split into two trees — the name must survive
        // the round-trip via its FIRST tree (the one with the open left
        // bound; serialize stamps only that one).
        createTreeChange(
            zdb.kv.makeTreeId(16, 0),
            107,
            &.{},
            m_raw,
            .posting_index,
        ),
        createTreeChange(
            zdb.kv.makeTreeId(16, 1),
            107,
            m_raw,
            &.{},
            .posting_index,
        ),
    });
    try src.registerTableName(.{ .namespace = 107, .table_id = 16 }, "users");
    try src.commit(2, &.{
        createSSTableChange(10, 107, 1, 0, no_extent),
        createSSTableChange(11, 107, 1, 0, no_extent),
        createSSTableChange(12, 107, 1, 1, no_extent),
        createSSTableChange(13, 107, 1, 3, no_extent),
        createSSTableChange(20, 107, 2, 2, no_extent),
        // Same table_id present in both trees — shared post-split SST.
        createSSTableChange(50, 107, 1, 3, no_extent),
        createSSTableChange(50, 107, 2, 3, no_extent),
    }, &.{});

    // Serialize.
    const bytes = try src.serialize(alloc);
    defer alloc.free(bytes);
    try std.testing.expect(bytes.len > 0);

    // Restore into a fresh state.
    var dst = _manifest.Manifest.init(alloc);
    defer dst.deinit();
    try dst.restoreFrom(bytes);

    // Counts match.
    try std.testing.expectEqual(src.trees.count(), dst.trees.count());
    try std.testing.expectEqual(src.tableCount(), dst.tableCount());

    // Tree 1: range, namespace, level 0/1/3 tables, shared table at L3.
    const t1 = dst.trees.get(tk(107, 1)).?;
    try std.testing.expectEqual(@as(u64, 107), t1.namespace_id);
    try std.testing.expectEqualStrings("aaa", t1.left.primary);
    try std.testing.expectEqualStrings("mmm", t1.right.primary);
    try std.testing.expect(t1.levels.items[0].tables.contains(10));
    try std.testing.expect(t1.levels.items[0].tables.contains(11));
    try std.testing.expect(t1.levels.items[1].tables.contains(12));
    try std.testing.expect(t1.levels.items[3].tables.contains(13));
    try std.testing.expect(t1.levels.items[3].tables.contains(50));

    // Tree 2: range, namespace, table 20 at L2, shared 50 at L3.
    const t2 = dst.trees.get(tk(107, 2)).?;
    try std.testing.expectEqual(@as(u64, 107), t2.namespace_id);
    try std.testing.expectEqualStrings("mmm", t2.left.primary);
    try std.testing.expectEqualStrings("zzz", t2.right.primary);
    try std.testing.expect(t2.levels.items[2].tables.contains(20));
    try std.testing.expect(t2.levels.items[3].tables.contains(50));
    try std.testing.expectEqual(_manifest.TreeKind.rows, t2.kind);
    try std.testing.expectEqual(_manifest.TreeKind.posting_index, t1.kind);

    // The table name rode the snapshot (stamped on its tree's change).
    try std.testing.expectEqual(@as(?u32, 16), dst.lookupTable(107, "users"));
}

// -------------------------------------------------------------------------
// Restoring into a non-empty state replaces (not merges) the contents
// -------------------------------------------------------------------------

test "manifest restoreFrom replaces existing state" {
    const alloc = std.testing.allocator;

    var src = _manifest.Manifest.init(alloc);
    defer src.deinit();
    try src.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
    });
    try src.commit(2, &.{createSSTableChange(10, 0, 1, 0, no_extent)}, &.{});
    const bytes = try src.serialize(alloc);
    defer alloc.free(bytes);

    // Fill `dst` with garbage that should disappear after restore.
    var dst = _manifest.Manifest.init(alloc);
    defer dst.deinit();
    try dst.commit(3, &.{}, &.{
        createTreeChange(99, 0, &.{}, &.{}, .posting_index),
    });
    try dst.commit(4, &.{createSSTableChange(999, 0, 99, 0, no_extent)}, &.{});
    try dst.registerTableName(.{ .namespace = 100, .table_id = 55 }, "stale");
    try std.testing.expect(dst.trees.contains(tk(0, 99)));

    try dst.restoreFrom(bytes);

    try std.testing.expect(!dst.trees.contains(tk(0, 99)));
    try std.testing.expect(dst.trees.contains(tk(0, 1)));
    try std.testing.expectEqual(@as(u32, 1), dst.tableCount());
    // Pre-restore table names are gone too (freed, not leaked).
    try std.testing.expect(dst.lookupTable(100, "stale") == null);
}

// -------------------------------------------------------------------------
// Table names: register / lookup / drop
// -------------------------------------------------------------------------

test "manifest table names: register/lookup/drop" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.registerTableName(
        .{ .namespace = 105, .table_id = 16 },
        "users",
    );
    try std.testing.expectEqual(
        @as(?u32, 16),
        manifest.lookupTable(105, "users"),
    );

    // Idempotent re-apply; a collision (build-side bug) is skipped, never
    // clobbered.
    try manifest.registerTableName(
        .{ .namespace = 105, .table_id = 16 },
        "users",
    );
    try manifest.registerTableName(
        .{ .namespace = 105, .table_id = 17 },
        "users",
    );
    try std.testing.expectEqual(
        @as(?u32, 16),
        manifest.lookupTable(105, "users"),
    );

    // Empty names record nothing; lookups are namespace-scoped.
    try manifest.registerTableName(.{ .namespace = 105, .table_id = 18 }, "");
    try std.testing.expect(manifest.lookupTable(105, "") == null);
    try std.testing.expect(manifest.lookupTable(106, "users") == null);

    // dropTable frees the name for a re-register at a fresh id; idempotent.
    manifest.dropTable(.{ .namespace = 105, .table_id = 16 });
    try std.testing.expect(manifest.lookupTable(105, "users") == null);
    manifest.dropTable(.{ .namespace = 105, .table_id = 16 });
    try manifest.registerTableName(
        .{ .namespace = 105, .table_id = 19 },
        "users",
    );
    try std.testing.expectEqual(
        @as(?u32, 19),
        manifest.lookupTable(105, "users"),
    );

    // dropNamespace forgets every table name (leak-checked by the test
    // allocator).
    try manifest.registerTableName(
        .{ .namespace = 105, .table_id = 20 },
        "orders",
    );
    manifest.dropNamespace(105);
    try std.testing.expect(manifest.lookupTable(105, "users") == null);
    try std.testing.expect(manifest.lookupTable(105, "orders") == null);
}

// -------------------------------------------------------------------------
// clone produces an independent deep copy
// -------------------------------------------------------------------------

test "manifest clone is independent of source" {
    const alloc = std.testing.allocator;

    var src = _manifest.Manifest.init(alloc);
    defer src.deinit();
    try src.commit(1, &.{}, &.{
        createTreeChange(1, 5, &.{}, &.{}, .posting_index),
    });
    try src.commit(2, &.{createSSTableChange(10, 5, 1, 0, no_extent)}, &.{});

    var copy = _manifest.Manifest.init(alloc);
    defer copy.deinit();
    try src.cloneInto(&copy);

    // Mutating src must not affect copy.
    try src.commit(3, &.{createSSTableChange(11, 5, 1, 0, no_extent)}, &.{});
    try std.testing.expectEqual(@as(u32, 2), src.tableCount());
    try std.testing.expectEqual(@as(u32, 1), copy.tableCount());
    try std.testing.expect(
        copy.trees.get(tk(5, 1)).?.levels.items[0].tables.contains(10),
    );
    try std.testing.expect(
        !copy.trees.get(tk(5, 1)).?.levels.items[0].tables.contains(11),
    );

    // Mutating copy must not affect src.
    try copy.commit(4, &.{}, &.{deleteTreeChange(1, 5)});
    try std.testing.expectEqual(@as(u32, 0), copy.trees.count());
    try std.testing.expectEqual(@as(u32, 1), src.trees.count());
}

// -------------------------------------------------------------------------
// Per-namespace table_id counter: allocate, advance, snapshot round-trip
// -------------------------------------------------------------------------

test "manifest table_id counter allocate/advance/persist" {
    const alloc = std.testing.allocator;
    const first = _manifest.first_table_id;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    // First allocation in a namespace floors at first_table_id; then +1 each.
    try std.testing.expectEqual(first, try manifest.nextTableId(16));
    try std.testing.expectEqual(first + 1, try manifest.nextTableId(16));
    // A different namespace is independent.
    try std.testing.expectEqual(first, try manifest.nextTableId(17));

    // advanceTableId only ever raises: an id below the current high-water-mark
    // is ignored (next = first+1 < the counter, which is already at first+2)...
    try manifest.advanceTableId(.{ .namespace = 16, .table_id = first });
    try std.testing.expectEqual(first + 2, try manifest.nextTableId(16));
    // ...but a higher id jumps the counter forward.
    try manifest.advanceTableId(.{ .namespace = 16, .table_id = 1000 });
    try std.testing.expectEqual(@as(u32, 1001), try manifest.nextTableId(16));

    // Counters survive serialize → restore (the snapshot path).
    const bytes = try manifest.serialize(alloc);
    defer alloc.free(bytes);
    var dst = _manifest.Manifest.init(alloc);
    defer dst.deinit();
    try dst.restoreFrom(bytes);
    try std.testing.expectEqual(@as(u32, 1002), try dst.nextTableId(16));
    try std.testing.expectEqual(first + 1, try dst.nextTableId(17));
}

test "manifest next_ns_id allocate/advance/persist" {
    const alloc = std.testing.allocator;
    const first = _manifest.first_namespace;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    // Fresh state allocates from first_namespace; then +1 each.
    try std.testing.expectEqual(first, manifest.nextNamespace());
    try std.testing.expectEqual(first + 1, manifest.nextNamespace());

    // advance only raises: a reserved (sub-floor) id is ignored...
    manifest.advanceNamespace(3);
    try std.testing.expectEqual(first + 2, manifest.nextNamespace());
    // ...a higher id jumps the counter forward.
    manifest.advanceNamespace(100);
    try std.testing.expectEqual(@as(u64, 101), manifest.nextNamespace());

    // Survives serialize → restore (the snapshot path).
    const bytes = try manifest.serialize(alloc);
    defer alloc.free(bytes);
    var dst = _manifest.Manifest.init(alloc);
    defer dst.deinit();
    try dst.restoreFrom(bytes);
    try std.testing.expectEqual(@as(u64, 102), dst.nextNamespace());
}

// -------------------------------------------------------------------------
// File ids
// -------------------------------------------------------------------------

test "manifest newFileId draws nonzero, distinct ids" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(tree_id, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(2, &.{
        createSSTableChange(10, 0, tree_id, 0, no_extent),
        createSSTableChange(20, 0, tree_id, 1, no_extent),
    }, &.{});

    // Every draw is nonzero, pairwise distinct, and never a listed id.
    var seen = std.AutoHashMap(u64, void).init(alloc);
    defer seen.deinit();
    for (0..100) |_| {
        const id = manifest.newFileId();
        try std.testing.expect(id != 0 and id != 10 and id != 20);
        try std.testing.expect(!seen.contains(id));
        try seen.put(id, {});
    }
}

// -------------------------------------------------------------------------
// Retire list: a file leaves the manifest at the index of the entry that
// removed its last listing.
// -------------------------------------------------------------------------

test "manifest retires a file at the index that drops its last listing" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
        createTreeChange(2, 0, &.{}, &.{}, .posting_index),
    });

    // Listed by tree 1 at index 2, shared with tree 2 at index 3.
    try manifest.commit(2, &.{
        createSSTableChange(10, 0, 1, 0, no_extent),
    }, &.{});
    try manifest.commit(3, &.{
        createSSTableChange(10, 0, 2, 0, no_extent),
    }, &.{});
    try std.testing.expectEqual(@as(u32, 2), manifest.ssts.get(10).?.refs);

    // The first delete leaves a listing standing: nothing to retire.
    try manifest.commit(5, &.{deleteSSTableChange(10, 0, 1, 0)}, &.{});
    try std.testing.expect(!manifest.to_delete.contains(10));

    // The second takes the last one: retired at that entry's index.
    try manifest.commit(7, &.{deleteSSTableChange(10, 0, 2, 0)}, &.{});
    try std.testing.expectEqual(@as(u64, 7), manifest.to_delete.get(10).?);

    // A sweep that has not reached index 7 takes nothing.
    const early = try manifest.takeToDelete(alloc, 6);
    defer alloc.free(early);
    try std.testing.expectEqual(@as(usize, 0), early.len);
    try std.testing.expectEqual(@as(u32, 1), manifest.to_delete.count());

    // One that has takes the record, exactly once.
    const taken = try manifest.takeToDelete(alloc, 7);
    defer alloc.free(taken);
    try std.testing.expectEqual(@as(usize, 1), taken.len);
    try std.testing.expectEqual(@as(u64, 10), taken[0].sst_id);
    try std.testing.expectEqual(@as(u64, 7), taken[0].index);
    try std.testing.expectEqual(@as(u32, 0), manifest.to_delete.count());
}

test "manifest tree delete retires every table of the tree" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(4, &.{
        createSSTableChange(11, 0, 1, 0, no_extent),
        createSSTableChange(12, 0, 1, 0, no_extent),
        createSSTableChange(13, 0, 1, 1, no_extent),
    }, &.{});

    try manifest.commit(9, &.{}, &.{deleteTreeChange(1, 0)});

    try std.testing.expectEqual(@as(u32, 3), manifest.to_delete.count());
    for ([_]u64{ 11, 12, 13 }) |id|
        try std.testing.expectEqual(@as(u64, 9), manifest.to_delete.get(id).?);
}

test "manifest restoreFrom leaves the retire list empty" {
    const alloc = std.testing.allocator;
    var src = _manifest.Manifest.init(alloc);
    defer src.deinit();

    try src.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
    });
    try src.commit(4, &.{
        createSSTableChange(10, 0, 1, 0, no_extent),
        createSSTableChange(20, 0, 1, 0, no_extent),
    }, &.{});
    try src.commit(6, &.{deleteSSTableChange(20, 0, 1, 0)}, &.{});
    try std.testing.expectEqual(@as(u32, 1), src.to_delete.count());

    const bytes = try src.serialize(alloc);
    defer alloc.free(bytes);

    var dst = _manifest.Manifest.init(alloc);
    defer dst.deinit();
    // Pre-load the destination with a retire of its own: restore replaces
    // the whole state, so that record goes too.
    try dst.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
    });
    try dst.commit(2, &.{createSSTableChange(99, 0, 1, 0, no_extent)}, &.{});
    try dst.commit(3, &.{deleteSSTableChange(99, 0, 1, 0)}, &.{});
    try std.testing.expectEqual(@as(u32, 1), dst.to_delete.count());

    try dst.restoreFrom(bytes);
    try std.testing.expectEqual(@as(u32, 0), dst.to_delete.count());
    try std.testing.expect(dst.ssts.contains(10));
    try std.testing.expect(!dst.ssts.contains(20));
}

test "manifest knowsFile covers listed and retired ids" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(4, &.{
        createSSTableChange(10, 0, 1, 0, no_extent),
        createSSTableChange(20, 0, 1, 0, no_extent),
    }, &.{});
    try manifest.commit(6, &.{deleteSSTableChange(20, 0, 1, 0)}, &.{});

    try std.testing.expect(manifest.knowsFile(10)); // listed
    try std.testing.expect(manifest.knowsFile(20)); // retired
    try std.testing.expect(!manifest.knowsFile(30));

    // A sweep of the retired id stops the manifest knowing it.
    const taken = try manifest.takeToDelete(alloc, 6);
    defer alloc.free(taken);
    try std.testing.expectEqual(@as(usize, 1), taken.len);
    try std.testing.expect(!manifest.knowsFile(20));
    try std.testing.expect(manifest.knowsFile(10));
}

test "manifest unknownFiles picks SST-named files the manifest does not know" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(4, &.{
        createSSTableChange(0x10, 0, 1, 0, no_extent),
        createSSTableChange(0x20, 0, 1, 0, no_extent),
    }, &.{});
    try manifest.commit(6, &.{deleteSSTableChange(0x20, 0, 1, 0)}, &.{});

    // A `.names` listing: regular files only, no stat.
    const entries = [_]vfs.ObjectInfo{
        // Unknown SST names.
        .{ .name = "00000000000000aa.sst", .stat = null },
        .{ .name = "00000000000000bb.sst", .stat = null },
        // Listed by a tree.
        .{ .name = "0000000000000010.sst", .stat = null },
        // ToDelete but not swept yet.
        .{ .name = "0000000000000020.sst", .stat = null },
        // Not an SST name at all.
        .{ .name = "NOTES.txt", .stat = null },
        .{ .name = "00001.wal", .stat = null },
        // SST-shaped but not 16 hex digits.
        .{ .name = "00000000000000zz.sst", .stat = null },
    };

    const unknown = try manifest.unknownFiles(alloc, &entries);
    defer alloc.free(unknown);
    try std.testing.expectEqualSlices(u64, &.{ 0xaa, 0xbb }, unknown);
}

test "manifest newFileId never draws a listed or retired id" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(4, &.{
        createSSTableChange(10, 0, 1, 0, no_extent),
        createSSTableChange(20, 0, 1, 0, no_extent),
    }, &.{});
    // 20 leaves the manifest but stays in the retire list: its file may
    // still be on disk under that name until the next sweep.
    try manifest.commit(6, &.{deleteSSTableChange(20, 0, 1, 0)}, &.{});
    try std.testing.expect(manifest.ssts.contains(10));
    try std.testing.expect(manifest.to_delete.contains(20));

    for (0..1000) |_| {
        const id = manifest.newFileId();
        try std.testing.expect(id != 0);
        try std.testing.expect(!manifest.knowsFile(id));
    }
}

test "manifest sst refs across create, share, and delete" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
        createTreeChange(2, 0, &.{}, &.{}, .posting_index),
    });

    // One file listed by tree 1 at index 5.
    try manifest.commit(5, &.{
        createSSTableChange(77, 0, 1, 0, no_extent),
    }, &.{});
    try std.testing.expectEqual(@as(u32, 1), manifest.ssts.get(77).?.refs);
    try std.testing.expectEqual(@as(u64, 5), manifest.raftIndexOf(77));

    // A split hands the same file to tree 2 later: a second listing, but
    // the file keeps the index that introduced it.
    try manifest.commit(9, &.{
        createSSTableChange(77, 0, 2, 0, no_extent),
    }, &.{});
    try std.testing.expectEqual(@as(u32, 2), manifest.ssts.get(77).?.refs);
    try std.testing.expectEqual(@as(u64, 5), manifest.raftIndexOf(77));

    // One delete leaves the other listing standing.
    try manifest.commit(11, &.{deleteSSTableChange(77, 0, 1, 0)}, &.{});
    try std.testing.expectEqual(@as(u32, 1), manifest.ssts.get(77).?.refs);

    // Dropping the last listing's tree forgets the file.
    try manifest.commit(12, &.{}, &.{deleteTreeChange(2, 0)});
    try std.testing.expect(manifest.ssts.get(77) == null);
}

test "manifest serialize round-trip preserves raft_index" {
    const alloc = std.testing.allocator;
    var src = _manifest.Manifest.init(alloc);
    defer src.deinit();

    try src.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
    });
    try src.commit(4, &.{createSSTableChange(10, 0, 1, 0, no_extent)}, &.{});
    try src.commit(6, &.{createSSTableChange(20, 0, 1, 0, no_extent)}, &.{});
    try src.commit(8, &.{createSSTableChange(30, 0, 1, 1, no_extent)}, &.{});

    const bytes = try src.serialize(alloc);
    defer alloc.free(bytes);

    var dst = _manifest.Manifest.init(alloc);
    defer dst.deinit();
    try dst.restoreFrom(bytes);

    try std.testing.expectEqual(@as(u64, 4), dst.raftIndexOf(10));
    try std.testing.expectEqual(@as(u64, 6), dst.raftIndexOf(20));
    try std.testing.expectEqual(@as(u64, 8), dst.raftIndexOf(30));
}

// -------------------------------------------------------------------------
// apply: structural changes plus the name and counter effects of `cmd`
// -------------------------------------------------------------------------

/// The create_ns change a namespace create proposes, cut to the one tree
/// that matters here: the default table's, carrying the database name and
/// the tenant.
fn nsCreateCmd(
    tcs: *[1]_manifest.TreeChange,
    namespace: u64,
    tenant_id: u64,
    name: []const u8,
) _manifest.ManifestChange {
    tcs[0] = createTreeChange(
        zdb.kv.makeTreeId(0, 0),
        namespace,
        &.{},
        &.{},
        .posting_index,
    );
    tcs[0].name = name;
    tcs[0].tenant_id = tenant_id;
    return .{ .tree_changes = tcs, .cmd = .create_ns };
}

/// The create_tenant change for tenant `id` named `name`.
fn tenantCmd(
    ts: *[1]_manifest.TenantRecord,
    id: u64,
    name: []const u8,
) _manifest.ManifestChange {
    ts[0] = .{ .id = id, .name = name };
    return .{ .tenants = ts, .cmd = .create_tenant };
}

/// Apply create_tenant for `id`, which every user namespace needs first.
fn addTenant(m: *_manifest.Manifest, id: u64, name: []const u8) !void {
    var ts: [1]_manifest.TenantRecord = undefined;
    try std.testing.expect(try m.apply(1, tenantCmd(&ts, id, name)));
}

/// The create_tree change a CREATE TABLE proposes: one tree at split 0 for
/// `table_key`'s table, carrying the table name.
fn createTreeCmd(
    tcs: *[1]_manifest.TreeChange,
    table_key: zdb.kv.TableKey,
    name: []const u8,
) _manifest.ManifestChange {
    tcs[0] = createTreeChange(
        zdb.kv.makeTreeId(table_key.table_id, 0),
        table_key.namespace,
        &.{},
        &.{},
        .posting_index,
    );
    tcs[0].name = name;
    return .{ .tree_changes = tcs, .cmd = .create_tree };
}

test "manifest apply create_ns registers the name and advances next_ns_id" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();
    try addTenant(&manifest, 1, "acme");

    var tcs: [1]_manifest.TreeChange = undefined;
    try std.testing.expect(
        try manifest.apply(5, nsCreateCmd(&tcs, 100, 1, "shop")),
    );

    try std.testing.expect(manifest.trees.contains(tk(100, 0)));
    try std.testing.expectEqual(
        @as(?u64, 100),
        manifest.lookupNamespace(1, "shop"),
    );
    try std.testing.expectEqual(@as(?u64, 1), manifest.tenantOf(100));
    // The counter moved past the created namespace, so the next allocation
    // cannot re-issue it.
    try std.testing.expectEqual(@as(u64, 101), manifest.nextNamespace());
}

test "manifest apply create_tree registers the name and advances the table id" {
    const alloc = std.testing.allocator;
    const first = _manifest.first_table_id;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    var tcs: [1]_manifest.TreeChange = undefined;
    _ = try manifest.apply(6, createTreeCmd(
        &tcs,
        .{ .namespace = 100, .table_id = first },
        "users",
    ));

    try std.testing.expect(
        manifest.trees.contains(tk(100, zdb.kv.makeTreeId(first, 0))),
    );
    try std.testing.expectEqual(
        @as(?u32, first),
        manifest.lookupTable(100, "users"),
    );
    try std.testing.expectEqual(first + 1, try manifest.nextTableId(100));
}

test "manifest apply is idempotent for a re-applied create" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();
    try addTenant(&manifest, 1, "acme");
    try addTenant(&manifest, 1, "acme"); // re-applied tenant: no change

    // The WAL-replay-over-snapshot case: the same entry applied twice. The
    // second must leave one tree and leak nothing (std.testing.allocator
    // fails the test if the first tree's keys are dropped on the floor).
    var tcs: [1]_manifest.TreeChange = undefined;
    const mc = nsCreateCmd(&tcs, 100, 1, "shop");
    try std.testing.expect(try manifest.apply(5, mc));
    try std.testing.expect(try manifest.apply(5, mc));

    try std.testing.expectEqual(@as(u32, 1), manifest.trees.count());
    try std.testing.expectEqual(
        @as(?u64, 100),
        manifest.lookupNamespace(1, "shop"),
    );
    try std.testing.expectEqual(@as(u32, 1), manifest.tenants.count());
}

test "manifest apply drop_tree removes the table name" {
    const alloc = std.testing.allocator;
    const first = _manifest.first_table_id;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    var tcs: [1]_manifest.TreeChange = undefined;
    _ = try manifest.apply(6, createTreeCmd(
        &tcs,
        .{ .namespace = 100, .table_id = first },
        "users",
    ));

    var dtcs = [_]_manifest.TreeChange{
        deleteTreeChange(zdb.kv.makeTreeId(first, 0), 100),
    };
    _ = try manifest.apply(7, .{ .tree_changes = &dtcs, .cmd = .drop_tree });

    try std.testing.expect(manifest.lookupTable(100, "users") == null);
    try std.testing.expectEqual(@as(u32, 0), manifest.trees.count());
    // The id is not rewound: ids are never reused.
    try std.testing.expectEqual(first + 1, try manifest.nextTableId(100));
}

test "manifest apply drop_ns removes the namespace name" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();
    try addTenant(&manifest, 1, "acme");

    var tcs: [1]_manifest.TreeChange = undefined;
    _ = try manifest.apply(5, nsCreateCmd(&tcs, 100, 1, "shop"));

    var dtcs = [_]_manifest.TreeChange{deleteTreeChange(0, 100)};
    _ = try manifest.apply(8, .{ .tree_changes = &dtcs, .cmd = .drop_ns });

    try std.testing.expect(manifest.lookupNamespace(1, "shop") == null);
    try std.testing.expectEqual(@as(u32, 0), manifest.trees.count());
}

test "manifest: two tenants can each have a database with the same name" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();
    try addTenant(&manifest, 1, "acme");
    try addTenant(&manifest, 2, "globex");

    var a: [1]_manifest.TreeChange = undefined;
    var b: [1]_manifest.TreeChange = undefined;
    try std.testing.expect(
        try manifest.apply(5, nsCreateCmd(&a, 100, 1, "users")),
    );
    try std.testing.expect(
        try manifest.apply(6, nsCreateCmd(&b, 101, 2, "users")),
    );
    try std.testing.expectEqual(
        @as(?u64, 100),
        manifest.lookupNamespace(1, "users"),
    );
    try std.testing.expectEqual(
        @as(?u64, 101),
        manifest.lookupNamespace(2, "users"),
    );
    // A tenant never finds another tenant's database by name.
    try std.testing.expect(manifest.lookupNamespace(3, "users") == null);
}

test "manifest: create_ns for a missing tenant is skipped, not an error" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    var tcs: [1]_manifest.TreeChange = undefined;
    // No tenant 7, and no tenant at all: both refused, nothing applied.
    try std.testing.expect(
        !try manifest.apply(5, nsCreateCmd(&tcs, 100, 7, "shop")),
    );
    try std.testing.expect(
        !try manifest.apply(6, nsCreateCmd(&tcs, 101, 0, "shop")),
    );
    try std.testing.expectEqual(@as(u32, 0), manifest.trees.count());
    try std.testing.expect(manifest.tenantOf(100) == null);
}

test "manifest: create_tenant refuses id 0 and a taken name or id" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();
    try addTenant(&manifest, 1, "acme");

    var ts: [1]_manifest.TenantRecord = undefined;
    try std.testing.expect(!try manifest.apply(2, tenantCmd(&ts, 0, "zero")));
    try std.testing.expect(!try manifest.apply(3, tenantCmd(&ts, 2, "acme")));
    try std.testing.expect(!try manifest.apply(4, tenantCmd(&ts, 1, "other")));
    try std.testing.expectEqual(@as(u32, 1), manifest.tenants.count());
    try std.testing.expectEqual(@as(?u64, 1), manifest.lookupTenant("acme"));
    // The counter moved past the applied tenant only.
    try std.testing.expectEqual(@as(u64, 2), manifest.nextTenant());
}

test "manifest: reserved namespaces carry no tenant and make no tables" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    var tcs: [1]_manifest.TreeChange = undefined;
    // Below first_namespace no tenant is needed, and none is recorded.
    try std.testing.expect(
        try manifest.apply(5, nsCreateCmd(
            &tcs,
            _manifest.auth_namespace,
            0,
            "",
        )),
    );
    try std.testing.expect(manifest.trees.contains(tk(1, 0)));
    try std.testing.expect(!manifest.namespaces.contains(1));
    try std.testing.expect(manifest.tenantOf(1) == null);
    try std.testing.expectError(
        error.ReservedNamespace,
        manifest.nextTableId(1),
    );
}

test "manifest: an unnamed user namespace keeps its tenant" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();
    try addTenant(&manifest, 1, "acme");

    var tcs: [1]_manifest.TreeChange = undefined;
    try std.testing.expect(
        try manifest.apply(5, nsCreateCmd(&tcs, 100, 1, "")),
    );
    try std.testing.expectEqual(@as(?u64, 1), manifest.tenantOf(100));
}

test "manifest: branch_ns takes the change's tenant" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();
    try addTenant(&manifest, 1, "acme");
    try addTenant(&manifest, 2, "globex");

    var tcs: [1]_manifest.TreeChange = undefined;
    _ = try manifest.apply(5, nsCreateCmd(&tcs, 100, 1, "src"));
    var btcs = [_]_manifest.TreeChange{
        createTreeChange(zdb.kv.makeTreeId(0, 0), 101, &.{}, &.{}, .none),
    };
    btcs[0].name = "dst";
    btcs[0].tenant_id = 2;
    try std.testing.expect(try manifest.apply(6, .{
        .tree_changes = &btcs,
        .cmd = .branch_ns,
        .branch_from = 100,
    }));
    try std.testing.expectEqual(@as(?u64, 2), manifest.tenantOf(101));
    try std.testing.expectEqual(
        @as(?u64, 101),
        manifest.lookupNamespace(2, "dst"),
    );
    try std.testing.expect(manifest.lookupNamespace(1, "dst") == null);
}

test "manifest: a snapshot round trip keeps tenants and their namespaces" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();
    try addTenant(&manifest, 1, "acme");
    try addTenant(&manifest, 2, "globex");

    var a: [1]_manifest.TreeChange = undefined;
    var b: [1]_manifest.TreeChange = undefined;
    var c: [1]_manifest.TreeChange = undefined;
    _ = try manifest.apply(5, nsCreateCmd(&a, 100, 1, "users"));
    _ = try manifest.apply(6, nsCreateCmd(&b, 101, 2, "users"));
    _ = try manifest.apply(7, nsCreateCmd(&c, 102, 2, ""));
    _ = manifest.nextTenant(); // a burned id is not reissued after restore

    const bytes = try manifest.serialize(alloc);
    defer alloc.free(bytes);
    var restored = _manifest.Manifest.init(alloc);
    defer restored.deinit();
    try restored.restoreFrom(bytes);

    try std.testing.expectEqual(@as(?u64, 1), restored.lookupTenant("acme"));
    try std.testing.expectEqual(@as(?u64, 2), restored.lookupTenant("globex"));
    try std.testing.expectEqual(
        @as(?u64, 100),
        restored.lookupNamespace(1, "users"),
    );
    try std.testing.expectEqual(
        @as(?u64, 101),
        restored.lookupNamespace(2, "users"),
    );
    try std.testing.expectEqual(@as(?u64, 2), restored.tenantOf(102));
    try std.testing.expectEqual(@as(u64, 4), restored.nextTenant());
}

test "manifest takeToEvict hands over ids whose last listing went, once" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
        createTreeChange(2, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(2, &.{
        createSSTableChange(10, 0, 1, 0, no_extent),
        createSSTableChange(20, 0, 1, 0, no_extent),
    }, &.{});
    // 20 is shared with tree 2: one delete leaves it listed.
    try manifest.commit(3, &.{
        createSSTableChange(20, 0, 2, 0, no_extent),
    }, &.{});

    try manifest.commit(4, &.{
        deleteSSTableChange(10, 0, 1, 0),
        deleteSSTableChange(20, 0, 1, 0),
    }, &.{});
    {
        const gone = try manifest.takeToEvict();
        defer alloc.free(gone);
        try std.testing.expectEqualSlices(u64, &.{10}, gone);
    }
    // Drained: nothing until the next unlist.
    {
        const none = try manifest.takeToEvict();
        defer alloc.free(none);
        try std.testing.expectEqual(@as(usize, 0), none.len);
    }
    try manifest.commit(5, &.{deleteSSTableChange(20, 0, 2, 0)}, &.{});
    {
        const gone = try manifest.takeToEvict();
        defer alloc.free(gone);
        try std.testing.expectEqualSlices(u64, &.{20}, gone);
    }
}

test "manifest split move in one entry keeps the file listed and unretired" {
    const alloc = std.testing.allocator;
    var manifest = _manifest.Manifest.init(alloc);
    defer manifest.deinit();

    try manifest.commit(1, &.{}, &.{
        createTreeChange(1, 0, &.{}, &.{}, .posting_index),
        createTreeChange(2, 0, &.{}, &.{}, .posting_index),
    });
    try manifest.commit(5, &.{
        createSSTableChange(77, 0, 1, 0, no_extent),
    }, &.{});

    // A split moves the file: the delete from the old tree is listed
    // before the create in the new one, as buildSplit emits them.
    try manifest.commit(9, &.{
        deleteSSTableChange(77, 0, 1, 0),
        createSSTableChange(77, 0, 2, 0, no_extent),
    }, &.{});
    try std.testing.expectEqual(@as(u32, 1), manifest.ssts.get(77).?.refs);
    try std.testing.expectEqual(@as(u64, 5), manifest.raftIndexOf(77));
    try std.testing.expect(!manifest.to_delete.contains(77));
    const to_evict = try manifest.takeToEvict();
    defer alloc.free(to_evict);
    try std.testing.expectEqual(@as(usize, 0), to_evict.len);

    // The same pair through `apply` (the snapshot draft's path).
    var scs = [_]_manifest.SSTableChange{
        deleteSSTableChange(77, 0, 2, 0),
        createSSTableChange(77, 0, 1, 0, no_extent),
    };
    _ = try manifest.apply(10, .{
        .cmd = .tree_split,
        .sstable_changes = &scs,
    });
    try std.testing.expectEqual(@as(u32, 1), manifest.ssts.get(77).?.refs);
    try std.testing.expect(!manifest.to_delete.contains(77));
}
