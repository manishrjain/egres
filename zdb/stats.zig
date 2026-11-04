// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The human-readable stats dump of a Forest: a top-level summary, one
//! tree, or one namespace. Every function here takes a `*Forest` and
//! leaves it unchanged; `Forest` aliases each declaration back in, so
//! callers keep using method syntax.

const std = @import("std");
const _table = @import("table.zig");
const _tree = @import("tree.zig");

const Forest = @import("forest.zig").Forest;
const TreeKey = @import("kv.zig").TreeKey;

/// What slice of the forest the caller wants to see. Default is a
/// top-level summary that aggregates across all trees.
pub const StatsScope = union(enum) {
    forest,
    tree: TreeKey,
    namespace: u64,
};

pub const StatsOpts = struct {
    scope: StatsScope = .forest,
};

/// writeStats produces a hierarchical, human-readable text dump of
/// current LSM state to `w`. Unlike the Prometheus metrics endpoint,
/// this is for ops drill-down: per-tree shape, per-level sizes,
/// pending compactions. Snapshot is best-effort — locks are taken
/// briefly per section, so a concurrent flush/compaction may show
/// itself half-applied. Acceptable for a debug endpoint.
pub fn writeStats(self: *Forest, w: *std.Io.Writer, opts: StatsOpts) !void {
    switch (opts.scope) {
        .forest => try self.writeForestStats(w),
        .tree => |id| try self.writeTreeStats(w, id),
        .namespace => |ns| try self.writeNamespaceStats(w, ns),
    }
}

pub fn writeForestStats(self: *Forest, w: *std.Io.Writer) !void {
    const mt = self.memtables.stats();
    try w.print("forest:\n", .{});
    try w.print("  memtables: active=", .{});
    try writeBytes(w, mt.active_bytes);
    try w.print(" immutable={d} (", .{mt.immutable_count});
    try writeBytes(w, mt.immutable_bytes);
    try w.print(" pending flush)\n", .{});
    try w.print("  flush_pending: {d}\n", .{mt.immutable_count});
    if (_table.table.block_cache) |bc| {
        try w.print("  block_cache: entries={d} bytes=", .{bc.count()});
        try writeBytes(w, bc.totalCost());
        const hits = bc.hits.load(.monotonic);
        const misses = bc.misses.load(.monotonic);
        const ghost = bc.ghostHits();
        try w.print(" hits={d} ({d:.1}%) misses={d} ({d:.1}%) " ++
            "ghost_hits={d} ({d:.1}% of misses)\n", .{
            hits,   pct(hits, hits + misses),
            misses, pct(misses, hits + misses),
            ghost,  pct(ghost, misses),
        });
    }
    if (self.store.chunkCache()) |cc| {
        if (!cc.enabled()) {
            try w.print("  chunk_cache: off\n", .{});
        } else {
            const st = cc.stats();
            try w.print("  chunk_cache: slots={d} used={d} chunk=", .{
                st.slots, st.used,
            });
            try writeBytes(w, st.chunk_bytes);
            try w.print(" hits={d} ({d:.1}%) misses={d} puts={d} " ++
                "drops={d} corrupt={d}", .{
                st.hits,   pct(st.hits, st.hits + st.misses),
                st.misses, st.puts,
                st.drops,  st.corrupt,
            });
            // Wastage: pages fetched and never read, over retired slots.
            try w.print(" unread={d}/{d} pages ({d:.1}%) chunks={d}\n", .{
                st.unread_pages,                        st.evicted_pages,
                pct(st.unread_pages, st.evicted_pages), st.unread_chunks,
            });
        }
    }
    // What the open tables pin besides the cache (`Table.resident`).
    const res = &_table.table.resident;
    const index = res.index.load(.monotonic);
    const filter = res.filter.load(.monotonic);
    const egids = res.egids.load(.monotonic);
    try w.print("  tables: open={d} resident=", .{
        res.tables.load(.monotonic),
    });
    try writeBytes(w, index + filter + egids);
    try w.print(" (index ", .{});
    try writeBytes(w, index);
    try w.print(", filters ", .{});
    try writeBytes(w, filter);
    try w.print(", egid bitmaps ", .{});
    try writeBytes(w, egids);
    try w.print(")\n", .{});

    // Per-level aggregate (sum + max-with-tree).
    const max_levels = self.live.cur().max_levels;
    // Trees are named `<ns>:<id>`, the form `zdb tree=` takes.
    const no_tree: TreeKey = .{ .namespace = 0, .tree_id = 0 };
    const Agg = struct {
        tables_total: u64 = 0,
        tables_max: u64 = 0,
        tables_max_tree: TreeKey = no_tree,
        bytes_total: u64 = 0,
        bytes_max: u64 = 0,
        bytes_max_tree: TreeKey = no_tree,
    };
    const aggs = try self.allocator.alloc(Agg, max_levels);
    defer self.allocator.free(aggs);
    for (aggs) |*a| a.* = .{};

    // Per-level "compactions wanted" rollup.
    const Wanted = struct {
        count: u64 = 0,
        score_max: f64 = 0,
        score_max_tree: TreeKey = no_tree,
    };
    const wanted = try self.allocator.alloc(Wanted, max_levels);
    defer self.allocator.free(wanted);
    for (wanted) |*x| x.* = .{};

    // Largest tree (sum across levels).
    var largest_tree: TreeKey = no_tree;
    var largest_tree_size: i64 = 0;
    var tree_count: usize = 0;

    // Walk under the map lock, each table's lock shared while its list
    // is read, but only briefly: collect the numbers we need, release.
    // Each level access takes its own lock inside numTables/getTotalSize.
    {
        var tables_it = self.tables.iterator();
        defer tables_it.deinit();
        while (tables_it.next()) |kv| {
            const trees = kv.value_ptr.*;
            const held = trees.mu.hold(.shared, @src());
            defer held.release();
            for (trees.list(held)) |tree| {
                tree_count += 1;
                const tree_key: TreeKey = .{
                    .namespace = tree.namespace,
                    .tree_id = tree.tree_id,
                };

                const th = tree.mu.hold(.shared, @src());
                defer th.release();
                var tree_total: i64 = 0;
                for (tree.levels, 0..) |*lv, idx| {
                    if (idx >= max_levels) break;
                    const tcount: u64 = @intCast(lv.numTables(th));
                    const tbytes: u64 = @intCast(lv.getTotalSize(th));
                    tree_total += @intCast(tbytes);

                    aggs[idx].tables_total += tcount;
                    if (tcount > aggs[idx].tables_max) {
                        aggs[idx].tables_max = tcount;
                        aggs[idx].tables_max_tree = tree_key;
                    }
                    aggs[idx].bytes_total += tbytes;
                    if (tbytes > aggs[idx].bytes_max) {
                        aggs[idx].bytes_max = tbytes;
                        aggs[idx].bytes_max_tree = tree_key;
                    }
                }
                if (tree_total > largest_tree_size) {
                    largest_tree_size = tree_total;
                    largest_tree = tree_key;
                }

                if (tree.compact_candidate) |c| {
                    if (c.level < max_levels) {
                        wanted[c.level].count += 1;
                        if (c.score > wanted[c.level].score_max) {
                            wanted[c.level].score_max = c.score;
                            wanted[c.level].score_max_tree = tree_key;
                        }
                    }
                }
            }
        }
    }

    try w.print("  trees: {d}\n", .{tree_count});
    try w.print("  largest_tree: id={d}:{d} size=", .{
        largest_tree.namespace, largest_tree.tree_id,
    });
    try writeBytes(w, @intCast(largest_tree_size));
    try w.print("\n\n", .{});

    try w.print("  level summary (across all trees):\n", .{});
    for (aggs, 0..) |a, idx| {
        // Skip levels that have nothing — shows max_levels=7 dumps cleanly.
        if (a.tables_total == 0 and a.bytes_total == 0) continue;
        const tm = a.tables_max_tree;
        try w.print(
            "    L{d}: tables={d} (max={d} in tree {d}:{d})  bytes=",
            .{ idx, a.tables_total, a.tables_max, tm.namespace, tm.tree_id },
        );
        try writeBytes(w, a.bytes_total);
        try w.print(" (max=", .{});
        try writeBytes(w, a.bytes_max);
        try w.print(" in tree {d}:{d})\n", .{
            a.bytes_max_tree.namespace, a.bytes_max_tree.tree_id,
        });
    }

    var any_wanted = false;
    for (wanted) |x| if (x.count > 0) {
        any_wanted = true;
        break;
    };
    if (any_wanted) {
        try w.print("\n  compactions wanted:\n", .{});
        for (wanted, 0..) |x, idx| {
            if (x.count == 0) continue;
            const smax = x.score_max_tree;
            try w.print(
                "    L{d}: trees={d} score_max={d:.2} (tree {d}:{d})\n",
                .{ idx, x.count, x.score_max, smax.namespace, smax.tree_id },
            );
        }
    }
}

pub fn writeTreeStats(self: *Forest, w: *std.Io.Writer, tk: TreeKey) !void {
    const tree = self.getTree(tk.namespace, tk.tree_id) orelse {
        try w.print(
            "tree id={d}:{d}: not found\n",
            .{ tk.namespace, tk.tree_id },
        );
        return;
    };
    defer tree.release();

    // Numbers first, under the lock; printing after — the writer may be
    // a socket, and a slow client must not hold up installs into this
    // tree.
    // Per level: tables, bytes on disk and before compression (each
    // table's index records both), so a tree's ratio is visible.
    const LevelStat = struct { tables: usize, bytes: u64, raw: u64 };
    const per_level = try self.allocator.alloc(LevelStat, tree.levels.len);
    defer self.allocator.free(per_level);
    var pinned: _table.table.Resident = .{};
    const total: i64, const candidate: ?_tree.CompactCandidate = blk: {
        const held = tree.mu.hold(.shared, @src());
        defer held.release();
        for (tree.levels, per_level) |*lv, *st| {
            var raw: u64 = 0;
            for (lv.entries(held)) |e| {
                raw += e.table.index.uncompressed_size;
                pinned.index += e.table.resident.index;
                pinned.filter += e.table.resident.filter;
                pinned.egids += e.table.resident.egids;
            }
            st.* = .{
                .tables = lv.numTables(held),
                .bytes = @intCast(lv.getTotalSize(held)),
                .raw = raw,
            };
        }
        break :blk .{ tree.totalSize(held), tree.compact_candidate };
    };

    try w.print(
        "tree id={d}:{d} size=",
        .{ tk.namespace, tk.tree_id },
    );
    try writeBytes(w, @intCast(total));
    try w.print("\n", .{});

    for (per_level, 0..) |st, idx| {
        if (st.tables == 0 and st.bytes == 0) continue;
        try w.print("  L{d}: {d} tables, ", .{ idx, st.tables });
        try writeBytes(w, st.bytes);
        try w.print(" (raw ", .{});
        try writeBytes(w, st.raw);
        const ratio = @as(f64, @floatFromInt(st.raw)) /
            @as(f64, @floatFromInt(@max(st.bytes, 1)));
        try w.print(", {d:.1}x)\n", .{ratio});
    }
    try w.print("  resident: ", .{});
    try writeBytes(w, pinned.index + pinned.filter + pinned.egids);
    try w.print(" (index ", .{});
    try writeBytes(w, pinned.index);
    try w.print(", filters ", .{});
    try writeBytes(w, pinned.filter);
    try w.print(", egid bitmaps ", .{});
    try writeBytes(w, pinned.egids);
    try w.print(")\n", .{});
    if (candidate) |c| {
        try w.print(
            "  compact_candidate: L{d} {t} score={d:.2}\n",
            .{ c.level, c.job, c.score },
        );
    }
}

pub fn writeNamespaceStats(self: *Forest, w: *std.Io.Writer, ns_id: u64) !void {
    try w.print("namespace={d}:\n", .{ns_id});
    var any = false;
    // Tree keys come out under the manifest lock (the map they live in
    // is the apply loop's to rehash); writeTreeStats then takes each
    // tree's own lock.
    const keys = try self.manifest.treeKeys(self.allocator, ns_id);
    defer self.allocator.free(keys);
    for (keys) |tk| {
        any = true;
        try writeTreeStats(self, w, tk);
    }
    if (!any) try w.print("  (no trees)\n", .{});
}

/// Pretty-print a byte count: KiB / MiB / GiB / TiB. Two decimal places
/// of precision past the smallest meaningful unit.
/// `part` of `whole` in percent; 0 for an empty whole.
fn pct(part: u64, whole: u64) f64 {
    if (whole == 0) return 0;
    return @as(f64, @floatFromInt(part)) * 100.0 /
        @as(f64, @floatFromInt(whole));
}

pub fn writeBytes(w: *std.Io.Writer, b: u64) !void {
    const kib: u64 = 1024;
    const mib: u64 = 1024 * kib;
    const gib: u64 = 1024 * mib;
    const tib: u64 = 1024 * gib;
    if (b >= tib) {
        try w.print(
            "{d:.2}TiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(tib))},
        );
    } else if (b >= gib) {
        try w.print(
            "{d:.2}GiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(gib))},
        );
    } else if (b >= mib) {
        try w.print(
            "{d:.2}MiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(mib))},
        );
    } else if (b >= kib) {
        try w.print(
            "{d:.2}KiB",
            .{@as(f64, @floatFromInt(b)) / @as(f64, @floatFromInt(kib))},
        );
    } else {
        try w.print("{d}B", .{b});
    }
}
