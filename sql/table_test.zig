// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const zdb = @import("zdb");
const createTree = @import("../zdb/testing.zig").createTree;
const flags = @import("flags");
const sql = @import("sql");
const utils = @import("utils");

const vfs = zdb.vfs;
const Forest = zdb.forest.Forest;
const Config = zdb.config.Config;
const Key = zdb.kv.Key;
const Item = zdb.kv.Item;

const catalog = sql.catalog;
const schema = sql.schema;
const row = sql.row;
const Type = sql.Type;

fn tmpPath(alloc: std.mem.Allocator, tmp: *std.testing.TmpDir) ![:0]u8 {
    return tmp.dir.realPathFileAlloc(utils.context.g().io, ".", alloc);
}

// Write one catalog row per column into namespace `ns`'s catalog tree, keyed
// (table_id, column_id) BE, then reconstruct the schema via loadTableSchema.
test "sql.table: load reconstructs a user table's schema from catalog rows" {
    const alloc = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{});

    defer f_live.destroy();

    var store = try vfs.RemoteStore.initFile(alloc, dir_path, vfs.g());
    defer store.deinit();
    const f = try Forest.open(alloc, f_live, &store);
    defer f.close();

    const ns: u64 = 16;
    const user_table_id: u32 = 16;
    const pkey_index_id: u32 = 17;
    _ = try createTree(f, .{
        .namespace = ns,
        .table_id = catalog.table_id,
    }, .{}, .posting_index);
    _ = try createTree(f, .{
        .namespace = ns,
        .table_id = catalog.index_table_id,
    }, .{}, .posting_index);

    var catalog_ps = try schema.compile(
        alloc,
        catalog.logical_schema,
        catalog.logical_schema.current_version,
    );
    defer catalog_ps.deinit(alloc);
    var index_ps = try schema.compile(alloc, catalog.index_logical_schema, catalog.index_logical_schema.current_version);
    defer index_ps.deinit(alloc);

    // Synthetic table: (id int4 PRIMARY KEY, name text).
    const cols = [_]schema.LogicalColumn{
        .{ .column_id = 1, .name = "id", .ty = .int4, .pkey_position = 1 },
        .{ .column_id = 2, .name = "name", .ty = .text },
    };

    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();

    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    var items: std.ArrayList(Item) = .empty;
    defer items.deinit(alloc);
    for (cols) |c| {
        const cells = catalog.encodeColumn(user_table_id, c);
        const bytes = try row.marshalRow(alloc, &catalog_ps, &cells, &buf);
        const key = catalog.encodeKey(user_table_id, c.column_id);
        try items.append(alloc, .{
            .key = .{
                .namespace = ns,
                .table_id = catalog.table_id,
                .primary = try a.dupe(u8, &key),
            },
            .versions = .{ .one = .{
                .ts = 1,
                .meta = .{},
                .value = try a.dupe(u8, bytes),
            } },
        });
    }

    // One index row: a unique PK index on column 1, in its own tree.
    {
        const def = catalog.IndexDef{
            .base_table_id = user_table_id,
            .index_table_id = pkey_index_id,
            .column_id = 1,
            .unique = true,
            .valid_ts = 7,
        };
        const cells = catalog.encodeIndex(def);
        const bytes = try row.marshalRow(alloc, &index_ps, &cells, &buf);
        const ikey = catalog.encodeIndexKey(user_table_id, pkey_index_id);
        try items.append(alloc, .{
            .key = .{
                .namespace = ns,
                .table_id = catalog.index_table_id,
                .primary = try a.dupe(u8, &ikey),
            },
            .versions = .{ .one = .{
                .ts = 1,
                .meta = .{},
                .value = try a.dupe(u8, bytes),
            } },
        });
    }
    try f.put(items.items);

    var t = try sql.table.loadTableSchema(alloc, f, &catalog_ps, &index_ps, .{
        .namespace = ns,
        .table_id = user_table_id,
    });
    defer t.deinit();

    try testing.expectEqual(@as(usize, 2), t.columns.len);
    try testing.expectEqual(@as(u16, 1), t.version);

    // The PK index loads, and the pkey summary points at column 1's index.
    try testing.expectEqual(@as(usize, 1), t.indexes.len);
    try testing.expectEqual(pkey_index_id, t.indexes[0].index_table_id);
    try testing.expect(t.indexes[0].unique);
    try testing.expectEqual(@as(u64, 7), t.indexes[0].valid_ts);
    try testing.expect(!t.indexes[0].isBuilding());

    // scanIndexKeys classifies both directions: the index knows its
    // base; the base lists its index.
    var as_index = try sql.table.scanIndexKeys(alloc, f, .{
        .namespace = ns,
        .table_id = pkey_index_id,
    });
    defer as_index.table_ids.deinit(alloc);
    try testing.expect(as_index.is_index);
    try testing.expectEqual(user_table_id, as_index.base_table_id.?);
    var as_base = try sql.table.scanIndexKeys(alloc, f, .{
        .namespace = ns,
        .table_id = user_table_id,
    });
    defer as_base.table_ids.deinit(alloc);
    try testing.expect(!as_base.is_index);
    try testing.expectEqual(null, as_base.base_table_id);
    try testing.expectEqual(1, as_base.table_ids.items.len);
    try testing.expectEqual(pkey_index_id, as_base.table_ids.items[0]);
    // The index resolved its column to the live one — the PK column.
    try testing.expectEqual(&t.columns[0], t.indexes[0].column);
    try testing.expect(t.indexes[0].column.pkey_position != null);

    // Index.keys: a value yields its one encoded key; NULL yields none
    // under a unique index (NULLS DISTINCT).
    const one = try t.indexes[0].keys(alloc, .{ .int4 = 7 });
    defer alloc.free(one); // after the key it holds (defers run in reverse)
    defer alloc.free(one[0]);
    try testing.expectEqual(@as(usize, 1), one.len);
    const want = try sql.key.encodeIndexKey(alloc, .{ .int4 = 7 }, null);
    defer alloc.free(want);
    try testing.expectEqualSlices(u8, want, one[0]);
    const none = try t.indexes[0].keys(alloc, .null_value);
    try testing.expectEqual(@as(usize, 0), none.len);
    // A non-unique index keys NULL on the NULL tag byte.
    const posting = sql.table.Index{
        .index_table_id = 99,
        .unique = false,
        .valid_ts = 0,
        .column = &t.columns[1],
    };
    const tag = try posting.keys(alloc, .null_value);
    defer alloc.free(tag);
    defer alloc.free(tag[0]);
    const null_key = try sql.key.encodeIndexKey(alloc, .null_value, null);
    defer alloc.free(null_key);
    try testing.expectEqualSlices(u8, null_key, tag[0]);

    try testing.expectEqualStrings("id", t.columns[0].name);
    try testing.expectEqual(Type.int4, t.columns[0].ty);
    try testing.expectEqual(@as(?u16, 1), t.columns[0].pkey_position);

    try testing.expectEqualStrings("name", t.columns[1].name);
    try testing.expectEqual(Type.text, t.columns[1].ty);
    try testing.expectEqual(@as(?u16, null), t.columns[1].pkey_position);

    // Compiled layout: two live columns, one varlen (name).
    try testing.expectEqual(@as(usize, 2), t.ps.columns.len);
    try testing.expectEqual(@as(u16, 1), t.ps.n_varlen);

    // Name resolution: a pointer into `columns`, position included; the
    // egid pseudo-column resolves too but is not among `columns`.
    try testing.expectEqual(&t.columns[1], t.column("name").?);
    try testing.expectEqual(@as(usize, 1), t.column("name").?.pos);
    try testing.expect(t.column("missing") == null);
    try testing.expectEqual(&sql.table.egid_column, t.column("egid").?);
    try testing.expect(t.column("egid").?.isEgid());
    for (t.columns) |c| try testing.expect(!c.isEgid());
}

test "sql.table: load returns UndefinedTable when no catalog rows exist" {
    const alloc = testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const dir_path = try tmpPath(alloc, &tmp);
    defer alloc.free(dir_path);

    const f_live = try flags.Live(Config).create(alloc, .{});

    defer f_live.destroy();

    var store = try vfs.RemoteStore.initFile(alloc, dir_path, vfs.g());
    defer store.deinit();
    const f = try Forest.open(alloc, f_live, &store);
    defer f.close();

    const ns: u64 = 16;
    _ = try createTree(f, .{
        .namespace = ns,
        .table_id = catalog.table_id,
    }, .{}, .posting_index);

    var catalog_ps = try schema.compile(
        alloc,
        catalog.logical_schema,
        catalog.logical_schema.current_version,
    );
    defer catalog_ps.deinit(alloc);
    var index_ps = try schema.compile(alloc, catalog.index_logical_schema, catalog.index_logical_schema.current_version);
    defer index_ps.deinit(alloc);

    try testing.expectError(error.UndefinedTable, sql.table.loadTableSchema(
        alloc,
        f,
        &catalog_ps,
        &index_ps,
        .{ .namespace = ns, .table_id = 999 },
    ));
}
