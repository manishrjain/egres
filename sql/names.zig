// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Names as rows: table 3 in every namespace that has names.
//!
//! In a database, table 3 maps each table and index name to its
//! table_id; tables and indexes share one set of names, as in Postgres.
//! In the system namespace (zdb.manifest.system_namespace), table 3 maps
//! each database name to its namespace id. Key: the name bytes. Value:
//! the id, big-endian (4 bytes for a table, 8 for a database). Raw
//! key-value rows, not SQL rows, so a lookup needs no compiled schema.
//!
//! The rows are the only record of a name. A DDL writes and tombstones
//! them in the same raft entry as its tree change. Every lookup is a
//! point read at the node's latest applied timestamp, never at a
//! transaction's read timestamp: a forest.put makes an entry's rows
//! visible one at a time, and the applied timestamp hides a
//! half-applied entry. There is no cache.

const std = @import("std");
const zdb = @import("zdb");

const zen = zdb.buffers;
const Forest = zdb.forest.Forest;
const Key = zdb.kv.Key;
const Meta = zdb.kv.Meta;
const String = []const u8;

/// The names table's id, in every namespace.
pub const table_id = zdb.manifest.names_table_id;

pub const Error = error{NameExists};

/// The table_id `name` names in namespace `ns`, or null.
pub fn tableId(
    forest: *Forest,
    read_ts: u64,
    ns: u64,
    name: String,
) !?u32 {
    return readId(u32, forest, read_ts, ns, name);
}

/// The namespace id database `name` names, or null.
pub fn namespaceId(forest: *Forest, read_ts: u64, name: String) !?u64 {
    return readId(
        u64,
        forest,
        read_ts,
        zdb.manifest.system_namespace,
        name,
    );
}

/// Refuse any of `names` that already names something in namespace
/// `ns`, or appears twice in `names` (a CREATE TABLE's
/// generated `<t>_pkey` against a user's index name, say). Every create
/// calls this under ddl_mu, which with the per-term catch-up barrier
/// and waiting for apply is what keeps names unique. There is no length
/// limit: the SQL parser already cuts a typed name to 63 bytes, as
/// Postgres does, and a generated index name (`<t>_pkey`, `<t>_<c>_key`)
/// may be longer, as it always could be.
pub fn checkNew(
    forest: *Forest,
    read_ts: u64,
    ns: u64,
    names: []const String,
) !void {
    for (names, 0..) |name, i| {
        for (names[0..i]) |earlier| {
            if (std.mem.eql(u8, earlier, name)) return Error.NameExists;
        }
        if (try readValue(forest, read_ts, ns, name, &.{}) != null)
            return Error.NameExists;
    }
}

/// The row recording that `name` names table `id` in namespace `ns`.
pub fn tableNameKv(
    arloc: std.mem.Allocator,
    ns: u64,
    name: String,
    id: u32,
) !zen.KeyValueStruct {
    const value = try arloc.alloc(u8, 4);
    std.mem.writeInt(u32, value[0..4], id, .big);
    return .{ .key = try wireKey(arloc, ns, name), .value = value };
}

/// The row recording that database `name` is namespace `id`.
pub fn dbNameKv(
    arloc: std.mem.Allocator,
    name: String,
    id: u64,
) !zen.KeyValueStruct {
    const value = try arloc.alloc(u8, 8);
    std.mem.writeInt(u64, value[0..8], id, .big);
    return .{
        .key = try wireKey(arloc, zdb.manifest.system_namespace, name),
        .value = value,
    };
}

/// The tombstone removing `name` from namespace `ns`'s names: a table
/// or index name in a database, or a database name in the system
/// namespace.
pub fn tombstone(
    arloc: std.mem.Allocator,
    ns: u64,
    name: String,
) !zen.KeyValueStruct {
    return .{
        .key = try wireKey(arloc, ns, name),
        .value = "",
        .meta = (Meta{ .kind = .deleted }).toByte(),
    };
}

/// One tombstone per name in database `ns` whose row names one of
/// `ids`: what DROP TABLE removes for a table and its indexes. It reads
/// every name in the database, which is fine because DROP TABLE is rare
/// and a database holds few names, and it keeps each index's name in
/// this table alone.
pub fn tombstonesFor(
    arloc: std.mem.Allocator,
    forest: *Forest,
    read_ts: u64,
    ns: u64,
    ids: []const u32,
) ![]zen.KeyValueStruct {
    var cur = try forest.openCursor(
        arloc,
        .{ .namespace = ns, .table_id = table_id },
        zdb.kv.KeyRange.all,
        .{ .read_ts = read_ts },
    );
    defer cur.deinit();
    var out: std.ArrayList(zen.KeyValueStruct) = .empty;
    while (try cur.next()) |item| {
        const value = item.newest().value;
        // Every name in a database names a table_id.
        if (value.len != 4) return error.CorruptNameRow;
        const id = std.mem.readInt(u32, value[0..4], .big);
        if (std.mem.indexOfScalar(u32, ids, id) == null) continue;
        try out.append(arloc, try tombstone(arloc, ns, item.key.primary));
    }
    return out.toOwnedSlice(arloc);
}

fn wireKey(arloc: std.mem.Allocator, ns: u64, name: String) !String {
    return (Key{
        .namespace = ns,
        .table_id = table_id,
        .primary = name,
    }).marshal(arloc);
}

/// The id in `name`'s row of namespace `ns`, or null.
fn readId(
    comptime T: type,
    forest: *Forest,
    read_ts: u64,
    ns: u64,
    name: String,
) !?T {
    var buf: [@sizeOf(T)]u8 = undefined;
    const n = try readValue(forest, read_ts, ns, name, &buf) orelse
        return null;
    if (n != buf.len) return error.CorruptNameRow;
    return std.mem.readInt(T, &buf, .big);
}

/// Whether `name` has a row in namespace `ns`: its value's length, with
/// up to `buf.len` of its bytes copied into `buf`, or null.
fn readValue(
    forest: *Forest,
    read_ts: u64,
    ns: u64,
    name: String,
    buf: []u8,
) !?usize {
    if (name.len == 0) return null;
    // The item holds the key and one small value: the stack serves it.
    var sfa = std.heap.stackFallback(512, forest.allocator);
    const alloc = sfa.get();
    const found = try forest.get(alloc, .{
        .key = .{ .namespace = ns, .table_id = table_id, .primary = name },
        .read_ts = read_ts,
    }) orelse return null;
    defer found.free(alloc);
    const value = found.newest().value;
    const n = @min(value.len, buf.len);
    @memcpy(buf[0..n], value[0..n]);
    return value.len;
}
