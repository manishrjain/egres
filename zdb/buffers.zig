// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const schema = @import("schema");

// Port of badger/fb/flatbuffer.fbs:
// - table BlockOffset { key:[ubyte]; offset:uint; len:uint; }
// - table TableIndex { offsets:[BlockOffset]; bloom_filter:[ubyte];
//   max_version:uint64; ... }
pub const BlockIndex = schema.Table(&.{
    .{ .id = 1, .name = "key", .ty = schema.Bytes },
    .{ .id = 2, .name = "offset", .ty = schema.U32 },
    .{ .id = 3, .name = "len", .ty = schema.U32 },
});

pub const BinaryFuseFilter = schema.Table(&.{
    .{ .id = 1, .name = "seed", .ty = schema.U64 },
    .{ .id = 2, .name = "segment_length", .ty = schema.U32 },
    .{ .id = 3, .name = "segment_length_mask", .ty = schema.U32 },
    .{ .id = 4, .name = "segment_count", .ty = schema.U32 },
    .{ .id = 5, .name = "segment_count_length", .ty = schema.U32 },
    .{ .id = 6, .name = "fingerprints", .ty = schema.Array(schema.U16) },
});

pub const TableIndex = schema.Table(&.{
    // Bumped whenever the per-entry block encoding changes (table/builder.zig
    // is the source of truth for that layout).
    .{ .id = 1, .name = "format_version", .ty = schema.U32 },
    .{ .id = 2, .name = "max_version", .ty = schema.U64 },
    .{ .id = 3, .name = "key_count", .ty = schema.U32 },
    .{ .id = 4, .name = "uncompressed_size", .ty = schema.U32 },
    .{ .id = 5, .name = "compressed_size", .ty = schema.U32 },
    .{ .id = 6, .name = "stale_data_size", .ty = schema.U32 },
    .{ .id = 7, .name = "last_key", .ty = schema.Bytes },
    .{ .id = 8, .name = "blocks", .ty = schema.Array(BlockIndex) },
    .{ .id = 9, .name = "binary_fuse", .ty = BinaryFuseFilter },
    // The filter of an `egid` tree's table: a roaring bitmap
    // (zroar buffer) of every key in the table. Exact, so a miss is
    // certain and a hit is certain. Absent on a `bytes` tree's table,
    // which carries `binary_fuse` instead.
    .{ .id = 10, .name = "egid_bitmap", .ty = schema.Bytes },
});

pub const Op = schema.EnumDef(&.{
    .{ .name = "create", .tag = 0 },
    .{ .name = "delete", .tag = 1 },
    .{ .name = "update", .tag = 2 },
});

// TreeChange is used to create a new tree, delete a tree or update a tree's
// range. `tree_id` is namespace-local and packs (table_id << 32) | split_id, so
// the tree's SQL table is derived from it (Key.treeTableId) and never
// stored separately. `namespace_id` + `tree_id` together are the tree's global
// identity (Key.TreeKey).
//
// `name` names the object a CREATE introduces: the namespace's name on an
// ns_create (its tree_id-0 change, live and in snapshots), a table's name on
// a create_tree (future). Empty everywhere else (splits, drops, legacy admin
// namespaces).
/// What a tree's primary keys are. The builder picks the SST filter by
/// it: an `egid` tree's keys are 8-byte big-endian row ids, kept exactly
/// in a roaring bitmap; a `bytes` tree's keys go through a hash into a
/// binary fuse filter.
// What a tree holds, fixed at create. The key encoding follows from
// it: rows are keyed by egid, both index kinds by the indexed bytes.
// Only a posting index holds roaring states and deltas. Tag 0, `none`,
// is a change that names no kind: the unset default, which the manifest
// refuses at create, and a branch's carrier, whose trees take the
// source's kinds.
pub const TreeKind = schema.EnumDef(&.{
    .{ .name = "none", .tag = 0 },
    .{ .name = "rows", .tag = 1 },
    .{ .name = "unique_index", .tag = 2 },
    .{ .name = "posting_index", .tag = 3 },
});

pub const TreeChange = schema.Table(&.{
    // 0=CREATE, 1=DELETE, 2=UPDATE
    .{ .id = 1, .name = "op", .ty = Op },
    .{ .id = 2, .name = "namespace_id", .ty = schema.U64 },
    .{ .id = 3, .name = "tree_id", .ty = schema.U64 },
    .{ .id = 4, .name = "left_key", .ty = schema.Bytes },
    .{ .id = 5, .name = "right_key", .ty = schema.Bytes },
    .{ .id = 6, .name = "name", .ty = schema.Bytes },
    // create only: fixed for the tree's life, inherited by a split.
    .{ .id = 7, .name = "kind", .ty = TreeKind },
    // branch_table only: the table whose trees this table lists again.
    .{ .id = 8, .name = "from_table_id", .ty = schema.U32 },
});

// SSTableChange records one SST file (`sst_id`) entering or leaving a tree at a
// level. `sst_id` is the on-disk SST identifier (Manifest.newFileId), NOT a SQL
// table id — the SQL table is the tree, derivable from tree_id. One tree holds
// many SSTs; one SST can belong to many trees (shared after a split/clone,
// freed when its refcount hits 0). `namespace_id` + `tree_id` locate the owning
// tree — a single flush spans many namespaces, so the namespace can't be
// implied by the batch.
pub const SSTableChange = schema.Table(&.{
    .{ .id = 1, .name = "op", .ty = Op }, // 0=CREATE, 1=DELETE
    .{ .id = 2, .name = "level", .ty = schema.U32 }, // 0 for DELETE
    .{ .id = 3, .name = "sst_id", .ty = schema.U64 }, // on-disk SST id
    .{ .id = 4, .name = "tree_id", .ty = schema.U64 },
    .{ .id = 5, .name = "namespace_id", .ty = schema.U64 },
    // CRC32 of the SST file (CREATE only). Reserved: nothing sets it yet.
    .{ .id = 6, .name = "checksum", .ty = schema.U32 },
    // File size in bytes and where its index starts (CREATE only): with
    // both, opening the table is one read of [index_offset, size) —
    // no stat, no trailer round trip against the remote.
    .{ .id = 7, .name = "size", .ty = schema.U64 },
    .{ .id = 8, .name = "index_offset", .ty = schema.U64 },
    // log index of the entry that first listed this SST; 0 on a live
    // proposal (the apply fills it from its own entry), set in a snapshot
    // so restore recovers L0 order.
    .{ .id = 9, .name = "raft_index", .ty = schema.U64 },
});

// Per-namespace manifest state, persisted ONLY inside a snapshot (a
// serialized Manifest): the next-free table_id high-water-mark (so a
// re-elected leader never re-issues an id even after a DROP) and the
// database name (SQL CREATE DATABASE; empty for unnamed legacy namespaces).
// Live (non-snapshot) ManifestChanges leave `namespaces` empty.
pub const Namespace = schema.Table(&.{
    .{ .id = 1, .name = "id", .ty = schema.U64 },
    .{ .id = 2, .name = "next_table_id", .ty = schema.U32 },
    .{ .id = 3, .name = "name", .ty = schema.Bytes },
});

// Which kind of change a ManifestChange carries. `none` (0) is the zero value
// left by the snapshot path (Manifest.serialize sets only the state fields);
// it is never dispatched. DDL commands are verb-first and grouped as
// consecutive create/drop pairs by object.
pub const Command = schema.EnumDef(&.{
    .{ .name = "none", .tag = 0 },
    .{ .name = "new_memtable", .tag = 1 },
    .{ .name = "flush", .tag = 2 },
    .{ .name = "compaction", .tag = 3 },
    .{ .name = "tree_split", .tag = 4 },
    .{ .name = "create_ns", .tag = 5 },
    .{ .name = "drop_ns", .tag = 6 },
    .{ .name = "create_tree", .tag = 7 },
    .{ .name = "drop_tree", .tag = 8 },
    // CREATE INDEX backfill install: CREATEs into the bottom level,
    // valid only while that level is empty (Tree.applyBackfill).
    .{ .name = "index_backfill", .tag = 9 },
    // CREATE DATABASE dst TEMPLATE src: dst gets every tree of
    // `branch_from` listing the same SST files (shared, refcounted) and
    // the versions still in memtables, as of the entry. `tree_changes`
    // holds dst's tree-0 change alone, for its id and name; the rest is
    // derived from the manifest at apply, the same on every node
    // (Manifest.applyBranch).
    .{ .name = "branch_ns", .tag = 10 },
    // CREATE TABLE dst AS TABLE src: one create change per new table
    // (the base table and each of its indexes), carrying its name and
    // `from_table_id`; each lists every tree of its source under its
    // own id, the files shared, and takes the source's versions still
    // in memtables, as of the entry (Manifest.applyTableBranch).
    .{ .name = "branch_table", .tag = 11 },
});

pub const ManifestChange = schema.Table(&.{
    .{ .id = 1, .name = "tree_changes", .ty = schema.Array(TreeChange) },
    .{ .id = 2, .name = "sstable_changes", .ty = schema.Array(SSTableChange) },
    .{ .id = 3, .name = "cmd", .ty = Command },
    // for flush: which memtable was flushed
    .{ .id = 4, .name = "memtable_id", .ty = schema.U64 },
    // snapshot only
    .{ .id = 5, .name = "namespaces", .ty = schema.Array(Namespace) },
    // snapshot only: global namespace counter
    .{ .id = 6, .name = "next_ns_id", .ty = schema.U64 },
    // branch_ns only: the namespace copied.
    .{ .id = 7, .name = "branch_from", .ty = schema.U64 },
    // Rows a DDL entry writes with its tree change, so the two commit
    // as one: catalog rows today. Only create_ns, drop_ns,
    // create_tree, drop_tree, branch_ns and branch_table carry them.
    // `rows.namespace` is the entry's trees' namespace or the system
    // namespace; rows take the entry's commit timestamp, as a .data
    // entry's do. Manifest.apply and the snapshot draft ignore them:
    // rows reach a snapshot as SSTs.
    .{ .id = 8, .name = "rows", .ty = Mutations },
});

/// A single key-value write. `key` is a marshaled zdb key
/// [ns:u64 BE][table_id:u32 BE][primary] — self-describing, so one
/// Mutations entry can span tables. It carries no timestamp: apply
/// stamps every write with the enclosing `LogEntry.timestamp`, the
/// commit HLC shared by the whole batch. `meta` is zdb.Meta packed into
/// a byte (see zdb/kv.zig); 0 is Meta.raw, so a writer that never sets
/// it produces a plain put.
pub const KeyValue = schema.Table(&.{
    .{ .id = 1, .name = "key", .ty = schema.Bytes },
    .{ .id = 2, .name = "value", .ty = schema.Bytes },
    .{ .id = 3, .name = "meta", .ty = schema.U8 },
});

/// The value layout for Meta{ .kind = .delta, .encoding = .roaring }:
/// egids to add to / remove from the posting bitmap, layered on the
/// versions below it (zdb/compaction.zig's VersionTracker owns the fold,
/// zdb/validate.zig the write-side gate).
pub const RoaringDelta = schema.Table(&.{
    .{ .id = 1, .name = "adds", .ty = schema.Array(schema.U64) },
    .{ .id = 2, .name = "removes", .ty = schema.Array(schema.U64) },
});

/// A committed write set proposed through raft as a single .data entry —
/// one entry, one commit HLC, atomic across every key/table it touches.
/// An entry never spans namespaces (a session is bound to one database),
/// so `namespace` sits at the top level as the authority; apply validates
/// each key's own namespace bytes against it.
pub const Mutations = schema.Table(&.{
    .{ .id = 1, .name = "namespace", .ty = schema.U64 },
    .{ .id = 2, .name = "kvs", .ty = schema.Array(KeyValue) },
});

// An SST block, as packed records; table/builder.zig writes them and
// table/table.zig's BlockIterator reads them. On disk a block is its
// envelope, `Block`: the crc (zbuf.hash) over the codec byte and the
// payload as stored, so a reader verifies a block before decoding it,
// then the codec (a `u/compress.zig` Codec tag) the payload is stored
// with. The payload, decoded, is a `BlockPayload`: the entries behind
// their table of end offsets, the last end the region's length. An
// entry: its key as an overlap with the block's base key plus the
// differing tail, its versions' fixed headers, newest first, then the
// versions' bytes, each header's `off` its start among them.
pub const BlockVersion = schema.Packed(&.{
    .{ .id = 1, .name = "ts", .ty = schema.U64 },
    .{ .id = 2, .name = "meta", .ty = schema.U8 },
    .{ .id = 3, .name = "off", .ty = schema.U32 },
});

pub const BlockEntry = schema.Packed(&.{
    .{ .id = 1, .name = "key_overlap", .ty = schema.U16 },
    .{ .id = 2, .name = "key_diff", .ty = schema.Bytes, .len = .u16 },
    .{ .id = 3, .name = "versions", .ty = schema.Array(BlockVersion) },
    .{ .id = 4, .name = "values", .ty = schema.Bytes, .len = .rest },
});

pub const BlockPayload = schema.Packed(&.{
    .{ .id = 1, .name = "entries", .ty = schema.Array(BlockEntry) },
});

pub const Block = schema.Packed(&.{
    .{ .id = 1, .name = "crc", .ty = schema.U64 },
    .{ .id = 2, .name = "codec", .ty = schema.U8 },
    .{ .id = 3, .name = "payload", .ty = schema.Bytes, .len = .rest },
});

// zbuf tail stores table_id as u16, so use a module namespace in high bits and
// OR local ids into low bits.
const ZDB_TABLE_NS: u16 = 0xB600;

fn tableId(comptime local_id: u16) u16 {
    return ZDB_TABLE_NS | local_id;
}

pub const records: []const schema.Named = &.{
    .{ .name = "BlockIndex", .ty = BlockIndex, .id = tableId(1) },
    .{ .name = "TableIndex", .ty = TableIndex, .id = tableId(2) },
    .{ .name = "BinaryFuseFilter", .ty = BinaryFuseFilter, .id = tableId(3) },

    .{ .name = "TreeChange", .ty = TreeChange, .id = tableId(4) },
    .{ .name = "SSTableChange", .ty = SSTableChange, .id = tableId(5) },
    .{ .name = "ManifestChange", .ty = ManifestChange, .id = tableId(6) },
    .{ .name = "KeyValue", .ty = KeyValue, .id = tableId(7) },
    .{ .name = "Mutations", .ty = Mutations, .id = tableId(8) },
    .{ .name = "Namespace", .ty = Namespace, .id = tableId(9) },
    .{ .name = "RoaringDelta", .ty = RoaringDelta, .id = tableId(10) },

    // Packed records: no message id.
    .{ .name = "BlockVersion", .ty = BlockVersion },
    .{ .name = "BlockEntry", .ty = BlockEntry },
    .{ .name = "BlockPayload", .ty = BlockPayload },
    .{ .name = "Block", .ty = Block },
};
