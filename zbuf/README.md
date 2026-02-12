# Zbuf Module

This module implements a small, FlatBuffers-style wire format and a Zig
build-time generator that emits zero-copy readers and forward-writing
writers from Zig schema definitions.

## Generator

- Generator entrypoint: `zbuf/gen.zig`
- Schema DSL: `zbuf/schema.zig`
- Example test schema: `zbuf/buffers.zig`

Generated code includes:

- `XReader` (reader)
- `XWriter` (writer)
- `addTail()` / `parseTail()` helpers
- `parseMessageFromTail()` which returns the correct root reader based on the tail

## Build Commands

Generate code for a schema file:

```
zig build zbuf -- path/to/buffers.zig
```

This writes `path/to/buffers.gen.zig` and prints the generated path.

Run zbuf tests:

```
zig build test-zbuf
```

## Example Flow

```
var buf: std.ArrayList(u8) = .{};
var root = try gen.TableWriter.initWithBuf(alloc, &buf);
// set fields and finish
const table_finish = try root.finish();
const table_off = table_finish.offset;
try gen.addTail(alloc, &buf, gen.TableReader, table_off);

const msg = try gen.parseMessageFromTail(buf.items);
const view = switch (msg) {
    .Table => |v| v,
    else => unreachable,
};
```

## Table Layout

Each table begins with a fixed header:

- `u16 version`
- `u16 size_bytes` (bytes of the fixed header for this version)

### Table and Field IDs

Each table has a **table id** (set in the schema’s `tables` list). It is used
in the message tail to identify the root table type, but is not stored inside
the table itself.

Each field has a **stable field id** (the `.id` in the schema). Field names
may be renamed or reordered in the source without changing the wire layout.
The generator sorts fields by `id` in ascending order to determine the slot
order in the fixed header. New fields may only be appended with new ids, and
readers use `size_bytes` to skip unknown slots.

Supported field encodings:

- Scalars (`u32`, `u64`) stored inline in the fixed header.
- Bytes (`[]u8`) stored as `{ u32 off, u32 len }`.
- Arrays of tables (`[]T`) stored as `{ u32 index_off, u32 count }` where
  `index_off` points to a contiguous `u32[count]` of child offsets.
- Direct table fields (`T`) stored as `u32 off` pointing at the child table.
  A null table is encoded as `zbuf.NullOffset` (`0xFFFF_FFFF`).

All offsets are **relative to payload base**.

## Layout Overview

A message consists of a payload and an optional fixed-length tail. All table offsets
are **relative to the payload base** (the start of the payload), so adding the
tail does not change any internal offsets. The tail is optional; it is only
needed when the receiver does not already know the root table type and root
offset.

Message tail (optional, 16 bytes at end of buffer):

- `u32 table_off` = offset of root table from payload base
- `u16 table_id` = unique id for the root table
- `u16 table_version` = version of the root table schema
- `u32 reserved` = 0
- `u32 magic` = `0x4655425A` ("ZBUF")

Offsets in tables are relative to the start of the payload (0x0000).

### Example (Table) with Concrete Fields

Assume the test schema in `zbuf/buffers.zig` and a payload with:

```zig
pub const Cell = schema.Table(.{
    .{ .id = 1, .name = "tag", .ty = schema.U32 },
    .{ .id = 2, .name = "data", .ty = schema.Bytes },
});

pub const Row = schema.Table(.{
    .{ .id = 1, .name = "name", .ty = schema.Bytes },
    .{ .id = 2, .name = "value", .ty = schema.U64 },
    .{ .id = 3, .name = "cells", .ty = schema.Array(Cell) },
    .{ .id = 4, .name = "row_cell", .ty = Cell },
});

pub const Table = schema.Table(.{
    .{ .id = 1, .name = "rows", .ty = schema.Array(Row) },
    .{ .id = 2, .name = "payload", .ty = schema.Bytes },
    .{ .id = 3, .name = "count", .ty = schema.U32 },
    .{ .id = 4, .name = "table_row", .ty = Row },
});
```

Table header sizes for this schema:

- `Table` fixed header = `28` bytes
  - 2 bytes for `version`
	- 2 bytes for `size_bytes`
	- 4 bytes for `rows_off`
	- 4 bytes for `rows_count`
	- 4 bytes for `payload_off`
	- 4 bytes for `payload_len`
	- 4 bytes for `count`
	- 4 bytes for `table_row_off`
- `Row` fixed header = `32` bytes
	- 2 bytes for `version`
	- 2 bytes for `size_bytes`
	- 4 bytes for `name_off`
	- 4 bytes for `name_len`
	- 8 bytes for `value`
	- 4 bytes for `cells_off`
	- 4 bytes for `cells_count`
	- 4 bytes for `row_cell_off`
- `Cell` fixed header = `16` bytes
	- 2 bytes for `version`
	- 2 bytes for `size_bytes`
	- 4 bytes for `tag`
	- 4 bytes for `data_off`
	- 4 bytes for `data_len`

One possible payload layout (offsets from payload base `0x0000`), matching a
common build order (Cell -> Row -> Table):

```
0x0000  Cell header (0x10 bytes)
0x0010  Cell.data bytes "DATA"
0x0014  Row header (0x20 bytes)
0x0034  Row.name bytes "NAME"
0x0038  Row.cells index table (u32[1] = 0x0000)
0x003C  Table header (0x1C bytes)
0x0058  Table.rows index table (u32[1] = 0x0014)
0x005C  Table.payload bytes "PAYL"
```

Concrete header contents (little-endian), showing offsets/lengths:

```
Cell @0x0000
  u16 version      = 1
  u16 size_bytes   = 0x0010
  u32 tag          = 7
  u32 data_off     = 0x0010
  u32 data_len     = 4

Row @0x0014
  u16 version      = 1
  u16 size_bytes   = 0x0020
  u32 name_off     = 0x0034
  u32 name_len     = 4
  u64 value        = 42
  u32 cells_off   = 0x0038
  u32 cells_count = 1
  u32 row_cell_off= 0x0000

Table @0x003C
  u16 version      = 1
  u16 size_bytes   = 0x001C
  u32 rows_off    = 0x0058
  u32 rows_count  = 1
  u32 payload_off  = 0x005C
  u32 payload_len  = 4
  u32 count        = 1
  u32 table_row_off= 0x0014
```

If you choose to add a tail, it is appended after the payload:

```
tail (optional, 16 bytes)
┌────────────────────────────────────────────────────────────┐
│ table_off = 0x003C | table_id = Table | table_ver = 1 | ZBUF │
└────────────────────────────────────────────────────────────┘
```
