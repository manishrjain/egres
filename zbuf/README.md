# Zbuf Module

This module implements a small, FlatBuffers-style wire format and a Zig
build-time generator that emits zero-copy readers and forward-writing
builders from Zig schema definitions.

## Layout Overview

A message ends with a fixed tail. All table offsets are **relative to the
payload base** (the start of the payload), so adding the tail does not change
any internal offsets.

Message tail (16 bytes, at end of buffer):

- `u32 root_off` = offset of root table from payload base
- `u16 table_id` = unique id for the root table
- `u16 table_version` = version of the root table schema
- `u32 reserved` = 0
- `u32 magic` = `0x4655425A` ("ZBUF")

ASCII layout (payload + tail):

```
0x0000  [ payload bytes... ]
...
0xNNNN  u32 root_off
0xNNNN  u16 table_id
0xNNNN  u16 table_version
0xNNNN  u32 reserved (0)
0xNNNN  u32 magic (0x4655425A)
```

Offsets in tables are relative to the start of the payload (0x0000).

## Table Layout

Each table begins with a fixed header:

- `u16 version`
- `u16 size_bytes` (bytes of the fixed header for this version)

Fields are laid out by **stable field id** in ascending order. New fields
may only be appended, and readers use `size_bytes` to skip unknown slots.

Supported field encodings:

- Scalars (`u32`, `u64`) stored inline in the fixed header.
- Bytes (`[]u8`) stored as `{ u32 off, u32 len }`.
- Arrays of tables (`[]T`) stored as `{ u32 index_off, u32 count }` where
  `index_off` points to a contiguous `u32[count]` of child offsets.

All offsets are **relative to payload base**.

## Generator

- Generator entrypoint: `zbuf/gen.zig`
- Schema DSL: `zbuf/schema.zig`
- Example test schema: `zbuf/buffers.zig`

Generated code includes:

- `XView` (reader)
- `XBuilder` (writer)
- `addTail()` / `parseTail()` helpers
- `parseMessage()` which returns the correct root view based on the tail

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
var root = try gen.RootBuilder.start(alloc, &buf);
// set fields and finish
const root_off = root.finish();
try gen.addTail(alloc, &buf, gen.RootView, root_off);

const msg = try gen.parseMessage(buf.items);
const view = switch (msg) {
    .Root => |v| v,
    else => unreachable,
};
```
