// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const utils = @import("utils");
const String = utils.String;

/// Version of the per-entry block encoding, stored in TableIndex.format_version
/// and checked when a table is opened. Bump this whenever the entry layout in
/// table/builder.zig changes — including a change to what the meta byte's bits
/// MEAN (kv.zig Meta), since that mis-decodes just as silently as a shifted
/// field would.
///
/// 0 is never legal: an SST written before the field existed reads back 0 (zbuf
/// readers return 0 for slots past size_bytes), and that's exactly the case we
/// need to reject.
///
/// 1: [u64 crc][u8 codec][payload], the crc over the codec byte and the
///    stored payload; the payload, decoded per the codec byte, is
///    [u32 entry_count][u32 entry_end × n][entries region]
///    (buffers.zig's Block, BlockPayload and BlockEntry)
///    entries region contains one entry per key in this format:
///    [u16 key_overlap][u16 key_diff_len][key diff][packed versions]
///    packed versions, newest first:
///    [u32 count][count × { u64 ts, u8 meta, u32 off }][bytes]
///
pub const format_version: u32 = 1;

/// Size of the SST file trailer: [u32 index_len][u64 index_hash], the last
/// bytes of the file. It is what locates the TableIndex, whose offset isn't
/// known until every block has been written — which is why this one header
/// trails rather than leading like a block's does. Both the writer
/// (builder.zig finish) and the reader (table.zig initFromHandle) size the
/// trailer from here; nothing may hardcode 12.
pub const trailer_len: usize = 4 + 8;

/// Where a table sits in its file, as the manifest records it: the
/// table's first byte, its size, and where within it its index
/// starts, so an open is one read of [index_offset, size) with no stat
/// and no trailer round trip. A flush packs many trees' tables into
/// one file, each whole, at its own offset; a compaction's table is
/// alone in its file at offset 0.
pub const Extent = struct {
    offset: u64 = 0,
    size: u64,
    index_offset: u64,
};

pub const Error = error{
    CorruptTable,
    OutOfBounds,
};

pub const BlockMeta = struct {
    offset: u32,
    len: u32,
};

pub fn readU16LE(buf: String, off: usize) Error!u16 {
    if (off + 2 > buf.len) return error.OutOfBounds;
    const bytes: *const [2]u8 = @ptrCast(buf[off .. off + 2].ptr);
    return std.mem.readInt(u16, bytes, .little);
}

pub fn readU32LE(buf: String, off: usize) Error!u32 {
    if (off + 4 > buf.len) return error.OutOfBounds;
    const bytes: *const [4]u8 = @ptrCast(buf[off .. off + 4].ptr);
    return std.mem.readInt(u32, bytes, .little);
}

pub fn readU64LE(buf: String, off: usize) Error!u64 {
    if (off + 8 > buf.len) return error.OutOfBounds;
    const bytes: *const [8]u8 = @ptrCast(buf[off .. off + 8].ptr);
    return std.mem.readInt(u64, bytes, .little);
}

pub fn writeU16LE(
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    value: u16,
) !void {
    var bytes: [2]u8 = undefined;
    std.mem.writeInt(u16, &bytes, value, .little);
    try out.appendSlice(allocator, bytes[0..]);
}

pub fn writeU32LE(
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    value: u32,
) !void {
    var bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &bytes, value, .little);
    try out.appendSlice(allocator, bytes[0..]);
}

pub fn writeU64LE(
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
    value: u64,
) !void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    try out.appendSlice(allocator, bytes[0..]);
}
