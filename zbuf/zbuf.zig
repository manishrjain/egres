const std = @import("std");

pub const Error = error{
    OutOfBounds,
    Invalid,
};

pub const Ownership = enum {
    borrowed,
    owned,
};

pub const NullOffset: u32 = 0xFFFF_FFFF;

pub const MessageTail = struct {
    table_id: u16,
};

// 2-byte magic ("ZB" little-endian) — enough to discriminate zbuf bytes from
// arbitrary noise without paying for a 4-byte signature on every message.
pub const message_magic: u16 = 0x425A;
// 4-byte tail: u16 table_id + u16 magic. The root table is required to live
// at payload offset 0. appendMessageTail accepts the caller's offset only to
// validate that invariant; the offset is not stored.
pub const message_tail_size: u16 = 4;
const payload_alignment: usize = 4;

pub fn appendMessageTail(
    list: *std.ArrayList(u8),
    alloc: std.mem.Allocator,
    table_id: u16,
    offset: u32,
) !void {
    if (offset != 0) return Error.Invalid;
    const start = list.items.len;
    try appendZeros(list, alloc, message_tail_size);
    try writeU16At(list.items, start + 0, table_id);
    try writeU16At(list.items, start + 2, message_magic);
}

pub fn readMessageTail(buf: []const u8) !MessageTail {
    if (buf.len < message_tail_size) return Error.OutOfBounds;
    const start = buf.len - message_tail_size;
    const magic = try readU16(buf, start + 2);
    if (magic != message_magic) return Error.Invalid;
    const table_id = try readU16(buf, start + 0);
    return .{ .table_id = table_id };
}

// The single hash function used for ALL framing and integrity checks in
// this project. Centralizing here means one decision, one comment, one
// place to swap implementations.
//
// Choice: std.hash.XxHash3 (Zig stdlib, AVX2-vectorized via @Vector(8, u64)).
// ~67 GiB/s on Zen 3 — ~5× faster than scalar SSE4.2 CRC32C, ~115× faster
// than software CRC. Full u64 output gives a 2⁻⁶⁴ false-negative rate on
// random/torn-write corruption (vs. 2⁻³² for a 32-bit CRC). CRC32C retains
// a theoretical edge on single-bit and small-burst errors (provably 100%
// vs. xxhash3's 1 − 2⁻⁶⁴) but the gap is ~10⁻¹⁰ — irrelevant at NVMe
// error rates.
pub fn hash(payload: []const u8) u64 {
    return std.hash.XxHash3.hash(0, payload);
}

/// 12-byte self-describing prefix for an arbitrary serialized payload:
/// `[u64 hash][u32 length][payload bytes]`. Lets the reader bound the
/// payload inside a larger fixed-size region (e.g. a zero-padded WAL
/// header) and verify it with one hash pass — no magic number / separate
/// length file needed.
///
/// All four items below (hdr_size, write, append, read) operate on the
/// same wire format; they're co-located so a future format tweak only
/// needs to touch one block.
pub const Frame = struct {
    pub const hdr_size: u16 = 12;

    /// In-place writer: place the [hash | len | payload] frame at the
    /// start of `dest`. `dest` must be at least `hdr_size + payload.len`
    /// bytes long. Bytes after the payload are left untouched, so callers
    /// using a fixed-size region should @memset the region first.
    ///
    /// Zero-copy fast path: if `payload` already aliases the destination
    /// slot (e.g. the caller wrapped `dest[hdr_size..]` as an ArrayList
    /// build buffer and wrote the payload into it directly), the copy is
    /// skipped. One API serves both "build elsewhere, copy in" and "build
    /// in place, just stamp the prefix" callers.
    pub fn write(dest: []u8, payload: []const u8) !void {
        if (payload.len > std.math.maxInt(u32)) return Error.OutOfBounds;
        if (dest.len < @as(usize, hdr_size) + payload.len) return Error.OutOfBounds;
        try writeU64At(dest, 0, hash(payload));
        try writeU32At(dest, 8, @intCast(payload.len));
        const slot = dest[hdr_size .. hdr_size + payload.len];
        if (payload.ptr != slot.ptr) @memcpy(slot, payload);
    }

    /// Streaming writer: append a framed payload onto an ArrayList.
    /// Mirrors `write` for callers that build their output incrementally.
    /// Returns the total number of bytes appended (prefix + payload).
    pub fn append(
        list: *std.ArrayList(u8),
        alloc: std.mem.Allocator,
        payload: []const u8,
    ) !usize {
        if (payload.len > std.math.maxInt(u32)) return Error.OutOfBounds;
        try list.ensureUnusedCapacity(alloc, hdr_size + payload.len);
        const start = list.items.len;
        const sz = hdr_size + payload.len;
        list.items.len += sz;
        try write(list.items[start..], payload);
        return sz;
    }

    /// Reads the frame prefix from `buf`, validates the encoded length fits
    /// within `buf`, and verifies the hash against the payload bytes.
    /// Returns a slice into `buf` covering just the payload (no copy).
    pub fn read(buf: []const u8) ![]const u8 {
        if (buf.len < hdr_size) return Error.OutOfBounds;
        const stored_hash = try readU64(buf, 0);
        const len = try readU32(buf, 8);
        const end = @as(usize, hdr_size) + @as(usize, len);
        if (end > buf.len) return Error.OutOfBounds;
        const payload = buf[hdr_size..end];
        if (hash(payload) != stored_hash) return Error.Invalid;
        return payload;
    }
};
pub fn readU8(buf: []const u8, off: usize) !u8 {
    if (off + 1 > buf.len) return Error.OutOfBounds;
    return buf[off];
}

pub fn readU16(buf: []const u8, off: usize) !u16 {
    if (off + 2 > buf.len) return Error.OutOfBounds;
    return readIntLittle(u16, buf, off);
}

pub fn readU32(buf: []const u8, off: usize) !u32 {
    if (off + 4 > buf.len) return Error.OutOfBounds;
    return readIntLittle(u32, buf, off);
}

pub fn readU64(buf: []const u8, off: usize) !u64 {
    if (off + 8 > buf.len) return Error.OutOfBounds;
    return readIntLittle(u64, buf, off);
}

pub fn writeU8At(buf: []u8, off: usize, v: u8) !void {
    if (off + 1 > buf.len) return Error.OutOfBounds;
    buf[off] = v;
}

pub fn writeU16At(buf: []u8, off: usize, v: u16) !void {
    if (off + 2 > buf.len) return Error.OutOfBounds;
    writeIntLittle(u16, buf, off, v);
}

pub fn writeU32At(buf: []u8, off: usize, v: u32) !void {
    if (off + 4 > buf.len) return Error.OutOfBounds;
    writeIntLittle(u32, buf, off, v);
}

pub fn writeU64At(buf: []u8, off: usize, v: u64) !void {
    if (off + 8 > buf.len) return Error.OutOfBounds;
    writeIntLittle(u64, buf, off, v);
}

pub fn alignForward(value: usize, alignment: usize) usize {
    return (value + alignment - 1) & ~(alignment - 1);
}

pub fn appendZeros(list: *std.ArrayList(u8), alloc: std.mem.Allocator, count: usize) !void {
    try list.ensureUnusedCapacity(alloc, count);
    const start = list.items.len;
    list.items.len += count;
    @memset(list.items[start..], 0);
}

pub fn reserveHeader(list: *std.ArrayList(u8), alloc: std.mem.Allocator, size_bytes: u16) !u32 {
    const base = @as(u32, @intCast(list.items.len));
    try appendZeros(list, alloc, size_bytes);
    return base;
}

pub fn appendBytes(list: *std.ArrayList(u8), alloc: std.mem.Allocator, data: []const u8) !u32 {
    const aligned = alignForward(list.items.len, payload_alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off = @as(u32, @intCast(list.items.len));
    try list.appendSlice(alloc, data);
    return off;
}

pub fn appendU32Slice(list: *std.ArrayList(u8), alloc: std.mem.Allocator, values: []const u32) !u32 {
    const aligned = alignForward(list.items.len, payload_alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off = @as(u32, @intCast(list.items.len));
    try list.ensureUnusedCapacity(alloc, values.len * 4);
    for (values) |v| {
        const start = list.items.len;
        list.items.len += 4;
        writeIntLittle(u32, list.items, start, v);
    }
    return off;
}

pub fn appendU8Slice(list: *std.ArrayList(u8), alloc: std.mem.Allocator, values: []const u8) !u32 {
    const aligned = alignForward(list.items.len, payload_alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off = @as(u32, @intCast(list.items.len));
    try list.appendSlice(alloc, values);
    return off;
}

pub fn appendU16Slice(list: *std.ArrayList(u8), alloc: std.mem.Allocator, values: []const u16) !u32 {
    const aligned = alignForward(list.items.len, payload_alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off = @as(u32, @intCast(list.items.len));
    try list.ensureUnusedCapacity(alloc, values.len * 2);
    for (values) |v| {
        const start = list.items.len;
        list.items.len += 2;
        writeIntLittle(u16, list.items, start, v);
    }
    return off;
}

pub fn appendU64Slice(list: *std.ArrayList(u8), alloc: std.mem.Allocator, values: []const u64) !u32 {
    const aligned = alignForward(list.items.len, payload_alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off = @as(u32, @intCast(list.items.len));
    try list.ensureUnusedCapacity(alloc, values.len * 8);
    for (values) |v| {
        const start = list.items.len;
        list.items.len += 8;
        writeIntLittle(u64, list.items, start, v);
    }
    return off;
}

pub fn sliceAt(buf: []const u8, off: u32, len: u32) ![]const u8 {
    const start: usize = off;
    const end: usize = start + len;
    if (end > buf.len) return Error.OutOfBounds;
    return buf[start..end];
}

pub fn patchU8(buf: []u8, off: u32, v: u8) !void {
    try writeU8At(buf, off, v);
}

pub fn patchU32(buf: []u8, off: u32, v: u32) !void {
    try writeU32At(buf, off, v);
}

pub fn patchU16(buf: []u8, off: u32, v: u16) !void {
    try writeU16At(buf, off, v);
}

pub fn patchU64(buf: []u8, off: u32, v: u64) !void {
    try writeU64At(buf, off, v);
}

fn readIntLittle(comptime T: type, buf: []const u8, off: usize) T {
    const size = @sizeOf(T);
    const slice = buf[off .. off + size];
    const ptr = @as(*const [size]u8, @ptrCast(slice.ptr));
    return std.mem.readInt(T, ptr, .little);
}

fn writeIntLittle(comptime T: type, buf: []u8, off: usize, v: T) void {
    const size = @sizeOf(T);
    const slice = buf[off .. off + size];
    const ptr = @as(*[size]u8, @ptrCast(slice.ptr));
    std.mem.writeInt(T, ptr, v, .little);
}
