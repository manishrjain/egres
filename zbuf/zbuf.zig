const std = @import("std");

pub const Error = error{
    OutOfBounds,
    Invalid,
};

pub const NullOffset: u32 = 0xFFFF_FFFF;

pub const MessageTail = struct {
    table_id: u16,
    table_version: u16,
    offset: u32, // offset from payload base
};

pub const message_magic: u32 = 0x4655425A; // "ZBUF" little-endian
pub const message_tail_size: u16 = 16;

pub fn appendMessageTail(list: *std.ArrayList(u8), alloc: std.mem.Allocator, table_id: u16, table_version: u16, offset: u32) !void {
    const start = list.items.len;
    try appendZeros(list, alloc, message_tail_size);
    try writeU32At(list.items, start + 0, offset);
    try writeU16At(list.items, start + 4, table_id);
    try writeU16At(list.items, start + 6, table_version);
    try writeU32At(list.items, start + 8, 0);
    try writeU32At(list.items, start + 12, message_magic);
}

pub fn readMessageTail(buf: []const u8) !MessageTail {
    if (buf.len < message_tail_size) return Error.OutOfBounds;
    const start = buf.len - message_tail_size;
    const magic = try readU32(buf, start + 12);
    if (magic != message_magic) return Error.Invalid;
    const offset = try readU32(buf, start + 0);
    const table_id = try readU16(buf, start + 4);
    const table_version = try readU16(buf, start + 6);
    return .{
        .table_id = table_id,
        .table_version = table_version,
        .offset = offset,
    };
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

pub fn appendBytesAligned(list: *std.ArrayList(u8), alloc: std.mem.Allocator, data: []const u8, alignment: usize) !u32 {
    const aligned = alignForward(list.items.len, alignment);
    if (aligned > list.items.len) {
        try appendZeros(list, alloc, aligned - list.items.len);
    }
    const off = @as(u32, @intCast(list.items.len));
    try list.appendSlice(alloc, data);
    return off;
}

pub fn appendU32SliceAligned(list: *std.ArrayList(u8), alloc: std.mem.Allocator, values: []const u32, alignment: usize) !u32 {
    const aligned = alignForward(list.items.len, alignment);
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

pub fn sliceAt(buf: []const u8, off: u32, len: u32) ![]const u8 {
    const start: usize = off;
    const end: usize = start + len;
    if (end > buf.len) return Error.OutOfBounds;
    return buf[start..end];
}

pub fn patchU32(buf: []u8, off: u32, v: u32) !void {
    try writeU32At(buf, off, v);
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
