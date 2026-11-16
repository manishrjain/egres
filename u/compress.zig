// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const String = @import("utils.zig").String;

// Shared compression codecs for Egres modules.
// - .none: passthrough
// - .zstd: zstd frame format
// - .lz4: [u32 raw_len][lz4 block]
pub const Codec = enum(u8) {
    none = 0,
    lz4 = 1,
    zstd = 2,
};

pub const Error = std.mem.Allocator.Error || error{
    CompressionFailed,
    DecompressionFailed,
    UnknownFrameSize,
    CorruptFrame,
};

const zstd = @cImport({
    @cInclude("zstd.h");
});
const lz4 = @cImport({
    @cInclude("lz4.h");
});
const shim = @cImport({
    @cInclude("u/compress_shim.h");
});

/// How `maybePack` compresses: under `codec`, an input of at least
/// `min_bytes`, and only if that saves at least `min_saving` of the
/// input's bytes, a fraction: 1.0 / 8.0 keeps a result an eighth
/// smaller or better.
pub const PackRule = struct {
    codec: Codec,
    min_bytes: usize,
    min_saving: f64,
};

/// What `maybePack` returns: the bytes to store, and the codec they
/// are in. Borrowed, not owned: `data` is the caller's `scratch` when
/// compressed and the caller's input when not.
pub const Packed = struct {
    data: String,
    codec: Codec,
};

/// Whether bytes that compressed from `raw` to `compressed` saved at
/// least `min_saving` of themselves, a fraction.
pub fn worthKeeping(raw: u64, compressed: u64, min_saving: f64) bool {
    const saved: f64 = @floatFromInt(raw -| compressed);
    const raw_f: f64 = @floatFromInt(raw);
    return saved >= raw_f * min_saving;
}

/// `input` compressed as `rule` says, when it says that is worth it,
/// else `input` itself under `.none`. Compressed, the result is in
/// `scratch` (emptied first) and valid until `scratch` is next used;
/// nothing is allocated beyond what `scratch` has to grow by, so a
/// caller that keeps its `scratch` allocates nothing. `maybeUnpack`
/// with the returned codec gives `input` back.
///
/// Conditional twice. A small input is not tried: there is little to
/// save. A larger one is kept compressed only when that saved enough,
/// so data that does not compress costs one attempt and is stored as
/// it came. A compressor failure is not an error either: the input is
/// returned.
pub fn maybePack(
    alloc: std.mem.Allocator,
    input: String,
    rule: PackRule,
    scratch: *std.ArrayList(u8),
) std.mem.Allocator.Error!Packed {
    if (rule.codec == .none or input.len < rule.min_bytes) return .{
        .data = input,
        .codec = .none,
    };

    scratch.items.len = 0;
    if (compress(alloc, rule.codec, input, scratch)) |packed_len| {
        if (worthKeeping(input.len, packed_len, rule.min_saving))
            return .{ .data = scratch.items, .codec = rule.codec };
        //
    } else |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        // Nothing usable was written.
        else => scratch.items.len = 0,
    }
    return .{ .data = input, .codec = .none };
}

/// The bytes `maybePack` was given, from what it returned. `codec_tag`
/// is the codec as a stored byte (`@intFromEnum`), which is how a
/// record on disk carries it. Stored under `.none`, the result is
/// `data` itself, with no copy; compressed, it is decoded into
/// `scratch` (emptied first) and valid until `scratch` is next used.
pub fn maybeUnpack(
    alloc: std.mem.Allocator,
    codec_tag: u8,
    data: String,
    scratch: *std.ArrayList(u8),
) (Error || error{UnknownCodec})!String {
    const codec = std.enums.fromInt(Codec, codec_tag) orelse
        return error.UnknownCodec;
    if (codec == .none) return data;

    scratch.items.len = 0;
    _ = try decompress(alloc, codec, data, scratch);
    return scratch.items;
}

/// Compresses input into out, appending the result. Returns the number of
/// bytes appended. Access the data via out.items[out.items.len - sz ..].
pub fn compress(
    alloc: std.mem.Allocator,
    codec: Codec,
    input: String,
    out: *std.ArrayList(u8),
) Error!usize {
    return switch (codec) {
        .none => noneInto(alloc, input, out),
        .zstd => compressZstdInto(alloc, input, out),
        .lz4 => compressLz4Into(alloc, input, out),
    };
}

/// Decompresses input into out, appending the result. Returns the number of
/// bytes appended. Access the data via out.items[out.items.len - sz ..].
pub fn decompress(
    alloc: std.mem.Allocator,
    codec: Codec,
    input: String,
    out: *std.ArrayList(u8),
) Error!usize {
    return switch (codec) {
        .none => noneInto(alloc, input, out),
        .zstd => decompressZstdInto(alloc, input, out),
        .lz4 => decompressLz4Into(alloc, input, out),
    };
}

/// The most bytes `compressTo` writes for `len` bytes of input: what a
/// caller sizes a fixed buffer by. 0 when one call cannot take that
/// many (lz4 past 2 GiB).
pub fn bound(codec: Codec, len: usize) usize {
    return switch (codec) {
        .none => len,
        .zstd => zstd.ZSTD_compressBound(len),
        .lz4 => if (len > std.math.maxInt(i32))
            0
        else
            4 + @as(usize, @intCast(lz4.LZ4_compressBound(@intCast(len)))),
    };
}

/// `compress` into a fixed buffer: `input` compressed at the start of
/// `dst`, which `bound` sized. Returns the number of bytes written. For
/// a caller whose output must stay where it put it, such as a
/// page-aligned buffer.
pub fn compressTo(codec: Codec, input: String, dst: []u8) Error!usize {
    return switch (codec) {
        .none => {
            if (dst.len < input.len) return error.CompressionFailed;
            @memcpy(dst[0..input.len], input);
            return input.len;
        },
        .zstd => compressZstdTo(input, dst),
        .lz4 => compressLz4To(input, dst),
    };
}

/// `decompress` into a fixed buffer: `input` decoded at the start of
/// `dst`, which must hold all of it. Returns the number of bytes
/// written.
pub fn decompressTo(codec: Codec, input: String, dst: []u8) Error!usize {
    return switch (codec) {
        .none => {
            if (dst.len < input.len) return error.DecompressionFailed;
            @memcpy(dst[0..input.len], input);
            return input.len;
        },
        .zstd => decompressZstdTo(input, dst),
        .lz4 => decompressLz4To(input, dst),
    };
}

fn noneInto(
    alloc: std.mem.Allocator,
    input: String,
    out: *std.ArrayList(u8),
) Error!usize {
    try out.appendSlice(alloc, input);
    return input.len;
}

fn compressZstdInto(
    alloc: std.mem.Allocator,
    input: String,
    out: *std.ArrayList(u8),
) Error!usize {
    try out.ensureUnusedCapacity(alloc, bound(.zstd, input.len));
    const written = try compressZstdTo(input, out.unusedCapacitySlice());
    out.items.len += written;
    return written;
}

fn compressZstdTo(input: String, dst: []u8) Error!usize {
    const written = zstd.ZSTD_compress(dst.ptr, dst.len, input.ptr, input.len, 1);
    if (zstd.ZSTD_isError(written) != 0) return error.CompressionFailed;
    return written;
}

fn decompressZstdInto(
    alloc: std.mem.Allocator,
    input: String,
    out: *std.ArrayList(u8),
) Error!usize {
    const frame_size = zstd.ZSTD_getFrameContentSize(input.ptr, input.len);
    if (frame_size == shim.egres_zstd_contentsize_error()) return error.DecompressionFailed;
    if (frame_size == shim.egres_zstd_contentsize_unknown()) return error.UnknownFrameSize;
    if (frame_size > std.math.maxInt(usize)) return error.DecompressionFailed;
    const needed: usize = @intCast(frame_size);
    try out.ensureUnusedCapacity(alloc, needed);
    const dst = out.unusedCapacitySlice()[0..needed];
    const written = try decompressZstdTo(input, dst);
    out.items.len += written;
    return written;
}

fn decompressZstdTo(input: String, dst: []u8) Error!usize {
    const written = zstd.ZSTD_decompress(dst.ptr, dst.len, input.ptr, input.len);
    if (zstd.ZSTD_isError(written) != 0) return error.DecompressionFailed;
    return written;
}

fn compressLz4Into(
    alloc: std.mem.Allocator,
    input: String,
    out: *std.ArrayList(u8),
) Error!usize {
    const max = bound(.lz4, input.len);
    if (max == 0) return error.CompressionFailed;
    try out.ensureUnusedCapacity(alloc, max);
    const written = try compressLz4To(input, out.unusedCapacitySlice());
    out.items.len += written;
    return written;
}

fn compressLz4To(input: String, dst: []u8) Error!usize {
    if (input.len > std.math.maxInt(i32)) return error.CompressionFailed;
    if (dst.len < 4) return error.CompressionFailed;
    writeU32LE(dst[0..4], @intCast(input.len));
    const room: usize = @min(dst.len - 4, std.math.maxInt(i32));
    const written_i32 = lz4.LZ4_compress_default(
        input.ptr,
        dst[4..].ptr,
        @intCast(input.len),
        @intCast(room),
    );
    if (written_i32 <= 0) return error.CompressionFailed;
    return 4 + @as(usize, @intCast(written_i32));
}

fn decompressLz4Into(
    alloc: std.mem.Allocator,
    input: String,
    out: *std.ArrayList(u8),
) Error!usize {
    const raw_len = try lz4RawLen(input);
    try out.ensureUnusedCapacity(alloc, raw_len);
    const written = try decompressLz4To(input, out.unusedCapacitySlice());
    out.items.len += written;
    return written;
}

fn decompressLz4To(input: String, dst: []u8) Error!usize {
    const raw_len = try lz4RawLen(input);
    if (raw_len > dst.len) return error.DecompressionFailed;
    const compressed = input[4..];
    if (compressed.len > std.math.maxInt(i32)) return error.DecompressionFailed;
    const written_i32 = lz4.LZ4_decompress_safe(
        compressed.ptr,
        dst.ptr,
        @intCast(compressed.len),
        @intCast(raw_len),
    );
    if (written_i32 < 0) return error.DecompressionFailed;
    if (@as(usize, @intCast(written_i32)) != raw_len) return error.CorruptFrame;
    return raw_len;
}

/// The decoded length an lz4 block declares, in its `[u32 raw_len]`.
fn lz4RawLen(input: String) Error!usize {
    if (input.len < 4) return error.CorruptFrame;
    const raw_len: usize = readU32LE(input[0..4]);
    if (raw_len > std.math.maxInt(i32)) return error.DecompressionFailed;
    return raw_len;
}

fn writeU32LE(dst: []u8, v: u32) void {
    var tmp: [4]u8 = undefined;
    std.mem.writeInt(u32, &tmp, v, .little);
    @memcpy(dst, tmp[0..]);
}

fn readU32LE(src: String) u32 {
    const p: *const [4]u8 = @ptrCast(src.ptr);
    return std.mem.readInt(u32, p, .little);
}
