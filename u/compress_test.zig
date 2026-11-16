// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const utils = @import("utils");
const String = utils.String;
const compress = utils.compression;

fn makePayload(alloc: std.mem.Allocator, len: usize) ![]u8 {
    var out = try alloc.alloc(u8, len);
    var prng = std.Random.DefaultPrng.init(0xBADC0FFEE0DDF00D);
    prng.random().bytes(out);

    // Add repetition so real codecs can compress something meaningful.
    const pat = "egres-egres-egres";
    var i: usize = 0;
    while (i + pat.len <= out.len) : (i += 64) {
        @memcpy(out[i .. i + pat.len], pat);
    }
    return out;
}

fn expectRoundTrip(
    alloc: std.mem.Allocator,
    codec: compress.Codec,
    data: String,
) !void {
    var enc_buf = std.ArrayList(u8).empty;
    defer enc_buf.deinit(alloc);
    const enc_sz = try compress.compress(alloc, codec, data, &enc_buf);
    const encoded = enc_buf.items[enc_buf.items.len - enc_sz ..];

    var dec_buf = std.ArrayList(u8).empty;
    defer dec_buf.deinit(alloc);
    const dec_sz = try compress.decompress(alloc, codec, encoded, &dec_buf);
    const decoded = dec_buf.items[dec_buf.items.len - dec_sz ..];

    utils.testPrint(
        "[u.compress] codec={s} input_len={} compressed_len={} output_len={}\n",
        .{ @tagName(codec), data.len, enc_sz, dec_sz },
    );

    try std.testing.expectEqualSlices(u8, data, decoded);
}

test "none compression roundtrip" {
    const data = try makePayload(std.testing.allocator, 128 * 1024);
    defer std.testing.allocator.free(data);
    try expectRoundTrip(std.testing.allocator, .none, data);
}

test "zstd compression roundtrip" {
    const data = try makePayload(std.testing.allocator, 256 * 1024);
    defer std.testing.allocator.free(data);
    try expectRoundTrip(std.testing.allocator, .zstd, data);
}

test "lz4 compression roundtrip" {
    const data = try makePayload(std.testing.allocator, 256 * 1024);
    defer std.testing.allocator.free(data);
    try expectRoundTrip(std.testing.allocator, .lz4, data);
}

test "compressTo and decompressTo: a fixed buffer of bound bytes holds it" {
    const alloc = std.testing.allocator;
    const data = try makePayload(alloc, 64 * 1024);
    defer alloc.free(data);
    for ([_]compress.Codec{ .none, .zstd, .lz4 }) |codec| {
        const enc = try alloc.alloc(u8, compress.bound(codec, data.len));
        defer alloc.free(enc);
        const enc_sz = try compress.compressTo(codec, data, enc);

        const dec = try alloc.alloc(u8, data.len);
        defer alloc.free(dec);
        const dec_sz = try compress.decompressTo(codec, enc[0..enc_sz], dec);
        try std.testing.expectEqualSlices(u8, data, dec[0..dec_sz]);

        // One byte short of the input is too small to decode into.
        try std.testing.expect(std.meta.isError(
            compress.decompressTo(codec, enc[0..enc_sz], dec[1..]),
        ));
    }
}

test "lz4 detects truncated frame" {
    const data = "hello world hello world hello world";
    var enc_buf = std.ArrayList(u8).empty;
    defer enc_buf.deinit(std.testing.allocator);
    const enc_sz = try compress.compress(std.testing.allocator, .lz4, data, &enc_buf);
    const encoded = enc_buf.items[enc_buf.items.len - enc_sz ..];

    const truncated = encoded[0 .. encoded.len - 1];
    var dec_buf = std.ArrayList(u8).empty;
    defer dec_buf.deinit(std.testing.allocator);
    try std.testing.expectError(
        error.DecompressionFailed,
        compress.decompress(std.testing.allocator, .lz4, truncated, &dec_buf),
    );
}

test "zstd rejects garbage bytes" {
    const garbage = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    var dec_buf = std.ArrayList(u8).empty;
    defer dec_buf.deinit(std.testing.allocator);
    try std.testing.expectError(
        error.DecompressionFailed,
        compress.decompress(std.testing.allocator, .zstd, &garbage, &dec_buf),
    );
}

/// `maybePack` then `maybeUnpack` with the codec it chose gives the
/// input back. Returns that codec.
fn packRoundTrip(input: String, rule: compress.PackRule) !compress.Codec {
    const alloc = std.testing.allocator;
    var pack_scratch: std.ArrayList(u8) = .empty;
    defer pack_scratch.deinit(alloc);
    const stored = try compress.maybePack(alloc, input, rule, &pack_scratch);
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(alloc);
    const got = try compress.maybeUnpack(
        alloc,
        @intFromEnum(stored.codec),
        stored.data,
        &scratch,
    );
    try std.testing.expectEqualSlices(u8, input, got);
    if (stored.codec == .none) {
        // Not compressed: the input itself, both ways, with no copy.
        try std.testing.expectEqual(input.ptr, stored.data.ptr);
        try std.testing.expectEqual(input.len, stored.data.len);
        try std.testing.expectEqual(input.ptr, got.ptr);
    } else {
        // Compressed: in the caller's scratch.
        try std.testing.expectEqual(pack_scratch.items.ptr, stored.data.ptr);
        // Kept compressed means smaller by the saving asked for.
        const saved: f64 = @floatFromInt(input.len - stored.data.len);
        const raw: f64 = @floatFromInt(input.len);
        try std.testing.expect(saved >= raw * rule.min_saving);
    }
    return stored.codec;
}

const pack_rule: compress.PackRule = .{
    .codec = .lz4,
    .min_bytes = 512,
    .min_saving = 1.0 / 8.0,
};

test "maybePack: a large input that compresses is kept compressed" {
    const input = "the quick brown fox jumps over the lazy dog. " ** 200;
    try std.testing.expectEqual(
        compress.Codec.lz4,
        try packRoundTrip(input, pack_rule),
    );
}

test "maybePack: an input below min_bytes is not tried" {
    // It would compress, and is returned as it came.
    const small = "a" ** (pack_rule.min_bytes - 1);
    try std.testing.expectEqual(
        compress.Codec.none,
        try packRoundTrip(small, pack_rule),
    );
    try std.testing.expectEqual(
        compress.Codec.none,
        try packRoundTrip("", pack_rule),
    );
    // At min_bytes it is tried.
    const at = "a" ** pack_rule.min_bytes;
    try std.testing.expectEqual(
        compress.Codec.lz4,
        try packRoundTrip(at, pack_rule),
    );
}

test "maybePack: a saving below min_saving is not kept" {
    var input: [16384]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(5);
    // Random bytes: nothing saved.
    prng.random().bytes(&input);
    try std.testing.expectEqual(
        compress.Codec.none,
        try packRoundTrip(&input, pack_rule),
    );
    // Half zeros: about half saved, kept.
    @memset(input[input.len / 2 ..], 0);
    try std.testing.expectEqual(
        compress.Codec.lz4,
        try packRoundTrip(&input, pack_rule),
    );
    // One sixteenth zeros: under an eighth saved, not kept.
    prng.random().bytes(&input);
    @memset(input[input.len - input.len / 16 ..], 0);
    try std.testing.expectEqual(
        compress.Codec.none,
        try packRoundTrip(&input, pack_rule),
    );
}

test "maybePack: codec none returns the input, and scratch is reused" {
    const alloc = std.testing.allocator;
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(alloc);
    const input = "a" ** 4096;
    var no_codec = pack_rule;
    no_codec.codec = .none;
    const plain = try compress.maybePack(alloc, input, no_codec, &scratch);
    try std.testing.expectEqual(compress.Codec.none, plain.codec);
    try std.testing.expectEqual(@as([*]const u8, input), plain.data.ptr);
    try std.testing.expectEqual(@as(usize, 0), scratch.capacity);

    // Two packs through one scratch: the second reuses the first's
    // buffer, with nothing allocated.
    const first = try compress.maybePack(alloc, input, pack_rule, &scratch);
    try std.testing.expectEqual(compress.Codec.lz4, first.codec);
    const buf = scratch.items.ptr;
    const cap = scratch.capacity;
    const second = try compress.maybePack(alloc, input, pack_rule, &scratch);
    try std.testing.expectEqual(buf, second.data.ptr);
    try std.testing.expectEqual(cap, scratch.capacity);
}

test "maybeUnpack: a codec byte it does not know is an error" {
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(std.testing.allocator);
    try std.testing.expectError(
        error.UnknownCodec,
        compress.maybeUnpack(std.testing.allocator, 200, "abc", &scratch),
    );
}
