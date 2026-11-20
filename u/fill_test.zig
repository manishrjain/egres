// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const utils = @import("utils");

test "fill: every length through the 64-byte loop and the tail" {
    var buf: [300]u8 = undefined;
    var len: usize = 0;
    while (len <= 200) : (len += 1) {
        @memset(&buf, 0x11);
        utils.fill(buf[7 .. 7 + len], 0xAB);
        for (buf[0..7]) |b| try std.testing.expectEqual(0x11, b);
        for (buf[7 .. 7 + len]) |b| try std.testing.expectEqual(0xAB, b);
        for (buf[7 + len ..]) |b| try std.testing.expectEqual(0x11, b);
    }
}

test "fill: a typed slice is filled by its bytes" {
    var words: [9]u64 = undefined;
    utils.fill(&words, 0);
    for (words) |w| try std.testing.expectEqual(0, w);
    utils.fill(words[2..5], 0xFF);
    try std.testing.expectEqual(0, words[1]);
    try std.testing.expectEqual(std.math.maxInt(u64), words[2]);
    try std.testing.expectEqual(std.math.maxInt(u64), words[4]);
    try std.testing.expectEqual(0, words[5]);
}
