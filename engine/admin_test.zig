// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const proto = @import("engine").admin;

// Build a set-batch body in memory: `count` records of (key + ' ' + value + '\n'),
// where key is 24 bytes zero-padded and value is `value_size` bytes of 'v'.
// Returned slice is heap-allocated; caller owns it.
fn buildBatchBody(alloc: std.mem.Allocator, count: u32, value_size: usize) ![]u8 {
    const key_size: usize = 24;
    const per_pair = key_size + 1 + value_size + 1;
    const body = try alloc.alloc(u8, count * per_pair);
    var off: usize = 0;
    var i: u32 = 0;
    while (i < count) : (i += 1) {
        _ = std.fmt.bufPrint(body[off .. off + key_size], "user{d:0>20}", .{i}) catch unreachable;
        off += key_size;
        body[off] = ' ';
        off += 1;
        @memset(body[off .. off + value_size], 'v');
        off += value_size;
        body[off] = '\n';
        off += 1;
    }
    return body;
}

test "readToken: literal token is a slice into the shared ArrayList" {
    const alloc = std.testing.allocator;
    var r = std.Io.Reader.fixed("hello world\n");
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    try buf.ensureUnusedCapacity(alloc, proto.max_token_size);

    const start = buf.items.len;
    try proto.readToken(&r, &buf);
    try std.testing.expectEqualStrings("hello", buf.items[start..]);
}

test "readToken: hex-prefixed token decoded in place" {
    const alloc = std.testing.allocator;
    // 0x68656c6c6f = "hello"
    var r = std.Io.Reader.fixed("0x68656c6c6f tail\n");
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    try buf.ensureUnusedCapacity(alloc, proto.max_token_size);

    const start = buf.items.len;
    try proto.readToken(&r, &buf);
    try std.testing.expectEqualStrings("hello", buf.items[start..]);
    // `buf.items.len` is truncated to the decoded length — no stale trailing bytes.
    try std.testing.expectEqual(@as(usize, 5), buf.items.len - start);
}

test "readToken: oversized token returns error.TokenTooLong" {
    const alloc = std.testing.allocator;
    // Non-ws bytes longer than the cap, then ws to end the token.
    const overshoot = proto.max_token_size + 100;
    const body = try alloc.alloc(u8, overshoot + 1);
    defer alloc.free(body);
    @memset(body[0..overshoot], 'a');
    body[overshoot] = '\n';

    var r = std.Io.Reader.fixed(body);
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    try buf.ensureUnusedCapacity(alloc, proto.max_token_size);

    try std.testing.expectError(error.TokenTooLong, proto.readToken(&r, &buf));
}

test "readToken: odd-length hex returns error.BadHex" {
    const alloc = std.testing.allocator;
    var r = std.Io.Reader.fixed("0x123 tail\n");
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(alloc);
    try buf.ensureUnusedCapacity(alloc, proto.max_token_size);

    try std.testing.expectError(error.BadHex, proto.readToken(&r, &buf));
}

// Throughput benchmark (was here as a `test "readToken: benchmark parse
// throughput"`) is now in `engine/admin_bench.zig` and runs via
// `zig build bench`.

test "parseLine: flag takes the group and the rest of the line" {
    const cmd = try proto.parseLine("flag runtime cpu_milli=4000 poll_us=0");
    try std.testing.expectEqualStrings("runtime", cmd.flag.group);
    try std.testing.expectEqualStrings(
        "cpu_milli=4000 poll_us=0",
        cmd.flag.input,
    );
    try std.testing.expectError(
        error.MissingArg,
        proto.parseLine("flag runtime"),
    );
    try std.testing.expectError(error.MissingArg, proto.parseLine("flag"));
}

test "parseLine: tenants takes no arguments" {
    const cmd = try proto.parseLine("tenants");
    try std.testing.expect(cmd == .tenants);
}
