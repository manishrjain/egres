const std = @import("std");
const proto = @import("protocol.zig");

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

test "readToken: benchmark parse throughput" {
    // This measures the server's parse path in isolation — no sockets, no
    // allocator contention beyond what the real handler does, no Raft. The
    // goal is to verify the parser can move bytes far faster than the
    // end-to-end pipeline we observe (~24 MB/s), so we can rule it out as
    // the bottleneck. Run with `-Doptimize=ReleaseFast` for numbers that
    // are comparable to a release build; Debug runs print a number too
    // but it's 10-100x slower.
    const alloc = std.testing.allocator;
    const count: u32 = 256;
    const value_size: usize = 1024;
    const body = try buildBatchBody(alloc, count, value_size);
    defer alloc.free(body);

    const iterations: u32 = 500;
    var total_bytes: u64 = 0;
    var total_tokens: u64 = 0;
    const t0 = try std.time.Instant.now();

    var it: u32 = 0;
    while (it < iterations) : (it += 1) {
        var r = std.Io.Reader.fixed(body);
        // Fresh ArrayList each batch — mirrors `handleSetBatch`'s per-request
        // scratch lifecycle, including the realloc chain as it grows.
        var input: std.ArrayList(u8) = .empty;
        defer input.deinit(alloc);
        try input.ensureTotalCapacity(alloc, 16 * 1024);

        var i: u32 = 0;
        while (i < count * 2) : (i += 1) {
            try input.ensureUnusedCapacity(alloc, proto.max_token_size);
            try proto.readToken(&r, &input);
        }
        total_bytes += body.len;
        total_tokens += count * 2;
    }

    const elapsed_ns = (try std.time.Instant.now()).since(t0);
    const secs = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;
    const mib_per_s = @as(f64, @floatFromInt(total_bytes)) / secs / (1024.0 * 1024.0);
    const tokens_per_s = @as(f64, @floatFromInt(total_tokens)) / secs;

    std.debug.print(
        "\n[protocol bench] {d} iters × {d}B body ({d} pairs × {d}B value)  " ++
            "→ {d:.1} MiB/s  ({d:.0} tokens/s, {d:.2} ms/iter)\n",
        .{
            iterations,
            body.len,
            count,
            value_size,
            mib_per_s,
            tokens_per_s,
            secs * 1000.0 / @as(f64, @floatFromInt(iterations)),
        },
    );
}
