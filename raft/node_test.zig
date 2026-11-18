// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const raft = @import("raft");

const ApplyWaiters = raft.ApplyWaiters;
const ProposalWaiter = raft.ProposalWaiter;

fn waiter(index: u64) ProposalWaiter {
    return .{ .wait_for = .apply, .entry = .{ .term = 1, .index = index } };
}

// The old queue was a channel of 1,024 whose full send parked the
// main loop; a push well past that must return at once, and the pops
// must come back in the order pushed, through the prefix drops in
// between.
test "ApplyWaiters: pushes never park and pop in order" {
    const alloc = std.testing.allocator;
    var q: ApplyWaiters = .{};
    defer q.deinit(alloc);

    const n = 3000;
    const ws = try alloc.alloc(ProposalWaiter, n);
    defer alloc.free(ws);
    for (ws, 1..) |*w, i| w.* = waiter(i);

    // Pushes and pops interleaved, so the popped prefix is dropped
    // while waiters are still queued.
    var pushed: usize = 0;
    var popped: usize = 0;
    while (pushed < n) {
        const batch = @min(n - pushed, 700);
        for (ws[pushed .. pushed + batch]) |*w| try q.push(alloc, w);
        pushed += batch;
        try std.testing.expectEqual(pushed - popped, q.len());
        while (popped + 1 < pushed) : (popped += 1) {
            const w = q.pop() orelse return error.TestUnexpectedResult;
            try std.testing.expectEqual(popped + 1, w.entry.index);
        }
    }
    const last = q.pop() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, n), last.entry.index);
    try std.testing.expectEqual(@as(?*ProposalWaiter, null), q.pop());
    try std.testing.expectEqual(@as(usize, 0), q.len());
}

test "ApplyWaiters: closed refuses a push and still pops what is queued" {
    const alloc = std.testing.allocator;
    var q: ApplyWaiters = .{};
    defer q.deinit(alloc);

    var a = waiter(1);
    var b = waiter(2);
    try q.push(alloc, &a);
    q.close();
    try std.testing.expectError(error.Closed, q.push(alloc, &b));
    try std.testing.expectEqual(@as(?*ProposalWaiter, &a), q.pop());
    try std.testing.expectEqual(@as(?*ProposalWaiter, null), q.pop());
}
