// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");
const fiber = @import("zfiber");

const fill: u8 = if (builtin.mode == .Debug) 0xAA else 0;

test "stacks: a stack comes back from the pool wiped" {
    var pool = try fiber.StackPool.init(std.testing.allocator);
    defer pool.deinit();

    var info: fiber.coro.Stack = undefined;
    try pool.get(&info, fiber.default_stack_size);
    const bytes: [*]u8 = @ptrFromInt(info.limit);
    const len = info.base - info.limit;
    // A shallow frame near the top and a deep one 600 KiB down, with
    // untouched pages between them, as a recursion that returned
    // leaves behind.
    const shallow = len - 100;
    const deep = len - 600 * 1024;
    bytes[shallow] = 0xCD;
    bytes[deep] = 0xCD;
    pool.put(info);

    var again: fiber.coro.Stack = undefined;
    try pool.get(&again, fiber.default_stack_size);
    try std.testing.expectEqual(info.limit, again.limit);
    try std.testing.expectEqual(fill, bytes[shallow]);
    try std.testing.expectEqual(fill, bytes[deep]);
    try std.testing.expectEqual(fill, bytes[deep + 4096]);
    pool.put(again);
}
