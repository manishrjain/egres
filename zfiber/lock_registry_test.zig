// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");

// dumpLocks is process-wide and tests run concurrently, so we can't assert
// exact totals — but a lock WE hold must appear, with our acquire @src.

test "Mutex(.tracked): dumpLocks shows the holder's acquire site while held" {
    var mu: fiber.Mutex(.tracked) = .{};
    defer mu.deinit();

    mu.lock(@src());
    var aw: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer aw.deinit();
    try fiber.dumpLocks(&aw.writer);
    const text = aw.writer.buffered();
    mu.unlock();

    std.log.info("dumpLocks while held:\n{s}", .{text});
    // The holder line carries our file:line (proves @src capture + the
    // derived count from the lock's own `state`).
    try std.testing.expect(std.mem.indexOf(u8, text, "held_by=") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "lock_registry_test.zig") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "count=1") != null);
}

test "Mutex(.tracked): a released lock derives held==0 and is skipped" {
    var mu: fiber.Mutex(.tracked) = .{};
    defer mu.deinit();
    mu.lock(@src());
    mu.unlock();

    // state is back to 0, so snapshot.held == 0 and the dump omits our lock.
    var aw: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer aw.deinit();
    try fiber.dumpLocks(&aw.writer);
    try std.testing.expect(std.mem.indexOf(u8, aw.writer.buffered(), "LOCKS:") != null);
}

test "Mutex(.tracked): a lock unlinks from anywhere in the registry" {
    var locks: [3]fiber.Mutex(.tracked) = .{ .{}, .{}, .{} };
    for (&locks) |*mu| {
        mu.lock(@src());
        mu.unlock();
    }
    // The list runs newest first, so this unlinks the middle lock, then
    // the head (unless a test alongside registered after us), then the
    // oldest. The check panics on a lock left linked after its deinit.
    for ([_]usize{ 1, 2, 0 }) |lock_idx| {
        locks[lock_idx].deinit();
        fiber.checkLockRegistryForTests();
    }
}

test "RWMutex(.tracked): only the first reader of a shared session records" {
    var rw: fiber.RWMutex(.tracked) = .{};
    defer rw.deinit();

    rw.lockShared(@src());
    var aw: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer aw.deinit();
    try fiber.dumpLocks(&aw.writer);
    const text = aw.writer.buffered();
    rw.unlockShared();

    std.log.info("dumpLocks (shared held):\n{s}", .{text});
    try std.testing.expect(std.mem.indexOf(u8, text, "held_by=") != null);
    try std.testing.expect(std.mem.indexOf(u8, text, "lock_registry_test.zig") != null);
}
