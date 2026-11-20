// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Fiber stacks, reused. A stack is an mmap with a guard page below
//! it, and a server starts and finishes a thousand fibers a second:
//! an mmap, an mprotect and a munmap each, the munmap a TLB shootdown
//! on every core, and one mapping per live fiber (44K at 300K
//! tenants, against a kernel limit of 65K unless raised). A finished
//! fiber's stack goes on the free list and the next spawn of that
//! size takes it. Only the default size is pooled, and the list is
//! capped so idle stacks do not hold their pages for good.
//!
//! A stack is wiped before it is kept. Fibers are not tenant-bound,
//! and a read of an undefined local on a fresh mapping saw zeros; on
//! a reused stack it would see the rows or query of whoever ran there
//! last. Only the pages the fiber touched are written, as mincore
//! reports them.

const std = @import("std");
const builtin = @import("builtin");
const zio = @import("zio");
const utils = @import("utils");
const _fb = @import("fiber.zig");
const Spinlock = @import("spinlock.zig").Spinlock;

const coro = zio.coro;
pub const StackInfo = coro.Stack;

pub const StackPool = struct {
    sl: Spinlock = .{},
    /// Capacity reserved at init, so a put under the spinlock never
    /// allocates.
    free: std.ArrayList(StackInfo),
    allocator: std.mem.Allocator,

    /// Idle stacks kept. Past it a stack is unmapped as before.
    pub const cap = 4096;

    pub fn init(allocator: std.mem.Allocator) !StackPool {
        var free: std.ArrayList(StackInfo) = .empty;
        try free.ensureTotalCapacityPrecise(allocator, cap);
        return .{ .free = free, .allocator = allocator };
    }

    pub fn deinit(self: *StackPool) void {
        for (self.free.items) |info| coro.stackFree(info);
        self.free.deinit(self.allocator);
    }

    /// A stack of `size` bytes, fully committed: a pooled one when the
    /// size is the default and one is idle, else freshly mapped.
    pub fn get(self: *StackPool, info: *StackInfo, size: usize) !void {
        if (size == _fb.default_stack_size) {
            self.sl.lock();
            const pooled = self.free.pop();
            self.sl.unlock();
            if (pooled) |p| {
                info.* = p;
                return;
            }
        }
        try coro.stackAlloc(info, size, size);
    }

    /// Keeps a default-size stack for the next `get`, wiped, or unmaps
    /// it. The wipe runs outside the spinlock, and only once there is
    /// room: a stack the pool will not keep is unmapped as it is.
    pub fn put(self: *StackPool, info: StackInfo) void {
        if (info.base - info.limit == _fb.default_stack_size and
            self.hasRoom())
        {
            wipe(info);
            self.sl.lock();
            defer self.sl.unlock();
            if (self.free.items.len < cap) {
                self.free.appendAssumeCapacity(info);
                return;
            }
        }
        coro.stackFree(info);
    }

    fn hasRoom(self: *StackPool) bool {
        self.sl.lock();
        defer self.sl.unlock();
        return self.free.items.len < cap;
    }
};

/// What a fresh `undefined` reads as: the compiler's fill in Debug, so
/// a stale read trips the same checks, zero as a new mapping elsewhere.
const fill_byte: u8 = if (builtin.mode == .Debug) 0xAA else 0;

/// Overwrites what the fiber wrote. A stack grows down from `base`, so
/// the touched pages run from the deepest one up to it, and the deepest
/// is the lowest page mincore reports resident. A page never touched
/// still maps the zero page and costs nothing to skip.
fn wipe(info: StackInfo) void {
    const page = std.heap.pageSize();
    const len = info.base - info.limit;
    const stack: [*]align(std.heap.page_size_min) u8 =
        @ptrFromInt(info.limit);
    var vec: [_fb.default_stack_size / std.heap.page_size_min]u8 =
        undefined;
    std.posix.mincore(stack, len, &vec) catch {
        utils.fill(stack[0..len], fill_byte);
        return;
    };
    const pages = len / page;
    for (vec[0..pages], 0..) |resident, idx| {
        if (resident & 1 == 0) continue;
        utils.fill(stack[idx * page .. len], fill_byte);
        return;
    }
}
