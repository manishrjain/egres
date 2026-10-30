// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The runtime's allocator, counted on an owner: a `Ctx` for a
//! statement's memory, a `Tenant` for work done on its behalf off any
//! statement. The owner is the allocator's data pointer, so a chunk
//! comes off the owner that handed it out, whichever fiber frees it.

const std = @import("std");
const _rt = @import("runtime.zig");
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;

/// The vtable over `Owner`, which takes the count through
/// `holdMem(*Owner, i64)` and `releaseMem(*Owner, i64)`.
pub fn vtable(comptime Owner: type) *const Allocator.VTable {
    const Fns = struct {
        fn owner(ptr: *anyopaque) *Owner {
            return @ptrCast(@alignCast(ptr));
        }

        fn held(ptr: *anyopaque, old_len: usize, new_len: usize) void {
            const o = owner(ptr);
            if (new_len >= old_len)
                o.holdMem(@intCast(new_len - old_len))
            else
                o.releaseMem(@intCast(old_len - new_len));
        }

        fn alloc(ptr: *anyopaque, len: usize, al: Alignment, ra: usize) ?[*]u8 {
            const base = _rt.runtime().allocator;
            const p = base.rawAlloc(len, al, ra) orelse return null;
            held(ptr, 0, len);
            return p;
        }

        fn resize(
            ptr: *anyopaque,
            memory: []u8,
            al: Alignment,
            new_len: usize,
            ra: usize,
        ) bool {
            const base = _rt.runtime().allocator;
            if (!base.rawResize(memory, al, new_len, ra)) return false;
            held(ptr, memory.len, new_len);
            return true;
        }

        fn remap(
            ptr: *anyopaque,
            memory: []u8,
            al: Alignment,
            new_len: usize,
            ra: usize,
        ) ?[*]u8 {
            const base = _rt.runtime().allocator;
            const p = base.rawRemap(memory, al, new_len, ra) orelse return null;
            held(ptr, memory.len, new_len);
            return p;
        }

        fn free(ptr: *anyopaque, memory: []u8, al: Alignment, ra: usize) void {
            _rt.runtime().allocator.rawFree(memory, al, ra);
            held(ptr, memory.len, 0);
        }

        const table: Allocator.VTable = .{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        };
    };
    return &Fns.table;
}
