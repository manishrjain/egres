// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const builtin = @import("builtin");
pub const compression = @import("compress.zig");
pub const hlc = @import("hlc.zig");
pub const host = @import("host.zig");
pub const date = @import("date.zig");
pub const log = @import("log.zig");
/// Process-wide context (gpa/arena/io/args) and the coarse clock live
/// here, at the bottom of the module graph, so every layer including
/// the fiber runtime reaches them without a cycle.
pub const context = @import("context.zig");
pub const mem = @import("mem.zig");
pub const coarse_clock = @import("coarse_clock.zig");
pub const coarseMs = coarse_clock.ms;
pub const timestampNow = coarse_clock.timestampNow;

/// A borrowed byte string: a name, row bytes, an encoded key. The
/// same type as []const u8; it reads better in a slice of them.
pub const String = []const u8;
pub const GuardAllocator = @import("guard_allocator.zig").GuardAllocator;

/// `mprotect` through libc, which every binary links: std.posix has no
/// wrapper in this Zig and a raw Linux syscall is Linux only. `addr`
/// must be page-aligned. `.SUCCESS` or the errno.
pub fn mprotect(addr: usize, len: usize, prot: std.c.PROT) std.c.E {
    const ptr: *align(std.heap.page_size_min) anyopaque = @ptrFromInt(addr);
    return std.posix.errno(std.c.mprotect(ptr, len, prot));
}

/// Empty an ArrayList and keep its buffer. `clearRetainingCapacity`
/// does the same but first memsets the whole slice with `undefined`,
/// a real 0xAA fill in Debug and ReleaseSafe, which showed up on hot
/// resets (a version merge per key on a full-table scan). One place to
/// change if the reset strategy ever changes.
pub fn clearRetainCap(list: anytype) void {
    list.items.len = 0;
}

/// An append-only byte buffer that grows by adding fixed-size chunks
/// instead of reallocating. Nothing already written ever moves, so a
/// large output (an SST) is copied once, in `copyTo`, rather than once
/// per doubling of an ArrayList, and needs no size guess up front.
/// The bytes are never contiguous: a reader copies them out.
///
/// Chunks are page aligned so that one day they can go to a direct-IO
/// write as they are, without the gather.
pub const ChunkBuffer = struct {
    pub const alignment = std.mem.Alignment.fromByteUnits(4096);
    const Chunk = []align(alignment.toByteUnits()) u8;

    allocator: std.mem.Allocator,
    chunk_bytes: usize,
    chunks: std.ArrayList(Chunk),
    /// Bytes written. Chunk i holds [i * chunk_bytes, (i + 1) *
    /// chunk_bytes) of them; the last chunk may be partly filled.
    len: usize,

    pub fn init(allocator: std.mem.Allocator, chunk_bytes: usize) ChunkBuffer {
        std.debug.assert(chunk_bytes > 0);
        return .{
            .allocator = allocator,
            .chunk_bytes = chunk_bytes,
            .chunks = .empty,
            .len = 0,
        };
    }

    pub fn deinit(self: *ChunkBuffer) void {
        for (self.chunks.items) |chunk| self.allocator.free(chunk);
        self.chunks.deinit(self.allocator);
    }

    /// Empty the buffer, keeping its chunks for the next fill.
    pub fn reset(self: *ChunkBuffer) void {
        self.len = 0;
    }

    /// The unwritten tail of the chunk `len` falls in, never empty:
    /// a fresh chunk is added once the last one is full.
    fn tail(self: *ChunkBuffer) ![]u8 {
        const used = self.len % self.chunk_bytes;
        const idx = self.len / self.chunk_bytes;
        if (idx == self.chunks.items.len) {
            const chunk = try self.allocator.alignedAlloc(
                u8,
                alignment,
                self.chunk_bytes,
            );
            errdefer self.allocator.free(chunk);
            try self.chunks.append(self.allocator, chunk);
        }
        return self.chunks.items[idx][used..];
    }

    pub fn appendSlice(self: *ChunkBuffer, bytes: String) !void {
        var rest = bytes;
        while (rest.len > 0) {
            const dst = try self.tail();
            const n = @min(dst.len, rest.len);
            @memcpy(dst[0..n], rest[0..n]);
            rest = rest[n..];
            self.len += n;
        }
    }

    pub fn appendNTimes(self: *ChunkBuffer, byte: u8, count: usize) !void {
        var rest = count;
        while (rest > 0) {
            const dst = try self.tail();
            const n = @min(dst.len, rest);
            @memset(dst[0..n], byte);
            rest -= n;
            self.len += n;
        }
    }

    /// Copy every written byte, in order, into `dst`, sized exactly.
    pub fn copyTo(self: *const ChunkBuffer, dst: []u8) void {
        std.debug.assert(dst.len == self.len);
        var off: usize = 0;
        for (self.chunks.items) |chunk| {
            if (off == self.len) break;
            const n = @min(chunk.len, self.len - off);
            @memcpy(dst[off .. off + n], chunk[0..n]);
            off += n;
        }
    }
};

pub fn testPrint(comptime fmt: String, args: anytype) void {
    if (!builtin.is_test) return;
    // std.debug.print(fmt, args);
    _ = fmt;
    _ = args;
}

/// Rate-limited log: each call site, told apart by its format, logs
/// at most once per interval, at the given level. For a line that a
/// loop would otherwise print on every pass, a retry above all.
pub fn logEvery(
    comptime level: std.log.Level,
    comptime interval_secs: i64,
    comptime fmt: String,
    args: anytype,
) void {
    if (!logDue(fmt, interval_secs * 1_000, coarse_clock.ms())) return;
    // std.log.log is private: pick the level's function.
    switch (level) {
        .err => std.log.err(fmt, args),
        .warn => std.log.warn(fmt, args),
        .info => std.log.info(fmt, args),
        .debug => std.log.debug(fmt, args),
    }
}

/// Whether the call site with format `site` logs at `now_ms`: its
/// first call does, then one per `interval_ms`.
pub fn logDue(comptime site: String, interval_ms: i64, now_ms: i64) bool {
    const S = struct {
        // A type that names nothing of its function is one type for
        // every instantiation, and its stamp one stamp for every call
        // site in the process. Naming the format makes it this site's.
        const of = site;
        /// Null until the first call: a clock that starts at zero, the
        /// simulation's, is not an interval old.
        var last: ?i64 = null;
    };
    if (S.last) |last| {
        if (now_ms - last <= interval_ms) return false;
    }
    S.last = now_ms;
    return true;
}

/// search finds the first index in [0, items.len) for which pred(c, items[i]) is true,
/// assuming the range is partitioned as [false…false, true…true].
/// Returns items.len when the predicate is never true (mirrors Go's sort.Search).
pub fn search(
    comptime T: type,
    items: []const T,
    c: anytype,
    comptime pred: fn (@TypeOf(c), T) bool,
) usize {
    var lo: usize = 0;
    var hi: usize = items.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (pred(c, items[mid])) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    return lo;
}

/// Lock-free, first-error-wins container. Safe to call set() from many threads;
/// only the first error is retained.
pub const SafeError = struct {
    raw: std.atomic.Value(u16) = std.atomic.Value(u16).init(0),

    pub fn set(self: *SafeError, e: anyerror) void {
        _ = self.raw.cmpxchgStrong(0, @intFromError(e), .acq_rel, .monotonic);
    }

    pub fn get(self: *const SafeError) ?anyerror {
        const v = self.raw.load(.acquire);
        if (v == 0) return null;
        return @errorFromInt(v);
    }
};

/// Heap-allocated, ref-counted byte buffer. Used to share payloads across
/// fan-out send paths (e.g. AE / file push) without per-peer memcpy: caller
/// wraps an owned slice in `RefValue.init`, then `incrRef` per peer and
/// hands each ref off to the transport queue. Each writer fiber `decrRef`s
/// after writing — when the count reaches zero, the data slice and the
/// RefValue itself are freed with the original allocator.
pub const RefValue = struct {
    data: []u8,
    allocator: std.mem.Allocator,
    ref: std.atomic.Value(i32),

    pub fn init(allocator: std.mem.Allocator, data: []u8) !*RefValue {
        const v = try allocator.create(RefValue);
        v.* = .{ .data = data, .allocator = allocator, .ref = std.atomic.Value(i32).init(1) };
        return v;
    }

    /// A freed value's count is left at this poison, so a late take or
    /// release on it trips the guards below instead of corrupting the
    /// heap silently. These checks stay on in every build mode: a
    /// refcount bug here is a cross-thread double free.
    const dead: i32 = -0x4000_0000;

    pub fn incrRef(self: *RefValue) *RefValue {
        const prev = self.ref.fetchAdd(1, .monotonic);
        if (prev <= 0) @panic("RefValue.incrRef on a freed value");
        return self;
    }

    pub fn decrRef(self: *RefValue) void {
        const prev = self.ref.fetchSub(1, .acq_rel);
        if (prev <= 0) @panic("RefValue.decrRef below zero (double release)");
        if (prev == 1) {
            self.ref.store(dead, .release);
            self.allocator.free(self.data);
            self.allocator.destroy(self);
        }
    }

    pub fn byteSize(self: *const RefValue) usize {
        return self.data.len;
    }

    /// What a cache charges for holding this value: its bytes and
    /// this header, which is its own allocation.
    pub fn cost(self: *const RefValue) usize {
        return self.data.len + @sizeOf(RefValue);
    }
};
