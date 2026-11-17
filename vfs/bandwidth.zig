// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! A device's bandwidth: who waits is the meter's business, through
//! `acquire` on the calling fiber's tenant. What is here is the disk
//! half only: bytes to pages, a write's cost in pages, and the
//! device's calibrated rate. The meter is for the store: the one
//! limiter whose device holds the tenants' data is `metered` and its
//! rate is the io capacity; every other device, a WAL or chunk cache
//! disk of its own, only counts.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const _vfs = @import("vfs.zig");

pub const Config = struct {
    bytes_per_sec: u64, // bytes per second
    write_cost: u8 = 4, // write multiplier - each write costs k * read cost.
    /// Random one-page reads per second the device serves; 0 when
    /// unknown, and an operation then costs its pages alone.
    read_ops_per_sec: u64 = 0,
};

pub const Limiter = struct {
    /// Cumulative pages and operations ever granted across the
    /// process lifetime, split by IOKind. Incremented at each grant;
    /// never decremented. Useful as a sanity check against the rate
    /// limit and against the expected on-disk footprint.
    total_read_pages: std.atomic.Value(i64) = .init(0),
    total_write_pages: std.atomic.Value(i64) = .init(0),
    total_read_ops: std.atomic.Value(i64) = .init(0),
    total_write_ops: std.atomic.Value(i64) = .init(0),
    /// In the meter's i64; the config's bytes are the u64 edge.
    pages_per_sec: i64,
    /// The least one operation costs, in pages: what the device moves
    /// in the time one operation takes it, the bandwidth over the
    /// iops. A point read's one page is then charged the device time
    /// it takes, which is what a tenant doing small random reads
    /// uses up; a large read is charged its pages as before.
    pages_per_op: i64,
    /// Set at startup on the tenants' device; startup only, so a
    /// plain field.
    metered: bool = false,
    conf: Config,

    /// Snapshot of the live limiter state, all counted in pages
    /// (multiply by `vfs.page_size` for bytes) and operations.
    pub const Status = struct {
        pages_per_sec: i64,
        pages_per_op: i64,
        metered: bool,
        total_read_pages: i64,
        total_write_pages: i64,
        total_read_ops: i64,
        total_write_ops: i64,
    };

    pub fn status(self: *const Limiter) Status {
        return .{
            .pages_per_sec = self.pages_per_sec,
            .pages_per_op = self.pages_per_op,
            .metered = self.metered,
            .total_read_pages = self.total_read_pages.load(.monotonic),
            .total_write_pages = self.total_write_pages.load(.monotonic),
            .total_read_ops = self.total_read_ops.load(.monotonic),
            .total_write_ops = self.total_write_ops.load(.monotonic),
        };
    }

    pub fn init(config: Config) Limiter {
        const pages_per_sec = config.bytes_per_sec / _vfs.page_size;
        const pages_per_op: u64 = if (config.read_ops_per_sec == 0)
            1
        else
            @max(pages_per_sec / config.read_ops_per_sec, 1);
        return .{
            .pages_per_sec = @intCast(pages_per_sec),
            .pages_per_op = @intCast(pages_per_op),
            .conf = config,
        };
    }

    /// Pays for the whole request, `ops` operations moving `bytes` in
    /// all, before the I/O starts, at the calling fiber's class and
    /// tenant, waiting as the meter says unless the fiber's io does not
    /// wait (`Ctx.io_waits`), when the cost is taken and the I/O goes
    /// ahead. The cost is the pages or the operations' `pages_per_op`,
    /// whichever is more; a write costs `write_cost` of either per page.
    pub fn grant(
        self: *Limiter,
        bytes: usize,
        ops: usize,
        kind: _vfs.IOKind,
    ) void {
        if (bytes == 0 or ops == 0) return;
        const wm: i64 = if (kind == .write) self.conf.write_cost else 1;
        const pages: i64 = @intCast(
            std.math.divCeil(usize, bytes, _vfs.page_size) catch unreachable,
        );
        const nops: i64 = @intCast(ops);
        const cost = @max(pages, nops * self.pages_per_op);
        if (self.metered) {
            const ctx = fiber.ctx();
            if (ctx.io_waits)
                ctx.tenant.acquire(.io, ctx.class, cost * wm)
            else
                ctx.tenant.charge(.io, ctx.class, cost * wm, utils.coarseMs());
            ctx.tenant.countOps(nops);
        }
        const counters = switch (kind) {
            .read => .{ &self.total_read_pages, &self.total_read_ops },
            .write => .{ &self.total_write_pages, &self.total_write_ops },
        };
        _ = counters[0].fetchAdd(pages, .monotonic);
        _ = counters[1].fetchAdd(nops, .monotonic);
    }
};
