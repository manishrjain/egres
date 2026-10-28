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
const _vfs = @import("vfs.zig");

pub const Config = struct {
    bytes_per_sec: u64, // bytes per second
    write_cost: u8 = 4, // write multiplier - each write costs k * read cost.
};

pub const Limiter = struct {
    /// Cumulative pages ever granted across the process lifetime,
    /// split by IOKind. Incremented at each grant; never
    /// decremented. Useful as a sanity check against the rate limit
    /// and against the expected on-disk footprint.
    total_read_pages: std.atomic.Value(i64) = .init(0),
    total_write_pages: std.atomic.Value(i64) = .init(0),
    /// In the meter's i64; the config's bytes are the u64 edge.
    pages_per_sec: i64,
    /// Set at startup on the tenants' device; startup only, so a
    /// plain field.
    metered: bool = false,
    conf: Config,

    /// Snapshot of the live limiter state, all counted in pages
    /// (multiply by `vfs.page_size` for bytes).
    pub const Status = struct {
        pages_per_sec: i64,
        metered: bool,
        total_read_pages: i64,
        total_write_pages: i64,
    };

    pub fn status(self: *const Limiter) Status {
        return .{
            .pages_per_sec = self.pages_per_sec,
            .metered = self.metered,
            .total_read_pages = self.total_read_pages.load(.monotonic),
            .total_write_pages = self.total_write_pages.load(.monotonic),
        };
    }

    pub fn init(config: Config) Limiter {
        return .{
            .pages_per_sec = @intCast(config.bytes_per_sec / _vfs.page_size),
            .conf = config,
        };
    }

    /// Pays for the whole request before the I/O starts, at the
    /// calling fiber's class and tenant, waiting as the meter says. A
    /// write costs `write_cost` pages per page.
    pub fn grant(self: *Limiter, bytes: usize, kind: _vfs.IOKind) void {
        if (bytes == 0) return;
        const wm: i64 = if (kind == .write) self.conf.write_cost else 1;
        const pages: i64 = @intCast(
            std.math.divCeil(usize, bytes, _vfs.page_size) catch unreachable,
        );
        if (self.metered) {
            const ctx = fiber.ctx();
            ctx.tenant.acquire(.io, ctx.class, pages * wm);
        }
        const counter = switch (kind) {
            .read => &self.total_read_pages,
            .write => &self.total_write_pages,
        };
        _ = counter.fetchAdd(pages, .monotonic);
    }
};
