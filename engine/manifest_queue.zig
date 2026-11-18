// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The manifest changes waiting to enter the log. One fiber, the
//! engine's executor, takes them one at a time, so a change is built
//! or checked against the trees every earlier change was applied to,
//! and nobody holds a lock across a raft round trip. A foreground
//! request, a user's DDL or the flush that unblocks writes, goes
//! ahead of every background one, a compaction or a split, however
//! many of those are waiting: there is no lock to barge.
//!
//! A flush's build runs for minutes on the routes of one moment, and
//! a split applied meanwhile would narrow a tree the flush still
//! writes whole. So a flush brackets its build with `flush_begin` and
//! `flush_end`, and a split is left in the queue in between.

const std = @import("std");
const fiber = @import("zfiber");
const zdb = @import("zdb");
const utils = @import("raft").utils;

const _manifest = zdb.manifest;
const TreeKey = zdb.kv.TreeKey;

pub const Class = enum { fg, bg };

pub const Kind = union(enum) {
    /// Built already: checked against the trees and proposed as it is.
    change: _manifest.ManifestChange,
    /// A compaction's tables, built already. A `move` re-lists one
    /// table a level down, two records and no I/O: the executor
    /// proposes every move waiting in one entry (`takeMoves`).
    compaction: Compaction,
    /// A flush's tables. The executor keeps those whose tree is still
    /// listed, `kept` of them moved to the front, and proposes those;
    /// the submitter removes the rest.
    flush: Flush,
    flush_begin,
    flush_end,
    /// Built by the executor, from the trees as they are then.
    drop_tables: DropTables,
    drop_ns: u64,
    split: TreeKey,

    pub const Flush = struct {
        changes: []_manifest.SSTableChange,
        memtable_id: u64,
        kept: usize = 0,
    };

    pub const Compaction = struct {
        changes: []_manifest.SSTableChange,
        move: bool,
    };

    pub const DropTables = struct {
        namespace: u64,
        table_ids: []const u32,
        kvs: []zdb.buffers.KeyValueStruct,
    };
};

/// One submitter's request, on its own stack for as long as it waits.
pub const Request = struct {
    class: Class,
    kind: Kind,
    queued_ms: i64 = 0,
    result: anyerror!void = {},
    done: fiber.Notify = .{},
};

pub const Status = struct {
    waiting: [2]u32,
    oldest_wait_ms: i64,
    flush_building: bool,
};

pub const ManifestQueue = struct {
    allocator: std.mem.Allocator,
    mu: fiber.Mutex(.tracked) = .{},
    /// The executor waits here for a request it can take.
    cond: fiber.Condition = .{},
    /// Per class, in the order submitted. A few at most: each
    /// compactor, the flush, a DDL or two.
    waiting: [2]std.ArrayList(*Request) = .{ .empty, .empty },
    flush_building: bool = false,
    /// Set once at shutdown: the executor is gone, so nothing queued
    /// would ever run.
    closed: bool = false,

    pub fn init(allocator: std.mem.Allocator) ManifestQueue {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *ManifestQueue) void {
        for (&self.waiting) |*list| list.deinit(self.allocator);
        self.mu.deinit();
    }

    /// Queues `req` and parks until the executor has run it; the
    /// result is the executor's. `error.Shutdown` once closed.
    pub fn submit(self: *ManifestQueue, req: *Request) anyerror!void {
        {
            self.mu.lock(@src());
            defer self.mu.unlock();
            if (self.closed) return error.Shutdown;

            req.queued_ms = utils.coarseMs();
            try self.waiting[@intFromEnum(req.class)].append(
                self.allocator,
                req,
            );
        }
        self.cond.signal();
        req.done.wait();
        return req.result;
    }

    /// The next request to run, the executor's alone: a foreground
    /// one first, else a background one that is not a split while a
    /// flush builds. Parks until there is one; null once closed.
    pub fn take(self: *ManifestQueue) ?*Request {
        self.mu.lock(@src());
        defer self.mu.unlock();

        while (!self.closed) {
            const fg = &self.waiting[@intFromEnum(Class.fg)];
            if (fg.items.len > 0) return fg.orderedRemove(0);

            const bg = &self.waiting[@intFromEnum(Class.bg)];
            for (bg.items, 0..) |req, i| {
                if (self.flush_building and req.kind == .split) continue;
                return bg.orderedRemove(i);
            }
            // Couldn't pick anything. Wait until a new submit is called.
            self.cond.wait(&self.mu);
        }
        return null;
    }

    /// Every background move waiting, out of the queue and appended
    /// to `out`, oldest first: the executor proposes them in one entry
    /// with the move it took, and completes each.
    pub fn takeMoves(
        self: *ManifestQueue,
        out: *std.ArrayList(*Request),
    ) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();

        const bg = &self.waiting[@intFromEnum(Class.bg)];
        var i: usize = 0;
        while (i < bg.items.len) {
            const req = bg.items[i];
            if (!isMove(req)) {
                i += 1;
                continue;
            }
            try out.append(self.allocator, req);
            _ = bg.orderedRemove(i);
        }
    }

    fn isMove(req: *const Request) bool {
        return switch (req.kind) {
            .compaction => |c| c.move,
            else => false,
        };
    }

    /// Hands the result to the request's submitter.
    pub fn complete(req: *Request, result: anyerror!void) void {
        req.result = result;
        req.done.notify();
    }

    /// Whether a flush is between its `flush_begin` and `flush_end`;
    /// the executor sets it as it runs the two. Clearing it may make a
    /// split runnable for a `take` already parked.
    pub fn setFlushBuilding(self: *ManifestQueue, on: bool) void {
        self.mu.lock(@src());
        self.flush_building = on;
        self.mu.unlock();
        if (!on) self.cond.signal();
    }

    /// Ends the queue: the executor's `take` returns null, every
    /// request waiting fails with `error.Shutdown`, and so does every
    /// submit from now on.
    pub fn close(self: *ManifestQueue) void {
        self.mu.lock(@src());
        self.closed = true;
        for (&self.waiting) |*list| {
            for (list.items) |req| complete(req, error.Shutdown);
            list.clearRetainingCapacity();
        }
        self.mu.unlock();
        self.cond.broadcast();
    }

    pub fn status(self: *ManifestQueue) Status {
        self.mu.lock(@src());
        defer self.mu.unlock();
        const now = utils.coarseMs();
        var oldest: i64 = 0;
        for (self.waiting) |list| {
            if (list.items.len > 0)
                oldest = @max(oldest, now - list.items[0].queued_ms);
        }
        return .{
            .waiting = .{
                @intCast(self.waiting[0].items.len),
                @intCast(self.waiting[1].items.len),
            },
            .oldest_wait_ms = oldest,
            .flush_building = self.flush_building,
        };
    }
};
