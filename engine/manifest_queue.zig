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
//!
//! A change whose build takes long, a flush's or a compaction's
//! tables, is `begin`un before the build and submitted after, so the
//! queue also knows what is being built: the admin dump lists it and
//! the watchdog warns when a build or a wait has gone on too long.

const std = @import("std");
const fiber = @import("zfiber");
const zdb = @import("zdb");
const utils = @import("raft").utils;

const log = std.log.scoped(.manifest_queue);
const _manifest = zdb.manifest;
const TreeKey = zdb.kv.TreeKey;

pub const Class = enum { fg, bg };

/// A flush or a new memtable works on no one tree.
pub const no_tree: TreeKey = .{ .namespace = 0, .tree_id = 0 };

pub const Kind = union(enum) {
    /// Built already: checked against the trees and proposed as it is.
    change: _manifest.ManifestChange,
    /// A compaction's tables, filled in once built. A `move` re-lists
    /// one table a level down, two records and no I/O: the executor
    /// proposes every move waiting in one entry (`takeMoves`).
    compaction: Compaction,
    /// A flush's tables, filled in once built. The executor keeps those
    /// whose tree is still listed, `kept` of them moved to the front,
    /// and proposes those; the submitter removes the rest.
    flush: Flush,
    flush_begin,
    flush_end,
    /// Built by the executor, from the trees as they are then.
    drop_tables: DropTables,
    drop_ns: u64,
    split: TreeKey,

    pub const Flush = struct {
        changes: []_manifest.SSTableChange = &.{},
        memtable_id: u64 = 0,
        kept: usize = 0,
    };

    pub const Compaction = struct {
        changes: []_manifest.SSTableChange = &.{},
        move: bool,
        /// The compaction's id in its own log lines, so a warning
        /// about it can be matched to them.
        cid: u64 = 0,
        src_level: u8 = 0,
    };

    pub const DropTables = struct {
        namespace: u64,
        table_ids: []const u32,
        kvs: []zdb.buffers.KeyValueStruct,
    };
};

/// One submitter's request, on its own stack for as long as it is
/// built or waits.
pub const Request = struct {
    class: Class,
    kind: Kind,
    tree: TreeKey = no_tree,
    /// When it was begun, or submitted if it never was.
    started_ms: i64 = 0,
    result: anyerror!void = {},
    done: fiber.Notify = .{},
};

pub const Status = struct {
    building: u32,
    waiting: [2]u32,
    oldest_wait_ms: i64,
    flush_building: bool,
};

pub const ManifestQueue = struct {
    allocator: std.mem.Allocator,
    mu: fiber.Mutex(.tracked) = .{},
    /// The executor waits here for a request it can take.
    cond: fiber.Condition = .{},
    /// Begun and not yet submitted: each compactor's job and the
    /// flush, their tables still being written.
    building: std.ArrayList(*Request) = .empty,
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
        self.building.deinit(self.allocator);
        for (&self.waiting) |*list| list.deinit(self.allocator);
        self.mu.deinit();
    }

    /// A request whose build starts now, for the dump and the
    /// watchdog; `submit` or `end` ends the build. Diagnostic only, so
    /// no room to list it costs the build nothing.
    pub fn begin(self: *ManifestQueue, req: *Request) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        req.started_ms = utils.coarseMs();
        self.building.append(self.allocator, req) catch {};
    }

    /// The end of a build begun, on every path out of it: nothing
    /// when `submit` ended it already.
    pub fn end(self: *ManifestQueue, req: *Request) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        self.unbuild(req);
    }

    fn unbuild(self: *ManifestQueue, req: *Request) void {
        for (self.building.items, 0..) |b, i| {
            if (b == req) {
                _ = self.building.orderedRemove(i);
                return;
            }
        }
    }

    /// Queues `req` and parks until the executor has run it; the
    /// result is the executor's. `error.Shutdown` once closed.
    pub fn submit(self: *ManifestQueue, req: *Request) anyerror!void {
        {
            self.mu.lock(@src());
            defer self.mu.unlock();
            self.unbuild(req);
            if (self.closed) return error.Shutdown;

            if (req.started_ms == 0) req.started_ms = utils.coarseMs();
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
    /// submit from now on. A build under way finds out at its submit.
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
                oldest = @max(oldest, now - list.items[0].started_ms);
        }
        return .{
            .building = @intCast(self.building.items.len),
            .waiting = .{
                @intCast(self.waiting[0].items.len),
                @intCast(self.waiting[1].items.len),
            },
            .oldest_wait_ms = oldest,
            .flush_building = self.flush_building,
        };
    }

    /// One request as the dump and the watchdog describe it.
    const Line = struct {
        phase: []const u8,
        kind: []const u8,
        tree: TreeKey,
        src_level: u8,
        cid: u64,
        age_ms: i64,

        fn of(phase: []const u8, req: *const Request, now: i64) Line {
            var line: Line = .{
                .phase = phase,
                .kind = @tagName(req.kind),
                .tree = req.tree,
                .src_level = 0,
                .cid = 0,
                .age_ms = now - req.started_ms,
            };
            if (req.kind == .compaction) {
                line.src_level = req.kind.compaction.src_level;
                line.cid = req.kind.compaction.cid;
            }
            return line;
        }
    };

    /// Appends what is being built and what waits, oldest first. A
    /// `compactions wanted` row in the forest stats with nothing
    /// building here means the compaction loop is starved; a build
    /// many seconds old is slow.
    pub fn write(self: *ManifestQueue, w: *std.Io.Writer) !void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        const now = utils.coarseMs();
        try w.print("\n  manifest changes{s}:\n", .{
            if (self.flush_building) " (flush building)" else "",
        });
        var any = false;
        for (self.building.items) |req| {
            try writeLine(w, Line.of("building", req, now));
            any = true;
        }
        for (self.waiting) |list| for (list.items) |req| {
            try writeLine(w, Line.of("waiting", req, now));
            any = true;
        };
        if (!any) try w.print("    (none)\n", .{});
    }

    fn writeLine(w: *std.Io.Writer, line: Line) !void {
        try w.print("    {s} {s}", .{ line.phase, line.kind });
        if (line.tree.namespace != 0) try w.print(
            " tree={d}:{d}",
            .{ line.tree.namespace, line.tree.tree_id },
        );
        if (line.cid != 0) try w.print(
            " L{d} cid={d}",
            .{ line.src_level, line.cid },
        );
        try w.print(" age_ms={d}\n", .{line.age_ms});
    }

    /// Warns about every request built or waiting for longer than
    /// `threshold_ms`: wherever the wedge is, the request stops
    /// ageing out.
    pub fn warnStuck(self: *ManifestQueue, threshold_ms: i64) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        const now = utils.coarseMs();
        for (self.building.items) |req| {
            warnLine(Line.of("building", req, now), threshold_ms);
        }
        for (self.waiting) |list| for (list.items) |req| {
            warnLine(Line.of("waiting", req, now), threshold_ms);
        };
    }

    fn warnLine(line: Line, threshold_ms: i64) void {
        if (line.age_ms < threshold_ms) return;
        log.warn("{s} {s} stuck: tree={d}:{d} L{d} cid={d} age_ms={d}", .{
            line.phase,        line.kind,      line.tree.namespace,
            line.tree.tree_id, line.src_level, line.cid,
            line.age_ms,
        });
    }
};
