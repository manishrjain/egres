// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The server's log line:
//!
//!     2026-10-22T14:05:31.123456Z INFO  raft: become-leader tick=12 ...
//!
//! Time first (UTC, RFC 3339, microseconds) so lines sort and compare
//! across boxes; the level upper-case and padded so messages line up;
//! then the scope. Installed through `std_options.logFn` in main.zig
//! in place of std.log.defaultLog, whose `info(scope): msg` form has
//! no time. No colour: the log is a file. Scripts that count levels
//! match the second field (`^[^ ]+ ERROR `).
//!
//! A line is formatted on the caller's stack and copied into a ring;
//! one writer thread drains the ring to stderr. The log is a file on
//! a busy disk, and an append into a page under writeback waits for
//! it: with the write on the caller, every fiber worker that logged
//! queued behind that wait on the stderr lock, 20 to 45 ms at a time.
//! Before `start`, and in tests, a line goes to stderr on the caller
//! as std.log does.

const std = @import("std");
const date = @import("date.zig");
const String = @import("utils.zig").String;

/// A line past this is cut; the newline stays.
const line_max = 1024;
const ring_bytes = 8 << 20;

pub fn logFn(
    comptime level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: String,
    args: anytype,
) void {
    // The real clock through the debug io: the same read as
    // `timestampNow`, but usable before the runtime and its process
    // context exist, where the first lines of main print.
    const io = std.Options.debug_io;
    const us = std.Io.Timestamp.now(io, .real).toMicroseconds();
    var buf: [line_max]u8 = undefined;
    var w = std.Io.Writer.fixed(&buf);
    writeLine(&w, us, level, scope, format, args) catch {};
    const line = terminated(&buf, w.buffered().len);
    if (sink.load(.acquire)) |s| return s.push(line);
    writeStderr(line);
}

/// The first `n` bytes of `buf` as a line: a cut line, one that filled
/// the buffer before its newline, gives its last byte to one.
fn terminated(buf: []u8, n: usize) String {
    if (n > 0 and buf[n - 1] == '\n') return buf[0..n];
    const end = @min(n + 1, buf.len);
    buf[end - 1] = '\n';
    return buf[0..end];
}

pub fn writeLine(
    w: *std.Io.Writer,
    unix_us: i64,
    comptime level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: String,
    args: anytype,
) !void {
    try date.formatTimestampUtc(w, unix_us);
    const prefix = comptime " " ++ levelText(level) ++ " " ++
        (if (scope == .default) "egres" else @tagName(scope)) ++ ": ";
    try w.writeAll(prefix);
    try w.print(format ++ "\n", args);
}

/// Five wide, so the scope starts in the same column on every line.
fn levelText(comptime level: std.log.Level) String {
    return switch (level) {
        .err => "ERROR",
        .warn => "WARN ",
        .info => "INFO ",
        .debug => "DEBUG",
    };
}

fn writeStderr(bytes: String) void {
    const io = std.Options.debug_io;
    const prev = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(prev);

    var buffer: [256]u8 = undefined;
    const w = &std.debug.lockStderr(&buffer).file_writer.interface;
    defer std.debug.unlockStderr();
    w.writeAll(bytes) catch {};
}

/// Bytes of whole lines waiting for the writer, oldest first, wrapping
/// at the end of `buf`. A line that does not fit is dropped and
/// counted, so a caller never waits for the disk.
pub const Ring = struct {
    buf: []u8,
    head: usize = 0,
    len: usize = 0,
    dropped: u64 = 0,

    /// False when the line was dropped.
    pub fn push(self: *Ring, line: String) bool {
        if (line.len > self.buf.len - self.len) {
            self.dropped += 1;
            return false;
        }
        const tail = (self.head + self.len) % self.buf.len;
        const first = @min(line.len, self.buf.len - tail);
        @memcpy(self.buf[tail..][0..first], line[0..first]);
        @memcpy(self.buf[0 .. line.len - first], line[first..]);
        self.len += line.len;
        return true;
    }

    /// The oldest bytes that lie contiguous in `buf`; empty when there
    /// are none. `advance` releases them once written.
    pub fn peek(self: *const Ring) String {
        const n = @min(self.len, self.buf.len - self.head);
        return self.buf[self.head..][0..n];
    }

    pub fn advance(self: *Ring, n: usize) void {
        std.debug.assert(n <= self.len);
        self.head = (self.head + n) % self.buf.len;
        self.len -= n;
    }
};

/// The ring and the thread that drains it. Callers push under the
/// mutex and signal; the drainer writes outside the mutex, so a slow
/// write holds nobody but itself.
const Sink = struct {
    allocator: std.mem.Allocator,
    threaded: std.Io.Threaded = .init_single_threaded,
    mu: std.Io.Mutex = .init,
    cond: std.Io.Condition = .init,
    ring: Ring,
    stopping: bool = false,
    thread: std.Thread = undefined,

    fn io(self: *Sink) std.Io {
        return self.threaded.io();
    }

    fn push(self: *Sink, line: String) void {
        self.mu.lockUncancelable(self.io());
        defer self.mu.unlock(self.io());
        if (self.ring.push(line)) self.cond.signal(self.io());
    }

    fn drain(self: *Sink) void {
        var dropped_buf: [128]u8 = undefined;
        while (true) {
            self.mu.lockUncancelable(self.io());
            while (self.ring.len == 0 and !self.stopping)
                self.cond.waitUncancelable(self.io(), &self.mu);

            const chunk = self.ring.peek();
            const dropped = self.ring.dropped;
            if (chunk.len == 0 and dropped == 0 and self.stopping) {
                self.mu.unlock(self.io());
                return;
            }
            self.mu.unlock(self.io());

            writeStderr(chunk);

            self.mu.lockUncancelable(self.io());
            self.ring.advance(chunk.len);

            // Reported once the ring has drained: the lines around the
            // drop were the ones that did not fit, and the count stays
            // exact while callers keep dropping.
            const report = self.ring.len == 0 and self.ring.dropped > 0;
            const count = self.ring.dropped;
            if (report) self.ring.dropped = 0;
            self.mu.unlock(self.io());

            if (report) {
                const dio = std.Options.debug_io;
                const us = std.Io.Timestamp.now(dio, .real).toMicroseconds();
                var w = std.Io.Writer.fixed(&dropped_buf);
                writeLine(&w, us, .warn, .default, "log: {d} lines dropped," ++
                    " the ring was full", .{count}) catch {};
                writeStderr(w.buffered());
            }
        }
    }

    /// Writes what the ring holds from the calling thread, for a
    /// process that is about to end without `stop`.
    fn flush(self: *Sink) void {
        self.mu.lockUncancelable(self.io());
        defer self.mu.unlock(self.io());

        while (self.ring.len > 0) {
            const chunk = self.ring.peek();
            writeStderr(chunk);
            self.ring.advance(chunk.len);
        }
    }
};

var sink: std.atomic.Value(?*Sink) = .init(null);

/// Starts the writer thread; every line from here on goes through the
/// ring. Once per process, before the runtime's workers start.
pub fn start(allocator: std.mem.Allocator) !void {
    std.debug.assert(sink.load(.acquire) == null);
    const s = try allocator.create(Sink);
    errdefer allocator.destroy(s);

    const buf = try allocator.alloc(u8, ring_bytes);
    errdefer allocator.free(buf);

    s.* = .{ .allocator = allocator, .ring = .{ .buf = buf } };
    s.thread = try std.Thread.spawn(.{}, Sink.drain, .{s});
    sink.store(s, .release);
}

/// Every buffered line to stderr, on the caller: for a panic or an
/// exit that skips `stop`, so the lines before it are not lost.
pub fn flush() void {
    const s = sink.load(.acquire) orelse return;
    s.flush();
}

/// Writes what the ring holds and joins the writer. Lines logged after
/// this go to stderr on the caller again.
pub fn stop() void {
    const s = sink.swap(null, .acq_rel) orelse return;
    s.mu.lockUncancelable(s.io());
    s.stopping = true;
    s.cond.signal(s.io());
    s.mu.unlock(s.io());
    s.thread.join();
    s.allocator.free(s.ring.buf);
    s.allocator.destroy(s);
}
