//! TCP networking that cooperatively parks the calling fiber on libxev
//! completions. The public surface mirrors `zio.net` so the rest of the
//! codebase migrates by swapping `zio.net` → `fiber.net`:
//!
//!   - `IpAddress.parseIpAndPort` / `.listen(opts)` / `.connect(opts)`
//!   - `Server.accept(opts)` / `.shutdown` / `.close`
//!   - `Stream.read` / `.readAll` / `.write` / `.writeAll` / `.shutdown`
//!     / `.close`, plus the buffered `reader(buf)` / `writer(buf)`
//!     adapters that expose a `std.Io.Reader` / `std.Io.Writer`
//!     (`interface`) — this is what the pgwire and zdb protocol stacks
//!     are written against.
//!
//! Unlike zio, the read/write methods take no timeout argument (fiber has
//! no per-op cancellation yet) — call sites drop the trailing timeout.
//! The Reader/Writer adapters do single-buffer socket reads/writes rather
//! than vectored I/O (libxev's TCP watcher takes one buffer per op); the
//! `std.Io.Writer` buffer coalesces small writes so framed sends still go
//! out in one syscall on flush.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const xev = @import("xev");
const fb = @import("fiber.zig");
const rt = @import("runtime.zig");
const fsops = @import("fsops.zig");

const Fiber = fb.Fiber;
const Sionet = std.Io.net;

/// Put a socket fd into non-blocking mode. `std.Io.net`'s listen socket is
/// blocking, but libxev drives readiness-based backends (epoll/kqueue/iocp)
/// which require non-blocking fds — `xev.TCP.init` sets this itself, so we
/// must match for the std.Io-created listen fd. io_uring tolerates blocking
/// fds, so it's a harmless no-op effect there (still issued, so this path is
/// exercised on the io_uring build).
fn setNonBlocking(fd: Sionet.Socket.Handle) !void {
    if (builtin.os.tag == .windows) {
        const ws2 = std.os.windows.ws2_32;
        var mode: c_ulong = 1;
        if (ws2.ioctlsocket(fd, ws2.FIONBIO, &mode) != 0) return error.SetNonBlockingFailed;
        return;
    }
    // `std.posix.fcntl` is stripped in this build; use the system fcntl +
    // `posix.errno` (correct here because system fcntl returns -1 on error,
    // unlike raw `std.os.linux.*`).
    const cur = posix.system.fcntl(fd, posix.F.GETFL, @as(u32, 0));
    switch (posix.errno(cur)) {
        .SUCCESS => {},
        else => return error.SetNonBlockingFailed,
    }
    var o: posix.O = @bitCast(@as(u32, @intCast(cur)));
    o.NONBLOCK = true;
    switch (posix.errno(posix.system.fcntl(fd, posix.F.SETFL, @as(u32, @bitCast(o))))) {
        .SUCCESS => {},
        else => return error.SetNonBlockingFailed,
    }
}

/// Direction passed to `shutdown`.
pub const ShutdownHow = std.Io.net.ShutdownHow;

// Socket close/shutdown go through `std.Io.net` (portable — e.g. closesocket
// on Windows) on the pool's blocking io. A minimal `Stream` carries just the
// handle; `address` is never read by close/shutdown.
fn socketClose(fd: Sionet.Socket.Handle) void {
    var s: Sionet.Stream = .{ .socket = .{ .handle = fd, .address = undefined } };
    s.close(fsops.io());
}

fn socketShutdown(fd: Sionet.Socket.Handle, how: ShutdownHow) !void {
    var s: Sionet.Stream = .{ .socket = .{ .handle = fd, .address = undefined } };
    return s.shutdown(fsops.io(), how);
}

// ───────────────────────── IpAddress ─────────────────────────

pub const IpAddress = struct {
    inner: std.Io.net.IpAddress,

    pub const ListenOptions = struct {
        reuse_address: bool = false,
        backlog: u31 = 128,
    };
    /// `timeout` accepted for zio API parity but ignored today (fiber has
    /// no per-op cancellation — TODO: honor a connect deadline).
    pub const ConnectOptions = struct {};
    pub const AcceptOptions = struct {};

    pub fn parseIpAndPort(name: []const u8) !IpAddress {
        return .{ .inner = try std.Io.net.IpAddress.parseLiteral(name) };
    }

    pub fn listen(self: IpAddress, options: ListenOptions) !Server {
        // Create + bind + listen via std.Io.net — portable, sets SO_REUSEADDR,
        // and resolves an ephemeral (port 0) bind into `socket.address`. Then
        // drive accept asynchronously through libxev on the resulting fd.
        const srv = try self.inner.listen(fsops.io(), .{
            .reuse_address = options.reuse_address,
            .kernel_backlog = options.backlog,
        });
        errdefer socketClose(srv.socket.handle);
        // std.Io.net's listen socket is blocking; xev's readiness backends
        // need it non-blocking. (Accepted sockets are xev's own domain.)
        try setNonBlocking(srv.socket.handle);
        return .{ .tcp = xev.TCP.initFd(srv.socket.handle), .bound_addr = srv.socket.address };
    }

    pub fn connect(self: IpAddress, options: ConnectOptions) !Stream {
        _ = options;
        var tcp = try xev.TCP.init(self.inner);
        errdefer socketClose(tcp.fd);

        const w = rt.currentWorker();
        const f = w.current_fiber.?;
        var c: xev.Completion = undefined;
        var waiter: ConnectWaiter = .{ .fiber = f, .result = {} };
        tcp.connect(&w.loop, &c, self.inner, ConnectWaiter, &waiter, connectCallback);
        rt.park();
        try waiter.result;
        return .{ .tcp = tcp };
    }
};

// ───────────────────────── Server ─────────────────────────

pub const Server = struct {
    tcp: xev.TCP,
    bound_addr: std.Io.net.IpAddress,

    pub fn accept(self: Server, options: IpAddress.AcceptOptions) !Stream {
        _ = options;
        const w = rt.currentWorker();
        const f = w.current_fiber.?;
        var c: xev.Completion = undefined;
        var waiter: AcceptWaiter = .{ .fiber = f, .result = undefined };
        self.tcp.accept(&w.loop, &c, AcceptWaiter, &waiter, acceptCallback);
        rt.park();
        const tcp = try waiter.result;
        return .{ .tcp = tcp };
    }

    pub fn shutdown(self: Server, how: ShutdownHow) !void {
        try socketShutdown(self.tcp.fd, how);
    }

    pub fn close(self: Server) void {
        socketClose(self.tcp.fd);
    }
};

// ───────────────────────── Stream ─────────────────────────

pub const Stream = struct {
    tcp: xev.TCP,

    /// Read up to `buf.len` bytes. Returns 0 only for a zero-length
    /// `buf`; end-of-stream surfaces as `error.EOF`.
    pub fn read(self: Stream, buf: []u8) xev.ReadError!usize {
        return socketRead(self.tcp, buf);
    }

    /// Read exactly `buf.len` bytes. `error.EndOfStream` if the peer
    /// closes before the buffer is filled.
    pub fn readAll(self: Stream, buf: []u8) (xev.ReadError || error{EndOfStream})!void {
        var off: usize = 0;
        while (off < buf.len) {
            const n = socketRead(self.tcp, buf[off..]) catch |err| switch (err) {
                error.EOF => return error.EndOfStream,
                else => |e| return e,
            };
            off += n;
        }
    }

    pub fn write(self: Stream, buf: []const u8) xev.WriteError!usize {
        return socketWrite(self.tcp, buf);
    }

    pub fn writeAll(self: Stream, buf: []const u8) xev.WriteError!void {
        var off: usize = 0;
        while (off < buf.len) {
            off += try socketWrite(self.tcp, buf[off..]);
        }
    }

    pub fn shutdown(self: Stream, how: ShutdownHow) !void {
        try socketShutdown(self.tcp.fd, how);
    }

    pub fn close(self: Stream) void {
        socketClose(self.tcp.fd);
    }

    /// Buffered reader exposing a `std.Io.Reader` via `.interface`.
    pub fn reader(self: Stream, buffer: []u8) Reader {
        return Reader.init(self.tcp, buffer);
    }

    /// Buffered writer exposing a `std.Io.Writer` via `.interface`.
    pub fn writer(self: Stream, buffer: []u8) Writer {
        return Writer.init(self.tcp, buffer);
    }

    // ── std.Io adapters (mirror zio.net.Stream.Reader/Writer) ──

    pub const Reader = struct {
        tcp: xev.TCP,
        interface: std.Io.Reader,
        err: ?xev.ReadError = null,

        pub fn init(tcp: xev.TCP, buffer: []u8) Reader {
            return .{
                .tcp = tcp,
                .interface = .{
                    .vtable = &.{
                        .stream = streamImpl,
                        .readVec = readVecImpl,
                    },
                    .buffer = buffer,
                    .seek = 0,
                    .end = 0,
                },
            };
        }

        fn readVecImpl(io_r: *std.Io.Reader, data: [][]u8) std.Io.Reader.Error!usize {
            const r: *Reader = @alignCast(@fieldParentPtr("interface", io_r));
            // Prefer reading directly into the first non-empty caller
            // buffer (no copy for large reads); otherwise fill the
            // interface's own buffer and return 0 (the VTable.readVec
            // contract permits this — used by delimiter scans).
            var dest: []u8 = undefined;
            var into_user = false;
            if (data.len > 0 and data[0].len > 0) {
                dest = data[0];
                into_user = true;
            } else {
                dest = io_r.buffer[io_r.end..];
                if (dest.len == 0) return 0;
            }
            const n = socketRead(r.tcp, dest) catch |err| switch (err) {
                error.EOF => return error.EndOfStream,
                else => |e| {
                    r.err = e;
                    return error.ReadFailed;
                },
            };
            if (into_user) return n;
            io_r.end += n;
            return 0;
        }

        fn streamImpl(
            io_r: *std.Io.Reader,
            io_w: *std.Io.Writer,
            limit: std.Io.Limit,
        ) std.Io.Reader.StreamError!usize {
            const dest = limit.slice(try io_w.writableSliceGreedy(1));
            var data: [1][]u8 = .{dest};
            const n = try readVecImpl(io_r, &data);
            io_w.advance(n);
            return n;
        }
    };

    pub const Writer = struct {
        tcp: xev.TCP,
        interface: std.Io.Writer,
        err: ?xev.WriteError = null,

        pub fn init(tcp: xev.TCP, buffer: []u8) Writer {
            return .{
                .tcp = tcp,
                .interface = .{
                    .vtable = &.{ .drain = drainImpl },
                    .buffer = buffer,
                },
            };
        }

        fn drainImpl(
            io_w: *std.Io.Writer,
            data: []const []const u8,
            splat: usize,
        ) std.Io.Writer.Error!usize {
            const w: *Writer = @alignCast(@fieldParentPtr("interface", io_w));
            var total: usize = 0;
            // 1. Flush the writer's own buffer (coalesced small writes).
            const buffered = io_w.buffered();
            if (buffered.len > 0) {
                writeAllTcp(w.tcp, buffered) catch |err| {
                    w.err = err;
                    return error.WriteFailed;
                };
                total += buffered.len;
            }
            // 2. Then each explicit slice; the last repeated `splat` times.
            if (data.len > 0) {
                for (data[0 .. data.len - 1]) |slice| {
                    if (slice.len == 0) continue;
                    writeAllTcp(w.tcp, slice) catch |err| {
                        w.err = err;
                        return error.WriteFailed;
                    };
                    total += slice.len;
                }
                const last = data[data.len - 1];
                if (last.len > 0) {
                    var i: usize = 0;
                    while (i < splat) : (i += 1) {
                        writeAllTcp(w.tcp, last) catch |err| {
                            w.err = err;
                            return error.WriteFailed;
                        };
                        total += last.len;
                    }
                }
            }
            return io_w.consume(total);
        }
    };
};

// ───────────────────────── low-level socket ops ─────────────────────────

fn socketRead(tcp: xev.TCP, buf: []u8) xev.ReadError!usize {
    const w = rt.currentWorker();
    const f = w.current_fiber.?;
    var c: xev.Completion = undefined;
    var waiter: ReadWaiter = .{ .fiber = f, .result = 0 };
    tcp.read(&w.loop, &c, .{ .slice = buf }, ReadWaiter, &waiter, readCallback);
    rt.park();
    return waiter.result;
}

fn socketWrite(tcp: xev.TCP, buf: []const u8) xev.WriteError!usize {
    const w = rt.currentWorker();
    const f = w.current_fiber.?;
    var c: xev.Completion = undefined;
    var waiter: WriteWaiter = .{ .fiber = f, .result = 0 };
    tcp.write(&w.loop, &c, .{ .slice = buf }, WriteWaiter, &waiter, writeCallback);
    rt.park();
    return waiter.result;
}

fn writeAllTcp(tcp: xev.TCP, buf: []const u8) xev.WriteError!void {
    var off: usize = 0;
    while (off < buf.len) {
        off += try socketWrite(tcp, buf[off..]);
    }
}

const ReadWaiter = struct {
    fiber: *Fiber,
    result: xev.ReadError!usize,
};

fn readCallback(
    ud: ?*ReadWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.TCP,
    _: xev.ReadBuffer,
    r: xev.ReadError!usize,
) xev.CallbackAction {
    const w = ud.?;
    w.result = r;
    rt.wake(w.fiber);
    return .disarm;
}

const WriteWaiter = struct {
    fiber: *Fiber,
    result: xev.WriteError!usize,
};

fn writeCallback(
    ud: ?*WriteWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.TCP,
    _: xev.WriteBuffer,
    r: xev.WriteError!usize,
) xev.CallbackAction {
    const w = ud.?;
    w.result = r;
    rt.wake(w.fiber);
    return .disarm;
}

const ConnectWaiter = struct {
    fiber: *Fiber,
    result: xev.ConnectError!void,
};

fn connectCallback(
    ud: ?*ConnectWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    _: xev.TCP,
    r: xev.ConnectError!void,
) xev.CallbackAction {
    const w = ud.?;
    w.result = r;
    rt.wake(w.fiber);
    return .disarm;
}

const AcceptWaiter = struct {
    fiber: *Fiber,
    result: xev.AcceptError!xev.TCP,
};

fn acceptCallback(
    ud: ?*AcceptWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    r: xev.AcceptError!xev.TCP,
) xev.CallbackAction {
    const w = ud.?;
    w.result = r;
    rt.wake(w.fiber);
    return .disarm;
}

