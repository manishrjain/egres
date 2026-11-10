// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

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
//! Every socket operation is one `Op` handed to `submit`, as file
//! operations are in `fs.zig`: one function sees them all, and under a
//! sim it hands every op to `simnet.zig` instead of the kernel.
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
const _fb = @import("fiber.zig");
const _rt = @import("runtime.zig");
const _fsops = @import("fsops.zig");
const _context = @import("utils").context;
const String = @import("utils").String;

const net_log = std.log.scoped(.net);

const Fiber = _fb.Fiber;
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
    s.close(_fsops.io());
}

fn socketShutdown(fd: Sionet.Socket.Handle, how: ShutdownHow) !void {
    var s: Sionet.Stream = .{ .socket = .{ .handle = fd, .address = undefined } };
    return s.shutdown(_fsops.io(), how);
}

// ───────────────────────── Op / submit ─────────────────────────

/// Every socket operation there is, with its arguments; `submit` runs
/// it.
pub const Op = union(enum) {
    listen: Listen,
    connect: IpAddress,
    accept: Server,
    read: Read,
    write: Write,
    shutdown: Shutdown,
    close: Stream,
    close_server: Server,

    pub const Tag = std.meta.Tag(Op);

    /// The payload of one `Op`: what `submit(tag, ...)` takes.
    pub fn Args(comptime tag: Tag) type {
        return @FieldType(Op, @tagName(tag));
    }

    pub const Listen = struct {
        addr: IpAddress,
        options: IpAddress.ListenOptions,
    };
    pub const Read = struct { stream: Stream, buf: []u8 };
    pub const Write = struct { stream: Stream, data: String };
    pub const Shutdown = struct { stream: Stream, how: ShutdownHow };
};

/// What `submit(tag, ...)` returns. Any error, as `fs.Result`: the
/// kernel's arrive under std's and libxev's names, the simulated
/// network's under its own.
pub fn Result(comptime tag: Op.Tag) type {
    return switch (tag) {
        .listen => anyerror!Server,
        .connect, .accept => anyerror!Stream,
        .read, .write => anyerror!usize,
        .shutdown => anyerror!void,
        .close, .close_server => void,
    };
}

/// The one door: runs `tag` with `args` and returns its result. Must be
/// called on a fiber: connect, accept, read and write park.
pub fn submit(comptime tag: Op.Tag, args: Op.Args(tag)) Result(tag) {
    if (_rt.simNet()) |net| {
        return net.apply(tag, args);
    }
    switch (tag) {
        .listen => return listenImpl(args),
        .connect => return connectImpl(args),
        .accept => return acceptImpl(args),
        .read => return socketRead(args.stream.tcp, args.buf),
        .write => return socketWrite(args.stream.tcp, args.data),
        .shutdown => return socketShutdown(args.stream.tcp.fd, args.how),
        .close, .close_server => socketClose(args.tcp.fd),
    }
}

// ───────────────────────── IpAddress ─────────────────────────

/// An endpoint URL split into its parts (see `parseEndpoint`).
pub const Endpoint = struct {
    host: []const u8,
    port: u16,
    tls: bool,
};

/// Parse `http://host[:port]` / `https://host[:port]`. The scheme is
/// required — it carries the TLS decision and the default port (80/443).
/// The host may be a DNS name, an IPv4 literal, or a bracketed IPv6
/// literal (`http://[::1]:8333`). Slices point into `url`.
pub fn parseEndpoint(url: []const u8) error{BadEndpoint}!Endpoint {
    var rest: []const u8 = undefined;
    var tls: bool = undefined;
    if (std.mem.startsWith(u8, url, "http://")) {
        rest = url["http://".len..];
        tls = false;
    } else if (std.mem.startsWith(u8, url, "https://")) {
        rest = url["https://".len..];
        tls = true;
    } else return error.BadEndpoint;

    var host = rest;
    var port: u16 = if (tls) 443 else 80;
    if (rest.len > 0 and rest[0] == '[') {
        const close = std.mem.indexOfScalar(u8, rest, ']') orelse return error.BadEndpoint;
        host = rest[1..close];

        const after = rest[close + 1 ..];
        if (after.len > 0) {
            if (after[0] != ':') return error.BadEndpoint;
            port = std.fmt.parseInt(u16, after[1..], 10) catch return error.BadEndpoint;
        }
    } else if (std.mem.lastIndexOfScalar(u8, rest, ':')) |colon| {
        host = rest[0..colon];
        port = std.fmt.parseInt(u16, rest[colon + 1 ..], 10) catch return error.BadEndpoint;
    }
    if (host.len == 0) return error.BadEndpoint;
    return .{ .host = host, .port = port, .tls = tls };
}

/// Resolve `host` — an IP literal or a DNS name — to a dialable address.
/// Parse-first, so hex-y IPv6 literals are never mistaken for names; DNS
/// otherwise, with the blocking lookup offloaded to the fsops pool — the
/// calling fiber parks, its worker thread keeps running other fibers.
pub fn resolveHost(host: []const u8, port: u16) !IpAddress {
    if (std.Io.net.IpAddress.parse(host, port)) |addr| {
        return .{ .inner = addr };
    } else |_| {}

    return _fsops.run(lookupHost, .{ host, port });
}

/// The blocking half of `resolveHost`: runs on a pool thread.
fn lookupHost(host: []const u8, port: u16) !IpAddress {
    const io = _fsops.io();
    const name = std.Io.net.HostName.init(host) catch return error.BadHostName;
    var results: [16]std.Io.net.HostName.LookupResult = undefined;
    var q = std.Io.Queue(std.Io.net.HostName.LookupResult).init(&results);
    try name.lookup(io, &q, .{ .port = port });
    while (q.getOne(io) catch null) |res| switch (res) {
        .address => |addr| return .{ .inner = addr },
        .canonical_name => {},
    };
    return error.UnknownHostName;
}

// ──────────────────────── Client connection ────────────────────────

/// One client connection: the TCP stream plus its persistent buffered
/// reader/writer — and, under TLS, the session state (handshake done
/// once at connect, reused for the connection's lifetime). Heap-
/// allocated and never moved: the std.Io reader/writer interfaces are
/// address-sensitive. All socket I/O — TLS records included — flows
/// through the fiber Stream (fibers park; worker threads never block).
pub const Conn = struct {
    stream: Stream,
    tls: ?*TlsConn,
    plain_rbuf: [4096]u8,
    plain_wbuf: [4096]u8,
    stream_reader: Stream.Reader,
    stream_writer: Stream.Writer,

    const tls_buf_len = std.crypto.tls.Client.min_buffer_len;

    const TlsConn = struct {
        sock_rbuf: [tls_buf_len]u8,
        sock_wbuf: [tls_buf_len]u8,
        read_buf: [tls_buf_len]u8,
        write_buf: [tls_buf_len]u8,
        // Handshake entropy (240B): a field rather than a fiber-stack
        // local — this struct is heap anyway, and fiber stacks are kept
        // for small scratch only.
        entropy: [std.crypto.tls.Client.Options.entropy_len]u8,
        stream_reader: Stream.Reader,
        stream_writer: Stream.Writer,
        client: std.crypto.tls.Client,
    };

    /// TLS parameters for `connect`. Trust roots are CALLER-owned: the
    /// runtime dials and handshakes, but when to load or refresh a CA
    /// bundle is application policy.
    pub const TlsOptions = struct {
        /// Server name — SNI + certificate verification.
        host: []const u8,
        ca_bundle: *std.crypto.Certificate.Bundle,
        ca_lock: *std.Io.RwLock,
    };

    /// Dial `addr` and, when `tls_opts` is set, complete the TLS
    /// handshake before returning.
    pub fn connect(a: std.mem.Allocator, addr: IpAddress, tls_opts: ?TlsOptions) !*Conn {
        const stream = try addr.connect(.{});
        errdefer stream.close();
        const conn = try a.create(Conn);
        errdefer a.destroy(conn);
        conn.stream = stream;
        conn.tls = null;
        if (tls_opts) |opts| {
            const t = try a.create(TlsConn);
            errdefer a.destroy(t);
            t.stream_reader = stream.reader(&t.sock_rbuf);
            t.stream_writer = stream.writer(&t.sock_wbuf);
            const io = _context.g().io;
            io.random(&t.entropy);
            t.client = std.crypto.tls.Client.init(
                &t.stream_reader.interface,
                &t.stream_writer.interface,
                .{
                    .host = .{ .explicit = opts.host },
                    .ca = .{ .bundle = .{
                        .gpa = a,
                        .io = io,
                        .lock = opts.ca_lock,
                        .bundle = opts.ca_bundle,
                    } },
                    .write_buffer = &t.write_buf,
                    .read_buffer = &t.read_buf,
                    .entropy = &t.entropy,
                    .realtime_now = std.Io.Timestamp.now(io, .real),
                },
            ) catch |err| {
                net_log.err("tls handshake with {s} failed: {s}", .{ opts.host, @errorName(err) });
                return error.TlsHandshakeFailed;
            };
            conn.tls = t;
        } else {
            conn.stream_reader = stream.reader(&conn.plain_rbuf);
            conn.stream_writer = stream.writer(&conn.plain_wbuf);
        }
        return conn;
    }

    pub fn rd(self: *Conn) *std.Io.Reader {
        return if (self.tls) |t| &t.client.reader else &self.stream_reader.interface;
    }

    pub fn wr(self: *Conn) *std.Io.Writer {
        return if (self.tls) |t| &t.client.writer else &self.stream_writer.interface;
    }

    /// Flush through every layer down to the socket (the TLS writer
    /// drains records into the stream writer, which buffers too).
    pub fn flushAll(self: *Conn) !void {
        try self.wr().flush();
        if (self.tls) |t| try t.stream_writer.interface.flush();
    }

    pub fn deinit(self: *Conn, a: std.mem.Allocator) void {
        self.stream.close();
        if (self.tls) |t| a.destroy(t);
        a.destroy(self);
    }
};

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
        return submit(.listen, .{ .addr = self, .options = options });
    }

    pub fn connect(self: IpAddress, options: ConnectOptions) !Stream {
        _ = options;
        return submit(.connect, self);
    }
};

// ───────────────────────── Server ─────────────────────────

pub const Server = struct {
    tcp: xev.TCP,
    bound_addr: std.Io.net.IpAddress,

    pub fn accept(self: Server, options: IpAddress.AcceptOptions) !Stream {
        _ = options;
        return submit(.accept, self);
    }

    pub fn close(self: Server) void {
        submit(.close_server, self);
    }

    /// The actual bound port — useful after listening on port 0.
    pub fn port(self: Server) u16 {
        return self.bound_addr.getPort();
    }
};

// ───────────────────────── Stream ─────────────────────────

pub const Stream = struct {
    tcp: xev.TCP,

    /// Read up to `buf.len` bytes. Returns 0 only for a zero-length
    /// `buf`; end-of-stream surfaces as `error.EOF`.
    pub fn read(self: Stream, buf: []u8) !usize {
        return submit(.read, .{ .stream = self, .buf = buf });
    }

    /// Read exactly `buf.len` bytes. `error.EndOfStream` if the peer
    /// closes before the buffer is filled.
    pub fn readAll(self: Stream, buf: []u8) !void {
        var off: usize = 0;
        while (off < buf.len) {
            off += self.read(buf[off..]) catch |err| switch (err) {
                error.EOF => return error.EndOfStream,
                else => |e| return e,
            };
        }
    }

    pub fn write(self: Stream, buf: String) !usize {
        return submit(.write, .{ .stream = self, .data = buf });
    }

    pub fn writeAll(self: Stream, buf: String) !void {
        var off: usize = 0;
        while (off < buf.len) {
            off += try self.write(buf[off..]);
        }
    }

    pub fn shutdown(self: Stream, how: ShutdownHow) !void {
        return submit(.shutdown, .{ .stream = self, .how = how });
    }

    pub fn close(self: Stream) void {
        submit(.close, self);
    }

    /// Raw socket fd — for `setsockopt` and friends.
    pub fn handle(self: Stream) Sionet.Socket.Handle {
        return self.tcp.fd;
    }

    /// Disable Nagle's algorithm (TCP_NODELAY).
    pub fn setNoDelay(self: Stream, on: bool) !void {
        // A simulated stream has no socket to set options on.
        if (_rt.isSim()) return;
        const TCP = if (builtin.os.tag == .macos) std.c.TCP else std.os.linux.TCP;
        try posix.setsockopt(self.tcp.fd, posix.IPPROTO.TCP, TCP.NODELAY, &std.mem.toBytes(@as(c_int, if (on) 1 else 0)));
    }

    /// Enable / disable SO_KEEPALIVE.
    pub fn setKeepAlive(self: Stream, on: bool) !void {
        if (_rt.isSim()) return;
        try posix.setsockopt(self.tcp.fd, posix.SOL.SOCKET, posix.SO.KEEPALIVE, &std.mem.toBytes(@as(c_int, if (on) 1 else 0)));
    }

    /// Buffered reader exposing a `std.Io.Reader` via `.interface`.
    pub fn reader(self: Stream, buffer: []u8) Reader {
        return Reader.init(self, buffer);
    }

    /// Buffered writer exposing a `std.Io.Writer` via `.interface`.
    pub fn writer(self: Stream, buffer: []u8) Writer {
        return Writer.init(self, buffer);
    }

    // ── std.Io adapters (mirror zio.net.Stream.Reader/Writer) ──

    pub const Reader = struct {
        stream: Stream,
        interface: std.Io.Reader,

        pub fn init(stream: Stream, buffer: []u8) Reader {
            return .{
                .stream = stream,
                .interface = .{
                    .vtable = &.{ .stream = streamImpl },
                    .buffer = buffer,
                    .seek = 0,
                    .end = 0,
                },
            };
        }

        // `stream` is the one required Reader vtable method; std's default
        // `readVec`/`discard` route through it. For a large caller buffer the
        // default streams straight into it (no copy); for small reads it fills
        // `interface.buffer` — both end up here as a single `writableSliceGreedy`
        // destination, matching libxev's one-buffer-per-op TCP watcher.
        fn streamImpl(
            io_r: *std.Io.Reader,
            io_w: *std.Io.Writer,
            limit: std.Io.Limit,
        ) std.Io.Reader.StreamError!usize {
            const r: *Reader = @alignCast(@fieldParentPtr("interface", io_r));
            const dest = limit.slice(try io_w.writableSliceGreedy(1));
            const n = r.stream.read(dest) catch |err| switch (err) {
                error.EOF => return error.EndOfStream,
                else => return error.ReadFailed,
            };
            io_w.advance(n);
            return n;
        }
    };

    pub const Writer = struct {
        stream: Stream,
        interface: std.Io.Writer,

        pub fn init(stream: Stream, buffer: []u8) Writer {
            return .{
                .stream = stream,
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
                w.stream.writeAll(buffered) catch
                    return error.WriteFailed;
                total += buffered.len;
            }
            // 2. Then each explicit slice; the last repeated `splat` times.
            if (data.len > 0) {
                for (data[0 .. data.len - 1]) |slice| {
                    if (slice.len == 0) continue;
                    w.stream.writeAll(slice) catch
                        return error.WriteFailed;
                    total += slice.len;
                }
                const last = data[data.len - 1];
                if (last.len > 0) {
                    var i: usize = 0;
                    while (i < splat) : (i += 1) {
                        w.stream.writeAll(last) catch
                            return error.WriteFailed;
                        total += last.len;
                    }
                }
            }
            return io_w.consume(total);
        }
    };
};

// ───────────────────────── low-level socket ops ─────────────────────────

// TODO: Add timeouts to socketRead and socketWrite, perhaps.
fn socketRead(tcp: xev.TCP, buf: []u8) xev.ReadError!usize {
    const w = _rt.currentWorker();
    const f = w.current_fiber.?;
    var c: xev.Completion = undefined;
    var waiter: ReadWaiter = .{ .fiber = f, .result = 0 };
    tcp.read(&w.loop, &c, .{ .slice = buf }, ReadWaiter, &waiter, readCallback);
    parkAssertDone(&waiter.done);
    return waiter.result;
}

fn socketWrite(tcp: xev.TCP, buf: []const u8) xev.WriteError!usize {
    const w = _rt.currentWorker();
    const f = w.current_fiber.?;
    var c: xev.Completion = undefined;
    var waiter: WriteWaiter = .{ .fiber = f, .result = 0 };
    tcp.write(&w.loop, &c, .{ .slice = buf }, WriteWaiter, &waiter, writeCallback);
    parkAssertDone(&waiter.done);
    return waiter.result;
}

fn listenImpl(op: Op.Listen) !Server {
    // Create + bind + listen via std.Io.net — portable, sets SO_REUSEADDR,
    // and resolves an ephemeral (port 0) bind into `socket.address`. Then
    // drive accept asynchronously through libxev on the resulting fd.
    const srv = try op.addr.inner.listen(_fsops.io(), .{
        .reuse_address = op.options.reuse_address,
        .kernel_backlog = op.options.backlog,
    });
    errdefer socketClose(srv.socket.handle);
    // std.Io.net's listen socket is blocking; xev's readiness backends
    // need it non-blocking. (Accepted sockets are xev's own domain.)
    try setNonBlocking(srv.socket.handle);
    return .{
        .tcp = xev.TCP.initFd(srv.socket.handle),
        .bound_addr = srv.socket.address,
    };
}

fn connectImpl(addr: IpAddress) !Stream {
    var tcp = try xev.TCP.init(addr.inner);
    errdefer socketClose(tcp.fd);

    const w = _rt.currentWorker();
    const f = w.current_fiber.?;
    var c: xev.Completion = undefined;
    var waiter: ConnectWaiter = .{ .fiber = f, .result = {} };
    tcp.connect(
        &w.loop,
        &c,
        addr.inner,
        ConnectWaiter,
        &waiter,
        connectCallback,
    );

    parkAssertDone(&waiter.done);

    try waiter.result;
    const stream: Stream = .{ .tcp = tcp };
    // NODELAY is a universal low-latency default — apply it to every
    // connected stream at creation so callers don't have to.
    stream.setNoDelay(true) catch {};
    return stream;
}

fn acceptImpl(srv: Server) !Stream {
    const w = _rt.currentWorker();
    const f = w.current_fiber.?;
    var c: xev.Completion = undefined;
    var waiter: AcceptWaiter = .{ .fiber = f, .result = undefined };
    srv.tcp.accept(&w.loop, &c, AcceptWaiter, &waiter, acceptCallback);

    parkAssertDone(&waiter.done);

    const tcp = try waiter.result;
    const stream: Stream = .{ .tcp = tcp };
    stream.setNoDelay(true) catch {}; // low-latency default (see connect)
    return stream;
}

/// Park until the op's completion callback has fired (`done`).
fn parkAssertDone(done: *std.atomic.Value(bool)) void {
    _rt.park();
    // Invariant: resume ONLY when the io_uring completion fired and set `done`.
    // A resume with `!done` is a spurious wake — fail loud at the source rather
    // than silently re-parking.
    if (!done.load(.acquire)) {
        @panic("net: fiber woken before its I/O completed — spurious wake");
    }
}

const ReadWaiter = struct {
    fiber: *Fiber,
    result: xev.ReadError!usize,
    done: std.atomic.Value(bool) = .init(false),
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
    w.done.store(true, .release);
    _rt.wake(w.fiber);
    return .disarm;
}

const WriteWaiter = struct {
    fiber: *Fiber,
    result: xev.WriteError!usize,
    done: std.atomic.Value(bool) = .init(false),
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
    w.done.store(true, .release);
    _rt.wake(w.fiber);
    return .disarm;
}

const ConnectWaiter = struct {
    fiber: *Fiber,
    result: xev.ConnectError!void,
    done: std.atomic.Value(bool) = .init(false),
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
    w.done.store(true, .release);
    _rt.wake(w.fiber);
    return .disarm;
}

const AcceptWaiter = struct {
    fiber: *Fiber,
    result: xev.AcceptError!xev.TCP,
    done: std.atomic.Value(bool) = .init(false),
};

fn acceptCallback(
    ud: ?*AcceptWaiter,
    _: *xev.Loop,
    _: *xev.Completion,
    r: xev.AcceptError!xev.TCP,
) xev.CallbackAction {
    const w = ud.?;
    w.result = r;
    w.done.store(true, .release);
    _rt.wake(w.fiber);
    return .disarm;
}
