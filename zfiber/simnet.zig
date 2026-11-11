// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The simulation's network: every `net.submit` under a sim lands here
//! instead of the kernel (`Sim.net`). Listeners by port, and each
//! connection as two ends in memory, an end holding the bytes its peer
//! wrote and it has not read yet. The semantics are the ones the code
//! above relies on: a connect to a port nobody listens on is refused, a
//! read parks until bytes arrive or no more can, and a write parks
//! while the peer's unread bytes fill the window. A close ends the
//! peer's reads once it has read what was sent, and fails its writes.
//!
//! Connect, accept, read and write park for a virtual duration drawn
//! from the network's own PRNG, as the disk's ops do: a socket op stays
//! a yield point, and a run of one seed replays the same durations.
//!
//! One host: an address is its port. Nothing is lost, cut or delayed
//! beyond that. One worker runs every fiber under a sim, so nothing in
//! here locks.

const std = @import("std");
const xev = @import("xev");
const _dl = @import("deadlines.zig");
const _fb = @import("fiber.zig");
const _net = @import("net.zig");
const _rt = @import("runtime.zig");

const Allocator = std.mem.Allocator;
const Fiber = _fb.Fiber;
const Op = _net.Op;
const Server = _net.Server;
const Stream = _net.Stream;

/// How many unread bytes an end holds before a write to it parks: the
/// socket buffers of one connection.
pub const window: usize = 64 << 10;

/// Where a listen on port 0 starts looking for a port.
const first_port: u16 = 49152;

pub const SimNet = struct {
    alloc: Allocator,
    /// The durations. Its own, apart from the scheduler's and the
    /// disk's draws.
    prng: std.Random.DefaultPrng,
    /// Listening sockets by `Server` fd; a free slot is null and reused.
    listeners: std.ArrayList(?*Listener) = .empty,
    /// Connection ends by `Stream` fd; a free slot is null and reused.
    ends: std.ArrayList(?*End) = .empty,
    /// The next port a listen on port 0 tries.
    next_port: u16 = first_port,

    const Listener = struct {
        port: u16,
        /// The server ends of connects not accepted yet, oldest first.
        /// They have no fd until the accept.
        pending: std.ArrayList(*End) = .empty,
        /// The fiber parked in `accept`.
        acceptor: ?*Fiber = null,
        /// Closed by its owner: the port is free again. The slot is
        /// held until the fibers parked on it have run (`parked`).
        closed: bool = false,
        parked: u32 = 0,
    };

    const End = struct {
        /// The other end; null once either end closed.
        peer: ?*End = null,
        /// What the peer wrote and this end has not read yet.
        inbox: std.ArrayList(u8) = .empty,
        /// No more bytes will arrive: the peer closed or shut down its
        /// sending side, or this end shut down its receiving side.
        ended: bool = false,
        /// This end shut down its receiving side: what the peer still
        /// writes is taken and dropped.
        recv_shut: bool = false,
        /// This end shut down its sending side: its writes fail.
        send_shut: bool = false,
        /// Closed by its owner. The slot is held until the fibers
        /// parked on it have run (`parked`).
        closed: bool = false,
        /// The fiber parked in a read here, for bytes.
        reader: ?*Fiber = null,
        /// The fiber parked in a write from here, for room at the peer.
        writer: ?*Fiber = null,
        parked: u32 = 0,
    };

    pub fn init(alloc: Allocator, seed: u64) SimNet {
        return .{
            .alloc = alloc,
            .prng = std.Random.DefaultPrng.init(seed ^ 0x5e7),
        };
    }

    pub fn deinit(self: *SimNet) void {
        for (self.listeners.items) |slot| if (slot) |l| {
            for (l.pending.items) |end| self.destroyEnd(end);
            self.destroyListener(l);
        };
        self.listeners.deinit(self.alloc);
        for (self.ends.items) |slot| if (slot) |end| self.destroyEnd(end);
        self.ends.deinit(self.alloc);
    }

    /// Run one op against the network, after its wait. An error carries
    /// the name the real socket gives the same condition, so a caller's
    /// handling of `EOF` or `ConnectionRefused` is the same on both.
    pub fn apply(
        self: *SimNet,
        comptime tag: Op.Tag,
        args: Op.Args(tag),
    ) _net.Result(tag) {
        record(tag, args);
        self.wait(tag);
        switch (tag) {
            .listen => return self.listen(args),
            .connect => return self.connect(args),
            .accept => return self.accept(args),
            .read => return self.read(args),
            .write => return self.write(args),
            .shutdown => return self.shutdown(args),
            .close => self.close(args),
            .close_server => self.closeListener(args),
        }
    }

    /// The op into the run's trace.
    fn record(comptime tag: Op.Tag, args: Op.Args(tag)) void {
        const sim = _rt.runtime().simState().?;
        sim.recordOp("net." ++ @tagName(tag), args);
    }

    /// Park for what `tag` takes on the wire, 0 or 1 virtual ms. 0 ms
    /// still yields, so the fiber gives up the worker as a real
    /// completion would make it. Listen, shutdown and close are calls
    /// the kernel answers at once.
    fn wait(self: *SimNet, comptime tag: Op.Tag) void {
        switch (tag) {
            .connect, .accept, .read, .write => {},
            .listen, .shutdown, .close, .close_server => return,
        }
        const ms = self.prng.random().uintLessThan(u64, 2);
        if (ms == 0) _rt.yield() else _dl.sleepMs(ms);
    }

    // ───────────────────────── listeners ─────────────────────────

    fn listen(self: *SimNet, op: Op.Listen) !Server {
        var addr = op.addr.inner;
        var port = addr.getPort();
        if (port == 0) {
            port = self.freePort();
        } else if (self.listenerOn(port) != null) {
            return error.AddressInUse;
        }
        addr.setPort(port);

        const fd = try freeSlot(Listener, self.alloc, &self.listeners);
        const l = try self.alloc.create(Listener);
        l.* = .{ .port = port };
        self.listeners.items[fd] = l;
        return .{ .tcp = xev.TCP.initFd(@intCast(fd)), .bound_addr = addr };
    }

    fn accept(self: *SimNet, srv: Server) !Stream {
        const fd: usize = @intCast(srv.tcp.fd);
        const l = self.listenerAt(fd) orelse return error.SocketNotListening;
        l.parked += 1;
        defer {
            l.parked -= 1;
            if (l.closed and l.parked == 0) self.freeListener(fd);
        }
        while (l.pending.items.len == 0) {
            if (l.closed) return error.ConnectionAborted;
            std.debug.assert(l.acceptor == null);
            l.acceptor = _rt.currentFiber();
            _rt.park();
            l.acceptor = null;
        }

        const end_fd = try freeSlot(End, self.alloc, &self.ends);
        self.ends.items[end_fd] = l.pending.orderedRemove(0);
        return streamOf(end_fd);
    }

    /// The port is free at once; a fiber parked in `accept` returns
    /// with an error, and a connect not accepted yet sees its peer gone.
    fn closeListener(self: *SimNet, srv: Server) void {
        const fd: usize = @intCast(srv.tcp.fd);
        const l = self.listenerAt(fd) orelse return;
        l.closed = true;
        for (l.pending.items) |end| {
            detach(end);
            self.destroyEnd(end);
        }
        l.pending.clearRetainingCapacity();
        wake(&l.acceptor);
        if (l.parked == 0) self.freeListener(fd);
    }

    /// The open listener at `fd`.
    fn listenerAt(self: *SimNet, fd: usize) ?*Listener {
        if (fd >= self.listeners.items.len) return null;
        const l = self.listeners.items[fd] orelse return null;
        return if (l.closed) null else l;
    }

    fn listenerOn(self: *SimNet, port: u16) ?*Listener {
        for (self.listeners.items) |slot| {
            const l = slot orelse continue;
            if (!l.closed and l.port == port) return l;
        }
        return null;
    }

    fn freePort(self: *SimNet) u16 {
        while (true) {
            const port = self.next_port;
            self.next_port = if (port == std.math.maxInt(u16))
                first_port
            else
                port + 1;
            if (self.listenerOn(port) == null) return port;
        }
    }

    fn freeListener(self: *SimNet, fd: usize) void {
        self.destroyListener(self.listeners.items[fd].?);
        self.listeners.items[fd] = null;
    }

    fn destroyListener(self: *SimNet, l: *Listener) void {
        l.pending.deinit(self.alloc);
        self.alloc.destroy(l);
    }

    // ───────────────────────── connections ─────────────────────────

    fn connect(self: *SimNet, addr: _net.IpAddress) !Stream {
        const l = self.listenerOn(addr.inner.getPort()) orelse
            return error.ConnectionRefused;
        const fd = try freeSlot(End, self.alloc, &self.ends);
        const client = try self.alloc.create(End);
        errdefer self.alloc.destroy(client);
        const server = try self.alloc.create(End);
        errdefer self.alloc.destroy(server);
        client.* = .{ .peer = server };
        server.* = .{ .peer = client };

        try l.pending.append(self.alloc, server);
        self.ends.items[fd] = client;
        wake(&l.acceptor);
        return streamOf(fd);
    }

    fn read(self: *SimNet, op: Op.Read) !usize {
        const fd: usize = @intCast(op.stream.tcp.fd);
        const end = self.endAt(fd) orelse return error.Unexpected;
        if (op.buf.len == 0) return 0;
        end.parked += 1;
        defer self.unpark(fd, end);

        while (end.inbox.items.len == 0) {
            if (end.closed) return error.Unexpected;
            if (end.ended) return error.EOF;
            std.debug.assert(end.reader == null);

            end.reader = _rt.currentFiber();
            _rt.park();
            end.reader = null;
        }
        if (end.closed) return error.Unexpected;

        const n = self.some(@min(op.buf.len, end.inbox.items.len));
        @memcpy(op.buf[0..n], end.inbox.items[0..n]);
        end.inbox.replaceRangeAssumeCapacity(0, n, &.{});
        // Room again for a writer the window held.
        if (end.peer) |peer| wake(&peer.writer);
        return n;
    }

    fn write(self: *SimNet, op: Op.Write) !usize {
        const fd: usize = @intCast(op.stream.tcp.fd);
        const end = self.endAt(fd) orelse return error.Unexpected;
        if (op.data.len == 0) return 0;
        end.parked += 1;
        defer self.unpark(fd, end);
        while (true) {
            if (end.closed) return error.Unexpected;
            if (end.send_shut) return error.BrokenPipe;
            const peer = end.peer orelse return error.BrokenPipe;
            if (peer.recv_shut) return op.data.len;

            const room = window -| peer.inbox.items.len;
            if (room > 0) {
                const n = self.some(@min(op.data.len, room));
                try peer.inbox.appendSlice(self.alloc, op.data[0..n]);
                wake(&peer.reader);
                return n;
            }
            std.debug.assert(end.writer == null);
            end.writer = _rt.currentFiber();
            _rt.park();
            end.writer = null;
        }
    }

    /// How many of the `n` bytes a read or a write could move it does
    /// move: all of them.
    /// TODO: some of the time a part, at least one byte, as TCP may. A
    /// caller that reads a message with one read then fails:
    /// `net_test`'s echo test does.
    fn some(_: *SimNet, n: usize) usize {
        return n;
    }

    fn shutdown(self: *SimNet, op: Op.Shutdown) !void {
        const fd: usize = @intCast(op.stream.tcp.fd);
        const end = self.endAt(fd) orelse return error.SocketUnconnected;
        if (op.how != .recv) {
            end.send_shut = true;
            wake(&end.writer);
            if (end.peer) |peer| {
                peer.ended = true;
                wake(&peer.reader);
            }
        }
        if (op.how != .send) {
            end.recv_shut = true;
            end.ended = true;
            end.inbox.clearRetainingCapacity();
            wake(&end.reader);
            // Its bytes go nowhere now, so nothing holds it back.
            if (end.peer) |peer| wake(&peer.writer);
        }
    }

    /// The fd is gone for its owner; the peer reads what was sent and
    /// then the end, and its writes fail. A fiber parked on this end
    /// returns with an error.
    fn close(self: *SimNet, stream: Stream) void {
        const fd: usize = @intCast(stream.tcp.fd);
        const end = self.endAt(fd) orelse return;
        end.closed = true;
        detach(end);
        wake(&end.reader);
        wake(&end.writer);
        if (end.parked == 0) self.freeEnd(fd);
    }

    /// `end` and its peer stop knowing each other: nothing more
    /// arrives at the peer, and a fiber parked on it runs to see that.
    fn detach(end: *End) void {
        const peer = end.peer orelse return;
        end.peer = null;
        peer.peer = null;
        peer.ended = true;
        wake(&peer.reader);
        wake(&peer.writer);
    }

    /// The end at `fd`, unless its owner closed it.
    fn endAt(self: *SimNet, fd: usize) ?*End {
        if (fd >= self.ends.items.len) return null;
        const end = self.ends.items[fd] orelse return null;
        return if (end.closed) null else end;
    }

    /// A fiber that was parked on `end` is through with it; the last
    /// one out of a closed end frees its slot.
    fn unpark(self: *SimNet, fd: usize, end: *End) void {
        end.parked -= 1;
        if (end.closed and end.parked == 0) self.freeEnd(fd);
    }

    fn freeEnd(self: *SimNet, fd: usize) void {
        self.destroyEnd(self.ends.items[fd].?);
        self.ends.items[fd] = null;
    }

    fn destroyEnd(self: *SimNet, end: *End) void {
        end.inbox.deinit(self.alloc);
        self.alloc.destroy(end);
    }

    fn streamOf(fd: usize) Stream {
        return .{ .tcp = xev.TCP.initFd(@intCast(fd)) };
    }

    /// The index of a null slot in `list`, a new one at its end if all
    /// are taken.
    fn freeSlot(
        comptime T: type,
        alloc: Allocator,
        list: *std.ArrayList(?*T),
    ) Allocator.Error!usize {
        for (list.items, 0..) |slot, i| if (slot == null) return i;
        try list.append(alloc, null);
        return list.items.len - 1;
    }

    /// Wake the fiber `slot` names, once: a fiber is named only while
    /// it is parked here, so no wake is left over for its next park.
    fn wake(slot: *?*Fiber) void {
        const fiber = slot.* orelse return;
        slot.* = null;
        _rt.wake(fiber);
    }
};
