const std = @import("std");
const zio = @import("zio");
const fiber = @import("fiber");
const utils = @import("utils");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.mux);

// Frame header: 11 bytes on the wire.
//
//   [0:4]  length        (u32 LE)
//   [4:6]  message_type  (u16 LE)
//   [6:10] stream_id     (u32 LE)
//   [10]    flags         (u8)
//
pub const FrameHeader = struct {
    length: u32,
    message_type: u16,
    stream_id: u32,
    flags: u8,

    pub const encoded_size: usize = 11;
    pub const syn_flag: u8 = 0x1;
    pub const fin_flag: u8 = 0x2;

    pub fn encode(self: FrameHeader, buf: *[encoded_size]u8) void {
        std.mem.writeInt(u32, buf[0..4], self.length, .little);
        std.mem.writeInt(u16, buf[4..6], self.message_type, .little);
        std.mem.writeInt(u32, buf[6..10], self.stream_id, .little);
        buf[10] = self.flags;
    }

    pub fn decode(buf: *const [encoded_size]u8) FrameHeader {
        return .{
            .length = std.mem.readInt(u32, buf[0..4], .little),
            .message_type = std.mem.readInt(u16, buf[4..6], .little),
            .stream_id = std.mem.readInt(u32, buf[6..10], .little),
            .flags = buf[10],
        };
    }

    pub fn hasSyn(self: FrameHeader) bool {
        return (self.flags & syn_flag) != 0;
    }

    pub fn hasFin(self: FrameHeader) bool {
        return (self.flags & fin_flag) != 0;
    }
};

pub const Frame = struct {
    header: FrameHeader,
    payload: []u8,

    pub fn done(self: Frame) bool {
        return self.header.hasFin();
    }

    pub fn deinit(self: *Frame, allocator: Allocator) void {
        if (self.payload.len > 0) {
            allocator.free(self.payload);
        }
        self.payload = &.{};
    }
};

const channel_capacity = 4;

// TODO: We should add a mechanism so if we're getting done with stream ids, we need to
// close TCP and re-establish TCP connection. Also, we're doing the tcp conn outside
// of Mux. Would have been more encapsulated if TCP conn was getting established here?
pub const Mux = struct {
    pub const Stream = struct {
        mux: *Mux,
        stream_id: u32,
        syn_sent: bool = true,
        channel_buf: [channel_capacity]Frame = undefined,
        channel: zio.Channel(Frame) = undefined,

        /// Must be called on a stable (heap) pointer — not a stack temporary.
        fn initChannel(self: *Stream) void {
            self.channel = zio.Channel(Frame).init(&self.channel_buf);
        }

        /// Receive the next frame for this stream. Blocks until a frame arrives.
        /// Returns error.ChannelClosed on disconnect. Check frame.done() for normal end-of-stream.
        pub fn recv(self: *Stream) !Frame {
            return self.channel.receive();
        }

        /// Send a frame on this stream via the mux writer.
        /// The first send on an outbound stream automatically includes the syn flag.
        pub fn send(self: *Stream, message_type: u16, payload: []const u8, flags: u8) !void {
            var f = flags;
            if (!self.syn_sent) {
                f |= FrameHeader.syn_flag;
                self.syn_sent = true;
            }
            try self.mux.writeFrame(.{
                .length = @intCast(payload.len),
                .message_type = message_type,
                .flags = f,
                .stream_id = self.stream_id,
            }, payload);
        }

        /// Send a fin frame and close the stream.
        pub fn finish(self: *Stream, message_type: u16) !void {
            try self.send(message_type, &.{}, FrameHeader.fin_flag);
            self.mux.destroyStream(self.stream_id);
        }
    };

    allocator: Allocator,
    tcp_stream: zio.net.Stream,
    writer_mu: utils.TrackedMutex,
    streams: std.AutoHashMap(u32, *Stream),
    next_stream_id: std.atomic.Value(u32),
    on_stream: *const fn (*Stream) void,
    group: fiber.Group(void),
    closed: bool,
    mu: utils.TrackedMutex,

    pub fn init(
        allocator: Allocator,
        tcp_stream: zio.net.Stream,
        initial_stream_id: u32,
        on_stream: *const fn (*Stream) void,
    ) !*Mux {
        const self = try allocator.create(Mux);
        self.* = .{
            .allocator = allocator,
            .tcp_stream = tcp_stream,
            .writer_mu = .{},
            .streams = std.AutoHashMap(u32, *Stream).init(allocator),
            .next_stream_id = std.atomic.Value(u32).init(initial_stream_id),
            .on_stream = on_stream,
            .group = fiber.Group(void).init(allocator),
            .closed = false,
            .mu = .{},
        };
        return self;
    }

    pub fn deinit(self: *Mux) void {
        log.info("Mux.deinit", .{});
        self.closeConn();
        self.closeChannels(); // close all channels, so we can exit the handlers.

        log.info("Mux.deinit. tcp closed", .{});
        self.group.wait();
        self.group.deinit();
        log.info("Mux.deinit. group wait done", .{});
        self.destroyStreams();
        self.streams.deinit();
        self.mu.deinit();
        self.writer_mu.deinit();
        self.allocator.destroy(self);
        log.info("Mux.deinit OK", .{});
    }

    pub fn closeConn(self: *Mux) void {
        self.mu.lockUncancelable(@src());
        self.mu.unlock();

        if (self.closed) return;
        self.tcp_stream.close();
        self.closed = true;
    }

    /// Create a new outbound stream. The first send() will include the syn flag.
    pub fn newStream(self: *Mux) !*Stream {
        const sid = self.allocStreamId();
        return try self.createStream(sid, false);
    }

    pub fn run(self: *Mux) void {
        self.readerLoop() catch |err| {
            log.err("Mux.readerLoop exited with err: {}", .{err});
        };
    }

    pub fn readerLoop(self: *Mux) !void {
        defer std.debug.print("returning from readerLoop\n", .{});

        const tcp_buf = try self.allocator.alloc(u8, 4096);
        defer self.allocator.free(tcp_buf);
        // Not setting any timeouts here. channel.Send below can block if we're unable to process
        // the incoming data as fast as it is arriving. Instead, we have an OS level keepalive
        // mechanism in place via configureSocket.
        var buf_reader = self.tcp_stream.reader(tcp_buf);
        const reader = &buf_reader.interface;
        var hdr_buf: [FrameHeader.encoded_size]u8 = @splat(0);

        while (true) {
            reader.readSliceAll(&hdr_buf) catch |err| {
                std.debug.print("readAll for hdr_buf caught: {}\n", .{err});
                return;
            };

            const header = FrameHeader.decode(&hdr_buf);
            log.debug("got header raw={any} decoded={any}", .{ hdr_buf, header });

            const payload: []u8 = if (header.length > 0) blk: {
                const buf = self.allocator.alloc(u8, header.length) catch return error.OutOfMemory;
                reader.readSliceAll(buf) catch {
                    self.allocator.free(buf);
                    return;
                };
                break :blk buf;
            } else @constCast(&[_]u8{});

            const frame = Frame{
                .header = header,
                .payload = payload,
            };
            log.debug("got frame with payload.len {d}", .{payload.len});

            var stream: ?*Stream = null;
            if (header.hasSyn()) {
                stream = self.createStream(header.stream_id, true) catch {
                    if (payload.len > 0) self.allocator.free(payload);
                    continue;
                };

                stream.?.channel.send(frame) catch {
                    if (payload.len > 0) self.allocator.free(payload);
                    continue;
                };

                self.group.spawn(.{ .lane = .raft }, handleInbound, .{ self, stream.? }) catch {
                    // do nothing
                };
            } else {
                self.mu.lock(@src());
                stream = self.streams.get(header.stream_id);
                self.mu.unlock();

                if (stream) |s| {
                    s.channel.send(frame) catch {
                        if (payload.len > 0) self.allocator.free(payload);
                    };
                } else {
                    if (payload.len > 0) self.allocator.free(payload);
                }
            }

            if (header.hasFin()) {
                if (stream) |s| {
                    s.channel.close(.graceful);
                }
            }
        }
    }

    fn handleInbound(self: *Mux, stream: *Stream) void {
        self.on_stream(stream);
        self.destroyStream(stream.stream_id);
    }

    pub fn writeFrame(self: *Mux, header: FrameHeader, payload: []const u8) !void {
        self.writer_mu.lock(@src());
        defer self.writer_mu.unlock();

        var hdr_buf: [FrameHeader.encoded_size]u8 = undefined;
        header.encode(&hdr_buf);
        // We are not setting any timeouts here. If we have a big payload, then
        // adding timeouts won't make sense -- particularly if the receiver is applying
        // backpressure. Instead, configureSocket is doing OS level keep alive checking to ensure
        // that the connection is still alive.
        self.tcp_stream.writeAll(&hdr_buf, .none) catch return error.BrokenPipe;
        if (payload.len > 0) {
            self.tcp_stream.writeAll(payload, .none) catch return error.BrokenPipe;
        }
    }

    fn allocStreamId(self: *Mux) u32 {
        return self.next_stream_id.fetchAdd(2, .monotonic);
    }

    fn createStream(self: *Mux, stream_id: u32, syn_sent: bool) !*Stream {
        const stream = try self.allocator.create(Stream);
        stream.* = .{ .mux = self, .stream_id = stream_id, .syn_sent = syn_sent };
        stream.initChannel();

        self.mu.lock(@src());
        defer self.mu.unlock();
        try self.streams.put(stream_id, stream);
        return stream;
    }

    pub fn destroyStream(self: *Mux, stream_id: u32) void {
        self.mu.lock(@src());
        defer self.mu.unlock();
        if (self.streams.fetchRemove(stream_id)) |kv| {
            self.allocator.destroy(kv.value);
        }
    }

    fn closeChannels(self: *Mux) void {
        self.mu.lockUncancelable(@src());
        defer self.mu.unlock();

        var it = self.streams.valueIterator();
        while (it.next()) |s| {
            s.*.channel.close(.immediate);
        }
    }

    fn destroyStreams(self: *Mux) void {
        self.mu.lockUncancelable(@src());
        defer self.mu.unlock();

        var it = self.streams.valueIterator();
        while (it.next()) |s| {
            // Drain unconsumed frames to free their payloads.
            while (s.*.channel.tryReceive()) |f| {
                var frame = f;
                frame.deinit(self.allocator);
            } else |_| {}
            self.allocator.destroy(s.*);
        }
        self.streams.clearAndFree();
    }
};
