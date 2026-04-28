const std = @import("std");
const zio = @import("zio");
const utils = @import("utils");
const Allocator = std.mem.Allocator;
const log = std.log.scoped(.serial);

/// Header: [4 bytes LE length][2 bytes LE message_type]
pub const header_size: usize = 6;
/// All message type constants in one place to avoid conflicts.
pub const msg_type_raft: u16 = 1;
pub const msg_type_join_request: u16 = 2;
pub const msg_type_ping: u16 = 3;
pub const msg_type_pong: u16 = 4;
pub const msg_type_file_push: u16 = 5; // [8B file_id][4B crc32][file data...]
pub const msg_type_file_ack: u16 = 6; // [8B file_id][8B node_id]
pub const msg_type_file_request: u16 = 7; // [N * 8B file_ids]

pub const OutboundMsg = struct {
    message_type: u16,
    /// Refcounted payload. The queue holds one ref; writerLoop decrRefs
    /// after sending. Multiple peers can share the same RefValue via
    /// `incrRef` so fan-out sends avoid memcpy.
    value: *utils.RefValue,
};

pub const Serial = struct {
    const buf_size: usize = 4096;

    allocator: Allocator,
    tcp_stream: zio.net.Stream,
    writer_mu: zio.Mutex,
    write_buf: []u8,
    read_buf: []u8,
    buf_reader: zio.net.Stream.Reader,
    outbound: zio.Channel(OutboundMsg),
    last_recv_ms: std.atomic.Value(i64),
    closed: bool,
    drop_messages: std.atomic.Value(bool),

    pub fn init(allocator: Allocator, tcp_stream: zio.net.Stream) !*Serial {
        const write_buf = try allocator.alloc(u8, buf_size);
        errdefer allocator.free(write_buf);
        const read_buf = try allocator.alloc(u8, buf_size);
        errdefer allocator.free(read_buf);
        const self = try allocator.create(Serial);
        self.* = .{
            .allocator = allocator,
            .tcp_stream = tcp_stream,
            .writer_mu = .init,
            .write_buf = write_buf,
            .read_buf = read_buf,
            .buf_reader = tcp_stream.reader(read_buf),
            .outbound = try zio.Channel(OutboundMsg).initAlloc(allocator, 256),
            .last_recv_ms = std.atomic.Value(i64).init(std.time.milliTimestamp()),
            .closed = false,
            .drop_messages = std.atomic.Value(bool).init(false),
        };
        return self;
    }

    pub fn deinit(self: *Serial) void {
        self.closeConn();
        self.outbound.deinit();
        self.allocator.free(self.write_buf);
        self.allocator.free(self.read_buf);
        self.allocator.destroy(self);
    }

    pub fn closeConn(self: *Serial) void {
        self.outbound.close(.graceful);
        while (self.outbound.tryReceive()) |msg| {
            msg.value.decrRef();
        } else |_| {}

        if (!self.closed) {
            self.tcp_stream.shutdown(.both) catch {};
            self.closed = true;
        }
    }

    /// Queue an outbound message. Non-blocking; drops on full channel.
    /// Borrows `value` — `incrRef`s on successful enqueue (the writer fiber
    /// decrRefs after sending). On drop or full channel, no incrRef happens
    /// so the caller's refcount is unchanged. Caller retains its ref.
    pub fn queueMsg(self: *Serial, message_type: u16, value: *utils.RefValue) void {
        if (self.drop_messages.load(.acquire)) return;
        _ = value.incrRef();
        self.outbound.trySend(.{ .message_type = message_type, .value = value }) catch |err| {
            log.err("queueMsg: channel full, dropping message: {s}", .{@errorName(err)});
            value.decrRef();
        };
    }

    /// Send a framed message. Thread-safe via writer_mu.
    pub fn send(self: *Serial, message_type: u16, payload: []const u8) !void {
        try self.writer_mu.lock();
        defer self.writer_mu.unlock();

        var buf_writer = self.tcp_stream.writer(self.write_buf);
        const writer = &buf_writer.interface;

        var hdr: [header_size]u8 = undefined;
        std.mem.writeInt(u32, hdr[0..4], @intCast(payload.len), .little);
        std.mem.writeInt(u16, hdr[4..6], message_type, .little);
        writer.writeAll(&hdr) catch return error.BrokenPipe;
        if (payload.len > 0) {
            writer.writeAll(payload) catch return error.BrokenPipe;
        }
        writer.flush() catch return error.BrokenPipe;
    }

    /// Read a single framed message. Caller owns the returned payload and must free it.
    /// Returns error on EOF or read failure.
    pub fn recvOne(self: *Serial) !struct { message_type: u16, payload: []u8 } {
        const reader = &self.buf_reader.interface;

        var hdr: [header_size]u8 = @splat(0);
        reader.readSliceAll(&hdr) catch return error.EndOfStream;

        const length = std.mem.readInt(u32, hdr[0..4], .little);
        const message_type = std.mem.readInt(u16, hdr[4..6], .little);

        if (length == 0) {
            const empty: []u8 = &.{};
            return .{ .message_type = message_type, .payload = empty };
        }

        const payload = try self.allocator.alloc(u8, length);
        errdefer self.allocator.free(payload);

        reader.readSliceAll(payload) catch {
            self.allocator.free(payload);
            return error.EndOfStream;
        };

        return .{ .message_type = message_type, .payload = payload };
    }

    pub const OnMessageFn = *const fn (*anyopaque, *Serial, u16, []const u8) void;

    /// Run reader + writer loops. Blocks until TCP closes or errors.
    /// Reader calls on_message for each inbound frame.
    /// Writer drains the outbound channel and sends frames.
    pub fn run(self: *Serial, ctx: *anyopaque, on_message: OnMessageFn) void {
        var group = zio.group();
        group.spawn(writerLoop, .{self}) catch |err| {
            log.err("failed to spawn writer loop: {s}", .{@errorName(err)});
            return;
        };

        // Reader loop runs inline.
        self.readerLoop(ctx, on_message);

        // Reader exited (TCP closed). Close outbound channel so writer exits too.
        self.outbound.close(.graceful);
        group.wait() catch |err| {
            log.err("writer group wait error: {s}", .{@errorName(err)});
        };
    }

    fn readerLoop(self: *Serial, ctx: *anyopaque, on_message: OnMessageFn) void {
        while (true) {
            const msg = self.recvOne() catch |err| {
                log.debug("reader exited: {s}", .{@errorName(err)});
                return;
            };
            defer if (msg.payload.len > 0) self.allocator.free(msg.payload);

            // Update the liveness of this connection.
            self.last_recv_ms.store(std.time.milliTimestamp(), .release);
            if (msg.message_type == msg_type_pong) continue; // liveness ack, nothing to do
            if (msg.message_type == msg_type_ping) {
                self.send(msg_type_pong, &.{}) catch |err| {
                    log.err("pong send failed: {s}", .{@errorName(err)});
                };
                continue;
            }

            if (!self.drop_messages.load(.acquire)) {
                on_message(ctx, self, msg.message_type, msg.payload);
            }
        }
    }

    fn writerLoop(self: *Serial) void {
        while (true) {
            const msg = self.outbound.receive() catch return;
            defer msg.value.decrRef();
            self.send(msg.message_type, msg.value.data) catch |err| {
                log.err("writerLoop send failed: {s}", .{@errorName(err)});
                return;
            };
        }
    }
};
