//! Layer 2: zio-backed I/O driver for the pgwire state machine.
//!
//! This is the *only* file in pgwire that is allowed to know about
//! sockets — proto.zig / conn_state.zig / parser.zig must remain pure.
//!
//! Caller pattern (caller owns the accept loop):
//!
//!     while (true) {
//!         const stream = try server.accept();
//!         try group.spawn(handle, .{ stream });
//!     }
//!     fn handle(stream: zio.net.Stream) !void {
//!         defer stream.close();
//!         var h = MyHandler{};
//!         try pgwire.serve(allocator, &stream, &h, .{});
//!     }
//!
//! The handler's surface area:
//!
//!  - `fn onQuery(self, state: *ConnState, sql: []const u8) !void`
//!    Required. Drives a simple-query (Q) message. Handler emits
//!    RowDescription (0..1) → DataRow (0..N) → CommandComplete (or
//!    sendError on failure). Driver emits the trailing RFQ.
//!
//!  - `fn onExecute(self, state: *ConnState, ctx: ExecuteCtx) !void`
//!    Optional. Required if any client uses the extended-query
//!    protocol (most non-libpq drivers). Handler emits the row
//!    stream same as onQuery, but does NOT receive RFQ from the
//!    driver — extended batches close on Sync, which the state
//!    machine handles itself. If absent, the driver replies with
//!    ErrorResponse and the rest of the batch resyncs at Sync.
//!
//!  - `fn onDescribe(self, state: *ConnState, ctx: DescribeCtx) !void`
//!    Optional. Required by clients that send Describe before
//!    Bind+Execute (pgx default mode does). Handler must emit
//!    RowDescription or NoData (the state machine has already
//!    emitted ParameterDescription for the statement-kind). If
//!    absent, we fall back to NoData which works for some clients
//!    and breaks others.

const std = @import("std");
const fiber = @import("fiber");

const conn_state = @import("conn_state.zig");
const proto = @import("proto.zig");

pub const Stream = fiber.net.Stream;

pub const Options = struct {
    /// Per-read scratch buffer size. Chosen large enough that Startup
    /// + Query fit in one read, small enough not to waste heap on
    /// idle connections.
    read_chunk: usize = 16 * 1024,
    /// Per-write buffer size. The state machine writes frames into
    /// this buffer; once it fills, the buffered Writer's drain hook
    /// fires a socket write, which gives us TCP backpressure during
    /// large result sets without ever holding the whole result in
    /// memory.
    write_chunk: usize = 16 * 1024,
    /// Configuration handed to ConnState.
    conn: conn_state.Config = .{},
};

/// Drive a Postgres v3 simple-query session over `stream`. Returns
/// when the peer terminates or the stream errors. The caller still
/// owns the stream — we don't close it.
pub fn serve(
    alloc: std.mem.Allocator,
    stream: *Stream,
    handler: anytype,
    opts: Options,
) !void {
    // Heap-allocated buffers — zio fibers have small stacks, so we
    // never put multi-KiB buffers on the stack here. (See
    // feedback_no_stack_buffers in the project memory.)
    const read_buf = try alloc.alloc(u8, opts.read_chunk);
    defer alloc.free(read_buf);
    const write_buf = try alloc.alloc(u8, opts.write_chunk);
    defer alloc.free(write_buf);

    var r = stream.reader(read_buf);
    var w = stream.writer(write_buf);

    var state = conn_state.ConnState.init(alloc, &r.interface, &w.interface, opts.conn);
    defer state.deinit();

    // The state machine pulls bytes through `r.interface` itself — when
    // its buffer is empty, the reader's drain hook fiber-blocks on the
    // socket. Frames flow out via `w.interface`; auto-flush triggers
    // when the write buffer fills (large result sets), explicit flush
    // below handles logical boundaries (RFQ, errors).
    while (state.phase != .closed) {
        try handleEvent(&state, handler);
        try w.interface.flush();
    }

    // Final flush — emit any trailing fatal error written during the
    // last event. Connection is dying; swallow errors.
    w.interface.flush() catch {};
}

fn handleEvent(state: *conn_state.ConnState, handler: anytype) !void {
    const H = @TypeOf(handler.*);
    const ev = try state.nextEvent();
    switch (ev) {
        .startup_complete => {
            // State machine has already enqueued the AuthOk +
            // ParameterStatus + ReadyForQuery batch. Nothing more
            // for us to do — outer loop flushes.
        },
        .query => |sql| {
            try handler.onQuery(state, sql);
            try state.endCommand();
        },
        .execute => |ctx| {
            if (@hasDecl(H, "onExecute")) {
                try handler.onExecute(state, ctx);
            } else {
                // No extended-query handler — the client will
                // see this via the next Sync's RFQ.
                try state.sendError(.{
                    .code = .{ 'X', 'X', '0', '0', '0' },
                    .message = "handler does not implement onExecute",
                });
                state.ext_failed = true;
            }
        },
        .describe => |ctx| {
            if (@hasDecl(H, "onDescribe")) {
                try handler.onDescribe(state, ctx);
            } else {
                // Spec-compliant fallback: NoData is legal but
                // tells the client we don't have a row schema
                // to share. Some clients tolerate it; some don't.
                try state.sendNoData();
            }
        },
        .sync => {
            // State machine has already emitted RFQ. Outer loop
            // will flush.
        },
        .terminate => {
            // phase is now .closed; outer loop's check breaks out.
        },
    }
}

test {
    std.testing.refAllDecls(@This());
}
