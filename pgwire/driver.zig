// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Layer 2: fiber-backed I/O for the pgwire state machine.
//!
//! This is the *only* file in pgwire that is allowed to know about
//! sockets — proto.zig / conn_state.zig / parser.zig must remain pure.
//!
//! A `Session` is one connection's plumbing: the read and write
//! buffers, the stream's reader and writer, and the `ConnState` that
//! pulls frames through them. The front-end owns the accept loop AND
//! the event dispatch, so its handler is a concrete type and every
//! call on it is a plain call:
//!
//!     const session = try pgwire.Session.open(alloc, &stream, .{});
//!     defer session.close();
//!     while (!session.closed()) {
//!         const state = &session.state;
//!         const ev = try session.nextEvent();
//!         defer state.releaseBuffers();
//!         switch (ev) {
//!             .startup_complete => |params| try h.onStartup(state, params),
//!             .query => |sql| {
//!                 try h.onQuery(state, sql);
//!                 try state.endCommand();
//!             },
//!             .execute => |ctx| try h.onExecute(state, ctx),
//!             .describe => |ctx| try h.onDescribe(state, ctx),
//!             .sync, .flush, .terminate => {},
//!         }
//!         if (pgwire.Session.flushAfter(ev)) try session.flush();
//!     }
//!
//! What each event owes (see `conn_state.Event`):
//!
//!  - `startup_complete`: the state machine already queued AuthOk +
//!    ParameterStatus + ReadyForQuery. The params borrow the read
//!    buffer — dupe anything kept.
//!  - `query`: the handler emits RowDescription (0..1) → DataRow
//!    (0..N) → CommandComplete (or sendError), and the front-end
//!    then calls `endCommand` for the trailing ReadyForQuery.
//!  - `execute`: the same row stream, but NO ReadyForQuery — an
//!    extended batch closes on Sync, which the state machine answers
//!    itself. An error must also set `state.ext_failed` so the rest
//!    of the batch is dropped until Sync.
//!  - `describe`: RowDescription or NoData (ParameterDescription was
//!    already sent for the statement kind).
//!  - `sync`, `flush`, `terminate`: nothing; the flush after the
//!    switch sends what the state machine queued.
//!
//! Replies to Execute and Describe stay in the write buffer until the
//! batch's Sync (or a Flush, or the buffer filling), as Postgres does:
//! one socket write per batch instead of one per message. `flushAfter`
//! says which events end a batch.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;

const _conn_state = @import("conn_state.zig");
const _proto = @import("proto.zig");

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
    conn: _conn_state.Config = .{},
};

/// One connection's I/O plumbing around a `ConnState`. Heap-allocated
/// (`open`) because the state machine keeps pointers to the reader
/// and writer interfaces, which in turn find their owner by field
/// offset — none of it may move. The caller still owns the stream;
/// `close` does not close it.
pub const Session = struct {
    alloc: std.mem.Allocator,
    read_buf: []u8,
    write_buf: []u8,
    reader: Stream.Reader,
    writer: Stream.Writer,
    state: _conn_state.ConnState,

    pub fn open(
        alloc: std.mem.Allocator,
        stream: *Stream,
        opts: Options,
    ) !*Session {
        const self = try alloc.create(Session);
        errdefer alloc.destroy(self);
        // Heap-allocated buffers — fibers have small stacks, so we
        // never put multi-KiB buffers on the stack here.
        self.alloc = alloc;
        self.read_buf = try alloc.alloc(u8, opts.read_chunk);
        errdefer alloc.free(self.read_buf);
        self.write_buf = try alloc.alloc(u8, opts.write_chunk);
        errdefer alloc.free(self.write_buf);
        // TODO: Add timeouts
        self.reader = stream.reader(self.read_buf);
        self.writer = stream.writer(self.write_buf);
        self.state = _conn_state.ConnState.init(
            alloc,
            &self.reader.interface,
            &self.writer.interface,
            opts.conn,
        );
        return self;
    }

    /// Final flush — a trailing fatal error written during the last
    /// event — then teardown. The connection is dying; flush errors
    /// are swallowed.
    pub fn close(self: *Session) void {
        self.writer.interface.flush() catch {};
        self.state.deinit();
        self.alloc.free(self.write_buf);
        self.alloc.free(self.read_buf);
        self.alloc.destroy(self);
    }

    /// The peer terminated, the reader hit EOF, or a fatal error went
    /// out: the loop is over.
    pub fn closed(self: *const Session) bool {
        return self.state.phase == .closed;
    }

    /// The next event. The state machine pulls bytes through the
    /// reader itself — when its buffer is empty, the reader's drain
    /// hook fiber-blocks on the socket.
    pub fn nextEvent(self: *Session) !_conn_state.Event {
        return self.state.nextEvent();
    }

    /// Send what the event queued: auto-flush already fired whenever
    /// the write buffer filled (large result sets); this is the
    /// logical boundary — ReadyForQuery, an error, a Sync.
    pub fn flush(self: *Session) !void {
        try self.writer.interface.flush();
    }

    /// Whether the replies to `ev` go on the wire now. Execute and
    /// Describe replies wait for the batch's Sync or a Flush; every
    /// other event ends what the client is waiting on.
    pub fn flushAfter(ev: _conn_state.Event) bool {
        return switch (ev) {
            .execute, .describe => false,
            .startup_complete, .query, .sync, .flush, .terminate => true,
        };
    }
};

test {
    std.testing.refAllDecls(@This());
}
