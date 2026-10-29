// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Pure-logic Postgres connection state machine. NO sockets, no runtime I/O.
//!
//! Caller pattern:
//!   1. Construct a `ConnState` with input/output writers, allocator,
//!      and config. The state machine pulls bytes from `in` and pushes
//!      frames into `out` — both are `*std.Io.Reader` / `*std.Io.Writer`
//!      interfaces, so production wires them to `fiber.net.Stream.Reader`
//!      / `Writer` and tests use `Reader.fixed` / `Writer.Allocating`.
//!   2. Call `nextEvent()` in a loop. It blocks pulling bytes from `in`
//!      (or returns `error.EndOfStream` for an exhausted fixed reader),
//!      processes frames, and surfaces high-level events. Outbound
//!      frames flow into `out` as a side effect.
//!   3. On `query` events the caller produces rows and calls one of
//!      `sendRowDescription`/`sendDataRow`/`sendCommandComplete` (or
//!      `sendError`), then `endCommand` to emit ReadyForQuery.
//!
//! The split between "state machine" and "driver" exists so this file
//! has zero socket / runtime code. Tests drive it with byte fixtures via
//! `Reader.fixed` and inspect the output via `Allocating.writer.buffered()`.

const std = @import("std");
const utils = @import("utils");
const String = utils.String;
const _proto = @import("proto.zig");
const _types = @import("types.zig");
/// `-Dversion`: the release tag, or `dev`.
const egres_version: String = @import("build_options").version;

pub const Phase = enum {
    /// Awaiting the very first frame (Startup3 / SSLRequest / etc.).
    awaiting_startup,
    /// Authenticated; awaiting a Query / Terminate / future extended
    /// query frames.
    ready,
    /// Mid-query: the caller has begun emitting a result and must
    /// finish before reading the next event. We don't actually enforce
    /// this — but the phase makes test assertions simpler.
    in_query,
    /// Connection terminated (peer sent 'X', clean EOF, or we sent a
    /// fatal error).
    closed,
};

/// A prepared statement registered by Parse. Owned by the registry.
pub const Statement = struct {
    sql: []u8,
    param_oids: []u32,
};

/// A portal bound to a statement by Bind. Owned by the registry.
pub const Portal = struct {
    stmt_name: []u8,
    /// An Execute stopped at its row limit with rows left. Postgres
    /// resumes such a portal on the next Execute; here the next
    /// Execute is refused, since the statement's cursor is gone with
    /// it. A Bind to the same name replaces the portal.
    suspended: bool = false,
    /// Per-param raw bytes; null = SQL NULL. The interpretation
    /// (text vs binary) is dictated by `param_formats`, which is
    /// expanded out per-param so handlers don't have to redo the
    /// 0-length / 1-length / per-param dispatch from the wire.
    params: []?[]u8,
    /// One format code per param; same length as `params`.
    /// 0 = text, 1 = binary.
    param_formats: []i16,
    result_formats: []i16,
};

/// Resolved view of an Execute: the SQL we'd run plus the bound
/// param values. Slices borrow from the portal's owned storage and
/// stay valid until the portal is closed (or until Sync if it's the
/// unnamed portal — see Postgres protocol §3.4).
pub const ExecuteCtx = struct {
    sql: String,
    params: []const ?String,
    /// One format code per param (0=text, 1=binary). Same length
    /// as `params`. The handler is responsible for decoding each
    /// param's bytes according to its format — see
    /// `pgwire.binary` for the primitive binary decoders.
    param_formats: []const i16,
    /// The parameter types the client declared in Parse, by position;
    /// shorter than `params` (or empty) when it left them to the
    /// backend, and 0 for an individual unspecified one. A binary
    /// value is decoded by its declared type.
    param_oids: []const u32,
    /// One format code per result column. Wire conventions: empty
    /// means "all text"; length 1 means "apply to every column";
    /// otherwise per-column. We pass the raw shape through so the
    /// handler can mirror the spec rather than expanding here.
    result_formats: []const i16,
    /// The Execute's row limit; 0 = every row.
    max_rows: u32,
};

pub const DescribeKind = _proto.DescribeKind;

/// Resolved view of a Describe. For 'statement' kind, `params` is
/// empty (Bind hasn't happened yet). For 'portal' kind, `params`
/// holds the bound values + per-param formats. The state machine has
/// already emitted ParameterDescription before surfacing this event
/// for the statement kind, so the handler only owes RowDescription /
/// NoData.
pub const DescribeCtx = struct {
    kind: DescribeKind,
    sql: String,
    params: []const ?String,
    /// Per-param format codes (text/binary). Empty for statement
    /// kind; same length as `params` for portal kind.
    param_formats: []const i16 = &.{},
};

pub const Event = union(enum) {
    /// Completed startup parameter parse. Slices borrow from
    /// `scratch_in`; copy them out before the next `nextEvent` call,
    /// which resets scratch_in.
    startup_complete: _proto.StartupParams,
    /// A simple Query message arrived. `sql` borrows from `scratch_in`
    /// and is invalidated on the next `nextEvent` call.
    query: String,
    /// Extended-query Execute. Slices borrow from the portal's
    /// alloc-owned storage (stable across `nextEvent` until the portal
    /// is closed).
    execute: ExecuteCtx,
    /// Extended-query Describe. Slices borrow from statement/portal
    /// alloc-owned storage. State machine has already emitted
    /// ParameterDescription for kind=statement.
    describe: DescribeCtx,
    /// Extended-query Sync arrived. State machine has already emitted
    /// ReadyForQuery; this is just a notification so the driver can
    /// flush.
    sync,
    /// Extended-query Flush arrived: the client wants every reply
    /// queued so far on the wire now, ahead of any Sync. The front-end
    /// flushes; nothing else changes.
    flush,
    /// Frontend asked us to close ('X' frame), the peer cleanly closed
    /// the reader (EOF before next header), or we sent a fatal error.
    /// Caller should drop the connection.
    terminate,
};

pub const Config = struct {
    /// Sent in BackendKeyData. We have no cancel support, so a
    /// constant is fine; see TODO on encodeBackendKeyData in proto.zig.
    backend_pid: u32 = 1,
    backend_secret: u32 = 0,
    /// Sent in ParameterStatus messages right after AuthenticationOk.
    /// libpq will pick these up and use them to format input/output.
    /// The server version is the Postgres release Egres presents:
    /// clients parse the leading number to pick their code paths and
    /// read nothing past it, as with a vendor suffix on Postgres.
    server_version: String = "16.0 (Egres " ++ egres_version ++ ")",
    egres_version: String = egres_version,
    server_encoding: String = "UTF8",
    client_encoding: String = "UTF8",
    date_style: String = "ISO, MDY",
    timezone: String = "UTC",
    interval_style: String = "postgres",
    /// Username we report back as the session role. Real Postgres
    /// echoes the StartupMessage's `user` field; we use a constant
    /// because there's no auth/role backing yet.
    /// TODO: echo the actual `user` from the StartupMessage params
    /// (paired with the auth TODO on encodeAuthenticationOk).
    session_authorization: String = "postgres",
    /// Initial transaction status to send after every query batch.
    /// Always idle today because we have no transaction support.
    /// TODO: real BEGIN/COMMIT/ROLLBACK driving tx_status to .in_tx /
    /// .failed_tx.
    initial_tx_status: _types.TxStatus = .idle,
};

/// Errors surfaced from the state machine. Reader/Writer interface
/// errors flow through verbatim — the driver translates them into
/// "connection failed" handling. Body-parse failures collapse into
/// `BadProtocol`.
pub const StateError = error{
    /// Frame on the wire was malformed (length mismatch, bad header,
    /// body parser rejected it).
    BadProtocol,
    /// Allocation failure inside the state machine (statement /
    /// portal registries, scratch_in growth).
    OutOfMemory,
} || std.Io.Reader.Error || std.Io.Writer.Error;

pub const ConnState = struct {
    cfg: Config,
    phase: Phase = .awaiting_startup,

    /// Caller-supplied reader the state machine pulls input bytes
    /// from. Production: `fiber.net.Stream.Reader`; tests:
    /// `std.Io.Reader.fixed(bytes)`. The reader must have a buffer
    /// of at least 8 bytes (we peek the largest header — startup's
    /// length+code — before tossing).
    in: *std.Io.Reader,
    /// Caller-supplied writer the state machine sends frames into.
    /// Production: `fiber.net.Stream.Writer` for streaming flushes;
    /// tests: `std.Io.Writer.Allocating` (inspect via `.buffered()`).
    out: *std.Io.Writer,
    /// Body of the *current* input frame. `clearRetainingCapacity`'d
    /// at the top of every frame; grows to fit the largest body seen.
    /// Event slices that point here (`startup_complete.params`,
    /// `query.sql`) are invalidated on the next `nextEvent` call.
    scratch_in: std.ArrayList(u8) = .empty,
    /// Reusable scratch for building length-prefixed *output* frame
    /// bodies before writeFrame ships them to `out`.
    scratch_out: std.Io.Writer.Allocating,
    alloc: std.mem.Allocator,
    /// Transaction status byte for ReadyForQuery; the SQL handler flips
    /// it on BEGIN/COMMIT/ROLLBACK and statement failure.
    tx_status: _types.TxStatus = .idle,
    /// True once `sendError` fired. The SQL handler clears it per
    /// statement and reads it afterwards — "did this statement error?" —
    /// to drive failed-transaction state without threading a result
    /// through every reply site.
    error_sent: bool = false,

    /// Registry of prepared statements by name. Empty string is the
    /// "unnamed" slot, overwritten on each Parse with name "".
    statements: std.StringHashMapUnmanaged(Statement) = .{},
    /// Registry of portals by name. Empty string is the unnamed
    /// portal, overwritten on each Bind to "".
    portals: std.StringHashMapUnmanaged(Portal) = .{},
    /// The name of the portal the current Execute runs, the map's own
    /// key (valid until that portal is dropped), so sendPortalSuspended
    /// can mark it.
    exec_portal: String = "",
    /// Once an error fires inside an ext-query batch, every Parse/
    /// Bind/Execute/Describe/Close after it is silently dropped
    /// until Sync, per Postgres protocol §4.1.4. Cleared on Sync.
    ext_failed: bool = false,

    pub fn init(
        alloc: std.mem.Allocator,
        in: *std.Io.Reader,
        out: *std.Io.Writer,
        cfg: Config,
    ) ConnState {
        return .{
            .alloc = alloc,
            .in = in,
            .out = out,
            .scratch_out = .init(alloc),
            .cfg = cfg,
            .tx_status = cfg.initial_tx_status,
        };
    }

    pub fn deinit(self: *ConnState) void {
        self.scratch_in.deinit(self.alloc);
        self.scratch_out.deinit();
        self.dropAllStatements();
        self.dropAllPortals();
        self.statements.deinit(self.alloc);
        self.portals.deinit(self.alloc);
    }

    fn freeStatement(self: *ConnState, s: *Statement) void {
        self.alloc.free(s.sql);
        self.alloc.free(s.param_oids);
    }

    fn freePortal(self: *ConnState, p: *Portal) void {
        self.alloc.free(p.stmt_name);
        for (p.params) |maybe| if (maybe) |bytes| self.alloc.free(bytes);
        self.alloc.free(p.params);
        self.alloc.free(p.param_formats);
        self.alloc.free(p.result_formats);
    }

    fn dropAllStatements(self: *ConnState) void {
        var it = self.statements.iterator();
        while (it.next()) |e| {
            self.alloc.free(e.key_ptr.*);
            self.freeStatement(e.value_ptr);
        }
        self.statements.clearRetainingCapacity();
    }

    fn dropAllPortals(self: *ConnState) void {
        var it = self.portals.iterator();
        while (it.next()) |e| {
            self.alloc.free(e.key_ptr.*);
            self.freePortal(e.value_ptr);
        }
        self.portals.clearRetainingCapacity();
        self.exec_portal = "";
    }

    /// Drop a single statement and any portals derived from it.
    /// Per spec §3.4, closing a statement implicitly closes its
    /// portals.
    fn dropStatement(self: *ConnState, name: String) void {
        if (self.statements.fetchRemove(name)) |kv| {
            var stmt = kv.value;
            self.alloc.free(kv.key);
            self.freeStatement(&stmt);
        }
        // Walk portals and drop any that reference this statement.
        // Two-pass to avoid mutating during iteration.
        var to_remove: std.ArrayList(String) = .empty;
        defer to_remove.deinit(self.alloc);
        var it = self.portals.iterator();
        while (it.next()) |e| {
            if (std.mem.eql(u8, e.value_ptr.stmt_name, name)) {
                to_remove.append(self.alloc, e.key_ptr.*) catch return;
            }
        }
        for (to_remove.items) |k| self.dropPortal(k);
    }

    fn dropPortal(self: *ConnState, name: String) void {
        if (self.portals.fetchRemove(name)) |kv| {
            var portal = kv.value;
            // exec_portal is this key's slice; never leave it dangling.
            if (self.exec_portal.ptr == kv.key.ptr) self.exec_portal = "";
            self.alloc.free(kv.key);
            self.freePortal(&portal);
        }
    }

    /// Pull the next high-level event from the wire. Blocks on `in`
    /// when no buffered bytes are available (production), or returns
    /// `error.EndOfStream` for an exhausted fixed reader (tests).
    /// Clean EOF before the next header is mapped to `.terminate`.
    pub fn nextEvent(self: *ConnState) StateError!Event {
        return switch (self.phase) {
            .awaiting_startup => self.consumeStartup(),
            .ready, .in_query => self.consumeReady(),
            .closed => .terminate,
        };
    }

    /// Read one startup-class frame ([u32 len][u32 code][body])
    /// into scratch_in and return the code. Asserts len ≥ 8 (length
    /// includes itself + code). On clean EOF before the length,
    /// returns `error.EndOfStream` for the caller to map to a
    /// `.terminate` event.
    fn readStartupFrame(self: *ConnState) StateError!u32 {
        const len = try self.in.takeInt(u32, .big);
        if (len < 8 or len > _proto.max_message_body) return error.BadProtocol;
        const code = try self.in.takeInt(u32, .big);
        try self.scratch_in.resize(self.alloc, len - 8);
        try self.in.readSliceAll(self.scratch_in.items);
        return code;
    }

    /// Read one tagged frame ([u8 tag][u32 len][body]) into
    /// scratch_in and return the tag byte. Length includes itself
    /// (not the tag); body is `len - 4` bytes. EOF before the header
    /// returns `error.EndOfStream` for caller to map to `.terminate`.
    fn readTaggedFrame(self: *ConnState) StateError!u8 {
        const tag = try self.in.takeByte();
        const len = try self.in.takeInt(u32, .big);
        if (len < 4 or len > _proto.max_message_body + 4) return error.BadProtocol;
        try self.scratch_in.resize(self.alloc, len - 4);
        try self.in.readSliceAll(self.scratch_in.items);
        return tag;
    }

    fn consumeStartup(self: *ConnState) StateError!Event {
        while (true) {
            const code = self.readStartupFrame() catch |err| switch (err) {
                error.EndOfStream => {
                    self.phase = .closed;
                    return .terminate;
                },
                else => return err,
            };

            // SSLRequest / GSSEncRequest: client sniffs to see if we
            // support TLS. We send 'N' (no) and let it retry plain.
            // The byte goes raw — not framed — per protocol. It must
            // reach the wire before we read again: the client sends
            // nothing more until it sees the answer.
            if (code == _types.ssl_request_code or
                code == _types.gss_enc_request_code)
            {
                try self.out.writeByte('N');
                try self.out.flush();
                continue;
            }
            // CancelRequest: we have no cancel keys, so ignore and
            // close immediately. libpq opens a separate socket for
            // this so dropping it is safe.
            if (code == _types.cancel_request_code) {
                self.phase = .closed;
                return .terminate;
            }
            if (code != _types.protocol_version_3) {
                // Unknown version — send a fatal error and close.
                try self.emitFatal("unsupported frontend protocol", .{
                    'X', 'X', '0', '0', '0',
                });
                self.phase = .closed;
                return .terminate;
            }

            const params = _proto.parseStartupParams(self.scratch_in.items) catch
                return error.BadProtocol;

            // AuthenticationOk + a small bundle of ParameterStatus +
            // BackendKeyData + ReadyForQuery. Everything goes into
            // `out` in one batch so the driver can flush once.
            try _proto.encodeAuthenticationOk(self.out);
            // The set of ParameterStatus messages a real Postgres
            // backend sends after AuthOk. Some clients refuse to
            // proceed without specific entries — pgx's simple-query
            // mode, for example, hard-requires
            // `standard_conforming_strings=on`.
            const announce = [_]struct { k: String, v: String }{
                .{ .k = "server_version", .v = self.cfg.server_version },
                .{ .k = "egres_version", .v = self.cfg.egres_version },
                .{ .k = "server_encoding", .v = self.cfg.server_encoding },
                .{ .k = "client_encoding", .v = self.cfg.client_encoding },
                .{ .k = "DateStyle", .v = self.cfg.date_style },
                .{ .k = "TimeZone", .v = self.cfg.timezone },
                .{ .k = "IntervalStyle", .v = self.cfg.interval_style },
                .{ .k = "integer_datetimes", .v = "on" },
                .{ .k = "standard_conforming_strings", .v = "on" },
                .{ .k = "is_superuser", .v = "off" },
                .{ .k = "session_authorization", .v = self.cfg.session_authorization },
                .{ .k = "application_name", .v = "" },
            };
            for (announce) |p| {
                try _proto.encodeParameterStatus(&self.scratch_out, p.k, p.v);
                try _proto.writeFrame(self.out, .parameter_status, self.scratch_out.writer.buffered());
            }
            try _proto.encodeBackendKeyData(
                self.out,
                self.cfg.backend_pid,
                self.cfg.backend_secret,
            );
            try _proto.encodeReadyForQuery(self.out, self.tx_status);

            self.phase = .ready;
            return .{ .startup_complete = params };
        }
    }

    fn consumeReady(self: *ConnState) StateError!Event {
        // Loop over silent messages (Parse / Bind / Close / Flush /
        // dropped-while-failed) so a pipelined batch like
        // Parse+Describe+Sync surfaces the Describe event in the same
        // call rather than returning intermediate "nothing happened"
        // signals. Each iteration consumes exactly one frame.
        while (true) {
            const tag_byte = self.readTaggedFrame() catch |err| switch (err) {
                error.EndOfStream => {
                    self.phase = .closed;
                    return .terminate;
                },
                else => return err,
            };

            const tag: _proto.FrontendTag = blk: {
                inline for (std.meta.fields(_proto.FrontendTag)) |f| {
                    if (f.value == tag_byte) {
                        break :blk @enumFromInt(f.value);
                    }
                }
                try self.emitFatal(
                    "unknown frontend message",
                    _types.sqlstate.internal_error,
                );
                self.phase = .closed;
                return .terminate;
            };

            const body = self.scratch_in.items;
            const is_ext_msg = switch (tag) {
                .parse, .bind, .execute, .describe, .close, .flush => true,
                else => false,
            };
            if (self.ext_failed and is_ext_msg) continue;

            switch (tag) {
                .query => {
                    const sql = _proto.parseQuery(body) catch
                        return error.BadProtocol;
                    self.phase = .in_query;
                    self.ext_failed = false;
                    return .{ .query = sql };
                },
                .terminate => {
                    self.phase = .closed;
                    return .terminate;
                },
                .parse => {
                    try self.handleParse(body);
                    continue;
                },
                .bind => {
                    try self.handleBind(body);
                    continue;
                },
                .execute => {
                    if (try self.handleExecute(body)) |ev| return ev;
                    continue;
                },
                .describe => {
                    if (try self.handleDescribe(body)) |ev| return ev;
                    continue;
                },
                .close => {
                    try self.handleClose(body);
                    continue;
                },
                .sync => {
                    self.ext_failed = false;
                    try _proto.encodeReadyForQuery(self.out, self.tx_status);
                    return .sync;
                },
                .flush => return .flush,
                else => {
                    try self.emitErrorReady(
                        "unsupported message",
                        _types.sqlstate.internal_error,
                    );
                    self.ext_failed = true;
                    continue;
                },
            }
        }
    }

    // ---------- Extended-query handlers ----------

    /// Mark current ext-batch as failed and emit ErrorResponse. The
    /// caller-issued error message lands in `out`; everything else
    /// extended-query-shaped is dropped until Sync.
    fn extError(
        self: *ConnState,
        msg: String,
        code: [5]u8,
    ) StateError!void {
        self.ext_failed = true;
        try self.writeError(.{
            .severity = _types.severity_error,
            .code = code,
            .message = msg,
        });
    }

    fn handleParse(self: *ConnState, body: String) StateError!void {
        // Cap how many param OIDs a single Parse may carry. Postgres
        // tolerates int16-max but real workloads stay tiny; this
        // gives us a stack-friendly upper bound.
        var oids_storage: [256]u32 = undefined;
        const p = _proto.parseParse(body, &oids_storage) catch {
            try self.extError(
                "malformed Parse message",
                _types.sqlstate.internal_error,
            );
            return;
        };

        // Replace any existing entry under the same name. Free the
        // old one — statement deletion implicitly drops dependent
        // portals, and we drop them on re-Parse too since the bound
        // SQL would otherwise be stale.
        if (self.statements.contains(p.name)) self.dropStatement(p.name);

        const sql_dup = self.alloc.dupe(u8, p.sql) catch return error.OutOfMemory;
        errdefer self.alloc.free(sql_dup);
        const oids_dup = self.alloc.dupe(u32, p.param_oids) catch
            return error.OutOfMemory;
        errdefer self.alloc.free(oids_dup);
        const name_dup = self.alloc.dupe(u8, p.name) catch
            return error.OutOfMemory;
        errdefer self.alloc.free(name_dup);

        try self.statements.put(self.alloc, name_dup, .{
            .sql = sql_dup,
            .param_oids = oids_dup,
        });
        try _proto.encodeParseComplete(self.out);
    }

    fn handleBind(self: *ConnState, body: String) StateError!void {
        // Same comment as handleParse re: the upper bounds.
        var pfmt_storage: [256]i16 = undefined;
        var params_storage: [256]_proto.BindParam = undefined;
        var rfmt_storage: [256]i16 = undefined;
        const b = _proto.parseBind(
            body,
            &pfmt_storage,
            &params_storage,
            &rfmt_storage,
        ) catch {
            try self.extError(
                "malformed Bind message",
                _types.sqlstate.internal_error,
            );
            return;
        };

        if (!self.statements.contains(b.statement_name)) {
            try self.extError(
                "Bind references unknown prepared statement",
                _types.sqlstate.internal_error,
            );
            return;
        }

        // Both 0 (text) and 1 (binary) are valid format codes; the
        // protocol leaves higher numbers reserved. Reject anything
        // else early so we don't carry garbage through to the
        // handler. Per-OID dispatch (i.e. "do we actually have a
        // binary decoder for *this* OID?") is the handler's job —
        // the state machine just enforces protocol shape.
        for (b.param_formats) |f| if (f != 0 and f != 1) {
            try self.extError(
                "invalid parameter format code (must be 0 or 1)",
                _types.sqlstate.internal_error,
            );
            return;
        };
        for (b.result_formats) |f| if (f != 0 and f != 1) {
            try self.extError(
                "invalid result format code (must be 0 or 1)",
                _types.sqlstate.internal_error,
            );
            return;
        };

        // Drop any existing portal under the same name (replace
        // semantics — same as Parse with the unnamed slot).
        if (self.portals.contains(b.portal_name)) self.dropPortal(b.portal_name);

        const stmt_dup = self.alloc.dupe(u8, b.statement_name) catch
            return error.OutOfMemory;
        errdefer self.alloc.free(stmt_dup);

        const params_dup = self.alloc.alloc(?[]u8, b.params.len) catch
            return error.OutOfMemory;
        errdefer self.alloc.free(params_dup);
        // Initialize all entries to null first so partial-failure
        // cleanup in errdefer can't deref garbage.
        for (params_dup) |*slot| slot.* = null;
        errdefer for (params_dup) |maybe| {
            if (maybe) |bytes| self.alloc.free(bytes);
        };
        for (b.params, 0..) |maybe, i| {
            if (maybe) |bytes| {
                params_dup[i] = self.alloc.dupe(u8, bytes) catch
                    return error.OutOfMemory;
            }
        }

        // Expand the wire-shape param_formats (size 0 / 1 / N) into
        // a per-param array so handlers don't have to redo the
        // dispatch. 0 → all text; 1 → broadcast that single code.
        const pfmt_dup = self.alloc.alloc(i16, b.params.len) catch
            return error.OutOfMemory;
        errdefer self.alloc.free(pfmt_dup);
        if (b.param_formats.len == 0) {
            for (pfmt_dup) |*slot| slot.* = 0;
        } else if (b.param_formats.len == 1) {
            for (pfmt_dup) |*slot| slot.* = b.param_formats[0];
        } else if (b.param_formats.len == b.params.len) {
            @memcpy(pfmt_dup, b.param_formats);
        } else {
            try self.extError(
                "Bind: param_formats count must be 0, 1, or n_params",
                _types.sqlstate.internal_error,
            );
            return;
        }

        const rfmt_dup = self.alloc.dupe(i16, b.result_formats) catch
            return error.OutOfMemory;
        errdefer self.alloc.free(rfmt_dup);

        const portal_name_dup = self.alloc.dupe(u8, b.portal_name) catch
            return error.OutOfMemory;
        errdefer self.alloc.free(portal_name_dup);

        try self.portals.put(self.alloc, portal_name_dup, .{
            .stmt_name = stmt_dup,
            .params = params_dup,
            .param_formats = pfmt_dup,
            .result_formats = rfmt_dup,
        });
        try _proto.encodeBindComplete(self.out);
    }

    /// Returns null when an error was emitted (caller should continue
    /// the consumeReady loop), or the `.execute` event when the
    /// portal+statement resolved successfully.
    fn handleExecute(self: *ConnState, body: String) StateError!?Event {
        const e = _proto.parseExecute(body) catch {
            try self.extError(
                "malformed Execute message",
                _types.sqlstate.internal_error,
            );
            return null;
        };
        const entry = self.portals.getEntry(e.portal_name) orelse {
            try self.extError(
                "Execute references unknown portal",
                _types.sqlstate.internal_error,
            );
            return null;
        };
        const portal = entry.value_ptr.*;
        if (portal.suspended) {
            try self.extError(
                "resuming a suspended portal is not supported",
                _types.sqlstate.feature_not_supported,
            );
            return null;
        }
        const stmt = self.statements.get(portal.stmt_name) orelse {
            // Should be unreachable since Bind validates the
            // statement name, but defensively close out.
            try self.extError(
                "Execute portal references missing statement",
                _types.sqlstate.internal_error,
            );
            return null;
        };

        self.phase = .in_query;
        self.exec_portal = entry.key_ptr.*;
        return .{ .execute = .{
            .sql = stmt.sql,
            .params = @ptrCast(portal.params),
            .param_formats = portal.param_formats,
            .param_oids = stmt.param_oids,
            .result_formats = portal.result_formats,
            .max_rows = e.max_rows,
        } };
    }

    /// Same null-vs-event convention as handleExecute.
    fn handleDescribe(self: *ConnState, body: String) StateError!?Event {
        const d = _proto.parseDescribe(body) catch {
            try self.extError(
                "malformed Describe message",
                _types.sqlstate.internal_error,
            );
            return null;
        };
        switch (d.kind) {
            .statement => {
                const stmt = self.statements.get(d.name) orelse {
                    try self.extError(
                        "Describe references unknown statement",
                        _types.sqlstate.internal_error,
                    );
                    return null;
                };
                // ParameterDescription is fully owned by the state
                // machine — we just echo whatever Parse said.
                try _proto.encodeParameterDescription(&self.scratch_out, stmt.param_oids);
                try _proto.writeFrame(self.out, .parameter_description, self.scratch_out.writer.buffered());
                return .{ .describe = .{
                    .kind = .statement,
                    .sql = stmt.sql,
                    .params = &.{},
                } };
            },
            .portal => {
                const portal = self.portals.get(d.name) orelse {
                    try self.extError(
                        "Describe references unknown portal",
                        _types.sqlstate.internal_error,
                    );
                    return null;
                };
                const stmt = self.statements.get(portal.stmt_name) orelse {
                    try self.extError(
                        "Describe portal missing statement",
                        _types.sqlstate.internal_error,
                    );
                    return null;
                };
                return .{ .describe = .{
                    .kind = .portal,
                    .sql = stmt.sql,
                    .params = @ptrCast(portal.params),
                    .param_formats = portal.param_formats,
                } };
            },
        }
    }

    fn handleClose(self: *ConnState, body: String) StateError!void {
        const c = _proto.parseClose(body) catch {
            try self.extError(
                "malformed Close message",
                _types.sqlstate.internal_error,
            );
            return;
        };
        switch (c.kind) {
            .statement => self.dropStatement(c.name),
            .portal => self.dropPortal(c.name),
        }
        // Spec says CloseComplete is emitted whether or not the
        // target existed.
        try _proto.encodeCloseComplete(self.out);
    }

    /// Emit an ErrorResponse frame from a pre-built ErrorField. Shared
    /// by `emitFatal`, `emitErrorReady`, `extError`, and the public
    /// `sendError` — they all build the body in scratch and ship it
    /// here.
    fn writeError(self: *ConnState, f: _proto.ErrorField) StateError!void {
        try _proto.encodeErrorResponse(&self.scratch_out, f);
        try _proto.writeFrame(self.out, .error_response, self.scratch_out.writer.buffered());
    }

    fn emitFatal(
        self: *ConnState,
        msg: String,
        code: [5]u8,
    ) StateError!void {
        try self.writeError(.{
            .severity = "FATAL",
            .code = code,
            .message = msg,
        });
    }

    fn emitErrorReady(
        self: *ConnState,
        msg: String,
        code: [5]u8,
    ) StateError!void {
        try self.writeError(.{
            .severity = _types.severity_error,
            .code = code,
            .message = msg,
        });
        try _proto.encodeReadyForQuery(self.out, self.tx_status);
    }

    // ---------- Caller-side response builders ----------

    pub fn sendRowDescription(
        self: *ConnState,
        fields: []const _types.FieldDescription,
    ) StateError!void {
        try _proto.encodeRowDescription(&self.scratch_out, fields);
        try _proto.writeFrame(self.out, .row_description, self.scratch_out.writer.buffered());
    }

    pub fn sendDataRow(
        self: *ConnState,
        cols: []const _proto.DataValue,
    ) StateError!void {
        try _proto.encodeDataRow(&self.scratch_out, cols);
        try _proto.writeFrame(self.out, .data_row, self.scratch_out.writer.buffered());
    }

    /// Streaming DataRow: format each column directly into the output scratch,
    /// then `finish`. See proto.DataRowWriter — one format pass, one frame
    /// write (no per-cell buffering).
    pub fn beginDataRow(self: *ConnState, field_count: u16) StateError!_proto.DataRowWriter {
        return _proto.DataRowWriter.begin(&self.scratch_out, self.out, field_count);
    }

    pub fn sendCommandComplete(self: *ConnState, tag: String) StateError!void {
        try _proto.encodeCommandComplete(&self.scratch_out, tag);
        try _proto.writeFrame(self.out, .command_complete, self.scratch_out.writer.buffered());
    }

    pub fn sendEmptyQueryResponse(self: *ConnState) StateError!void {
        try _proto.encodeEmptyQueryResponse(self.out);
    }

    /// PortalSuspended ('s') in place of CommandComplete: the Execute's
    /// row limit was reached with rows left. The portal is marked so
    /// the next Execute on it is refused rather than restarted.
    pub fn sendPortalSuspended(self: *ConnState) StateError!void {
        if (self.portals.getPtr(self.exec_portal)) |portal|
            portal.suspended = true;
        try _proto.encodePortalSuspended(self.out);
    }

    pub fn sendError(self: *ConnState, f: _proto.ErrorField) StateError!void {
        self.error_sent = true;
        try self.writeError(f);
    }

    /// NoData ('n'): legal response to Describe when no result rows
    /// will be produced (e.g. INSERT/DELETE without RETURNING).
    pub fn sendNoData(self: *ConnState) StateError!void {
        try _proto.encodeNoData(self.out);
    }

    /// Wrap up the current command: emit ReadyForQuery and flip back
    /// to `.ready`.
    pub fn endCommand(self: *ConnState) StateError!void {
        try _proto.encodeReadyForQuery(self.out, self.tx_status);
        self.phase = .ready;
    }
};

test {
    std.testing.refAllDecls(@This());
}
