// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! The SQL front-end: a pgwire listener over the engine. Owns the
//! accept loop, the per-database txn machinery (lock table + commit
//! oracle + table cache), and the per-connection Handler — session
//! state, statement dispatch, the DML restart driver, and transaction
//! lifetime. The statements themselves live in siblings: ddl.zig
//! (catalog/manifest choreography), dml.zig (INSERT/SELECT/DELETE/
//! UPDATE), plan.zig (index vs. scan) and exec.zig (the row
//! pipeline). Mirrors AdminServer's accept loop, and shuts down with
//! the engine via `node.isShuttingDown()`.

const std = @import("std");
const engine = @import("engine");
const fiber = @import("zfiber");
const utils = @import("utils");
const pgwire = @import("pgwire");
const zdb = @import("zdb");
const zen = zdb.buffers;

const _catalog = @import("catalog.zig");
const _parse = @import("parse.zig");
const _ddl = @import("ddl.zig");
const _index_ddl = @import("index_ddl.zig");
const _dml = @import("dml.zig");
const _exec = @import("exec.zig");
const _row = @import("row.zig");
const _key = @import("key.zig");
const _schema = @import("schema.zig");
const _table = @import("table.zig");
const _txn = @import("txn.zig");
const _database = @import("database.zig");
const _value = @import("value.zig");
const Oracle = @import("oracle.zig").Oracle;

const log = std.log.scoped(.sql);

const String = _value.String;

/// The default database, present from a node's very first boot — created
/// by the WAL's genesis entry (see egres seedGenesis), never at runtime.
/// Sessions that name no database land here (psql defaults dbname to the
/// user name; that also misses and falls back per onStartup).
pub const default_database = "egres";

pub const Database = _database.Database;

pub const Server = struct {
    engine: *engine.Engine,
    listen_addr: fiber.net.IpAddress,
    /// Connections accepted so far; deals each new connection's fiber to
    /// the next lane worker (see runListener). Only the accept loop
    /// touches it.
    conn_seq: u64 = 0,
    /// Compiled layout of the CATALOG table's own rows — NOT a user-table
    /// schema. Every namespace's catalog (table 1) has the identical
    /// compiled-in schema (catalog.logical_schema; it can't live in a
    /// catalog, that would be circular), so one compile serves all
    /// databases. User tables' schemas live as rows inside each catalog
    /// and get their own per-(table, version) compiles when reads land.
    catalog_ps: _schema.PhysicalSchema,
    /// Compiled layout of the INDEX catalog table's rows (table 2), the
    /// same compiled-in schema in every namespace — one compile serves all.
    index_catalog_ps: _schema.PhysicalSchema,
    /// Serializes DDL: the leader-side name-uniqueness checks in
    /// buildNsCreate/buildCreateTable are only sound if one DDL
    /// runs at a time.
    ddl_mu: fiber.Mutex(.tracked) = .{},
    /// Per-database txn state (see Database), keyed by namespace id.
    /// Heap entries so held pointers stay valid across map growth, and a
    /// Database is NEVER deleted once created (DROP DATABASE teardown is
    /// deferred work) — so a pointer fetched under `databases_mu` stays
    /// valid after the lock drops. Created by the first SQL txn touching
    /// the database — see `database` for why creation must precede the
    /// txn's read_ts.
    databases_mu: fiber.Mutex(.tracked) = .{},
    databases: std.AutoHashMap(u64, *Database),
    next_txn_id: std.atomic.Value(u64) = .init(1),

    pub fn init(eng: *engine.Engine, addr: String) !Server {
        return .{
            .engine = eng,
            .listen_addr = try fiber.net.IpAddress.parseIpAndPort(addr),
            .catalog_ps = try _schema.compile(eng.allocator, _catalog.logical_schema, _catalog.logical_schema.current_version),
            .index_catalog_ps = try _schema.compile(eng.allocator, _catalog.index_logical_schema, _catalog.index_logical_schema.current_version),
            .databases = std.AutoHashMap(u64, *Database).init(eng.allocator),
        };
    }

    pub fn deinit(self: *Server) void {
        self.catalog_ps.deinit(self.engine.allocator);
        self.index_catalog_ps.deinit(self.engine.allocator);
        var dbs = self.databases.valueIterator();
        while (dbs.next()) |d| {
            d.*.deinit();
            self.engine.allocator.destroy(d.*);
        }
        self.databases.deinit();
        self.ddl_mu.deinit();
        self.databases_mu.deinit();
    }

    /// The Database for `ns`, created on first use and never deleted
    /// after — callers may hold the pointer without `databases_mu`. A
    /// txn must resolve this BEFORE reading its read_ts: once the
    /// Database exists, every later-applying commit lands in its oracle
    /// (the apply observer only updates existing Databases), so no
    /// commit a live txn could conflict with is ever missed.
    pub fn database(self: *Server, ns: u64) !*Database {
        self.databases_mu.lock(@src());
        defer self.databases_mu.unlock();
        const gop = try self.databases.getOrPut(ns);
        if (!gop.found_existing) {
            const d = self.engine.allocator.create(Database) catch |err| {
                _ = self.databases.remove(ns);
                return err;
            };
            d.* = Database.init(self.engine.allocator, ns) catch |err| {
                self.engine.allocator.destroy(d);
                _ = self.databases.remove(ns);
                return err;
            };
            gop.value_ptr.* = d;
        }
        return gop.value_ptr.*;
    }

    /// Age the per-database conflict oracles (see Oracle.rotate) on a
    /// fixed cadence: an oracle entry then lives one to two periods —
    /// the bound on oracle memory AND on how long a txn can run before
    /// horizon conservatism may force a bump or abort. Runs until
    /// shutdown; `waitTimeout` doubles as the sleep.
    pub fn runOracleRotation(self: *Server) void {
        const period_ms: u64 = 10 * 60 * 1000;

        while (!self.engine.node.isShuttingDown()) {
            const shutdown = &self.engine.node.shutdown;
            if (shutdown.waitForStart(.shutdown, period_ms)) return;

            self.databases_mu.lock(@src());
            defer self.databases_mu.unlock();

            var it = self.databases.valueIterator();
            while (it.next()) |d| d.*.oracle.rotate(period_ms - 1000);
        }
    }

    /// The commit-stream hook to register with the engine (before eng.start).
    pub fn applyObserver(self: *Server) engine.ApplyObserver {
        return .{ .ctx = self, .onData = onDataApplied };
    }

    /// Apply-fiber callback (every node): hand the committed entry to its
    /// database, which advances egid counters and records the oracle (see
    /// Database.applyCommitted). `databases_mu` is held only for the map
    /// probe — a Database is never deleted once created, so the pointer
    /// stays valid after the drop and applyCommitted runs under
    /// per-database locks only. No Database = no SQL txn state for this
    /// namespace (raw KV traffic) — nothing to maintain.
    fn onDataApplied(ctx: *anyopaque, ns: u64, items: []const zdb.kv.Item) void {
        const self: *Server = @ptrCast(@alignCast(ctx));
        self.databases_mu.lock(@src());
        const db = self.databases.get(ns);
        self.databases_mu.unlock();
        if (db) |d| d.applyCommitted(items);
    }

    /// Load `table_id`'s schema onto the heap — the part of a cache miss
    /// that needs no lock. DDL holds the table lock exclusively
    /// and DML holds it shared before it gets here, so the catalog rows
    /// cannot change under the read, and a park on it stalls no other
    /// statement's lookup and not the apply fiber. `next_egid` stays 0
    /// until publishTable seeds it.
    fn loadTable(self: *Server, db: *Database, table_id: u32) !*_table.Table {
        var loaded = try _table.loadTableSchema(
            self.engine.allocator,
            self.engine.forest,
            &self.catalog_ps,
            &self.index_catalog_ps,
            .{ .namespace = db.ns, .table_id = table_id },
        );
        errdefer loaded.deinit();
        const t = try self.engine.allocator.create(_table.Table);
        t.* = loaded;
        return t;
    }

    /// The cached Table for `table_key` — schema for INSERT/SELECT.
    pub fn openTable(
        self: *Server,
        table_key: zdb.kv.TableKey,
    ) !*_table.Table {
        return self.openTableIn(
            try self.database(table_key.namespace),
            table_key.table_id,
        );
    }

    /// `openTable` for a caller that already holds the Database — the
    /// per-statement path (Handler.openTable), which resolves it once
    /// per session instead of under `databases_mu` every statement. A
    /// hit costs a shared hold; a miss (first touch of a table) loads
    /// the schema with no lock held, then seeds and publishes under
    /// the exclusive hold — unless another session published first,
    /// in which case ours is the wasted copy (once per table per
    /// process at most).
    pub fn openTableIn(
        self: *Server,
        db: *Database,
        table_id: u32,
    ) !*_table.Table {
        if (db.cachedTable(table_id)) |t| return t;
        const fresh = try self.loadTable(db, table_id);
        const stored = db.openAndCacheTable(self.engine.forest, table_id, fresh) catch |err| {
            db.destroyTable(fresh);
            return err;
        };
        if (stored != fresh) db.destroyTable(fresh);
        return stored;
    }

    /// Evict `table_id`'s cached Table (see Database.forgetTable).
    pub fn forgetTable(self: *Server, table_key: zdb.kv.TableKey) !void {
        (try self.database(table_key.namespace))
            .forgetTable(table_key.table_id);
    }

    /// Reserve the next egid for a table (leader path). newTxn already ran
    /// the catch-up barrier, so `next_egid` reflects the applied committed
    /// tail (onDataApplied only advances it) — the id handed out is past
    /// every committed row.
    pub fn allocEgid(self: *Server, table_key: zdb.kv.TableKey) !u64 {
        return self.allocEgidIn(
            try self.database(table_key.namespace),
            table_key.table_id,
        );
    }

    /// `allocEgid` with the Database in hand (see openTableIn). A hit
    /// costs a shared hold plus one fetchAdd; the caller's shared
    /// table lock keeps the Table alive past the hold.
    pub fn allocEgidIn(self: *Server, db: *Database, table_id: u32) !u64 {
        const t = try self.openTableIn(db, table_id);
        const egid = t.next_egid.fetchAdd(1, .monotonic);
        // egids start at 1 (updateEgidAndPut seeds the counter); 0 is
        // never a row.
        std.debug.assert(egid >= 1);
        return egid;
    }

    pub fn runListener(self: *Server) void {
        const listener = self.listen_addr.listen(.{ .reuse_address = true }) catch |err| {
            log.err("listen failed: {s}", .{@errorName(err)});
            return;
        };
        defer listener.close();
        // Break the blocking `accept()` at the drain (see AdminServer): a
        // watcher fiber self-connects to our address when the drain
        // starts. No connection is taken from then on.
        fiber.spawnDetached(
            .{},
            engine.Engine.stopListenerOnStart,
            .{ self.engine, self.listen_addr, .drain },
        ) catch {};

        log.info("sql listener started on port {d} (pgwire)", .{listener.port()});
        while (!self.engine.node.shutdown.isDraining()) {
            const stream = listener.accept(.{}) catch |err| {
                log.err("accept failed: {s}", .{@errorName(err)});
                return;
            };
            // Shutdown watcher's self-connection: drop it and exit.
            if (self.engine.node.shutdown.isDraining()) {
                stream.close();
                return;
            }
            // Round-robin over the lane's workers. A connection's fiber
            // stays where it is placed (its I/O completes on that
            // worker's ring), so the spread at accept IS the steady-state
            // spread; the scheduler's local push would pile every
            // connection onto this worker and leave stealing to sort it
            // out one fiber at a time.
            self.conn_seq +%= 1;
            fiber.spawnDetached(
                .{ .worker_id = @truncate(self.conn_seq) },
                handleConn,
                .{ self, stream },
            ) catch {
                stream.close();
                continue;
            };
        }
    }
};

fn handleConn(self: *Server, stream: fiber.net.Stream) void {
    var s = stream;
    defer s.close();

    var handler = Handler{ .srv = self };
    // LIFO: deinit (roll back any open txn — locks released, pending
    // writes dropped) runs BEFORE the socket closes above. Covers clean
    // Terminate and every error exit from serve.
    defer handler.deinit();
    handler.serve(&s) catch |err| switch (err) {
        // A client that went away: psql and pgbench end this way.
        error.ReadFailed, error.WriteFailed, error.EndOfStream => {
            log.debug("conn closed: {s}", .{@errorName(err)});
        },
        else => log.warn("conn serve error: {s}", .{@errorName(err)}),
    };
}

/// Statements are counted in flight from `Ctx.attach` to `detach`; the
/// drain in egres main waits for the count to reach zero. An idle
/// connection, parked reading the socket, is not in flight: it holds
/// nothing the teardown frees and exits with the process. It is told
/// nothing either, since the fiber runtime has no read timeout to wake
/// it, so its client learns of the shutdown from the closed socket.
const admin_shutdown_msg =
    "terminating connection: server is shutting down";

/// The session's last message: a FATAL ErrorResponse the way Postgres
/// ends a session on shutdown, after which `serve` returns and the
/// socket closes. The loop sends it where a statement would have gone
/// next, so a client waiting on a reply gets this instead of EOF.
fn replyShutdown(state: *pgwire.ConnState) !void {
    try state.sendError(.{
        .severity = pgwire.severity_fatal,
        .code = pgwire.sqlstate.admin_shutdown,
        .message = admin_shutdown_msg,
    });
}

/// A parsed statement kept for the session (Handler.parsed): the
/// arena owns the libpg_query tree, the Stmt borrowing from it, and
/// the SQL text that keys the entry.
const ParsedStmt = struct {
    arena: std.heap.ArenaAllocator,
    stmt: _parse.Stmt,
};

/// How many parsed statements a session keeps before the cache is
/// emptied and refilled. pgbench has five; a driver's statement cache
/// is typically this size or smaller.
const max_parsed = 256;

/// One connection: the pgwire event loop (`serve`) and the four
/// handlers it dispatches to. Parses via
/// libpg_query and dispatches the statements this slice supports;
/// everything else is a clean error. The extended protocol's
/// Execute runs the same statements (runStatement) with three
/// differences, all read off `mode`: no RowDescription (Describe
/// sent it), no ReadyForQuery (Sync's), and an error drops the rest
/// of the batch (ext_failed).
pub const Handler = struct {
    pub const Mode = enum { simple, extended };

    srv: *Server,
    /// Which protocol the statement in flight came in on. Set by the
    /// two entry points (onQuery, onExecute) before runStatement.
    mode: Mode = .simple,
    /// The Bind values for the statement in flight (Execute), empty
    /// for a simple query. Every `$n` a statement carries resolves
    /// here (parse.litToValue).
    params: _parse.Params = .{},
    /// Parsed statements by SQL text, for the extended protocol: a
    /// prepared statement is parsed once per session, and the
    /// unnamed statement re-sent with the same text hits the same
    /// entry. Entries live until the session ends or the cache
    /// overflows (max_parsed). The simple path keeps its per-query
    /// arena and never looks here.
    parsed: std.StringHashMapUnmanaged(*ParsedStmt) = .empty,
    /// The session's namespace, resolved once from the startup packet's
    /// database (Postgres resolves at connect too). 0 = the name didn't
    /// exist at connect; every query then fails with 3D000. Re-validated
    /// per DDL — the database can be dropped mid-session, and a dead id
    /// must not reach the manifest (nextTableId's getOrPut would resurrect
    /// its counter entry).
    namespace: u64 = 0,
    /// The session's Database, resolved once: Server.database never
    /// frees one, so the pointer is good for the session and every
    /// statement skips the `databases_mu` lookup (three per statement
    /// otherwise — and each was a contended hand-off under load).
    db: ?*Database = null,
    /// The live transaction, `t.implicit` telling which kind: an open
    /// interactive block (BEGIN..COMMIT/ROLLBACK, implicit=false, lives
    /// across statements) or the auto-commit txn of the DML statement
    /// currently executing (implicit=true, ended by runDml). Never both
    /// — a block's statements run in the block's txn. Owned HERE, not in
    /// the statement handler, so a statement restart (read_ts bump) can
    /// re-run the handler without destroying the txn — its locks and
    /// bumped read_ts must survive the retry. Heap — it outlives
    /// individual statements.
    txn: ?*_txn.Txn = null,
    /// The executor's row batch — one per connection, created on the
    /// first DML and reused by every statement after (a portal will
    /// keep it across Execute messages). See `batch`.
    exec_batch: ?_exec.Batch = null,

    /// Session teardown: roll back any open transaction. Wired as a
    /// defer in handleConn, so it runs on every exit from serve.
    pub fn deinit(self: *Handler) void {
        if (self.txn) |t| {
            self.destroyTxn(t);
            self.txn = null;
        }
        if (self.exec_batch) |*b| b.deinit();
        self.clearParsed();
        self.parsed.deinit(self.srv.engine.allocator);
    }

    fn clearParsed(self: *Handler) void {
        var it = self.parsed.valueIterator();
        while (it.next()) |entry| {
            entry.*.arena.deinit();
            self.srv.engine.allocator.destroy(entry.*);
        }
        self.parsed.clearRetainingCapacity();
    }

    /// The parsed form of `sql`, from the session's cache or parsed
    /// into it. null = a syntax error (not cached: the client is told
    /// and does not re-send it). A full cache is emptied first.
    fn cachedStatement(self: *Handler, sql: String) !?_parse.Stmt {
        if (self.parsed.get(sql)) |entry| return entry.stmt;
        if (self.parsed.count() >= max_parsed) self.clearParsed();

        const gpa = self.srv.engine.allocator;
        const entry = try gpa.create(ParsedStmt);
        errdefer gpa.destroy(entry);

        entry.arena = std.heap.ArenaAllocator.init(gpa);
        errdefer entry.arena.deinit();

        const arloc = entry.arena.allocator();
        entry.stmt = _parse.statement(arloc, sql) catch |err| switch (err) {
            error.OutOfMemory => return err,
            error.SyntaxError => return null,
        };
        try self.parsed.put(gpa, try arloc.dupe(u8, sql), entry);
        return entry.stmt;
    }

    /// The connection's event loop, from the startup handshake to
    /// Terminate or a dead socket. A simple query gets its
    /// ReadyForQuery here; an extended batch gets it from the state
    /// machine at Sync (see pgwire.driver).
    fn serve(self: *Handler, stream: *fiber.net.Stream) !void {
        const session = try pgwire.Session.open(
            self.srv.engine.allocator,
            stream,
            .{},
        );
        defer session.close();
        while (!session.closed()) {
            const state = &session.state;
            // A draining server takes no further message: the session
            // ends here with the FATAL, at a message boundary, so the
            // reply to the last statement went out whole. Ctx.attach
            // refuses a statement that slipped past this check, and
            // once the drain's grace is over maybeYield stops one still
            // running; both come back as error.Shutdown and end the
            // session the same way.
            if (self.srv.engine.node.shutdown.isDraining())
                return replyShutdown(state);

            const ev = try session.nextEvent();
            self.dispatch(state, ev) catch |err| switch (err) {
                error.Shutdown => return replyShutdown(state),
                else => return err,
            };
            if (pgwire.Session.flushAfter(ev)) try session.flush();
        }
    }

    fn dispatch(
        self: *Handler,
        state: *pgwire.ConnState,
        ev: pgwire.Event,
    ) !void {
        // The three events that are statements run under the fiber's
        // context: everything below records into it through
        // fiber.ctx(), finish keeps the trace if it was slow, and start
        // counts it in flight for the drain. Describe is one because
        // the first Describe of a statement is where it gets parsed.
        // The other events are bookkeeping between statements.
        switch (ev) {
            .startup_complete => |params| try self.onStartup(state, params),
            .query => |sql| {
                if (!try self.startCtx(state, sql, .simple))
                    return state.endCommand();
                defer fiber.ctx().finish();

                try self.onQuery(state, sql);
                try state.endCommand();
            },
            .execute => |ctx| {
                if (!try self.startCtx(state, ctx.sql, .extended)) return;
                defer fiber.ctx().finish();

                try self.onExecute(state, ctx);
            },
            .describe => |ctx| {
                if (!try self.startCtx(state, ctx.sql, .extended)) return;
                defer fiber.ctx().finish();

                try self.onDescribe(state, ctx);
            },
            .sync, .flush, .terminate => {},
        }
    }

    /// The session's row batch, made on first use.
    pub fn batch(self: *Handler) !*_exec.Batch {
        if (self.exec_batch == null) {
            self.exec_batch = try _exec.Batch.init(self.srv.engine.allocator);
        }
        return &self.exec_batch.?;
    }

    pub fn destroyTxn(self: *Handler, t: *_txn.Txn) void {
        t.deinit();
        self.srv.engine.allocator.destroy(t);
    }

    /// Starts the statement's context, or answers the tenant's memory
    /// refusal, out of memory, and skips the statement: false. In the
    /// extended protocol the rest of the batch is skipped to the Sync.
    fn startCtx(
        self: *Handler,
        state: *pgwire.ConnState,
        sql: String,
        mode: Mode,
    ) !bool {
        fiber.ctx().start(sql, self.txn != null) catch |err| switch (err) {
            error.MemShare => {
                try replyMemShare(state);
                if (mode == .extended) state.ext_failed = true;
                return false;
            },
            else => return err,
        };
        return true;
    }

    pub fn onStartup(self: *Handler, state: *pgwire.ConnState, params: pgwire.proto.StartupParams) !void {
        _ = state;
        const requested = params.database orelse "";
        self.namespace = self.srv.engine.forest.manifest.lookupNamespace(
            if (requested.len > 0) requested else default_database,
        ) orelse 0;
        // The database is the tenant: every statement on this fiber is
        // charged to it from here. A name that resolved to nothing
        // stays on the system's, and is refused per statement anyway.
        if (self.namespace != 0) {
            fiber.ctx().tenant = try fiber.meter.tenants.get(
                self.namespace,
                utils.coarseMs(),
            );
        }
    }

    /// The session's namespace if it still exists, else a SQL error and null.
    pub fn sessionNamespace(self: *Handler, state: *pgwire.ConnState) !?u64 {
        if (self.namespace != 0 and self.srv.engine.forest.hasNamespace(self.namespace))
            return self.namespace;
        try replyErrorCode(state, pgwire.sqlstate.invalid_catalog_name, "database does not exist");
        return null;
    }

    /// Catch-up barrier for DDL: this node must be a CAUGHT-UP leader
    /// (see Engine.catchUp), else it replies and returns false — DDL's
    /// leader-side name-uniqueness checks are only sound on one. Callers
    /// hold ddl_mu across it; the barrier itself takes no lock. (DML gets
    /// its freshness best-effort in newTxn, not here.)
    pub fn ddlReady(self: *Handler, state: *pgwire.ConnState) !bool {
        self.srv.engine.catchUp() catch |err| {
            switch (err) {
                error.NotLeader => try replyError(state, "not the leader"),
                else => {
                    log.err("ddl catch-up barrier: {s}", .{@errorName(err)});
                    try replyError(state, "raft catch-up failed");
                },
            }
            return false;
        };
        return true;
    }

    /// A statement (or Describe) ended: any error inside an open block
    /// aborts the txn — locks release NOW (a deadlock victim must not
    /// keep its cycle alive until the client's ROLLBACK), and the
    /// session enters E state until COMMIT/ROLLBACK ends the block.
    /// Wired as a defer by every entry point.
    fn failBlockOnError(self: *Handler, state: *pgwire.ConnState) void {
        const t = self.txn orelse return;
        // Blocks only: an implicit txn can linger here solely when a
        // reply failed mid-statement (dying connection) — session
        // teardown destroys it.
        if (state.error_sent and !t.implicit and !t.failed) {
            t.failed = true;
            t.releaseLocks();
            state.tx_status = .failed_tx;
        }
    }

    /// Whether `sql` is nothing but whitespace and semicolons — an
    /// empty query, answered with EmptyQueryResponse on both paths.
    fn isEmptyQuery(sql: String) bool {
        return std.mem.trim(u8, sql, " \t\r\n;").len == 0;
    }

    fn replySyntaxError(state: *pgwire.ConnState) !void {
        try state.sendError(.{
            .code = pgwire.sqlstate.syntax_error,
            .message = "syntax error",
        });
    }

    /// The simple query protocol: one statement per message, parsed
    /// into a per-query arena and run. The driver sends ReadyForQuery
    /// after.
    pub fn onQuery(self: *Handler, state: *pgwire.ConnState, sql_text: String) !void {
        self.mode = .simple;
        self.params = .{};
        state.error_sent = false;
        defer self.failBlockOnError(state);

        if (isEmptyQuery(sql_text)) return state.sendEmptyQueryResponse();

        // Parse into an arena: the Stmt borrows its strings from the
        // libpg_query tree, and the arena reclaims all of it — tree and
        // borrowed statement alike — when this scope ends.
        var arena = fiber.ctx().newArena();
        defer arena.deinit();
        const stmt = _parse.statement(arena.allocator(), sql_text) catch
            return replySyntaxError(state);
        fiber.ctx().record("parsed", .{});
        try self.runStatement(state, stmt);
    }

    /// The extended protocol's Execute: the portal's statement, parsed
    /// once per session (cachedStatement), run with its Bind values as
    /// `params`. Sync sends ReadyForQuery; an error here drops every
    /// message before it (ext_failed). What Execute cannot do yet is
    /// refused up front: a row limit (PortalSuspended and resuming a
    /// portal) and binary result columns.
    pub fn onExecute(
        self: *Handler,
        state: *pgwire.ConnState,
        ctx: pgwire.ExecuteCtx,
    ) !void {
        self.mode = .extended;
        self.params = .{ .values = ctx.params, .formats = ctx.param_formats };
        defer self.params = .{};

        state.error_sent = false;
        defer self.failBlockOnError(state);
        defer if (state.error_sent) {
            state.ext_failed = true;
        };

        const unsupported = pgwire.sqlstate.feature_not_supported;
        if (ctx.max_rows != 0) {
            return replyErrorCode(
                state,
                unsupported,
                "portal row limits are not supported",
            );
        }
        for (ctx.result_formats) |format| if (format != 0) {
            return replyErrorCode(
                state,
                unsupported,
                "binary result format is not supported",
            );
        };
        if (isEmptyQuery(ctx.sql)) return state.sendEmptyQueryResponse();

        const stmt = (try self.cachedStatement(ctx.sql)) orelse
            return replySyntaxError(state);
        fiber.ctx().record("bound {d} params", .{ctx.params.len});
        const needed = stmt.paramCount();
        if (needed != ctx.params.len) {
            var arena = fiber.ctx().newArena();
            defer arena.deinit();
            const msg = try std.fmt.allocPrint(
                arena.allocator(),
                "bind message supplies {d} parameters, " ++
                    "but prepared statement requires {d}",
                .{ ctx.params.len, needed },
            );
            return replyErrorCode(
                state,
                pgwire.sqlstate.protocol_violation,
                msg,
            );
        }
        try self.runStatement(state, stmt);
    }

    /// The extended protocol's Describe: a SELECT's RowDescription, NoData
    /// for everything else. The state machine already sent
    /// ParameterDescription for the statement kind (echoing the client's
    /// OIDs; inferring them is future work). Like Execute, an error drops
    /// the batch and fails an open block.
    pub fn onDescribe(
        self: *Handler,
        state: *pgwire.ConnState,
        ctx: pgwire.DescribeCtx,
    ) !void {
        state.error_sent = false;
        defer self.failBlockOnError(state);
        defer if (state.error_sent) {
            state.ext_failed = true;
        };

        if (isEmptyQuery(ctx.sql)) return state.sendNoData();
        const stmt = (try self.cachedStatement(ctx.sql)) orelse
            return replySyntaxError(state);
        if (self.txn) |t| if (t.failed) return replyFailedBlock(state);
        switch (stmt) {
            .select => |sel| try _dml.describeSelect(self, state, sel),
            .select_row => |sr| try _dml.describeSelectRow(state, sr),
            else => try state.sendNoData(),
        }
    }

    /// Run one parsed statement: the block-state checks, then the
    /// dispatch. Shared by both protocols; `mode` and `params` are
    /// already set for it.
    fn runStatement(
        self: *Handler,
        state: *pgwire.ConnState,
        stmt: _parse.Stmt,
    ) !void {
        if (self.txn) |t| {
            // A failed block accepts only COMMIT/ROLLBACK (both roll
            // back) until the client ends it.
            if (t.failed and stmt != .commit_txn and stmt != .rollback_txn)
                return replyFailedBlock(state);
            // Leadership moved since BEGIN: nothing this txn read or
            // buffered can be trusted — abort eagerly (the defer above
            // flips the failed state).
            if (!t.failed and self.srv.engine.node.current_term != t.term)
                return replyErrorCode(state, pgwire.sqlstate.serialization_failure, "leader changed, transaction aborted");
            // DDL mixes manifest state with the data plane; keep it out
            // of transaction blocks.
            switch (stmt) {
                .create_database,
                .drop_database,
                .create_table,
                .drop_table,
                .create_index,
                .drop_index,
                => return replyErrorCode(state, pgwire.sqlstate.active_sql_transaction, "DDL inside a transaction block is not supported"),
                else => {},
            }
        }

        switch (stmt) {
            .create_database => |cd| try _ddl.createDatabase(self, state, cd),
            .drop_database => |name| try _ddl.dropDatabase(self, state, name),
            .create_table => |ct| try _ddl.createTable(self, state, ct),
            .create_table_as => |cta| try _ddl.createTableAs(self, state, cta),
            .drop_table => |dt| try _ddl.dropTable(self, state, dt),
            .create_index => |ci| try _index_ddl.createIndex(self, state, ci),
            .drop_index => |di| try _index_ddl.dropIndex(self, state, di),
            .insert, .select, .delete, .update => try self.runDml(state, stmt),
            .select_row => |sr| try _dml.selectRow(self, state, sr),
            .begin_txn => try self.beginTxn(state),
            .commit_txn => try self.endTxn(state, true),
            .rollback_txn => try self.endTxn(state, false),
            .set_var => try state.sendCommandComplete("SET"),
            .reset_var => try state.sendCommandComplete("RESET"),
            .invalid => |msg| try replyErrorCode(state, pgwire.sqlstate.feature_not_supported, msg),
            .unsupported => try replyError(state, "unsupported statement"),
        }
    }

    /// Run one DML statement, transparently re-executing it when a row
    /// lock's read_ts bump invalidates its in-flight snapshot
    /// (error.Restart): the txn keeps its locks and bumped read_ts, its
    /// statement-scoped read tracking rewinds, and the handler re-runs
    /// against the new snapshot. The client never sees a restart. The
    /// implicit txn (if this statement created one) ends here, after
    /// the handler committed it — which keeps its locks held until the
    /// commit has applied.
    fn runDml(self: *Handler, state: *pgwire.ConnState, stmt: _parse.Stmt) !void {
        // A write can only commit on the leader — reject it up front
        // rather than scanning, locking, and failing at propose. Reads
        // run on any node (newTxn sets read_ts best-effort), but FOR
        // UPDATE takes row locks and bumps against the oracle — leader-
        // local state — so it is gated like a write. Re-routing to the
        // leader is future work.
        switch (stmt) {
            .insert, .delete, .update => if (!self.srv.engine.node.isLeader())
                return replyError(state, "not the leader"),
            .select => |sel| if (sel.for_update and
                !self.srv.engine.node.isLeader())
                return replyError(state, "not the leader"),
            else => {},
        }
        // Each restart re-raises the floor to a fresh, strictly higher
        // commit — progress, not a loop — but a pathological rate of
        // conflicting commits could starve us; cap and surface it.
        const max_restarts = 8;
        if (self.txn) |t| try t.beginStatement();
        var attempts: u32 = 0;
        while (true) {
            const result = switch (stmt) {
                .insert => |ins| _dml.insert(self, state, ins),
                .select => |sel| _dml.select(self, state, sel),
                .delete => |del| _dml.delete(self, state, del),
                .update => |up| _dml.update(self, state, up),
                else => unreachable,
            };
            result catch |err| {
                if (err == error.ReadBelowMinRead) {
                    self.endImplicit();
                    return replySnapshotTooOld(state);
                }
                if (err != error.Restart) {
                    self.endImplicit();
                    return err;
                }
                // Restart statement
                attempts += 1;
                const t = self.txn orelse
                    unreachable; // Restart comes from a lock, locks from a txn
                if (attempts <= max_restarts) {
                    t.restartStatement();
                    continue;
                }
                try replyErrorCode(
                    state,
                    pgwire.sqlstate.serialization_failure,
                    "too many statement restarts, retry the transaction",
                );
            };
            break;
        }
        self.endImplicit();
    }

    /// The Database for `ns`: the cached one when it is the session's,
    /// else resolved (and cached) through Server.database.
    pub fn sessionDb(self: *Handler, ns: u64) !*Database {
        if (self.db) |d| {
            if (d.ns == ns) return d;
        }
        const d = try self.srv.database(ns);
        self.db = d;
        return d;
    }

    /// The cached Table for `table_key`, through the session's
    /// Database — the per-statement schema lookup.
    pub fn openTable(
        self: *Handler,
        table_key: zdb.kv.TableKey,
    ) !*_table.Table {
        return self.srv.openTableIn(
            try self.sessionDb(table_key.namespace),
            table_key.table_id,
        );
    }

    /// Reserve the next egid for a table (see Server.allocEgid).
    pub fn allocEgid(self: *Handler, table_key: zdb.kv.TableKey) !u64 {
        return self.srv.allocEgidIn(
            try self.sessionDb(table_key.namespace),
            table_key.table_id,
        );
    }

    pub fn newTxn(self: *Handler, ns: u64) !*_txn.Txn {
        const eng = self.srv.engine;
        // Best-effort catch-up: a leader waits out its committed tail
        // (once per term) so read_ts covers it; a non-leader keeps what
        // it has rather than erroring here.
        eng.catchUp() catch {};
        // Database first, read_ts second: a commit applying between
        // the two lands in this database's now-existing oracle, so a
        // conflict newer than our snapshot can never go unrecorded.
        const db = try self.sessionDb(ns);
        const t = try eng.allocator.create(_txn.Txn);
        errdefer eng.allocator.destroy(t);
        t.* = _txn.Txn.init(
            eng.allocator,
            self.srv.next_txn_id.fetchAdd(1, .monotonic),
            eng.node.appliedTs(),
            eng.node.current_term,
            db,
            eng.forest,
        );
        t.start_us = utils.timestampNow(.realtime, .micros);
        return t;
    }

    /// The transaction a DML statement runs in: the open interactive
    /// block, or a fresh implicit txn (implicit=true) the statement itself
    /// commits.
    pub fn statementTxn(self: *Handler, ns: u64) !*_txn.Txn {
        // The open block's txn — or, on a restart re-entry, the
        // statement's own implicit txn (locks + bumped read_ts intact).
        if (self.txn) |t| return t;

        const t = try self.newTxn(ns);
        t.implicit = true;
        self.txn = t;
        return t;
    }

    /// The txn a DML statement runs in, holding its shared table
    /// lock (the global lock order is table → row). Null means the
    /// statement is over: a lock error was already reported, or the
    /// database was dropped. DROP TABLE and DROP DATABASE queue
    /// behind the table lock; writer preference then parks new DML
    /// behind the pending drop. The `dropped` check comes AFTER the
    /// take: see Database.dropped for why that order is what makes
    /// the drop's pass over the tables complete.
    pub fn dmlTxn(
        self: *Handler,
        state: *pgwire.ConnState,
        table_key: zdb.kv.TableKey,
    ) !?*_txn.Txn {
        const tx = try self.statementTxn(table_key.namespace);
        tx.lockTable(table_key.table_id, .shared) catch |err| {
            try replyLockErr(state, err);
            return null;
        };
        const db = try self.sessionDb(table_key.namespace);
        if (db.dropped.load(.acquire)) {
            try replyErrorCode(
                state,
                pgwire.sqlstate.invalid_catalog_name,
                "database does not exist",
            );
            return null;
        }
        return tx;
    }

    /// End the statement's implicit txn, if that is what's live. The
    /// `implicit` guard is load-bearing: runDml calls this after every
    /// DML statement, including ones inside an open block — whose txn
    /// must survive until COMMIT/ROLLBACK.
    pub fn endImplicit(self: *Handler) void {
        if (self.txn) |t| {
            if (!t.implicit) return;
            self.destroyTxn(t);
            self.txn = null;
        }
    }

    fn beginTxn(self: *Handler, state: *pgwire.ConnState) !void {
        if (self.txn != null) {
            // Postgres warns and no-ops.
            log.debug("BEGIN inside a transaction block — no-op", .{});
            return state.sendCommandComplete("BEGIN");
        }
        const ns = try self.sessionNamespace(state) orelse return;
        const t = try self.newTxn(ns);
        self.txn = t;
        state.tx_status = .in_tx;
        try state.sendCommandComplete("BEGIN");
    }

    /// COMMIT (commit=true) or ROLLBACK ends the block either way:
    /// COMMIT of a failed txn rolls back and says so (tag ROLLBACK,
    /// Postgres behavior); outside a block both warn and no-op.
    fn endTxn(self: *Handler, state: *pgwire.ConnState, commit: bool) !void {
        const t = self.txn orelse {
            log.debug("{s} outside a transaction block — no-op", .{
                if (commit) "COMMIT" else "ROLLBACK",
            });
            return state.sendCommandComplete(if (commit) "COMMIT" else "ROLLBACK");
        };
        defer {
            self.destroyTxn(t);
            self.txn = null;
            state.tx_status = .idle;
        }
        if (commit and !t.failed) {
            var arena = fiber.ctx().newArena();
            defer arena.deinit();
            if (!try self.commitOrReply(state, arena.allocator(), t, "commit", "txn")) return;
            return state.sendCommandComplete("COMMIT");
        }
        try state.sendCommandComplete("ROLLBACK");
    }

    /// Commit an implicit txn: propose the pending write set as ONE
    /// .data entry and wait for local apply, so the statement's effects
    /// are readable the moment the client sees CommandComplete. Empty
    /// write set → nothing to propose. `error.LeaderChanged` if the term
    /// moved since begin: another leader may have committed writes our
    /// locked reads never saw, so the write set must not be proposed.
    pub fn commitTxn(self: *Handler, a: std.mem.Allocator, tx: *_txn.Txn) !void {
        const kvs = try tx.wireKvs(a);
        if (kvs.len == 0) return;
        fiber.ctx().record("{d} kvs on the wire", .{kvs.len});
        const node = self.srv.engine.node;
        if (!node.isLeader() or node.current_term != tx.term)
            return error.LeaderChanged;
        try self.srv.engine.proposeWrites(tx.ns, kvs, .apply);
    }

    /// Commit + shared reply mapping for DML: leader change → 40001
    /// (retry); anything else logs and reports a generic failure.
    /// Returns true when committed — the caller sends CommandComplete.
    pub fn commitOrReply(
        self: *Handler,
        state: *pgwire.ConnState,
        a: std.mem.Allocator,
        tx: *_txn.Txn,
        comptime verb: String,
        table_name: String,
    ) !bool {
        self.commitTxn(a, tx) catch |err| switch (err) {
            error.LeaderChanged => {
                try replyErrorCode(state, pgwire.sqlstate.serialization_failure, "leader changed, transaction aborted");
                return false;
            },
            else => {
                log.err(
                    verb ++ " '{s}' (txn={d}): propose: {s}",
                    .{ table_name, tx.id, @errorName(err) },
                );
                try replyError(state, verb ++ " failed");
                return false;
            },
        };
        return true;
    }
};

/// 25P02: the block is aborted and only COMMIT/ROLLBACK is taken.
fn replyFailedBlock(state: *pgwire.ConnState) !void {
    return replyErrorCode(
        state,
        pgwire.sqlstate.in_failed_sql_transaction,
        "current transaction is aborted, commands ignored until end of " ++
            "transaction block",
    );
}

/// Map a lock failure onto the wire. Deadlock: this statement's wait
/// would have closed a cycle — it is the victim; 40P01, the client
/// retries. Upgrade: SQL never upgrades shared→exclusive by
/// construction (DDL is rejected inside txn blocks), so this is an
/// internal bug surfaced loudly rather than a hang.
pub fn replyLockErr(state: *pgwire.ConnState, err: anyerror) !void {
    return switch (err) {
        error.Deadlock => replyErrorCode(
            state,
            pgwire.sqlstate.deadlock_detected,
            "deadlock detected — retry the transaction",
        ),
        error.Conflict => replyErrorCode(
            state,
            pgwire.sqlstate.serialization_failure,
            "could not serialize access due to concurrent update",
        ),
        error.Upgrade => {
            log.err("shared->exclusive lock upgrade attempted", .{});
            return replyError(state, "internal error: lock upgrade");
        },
        // error.Restart propagates here: the statement driver retries.
        else => err,
    };
}

/// One resolved SET assignment: the cell's decode-order index + its typed
pub fn replyError(state: *pgwire.ConnState, msg: String) !void {
    try state.sendError(.{
        .code = pgwire.sqlstate.internal_error,
        .message = msg,
    });
}

pub fn replyErrorCode(state: *pgwire.ConnState, code: [5]u8, msg: String) !void {
    try state.sendError(.{ .code = code, .message = msg });
}

/// The tenant waited its bound for memory and is still over its share
/// (`Tenant.payForMem`): the statement did not start.
fn replyMemShare(state: *pgwire.ConnState) !void {
    return replyErrorCode(
        state,
        pgwire.sqlstate.out_of_memory,
        "out of memory: the database is over its memory share",
    );
}

/// The statement holds more than the per-statement cap
/// (`--memory query_bytes`): a yield point stopped it.
pub fn replyMemLimit(state: *pgwire.ConnState) !void {
    return replyErrorCode(
        state,
        pgwire.sqlstate.out_of_memory,
        "out of memory: the statement is over its memory limit",
    );
}

/// A read was refused as below the read floor
/// (`error.ReadBelowMinRead`): the txn was open longer than the forest
/// holds the floor for, or on a node whose floor is another node's to
/// set. The versions it would read may be gone, so it cannot go on; a
/// new txn reads at a current timestamp.
pub fn replySnapshotTooOld(state: *pgwire.ConnState) !void {
    return replyErrorCode(
        state,
        pgwire.sqlstate.snapshot_too_old,
        "snapshot too old: the transaction's snapshot is below the " ++
            "read floor, retry it",
    );
}
