// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! S3-compatible object store client — the `s3` Store backend. A
//! minimal HTTP/1.1 client over fiber.net with SigV4-signed
//! PUT/GET/HEAD/DELETE, path-style URLs (`/bucket/prefix/key`), TLS, and
//! a keep-alive connection pool. HTTP/1.1 carries one response at a
//! time per connection and S3 offers neither multiplexing nor
//! multi-range GETs, so concurrency is connections: `max_inflight`
//! slots, each a fiber holding one connection, and `batchRead` fans a
//! batch out over them. The endpoint is a URL —
//! `http://host[:port]` or `https://host[:port]` — the scheme decides
//! TLS, the host may be a DNS name or IP literal (resolved ONCE at init;
//! per-dial re-resolution for endpoint DNS rotation is a recorded
//! follow-up). Deliberate v1 limits: Content-Length bodies only
//! (chunked → error), single PUT ≤ 5 GiB (no multipart).
//!
//! Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY at
//! init, plus AWS_SESSION_TOKEN when the keys are temporary (SSO, an
//! assumed role). Local testing: SeaweedFS — `weed server -s3`.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const _sigv4 = @import("sigv4.zig");
const _lookahead = @import("lookahead.zig");
const _vfs = @import("vfs.zig");
const _chunkcache = @import("chunkcache.zig");
const Chunk = _chunkcache.Chunk;

const log = std.log.scoped(.s3);
const page_size = _vfs.page_size;

const max_put_bytes: u64 = 5 << 30; // single-PUT ceiling (S3 hard limit)
const attempts_max = 3; // connect/5xx retries per request
const idle_ttl_ms = 30_000; // drop idle conns older than this at checkout

pub const S3Config = struct {
    /// Endpoint URL: `http://host[:port]` or `https://host[:port]`. The
    /// scheme decides TLS (and the default port); the host may be a DNS
    /// name ("s3.us-east-005.backblazeb2.com"), an IPv4 literal
    /// ("127.0.0.1"), or a bracketed IPv6 literal ("[::1]").
    endpoint: String,
    bucket: String,
    /// Optional key prefix inside the bucket. Unreserved URI characters
    /// only — keys are used verbatim in the request path (no
    /// percent-encoding pass).
    prefix: String = "",
    /// SigV4 signing region. Empty = read off the endpoint host
    /// (regionFromHost); set it when the host carries none (R2, a
    /// VPC endpoint) and the service rejects us-east-1.
    region: String = "",
    /// Requests in flight at once, which is also the number of
    /// keep-alive connections held: one response at a time per
    /// connection.
    max_inflight: u32 = 32,
    cache: _chunkcache.ChunkCache.Config = .{},
};

pub const S3Store = struct {
    a: std.mem.Allocator,
    arena: std.heap.ArenaAllocator,
    /// Original endpoint URL, for logs.
    endpoint: String,
    bucket: String,
    /// SigV4 signing region: S3Config.region, else off the host.
    region: String,
    prefix: String,
    /// Bare host name — TLS SNI + certificate verification.
    host: String,
    /// Host header value (SigV4 signs it): the bare host, `:port`
    /// appended when it isn't the scheme default — servers on custom
    /// ports expect the port in Host.
    host_header: String,
    /// Resolved once at init; every dial reuses it. Per-dial
    /// re-resolution (endpoint DNS rotation) is a recorded follow-up.
    dial_addr: fiber.net.IpAddress,
    use_tls: bool,
    // Environ-backed slices: process lifetime, never freed.
    access_key: String,
    secret_key: String,
    /// AWS_SESSION_TOKEN; null for long-lived keys.
    session_token: ?String,
    // System trust roots, loaded once at init when use_tls.
    ca_bundle: std.crypto.Certificate.Bundle = .empty,
    ca_lock: std.Io.RwLock = .init,
    /// Connection slots, `max_inflight` of them, shared by every caller
    /// (prefetch, apply, uploads, Object reads). A request holds one
    /// slot from `receive` to `send`; when all are out the next caller
    /// parks FIFO, so the channel is both the pool and the in-flight
    /// cap. A slot's conn is reusable only after its response was
    /// fully drained — parseResponse guarantees that.
    conns: fiber.Channel(Slot),
    /// Local-disk chunk cache in front of every read (chunkcache.zig).
    cache: _chunkcache.ChunkCache,

    const Conn = fiber.net.Conn;

    /// One connection slot. `conn` is null before the first dial and
    /// after a failure closed it; `since_ms` is when it was last
    /// returned, for the idle TTL.
    const Slot = struct {
        conn: ?*Conn = null,
        since_ms: i64 = 0,
    };

    pub fn init(a: std.mem.Allocator, cfg: S3Config) !S3Store {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const arloc = arena.allocator();

        const ctx = utils.context.g();
        const access = ctx.env("AWS_ACCESS_KEY_ID") orelse "";
        const secret = ctx.env("AWS_SECRET_ACCESS_KEY") orelse "";
        if (access.len == 0 or secret.len == 0) {
            log.err("s3 store requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in the environment", .{});
            return error.MissingCredentials;
        }
        const token = ctx.env("AWS_SESSION_TOKEN") orelse "";
        const ep = fiber.net.parseEndpoint(cfg.endpoint) catch {
            log.err("s3 endpoint must be http(s)://host[:port], got '{s}'", .{cfg.endpoint});
            return error.BadEndpoint;
        };
        // Resolve now — fail fast on a bad endpoint instead of erroring
        // per request.
        const dial_addr = fiber.net.resolveHost(ep.host, ep.port) catch |err| {
            log.err("resolving '{s}' failed: {s}", .{ ep.host, @errorName(err) });
            return error.BadEndpoint;
        };
        const default_port = ep.port == @as(u16, if (ep.tls) 443 else 80);
        var ret: S3Store = .{
            .a = a,
            // Placeholder — the arena is moved in AFTER the literal, once
            // every arloc allocation above has mutated its state; copying
            // it earlier would snapshot a state missing those buffers.
            .arena = undefined,
            .endpoint = try arloc.dupe(u8, cfg.endpoint),
            .bucket = try arloc.dupe(u8, cfg.bucket),
            .region = try arloc.dupe(u8, if (cfg.region.len > 0)
                cfg.region
            else
                regionFromHost(ep.host)),
            .prefix = try arloc.dupe(u8, cfg.prefix),
            .host = try arloc.dupe(u8, ep.host),
            .host_header = if (default_port)
                try arloc.dupe(u8, ep.host)
            else
                try std.fmt.allocPrint(arloc, "{s}:{d}", .{ ep.host, ep.port }),
            .dial_addr = dial_addr,
            .use_tls = ep.tls,
            .access_key = access,
            .secret_key = secret,
            .session_token = if (token.len > 0) token else null,
            .conns = try fiber.Channel(Slot).initAlloc(a, cfg.max_inflight),
            .cache = try _chunkcache.ChunkCache.init(a, cfg.cache),
        };
        ret.arena = arena;
        // Every slot starts in the channel, empty; capacity equals the
        // slot count, so a send can never block.
        for (0..cfg.max_inflight) |_| ret.conns.trySend(.{}) catch unreachable;
        if (ep.tls) {
            const io = ctx.io;
            ret.ca_bundle.rescan(a, io, std.Io.Timestamp.now(io, .real)) catch |err| {
                log.err("loading system CA bundle failed: {s}", .{@errorName(err)});
                return error.NoTrustRoots;
            };
        }
        return ret;
    }

    pub fn deinit(self: *S3Store) void {
        self.conns.close(.graceful);
        while (self.conns.tryReceive()) |slot| {
            if (slot.conn) |c| c.deinit(self.a);
        } else |_| {}
        self.conns.deinit();
        self.cache.deinit();
        self.arena.deinit();
        if (self.use_tls) self.ca_bundle.deinit(self.a);
    }

    /// Dial + (optionally) TLS-handshake a new pooled connection.
    fn newConn(self: *S3Store) !*Conn {
        return Conn.connect(self.a, self.dial_addr, if (self.use_tls) .{
            .host = self.host,
            .ca_bundle = &self.ca_bundle,
            .ca_lock = &self.ca_lock,
        } else null);
    }

    /// Take a slot, parking while all are in flight. A conn idle past
    /// the TTL is closed (servers drop quiet keep-alives; a dead one
    /// would waste a retry) and the next slot tried — the channel is
    /// FIFO, so later slots are fresher — until one has a live conn or
    /// none; the caller dials into an empty slot. Emptied slots go back
    /// to the tail, so the loop ends within one pass.
    fn acquire(self: *S3Store) !Slot {
        var slot = self.conns.receive() catch return error.Shutdown;
        const now = utils.coarseMs();
        while (slot.conn) |conn| {
            if (now - slot.since_ms <= idle_ttl_ms) break;
            conn.deinit(self.a);
            slot.conn = null;
            const next = self.conns.tryReceive() catch break;
            // Only a closed channel (deinit) refuses; the slot is empty.
            self.conns.send(slot) catch {};
            slot = next;
        }
        return slot;
    }

    /// Return a slot. Capacity equals the slot count, so this never
    /// blocks; a closed channel (deinit) just drops the conn.
    fn release(self: *S3Store, slot_in: Slot) void {
        var slot = slot_in;
        slot.since_ms = utils.coarseMs();
        self.conns.send(slot) catch {
            if (slot.conn) |c| c.deinit(self.a);
        };
    }

    pub fn put(self: *S3Store, name: String, data: String) !void {
        if (data.len > max_put_bytes) return error.ObjectTooLarge;
        var resp = try self.objectRequest(name, .{
            .method = .PUT,
            .body = data,
        });
        defer resp.deinit(self.a);
        if (resp.status != 200) return self.statusErr(.PUT, name, &resp);
    }

    pub fn exists(self: *S3Store, name: String) !bool {
        var resp = try self.objectRequest(name, .{ .method = .HEAD });
        defer resp.deinit(self.a);

        return switch (resp.status) {
            200 => true,
            404 => false,
            else => self.statusErr(.HEAD, name, &resp),
        };
    }

    pub fn delete(self: *S3Store, name: String) !void {
        var resp = try self.objectRequest(name, .{ .method = .DELETE });
        defer resp.deinit(self.a);
        // S3 DELETE is idempotent: 204 whether or not the key existed.
        switch (resp.status) {
            200, 204, 404 => {},
            else => return self.statusErr(.DELETE, name, &resp),
        }
        self.cache.forget(name);
    }

    /// HEAD for size + existence; ranged GETs happen per read().
    pub fn head(self: *S3Store, name: String) !u64 {
        return (try self.stat(name)).size;
    }

    /// HEAD for size and modification time. error.FileNotFound if
    /// absent.
    pub fn stat(self: *S3Store, name: String) !_vfs.FileStat {
        var resp = try self.objectRequest(name, .{ .method = .HEAD });
        defer resp.deinit(self.a);

        return switch (resp.status) {
            200 => .{
                .size = resp.content_length,
                .mtime_s = resp.last_modified_s,
            },
            404 => error.FileNotFound,
            else => self.statusErr(.HEAD, name, &resp),
        };
    }

    /// One ranged GET: [offset, offset+buf.len) of `name` straight into
    /// `buf`, written once, off the socket. The wire primitive every
    /// read builds on; `read` is the one that goes through the cache.
    fn getNoCache(self: *S3Store, name: String, offset: u64, buf: []u8) !void {
        if (buf.len == 0) return;
        var resp = try self.objectRequest(name, .{
            .method = .GET,
            .range = .{ offset, buf.len },
            .sink = .{ .into = buf },
        });
        defer resp.deinit(self.a);
        switch (resp.status) {
            // 206 for a partial range; some servers reply 200 when the
            // range covers the whole object.
            200, 206 => {},
            404 => return error.FileNotFound,
            else => return self.statusErr(.GET, name, &resp),
        }
    }

    /// Whether the current fiber's reads go through the chunk cache,
    /// lookup and admission both. A compaction bypasses it: it reads
    /// each input once, so caching those chunks would churn the cache
    /// for hits that never come.
    fn useCache(self: *const S3Store) bool {
        return self.cache.enabled() and fiber.ctx().cache == .use;
    }

    /// Read [offset, offset+dst.len) of `name`, an object of `size`
    /// bytes. Through the chunk cache when `cached`: each covering chunk
    /// is copied from disk on a hit, or fetched whole and stored on a
    /// miss. Otherwise one exact ranged GET.
    pub fn read(
        self: *S3Store,
        name: String,
        size: u64,
        offset: u64,
        dst: []u8,
    ) !void {
        if (dst.len == 0) return;
        if (!self.useCache()) return self.getNoCache(name, offset, dst);

        // A range across chunks goes through the batch path: its
        // chunks are fetched concurrently, not one after another.
        const c = self.cache.chunkBytes();
        if (offset / c != (offset + dst.len - 1) / c) {
            var arena = std.heap.ArenaAllocator.init(self.a);
            defer arena.deinit();
            var reqs = [_]_vfs.FileHandle.ReadRequest{
                .{ .offset = offset, .len = dst.len },
            };
            try self.batchRead(name, size, &arena, &reqs);
            @memcpy(dst, reqs[0].data);
            return;
        }

        // One chunk: loaded straight into `dst`, no arena or fan-out.
        var chunk = Chunk{
            .idx = offset / c,
            .chunk_len = chunkLen(c, size, offset / c),
            .req_lo = offset % c,
            .req_hi = offset % c + dst.len,
            .req_buf = dst,
        };
        try self.readChunk(name, c, &chunk);
    }

    /// One chunk's span of a batch: for a cached read the slot file,
    /// else the whole chunk fetched and stored; for an uncached read
    /// the span from the remote.
    fn readChunk(
        self: *S3Store,
        name: String,
        chunk_bytes: u64,
        chunk: *Chunk,
    ) anyerror!void {
        const c = chunk;
        if (!self.useCache()) {
            // Uncached: just the span [req_lo, req_hi) from the remote.
            const off = c.idx * chunk_bytes + c.req_lo;
            return self.getNoCache(name, off, c.req_buf);
        }

        if (try self.cache.read(name, c)) return;
        try self.getAndCache(name, c);
    }

    /// A cached read's miss: the chunk whole from the remote, stored,
    /// and its span [req_lo, req_hi) copied to `req_buf`.
    fn getAndCache(
        self: *S3Store,
        name: String,
        chunk: *const Chunk,
    ) !void {
        const c = self.cache.chunkBytes();
        const buf = try allocChunk(self.a, chunk.chunk_len);
        defer self.a.free(buf);

        // buf.len might be longer because it allocates page-aligned buffer.
        const whole = buf[0..chunk.chunk_len];
        try self.getNoCache(name, chunk.idx * c, whole);
        try self.cache.write(name, chunk.idx, whole);
        @memcpy(chunk.req_buf, whole[chunk.req_lo..chunk.req_hi]);
    }

    /// A chunk buffer the cache can write to its slot file as is:
    /// page-aligned, whole pages. The caller slices it to `len`.
    fn allocChunk(a: std.mem.Allocator, len: u64) ![]align(page_size) u8 {
        const full = std.mem.alignForward(usize, @intCast(len), page_size);
        return a.alignedAlloc(u8, _vfs.page_alignment, full);
    }

    /// Bytes in chunk `idx` of an object of `size` bytes: `c`, or the
    /// tail for the last chunk.
    fn chunkLen(c: u64, size: u64, idx: u64) u64 {
        return @min(c, size - idx * c);
    }

    /// Byte range [off, off+len) cut into `chunk_bytes` chunks,
    /// walked as one range per chunk it touches, in order.
    pub const Ranges = struct {
        chunk_bytes: u64,
        off: u64,
        end: u64,
        idx: u64,

        /// The part of the byte range inside one chunk.
        pub const Range = struct {
            idx: u64,
            /// Bounds within the chunk.
            lo: u64,
            hi: u64,
            /// Position within the whole byte range.
            at: u64,

            pub fn len(self: Range) u64 {
                return self.hi - self.lo;
            }
        };

        pub fn init(chunk_bytes: u64, off: u64, len: u64) Ranges {
            return .{
                .chunk_bytes = chunk_bytes,
                .off = off,
                .end = off + len,
                .idx = off / chunk_bytes,
            };
        }

        pub fn next(self: *Ranges) ?Range {
            const start = self.idx * self.chunk_bytes;
            // An empty byte range yields nothing though its chunk exists.
            if (self.off >= self.end or start >= self.end) return null;
            defer self.idx += 1;
            const lo = @max(self.off, start) - start;
            return .{
                .idx = self.idx,
                .lo = lo,
                .hi = @min(self.end, start + self.chunk_bytes) - start,
                .at = start + lo - self.off,
            };
        }

        /// The byte range assembled from loaded chunks: a slice into the
        /// chunk when it lies in one, else a copy in `arloc` stitched
        /// from each — the one case that copies.
        pub fn gather(
            self: *Ranges,
            arloc: std.mem.Allocator,
            chunks: *const ChunkMap,
        ) ![]u8 {
            const total = self.end - self.off;
            const first = self.next() orelse return &.{};

            const lead = chunks.get(first.idx).?.bytes(first.lo, first.hi);
            if (first.len() == total) return lead;

            const out = try arloc.alloc(u8, total);
            @memcpy(out[0..lead.len], lead);
            while (self.next()) |rng| {
                const src = chunks.get(rng.idx).?.bytes(rng.lo, rng.hi);
                @memcpy(out[rng.at..][0..src.len], src);
            }
            return out;
        }
    };

    /// The chunk size a batch is planned on: the cache's for a cached
    /// read, so each chunk is fetched and kept whole; for an uncached
    /// one (compaction) at least `uncached_span`, so a run of blocks is
    /// one request however small the cache's chunks are.
    fn planChunkBytes(self: *const S3Store) u64 {
        const c = self.cache.chunkBytes();
        if (self.useCache()) return c;
        return @max(c, uncached_span);
    }

    /// Past the knee of every endpoint measured so far (`--probe` says
    /// where a given one's is): a request this large costs about its
    /// transfer time, not its round trip.
    const uncached_span: u64 = 4 << 20;

    /// Fill a batch of reads of `name`, an object of `size` bytes, with
    /// every `req.data` in `arena`. The requests are mapped onto chunks
    /// (`planChunkBytes`, `planChunks`), loaded concurrently —
    /// one fiber and one connection per chunk, bounded by the slot
    /// count — then each request is sliced out of its chunk
    /// (`Ranges.gather`).
    /// A chunk comes from the cache on a hit; on a miss it is fetched
    /// whole and stored for a `cached` read, else only the span the
    /// requests want. No copy after the socket. Requests may come in any
    /// order.
    pub fn batchRead(
        self: *S3Store,
        name: String,
        size: u64,
        arena: *std.heap.ArenaAllocator,
        requests: []_vfs.FileHandle.ReadRequest,
    ) !void {
        if (requests.len == 0) return;
        const arloc = arena.allocator();
        const chunk_bytes = self.planChunkBytes();
        var chunks = try planChunks(arloc, chunk_bytes, size, requests);

        var group = try fiber.Group(anyerror!void).initCapacity(
            self.a,
            chunks.count(),
        );
        defer group.deinit();
        for (chunks.values()) |*chunk| {
            try group.spawn(.{}, readChunk, .{
                self, name, chunk_bytes, chunk,
            });
        }
        for (group.wait()) |res| try res;

        for (requests) |*req| {
            var it = Ranges.init(chunk_bytes, req.offset, req.len);
            req.data = try it.gather(arloc, &chunks);
        }
    }

    /// The chunks one batch covers, by index.
    pub const ChunkMap = std.AutoArrayHashMapUnmanaged(u64, Chunk);

    /// Map `requests` onto chunks: each chunk touched once, with
    /// the span the requests want of it. Buffers are allocated here,
    /// before any fan-out: the arena is not shared across fibers. Pure,
    /// so it is testable without a store.
    pub fn planChunks(
        arloc: std.mem.Allocator,
        chunk_bytes: u64,
        size: u64,
        requests: []const _vfs.FileHandle.ReadRequest,
    ) !ChunkMap {
        var chunks: ChunkMap = .empty;
        for (requests) |req| {
            var it = Ranges.init(chunk_bytes, req.offset, req.len);
            // If we get multiple ranges here, that means our read request is
            // crossing a chunk boundary.
            while (it.next()) |rng| {
                const res = try chunks.getOrPut(arloc, rng.idx);
                if (!res.found_existing) {
                    res.value_ptr.* = .{
                        .idx = rng.idx,
                        .chunk_len = chunkLen(chunk_bytes, size, rng.idx),
                        .req_lo = rng.lo,
                        .req_hi = rng.hi,
                    };
                    continue;
                }
                const chunk = res.value_ptr;
                chunk.req_lo = @min(chunk.req_lo, rng.lo);
                chunk.req_hi = @max(chunk.req_hi, rng.hi);
            }
        }
        for (chunks.values()) |*chunk| {
            chunk.req_hi = @min(chunk.req_hi, chunk.chunk_len);
            const len = chunk.req_hi - chunk.req_lo;
            chunk.req_buf = (try allocChunk(arloc, len))[0..len];
        }
        return chunks;
    }

    /// Every object under the configured prefix, paged through
    /// ListObjectsV2 until the server stops truncating. Caller releases
    /// the result with `_vfs.freeObjectInfos`. Names come back with the
    /// prefix stripped, so they match what `put`/`open` take. ListObjectsV2
    /// carries size and mtime whatever `detail` asks, so `stat` is always
    /// set; the parameter exists for the shared `Store.list` shape.
    pub fn list(
        self: *S3Store,
        alloc: std.mem.Allocator,
        detail: _vfs.ListDetail,
    ) ![]_vfs.ObjectInfo {
        const path = try std.fmt.allocPrint(self.a, "/{s}", .{self.bucket});
        defer self.a.free(path);

        var out: std.ArrayList(_vfs.ObjectInfo) = .empty;
        errdefer {
            for (out.items) |i| alloc.free(i.name);
            out.deinit(alloc);
        }

        var token: ?[]u8 = null;
        defer if (token) |t| self.a.free(t);
        while (true) {
            const query = try self.listQuery(token);
            defer self.a.free(query);

            var resp = try self.request(.{
                .method = .GET,
                .path = path,
                .query = query,
                .sink = .alloc,
            });
            defer resp.deinit(self.a);
            if (resp.status != 200)
                return self.statusErr(.GET, path, &resp);
            const body = resp.body orelse return error.MissingBody;

            try appendListPage(alloc, body, self.prefix, detail, &out);

            const next = xmlValue(body, "NextContinuationToken");
            const truncated = std.mem.eql(
                u8,
                xmlValue(body, "IsTruncated") orelse "false",
                "true",
            );
            if (!truncated or next == null) break;
            if (token) |t| self.a.free(t);
            token = try self.a.dupe(u8, next.?);
        }
        return out.toOwnedSlice(alloc);
    }

    /// The canonical query string for one ListObjectsV2 page: parameters
    /// sorted by name and URI-encoded, which is what SigV4 signs and what
    /// goes on the request line. Caller frees.
    fn listQuery(self: *S3Store, token: ?String) ![]u8 {
        var buf: std.ArrayList(u8) = .empty;
        errdefer buf.deinit(self.a);
        // Sorted: continuation-token < list-type < prefix.
        if (token) |t| {
            try buf.appendSlice(self.a, "continuation-token=");
            try uriEncode(self.a, &buf, t);
            try buf.append(self.a, '&');
        }
        try buf.appendSlice(self.a, "list-type=2");
        if (self.prefix.len > 0) {
            try buf.appendSlice(self.a, "&prefix=");
            try uriEncode(self.a, &buf, self.prefix);
            // A prefix names a folder: match the keys under it, not one
            // whose name merely starts with the same letters.
            try uriEncode(self.a, &buf, "/");
        }
        return buf.toOwnedSlice(self.a);
    }

    fn statusErr(
        self: *S3Store,
        method: Method,
        name: String,
        resp: *const Response,
    ) error{S3Error} {
        _ = self;
        log.err("s3 {s} {s}: status={d} body={s}", .{
            @tagName(method), name, resp.status, if (resp.body) |b| b else "",
        });
        return error.S3Error;
    }

    pub const Method = enum { GET, PUT, HEAD, DELETE };

    /// One request. `path` and `query` are the canonical forms SigV4
    /// signs (the query sorted and URI-encoded); `range` rides unsigned
    /// (SigV4 requires host + x-amz-* only).
    pub const Req = struct {
        method: Method,
        /// `objectRequest` fills it from the object name.
        path: String = "",
        query: String = "",
        body: ?String = null,
        range: ?[2]u64 = null,
        sink: BodySink = .discard,
        /// Non-null → record time-to-first-byte (request flushed →
        /// first response byte on the wire). Calibration only.
        ttfb_ns: ?*u64 = null,
    };

    pub const Response = struct {
        status: u16,
        content_length: u64 = 0,
        /// The Last-Modified header as Unix seconds; 0 when absent.
        last_modified_s: i64 = 0,
        body: ?[]u8 = null,
        /// Server refused keep-alive (Connection: close / HTTP/1.0) —
        /// the conn must not go back to the pool.
        conn_close: bool = false,

        pub fn deinit(self: *Response, a: std.mem.Allocator) void {
            if (self.body) |b| a.free(b);
            self.body = null;
        }
    };

    /// One signed HTTP request on a connection slot. Bounded retries on
    /// connect failures, transport errors and 5xx; definitive statuses
    /// (2xx/4xx) return immediately. A failure closes the slot's conn and
    /// the retry dials a fresh one into the same slot, so a stale
    /// keep-alive costs one attempt, never the slot.
    fn request(self: *S3Store, req: Req) !Response {
        const method = @tagName(req.method);
        var slot = try self.acquire();
        defer self.release(slot);
        var attempt: u32 = 0;
        while (true) : (attempt += 1) {
            const conn = slot.conn orelse self.newConn() catch |err| {
                if (attempt + 1 >= attempts_max) return err;
                log.warn("s3 {s} {s}: connect: {s}, retrying", .{
                    method, req.path, @errorName(err),
                });
                fiber.sleepMs(100 * (attempt + 1));
                continue;
            };
            slot.conn = conn;
            const resp = self.requestOnce(conn, req) catch |err| {
                conn.deinit(self.a);
                slot.conn = null;
                if (attempt + 1 >= attempts_max) return err;
                log.warn("s3 {s} {s}: {s}, retrying", .{
                    method, req.path, @errorName(err),
                });
                fiber.sleepMs(100 * (attempt + 1));
                continue;
            };
            // The response is fully drained — the conn is reusable unless
            // the server said otherwise.
            if (resp.conn_close) {
                conn.deinit(self.a);
                slot.conn = null;
            }
            if (resp.status >= 500 and attempt + 1 < attempts_max) {
                var r = resp;
                r.deinit(self.a);
                log.warn("s3 {s} {s}: status={d}, retrying", .{
                    method, req.path, resp.status,
                });
                fiber.sleepMs(100 * (attempt + 1));
                continue;
            }
            return resp;
        }
    }

    /// `request` against one object, whose key is `name` under the
    /// bucket and the configured prefix. No query parameters.
    fn objectRequest(self: *S3Store, name: String, req_in: Req) !Response {
        var req = req_in;
        req.path = try self.objectPath(name);
        defer self.a.free(req.path);
        return self.request(req);
    }

    /// The request path of object `name`: `/bucket[/prefix]/name`.
    /// Caller frees.
    fn objectPath(self: *S3Store, name: String) ![]u8 {
        if (self.prefix.len > 0) return std.fmt.allocPrint(
            self.a,
            "/{s}/{s}/{s}",
            .{ self.bucket, self.prefix, name },
        );
        return std.fmt.allocPrint(self.a, "/{s}/{s}", .{ self.bucket, name });
    }

    fn requestOnce(self: *S3Store, conn: *Conn, req: Req) !Response {
        const a = self.a;
        const method = @tagName(req.method);
        const path = req.path;
        const query = req.query;
        const body = req.body;
        const now_s: u64 = @intCast(@divTrunc(utils.coarseMs(), 1000));
        const amz = _sigv4.AmzDate.fromEpochSeconds(now_s);
        const payload_hash = _sigv4.hashHex(body orelse "");
        const auth = try _sigv4.authorization(
            a,
            self.access_key,
            self.secret_key,
            self.region,
            method,
            path,
            query,
            self.host_header,
            &payload_hash,
            &amz,
            self.session_token,
        );
        defer a.free(auth);

        const target = if (query.len > 0)
            try std.fmt.allocPrint(a, "{s}?{s}", .{ path, query })
        else
            try a.dupe(u8, path);
        defer a.free(target);

        var head_buf: std.ArrayList(u8) = .empty;
        defer head_buf.deinit(a);
        // Keep-alive (the HTTP/1.1 default — no Connection header): the
        // caller pools the conn after the response is fully drained.
        try head_buf.print(a, "{s} {s} HTTP/1.1\r\n" ++
            "Host: {s}\r\nx-amz-date: {s}\r\nx-amz-content-sha256: {s}\r\n" ++
            "Authorization: {s}\r\nContent-Length: {d}\r\n", .{
            method,                     target,        self.host_header,
            &amz.stamp,                 &payload_hash, auth,
            if (body) |b| b.len else 0,
        });
        if (self.session_token) |t| {
            try head_buf.print(a, "x-amz-security-token: {s}\r\n", .{t});
        }
        if (req.range) |r| {
            try head_buf.print(a, "Range: bytes={d}-{d}\r\n", .{
                r[0], r[0] + r[1] - 1,
            });
        }
        try head_buf.appendSlice(a, "\r\n");

        const w = conn.wr();
        w.writeAll(head_buf.items) catch return error.BrokenPipe;
        if (body) |b| w.writeAll(b) catch return error.BrokenPipe;
        conn.flushAll() catch return error.BrokenPipe;

        const reader = conn.rd();
        if (req.ttfb_ns) |out| {
            const sent = utils.timestampNow(.monotonic, .nanos);
            _ = reader.peekByte() catch return error.BadResponse;
            out.* = @intCast(utils.timestampNow(.monotonic, .nanos) - sent);
        }
        return parseResponse(reader, a, req.method, req.sink);
    }

    /// Measure this endpoint's ideal readahead (the calibration policy
    /// lives in vfs/lookahead.zig; this store contributes probeRead and
    /// the object ops). For `--probe`; the server's uncached reads use
    /// `uncached_span` instead.
    pub fn calibrateLookahead(self: *S3Store, block_size: u64) !u64 {
        return _lookahead.calibrate(self.a, self, block_size);
    }

    /// One timed ranged GET of the calibration probe object — the
    /// measurement primitive behind lookahead.calibrate. A retry inside
    /// `request` inflates one sample; calibrate takes medians.
    pub fn probeRead(self: *S3Store, off: u64, len: u64) !_lookahead.Probe {
        const path = try self.objectPath(_lookahead.probe_object);
        defer self.a.free(path);
        var ttfb: u64 = 0;
        const t0 = utils.timestampNow(.monotonic, .nanos);
        var resp = try self.request(.{
            .method = .GET,
            .path = path,
            .range = .{ off, len },
            .sink = .alloc,
            .ttfb_ns = &ttfb,
        });
        const total: u64 =
            @intCast(utils.timestampNow(.monotonic, .nanos) - t0);
        defer resp.deinit(self.a);
        if (resp.status != 200 and resp.status != 206) return error.S3Error;
        // The probe object carries a position-dependent pattern — verify
        // we actually received the bytes we timed.
        const body = resp.body orelse return error.MissingBody;
        if (body.len != len) return error.ShortRead;
        if (!_lookahead.verifyProbeData(off, body)) return error.BadProbeData;
        return .{ .size = len, .ttfb_ns = ttfb, .total_ns = total };
    }
};

/// Where a response body goes. An error status (>= 300) is allocated
/// whatever the sink says, so `statusErr` can log the XML.
pub const BodySink = union(enum) {
    /// Drain it so the connection stays reusable (PUT, HEAD, DELETE).
    discard,
    /// `Response.body`, owned by the store allocator (list, probe).
    alloc,
    /// Read exactly `len` bytes straight into this buffer; a body of
    /// another length is drained and reported as ShortRead.
    into: []u8,
};

/// Parse one HTTP/1.1 response: status line, headers, then the body where
/// `sink` says (or allocated on an error status, whose XML body we log)
/// — HEAD advertises Content-Length but never carries a body. ALWAYS consumes
/// the response through its final body byte (unwanted bodies are
/// discarded), so on return the connection is clean for the next request.
/// Pub for tests (canned buffers via std.Io.Reader.fixed).
pub fn parseResponse(
    reader: *std.Io.Reader,
    a: std.mem.Allocator,
    method: S3Store.Method,
    sink: BodySink,
) !S3Store.Response {
    // Status line: "HTTP/1.1 200 OK".
    const status_line = reader.takeDelimiterInclusive('\n') catch return error.BadResponse;
    if (status_line.len < 12 or !std.mem.startsWith(
        u8,
        status_line,
        "HTTP/1.",
    )) return error.BadResponse;
    const status = std.fmt.parseInt(
        u16,
        status_line[9..12],
        10,
    ) catch return error.BadResponse;

    var resp = S3Store.Response{ .status = status };
    // HTTP/1.0 has no keep-alive by default.
    var chunked = false;
    resp.conn_close = std.mem.startsWith(u8, status_line, "HTTP/1.0");

    while (true) {
        const line_raw = reader.takeDelimiterInclusive('\n') catch return error.BadResponse;
        const line = std.mem.trimEnd(u8, line_raw, "\r\n");
        if (line.len == 0) break;

        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const hname = line[0..colon];
        const hval = std.mem.trim(u8, line[colon + 1 ..], " ");

        if (std.ascii.eqlIgnoreCase(hname, "content-length")) {
            resp.content_length = std.fmt.parseInt(u64, hval, 10) catch return error.BadResponse;
        } else if (std.ascii.eqlIgnoreCase(hname, "last-modified")) {
            resp.last_modified_s = utils.date.parseHttpDate(hval) catch
                return error.BadResponse;
        } else if (std.ascii.eqlIgnoreCase(hname, "transfer-encoding")) {
            chunked = true;
        } else if (std.ascii.eqlIgnoreCase(hname, "connection")) {
            if (std.ascii.eqlIgnoreCase(hval, "close")) resp.conn_close = true;
        }
    }
    if (chunked) return error.UnsupportedChunkedEncoding;

    const has_body = resp.content_length > 0 and method != .HEAD;
    if (!has_body) return resp;
    if (resp.status >= 300 or sink == .alloc) {
        const buf = try a.alloc(u8, @intCast(resp.content_length));
        errdefer a.free(buf);
        reader.readSliceAll(buf) catch return error.BadResponse;
        resp.body = buf;
        return resp;
    }
    if (sink == .into and sink.into.len == resp.content_length) {
        reader.readSliceAll(sink.into) catch return error.BadResponse;
        return resp;
    }
    // Drain the unwanted body so the connection stays reusable.
    reader.discardAll64(resp.content_length) catch return error.BadResponse;
    if (sink == .into) {
        log.err("s3 {s}: asked {d} bytes, got {d}", .{
            @tagName(method), sink.into.len, resp.content_length,
        });
        return error.ShortRead;
    }
    return resp;
}

/// The SigV4 signing region for an endpoint host. AWS and Backblaze
/// put it in the host (`s3.us-west-2.amazonaws.com`,
/// `s3-us-west-2.amazonaws.com`, `s3.us-east-005.backblazeb2.com`);
/// MinIO, SeaweedFS and IP endpoints carry none and accept the
/// default.
pub fn regionFromHost(host: String) String {
    const rest = if (std.mem.startsWith(u8, host, "s3."))
        host[3..]
    else if (std.mem.startsWith(u8, host, "s3-"))
        host[3..]
    else
        return "us-east-1";
    const end = std.mem.indexOfScalar(u8, rest, '.') orelse rest.len;
    // "s3.amazonaws.com": the label after "s3." is the domain, not a
    // region. A region always has at least two labels after it.
    if (std.mem.count(u8, rest[end..], ".") < 2) return "us-east-1";
    return rest[0..end];
}

// ───────────────────────── ListObjectsV2 parsing ─────────────────────────
//
// The response is XML, and the three fields we read (`Key`, `Size`,
// `LastModified`) are flat text elements inside each `<Contents>`. That is
// small enough to scan for tags directly — an XML library would be a
// dependency and a parse tree for three strings.

/// The text between the first `<tag>` and its `</tag>` in `xml`, or null.
/// Borrows `xml`.
fn xmlValue(xml: String, comptime tag: String) ?String {
    const open = "<" ++ tag ++ ">";
    const close = "</" ++ tag ++ ">";
    const a = std.mem.indexOf(u8, xml, open) orelse return null;
    const from = a + open.len;
    const b = std.mem.indexOfPos(u8, xml, from, close) orelse return null;
    return xml[from..b];
}

/// Append one ListObjectsV2 page's objects to `out`. `prefix` is stripped
/// from each key (with its `/` separator) so the names match what the
/// store's other ops take. Names are duped out of `alloc`; the caller owns
/// them and frees on error.
fn appendListPage(
    alloc: std.mem.Allocator,
    body: String,
    prefix: String,
    _: _vfs.ListDetail,
    out: *std.ArrayList(_vfs.ObjectInfo),
) !void {
    var rest = body;
    while (std.mem.indexOf(u8, rest, "<Contents>")) |start| {
        const from = start + "<Contents>".len;
        const end = std.mem.indexOfPos(u8, rest, from, "</Contents>") orelse
            return error.BadListResponse;
        const item = rest[from..end];
        rest = rest[end..];

        const key = xmlValue(item, "Key") orelse return error.BadListResponse;
        const size_txt = xmlValue(item, "Size") orelse return error.BadListResponse;
        const when = xmlValue(item, "LastModified") orelse
            return error.BadListResponse;

        // A "key" that is just the prefix folder itself is not an object.
        var name = key;
        if (prefix.len > 0) {
            if (!std.mem.startsWith(u8, key, prefix)) continue;
            if (key.len <= prefix.len + 1) continue;
            name = key[prefix.len + 1 ..];
        }

        const stat: _vfs.FileStat = .{
            .size = try std.fmt.parseInt(u64, size_txt, 10),
            .mtime_s = @divFloor(try utils.date.parseTimestamp(when, true), std.time.us_per_s),
        };

        const owned = try alloc.dupe(u8, name);
        errdefer alloc.free(owned);
        try out.append(alloc, .{ .name = owned, .stat = stat });
    }
}

/// Percent-encode `s` into `buf` per SigV4's rules: the unreserved set
/// (`A-Za-z0-9-_.~`) passes through, every other byte becomes `%XX` with
/// uppercase hex. `/` is encoded too, which is what a query-parameter
/// value wants.
fn uriEncode(a: std.mem.Allocator, buf: *std.ArrayList(u8), s: String) !void {
    for (s) |c| {
        const unreserved = (c >= 'A' and c <= 'Z') or (c >= 'a' and c <= 'z') or
            (c >= '0' and c <= '9') or c == '-' or c == '_' or c == '.' or c == '~';
        if (unreserved) {
            try buf.append(a, c);
        } else {
            try buf.print(a, "%{X:0>2}", .{c});
        }
    }
}

// Pub for tests: canned ListObjectsV2 bodies, no network.
pub const list_parsing_for_tests = struct {
    pub const appendPage = appendListPage;
    pub const encode = uriEncode;
};
