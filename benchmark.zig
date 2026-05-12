const std = @import("std");
const zio = @import("zio");
const flags = @import("flags");
const proto = @import("zdb-server/protocol.zig");

const Timestamp = zio.Timestamp;
const Duration = zio.Duration;
const log = std.log.scoped(.zdb_client);

/// Flag-registered config. `phase` is a string ("load" / "run" / "both")
/// — flags.zig doesn't support enum-typed fields. Two callsites below
/// compare with `std.mem.eql` instead of an enum tag.
const Args = struct {
    addr: []const u8 = "127.0.0.1:8001",
    clients: u32 = 1,
    records: u64 = 1000,
    ops: u64 = 10000,
    read_pct: u8 = 50,
    value_size: u32 = 100,
    namespaces: u32 = 1,
    verify_clients: u32 = 0,
    batch_size: u32 = 1,
    phase: []const u8 = "both",

    pub const docs = .{
        .self = "ZDB benchmark client (YCSB-shaped load + run phases).",
        .addr = "Server address as ip:port.",
        .clients = "Concurrent writer fiber count.",
        .records = "Records to insert during load phase.",
        .ops = "Operations per client during run phase.",
        .read_pct = "Read percentage [0..100]: 0=write-only, 50=YCSB-A, 95=YCSB-B, 100=YCSB-C.",
        .value_size = "Value size in bytes.",
        .namespaces = "Spread traffic across N namespaces; each must already exist (ns create <id>). Keys hash deterministically: k → (k % namespaces) + 1.",
        .verify_clients = "Concurrent read-verifier fibers active during load phase (0 disables).",
        .batch_size = "Writes per set-batch proposal (1 = no batching). Higher amortizes Raft commit cost.",
        .phase = "Which phase(s) to run: \"load\" (writes only), \"run\" (mixed read/write workload over already-loaded data), or \"both\" (load then run).",
    };
};

/// Map a key id to its namespace. Deterministic so writers and verifiers
/// addressing the same key agree on the target namespace.
fn nsFor(key_id: u64, namespaces: u32) u32 {
    return @intCast((key_id % @as(u64, namespaces)) + 1);
}

fn usage(alloc: std.mem.Allocator) void {
    var help: std.ArrayList(u8) = .empty;
    defer help.deinit(alloc);
    flags.g().writeHelpAll(&help) catch {};
    const stderr = std.fs.File.stderr();
    _ = stderr.write(help.items) catch {};
    _ = stderr.write("\n") catch {};
}

// --- Text protocol (client side) ---

fn sendSet(stream: zio.net.Stream, ns: u32, key: []const u8, value: []const u8) !void {
    var hdr_buf: [128]u8 = undefined;
    const hdr = std.fmt.bufPrint(&hdr_buf, "set {d} {s} ", .{ ns, key }) catch unreachable;
    try stream.writeAll(hdr, .none);
    try stream.writeAll(value, .none);
    try stream.writeAll("\n", .none);
}

/// One-shot batch write in the human-friendly text protocol:
///
///   set-batch <ns> <count>
///   <k1> <v1>
///   <k2> <v2>
///   ...
///
/// Newlines are cosmetic — the server tokenizes on any whitespace. All
/// key/value bytes are emitted literally (our fillValue avoids whitespace
/// and leading `0x`, so no hex encoding is needed on the hot path).
/// Packs everything into one buffer and sends with a single writeAll.
fn sendSetBatch(
    allocator: std.mem.Allocator,
    stream: zio.net.Stream,
    ns: u32,
    keys: []const []const u8,
    values: []const []const u8,
) !void {
    std.debug.assert(keys.len == values.len);

    var body_bytes: usize = 0;
    for (keys, values) |k, v| body_bytes += k.len + v.len + 2; // key + space + val + '\n'

    const hdr_cap: usize = 64;
    const buf = try allocator.alloc(u8, hdr_cap + body_bytes);
    defer allocator.free(buf);
    const hdr = try std.fmt.bufPrint(buf, "set-batch {d} {d}\n", .{ ns, keys.len });

    var off: usize = hdr.len;
    for (keys, values) |k, v| {
        @memcpy(buf[off .. off + k.len], k);
        off += k.len;
        buf[off] = ' ';
        off += 1;
        @memcpy(buf[off .. off + v.len], v);
        off += v.len;
        buf[off] = '\n';
        off += 1;
    }
    try stream.writeAll(buf[0..off], .none);
}

const SetResult = struct {
    ok: bool,
    /// HLC timestamp assigned by the leader. Clients track the max seen and
    /// pass it to subsequent gets as `read_ts` for read-your-writes.
    commit_ts: u64 = 0,
    not_leader: bool = false,
    leader_id: u64 = 0,
};

fn recvSetResponse(stream: zio.net.Stream) !SetResult {
    var buf: [256]u8 = undefined;
    const line = try readResponseLine(stream, &buf);
    if (std.mem.startsWith(u8, line, "OK ")) {
        const ts_str = line["OK ".len..];
        const commit_ts = std.fmt.parseInt(u64, ts_str, 10) catch 0;
        return .{ .ok = true, .commit_ts = commit_ts };
    } else if (std.mem.eql(u8, line, "OK")) {
        return .{ .ok = true };
    } else if (std.mem.startsWith(u8, line, "NOT_LEADER ")) {
        const id_str = line["NOT_LEADER ".len..];
        const leader_id = std.fmt.parseInt(u64, id_str, 10) catch 0;
        return .{ .ok = false, .not_leader = true, .leader_id = leader_id };
    } else {
        return .{ .ok = false };
    }
}

fn sendGet(stream: zio.net.Stream, ns: u32, key: []const u8, read_ts: u64) !void {
    var hdr_buf: [128]u8 = undefined;
    const hdr = std.fmt.bufPrint(&hdr_buf, "get {d} {s} {d}\n", .{ ns, key, read_ts }) catch unreachable;
    try stream.writeAll(hdr, .none);
}

const GetResult = struct {
    found: bool,
    ts: u64 = 0,
    /// The value bytes, sliced into the caller-provided buffer. Empty on miss.
    value: []const u8 = "",
};

/// Reads the response for a `get` into the caller-provided `buf` (which must
/// be large enough for the header + value — callers size it for max value_size).
/// On FOUND, `value` slices into `buf` past the "FOUND <ts> " header.
fn recvGetResponse(stream: zio.net.Stream, buf: []u8) !GetResult {
    const line = try readResponseLine(stream, buf);
    if (std.mem.eql(u8, line, "NOT_FOUND")) {
        return .{ .found = false };
    } else if (std.mem.startsWith(u8, line, "FOUND ")) {
        const rest = line["FOUND ".len..];
        // Parse ts from "FOUND <ts> <value>"
        if (std.mem.indexOfScalar(u8, rest, ' ')) |sep| {
            const ts = std.fmt.parseInt(u64, rest[0..sep], 10) catch 0;
            return .{ .found = true, .ts = ts, .value = rest[sep + 1 ..] };
        }
        return .{ .found = true };
    } else {
        return .{ .found = false };
    }
}

fn readResponseLine(stream: zio.net.Stream, buf: []u8) ![]const u8 {
    var pos: usize = 0;
    while (pos < buf.len) {
        var byte: [1]u8 = undefined;
        try stream.readAll(&byte, .none);
        if (byte[0] == '\n') {
            const end = if (pos > 0 and buf[pos - 1] == '\r') pos - 1 else pos;
            return buf[0..end];
        }
        buf[pos] = byte[0];
        pos += 1;
    }
    return error.LineTooLong;
}

// --- Key + value generation ---

/// Writes `user{id:016d}` into `buf` and returns the slice of bytes written.
/// `buf` must be at least 20 bytes. Changed from `*[24]u8` to `[]u8` so the
/// same function works for stack-fixed buffers and for slices into a larger
/// pre-allocated batch buffer (both coerce to `[]u8`).
fn formatKey(buf: []u8, id: u64) []const u8 {
    return std.fmt.bufPrint(buf, "user{d:0>16}", .{id}) catch unreachable;
}

/// A pool of printable-ASCII bytes used as the source of every value the
/// Writes a deterministic byte pattern for key `id` into `buf`. Each id
/// gets its own PRNG seeded by `id`, so the value bytes are statistically
/// independent across keys and effectively incompressible (matches
/// YCSB's RandomCharByteIterator semantics). Any reader that knows `id`
/// reproduces the exact bytes for verification.
///
/// Alphabet is 0x31..0x7E — printable ASCII excluding space, control
/// chars, and '0' — so the batch protocol's whitespace tokenization
/// works without hex-encoding and no value can start with "0x".
fn fillValue(buf: []u8, id: u64) void {
    var prng = std.Random.DefaultPrng.init(id);
    const r = prng.random();
    const span: u8 = 0x7E - 0x31 + 1;
    for (buf) |*b| b.* = 0x31 + r.intRangeLessThan(u8, 0, span);
}

/// valueMatches returns true iff `got` equals the expected pattern for id.
fn valueMatches(got: []const u8, id: u64) bool {
    var expected_buf: [16 * 1024]u8 = undefined;
    if (got.len > expected_buf.len) return false;
    const expected = expected_buf[0..got.len];
    fillValue(expected, id);
    return std.mem.eql(u8, got, expected);
}

/// getWithRetry issues a GET and retries on NOT_FOUND with exponential backoff.
/// The server already blocks until apply_ts reaches read_ts, so NOT_FOUND
/// after waiting is suspicious (possible data loss or a lagging follower).
/// We still retry a few times as defense in depth.
///
/// Caller provides `buf` for the response (sized to hold header + value) and
/// `read_ts` (typically `SharedState.max_commit_ts` so followers catch up to
/// every previously-acked write).
fn getWithRetry(stream: zio.net.Stream, ns: u32, key: []const u8, read_ts: u64, buf: []u8) !GetResult {
    const max_retries: u32 = 5;
    var delay_ms: u64 = 10;
    var attempt: u32 = 0;
    while (true) : (attempt += 1) {
        try sendGet(stream, ns, key, read_ts);
        const result = try recvGetResponse(stream, buf);
        if (result.found) return result;
        if (attempt >= max_retries) return result;
        zio.sleep(zio.Duration.fromMilliseconds(delay_ms)) catch return result;
        delay_ms *= 2;
    }
}

// --- Shared state ---

/// SharedState is shared across all worker fibers (writers + verifiers).
/// Atomics are used so verifiers can race-read high_water and loader_done.
const SharedState = struct {
    /// Highest (any-worker) key id + 1 that has been acked. Loose progress
    /// signal used by the verifier to decide "has writing started / is it
    /// still advancing" — NOT used to pick which key to read.
    high_water: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Per-worker count of successfully-written keys within that worker's
    /// range. Verifiers pick a worker uniformly then pick a random offset
    /// in [0, per_worker_written[w]) so they only ever read keys that have
    /// actually been acked by that worker — avoids the off-by-contiguous
    /// bug where a global high_water doesn't imply every key below it was
    /// written (workers have disjoint ranges).
    /// Allocated in runBenchmark to len = args.clients.
    per_worker_written: []std.atomic.Value(u64) = &.{},
    /// Max commit timestamp observed across all workers. Verifiers use this
    /// as `read_ts` so followers block until apply_ts has caught up to every
    /// previously-acked write.
    max_commit_ts: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Number of load workers that have finished. Verifier fibers exit once
    /// this reaches clients.
    loaders_done: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    /// Count of read-verify mismatches observed across all workers.
    /// Nonzero at exit == benchmark failure.
    mismatches: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Number of verify reads attempted (for reporting).
    verify_reads: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Number of verify reads that hit NOT_FOUND despite high_water covering
    /// the key. A brief window is expected (commit ack → apply lag on a
    /// follower connection); sustained nonzero is a bug.
    verify_misses: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
};

// --- Per-worker stats ---

const WorkerStats = struct {
    read_ok: u64 = 0,
    read_miss: u64 = 0,
    read_err: u64 = 0,
    write_ok: u64 = 0,
    write_err: u64 = 0,
    total_read_ns: u64 = 0,
    total_write_ns: u64 = 0,
    min_read_ns: u64 = std.math.maxInt(u64),
    max_read_ns: u64 = 0,
    min_write_ns: u64 = std.math.maxInt(u64),
    max_write_ns: u64 = 0,

    fn merge(self: *WorkerStats, other: WorkerStats) void {
        self.read_ok += other.read_ok;
        self.read_miss += other.read_miss;
        self.read_err += other.read_err;
        self.write_ok += other.write_ok;
        self.write_err += other.write_err;
        self.total_read_ns += other.total_read_ns;
        self.total_write_ns += other.total_write_ns;
        self.min_read_ns = @min(self.min_read_ns, other.min_read_ns);
        self.max_read_ns = @max(self.max_read_ns, other.max_read_ns);
        self.min_write_ns = @min(self.min_write_ns, other.min_write_ns);
        self.max_write_ns = @max(self.max_write_ns, other.max_write_ns);
    }
};

// --- Worker coroutine ---

const WorkerCtx = struct {
    id: u32,
    args: *const Args,
    server_addr: zio.net.IpAddress,
    shared: *SharedState,
    stats: WorkerStats = .{},
    load_elapsed_ns: u64 = 0,
    run_elapsed_ns: u64 = 0,
    err: ?anyerror = null,
};

fn workerFn(ctx: *WorkerCtx) void {
    workerImpl(ctx) catch |err| {
        ctx.err = err;
    };
}

fn workerImpl(ctx: *WorkerCtx) !void {
    const args = ctx.args;

    log.info("worker {d}: connecting to server", .{ctx.id});
    const stream = ctx.server_addr.connect(.{}) catch |err| {
        log.err("worker {d}: connect failed: {s}", .{ ctx.id, @errorName(err) });
        return err;
    };
    defer stream.close();
    stream.socket.setNoDelay(true) catch {};
    log.info("worker {d}: connected", .{ctx.id});

    const alloc = std.heap.page_allocator;
    var read_buf: [16 * 1024 + 256]u8 = undefined;
    // Clamp to the server's wire-level cap so we never generate a value
    // the server would reject with error.TokenTooLong. Single source of
    // truth lives in protocol.zig.
    const value_size: usize = @min(args.value_size, proto.max_token_size);
    const bs: u32 = @max(args.batch_size, 1);
    // Pre-alloc batch buffers once per worker (keys + values flat + their
    // slice-of-slice headers). Reused every batch iteration.
    const keys_flat = try alloc.alloc(u8, bs * 24);
    defer alloc.free(keys_flat);
    const vals_flat = try alloc.alloc(u8, bs * value_size);
    defer alloc.free(vals_flat);
    const keys_slices = try alloc.alloc([]const u8, bs);
    defer alloc.free(keys_slices);
    const vals_slices = try alloc.alloc([]const u8, bs);
    defer alloc.free(vals_slices);
    // PRNG only used for run-phase random op selection (read vs write).
    var prng = std.Random.DefaultPrng.init(@as(u64, ctx.id) ^ 0xdeadbeef);

    // Load phase: each worker inserts its slice of records as batches of up
    // to `bs` keys per `set_batch`. Batches break on namespace boundaries
    // (all kvs in one batch must share a namespace — that's how the server
    // is structured).
    if (std.mem.eql(u8, args.phase, "load") or std.mem.eql(u8, args.phase, "both")) {
        const total = args.records;
        const n_clients: u64 = args.clients;
        const per_worker = total / n_clients;
        const start_key = per_worker * ctx.id;
        const end_key = if (ctx.id == args.clients - 1) total else start_key + per_worker;

        log.info("worker {d}: load phase keys [{d}..{d}) batch_size={d}", .{ ctx.id, start_key, end_key, bs });
        const t0 = Timestamp.now(.monotonic);
        var k: u64 = start_key;
        while (k < end_key) {
            // Fill the next batch — stop early on namespace boundary.
            var n: u32 = 0;
            var batch_ns: u32 = 0;
            while (n < bs and k < end_key) {
                const this_ns = nsFor(k, args.namespaces);
                if (n == 0) {
                    batch_ns = this_ns;
                } else if (this_ns != batch_ns) {
                    break;
                }
                keys_slices[n] = formatKey(keys_flat[n * 24 .. n * 24 + 24], k);
                const vslot = vals_flat[n * value_size .. (n + 1) * value_size];
                fillValue(vslot, k);
                vals_slices[n] = vslot;
                n += 1;
                k += 1;
            }
            if (n == 0) break;

            try sendSetBatch(alloc, stream, batch_ns, keys_slices[0..n], vals_slices[0..n]);
            const resp = try recvSetResponse(stream);
            if (resp.ok) {
                ctx.stats.write_ok += n;
                _ = ctx.shared.high_water.fetchMax(k, .release);
                _ = ctx.shared.max_commit_ts.fetchMax(resp.commit_ts, .release);
                // All N writes are durable after this single ack.
                ctx.shared.per_worker_written[ctx.id].store(k - start_key, .release);
            } else {
                ctx.stats.write_err += n;
                if (resp.not_leader) {
                    log.warn("worker {d}: not leader (leader_id={d})", .{ ctx.id, resp.leader_id });
                }
            }
            // Progress every ~1000 records.
            const done = ctx.stats.write_ok + ctx.stats.write_err;
            if (done / 1000 != (done - n) / 1000) {
                log.info("worker {d}: loaded {d}/{d} records", .{
                    ctx.id, done, end_key - start_key,
                });
            }
        }
        const t1 = Timestamp.now(.monotonic);
        ctx.load_elapsed_ns = Timestamp.durationTo(t0, t1).toNanoseconds();
        _ = ctx.shared.loaders_done.fetchAdd(1, .release);
        log.info("worker {d}: load complete — {d} ok, {d} err, {d:.1} ms", .{
            ctx.id,
            ctx.stats.write_ok,
            ctx.stats.write_err,
            @as(f64, @floatFromInt(ctx.load_elapsed_ns)) / 1e6,
        });
    }

    // Run phase: random read/write mix. Reads retry on NOT_FOUND and verify
    // the returned value bytes against the expected deterministic pattern.
    if (std.mem.eql(u8, args.phase, "run") or std.mem.eql(u8, args.phase, "both")) {
        log.info("worker {d}: run phase — {d} ops, {d}% reads", .{ ctx.id, args.ops, args.read_pct });
        const t0 = Timestamp.now(.monotonic);
        var i: u64 = 0;
        while (i < args.ops) : (i += 1) {
            const r = prng.random().intRangeAtMost(u8, 0, 99);
            var key_buf: [24]u8 = undefined;
            const key_id = prng.random().intRangeLessThan(u64, 0, args.records);
            const key = formatKey(&key_buf, key_id);
            const ns = nsFor(key_id, args.namespaces);

            if (r < args.read_pct) {
                const read_ts = ctx.shared.max_commit_ts.load(.acquire);
                const op_start = Timestamp.now(.monotonic);
                const result = getWithRetry(stream, ns, key, read_ts, &read_buf) catch {
                    ctx.stats.read_err += 1;
                    continue;
                };
                const lat = Timestamp.durationTo(op_start, Timestamp.now(.monotonic)).toNanoseconds();
                ctx.stats.total_read_ns += lat;
                ctx.stats.min_read_ns = @min(ctx.stats.min_read_ns, lat);
                ctx.stats.max_read_ns = @max(ctx.stats.max_read_ns, lat);
                if (result.found) {
                    if (valueMatches(result.value, key_id)) {
                        ctx.stats.read_ok += 1;
                    } else {
                        _ = ctx.shared.mismatches.fetchAdd(1, .monotonic);
                        log.err("worker {d}: VALUE MISMATCH for key id={d}", .{ ctx.id, key_id });
                    }
                } else {
                    ctx.stats.read_miss += 1;
                }
            } else {
                // Reuse slot 0 of the batch value buffer for this single write.
                const vslot = vals_flat[0..value_size];
                fillValue(vslot, key_id);
                const op_start = Timestamp.now(.monotonic);
                sendSet(stream, ns, key, vslot) catch {
                    ctx.stats.write_err += 1;
                    continue;
                };
                const resp = recvSetResponse(stream) catch {
                    ctx.stats.write_err += 1;
                    continue;
                };
                const lat = Timestamp.durationTo(op_start, Timestamp.now(.monotonic)).toNanoseconds();
                ctx.stats.total_write_ns += lat;
                ctx.stats.min_write_ns = @min(ctx.stats.min_write_ns, lat);
                ctx.stats.max_write_ns = @max(ctx.stats.max_write_ns, lat);
                if (resp.ok) {
                    ctx.stats.write_ok += 1;
                    _ = ctx.shared.max_commit_ts.fetchMax(resp.commit_ts, .release);
                } else {
                    ctx.stats.write_err += 1;
                }
            }
            // Progress every 1000 ops.
            const done = ctx.stats.read_ok + ctx.stats.read_miss + ctx.stats.read_err + ctx.stats.write_ok + ctx.stats.write_err;
            if (done % 1000 == 0) {
                log.info("worker {d}: {d}/{d} ops done", .{ ctx.id, done, args.ops });
            }
        }
        const t1 = Timestamp.now(.monotonic);
        ctx.run_elapsed_ns = Timestamp.durationTo(t0, t1).toNanoseconds();
        log.info("worker {d}: run complete — reads={d} writes={d} {d:.1} ms", .{
            ctx.id,
            ctx.stats.read_ok + ctx.stats.read_miss,
            ctx.stats.write_ok,
            @as(f64, @floatFromInt(ctx.run_elapsed_ns)) / 1e6,
        });
    }
}

// --- Verifier worker (runs alongside load; reads random keys and verifies) ---

fn verifyWorkerFn(ctx: *WorkerCtx) void {
    verifyWorkerImpl(ctx) catch |err| {
        ctx.err = err;
    };
}

fn verifyWorkerImpl(ctx: *WorkerCtx) !void {
    const args = ctx.args;
    const stream = ctx.server_addr.connect(.{}) catch |err| {
        log.err("verifier {d}: connect failed: {s}", .{ ctx.id, @errorName(err) });
        return err;
    };
    defer stream.close();
    stream.socket.setNoDelay(true) catch {};
    log.info("verifier {d}: connected", .{ctx.id});

    var read_buf: [16 * 1024 + 256]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(@as(u64, ctx.id) ^ 0xcafebabe);
    const wait = zio.Duration.fromMilliseconds(20);

    const per_worker = args.records / args.clients;

    while (true) {
        const hw = ctx.shared.high_water.load(.acquire);
        if (hw == 0) {
            if (ctx.shared.loaders_done.load(.acquire) >= args.clients) return;
            zio.sleep(wait) catch return;
            continue;
        }

        // Pick a worker uniformly, then a random offset within that worker's
        // range of already-acked keys. This guarantees `j` refers to a key
        // the worker has completed writing — the global high_water doesn't
        // give that guarantee since workers cover disjoint ranges.
        const w = prng.random().intRangeLessThan(u32, 0, args.clients);
        const written = ctx.shared.per_worker_written[w].load(.acquire);
        if (written == 0) {
            zio.sleep(wait) catch return;
            continue;
        }
        const offset = prng.random().intRangeLessThan(u64, 0, written);
        const j = @as(u64, w) * per_worker + offset;
        var key_buf: [24]u8 = undefined;
        const key = formatKey(&key_buf, j);
        const ns = nsFor(j, args.namespaces);

        // Pass max_commit_ts as read_ts so the follower's handleGet waits for
        // its apply_ts to catch up to every previously-acked write before
        // serving. Without this, a verifier connected to a lagging follower
        // could see NOT_FOUND for a key the leader already committed.
        const read_ts = ctx.shared.max_commit_ts.load(.acquire);
        const result = getWithRetry(stream, ns, key, read_ts, &read_buf) catch {
            ctx.stats.read_err += 1;
            continue;
        };
        _ = ctx.shared.verify_reads.fetchAdd(1, .monotonic);
        if (result.found) {
            if (valueMatches(result.value, j)) {
                ctx.stats.read_ok += 1;
            } else {
                _ = ctx.shared.mismatches.fetchAdd(1, .monotonic);
                log.err("verifier {d}: VALUE MISMATCH for key id={d}", .{ ctx.id, j });
            }
        } else {
            // Sustained NOT_FOUND past retries — a real data-loss or read-skew
            // symptom. Log loudly; count for the summary.
            _ = ctx.shared.verify_misses.fetchAdd(1, .monotonic);
            ctx.stats.read_miss += 1;
            log.err("verifier {d}: MISS after retries for key id={d} (hw={d})", .{ ctx.id, j, hw });
        }

        // Exit condition: all loaders done AND we've seen stable high_water.
        if (ctx.shared.loaders_done.load(.acquire) >= args.clients and
            ctx.shared.high_water.load(.acquire) == hw)
        {
            return;
        }
    }
}

// --- Printing results ---

fn printResults(args: *const Args, contexts: []WorkerCtx, shared: *const SharedState) void {
    var total = WorkerStats{};
    var load_ns: u64 = 0;
    var run_ns: u64 = 0;
    var had_error = false;

    for (contexts) |*ctx| {
        if (ctx.err) |err| {
            std.debug.print("worker {d}: error: {s}\n", .{ ctx.id, @errorName(err) });
            had_error = true;
        }
        total.merge(ctx.stats);
        load_ns = @max(load_ns, ctx.load_elapsed_ns);
        run_ns = @max(run_ns, ctx.run_elapsed_ns);
    }

    // Verification summary (nonzero mismatches → load-test failure).
    const vr = shared.verify_reads.load(.monotonic);
    const vm = shared.verify_misses.load(.monotonic);
    const mm = shared.mismatches.load(.monotonic);
    if (args.verify_clients > 0 or vr > 0 or mm > 0 or vm > 0) {
        std.debug.print("--- Verification ---\n", .{});
        std.debug.print("  verify reads:   {d}\n", .{vr});
        std.debug.print("  mismatches:     {d}\n", .{mm});
        std.debug.print("  persistent miss: {d}\n", .{vm});
    }

    if (had_error) {
        std.debug.print("\n", .{});
    }

    if (std.mem.eql(u8, args.phase, "load") or std.mem.eql(u8, args.phase, "both")) {
        const load_recs = total.write_ok + total.write_err;
        if (load_ns > 0 and load_recs > 0) {
            const load_ops_sec = load_recs * 1_000_000_000 / load_ns;
            std.debug.print("--- Load Phase ---\n", .{});
            std.debug.print("  records:   {d}\n", .{load_recs});
            std.debug.print("  elapsed:   {d:.2} ms\n", .{@as(f64, @floatFromInt(load_ns)) / 1e6});
            std.debug.print("  throughput: {d} ops/sec\n", .{load_ops_sec});
            std.debug.print("  errors:    {d}\n", .{total.write_err});
        }
        // Reset write stats if both phases — load writes shouldn't mix with run writes.
        if (std.mem.eql(u8, args.phase, "both")) {
            // Re-aggregate from scratch for run phase reporting below.
            total = .{};
            for (contexts) |*ctx| {
                total.merge(ctx.stats);
            }
        }
    }

    if (std.mem.eql(u8, args.phase, "run") or std.mem.eql(u8, args.phase, "both")) {
        const total_ops = total.read_ok + total.read_miss + total.read_err + total.write_ok + total.write_err;
        if (run_ns > 0 and total_ops > 0) {
            const ops_sec = total_ops * 1_000_000_000 / run_ns;
            const reads = total.read_ok + total.read_miss;
            const writes = total.write_ok;

            std.debug.print("--- Run Phase ---\n", .{});
            std.debug.print("  operations: {d} ({d} reads, {d} writes)\n", .{ total_ops, reads, writes });
            std.debug.print("  elapsed:    {d:.2} ms\n", .{@as(f64, @floatFromInt(run_ns)) / 1e6});
            std.debug.print("  throughput: {d} ops/sec\n", .{ops_sec});
            std.debug.print("  errors:     {d} read, {d} write\n", .{ total.read_err, total.write_err });

            if (reads > 0) {
                const avg_read = total.total_read_ns / reads;
                std.debug.print("  read  avg: {d:.2} us, min: {d:.2} us, max: {d:.2} us\n", .{
                    @as(f64, @floatFromInt(avg_read)) / 1e3,
                    @as(f64, @floatFromInt(total.min_read_ns)) / 1e3,
                    @as(f64, @floatFromInt(total.max_read_ns)) / 1e3,
                });
            }
            if (writes > 0) {
                const avg_write = total.total_write_ns / writes;
                std.debug.print("  write avg: {d:.2} us, min: {d:.2} us, max: {d:.2} us\n", .{
                    @as(f64, @floatFromInt(avg_write)) / 1e3,
                    @as(f64, @floatFromInt(total.min_write_ns)) / 1e3,
                    @as(f64, @floatFromInt(total.max_write_ns)) / 1e3,
                });
            }

            if (total.read_miss > 0) {
                std.debug.print("  read misses: {d}\n", .{total.read_miss});
            }
        }
    }
}

// --- Main ---

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    flags.initGlobal(allocator);
    defer flags.deinitGlobal();
    const live = try flags.g().register("bench", Args);

    for (argv[1..]) |a| {
        if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            usage(allocator);
            return;
        }
    }

    var leftover: std.ArrayList([]const u8) = .empty;
    defer leftover.deinit(allocator);
    flags.g().parseArgv(argv[1..], &leftover) catch |err| {
        std.log.err("flag parse failed: {s}", .{@errorName(err)});
        usage(allocator);
        std.process.exit(1);
    };
    if (leftover.items.len != 0) {
        std.log.err("unexpected positional '{s}' — config goes through --bench flag", .{leftover.items[0]});
        usage(allocator);
        std.process.exit(1);
    }

    const args = live.snapshot();
    if (args.read_pct > 100) {
        std.log.err("--bench read_pct must be in [0..100]", .{});
        std.process.exit(1);
    }
    if (args.namespaces == 0 or args.batch_size == 0) {
        std.log.err("--bench namespaces and batch_size must be > 0", .{});
        std.process.exit(1);
    }
    if (!std.mem.eql(u8, args.phase, "load") and !std.mem.eql(u8, args.phase, "run") and !std.mem.eql(u8, args.phase, "both")) {
        std.log.err("--bench phase must be \"load\", \"run\", or \"both\"", .{});
        std.process.exit(1);
    }

    std.debug.print("zdb-client: addr={s} clients={d} records={d} ops={d} read%={d} value_size={d} phase={s}\n", .{
        args.addr, args.clients, args.records, args.ops, args.read_pct, args.value_size, args.phase,
    });

    const n_threads: u6 = @intCast(@max(2, args.clients));
    var rt = try zio.Runtime.init(allocator, .{
        .executors = @enumFromInt(n_threads),
    });
    defer rt.deinit();

    log.info("runtime initialized with {d} threads, starting benchmark", .{n_threads});
    // runBenchmark uses zio.group() / zio.net which require a current executor,
    // so we spawn it as a root task on the runtime instead of calling from
    // the main thread (which is never an executor after Runtime.init).
    var root = try rt.spawn(runBenchmark, .{ allocator, &args });
    const mismatches = root.join();
    if (mismatches > 0) {
        std.debug.print("FAIL: {d} value mismatches observed — data integrity failure\n", .{mismatches});
        std.process.exit(1);
    }
}

fn runBenchmark(allocator: std.mem.Allocator, args: *const Args) u64 {
    log.info("runBenchmark", .{});
    const server_addr = zio.net.IpAddress.parseIpAndPort(args.addr) catch |err| {
        log.err("invalid address '{s}': {s}", .{ args.addr, @errorName(err) });
        return 0;
    };
    log.info("server_addr done", .{});

    // Shared state seen by all workers (atomics for lock-free progress).
    var shared: SharedState = .{};
    const pw = allocator.alloc(std.atomic.Value(u64), args.clients) catch {
        log.err("out of memory allocating per_worker_written", .{});
        return 0;
    };
    defer allocator.free(pw);
    for (pw) |*slot| slot.* = std.atomic.Value(u64).init(0);
    shared.per_worker_written = pw;

    // Writer contexts + verifier contexts get a contiguous block so we can
    // aggregate stats with one pass at the end.
    const total_workers = args.clients + args.verify_clients;
    const contexts = allocator.alloc(WorkerCtx, total_workers) catch {
        log.err("out of memory allocating worker contexts", .{});
        return 0;
    };
    defer allocator.free(contexts);

    for (contexts, 0..) |*ctx, i| {
        ctx.* = .{
            .id = @intCast(i),
            .args = args,
            .server_addr = server_addr,
            .shared = &shared,
        };
    }

    log.info("spawning {d} writer(s) + {d} verifier(s)", .{ args.clients, args.verify_clients });

    var group = zio.group();
    defer group.cancel();

    // Writers = contexts[0..clients]; verifiers = contexts[clients..]
    for (contexts[0..args.clients]) |*ctx| {
        group.spawn(workerFn, .{ctx}) catch {
            log.err("failed to spawn worker {d}", .{ctx.id});
            return 0;
        };
    }
    for (contexts[args.clients..]) |*ctx| {
        group.spawn(verifyWorkerFn, .{ctx}) catch {
            log.err("failed to spawn verifier {d}", .{ctx.id});
            return 0;
        };
    }

    // Wait for all workers.
    group.wait() catch return 0;

    log.info("all workers finished", .{});
    printResults(args, contexts, &shared);
    return shared.mismatches.load(.monotonic);
}
