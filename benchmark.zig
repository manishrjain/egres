const std = @import("std");
const zio = @import("zio");

const Timestamp = zio.Timestamp;
const Duration = zio.Duration;
const log = std.log.scoped(.zdb_client);

const Args = struct {
    addr: []const u8 = "127.0.0.1:8001",
    clients: u32 = 1,
    records: u64 = 1000,
    ops: u64 = 10000,
    read_pct: u8 = 50,
    value_size: u32 = 100,
    phase: Phase = .both,
    /// Number of namespaces to spread traffic across. Each id in [1..=namespaces]
    /// must already exist (create via `ns create <id>` before loading). A key
    /// with id k goes to namespace nsFor(k) = (k % namespaces) + 1.
    namespaces: u32 = 1,
    verify_clients: u32 = 0, // concurrent verifier fibers during load phase

    const Phase = enum { load, run, both };
};

/// Map a key id to its namespace. Deterministic so writers and verifiers
/// addressing the same key agree on the target namespace.
fn nsFor(key_id: u64, namespaces: u32) u32 {
    return @intCast((key_id % @as(u64, namespaces)) + 1);
}

fn usage() void {
    std.debug.print(
        \\usage: zdb-client [options]
        \\
        \\  --addr <ip:port>        server address (default: 127.0.0.1:8001)
        \\  --clients <n>           concurrent writer clients (default: 1)
        \\  --records <n>           records to load (default: 1000)
        \\  --ops <n>               operations per client in run phase (default: 10000)
        \\  --read-pct <0-100>      read percentage (default: 50, i.e. workload A)
        \\  --value-size <n>        value size in bytes (default: 100)
        \\  --namespaces <n>        spread traffic across n namespaces (default: 1).
        \\                          Each id in [1..=n] must exist (ns create <id>).
        \\  --verify-clients <n>    concurrent read-verifier fibers during load (default: 0)
        \\  --load                  load phase only
        \\  --run                   run phase only
        \\
        \\Workload presets (set via --read-pct):
        \\  50  = YCSB-A (read-heavy update)
        \\  95  = YCSB-B (read-mostly)
        \\  100 = YCSB-C (read-only)
        \\  0   = write-only
        \\
        \\Values are deterministic from key id. Reads in the run phase and the
        \\verifier fibers compare returned bytes to the expected pattern; the
        \\benchmark exits nonzero if any mismatch is observed.
        \\
    , .{});
}

fn parseArgs(argv: []const []const u8) !Args {
    var args = Args{};
    var i: usize = 1;
    while (i < argv.len) {
        const arg = argv[i];
        if (std.mem.eql(u8, arg, "--addr")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.addr = argv[i];
        } else if (std.mem.eql(u8, arg, "--clients")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.clients = try std.fmt.parseInt(u32, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--records")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.records = try std.fmt.parseInt(u64, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--ops")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.ops = try std.fmt.parseInt(u64, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--read-pct")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.read_pct = try std.fmt.parseInt(u8, argv[i], 10);
            if (args.read_pct > 100) return error.InvalidArgs;
        } else if (std.mem.eql(u8, arg, "--value-size")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.value_size = try std.fmt.parseInt(u32, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--namespaces")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.namespaces = try std.fmt.parseInt(u32, argv[i], 10);
            if (args.namespaces == 0) return error.InvalidArgs;
        } else if (std.mem.eql(u8, arg, "--verify-clients")) {
            i += 1;
            if (i >= argv.len) return error.InvalidArgs;
            args.verify_clients = try std.fmt.parseInt(u32, argv[i], 10);
        } else if (std.mem.eql(u8, arg, "--load")) {
            args.phase = .load;
        } else if (std.mem.eql(u8, arg, "--run")) {
            args.phase = .run;
        } else {
            return error.InvalidArgs;
        }
        i += 1;
    }
    return args;
}

// --- Text protocol (client side) ---

fn sendSet(stream: zio.net.Stream, ns: u32, key: []const u8, value: []const u8) !void {
    var hdr_buf: [128]u8 = undefined;
    const hdr = std.fmt.bufPrint(&hdr_buf, "set {d} {s} ", .{ ns, key }) catch unreachable;
    try stream.writeAll(hdr, .none);
    try stream.writeAll(value, .none);
    try stream.writeAll("\n", .none);
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

fn formatKey(buf: *[24]u8, id: u64) []const u8 {
    const result = std.fmt.bufPrint(buf, "user{d:0>16}", .{id}) catch unreachable;
    return result;
}

/// A pool of printable-ASCII bytes used as the source of every value the
/// benchmark writes. Pre-filled once at startup (see `initValueBuffer`) and
/// read-only thereafter — no per-value PRNG cost on the hot path.
const value_buffer_size: usize = 4 * 1024 * 1024;
var value_buffer: [value_buffer_size]u8 = undefined;

fn initValueBuffer() void {
    var prng = std.Random.DefaultPrng.init(0xc0ffee);
    for (&value_buffer) |*b| {
        b.* = 0x20 + prng.random().intRangeLessThan(u8, 0, 95); // 0x20..0x7E
    }
}

/// Writes a deterministic byte pattern for key `id` into `buf`. The bytes are
/// a window into the shared `value_buffer` starting at offset `id % size`,
/// wrapping around the end if needed. Adjacent ids produce adjacent windows
/// — good cache locality for the memcpy, and any reader that knows `id` can
/// reconstruct the exact bytes for verification.
fn fillValue(buf: []u8, id: u64) void {
    std.debug.assert(buf.len <= value_buffer_size);
    const start: usize = @intCast(id % value_buffer_size);
    const first = @min(buf.len, value_buffer_size - start);
    @memcpy(buf[0..first], value_buffer[start .. start + first]);
    if (first < buf.len) {
        @memcpy(buf[first..], value_buffer[0 .. buf.len - first]);
    }
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

    // Deterministic per-key value buffer (reused per write) + read buffer
    // sized for our max value size plus the "FOUND <ts> " header.
    var value_buf: [16 * 1024]u8 = undefined;
    var read_buf: [16 * 1024 + 256]u8 = undefined;
    const value_size: usize = @min(args.value_size, value_buf.len);
    // PRNG only used for run-phase random op selection (read vs write).
    var prng = std.Random.DefaultPrng.init(@as(u64, ctx.id) ^ 0xdeadbeef);

    // Load phase: each worker inserts its slice of records. We bump high_water
    // after each ack'd write so verifier fibers can safely read what's been
    // committed.
    if (args.phase == .load or args.phase == .both) {
        const total = args.records;
        const n_clients: u64 = args.clients;
        const per_worker = total / n_clients;
        const start_key = per_worker * ctx.id;
        const end_key = if (ctx.id == args.clients - 1) total else start_key + per_worker;

        log.info("worker {d}: load phase keys [{d}..{d})", .{ ctx.id, start_key, end_key });
        const t0 = Timestamp.now(.monotonic);
        var k: u64 = start_key;
        while (k < end_key) : (k += 1) {
            var key_buf: [24]u8 = undefined;
            const key = formatKey(&key_buf, k);
            fillValue(value_buf[0..value_size], k);
            try sendSet(stream, nsFor(k, args.namespaces), key, value_buf[0..value_size]);
            const resp = try recvSetResponse(stream);
            if (resp.ok) {
                ctx.stats.write_ok += 1;
                _ = ctx.shared.high_water.fetchMax(k + 1, .release);
                _ = ctx.shared.max_commit_ts.fetchMax(resp.commit_ts, .release);
                // k - start_key == number of writes completed by this worker
                // BEFORE this one, so +1 = "count of keys acked so far". Store
                // instead of fetchAdd so ordering is crash-safe (every ack
                // gives a strictly greater value).
                ctx.shared.per_worker_written[ctx.id].store(k - start_key + 1, .release);
            } else {
                ctx.stats.write_err += 1;
                if (resp.not_leader) {
                    log.warn("worker {d}: not leader (leader_id={d})", .{ ctx.id, resp.leader_id });
                }
            }
            // Progress every 1000 records.
            if ((ctx.stats.write_ok + ctx.stats.write_err) % 1000 == 0) {
                log.info("worker {d}: loaded {d}/{d} records", .{
                    ctx.id, ctx.stats.write_ok + ctx.stats.write_err, end_key - start_key,
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
    if (args.phase == .run or args.phase == .both) {
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
                fillValue(value_buf[0..value_size], key_id);
                const op_start = Timestamp.now(.monotonic);
                sendSet(stream, ns, key, value_buf[0..value_size]) catch {
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

    if (args.phase == .load or args.phase == .both) {
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
        if (args.phase == .both) {
            // Re-aggregate from scratch for run phase reporting below.
            total = .{};
            for (contexts) |*ctx| {
                total.merge(ctx.stats);
            }
        }
    }

    if (args.phase == .run or args.phase == .both) {
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

    const args = parseArgs(argv) catch {
        usage();
        return;
    };

    std.debug.print("zdb-client: addr={s} clients={d} records={d} ops={d} read%={d} value_size={d}\n", .{
        args.addr, args.clients, args.records, args.ops, args.read_pct, args.value_size,
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
    // Fill the shared value pool once, before any fibers spawn — workers then
    // only pay for memcpy, not per-byte PRNG.
    initValueBuffer();
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
