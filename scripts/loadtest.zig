//! Load-test driver for a local zdb Raft cluster.
//!
//! Subcommand (positional, defaults to `run` if omitted), then flags:
//!
//!   loadtest                                      # run, all defaults
//!   loadtest run --run nodes=1; duration-ms=30m;  # single-node 30-min run
//!   loadtest stats --stats node=2;                # one-shot stats dump
//!   loadtest clean                                # wipe artifacts
//!   loadtest run --ports raft-port-base=9100;     # shared port override
//!
//! Run `loadtest --help` for every flag field with its default and docstring.

const std = @import("std");
const zio = @import("zio");
const flags = @import("flags");

// --- config -----------------------------------------------------------------

// Shared address knobs, used by `run` (to spawn servers) and `stats` (to
// connect). Registered as `--ports ...`.
const PortsCfg = struct {
    raft_port_base: u16 = 9000,
    client_port_base: u16 = 8000,
    pub const docs = .{
        .self = "TCP port bases shared by `run` and `stats`.",
        .raft_port_base = "Node i listens for raft peers on 127.0.0.1:(base+i).",
        .client_port_base = "Node i listens for clients/stats on 127.0.0.1:(base+i).",
    };
};

// Tunables for the main `run` subcommand.
const RunCfg = struct {
    nodes: u32 = 3,
    namespaces: u32 = 1,
    clients: u32 = 16,
    verify_clients: u32 = 0, // disabled for raw Raft throughput measurement
    records: u64 = 100_000_000,
    value_size: u32 = 1024,
    /// Writes per Raft proposal. Amortizes commit cost across N ops.
    batch_size: u32 = 256,
    /// Wall-clock budget for the run. `_ms` suffix → accepts `30s`, `5m`, etc.
    duration_ms: u64 = 360_000, // 6 minutes
    monitor_interval_ms: u64 = 10_000,
    bootstrap_timeout_ms: u64 = 30_000,
    stop_grace_ms: u64 = 10000,
    /// SIGKILL any stale `zdb-server` / `zdb-bench` processes before starting.
    /// Useful between iterative runs where a previous server crashed without
    /// releasing its raft port — saves the user from running pkill manually.
    kill_existing: bool = false,
    /// Where to put `data/`, `logs/`, and `metrics.tsv`. Empty string ⇒
    /// repo root (legacy default). Use this to point to a larger
    /// filesystem (e.g. /data/egres).
    workdir: []const u8 = "",

    pub const docs = .{
        .self = "Full benchmark run: clean → start cluster → bootstrap → loadgen → stop.",
        .nodes = "Number of Raft cluster nodes to spawn (set to 1 for leader-only bench).",
        .namespaces = "How many namespaces the bootstrap step creates.",
        .clients = "Benchmark writer fiber count.",
        .verify_clients = "Verifier fiber count (0 disables; non-zero slows writers).",
        .records = "Total records the benchmark should insert.",
        .value_size = "Bytes per value in generated load.",
        .batch_size = "Entries per set-batch proposal (higher = more amortization).",
        .duration_ms = "Wall-clock budget for the run (accepts 30s / 5m / 2h / bare ms).",
        .monitor_interval_ms = "Sampling interval for the /stats monitor fiber.",
        .bootstrap_timeout_ms = "Giveup budget for the namespace-create bootstrap loop.",
        .stop_grace_ms = "SIGTERM-to-SIGKILL grace window when tearing servers down.",
        .kill_existing = "SIGKILL any stale zdb-server/zdb-bench processes before starting (best-effort).",
        .workdir = "Directory holding data/, logs/, metrics.tsv. Empty = repo root.",
    };
};

// Tunables for the `stats` subcommand (just which node to query).
const StatsCfg = struct {
    node: u32 = 1,
    pub const docs = .{
        .self = "Prometheus-format stats dump from a single node.",
        .node = "Which node to query.",
    };
};

// Module globals, populated from `live.snapshot()` in `main()` after CLI
// parsing. Each helper reads `cfg_<module>.<field>` — this is a single-shot
// CLI tool so a snapshot-at-startup is sufficient.
var cfg_ports: PortsCfg = .{};
var cfg_run: RunCfg = .{};
var cfg_stats: StatsCfg = .{};

// --- path helpers -----------------------------------------------------------

fn repoRoot(alloc: std.mem.Allocator) ![]u8 {
    // Driver sits at <repo>/zig-out/bin/loadtest; walk up two dirs.
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const exe = try std.fs.selfExePath(&buf);
    const bin_dir = std.fs.path.dirname(exe) orelse return error.BadExePath;
    const zig_out = std.fs.path.dirname(bin_dir) orelse return error.BadExePath;
    const root = std.fs.path.dirname(zig_out) orelse return error.BadExePath;
    return try alloc.dupe(u8, root);
}

fn raftAddrStr(alloc: std.mem.Allocator, node: u32) ![]u8 {
    return try std.fmt.allocPrint(alloc, "127.0.0.1:{d}", .{cfg_ports.raft_port_base + node});
}

fn clientAddrStr(alloc: std.mem.Allocator, node: u32) ![]u8 {
    return try std.fmt.allocPrint(alloc, "127.0.0.1:{d}", .{cfg_ports.client_port_base + node});
}

// --- TCP protocol helpers (zio) ---------------------------------------------

fn connectNode(node: u32) !zio.net.Stream {
    var buf: [32]u8 = undefined;
    const addr_str = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{cfg_ports.client_port_base + node});
    const addr = try zio.net.IpAddress.parseIpAndPort(addr_str);
    return try addr.connect(.{});
}

/// Read up to one '\n' into `buf`, returning the line (without the newline).
/// Byte-at-a-time; fine for low-frequency protocol lines.
fn readLine(stream: zio.net.Stream, buf: []u8) ![]const u8 {
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

/// Send a single command, read one line of response. Caller owns the result.
fn sendLineRecvLine(alloc: std.mem.Allocator, node: u32, cmd: []const u8) ![]u8 {
    const stream = try connectNode(node);
    defer stream.close();
    try stream.writeAll(cmd, .none);
    var buf: [1024]u8 = undefined;
    const line = try readLine(stream, &buf);
    return try alloc.dupe(u8, line);
}

/// Send `stats`, read until a blank-line terminator. Caller owns buffer.
fn sendStats(alloc: std.mem.Allocator, node: u32) ![]u8 {
    const stream = try connectNode(node);
    defer stream.close();
    try stream.writeAll("zdb\n", .none);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(alloc);
    var line_buf: [4096]u8 = undefined;
    while (true) {
        const line = readLine(stream, &line_buf) catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        if (line.len == 0) break; // blank line terminator
        try out.appendSlice(alloc, line);
        try out.append(alloc, '\n');
    }
    return try out.toOwnedSlice(alloc);
}

/// Sum the values of all Prometheus metric lines whose name begins with
/// `prefix` and whose text contains `label_contains` (if provided). Good
/// enough for flat sums — no proper label parser required.
fn sumMetric(blob: []const u8, prefix: []const u8, label_contains: ?[]const u8) u64 {
    var sum: u64 = 0;
    var it = std.mem.splitScalar(u8, blob, '\n');
    while (it.next()) |line| {
        if (!std.mem.startsWith(u8, line, prefix)) continue;
        // Reject prefix-extensions: line[prefix.len] must be `{` or ` `.
        if (line.len <= prefix.len) continue;
        const c = line[prefix.len];
        if (c != '{' and c != ' ') continue;
        if (label_contains) |needle| {
            if (std.mem.indexOf(u8, line, needle) == null) continue;
        }
        const last_space = std.mem.lastIndexOfScalar(u8, line, ' ') orelse continue;
        const val_str = line[last_space + 1 ..];
        const v = std.fmt.parseInt(u64, val_str, 10) catch continue;
        sum += v;
    }
    return sum;
}

// --- filesystem helpers -----------------------------------------------------

fn dirSizeBytes(alloc: std.mem.Allocator, path: []const u8) !u64 {
    var dir = std.fs.cwd().openDir(path, .{ .iterate = true }) catch return 0;
    defer dir.close();
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    var total: u64 = 0;
    while (try walker.next()) |entry| {
        if (entry.kind != .file) continue;
        const stat = entry.dir.statFile(entry.basename) catch continue;
        total += stat.size;
    }
    return total;
}

// --- process management (std, not zio) -------------------------------------

const ServerProc = struct {
    node: u32,
    child: std.process.Child,
    log_file: std.fs.File,
};

fn spawnServer(
    alloc: std.mem.Allocator,
    server_bin: []const u8,
    data_dir: []const u8,
    log_path: []const u8,
    node: u32,
    seeds: ?[]const u8,
) !ServerProc {
    try std.fs.cwd().makePath(data_dir);
    const log_file = try std.fs.cwd().createFile(log_path, .{ .truncate = true });

    const client_s = try clientAddrStr(alloc, node);
    defer alloc.free(client_s);

    // Route the child's stdout+stderr into a per-node log file by launching
    // through `sh -c ... >log 2>&1`. Zig's std.process.Child doesn't expose
    // a first-class way to dup2 a fd into the child's stdout/stderr in
    // 0.15, so a shell wrapper is the simplest portable path. The `exec`
    // prefix means sh replaces itself with the server process — so
    // `child.id` IS the server PID, and SIGTERM to it goes to the server,
    // not a stray sh. We use the canonical flags.zig form
    // `--<module>="k=v; k=v"` — one quoted payload per registered config.
    // `offset=node` keeps each server's raft/client ports distinct without
    // duplicating the per-port arithmetic on this side.
    var cmd_buf: std.ArrayList(u8) = .empty;
    defer cmd_buf.deinit(alloc);
    var w = cmd_buf.writer(alloc);
    // flags.zig renders field names as kebab-case in the help output and
    // accepts the same form in payloads (`raft_port` → `raft-port`,
    // `cluster_id` → `cluster-id`, etc). Match that here.
    try w.print(
        "exec '{s}' --listen=\"raft-port={d}; client-port={d}; offset={d}\" --raft=\"id={d}; cluster-id=1; tick-ms=20",
        .{ server_bin, cfg_ports.raft_port_base, cfg_ports.client_port_base, node, node },
    );
    if (seeds) |s| try w.print("; seeds={s}", .{s});
    try w.print("\" --data=\"dir={s}\" >'{s}' 2>&1", .{ data_dir, log_path });
    try cmd_buf.append(alloc, 0); // null-terminate for sh

    var child = std.process.Child.init(
        &.{ "/bin/sh", "-c", cmd_buf.items[0 .. cmd_buf.items.len - 1] },
        alloc,
    );
    child.stdin_behavior = .Ignore;
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    try child.spawn();
    std.log.info("node {d} started pid={d} client={s}", .{ node, child.id, client_s });
    return .{ .node = node, .child = child, .log_file = log_file };
}

/// Best-effort SIGKILL for any stale `zdb-server` / `zdb-bench` processes.
/// Useful when a previous run crashed and left a server bound to the raft
/// port. We use `pkill -9 -f` rather than tracking PIDs ourselves because
/// the stragglers are by definition not children of this process.
///
/// pkill exits 0 on at least one match, 1 on no match — both are fine
/// outcomes, so we discard the exit code. A 500ms pause after lets
/// SIGKILL actually take effect and ports release before we try to bind.
fn killStaleServers(alloc: std.mem.Allocator) !void {
    inline for (.{ "zdb-server", "zdb-bench" }) |name| {
        var killer = std.process.Child.init(&.{ "pkill", "-9", "-f", name }, alloc);
        killer.stdin_behavior = .Ignore;
        killer.stdout_behavior = .Ignore;
        killer.stderr_behavior = .Ignore;
        _ = killer.spawnAndWait() catch {};
        std.log.info("killed any stale {s} processes", .{name});
    }
    try zio.sleep(zio.Duration.fromMilliseconds(500));
}

fn stopServer(sp: *ServerProc) void {
    _ = std.posix.kill(sp.child.id, std.posix.SIG.TERM) catch {};
    zio.sleep(zio.Duration.fromMilliseconds(cfg_run.stop_grace_ms)) catch {};
    _ = sp.child.kill() catch {};
    _ = sp.child.wait() catch {};
    sp.log_file.close();
}

// --- bootstrap ---------------------------------------------------------------

/// Promote every non-bootstrap node from learner → voter. Joiners (nodes
/// 2..N) come up as learners by default; without this step they never
/// participate in quorum and the leader's `quorum_size` stays at 1.
fn promoteLearnersToVoters(alloc: std.mem.Allocator) !void {
    var leader_node: u32 = 1;
    var node: u32 = 2;
    while (node <= cfg_run.nodes) : (node += 1) {
        const deadline = std.time.milliTimestamp() + @as(i64, @intCast(cfg_run.bootstrap_timeout_ms));
        while (true) {
            if (std.time.milliTimestamp() > deadline) {
                std.log.err("promote timeout for node {d}", .{node});
                return error.PromoteTimeout;
            }
            const cmd = try std.fmt.allocPrint(alloc, "conf to_voter {d}\n", .{node});
            defer alloc.free(cmd);
            const resp = sendLineRecvLine(alloc, leader_node, cmd) catch {
                try zio.sleep(zio.Duration.fromMilliseconds(500));
                continue;
            };
            defer alloc.free(resp);

            if (std.mem.startsWith(u8, resp, "OK")) {
                std.log.info("promoted node {d} to voter via leader {d}", .{ node, leader_node });
                break;
            }
            if (std.mem.startsWith(u8, resp, "NOT_LEADER ")) {
                const id_str = resp["NOT_LEADER ".len..];
                const leader_id = std.fmt.parseInt(u32, id_str, 10) catch 0;
                if (leader_id != 0 and leader_id != leader_node) {
                    leader_node = leader_id;
                }
                try zio.sleep(zio.Duration.fromMilliseconds(200));
                continue;
            }
            try zio.sleep(zio.Duration.fromMilliseconds(500));
        }
    }
}

fn bootstrapNamespaces(alloc: std.mem.Allocator, ns_count: u32) !void {
    var leader_node: u32 = 1;
    const deadline = std.time.milliTimestamp() + @as(i64, @intCast(cfg_run.bootstrap_timeout_ms));

    var ns: u32 = 1;
    while (ns <= ns_count) : (ns += 1) {
        while (true) {
            if (std.time.milliTimestamp() > deadline) {
                std.log.err("bootstrap timeout creating ns {d}", .{ns});
                return error.BootstrapTimeout;
            }
            const cmd = try std.fmt.allocPrint(alloc, "ns create {d}\n", .{ns});
            defer alloc.free(cmd);
            const resp = sendLineRecvLine(alloc, leader_node, cmd) catch {
                try zio.sleep(zio.Duration.fromMilliseconds(500));
                continue;
            };
            defer alloc.free(resp);

            if (std.mem.startsWith(u8, resp, "OK")) {
                std.log.info("namespace {d} created on node {d}", .{ ns, leader_node });
                break;
            }
            if (std.mem.startsWith(u8, resp, "NOT_LEADER ")) {
                const id_str = resp["NOT_LEADER ".len..];
                const leader_id = std.fmt.parseInt(u32, id_str, 10) catch 0;
                if (leader_id != 0 and leader_id != leader_node) {
                    leader_node = leader_id;
                    std.log.info("redirected to leader {d}", .{leader_node});
                }
                try zio.sleep(zio.Duration.fromMilliseconds(200));
                continue;
            }
            try zio.sleep(zio.Duration.fromMilliseconds(500));
        }
    }
}

// --- monitor (zio fiber) ----------------------------------------------------

const MonitorCtx = struct {
    alloc: std.mem.Allocator,
    data_dir_root: []const u8,
    metrics_path: []const u8,
    stop: std.atomic.Value(bool),
};

fn monitorFiber(ctx: *MonitorCtx) void {
    const metrics_file = std.fs.cwd().createFile(ctx.metrics_path, .{ .truncate = true }) catch |err| {
        std.log.err("monitor: can't create metrics file: {s}", .{@errorName(err)});
        return;
    };
    defer metrics_file.close();
    _ = metrics_file.write(
        "ts_unix\tnode\tflushes\tcompactions\tuser_bytes\tflush_bytes\tcompact_bytes\tdisk_bytes\n",
    ) catch {};

    while (!ctx.stop.load(.acquire)) {
        const now_s = std.time.timestamp();
        var node: u32 = 1;
        while (node <= cfg_run.nodes) : (node += 1) {
            const blob = sendStats(ctx.alloc, node) catch continue;
            defer ctx.alloc.free(blob);

            const flushes = sumMetric(blob, "zdb_flushes_total", null);
            const compactions = sumMetric(blob, "zdb_compactions_total", null);
            const user_b = sumMetric(blob, "zdb_bytes_written_total", "source=\"user\"");
            const flush_b = sumMetric(blob, "zdb_bytes_written_total", "source=\"flush\"");
            const compact_b = sumMetric(blob, "zdb_bytes_written_total", "source=\"compact\"");

            const data_dir = std.fmt.allocPrint(
                ctx.alloc,
                "{s}/node{d}",
                .{ ctx.data_dir_root, node },
            ) catch continue;
            defer ctx.alloc.free(data_dir);
            const disk_b = dirSizeBytes(ctx.alloc, data_dir) catch 0;

            var line_buf: [256]u8 = undefined;
            const line = std.fmt.bufPrint(&line_buf, "{d}\t{d}\t{d}\t{d}\t{d}\t{d}\t{d}\t{d}\n", .{
                now_s, node, flushes, compactions, user_b, flush_b, compact_b, disk_b,
            }) catch continue;
            _ = metrics_file.write(line) catch {};
        }
        zio.sleep(zio.Duration.fromMilliseconds(cfg_run.monitor_interval_ms)) catch break;
    }
}

// --- subcommands ------------------------------------------------------------

fn cleanWorkdir(alloc: std.mem.Allocator, work_root: []const u8, keep_logs: bool) !void {
    const always = [_][]const u8{ "data", "metrics.tsv" };
    for (always) |name| {
        const p = try std.fs.path.join(alloc, &.{ work_root, name });
        defer alloc.free(p);
        std.fs.cwd().deleteTree(p) catch {};
    }
    if (!keep_logs) {
        const p = try std.fs.path.join(alloc, &.{ work_root, "logs" });
        defer alloc.free(p);
        std.fs.cwd().deleteTree(p) catch {};
    }
    std.log.info("cleaned {s} (keep_logs={})", .{ work_root, keep_logs });
}

fn cmdClean(alloc: std.mem.Allocator) !void {
    // `loadtest clean` honors the same --run="workdir=..." override. If
    // unset, falls back to repo root (legacy behavior).
    const root = try repoRoot(alloc);
    defer alloc.free(root);
    const work_root = if (cfg_run.workdir.len > 0) cfg_run.workdir else root;
    return cleanWorkdir(alloc, work_root, false);
}

fn cmdStats(alloc: std.mem.Allocator, node: u32) !void {
    const blob = try sendStats(alloc, node);
    defer alloc.free(blob);
    const stdout = std.fs.File.stdout();
    _ = try stdout.write(blob);
}

fn cmdRun(alloc: std.mem.Allocator) !void {
    const root = try repoRoot(alloc);
    defer alloc.free(root);
    // Server/bench binaries always live in the repo's zig-out/bin; only
    // the writable workdir (data + logs + metrics) is configurable.
    const work_root = if (cfg_run.workdir.len > 0)
        try alloc.dupe(u8, cfg_run.workdir)
    else
        try alloc.dupe(u8, root);
    defer alloc.free(work_root);
    const data_root = try std.fs.path.join(alloc, &.{ work_root, "data" });
    defer alloc.free(data_root);
    const log_root = try std.fs.path.join(alloc, &.{ work_root, "logs" });
    defer alloc.free(log_root);
    const metrics_path = try std.fs.path.join(alloc, &.{ work_root, "metrics.tsv" });
    defer alloc.free(metrics_path);
    const server_bin = try std.fs.path.join(alloc, &.{ root, "zig-out", "bin", "zdb-server" });
    defer alloc.free(server_bin);
    const bench_bin = try std.fs.path.join(alloc, &.{ root, "zig-out", "bin", "zdb-bench" });
    defer alloc.free(bench_bin);

    if (cfg_run.kill_existing) try killStaleServers(alloc);

    // Start every run from a clean log directory so node logs and final
    // stats files always belong to just this run.
    try cleanWorkdir(alloc, work_root, false);
    try std.fs.cwd().makePath(data_root);
    try std.fs.cwd().makePath(log_root);

    // --- start N nodes ---
    // Heap-allocated — `cfg_run.nodes` is runtime, set via `--loadtest nodes=N;`,
    // so a fixed-size array won't do. `started` tracks how many were actually
    // spawned; the defer tears down exactly that prefix if we bail partway.
    const servers = try alloc.alloc(ServerProc, cfg_run.nodes);
    defer alloc.free(servers);
    const seed_raft = try raftAddrStr(alloc, 1);
    defer alloc.free(seed_raft);

    var started: u32 = 0;
    defer {
        var i: u32 = 0;
        while (i < started) : (i += 1) stopServer(&servers[i]);
    }

    var i: u32 = 0;
    while (i < cfg_run.nodes) : (i += 1) {
        const node = i + 1;
        const data_dir = try std.fmt.allocPrint(alloc, "{s}/node{d}", .{ data_root, node });
        defer alloc.free(data_dir);
        const log_path = try std.fmt.allocPrint(alloc, "{s}/node{d}.log", .{ log_root, node });
        defer alloc.free(log_path);
        const seeds: ?[]const u8 = if (node == 1) null else seed_raft;
        servers[i] = try spawnServer(alloc, server_bin, data_dir, log_path, node, seeds);
        started = i + 1;
        if (node == 1) try zio.sleep(zio.Duration.fromSeconds(1));
    }

    // --- let Raft elect, then bootstrap ---
    try zio.sleep(zio.Duration.fromSeconds(1));
    // Promote joining nodes from learner → voter so we run as a real
    // multi-voter cluster. Skipped automatically when nodes == 1.
    try promoteLearnersToVoters(alloc);
    try bootstrapNamespaces(alloc, cfg_run.namespaces);

    // --- monitor fiber ---
    var mon_ctx: MonitorCtx = .{
        .alloc = alloc,
        .data_dir_root = data_root,
        .metrics_path = metrics_path,
        .stop = std.atomic.Value(bool).init(false),
    };
    var mon_handle = try zio.spawn(monitorFiber, .{&mon_ctx});

    // --- load generator (child process) ---
    const leader_client_addr = try clientAddrStr(alloc, 1);
    defer alloc.free(leader_client_addr);
    // zdb-bench parses one quoted `--bench="k=v; k=v"` payload per the
    // flags.zig convention. Field names are kebab-case (read-pct,
    // verify-clients, value-size, batch-size).
    const bench_payload = try std.fmt.allocPrint(
        alloc,
        "addr={s}; clients={d}; verify-clients={d}; records={d}; value-size={d}; namespaces={d}; batch-size={d}; phase=load",
        .{
            leader_client_addr,
            cfg_run.clients,
            cfg_run.verify_clients,
            cfg_run.records,
            cfg_run.value_size,
            cfg_run.namespaces,
            cfg_run.batch_size,
        },
    );
    defer alloc.free(bench_payload);
    const bench_flag = try std.fmt.allocPrint(alloc, "--bench={s}", .{bench_payload});
    defer alloc.free(bench_flag);

    var bench = std.process.Child.init(&.{
        bench_bin,
        bench_flag,
    }, alloc);
    bench.stdout_behavior = .Inherit;
    bench.stderr_behavior = .Inherit;
    try bench.spawn();
    std.log.info("benchmark started pid={d}, duration_ms={d}", .{ bench.id, cfg_run.duration_ms });

    // --- wait for duration OR bench exit ---
    const duration_ms: u64 = cfg_run.duration_ms;
    const start_ms = std.time.milliTimestamp();
    while (true) {
        // Poll for bench exit via waitpid(WNOHANG).
        const w = std.posix.waitpid(bench.id, std.posix.W.NOHANG);
        if (w.pid == bench.id) break; // bench exited on its own
        if (std.time.milliTimestamp() - start_ms >= @as(i64, @intCast(duration_ms))) {
            std.log.info("duration reached, stopping bench", .{});
            _ = std.posix.kill(bench.id, std.posix.SIG.TERM) catch {};
            _ = bench.wait() catch {};
            break;
        }
        try zio.sleep(zio.Duration.fromSeconds(1));
    }

    // --- stop monitor, servers (via defer) ---
    mon_ctx.stop.store(true, .release);
    _ = mon_handle.join();

    // Dump a final /stats blob from each node so the histograms (which the
    // tsv monitor doesn't track) are visible post-run. Written to disk
    // alongside the per-node logs; printed file paths in the summary log.
    var sn: u32 = 1;
    while (sn <= cfg_run.nodes) : (sn += 1) {
        const blob = sendStats(alloc, sn) catch continue;
        defer alloc.free(blob);
        const path = std.fmt.allocPrint(alloc, "{s}/stats-node{d}.txt", .{ log_root, sn }) catch continue;
        defer alloc.free(path);
        const f = std.fs.cwd().createFile(path, .{ .truncate = true }) catch continue;
        defer f.close();
        _ = f.write(blob) catch {};
        std.log.info("final stats dumped: {s}", .{path});
    }

    std.log.info("run complete; metrics at {s}", .{metrics_path});
}

// --- entry ------------------------------------------------------------------

const Mode = enum { run, stats, clean };

fn parseSubcommand(arg: []const u8) ?Mode {
    if (std.mem.eql(u8, arg, "run")) return .run;
    if (std.mem.eql(u8, arg, "stats")) return .stats;
    if (std.mem.eql(u8, arg, "clean")) return .clean;
    return null;
}

fn usage(alloc: std.mem.Allocator) void {
    const stderr = std.fs.File.stderr();
    _ = stderr.write(
        \\usage: loadtest [subcommand] [--<module> k=v; k=v; ...]
        \\
        \\subcommands (positional, defaults to `run` if omitted):
        \\  run        Full end-to-end: clean, start cluster, bootstrap, loadgen, stop.
        \\  stats      Dump Prometheus-format stats from one node.
        \\  clean      Remove loadtest data / logs / metrics.
        \\
        \\Flag modules (pass as `--<module> <field>=<value>; ...`):
        \\
    ) catch {};

    // Registry-rendered help: module name on one line, then one
    // `<field>=<default> (<type>); <doc>` line per field.
    var help: std.ArrayList(u8) = .empty;
    defer help.deinit(alloc);
    flags.g().writeHelpAll(&help) catch {};
    _ = stderr.write(help.items) catch {};
    _ = stderr.write("\n") catch {};
}

// Root task — runs inside the zio runtime as a spawned task, so every
// zio primitive inside the command functions can resolve its executor.
fn rootTask(alloc: std.mem.Allocator, mode: Mode) void {
    const result = switch (mode) {
        .run => cmdRun(alloc),
        .stats => cmdStats(alloc, cfg_stats.node),
        .clean => cmdClean(alloc),
    };
    result catch |err| std.log.err("{s} failed: {s}", .{ @tagName(mode), @errorName(err) });
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const argv = try std.process.argsAlloc(alloc);
    defer std.process.argsFree(alloc, argv);

    // Bootstrap fiber: flag init and the rest of setup runs in fiber
    // context so flag locks aren't blocking an executor thread.
    const bootstrap_rt = try zio.Runtime.init(alloc, .{ .executors = .exact(1) });
    defer bootstrap_rt.deinit();
    var handle = try bootstrap_rt.spawn(bootstrap, .{ alloc, argv });
    try handle.join();
}

fn bootstrap(alloc: std.mem.Allocator, argv: [][:0]u8) !void {
    // --- step 1: extract the positional subcommand (argv[1] if present and
    // doesn't start with `--`). Anything else goes to the flag parser. This
    // keeps the verb natural (`loadtest run`) without needing flag gymnastics
    // like `--clean=` for no-arg subcommands.
    var mode: Mode = .run;
    var flag_start: usize = 1;
    if (argv.len >= 2 and !std.mem.startsWith(u8, argv[1], "--")) {
        mode = parseSubcommand(argv[1]) orelse {
            std.log.err("unknown subcommand '{s}'", .{argv[1]});
            // usage() needs the registry populated for writeHelpAll; do it
            // after registration below. Here we just exit.
            std.process.exit(1);
        };
        flag_start = 2;
    }

    // --- step 2: register flag modules and parse argv[flag_start..].
    flags.initGlobal(alloc);
    defer flags.deinitGlobal();
    const live_ports = try flags.g().register("ports", PortsCfg);
    const live_run = try flags.g().register("run", RunCfg);
    const live_stats = try flags.g().register("stats", StatsCfg);

    // --help is terminal; handle it after registration so the rendered help
    // includes every module's fields.
    for (argv[1..]) |a| {
        if (std.mem.eql(u8, a, "--help") or std.mem.eql(u8, a, "-h")) {
            usage(alloc);
            return;
        }
    }

    // No positional args beyond the subcommand — any leftover from parseArgv
    // is a user error.
    var leftover: std.ArrayList([]const u8) = .empty;
    defer leftover.deinit(alloc);
    flags.g().parseArgv(argv[flag_start..], &leftover) catch |err| {
        std.log.err("flag parse failed: {s}", .{@errorName(err)});
        usage(alloc);
        std.process.exit(1);
    };
    if (leftover.items.len != 0) {
        std.log.err("unexpected positional '{s}' — config goes through --<module> flags", .{leftover.items[0]});
        usage(alloc);
        std.process.exit(1);
    }

    cfg_ports = live_ports.snapshot();
    cfg_run = live_run.snapshot();
    cfg_stats = live_stats.snapshot();

    // --- step 3: spawn the root task on the zio runtime. Every zio.* call
    // inside the command functions resolves its executor from here.
    var rt = try zio.Runtime.init(alloc, .{ .executors = .exact(2) });
    defer rt.deinit();
    var root = try rt.spawn(rootTask, .{ alloc, mode });
    _ = root.join();
}
