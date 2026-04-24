//! Load-test driver for a local zdb Raft cluster.
//!
//! Subcommands:
//!   run [HOURS=0.1]    Clean → start → bootstrap → monitor → loadgen → stop.
//!   stats [NODE=1]     One-shot stats dump for a node (Prometheus text).
//!   clean              Remove loadtest-* directories and metrics file.

const std = @import("std");
const zio = @import("zio");

// --- config -----------------------------------------------------------------

const Config = struct {
    nodes: u32 = 3,
    namespaces: u32 = 1,
    clients: u32 = 16,
    verify_clients: u32 = 8,
    records: u64 = 100_000_000,
    value_size: u32 = 1024,
    raft_port_base: u16 = 9000,
    client_port_base: u16 = 8000,
    monitor_interval_ms: u64 = 10_000,
    bootstrap_timeout_ms: u64 = 30_000,
    stop_grace_ms: u64 = 2000,
};

const cfg: Config = .{};

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
    return try std.fmt.allocPrint(alloc, "127.0.0.1:{d}", .{cfg.raft_port_base + node});
}

fn clientAddrStr(alloc: std.mem.Allocator, node: u32) ![]u8 {
    return try std.fmt.allocPrint(alloc, "127.0.0.1:{d}", .{cfg.client_port_base + node});
}

// --- TCP protocol helpers (zio) ---------------------------------------------

fn connectNode(node: u32) !zio.net.Stream {
    var buf: [32]u8 = undefined;
    const addr_str = try std.fmt.bufPrint(&buf, "127.0.0.1:{d}", .{cfg.client_port_base + node});
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
    try stream.writeAll("stats\n", .none);

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

    const raft_s = try raftAddrStr(alloc, node);
    defer alloc.free(raft_s);
    const client_s = try clientAddrStr(alloc, node);
    defer alloc.free(client_s);

    // We route the child's stdout+stderr into a per-node log file by launching
    // through `sh -c ... >log 2>&1`. Zig's std.process.Child doesn't expose a
    // first-class way to dup2 a fd into the child's stdout/stderr in 0.15, so
    // a shell wrapper is the simplest portable path. The `exec` prefix means
    // sh replaces itself with the server process — so `child.id` IS the
    // server PID, and SIGTERM to it goes to the server, not a stray sh.
    var cmd_buf: std.ArrayList(u8) = .empty;
    defer cmd_buf.deinit(alloc);
    var w = cmd_buf.writer(alloc);
    try w.print("exec '{s}' --id {d} --cluster-id 1 --raft-addr '{s}' --client-addr '{s}' --data-dir '{s}'", .{ server_bin, node, raft_s, client_s, data_dir });
    if (seeds) |s| try w.print(" --seeds '{s}'", .{s});
    try w.print(" >'{s}' 2>&1", .{log_path});
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

fn stopServer(sp: *ServerProc) void {
    _ = std.posix.kill(sp.child.id, std.posix.SIG.TERM) catch {};
    zio.sleep(zio.Duration.fromMilliseconds(cfg.stop_grace_ms)) catch {};
    _ = sp.child.kill() catch {};
    _ = sp.child.wait() catch {};
    sp.log_file.close();
}

// --- bootstrap ---------------------------------------------------------------

fn bootstrapNamespaces(alloc: std.mem.Allocator, ns_count: u32) !void {
    var leader_node: u32 = 1;
    const deadline = std.time.milliTimestamp() + @as(i64, @intCast(cfg.bootstrap_timeout_ms));

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
        while (node <= cfg.nodes) : (node += 1) {
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
        zio.sleep(zio.Duration.fromMilliseconds(cfg.monitor_interval_ms)) catch break;
    }
}

// --- subcommands ------------------------------------------------------------

fn cmdClean(alloc: std.mem.Allocator) !void {
    const root = try repoRoot(alloc);
    defer alloc.free(root);
    inline for (.{ "loadtest-data", "loadtest-logs", "loadtest-metrics.tsv" }) |name| {
        const p = try std.fs.path.join(alloc, &.{ root, name });
        defer alloc.free(p);
        std.fs.cwd().deleteTree(p) catch {};
    }
    std.log.info("cleaned", .{});
}

fn cmdStats(alloc: std.mem.Allocator, node: u32) !void {
    const blob = try sendStats(alloc, node);
    defer alloc.free(blob);
    const stdout = std.fs.File.stdout();
    _ = try stdout.write(blob);
}

fn cmdRun(alloc: std.mem.Allocator, hours: f64) !void {
    const root = try repoRoot(alloc);
    defer alloc.free(root);
    const data_root = try std.fs.path.join(alloc, &.{ root, "loadtest-data" });
    defer alloc.free(data_root);
    const log_root = try std.fs.path.join(alloc, &.{ root, "loadtest-logs" });
    defer alloc.free(log_root);
    const metrics_path = try std.fs.path.join(alloc, &.{ root, "loadtest-metrics.tsv" });
    defer alloc.free(metrics_path);
    const server_bin = try std.fs.path.join(alloc, &.{ root, "zig-out", "bin", "zdb-server" });
    defer alloc.free(server_bin);
    const bench_bin = try std.fs.path.join(alloc, &.{ root, "zig-out", "bin", "zdb-bench" });
    defer alloc.free(bench_bin);

    try cmdClean(alloc);
    try std.fs.cwd().makePath(data_root);
    try std.fs.cwd().makePath(log_root);

    // --- start N nodes ---
    var servers: [cfg.nodes]ServerProc = undefined;
    const seed_raft = try raftAddrStr(alloc, 1);
    defer alloc.free(seed_raft);

    var started: u32 = 0;
    defer {
        var i: u32 = 0;
        while (i < started) : (i += 1) stopServer(&servers[i]);
    }

    var i: u32 = 0;
    while (i < cfg.nodes) : (i += 1) {
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
    try bootstrapNamespaces(alloc, cfg.namespaces);

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
    const clients_str = try std.fmt.allocPrint(alloc, "{d}", .{cfg.clients});
    defer alloc.free(clients_str);
    const verify_str = try std.fmt.allocPrint(alloc, "{d}", .{cfg.verify_clients});
    defer alloc.free(verify_str);
    const records_str = try std.fmt.allocPrint(alloc, "{d}", .{cfg.records});
    defer alloc.free(records_str);
    const value_size_str = try std.fmt.allocPrint(alloc, "{d}", .{cfg.value_size});
    defer alloc.free(value_size_str);
    const ns_str = try std.fmt.allocPrint(alloc, "{d}", .{cfg.namespaces});
    defer alloc.free(ns_str);

    var bench = std.process.Child.init(&.{
        bench_bin,
        "--addr",           leader_client_addr,
        "--clients",        clients_str,
        "--verify-clients", verify_str,
        "--records",        records_str,
        "--value-size",     value_size_str,
        "--namespaces",     ns_str,
        "--load",
    }, alloc);
    bench.stdout_behavior = .Inherit;
    bench.stderr_behavior = .Inherit;
    try bench.spawn();
    std.log.info("benchmark started pid={d}, duration={d:.2}h", .{ bench.id, hours });

    // --- wait for duration OR bench exit ---
    const duration_ms: u64 = @intFromFloat(hours * 3600.0 * 1000.0);
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

    std.log.info("run complete; metrics at {s}", .{metrics_path});
}

// --- entry ------------------------------------------------------------------

fn usage() void {
    const stderr = std.fs.File.stderr();
    _ = stderr.write(
        \\usage: loadtest <subcommand> [args]
        \\
        \\subcommands:
        \\  run [HOURS=0.1]    Full end-to-end: clean, start cluster, bootstrap,
        \\                     monitor, run benchmark, stop.
        \\  stats [NODE=1]     Dump Prometheus-format stats from a node.
        \\  clean              Remove loadtest data/logs/metrics.
        \\
    ) catch {};
}

// Root task — runs inside the zio runtime as a spawned task, so every
// zio primitive inside the command functions can resolve its executor.
const TaskArgs = struct {
    alloc: std.mem.Allocator,
    mode: enum { run, stats, clean },
    hours: f64 = 0.1,
    node: u32 = 1,
};

fn rootTask(args: TaskArgs) void {
    const result = switch (args.mode) {
        .run => cmdRun(args.alloc, args.hours),
        .stats => cmdStats(args.alloc, args.node),
        .clean => cmdClean(args.alloc),
    };
    result catch |err| std.log.err("{s} failed: {s}", .{ @tagName(args.mode), @errorName(err) });
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const argv = try std.process.argsAlloc(alloc);
    defer std.process.argsFree(alloc, argv);

    if (argv.len < 2) {
        usage();
        std.process.exit(1);
    }

    const cmd = argv[1];
    var task_args: TaskArgs = .{ .alloc = alloc, .mode = .run };
    if (std.mem.eql(u8, cmd, "run")) {
        task_args.mode = .run;
        if (argv.len > 2) task_args.hours = std.fmt.parseFloat(f64, argv[2]) catch 0.1;
    } else if (std.mem.eql(u8, cmd, "stats")) {
        task_args.mode = .stats;
        if (argv.len > 2) task_args.node = std.fmt.parseInt(u32, argv[2], 10) catch 1;
    } else if (std.mem.eql(u8, cmd, "clean")) {
        task_args.mode = .clean;
    } else {
        usage();
        std.process.exit(1);
    }

    // Start the zio runtime, spawn the root task on it, and block main on
    // its join handle. This is what gives every zio.* call inside the
    // command functions a valid executor to dispatch on.
    var rt = try zio.Runtime.init(alloc, .{ .executors = .exact(2) });
    defer rt.deinit();
    var root = try rt.spawn(rootTask, .{task_args});
    _ = root.join();
}
