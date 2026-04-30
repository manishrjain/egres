// DiskWAL micro-benchmark: measures how fast we can append + (optionally)
// fsync records through the raw `raft.diskwal` layer, bypassing Raft itself.
// This isolates the WAL from everything above it (propose queue, quorum,
// apply loop) so we can tell whether the end-to-end ceiling we see in the
// zdb-server load test is the WAL or something upstream.
//
// Run:   zig build bench-diskwal -Doptimize=ReleaseFast -- [flags]
// Flags (all optional — omit to run the full matrix):
//   --dir <path>        scratch WAL directory (default /tmp/diskwal-bench)
//   --payload <bytes>   entry payload size (default: matrix mode)
//   --batch <count>     entries per append call (default: matrix mode)
//   --fsync <mode>      never | per-batch | per-append (default: matrix mode)
//   --duration <sec>    per-cell duration (default 3s)

const std = @import("std");
const zio = @import("zio");
const raft = @import("raft");
const vfs = @import("vfs");

const zen = raft.buffers;
const diskwal = raft.diskwal;

const Allocator = std.mem.Allocator;
const Instant = std.time.Instant;

const FsyncMode = enum {
    never,
    per_batch,
    per_append,

    fn label(self: FsyncMode) []const u8 {
        return switch (self) {
            .never => "never",
            .per_batch => "per-batch",
            .per_append => "per-append",
        };
    }
};

const Args = struct {
    dir: []const u8 = "/tmp/diskwal-bench",
    payload_size: ?usize = null,
    batch_size: ?u32 = null,
    fsync_mode: ?FsyncMode = null,
    duration_sec: u32 = 3,
};

const Result = struct {
    bytes: u64,
    entries: u64,
    batches: u64,
    secs: f64,
    p50_us: u64,
    p99_us: u64,
    max_us: u64,
};

fn parseArgs(argv: []const [:0]u8) !Args {
    var args: Args = .{};
    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const a = argv[i];
        if (std.mem.eql(u8, a, "--dir")) {
            i += 1;
            args.dir = argv[i];
        } else if (std.mem.eql(u8, a, "--payload")) {
            i += 1;
            args.payload_size = try std.fmt.parseInt(usize, argv[i], 10);
        } else if (std.mem.eql(u8, a, "--batch")) {
            i += 1;
            args.batch_size = try std.fmt.parseInt(u32, argv[i], 10);
        } else if (std.mem.eql(u8, a, "--fsync")) {
            i += 1;
            args.fsync_mode = if (std.mem.eql(u8, argv[i], "never"))
                .never
            else if (std.mem.eql(u8, argv[i], "per-batch"))
                .per_batch
            else if (std.mem.eql(u8, argv[i], "per-append"))
                .per_append
            else
                return error.BadFsyncMode;
        } else if (std.mem.eql(u8, a, "--duration")) {
            i += 1;
            args.duration_sec = try std.fmt.parseInt(u32, argv[i], 10);
        } else {
            std.debug.print("unknown flag: {s}\n", .{a});
            return error.BadArg;
        }
    }
    return args;
}

/// Run one cell of the matrix: fixed payload, batch, fsync. Re-creates the
/// dir from scratch so every cell starts with a fresh WAL — otherwise a
/// later cell inherits the previous cell's rotation state.
fn runOne(
    alloc: Allocator,
    dir: []const u8,
    payload_size: usize,
    batch_size: u32,
    fsync_mode: FsyncMode,
    duration_sec: u32,
) !Result {
    std.fs.cwd().deleteTree(dir) catch {};
    try std.fs.cwd().makePath(dir);

    var dwal = try diskwal.DiskWAL.openPath(dir, alloc, 0);
    defer dwal.deinit();

    const payload = try alloc.alloc(u8, payload_size);
    defer alloc.free(payload);
    // Fill with a non-trivial pattern so the page cache / filesystem can't
    // dedupe or compress it away.
    for (payload, 0..) |*b, idx| b.* = @truncate(idx ^ 0xA5);

    // Reusable per-batch scratch. We rebuild entries every iteration (indexes
    // must be monotonic), but the ArrayList + the `payload` slice stay put.
    var batch: std.ArrayList(zen.LogEntryBuffer) = .empty;
    defer {
        for (batch.items) |*e| e.deinit();
        batch.deinit(alloc);
    }
    try batch.ensureTotalCapacity(alloc, batch_size);

    // Latency sampling — per-append for per_append mode, per-batch otherwise.
    // Capped at 1M samples to keep memory bounded on high-rate cells.
    var latencies_us: std.ArrayList(u64) = .empty;
    defer latencies_us.deinit(alloc);
    try latencies_us.ensureTotalCapacity(alloc, 1_000_000);

    const duration_ns: u64 = @as(u64, duration_sec) * 1_000_000_000;
    var next_index: u64 = 1;
    var total_bytes: u64 = 0;
    var total_entries: u64 = 0;
    var total_batches: u64 = 0;

    const t_start = try Instant.now();
    while (true) {
        if ((try Instant.now()).since(t_start) >= duration_ns) break;

        // Rebuild the batch — each LogEntryBuffer is owned by us until we
        // hand it to the WAL (which copies the bytes out).
        for (batch.items) |*e| e.deinit();
        batch.clearRetainingCapacity();
        var k: u32 = 0;
        while (k < batch_size) : (k += 1) {
            var writer = try zen.LogEntryWriter.init(alloc);
            defer writer.deinit();
            try writer.set(.{
                .index = next_index,
                .term = 1,
                .timestamp = 0,
                .data = payload,
                .entry_type = zen.EntryTypeEnum.data,
                .conf_change = zen.NullOffset,
            });
            batch.appendAssumeCapacity(try writer.finishWithTail());
            next_index += 1;
        }

        switch (fsync_mode) {
            .per_append => {
                // Append + fsync each entry individually — models the worst
                // case where every record waits on disk.
                for (batch.items) |*e| {
                    const t_op = try Instant.now();
                    try dwal.wal_store.addEntries(&.{e.*});
                    try dwal.wal_store.sync(true);
                    recordLatency(&latencies_us, (try Instant.now()).since(t_op));
                }
            },
            .per_batch, .never => {
                const t_op = try Instant.now();
                try dwal.wal_store.addEntries(batch.items);
                if (fsync_mode == .per_batch) try dwal.wal_store.sync(true);
                recordLatency(&latencies_us, (try Instant.now()).since(t_op));
            },
        }

        total_bytes += payload_size * batch_size;
        total_entries += batch_size;
        total_batches += 1;
    }

    const elapsed_ns = (try Instant.now()).since(t_start);
    const secs = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;

    std.mem.sort(u64, latencies_us.items, {}, comptime std.sort.asc(u64));
    const n = latencies_us.items.len;
    return .{
        .bytes = total_bytes,
        .entries = total_entries,
        .batches = total_batches,
        .secs = secs,
        .p50_us = if (n > 0) latencies_us.items[n / 2] else 0,
        .p99_us = if (n > 0) latencies_us.items[(n * 99) / 100] else 0,
        .max_us = if (n > 0) latencies_us.items[n - 1] else 0,
    };
}

fn recordLatency(list: *std.ArrayList(u64), op_ns: u64) void {
    if (list.items.len < list.capacity) {
        list.appendAssumeCapacity(op_ns / 1000);
    }
}

fn printHeader() void {
    std.debug.print(
        \\  payload  batch  fsync        MiB/s   entries/s   p50_us   p99_us   max_us
        \\  -------  -----  ----------  ------  ----------  -------  -------  -------
        \\
    , .{});
}

fn printRow(payload: usize, batch: u32, fsync_mode: FsyncMode, r: Result) void {
    const mib_s = @as(f64, @floatFromInt(r.bytes)) / r.secs / (1024.0 * 1024.0);
    const ent_s = @as(f64, @floatFromInt(r.entries)) / r.secs;
    std.debug.print(
        "  {d:>7}  {d:>5}  {s:<10}  {d:>6.1}  {d:>10.0}  {d:>7}  {d:>7}  {d:>7}\n",
        .{ payload, batch, fsync_mode.label(), mib_s, ent_s, r.p50_us, r.p99_us, r.max_us },
    );
}

fn runMatrix(alloc: Allocator, args: Args) !void {
    const payloads = [_]usize{ 128, 1024, 4 * 1024, 16 * 1024 };
    const batches = [_]u32{ 1, 16, 256 };
    // per_append is informational but very slow; skip in the default matrix.
    const fsyncs = [_]FsyncMode{ .never, .per_batch };

    std.debug.print(
        "\nDiskWAL bench: {d}s per cell, dir={s}\n\n",
        .{ args.duration_sec, args.dir },
    );
    printHeader();
    for (payloads) |p| {
        for (batches) |b| {
            for (fsyncs) |f| {
                const r = try runOne(alloc, args.dir, p, b, f, args.duration_sec);
                printRow(p, b, f, r);
            }
        }
    }
}

fn rootTask(args: Args) !void {
    // VFS uses a global bandwidth limiter; set it high enough that our
    // benchmark never hits the cap (we want to measure the WAL, not the
    // throttle).
    vfs.VFS.initGlobal(.{ .bytes_per_sec = 10 * 1024 * 1024 * 1024 });

    const alloc = std.heap.c_allocator;

    const specific = args.payload_size != null and args.batch_size != null and args.fsync_mode != null;
    if (specific) {
        printHeader();
        const r = try runOne(
            alloc,
            args.dir,
            args.payload_size.?,
            args.batch_size.?,
            args.fsync_mode.?,
            args.duration_sec,
        );
        printRow(args.payload_size.?, args.batch_size.?, args.fsync_mode.?, r);
    } else {
        try runMatrix(alloc, args);
    }
}

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const argv = try std.process.argsAlloc(alloc);
    defer std.process.argsFree(alloc, argv);
    const args = try parseArgs(argv);

    // DiskWAL internally uses zio.File for I/O, which needs a running
    // executor — so we wrap the work in a zio task the same way the
    // loadtest driver and benchmark client do. `spawn` starts the task
    // on the runtime's thread pool; `join` blocks main until it finishes.
    var rt = try zio.Runtime.init(alloc, .{});
    defer rt.deinit();

    var handle = try rt.spawn(rootTask, .{args});
    try handle.join();
}
