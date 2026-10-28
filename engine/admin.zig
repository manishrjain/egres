// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");
const raft = @import("raft");
const zdb = @import("zdb");
const galloc = @import("galloc");
const _governor = @import("governor.zig");
const m = @import("metrics");
const flags = @import("flags");
const utils = @import("raft").utils;
const String = utils.String;
const Engine = @import("engine.zig").Engine;

const zdb_bufs = zdb.buffers;
const manifest = zdb.manifest;
const Forest = zdb.forest.Forest;
const Item = zdb.kv.Item;
const Key = zdb.kv.Key;
const TreeKey = zdb.kv.TreeKey;
const vfs = zdb.vfs;
const metrics = zdb.metrics;
const log = std.log.scoped(.admin);

/// Self-reference: the AdminServer code below keeps `proto.*` qualifiers for
/// the protocol decls defined in this same file.
const proto = @This();

pub const Command = union(enum) {
    set: SetCmd,
    del: DelCmd,
    set_batch: SetBatchCmd,
    get: GetCmd,
    scan: ScanCmd,
    conf: ConfCmd,
    ns_create: NsCreateCmd,
    zdb: ZdbCmd,
    raft: RaftCmd,
    vfs: VfsCmd,
    mem: MemCmd,
    locks: LocksCmd,
    workers: WorkersCmd,
    profile: ProfileCmd,
    traces: TracesCmd,
    help: HelpCmd,
};

pub const VfsCmd = struct {};

pub const MemCmd = struct {};

pub const LocksCmd = struct {};

pub const WorkersCmd = struct {};
/// `profile cpu <secs> [hz] [top [N] | folded [lines]]`: run the
/// sampling profiler for `secs` and reply with its output.
pub const ProfileCmd = struct {
    secs: u32,
    hz: u32 = 200,
    output: fiber.profile.Output = .{ .top = 20 },
};

/// `traces [min_ms]`: the statement traces the sampler kept, those
/// that took at least `min_ms`.
pub const TracesCmd = struct {
    min_ms: u64 = 0,
};

pub const HelpCmd = struct {};

/// Static text dumped by the `help` command. Lives next to the parser so
/// any time we add or change a verb, the help text gets touched in the
/// same diff. Keep entries one-line where possible.
pub const help_text =
    \\Available commands (text protocol, '\n' delimited):
    \\
    \\  help                            show this message
    \\  set <ns> <key> <value>          single write
    \\  del <ns> <key>                  delete: writes a tombstone version, so
    \\                                  the key reads as absent from now on.
    \\                                  Bytes are reclaimed later, by compaction
    \\  set-batch <ns> <count>          batch write; followed by count*2
    \\                                  whitespace-separated tokens (k1 v1 k2 v2 ...)
    \\                                  tokens prefixed with 0x are hex-decoded
    \\  get <ns> <key> [<read_ts>]      read; optional read_ts blocks until
    \\                                  this node's apply_ts >= read_ts
    \\  scan <ns> <start_key> <end_key> <max_count> [keys|kv|sizes]
    \\       [versions=<n>] [deleted] [read_ts=<n>]
    \\                                  forward range scan over [start_key,
    \\                                  end_key). Pass `_` for either bound
    \\                                  to leave it open. One ITEM per
    \\                                  line, hex-encoded key + value.
    \\                                  END <count> terminates. Default
    \\                                  mode kv.
    \\                                  versions=<n>: up to n versions per
    \\                                  key, newest first (default 1).
    \\                                  deleted: also emit tombstones and the
    \\                                  versions behind them; without it a
    \\                                  deleted key is skipped entirely.
    \\                                  read_ts=<n>: scan as of timestamp n,
    \\                                  blocking until apply_ts >= n first
    \\                                  (same contract as get). Default latest.
    \\  conf <action> <node_id> [<addr>] cluster membership change
    \\                                  actions: add, to_voter, to_learner, remove, addr
    \\  ns create <ns>                  create a namespace
    \\  zdb                             hierarchical forest summary
    \\  zdb metrics                     Prometheus-format storage metrics dump
    \\  zdb tree=<ns>:<id>              forest summary scoped to one tree
    \\  zdb namespace=<id>              forest summary for trees in one namespace
    \\  raft                            cluster topology + per-peer log progress
    \\  raft entry <N>                  dump WAL entry N (one human-readable line)
    \\  raft entry <lo> <hi>            dump WAL entries [lo, hi], one per line
    \\  vfs                             VFS bandwidth limiter snapshot
    \\  mem                             memory as egres sees it (live bytes,
    \\                                  cache, tables) and as the allocator does
    \\  locks                           dump every tracked mutex: holder src,
    \\                                  waiter count, last-waiter src
    \\  workers                         per-worker CPU histogram + imbalance
    \\                                  ratio (work distribution across workers),
    \\                                  and the coarse clock's gap histogram
    \\  profile cpu <secs> [hz] [top [N] | folded [lines]]
    \\                                  sample every worker's CPU for <secs>
    \\                                  (default 200 Hz); reply with pprof's
    \\                                  top table (N rows, default 20) or
    \\                                  folded stacks for a flamegraph, by
    \\                                  function or by file:line
    \\
;

pub const NsCreateCmd = struct {
    ns: u32,
};

/// `zdb`                 → hierarchical forest summary
/// `zdb metrics`         → Prometheus dump (the scrapers read this)
/// `zdb tree=<ns>:<N>`   → forest summary scoped to one tree
/// `zdb namespace=<N>`   → forest summary for trees in one namespace
pub const ZdbCmd = struct {
    filter: Filter = .forest,
    tree_id: u64 = 0,
    ns_id: u32 = 0,

    pub const Filter = enum { prom, forest, tree, namespace };
};

/// `raft`            → cluster topology + per-peer raft progress
/// `raft entry <N>`  → human-readable dump of WAL entry N
/// `raft entry <lo> <hi>` → dump entries in [lo, hi] (inclusive), one line each
pub const RaftCmd = struct {
    sub: SubCmd = .topology,
    /// `entry`: single index when hi==lo; range [lo, hi] otherwise.
    lo: u64 = 0,
    hi: u64 = 0,

    pub const SubCmd = enum { topology, entry };
};

pub const SetCmd = struct {
    ns: u32,
    key: String,
    value: String,
};

/// `del <ns> <key>` — write a tombstone over `key`. A delete is a write, not
/// an erasure: it appends a new version whose meta kind is `deleted`, which
/// shadows every older version on read. The bytes go away later, when a bottom-
/// level compaction finds the tombstone below `snapshot_ts`.
pub const DelCmd = struct {
    ns: u32,
    key: String,
};

/// Header for a batched write. The body is `count * 2` whitespace-delimited
/// tokens (key1, value1, key2, value2, ...). Whitespace is any run of
/// space / tab / LF / CR — newlines are cosmetic. A token starting with `0x`
/// is hex-decoded; otherwise taken as literal bytes. To send a literal
/// starting with `0x` or containing whitespace, hex-encode the whole token.
pub const SetBatchCmd = struct {
    ns: u32,
    count: u32,
};

pub const GetCmd = struct {
    ns: u32,
    key: String,
    /// Optional read timestamp. 0 means "serve local applied state without
    /// blocking." If > 0, the server waits until this node's apply_ts has
    /// reached read_ts (with a timeout) so the returned view includes every
    /// mutation up to read_ts.
    read_ts: u64 = 0,
};

/// `scan <ns> <start_key> <end_key> <max_count> [keys|kv|sizes]
///      [versions=<n>] [deleted]` —
/// forward range scan over [start_key, end_key) on the tree owning
/// `start_key`. The literal token `_` in either bound means "open"
/// (no lower / no upper bound). Returns up to `max_count` `ITEM`
/// lines, then a single `END <emitted>` line.
///
/// `versions=<n>` returns up to n versions of each key, newest first
/// (default 1). `deleted` also surfaces tombstones and the versions behind
/// them; without it a deleted key is skipped entirely. `read_ts=<n>` scans
/// as of that timestamp.
pub const ScanCmd = struct {
    ns: u32,
    start_key: String,
    /// Empty slice means "no upper bound" — either explicitly via the
    /// `_` token or implicitly because the caller wants to scan to EOF.
    end_key: String,
    max_count: u64,
    mode: Mode = .kv,
    num_versions: u32 = 1,
    include_deleted: bool = false,
    /// Optional read timestamp, same convention as `get`: 0 means "serve
    /// local applied state without blocking, at the latest version." If > 0,
    /// the server waits until this node's apply_ts reaches it, then reports
    /// the world as of that timestamp.
    read_ts: u64 = 0,

    pub const Mode = enum { keys, kv, sizes };
};

pub const ConfCmd = struct {
    action: Action,
    node_id: u64,
    addr: String = "",

    pub const Action = enum {
        add,
        to_voter,
        to_learner,
        remove,
        addr,
    };
};

/// Read one '\n'-delimited line into `buf` (without the trailing CR/LF).
/// Errors:
///   - `error.EndOfStream` — peer closed; treat as clean EOF.
///   - `error.LineTooLong` — line exceeded `buf`; the stream is mid-line
///     so the byte cursor is at an unknown offset; caller must close.
///   - other I/O errors — propagate; caller should close.
/// Stream is at a known boundary (just past '\n') only on success. Pair
/// with `parseLine` so command-level parse errors can be recovered without
/// closing the connection.
pub fn readLine(r: *std.Io.Reader, buf: []u8) !String {
    // takeDelimiterInclusive returns the bytes up to AND INCLUDING the
    // '\n' so the cursor advances past the delimiter. The Exclusive
    // variant leaves the '\n' in the buffer, which makes the next read
    // return an empty slice and would loop forever. Strip the trailing
    // '\n' (and an optional '\r' before it) before copying into the
    // caller's buffer.
    const line = try r.takeDelimiterInclusive('\n');
    var end = line.len - 1; // drop '\n'
    if (end > 0 and line[end - 1] == '\r') end -= 1;
    if (end > buf.len) return error.LineTooLong;
    @memcpy(buf[0..end], line[0..end]);
    return buf[0..end];
}

fn isWs(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\n' or c == '\r';
}

/// Hard cap on the bytes of any single whitespace-delimited wire token
/// (a key OR a value in a `set-batch` body). Also the per-token unused
/// capacity the caller must reserve on the scratch `ArrayList` — see
/// `readToken`. Set once here so the caller's reservation and the
/// parser's rejection threshold can't drift apart.
pub const max_token_size: usize = 64 * 1024;

/// One parsed key/value pair from the wire. Slices live in the
/// surrounding `ParsedBatch.buf` (or in line_buf for single-set
/// commands); the consumer must not retain them past the parse's
/// owning structure's deinit.
/// `meta` defaults to `.raw`, so `set` and `set-batch` construct these
/// unchanged and produce plain puts. `del` sets `kind = .deleted`. The field is
/// what `Engine.encodeMutations` probes for to decide whether to put a meta
/// byte on the wire.
pub const KV = struct {
    key: String,
    value: String,
    meta: zdb.kv.Meta = .{},
};

/// Fully parsed `set-batch` body: `count * 2` whitespace-delimited
/// tokens organized into `count` (key, value) pairs. `buf` owns the
/// concatenated token bytes; `kvs[i].key` / `kvs[i].value` slice into it.
pub const ParsedBatch = struct {
    ns: u32,
    kvs: []KV,
    buf: []u8,

    pub fn deinit(self: *ParsedBatch, alloc: std.mem.Allocator) void {
        alloc.free(self.kvs);
        alloc.free(self.buf);
    }
};

/// Read the body of a `set-batch` command — `count * 2` whitespace-
/// delimited tokens (key1 value1 key2 value2 …) — into a stable
/// `ParsedBatch`. Caller is responsible for `deinit`.
///
/// Two-phase: first concatenate all token bytes into `input` while
/// recording (start, end) offsets in `ranges`, then resolve offsets to
/// slices. We can't capture slices mid-loop because each
/// `ensureUnusedCapacity` call may relocate `input`'s backing storage.
pub fn readSetBatchBody(
    r: *std.Io.Reader,
    ns: u32,
    count: u32,
    alloc: std.mem.Allocator,
) !ParsedBatch {
    var input: std.ArrayList(u8) = .empty;
    errdefer input.deinit(alloc);
    try input.ensureTotalCapacity(alloc, 16 * 1024);

    const Range = struct { start: u32, end: u32 };
    const ranges = try alloc.alloc([2]Range, count);
    defer alloc.free(ranges);

    var i: u32 = 0;
    while (i < count) : (i += 1) {
        try input.ensureUnusedCapacity(alloc, max_token_size);
        const k_start: u32 = @intCast(input.items.len);
        try readToken(r, &input);
        const k_end: u32 = @intCast(input.items.len);

        try input.ensureUnusedCapacity(alloc, max_token_size);
        const v_start: u32 = @intCast(input.items.len);
        try readToken(r, &input);
        const v_end: u32 = @intCast(input.items.len);

        ranges[i] = .{
            .{ .start = k_start, .end = k_end },
            .{ .start = v_start, .end = v_end },
        };
    }

    // input is stable now (no more growth). Hand its backing buffer to
    // the ParsedBatch so the kv slices remain valid until deinit.
    const buf = try input.toOwnedSlice(alloc);
    errdefer alloc.free(buf);
    const kvs = try alloc.alloc(KV, count);
    for (ranges, 0..) |rng, j| kvs[j] = .{
        .key = buf[rng[0].start..rng[0].end],
        .value = buf[rng[1].start..rng[1].end],
    };

    return .{ .ns = ns, .kvs = kvs, .buf = buf };
}

/// Read one whitespace-delimited token, appending its bytes onto `buf`.
/// Leading whitespace is skipped; the token ends at the next whitespace
/// byte (or EOF). If prefixed with `0x`, the payload is hex-decoded in
/// place — the decoded output is exactly half the hex input length, and
/// `buf.items.len` is truncated so only the decoded bytes remain.
///
/// Contract with the caller:
/// - Before calling, caller must `ensureUnusedCapacity(max_token_size)`
///   on `buf` so `appendAssumeCapacity` is safe for the whole token.
/// - `readToken` rejects any token that would exceed `max_token_size`
///   with `error.TokenTooLong` before the overflowing append, so a
///   malicious oversized token cannot trip the `appendAssumeCapacity`
///   assertion.
///
/// To recover the token's position, snapshot `buf.items.len` before and
/// after the call — the token lives at `buf.items[before..after]`.
/// Don't eagerly materialize that slice if more `readToken` calls will
/// follow with intervening `ensureUnusedCapacity`, since a reallocation
/// will invalidate it; keep offsets instead and resolve them once the
/// buffer is stable.
pub fn readToken(r: *std.Io.Reader, buf: *std.ArrayList(u8)) !void {
    const start = buf.items.len;
    // Skip leading whitespace, then keep the first non-ws byte. The
    // caller's capacity guarantee covers this single-byte append, so
    // no bounds check needed here (max_token_size is always >= 1).
    while (true) {
        const b = try r.takeByte();
        if (!isWs(b)) {
            buf.appendAssumeCapacity(b);
            break;
        }
    }
    // Collect until the next whitespace (or EOF). Check the length BEFORE
    // each append — so the `max_token_size + 1`-th byte is rejected
    // instead of overflowing the caller's reservation.
    while (true) {
        const b = r.takeByte() catch |err| switch (err) {
            error.EndOfStream => break,
            else => return err,
        };
        if (isWs(b)) break;
        if (buf.items.len - start >= max_token_size) return error.TokenTooLong;
        buf.appendAssumeCapacity(b);
    }

    // Hex-encoded token? Decode in place. Write position (start + i)
    // always trails read position (start + 2 + 2*i), so the overlap is
    // safe — hexToBytes only touches a source byte before the destination
    // byte that would overwrite it.
    const bytes = buf.items[start..];
    if (bytes.len >= 2 and bytes[0] == '0' and bytes[1] == 'x') {
        const hex = bytes[2..];
        if (hex.len % 2 != 0) return error.BadHex;
        _ = try std.fmt.hexToBytes(bytes[0 .. hex.len / 2], hex);
        buf.items.len = start + hex.len / 2;
    }
}

fn parseProfile(it: *std.mem.SplitIterator(u8, .scalar)) !Command {
    const what = it.next() orelse return error.MissingArg;
    if (!std.mem.eql(u8, what, "cpu")) return error.InvalidArg;
    const secs_s = it.next() orelse return error.MissingArg;
    var req: ProfileCmd = .{
        .secs = std.fmt.parseInt(u32, secs_s, 10) catch return error.InvalidArg,
    };
    if (req.secs == 0 or req.secs > 3600) return error.InvalidArg;
    var tok = it.next();
    if (tok) |t| {
        if (std.fmt.parseInt(u32, t, 10) catch null) |hz| {
            if (hz == 0 or hz > 10_000) return error.InvalidArg;
            req.hz = hz;
            tok = it.next();
        }
    }
    if (tok) |t| {
        if (std.mem.eql(u8, t, "top")) {
            if (it.next()) |n| {
                req.output = .{ .top = std.fmt.parseInt(usize, n, 10) catch
                    return error.InvalidArg };
            }
        } else if (std.mem.eql(u8, t, "folded")) {
            req.output = .{ .folded = .{} };
            if (it.next()) |l| {
                if (!std.mem.eql(u8, l, "lines")) return error.InvalidArg;
                req.output = .{ .folded = .{ .lines = true } };
            }
        } else return error.InvalidArg;
    }
    return .{ .profile = req };
}

pub fn parseLine(line: String) !Command {
    var it = std.mem.splitScalar(u8, line, ' ');
    const cmd = it.next() orelse return error.EmptyCommand;

    if (std.mem.eql(u8, cmd, "set")) {
        const ns_str = it.next() orelse return error.MissingNamespace;
        const key = it.next() orelse return error.MissingKey;
        const value = it.rest();
        if (value.len == 0) return error.MissingValue;

        return .{ .set = .{
            .ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace,
            .key = key,
            .value = value,
        } };
    } else if (std.mem.eql(u8, cmd, "del")) {
        const ns_str = it.next() orelse return error.MissingNamespace;
        const key = it.next() orelse return error.MissingKey;
        return .{ .del = .{
            .ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace,
            .key = key,
        } };
    } else if (std.mem.eql(u8, cmd, "set-batch")) {
        const ns_str = it.next() orelse return error.MissingNamespace;
        const count_str = it.next() orelse return error.MissingCount;
        return .{ .set_batch = .{
            .ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace,
            .count = std.fmt.parseInt(u32, count_str, 10) catch return error.InvalidCount,
        } };
    } else if (std.mem.eql(u8, cmd, "get")) {
        const ns_str = it.next() orelse return error.MissingNamespace;
        const key = it.next() orelse return error.MissingKey;
        // read_ts is optional; if absent, follower serves local applied state.
        const read_ts: u64 = if (it.next()) |ts_str|
            (std.fmt.parseInt(u64, ts_str, 10) catch return error.InvalidTimestamp)
        else
            0;

        return .{ .get = .{
            .ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace,
            .key = key,
            .read_ts = read_ts,
        } };
    } else if (std.mem.eql(u8, cmd, "scan")) {
        const ns_str = it.next() orelse return error.MissingNamespace;
        const start_tok = it.next() orelse return error.MissingKey;
        const end_tok = it.next() orelse return error.MissingKey;
        const count_str = it.next() orelse return error.MissingCount;
        // Trailing options in any order: a mode name, versions=<n>, deleted,
        // read_ts=<n>.
        var mode: ScanCmd.Mode = .kv;
        var num_versions: u32 = 1;
        var include_deleted = false;
        var scan_read_ts: u64 = 0;
        while (it.next()) |tok| {
            if (std.mem.eql(u8, tok, "keys")) {
                mode = .keys;
            } else if (std.mem.eql(u8, tok, "kv")) {
                mode = .kv;
            } else if (std.mem.eql(u8, tok, "sizes")) {
                mode = .sizes;
            } else if (std.mem.eql(u8, tok, "deleted")) {
                include_deleted = true;
            } else if (std.mem.startsWith(u8, tok, "versions=")) {
                const n_str = tok["versions=".len..];
                num_versions = std.fmt.parseInt(u32, n_str, 10) catch
                    return error.InvalidArg;
            } else if (std.mem.startsWith(u8, tok, "read_ts=")) {
                const ts_str = tok["read_ts=".len..];
                scan_read_ts = std.fmt.parseInt(u64, ts_str, 10) catch
                    return error.InvalidArg;
            } else {
                return error.InvalidArg;
            }
        }
        // `_` is the open-bound sentinel (a literal underscore would need
        // to be sent hex-encoded — not currently supported by the scan
        // parser, which takes raw text tokens like `get`).
        const start_key: String = if (std.mem.eql(u8, start_tok, "_")) "" else start_tok;
        const end_key: String = if (std.mem.eql(u8, end_tok, "_")) "" else end_tok;
        return .{ .scan = .{
            .ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace,
            .start_key = start_key,
            .end_key = end_key,
            .max_count = std.fmt.parseInt(u64, count_str, 10) catch return error.InvalidCount,
            .mode = mode,
            .num_versions = num_versions,
            .include_deleted = include_deleted,
            .read_ts = scan_read_ts,
        } };
    } else if (std.mem.eql(u8, cmd, "conf")) {
        const action_str = it.next() orelse return error.MissingAction;
        const node_id_str = it.next() orelse return error.MissingNodeId;
        const action: ConfCmd.Action = if (std.mem.eql(u8, action_str, "add"))
            .add
        else if (std.mem.eql(u8, action_str, "to_voter"))
            .to_voter
        else if (std.mem.eql(u8, action_str, "to_learner"))
            .to_learner
        else if (std.mem.eql(u8, action_str, "remove"))
            .remove
        else if (std.mem.eql(u8, action_str, "addr"))
            .addr
        else
            return error.InvalidAction;

        const node_id = std.fmt.parseInt(u64, node_id_str, 10) catch return error.InvalidNodeId;
        const addr = if (action == .add or action == .addr) (it.next() orelse return error.MissingAddr) else "";

        return .{ .conf = .{
            .action = action,
            .node_id = node_id,
            .addr = addr,
        } };
    } else if (std.mem.eql(u8, cmd, "ns")) {
        // ns create <ns_id>
        const sub = it.next() orelse return error.MissingSubcommand;
        if (!std.mem.eql(u8, sub, "create")) return error.UnknownCommand;
        const ns_str = it.next() orelse return error.MissingNamespace;
        const ns = std.fmt.parseInt(u32, ns_str, 10) catch return error.InvalidNamespace;
        if (ns == 0) return error.InvalidNamespace; // 0 is a sentinel
        return .{ .ns_create = .{ .ns = ns } };
    } else if (std.mem.eql(u8, cmd, "zdb")) {
        const sub = it.next() orelse return .{ .zdb = .{} };
        if (std.mem.eql(u8, sub, "metrics")) {
            return .{ .zdb = .{ .filter = .prom } };
        }
        // Look for `tree=<N>` or `namespace=<N>`. Anything else is an error.
        const eq = std.mem.indexOfScalar(u8, sub, '=') orelse return error.InvalidZdbFilter;
        const k = sub[0..eq];
        const v = sub[eq + 1 ..];
        if (std.mem.eql(u8, k, "tree")) {
            // tree=<id> targets namespace 0; tree=<ns>:<id> a namespace-local tree
            // (tree ids are only unique within a namespace).
            if (std.mem.indexOfScalar(u8, v, ':')) |c| {
                const ns = std.fmt.parseInt(u32, v[0..c], 10) catch return error.InvalidZdbFilter;
                const id = std.fmt.parseInt(u64, v[c + 1 ..], 10) catch return error.InvalidZdbFilter;
                return .{ .zdb = .{ .filter = .tree, .ns_id = ns, .tree_id = id } };
            }
            const id = std.fmt.parseInt(u64, v, 10) catch return error.InvalidZdbFilter;
            return .{ .zdb = .{ .filter = .tree, .tree_id = id } };
        }
        if (std.mem.eql(u8, k, "namespace")) {
            const id = std.fmt.parseInt(u32, v, 10) catch return error.InvalidZdbFilter;
            return .{ .zdb = .{ .filter = .namespace, .ns_id = id } };
        }
        return error.InvalidZdbFilter;
    } else if (std.mem.eql(u8, cmd, "raft")) {
        const sub = it.next() orelse return .{ .raft = .{} };
        if (!std.mem.eql(u8, sub, "entry")) return error.UnknownCommand;
        const lo_str = it.next() orelse return error.MissingArg;
        const lo = std.fmt.parseInt(u64, lo_str, 10) catch return error.InvalidArg;
        const hi: u64 = if (it.next()) |hi_str|
            (std.fmt.parseInt(u64, hi_str, 10) catch return error.InvalidArg)
        else
            lo;
        if (hi < lo) return error.InvalidArg;
        return .{ .raft = .{ .sub = .entry, .lo = lo, .hi = hi } };
    } else if (std.mem.eql(u8, cmd, "vfs")) {
        return .{ .vfs = .{} };
    } else if (std.mem.eql(u8, cmd, "mem")) {
        return .{ .mem = .{} };
    } else if (std.mem.eql(u8, cmd, "locks")) {
        return .{ .locks = .{} };
    } else if (std.mem.eql(u8, cmd, "workers")) {
        return .{ .workers = .{} };
    } else if (std.mem.eql(u8, cmd, "profile")) {
        return parseProfile(&it);
    } else if (std.mem.eql(u8, cmd, "traces")) {
        var req: TracesCmd = .{};
        if (it.next()) |ms| {
            req.min_ms = std.fmt.parseInt(u64, ms, 10) catch
                return error.InvalidArg;
        }
        return .{ .traces = req };
    } else if (std.mem.eql(u8, cmd, "help")) {
        return .{ .help = .{} };
    } else {
        return error.UnknownCommand;
    }
}

/// Write a successful response: "OK\n". Used for conf/ns_create where there's
/// no committed timestamp to surface.
pub fn writeOk(stream: fiber.net.Stream) !void {
    try stream.writeAll("OK\n");
}

/// Write a successful set response carrying the committed HLC timestamp:
/// "OK <ts>\n". Clients track the max observed commit ts for read-your-writes.
pub fn writeSetOk(stream: fiber.net.Stream, commit_ts: u64) !void {
    var buf: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "OK {d}\n", .{commit_ts}) catch unreachable;
    try stream.writeAll(s);
}

/// Write the static help text followed by a blank line. Same termination
/// shape as `zdb` / `raft` so simple line-buffered clients can read until
/// `\n\n`.
pub fn writeHelp(stream: fiber.net.Stream) !void {
    try stream.writeAll(help_text);
    try stream.writeAll("\n");
}

/// Write an error response: "ERR <msg>\n"
pub fn writeErr(stream: fiber.net.Stream, msg: String) !void {
    try stream.writeAll("ERR ");
    try stream.writeAll(msg);
    try stream.writeAll("\n");
}

/// Write a not-leader response: "NOT_LEADER <leader_id>\n"
pub fn writeNotLeader(stream: fiber.net.Stream, leader_id: u64) !void {
    var buf: [32]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "NOT_LEADER {d}\n", .{leader_id}) catch unreachable;
    try stream.writeAll(s);
}

/// Write a get-found response: "FOUND <ts> <value>\n"
pub fn writeFound(stream: fiber.net.Stream, ts: u64, value: String) !void {
    var buf: [64]u8 = undefined;
    const hdr = std.fmt.bufPrint(&buf, "FOUND {d} ", .{ts}) catch unreachable;
    try stream.writeAll(hdr);
    try stream.writeAll(value);
    try stream.writeAll("\n");
}

/// Write a get-not-found response: "NOT_FOUND\n"
pub fn writeNotFound(stream: fiber.net.Stream) !void {
    try stream.writeAll("NOT_FOUND\n");
}

/// Write a single scan ITEM line through a buffered `std.Io.Writer`.
/// Hex-encodes both key and value so the frame is text-only (one per
/// `\n`-terminated line).
///   .kv     → "ITEM <ts> <hex_key> <hex_value>\n"
///   .keys   → "ITEM <ts> <hex_key>\n"
///   .sizes  → "ITEM <ts> <hex_key> <vlen>\n"
/// The caller owns the writer's buffer + flushes after the final ITEM
/// (or END). Per-byte hex appends pass through the buffer, so the hot
/// path is a `memcpy` into the writer's scratch — no syscalls until the
/// buffer fills.
pub fn writeScanItem(
    w: *std.Io.Writer,
    mode: ScanCmd.Mode,
    ts: u64,
    key: String,
    value: String,
) !void {
    try w.print("ITEM {d} ", .{ts});
    try writeHex(w, key);
    switch (mode) {
        .keys => {},
        .sizes => try w.print(" {d}", .{value.len}),
        .kv => {
            try w.writeByte(' ');
            try writeHex(w, value);
        },
    }
    try w.writeByte('\n');
}

/// Write the scan terminator: "END <emitted_count>\n".
pub fn writeScanEnd(w: *std.Io.Writer, count: u64) !void {
    try w.print("END {d}\n", .{count});
}

fn writeHex(w: *std.Io.Writer, bytes: String) !void {
    const hex = "0123456789abcdef";
    for (bytes) |b| {
        try w.writeByte(hex[b >> 4]);
        try w.writeByte(hex[b & 0xf]);
    }
}

/// The admin protocol front-end: a line-protocol listener that translates
/// admin/data commands into Engine calls. Holds a borrowed `*Engine`.
pub const AdminServer = struct {
    engine: *Engine,
    listen_addr: fiber.net.IpAddress,

    pub fn init(engine: *Engine, client_addr: String) !AdminServer {
        return .{
            .engine = engine,
            .listen_addr = try fiber.net.IpAddress.parseIpAndPort(client_addr),
        };
    }

    pub fn deinit(self: *AdminServer) void {
        _ = self;
    }

    pub fn runListener(self: *AdminServer) void {
        log.debug("inside runListener", .{});
        defer log.info("client loop exiting (shutdown)", .{});

        const listener = self.listen_addr.listen(.{ .reuse_address = true }) catch |err| {
            log.err("failed to listen on client addr: {}", .{err});
            return;
        };
        defer listener.close();

        // Break out of the blocking `accept()` at shutdown: a watcher fiber
        // self-connects to our address when the engine's shutdown channel
        // closes, so the `accept()` below returns and this loop exits.
        fiber.spawnDetached(
            .{},
            Engine.stopListenerOnStart,
            .{ self.engine, self.listen_addr, .shutdown },
        ) catch {};

        log.info("client TCP listener started, accepting connections", .{});

        // Per-connection handlers are detached: each frees itself when its
        // connection closes and `handleClient` returns. Without this, the
        // accumulating handles would pin one ~1 MiB-stack fiber per connection
        // in the registry for the whole server lifetime. Handlers self-exit on
        // `isShuttingDown()`, so no collective join is needed at shutdown.
        var conn_id: u64 = 0;
        while (!self.engine.node.isShuttingDown()) {
            const stream = listener.accept(.{}) catch |err| {
                log.err("runListener: accept failed: {}", .{err});
                return;
            };
            // The shutdown watcher wakes this accept by self-connecting; once
            // shutting down, drop that throwaway connection and exit.
            if (self.engine.node.isShuttingDown()) {
                stream.close();
                return;
            }
            conn_id += 1;
            log.debug("accepted client connection #{d}", .{conn_id});
            fiber.spawnDetached(.{}, handleClient, .{ self, stream }) catch {
                log.err("failed to spawn handler for connection #{d}", .{conn_id});
                stream.close();
                continue;
            };
        }
    }

    fn handleClient(self: *AdminServer, stream: fiber.net.Stream) void {
        defer stream.close();

        var ops: u64 = 0;
        // One buffered reader per connection — protocol reads (command
        // headers, batch tokens) come from the 256 KiB buffer instead of
        // hitting io_uring per byte. Buffers heap-allocated because fibers
        // have small stacks.
        const reader_buf = self.engine.allocator.alloc(u8, 256 * 1024) catch return;
        defer self.engine.allocator.free(reader_buf);

        const line_buf = self.engine.allocator.alloc(u8, 64 * 1024) catch return;
        defer self.engine.allocator.free(line_buf);

        var reader = stream.reader(reader_buf);
        const r = &reader.interface;

        while (!self.engine.node.isShuttingDown()) {
            // Step 1: pull a line. Stream-level errors (LineTooLong, I/O)
            // close the connection — byte stream is at an unknown offset.
            const line = proto.readLine(r, line_buf) catch |err| switch (err) {
                error.EndOfStream => {
                    log.debug("client disconnected after {d} ops (EOF)", .{ops});
                    return;
                },
                else => {
                    log.debug("client stream error after {d} ops: {s}; " ++
                        "closing", .{ ops, @errorName(err) });
                    proto.writeErr(stream, @errorName(err)) catch {};
                    return;
                },
            };
            if (line.len == 0) continue; // blank line — common from interactive clients

            // Step 2: parse the line. Parse errors are RECOVERABLE — the
            // line is fully consumed, the stream is at a clean boundary,
            // so we send ERR and keep the connection open. Common in
            // interactive use (typo a command, fix and retry).
            const cmd = proto.parseLine(line) catch |err| {
                proto.writeErr(stream, @errorName(err)) catch return;
                continue;
            };
            ops += 1;

            switch (cmd) {
                .set => |req| {
                    log.debug("SET ns={d} key={s} value_len={d}", .{
                        req.ns, req.key, req.value.len,
                    });
                    self.handleSet(stream, req) catch return;
                },
                .del => |req| {
                    log.debug("DEL ns={d} key={s}", .{ req.ns, req.key });
                    self.handleDel(stream, req) catch return;
                },
                .set_batch => |req| {
                    log.debug("SET_BATCH ns={d} count={d}", .{ req.ns, req.count });
                    var batch = proto.readSetBatchBody(r, req.ns, req.count, self.engine.allocator) catch |err| {
                        proto.writeErr(stream, @errorName(err)) catch {};
                        return;
                    };
                    defer batch.deinit(self.engine.allocator);
                    self.handleSetBatch(stream, batch) catch return;
                },
                .get => |req| {
                    log.debug("GET ns={d} key={s} read_ts={d}", .{ req.ns, req.key, req.read_ts });
                    self.handleGet(stream, req) catch return;
                },
                .scan => |req| {
                    log.debug("SCAN ns={d} start_key={s} max={d} mode={s} versions={d} deleted={}", .{
                        req.ns,             req.start_key,    req.max_count,
                        @tagName(req.mode), req.num_versions, req.include_deleted,
                    });
                    self.handleScan(stream, req) catch return;
                },
                .conf => |req| {
                    log.info("CONF {s} node={d}", .{ @tagName(req.action), req.node_id });
                    self.handleConf(stream, req) catch return;
                },
                .ns_create => |req| {
                    log.info("NS CREATE ns={d}", .{req.ns});
                    self.handleNsCreate(stream, req) catch return;
                },
                .zdb => |req| {
                    self.handleZdb(stream, req) catch return;
                },
                .raft => |req| {
                    self.handleRaft(stream, req) catch return;
                },
                .vfs => {
                    self.handleVfs(stream) catch return;
                },
                .mem => {
                    self.handleMem(stream) catch return;
                },
                .locks => {
                    self.handleLocks(stream) catch return;
                },
                .workers => {
                    self.handleWorkers(stream) catch return;
                },
                .profile => |req| {
                    self.handleProfile(stream, req) catch return;
                },
                .traces => |req| {
                    self.handleTraces(stream, req) catch return;
                },
                .help => {
                    proto.writeHelp(stream) catch return;
                },
            }
        }
        log.debug("client handler exiting after {d} ops (shutdown)", .{ops});
    }

    fn handleSet(self: *AdminServer, stream: fiber.net.Stream, req: proto.SetCmd) !void {
        return self.proposeOneKV(stream, req.ns, .{
            .key = req.key,
            .value = req.value,
        }, "SET");
    }

    /// A delete is a put of an empty value with meta kind `deleted` — same wire
    /// shape, same propose path, same commit semantics as `set`.
    fn handleDel(self: *AdminServer, stream: fiber.net.Stream, req: proto.DelCmd) !void {
        return self.proposeOneKV(stream, req.ns, .{
            .key = req.key,
            .value = "",
            .meta = .{ .kind = .deleted },
        }, "DEL");
    }

    /// Propose a single KV through Raft and block until it commits. `verb`
    /// is only used to label log lines, so `set` and `del` stay
    /// distinguishable in the log while sharing one code path.
    fn proposeOneKV(
        self: *AdminServer,
        stream: fiber.net.Stream,
        ns: u32,
        kv: proto.KV,
        comptime verb: String,
    ) !void {
        // Reject up-front if the destination namespace has no route table.
        // Otherwise the mutation is durably committed through Raft but
        // `buildFlush` will silently drop it (no tree to route to), and the
        // immutable memtable never gets popped — RAM grows without bound.
        if (!self.engine.forest.hasNamespace(ns)) {
            try writeUnknownNamespace(stream, ns);
            return;
        }
        // Encode as a single-item Mutations batch — the wire format is the
        // same regardless of batch size, so the apply path doesn't need to
        // know whether this write came from a `set` or a `set_batch`.
        // Wire keys are full marshaled keys; admin writes land in the
        // raw-KV table (table_id 0).
        const alloc = self.engine.allocator;
        const wire_key = try (Key{ .namespace = ns, .primary = kv.key }).marshal(alloc);
        defer alloc.free(wire_key);
        var kvs = [_]zdb_bufs.KeyValueStruct{
            .{ .key = wire_key, .value = kv.value, .meta = kv.meta.toByte() },
        };
        try self.proposeKvs(stream, ns, &kvs, verb);
    }

    /// The one way an admin write reaches raft: `Engine.proposeWrites`,
    /// which every data proposal in the process goes through (so the
    /// L0 brake applies here too), waiting for commit. Replies OK, not
    /// leader, or the error's name.
    fn proposeKvs(
        self: *AdminServer,
        stream: fiber.net.Stream,
        ns: u32,
        kvs: []zdb_bufs.KeyValueStruct,
        comptime verb: String,
    ) !void {
        self.engine.proposeWrites(ns, kvs, .commit) catch |err| switch (err) {
            error.NotLeader => {
                const leader = self.engine.node.getLeaderId() orelse 0;
                log.info(verb ++ " rejected: not leader (leader={d})", .{leader});
                return proto.writeNotLeader(stream, leader);
            },
            else => {
                log.err(verb ++ " failed: {s}", .{@errorName(err)});
                return proto.writeErr(stream, @errorName(err));
            },
        };
        log.debug(verb ++ " committed", .{});
        try proto.writeOk(stream);
    }

    /// Tokenize `count * 2` whitespace-delimited tokens from the buffered
    /// reader into (key, value) pairs, encode as a single Mutations,
    /// propose once. Each token is hex-decoded if prefixed with `0x`,
    /// literal otherwise.
    fn handleSetBatch(self: *AdminServer, stream: fiber.net.Stream, batch: proto.ParsedBatch) !void {
        if (!self.engine.forest.hasNamespace(batch.ns)) {
            try writeUnknownNamespace(stream, batch.ns);
            return;
        }
        // Wire keys are full marshaled keys (raw-KV table_id 0); the
        // arena owns them until the payload is encoded.
        var arena = std.heap.ArenaAllocator.init(self.engine.allocator);
        defer arena.deinit();
        const aa = arena.allocator();
        const kvs = try aa.alloc(zdb_bufs.KeyValueStruct, batch.kvs.len);
        for (batch.kvs, kvs) |src, *dst| dst.* = .{
            .key = try (Key{ .namespace = batch.ns, .primary = src.key }).marshal(aa),
            .value = src.value,
            .meta = src.meta.toByte(),
        };
        try self.proposeKvs(stream, batch.ns, kvs, "set_batch");
    }

    fn handleGet(self: *AdminServer, stream: fiber.net.Stream, req: proto.GetCmd) !void {
        // If the client sent a read_ts, wait for this node's apply_ts to reach
        // it (5s timeout). No read_ts → serve local applied state immediately.
        if (req.read_ts > 0) {
            self.engine.node.waitForApplied(req.read_ts, 5000) catch {
                try proto.writeErr(stream, "read_stale");
                return;
            };
        }
        // MVCC lookup at the requested timestamp. 0 → latest (maxInt).
        const search_ts: u64 = if (req.read_ts == 0) std.math.maxInt(u64) else req.read_ts;
        const search_key = Key{
            .namespace = req.ns,
            .primary = req.key,
        };
        const result = self.engine.forest.get(self.engine.allocator, .{
            .key = search_key,
            .read_ts = search_ts,
        }) catch |err| {
            log.err("forest.get failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, "get failed");
            return;
        };
        if (result) |item| {
            defer item.free(self.engine.allocator);
            const ver = item.newest();
            log.debug(
                "GET found: ts={d} value_len={d}",
                .{ ver.ts, ver.value.len },
            );
            try proto.writeFound(stream, ver.ts, ver.value);
        } else {
            log.debug("GET miss", .{});
            try proto.writeNotFound(stream);
        }
    }

    fn handleScan(self: *AdminServer, stream: fiber.net.Stream, req: proto.ScanCmd) !void {
        // Same read_ts contract as `get`: a positive value is first a
        // freshness barrier (wait for this node to have applied that far),
        // then the as-of timestamp for the scan itself. 0 → latest.
        if (req.read_ts > 0) {
            self.engine.node.waitForApplied(req.read_ts, 5000) catch {
                try proto.writeErr(stream, "read_stale");
                return;
            };
        }
        const read_ts: u64 = if (req.read_ts == 0) std.math.maxInt(u64) else req.read_ts;

        // Scan range = [start, end). The bounds name primaries;
        // visibility is decided by read_ts, not by them. An empty
        // primary on either side means no bound on that side.
        const scan_start: Key = if (req.start_key.len == 0) Key.inf else .{
            .namespace = req.ns,
            .primary = req.start_key,
        };
        const scan_end: Key = if (req.end_key.len == 0) Key.inf else .{
            .namespace = req.ns,
            .primary = req.end_key,
        };
        // Admin scans the raw-KV table (table_id 0); the cursor confines
        // the range to (req.ns, 0) across the namespace-wide memtable.
        const scan_range = zdb.kv.KeyRange{
            .left = scan_start,
            .right = scan_end,
        };

        // 4 KiB buffered writer — 1-2 items fit per drain at the bench's
        // 4 KiB value size; smaller modes pack many items per drain.
        var buf: [4096]u8 = undefined;
        var sw = stream.writer(&buf);
        const w = &sw.interface;

        const Ctx = struct {
            w: *std.Io.Writer,
            mode: proto.ScanCmd.Mode,
            ns: u32,

            fn emit(c: *@This(), item: zdb.kv.Item) anyerror!bool {
                // Memtables are forest-wide; an item with a different
                // namespace means we've walked off the end of ours.
                if (item.key.namespace != c.ns) return false;
                // One line per version, so num_versions keeps its
                // meaning for the client.
                for (item.versions.slice()) |ver| {
                    try proto.writeScanItem(
                        c.w,
                        c.mode,
                        ver.ts,
                        item.key.primary,
                        ver.value,
                    );
                }
                return true;
            }
        };
        var cctx = Ctx{ .w = w, .mode = req.mode, .ns = req.ns };

        const emitted = self.engine.forest.scan(
            self.engine.allocator,
            .{ .namespace = req.ns, .table_id = 0 },
            scan_range,
            .{
                .max_rows = req.max_count,
                .num_versions = req.num_versions,
                .include_deleted = req.include_deleted,
                .read_ts = read_ts,
            },
            &cctx,
            Ctx.emit,
        ) catch |err| {
            log.err("forest.scan failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, "scan failed");
            return;
        };
        try proto.writeScanEnd(w, emitted);
        try w.flush();
    }

    fn handleLocks(self: *AdminServer, stream: fiber.net.Stream) !void {
        // Walks the tracked-lock registry (zfiber/lock_registry.zig) and emits
        // one line per held lock: holder @src, hold time, count, waiters. Read
        // while wedged to find the lock / call site that's blocking everyone.
        var body: std.Io.Writer.Allocating = .init(self.engine.allocator);
        defer body.deinit();
        try fiber.dumpLocks(&body.writer);
        try body.writer.writeByte('\n');
        try stream.writeAll(body.writer.buffered());
    }

    fn handleWorkers(self: *AdminServer, stream: fiber.net.Stream) !void {
        // Per-worker accumulated CPU time as a histogram, plus a max/avg
        // imbalance ratio (1.0 == perfectly balanced). Shows how evenly the
        // fiber runtime spread work across its workers under load.
        var body: std.Io.Writer.Allocating = .init(self.engine.allocator);
        defer body.deinit();
        try fiber.dumpWorkerCpu(&body.writer);
        try body.writer.writeByte('\n');
        try stream.writeAll(body.writer.buffered());
    }

    /// Run the sampling profiler for the asked seconds and reply with
    /// its output: the top table or folded stacks (see zfiber/profile).
    /// One at a time: the sampler is process-wide.
    fn handleProfile(
        self: *AdminServer,
        stream: fiber.net.Stream,
        req: ProfileCmd,
    ) !void {
        fiber.profile.start(self.engine.allocator, .{
            .hz = req.hz,
            .secs = req.secs,
        }) catch |err| {
            log.err("profile start: {s}", .{@errorName(err)});
            return proto.writeErr(stream, @errorName(err));
        };
        fiber.sleepMs(@as(u64, req.secs) * 1000);

        var body: std.Io.Writer.Allocating = .init(self.engine.allocator);
        defer body.deinit();
        const report = fiber.profile.stop(&body.writer, req.output) catch |err|
            {
                log.err("profile stop: {s}", .{@errorName(err)});
                return proto.writeErr(stream, @errorName(err));
            };
        log.info("profile cpu {d}s @ {d} Hz: {d} samples, {d} dropped", .{
            req.secs, req.hz, report.samples, report.dropped,
        });
        try body.writer.writeByte('\n');
        try stream.writeAll(body.writer.buffered());
    }

    /// Reply with the kept statement traces (see zfiber/ctx).
    fn handleTraces(
        self: *AdminServer,
        stream: fiber.net.Stream,
        req: TracesCmd,
    ) !void {
        var body: std.Io.Writer.Allocating = .init(self.engine.allocator);
        defer body.deinit();

        try fiber.writeTraces(&body.writer, req.min_ms);
        try body.writer.writeByte('\n');
        try stream.writeAll(body.writer.buffered());
    }

    fn handleVfs(self: *AdminServer, stream: fiber.net.Stream) !void {
        _ = self;
        // One section per device limiter, then the global VFS's (files
        // on no calibrated device), counted in pages (multiply by VFS page
        // size for bytes); then what every grant paid, process wide by
        // class, which is the meter's and not any one device's.
        var buf: [4096]u8 = undefined;
        var sw = stream.writer(&buf);
        const w = &sw.interface;
        for (vfs.calibrate.devices()) |d| {
            try writeLimiter(w, d.path, d.vfs.bw);
        }
        try writeLimiter(w, "global", vfs.g().bw);
        try w.print("vfs grants (all devices):\n", .{});
        for (&fiber.meter.stats[@intFromEnum(fiber.Kind.io)], 0..) |*wt, i| {
            const grants = wt.grants.load(.monotonic);
            const wait_ms = wt.wait_ms.load(.monotonic);
            try w.print("  {s}: grants={d} wait_ms={d} mean_wait_us={d}\n", .{
                class_names[i],
                grants,
                wait_ms,
                wait_ms * 1000 / @max(grants, 1),
            });
        }
        // Clients read to the blank line.
        try w.writeByte('\n');
        try w.flush();
    }

    /// `meter.stats` is indexed by `fiber.IoClass`.
    const class_names = [_]String{ "fg", "bg" };

    fn writeLimiter(
        w: *std.Io.Writer,
        name: String,
        bw: *const vfs.bandwidth.Limiter,
    ) !void {
        const s = bw.status();
        try w.print("vfs bandwidth {s} (pages):\n", .{name});
        try w.print("  rate:          {d} pages/s\n", .{s.pages_per_sec});
        try w.print("  metered:       {}\n", .{s.metered});
        try w.print("  total_read:    {d}\n", .{s.total_read_pages});
        try w.print("  total_write:   {d}\n", .{s.total_write_pages});
    }

    /// The meter's io wait counters in Prometheus text, beside the zdb
    /// and raft registries, so the dashboard reads them the same way;
    /// one series per class, over every device.
    fn writeLimiterMetrics(w: *std.Io.Writer) !void {
        const names = [_]String{
            "vfs_bandwidth_grants_total",
            "vfs_bandwidth_wait_us_total",
        };
        for (names) |n| try w.print("# TYPE {s} counter\n", .{n});
        for (&fiber.meter.stats[@intFromEnum(fiber.Kind.io)], 0..) |*wt, i| {
            const vals = [_]u64{
                wt.grants.load(.monotonic),
                wt.wait_ms.load(.monotonic) * 1000,
            };
            for (names, vals) |n, val| {
                try w.print("{s}{{class=\"{s}\"}} {d}\n", .{
                    n, class_names[i], val,
                });
            }
        }
    }

    /// The chunk cache's counters in Prometheus text; nothing when
    /// there is no slot file.
    fn writeChunkCacheMetrics(
        self: *AdminServer,
        w: *std.Io.Writer,
    ) !void {
        const cc = self.engine.forest.store.chunkCache() orelse return;
        if (!cc.enabled()) return;
        const st = cc.stats();
        const Metric = struct { String, String, u64 };
        const rows = [_]Metric{
            .{ "chunk_cache_slots", "gauge", st.slots },
            .{ "chunk_cache_used", "gauge", st.used },
            .{ "chunk_cache_hits_total", "counter", st.hits },
            .{ "chunk_cache_misses_total", "counter", st.misses },
            .{ "chunk_cache_puts_total", "counter", st.puts },
            .{ "chunk_cache_drops_total", "counter", st.drops },
            .{ "chunk_cache_corrupt_total", "counter", st.corrupt },
            .{ "chunk_cache_evicted_pages_total", "counter", st.evicted_pages },
            .{ "chunk_cache_unread_pages_total", "counter", st.unread_pages },
            .{ "chunk_cache_unread_chunks_total", "counter", st.unread_chunks },
        };
        for (rows) |mt| {
            try w.print("# TYPE {s} {s}\n{s} {d}\n", .{
                mt[0], mt[1], mt[0], mt[2],
            });
        }
    }

    /// Where the process's memory is, as Egres sees it (the bytes it
    /// holds through `CountingAlloc`, and what holds them; `other
    /// live` is live minus the three accounted owners), then as the
    /// allocator sees it (`galloc.snapshot`: resident, retained, and so
    /// on). Resident minus live is the allocator's own.
    fn handleMem(self: *AdminServer, stream: fiber.net.Stream) !void {
        var buf: [4096]u8 = undefined;
        var sw = stream.writer(&buf);
        const w = &sw.interface;
        const counting = utils.context.counting();
        const live = counting.live_bytes.load(.monotonic);
        const cache: u64 = if (zdb.table.table.block_cache) |bc|
            bc.totalCost()
        else
            0;
        const res = &zdb.table.table.resident;
        const tables = res.index.load(.monotonic) +
            res.filter.load(.monotonic) + res.egids.load(.monotonic);
        const mt = self.engine.forest.memtables.stats();
        const memtables = mt.active_bytes + mt.immutable_bytes;

        try w.print("egres:\n", .{});
        try memLine(w, "live", live, "held by the program");
        try w.print("  over_limit:   {d}   (allocations past the " ++
            "total)\n", .{counting.over_limit.load(.monotonic)});
        try memLine(w, "block_cache", cache, "block bytes");
        try memLine(w, "tables", tables, "index, filters, egid bitmaps");
        try memLine(w, "memtables", memtables, "active + immutable");
        // Includes the cache's maps and rings, and blocks it evicted that
        // readers still hold.
        const other = live -| (cache + tables + memtables);
        try memLine(w, "other live", other, "");
        if (_governor.g()) |gov| {
            try w.print("governor:\n", .{});
            try memLine(w, "total", gov.total, "--cache total_bytes, resident");
            try w.print("  ratio:        {d:.3}   (resident per live " ++
                "byte, recent peak; last {d:.3})\n", .{
                gov.ratio.cur, gov.ratio.measured,
            });
            try memLine(w, "live_target", gov.liveTarget(), "total / ratio");
            try memLine(w, "margin", gov.margin, "recent peak of the rest");
            try memLine(w, "cache_cap", gov.knob.now, "in force");
        }
        try w.print("{s}:\n", .{galloc.kind});
        const snap = galloc.snapshot();
        inline for (std.meta.fields(@TypeOf(snap))) |f| {
            if (@field(snap, f.name)) |v| try memLine(w, f.name, v, "");
        }
        // Clients read to the blank line.
        try w.writeByte('\n');
        try w.flush();
    }

    fn memLine(
        w: *std.Io.Writer,
        name: String,
        bytes: ?u64,
        note: String,
    ) !void {
        try w.print("  {s}:", .{name});
        var pad: usize = 14 -| (name.len + 1);
        while (pad > 0) : (pad -= 1) try w.writeByte(' ');
        if (bytes) |b|
            try zdb.stats.writeBytes(w, b)
        else
            try w.print("unknown", .{});
        if (note.len > 0) try w.print("   ({s})", .{note});
        try w.print("\n", .{});
    }

    fn handleConf(self: *AdminServer, stream: fiber.net.Stream, req: proto.ConfCmd) !void {
        // This release runs a single node (main.zig refuses seeds too).
        const single_node_release = true;
        if (single_node_release) {
            try proto.writeErr(stream, "cluster membership changes are" ++
                " not available in this release");
            return;
        }
        const change_type: raft.ConfChangeType = switch (req.action) {
            .add => .add_learner,
            .to_voter => .to_voter,
            .to_learner => .to_learner,
            .remove => .remove_node,
            .addr => .update_addr,
        };

        // Validate address for commands that require it.
        if (req.action == .add or req.action == .addr) {
            _ = fiber.net.IpAddress.parseIpAndPort(req.addr) catch {
                try proto.writeErr(stream, "invalid address");
                return;
            };
        }

        var w: raft.ProposalWaiter = .{};
        self.engine.node.proposeConfChange(.{
            .change_type = change_type,
            .node_id = req.node_id,
            .addr = req.addr,
        }, &w) catch |err| {
            log.err("conf change failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, @errorName(err));
            return;
        };
        const status = w.wait();
        switch (status) {
            .committed => try proto.writeOk(stream),
            .not_leader => {
                const leader = self.engine.node.getLeaderId() orelse 0;
                try proto.writeNotLeader(stream, leader);
            },
            else => try proto.writeErr(stream, @tagName(status)),
        }
    }

    fn handleRaft(self: *AdminServer, stream: fiber.net.Stream, req: proto.RaftCmd) !void {
        if (req.sub == .entry) return self.handleRaftEntry(stream, req.lo, req.hi);
        // Best-effort snapshot of node + peer state. Races against an
        // in-flight conf change are possible (peers map mutated by
        // mainLoop's applyConfChange); accepted because this is an admin
        // endpoint, not a hot path. Output is one `node` line per cluster
        // member with the same column shape, so progress is visually
        // alignable. The local node's line carries extra cluster-wide
        // fields (term/leader/quorum/commit/apply/first) at the end.
        // Blank line terminates — same shape as `stats` so simple clients
        // can read until `\n\n`.
        var body: std.Io.Writer.Allocating = .init(self.engine.allocator);
        defer body.deinit();
        const w = &body.writer;

        const node = self.engine.node;
        const leader_id = node.getLeaderId() orelse 0;
        const last_idx = node.log.lastIndex();

        // Self row — `last` = local last_index (Raft's `lastLogIndex`),
        // `next` = last+1 (the index the next append would write).
        // Role is left-padded to 9 chars so the column aligns with peer
        // rows regardless of "leader" / "follower" / "voter" / etc.
        // Cluster-wide extras (term/leader/quorum/commit/apply/log range)
        // come at the end of the line.
        try w.print(
            "node id={d} role={s:<9} last={d} next={d} addr={s}  term={d} leader={d} quorum={d} commit={d} apply={d} log=[{d},{d}]\n",
            .{
                node.id,
                @tagName(node.state),
                last_idx,
                last_idx + 1,
                node.config.listen_addr,
                node.current_term,
                leader_id,
                node.quorum_size,
                node.commit_index.load(),
                node.apply_index.load(.acquire),
                node.log.firstIndex(),
                last_idx,
            },
        );

        // Peer rows. Only the leader has authoritative match/next per
        // peer (Raft's `matchIndex[]` is leader-side state); followers
        // don't track each other's replication progress. So followers
        // show just role + addr — the operator should query the leader
        // (route via `leader=` in the self-row above) for real progress.
        const is_leader = node.state == .leader;
        var it = node.peers.iterator();
        while (it.next()) |kv| {
            const peer = kv.value_ptr.*;
            if (is_leader) {
                try w.print("node id={d} role={s:<9} last={d} next={d} addr={s}\n", .{
                    kv.key_ptr.*,
                    @tagName(peer.role),
                    peer.last_index,
                    peer.next_index,
                    peer.addr,
                });
            } else {
                try w.print("node id={d} role={s:<9} addr={s}\n", .{
                    kv.key_ptr.*,
                    @tagName(peer.role),
                    peer.addr,
                });
            }
        }
        try w.writeByte('\n');
        try stream.writeAll(body.writer.buffered());
    }

    /// Dump WAL entries [lo, hi] (inclusive) as one human-readable line each.
    /// Used for poking at the raft log when an apply error wedges progress
    /// at a known index. Out-of-range or missing entries become an error
    /// line so the caller can tell missing-vs-empty apart.
    fn handleRaftEntry(self: *AdminServer, stream: fiber.net.Stream, lo: u64, hi: u64) !void {
        var body: std.Io.Writer.Allocating = .init(self.engine.allocator);
        defer body.deinit();
        const w = &body.writer;

        var result = self.engine.node.log.entriesRange(.{ .lo = lo, .hi = hi + 1 }) catch |err| {
            try w.print("err={s}\n", .{@errorName(err)});
            try w.writeByte('\n');
            try stream.writeAll(body.writer.buffered());
            return;
        };
        defer result.deinit();

        // Walk the requested range and report any indices the WAL didn't
        // return (gap before, between, or after entries).
        var expected: u64 = lo;
        for (result.entries) |stored| {
            while (expected < stored.index) : (expected += 1) {
                try w.print("idx={d} <missing>\n", .{expected});
            }
            try formatEntry(self.engine.allocator, w, stored);
            expected = stored.index + 1;
        }
        while (expected <= hi) : (expected += 1) {
            try w.print("idx={d} <missing>\n", .{expected});
        }
        try w.writeByte('\n');
        try stream.writeAll(body.writer.buffered());
    }

    fn handleZdb(self: *AdminServer, stream: fiber.net.Stream, req: proto.ZdbCmd) !void {
        // Snapshot into a buffer so we write in a single syscall. Clients
        // read until '\n\n' so we always terminate the body with a blank line.
        var body: std.Io.Writer.Allocating = .init(self.engine.allocator);
        defer body.deinit();
        switch (req.filter) {
            .prom => {
                // Both metric namespaces (zdb storage + raft propose-phase)
                // walk through the same writer so they appear in one blob.
                try m.write(zdb.metrics.g(), &body.writer);
                try m.write(raft.metrics.g(), &body.writer);
                try writeLimiterMetrics(&body.writer);
                try self.writeChunkCacheMetrics(&body.writer);
                // Driven from the workers' loop tops: a gap is how long
                // all of them were away from it.
                try utils.coarse_clock.writeGapMetrics(&body.writer);
            },
            .forest => {
                try self.engine.forest.writeStats(&body.writer, .{ .scope = .forest });
                try self.engine.writeInFlight(&body.writer);
            },
            .tree => try self.engine.forest.writeStats(&body.writer, .{ .scope = .{ .tree = .{ .namespace = req.ns_id, .tree_id = req.tree_id } } }),
            .namespace => try self.engine.forest.writeStats(&body.writer, .{ .scope = .{ .namespace = req.ns_id } }),
        }
        try body.writer.writeByte('\n');
        try stream.writeAll(body.writer.buffered());
    }

    fn handleNsCreate(self: *AdminServer, stream: fiber.net.Stream, req: proto.NsCreateCmd) !void {
        if (!self.engine.node.isLeader()) {
            const leader = self.engine.node.getLeaderId() orelse 0;
            try proto.writeNotLeader(stream, leader);
            return;
        }
        // buildNsCreate reads applied state (ns existence), so the
        // leader must be caught up first (see Engine.catchUp).
        self.engine.catchUp() catch |err| {
            log.err("ns create: catch-up barrier: {s}", .{@errorName(err)});
            try proto.writeErr(stream, @errorName(err));
            return;
        };
        var arena = std.heap.ArenaAllocator.init(self.engine.allocator);
        defer arena.deinit();
        const nr = self.engine.forest.buildNsCreate(
            arena.allocator(),
            .{ .given = req.ns },
            "",
        ) catch |err| {
            log.err("ns create failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, @errorName(err));
            return;
        };
        self.engine.proposeMC(nr.change, .apply) catch |err| {
            log.err("ns create failed: {s}", .{@errorName(err)});
            try proto.writeErr(stream, @errorName(err));
            return;
        };
        try proto.writeOk(stream);
    }
};

fn writeUnknownNamespace(stream: fiber.net.Stream, ns: u32) !void {
    var buf: [64]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "unknown namespace {d}", .{ns}) catch unreachable;
    try proto.writeErr(stream, msg);
}

/// Render one StoredEntry as a single-line human-readable summary.
/// Format: `idx=I term=T ts=SECS.LOGICAL type=X bytes=B detail=...`
/// HLC is split into <unix_seconds>.<logical> so log times are visually
/// scannable instead of a giant u64.
fn formatEntry(alloc: std.mem.Allocator, w: *std.Io.Writer, stored: raft.StoredEntry) !void {
    const reader = raft.buffers.LogEntryReader.parseRoot(stored.bytes) catch |err| {
        try w.print("idx={d} term={d} bytes={d} parse_err={s}\n", .{ stored.index, stored.term, stored.bytes.len, @errorName(err) });
        return;
    };
    const ety = reader.entryType();
    const hlc = utils.hlc.HLC.fromU64(reader.timestamp());
    // The data as proposed; `stored_bytes` is what the entry holds,
    // smaller when it was compressed.
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(alloc);
    const data = try utils.compression.maybeUnpack(
        alloc,
        reader.codec(),
        reader.data(),
        &scratch,
    );
    try w.print(
        "idx={d} term={d} ts={d}.{d} type={s} data_bytes={d} " ++
            "stored_bytes={d}",
        .{
            stored.index,  stored.term, hlc.seconds,       hlc.logical,
            @tagName(ety), data.len,    reader.data().len,
        },
    );
    if (data.len == 0) {
        try w.print(" detail=tick\n", .{});
        return;
    }
    switch (ety) {
        .data, .manifest => {
            const parsed = zdb_bufs.parseMessageWithTail(data) catch |err| {
                try w.print(" detail=parse_err({s})\n", .{@errorName(err)});
                return;
            };
            switch (parsed) {
                .Mutations => |ms| {
                    const count = ms.kvsLen();
                    try w.print(" detail=Mutations ns={d} count={d}", .{ ms.namespace(), count });
                    if (count > 0) {
                        const kv = ms.kvsAt(0) catch {
                            try w.writeByte('\n');
                            return;
                        };
                        try w.print(" first_key={x}", .{kv.key()});
                    }
                    try w.writeByte('\n');
                },
                .ManifestChange => |mc| {
                    const mcs = mc.toStruct(alloc) catch |err| {
                        try w.print(" detail=ManifestChange err={s}\n", .{@errorName(err)});
                        return;
                    };
                    defer {
                        mcs.deinit(alloc);
                        alloc.destroy(mcs);
                    }
                    try w.print(" detail=ManifestChange cmd={t} memtable_id={d} sstable_changes={d} tree_changes={d}", .{
                        mcs.cmd, mcs.memtable_id, mcs.sstable_changes.len, mcs.tree_changes.len,
                    });
                    try w.writeByte('\n');
                    for (mcs.sstable_changes, 0..) |tc, idx| {
                        try w.print("  tc[{d}]=(op={s} fid={d} tree={d} lvl={d})\n", .{
                            idx, @tagName(tc.op), tc.sst_id, tc.tree_id, tc.level,
                        });
                    }
                },
                else => try w.print(" detail=Other({s})\n", .{@tagName(parsed)}),
            }
        },
        .conf => try w.print(" detail=ConfChange\n", .{}),
        .snapshot => try w.print(" detail=Snapshot\n", .{}),
        else => try w.print(" detail=unknown\n", .{}),
    }
}
