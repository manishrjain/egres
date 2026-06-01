const std = @import("std");
const utils = @import("utils");

/// Encoded file_id layout: 16 bits node_id (high) | 48 bits local counter (low).
/// 48 bits is ~281T ids per node — at 1M ids/sec sustained that's ~9 years,
/// effectively infinite for any realistic workload. 16 bits is 65k nodes,
/// safely larger than any realistic cluster size.
pub const LOCAL_BITS: u6 = 48;
const LOCAL_MASK: u64 = (@as(u64, 1) << LOCAL_BITS) - 1;

pub fn encode(node_id: u16, local_v: u64) u64 {
    std.debug.assert(local_v <= LOCAL_MASK);
    return (@as(u64, node_id) << LOCAL_BITS) | local_v;
}

pub fn node(file_id: u64) u16 {
    return @intCast(file_id >> LOCAL_BITS);
}

pub fn local(file_id: u64) u64 {
    return file_id & LOCAL_MASK;
}

/// Reverse of `sstPath` — extract the encoded file_id from an SST
/// filename / path. Returns 0 on any mismatch (wrong extension, wrong
/// stem layout, non-hex chars). 0 is the safe sentinel: it isn't a
/// valid encoded id (node 0 is reserved by `bind`'s assertion), so
/// callers can use 0 as "skip cache" or "non-SST file".
pub fn fromPath(path: []const u8) u64 {
    const basename = std.fs.path.basename(path);
    if (!std.mem.endsWith(u8, basename, ".sst")) return 0;
    const stem = basename[0 .. basename.len - 4];
    if (stem.len != 17 or stem[4] != '_') return 0; // 4 + '_' + 12
    const n = std.fmt.parseInt(u16, stem[0..4], 16) catch return 0;
    const lo = std.fmt.parseInt(u64, stem[5..17], 16) catch return 0;
    return encode(n, lo);
}

/// Build the SST path for an encoded `file_id`. Filename layout is
/// `<node:04x>_<local:012x>.sst` — fixed-width hex (4 + 12 = 16 chars),
/// so directory listings sort by (node, local) lexicographically.
/// Pass `dir = ""` to get the bare filename without any prefix; otherwise
/// the result is `<dir>/<filename>`. Caller owns the returned slice.
pub fn sstPath(allocator: std.mem.Allocator, dir: []const u8, file_id: u64) ![]u8 {
    if (dir.len == 0) return std.fmt.allocPrint(
        allocator,
        "{x:0>4}_{x:0>12}.sst",
        .{ node(file_id), local(file_id) },
    );
    return std.fmt.allocPrint(
        allocator,
        "{s}/{x:0>4}_{x:0>12}.sst",
        .{ dir, node(file_id), local(file_id) },
    );
}

/// Local-counter values reserved per HardState write. A crash forfeits at
/// most this many ids in our node's space.
pub const LEASE_BATCH: u64 = 1024;

/// Persist hook: must durably write `new_max` (the new ceiling for our
/// node's local counter) to backing storage BEFORE returning. `FileIds`
/// only hands out ids strictly below the ceiling, so a crash recovery
/// that resumes the local counter at the persisted ceiling never reuses
/// an id this process already wrote to disk.
pub const PersistFn = *const fn (ctx: *anyopaque, new_max: u64) anyerror!void;

/// Per-node file_id allocator: bumps a local counter, extends a durable
/// HardState lease in batches. `bind` must be called before `allocate`;
/// `allocate` errors with `NoLeaseCallback` otherwise.
pub const FileIds = struct {
    /// Next local counter value to hand out (`fetchAdd` consumer).
    next: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Highest local value covered by the durable lease. `allocate`
    /// extends this in `LEASE_BATCH`-sized steps.
    lease_high: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Serializes lease extensions so concurrent allocators bump
    /// HardState exactly once per batch.
    mu: utils.TrackedMutex = .{},
    node_id: u16 = 0,
    persist_fn: ?PersistFn = null,
    persist_ctx: ?*anyopaque = null,

    /// Wire to a node identity + persistence backend. `initial_max` is the
    /// value loaded from HardState on startup; the local counter resumes
    /// at it (every id below it was already reserved in a prior lease).
    pub fn bind(
        self: *FileIds,
        node_id: u16,
        initial_max: u64,
        ctx: *anyopaque,
        persist_fn: PersistFn,
    ) void {
        std.debug.assert(node_id != 0); // 0 is reserved for "unset"
        self.node_id = node_id;
        self.next.store(initial_max, .release);
        self.lease_high.store(initial_max, .release);
        self.persist_ctx = ctx;
        self.persist_fn = persist_fn;
    }

    pub fn allocate(self: *FileIds) !u64 {
        const persist = self.persist_fn orelse return error.NoLeaseCallback;
        const ctx = self.persist_ctx.?;
        const v = self.next.fetchAdd(1, .monotonic);
        while (v >= self.lease_high.load(.acquire)) {
            self.mu.lock(@src());
            defer self.mu.unlock();
            // Re-check under lock — another allocator may have extended.
            if (v < self.lease_high.load(.acquire)) break;
            // Always extend to the next multiple of `LEASE_BATCH`. Keeps
            // ceilings aligned across restarts so each lease is exactly
            // `LEASE_BATCH` ids long (no growing/shrinking batch sizes).
            const new_high = std.mem.alignForward(u64, v + 1, LEASE_BATCH);
            try persist(ctx, new_high);
            self.lease_high.store(new_high, .release);
        }
        return encode(self.node_id, v);
    }
};
