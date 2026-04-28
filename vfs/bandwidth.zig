const std = @import("std");
const zio = @import("zio");
const vfs = @import("vfs.zig");

pub const Config = struct {
    bytes_per_sec: u64, // bytes per second
    write_cost: u8 = 8, // write multiplier - each write costs k * read cost.
    timeout_ms: u64 = 100, // After this timeout, provide min_alloc.
    min_alloc_bytes: u64 = 8 * vfs.page_size, // default: write_cost (8) * page_size
};

/// Atomically subtract `n` from `v`, clamping at zero.
fn atomicSaturatingSub(v: *std.atomic.Value(u64), n: u64) void {
    while (true) {
        const old = v.load(.monotonic);
        if (v.cmpxchgWeak(old, old -| n, .release, .monotonic) == null) return;
    }
}

/// Represents an ongoing I/O operation with bandwidth tracking.
/// Not thread-safe — must be used from a single thread/coroutine.
pub const Spinner = struct {
    limiter: *Limiter,
    priority: vfs.Priority,
    kind: vfs.IOKind,
    rem_pages: u64, // pages still needed (rounded up)
    rem_bytes: u64, // exact bytes still needed

    /// Returns next chunk of granted bytes.
    /// Blocks until pages are available for this priority.
    /// Returns 0 when remaining is fully satisfied — spinner is done.
    pub fn acquireBytes(self: *Spinner) usize {
        if (self.rem_bytes == 0) return 0;

        const wm: u64 = if (self.kind == .write) self.limiter.conf.write_cost else 1;
        const granted_pages = self.acquireFromLimiter(self.rem_pages * wm) / wm;
        self.deductPages(granted_pages);

        const granted_bytes = granted_pages * vfs.page_size;
        // On the last chunk, rem_bytes may be less than granted_bytes.
        // Return exactly what's left, so caller gets exactly how much they asked for.
        const result = @min(self.rem_bytes, granted_bytes);
        self.rem_bytes -= result;
        return result;
    }

    /// Update remaining and demand_p1 after a grant.
    fn deductPages(self: *Spinner, pages: u64) void {
        self.rem_pages -|= pages;
        if (self.priority == .p1) {
            atomicSaturatingSub(&self.limiter.demand_p1, pages);
        }
    }

    /// Acquire raw pages (in cost units). Handles priority, blocking, timeout.
    /// Caller is responsible for cost/page conversion.
    fn acquireFromLimiter(self: *Spinner, pages: u64) u64 {
        const limiter = self.limiter;
        const start = std.time.milliTimestamp();

        while (true) {
            limiter.tryRefill();
            const cur = limiter.pages.load(.monotonic);

            // Compute how many pages this priority can see.
            const available = switch (self.priority) {
                .p0 => pages, // P0 always gets full grant (saturating subtract).
                .p1 => cur,
                .p2 => cur -| limiter.demand_p1.load(.monotonic),
            };

            if (available == 0 and self.priority != .p0) {
                // No pages available — sleep and retry, or timeout.
                if (std.time.milliTimestamp() - start >= @as(i64, @intCast(limiter.conf.timeout_ms))) {
                    return limiter.min_alloc_pages;
                }
                zio.sleep(.fromMilliseconds(2)) catch {};
                continue;
            }

            // Try to deduct pages via CAS.
            const want = @min(pages, available);
            if (self.priority == .p0) {
                // P0: saturating subtract, always succeeds.
                atomicSaturatingSub(&limiter.pages, want);
                return want;
            }

            if (limiter.pages.cmpxchgWeak(cur, cur - want, .monotonic, .monotonic) == null) {
                return want;
            }
            // CAS failed — another thread grabbed pages, retry.
        }
    }

    pub fn deinit(self: *Spinner) void {
        // Clear any remaining demand from the P1 counter.
        if (self.priority == .p1 and self.rem_pages > 0) {
            atomicSaturatingSub(&self.limiter.demand_p1, self.rem_pages);
        }
    }
};

pub const Limiter = struct {
    pages: std.atomic.Value(u64), // token bucket in page units
    last_refill: std.atomic.Value(i64), // monotonic millis
    demand_p1: std.atomic.Value(u64), // sum of all active P1 spinners' remaining (pages)
    pages_per_sec: u64,
    min_alloc_pages: u64,
    conf: Config,

    pub fn init(config: Config) Limiter {
        const pps = config.bytes_per_sec / vfs.page_size;
        return .{
            .pages = std.atomic.Value(u64).init(pps),
            .last_refill = std.atomic.Value(i64).init(std.time.milliTimestamp()),
            .demand_p1 = std.atomic.Value(u64).init(0),
            .pages_per_sec = pps,
            .min_alloc_pages = @max(config.min_alloc_bytes / vfs.page_size, 1),
            .conf = config,
        };
    }

    /// Create a spinner for an I/O operation at the given priority.
    pub fn spinner(self: *Limiter, priority: vfs.Priority, bytes: usize, kind: vfs.IOKind) Spinner {
        const pages: u64 = std.mem.alignForward(u64, @intCast(bytes), vfs.page_size) / vfs.page_size;
        if (priority == .p1) {
            _ = self.demand_p1.fetchAdd(pages, .release);
        }
        return .{
            .limiter = self,
            .priority = priority,
            .kind = kind,
            .rem_pages = pages,
            .rem_bytes = @intCast(bytes),
        };
    }

    /// Try to refill the bucket. CAS on last_refill ensures only one thread
    /// wins per time window — losers skip and proceed to page deduction.
    fn tryRefill(self: *Limiter) void {
        const old_refill = self.last_refill.load(.monotonic);
        const now = std.time.milliTimestamp();
        const elapsed_ms = now - old_refill;
        if (elapsed_ms <= 0) return;

        // Clamp elapsed to one second: the bucket caps at `pages_per_sec`
        // anyway (one second's worth of capacity), so longer gaps add no
        // more pages — and clamping prevents `elapsed * pages_per_sec`
        // from overflowing u64 on long pauses or extreme rate configs.
        const elapsed: u64 = @min(@as(u64, @intCast(elapsed_ms)), 1_000);
        const added = elapsed * self.pages_per_sec / 1_000;
        if (added == 0) return;

        // Race to claim this refill window.
        if (self.last_refill.cmpxchgStrong(old_refill, now, .monotonic, .monotonic) != null) return;

        // We won — add pages, capped at rate.
        while (true) {
            const old_pages = self.pages.load(.monotonic);
            const new_pages = @min(old_pages + added, self.pages_per_sec);
            if (self.pages.cmpxchgWeak(old_pages, new_pages, .monotonic, .monotonic) == null) break;
        }
    }
};
