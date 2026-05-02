// Hash throughput benchmark — three columns per row:
//
//   1. `std.hash.crc.Crc32Iscsi.hash` — table-based software CRC.
//   2. `crc32cHw` (this file)         — inline SSE4.2 `crc32q` for 8-byte
//                                       chunks + std software CRC for the
//                                       0–7 byte tail. Bit-compatible with
//                                       column 1. Kept here as historical
//                                       reference; production used to ship
//                                       this before switching to xxhash3.
//   3. `std.hash.XxHash3.hash`        — what zbuf framing now ships.
//                                       Modern non-cryptographic hash, uses
//                                       @Vector(8, u64) blocks → AVX2 SIMD
//                                       on x86_64.
//
// We measure all three so the speed gap motivating the switch stays
// inspectable. Production bytes flow through column 3 only — see
// `zbuf/zbuf.zig` for the rationale on why xxhash3 over CRC.

const std = @import("std");
const builtin = @import("builtin");

// ── Hardware CRC32C (kept local to the bench) ─────────────────────────────

inline fn crc32qStep(crc_in: u64, v: u64) u64 {
    return asm ("crc32q %[v], %[c]"
        : [c] "=r" (-> u64),
        : [v] "r" (v),
          [crcin] "0" (crc_in),
    );
}

/// Hybrid hardware/software CRC32C. Bit-compatible with Crc32Iscsi.hash.
/// Hardware path used for 8-byte chunks; std software CRC handles the 0–7
/// byte tail (sidesteps the self-hosted x86 backend's missing crc32b
/// encoding without changing output bits).
fn crc32cHw(payload: []const u8) u32 {
    var crc: u64 = 0xFFFF_FFFF;
    var i: usize = 0;
    while (i + 8 <= payload.len) : (i += 8) {
        const v = std.mem.readInt(u64, payload[i..][0..8], .little);
        crc = crc32qStep(crc, v);
    }
    var sw = std.hash.crc.Crc32Iscsi{ .crc = @truncate(crc) };
    sw.update(payload[i..]);
    return sw.final();
}

// ── Benchmark harness ─────────────────────────────────────────────────────

const TARGET_BYTES: u64 = 1 * 1024 * 1024 * 1024; // ~1 GiB total work per row.

const SIZES = [_]usize{
    16,
    64,
    256,
    1024,
    4 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1 * 1024 * 1024,
    4 * 1024 * 1024,
    16 * 1024 * 1024,
};

// Mutate one byte of the payload per iteration so LLVM can't hoist the hash
// computation out of the loop (which it does for tiny inputs to XxHash3,
// producing nonsensical multi-TiB/s numbers). One-byte write per iter is
// ≪ the hash work at every measured size.
fn benchSw(payload: []u8, iters: u64) struct { ns: u64 } {
    var sink: u32 = 0;
    const t0 = std.time.nanoTimestamp();
    for (0..iters) |i| {
        payload[0] = @truncate(i);
        sink ^= std.hash.crc.Crc32Iscsi.hash(payload);
    }
    const t1 = std.time.nanoTimestamp();
    std.mem.doNotOptimizeAway(sink);
    return .{ .ns = @intCast(t1 - t0) };
}

fn benchHw(payload: []u8, iters: u64) struct { ns: u64 } {
    var sink: u32 = 0;
    const t0 = std.time.nanoTimestamp();
    for (0..iters) |i| {
        payload[0] = @truncate(i);
        sink ^= crc32cHw(payload);
    }
    const t1 = std.time.nanoTimestamp();
    std.mem.doNotOptimizeAway(sink);
    return .{ .ns = @intCast(t1 - t0) };
}

fn benchXxHash3(payload: []u8, iters: u64) struct { ns: u64 } {
    var sink: u64 = 0;
    const t0 = std.time.nanoTimestamp();
    for (0..iters) |i| {
        payload[0] = @truncate(i);
        sink ^= std.hash.XxHash3.hash(0, payload);
    }
    const t1 = std.time.nanoTimestamp();
    std.mem.doNotOptimizeAway(sink);
    return .{ .ns = @intCast(t1 - t0) };
}

fn mibPerSec(ns: u64, iters: u64, size: usize) f64 {
    const total_bytes: f64 = @floatFromInt(iters * size);
    const seconds: f64 = @as(f64, @floatFromInt(ns)) / 1e9;
    return total_bytes / (seconds * 1024.0 * 1024.0);
}

fn fmtSize(buf: []u8, size: usize) ![]const u8 {
    if (size >= 1024 * 1024) return std.fmt.bufPrint(buf, "{d} MiB", .{size / (1024 * 1024)});
    if (size >= 1024) return std.fmt.bufPrint(buf, "{d} KiB", .{size / 1024});
    return std.fmt.bufPrint(buf, "{d} B", .{size});
}

pub fn main() !void {
    if (builtin.cpu.arch != .x86_64) {
        std.debug.print("Hardware CRC path requires x86_64 (target={s}); aborting.\n", .{@tagName(builtin.cpu.arch)});
        return;
    }

    const alloc = std.heap.page_allocator;
    const max_size = SIZES[SIZES.len - 1];
    const buf = try alloc.alloc(u8, max_size);
    defer alloc.free(buf);

    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    prng.random().bytes(buf);

    // Sanity check: hardware CRC32C and std software CRC32C must agree.
    const probe = buf[0..1024];
    const sw_v = std.hash.crc.Crc32Iscsi.hash(probe);
    const hw_v = crc32cHw(probe);
    if (sw_v != hw_v) {
        std.debug.print("MISMATCH! sw=0x{x:0>8} hw=0x{x:0>8}\n", .{ sw_v, hw_v });
        return error.CrcMismatch;
    }

    std.debug.print("# Hash throughput: CRC32C software, CRC32C hardware, XxHash3 (shipped)\n", .{});
    std.debug.print("# {d} MiB total work per row. Single-threaded.\n\n", .{TARGET_BYTES >> 20});
    std.debug.print(
        "{s:>10} | {s:>14} | {s:>14} | {s:>14} | {s:>10}\n",
        .{ "size", "crc32c-sw", "crc32c-hw", "xxhash3", "xx vs hw" },
    );
    std.debug.print(
        "{s:->10}-+-{s:->14}-+-{s:->14}-+-{s:->14}-+-{s:->10}\n",
        .{ "", "", "", "", "" },
    );

    for (SIZES) |size| {
        const payload = buf[0..size];
        const iters: u64 = @max(1, TARGET_BYTES / @as(u64, size));

        const sw = benchSw(payload, iters);
        const hw = benchHw(payload, iters);
        const xx = benchXxHash3(payload, iters);

        const sw_rate = mibPerSec(sw.ns, iters, size);
        const hw_rate = mibPerSec(hw.ns, iters, size);
        const xx_rate = mibPerSec(xx.ns, iters, size);
        const xx_vs_hw = xx_rate / hw_rate;

        var name_buf: [16]u8 = undefined;
        const name = try fmtSize(&name_buf, size);
        std.debug.print(
            "{s:>10} | {d:>14.1} | {d:>14.1} | {d:>14.1} | {d:>9.2}x\n",
            .{ name, sw_rate, hw_rate, xx_rate, xx_vs_hw },
        );
    }
}
