const std = @import("std");
const zdb = @import("zdb");
const vfs = zdb.vfs;
const Table = zdb.table.Table;
const TableIterator = zdb.table.TableIterator;
const Key = zdb.key.Key;

/// Returns true on success, false on any failure. Prints the result line
/// directly so we don't need to retain pointers into the table after it's
/// dropped (smallest/biggest live in the table's index memory).
fn verifyOne(allocator: std.mem.Allocator, path: []const u8) bool {
    const base = std.fs.path.basename(path);

    const t = Table.open(allocator, path, .p2) catch |err| {
        std.debug.print("FAIL {s} open={s}\n", .{ base, @errorName(err) });
        return false;
    };
    defer t.decrRef();

    const file_size = t.handle.file_size;
    const fid = t.file_id;

    var it = TableIterator.init(allocator, t, .p2);
    defer it.deinit();
    it.seekRange(.{}, .{}) catch |err| {
        std.debug.print("FAIL {s} size={d} fid={d} seek={s}\n", .{ base, file_size, fid, @errorName(err) });
        return false;
    };

    var prev_buf: std.ArrayList(u8) = .empty;
    defer prev_buf.deinit(allocator);
    var keys: u64 = 0;
    var have_prev = false;

    while (it.itemRef()) |item| {
        if (have_prev) {
            const prev = Key.init(prev_buf.items);
            if (item.key.order(prev) != .gt) {
                std.debug.print("FAIL {s} fid={d} bad_order_at={d}\n", .{ base, fid, keys });
                return false;
            }
        }
        prev_buf.clearRetainingCapacity();
        _ = item.key.marshalTo(allocator, &prev_buf) catch |err| {
            std.debug.print("FAIL {s} fid={d} marshal={s}\n", .{ base, fid, @errorName(err) });
            return false;
        };
        have_prev = true;
        keys += 1;
        it.next() catch |err| {
            std.debug.print("FAIL {s} fid={d} keys={d} next={s}\n", .{ base, fid, keys, @errorName(err) });
            return false;
        };
    }
    std.debug.print("OK   {s} size={d} fid={d} keys={d} smallest_pri={x} biggest_pri={x}\n", .{
        base, file_size, fid, keys, t.smallest.primary, t.biggest.primary,
    });
    return true;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const argv = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, argv);

    const dir_path = if (argv.len >= 2) argv[1] else {
        std.debug.print("usage: sst-verify <dir>\n", .{});
        std.process.exit(2);
    };

    // Same defaults the server uses; the verifier only reads, but the global
    // VFS still needs a bandwidth controller.
    vfs.VFS.initGlobal(.{ .bytes_per_sec = 500 * 1024 * 1024 });

    var dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
    defer dir.close();

    var paths: std.ArrayList([]const u8) = .empty;
    defer {
        for (paths.items) |p| allocator.free(p);
        paths.deinit(allocator);
    }
    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".sst")) continue;
        const full = try std.fs.path.join(allocator, &.{ dir_path, entry.name });
        try paths.append(allocator, full);
    }
    std.mem.sort([]const u8, paths.items, {}, struct {
        fn lt(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.lessThan(u8, a, b);
        }
    }.lt);

    var ok: u64 = 0;
    var bad: u64 = 0;
    for (paths.items) |p| {
        if (verifyOne(allocator, p)) ok += 1 else bad += 1;
    }
    std.debug.print("\nsummary: {d} OK, {d} FAIL\n", .{ ok, bad });
    if (bad > 0) std.process.exit(1);
}
