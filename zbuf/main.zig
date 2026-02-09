const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var args = std.process.args();
    const prog = args.next() orelse "zbuf";
    const input_path = args.next() orelse {
        std.debug.print("usage: {s} <path/to/buffers.zig>\n", .{prog});
        return error.Invalid;
    };

    const out_path = try deriveOutPath(alloc, input_path);
    defer alloc.free(out_path);

    const buffers_arg = try std.fmt.allocPrint(alloc, "-Mroot:buffers={s}", .{input_path});
    defer alloc.free(buffers_arg);
    const buffers_schema_arg = try std.fmt.allocPrint(alloc, "-Mroot:buffers:schema=zbuf/schema.zig", .{});
    defer alloc.free(buffers_schema_arg);

    const argv = [_][]const u8{
        "zig",
        "run",
        "-Mroot=zbuf/gen.zig",
        buffers_arg,
        buffers_schema_arg,
        "--",
        out_path,
    };

    if (!std.process.can_spawn) {
        std.debug.print("process spawning not supported on this platform\n", .{});
        return error.Unsupported;
    }

    const res = try std.process.Child.run(.{
        .allocator = alloc,
        .argv = &argv,
    });
    defer alloc.free(res.stdout);
    defer alloc.free(res.stderr);

    switch (res.term) {
        .Exited => |code| if (code != 0) {
            std.debug.print(
                "zig run failed with exit code {d}\n{s}\n{s}\n",
                .{ code, res.stdout, res.stderr },
            );
            return error.ChildFailed;
        },
        else => {
            std.debug.print(
                "zig run terminated unexpectedly\n{s}\n{s}\n",
                .{ res.stdout, res.stderr },
            );
            return error.ChildFailed;
        },
    }
}

fn deriveOutPath(alloc: std.mem.Allocator, input: []const u8) ![]const u8 {
    if (std.mem.endsWith(u8, input, ".zig")) {
        const base_len = input.len - 4;
        return std.fmt.allocPrint(alloc, "{s}.gen.zig", .{input[0..base_len]});
    }
    return std.fmt.allocPrint(alloc, "{s}.gen.zig", .{input});
}
