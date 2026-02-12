const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    addServer(b, target, optimize);

    const schema_mod = addSchemaModule(b, target, optimize);
    const regen_all_buffers_step = addRegenAllBuffers(b, target, optimize, schema_mod);

    addZbufCli(b, target, optimize, schema_mod);
    const zbuf_test_step = addZbufTests(b, target, optimize, schema_mod);
    const raft_test_step = addRaftTests(b, target, optimize, schema_mod);

    const all_test_step = b.step("test", "Run all tests");
    all_test_step.dependOn(regen_all_buffers_step);
    all_test_step.dependOn(zbuf_test_step);
    all_test_step.dependOn(raft_test_step);
}

fn addRegenAllBuffers(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    schema_mod: *std.Build.Module,
) *std.Build.Step {
    const step = b.step("buffers", "Regenerate all discovered */buffers.gen.zig files");
    const schema_paths = collectBufferSchemas(b);
    for (schema_paths, 0..) |schema_path, idx| {
        const out_path = b.fmt("{s}.gen.zig", .{schema_path[0 .. schema_path.len - 4]});

        const buffers_mod = b.createModule(.{
            .root_source_file = b.path(schema_path),
            .target = target,
            .optimize = optimize,
        });
        buffers_mod.addImport("schema", schema_mod);

        const gen = b.addExecutable(.{
            .name = b.fmt("zbuf-gen-all-{d}", .{idx}),
            .root_module = b.createModule(.{
                .root_source_file = b.path("zbuf/gen.zig"),
                .target = target,
                .optimize = optimize,
            }),
        });
        gen.root_module.addImport("schema", schema_mod);
        gen.root_module.addImport("buffers", buffers_mod);

        const run_gen = b.addRunArtifact(gen);
        run_gen.addArg(out_path);
        step.dependOn(&run_gen.step);
    }
    return step;
}

fn collectBufferSchemas(b: *std.Build) [][]const u8 {
    var paths = std.ArrayList([]const u8).empty;
    var root_dir = std.fs.cwd().openDir(".", .{ .iterate = true }) catch @panic("failed to open workspace dir");
    defer root_dir.close();
    var walker = root_dir.walk(b.allocator) catch @panic("failed to walk workspace");
    defer walker.deinit();

    while (walker.next() catch @panic("failed to read workspace entry")) |entry| {
        if (entry.kind != .file) continue;
        const base = std.fs.path.basename(entry.path);
        if (!std.mem.eql(u8, base, "buffers.zig") and !std.mem.eql(u8, base, "buffers")) continue;
        if (isSkippedPath(entry.path)) continue;
        const dup = b.allocator.dupe(u8, entry.path) catch @panic("out of memory collecting buffer schemas");
        paths.append(b.allocator, dup) catch @panic("out of memory collecting buffer schemas");
    }
    return paths.toOwnedSlice(b.allocator) catch @panic("out of memory collecting buffer schemas");
}

fn isSkippedPath(path: []const u8) bool {
    return std.mem.startsWith(u8, path, ".zig-cache/") or
        std.mem.startsWith(u8, path, ".git/") or
        std.mem.startsWith(u8, path, "zig-out/") or
        std.mem.startsWith(u8, path, "node_modules/");
}

fn addServer(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) void {
    const exe = b.addExecutable(.{
        .name = "server",
        .root_module = b.createModule(.{
            .root_source_file = b.path("server.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    b.installArtifact(exe);

    const run_cmd = b.addRunArtifact(exe);
    run_cmd.step.dependOn(b.getInstallStep());
    if (b.args) |args| run_cmd.addArgs(args);

    const run_step = b.step("run", "Run the server");
    run_step.dependOn(&run_cmd.step);
}

fn addSchemaModule(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode) *std.Build.Module {
    return b.createModule(.{
        .root_source_file = b.path("zbuf/schema.zig"),
        .target = target,
        .optimize = optimize,
    });
}

fn addZbufCli(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, schema_mod: *std.Build.Module) void {
    // Zbuf CLI: zig build zbuf -- path/to/buffers.zig
    // Defaults to u/buffers.zig if no args are provided.
    const buffers_path = if (b.args) |args| blk: {
        if (args.len > 0) break :blk args[0];
        break :blk "u/buffers.zig";
    } else "u/buffers.zig";

    const out_path = blk: {
        if (std.mem.endsWith(u8, buffers_path, ".zig")) {
            break :blk b.fmt("{s}.gen.zig", .{buffers_path[0 .. buffers_path.len - 4]});
        }
        break :blk b.fmt("{s}.gen.zig", .{buffers_path});
    };

    const buffers_dyn_mod = b.createModule(.{
        .root_source_file = b.path(buffers_path),
        .target = target,
        .optimize = optimize,
    });
    buffers_dyn_mod.addImport("schema", schema_mod);

    const zbuf_gen_dyn = b.addExecutable(.{
        .name = "zbuf-gen-dyn",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zbuf/gen.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    zbuf_gen_dyn.root_module.addImport("schema", schema_mod);
    zbuf_gen_dyn.root_module.addImport("buffers", buffers_dyn_mod);

    const run_zbuf_gen_dyn = b.addRunArtifact(zbuf_gen_dyn);
    run_zbuf_gen_dyn.addArg(out_path);

    const zbuf_cli_step = b.step("zbuf", "Generate buffers code from a schema path");
    zbuf_cli_step.dependOn(&run_zbuf_gen_dyn.step);
}

fn addZbufTests(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, schema_mod: *std.Build.Module) *std.Build.Step {
    const zbuf_mod = b.createModule(.{
        .root_source_file = b.path("zbuf.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Test schema and generated code live under zbuf/.
    const test_buffers_mod = b.createModule(.{
        .root_source_file = b.path("zbuf/buffers.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_buffers_mod.addImport("schema", schema_mod);

    const test_zbuf_gen = b.addExecutable(.{
        .name = "zbuf-gen-test",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zbuf/gen.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    test_zbuf_gen.root_module.addImport("schema", schema_mod);
    test_zbuf_gen.root_module.addImport("buffers", test_buffers_mod);

    const run_test_zbuf_gen = b.addRunArtifact(test_zbuf_gen);
    run_test_zbuf_gen.addArg("zbuf/buffers.gen.zig");

    const buffers_gen_mod = b.createModule(.{
        .root_source_file = b.path("zbuf/buffers.gen.zig"),
        .target = target,
        .optimize = optimize,
    });
    buffers_gen_mod.addImport("zbuf", zbuf_mod);

    const zbuf_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("zbuf/buffers_test.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    zbuf_tests.root_module.addImport("buffers_gen", buffers_gen_mod);
    zbuf_tests.root_module.addImport("zbuf", zbuf_mod);
    zbuf_tests.step.dependOn(&run_test_zbuf_gen.step);

    const run_zbuf_tests = b.addRunArtifact(zbuf_tests);

    const zbuf_negative_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("zbuf/negative_test.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_zbuf_negative_tests = b.addRunArtifact(zbuf_negative_tests);

    const zbuf_test_step = b.step("test-zbuf", "Run zbuf serialization tests");
    zbuf_test_step.dependOn(&run_zbuf_tests.step);
    zbuf_test_step.dependOn(&run_zbuf_negative_tests.step);
    return zbuf_test_step;
}

fn addRaftTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    schema_mod: *std.Build.Module,
) *std.Build.Step {
    const zbuf_mod = b.createModule(.{
        .root_source_file = b.path("zbuf.zig"),
        .target = target,
        .optimize = optimize,
    });
    const raft_buffers_mod = b.createModule(.{
        .root_source_file = b.path("raft/buffers.zig"),
        .target = target,
        .optimize = optimize,
    });
    raft_buffers_mod.addImport("schema", schema_mod);
    const raft_gen = b.addExecutable(.{
        .name = "zbuf-gen-raft",
        .root_module = b.createModule(.{
            .root_source_file = b.path("zbuf/gen.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    raft_gen.root_module.addImport("schema", schema_mod);
    raft_gen.root_module.addImport("buffers", raft_buffers_mod);
    const run_raft_gen = b.addRunArtifact(raft_gen);
    run_raft_gen.addArg("raft/buffers.gen.zig");

    const raft_buffers_gen_mod = b.createModule(.{
        .root_source_file = b.path("raft/buffers.gen.zig"),
        .target = target,
        .optimize = optimize,
    });
    raft_buffers_gen_mod.addImport("zbuf", zbuf_mod);

    const raft_mod = b.createModule(.{
        .root_source_file = b.path("raft/raft.zig"),
        .target = target,
        .optimize = optimize,
    });
    raft_mod.addImport("buffers_gen", raft_buffers_gen_mod);
    raft_mod.addImport("zbuf", zbuf_mod);

    const raft_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("raft/testing/tests.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    raft_tests.root_module.addImport("raft", raft_mod);
    raft_tests.step.dependOn(&run_raft_gen.step);
    const run_raft_tests = b.addRunArtifact(raft_tests);
    const run_raft_tests_trace = b.addRunArtifact(raft_tests);
    run_raft_tests_trace.stdio = .inherit;

    const raft_test_step = b.step("test-raft", "Run raft tests");
    raft_test_step.dependOn(&run_raft_tests.step);

    const raft_test_trace_step = b.step("test-raft-trace", "Run raft tests with raft debug logs");
    raft_test_trace_step.dependOn(&run_raft_tests_trace.step);
    return raft_test_step;
}
