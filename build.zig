const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    addServer(b, target, optimize);

    const schema_mod = addSchemaModule(b, target, optimize);

    addZbufCli(b, target, optimize, schema_mod);
    addZbufTests(b, target, optimize, schema_mod);
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

fn addZbufTests(b: *std.Build, target: std.Build.ResolvedTarget, optimize: std.builtin.OptimizeMode, schema_mod: *std.Build.Module) void {
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
    const zbuf_test_step = b.step("test-zbuf", "Run zbuf serialization tests");
    zbuf_test_step.dependOn(&run_zbuf_tests.step);
}
