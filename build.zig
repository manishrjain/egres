const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

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

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const run_step = b.step("run", "Run the server");
    run_step.dependOn(&run_cmd.step);

    // Shared schema module for wire generation.
    const schema_mod = b.createModule(.{
        .root_source_file = b.path("wire/schema.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Wire CLI: zig build wire -- path/to/buffers.zig
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

    const wire_gen_dyn = b.addExecutable(.{
        .name = "wire-gen-dyn",
        .root_module = b.createModule(.{
            .root_source_file = b.path("wire/gen.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    wire_gen_dyn.root_module.addImport("schema", schema_mod);
    wire_gen_dyn.root_module.addImport("buffers", buffers_dyn_mod);
    const run_wire_gen_dyn = b.addRunArtifact(wire_gen_dyn);
    run_wire_gen_dyn.addArg(out_path);

    const wire_cli_step = b.step("wire", "Generate buffers code from a schema path");
    wire_cli_step.dependOn(&run_wire_gen_dyn.step);

    const buffers_mod = b.createModule(.{
        .root_source_file = b.path("u/buffers.zig"),
        .target = target,
        .optimize = optimize,
    });
    buffers_mod.addImport("schema", schema_mod);

    const wire_gen = b.addExecutable(.{
        .name = "wire-gen",
        .root_module = b.createModule(.{
            .root_source_file = b.path("wire/gen.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    wire_gen.root_module.addImport("buffers", buffers_mod);

    const run_wire_gen = b.addRunArtifact(wire_gen);
    run_wire_gen.addArg("u/buffers.gen.zig");
    const gen_wire_step = b.step("gen-wire", "Generate wire code");
    gen_wire_step.dependOn(&run_wire_gen.step);

    const wire_mod = b.createModule(.{
        .root_source_file = b.path("wire.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Test schema and generated code live under wire/.
    const test_buffers_mod = b.createModule(.{
        .root_source_file = b.path("wire/buffers.zig"),
        .target = target,
        .optimize = optimize,
    });
    test_buffers_mod.addImport("schema", schema_mod);

    const test_wire_gen = b.addExecutable(.{
        .name = "wire-gen-test",
        .root_module = b.createModule(.{
            .root_source_file = b.path("wire/gen.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    test_wire_gen.root_module.addImport("schema", schema_mod);
    test_wire_gen.root_module.addImport("buffers", test_buffers_mod);
    const run_test_wire_gen = b.addRunArtifact(test_wire_gen);
    run_test_wire_gen.addArg("wire/buffers.gen.zig");

    const buffers_gen_mod = b.createModule(.{
        .root_source_file = b.path("wire/buffers.gen.zig"),
        .target = target,
        .optimize = optimize,
    });
    buffers_gen_mod.addImport("wire", wire_mod);

    const wire_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("wire/buffers_test.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    wire_tests.root_module.addImport("buffers_gen", buffers_gen_mod);
    wire_tests.root_module.addImport("wire", wire_mod);
    wire_tests.step.dependOn(&run_test_wire_gen.step);
    const run_wire_tests = b.addRunArtifact(wire_tests);
    const wire_test_step = b.step("test-wire", "Run wire serialization tests");
    wire_test_step.dependOn(&run_wire_tests.step);

    // Flatbuffer roundtrip tests
    const fb_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("fb/test_raft_fb.zig"),
            .target = target,
            .optimize = optimize,
            .link_libc = true,
        }),
    });
    fb_tests.root_module.addIncludePath(b.path("fb/generated"));
    fb_tests.root_module.addIncludePath(b.path("fb/include"));
    fb_tests.addCSourceFiles(.{
        .files = &.{
            "fb/runtime/builder.c",
            "fb/runtime/emitter.c",
            "fb/runtime/refmap.c",
            "fb/runtime/verifier.c",
            "fb/raft_fb_shim.c",
        },
        .flags = &.{"-std=c11"},
        .root = b.path("."),
    });

    const run_fb_tests = b.addRunArtifact(fb_tests);
    const test_step = b.step("test-fb", "Run flatbuffer roundtrip tests");
    test_step.dependOn(&run_fb_tests.step);
}
