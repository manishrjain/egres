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

    // Wire code generation
    const wire_gen = b.addExecutable(.{
        .name = "wire-gen",
        .root_module = b.createModule(.{
            .root_source_file = b.path("wire/gen.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    const run_wire_gen = b.addRunArtifact(wire_gen);
    run_wire_gen.addArg("wire/generated.gen.zig");
    const gen_wire_step = b.step("gen-wire", "Generate wire code");
    gen_wire_step.dependOn(&run_wire_gen.step);

    const wire_tests = b.addTest(.{
        .root_module = b.createModule(.{
            .root_source_file = b.path("wire/test_wire.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    wire_tests.step.dependOn(&run_wire_gen.step);
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
