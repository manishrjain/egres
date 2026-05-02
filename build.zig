const std = @import("std");

const RaftTestSteps = struct {
    default_step: *std.Build.Step,
    all_step: *std.Build.Step,
};

fn bigTestNameLessThan(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

fn discoverBigTestNames(b: *std.Build) []const []const u8 {
    const result = std.process.Child.run(.{
        .allocator = b.allocator,
        .argv = &.{
            "rg",
            "-n",
            "--no-heading",
            "--glob",
            "*.zig",
            "-F",
            "test \"BIGTEST ",
            ".",
        },
    }) catch |err| switch (err) {
        error.FileNotFound => @panic("missing required tool `rg` in PATH for BIGTEST discovery"),
        else => @panic("failed to execute `rg` for BIGTEST discovery"),
    };
    defer b.allocator.free(result.stdout);
    defer b.allocator.free(result.stderr);

    const ok = switch (result.term) {
        .Exited => |code| code == 0,
        else => false,
    };
    if (!ok) {
        std.debug.print("rg BIGTEST discovery failed:\n{s}\n", .{result.stderr});
        @panic("failed to discover BIGTEST names with `rg`");
    }

    var seen = std.StringHashMap(void).init(b.allocator);
    defer seen.deinit();

    var names = std.ArrayList([]const u8).empty;
    var lines = std.mem.splitScalar(u8, result.stdout, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;

        const marker = "test \"";
        const marker_pos = std.mem.indexOf(u8, line, marker) orelse continue;
        const name_start = marker_pos + marker.len;
        const tail = line[name_start..];
        const name_end = std.mem.indexOfScalar(u8, tail, '"') orelse continue;
        const name = tail[0..name_end];
        if (!std.mem.startsWith(u8, name, "BIGTEST ")) continue;
        if (seen.contains(name)) continue;

        const dup = b.allocator.dupe(u8, name) catch @panic("OOM duplicating BIGTEST name");
        names.append(b.allocator, dup) catch @panic("OOM storing BIGTEST names");
        seen.put(dup, {}) catch @panic("OOM indexing BIGTEST names");
    }

    if (names.items.len == 0) {
        @panic("no BIGTEST names discovered via `rg`; check tests naming convention");
    }

    std.mem.sort([]const u8, names.items, {}, bigTestNameLessThan);
    return names.toOwnedSlice(b.allocator) catch @panic("OOM finalizing BIGTEST list");
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    // `-Drelease` is a shortcut for `-Doptimize=ReleaseFast`. Tests + the
    // default build still go through `standardOptimizeOption` (i.e. Debug
    // unless explicitly overridden), so attaching a debugger or running
    // safety-checked tests stays the path of least resistance. The bench-*
    // steps that hardcode `.ReleaseFast` in their own `addExecutable` are
    // unaffected.
    const release_mode = b.option(bool, "release", "Shortcut for -Doptimize=ReleaseFast") orelse false;
    const optimize: std.builtin.OptimizeMode = if (release_mode)
        .ReleaseFast
    else
        b.standardOptimizeOption(.{});

    const schema_mod = makeSchemaModule(b, target, optimize);
    const regen_buffers_step = addRegenAllBuffers(b, target, optimize, schema_mod);
    const raft_mod = makeRaftModule(b, target, optimize);

    addZbufCli(b, target, optimize, schema_mod);

    addZdbServer(b, target, optimize, raft_mod, regen_buffers_step);
    addZdbBench(b, target, optimize);
    addLoadtestDriver(b, target, optimize);
    const zdb_test_step = addZdbTests(b, target, optimize, regen_buffers_step);
    const utils_test_step = addUCompressTests(b, target, optimize);
    const zbuf_test_step = addZbufTests(b, target, optimize, regen_buffers_step);
    const raft_test_steps = addRaftTests(b, target, optimize, regen_buffers_step, raft_mod);
    // TODO: rewrite multiproc harness to use zdb-server
    _ = addRaftMultiprocHarness(b, target, optimize, raft_mod, regen_buffers_step);
    addBenchZio(b, target, optimize);
    addBenchDiskwal(b, target, optimize, raft_mod, regen_buffers_step);
    addBenchQueue(b, target, optimize, raft_mod);
    addBenchCrc(b, target, optimize);
    const net_test_step = addNetTests(b, target, optimize);

    const all_test_step = b.step("test", "Run all tests");
    all_test_step.dependOn(regen_buffers_step);
    all_test_step.dependOn(zdb_test_step);
    all_test_step.dependOn(utils_test_step);
    all_test_step.dependOn(zbuf_test_step);
    all_test_step.dependOn(raft_test_steps.all_step);
    // TODO: rewrite multiproc harness to use zdb-server
    // all_test_step.dependOn(multiproc_step);
    all_test_step.dependOn(net_test_step);
}

fn makeModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    root_source_file: []const u8,
) *std.Build.Module {
    return b.createModule(.{
        .root_source_file = b.path(root_source_file),
        .target = target,
        .optimize = optimize,
    });
}

fn makeSchemaModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Module {
    return makeModule(b, target, optimize, "zbuf/schema.zig");
}

fn generatedPathForSchema(b: *std.Build, schema_path: []const u8) []const u8 {
    if (std.mem.endsWith(u8, schema_path, ".zig")) {
        return b.fmt("{s}.gen.zig", .{schema_path[0 .. schema_path.len - 4]});
    }
    return b.fmt("{s}.gen.zig", .{schema_path});
}

fn addZbufGenRun(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    schema_mod: *std.Build.Module,
    buffers_path: []const u8,
    out_path: []const u8,
    exe_name: []const u8,
) *std.Build.Step {
    const buffers_mod = makeModule(b, target, optimize, buffers_path);
    buffers_mod.addImport("schema", schema_mod);

    const gen = b.addExecutable(.{
        .name = exe_name,
        .root_module = makeModule(b, target, optimize, "zbuf/gen.zig"),
    });
    gen.root_module.addImport("schema", schema_mod);
    gen.root_module.addImport("buffers", buffers_mod);

    const run_gen = b.addRunArtifact(gen);
    run_gen.addArg(out_path);

    const fmt_gen = b.addFmt(.{
        .paths = &.{out_path},
    });
    fmt_gen.step.dependOn(&run_gen.step);
    return &fmt_gen.step;
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
        const out_path = generatedPathForSchema(b, schema_path);
        const run_step = addZbufGenRun(
            b,
            target,
            optimize,
            schema_mod,
            schema_path,
            out_path,
            b.fmt("zbuf-gen-all-{d}", .{idx}),
        );
        step.dependOn(run_step);
    }
    return step;
}

fn collectBufferSchemas(b: *std.Build) [][]const u8 {
    var paths = std.ArrayList([]const u8).empty;
    var root_dir = std.fs.cwd().openDir(".", .{ .iterate = true }) catch {
        @panic("failed to open workspace dir");
    };
    defer root_dir.close();

    var walker = root_dir.walk(b.allocator) catch @panic("failed to walk workspace");
    defer walker.deinit();

    while (walker.next() catch @panic("failed to read workspace entry")) |entry| {
        if (entry.kind != .file) continue;
        const base = std.fs.path.basename(entry.path);
        if (!std.mem.eql(u8, base, "buffers.zig") and !std.mem.eql(u8, base, "buffers")) continue;
        if (isSkippedPath(entry.path)) continue;

        const dup = b.allocator.dupe(u8, entry.path) catch {
            @panic("out of memory collecting buffer schemas");
        };
        paths.append(b.allocator, dup) catch {
            @panic("out of memory collecting buffer schemas");
        };
    }
    return paths.toOwnedSlice(b.allocator) catch {
        @panic("out of memory collecting buffer schemas");
    };
}

fn isSkippedPath(path: []const u8) bool {
    return std.mem.startsWith(u8, path, ".zig-cache/") or
        std.mem.startsWith(u8, path, ".git/") or
        std.mem.startsWith(u8, path, "zig-out/") or
        std.mem.startsWith(u8, path, "node_modules/");
}

fn addZbufCli(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    schema_mod: *std.Build.Module,
) void {
    // Zbuf CLI: zig build zbuf -- path/to/buffers.zig
    // Defaults to zbuf/buffers.zig (the test/example schema) if no args.
    const buffers_path = if (b.args) |args| blk: {
        if (args.len > 0) break :blk args[0];
        break :blk "zbuf/buffers.zig";
    } else "zbuf/buffers.zig";

    const out_path = generatedPathForSchema(b, buffers_path);
    const run_gen_step = addZbufGenRun(
        b,
        target,
        optimize,
        schema_mod,
        buffers_path,
        out_path,
        "zbuf-gen-dyn",
    );

    const zbuf_cli_step = b.step("zbuf", "Generate buffers code from a schema path");
    zbuf_cli_step.dependOn(run_gen_step);
}

fn addZbufTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    regen_buffers_step: *std.Build.Step,
) *std.Build.Step {
    const zbuf_mod = makeModule(b, target, optimize, "zbuf.zig");

    const buffers_gen_mod = makeModule(b, target, optimize, "zbuf/buffers.gen.zig");
    buffers_gen_mod.addImport("zbuf", zbuf_mod);

    const zbuf_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zbuf/buffers_test.zig"),
    });
    zbuf_tests.root_module.addImport("buffers_gen", buffers_gen_mod);
    zbuf_tests.root_module.addImport("zbuf", zbuf_mod);
    zbuf_tests.step.dependOn(regen_buffers_step);

    const zbuf_negative_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zbuf/negative_test.zig"),
    });
    addVendoredCompressionSources(b, zbuf_tests);
    addVendoredCompressionSources(b, zbuf_negative_tests);

    const zbuf_test_step = b.step("test-zbuf", "Run zbuf serialization tests");
    zbuf_test_step.dependOn(regen_buffers_step);
    zbuf_test_step.dependOn(&b.addRunArtifact(zbuf_tests).step);
    zbuf_test_step.dependOn(&b.addRunArtifact(zbuf_negative_tests).step);
    return zbuf_test_step;
}

fn addZdbTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    regen_buffers_step: *std.Build.Step,
) *std.Build.Step {
    const zbuf_mod = makeModule(b, target, optimize, "zbuf.zig");
    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });
    const zio_mod = zio_dep.module("zio");

    const metrics_dep = b.dependency("metrics", .{
        .target = target,
        .optimize = optimize,
    });
    const metrics_mod = metrics_dep.module("metrics");

    const utils_mod = makeModule(b, target, optimize, "u/utils.zig");
    utils_mod.addIncludePath(b.path("."));
    utils_mod.addIncludePath(b.path("third_party/lz4/lib"));
    utils_mod.addIncludePath(b.path("third_party/zstd/lib"));
    utils_mod.addImport("zio", zio_mod);

    const vfs_mod = makeModule(b, target, optimize, "vfs/vfs.zig");
    vfs_mod.addImport("zio", zio_mod);

    const zdb_mod = makeModule(b, target, optimize, "zdb.zig");
    zdb_mod.addImport("zbuf", zbuf_mod);
    zdb_mod.addImport("utils", utils_mod);
    zdb_mod.addImport("zio", zio_mod);
    zdb_mod.addImport("vfs", vfs_mod);
    zdb_mod.addImport("metrics", metrics_mod);

    const zdb_core_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zdb/core_test.zig"),
    });
    zdb_core_tests.root_module.addImport("zdb", zdb_mod);
    zdb_core_tests.step.dependOn(regen_buffers_step);

    const zdb_skl_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zdb/skl/test.zig"),
    });
    zdb_skl_tests.root_module.addImport("zdb", zdb_mod);
    zdb_skl_tests.root_module.addImport("utils", utils_mod);
    zdb_skl_tests.step.dependOn(regen_buffers_step);

    const zdb_table_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zdb/table/test.zig"),
    });
    addVendoredCompressionSources(b, zdb_core_tests);
    addVendoredCompressionSources(b, zdb_skl_tests);
    addVendoredCompressionSources(b, zdb_table_tests);
    zdb_table_tests.root_module.addImport("zdb", zdb_mod);
    zdb_table_tests.step.dependOn(regen_buffers_step);

    const zdb_manifest_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zdb/manifest_test.zig"),
    });
    addVendoredCompressionSources(b, zdb_manifest_tests);
    zdb_manifest_tests.root_module.addImport("zdb", zdb_mod);
    zdb_manifest_tests.step.dependOn(regen_buffers_step);

    const zdb_levels_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zdb/levels_test.zig"),
    });
    addVendoredCompressionSources(b, zdb_levels_tests);
    zdb_levels_tests.root_module.addImport("zdb", zdb_mod);
    zdb_levels_tests.step.dependOn(regen_buffers_step);

    const zdb_forest_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zdb/forest_test.zig"),
    });
    addVendoredCompressionSources(b, zdb_forest_tests);
    zdb_forest_tests.root_module.addImport("zdb", zdb_mod);
    zdb_forest_tests.step.dependOn(regen_buffers_step);

    const zdb_cache_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zdb/cache.zig"),
    });
    zdb_cache_tests.root_module.addImport("utils", utils_mod);

    zdb_levels_tests.root_module.addImport("zio", zio_mod);
    zdb_levels_tests.root_module.addImport("utils", utils_mod);
    zdb_forest_tests.root_module.addImport("zio", zio_mod);
    zdb_forest_tests.root_module.addImport("utils", utils_mod);

    const zdb_vfs_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "vfs/vfs_test.zig"),
    });
    zdb_vfs_tests.root_module.addImport("zio", zio_mod);
    zdb_vfs_tests.linkLibC();

    const zdb_bw_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "vfs/bandwidth_test.zig"),
    });
    zdb_bw_tests.root_module.addImport("zio", zio_mod);
    zdb_bw_tests.linkLibC();

    const proto_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "zdb-server/protocol_test.zig"),
    });
    proto_tests.root_module.addImport("zio", zio_mod);

    const zdb_test_step = b.step("test-zdb", "Run zdb core tests");
    zdb_test_step.dependOn(&b.addRunArtifact(zdb_core_tests).step);
    zdb_test_step.dependOn(&b.addRunArtifact(zdb_skl_tests).step);
    zdb_test_step.dependOn(&b.addRunArtifact(zdb_table_tests).step);
    zdb_test_step.dependOn(&b.addRunArtifact(zdb_manifest_tests).step);
    zdb_test_step.dependOn(&b.addRunArtifact(zdb_levels_tests).step);
    zdb_test_step.dependOn(&b.addRunArtifact(zdb_forest_tests).step);
    zdb_test_step.dependOn(&b.addRunArtifact(zdb_cache_tests).step);
    zdb_test_step.dependOn(&b.addRunArtifact(zdb_vfs_tests).step);
    zdb_test_step.dependOn(&b.addRunArtifact(zdb_bw_tests).step);
    zdb_test_step.dependOn(&b.addRunArtifact(proto_tests).step);
    return zdb_test_step;
}

fn addUCompressTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step {
    const utils_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "u/tests.zig"),
    });
    addVendoredCompressionSources(b, utils_tests);

    const step = b.step("test-utils", "Run utility tests");
    step.dependOn(&b.addRunArtifact(utils_tests).step);
    return step;
}

fn addDiskwalTestRun(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    raft_mod: *std.Build.Module,
    build_options_mod: *std.Build.Module,
    regen_buffers_step: *std.Build.Step,
    filters: []const []const u8,
) *std.Build.Step {
    const test_artifact = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "raft/diskwal_test.zig"),
        .filters = filters,
    });
    addVendoredCompressionSources(b, test_artifact);
    test_artifact.root_module.addImport("raft", raft_mod);
    test_artifact.root_module.addImport("vfs", raft_mod.import_table.get("vfs").?);
    test_artifact.root_module.addImport("zio", raft_mod.import_table.get("zio").?);
    test_artifact.root_module.addImport("zbuf", raft_mod.import_table.get("zbuf").?);
    test_artifact.root_module.addImport("build_options", build_options_mod);
    test_artifact.step.dependOn(regen_buffers_step);
    return &b.addRunArtifact(test_artifact).step;
}

fn makeRaftModule(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Module {
    const zbuf_mod = makeModule(b, target, optimize, "zbuf.zig");

    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });
    const zio_mod = zio_dep.module("zio");

    const utils_mod = makeModule(b, target, optimize, "u/utils.zig");
    utils_mod.addImport("zio", zio_mod);

    const raft_buffers_gen_mod = makeModule(b, target, optimize, "raft/buffers.gen.zig");
    raft_buffers_gen_mod.addImport("zbuf", zbuf_mod);

    const serial_mod = makeModule(b, target, optimize, "net/serial.zig");
    serial_mod.addImport("zio", zio_mod);
    serial_mod.addImport("utils", utils_mod);

    const transport_mod = makeModule(b, target, optimize, "net/transport.zig");
    transport_mod.addImport("zio", zio_mod);
    transport_mod.addImport("serial", serial_mod);
    transport_mod.addImport("utils", utils_mod);

    const vfs_mod = makeModule(b, target, optimize, "vfs/vfs.zig");
    vfs_mod.addImport("zio", zio_mod);

    // karlseguin/metrics — shared with zdb_mod via the same b.dependency call;
    // the build system de-duplicates so we don't get two compiled copies.
    const metrics_dep = b.dependency("metrics", .{
        .target = target,
        .optimize = optimize,
    });
    const metrics_mod = metrics_dep.module("metrics");

    const raft_mod = makeModule(b, target, optimize, "raft/raft.zig");
    raft_mod.addImport("buffers_gen", raft_buffers_gen_mod);
    raft_mod.addImport("zbuf", zbuf_mod);
    raft_mod.addImport("utils", utils_mod);
    raft_mod.addImport("zio", zio_mod);
    raft_mod.addImport("serial", serial_mod);
    raft_mod.addImport("transport", transport_mod);
    raft_mod.addImport("vfs", vfs_mod);
    raft_mod.addImport("metrics", metrics_mod);
    return raft_mod;
}

fn addZdbServer(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    raft_mod: *std.Build.Module,
    regen_buffers_step: *std.Build.Step,
) void {
    // Reuse modules already created inside raft_mod to avoid duplicate-file errors.
    const zio_mod = raft_mod.import_table.get("zio").?;
    const zbuf_mod = raft_mod.import_table.get("zbuf").?;
    const utils_mod = raft_mod.import_table.get("utils").?;
    // utils needs C include paths for compress.zig (lz4/zstd headers).
    utils_mod.addIncludePath(b.path("."));
    utils_mod.addIncludePath(b.path("third_party/lz4/lib"));
    utils_mod.addIncludePath(b.path("third_party/zstd/lib"));

    const runtime_mod = makeModule(b, target, optimize, "runtime.zig");
    runtime_mod.addImport("zio", zio_mod);

    const vfs_mod = raft_mod.import_table.get("vfs").?;

    const metrics_dep = b.dependency("metrics", .{
        .target = target,
        .optimize = optimize,
    });
    const metrics_mod = metrics_dep.module("metrics");

    const zdb_mod = makeModule(b, target, optimize, "zdb.zig");
    zdb_mod.addImport("zbuf", zbuf_mod);
    zdb_mod.addImport("utils", utils_mod);
    zdb_mod.addImport("zio", zio_mod);
    zdb_mod.addImport("vfs", vfs_mod);
    zdb_mod.addImport("metrics", metrics_mod);

    const zdb_server_mod = makeModule(b, target, optimize, "zdb-server/server.zig");
    zdb_server_mod.addImport("zio", zio_mod);
    zdb_server_mod.addImport("raft", raft_mod);
    zdb_server_mod.addImport("zdb", zdb_mod);
    zdb_server_mod.addImport("runtime", runtime_mod);
    zdb_server_mod.addImport("metrics", metrics_mod);

    // `flags` is its own module with no transitive deps; needed by main.zig
    // for CLI parsing (single quoted `--node="k=v; k=v"` payload form).
    const flags_mod = makeModule(b, target, optimize, "u/flags.zig");

    const exe = b.addExecutable(.{
        .name = "zdb-server",
        .root_module = makeModule(b, target, optimize, "zdb-server/main.zig"),
    });
    addVendoredCompressionSources(b, exe);
    exe.root_module.addImport("zio", zio_mod);
    exe.root_module.addImport("raft", raft_mod);
    exe.root_module.addImport("zdb", zdb_mod);
    exe.root_module.addImport("runtime", runtime_mod);
    exe.root_module.addImport("zdb_server", zdb_server_mod);
    exe.root_module.addImport("metrics", metrics_mod);
    exe.root_module.addImport("flags", flags_mod);
    exe.step.dependOn(regen_buffers_step);
    b.installArtifact(exe);

    const install = b.addInstallArtifact(exe, .{});
    const zdb_server_step = b.step("zdb-server", "Build zdb server binary");
    zdb_server_step.dependOn(&install.step);
}

fn addZdbBench(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });
    const zio_mod = zio_dep.module("zio");

    const flags_mod = makeModule(b, target, optimize, "u/flags.zig");

    const exe = b.addExecutable(.{
        .name = "zdb-bench",
        .root_module = makeModule(b, target, optimize, "benchmark.zig"),
    });
    exe.root_module.addImport("zio", zio_mod);
    exe.root_module.addImport("flags", flags_mod);
    b.installArtifact(exe);

    const install = b.addInstallArtifact(exe, .{});
    const step = b.step("zdb-bench", "Build zdb YCSB benchmark client");
    step.dependOn(&install.step);
}

fn addLoadtestDriver(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });
    const zio_mod = zio_dep.module("zio");

    // `flags` is its own module — it only depends on std, so no need to
    // rope it through utils_mod (which would pull in lz4/zstd headers).
    const flags_mod = makeModule(b, target, optimize, "u/flags.zig");

    const exe = b.addExecutable(.{
        .name = "loadtest",
        .root_module = makeModule(b, target, optimize, "scripts/loadtest.zig"),
    });
    exe.root_module.addImport("zio", zio_mod);
    exe.root_module.addImport("flags", flags_mod);
    b.installArtifact(exe);

    const install = b.addInstallArtifact(exe, .{});
    const step = b.step("loadtest", "Build the zdb load-test driver");
    step.dependOn(&install.step);
}

fn addRaftMultiprocHarness(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    raft_mod: *std.Build.Module,
    regen_buffers_step: *std.Build.Step,
) *std.Build.Step {
    const harness_exe = b.addExecutable(.{
        .name = "raft-multiproc-harness",
        .root_module = makeModule(b, target, optimize, "raft/testing/multiproc_harness.zig"),
    });
    addVendoredCompressionSources(b, harness_exe);
    harness_exe.root_module.addImport("raft", raft_mod);

    const run = b.addRunArtifact(harness_exe);
    run.step.dependOn(regen_buffers_step);
    run.step.dependOn(b.getInstallStep());
    run.addArg("zig-out/bin/raftd");

    const step = b.step(
        "test-raft-multiproc",
        "Run multi-process raft integration harness",
    );
    step.dependOn(&run.step);
    return step;
}

fn addRaftTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    regen_buffers_step: *std.Build.Step,
    raft_mod: *std.Build.Module,
) RaftTestSteps {
    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });

    const raft_tests = b.addTest(.{
        .root_module = makeModule(b, target, optimize, "raft/testing/tests.zig"),
    });
    addVendoredCompressionSources(b, raft_tests);
    raft_tests.root_module.addImport("raft", raft_mod);
    raft_tests.root_module.addImport("vfs", raft_mod.import_table.get("vfs").?);
    raft_tests.root_module.addImport("zio", zio_dep.module("zio"));
    raft_tests.step.dependOn(regen_buffers_step);

    const run_raft_tests = b.addRunArtifact(raft_tests);
    const run_raft_tests_trace = b.addRunArtifact(raft_tests);
    run_raft_tests_trace.stdio = .inherit;

    const raft_small_opts = b.addOptions();
    raft_small_opts.addOption(bool, "enable_bigtests", false);
    const raft_all_opts = b.addOptions();
    raft_all_opts.addOption(bool, "enable_bigtests", true);

    const run_diskwal_tests = addDiskwalTestRun(
        b,
        target,
        optimize,
        raft_mod,
        raft_small_opts.createModule(),
        regen_buffers_step,
        &.{},
    );
    const all_bigtests_opts_mod = raft_all_opts.createModule();
    const bigtest_names = discoverBigTestNames(b);

    const raft_test_step = b.step("test-raft", "Run raft tests");
    raft_test_step.dependOn(regen_buffers_step);
    raft_test_step.dependOn(&run_raft_tests.step);
    raft_test_step.dependOn(run_diskwal_tests);

    const raft_test_all_step = b.step("test-raft-all", "Run raft tests including BIGTEST cases");
    raft_test_all_step.dependOn(regen_buffers_step);
    raft_test_all_step.dependOn(&run_raft_tests.step);
    raft_test_all_step.dependOn(run_diskwal_tests);
    for (bigtest_names) |name| {
        const one_filter = b.allocator.alloc([]const u8, 1) catch {
            @panic("OOM allocating BIGTEST filter slice");
        };
        one_filter[0] = name;
        // TODO: For now, we're only doing BIGTEST here. Later, we can expand to other modules.
        // rg would help easily expand to those modules, so let's keep it for now.
        const run_big = addDiskwalTestRun(
            b,
            target,
            optimize,
            raft_mod,
            all_bigtests_opts_mod,
            regen_buffers_step,
            one_filter,
        );
        raft_test_all_step.dependOn(run_big);
    }

    const raft_test_trace_step = b.step("test-raft-trace", "Run raft tests with raft debug logs");
    raft_test_trace_step.dependOn(&run_raft_tests_trace.step);

    return .{
        .default_step = raft_test_step,
        .all_step = raft_test_all_step,
    };
}

fn addBenchZio(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });
    const zio_mod = zio_dep.module("zio");

    const bench_exe = b.addExecutable(.{
        .name = "zio-bench",
        .root_module = makeModule(b, target, .ReleaseFast, "benchmarks/zio_bench.zig"),
    });
    bench_exe.root_module.addImport("zio", zio_mod);
    bench_exe.linkLibC();

    const run_bench = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench-zio", "Run zio vs std.Thread benchmarks");
    bench_step.dependOn(&run_bench.step);
}

fn addBenchCrc(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) void {
    _ = optimize;
    const bench_exe = b.addExecutable(.{
        .name = "crc-bench",
        // Always ReleaseFast — Debug bounds checks dominate the inner
        // loop and would make the throughput numbers meaningless.
        .root_module = makeModule(b, target, .ReleaseFast, "benchmarks/crc_bench.zig"),
    });

    const run_bench = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench-crc", "Run hash throughput benchmark (CRC32C vs xxhash3)");
    bench_step.dependOn(&run_bench.step);
}

fn addBenchQueue(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    raft_mod: *std.Build.Module,
) void {
    // Reuse zio + utils modules already wired through raft_mod so we don't
    // recompile them — same trick as bench-diskwal.
    const zio_mod = raft_mod.import_table.get("zio").?;
    const utils_mod = raft_mod.import_table.get("utils").?;

    const bench_exe = b.addExecutable(.{
        .name = "queue-bench",
        // Always ReleaseFast — Debug Mutex/Condvar overhead swamps the
        // measurement, and we're comparing fast paths.
        .root_module = makeModule(b, target, .ReleaseFast, "benchmarks/queue_bench.zig"),
    });
    bench_exe.root_module.addImport("zio", zio_mod);
    bench_exe.root_module.addImport("utils", utils_mod);
    _ = optimize;

    const run_bench = b.addRunArtifact(bench_exe);
    const bench_step = b.step("bench-queue", "Run utils.Queue vs zio.Channel benchmark");
    bench_step.dependOn(&run_bench.step);
}

fn addBenchDiskwal(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    raft_mod: *std.Build.Module,
    regen_buffers_step: *std.Build.Step,
) void {
    // Reuse modules already wired through raft_mod so we don't recompile
    // vfs/zio from scratch and get duplicate-file errors.
    const zio_mod = raft_mod.import_table.get("zio").?;
    const vfs_mod = raft_mod.import_table.get("vfs").?;

    const bench_exe = b.addExecutable(.{
        .name = "diskwal-bench",
        // Always ReleaseFast — a Debug WAL bench is useless (allocator
        // overhead swamps the measurement).
        .root_module = makeModule(b, target, .ReleaseFast, "benchmarks/diskwal_bench.zig"),
    });
    bench_exe.root_module.addImport("zio", zio_mod);
    bench_exe.root_module.addImport("raft", raft_mod);
    bench_exe.root_module.addImport("vfs", vfs_mod);
    bench_exe.step.dependOn(regen_buffers_step);
    b.installArtifact(bench_exe);
    _ = optimize;

    const run_bench = b.addRunArtifact(bench_exe);
    if (b.args) |fwd_args| run_bench.addArgs(fwd_args);
    const bench_step = b.step("bench-diskwal", "Run DiskWAL append/fsync benchmark");
    bench_step.dependOn(&run_bench.step);
}

fn addNetTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step {
    const zio_dep = b.dependency("zio", .{
        .target = target,
        .optimize = optimize,
    });
    const zio_mod = zio_dep.module("zio");

    const utils_mod_test = makeModule(b, target, optimize, "u/utils.zig");
    utils_mod_test.addImport("zio", zio_mod);

    const serial_mod_test = makeModule(b, target, optimize, "net/serial.zig");
    serial_mod_test.addImport("zio", zio_mod);
    serial_mod_test.addImport("utils", utils_mod_test);

    const transport_mod = makeModule(b, target, optimize, "net/transport.zig");
    transport_mod.addImport("zio", zio_mod);
    transport_mod.addImport("serial", serial_mod_test);
    transport_mod.addImport("utils", utils_mod_test);

    // TODO: Rewrite transport_test.zig to use Serial instead of Mux.
    const net_test_step = b.step("test-net", "Run net tests");
    return net_test_step;
}

fn addVendoredCompressionSources(b: *std.Build, compile: *std.Build.Step.Compile) void {
    compile.linkLibC();
    compile.root_module.addIncludePath(b.path("."));
    compile.root_module.addIncludePath(b.path("third_party/lz4/lib"));
    compile.root_module.addIncludePath(b.path("third_party/zstd/lib"));

    compile.root_module.addCSourceFiles(.{
        .root = b.path("."),
        .files = &.{
            "third_party/lz4/lib/lz4.c",
            "u/compress_shim.c",
            "third_party/zstd/lib/common/debug.c",
            "third_party/zstd/lib/common/entropy_common.c",
            "third_party/zstd/lib/common/error_private.c",
            "third_party/zstd/lib/common/fse_decompress.c",
            "third_party/zstd/lib/common/pool.c",
            "third_party/zstd/lib/common/threading.c",
            "third_party/zstd/lib/common/xxhash.c",
            "third_party/zstd/lib/common/zstd_common.c",
            "third_party/zstd/lib/compress/fse_compress.c",
            "third_party/zstd/lib/compress/hist.c",
            "third_party/zstd/lib/compress/huf_compress.c",
            "third_party/zstd/lib/compress/zstd_compress.c",
            "third_party/zstd/lib/compress/zstd_compress_literals.c",
            "third_party/zstd/lib/compress/zstd_compress_sequences.c",
            "third_party/zstd/lib/compress/zstd_compress_superblock.c",
            "third_party/zstd/lib/compress/zstd_double_fast.c",
            "third_party/zstd/lib/compress/zstd_fast.c",
            "third_party/zstd/lib/compress/zstd_lazy.c",
            "third_party/zstd/lib/compress/zstd_ldm.c",
            "third_party/zstd/lib/compress/zstd_opt.c",
            "third_party/zstd/lib/compress/zstd_preSplit.c",
            "third_party/zstd/lib/compress/zstdmt_compress.c",
            "third_party/zstd/lib/decompress/huf_decompress.c",
            "third_party/zstd/lib/decompress/zstd_ddict.c",
            "third_party/zstd/lib/decompress/zstd_decompress.c",
            "third_party/zstd/lib/decompress/zstd_decompress_block.c",
        },
        .flags = &.{
            "-I.",
            "-Ithird_party/lz4/lib",
            "-Ithird_party/zstd/lib",
            "-DZSTD_MULTITHREAD=0",
            "-DZSTD_DISABLE_ASM=1",
        },
    });
}
