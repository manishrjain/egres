const std = @import("std");

/// Central registry of every singleton module in this build. Constructed
/// once in `makeModules` and threaded into every `addX` function. The
/// rule for membership: if a `*Module` is shared across more than one
/// consumer, it goes here. Per-call locals (e.g. the per-schema
/// `buffers_mod`, the zdb-server's `zdb_server_mod`, raft's private
/// `raft_buffers_gen` module) stay where they're created.
///
/// Field names are required to match the import names consumers use
/// (`@import("zbuf")` ↔ `mods.zbuf`). `addImports` enforces this via
/// `@field`, so a typo is a compile error rather than a runtime miss.
const Modules = struct {
    // External deps
    zio: *std.Build.Module,
    metrics: *std.Build.Module,

    // Schema / codegen
    schema: *std.Build.Module,            // zbuf/schema.zig (used by zbuf gen tool)
    buffers_gen: *std.Build.Module,       // zbuf/buffers.gen.zig

    // Infra
    flags: *std.Build.Module,             // u/flags.zig
    utils: *std.Build.Module,             // u/utils.zig (lz4/zstd C includes baked in)
    vfs: *std.Build.Module,               // vfs/vfs.zig
    runtime: *std.Build.Module,           // runtime.zig

    // Wire
    serial: *std.Build.Module,            // net/serial.zig
    transport: *std.Build.Module,         // net/transport.zig

    // Domain
    zbuf: *std.Build.Module,              // zbuf.zig
    raft: *std.Build.Module,              // raft/raft.zig
    zdb: *std.Build.Module,               // zdb.zig
    pgwire: *std.Build.Module,            // pgwire.zig (libpg_query include baked in)
};

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    // `-Drelease` is a shortcut for `-Doptimize=ReleaseFast`. Tests + the
    // default build still go through `standardOptimizeOption` (i.e. Debug
    // unless explicitly overridden), so attaching a debugger or running
    // safety-checked tests stays the path of least resistance. The bench
    // step (in addAllTests) hardcodes `.ReleaseFast` for its own root
    // module and is unaffected.
    const release_mode = b.option(bool, "release", "Shortcut for -Doptimize=ReleaseFast") orelse false;
    const optimize: std.builtin.OptimizeMode = if (release_mode)
        .ReleaseFast
    else
        b.standardOptimizeOption(.{});

    const mods = makeModules(b, target, optimize);
    const regen_buffers_step = addRegenAllBuffers(b, target, optimize, mods);

    addZbufCli(b, target, optimize, mods);
    addZdbServer(b, target, optimize, mods, regen_buffers_step);
    addZdbBench(b, target, optimize, mods);
    addLoadtestDriver(b, target, optimize, mods);
    addSstVerify(b, target, optimize, mods);
    // `zig build test` — one binary, auto-discovered from `*_test.zig` /
    // `test.zig` files. Replaces the previous test-zdb / test-zbuf /
    // test-utils / test-raft / test-net per-file plumbing.
    _ = addAllTests(b, target, optimize, mods, regen_buffers_step);
    // TODO: rewrite multiproc harness to use zdb-server
    _ = addRaftMultiprocHarness(b, target, optimize, mods, regen_buffers_step);
    addPgwireShakeout(b, target, optimize, mods);
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

fn makeModules(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
) Modules {
    // External deps — fetched once, shared across every consumer. b.dependency
    // memoizes per (name, args), so even repeated calls would resolve to the
    // same artifact, but pulling once keeps the dependency surface explicit.
    const zio = b.dependency("zio", .{ .target = target, .optimize = optimize }).module("zio");
    const metrics = b.dependency("metrics", .{ .target = target, .optimize = optimize }).module("metrics");

    // Schema / codegen
    const schema = makeModule(b, target, optimize, "zbuf/schema.zig");
    const zbuf = makeModule(b, target, optimize, "zbuf.zig");

    const buffers_gen = makeModule(b, target, optimize, "zbuf/buffers.gen.zig");
    buffers_gen.addImport("zbuf", zbuf);

    // Infra
    const utils = makeModule(b, target, optimize, "u/utils.zig");
    utils.addImport("zio", zio);
    // utils/compress.zig does @cImport for lz4/zstd. Bake the include paths
    // onto the module so every consumer gets them automatically — no more
    // "remember to call addIncludePath" footgun across multiple addX functions.
    utils.addIncludePath(b.path("."));
    utils.addIncludePath(b.path("third_party/lz4/lib"));
    utils.addIncludePath(b.path("third_party/zstd/lib"));

    const flags = makeModule(b, target, optimize, "u/flags.zig");
    flags.addImport("zio", zio);

    const vfs = makeModule(b, target, optimize, "vfs/vfs.zig");
    vfs.addImport("zio", zio);

    const runtime = makeModule(b, target, optimize, "runtime.zig");
    runtime.addImport("zio", zio);

    // Wire
    const serial = makeModule(b, target, optimize, "net/serial.zig");
    serial.addImport("zio", zio);
    serial.addImport("utils", utils);

    const transport = makeModule(b, target, optimize, "net/transport.zig");
    transport.addImport("zio", zio);
    transport.addImport("serial", serial);
    transport.addImport("utils", utils);

    // Domain. Note: raft has its own generated buffers (`raft/buffers.gen.zig`)
    // distinct from zbuf's (`zbuf/buffers.gen.zig`); both are imported as
    // `"buffers_gen"` by their respective consumers. That's the one place where
    // import name → field name doesn't roundtrip, so raft's copy stays a local
    // here rather than going on `Modules`.
    const raft_buffers_gen = makeModule(b, target, optimize, "raft/buffers.gen.zig");
    raft_buffers_gen.addImport("zbuf", zbuf);

    const raft = makeModule(b, target, optimize, "raft/raft.zig");
    raft.addImport("buffers_gen", raft_buffers_gen);
    raft.addImport("zbuf", zbuf);
    raft.addImport("utils", utils);
    raft.addImport("zio", zio);
    raft.addImport("serial", serial);
    raft.addImport("transport", transport);
    raft.addImport("vfs", vfs);
    raft.addImport("metrics", metrics);

    const zdb = makeModule(b, target, optimize, "zdb.zig");
    zdb.addImport("zbuf", zbuf);
    zdb.addImport("utils", utils);
    zdb.addImport("zio", zio);
    zdb.addImport("vfs", vfs);
    zdb.addImport("metrics", metrics);

    const pgwire = makeModule(b, target, optimize, "pgwire.zig");
    pgwire.addImport("zio", zio);
    // pgwire.parser does @cImport("pg_query.h"). Header path lives on the
    // module; the C archive itself is linked per-executable via
    // addLibpgQuerySources.
    pgwire.addIncludePath(b.path("third_party/libpg_query"));

    return .{
        .zio = zio,
        .metrics = metrics,
        .schema = schema,
        .buffers_gen = buffers_gen,
        .flags = flags,
        .utils = utils,
        .vfs = vfs,
        .runtime = runtime,
        .serial = serial,
        .transport = transport,
        .zbuf = zbuf,
        .raft = raft,
        .zdb = zdb,
        .pgwire = pgwire,
    };
}

/// Add module imports by field name. The import name and the `Modules`
/// field name must match — typos surface as `no field named X` compile
/// errors via `@field`. Pass mismatches via `mod.addImport("name", ptr)`
/// directly (e.g. for locally-built modules not on `Modules`).
fn addImports(mod: *std.Build.Module, mods: Modules, comptime names: anytype) void {
    inline for (names) |name| {
        mod.addImport(name, @field(mods, name));
    }
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
    mods: Modules,
    buffers_path: []const u8,
    out_path: []const u8,
    exe_name: []const u8,
) *std.Build.Step {
    const buffers_mod = makeModule(b, target, optimize, buffers_path);
    buffers_mod.addImport("schema", mods.schema);

    const gen = b.addExecutable(.{
        .name = exe_name,
        .root_module = makeModule(b, target, optimize, "zbuf/gen.zig"),
    });
    gen.root_module.addImport("schema", mods.schema);
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
    mods: Modules,
) *std.Build.Step {
    const step = b.step("buffers", "Regenerate all discovered */buffers.gen.zig files");
    const schema_paths = collectBufferSchemas(b);
    for (schema_paths, 0..) |schema_path, idx| {
        const out_path = generatedPathForSchema(b, schema_path);
        const run_step = addZbufGenRun(
            b,
            target,
            optimize,
            mods,
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
        std.mem.startsWith(u8, path, "node_modules/") or
        std.mem.startsWith(u8, path, "third_party/");
}

fn testFileLessThan(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

// Files that contain tests but don't match the `*_test.zig` / `test.zig`
// naming convention. Added explicitly to avoid silent skips. Audit when
// renaming or adding files in these locations.
const extra_test_files = [_][]const u8{
    "raft/testing/tests.zig",
};

// Test files that the auto-discovery walker finds but that don't currently
// compile against the live source. These were not in any pre-unification
// `addTest` step either, so excluding them matches existing behavior.
// Each entry should have a follow-up to rehabilitate or delete the file.
//
// - net/mux_test.zig:        references a `mux` module no one registers
// - net/transport_test.zig:  TODO note in old build.zig says "rewrite to
//                            use Serial instead of Mux"
// - raft/meta_test.zig:      uses `next_seq` and integer enum literals
//                            that no longer match raft.MetaStore / RoleEnum
const skip_test_files = [_][]const u8{
    "net/mux_test.zig",
    "net/transport_test.zig",
    "raft/meta_test.zig",
    // Legacy thread-vs-fiber benchmark. Uses std.Thread.spawn (removed
    // elsewhere) and produces a multi-cell sweep + p50/p99 latencies that
    // don't fit the simple `b.n` framework. Conclusion is captured in
    // MEMORY.md (std.posix.pread for VFS, zio everywhere else); kept on
    // disk as documentation, excluded from auto-discovery.
    "benchmarks/zio_bench.zig",
};

fn isSkippedTestFile(path: []const u8) bool {
    for (skip_test_files) |s| if (std.mem.eql(u8, s, path)) return true;
    return false;
}

fn collectTestFiles(b: *std.Build) [][]const u8 {
    var paths = std.ArrayList([]const u8).empty;
    var root_dir = std.fs.cwd().openDir(".", .{ .iterate = true }) catch {
        @panic("failed to open workspace dir");
    };
    defer root_dir.close();

    var walker = root_dir.walk(b.allocator) catch @panic("failed to walk workspace");
    defer walker.deinit();

    while (walker.next() catch @panic("failed to read workspace entry")) |entry| {
        if (entry.kind != .file) continue;
        const is_test = std.mem.endsWith(u8, entry.basename, "_test.zig");
        const is_bench = std.mem.endsWith(u8, entry.basename, "_bench.zig");
        if (!is_test and !is_bench) continue;
        if (isSkippedPath(entry.path)) continue;
        if (isSkippedTestFile(entry.path)) continue;

        const dup = b.allocator.dupe(u8, entry.path) catch {
            @panic("out of memory collecting test files");
        };
        paths.append(b.allocator, dup) catch {
            @panic("out of memory collecting test files");
        };
    }
    for (extra_test_files) |p| {
        const dup = b.allocator.dupe(u8, p) catch @panic("OOM");
        paths.append(b.allocator, dup) catch @panic("OOM");
    }
    std.mem.sort([]const u8, paths.items, {}, testFileLessThan);
    return paths.toOwnedSlice(b.allocator) catch {
        @panic("out of memory collecting test files");
    };
}

fn addAllTests(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    mods: Modules,
    regen_buffers_step: *std.Build.Step,
) *std.Build.Step {
    const test_files = collectTestFiles(b);

    // Generate a synthetic root that imports each discovered test file
    // by project-relative path. We must write it at the project root
    // because Zig 0.15+ rejects `@import` paths that escape the module's
    // package directory (the directory containing root_source_file).
    // Writing into `.zig-cache/...` would block all imports back into
    // the source tree. Path-imported files become part of root_module's
    // compilation unit, which is what Zig requires for their
    // `test "..."` blocks to be collected into `builtin.test_functions`.
    // (Anonymous modules don't work here: they're separate compilation
    // units, so their tests aren't run by the root's test_runner.)
    var src: std.ArrayList(u8) = .empty;
    src.appendSlice(b.allocator,
        "// AUTO-GENERATED by build.zig — do not edit. See addAllTests().\n" ++
            "test {\n",
    ) catch @panic("OOM");
    for (test_files) |path| {
        const line = std.fmt.allocPrint(b.allocator, "    _ = @import(\"{s}\");\n", .{path}) catch @panic("OOM");
        src.appendSlice(b.allocator, line) catch @panic("OOM");
    }
    src.appendSlice(b.allocator, "}\n") catch @panic("OOM");
    const root_owned = src.toOwnedSlice(b.allocator) catch @panic("OOM");

    const root_path = ".test_root.zig";
    {
        const f = std.fs.cwd().createFile(root_path, .{ .truncate = true }) catch @panic("create .test_root.zig");
        defer f.close();
        f.writeAll(root_owned) catch @panic("write .test_root.zig");
    }

    // BIG/BENCH gating happens at runtime via the test_runner CLI flags
    // (`--bigtest`, `--bench`); no compile-time `build_options` flag needed.
    const test_imports = .{
        "zbuf",   "zdb",    "raft",   "utils",      "zio", "vfs",
        "metrics", "buffers_gen", "transport", "serial", "pgwire",
    };

    const root_mod = b.createModule(.{
        .root_source_file = b.path(root_path),
        .target = target,
        .optimize = optimize,
    });
    addImports(root_mod, mods, test_imports);

    const all_tests = b.addTest(.{
        .root_module = root_mod,
        .test_runner = .{
            .path = b.path("u/test_runner.zig"),
            .mode = .simple,
        },
    });
    addVendoredCompressionSources(b, all_tests);
    addLibpgQuerySources(b, all_tests);

    const run = b.addRunArtifact(all_tests);
    run.step.dependOn(regen_buffers_step);
    if (b.args) |args| run.addArgs(args);

    const test_step = b.step("test", "Run every test in one binary (auto-discovered)");
    test_step.dependOn(&run.step);

    // `zig build bench` always builds in ReleaseFast — Debug bounds checks
    // and allocator overhead would dominate the inner loops and make the
    // numbers meaningless. We build a SECOND test artifact whose root is
    // ReleaseFast (same source files, separate compile). Imported modules
    // (zdb, raft, …) keep the user's optimize because most benchmarks
    // touch only std lib in their hot path; if a future bench reaches
    // deep into our modules, also pass `-Drelease` to compile them
    // ReleaseFast too.
    const bench_root_mod = b.createModule(.{
        .root_source_file = b.path(root_path),
        .target = target,
        .optimize = .ReleaseFast,
    });
    addImports(bench_root_mod, mods, test_imports);

    const all_bench = b.addTest(.{
        .root_module = bench_root_mod,
        .test_runner = .{
            .path = b.path("u/test_runner.zig"),
            .mode = .simple,
        },
    });
    addVendoredCompressionSources(b, all_bench);
    addLibpgQuerySources(b, all_bench);

    const run_bench = b.addRunArtifact(all_bench);
    run_bench.step.dependOn(regen_buffers_step);
    run_bench.addArg("--only-bench");
    if (b.args) |args| run_bench.addArgs(args);

    const bench_step = b.step("bench", "Run BENCH:-prefixed benchmarks (always built ReleaseFast)");
    bench_step.dependOn(&run_bench.step);
    return test_step;
}

fn addZbufCli(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    mods: Modules,
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
        mods,
        buffers_path,
        out_path,
        "zbuf-gen-dyn",
    );

    const zbuf_cli_step = b.step("zbuf", "Generate buffers code from a schema path");
    zbuf_cli_step.dependOn(run_gen_step);
}

fn addZdbServer(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    mods: Modules,
    regen_buffers_step: *std.Build.Step,
) void {
    const zdb_server_mod = makeModule(b, target, optimize, "zdb-server/server.zig");
    addImports(zdb_server_mod, mods, .{ "zio", "raft", "zdb", "runtime", "metrics", "flags" });

    const exe = b.addExecutable(.{
        .name = "zdb-server",
        .root_module = makeModule(b, target, optimize, "zdb-server/main.zig"),
    });
    addVendoredCompressionSources(b, exe);
    addImports(exe.root_module, mods, .{ "zio", "raft", "zdb", "runtime", "metrics", "flags" });
    exe.root_module.addImport("zdb_server", zdb_server_mod);
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
    mods: Modules,
) void {
    const exe = b.addExecutable(.{
        .name = "zdb-bench",
        .root_module = makeModule(b, target, optimize, "benchmark.zig"),
    });
    addImports(exe.root_module, mods, .{ "zio", "flags" });
    b.installArtifact(exe);

    const install = b.addInstallArtifact(exe, .{});
    const step = b.step("zdb-bench", "Build zdb YCSB benchmark client");
    step.dependOn(&install.step);
}

fn addLoadtestDriver(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    mods: Modules,
) void {
    const exe = b.addExecutable(.{
        .name = "loadtest",
        .root_module = makeModule(b, target, optimize, "scripts/loadtest.zig"),
    });
    addImports(exe.root_module, mods, .{ "zio", "flags" });
    b.installArtifact(exe);

    const install = b.addInstallArtifact(exe, .{});
    const step = b.step("loadtest", "Build the zdb load-test driver");
    step.dependOn(&install.step);
}

fn addSstVerify(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    mods: Modules,
) void {
    const exe = b.addExecutable(.{
        .name = "sst-verify",
        .root_module = makeModule(b, target, optimize, "scripts/sst_verify.zig"),
    });
    addImports(exe.root_module, mods, .{"zdb"});
    addVendoredCompressionSources(b, exe);
    b.installArtifact(exe);

    const install = b.addInstallArtifact(exe, .{});
    const step = b.step("sst-verify", "Build the SST verification tool");
    step.dependOn(&install.step);
}

fn addRaftMultiprocHarness(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    mods: Modules,
    regen_buffers_step: *std.Build.Step,
) *std.Build.Step {
    const harness_exe = b.addExecutable(.{
        .name = "raft-multiproc-harness",
        .root_module = makeModule(b, target, optimize, "raft/testing/multiproc_harness.zig"),
    });
    addVendoredCompressionSources(b, harness_exe);
    addImports(harness_exe.root_module, mods, .{"raft"});

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

/// libpg_query is the upstream Postgres parser carved out as a static
/// C library (see third_party/libpg_query/). The vendored copy keeps
/// only the JSON output path: protobuf encode/decode, the deparser,
/// and the standalone `pg_query_scan` are dropped — see
/// pgwire/README.md for what was excised and why.
fn addLibpgQuerySources(b: *std.Build, compile: *std.Build.Step.Compile) void {
    compile.linkLibC();
    compile.root_module.addIncludePath(b.path("third_party/libpg_query"));
    compile.root_module.addIncludePath(b.path("third_party/libpg_query/vendor"));
    compile.root_module.addIncludePath(b.path("third_party/libpg_query/src/include"));
    compile.root_module.addIncludePath(b.path("third_party/libpg_query/src/postgres/include"));

    compile.root_module.addCSourceFiles(.{
        .root = b.path("."),
        .files = collectLibpgQuerySources(b),
        .flags = &.{
            "-Wno-unused-function",
            "-Wno-unused-value",
            "-Wno-unused-variable",
            "-Wno-format",
            "-fno-strict-aliasing",
            "-fwrapv",
        },
    });
}

/// Walk the vendored libpg_query tree and collect every `.c` file we
/// want to compile. The vendored copy is already pruned (protobuf
/// encoder/decoder + deparser + standalone scanner are absent); this
/// just enumerates whatever is on disk under `src/`, `src/postgres/`,
/// and the xxhash vendor lib. `src/include/` is intentionally skipped
/// — those files are *.c by name but get `#include`-d into other
/// translation units, not compiled standalone.
fn collectLibpgQuerySources(b: *std.Build) [][]const u8 {
    var paths = std.ArrayList([]const u8).empty;
    const roots = [_][]const u8{
        "third_party/libpg_query/src",
        "third_party/libpg_query/src/postgres",
        "third_party/libpg_query/vendor/xxhash",
    };
    for (roots) |root_path| {
        var dir = std.fs.cwd().openDir(root_path, .{ .iterate = true }) catch
            @panic("failed to open libpg_query dir");
        defer dir.close();

        var it = dir.iterate();
        while (it.next() catch @panic("failed to read libpg_query dir")) |entry| {
            // Only this directory level — we walk each root separately
            // so the `src/include/` C-fragment files (which are
            // `#include`-d, not compiled) are skipped automatically.
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".c")) continue;
            const full = std.fs.path.join(
                b.allocator,
                &.{ root_path, entry.name },
            ) catch @panic("OOM joining libpg_query path");
            paths.append(b.allocator, full) catch
                @panic("OOM collecting libpg_query sources");
        }
    }
    if (paths.items.len == 0) {
        @panic("no libpg_query sources found — is the vendored tree present?");
    }
    // Sorted output keeps the build cache key stable across runs;
    // dir.iterate() order is filesystem-defined.
    std.mem.sort([]const u8, paths.items, {}, struct {
        fn lt(_: void, a: []const u8, c: []const u8) bool {
            return std.mem.lessThan(u8, a, c);
        }
    }.lt);
    return paths.toOwnedSlice(b.allocator) catch
        @panic("OOM finalizing libpg_query source list");
}

fn addPgwireShakeout(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    mods: Modules,
) void {
    const exe = b.addExecutable(.{
        .name = "pgwire-shakeout",
        .root_module = makeModule(b, target, optimize, "pgwire-shakeout/main.zig"),
    });
    addImports(exe.root_module, mods, .{ "zio", "pgwire" });
    addLibpgQuerySources(b, exe);
    b.installArtifact(exe);

    const install = b.addInstallArtifact(exe, .{});
    const step = b.step("pgwire-shakeout", "Build the pgwire psql shakeout server");
    step.dependOn(&install.step);
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
