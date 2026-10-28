// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

/// Central registry of every singleton module in this build. Constructed
/// once in `makeModules` and threaded into every `addX` function. The
/// rule for membership: if a `*Module` is shared across more than one
/// consumer, it goes here. Per-call locals (e.g. the per-schema
/// `buffers_mod`, egres's per-exe `engine_mod`, raft's private
/// `raft_buffers_gen` module) stay where they're created.
///
/// Field names are required to match the import names consumers use
/// (`@import("zbuf")` ↔ `mods.zbuf`). `addImports` enforces this via
/// `@field`, so a typo is a compile error rather than a runtime miss.
const Modules = struct {
    // External deps
    zio: *std.Build.Module,
    metrics: *std.Build.Module,
    zroar: *std.Build.Module,

    // Schema / codegen
    schema: *std.Build.Module, // zbuf/schema.zig (used by zbuf gen tool)
    buffers_gen: *std.Build.Module, // zbuf/buffers.gen.zig

    // Infra
    flags: *std.Build.Module, // u/flags.zig
    utils: *std.Build.Module, // u/utils.zig (lz4/zstd C includes baked in)
    cache: *std.Build.Module, // cache/cache.zig (S3-FIFO policy)
    vfs: *std.Build.Module, // vfs/vfs.zig

    // Wire
    serial: *std.Build.Module, // net/serial.zig
    transport: *std.Build.Module, // net/transport.zig

    // Domain
    zbuf: *std.Build.Module, // zbuf.zig
    raft: *std.Build.Module, // raft/raft.zig
    zdb: *std.Build.Module, // zdb.zig
    pgwire: *std.Build.Module, // pgwire.zig (libpg_query include baked in)
    zfiber: *std.Build.Module, // zfiber/fiber.zig
    engine: *std.Build.Module, // engine/root.zig (zdb+raft fused behind one API)
    sql: *std.Build.Module, // sql.zig (row codec + catalog/registry + store)
};

const jemalloc_version = "5.3.1";
const jemalloc_sha256 =
    "3826bc80232f22ed5c4662f3034f799ca316e819103bdc7bb99018a421706f92";

const mimalloc_version = "3.4.5";
const mimalloc_sha256 =
    "19a43af0645c57d348e729d5b31e23e912582911bb1047f795790834d3416221";

/// One vendored allocator: the bootstrap step that builds it and the
/// static archive to link. Every binary links exactly one, chosen by
/// `-Dallocator` — both archives define libc malloc/free (jemalloc via
/// its non-prefixed build, mimalloc via MI_OVERRIDE), so linking two
/// would clash, and one owner means C-library allocations route through
/// the selected backend too.
const AllocatorDep = struct {
    bootstrap: *std.Build.Step,
    library: []const u8,
};

const AllocatorBackend = enum { mimalloc, jemalloc };

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    // `-Drelease` is a shortcut for `-Doptimize=ReleaseFast`. Tests + the
    // default build still go through `standardOptimizeOption` (i.e. Debug
    // unless explicitly overridden), so attaching a debugger or running
    // safety-checked tests stays the path of least resistance. The bench
    // step (in addAllTests) hardcodes `.ReleaseFast` for its own root
    // module and is unaffected.
    const release_mode = b.option(bool, "release", "Shortcut for -Doptimize=ReleaseFast") orelse false;
    // Compile-time allocator selection: the chosen backend becomes the
    // exe's "galloc" module and the only allocator archive it links.
    const allocator_backend = b.option(
        AllocatorBackend,
        "allocator",
        "Allocator backing the process gpa (default: jemalloc)",
    ) orelse .jemalloc;
    const optimize: std.builtin.OptimizeMode = if (release_mode)
        .ReleaseFast
    else
        b.standardOptimizeOption(.{});
    // A non-native target gets its own jemalloc build and install dir
    // (see scripts/bootstrap-jemalloc.sh); the native one stays where
    // it is.
    const cross_target: ?[]const u8 = if (target.query.isNative())
        null
    else
        target.query.zigTriple(b.allocator) catch @panic("OOM");
    const jemalloc_library = b.fmt(
        ".deps/jemalloc/install/{s}{s}{s}/lib/libjemalloc.a",
        .{
            jemalloc_version,
            if (cross_target != null) "-" else "",
            cross_target orelse "",
        },
    );
    const mimalloc_library = b.fmt(
        ".deps/mimalloc/install/{s}/lib/libmimalloc.a",
        .{mimalloc_version},
    );

    const mods = makeModules(b, target, optimize);
    const regen_buffers_step = addRegenAllBuffers(b, optimize);
    const bootstrap_jemalloc = b.addSystemCommand(&.{
        "bash",
        "scripts/bootstrap-jemalloc.sh",
        jemalloc_version,
        jemalloc_sha256,
    });
    if (cross_target) |t| bootstrap_jemalloc.addArg(t);
    // The script compiles jemalloc with this Zig's `cc`, so no system C
    // compiler is needed; pass the executable rather than rely on PATH.
    bootstrap_jemalloc.setEnvironmentVariable("ZIG", b.graph.zig_exe);
    const bootstrap_mimalloc = b.addSystemCommand(&.{
        "bash",
        "scripts/bootstrap-mimalloc.sh",
        mimalloc_version,
        mimalloc_sha256,
    });
    const allocator_dep: AllocatorDep = switch (allocator_backend) {
        .jemalloc => .{
            .bootstrap = &bootstrap_jemalloc.step,
            .library = jemalloc_library,
        },
        .mimalloc => .{
            .bootstrap = &bootstrap_mimalloc.step,
            .library = mimalloc_library,
        },
    };
    // The selected backend's adapter becomes the "galloc" module — the
    // one allocator import production code and the generic allocator
    // tests use — and its archive rides along to every consumer.
    // Consumers read `galloc.kind` at comptime for the pieces that
    // differ per backend (main's maintenance fiber, the startup log).
    const galloc = makeModule(b, target, optimize, switch (allocator_backend) {
        .jemalloc => "jemalloc/root.zig",
        .mimalloc => "mimalloc/root.zig",
    });
    galloc.link_libc = true;
    galloc.addObjectFile(b.path(allocator_dep.library));
    // The engine's admin `mem` prints the backend's own counters.
    mods.engine.addImport("galloc", galloc);

    addZbufCli(b, optimize);
    // `zig build check`: type-check the server binary and every test file with
    // no codegen and no linking. ZLS runs it on save for diagnostics; the
    // per-file test roots registered in addAllTests are what let ZLS resolve
    // @import("zdb") and friends inside *_test.zig files.
    const check_step = b.step("check", "Type-check the egres binary and all tests (no codegen; ZLS build-on-save)");

    addEgres(
        b,
        target,
        optimize,
        mods,
        regen_buffers_step,
        galloc,
        allocator_dep,
        check_step,
    );
    addZdbBench(b, target, optimize, mods);
    addZioBench(b, target, optimize, mods);
    addStress(b, target, optimize, mods);
    // Load-test driver lives in `scripts/loadtest.py` (orchestration glue is
    // far cleaner in Python than in Zig's std.process/std.Io.Dir surface).
    addSstVerify(b, target, optimize, mods);
    // `zig build test` — one binary, auto-discovered from `*_test.zig` /
    // `test.zig` files. Replaces the previous test-zdb / test-zbuf /
    // test-utils / test-raft / test-net per-file plumbing.
    _ = addAllTests(
        b,
        target,
        optimize,
        mods,
        regen_buffers_step,
        galloc,
        allocator_dep,
        check_step,
    );
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
    const zroar = b.dependency("zroar", .{ .target = target, .optimize = optimize }).module("zroar");

    // Schema / codegen
    const schema = makeModule(b, target, optimize, "zbuf/schema.zig");
    const zbuf = makeModule(b, target, optimize, "zbuf.zig");

    const buffers_gen = makeModule(b, target, optimize, "zbuf/buffers.gen.zig");
    buffers_gen.addImport("zbuf", zbuf);

    // Infra. Only `fiber` imports zio now (for its coroutine engine,
    // `zio.coro`); every other module is zio-free post-migration.
    const utils = makeModule(b, target, optimize, "u/utils.zig");
    // utils/compress.zig does @cImport for lz4/zstd. Bake the include paths
    // onto the module so every consumer gets them automatically — no more
    // "remember to call addIncludePath" footgun across multiple addX functions.
    utils.addIncludePath(b.path("."));
    utils.addIncludePath(b.path("third_party/lz4/lib"));
    utils.addIncludePath(b.path("third_party/zstd/lib"));
    // The coarse clock keeps its gap histogram as a metric.
    utils.addImport("metrics", metrics);

    const flags = makeModule(b, target, optimize, "u/flags.zig");
    flags.addImport("utils", utils);

    const vfs = makeModule(b, target, optimize, "vfs/vfs.zig");
    vfs.addImport("utils", utils);

    // The in-tree fiber runtime — what production code spawns/parks on.
    // Wired into every module that used to go through the `runtime` seam.
    const xev = b.dependency("xev", .{ .target = target, .optimize = optimize }).module("xev");
    const fiber = makeModule(b, target, optimize, "zfiber/root.zig");
    fiber.addImport("zio", zio);
    fiber.addImport("xev", xev);

    fiber.addImport("utils", utils);
    flags.addImport("zfiber", fiber);
    vfs.addImport("zfiber", fiber);

    // The S3-FIFO policy, shared by the block cache (zdb) and the disk
    // chunk cache (vfs).
    const cache = makeModule(b, target, optimize, "cache/cache.zig");
    cache.addImport("utils", utils);
    cache.addImport("zfiber", fiber);
    vfs.addImport("cache", cache);

    // Wire
    const serial = makeModule(b, target, optimize, "net/serial.zig");
    serial.addImport("zfiber", fiber);
    serial.addImport("utils", utils);

    const transport = makeModule(b, target, optimize, "net/transport.zig");
    transport.addImport("zfiber", fiber);
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
    raft.addImport("zfiber", fiber);
    raft.addImport("serial", serial);
    raft.addImport("transport", transport);
    raft.addImport("vfs", vfs);
    raft.addImport("metrics", metrics);

    const zdb = makeModule(b, target, optimize, "zdb.zig");
    zdb.addImport("zbuf", zbuf);
    zdb.addImport("utils", utils);
    zdb.addImport("zfiber", fiber);
    zdb.addImport("vfs", vfs);
    zdb.addImport("cache", cache);
    zdb.addImport("metrics", metrics);
    zdb.addImport("zroar", zroar);
    zdb.addImport("flags", flags);

    const pgwire = makeModule(b, target, optimize, "pgwire.zig");
    pgwire.addImport("zfiber", fiber);
    pgwire.addImport("utils", utils);
    // pgwire.parser @cImports pg_query.h, libpg_query's internal header
    // (the raw parse entry) and Postgres's node headers, so it reads
    // the parse tree in place. The paths live on the module; the C
    // archive itself is linked per-executable via addLibpgQuerySources.
    pgwire.addIncludePath(b.path("third_party/libpg_query"));
    pgwire.addIncludePath(b.path("third_party/libpg_query/src"));
    pgwire.addIncludePath(b.path("third_party/libpg_query/vendor"));
    pgwire.addIncludePath(b.path("third_party/libpg_query/src/include"));
    pgwire.addIncludePath(
        b.path("third_party/libpg_query/src/postgres/include"),
    );

    // The engine: zdb (storage) + raft (consensus) fused behind one API.
    // Front-ends (egres exe, the sql store) sit on top; it never imports sql.
    const engine = makeModule(b, target, optimize, "engine/root.zig");
    engine.addImport("raft", raft);
    engine.addImport("zdb", zdb);
    engine.addImport("zfiber", fiber);
    engine.addImport("metrics", metrics);
    engine.addImport("flags", flags);

    // sql: the row codec is pure std logic; the DDL parser reuses pgwire's
    // libpg_query binding (the C archive itself is linked per-executable via
    // addLibpgQuerySources); the server (pgwire front-end) drives the
    // engine — the sanctioned direction is sql → engine → zdb/raft, and
    // engine never imports sql. sql also references zdb key types directly
    // (Key/KeyRange/Item) to build catalog + data scans against the forest.
    const sql = makeModule(b, target, optimize, "sql.zig");
    sql.addImport("pgwire", pgwire);
    sql.addImport("engine", engine);
    sql.addImport("zdb", zdb);
    sql.addImport("zfiber", fiber);
    sql.addImport("utils", utils);

    return .{
        .zio = zio,
        .metrics = metrics,
        .zroar = zroar,
        .schema = schema,
        .buffers_gen = buffers_gen,
        .flags = flags,
        .utils = utils,
        .cache = cache,
        .vfs = vfs,
        .serial = serial,
        .transport = transport,
        .zbuf = zbuf,
        .raft = raft,
        .zdb = zdb,
        .pgwire = pgwire,
        .zfiber = fiber,
        .engine = engine,
        .sql = sql,
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
    optimize: std.builtin.OptimizeMode,
    buffers_path: []const u8,
    out_path: []const u8,
    exe_name: []const u8,
) *std.Build.Step {
    // The generator runs at build time, so it is built for the host
    // even when `target` is a cross target; its modules follow.
    const host = b.graph.host;
    const schema = makeModule(b, host, optimize, "zbuf/schema.zig");
    const buffers_mod = makeModule(b, host, optimize, buffers_path);
    buffers_mod.addImport("schema", schema);

    const gen = b.addExecutable(.{
        .name = exe_name,
        .root_module = makeModule(b, host, optimize, "zbuf/gen.zig"),
    });
    gen.root_module.addImport("schema", schema);
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
    optimize: std.builtin.OptimizeMode,
) *std.Build.Step {
    const step = b.step("buffers", "Regenerate all discovered */buffers.gen.zig files");
    const schema_paths = collectBufferSchemas(b);
    for (schema_paths, 0..) |schema_path, idx| {
        const out_path = generatedPathForSchema(b, schema_path);
        const run_step = addZbufGenRun(
            b,
            optimize,
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
    const io = b.graph.io;
    var root_dir = std.Io.Dir.cwd().openDir(io, ".", .{ .iterate = true }) catch {
        @panic("failed to open workspace dir");
    };
    defer root_dir.close(io);

    var walker = root_dir.walk(b.allocator) catch @panic("failed to walk workspace");
    defer walker.deinit();

    while (walker.next(io) catch @panic("failed to read workspace entry")) |entry| {
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
        std.mem.startsWith(u8, path, "third_party/") or
        // Zig-package deps unpacked by `zig build` (see build.zig.zon);
        // their tests run in their own repos, not in ours.
        std.mem.startsWith(u8, path, "zig-pkg/") or
        // Graveyard for code no longer wired into the build (e.g. the old
        // Mux multiplexer + its tests). Excluded wholesale from discovery.
        std.mem.startsWith(u8, path, "unused/");
}

fn testFileLessThan(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

// Files that contain tests but don't match the `*_test.zig` / `test.zig`
// naming convention. Added explicitly to avoid silent skips. Audit when
// renaming or adding files in these locations.
const extra_test_files = [_][]const u8{};

// Test files that the auto-discovery walker finds but that don't currently
// compile against the live source. These were not in any pre-unification
// `addTest` step either, so excluding them matches existing behavior.
// Each entry should have a follow-up to rehabilitate or delete the file.
//
// (The Mux multiplexer + its mux_test/transport_test moved to `unused/`,
//  which isSkippedPath excludes wholesale — no entries needed here.)
//
// - raft/meta_test.zig:      uses `next_seq` and integer enum literals
//                            that no longer match raft.MetaStore / RoleEnum
const skip_test_files = [_][]const u8{
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
    const io = b.graph.io;
    var root_dir = std.Io.Dir.cwd().openDir(io, ".", .{ .iterate = true }) catch {
        @panic("failed to open workspace dir");
    };
    defer root_dir.close(io);

    var walker = root_dir.walk(b.allocator) catch @panic("failed to walk workspace");
    defer walker.deinit();

    while (walker.next(io) catch @panic("failed to read workspace entry")) |entry| {
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
    galloc: *std.Build.Module,
    allocator_dep: AllocatorDep,
    check_step: *std.Build.Step,
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
    src.appendSlice(
        b.allocator,
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
        const io = b.graph.io;
        const f = std.Io.Dir.cwd().createFile(io, root_path, .{ .truncate = true }) catch @panic("create .test_root.zig");
        defer f.close(io);
        f.writeStreamingAll(io, root_owned) catch @panic("write .test_root.zig");
    }

    // BIG/BENCH gating happens at runtime via the test_runner CLI flags
    // (`--bigtest`, `--bench`); no compile-time `build_options` flag needed.
    // `zio` stays available because auto-discovered `benchmarks/*_bench.zig`
    // tools (e.g. mutex_bench, zio_bench) still benchmark zio primitives; no
    // *test* uses it. Production modules above are wired zio-free.
    const test_imports = .{
        "zbuf",    "zdb",         "raft",      "utils",  "zio",    "vfs",
        "metrics", "buffers_gen", "transport", "serial", "pgwire", "zfiber",
        "sql",     "engine",      "flags",     "zroar",  "cache",
    };

    const root_mod = b.createModule(.{
        .root_source_file = b.path(root_path),
        .target = target,
        .optimize = optimize,
    });
    addImports(root_mod, mods, test_imports);
    // The build-selected allocator: the generic allocator tests
    // (egres/galloc_test.zig) exercise whichever backend `-Dallocator`
    // picked; its archive arrives via the galloc module.
    root_mod.addImport("galloc", galloc);
    root_mod.link_libc = true;

    // `zig build check`: one no-emit compile of the whole test root. Same
    // imports as the real test binary, no C sources, nothing installed or
    // run — semantic analysis only, which is all build-on-save needs.
    const check_mod = makeModule(b, target, optimize, root_path);
    addImports(check_mod, mods, test_imports);
    check_mod.addImport("galloc", galloc);
    check_mod.link_libc = true;
    const check_tests = b.addTest(.{
        .name = "check-tests",
        .root_module = check_mod,
        .test_runner = .{ .path = b.path("u/test_runner.zig"), .mode = .simple },
    });
    addCheckIncludePaths(b, check_tests);
    check_step.dependOn(&check_tests.step);

    // ZLS discovery only — nothing here is meant to be built. ZLS's build
    // runner lists the root module of every Compile step in the graph,
    // whichever steps actually run, and a file is only analyzable when it is
    // some module's root or a named import of one. .test_root.zig reaches
    // the test files by *relative* @import, which that listing does not
    // follow — so without these roots, go-to-definition is dead inside every
    // *_test.zig. They hang off their own step so `check` stays one compile.
    const roots_step = b.step("zls-roots", "Register each test file as a module root for ZLS (discovery only; do not run)");
    for (test_files) |path| {
        const m = makeModule(b, target, optimize, path);
        addImports(m, mods, test_imports);
        m.addImport("galloc", galloc);
        m.link_libc = true;
        const t = b.addTest(.{ .name = stepSafeName(b, "zls-root-", path), .root_module = m });
        addCheckIncludePaths(b, t);
        roots_step.dependOn(&t.step);
    }

    // Name the binary by optimize mode so the Debug and ReleaseFast test
    // binaries install SIDE BY SIDE (zig-out/bin/test-Debug vs
    // zig-out/bin/test-ReleaseFast) instead of clobbering each other — handy
    // for the compile-once / run-the-binary-many-times loop used to hunt
    // intermittent races.
    const test_bin_name = b.fmt("test-{s}", .{@tagName(optimize)});
    const all_tests = b.addTest(.{
        .name = test_bin_name,
        .root_module = root_mod,
        .test_runner = .{
            .path = b.path("u/test_runner.zig"),
            .mode = .simple,
        },
    });
    addVendoredCompressionSources(b, all_tests);
    addLibpgQuerySources(b, all_tests);
    all_tests.step.dependOn(allocator_dep.bootstrap);
    all_tests.step.dependOn(regen_buffers_step);

    // Install the test binary to zig-out/bin/<test_bin_name> so it sits at a
    // stable path you can run directly (skips the per-iteration build graph).
    const install_test = b.addInstallArtifact(all_tests, .{ .dest_dir = .{ .override = .bin } });

    const run = b.addRunArtifact(all_tests);
    run.step.dependOn(regen_buffers_step);
    if (b.args) |args| run.addArgs(args);

    // `zig build test` builds + installs the binary, THEN runs it.
    const test_step = b.step("test", "Run every test in one binary (auto-discovered)");
    test_step.dependOn(&install_test.step);
    test_step.dependOn(&run.step);

    // `zig build test-bin` builds + installs WITHOUT running — for the
    // compile-once, loop-the-binary workflow.
    const test_bin_step = b.step("test-bin", "Build + install the test binary (no run)");
    test_bin_step.dependOn(&install_test.step);

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
    bench_root_mod.addImport("galloc", galloc);
    bench_root_mod.link_libc = true;

    const all_bench = b.addTest(.{
        .root_module = bench_root_mod,
        .test_runner = .{
            .path = b.path("u/test_runner.zig"),
            .mode = .simple,
        },
    });
    addVendoredCompressionSources(b, all_bench);
    addLibpgQuerySources(b, all_bench);
    all_bench.step.dependOn(allocator_dep.bootstrap);

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
    optimize: std.builtin.OptimizeMode,
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
        optimize,
        buffers_path,
        out_path,
        "zbuf-gen-dyn",
    );

    const zbuf_cli_step = b.step("zbuf", "Generate buffers code from a schema path");
    zbuf_cli_step.dependOn(run_gen_step);
}

/// The egres SQL server: engine + admin protocol + a SQL (pgwire) front-end.
/// The exe links zdb's compression sources + pgwire's libpg_query and imports
/// both the engine and pgwire modules. `strip`/`omit_frame_pointer` stay off so
/// `dumpFibers` can symbolize parked fibers from the binary's own DWARF.
fn addEgres(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    mods: Modules,
    regen_buffers_step: *std.Build.Step,
    galloc: *std.Build.Module,
    allocator_dep: AllocatorDep,
    check_step: *std.Build.Step,
) void {
    const exe = b.addExecutable(.{
        .name = "egres",
        .root_module = makeModule(b, target, optimize, "egres/main.zig"),
    });
    exe.root_module.strip = false;
    exe.root_module.omit_frame_pointer = false;
    addVendoredCompressionSources(b, exe); // zdb compression (via engine)
    addLibpgQuerySources(b, exe); // pgwire's libpg_query
    addImports(exe.root_module, mods, .{
        "raft",   "zdb",    "zfiber", "metrics", "flags", "utils",
        "pgwire", "engine", "sql",
    });
    exe.root_module.addImport("galloc", galloc);
    exe.step.dependOn(allocator_dep.bootstrap);
    exe.step.dependOn(regen_buffers_step);
    b.installArtifact(exe);

    const install = b.addInstallArtifact(exe, .{});
    const step = b.step("egres", "Build the egres SQL server binary");
    step.dependOn(&install.step);

    // Type-check twin for `zig build check`: same modules and include paths,
    // no C sources, never installed or run, so nothing is emitted or linked.
    const check_exe = b.addExecutable(.{
        .name = "check-egres",
        .root_module = makeModule(b, target, optimize, "egres/main.zig"),
    });
    addCheckIncludePaths(b, check_exe);
    addImports(check_exe.root_module, mods, .{
        "raft",   "zdb",    "zfiber", "metrics", "flags", "utils",
        "pgwire", "engine", "sql",
    });
    check_exe.root_module.addImport("galloc", galloc);
    check_step.dependOn(&check_exe.step);
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
    addImports(exe.root_module, mods, .{ "zfiber", "flags", "utils" });
    b.installArtifact(exe);

    const install = b.addInstallArtifact(exe, .{});
    const step = b.step("zdb-bench", "Build zdb YCSB benchmark client");
    step.dependOn(&install.step);
}

/// Standalone thread-vs-zio-vs-fiber I/O & CPU micro-benchmark. Not a test
/// (it has its own `main`), so it's excluded from auto-discovery and built
/// here as its own exe.
///
/// ALWAYS ReleaseFast, regardless of `-Doptimize`: the benchmark's hot path
/// runs through the fiber scheduler and zio, so Debug bounds-checks +
/// allocator overhead there would dominate the inner loops and make the
/// numbers meaningless. We rebuild `zio`/`xev`/`fiber` at ReleaseFast here
/// (not reuse the shared `mods`, which follow the user's `-Doptimize`) so the
/// benchmarked runtime is genuinely optimized, not just the harness shell.
///
/// Step-only (no `installArtifact`): a bare `zig build` won't pay for this
/// ReleaseFast compile — it builds solely on `zig build bench-zio`.
fn addZioBench(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    _: std.builtin.OptimizeMode, // ignored — always ReleaseFast (see above)
    _: Modules, // ignored — deps rebuilt at ReleaseFast below
) void {
    const opt: std.builtin.OptimizeMode = .ReleaseFast;

    const zio = b.dependency("zio", .{ .target = target, .optimize = opt }).module("zio");
    const xev = b.dependency("xev", .{ .target = target, .optimize = opt }).module("xev");
    const fiber = makeModule(b, target, opt, "zfiber/root.zig");
    fiber.addImport("zio", zio);
    fiber.addImport("xev", xev);
    // zfiber reads the process context and drives the coarse clock in
    // utils; a ReleaseFast utils to match (its C includes are unused
    // here, but the module declares them).
    const utils = makeModule(b, target, opt, "u/utils.zig");
    utils.addIncludePath(b.path("."));
    utils.addIncludePath(b.path("third_party/lz4/lib"));
    utils.addIncludePath(b.path("third_party/zstd/lib"));
    const metrics = b.dependency("metrics", .{
        .target = target,
        .optimize = opt,
    }).module("metrics");
    utils.addImport("metrics", metrics);
    fiber.addImport("utils", utils);

    const exe = b.addExecutable(.{
        .name = "bench-zio",
        .root_module = makeModule(b, target, opt, "benchmarks/zio_bench.zig"),
    });
    exe.root_module.addImport("zio", zio);
    exe.root_module.addImport("zfiber", fiber);
    exe.root_module.addImport("utils", utils);

    const install = b.addInstallArtifact(exe, .{});
    const step = b.step("bench-zio", "Build the thread/zio/fiber benchmark (always ReleaseFast)");
    step.dependOn(&install.step);
}

/// `zig build stress` — the timeout/watermark/sleep correctness harness. Follows
/// the user's `-Doptimize`; run it `-Doptimize=ReleaseSafe` so assertions and
/// `strictSleepMs`'s spurious-wake panic are live. Installs to
/// `zig-out/bin/timeout-stress`, run it with flags (see the file's doc).
fn addStress(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    mods: Modules,
) void {
    const exe = b.addExecutable(.{
        .name = "timeout-stress",
        .root_module = makeModule(b, target, optimize, "benchmarks/timeout_stress.zig"),
    });
    addImports(exe.root_module, mods, .{ "zfiber", "utils" });
    const install = b.addInstallArtifact(exe, .{});
    const step = b.step("stress", "Build the timeout/watermark/sleep stress harness");
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
    addImports(exe.root_module, mods, .{ "zdb", "utils" });
    addVendoredCompressionSources(b, exe);
    b.installArtifact(exe);

    const install = b.addInstallArtifact(exe, .{});
    const step = b.step("sst-verify", "Build the SST verification tool");
    step.dependOn(&install.step);
}

/// libpg_query is the upstream Postgres parser carved out as a static
/// C library (see third_party/libpg_query/). The vendored copy keeps
/// only the JSON output path: protobuf encode/decode, the deparser,
/// and the standalone `pg_query_scan` are dropped — see
/// pgwire/README.md for what was excised and why.
/// The include paths addLibpgQuerySources and addVendoredCompressionSources put
/// on a compile, minus the C sources: enough for every @cImport to resolve in a
/// type-check-only build (`zig build check`) without compiling a line of C.
fn addCheckIncludePaths(b: *std.Build, compile: *std.Build.Step.Compile) void {
    const m = compile.root_module;
    m.link_libc = true;
    m.addIncludePath(b.path("."));
    m.addIncludePath(b.path("third_party/lz4/lib"));
    m.addIncludePath(b.path("third_party/zstd/lib"));
    m.addIncludePath(b.path("third_party/libpg_query"));
    m.addIncludePath(b.path("third_party/libpg_query/vendor"));
    m.addIncludePath(b.path("third_party/libpg_query/src/include"));
    m.addIncludePath(b.path("third_party/libpg_query/src/postgres/include"));
}

/// "prefix" ++ path with '/' and '.' turned into '-', for a Compile step name.
fn stepSafeName(b: *std.Build, prefix: []const u8, path: []const u8) []const u8 {
    const out = b.allocator.alloc(u8, prefix.len + path.len) catch @panic("OOM");
    @memcpy(out[0..prefix.len], prefix);
    for (path, prefix.len..) |c, i| out[i] = if (c == '/' or c == '.') '-' else c;
    return out;
}

fn addLibpgQuerySources(b: *std.Build, compile: *std.Build.Step.Compile) void {
    compile.root_module.link_libc = true;
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
    const io = b.graph.io;
    const roots = [_][]const u8{
        "third_party/libpg_query/src",
        "third_party/libpg_query/src/postgres",
        "third_party/libpg_query/vendor/xxhash",
    };
    for (roots) |root_path| {
        var dir = std.Io.Dir.cwd().openDir(io, root_path, .{ .iterate = true }) catch
            @panic("failed to open libpg_query dir");
        defer dir.close(io);

        var it = dir.iterate();
        while (it.next(io) catch @panic("failed to read libpg_query dir")) |entry| {
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

fn addVendoredCompressionSources(b: *std.Build, compile: *std.Build.Step.Compile) void {
    compile.root_module.link_libc = true;
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
