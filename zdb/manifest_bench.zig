// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! A snapshot's manifest written and read back, on a real one:
//! `snapshot.bin` from a server's WAL directory, named by
//! EGRES_SNAPSHOT_BIN (skipped when it is not set). The tenancy
//! benchmark's 84,000 databases give a 43 MB manifest.
//!
//! Run with `EGRES_SNAPSHOT_BIN=<wal>/snapshot.bin zig build bench
//! -Drelease -- --filter "snapshot manifest"`.

const std = @import("std");
const utils = @import("utils");
const String = utils.String;
const zdb = @import("zdb");
const zbuf = @import("zbuf").zbuf;
const raft = @import("raft");
const fiber = @import("zfiber");
const galloc = @import("galloc");
const bench = fiber.bench;
const vfs = zdb.vfs;

const Manifest = zdb.manifest.Manifest;

/// The manifest the snapshot holds, restored into `m` (a fresh `init`,
/// at its final address: the restore takes its lock).
fn load(gpa: std.mem.Allocator, m: *Manifest) !void {
    const env = std.c.getenv("EGRES_SNAPSHOT_BIN") orelse
        return error.SkipZigTest;
    const path: String = std.mem.span(env);
    const file = try vfs.readFile(path, gpa);
    defer gpa.free(file);
    const payload = try zbuf.Frame.read(file);
    const snap = try raft.buffers.SnapshotReader.parseRoot(payload);
    try m.restoreFrom(snap.data());
}

test "BENCH: snapshot manifest serialize" {
    // The server's allocator: the harness's page allocator maps a page
    // per allocation, and a restore makes millions of small ones. Taken
    // at run time, since on macOS it first registers jemalloc's zone.
    const gpa = galloc.allocator();
    const b = bench.current();
    var src = Manifest.init(gpa);
    defer src.deinit();
    try load(gpa, &src);
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        const bytes = try src.serialize(gpa);
        b.addBytes(bytes.len);
        gpa.free(bytes);
    }
}

// A restore as the snapshot build makes one: a draft on an arena, freed
// whole.
fn restore(gpa: std.mem.Allocator, bytes: String) !void {
    var arloc = std.heap.ArenaAllocator.init(gpa);
    defer arloc.deinit();
    var draft = Manifest.init(arloc.allocator());
    defer draft.deinitOnArena();
    try draft.restoreFrom(bytes);
}

test "BENCH: snapshot manifest restore" {
    const gpa = galloc.allocator();
    const b = bench.current();
    var src = Manifest.init(gpa);
    defer src.deinit();
    try load(gpa, &src);
    const bytes = try src.serialize(gpa);
    defer gpa.free(bytes);
    // Untimed: the first restore is slower by up to 80 ms with 84,000
    // tenants, by an amount set by what allocated before it, not by
    // the bytes.
    try restore(gpa, bytes);
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        try restore(gpa, bytes);
        b.addBytes(bytes.len);
    }
}
