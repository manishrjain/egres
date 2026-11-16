// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! A snapshot's manifest written and read back, on a real one: the
//! snapshot in `snapshot.bin` from a server's WAL directory, named by
//! EGRES_SNAPSHOT_BIN (skipped when it is not set), its manifest read
//! from the store directory EGRES_STORE_DIR names. A snapshot.bin from
//! before the manifest moved to the store holds it inline and needs no
//! store. The tenancy benchmark's 84,000 databases give a 43 MB
//! manifest.
//!
//! Run with `EGRES_SNAPSHOT_BIN=<wal>/snapshot.bin
//! EGRES_STORE_DIR=<store> zig build bench -Drelease -- --filter
//! "snapshot manifest"`.

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
    const raw = try loadRaw(gpa);
    defer gpa.free(raw);
    try m.restoreFrom(raw);
}

/// The serialized manifest of the snapshot EGRES_SNAPSHOT_BIN names.
/// Caller frees.
fn loadRaw(gpa: std.mem.Allocator) ![]u8 {
    const env = std.c.getenv("EGRES_SNAPSHOT_BIN") orelse
        return error.SkipZigTest;
    const path: String = std.mem.span(env);
    const file = try vfs.readFile(path, gpa);
    defer gpa.free(file);
    const payload = try zbuf.Frame.read(file);
    const raft_snap = try raft.buffers.SnapshotReader.parseRoot(payload);
    // Not a SnapshotObject: the manifest itself, from before it moved.
    const snap = zdb.buffers.SnapshotObjectReader.parseRoot(
        raft_snap.data(),
    ) catch return gpa.dupe(u8, raft_snap.data());
    const dir = std.c.getenv("EGRES_STORE_DIR") orelse
        return error.SkipZigTest;
    var store = try vfs.Store.initFile(gpa, std.mem.span(dir), vfs.g());
    defer store.deinit();
    return zdb.snapshot.read(gpa, &store, try snap.toStructValue(gpa));
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

test "BENCH: snapshot manifest pack" {
    const gpa = galloc.allocator();
    const b = bench.current();
    const raw = try loadRaw(gpa);
    defer gpa.free(raw);
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        var object = try zdb.snapshot.pack(gpa, raw);
        object.deinit(gpa);
        b.addBytes(raw.len);
    }
}

test "BENCH: snapshot manifest unpack" {
    const gpa = galloc.allocator();
    const b = bench.current();
    const raw = try loadRaw(gpa);
    defer gpa.free(raw);
    var object = try zdb.snapshot.pack(gpa, raw);
    defer object.deinit(gpa);
    const snap: zdb.buffers.SnapshotObjectStruct = .{
        .size = object.bytes.items.len,
        .raw_size = raw.len,
        .hash = object.hash,
    };
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        const got = try zdb.snapshot.unpack(gpa, object.bytes.items, snap);
        gpa.free(got);
        b.addBytes(raw.len);
    }
}
