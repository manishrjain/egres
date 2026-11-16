// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! A raft snapshot's manifest, as an object in the store. The log and
//! each node's snapshot.bin carry only a `SnapshotObject` naming it.
//! Inline, the manifest, 43 MB with 84,000 databases, was written to
//! every node's WAL and sent to every follower, after one lz4 call
//! over all of it that held its worker.
//!
//! The object is the serialized manifest cut into blocks, each
//! compressed with lz4 and framed with its own hash (`zbuf.Frame`),
//! then zero padded to a page multiple, as `Store.put` takes it. Each
//! block is a yield point on the way in and on the way out, and a
//! block that tore or rotted fails its hash. Blocks that each pass but
//! are not the leader's, out of order or from another object, fail
//! the manifest's hash in the `SnapshotObject`, which came through
//! raft.

const std = @import("std");
const fiber = @import("zfiber");
const utils = @import("utils");
const String = utils.String;
const vfs = @import("vfs");
const zbuf = @import("zbuf").zbuf;
const compression = utils.compression;
const _zen = @import("buffers.gen.zig");

/// A fresh object id. Random, as an SST's is, so two leaders that build
/// a snapshot at the same index never write one name: a store object
/// is written once. Zero is the "no snapshot" sentinel, and `taken`,
/// the installed snapshot's id, is redrawn too: a put under it would
/// overwrite the object a restart reads, and the sweep that deletes it
/// once the next snapshot installs would take the new one.
pub fn newId(taken: u64) u64 {
    while (true) {
        const id = fiber.random().int(u64);
        if (id != 0 and id != taken) return id;
    }
}

/// Raw bytes per block. lz4 matches reach 64 KiB back at most, so a
/// larger block saves nothing more, and one this size compresses in
/// about 100 µs.
pub const block_size = 64 << 10;

const ext = ".snap";

pub fn objectName(alloc: std.mem.Allocator, id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "{x:0>16}" ++ ext, .{id});
}

/// The id in a snapshot object's name, or null when `name` is not one:
/// an SST or anything else the store holds.
pub fn parseName(name: String) ?u64 {
    if (name.len != 16 + ext.len) return null;
    if (!std.mem.endsWith(u8, name, ext)) return null;
    return std.fmt.parseInt(u64, name[0..16], 16) catch null;
}

/// Page-aligned bytes, as `Store.put` takes them.
pub const Bytes = std.array_list.Aligned(u8, vfs.page_alignment);

/// What `pack` returns: the object, and the manifest's hash for its
/// `SnapshotObject`.
pub const Object = struct {
    /// The frames, zero padded to a page multiple: what the store
    /// keeps. Its capacity was sized for blocks that do not compress.
    bytes: Bytes,
    hash: u64,

    pub fn deinit(self: *Object, alloc: std.mem.Allocator) void {
        self.bytes.deinit(alloc);
    }
};

/// `raw`, a serialized manifest, as its object.
pub fn pack(alloc: std.mem.Allocator, raw: String) !Object {
    // Sized up front, so no block moves once written: growing a buffer
    // of this size copies all of it, in a step that does not yield.
    // Pages past the output are never touched.
    const blocks = std.math.divCeil(usize, raw.len, block_size) catch
        unreachable;
    const per_block = zbuf.Frame.hdr_size +
        compression.bound(.lz4, block_size);
    var out = try Bytes.initCapacity(
        alloc,
        std.mem.alignForward(usize, blocks * per_block, vfs.page_size),
    );
    errdefer out.deinit(alloc);

    // Hashed a block at a time, beside its compression, so the pass
    // over the whole keeps the same yield points.
    var hasher = std.hash.XxHash3.init(0);
    var off: usize = 0;
    while (off < raw.len) : (off += block_size) {
        try fiber.ctx().maybeYield();

        const block = raw[off..@min(off + block_size, raw.len)];
        hasher.update(block);
        // Compressed straight behind its frame's prefix, which is
        // stamped over it after, with no copy.
        const start = out.items.len;
        const payload = out.unusedCapacitySlice()[zbuf.Frame.hdr_size..];
        const n = try compression.compressTo(.lz4, block, payload);
        out.items.len += zbuf.Frame.hdr_size + n;
        try zbuf.Frame.write(out.items[start..], payload[0..n]);
    }
    const len = out.items.len;
    const pad = std.mem.alignForward(usize, len, vfs.page_size) - len;
    @memset(out.addManyAsSliceAssumeCapacity(pad), 0);
    return .{ .bytes = out, .hash = hasher.final() };
}

/// The serialized manifest in `object`, as `snap` describes it: the
/// object checked against `snap.size`, each block against its frame's
/// hash, and the whole against `snap.hash`. Caller frees.
pub fn unpack(
    alloc: std.mem.Allocator,
    object: String,
    snap: _zen.SnapshotObjectStruct,
) ![]u8 {
    if (object.len != snap.size) return error.CorruptSnapshot;
    const raw = try alloc.alloc(u8, @intCast(snap.raw_size));
    errdefer alloc.free(raw);

    var hasher = std.hash.XxHash3.init(0);
    var at: usize = 0;
    var done: usize = 0;
    while (done < raw.len) {
        try fiber.ctx().maybeYield();

        const payload = zbuf.Frame.read(object[at..]) catch
            return error.CorruptSnapshot;
        at += zbuf.Frame.hdr_size + payload.len;
        const n = compression.decompressTo(.lz4, payload, raw[done..]) catch
            return error.CorruptSnapshot;
        hasher.update(raw[done..][0..n]);
        done += n;
    }
    if (hasher.final() != snap.hash) return error.CorruptSnapshot;
    return raw;
}

/// The serialized manifest `snap` names, read from `store` and
/// unpacked. Caller frees.
pub fn read(
    alloc: std.mem.Allocator,
    store: *vfs.Store,
    snap: _zen.SnapshotObjectStruct,
) ![]u8 {
    const name = try objectName(alloc, snap.id);
    defer alloc.free(name);

    var object = try store.open(name, snap.size, .{});
    defer object.close();
    const got = try object.readAligned(alloc, 0, @intCast(snap.size));
    defer got.free();

    return unpack(alloc, got.data, snap);
}
