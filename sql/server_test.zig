// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const pgwire = @import("pgwire");

/// The SQLSTATE `replyStorageErr` answers `err` with.
fn storageCode(err: anyerror) ![5]u8 {
    const alloc = testing.allocator;
    var aw: std.Io.Writer.Allocating = .init(alloc);
    defer aw.deinit();
    var rdr = std.Io.Reader.fixed("");
    var state = pgwire.ConnState.init(alloc, &rdr, &aw.writer, .{});
    defer state.deinit();
    try sql.server.replyStorageErr(&state, err, "scan failed");
    return state.error_code;
}

test "server: a failed storage read is answered by what failed" {
    const codes = pgwire.sqlstate;
    // The bytes were read and are wrong.
    const corrupt = [_]anyerror{
        error.CorruptTable,
        error.ChecksumMismatch,
        error.CorruptPosting,
        error.CorruptIndexEntry,
    };
    for (corrupt) |err| {
        try testing.expectEqual(codes.data_corrupted, try storageCode(err));
    }
    // The store did not give the bytes: the simulated disk's name for
    // it, and the one a real disk's error comes under.
    const unread = [_]anyerror{ error.InputOutput, error.Unexpected };
    for (unread) |err| {
        try testing.expectEqual(codes.io_error, try storageCode(err));
    }
    try testing.expectEqual(
        codes.out_of_memory,
        try storageCode(error.OutOfMemory),
    );
}
