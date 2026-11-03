// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! pgwire — Postgres v3 wire protocol surface for egres.
//!
//! Layered structure:
//!   - proto / conn_state / parser / types: pure logic, no I/O. Do
//!     not import the fiber runtime in any of them.
//!   - driver: the only place sockets / fiber / syscalls appear.
//!
//! See pgwire/README.md for usage.

pub const proto = @import("pgwire/proto.zig");
pub const conn_state = @import("pgwire/conn_state.zig");
pub const parser = @import("pgwire/parser.zig");
pub const driver = @import("pgwire/driver.zig");
pub const types = @import("pgwire/types.zig");
pub const binary = @import("pgwire/binary.zig");

// Driver — one connection's I/O plumbing; the front-end runs the loop.
pub const Session = driver.Session;
pub const Options = driver.Options;
pub const Stream = driver.Stream;

// State machine — exposed for callers that want to drive pgwire from
// somewhere other than fiber.net.Stream (e.g. tests, alternate runtimes).
pub const ConnState = conn_state.ConnState;
pub const Config = conn_state.Config;
pub const Event = conn_state.Event;
pub const Phase = conn_state.Phase;
pub const ExecuteCtx = conn_state.ExecuteCtx;
pub const DescribeCtx = conn_state.DescribeCtx;
pub const Params = conn_state.Params;
pub const Portal = conn_state.Portal;
pub const DescribeKind = conn_state.DescribeKind;

// Parser — libpg_query, the parse tree read in place.
pub const parse = parser.parse;
pub const ParseTree = parser.Tree;
pub const ParseError = parser.ParseError;
pub const ErrorInfo = parser.ErrorInfo;

// Shared protocol types.
pub const FieldDescription = types.FieldDescription;
pub const WireFormat = types.WireFormat;
pub const Oid = types.Oid;
pub const oid = types.oid;
pub const TxStatus = types.TxStatus;
pub const sqlstate = types.sqlstate;
pub const severity_fatal = types.severity_fatal;

// Re-export proto.* for callers that want to talk frames directly
// (test fixtures, alternative drivers).
pub const DataValue = proto.DataValue;
pub const ErrorField = proto.ErrorField;
pub const BackendTag = proto.BackendTag;
pub const FrontendTag = proto.FrontendTag;
pub const decodeMessage = proto.decodeMessage;
pub const decodeStartup = proto.decodeStartup;

test {
    const std = @import("std");
    std.testing.refAllDecls(@This());
}
