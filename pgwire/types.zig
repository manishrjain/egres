// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Shared pgwire types and constants. No I/O.
//!
//! The Postgres wire protocol is document-of-record; OIDs come from
//! `src/include/catalog/pg_type.dat` in the upstream source. We only
//! enumerate the OIDs we currently emit. Adding new types means adding
//! to this table.

const std = @import("std");
const utils = @import("utils");
const String = utils.String;

pub const Oid = u32;

/// Subset of pg_type OIDs we emit. RowDescription always advertises
/// text format (format code 0) regardless of what `pgwire/binary.zig`
/// can actually encode.
/// TODO: publish per-column binary format codes in RowDescription
/// when the handler opts in.
pub const oid = struct {
    pub const text: Oid = 25;
    pub const int2: Oid = 21;
    pub const int4: Oid = 23;
    pub const int8: Oid = 20;
    pub const bool_: Oid = 16;
    pub const float4: Oid = 700;
    pub const float8: Oid = 701;
    pub const numeric: Oid = 1700;
    pub const timestamp: Oid = 1114;
    pub const timestamptz: Oid = 1184;
    pub const date: Oid = 1082;
    pub const bpchar: Oid = 1042;
    pub const varchar: Oid = 1043;
    pub const unknown: Oid = 705;
};

/// Postgres's datetime epoch, 2000-01-01 00:00:00 UTC, in Unix µs. Only
/// the BINARY wire format uses it: a `timestamp`/`timestamptz` travels
/// as int8 µs and a `date` as int4 days, both counted from this instant.
/// Egres stores and computes in Unix time; an encoder subtracts this
/// before the byte swap and a decoder adds it after. Text format never
/// sees it.
pub const pg_epoch_unix_us: i64 = 946_684_800_000_000;

/// One column descriptor for a result set. `format_code = 0` means text.
/// `type_modifier = -1` means "no modifier" (e.g. for varchar(N), N is
/// encoded here; we don't carry varchars yet).
pub const FieldDescription = struct {
    name: String,
    type_oid: Oid = oid.text,
    type_size: i16 = -1,
    type_modifier: i32 = -1,
    format_code: i16 = 0,
    /// Optional table OID + attribute number; 0/0 means "not from a
    /// table" (literal/expression). Defaulted to 0/0 because we have
    /// no catalog/query plan to derive real values from.
    /// TODO: derive table_oid + column_attr from the executor's plan
    /// once one exists.
    table_oid: Oid = 0,
    column_attr: i16 = 0,
};

/// SQLSTATE codes for ErrorResponse 'C' fields (like `oid` above, a
/// namespace so one export covers them all). 'XX000' is the catch-all
/// "internal_error" — used for everything without a specific code yet.
/// TODO: mirror Postgres' SQLSTATE taxonomy and emit the correct code
/// per error class (constraint violation, type mismatch, etc.).
pub const sqlstate = struct {
    pub const internal_error: [5]u8 = .{ 'X', 'X', '0', '0', '0' };
    pub const snapshot_too_old: [5]u8 = .{ '7', '2', '0', '0', '0' };
    pub const syntax_error: [5]u8 = .{ '4', '2', '6', '0', '1' };
    pub const duplicate_table: [5]u8 = .{ '4', '2', 'P', '0', '7' };
    pub const undefined_table: [5]u8 = .{ '4', '2', 'P', '0', '1' };
    pub const wrong_object_type: [5]u8 = .{ '4', '2', '8', '0', '9' };
    pub const undefined_object: [5]u8 = .{ '4', '2', '7', '0', '4' };
    pub const undefined_column: [5]u8 = .{ '4', '2', '7', '0', '3' };
    pub const undefined_function: [5]u8 = .{ '4', '2', '8', '8', '3' };
    pub const datatype_mismatch: [5]u8 = .{ '4', '2', '8', '0', '4' };
    pub const invalid_catalog_name: [5]u8 = .{ '3', 'D', '0', '0', '0' };
    pub const object_in_use: [5]u8 = .{ '5', '5', '0', '0', '6' };
    pub const feature_not_supported: [5]u8 = .{ '0', 'A', '0', '0', '0' };
    pub const protocol_violation: [5]u8 = .{ '0', '8', 'P', '0', '1' };
    pub const invalid_text_representation: [5]u8 = .{ '2', '2', 'P', '0', '2' };
    pub const string_data_right_truncation: [5]u8 = .{ '2', '2', '0', '0', '1' };
    pub const character_not_in_repertoire: [5]u8 = .{ '2', '2', '0', '2', '1' };
    pub const numeric_value_out_of_range: [5]u8 = .{ '2', '2', '0', '0', '3' };
    pub const division_by_zero: [5]u8 = .{ '2', '2', '0', '1', '2' };
    pub const not_null_violation: [5]u8 = .{ '2', '3', '5', '0', '2' };
    pub const unique_violation: [5]u8 = .{ '2', '3', '5', '0', '5' };
    pub const serialization_failure: [5]u8 = .{ '4', '0', '0', '0', '1' };
    pub const deadlock_detected: [5]u8 = .{ '4', '0', 'P', '0', '1' };
    pub const active_sql_transaction: [5]u8 = .{ '2', '5', '0', '0', '1' };
    pub const in_failed_sql_transaction: [5]u8 = .{ '2', '5', 'P', '0', '2' };
    pub const admin_shutdown: [5]u8 = .{ '5', '7', 'P', '0', '1' };
    pub const out_of_memory: [5]u8 = .{ '5', '3', '2', '0', '0' };
};

/// Severity strings used in ErrorResponse 'S' / 'V' fields. FATAL is
/// an error after which the server closes the connection.
pub const severity_error = "ERROR";
pub const severity_fatal = "FATAL";

/// Postgres protocol version 3.0, big-endian on the wire.
pub const protocol_version_3: u32 = (3 << 16) | 0;

/// SSLRequest / GSSEncRequest sentinel codes that some clients send
/// before the real Startup. We sniff for these so we can answer 'N'
/// (decline) and let the client retry without TLS.
pub const ssl_request_code: u32 = (1234 << 16) | 5679;
pub const gss_enc_request_code: u32 = (1234 << 16) | 5680;
pub const cancel_request_code: u32 = (1234 << 16) | 5678;

/// Status byte values for ReadyForQuery.
pub const TxStatus = enum(u8) {
    idle = 'I',
    in_tx = 'T',
    failed_tx = 'E',
};

test {
    std.testing.refAllDecls(@This());
}
