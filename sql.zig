// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! sql — the relational layer's physical row codec.
//!
//! value:  Type / Value / Row — runtime SQL types and typed cells.
//! schema: LogicalColumn / LogicalSchema / PhysicalSchema / compile — the
//!         logical catalog view and the compiled byte layout for a version.
//! row:    marshalRow / unmarshalRow — encode a row, and decode a
//!         projection of columns in O(1) each.
//! catalog: table 1 of every namespace — the on-disk store of that
//!         database's table schemas, plus its own binary-compiled
//!         bootstrap schema.
//! parse:  statement — SQL text → a typed Stmt via libpg_query.
//! txn:    Txn — the transaction context DML statements run inside
//!         (read_ts snapshot + pending-write skiplist).
//! oracle: Oracle — recent commit timestamps by key identity hash,
//!         for write-conflict detection (see docs/txn_conflicts.md).
//! server: Server — the pgwire front-end over the engine: listener,
//!         per-database txn machinery, and the per-connection Handler
//!         (session state, statement dispatch, DML restart driver,
//!         transaction lifetime).
//! ddl:    CREATE/DROP DATABASE and TABLE — manifest and catalog
//!         choreography.
//! index_ddl: CREATE/DROP INDEX, with CREATE INDEX's online backfill
//!         for posting and unique indexes.
//! dml:    INSERT / SELECT / DELETE / UPDATE statement bodies.
//! pred:   Pred — a WHERE bound to its table (columns, typed literals)
//!         and its SQL three-valued evaluation per row.
//! expr:   SetNode — a SET value bound to its table (arithmetic and
//!         `||` over columns and literals) and its per-row evaluation.
//! exec:   the pull executor — RowNodes exchanging one Batch (rows
//!         copied once at the source + a survivor bit set) and
//!         BitmapNodes turning index reads into egid sets.
//! plan:   the planner — a bound Pred to a tree of exec nodes; the one
//!         place that chooses index vs. scan.

const std = @import("std");

pub const value = @import("sql/value.zig");
pub const schema = @import("sql/schema.zig");
pub const row = @import("sql/row.zig");
pub const catalog = @import("sql/catalog.zig");
pub const table = @import("sql/table.zig");
pub const key = @import("sql/key.zig");
pub const parse = @import("sql/parse.zig");
pub const database = @import("sql/database.zig");
pub const txn = @import("sql/txn.zig");
pub const oracle = @import("sql/oracle.zig");
pub const server = @import("sql/server.zig");
pub const ddl = @import("sql/ddl.zig");
pub const index_ddl = @import("sql/index_ddl.zig");
pub const dml = @import("sql/dml.zig");
pub const exec = @import("sql/exec.zig");
pub const plan = @import("sql/plan.zig");
pub const pred = @import("sql/pred.zig");
pub const expr = @import("sql/expr.zig");
pub const vector = @import("sql/vector.zig");

pub const Server = server.Server;
pub const default_database = server.default_database;

pub const Type = value.Type;
pub const Value = value.Value;
pub const Interval = value.Interval;
pub const Row = value.Row;

pub const LogicalColumn = schema.LogicalColumn;
pub const LogicalSchema = schema.LogicalSchema;
pub const ColumnLayout = schema.ColumnLayout;
pub const PhysicalSchema = schema.PhysicalSchema;
pub const compile = schema.compile;

pub const marshalRow = row.marshalRow;
pub const unmarshalRow = row.unmarshalRow;

test {
    std.testing.refAllDecls(@This());
}
