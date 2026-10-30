// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Statement parsing: SQL text → a small typed `Stmt`, via libpg_query
//! (reused from pgwire). Only the statements the server executes are
//! recognized — CREATE/DROP DATABASE, TABLE, INDEX; INSERT, SELECT,
//! DELETE, UPDATE; BEGIN, COMMIT, ROLLBACK; SET, RESET — everything
//! else maps to `.unsupported`. A statement we recognize but must
//! reject (unsupported type, constraint, clause, …) maps to `.invalid`
//! carrying the client-facing reason — parse succeeded, this is a
//! semantic rejection. Syntax errors propagate as ParseError so the
//! endpoint can answer with a 42601.
//!
//! `statement` takes an arena and returns a `Stmt` whose every string
//! and slice lives in it: the walk copies names and literals out of the
//! parse tree, which is freed before it returns, and nothing is freed
//! individually. The statement is valid only while the arena lives, and
//! the caller drops the arena when the statement is done.

const std = @import("std");
const pgwire = @import("pgwire");
const parser = pgwire.parser;
const _value = @import("value.zig");
const Column = @import("table.zig").Column;

const String = _value.String;

/// One parsed `name type [PRIMARY KEY | UNIQUE]` column. `name` is
/// borrowed. A PRIMARY KEY column is unique too, but `unique` marks
/// only a bare UNIQUE constraint: the PK gets its own index.
pub const ColumnDef = struct {
    name: String,
    ty: _value.Type,
    /// `char(n)` / `varchar(n)` length; null for every other type.
    type_mod: ?u32 = null,
    pkey: bool = false,
    unique: bool = false,
};

pub const CreateTable = struct {
    name: String, // table name (borrowed)
    if_not_exists: bool = false,
    columns: []ColumnDef, // borrowed (and each column name)
};

pub const DropTable = struct {
    name: String, // table name (borrowed)
    missing_ok: bool = false, // IF EXISTS
};

/// CREATE TABLE `name` AS TABLE `source` [WITH [NO] DATA], or AS
/// SELECT * FROM `source`: the new table is a branch of the source,
/// every row and index as of the statement
/// (zdb.forest_ddl.buildTables). WITH NO DATA takes the
/// definition and the indexes, empty.
pub const CreateTableAs = struct {
    name: String, // borrowed
    source: String, // borrowed
    if_not_exists: bool = false,
    with_data: bool = true,
};

pub const CreateIndex = struct {
    name: ?String, // borrowed; null = derive "{table}_{column}_idx"
    table: String, // borrowed
    column: String, // borrowed
    unique: bool = false,
    if_not_exists: bool = false,
};

pub const DropIndex = struct {
    name: String, // index name (borrowed)
    missing_ok: bool = false, // IF EXISTS
};

/// One VALUES cell, still untyped: coerced to its column's Type at execute
/// (value.parseValue). `str`/`float` borrow their bytes. `unsupported` is a
/// non-literal item (DEFAULT, an expression) the executor rejects.
pub const Literal = union(enum) {
    int: i64,
    float: String, // borrowed; libpg_query keeps floats as text
    str: String, // borrowed
    bool_: bool,
    null_value,
    /// CURRENT_TIMESTAMP / LOCALTIMESTAMP: the transaction's start
    /// time, resolved by the executor (dml.coerceLiteral) — the one
    /// literal that is not a constant.
    current_timestamp,
    /// `$n`, 1-based as the client wrote it: a value the Bind message
    /// supplies at execute (Params.toValue), typed by the column it
    /// meets like a quoted literal.
    param: u16,
    unsupported,
};

/// The highest placeholder index a statement may use (`$65535`);
/// Postgres's own limit.
pub const max_params: u16 = std.math.maxInt(u16);

/// The `$n` placeholders met while parsing one statement: the highest
/// index, which is how many values the client must bind.
const ParamTally = struct {
    count: u16 = 0,

    /// Record `$number`; null when it is not a usable index (`$0`, or
    /// above max_params).
    fn note(self: *ParamTally, number: i64) ?Literal {
        if (number < 1 or number > max_params) return null;
        const n: u16 = @intCast(number);
        self.count = @max(self.count, n);
        return .{ .param = n };
    }
};

/// The reason a `$number` cannot be bound, in Postgres's wording.
fn badParamMsg(alloc: std.mem.Allocator, number: i64) parser.ParseError!String {
    if (number < 1) {
        return std.fmt.allocPrint(
            alloc,
            "there is no parameter ${d}",
            .{number},
        );
    }
    return std.fmt.allocPrint(
        alloc,
        "parameter number ${d} exceeds the maximum of {d}",
        .{ number, max_params },
    );
}

pub const Insert = struct {
    table: String, // borrowed
    columns: ?[]String, // borrowed target columns; null = positional (all columns)
    rows: [][]Literal, // borrowed rows of literals
    /// How many `$n` values Bind must supply: the highest index used.
    param_count: u16 = 0,
};

/// What a SELECT projects: `*` (all live columns), a named list (which
/// may include the `egid` pseudo-column), or `count(*)` — one int8 cell,
/// the number of rows the plan yields. Names are borrowed.
pub const Projection = union(enum) {
    star,
    columns: []String,
    count_star,
};

/// A single `column = literal` comparison — the equality leaf of a WHERE.
pub const Eq = struct {
    column: String, // borrowed
    literal: Literal,
};

/// A parsed WHERE tree. Leaves name one column; AND/OR hold their
/// children inline (a slice is already an indirection, and children
/// are never shared between parents); NOT is a pointer only because a
/// union cannot contain itself by value. Everything borrows the arena.
pub const Where = union(enum) {
    eq: Eq,
    is_null: NullTest,
    and_: []const Where,
    or_: []const Where,
    not: *const Where,

    /// `column IS NULL` (negate = false) / `column IS NOT NULL`.
    pub const NullTest = struct {
        column: String, // borrowed
        negate: bool,
    };
};

pub const Select = struct {
    table: String, // borrowed
    projection: Projection,
    where: ?Where,
    /// FOR UPDATE: lock every matched row exclusively (held to txn end).
    for_update: bool = false,
    /// LIMIT n: stop after n rows. null = every row (LIMIT ALL too).
    limit: ?usize = null,
    /// How many `$n` values Bind must supply: the highest index used.
    param_count: u16 = 0,
};

/// One target of a SELECT without FROM: a constant and its output
/// name, the alias or Postgres's `?column?`.
pub const Cell = struct {
    name: String, // borrowed
    value: Literal,
};

/// SELECT constants with no FROM, the one-row answer a pool or driver
/// uses to check a connection (`SELECT 1`). Constants only: a
/// function, a column, an expression or a `$n` is refused.
pub const SelectRow = struct {
    cells: []Cell, // borrowed
    /// LIMIT 0 drops the row; anything else keeps it.
    limit: ?usize = null,
};

pub const Delete = struct {
    table: String, // borrowed
    where: ?Where, // null = delete every row
    /// How many `$n` values Bind must supply: the highest index used.
    param_count: u16 = 0,
};

/// A SET value as the grammar built it: precedence and parentheses
/// are already in the tree's shape, and a minus before a number is
/// already folded into a negative literal (gram.y's doNegate), so
/// `-col` is the only unary form — it parses as `0 - col`; `+x` is
/// `x`. Everything borrows the arena; children are pointers only
/// because a union cannot hold itself by value.
pub const Expr = union(enum) {
    literal: Literal,
    column: String, // borrowed
    binop: BinOp,

    pub const Op = enum { add, sub, mul, div, mod, concat };
    pub const BinOp = struct { op: Op, lhs: *const Expr, rhs: *const Expr };
};

/// One SET assignment: `column = expr`.
pub const SetClause = struct {
    column: String, // borrowed
    value: Expr,
};

pub const Update = struct {
    table: String, // borrowed
    sets: []SetClause, // borrowed
    where: ?Where, // null = update every row
    /// How many `$n` values Bind must supply: the highest index used.
    param_count: u16 = 0,
};

/// CREATE DATABASE `name` [TEMPLATE `source`]: with a source, the new
/// database is a branch of it, every row as of the statement
/// (zdb.forest_ddl.buildNsBranch). Postgres's keyword; its TEMPLATE
/// copies the data too.
pub const CreateDatabase = struct {
    name: String, // borrowed
    source: ?String = null, // borrowed
};

pub const Stmt = union(enum) {
    create_database: CreateDatabase,
    drop_database: String, // database name (borrowed)
    create_table: CreateTable,
    create_table_as: CreateTableAs,
    drop_table: DropTable,
    create_index: CreateIndex,
    drop_index: DropIndex,
    insert: Insert,
    select: Select,
    select_row: SelectRow,
    delete: Delete,
    update: Update,
    begin_txn,
    commit_txn,
    rollback_txn,
    /// SET and RESET of any parameter, accepted and ignored: drivers
    /// send them on connect, and no setting changes anything here.
    /// SET TRANSACTION is ignored the way BEGIN's modes are.
    set_var,
    reset_var,
    invalid: String, // recognized but rejected; reason
    unsupported,

    /// How many `$n` values Bind must supply. Only DML carries
    /// placeholders; every other statement needs none.
    pub fn paramCount(self: Stmt) u16 {
        return switch (self) {
            .insert => |ins| ins.param_count,
            .select => |sel| sel.param_count,
            .delete => |del| del.param_count,
            .update => |up| up.param_count,
            else => 0,
        };
    }
};

const c = parser.c;

pub fn statement(alloc: std.mem.Allocator, sql: String) parser.ParseError!Stmt {
    // `alloc` is an arena: the Stmt's strings and slices land in it and
    // are never freed one by one. The tree itself lives in libpg_query's
    // per-thread context and goes at the end of this call, so the walk
    // below never parks (see pgwire.parser).
    const tree = try parser.parse(alloc, sql, null);
    defer tree.deinit();
    const node = tree.statement(0) orelse return .unsupported;

    // CREATE and DROP DATABASE are their own kinds, both carrying the
    // name in `dbname`.
    if (parser.cast(c.CreatedbStmt, node)) |s|
        return parseCreateDatabase(alloc, s);
    if (parser.cast(c.DropdbStmt, node)) |s|
        return .{ .drop_database = try dup(alloc, s.dbname) };
    if (parser.cast(c.CreateStmt, node)) |s| return parseCreateTable(alloc, s);
    if (parser.cast(c.CreateTableAsStmt, node)) |s|
        return parseCreateTableAs(alloc, s);
    if (parser.cast(c.DropStmt, node)) |s| return parseDropTable(alloc, s);
    if (parser.cast(c.IndexStmt, node)) |s| return parseCreateIndex(alloc, s);
    if (parser.cast(c.InsertStmt, node)) |s| return parseInsert(alloc, s);
    if (parser.cast(c.SelectStmt, node)) |s| return parseSelect(alloc, s);
    if (parser.cast(c.DeleteStmt, node)) |s| return parseDelete(alloc, s);
    if (parser.cast(c.UpdateStmt, node)) |s| return parseUpdate(alloc, s);
    if (parser.cast(c.TransactionStmt, node)) |s| return parseTransaction(s);
    if (parser.cast(c.VariableSetStmt, node)) |s| return parseSet(s);
    return .unsupported;
}

// ---------------------------------------------------------------------------
// Reading nodes. The tree is Postgres's own: a name is a List of String
// nodes (`public.t`), a constant an A_Const whose `val` is tagged
// Integer / Float / String / Boolean, an enum a C enum. Every string a
// Stmt keeps is copied into the arena.
// ---------------------------------------------------------------------------

/// Of the options, only TEMPLATE means anything here; the rest
/// (OWNER, ENCODING, ...) are accepted and ignored, as the catalog
/// they describe does not exist.
fn parseCreateDatabase(
    alloc: std.mem.Allocator,
    s: *c.CreatedbStmt,
) parser.ParseError!Stmt {
    var cd: CreateDatabase = .{ .name = try dup(alloc, s.dbname) };
    for (parser.items(s.options)) |cell| {
        const de = parser.cast(c.DefElem, cell.ptr_value) orelse continue;
        if (!std.mem.eql(u8, parser.str(de.defname), "template")) continue;

        const arg = parser.cast(c.String, de.arg) orelse
            return invalid("TEMPLATE takes a database name");
        cd.source = try dup(alloc, arg.sval);
    }
    return .{ .create_database = cd };
}

fn dup(alloc: std.mem.Allocator, s: [*c]const u8) parser.ParseError!String {
    return alloc.dupe(u8, parser.str(s));
}

/// The last part of a qualified name (`public.t` → `t`), copied; null
/// when the list is empty or its last item is not a String.
fn lastName(
    alloc: std.mem.Allocator,
    names: [*c]c.List,
) parser.ParseError!?String {
    const n = parser.items(names).len;
    if (n == 0) return null;
    const s = parser.cast(c.String, parser.nodeAt(names, n - 1)) orelse
        return null;
    return try dup(alloc, s.sval);
}

fn relname(
    alloc: std.mem.Allocator,
    rv: [*c]c.RangeVar,
) parser.ParseError!String {
    if (rv == null) return "";
    return dup(alloc, rv.*.relname);
}

/// A constant as a Literal, its text copied. Integers past int32 arrive
/// as Float, the digits as text, which is what `.float` carries.
fn literalFrom(
    alloc: std.mem.Allocator,
    ac: *const c.A_Const,
) parser.ParseError!Literal {
    if (ac.isnull) return .null_value;
    return switch (ac.val.node.type) {
        c.T_Integer => .{ .int = ac.val.ival.ival },
        c.T_Float => .{ .float = try dup(alloc, ac.val.fval.fval) },
        c.T_String => .{ .str = try dup(alloc, ac.val.sval.sval) },
        c.T_Boolean => .{ .bool_ = ac.val.boolval.boolval },
        else => .unsupported,
    };
}

/// CREATE TABLE. Only bare `name type` columns with an optional
/// single-column PRIMARY KEY or UNIQUE parse; any other constraint (NOT
/// NULL, FK, DEFAULT, table-level) is a clean `.invalid`.
fn parseCreateTable(
    alloc: std.mem.Allocator,
    cs: *const c.CreateStmt,
) parser.ParseError!Stmt {
    var columns: std.ArrayList(ColumnDef) = .empty;
    var pkey_seen = false;
    for (parser.items(cs.tableElts)) |cell| {
        const cd = parser.cast(c.ColumnDef, cell.ptr_value) orelse {
            if (parser.cast(c.Constraint, cell.ptr_value) != null)
                return invalid("table-level constraints are not supported");
            return .unsupported;
        };
        const colname = try dup(alloc, cd.colname);
        if (std.ascii.eqlIgnoreCase(colname, "egid"))
            return invalid("column name \"egid\" is reserved");
        for (columns.items) |col| {
            if (std.mem.eql(u8, col.name, colname))
                return invalidFmt(
                    alloc,
                    "duplicate column \"{s}\"",
                    .{colname},
                );
        }

        // `int` arrives pg_catalog-qualified: names = [pg_catalog, int4];
        // `char(84)` is [pg_catalog, bpchar] with typmods = [84]. A bare
        // `char` arrives as `bpchar` with typmods [1] (the grammar fills
        // it), a bare `varchar` with none.
        if (cd.typeName == null) return .unsupported;
        const tn = cd.typeName.*;
        const tyname = try lastName(alloc, tn.names) orelse return .unsupported;
        var ty = _value.Type.fromName(tyname) orelse
            return invalidFmt(alloc, "unsupported type \"{s}\"", .{tyname});

        // A length modifier: only `char(n)` / `varchar(n)` take one, a
        // single integer in 1..max_type_mod. A lengthless `char` is
        // `char(1)`; a lengthless `varchar` stays `.text`.
        const typmods = parser.items(tn.typmods);
        var type_mod: ?u32 = null;
        if (typmods.len > 0) {
            const is_varchar = std.mem.eql(u8, tyname, "varchar");
            const is_vector = ty == .vector;
            if (ty != .bpchar and !is_varchar and !is_vector)
                return invalidFmt(alloc, "type modifier is not allowed for type \"{s}\"", .{tyname});
            const ac = if (typmods.len == 1)
                parser.cast(c.A_Const, typmods[0].ptr_value)
            else
                null;
            if (ac == null or ac.?.val.node.type != c.T_Integer)
                return invalidFmt(
                    alloc,
                    "invalid type modifier for type \"{s}\"",
                    .{tyname},
                );
            const n = ac.?.val.ival.ival;
            if (n < 1)
                return invalidFmt(alloc, "length for type {s} must be at least 1", .{tyname});
            if (is_vector and n > _value.Type.max_vector_dimensions)
                return invalidFmt(alloc, "dimensions for type vector cannot exceed {d}", //
                    .{_value.Type.max_vector_dimensions});
            if (n > _value.Type.max_type_mod)
                return invalidFmt(alloc, "length for type {s} cannot exceed {d}", //
                    .{ tyname, _value.Type.max_type_mod });
            type_mod = @intCast(n);
            if (is_varchar) ty = .varchar;
        } else if (ty == .bpchar) {
            type_mod = 1;
        } else if (ty == .vector) {
            return invalid("vector requires a dimension");
        }

        var pkey = false;
        var unique = false;
        for (parser.items(cd.constraints)) |ccell| {
            const con = parser.cast(c.Constraint, ccell.ptr_value) orelse
                return .unsupported;
            switch (con.contype) {
                c.CONSTR_PRIMARY => pkey = true,
                c.CONSTR_UNIQUE => unique = true,
                else => return invalid(
                    "only PRIMARY KEY and UNIQUE column constraints are supported",
                ),
            }
        }
        if (pkey) {
            if (pkey_seen)
                return invalid("multiple PRIMARY KEY columns are not supported");
            pkey_seen = true;
            unique = false; // the PK index already enforces it
        }

        try columns.append(alloc, .{
            .name = colname,
            .ty = ty,
            .type_mod = type_mod,
            .pkey = pkey,
            .unique = unique,
        });
    }

    return .{ .create_table = .{
        .name = try relname(alloc, cs.relation),
        .if_not_exists = cs.if_not_exists,
        .columns = try columns.toOwnedSlice(alloc),
    } };
}

/// DROP TABLE / DROP INDEX (one DropStmt serves both, discriminated by
/// removeType). Other object kinds (VIEW, …) are `.unsupported`;
/// multi-object DROPs are rejected.
fn parseDropTable(
    alloc: std.mem.Allocator,
    ds: *const c.DropStmt,
) parser.ParseError!Stmt {
    const is_index = ds.removeType == c.OBJECT_INDEX;
    if (!is_index and ds.removeType != c.OBJECT_TABLE) return .unsupported;
    const objects = parser.items(ds.objects);
    if (objects.len != 1) {
        return if (is_index)
            invalid("DROP INDEX with multiple indexes is not supported")
        else
            invalid("DROP TABLE with multiple tables is not supported");
    }
    if (is_index and ds.concurrent)
        return invalid("DROP INDEX CONCURRENTLY is not supported");
    // Each object is itself a qualified name: a List of String.
    const parts = parser.cast(c.List, objects[0].ptr_value) orelse
        return .unsupported;
    const name = try lastName(alloc, parts) orelse return .unsupported;

    if (is_index) {
        return .{ .drop_index = .{
            .name = name,
            .missing_ok = ds.missing_ok,
        } };
    }
    return .{ .drop_table = .{
        .name = name,
        .missing_ok = ds.missing_ok,
    } };
}

/// CREATE [UNIQUE] INDEX. v1: a single plain column, whole-table,
/// default btree — every narrowing (CONCURRENTLY, partial,
/// multi-column, expression, INCLUDE, TABLESPACE, other access
/// methods, DESC / NULLS FIRST ordering) is a clean `.invalid`.
/// ASC / NULLS LAST are accepted: they are this index's semantics
/// already (the NULL tag byte sorts last).
/// The query must be the whole of one table: `TABLE x` or `SELECT *
/// FROM x`, nothing else (no WHERE, LIMIT, column list or ordering),
/// since the branch is of the table, not of a result. Only a table
/// (not a materialized view), with no column names of its own.
fn parseCreateTableAs(
    alloc: std.mem.Allocator,
    s: *const c.CreateTableAsStmt,
) parser.ParseError!Stmt {
    if (s.objtype != c.OBJECT_TABLE) return .unsupported;
    if (s.into == null) return .unsupported;
    const into = s.into.*;
    if (into.rel == null) return .unsupported;
    const rel = into.rel.*;
    if (into.colNames != null)
        return invalid("CREATE TABLE AS takes no column list");
    const ss = parser.cast(c.SelectStmt, s.query) orelse return .unsupported;
    const whole_table = ss.whereClause == null and ss.limitCount == null and
        ss.sortClause == null and ss.groupClause == null and
        ss.distinctClause == null and ss.havingClause == null and
        ss.limitOffset == null and ss.lockingClause == null and
        ss.withClause == null and ss.valuesLists == null;
    if (!whole_table)
        return invalid("CREATE TABLE AS supports TABLE x or SELECT * FROM x");
    const from = parser.items(ss.fromClause);
    if (from.len != 1) return invalid("CREATE TABLE AS reads one table");
    const rv = parser.cast(c.RangeVar, from[0].ptr_value) orelse
        return invalid("CREATE TABLE AS reads one table");
    const targets = parser.items(ss.targetList);
    if (targets.len != 1) return invalid("CREATE TABLE AS takes SELECT *");
    const rt = parser.cast(c.ResTarget, targets[0].ptr_value) orelse
        return .unsupported;
    const cr = parser.cast(c.ColumnRef, rt.val) orelse
        return invalid("CREATE TABLE AS takes SELECT *");
    const fields = parser.items(cr.fields);
    if (fields.len != 1 or parser.cast(c.A_Star, fields[0].ptr_value) == null)
        return invalid("CREATE TABLE AS takes SELECT *");
    return .{ .create_table_as = .{
        .name = try dup(alloc, rel.relname),
        .source = try dup(alloc, rv.relname),
        .if_not_exists = s.if_not_exists,
        .with_data = !into.skipData,
    } };
}

fn parseCreateIndex(
    alloc: std.mem.Allocator,
    is: *const c.IndexStmt,
) parser.ParseError!Stmt {
    if (is.concurrent)
        return invalid("CREATE INDEX CONCURRENTLY is not supported");
    if (is.whereClause != null)
        return invalid("partial indexes are not supported");
    if (parser.items(is.indexIncludingParams).len > 0)
        return invalid("INCLUDE columns are not supported");
    if (is.tableSpace != null) return invalid("TABLESPACE is not supported");
    const method = parser.str(is.accessMethod);
    if (method.len > 0 and !std.mem.eql(u8, method, "btree"))
        return invalidFmt(
            alloc,
            "access method \"{s}\" is not supported",
            .{method},
        );
    const params = parser.items(is.indexParams);
    if (params.len != 1)
        return invalid("only single-column indexes are supported");
    const elem = parser.cast(c.IndexElem, params[0].ptr_value) orelse
        return .unsupported;
    if (elem.name == null)
        return invalid("expression indexes are not supported");
    if (elem.ordering == c.SORTBY_DESC)
        return invalid("DESC index ordering is not supported");
    if (elem.nulls_ordering == c.SORTBY_NULLS_FIRST)
        return invalid("NULLS FIRST index ordering is not supported");

    return .{ .create_index = .{
        .name = if (is.idxname != null) try dup(alloc, is.idxname) else null,
        .table = try relname(alloc, is.relation),
        .column = try dup(alloc, elem.name),
        .unique = is.unique,
        .if_not_exists = is.if_not_exists,
    } };
}

/// INSERT ... VALUES. ON CONFLICT / RETURNING / INSERT…SELECT are rejected
/// (before any allocation, so no cleanup); arity, unknown columns, and literal
/// coercion are the executor's job.
fn parseInsert(
    alloc: std.mem.Allocator,
    is: *const c.InsertStmt,
) parser.ParseError!Stmt {
    if (is.onConflictClause != null) return invalid("ON CONFLICT is not supported");
    if (parser.items(is.returningList).len > 0)
        return invalid("RETURNING is not supported");
    // VALUES rows nest under a SelectStmt with `valuesLists`; an
    // INSERT ... SELECT has one without.
    const sel = parser.cast(c.SelectStmt, is.selectStmt) orelse
        return invalid("INSERT requires VALUES");
    const value_lists = parser.items(sel.valuesLists);
    if (value_lists.len == 0)
        return invalid("only INSERT ... VALUES is supported");

    var columns: ?[]String = null;
    const col_cells = parser.items(is.cols);
    if (col_cells.len > 0) {
        const cols = try alloc.alloc(String, col_cells.len);
        for (col_cells, cols) |cell, *dst| {
            const rt = parser.cast(c.ResTarget, cell.ptr_value) orelse
                return .unsupported;
            dst.* = try dup(alloc, rt.name);
        }
        columns = cols;
    }

    var params = ParamTally{};
    const rows = try alloc.alloc([]Literal, value_lists.len);
    for (value_lists, rows) |lcell, *row| {
        const list = parser.cast(c.List, lcell.ptr_value) orelse
            return .unsupported;
        const cells = parser.items(list);
        const lits = try alloc.alloc(Literal, cells.len);
        for (cells, lits) |cell, *lit| {
            if (parser.cast(c.ParamRef, cell.ptr_value)) |pr| {
                lit.* = params.note(pr.number) orelse
                    return .{ .invalid = try badParamMsg(alloc, pr.number) };
                continue;
            }
            lit.* = try valueLiteral(alloc, cell.ptr_value);
        }
        row.* = lits;
    }

    return .{ .insert = .{
        .table = try relname(alloc, is.relation),
        .columns = columns,
        .rows = rows,
        .param_count = params.count,
    } };
}

/// A VALUES item as a Literal: a constant, or a value function (a
/// `$n` is the caller's: it needs the statement's tally). Anything
/// else (DEFAULT, an expression) is `.unsupported`.
fn valueLiteral(
    alloc: std.mem.Allocator,
    p: ?*anyopaque,
) parser.ParseError!Literal {
    if (parser.cast(c.A_Const, p)) |ac| return literalFrom(alloc, ac);
    const svf = parser.cast(c.SQLValueFunction, p) orelse return .unsupported;
    // The precision forms (CURRENT_TIMESTAMP(3)) keep the full µs;
    // LOCALTIMESTAMP is the same instant on a UTC-only server.
    return switch (svf.op) {
        c.SVFOP_CURRENT_TIMESTAMP,
        c.SVFOP_CURRENT_TIMESTAMP_N,
        c.SVFOP_LOCALTIMESTAMP,
        c.SVFOP_LOCALTIMESTAMP_N,
        => .current_timestamp,
        else => .unsupported,
    };
}

pub const Params = struct {
    values: []const ?String = &.{},
    formats: []const i16 = &.{},

    /// `$index` as a cell of `col`. A text value takes the path a
    /// quoted literal takes — column-typed parsing, the string types'
    /// length rules; a binary one decodes through pgwire.binary for
    /// the six primitives it covers and is `error.UnsupportedType`
    /// for every other column type. An index past what Bind supplied
    /// is `error.UnboundParam` (the runner checks the count first, so
    /// only a bug reaches it).
    pub fn toValue(
        self: Params,
        alloc: std.mem.Allocator,
        index: u16,
        col: *const Column,
    ) !_value.Value {
        if (index == 0 or index > self.values.len) return error.UnboundParam;

        const bytes = self.values[index - 1] orelse return .null_value;
        if (self.isBinary(index)) return binaryValue(col.ty, bytes);
        if (col.ty.isString())
            return .{ .text = try _value.coerceString(bytes, col) };
        if (col.ty == .vector)
            return _value.parseVector(alloc, bytes, col.type_mod orelse 0);
        return _value.parseValue(col.ty, bytes);
    }

    /// `$index`'s bytes as bound, null for SQL NULL, with no column to
    /// type them (a placeholder under a SET operator). Text only: a
    /// binary value has no type to decode with, `error.UnsupportedType`.
    pub fn rawText(self: Params, index: u16) !?String {
        if (index == 0 or index > self.values.len) return error.UnboundParam;
        if (self.isBinary(index)) return error.UnsupportedType;
        return self.values[index - 1];
    }

    fn isBinary(self: Params, index: u16) bool {
        return index <= self.formats.len and self.formats[index - 1] == 1;
    }
};

/// A binary-format parameter as a cell of `ty`: the six primitives
/// pgwire.binary decodes; anything else has no decoder here.
fn binaryValue(ty: _value.Type, bytes: String) !_value.Value {
    const binary = pgwire.binary;
    return switch (ty) {
        .bool_ => .{ .bool_ = try binary.decodeBool(bytes) },
        .int2 => .{ .int2 = try binary.decodeInt2(bytes) },
        .int4 => .{ .int4 = try binary.decodeInt4(bytes) },
        .int8 => .{ .int8 = try binary.decodeInt8(bytes) },
        .float4 => .{ .float4 = try binary.decodeFloat4(bytes) },
        .float8 => .{ .float8 = try binary.decodeFloat8(bytes) },
        else => error.UnsupportedType,
    };
}

/// Coerce a parsed literal to a cell of `col`, reusing value.parseValue
/// by rendering non-string literals to text first (int/bool stringified
/// into `a`, so a text-typed column can borrow it). A string column goes
/// through value.coerceString with its length. NULL passes through; a
/// non-literal item is `error.UnsupportedLiteral`; a `$n` is the bound
/// value (Params.toValue).
pub fn litToValue(
    a: std.mem.Allocator,
    lit: Literal,
    col: *const Column,
    params: Params,
) !_value.Value {
    const text: String = switch (lit) {
        .null_value => return .null_value,
        .unsupported => return error.UnsupportedLiteral,
        // Resolved by the executor before coercion (dml.coerceLiteral);
        // a position without a transaction time (SET, WHERE) never
        // produces it.
        .current_timestamp => return error.UnsupportedLiteral,
        .param => |n| return params.toValue(a, n, col),
        .str, .float => |s| s,
        .int => |n| try std.fmt.allocPrint(a, "{d}", .{n}),
        .bool_ => |b| if (b) "t" else "f",
    };
    if (col.ty.isString())
        return .{ .text = try _value.coerceString(text, col) };
    if (col.ty == .vector)
        return _value.parseVector(a, text, col.type_mod orelse 0);
    return _value.parseValue(col.ty, text);
}

/// A parsed WHERE tree or a rejection reason (borrowed).
const ParsedWhere = union(enum) {
    where: Where,
    invalid: String,

    fn reject(msg: String) ParsedWhere {
        return .{ .invalid = msg };
    }
};

/// The column a WHERE operand names — the last field of a qualified
/// reference, copied — or null when the operand is not a plain column.
fn columnOf(alloc: std.mem.Allocator, p: anytype) parser.ParseError!?String {
    const cr = parser.cast(c.ColumnRef, p) orelse return null;
    return lastName(alloc, cr.fields);
}

/// The operator an A_Expr applies: the last part of its name, when it
/// is a plain `AEXPR_OP` with one.
fn operatorOf(ae: *const c.A_Expr) ?String {
    if (ae.kind != c.AEXPR_OP) return null;
    const n = parser.items(ae.name).len;
    if (n == 0) return null;
    const s = parser.cast(c.String, parser.nodeAt(ae.name, n - 1)) orelse
        return null;
    return parser.str(s.sval);
}

/// Parse a WHERE tree, shared by SELECT, DELETE, and UPDATE: `column =
/// literal` leaves, `column IS [NOT] NULL`, and AND / OR / NOT over
/// them. Anything else (another operator, a subquery, column-vs-column,
/// a bare boolean column) yields a clean `.invalid` reason; only OOM
/// propagates. AND/OR children land in one arena slice, NOT's in one
/// arena cell. Postgres flattens `a AND b AND c` into one node with
/// three args; NOT always carries exactly one.
fn parseWhere(
    alloc: std.mem.Allocator,
    params: *ParamTally,
    p: ?*c.Node,
) parser.ParseError!ParsedWhere {
    if (parser.cast(c.BoolExpr, p)) |be| {
        const args = parser.items(be.args);
        if (be.boolop == c.NOT_EXPR) {
            if (args.len != 1)
                return ParsedWhere.reject("NOT takes exactly one operand");
            const child = try alloc.create(Where);
            const arg = parser.asNode(args[0].ptr_value);
            child.* = switch (try parseWhere(alloc, params, arg)) {
                .where => |x| x,
                .invalid => |msg| return .{ .invalid = msg },
            };
            return .{ .where = .{ .not = child } };
        }
        const is_and = be.boolop == c.AND_EXPR;
        if (!is_and and be.boolop != c.OR_EXPR)
            return ParsedWhere.reject("unsupported WHERE clause");
        const kids = try alloc.alloc(Where, args.len);
        for (args, kids) |arg, *kid| {
            const node = parser.asNode(arg.ptr_value);
            kid.* = switch (try parseWhere(alloc, params, node)) {
                .where => |x| x,
                .invalid => |msg| return .{ .invalid = msg },
            };
        }
        return .{ .where = if (is_and) .{ .and_ = kids } else .{ .or_ = kids } };
    }

    if (parser.cast(c.NullTest, p)) |nt| {
        const column = try columnOf(alloc, nt.arg) orelse
            return ParsedWhere.reject("IS NULL operand must be a column");
        const negate = switch (nt.nulltesttype) {
            c.IS_NULL => false,
            c.IS_NOT_NULL => true,
            else => return ParsedWhere.reject("unsupported WHERE clause"),
        };
        return .{ .where = .{ .is_null = .{ .column = column, .negate = negate } } };
    }

    const ae = parser.cast(c.A_Expr, p) orelse
        return ParsedWhere.reject("unsupported WHERE clause");
    const op = operatorOf(ae) orelse "";
    if (!std.mem.eql(u8, op, "="))
        return ParsedWhere.reject(
            "only `column = value` comparisons are supported in WHERE",
        );

    const column = try columnOf(alloc, ae.lexpr) orelse
        return ParsedWhere.reject("WHERE left side must be a column");
    if (parser.cast(c.ParamRef, ae.rexpr)) |pr| {
        const lit = params.note(pr.number) orelse
            return .{ .invalid = try badParamMsg(alloc, pr.number) };
        return .{ .where = .{ .eq = .{ .column = column, .literal = lit } } };
    }
    const ac = parser.cast(c.A_Const, ae.rexpr) orelse
        return ParsedWhere.reject("WHERE right side must be a literal");
    const lit = try literalFrom(alloc, ac);
    if (lit == .unsupported)
        return ParsedWhere.reject("WHERE right side must be a literal");
    return .{ .where = .{ .eq = .{ .column = column, .literal = lit } } };
}

/// A parsed SET expression or a rejection reason (borrowed).
const ParsedExpr = union(enum) {
    expr: Expr,
    invalid: String,

    fn reject(msg: String) ParsedExpr {
        return .{ .invalid = msg };
    }
};

/// The operator an A_Expr names, or null for one SET does not do.
fn opFrom(name: String) ?Expr.Op {
    if (std.mem.eql(u8, name, "||")) return .concat;
    if (name.len != 1) return null;
    return switch (name[0]) {
        '+' => .add,
        '-' => .sub,
        '*' => .mul,
        '/' => .div,
        '%' => .mod,
        else => null,
    };
}

/// Parse a SET value: a literal, a `$n`, a column, or `+ - * / % ||`
/// over them, recursively. Anything else (a function, a cast, a
/// subquery, DEFAULT, another operator) yields a clean `.invalid`
/// reason; only OOM propagates. Each child lands in one arena cell.
fn parseExpr(
    arloc: std.mem.Allocator,
    params: *ParamTally,
    p: ?*c.Node,
) parser.ParseError!ParsedExpr {
    if (parser.cast(c.A_Const, p)) |ac| {
        const lit = try literalFrom(arloc, ac);
        if (lit == .unsupported)
            return ParsedExpr.reject("unsupported literal in SET");
        return .{ .expr = .{ .literal = lit } };
    }
    if (parser.cast(c.ParamRef, p)) |pr| {
        const lit = params.note(pr.number) orelse
            return .{ .invalid = try badParamMsg(arloc, pr.number) };
        return .{ .expr = .{ .literal = lit } };
    }
    if (parser.cast(c.ColumnRef, p) != null) {
        const column = try columnOf(arloc, p) orelse
            return ParsedExpr.reject("unsupported column reference in SET");
        return .{ .expr = .{ .column = column } };
    }
    const ae = parser.cast(c.A_Expr, p) orelse return ParsedExpr.reject(
        "SET value must be a literal, a column, or an operator " ++
            "expression over them",
    );
    const bad_op = "only + - * / % || are supported in SET";
    const op = opFrom(operatorOf(ae) orelse "") orelse
        return ParsedExpr.reject(bad_op);

    if (ae.rexpr == null) return ParsedExpr.reject(bad_op);
    const rhs = try arloc.create(Expr);
    rhs.* = switch (try parseExpr(arloc, params, parser.asNode(ae.rexpr))) {
        .expr => |e| e,
        .invalid => |msg| return .{ .invalid = msg },
    };
    const lhs = try arloc.create(Expr);
    if (ae.lexpr != null) {
        lhs.* = switch (try parseExpr(arloc, params, parser.asNode(ae.lexpr))) {
            .expr => |e| e,
            .invalid => |msg| return .{ .invalid = msg },
        };
    } else switch (op) {
        // Unary: `-x` is `0 - x`, `+x` is `x`. (`-1` never gets
        // here — the grammar folds it into the literal.)
        .sub => lhs.* = .{ .literal = .{ .int = 0 } },
        .add => return .{ .expr = rhs.* },
        else => return ParsedExpr.reject(bad_op),
    }
    return .{ .expr = .{ .binop = .{ .op = op, .lhs = lhs, .rhs = rhs } } };
}

/// SELECT [cols|*|count(*)] FROM one_table [WHERE ...] [FOR UPDATE]
/// [LIMIT n], or constants with no FROM (parseSelectRow). Joins,
/// subqueries, ORDER BY / GROUP BY / OFFSET / DISTINCT and expression
/// targets are rejected.
fn parseSelect(
    alloc: std.mem.Allocator,
    ss: *const c.SelectStmt,
) parser.ParseError!Stmt {
    if (ss.sortClause != null or ss.groupClause != null or
        ss.distinctClause != null or ss.havingClause != null or
        ss.limitOffset != null)
        return invalid(
            "ORDER BY / GROUP BY / OFFSET / DISTINCT are not supported",
        );

    // LIMIT: an integer literal, or ALL (a NULL constant: no limit).
    // A parameter, an expression, or WITH TIES is refused (the vendored
    // grammar rejects WITH TIES itself today; the check is for one that
    // does not, so it can never pass as a plain LIMIT).
    var limit: ?usize = null;
    if (ss.limitCount != null) {
        if (ss.limitOption == c.LIMIT_OPTION_WITH_TIES)
            return invalid("FETCH ... WITH TIES is not supported");
        const ac = parser.cast(c.A_Const, ss.limitCount) orelse
            return invalid("only an integer literal LIMIT is supported");
        if (!ac.isnull) {
            if (ac.val.node.type != c.T_Integer)
                return invalid("only an integer literal LIMIT is supported");
            const n = ac.val.ival.ival;
            if (n < 0) return invalid("LIMIT must not be negative");
            limit = @intCast(n);
        }
    }

    const from = parser.items(ss.fromClause);
    if (from.len == 0) return parseSelectRow(alloc, ss, limit);
    if (from.len != 1)
        return invalid("SELECT must read exactly one table");
    const rv = parser.cast(c.RangeVar, from[0].ptr_value) orelse
        return invalid("only table sources are supported");

    // Every target must be a column reference, except the one aggregate,
    // a lone `count(*)`; a `*` anywhere makes it a star projection.
    const targets = parser.items(ss.targetList);
    var star = false;
    var count_star = false;
    for (targets) |cell| {
        const rt = parser.cast(c.ResTarget, cell.ptr_value) orelse
            return .unsupported;
        if (parser.cast(c.FuncCall, rt.val)) |fc| {
            if (!isCountStar(fc))
                return invalid("only count(*) is supported in SELECT");
            count_star = true;
            continue;
        }
        const cr = parser.cast(c.ColumnRef, rt.val) orelse
            return invalid("only column references are supported in SELECT");
        for (parser.items(cr.fields)) |f| {
            if (parser.cast(c.A_Star, f.ptr_value) != null) star = true;
        }
    }
    if (count_star and targets.len != 1)
        return invalid("count(*) cannot be combined with other targets");
    const locking = parser.items(ss.lockingClause);
    if (count_star and locking.len > 0)
        return invalid("FOR UPDATE is not allowed with count(*)");

    var cols: std.ArrayList(String) = .empty;
    if (!star and !count_star) {
        for (targets) |cell| {
            const rt = parser.cast(c.ResTarget, cell.ptr_value).?;
            const column = try columnOf(alloc, rt.val) orelse
                return invalid("unsupported column reference");
            try cols.append(alloc, column);
        }
    }

    var params = ParamTally{};
    var where: ?Where = null;
    if (ss.whereClause != null) switch (try parseWhere(
        alloc,
        &params,
        parser.asNode(ss.whereClause),
    )) {
        .where => |x| where = x,
        .invalid => |msg| return .{ .invalid = msg },
    };

    // FOR UPDATE / FOR SHARE / NOWAIT / SKIP LOCKED: plain FOR UPDATE
    // is strength LCS_FORUPDATE with waitPolicy LockWaitBlock.
    var for_update = false;
    if (locking.len > 0) {
        if (locking.len > 1)
            return invalid("only a single FOR UPDATE clause is supported");
        const lc = parser.cast(c.LockingClause, locking[0].ptr_value) orelse
            return invalid("unsupported locking clause");
        if (lc.strength != c.LCS_FORUPDATE)
            return invalid("only FOR UPDATE is supported");
        if (lc.waitPolicy != c.LockWaitBlock)
            return invalid("NOWAIT / SKIP LOCKED are not supported");
        if (parser.items(lc.lockedRels).len > 0)
            return invalid("FOR UPDATE OF is not supported");
        for_update = true;
    }

    const projection: Projection = if (count_star)
        .count_star
    else if (star)
        .star
    else
        .{ .columns = try cols.toOwnedSlice(alloc) };
    return .{ .select = .{
        .table = try dup(alloc, rv.relname),
        .projection = projection,
        .where = where,
        .for_update = for_update,
        .limit = limit,
        .param_count = params.count,
    } };
}

/// SELECT constant [AS name], ... with no FROM: one row, nothing
/// read. VALUES has no target list and is refused; WHERE and FOR
/// UPDATE have nothing to apply to.
fn parseSelectRow(
    alloc: std.mem.Allocator,
    ss: *const c.SelectStmt,
    limit: ?usize,
) parser.ParseError!Stmt {
    if (ss.valuesLists != null) return invalid("VALUES is not supported");
    if (ss.whereClause != null or ss.lockingClause != null)
        return invalid("WHERE and FOR UPDATE need a FROM");
    const targets = parser.items(ss.targetList);
    if (targets.len == 0) return .unsupported;
    const cells = try alloc.alloc(Cell, targets.len);
    for (targets, cells) |t, *cell| {
        const rt = parser.cast(c.ResTarget, t.ptr_value) orelse
            return .unsupported;
        const ac = parser.cast(c.A_Const, rt.val) orelse
            return invalid("only constants are supported without a FROM");
        const lit = try literalFrom(alloc, ac);
        if (lit == .unsupported)
            return invalid("only constants are supported without a FROM");
        cell.* = .{
            .name = if (rt.name != null)
                try dup(alloc, rt.name)
            else
                "?column?",
            .value = lit,
        };
    }
    return .{ .select_row = .{ .cells = cells, .limit = limit } };
}

/// SET and RESET, any kind and parameter (Stmt.set_var).
fn parseSet(vs: *const c.VariableSetStmt) Stmt {
    return switch (vs.kind) {
        c.VAR_RESET, c.VAR_RESET_ALL => .reset_var,
        else => .set_var,
    };
}

/// Whether a target's function call is exactly `count(*)`: no
/// arguments, no DISTINCT, no FILTER, no OVER.
fn isCountStar(fc: *const c.FuncCall) bool {
    const n = parser.items(fc.funcname).len;
    if (n == 0) return false;
    const s = parser.cast(c.String, parser.nodeAt(fc.funcname, n - 1)) orelse
        return false;
    return std.mem.eql(u8, parser.str(s.sval), "count") and fc.agg_star and
        fc.args == null and !fc.agg_distinct and
        fc.agg_filter == null and fc.over == null;
}

/// Reject a recognized statement with a static client-facing reason —
/// borrowed straight from .rodata, so no allocation.
fn invalid(msg: String) Stmt {
    return .{ .invalid = msg };
}

/// Reject with a formatted reason, allocated into the parse arena.
fn invalidFmt(
    alloc: std.mem.Allocator,
    comptime fmt: String,
    args: anytype,
) parser.ParseError!Stmt {
    return .{ .invalid = try std.fmt.allocPrint(alloc, fmt, args) };
}

/// BEGIN / START TRANSACTION / COMMIT / ROLLBACK. Transaction modes
/// (ISOLATION LEVEL …) are ignored; savepoints and two-phase commit
/// are rejected with a reason.
fn parseTransaction(ts: *const c.TransactionStmt) Stmt {
    return switch (ts.kind) {
        c.TRANS_STMT_BEGIN, c.TRANS_STMT_START => .begin_txn,
        c.TRANS_STMT_COMMIT => .commit_txn,
        c.TRANS_STMT_ROLLBACK => .rollback_txn,
        else => invalid("unsupported transaction statement"),
    };
}

/// DELETE FROM one_table [WHERE ...]. USING, RETURNING, WITH, and any
/// WHERE shape parseWhere rejects carry a reason. `WHERE egid = N` parses
/// as an ordinary column comparison — egid resolution stays server-side.
fn parseDelete(
    alloc: std.mem.Allocator,
    ds: *const c.DeleteStmt,
) parser.ParseError!Stmt {
    if (ds.usingClause != null)
        return invalid("DELETE ... USING is not supported");
    if (ds.returningList != null)
        return invalid("RETURNING is not supported");
    if (ds.withClause != null)
        return invalid("WITH is not supported");

    var params = ParamTally{};
    var where: ?Where = null;
    if (ds.whereClause != null) switch (try parseWhere(
        alloc,
        &params,
        parser.asNode(ds.whereClause),
    )) {
        .where => |x| where = x,
        .invalid => |msg| return .{ .invalid = msg },
    };

    return .{ .delete = .{
        .table = try relname(alloc, ds.relation),
        .where = where,
        .param_count = params.count,
    } };
}

/// UPDATE one_table SET col = expr [, ...] [WHERE ...]. FROM,
/// RETURNING, WITH, SET values parseExpr does not take, and duplicate
/// SET columns are rejected with a reason.
fn parseUpdate(
    alloc: std.mem.Allocator,
    us: *const c.UpdateStmt,
) parser.ParseError!Stmt {
    if (us.fromClause != null)
        return invalid("UPDATE ... FROM is not supported");
    if (us.returningList != null)
        return invalid("RETURNING is not supported");
    if (us.withClause != null)
        return invalid("WITH is not supported");
    const targets = parser.items(us.targetList);
    if (targets.len == 0) return .unsupported;

    var params = ParamTally{};
    var where: ?Where = null;
    if (us.whereClause != null) switch (try parseWhere(
        alloc,
        &params,
        parser.asNode(us.whereClause),
    )) {
        .where => |x| where = x,
        .invalid => |msg| return .{ .invalid = msg },
    };

    var sets: std.ArrayList(SetClause) = .empty;
    for (targets) |cell| {
        const rt = parser.cast(c.ResTarget, cell.ptr_value) orelse
            return .unsupported;
        if (rt.name == null) return .unsupported;
        const column = try dup(alloc, rt.name);
        for (sets.items) |s| {
            if (std.mem.eql(u8, s.column, column))
                return invalid("duplicate column in SET");
        }
        const val = parser.asNode(rt.val);
        const value = switch (try parseExpr(alloc, &params, val)) {
            .expr => |e| e,
            .invalid => |msg| return .{ .invalid = msg },
        };
        try sets.append(alloc, .{ .column = column, .value = value });
    }

    return .{ .update = .{
        .table = try relname(alloc, us.relation),
        .sets = try sets.toOwnedSlice(alloc),
        .where = where,
        .param_count = params.count,
    } };
}
