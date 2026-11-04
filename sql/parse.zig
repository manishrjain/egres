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
const _funcs = @import("funcs.zig");
const _value = @import("value.zig");
const _numeric = @import("numeric.zig");
const Column = @import("table.zig").Column;

const String = _value.String;

/// One parsed `name type [PRIMARY KEY | UNIQUE | NOT NULL]` column.
/// `name` is borrowed. A PRIMARY KEY column is unique too, but
/// `unique` marks only a bare UNIQUE constraint: the PK gets its own
/// index. It is NOT NULL too, and `not_null` does say so.
pub const ColumnDef = struct {
    name: String,
    ty: _value.Type,
    /// `char(n)` / `varchar(n)` length; null for every other type.
    type_mod: ?u32 = null,
    pkey: bool = false,
    unique: bool = false,
    not_null: bool = false,
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

pub const RenameColumn = struct {
    table: String, // borrowed
    old_name: String, // borrowed
    new_name: String, // borrowed
    missing_ok: bool = false,
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
/// (value.textToValue). `str`/`float` borrow their bytes. `unsupported` is a
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
    on_conflict: ?OnConflict = null,
    /// RETURNING: the written rows, projected as a SELECT's are.
    returning: ?Projection = null,
    /// How many `$n` values Bind must supply: the highest index used.
    param_count: u16 = 0,
};

/// INSERT ... ON CONFLICT: the column whose unique index a row may
/// collide on, and what it does then instead of inserting.
pub const OnConflict = struct {
    /// The conflict target's column; null when none was named, which
    /// DO NOTHING takes as any unique constraint.
    column: ?String, // borrowed
    action: Action,

    pub const Action = union(enum) {
        nothing,
        update: DoUpdate,
    };
    /// DO UPDATE SET ... [WHERE ...]: the SET values may name the
    /// existing row's columns and `EXCLUDED.column`, the proposed
    /// row's; the WHERE, on the existing row, decides update or skip.
    pub const DoUpdate = struct {
        sets: []SetClause, // borrowed
        where: ?Where,
    };
};

/// What a SELECT projects: `*` (all live columns), a named list (which
/// may include the `egid` pseudo-column), or `count(*)` — one int8 cell,
/// the number of rows the plan yields. Names are borrowed.
pub const Projection = union(enum) {
    star,
    targets: []Target,
    count_star,
};

/// One SELECT target: an expression over the row and its output name,
/// the alias or what Postgres prints without one: a column's name, a
/// function's, else `?column?`.
pub const Target = struct {
    name: String, // borrowed
    expr: Expr,
};

/// The comparison a WHERE leaf applies. `=` is the one an index
/// answers; the rest are checked row by row.
pub const CmpOp = enum {
    eq,
    ne,
    lt,
    le,
    gt,
    ge,
    /// `a IS DISTINCT FROM b` / `a IS NOT DISTINCT FROM b`: `<>` / `=`
    /// with two NULLs the same and a NULL and a value different, so
    /// never unknown (pred.eval).
    differs,
    same,

    pub fn fromName(name: String) ?CmpOp {
        const table = .{
            .{ "=", .eq },  .{ "<>", .ne }, .{ "!=", .ne }, .{ "<", .lt },
            .{ "<=", .le }, .{ ">", .gt },  .{ ">=", .ge },
        };
        inline for (table) |entry| {
            if (std.mem.eql(u8, name, entry[0])) return entry[1];
        }
        return null;
    }

    /// The same comparison with its operands swapped: `5 < a` is
    /// `a > 5`.
    pub fn flipped(self: CmpOp) CmpOp {
        return switch (self) {
            .eq => .eq,
            .ne => .ne,
            .lt => .gt,
            .le => .ge,
            .gt => .lt,
            .ge => .le,
            .differs => .differs,
            .same => .same,
        };
    }

    /// Whether `order`, of the column's cell against the value, is
    /// what the comparison asks for.
    pub fn holds(self: CmpOp, order: std.math.Order) bool {
        return switch (self) {
            .eq, .same => order == .eq,
            .ne, .differs => order != .eq,
            .lt => order == .lt,
            .le => order != .gt,
            .gt => order == .gt,
            .ge => order != .lt,
        };
    }
};

/// A single `column op literal` comparison — the leaf of a WHERE. The
/// column is always on the left: a literal written there turns the
/// operator around.
/// `lhs op rhs`. When one side alone is a bare column it is the left
/// one: the parser turns the operator to put it there, the shape the
/// planner answers from an index.
pub const Cmp = struct {
    lhs: *const Expr,
    op: CmpOp = .eq,
    rhs: *const Expr,
};

/// A parsed WHERE tree. A leaf compares two expressions or tests one
/// for NULL or a truth value; AND/OR hold their children inline (a
/// slice is already an indirection, and children are never shared
/// between parents); NOT is a pointer only because a union cannot
/// contain itself by value. `IN (...)` and `BETWEEN` are not leaves:
/// the parser spells them as the OR / AND of comparisons they mean.
/// Everything borrows the arena.
pub const Where = union(enum) {
    cmp: Cmp,
    is_null: NullTest,
    bool_test: BoolTest,
    and_: []const Where,
    or_: []const Where,
    not: *const Where,

    /// `expr IS NULL` (negate = false) / `expr IS NOT NULL`.
    pub const NullTest = struct {
        expr: *const Expr,
        negate: bool,
    };
    /// `expr IS [NOT] TRUE | FALSE | UNKNOWN`, over a boolean.
    pub const BoolTest = struct {
        expr: *const Expr,
        is: BoolIs,
        negate: bool,
    };
    pub const BoolIs = enum { true_, false_, unknown };
};

pub const Select = struct {
    table: String, // borrowed
    projection: Projection,
    where: ?Where,
    /// FOR UPDATE: lock every matched row exclusively (held to txn end).
    for_update: bool = false,
    /// LIMIT n: stop after n rows. null = every row (LIMIT ALL too).
    limit: ?usize = null,
    /// OFFSET n: the first n rows, after the ordering, are dropped.
    offset: usize = 0,
    /// ORDER BY, first key first; empty = the source's order.
    order_by: []const OrderBy = &.{},
    /// How many `$n` values Bind must supply: the highest index used.
    param_count: u16 = 0,
};

/// One ORDER BY key, `expr [ASC | DESC] [NULLS FIRST | LAST]`, with
/// Postgres's defaults: ASC puts NULLs last, DESC first. `ORDER BY 2`
/// and `ORDER BY alias` are the target's expression (parseOrderBy).
pub const OrderBy = struct {
    expr: Expr,
    desc: bool,
    nulls_first: bool,
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
    /// RETURNING: the deleted rows as they were, projected.
    returning: ?Projection = null,
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
    /// `EXCLUDED.column` in ON CONFLICT DO UPDATE: the row the INSERT
    /// proposed (borrowed name).
    excluded: String,
    binop: BinOp,
    call: Call,

    pub const Op = enum { add, sub, mul, div, mod, concat };
    pub const BinOp = struct { op: Op, lhs: *const Expr, rhs: *const Expr };
    pub const Call = struct { func: *const _funcs.Func, args: []const Expr };
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
    /// RETURNING: the rows as rewritten, projected.
    returning: ?Projection = null,
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

/// SHOW `name`: the parameter as the client spelled it (lowercased by
/// the grammar unless quoted); `SHOW TRANSACTION ISOLATION LEVEL` and
/// the other keyword forms arrive under their parameter names.
pub const Show = struct {
    name: String, // borrowed
};

pub const Stmt = union(enum) {
    create_database: CreateDatabase,
    drop_database: String, // database name (borrowed)
    create_table: CreateTable,
    create_table_as: CreateTableAs,
    drop_table: DropTable,
    rename_column: RenameColumn,
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
    /// SAVEPOINT / RELEASE [SAVEPOINT] / ROLLBACK TO [SAVEPOINT], each
    /// with its name (borrowed).
    savepoint: String,
    release_savepoint: String,
    rollback_to: String,
    /// SET and RESET of any parameter, accepted and ignored: drivers
    /// send them on connect, and no setting changes anything here.
    /// SET TRANSACTION is ignored the way BEGIN's modes are.
    set_var,
    reset_var,
    /// DEALLOCATE name, or DEALLOCATE ALL as null: the prepared
    /// statement(s) the session drops, as a Close message would.
    deallocate: ?String,
    /// SHOW of one parameter: the server answers from its fixed
    /// settings (server.showVar).
    show_var: Show,
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
    if (parser.cast(c.TransactionStmt, node)) |s|
        return parseTransaction(alloc, s);
    if (parser.cast(c.VariableSetStmt, node)) |s| return parseSet(s);
    if (parser.cast(c.VariableShowStmt, node)) |s|
        return .{ .show_var = .{ .name = try dup(alloc, s.name) } };
    if (parser.cast(c.DeallocateStmt, node)) |s| return .{
        .deallocate = if (s.isall) null else try dup(alloc, s.name),
    };
    if (parser.cast(c.RenameStmt, node)) |s|
        return parseRenameColumn(alloc, s);
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

fn parseRenameColumn(
    alloc: std.mem.Allocator,
    s: *const c.RenameStmt,
) parser.ParseError!Stmt {
    if (s.renameType != c.OBJECT_COLUMN or
        s.relationType != c.OBJECT_TABLE)
        return .unsupported;
    return .{ .rename_column = .{
        .table = try relname(alloc, s.relation),
        .old_name = try dup(alloc, s.subname),
        .new_name = try dup(alloc, s.newname),
        .missing_ok = s.missing_ok,
    } };
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

/// The constant `p` is, seeing through one cast of a constant to a
/// type (`'6'::int8`, `CAST('x' AS text)`): the form a driver inlines a
/// bound value in when it cannot send it as a parameter (pgjdbc in
/// simple query mode). The named type is not used: the column the
/// constant meets types it, as it does a bare literal.
fn constant(p: ?*anyopaque) ?*c.A_Const {
    if (parser.cast(c.A_Const, p)) |ac| return ac;
    const tc = parser.cast(c.TypeCast, p) orelse return null;
    return parser.cast(c.A_Const, tc.arg);
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
        // `char(1)`; a lengthless `varchar` stays `.text`. `numeric(p,
        // s)` packs both into the one modifier (value.Type.numeric).
        const typmods = parser.items(tn.typmods);
        var type_mod: ?u32 = null;
        if (typmods.len > 0 and ty == .numeric) {
            type_mod = switch (try numericTypmod(alloc, typmods)) {
                .mod => |m| m,
                .stmt => |rejected| return rejected,
            };
        } else if (typmods.len > 0) {
            const is_varchar = std.mem.eql(u8, tyname, "varchar");
            if (ty != .bpchar and !is_varchar)
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
            if (n > _value.Type.max_type_mod)
                return invalidFmt(alloc, "length for type {s} cannot exceed {d}", //
                    .{ tyname, _value.Type.max_type_mod });
            type_mod = @intCast(n);
            if (is_varchar) ty = .varchar;
        } else if (ty == .bpchar) {
            type_mod = 1;
        }

        var pkey = false;
        var unique = false;
        var not_null = false;
        for (parser.items(cd.constraints)) |ccell| {
            const con = parser.cast(c.Constraint, ccell.ptr_value) orelse
                return .unsupported;
            switch (con.contype) {
                c.CONSTR_PRIMARY => pkey = true,
                c.CONSTR_UNIQUE => unique = true,
                c.CONSTR_NOTNULL => not_null = true,
                // An explicit NULL is the default, spelled out.
                c.CONSTR_NULL => {},
                else => return invalid(
                    "only PRIMARY KEY, UNIQUE and NOT NULL column " ++
                        "constraints are supported",
                ),
            }
        }
        if (pkey) {
            if (pkey_seen)
                return invalid("multiple PRIMARY KEY columns are not supported");
            pkey_seen = true;
            unique = false; // the PK index already enforces it
            not_null = true;
        }

        try columns.append(alloc, .{
            .name = colname,
            .ty = ty,
            .type_mod = type_mod,
            .pkey = pkey,
            .unique = unique,
            .not_null = not_null,
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

/// INSERT ... VALUES [ON CONFLICT ...] [RETURNING ...]. INSERT…SELECT
/// is rejected; arity, unknown columns, and literal coercion are the
/// executor's job.
fn parseInsert(
    alloc: std.mem.Allocator,
    is: *const c.InsertStmt,
) parser.ParseError!Stmt {
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
    var on_conflict: ?OnConflict = null;
    if (is.onConflictClause != null) {
        on_conflict = switch (try parseOnConflict(
            alloc,
            &params,
            is.onConflictClause,
        )) {
            .clause => |x| x,
            .stmt => |rejected| return rejected,
        };
    }
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
    const ret = try parseReturning(alloc, &params, is.returningList);
    const returning = switch (ret) {
        .projection => |x| x,
        .stmt => |rejected| return rejected,
    };

    return .{ .insert = .{
        .table = try relname(alloc, is.relation),
        .columns = columns,
        .rows = rows,
        .on_conflict = on_conflict,
        .returning = returning,
        .param_count = params.count,
    } };
}

const ParsedOnConflict = union(enum) {
    clause: OnConflict,
    stmt: Stmt, // the rejection to return
};

/// ON CONFLICT [(column)] DO NOTHING | DO UPDATE SET ... [WHERE ...].
/// A constraint by name, a partial-index WHERE on the target, more
/// than one column or an expression carry a reason.
fn parseOnConflict(
    alloc: std.mem.Allocator,
    params: *ParamTally,
    oc: *const c.OnConflictClause,
) parser.ParseError!ParsedOnConflict {
    var column: ?String = null;
    if (oc.infer != null) {
        const infer: *const c.InferClause = oc.infer;
        if (infer.conname != null)
            return .{ .stmt = invalid("ON CONFLICT ON CONSTRAINT is not supported") };
        if (infer.whereClause != null)
            return .{ .stmt = invalid("a WHERE on the ON CONFLICT target is not supported") };
        const elems = parser.items(infer.indexElems);
        if (elems.len != 1)
            return .{ .stmt = invalid("ON CONFLICT takes one column") };
        const elem = parser.cast(c.IndexElem, elems[0].ptr_value) orelse
            return .{ .stmt = .unsupported };
        if (elem.name == null)
            return .{ .stmt = invalid("ON CONFLICT takes a column, not an expression") };
        column = try dup(alloc, elem.name);
    }
    switch (oc.action) {
        c.ONCONFLICT_NOTHING => return .{ .clause = .{
            .column = column,
            .action = .nothing,
        } },
        c.ONCONFLICT_UPDATE => {},
        else => return .{ .stmt = invalid("unsupported ON CONFLICT action") },
    }
    if (column == null)
        return .{ .stmt = invalid("ON CONFLICT DO UPDATE requires a conflict target") };
    const sets = switch (try parseSets(alloc, params, oc.targetList)) {
        .sets => |s| s,
        .stmt => |rejected| return .{ .stmt = rejected },
    };
    var where: ?Where = null;
    if (oc.whereClause != null) switch (try parseWhere(
        alloc,
        params,
        parser.asNode(oc.whereClause),
    )) {
        .where => |x| where = x,
        .invalid => |msg| return .{ .stmt = .{ .invalid = msg } },
    };
    return .{ .clause = .{
        .column = column,
        .action = .{ .update = .{ .sets = sets, .where = where } },
    } };
}

/// A VALUES item as a Literal: a constant, or a value function (a
/// `$n` is the caller's: it needs the statement's tally). Anything
/// else (DEFAULT, an expression) is `.unsupported`.
fn valueLiteral(
    alloc: std.mem.Allocator,
    p: ?*anyopaque,
) parser.ParseError!Literal {
    if (constant(p)) |ac| return literalFrom(alloc, ac);
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

/// `$index` of `params`, the portal's Bind values (pgwire.Params), as a
/// cell of `col`. A text value takes the path a quoted literal takes —
/// column-typed parsing, the string types' length rules — whatever
/// type the client declared, as Postgres reads an unknown-typed
/// literal. A binary value is decoded by its declared type
/// (`binaryValue`) and then assigned to the column's
/// (value.valueToColumn): a driver sends an int8 for a bigint column
/// and an int4 for an int one, and a column of another width or kind
/// takes it the way SET assigns a number. An index past what Bind
/// supplied is `error.UnboundParam` (the runner checks the count
/// first, so only a bug reaches it).
pub fn paramToValue(
    params: pgwire.Params,
    alloc: std.mem.Allocator,
    index: u16,
    col: *const Column,
) !_value.Value {
    if (index == 0 or index > params.values.len) return error.UnboundParam;

    const bytes = params.values[index - 1] orelse return .null_value;
    if (isBinary(params, index)) {
        const typed = try binaryValue(params, index, bytes);
        return _value.valueToColumn(alloc, typed, col);
    }
    return _value.textToColumn(alloc, col, bytes);
}

/// `$index` with no column to type it (a placeholder under a SET
/// operator): a binary value as its declared type, a text value as
/// `.text` for the caller to read (expr.literalValue reads a
/// number when it is one), null for SQL NULL.
pub fn untypedParam(params: pgwire.Params, index: u16) !_value.Value {
    if (index == 0 or index > params.values.len) return error.UnboundParam;
    const bytes = params.values[index - 1] orelse return .null_value;
    if (isBinary(params, index)) return binaryValue(params, index, bytes);
    return .{ .text = bytes };
}

/// Whether Bind supplied `$index`: false before Bind, when a
/// statement's Describe binds `SELECT n + $1 FROM t` by type alone.
pub fn paramBound(params: pgwire.Params, index: u16) bool {
    return index >= 1 and index <= params.values.len;
}

/// The type Parse declared for `$index`, null when it declared none
/// or one with no cell here (value.typeFromOid).
pub fn paramDeclared(params: pgwire.Params, index: u16) ?_value.Type {
    if (index == 0 or index > params.oids.len) return null;
    return _value.typeFromOid(params.oids[index - 1]);
}

fn isBinary(params: pgwire.Params, index: u16) bool {
    return index <= params.wirefmts.len and
        params.wirefmts[index - 1] == .binary;
}

/// A binary-format value as a cell of the type the client declared
/// for `$index`: the primitives pgwire.binary decodes, bytea and
/// uuid (raw bytes), and the string types (their binary format is
/// the text itself). A declared type with no decoder here, or none
/// declared, is `error.UnsupportedType`: the bytes cannot be read.
fn binaryValue(params: pgwire.Params, index: u16, bytes: String) !_value.Value {
    const binary = pgwire.binary;
    const pg_epoch_us = pgwire.types.pg_epoch_unix_us;
    const oid: u32 = if (index <= params.oids.len)
        params.oids[index - 1]
    else
        0;
    return switch (oid) {
        pgwire.oid.bool_ => .{ .bool_ = try binary.decodeBool(bytes) },
        pgwire.oid.int2 => .{ .int2 = try binary.decodeInt2(bytes) },
        pgwire.oid.int4 => .{ .int4 = try binary.decodeInt4(bytes) },
        pgwire.oid.int8 => .{ .int8 = try binary.decodeInt8(bytes) },
        pgwire.oid.float4 => .{ .float4 = try binary.decodeFloat4(bytes) },
        pgwire.oid.float8 => .{ .float8 = try binary.decodeFloat8(bytes) },
        // The binary time types count from the 2000 epoch; storage
        // from 1970 (value.zig).
        pgwire.oid.date => .{
            .date = try binary.decodeInt4(bytes) +
                pgwire.types.pg_epoch_unix_days,
        },
        pgwire.oid.timestamp => .{
            .timestamp = try binary.decodeInt8(bytes) + pg_epoch_us,
        },
        pgwire.oid.timestamptz => .{
            .timestamptz = try binary.decodeInt8(bytes) + pg_epoch_us,
        },
        pgwire.oid.bytea => .{ .bytea = bytes },
        pgwire.oid.numeric => .{
            .numeric = try _value.Numeric.decodeBinary(bytes),
        },
        pgwire.oid.uuid => if (bytes.len == 16)
            .{ .uuid = bytes[0..16].* }
        else
            error.BadLength,
        pgwire.oid.text, pgwire.oid.varchar, pgwire.oid.bpchar => .{
            .text = bytes,
        },
        else => error.UnsupportedType,
    };
}

/// Coerce a parsed literal to a cell of `col` (value.textToColumn) by
/// rendering non-string literals to text first (int/bool stringified
/// into `a`, so a text-typed column can borrow it). NULL passes
/// through; a non-literal item is `error.UnsupportedLiteral`; a `$n`
/// is the bound value (Params.toValue).
pub fn litToValue(
    a: std.mem.Allocator,
    lit: Literal,
    col: *const Column,
    params: pgwire.Params,
) !_value.Value {
    const text: String = switch (lit) {
        .null_value => return .null_value,
        .unsupported => return error.UnsupportedLiteral,
        // Resolved by the executor before coercion (dml.coerceLiteral);
        // a position without a transaction time (SET, WHERE) never
        // produces it.
        .current_timestamp => return error.UnsupportedLiteral,
        .param => |n| return paramToValue(params, a, n, col),
        .str, .float => |s| s,
        .int => |n| try std.fmt.allocPrint(a, "{d}", .{n}),
        .bool_ => |b| if (b) "t" else "f",
    };
    return _value.textToColumn(a, col, text);
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

/// Whether `p` is `EXCLUDED.column`: a two-part column reference
/// whose qualifier is that keyword (lowercased by the grammar). Any
/// other qualifier, the table or its alias, is dropped by columnOf.
fn isExcludedRef(p: anytype) bool {
    const cr = parser.cast(c.ColumnRef, p) orelse return false;
    if (parser.items(cr.fields).len != 2) return false;
    const s = parser.cast(c.String, parser.nodeAt(cr.fields, 0)) orelse
        return false;
    return std.mem.eql(u8, parser.str(s.sval), "excluded");
}

/// The operator an A_Expr names, the last part of it: `=` for an
/// `AEXPR_OP`, and for `IN` too (`<>` for NOT IN).
fn operatorOf(ae: *const c.A_Expr) ?String {
    const n = parser.items(ae.name).len;
    if (n == 0) return null;
    const s = parser.cast(c.String, parser.nodeAt(ae.name, n - 1)) orelse
        return null;
    return parser.str(s.sval);
}

/// Parse a WHERE tree, shared by SELECT, DELETE, and UPDATE: `expr op
/// expr` leaves (`= <> < <= > >=`, `IS [NOT] DISTINCT FROM`, each
/// side a parseExpr expression), `expr IS [NOT] NULL`, `expr IS [NOT]
/// TRUE | FALSE | UNKNOWN`, a bare boolean column, and AND / OR / NOT
/// over them. `IN (...)` and `BETWEEN` become those: `id IN (1, 2)`
/// is `id = 1 OR id = 2` and `n BETWEEN 2 AND 5` is `n >= 2 AND n <=
/// 5`, as Postgres rewrites BETWEEN itself. Anything else (another
/// operator, a subquery, LIKE) yields a clean `.invalid` reason; only
/// OOM propagates. AND/OR children land in one arena slice, NOT's in
/// one arena cell. Postgres flattens `a AND b AND c` into one node
/// with three args; NOT always carries exactly one.
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
        const negate = switch (nt.nulltesttype) {
            c.IS_NULL => false,
            c.IS_NOT_NULL => true,
            else => return ParsedWhere.reject("unsupported WHERE clause"),
        };
        const operand = switch (try leafExpr(alloc, params, parser.asNode(nt.arg))) {
            .expr => |e| e,
            .invalid => |msg| return .{ .invalid = msg },
        };
        return .{ .where = .{ .is_null = .{ .expr = operand, .negate = negate } } };
    }

    if (parser.cast(c.BooleanTest, p)) |bt| {
        const is: Where.BoolIs, const negate = switch (bt.booltesttype) {
            c.IS_TRUE => .{ .true_, false },
            c.IS_NOT_TRUE => .{ .true_, true },
            c.IS_FALSE => .{ .false_, false },
            c.IS_NOT_FALSE => .{ .false_, true },
            c.IS_UNKNOWN => .{ .unknown, false },
            c.IS_NOT_UNKNOWN => .{ .unknown, true },
            else => return ParsedWhere.reject("unsupported WHERE clause"),
        };
        const operand = switch (try leafExpr(alloc, params, parser.asNode(bt.arg))) {
            .expr => |e| e,
            .invalid => |msg| return .{ .invalid = msg },
        };
        return .{ .where = .{ .bool_test = .{
            .expr = operand,
            .is = is,
            .negate = negate,
        } } };
    }

    // `WHERE active`: the column against true, unknown when NULL.
    if (parser.cast(c.ColumnRef, p) != null) {
        const col = switch (try leafExpr(alloc, params, p)) {
            .expr => |e| e,
            .invalid => |msg| return .{ .invalid = msg },
        };
        const yes = try alloc.create(Expr);
        yes.* = .{ .literal = .{ .bool_ = true } };
        return .{ .where = cmpOf(col, .eq, yes) };
    }

    const ae = parser.cast(c.A_Expr, p) orelse
        return ParsedWhere.reject("unsupported WHERE clause");
    if (isExcludedRef(ae.lexpr) or isExcludedRef(ae.rexpr))
        return ParsedWhere.reject("EXCLUDED is not supported in a WHERE");
    const lhs = switch (try leafExpr(alloc, params, parser.asNode(ae.lexpr))) {
        .expr => |e| e,
        .invalid => |msg| return .{ .invalid = msg },
    };
    switch (ae.kind) {
        c.AEXPR_OP, c.AEXPR_DISTINCT, c.AEXPR_NOT_DISTINCT => {
            const op: CmpOp = switch (ae.kind) {
                c.AEXPR_DISTINCT => .differs,
                c.AEXPR_NOT_DISTINCT => .same,
                else => CmpOp.fromName(operatorOf(ae) orelse "") orelse
                    return ParsedWhere.reject(
                        "only = <> < <= > >= comparisons are supported in WHERE",
                    ),
            };
            const rhs = switch (try leafExpr(alloc, params, parser.asNode(ae.rexpr))) {
                .expr => |e| e,
                .invalid => |msg| return .{ .invalid = msg },
            };
            return .{ .where = cmpOf(lhs, op, rhs) };
        },
        // `id IN (1, 2)` is `id = 1 OR id = 2`; NOT IN negates the OR.
        c.AEXPR_IN => {
            const list = parser.cast(c.List, ae.rexpr) orelse
                return ParsedWhere.reject("IN takes a list of values");
            const vals = parser.items(list);
            if (vals.len == 0) return ParsedWhere.reject("IN takes a list of values");
            const kids = try alloc.alloc(Where, vals.len);
            for (vals, kids) |val, *kid| {
                const rhs = switch (try leafExpr(alloc, params, parser.asNode(val.ptr_value))) {
                    .expr => |e| e,
                    .invalid => |msg| return .{ .invalid = msg },
                };
                kid.* = cmpOf(lhs, .eq, rhs);
            }
            const any = Where{ .or_ = kids };
            if (std.mem.eql(u8, operatorOf(ae) orelse "", "=")) return .{ .where = any };
            const not = try alloc.create(Where);
            not.* = any;
            return .{ .where = .{ .not = not } };
        },
        // `n BETWEEN 2 AND 5` is `n >= 2 AND n <= 5`; NOT BETWEEN is
        // `n < 2 OR n > 5`, as Postgres rewrites both.
        c.AEXPR_BETWEEN, c.AEXPR_NOT_BETWEEN => {
            const list = parser.cast(c.List, ae.rexpr) orelse
                return ParsedWhere.reject("BETWEEN takes two bounds");
            const bounds = parser.items(list);
            if (bounds.len != 2) return ParsedWhere.reject("BETWEEN takes two bounds");
            const lo = switch (try leafExpr(alloc, params, parser.asNode(bounds[0].ptr_value))) {
                .expr => |e| e,
                .invalid => |msg| return .{ .invalid = msg },
            };
            const hi = switch (try leafExpr(alloc, params, parser.asNode(bounds[1].ptr_value))) {
                .expr => |e| e,
                .invalid => |msg| return .{ .invalid = msg },
            };
            const kids = try alloc.alloc(Where, 2);
            if (ae.kind == c.AEXPR_BETWEEN) {
                kids[0] = cmpOf(lhs, .ge, lo);
                kids[1] = cmpOf(lhs, .le, hi);
                return .{ .where = .{ .and_ = kids } };
            }
            kids[0] = cmpOf(lhs, .lt, lo);
            kids[1] = cmpOf(lhs, .gt, hi);
            return .{ .where = .{ .or_ = kids } };
        },
        else => return ParsedWhere.reject("unsupported WHERE clause"),
    }
}

/// `lhs op rhs` with a bare column on the left: a literal written
/// there turns the operator around, `5 < a` is `a > 5`.
fn cmpOf(lhs: *const Expr, op: CmpOp, rhs: *const Expr) Where {
    if (lhs.* != .column and rhs.* == .column)
        return .{ .cmp = .{ .lhs = rhs, .op = op.flipped(), .rhs = lhs } };
    return .{ .cmp = .{ .lhs = lhs, .op = op, .rhs = rhs } };
}

/// One side of a WHERE leaf, parsed into its own arena cell.
const ParsedLeaf = union(enum) { expr: *const Expr, invalid: String };

fn leafExpr(
    alloc: std.mem.Allocator,
    params: *ParamTally,
    p: ?*c.Node,
) parser.ParseError!ParsedLeaf {
    const out = try alloc.create(Expr);
    out.* = switch (try parseExpr(alloc, params, p)) {
        .expr => |e| e,
        .invalid => |msg| return .{ .invalid = msg },
    };
    return .{ .expr = out };
}

/// A parsed expression or a rejection reason (borrowed).
const ParsedExpr = union(enum) {
    expr: Expr,
    invalid: String,

    fn reject(msg: String) ParsedExpr {
        return .{ .invalid = msg };
    }
};

/// The operator an A_Expr names, or null for one an expression does
/// not do.
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

/// Parse an expression, a SET value, a WHERE operand or a SELECT
/// target: a literal, a `$n`, a column, a call of a funcs.zig function
/// over expressions, or `+ - * / % ||` over them, recursively.
/// Anything else (another function, a cast, a subquery, DEFAULT,
/// another operator) yields a clean `.invalid` reason; only OOM
/// propagates. Each child lands in one arena cell.
fn parseExpr(
    arloc: std.mem.Allocator,
    params: *ParamTally,
    p: ?*c.Node,
) parser.ParseError!ParsedExpr {
    if (constant(p)) |ac| {
        const lit = try literalFrom(arloc, ac);
        if (lit == .unsupported)
            return ParsedExpr.reject("unsupported literal");
        return .{ .expr = .{ .literal = lit } };
    }
    if (parser.cast(c.ParamRef, p)) |pr| {
        const lit = params.note(pr.number) orelse
            return .{ .invalid = try badParamMsg(arloc, pr.number) };
        return .{ .expr = .{ .literal = lit } };
    }
    if (parser.cast(c.ColumnRef, p)) |cr| {
        if (isExcludedRef(p)) {
            const name = try lastName(arloc, cr.fields) orelse
                return ParsedExpr.reject("unsupported column reference");
            return .{ .expr = .{ .excluded = name } };
        }
        const column = try columnOf(arloc, p) orelse
            return ParsedExpr.reject("unsupported column reference");
        return .{ .expr = .{ .column = column } };
    }
    if (parser.cast(c.FuncCall, p)) |fc| {
        const name = funcName(fc) orelse "";
        const func = _funcs.byName(name) orelse return .{
            .invalid = try std.fmt.allocPrint(
                arloc,
                "function {s}() is not supported",
                .{name},
            ),
        };
        if (fc.agg_star or fc.agg_distinct or fc.agg_filter != null or
            fc.over != null or fc.agg_order != null)
        {
            return ParsedExpr.reject(
                "aggregate forms are not supported",
            );
        }
        return parseCall(arloc, params, func, fc.args);
    }
    if (parser.cast(c.CoalesceExpr, p)) |ce|
        return parseCall(arloc, params, &_funcs.coalesce, ce.args);
    if (parser.cast(c.MinMaxExpr, p)) |mm| {
        const func = if (mm.op == c.IS_GREATEST)
            &_funcs.greatest
        else
            &_funcs.least;
        return parseCall(arloc, params, func, mm.args);
    }
    const ae = parser.cast(c.A_Expr, p) orelse return ParsedExpr.reject(
        "an expression must be a literal, a column, a function call, " ++
            "or an operator expression over them",
    );
    if (ae.kind == c.AEXPR_NULLIF) {
        return parseArgs(arloc, params, &_funcs.nullif, &.{
            parser.asNode(ae.lexpr), parser.asNode(ae.rexpr),
        });
    }
    const bad_op = "only the + - * / % || operators are supported";
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

/// A call of `func` on the grammar's argument list.
fn parseCall(
    arloc: std.mem.Allocator,
    params: *ParamTally,
    func: *const _funcs.Func,
    list: [*c]c.List,
) parser.ParseError!ParsedExpr {
    const cells = parser.items(list);
    const nodes = try arloc.alloc(?*c.Node, cells.len);
    for (cells, nodes) |cell, *node| node.* = parser.asNode(cell.ptr_value);
    return parseArgs(arloc, params, func, nodes);
}

/// A call of `func` on `nodes`, each an expression, as many as the
/// function takes.
fn parseArgs(
    arloc: std.mem.Allocator,
    params: *ParamTally,
    func: *const _funcs.Func,
    nodes: []const ?*c.Node,
) parser.ParseError!ParsedExpr {
    if (!func.takes(nodes.len)) {
        return .{ .invalid = try arityMsg(arloc, func) };
    }
    const args = try arloc.alloc(Expr, nodes.len);
    for (nodes, args) |node, *arg| {
        arg.* = switch (try parseExpr(arloc, params, node)) {
            .expr => |e| e,
            .invalid => |msg| return .{ .invalid = msg },
        };
    }
    return .{ .expr = .{ .call = .{ .func = func, .args = args } } };
}

/// "function f() takes 2 arguments", "at least 1 argument",
/// "1 to 2 arguments".
fn arityMsg(arloc: std.mem.Allocator, func: *const _funcs.Func) ![]u8 {
    const min = func.min_args;
    const plural: String = if (min == 1) "" else "s";
    const max = func.max_args orelse return std.fmt.allocPrint(
        arloc,
        "function {s}() takes at least {d} argument{s}",
        .{ func.name, min, plural },
    );
    if (max == min) return std.fmt.allocPrint(
        arloc,
        "function {s}() takes {d} argument{s}",
        .{ func.name, min, plural },
    );
    return std.fmt.allocPrint(
        arloc,
        "function {s}() takes {d} to {d} arguments",
        .{ func.name, min, max },
    );
}

/// The unqualified name of a function call, lowercased by the grammar
/// unless it was quoted.
fn funcName(fc: *const c.FuncCall) ?String {
    const n = parser.items(fc.funcname).len;
    if (n == 0) return null;
    const s = parser.cast(c.String, parser.nodeAt(fc.funcname, n - 1)) orelse
        return null;
    return parser.str(s.sval);
}

/// SELECT [exprs|*|count(*)] FROM one_table [WHERE ...] [FOR UPDATE]
/// [ORDER BY ...] [LIMIT n] [OFFSET n], or constants with no FROM
/// (parseSelectRow). Joins, subqueries and GROUP BY / DISTINCT /
/// HAVING are rejected.
fn parseSelect(
    alloc: std.mem.Allocator,
    ss: *const c.SelectStmt,
) parser.ParseError!Stmt {
    if (ss.groupClause != null or ss.distinctClause != null or
        ss.havingClause != null)
        return invalid("GROUP BY / DISTINCT / HAVING are not supported");

    // LIMIT and OFFSET: an integer literal, or LIMIT ALL (a NULL
    // constant: no limit). A parameter, an expression, or WITH TIES is
    // refused (the vendored grammar rejects WITH TIES itself today;
    // the check is for one that does not, so it can never pass as a
    // plain LIMIT).
    if (ss.limitOption == c.LIMIT_OPTION_WITH_TIES)
        return invalid("FETCH ... WITH TIES is not supported");
    var limit: ?usize = null;
    if (ss.limitCount != null) {
        const ac = parser.cast(c.A_Const, ss.limitCount) orelse
            return invalid("only an integer literal LIMIT is supported");
        if (!ac.isnull) {
            const n = intConst(ac) orelse
                return invalid("only an integer literal LIMIT is supported");
            if (n < 0) return invalid("LIMIT must not be negative");
            limit = @intCast(n);
        }
    }
    var offset: usize = 0;
    if (ss.limitOffset != null) {
        const ac = parser.cast(c.A_Const, ss.limitOffset) orelse
            return invalid("only an integer literal OFFSET is supported");
        if (!ac.isnull) {
            const n = intConst(ac) orelse
                return invalid("only an integer literal OFFSET is supported");
            if (n < 0) return invalid("OFFSET must not be negative");
            offset = @intCast(n);
        }
    }

    const from = parser.items(ss.fromClause);
    if (from.len == 0) {
        if (ss.sortClause != null or ss.limitOffset != null)
            return invalid("ORDER BY and OFFSET need a FROM");
        return parseSelectRow(alloc, ss, limit);
    }
    if (from.len != 1)
        return invalid("SELECT must read exactly one table");
    const rv = parser.cast(c.RangeVar, from[0].ptr_value) orelse
        return invalid("only table sources are supported");

    var params = ParamTally{};
    const parsed = try parseTargets(alloc, &params, ss.targetList, .select);
    const projection = switch (parsed) {
        .projection => |x| x,
        .stmt => |rejected| return rejected,
    };
    const count_star = projection == .count_star;
    const locking = parser.items(ss.lockingClause);
    if (count_star and locking.len > 0)
        return invalid("FOR UPDATE is not allowed with count(*)");

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

    var order_by: []const OrderBy = &.{};
    if (ss.sortClause != null) {
        if (count_star)
            return invalid("ORDER BY is not allowed with count(*)");
        order_by = switch (try parseOrderBy(
            alloc,
            &params,
            ss.sortClause,
            projection,
        )) {
            .keys => |keys| keys,
            .invalid => |msg| return .{ .invalid = msg },
        };
    }

    return .{ .select = .{
        .table = try dup(alloc, rv.relname),
        .projection = projection,
        .where = where,
        .for_update = for_update,
        .limit = limit,
        .offset = offset,
        .order_by = order_by,
        .param_count = params.count,
    } };
}

/// An integer constant's value; null for a constant of another kind.
fn intConst(ac: *const c.A_Const) ?i64 {
    if (ac.val.node.type != c.T_Integer) return null;
    return ac.val.ival.ival;
}

const ParsedOrderBy = union(enum) { keys: []const OrderBy, invalid: String };

/// `SELECT n, name AS who FROM t ORDER BY who DESC, 1, n + 1`: each key
/// is an expression over the row, bound as a target is. An ordinal is
/// the target in that position and a bare name that is a target's
/// output name is that target's expression, the names Postgres
/// resolves first; `*` has no targets to point at. USING is refused.
fn parseOrderBy(
    alloc: std.mem.Allocator,
    params: *ParamTally,
    list: [*c]const c.List,
    projection: Projection,
) parser.ParseError!ParsedOrderBy {
    const items = parser.items(list);
    const keys = try alloc.alloc(OrderBy, items.len);
    for (items, keys) |cell, *key| {
        const sb = parser.cast(c.SortBy, cell.ptr_value) orelse
            return .{ .invalid = "unsupported ORDER BY" };
        const desc = switch (sb.sortby_dir) {
            c.SORTBY_DEFAULT, c.SORTBY_ASC => false,
            c.SORTBY_DESC => true,
            else => return .{ .invalid = "ORDER BY USING is not supported" },
        };
        const nulls_first = switch (sb.sortby_nulls) {
            c.SORTBY_NULLS_FIRST => true,
            c.SORTBY_NULLS_LAST => false,
            else => desc,
        };
        const targets: []const Target = switch (projection) {
            .targets => |t| t,
            else => &.{},
        };
        const node = parser.asNode(sb.node);
        const expr = switch (try orderKeyExpr(alloc, params, node, targets)) {
            .expr => |e| e,
            .invalid => |msg| return .{ .invalid = msg },
        };
        key.* = .{ .expr = expr, .desc = desc, .nulls_first = nulls_first };
    }
    return .{ .keys = keys };
}

/// One ORDER BY key's expression: `ORDER BY 2` is the second target's,
/// `ORDER BY who` the target named who when there is one, else the
/// expression itself.
fn orderKeyExpr(
    alloc: std.mem.Allocator,
    params: *ParamTally,
    node: ?*c.Node,
    targets: []const Target,
) parser.ParseError!ParsedExpr {
    if (constant(node)) |ac| if (ac.val.node.type == c.T_Integer) {
        const n = ac.val.ival.ival;
        if (n < 1 or n > targets.len)
            return ParsedExpr.reject("ORDER BY position is not in select list");
        return .{ .expr = targets[@intCast(n - 1)].expr };
    };
    if (try bareName(alloc, node)) |name| {
        for (targets) |t| {
            if (std.mem.eql(u8, t.name, name)) return .{ .expr = t.expr };
        }
    }
    return parseExpr(alloc, params, node);
}

/// An unqualified column reference's name, copied; null for anything
/// else, a qualified `t.who` included: only a bare name can be an
/// output name.
fn bareName(alloc: std.mem.Allocator, p: anytype) parser.ParseError!?String {
    const cr = parser.cast(c.ColumnRef, p) orelse return null;
    if (parser.items(cr.fields).len != 1) return null;
    return lastName(alloc, cr.fields);
}

/// Whether a call is an aggregate: one of the standard five by name,
/// or any call in an aggregate or window form.
fn isAggregate(fc: *const c.FuncCall) bool {
    if (fc.agg_star or fc.agg_distinct or fc.agg_filter != null or
        fc.agg_order != null or fc.over != null) return true;
    const name = funcName(fc) orelse return false;
    const aggregates = [_]String{ "count", "sum", "avg", "min", "max" };
    for (aggregates) |agg| {
        if (std.mem.eql(u8, name, agg)) return true;
    }
    return false;
}

const ParsedTargets = union(enum) {
    projection: Projection,
    stmt: Stmt,
};

/// A target list, SELECT's or RETURNING's: a `*` anywhere makes it a
/// star projection; every other target is an expression. count(*),
/// the one aggregate, is SELECT's alone and stands by itself.
fn parseTargets(
    alloc: std.mem.Allocator,
    params: *ParamTally,
    list: [*c]const c.List,
    clause: enum { select, returning },
) parser.ParseError!ParsedTargets {
    const targets = parser.items(list);
    var star = false;
    var count_star = false;
    var outs: std.ArrayList(Target) = .empty;
    for (targets) |cell| {
        const rt = parser.cast(c.ResTarget, cell.ptr_value) orelse
            return .{ .stmt = .unsupported };
        if (parser.cast(c.FuncCall, rt.val)) |fc| {
            if (isCountStar(fc) and clause == .select) {
                count_star = true;
                continue;
            }
            if (isAggregate(fc)) return .{ .stmt = invalid(switch (clause) {
                .select => "only count(*) is supported among aggregates",
                .returning => "aggregates are not allowed in RETURNING",
            }) };
        }
        if (parser.cast(c.ColumnRef, rt.val)) |cr| {
            for (parser.items(cr.fields)) |f| {
                if (parser.cast(c.A_Star, f.ptr_value) != null) star = true;
            }
            if (star) continue;
        }
        const node = parser.asNode(rt.val);
        const expr = switch (try parseExpr(alloc, params, node)) {
            .expr => |e| e,
            .invalid => |msg| return .{ .stmt = .{ .invalid = msg } },
        };
        try outs.append(alloc, .{
            .name = try targetName(alloc, rt, expr),
            .expr = expr,
        });
    }
    if (count_star and targets.len != 1) return .{
        .stmt = invalid("count(*) cannot be combined with other targets"),
    };
    if (count_star) return .{ .projection = .count_star };
    if (star) return .{ .projection = .star };
    return .{ .projection = .{ .targets = try outs.toOwnedSlice(alloc) } };
}

/// The outcome of a RETURNING list: null without one.
const ParsedReturning = union(enum) {
    projection: ?Projection,
    stmt: Stmt,
};

/// INSERT, UPDATE or DELETE ... RETURNING id, name: a target list as
/// a SELECT's, minus aggregates. Absent in the grammar = null.
fn parseReturning(
    alloc: std.mem.Allocator,
    params: *ParamTally,
    list: [*c]const c.List,
) parser.ParseError!ParsedReturning {
    if (parser.items(list).len == 0) return .{ .projection = null };
    return switch (try parseTargets(alloc, params, list, .returning)) {
        .projection => |x| .{ .projection = x },
        .stmt => |rejected| .{ .stmt = rejected },
    };
}

const ParsedTypmod = union(enum) { mod: u32, stmt: Stmt };

/// NUMERIC(p) / NUMERIC(p, s): the precision in 1..38 (Postgres allows
/// 1000; numeric.zig holds 38 digits) and the scale in 0..p, packed as
/// Postgres packs them, `(p << 16) | s`, with its wording for a bad
/// one.
fn numericTypmod(
    alloc: std.mem.Allocator,
    typmods: []const c.ListCell,
) parser.ParseError!ParsedTypmod {
    if (typmods.len > 2) return .{
        .stmt = invalid("invalid type modifier for type numeric"),
    };
    var nums: [2]i64 = .{ 0, 0 };
    for (typmods, 0..) |cell, i| {
        const ac = parser.cast(c.A_Const, cell.ptr_value);
        if (ac == null or ac.?.val.node.type != c.T_Integer) return .{
            .stmt = invalid("invalid type modifier for type numeric"),
        };
        nums[i] = ac.?.val.ival.ival;
    }
    const precision = nums[0];
    const scale = if (typmods.len == 2) nums[1] else 0;
    if (precision < 1 or precision > _numeric.max_digits) return .{
        .stmt = try invalidFmt(
            alloc,
            "NUMERIC precision {d} must be between 1 and {d}",
            .{ precision, _numeric.max_digits },
        ),
    };
    if (scale < 0 or scale > precision) return .{
        .stmt = try invalidFmt(
            alloc,
            "NUMERIC scale {d} must be between 0 and precision {d}",
            .{ scale, precision },
        ),
    };
    const p: u32 = @intCast(precision);
    const s: u32 = @intCast(scale);
    return .{ .mod = (p << 16) | s };
}

/// A target's output name: its alias, else what Postgres prints, the
/// column's name for a column, the function's for a call, `?column?`
/// for anything else.
fn targetName(
    alloc: std.mem.Allocator,
    rt: *const c.ResTarget,
    expr: Expr,
) parser.ParseError!String {
    if (rt.name != null) return dup(alloc, rt.name);
    return switch (expr) {
        .column, .excluded => |name| name,
        .call => |call| call.func.name,
        .literal, .binop => "?column?",
    };
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
        const ac = constant(rt.val) orelse
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
    const name = funcName(fc) orelse return false;
    return std.mem.eql(u8, name, "count") and fc.agg_star and
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

/// BEGIN / START TRANSACTION / COMMIT / ROLLBACK and the savepoint
/// statements. Transaction modes (ISOLATION LEVEL …) are ignored;
/// two-phase commit is rejected with a reason.
fn parseTransaction(
    alloc: std.mem.Allocator,
    ts: *const c.TransactionStmt,
) parser.ParseError!Stmt {
    return switch (ts.kind) {
        c.TRANS_STMT_BEGIN, c.TRANS_STMT_START => .begin_txn,
        c.TRANS_STMT_COMMIT => .commit_txn,
        c.TRANS_STMT_ROLLBACK => .rollback_txn,
        c.TRANS_STMT_SAVEPOINT => .{
            .savepoint = try dup(alloc, ts.savepoint_name),
        },
        c.TRANS_STMT_RELEASE => .{
            .release_savepoint = try dup(alloc, ts.savepoint_name),
        },
        c.TRANS_STMT_ROLLBACK_TO => .{
            .rollback_to = try dup(alloc, ts.savepoint_name),
        },
        else => invalid("unsupported transaction statement"),
    };
}

/// DELETE FROM one_table [WHERE ...] [RETURNING ...]. USING, WITH, and
/// any WHERE shape parseWhere rejects carry a reason. `WHERE egid = N`
/// parses as an ordinary column comparison — egid resolution stays
/// server-side.
fn parseDelete(
    alloc: std.mem.Allocator,
    ds: *const c.DeleteStmt,
) parser.ParseError!Stmt {
    if (ds.usingClause != null)
        return invalid("DELETE ... USING is not supported");
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

    const ret = try parseReturning(alloc, &params, ds.returningList);
    const returning = switch (ret) {
        .projection => |x| x,
        .stmt => |rejected| return rejected,
    };

    return .{ .delete = .{
        .table = try relname(alloc, ds.relation),
        .where = where,
        .returning = returning,
        .param_count = params.count,
    } };
}

/// UPDATE one_table SET col = expr [, ...] [WHERE ...] [RETURNING ...].
/// FROM, WITH, SET values parseExpr does not take, and duplicate SET
/// columns are rejected with a reason.
fn parseUpdate(
    alloc: std.mem.Allocator,
    us: *const c.UpdateStmt,
) parser.ParseError!Stmt {
    if (us.fromClause != null)
        return invalid("UPDATE ... FROM is not supported");
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

    const sets = switch (try parseSets(alloc, &params, us.targetList)) {
        .sets => |s| s,
        .stmt => |rejected| return rejected,
    };
    const ret = try parseReturning(alloc, &params, us.returningList);
    const returning = switch (ret) {
        .projection => |x| x,
        .stmt => |rejected| return rejected,
    };

    return .{ .update = .{
        .table = try relname(alloc, us.relation),
        .sets = sets,
        .where = where,
        .returning = returning,
        .param_count = params.count,
    } };
}

const ParsedSets = union(enum) {
    sets: []SetClause,
    stmt: Stmt, // the rejection to return
};

/// The SET list of an UPDATE or an ON CONFLICT DO UPDATE: `column =
/// expr` each, no column twice.
fn parseSets(
    alloc: std.mem.Allocator,
    params: *ParamTally,
    list: [*c]c.List,
) parser.ParseError!ParsedSets {
    var sets: std.ArrayList(SetClause) = .empty;
    for (parser.items(list)) |cell| {
        const rt = parser.cast(c.ResTarget, cell.ptr_value) orelse
            return .{ .stmt = .unsupported };
        if (rt.name == null) return .{ .stmt = .unsupported };
        const column = try dup(alloc, rt.name);
        for (sets.items) |s| {
            if (std.mem.eql(u8, s.column, column))
                return .{ .stmt = invalid("duplicate column in SET") };
        }
        const val = parser.asNode(rt.val);
        const value = switch (try parseExpr(alloc, params, val)) {
            .expr => |e| e,
            .invalid => |msg| return .{ .stmt = .{ .invalid = msg } },
        };
        try sets.append(alloc, .{ .column = column, .value = value });
    }
    return .{ .sets = try sets.toOwnedSlice(alloc) };
}
