// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const pgwire = @import("pgwire");
const String = sql.value.String;
const parse = sql.parse;

test "sql.parse: CREATE/DROP DATABASE parse to typed statements" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const create = try parse.statement(alloc, "CREATE DATABASE app;");
    try testing.expectEqualStrings("app", create.create_database.name);
    try testing.expect(create.create_database.source == null);

    const drop = try parse.statement(alloc, "drop database app");
    try testing.expectEqualStrings("app", drop.drop_database);

    // Both spellings Postgres accepts for a source database.
    const b1 = try parse.statement(alloc, "CREATE DATABASE b TEMPLATE app");
    try testing.expectEqualStrings("b", b1.create_database.name);
    try testing.expectEqualStrings("app", b1.create_database.source.?);
    const b2 = try parse.statement(
        alloc,
        "CREATE DATABASE b WITH TEMPLATE = app OWNER = me",
    );
    try testing.expectEqualStrings("app", b2.create_database.source.?);
}

test "sql.parse: CREATE TABLE AS TABLE / SELECT * FROM parse to a branch" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const t1 = try parse.statement(alloc, "CREATE TABLE y AS TABLE x;");
    try testing.expectEqualStrings("y", t1.create_table_as.name);
    try testing.expectEqualStrings("x", t1.create_table_as.source);
    try testing.expect(t1.create_table_as.with_data);
    try testing.expect(!t1.create_table_as.if_not_exists);

    const t2 = try parse.statement(
        alloc,
        "CREATE TABLE IF NOT EXISTS y AS SELECT * FROM x WITH NO DATA",
    );
    try testing.expectEqualStrings("x", t2.create_table_as.source);
    try testing.expect(!t2.create_table_as.with_data);
    try testing.expect(t2.create_table_as.if_not_exists);

    // A result, not a table: refused.
    const where = try parse.statement(
        alloc,
        "CREATE TABLE y AS SELECT * FROM x WHERE id > 1",
    );
    try testing.expect(where == .invalid);
    const cols = try parse.statement(
        alloc,
        "CREATE TABLE y AS SELECT id FROM x",
    );
    try testing.expect(cols == .invalid);
    const named = try parse.statement(alloc, "CREATE TABLE y (a) AS TABLE x");
    try testing.expect(named == .invalid);
}

test "sql.parse: ALTER TABLE RENAME COLUMN is recognized" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const stmt = try parse.statement(
        alloc,
        "ALTER TABLE accounts RENAME COLUMN email TO contact_email;",
    );
    try testing.expect(stmt == .rename_column);

    const rename = stmt.rename_column;
    try testing.expectEqualStrings("accounts", rename.table);
    try testing.expectEqualStrings("email", rename.old_name);
    try testing.expectEqualStrings("contact_email", rename.new_name);
    try testing.expect(!rename.missing_ok);

    const optional = try parse.statement(alloc,
        \\ALTER TABLE IF EXISTS "Accounts" RENAME "Email" TO "Contact_Email";
    );
    try testing.expect(optional == .rename_column);
    try testing.expect(optional.rename_column.missing_ok);
    try testing.expectEqualStrings("Accounts", optional.rename_column.table);
    try testing.expectEqualStrings("Email", optional.rename_column.old_name);
    try testing.expectEqualStrings(
        "Contact_Email",
        optional.rename_column.new_name,
    );
}

test "sql.parse: other rename kinds stay unsupported" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const statements = [_]String{
        "ALTER TABLE accounts RENAME TO customers;",
        "ALTER VIEW account_view RENAME COLUMN email TO contact_email;",
    };
    for (statements) |statement| {
        const stmt = try parse.statement(alloc, statement);
        try testing.expect(stmt == .unsupported);
    }
}

test "sql.parse: everything else is unsupported; bad SQL is a syntax error" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    // A statement kind we don't handle at all (TRUNCATE) is unsupported.
    const trunc = try parse.statement(alloc, "TRUNCATE t;");
    try testing.expect(trunc == .unsupported);

    try testing.expectError(error.SyntaxError, parse.statement(alloc, "CREATE DATABASE;"));
}

test "sql.parse: CREATE TABLE parses columns, types, and the primary key" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const ct = try parse.statement(alloc, "CREATE TABLE t (id int PRIMARY KEY, name text);");
    try testing.expectEqualStrings("t", ct.create_table.name);
    try testing.expect(!ct.create_table.if_not_exists);
    const cols = ct.create_table.columns;
    try testing.expectEqual(@as(usize, 2), cols.len);
    try testing.expectEqualStrings("id", cols[0].name);
    try testing.expectEqual(sql.Type.int4, cols[0].ty); // `int` → pg_catalog.int4
    try testing.expect(cols[0].pkey);
    try testing.expectEqualStrings("name", cols[1].name);
    try testing.expectEqual(sql.Type.text, cols[1].ty);
    try testing.expect(!cols[1].pkey);
    try testing.expect(!cols[0].unique and !cols[1].unique);

    // UNIQUE marks a column; on the PRIMARY KEY column it is absorbed
    // (the PK index already enforces it).
    const uq = try parse.statement(
        alloc,
        "CREATE TABLE t (id int PRIMARY KEY UNIQUE, email text UNIQUE);",
    );
    try testing.expect(uq.create_table.columns[0].pkey);
    try testing.expect(!uq.create_table.columns[0].unique);
    try testing.expect(!uq.create_table.columns[1].pkey);
    try testing.expect(uq.create_table.columns[1].unique);

    // IF NOT EXISTS + schema-qualified name + our unsigned custom type.
    const ine = try parse.statement(alloc, "CREATE TABLE IF NOT EXISTS public.u (a uint4);");
    try testing.expectEqualStrings("u", ine.create_table.name);
    try testing.expect(ine.create_table.if_not_exists);
    try testing.expectEqual(sql.Type.uint4, ine.create_table.columns[0].ty);

    // Zero columns is legal Postgres.
    const empty = try parse.statement(alloc, "CREATE TABLE nothing ();");
    try testing.expectEqual(@as(usize, 0), empty.create_table.columns.len);
}

test "sql.parse: CREATE TABLE char(n) and varchar(n) carry their length" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const ct = try parse.statement(alloc,
        \\CREATE TABLE t (a char(84), b char, c character(3), d varchar(20),
        \\  e varchar, f character varying(5), g text);
    );
    const cols = ct.create_table.columns;
    try testing.expectEqual(@as(usize, 7), cols.len);
    try testing.expectEqual(sql.Type.bpchar, cols[0].ty);
    try testing.expectEqual(@as(?u32, 84), cols[0].type_mod);
    // A lengthless `char` is char(1).
    try testing.expectEqual(sql.Type.bpchar, cols[1].ty);
    try testing.expectEqual(@as(?u32, 1), cols[1].type_mod);
    try testing.expectEqual(sql.Type.bpchar, cols[2].ty);
    try testing.expectEqual(@as(?u32, 3), cols[2].type_mod);
    try testing.expectEqual(sql.Type.varchar, cols[3].ty);
    try testing.expectEqual(@as(?u32, 20), cols[3].type_mod);
    // A lengthless `varchar` is plain text.
    try testing.expectEqual(sql.Type.text, cols[4].ty);
    try testing.expectEqual(@as(?u32, null), cols[4].type_mod);
    try testing.expectEqual(sql.Type.varchar, cols[5].ty);
    try testing.expectEqual(@as(?u32, 5), cols[5].type_mod);
    try testing.expectEqual(sql.Type.text, cols[6].ty);
    try testing.expectEqual(@as(?u32, null), cols[6].type_mod);

    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "CREATE TABLE t (a char(0));", .needle = "at least 1" },
        .{ .sql = "CREATE TABLE t (a varchar(0));", .needle = "at least 1" },
        .{ .sql = "CREATE TABLE t (a char(10485761));", .needle = "cannot exceed" },
        .{ .sql = "CREATE TABLE t (a text(5));", .needle = "not allowed for type" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
    // Two lengths is a grammar error in Postgres, not a type check.
    try testing.expectError(error.SyntaxError, parse.statement(alloc, "CREATE TABLE t (a char(1,2));"));
}

test "sql.parse: NOT NULL is a column constraint; the PRIMARY KEY implies it" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const stmt = try parse.statement(
        arena.allocator(),
        "CREATE TABLE t (a int PRIMARY KEY, b text NOT NULL, " ++
            "c int NULL, d int);",
    );
    const cols = stmt.create_table.columns;
    try testing.expectEqual(@as(usize, 4), cols.len);
    try testing.expect(cols[0].not_null and cols[0].pkey);
    try testing.expect(cols[1].not_null and !cols[1].pkey);
    try testing.expect(!cols[2].not_null);
    try testing.expect(!cols[3].not_null);
}

test "sql.parse: a cast constant is the constant (pgjdbc simple mode)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const sel = try parse.statement(
        alloc,
        "SELECT val FROM t WHERE sk = '6'::int8;",
    );
    const eq = sel.select.where.?.cmp;
    try testing.expectEqualStrings("6", eq.rhs.literal.str);
    const ins = try parse.statement(
        alloc,
        "INSERT INTO t (id, val) VALUES (CAST('7' AS int8), 'x'::text);",
    );
    try testing.expectEqualStrings("7", ins.insert.rows[0][0].str);
    try testing.expectEqualStrings("x", ins.insert.rows[0][1].str);
    const upd = try parse.statement(
        alloc,
        "UPDATE t SET val = val || ',1'::text WHERE id = '6'::int8;",
    );
    const rhs = upd.update.sets[0].value.binop.rhs.*;
    try testing.expectEqualStrings(",1", rhs.literal.str);
    const row = try parse.statement(alloc, "SELECT 1::int8;");
    try testing.expectEqual(@as(i64, 1), row.select_row.cells[0].value.int);
    // A cast of anything but a constant is still not a literal.
    const bad = try parse.statement(alloc, "SELECT a FROM t WHERE a = b::int8;");
    try testing.expect(bad == .invalid);
}

test "sql.parse: SHOW names its parameter; SET and RESET are accepted" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();
    const iso = try parse.statement(alloc, "SHOW TRANSACTION ISOLATION LEVEL;");
    try testing.expectEqualStrings("transaction_isolation", iso.show_var.name);
    const style = try parse.statement(alloc, "SHOW DateStyle;");
    try testing.expectEqualStrings("datestyle", style.show_var.name);
    try testing.expect(try parse.statement(alloc, "SHOW ALL;") == .show_var);
    try testing.expect(try parse.statement(alloc, "SET x = 1;") == .set_var);
    try testing.expect(try parse.statement(alloc, "RESET x;") == .reset_var);
}

test "sql.parse: CREATE TABLE semantic rejections carry a reason" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "CREATE TABLE t (a int CHECK (a > 0));", .needle = "NOT NULL" },
        .{ .sql = "CREATE TABLE t (a serial);", .needle = "serial" },
        .{ .sql = "CREATE TABLE t (a int, b int, PRIMARY KEY (a, b));", .needle = "table-level" },
        .{ .sql = "CREATE TABLE t (a int primary key, b int primary key);", .needle = "multiple PRIMARY KEY" },
        .{ .sql = "CREATE TABLE t (a int, a text);", .needle = "duplicate" },
        .{ .sql = "DROP TABLE a, b;", .needle = "multiple tables" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
}

test "sql.parse: DROP TABLE variants" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const dt = try parse.statement(alloc, "DROP TABLE t;");
    try testing.expectEqualStrings("t", dt.drop_table.name);
    try testing.expect(!dt.drop_table.missing_ok);

    // IF EXISTS + schema-qualified: the last name part is the table.
    const ie = try parse.statement(alloc, "DROP TABLE IF EXISTS public.t;");
    try testing.expectEqualStrings("t", ie.drop_table.name);
    try testing.expect(ie.drop_table.missing_ok);

    // Non-table, non-index DROPs are a different feature, not a
    // rejection.
    const dv = try parse.statement(alloc, "DROP VIEW v;");
    try testing.expect(dv == .unsupported);
}

test "sql.parse: CREATE INDEX variants" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const ci = try parse.statement(alloc, "CREATE INDEX idx_color ON t (color);");
    try testing.expectEqualStrings("idx_color", ci.create_index.name.?);
    try testing.expectEqualStrings("t", ci.create_index.table);
    try testing.expectEqualStrings("color", ci.create_index.column);
    try testing.expect(!ci.create_index.if_not_exists);

    // Unnamed: the executor derives "{table}_{column}_idx".
    const anon = try parse.statement(alloc, "CREATE INDEX ON t (color);");
    try testing.expect(anon.create_index.name == null);

    // IF NOT EXISTS + schema-qualified table.
    const ine = try parse.statement(
        alloc,
        "CREATE INDEX IF NOT EXISTS i ON public.t (color);",
    );
    try testing.expect(ine.create_index.if_not_exists);
    try testing.expectEqualStrings("t", ine.create_index.table);

    // ASC / NULLS LAST are our semantics already — accepted.
    const ord = try parse.statement(
        alloc,
        "CREATE INDEX i ON t (color ASC NULLS LAST);",
    );
    try testing.expectEqualStrings("color", ord.create_index.column);
    try testing.expect(!ord.create_index.unique);

    const uq = try parse.statement(alloc, "CREATE UNIQUE INDEX u ON t (a);");
    try testing.expect(uq.create_index.unique);
    try testing.expectEqualStrings("u", uq.create_index.name.?);
}

test "sql.parse: CREATE INDEX rejections carry a reason" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cases = [_]struct { sql: String, needle: String }{
        .{
            .sql = "CREATE INDEX CONCURRENTLY i ON t (a);",
            .needle = "CONCURRENTLY",
        },
        .{ .sql = "CREATE INDEX i ON t (a, b);", .needle = "single-column" },
        .{ .sql = "CREATE INDEX i ON t ((a + 1));", .needle = "expression" },
        .{ .sql = "CREATE INDEX i ON t (a) WHERE a > 1;", .needle = "partial" },
        .{ .sql = "CREATE INDEX i ON t USING hash (a);", .needle = "hash" },
        .{ .sql = "CREATE INDEX i ON t (a DESC);", .needle = "DESC" },
        .{
            .sql = "CREATE INDEX i ON t (a NULLS FIRST);",
            .needle = "NULLS FIRST",
        },
        .{ .sql = "CREATE INDEX i ON t (a) INCLUDE (b);", .needle = "INCLUDE" },
        .{ .sql = "DROP INDEX a, b;", .needle = "multiple" },
        .{ .sql = "DROP INDEX CONCURRENTLY i;", .needle = "CONCURRENTLY" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
}

test "sql.parse: DROP INDEX variants" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const di = try parse.statement(alloc, "DROP INDEX i;");
    try testing.expectEqualStrings("i", di.drop_index.name);
    try testing.expect(!di.drop_index.missing_ok);

    const ie = try parse.statement(alloc, "DROP INDEX IF EXISTS public.i;");
    try testing.expectEqualStrings("i", ie.drop_index.name);
    try testing.expect(ie.drop_index.missing_ok);
}

test "sql.parse: CREATE TABLE reserves the egid column name" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const stmt = try parse.statement(alloc, "CREATE TABLE t (egid int8, name text);");
    try testing.expect(stmt == .invalid);
    try testing.expect(std.mem.indexOf(u8, stmt.invalid, "egid") != null);
}

test "sql.parse: INSERT ... VALUES parses rows, columns, and literals" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    // Positional, multi-row.
    const pos = try parse.statement(alloc, "INSERT INTO t VALUES (1, 'a'), (2, 'b');");
    try testing.expectEqualStrings("t", pos.insert.table);
    try testing.expect(pos.insert.columns == null);
    try testing.expectEqual(@as(usize, 2), pos.insert.rows.len);
    try testing.expectEqual(@as(i64, 1), pos.insert.rows[0][0].int);
    try testing.expectEqualStrings("a", pos.insert.rows[0][1].str);
    try testing.expectEqual(@as(i64, 2), pos.insert.rows[1][0].int);

    // Explicit column list + null + bool literals.
    const cols = try parse.statement(alloc, "INSERT INTO t (id, flag, note) VALUES (0, true, NULL);");
    try testing.expect(cols.insert.columns != null);
    try testing.expectEqual(@as(usize, 3), cols.insert.columns.?.len);
    try testing.expectEqualStrings("id", cols.insert.columns.?[0]);
    try testing.expectEqual(@as(i64, 0), cols.insert.rows[0][0].int); // zero-valued ival
    try testing.expectEqual(true, cols.insert.rows[0][1].bool_);
    try testing.expect(cols.insert.rows[0][2] == .null_value);

    // CURRENT_TIMESTAMP and LOCALTIMESTAMP (with or without a
    // precision) are the one non-constant literal; any other function
    // or expression is an unsupported item, rejected at execute.
    const ts = try parse.statement(
        alloc,
        "INSERT INTO t (at) VALUES (CURRENT_TIMESTAMP), (LOCALTIMESTAMP), " ++
            "(CURRENT_TIMESTAMP(3)), (now()), (1 + 1), (CURRENT_DATE);",
    );
    try testing.expect(ts.insert.rows[0][0] == .current_timestamp);
    try testing.expect(ts.insert.rows[1][0] == .current_timestamp);
    try testing.expect(ts.insert.rows[2][0] == .current_timestamp);
    try testing.expect(ts.insert.rows[3][0] == .unsupported);
    try testing.expect(ts.insert.rows[4][0] == .unsupported);
    try testing.expect(ts.insert.rows[5][0] == .unsupported);
}

test "sql.parse: INSERT rejections carry a reason" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "INSERT INTO t VALUES (1) RETURNING count(*);", .needle = "RETURNING" },
        .{ .sql = "INSERT INTO t SELECT * FROM u;", .needle = "VALUES" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
}

test "sql.parse: SELECT parses projection, table, and an equality filter" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    // Star, no filter.
    const s1 = try parse.statement(alloc, "SELECT * FROM t;");
    try testing.expectEqualStrings("t", s1.select.table);
    try testing.expect(s1.select.projection == .star);
    try testing.expect(s1.select.where == null);

    // Named columns incl. the egid pseudo-column + a WHERE filter.
    const s2 = try parse.statement(alloc, "SELECT egid, name FROM t WHERE id = 5;");
    try testing.expect(s2.select.projection == .targets);
    const targets = s2.select.projection.targets;
    try testing.expectEqual(@as(usize, 2), targets.len);
    try testing.expectEqualStrings("egid", targets[0].expr.column);
    try testing.expectEqualStrings("egid", targets[0].name);
    try testing.expectEqualStrings("name", targets[1].expr.column);
    const f = s2.select.where.?.cmp;
    try testing.expectEqualStrings("id", f.lhs.column);
    try testing.expectEqual(@as(i64, 5), f.rhs.literal.int);

    // count(*): its own projection kind; WHERE and LIMIT parse as usual.
    const c1 = try parse.statement(alloc, "SELECT count(*) FROM t;");
    try testing.expectEqualStrings("t", c1.select.table);
    try testing.expect(c1.select.projection == .count_star);
    try testing.expect(c1.select.where == null);
    const c2 = try parse.statement(
        alloc,
        "select COUNT(*) from t where id = 5 limit 0;",
    );
    try testing.expect(c2.select.projection == .count_star);
    try testing.expectEqualStrings("id", c2.select.where.?.cmp.lhs.column);
    try testing.expectEqual(@as(?usize, 0), c2.select.limit);
}

test "sql.parse: WHERE parses AND / OR / NOT and IS [NOT] NULL" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    // libpg_query flattens a chain of ANDs into ONE node — the planner
    // reads the top-level AND terms straight off it.
    const s1 = try parse.statement(alloc, "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3;");
    const and3 = s1.select.where.?.and_;
    try testing.expectEqual(@as(usize, 3), and3.len);
    try testing.expectEqualStrings("a", and3[0].cmp.lhs.column);
    try testing.expectEqualStrings("b", and3[1].cmp.lhs.column);
    try testing.expectEqualStrings("c", and3[2].cmp.lhs.column);
    try testing.expectEqual(@as(i64, 3), and3[2].cmp.rhs.literal.int);

    // Nesting, parentheses, NOT — shared by DELETE.
    const d = try parse.statement(alloc, "DELETE FROM t WHERE a = 1 OR (b = 2 AND NOT c = 'x');");
    const or2 = d.delete.where.?.or_;
    try testing.expectEqual(@as(usize, 2), or2.len);
    try testing.expectEqualStrings("a", or2[0].cmp.lhs.column);
    const and2 = or2[1].and_;
    try testing.expectEqual(@as(usize, 2), and2.len);
    try testing.expectEqualStrings("b", and2[0].cmp.lhs.column);
    const inner = and2[1].not.*;
    try testing.expectEqualStrings("c", inner.cmp.lhs.column);
    try testing.expectEqualStrings("x", inner.cmp.rhs.literal.str);

    // SQL precedence: AND binds tighter than OR.
    const p = try parse.statement(alloc, "SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3;");
    const top = p.select.where.?.or_;
    try testing.expectEqual(@as(usize, 2), top.len);
    try testing.expect(top[0] == .cmp);
    try testing.expectEqual(@as(usize, 2), top[1].and_.len);

    // IS NULL / IS NOT NULL — shared by UPDATE.
    const upd1 = try parse.statement(alloc, "UPDATE t SET a = 1 WHERE b IS NULL;");
    try testing.expectEqualStrings("b", upd1.update.where.?.is_null.expr.column);
    try testing.expect(!upd1.update.where.?.is_null.negate);
    const upd2 = try parse.statement(alloc, "UPDATE t SET a = 1 WHERE b IS NOT NULL AND c = 1;");
    const upd2_and = upd2.update.where.?.and_;
    try testing.expect(upd2_and[0].is_null.negate);
    try testing.expectEqualStrings("c", upd2_and[1].cmp.lhs.column);

    // Rejections anywhere in the tree surface the leaf's reason.
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "SELECT * FROM t WHERE a = 1 AND b ~ 'x';", .needle = "comparisons" },
        .{ .sql = "SELECT * FROM t WHERE a = 1 OR b LIKE 'x';", .needle = "unsupported WHERE" },
        .{ .sql = "SELECT * FROM t WHERE a BETWEEN SYMMETRIC 1 AND 2;", .needle = "unsupported WHERE" },
        .{ .sql = "SELECT * FROM t WHERE (a = 1) IS NULL;", .needle = "operators" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
}

test "sql.parse: IN and BETWEEN are ORs and ANDs of comparisons" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    // a IN (1, 2) is a = 1 OR a = 2; NOT IN negates the OR.
    const in = try parse.statement(alloc, "SELECT * FROM t WHERE a IN (1, 2);");
    const in_or = in.select.where.?.or_;
    try testing.expectEqual(@as(usize, 2), in_or.len);
    try testing.expectEqualStrings("a", in_or[0].cmp.lhs.column);
    try testing.expectEqual(parse.CmpOp.eq, in_or[0].cmp.op);
    try testing.expectEqual(@as(i64, 1), in_or[0].cmp.rhs.literal.int);
    try testing.expectEqual(@as(i64, 2), in_or[1].cmp.rhs.literal.int);
    const not_in = try parse.statement(alloc, "DELETE FROM t WHERE a NOT IN ($1, $2);");
    const not_in_or = not_in.delete.where.?.not.or_;
    try testing.expectEqual(@as(u16, 2), not_in_or[1].cmp.rhs.literal.param);
    try testing.expectEqual(@as(u16, 2), not_in.delete.param_count);
    // A column in the list goes left in its own comparison.
    const col_in = try parse.statement(alloc, "SELECT * FROM t WHERE 1 IN (a, 2);");
    const col_or = col_in.select.where.?.or_;
    try testing.expectEqualStrings("a", col_or[0].cmp.lhs.column);
    try testing.expectEqual(@as(i64, 1), col_or[0].cmp.rhs.literal.int);
    try testing.expectEqual(@as(i64, 1), col_or[1].cmp.lhs.literal.int);

    // a BETWEEN 1 AND 5 is a >= 1 AND a <= 5; NOT BETWEEN, a < 1 OR a > 5.
    const bt = try parse.statement(alloc, "SELECT * FROM t WHERE a BETWEEN 1 AND 5;");
    const bt_and = bt.select.where.?.and_;
    try testing.expectEqual(parse.CmpOp.ge, bt_and[0].cmp.op);
    try testing.expectEqual(@as(i64, 1), bt_and[0].cmp.rhs.literal.int);
    try testing.expectEqual(parse.CmpOp.le, bt_and[1].cmp.op);
    try testing.expectEqual(@as(i64, 5), bt_and[1].cmp.rhs.literal.int);
    const not_bt = try parse.statement(alloc, "SELECT * FROM t WHERE a NOT BETWEEN 1 AND 5;");
    const not_bt_or = not_bt.select.where.?.or_;
    try testing.expectEqual(parse.CmpOp.lt, not_bt_or[0].cmp.op);
    try testing.expectEqual(parse.CmpOp.gt, not_bt_or[1].cmp.op);
    try testing.expectEqualStrings("a", not_bt_or[1].cmp.lhs.column);
}

test "sql.parse: IS DISTINCT FROM, IS TRUE and a bare boolean column" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    const d = try parse.statement(alloc, "SELECT * FROM t WHERE a IS DISTINCT FROM 1;");
    try testing.expectEqual(parse.CmpOp.differs, d.select.where.?.cmp.op);
    const nd = try parse.statement(alloc, "SELECT * FROM t WHERE NULL IS NOT DISTINCT FROM a;");
    try testing.expectEqual(parse.CmpOp.same, nd.select.where.?.cmp.op);
    try testing.expectEqualStrings("a", nd.select.where.?.cmp.lhs.column);
    try testing.expect(nd.select.where.?.cmp.rhs.literal == .null_value);

    const cases = [_]struct { sql: String, is: parse.Where.BoolIs, negate: bool }{
        .{ .sql = "SELECT * FROM t WHERE b IS TRUE;", .is = .true_, .negate = false },
        .{ .sql = "SELECT * FROM t WHERE b IS NOT TRUE;", .is = .true_, .negate = true },
        .{ .sql = "SELECT * FROM t WHERE b IS FALSE;", .is = .false_, .negate = false },
        .{ .sql = "SELECT * FROM t WHERE b IS NOT FALSE;", .is = .false_, .negate = true },
        .{ .sql = "SELECT * FROM t WHERE b IS UNKNOWN;", .is = .unknown, .negate = false },
        .{ .sql = "SELECT * FROM t WHERE b IS NOT UNKNOWN;", .is = .unknown, .negate = true },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        const bt = stmt.select.where.?.bool_test;
        try testing.expectEqualStrings("b", bt.expr.column);
        try testing.expectEqual(case.is, bt.is);
        try testing.expectEqual(case.negate, bt.negate);
    }

    // WHERE b is b = true; NOT b wraps it.
    const bare = try parse.statement(alloc, "SELECT * FROM t WHERE b;");
    try testing.expectEqualStrings("b", bare.select.where.?.cmp.lhs.column);
    try testing.expect(bare.select.where.?.cmp.rhs.literal.bool_);
    const not_bare = try parse.statement(alloc, "SELECT * FROM t WHERE a = 1 AND NOT b;");
    try testing.expectEqualStrings("b", not_bare.select.where.?.and_[1].not.cmp.lhs.column);
}

test "sql.parse: ORDER BY keys, ordinals, output names and OFFSET" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    // Directions and NULL placement: ASC puts NULLs last, DESC first,
    // unless said otherwise.
    const s = try parse.statement(
        alloc,
        "SELECT a, b AS who FROM t ORDER BY a, b DESC, a NULLS FIRST, " ++
            "b DESC NULLS LAST OFFSET 3;",
    );
    const keys = s.select.order_by;
    try testing.expectEqual(@as(usize, 4), keys.len);
    try testing.expectEqualStrings("a", keys[0].expr.column);
    try testing.expect(!keys[0].desc and !keys[0].nulls_first);
    try testing.expect(keys[1].desc and keys[1].nulls_first);
    try testing.expect(!keys[2].desc and keys[2].nulls_first);
    try testing.expect(keys[3].desc and !keys[3].nulls_first);
    try testing.expectEqual(@as(usize, 3), s.select.offset);

    // ORDER BY 2 is the second target; ORDER BY who the target named
    // who; a name that is no target's is the row's column; an
    // expression binds a $n.
    const r = try parse.statement(
        alloc,
        "SELECT a + 1 AS n, b AS who FROM t ORDER BY 1, who, c, a * $1;",
    );
    const rk = r.select.order_by;
    try testing.expect(rk[0].expr == .binop);
    try testing.expectEqualStrings("b", rk[1].expr.column);
    try testing.expectEqualStrings("c", rk[2].expr.column);
    try testing.expectEqual(@as(u16, 1), rk[3].expr.binop.rhs.literal.param);
    try testing.expectEqual(@as(u16, 1), r.select.param_count);

    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "SELECT a FROM t ORDER BY 2;", .needle = "select list" },
        .{ .sql = "SELECT * FROM t ORDER BY 1;", .needle = "select list" },
        .{
            .sql = "SELECT count(*) FROM t ORDER BY 1;",
            .needle = "not allowed",
        },
        .{ .sql = "SELECT 1 ORDER BY 1;", .needle = "need a FROM" },
        .{ .sql = "SELECT a FROM t ORDER BY a USING <;", .needle = "USING" },
        .{ .sql = "SELECT a FROM t OFFSET -1;", .needle = "negative" },
        .{ .sql = "SELECT a FROM t OFFSET $1;", .needle = "literal OFFSET" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        const msg = stmt.invalid;
        try testing.expect(std.mem.indexOf(u8, msg, case.needle) != null);
    }
}

test "sql.parse: LIMIT takes an integer literal; ALL and NULL mean none" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cases = [_]struct { sql: String, limit: ?usize }{
        .{ .sql = "SELECT * FROM t;", .limit = null },
        .{ .sql = "SELECT * FROM t LIMIT 3;", .limit = 3 },
        .{ .sql = "SELECT * FROM t LIMIT 0;", .limit = 0 },
        .{ .sql = "SELECT * FROM t WHERE a = 1 LIMIT 2;", .limit = 2 },
        .{ .sql = "SELECT * FROM t FOR UPDATE LIMIT 5;", .limit = 5 },
        .{ .sql = "SELECT * FROM t FETCH FIRST 4 ROWS ONLY;", .limit = 4 },
        .{ .sql = "SELECT * FROM t LIMIT ALL;", .limit = null },
        .{ .sql = "SELECT * FROM t LIMIT NULL;", .limit = null },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .select);
        try testing.expectEqual(case.limit, stmt.select.limit);
    }
}

test "sql.parse: SELECT rejections carry a reason" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "SELECT * FROM t GROUP BY a;", .needle = "GROUP BY" },
        .{ .sql = "SELECT DISTINCT a FROM t;", .needle = "DISTINCT" },
        .{ .sql = "SELECT * FROM t LIMIT -1;", .needle = "negative" },
        .{ .sql = "SELECT * FROM t LIMIT 1 + 1;", .needle = "integer literal" },
        .{ .sql = "SELECT * FROM t LIMIT $1;", .needle = "integer literal" },
        .{ .sql = "SELECT * FROM t LIMIT 1.5;", .needle = "integer literal" },
        // FETCH ... WITH TIES is refused too, but the vendored grammar
        // already rejects the syntax, so it cannot be reached here.
        .{ .sql = "SELECT * FROM a, b;", .needle = "one table" },
        .{ .sql = "SELECT count(id) FROM t;", .needle = "count(*)" },
        .{ .sql = "SELECT sum(id) FROM t;", .needle = "count(*)" },
        .{ .sql = "SELECT count(DISTINCT id) FROM t;", .needle = "count(*)" },
        .{
            .sql = "SELECT count(*) FILTER (WHERE id = 1) FROM t;",
            .needle = "count(*)",
        },
        .{ .sql = "SELECT id, count(*) FROM t;", .needle = "other targets" },
        .{
            .sql = "SELECT count(*), count(*) FROM t;",
            .needle = "other targets",
        },
        .{
            .sql = "SELECT count(*) FROM t FOR UPDATE;",
            .needle = "FOR UPDATE",
        },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
}

test "sql.parse: SELECT without FROM is a row of constants" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const s1 = try parse.statement(alloc, "SELECT 1;");
    try testing.expect(s1 == .select_row);
    try testing.expectEqual(1, s1.select_row.cells.len);
    try testing.expectEqualStrings("?column?", s1.select_row.cells[0].name);
    try testing.expectEqual(1, s1.select_row.cells[0].value.int);
    try testing.expectEqual(null, s1.select_row.limit);

    const s2 = try parse.statement(
        alloc,
        "SELECT 1 AS one, 'a' b, true, NULL, 1.5 LIMIT 0;",
    );
    const cells = s2.select_row.cells;
    try testing.expectEqual(5, cells.len);
    try testing.expectEqualStrings("one", cells[0].name);
    try testing.expectEqualStrings("b", cells[1].name);
    try testing.expectEqualStrings("a", cells[1].value.str);
    try testing.expectEqual(true, cells[2].value.bool_);
    try testing.expect(cells[3].value == .null_value);
    try testing.expectEqualStrings("1.5", cells[4].value.float);
    try testing.expectEqual(0, s2.select_row.limit);

    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "SELECT now();", .needle = "constants" },
        .{ .sql = "SELECT count(*);", .needle = "constants" },
        .{ .sql = "SELECT a;", .needle = "constants" },
        .{ .sql = "SELECT $1;", .needle = "constants" },
        .{ .sql = "SELECT 1 + 1;", .needle = "constants" },
        .{ .sql = "SELECT 1 WHERE true;", .needle = "FROM" },
        .{ .sql = "SELECT 1 FOR UPDATE;", .needle = "FROM" },
        .{ .sql = "VALUES (1);", .needle = "VALUES" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        try testing.expect(
            std.mem.indexOf(u8, stmt.invalid, case.needle) != null,
        );
    }
}

test "sql.parse: numeric(p, s) packs the precision and scale" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const ct = try parse.statement(
        alloc,
        "CREATE TABLE t (a numeric(10, 2), b decimal(5), c numeric);",
    );
    const cols = ct.create_table.columns;
    try testing.expectEqual(sql.Type.numeric, cols[0].ty);
    try testing.expectEqual(@as(?u32, (10 << 16) | 2), cols[0].type_mod);
    try testing.expectEqual(sql.Type.numeric, cols[1].ty);
    try testing.expectEqual(@as(?u32, 5 << 16), cols[1].type_mod);
    try testing.expectEqual(@as(?u32, null), cols[2].type_mod);

    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "CREATE TABLE t (a numeric(0));", .needle = "precision 0" },
        .{ .sql = "CREATE TABLE t (a numeric(39));", .needle = "precision 39" },
        .{ .sql = "CREATE TABLE t (a numeric(5, 6));", .needle = "scale 6" },
        .{
            .sql = "CREATE TABLE t (a numeric(5, 1, 1));",
            .needle = "modifier",
        },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        const found = std.mem.indexOf(u8, stmt.invalid, case.needle);
        try testing.expect(found != null);
    }
}

test "sql.parse: DEALLOCATE names the statement, ALL is null" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const one = try parse.statement(alloc, "DEALLOCATE s1;");
    try testing.expectEqualStrings("s1", one.deallocate.?);
    const prep = try parse.statement(alloc, "DEALLOCATE PREPARE \"_pg3_0\";");
    try testing.expectEqualStrings("_pg3_0", prep.deallocate.?);
    const all = try parse.statement(alloc, "DEALLOCATE ALL;");
    try testing.expect(all.deallocate == null);
}

test "sql.parse: SET and RESET are accepted and ignored" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const sets = [_]String{
        "SET extra_float_digits = 3;",
        "SET application_name TO 'x';",
        "SET LOCAL search_path = public;",
        "SET TIME ZONE 'UTC';",
        "SET client_encoding = DEFAULT;",
        "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;",
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;",
    };
    for (sets) |text|
        try testing.expect(try parse.statement(alloc, text) == .set_var);
    const r1 = try parse.statement(alloc, "RESET search_path;");
    try testing.expect(r1 == .reset_var);
    const r2 = try parse.statement(alloc, "RESET ALL;");
    try testing.expect(r2 == .reset_var);
}

test "sql.parse: DELETE parses table and the optional equality filter" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    // No WHERE: delete every row.
    const d1 = try parse.statement(alloc, "DELETE FROM t;");
    try testing.expectEqualStrings("t", d1.delete.table);
    try testing.expect(d1.delete.where == null);

    // WHERE on a payload column.
    const d2 = try parse.statement(alloc, "DELETE FROM t WHERE id = 5;");
    const f2 = d2.delete.where.?.cmp;
    try testing.expectEqualStrings("id", f2.lhs.column);
    try testing.expectEqual(@as(i64, 5), f2.rhs.literal.int);

    // WHERE on the egid pseudo-column parses as a plain column filter.
    const d3 = try parse.statement(alloc, "DELETE FROM t WHERE egid = 3;");
    const f3 = d3.delete.where.?.cmp;
    try testing.expectEqualStrings("egid", f3.lhs.column);
    try testing.expectEqual(@as(i64, 3), f3.rhs.literal.int);

    // Qualified name keeps the last part; string literal.
    const d4 = try parse.statement(alloc, "DELETE FROM public.t WHERE name = 'x';");
    try testing.expectEqualStrings("t", d4.delete.table);
    const f4 = d4.delete.where.?.cmp;
    try testing.expectEqualStrings("name", f4.lhs.column);
    try testing.expectEqualStrings("x", f4.rhs.literal.str);
}

test "sql.parse: DELETE rejections carry a reason" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "DELETE FROM t USING u WHERE t.a = u.a;", .needle = "USING" },
        .{ .sql = "WITH x AS (SELECT 1) DELETE FROM t;", .needle = "WITH" },
        .{
            .sql = "DELETE FROM t WHERE id IN (SELECT id FROM u);",
            .needle = "WHERE",
        },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        const found = std.mem.indexOf(u8, stmt.invalid, case.needle);
        try testing.expect(found != null);
    }
}

test "sql.parse: UPDATE parses table, SET list, and the optional filter" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    // No WHERE: update every row. Multiple SETs, mixed literal kinds.
    const up1 = try parse.statement(
        alloc,
        "UPDATE t SET a = 5, b = 'x', c = NULL;",
    );
    try testing.expectEqualStrings("t", up1.update.table);
    try testing.expect(up1.update.where == null);
    try testing.expectEqual(3, up1.update.sets.len);
    try testing.expectEqualStrings("a", up1.update.sets[0].column);
    try testing.expectEqual(@as(i64, 5), up1.update.sets[0].value.literal.int);
    try testing.expectEqualStrings("b", up1.update.sets[1].column);
    try testing.expectEqualStrings("x", up1.update.sets[1].value.literal.str);
    try testing.expectEqualStrings("c", up1.update.sets[2].column);
    try testing.expect(up1.update.sets[2].value.literal == .null_value);

    // WHERE on a payload column; SET int 0 exercises libpg_query's
    // omitted-zero encoding ("ival":{} still means 0).
    const up2 = try parse.statement(alloc, "UPDATE t SET a = 0 WHERE id = 7;");
    try testing.expectEqual(@as(i64, 0), up2.update.sets[0].value.literal.int);
    const f2 = up2.update.where.?.cmp;
    try testing.expectEqualStrings("id", f2.lhs.column);
    try testing.expectEqual(@as(i64, 7), f2.rhs.literal.int);

    // WHERE on the egid pseudo-column parses as a plain column filter.
    const up3 = try parse.statement(alloc, "UPDATE t SET a = 1 WHERE egid = 3;");
    try testing.expectEqualStrings("egid", up3.update.where.?.cmp.lhs.column);
}

/// The first SET value of an UPDATE that must parse.
fn setValue(arloc: std.mem.Allocator, text: String) !parse.Expr {
    const stmt = try parse.statement(arloc, text);
    return stmt.update.sets[0].value;
}

test "sql.parse: SET expressions keep the grammar's operator tree" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const arloc = arena.allocator();
    const Op = parse.Expr.Op;

    // `a + 1`: an operator over a column and a literal.
    const s1 = try setValue(arloc, "UPDATE t SET a = a + 1;");
    try testing.expectEqual(Op.add, s1.binop.op);
    try testing.expectEqualStrings("a", s1.binop.lhs.column);
    try testing.expectEqual(@as(i64, 1), s1.binop.rhs.literal.int);

    // Precedence and parentheses are the tree's shape: `a * 2 + 1` is
    // add(mul(a, 2), 1); `(a + 1) * 2` is mul(add(a, 1), 2).
    const s2 = try setValue(arloc, "UPDATE t SET a = a * 2 + 1;");
    try testing.expectEqual(Op.add, s2.binop.op);
    try testing.expectEqual(Op.mul, s2.binop.lhs.binop.op);
    try testing.expectEqualStrings("a", s2.binop.lhs.binop.lhs.column);
    try testing.expectEqual(@as(i64, 2), s2.binop.lhs.binop.rhs.literal.int);
    try testing.expectEqual(@as(i64, 1), s2.binop.rhs.literal.int);
    const s3 = try setValue(arloc, "UPDATE t SET a = (a + 1) * 2;");
    try testing.expectEqual(Op.mul, s3.binop.op);
    try testing.expectEqual(Op.add, s3.binop.lhs.binop.op);
    try testing.expectEqual(@as(i64, 2), s3.binop.rhs.literal.int);

    // Unary minus on a column is `0 - a`; on a number the grammar
    // folds it into the literal, even behind another operator. `+a`
    // is `a`.
    const s4 = try setValue(arloc, "UPDATE t SET a = -a;");
    try testing.expectEqual(Op.sub, s4.binop.op);
    try testing.expectEqual(@as(i64, 0), s4.binop.lhs.literal.int);
    try testing.expectEqualStrings("a", s4.binop.rhs.column);
    const s5 = try setValue(arloc, "UPDATE t SET a = -1;");
    try testing.expectEqual(@as(i64, -1), s5.literal.int);
    const s6 = try setValue(arloc, "UPDATE t SET a = a - -1;");
    try testing.expectEqual(Op.sub, s6.binop.op);
    try testing.expectEqual(@as(i64, -1), s6.binop.rhs.literal.int);
    const s7 = try setValue(arloc, "UPDATE t SET a = +a;");
    try testing.expectEqualStrings("a", s7.column);

    // A bare column; `/` and `%` at one precedence level, left to
    // right; a float literal stays text.
    const s8 = try setValue(arloc, "UPDATE t SET a = b;");
    try testing.expectEqualStrings("b", s8.column);
    const s9 = try setValue(arloc, "UPDATE t SET a = a / 2 % 1.5;");
    try testing.expectEqual(Op.mod, s9.binop.op);
    try testing.expectEqual(Op.div, s9.binop.lhs.binop.op);
    try testing.expectEqualStrings("1.5", s9.binop.rhs.literal.float);

    // `||` is left-associative: `'p' || b || 's'` is
    // concat(concat('p', b), 's').
    const s10 = try setValue(arloc, "UPDATE t SET b = 'p' || b || 's';");
    try testing.expectEqual(Op.concat, s10.binop.op);
    try testing.expectEqualStrings("s", s10.binop.rhs.literal.str);
    const inner = s10.binop.lhs.binop;
    try testing.expectEqual(Op.concat, inner.op);
    try testing.expectEqualStrings("p", inner.lhs.literal.str);
    try testing.expectEqualStrings("b", inner.rhs.column);
}

test "sql.parse: transaction statements parse; two-phase commit is rejected" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    // Plain BEGIN: libpg_query omits `kind` (protobuf tag 0).
    const b = try parse.statement(alloc, "BEGIN;");
    try testing.expect(b == .begin_txn);

    const st = try parse.statement(alloc, "START TRANSACTION;");
    try testing.expect(st == .begin_txn);

    const c = try parse.statement(alloc, "COMMIT;");
    try testing.expect(c == .commit_txn);

    const r = try parse.statement(alloc, "ROLLBACK;");
    try testing.expect(r == .rollback_txn);

    const sp = try parse.statement(alloc, "PREPARE TRANSACTION 'gid';");
    try testing.expect(sp == .invalid);
}

test "sql.parse: UPDATE rejections carry a reason" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "UPDATE t SET a = 1 FROM u;", .needle = "FROM" },
        .{ .sql = "WITH x AS (SELECT 1) UPDATE t SET a = 1;", .needle = "WITH" },
        .{ .sql = "UPDATE t SET a = DEFAULT;", .needle = "an expression" },
        .{ .sql = "UPDATE t SET a = now();", .needle = "not supported" },
        .{ .sql = "UPDATE t SET a = (SELECT 1);", .needle = "an expression" },
        .{ .sql = "UPDATE t SET (a, b) = (1, 2);", .needle = "an expression" },
        .{ .sql = "UPDATE t SET a = a AND b;", .needle = "an expression" },
        .{ .sql = "UPDATE t SET a = a ^ 2;", .needle = "supported" },
        .{ .sql = "UPDATE t SET a = a + 1, a = 2;", .needle = "duplicate" },
        .{ .sql = "UPDATE t SET a = 1 WHERE b > (SELECT 1);", .needle = "an expression" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        const found = std.mem.indexOf(u8, stmt.invalid, case.needle);
        try testing.expect(found != null);
    }
}

test "sql.parse: $n placeholders parse in every literal position" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    // INSERT VALUES cells, one placeholder used twice.
    const ins = try parse.statement(
        alloc,
        "INSERT INTO t (a, b) VALUES ($1, $2), ($2, 'x');",
    );
    const rows = ins.insert.rows;
    try testing.expectEqual(@as(u16, 1), rows[0][0].param);
    try testing.expectEqual(@as(u16, 2), rows[0][1].param);
    try testing.expectEqual(@as(u16, 2), rows[1][0].param);
    try testing.expectEqualStrings("x", rows[1][1].str);
    try testing.expectEqual(@as(u16, 2), ins.insert.param_count);
    try testing.expectEqual(@as(u16, 2), ins.paramCount());

    // WHERE leaves, anywhere in the tree.
    const sel = try parse.statement(
        alloc,
        "SELECT * FROM t WHERE a = $1 AND (b = $3 OR NOT c = $2);",
    );
    const top = sel.select.where.?.and_;
    try testing.expectEqual(@as(u16, 1), top[0].cmp.rhs.literal.param);
    try testing.expectEqual(@as(u16, 3), top[1].or_[0].cmp.rhs.literal.param);
    try testing.expectEqual(@as(u16, 2), top[1].or_[1].not.cmp.rhs.literal.param);
    try testing.expectEqual(@as(u16, 3), sel.select.param_count);

    const del = try parse.statement(alloc, "DELETE FROM t WHERE a = $1;");
    try testing.expectEqual(@as(u16, 1), del.delete.where.?.cmp.rhs.literal.param);
    try testing.expectEqual(@as(u16, 1), del.delete.param_count);

    // SET: at the root, under an operator, and in the WHERE.
    const up = try parse.statement(
        alloc,
        "UPDATE t SET a = a + $1, b = $2 WHERE c = $3;",
    );
    const sets = up.update.sets;
    try testing.expectEqual(@as(u16, 1), sets[0].value.binop.rhs.literal.param);
    try testing.expectEqual(@as(u16, 2), sets[1].value.literal.param);
    try testing.expectEqual(@as(u16, 3), up.update.where.?.cmp.rhs.literal.param);
    try testing.expectEqual(@as(u16, 3), up.update.param_count);

    // The count is the highest index, whether or not the lower ones
    // are used; a statement without placeholders needs none.
    const gap = try parse.statement(alloc, "SELECT * FROM t WHERE a = $2;");
    try testing.expectEqual(@as(u16, 2), gap.select.param_count);
    const none = try parse.statement(alloc, "SELECT * FROM t WHERE a = 1;");
    try testing.expectEqual(@as(u16, 0), none.paramCount());
    const ddl = try parse.statement(alloc, "CREATE TABLE t (a int);");
    try testing.expectEqual(@as(u16, 0), ddl.paramCount());

    // `$0` and an index above the cap are rejected with a reason.
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "SELECT * FROM t WHERE a = $0;", .needle = "$0" },
        .{ .sql = "INSERT INTO t VALUES ($70000);", .needle = "65535" },
        .{ .sql = "UPDATE t SET a = $0;", .needle = "$0" },
        .{ .sql = "UPDATE t SET a = a + $0;", .needle = "$0" },
        .{ .sql = "DELETE FROM t WHERE a = $65536;", .needle = "65535" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        const found = std.mem.indexOf(u8, stmt.invalid, case.needle);
        try testing.expect(found != null);
    }
}

/// A column of `ty` for the Params cases; `n` is a string length.
fn paramCol(ty: sql.Type, n: ?u32) sql.table.Column {
    return .{
        .column_id = 1,
        .pos = 0,
        .name = "c",
        .ty = ty,
        .pkey_position = null,
        .type_mod = n,
    };
}

/// `$index` of `params` as a cell of a `ty` column.
fn bindParam(
    params: pgwire.Params,
    index: u16,
    ty: sql.Type,
    n: ?u32,
) !sql.Value {
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const col = paramCol(ty, n);
    return parse.litToValue(fba.allocator(), .{ .param = index }, &col, params);
}

test "sql.parse: Params bind $n text and binary values to a column" {
    const Value = sql.Value;
    const expectEqual = testing.expectEqual;
    const expectError = testing.expectError;

    // Text values take the literal path: column-typed parsing and the
    // string rules (char(n) stripped and length-checked); NULL is NULL.
    const text = pgwire.Params{
        .values = &.{ "42", "hi ", null, "2000-02-29", "abcd", "x" },
        .wirefmts = &.{ .text, .text, .text, .text, .text, .text },
    };
    try expectEqual(Value{ .int4 = 42 }, try bindParam(text, 1, .int4, null));
    const kept = try bindParam(text, 2, .text, null);
    try testing.expectEqualStrings("hi ", kept.text);
    const stripped = try bindParam(text, 2, .bpchar, 3);
    try testing.expectEqualStrings("hi", stripped.text);
    try testing.expect(try bindParam(text, 3, .int4, null) == .null_value);
    const date = try bindParam(text, 4, .date, null);
    try expectEqual(Value{ .date = 11016 }, date);
    try expectError(error.StringTooLong, bindParam(text, 5, .bpchar, 3));
    try expectError(error.InvalidCharacter, bindParam(text, 6, .int4, null));
    // Past what Bind supplied.
    try expectError(error.UnboundParam, bindParam(text, 7, .int4, null));
    // A plain literal is unaffected by the params in hand.
    const col = paramCol(.int4, null);
    var buf: [64]u8 = undefined;
    var fba = std.heap.FixedBufferAllocator.init(&buf);
    const a = fba.allocator();
    const lit = try parse.litToValue(a, .{ .int = 7 }, &col, text);
    try expectEqual(Value{ .int4 = 7 }, lit);

    // Binary values decode by the type the client declared, then
    // take the column's type: an int4 into an int4 column as is, an
    // int8 into an int4 column range-checked, an int4 into a float8
    // column as a float, a declared text into any column parsed as a
    // text-format value is. A float into an integer column is refused
    // rather than rounded; an undeclared type has no decoder (0A000).
    var f8_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &f8_bytes, @bitCast(@as(f64, 1.5)), .big);
    const minus_two = [_]u8{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe };
    const big = [_]u8{ 0, 0, 0, 1, 0, 0, 0, 0 };
    const oid = pgwire.oid;
    const binary = pgwire.Params{
        .values = &.{
            &.{ 0, 0, 0, 42 }, &.{1}, &minus_two, &f8_bytes, "hi", &.{ 0, 42 },
            &big,              "12",
        },
        .wirefmts = &([_]pgwire.WireFormat{.binary} ** 8),
        .oids = &.{
            oid.int4, oid.bool_,   oid.int8, oid.float8, oid.text, oid.int2,
            oid.int8, oid.varchar,
        },
    };
    try expectEqual(Value{ .int4 = 42 }, try bindParam(binary, 1, .int4, null));
    const flag = try bindParam(binary, 2, .bool_, null);
    try expectEqual(Value{ .bool_ = true }, flag);
    try expectEqual(Value{ .int8 = -2 }, try bindParam(binary, 3, .int8, null));
    try expectEqual(Value{ .int4 = -2 }, try bindParam(binary, 3, .int4, null));
    const widened = try bindParam(binary, 3, .float8, null);
    try expectEqual(Value{ .float8 = -2.0 }, widened);
    try expectError(error.IntOutOfRange, bindParam(binary, 3, .uint4, null));
    const float = try bindParam(binary, 4, .float8, null);
    try expectEqual(Value{ .float8 = 1.5 }, float);
    const narrowed = try bindParam(binary, 4, .float4, null);
    try expectEqual(Value{ .float4 = 1.5 }, narrowed);
    try expectError(error.TypeMismatch, bindParam(binary, 4, .int8, null));
    try expectError(error.TypeMismatch, bindParam(binary, 2, .int4, null));
    const str = try bindParam(binary, 5, .text, null);
    try testing.expectEqualStrings("hi", str.text);
    try expectEqual(Value{ .int8 = 42 }, try bindParam(binary, 6, .int8, null));
    try expectError(error.IntOutOfRange, bindParam(binary, 7, .int4, null));
    try expectEqual(Value{ .int4 = 12 }, try bindParam(binary, 8, .int4, null));
    // The wrong width for the declared type.
    const short = pgwire.Params{
        .values = &.{ &.{ 0, 42 }, &.{ 0, 0, 0, 1 } },
        .wirefmts = &.{ .binary, .binary },
        .oids = &.{oid.int4},
    };
    try expectError(error.BadLength, bindParam(short, 1, .int4, null));
    // No declared type: the bytes cannot be read.
    try expectError(error.UnsupportedType, bindParam(short, 2, .int4, null));

    // A per-param format list decides per value.
    const mixed = pgwire.Params{
        .values = &.{ "7", &.{ 0, 0, 0, 8 } },
        .wirefmts = &.{ .text, .binary },
        .oids = &.{ oid.int4, oid.int4 },
    };
    try expectEqual(Value{ .int4 = 7 }, try bindParam(mixed, 1, .int4, null));
    try expectEqual(Value{ .int4 = 8 }, try bindParam(mixed, 2, .int4, null));
}

// ---------------------------------------------------------------------------
// Parse cost. The load's statement is a 200-row INSERT with 1 KB text
// payloads; the point read is the other shape the server sees most.
// ---------------------------------------------------------------------------

const fiber = @import("zfiber");

fn bigInsertSql(alloc: std.mem.Allocator) !String {
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(alloc, "INSERT INTO big (id, n, payload) VALUES\n");
    var payload: [1000]u8 = undefined;
    for (&payload, 0..) |*b, i| b.* = 'A' + @as(u8, @intCast(i % 26));
    for (0..200) |i| {
        if (i > 0) try out.appendSlice(alloc, ",\n");
        try out.print(alloc, "({d}, 0, '{s}')", .{ i + 1, payload });
    }
    return out.toOwnedSlice(alloc);
}

fn benchParse(b: *fiber.bench.Bench, sql_text: String) !void {
    var arena = std.heap.ArenaAllocator.init(b.alloc);
    defer arena.deinit();
    b.resetTimer();
    var i: u64 = 0;
    while (i < b.n) : (i += 1) {
        _ = arena.reset(.retain_capacity);
        const stmt = try parse.statement(arena.allocator(), sql_text);
        std.mem.doNotOptimizeAway(stmt);
    }
}

test "BENCH: parse 200-row insert, 1 KB payloads" {
    const b = fiber.bench.current();
    const sql_text = try bigInsertSql(b.alloc);
    defer b.alloc.free(sql_text);
    try benchParse(b, sql_text);
}

test "BENCH: parse point select" {
    const b = fiber.bench.current();
    try benchParse(b, "SELECT id, n, payload FROM big WHERE id = 12345");
}

test "BENCH: parse update" {
    const b = fiber.bench.current();
    try benchParse(b, "UPDATE big SET n = n + 1 WHERE id = 12345");
}

test "sql.parse: WHERE comparisons carry their operator, the column left" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const cases = [_]struct { sql: String, op: parse.CmpOp, int: i64 }{
        .{ .sql = "SELECT * FROM t WHERE a < 5;", .op = .lt, .int = 5 },
        .{ .sql = "SELECT * FROM t WHERE a <= 5;", .op = .le, .int = 5 },
        .{ .sql = "SELECT * FROM t WHERE a > 5;", .op = .gt, .int = 5 },
        .{ .sql = "SELECT * FROM t WHERE a >= 5;", .op = .ge, .int = 5 },
        .{ .sql = "SELECT * FROM t WHERE a <> 5;", .op = .ne, .int = 5 },
        .{ .sql = "SELECT * FROM t WHERE a != 5;", .op = .ne, .int = 5 },
        .{ .sql = "SELECT * FROM t WHERE a = 5;", .op = .eq, .int = 5 },
        // A literal on the left turns the operator around.
        .{ .sql = "SELECT * FROM t WHERE 5 < a;", .op = .gt, .int = 5 },
        .{ .sql = "SELECT * FROM t WHERE 5 >= a;", .op = .le, .int = 5 },
        .{ .sql = "SELECT * FROM t WHERE 5 = a;", .op = .eq, .int = 5 },
        .{ .sql = "DELETE FROM t WHERE a > 7;", .op = .gt, .int = 7 },
        .{ .sql = "UPDATE t SET b = 1 WHERE 2 <> a;", .op = .ne, .int = 2 },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        const where = switch (stmt) {
            .select => |s| s.where.?,
            .delete => |d| d.where.?,
            .update => |u| u.where.?,
            else => unreachable,
        };
        try testing.expectEqualStrings("a", where.cmp.lhs.column);
        try testing.expectEqual(case.op, where.cmp.op);
        try testing.expectEqual(case.int, where.cmp.rhs.literal.int);
    }

    // A parameter on either side, and a comparison under NOT / AND.
    const p = try parse.statement(alloc, "SELECT * FROM t WHERE $1 <= a AND NOT b >= $2;");
    const and2 = p.select.where.?.and_;
    try testing.expectEqual(parse.CmpOp.ge, and2[0].cmp.op);
    try testing.expectEqual(@as(u16, 1), and2[0].cmp.rhs.literal.param);
    try testing.expectEqual(parse.CmpOp.ge, and2[1].not.cmp.op);
    try testing.expectEqual(@as(u16, 2), and2[1].not.cmp.rhs.literal.param);
    try testing.expectEqual(@as(u16, 2), p.select.param_count);
}

test "sql.parse: CONCAT and COALESCE in SET are calls over SET values" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const up = try parse.statement(
        alloc,
        "UPDATE t SET val = CONCAT(val, ',', $1), n = COALESCE(n, 0) + 1 " ++
            "WHERE id = $2;",
    );
    const concat = up.update.sets[0].value.call;
    try testing.expectEqualStrings("concat", concat.func.name);
    try testing.expectEqual(@as(usize, 3), concat.args.len);
    try testing.expectEqualStrings("val", concat.args[0].column);
    try testing.expectEqualStrings(",", concat.args[1].literal.str);
    try testing.expectEqual(@as(u16, 1), concat.args[2].literal.param);
    const plus = up.update.sets[1].value.binop;
    const coalesce = plus.lhs.call;
    try testing.expectEqualStrings("coalesce", coalesce.func.name);
    try testing.expectEqual(@as(usize, 2), coalesce.args.len);
    try testing.expectEqual(@as(i64, 0), coalesce.args[1].literal.int);
    try testing.expectEqual(@as(u16, 2), up.update.param_count);

    // The keyword forms are calls too, through their own grammar nodes.
    const kw = try parse.statement(
        alloc,
        "UPDATE t SET a = GREATEST(a, 1), b = NULLIF(b, 0), c = LEAST(c);",
    );
    const sets = kw.update.sets;
    try testing.expectEqualStrings("greatest", sets[0].value.call.func.name);
    try testing.expectEqualStrings("nullif", sets[1].value.call.func.name);
    try testing.expectEqual(@as(usize, 2), sets[1].value.call.args.len);
    try testing.expectEqualStrings("least", sets[2].value.call.func.name);

    // Any other function, and the aggregate forms, carry a reason.
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "UPDATE t SET a = now();", .needle = "now() is not supported" },
        .{ .sql = "UPDATE t SET a = concat();", .needle = "at least 1 argument" },
        .{ .sql = "UPDATE t SET a = lower();", .needle = "takes 1 argument" },
        .{ .sql = "UPDATE t SET a = substr(a);", .needle = "2 to 3 arguments" },
        .{ .sql = "UPDATE t SET a = left(a,1,2);", .needle = "2 arguments" },
        .{ .sql = "UPDATE t SET a = concat(DISTINCT a);", .needle = "aggregate" },
        .{ .sql = "UPDATE t SET a = concat(a) OVER ();", .needle = "aggregate" },
        .{ .sql = "UPDATE t SET a = concat((SELECT 1));", .needle = "an expression" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
}

test "sql.parse: savepoint statements carry their name" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const sp = try parse.statement(alloc, "SAVEPOINT upsert;");
    try testing.expectEqualStrings("upsert", sp.savepoint);
    const rel = try parse.statement(alloc, "RELEASE SAVEPOINT Upsert;");
    try testing.expectEqualStrings("upsert", rel.release_savepoint);
    const rel2 = try parse.statement(alloc, "release \"Upsert\"");
    try testing.expectEqualStrings("Upsert", rel2.release_savepoint);
    const rb = try parse.statement(alloc, "ROLLBACK TO SAVEPOINT upsert;");
    try testing.expectEqualStrings("upsert", rb.rollback_to);
    const rb2 = try parse.statement(alloc, "rollback to upsert");
    try testing.expectEqualStrings("upsert", rb2.rollback_to);
    try testing.expectEqual(@as(u16, 0), rb2.paramCount());
    const prepared = try parse.statement(alloc, "PREPARE TRANSACTION 'x';");
    try testing.expect(prepared == .invalid);
}

test "sql.parse: RETURNING is a projection on INSERT, UPDATE and DELETE" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const ins = try parse.statement(
        alloc,
        "INSERT INTO t (id, v) VALUES ($1, 'a') RETURNING id, v || '!' AS loud;",
    );
    const targets = ins.insert.returning.?.targets;
    try testing.expectEqual(@as(usize, 2), targets.len);
    try testing.expectEqualStrings("id", targets[0].name);
    try testing.expectEqualStrings("id", targets[0].expr.column);
    try testing.expectEqualStrings("loud", targets[1].name);
    try testing.expect(targets[1].expr == .binop);
    try testing.expectEqual(@as(u16, 1), ins.insert.param_count);

    const up = try parse.statement(
        alloc,
        "UPDATE t SET v = 'b' WHERE id = 1 RETURNING *;",
    );
    try testing.expect(up.update.returning.? == .star);
    const del = try parse.statement(
        alloc,
        "DELETE FROM t WHERE id = $1 RETURNING egid;",
    );
    try testing.expectEqualStrings("egid", del.delete.returning.?.targets[0].name);
    try testing.expectEqual(@as(u16, 1), del.delete.param_count);

    // Without the clause there is nothing to project.
    const plain = try parse.statement(alloc, "DELETE FROM t;");
    try testing.expect(plain.delete.returning == null);
    // Aggregates have no row to summarise.
    const agg = try parse.statement(
        alloc,
        "UPDATE t SET v = 'b' RETURNING count(*);",
    );
    try testing.expect(agg == .invalid);
}

test "sql.parse: INSERT ... ON CONFLICT names the column and the action" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const nothing = try parse.statement(
        alloc,
        "INSERT INTO t (id, v) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING;",
    );
    const n = nothing.insert.on_conflict.?;
    try testing.expectEqualStrings("id", n.column.?);
    try testing.expect(n.action == .nothing);
    const any = try parse.statement(
        alloc,
        "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING;",
    );
    try testing.expect(any.insert.on_conflict.?.column == null);

    // The upstream Jepsen upsert: an alias, EXCLUDED, a WHERE on the
    // existing row, parameters in every position.
    const up = try parse.statement(
        alloc,
        "INSERT INTO t AS x (id, sk, val) VALUES ($1, $2, $3) " ++
            "ON CONFLICT (id) DO UPDATE SET val = CONCAT(x.val, ',', " ++
            "EXCLUDED.val), sk = EXCLUDED.sk WHERE x.sk = $4;",
    );
    const u = up.insert.on_conflict.?;
    try testing.expectEqualStrings("id", u.column.?);
    const sets = u.action.update.sets;
    try testing.expectEqual(@as(usize, 2), sets.len);
    try testing.expectEqualStrings("val", sets[0].column);
    const call = sets[0].value.call;
    try testing.expectEqualStrings("val", call.args[0].column);
    try testing.expectEqualStrings("val", call.args[2].excluded);
    try testing.expectEqualStrings("sk", sets[1].value.excluded);
    const where = u.action.update.where.?.cmp;
    try testing.expectEqualStrings("sk", where.lhs.column);
    try testing.expectEqual(@as(u16, 4), where.rhs.literal.param);
    try testing.expectEqual(@as(u16, 4), up.insert.param_count);

    const cases = [_]struct { sql: String, needle: String }{
        .{
            .sql = "INSERT INTO t VALUES (1) ON CONFLICT ON CONSTRAINT t_pkey DO NOTHING;",
            .needle = "ON CONSTRAINT",
        },
        .{
            .sql = "INSERT INTO t VALUES (1) ON CONFLICT (a, b) DO NOTHING;",
            .needle = "one column",
        },
        .{
            .sql = "INSERT INTO t VALUES (1) ON CONFLICT (lower(a)) DO NOTHING;",
            .needle = "not an expression",
        },
        .{
            .sql = "INSERT INTO t VALUES (1) ON CONFLICT (a) WHERE a > 0 DO NOTHING;",
            .needle = "WHERE on the ON CONFLICT target",
        },
        .{
            .sql = "INSERT INTO t VALUES (1) ON CONFLICT DO UPDATE SET a = 1;",
            .needle = "requires a conflict target",
        },
        .{
            .sql = "INSERT INTO t VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 1 " ++
                "WHERE EXCLUDED.a = 1;",
            .needle = "EXCLUDED is not supported in a WHERE",
        },
        .{
            .sql = "INSERT INTO t VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 1, a = 2;",
            .needle = "duplicate",
        },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
}

test "sql.parse: expressions stand in SELECT targets and on both WHERE sides" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const stmt = try parse.statement(
        alloc,
        "SELECT upper(name) AS u, id + 1, lower(name), 7 FROM t " ++
            "WHERE length(name) > id AND 5 > 6 AND a = b AND nullif(id, 0) IS NULL;",
    );
    const targets = stmt.select.projection.targets;
    try testing.expectEqual(@as(usize, 4), targets.len);
    try testing.expectEqualStrings("u", targets[0].name);
    try testing.expectEqualStrings("upper", targets[0].expr.call.func.name);
    try testing.expectEqualStrings("?column?", targets[1].name);
    try testing.expect(targets[1].expr == .binop);
    try testing.expectEqualStrings("lower", targets[2].name);
    try testing.expectEqualStrings("?column?", targets[3].name);
    try testing.expectEqual(@as(i64, 7), targets[3].expr.literal.int);

    const terms = stmt.select.where.?.and_;
    try testing.expectEqual(@as(usize, 4), terms.len);
    // The bare column went left and the operator turned with it.
    try testing.expectEqualStrings("id", terms[0].cmp.lhs.column);
    try testing.expectEqual(parse.CmpOp.lt, terms[0].cmp.op);
    try testing.expectEqualStrings("length", terms[0].cmp.rhs.call.func.name);
    // Two constants, two columns: nothing to turn.
    try testing.expectEqual(@as(i64, 5), terms[1].cmp.lhs.literal.int);
    try testing.expectEqual(parse.CmpOp.gt, terms[1].cmp.op);
    try testing.expectEqualStrings("a", terms[2].cmp.lhs.column);
    try testing.expectEqualStrings("b", terms[2].cmp.rhs.column);
    try testing.expectEqualStrings("nullif", terms[3].is_null.expr.call.func.name);
    try testing.expect(!terms[3].is_null.negate);
}

fn repeat(alloc: std.mem.Allocator, piece: String, n: usize) ![]u8 {
    const out = try alloc.alloc(u8, piece.len * n);
    for (0..n) |i| @memcpy(out[i * piece.len ..][0..piece.len], piece);
    return out;
}

test "sql.parse: a WHERE or expression nested past the budget is refused" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    // Far past the parse stack budget whatever the build: a level
    // costs up to 5 KiB in Debug and 1 KiB in ReleaseSafe, so the
    // budget is 50 to 400 levels. The rejection comes from the budget,
    // not the guard page, and either frame may be the one that
    // crosses it, so both rejections carry the same message.
    const deep = 2000;
    const opens = try repeat(alloc, "(1 + ", deep);
    const closes = try repeat(alloc, ")", deep);
    const cases = [_]struct { sql: String, needle: String }{
        .{
            .sql = try std.mem.concat(alloc, u8, &.{
                "SELECT id FROM t WHERE ", try repeat(alloc, "NOT ", deep),
                "id = 1",
            }),
            .needle = "statement nested too deeply",
        },
        .{
            .sql = try std.mem.concat(alloc, u8, &.{
                "SELECT id FROM t WHERE ",
                try repeat(alloc, "(id = 1 AND ", deep),
                "id = 1",
                closes,
            }),
            .needle = "statement nested too deeply",
        },
        .{
            .sql = try std.mem.concat(alloc, u8, &.{
                "SELECT id FROM t WHERE id = ", opens, "1", closes,
            }),
            .needle = "statement nested too deeply",
        },
        .{
            .sql = try std.mem.concat(alloc, u8, &.{
                "UPDATE t SET v = ", opens, "1", closes,
            }),
            .needle = "statement nested too deeply",
        },
        .{
            .sql = try std.mem.concat(alloc, u8, &.{
                "SELECT ", try repeat(alloc, "coalesce(", deep), "1", closes,
                " FROM t",
            }),
            .needle = "statement nested too deeply",
        },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        const msg = stmt.invalid;
        try testing.expect(std.mem.indexOf(u8, msg, case.needle) != null);
    }

    // Under the budget in every build, nesting is a query like any
    // other.
    const fine = 32;
    const nots = try std.mem.concat(alloc, u8, &.{
        "SELECT id FROM t WHERE ", try repeat(alloc, "NOT ", fine), "id = 1",
    });
    try testing.expect((try parse.statement(alloc, nots)) == .select);
    const sums = try std.mem.concat(alloc, u8, &.{
        "UPDATE t SET v = ", try repeat(alloc, "(1 + ", fine),
        "1",                 try repeat(alloc, ")", fine),
    });
    try testing.expect((try parse.statement(alloc, sums)) == .update);
}
