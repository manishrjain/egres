// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
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

test "sql.parse: CREATE TABLE vector(n) carries and bounds its dimension" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();

    const ct = try parse.statement(alloc, "CREATE TABLE t (v vector(768));");
    try testing.expectEqual(sql.Type.vector, ct.create_table.columns[0].ty);
    try testing.expectEqual(@as(?u32, 768), ct.create_table.columns[0].type_mod);

    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "CREATE TABLE t (v vector);", .needle = "requires a dimension" },
        .{ .sql = "CREATE TABLE t (v vector(0));", .needle = "at least 1" },
        .{ .sql = "CREATE TABLE t (v vector(4097));", .needle = "cannot exceed" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
    }
}

test "sql.parse: CREATE TABLE semantic rejections carry a reason" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "CREATE TABLE t (a int NOT NULL);", .needle = "and UNIQUE" },
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
        .{ .sql = "INSERT INTO t VALUES (1) ON CONFLICT DO NOTHING;", .needle = "ON CONFLICT" },
        .{ .sql = "INSERT INTO t VALUES (1) RETURNING id;", .needle = "RETURNING" },
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
    try testing.expect(s2.select.projection == .columns);
    try testing.expectEqual(@as(usize, 2), s2.select.projection.columns.len);
    try testing.expectEqualStrings("egid", s2.select.projection.columns[0]);
    try testing.expectEqualStrings("name", s2.select.projection.columns[1]);
    const f = s2.select.where.?.eq;
    try testing.expectEqualStrings("id", f.column);
    try testing.expectEqual(@as(i64, 5), f.literal.int);

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
    try testing.expectEqualStrings("id", c2.select.where.?.eq.column);
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
    try testing.expectEqualStrings("a", and3[0].eq.column);
    try testing.expectEqualStrings("b", and3[1].eq.column);
    try testing.expectEqualStrings("c", and3[2].eq.column);
    try testing.expectEqual(@as(i64, 3), and3[2].eq.literal.int);

    // Nesting, parentheses, NOT — shared by DELETE.
    const d = try parse.statement(alloc, "DELETE FROM t WHERE a = 1 OR (b = 2 AND NOT c = 'x');");
    const or2 = d.delete.where.?.or_;
    try testing.expectEqual(@as(usize, 2), or2.len);
    try testing.expectEqualStrings("a", or2[0].eq.column);
    const and2 = or2[1].and_;
    try testing.expectEqual(@as(usize, 2), and2.len);
    try testing.expectEqualStrings("b", and2[0].eq.column);
    const inner = and2[1].not.*;
    try testing.expectEqualStrings("c", inner.eq.column);
    try testing.expectEqualStrings("x", inner.eq.literal.str);

    // SQL precedence: AND binds tighter than OR.
    const p = try parse.statement(alloc, "SELECT * FROM t WHERE a = 1 OR b = 2 AND c = 3;");
    const top = p.select.where.?.or_;
    try testing.expectEqual(@as(usize, 2), top.len);
    try testing.expect(top[0] == .eq);
    try testing.expectEqual(@as(usize, 2), top[1].and_.len);

    // IS NULL / IS NOT NULL — shared by UPDATE.
    const upd1 = try parse.statement(alloc, "UPDATE t SET a = 1 WHERE b IS NULL;");
    try testing.expectEqualStrings("b", upd1.update.where.?.is_null.column);
    try testing.expect(!upd1.update.where.?.is_null.negate);
    const upd2 = try parse.statement(alloc, "UPDATE t SET a = 1 WHERE b IS NOT NULL AND c = 1;");
    const upd2_and = upd2.update.where.?.and_;
    try testing.expect(upd2_and[0].is_null.negate);
    try testing.expectEqualStrings("c", upd2_and[1].eq.column);

    // Rejections anywhere in the tree surface the leaf's reason.
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "SELECT * FROM t WHERE a = 1 AND b > 2;", .needle = "column = value" },
        .{ .sql = "SELECT * FROM t WHERE a = 1 OR b;", .needle = "unsupported WHERE" },
        .{ .sql = "SELECT * FROM t WHERE NOT b;", .needle = "unsupported WHERE" },
        .{ .sql = "SELECT * FROM t WHERE a IS DISTINCT FROM 1;", .needle = "column = value" },
        .{ .sql = "SELECT * FROM t WHERE (a = 1) IS NULL;", .needle = "IS NULL operand" },
        .{ .sql = "SELECT * FROM t WHERE a = 1 AND b = c;", .needle = "literal" },
    };
    for (cases) |case| {
        const stmt = try parse.statement(alloc, case.sql);
        try testing.expect(stmt == .invalid);
        try testing.expect(std.mem.indexOf(u8, stmt.invalid, case.needle) != null);
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
        .{ .sql = "SELECT * FROM t ORDER BY a;", .needle = "ORDER BY" },
        .{ .sql = "SELECT * FROM t LIMIT 3 OFFSET 1;", .needle = "OFFSET" },
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
        .{ .sql = "SELECT * FROM t WHERE a > 5;", .needle = "column = value" },
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
    const f2 = d2.delete.where.?.eq;
    try testing.expectEqualStrings("id", f2.column);
    try testing.expectEqual(@as(i64, 5), f2.literal.int);

    // WHERE on the egid pseudo-column parses as a plain column filter.
    const d3 = try parse.statement(alloc, "DELETE FROM t WHERE egid = 3;");
    const f3 = d3.delete.where.?.eq;
    try testing.expectEqualStrings("egid", f3.column);
    try testing.expectEqual(@as(i64, 3), f3.literal.int);

    // Qualified name keeps the last part; string literal.
    const d4 = try parse.statement(alloc, "DELETE FROM public.t WHERE name = 'x';");
    try testing.expectEqualStrings("t", d4.delete.table);
    const f4 = d4.delete.where.?.eq;
    try testing.expectEqualStrings("name", f4.column);
    try testing.expectEqualStrings("x", f4.literal.str);
}

test "sql.parse: DELETE rejections carry a reason" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "DELETE FROM t USING u WHERE t.a = u.a;", .needle = "USING" },
        .{ .sql = "DELETE FROM t RETURNING id;", .needle = "RETURNING" },
        .{ .sql = "WITH x AS (SELECT 1) DELETE FROM t;", .needle = "WITH" },
        .{ .sql = "DELETE FROM t WHERE a > 5;", .needle = "column = value" },
        .{
            .sql = "DELETE FROM t WHERE id IN (SELECT id FROM u);",
            .needle = "WHERE",
        },
        .{ .sql = "DELETE FROM t WHERE a = b;", .needle = "literal" },
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
    const f2 = up2.update.where.?.eq;
    try testing.expectEqualStrings("id", f2.column);
    try testing.expectEqual(@as(i64, 7), f2.literal.int);

    // WHERE on the egid pseudo-column parses as a plain column filter.
    const up3 = try parse.statement(alloc, "UPDATE t SET a = 1 WHERE egid = 3;");
    try testing.expectEqualStrings("egid", up3.update.where.?.eq.column);
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

test "sql.parse: transaction statements parse; savepoints are rejected" {
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

    const sp = try parse.statement(alloc, "SAVEPOINT s1;");
    try testing.expect(sp == .invalid);
}

test "sql.parse: UPDATE rejections carry a reason" {
    const gpa = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const alloc = arena.allocator();
    const cases = [_]struct { sql: String, needle: String }{
        .{ .sql = "UPDATE t SET a = 1 FROM u;", .needle = "FROM" },
        .{ .sql = "UPDATE t SET a = 1 RETURNING id;", .needle = "RETURNING" },
        .{ .sql = "WITH x AS (SELECT 1) UPDATE t SET a = 1;", .needle = "WITH" },
        .{ .sql = "UPDATE t SET a = DEFAULT;", .needle = "SET value" },
        .{ .sql = "UPDATE t SET a = now();", .needle = "SET value" },
        .{ .sql = "UPDATE t SET a = '1'::int;", .needle = "SET value" },
        .{ .sql = "UPDATE t SET (a, b) = (1, 2);", .needle = "SET value" },
        .{ .sql = "UPDATE t SET a = a AND b;", .needle = "SET value" },
        .{ .sql = "UPDATE t SET a = a ^ 2;", .needle = "supported" },
        .{ .sql = "UPDATE t SET a = a + 1, a = 2;", .needle = "duplicate" },
        .{ .sql = "UPDATE t SET a = 1 WHERE b > 5;", .needle = "column = value" },
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
    try testing.expectEqual(@as(u16, 1), top[0].eq.literal.param);
    try testing.expectEqual(@as(u16, 3), top[1].or_[0].eq.literal.param);
    try testing.expectEqual(@as(u16, 2), top[1].or_[1].not.eq.literal.param);
    try testing.expectEqual(@as(u16, 3), sel.select.param_count);

    const del = try parse.statement(alloc, "DELETE FROM t WHERE a = $1;");
    try testing.expectEqual(@as(u16, 1), del.delete.where.?.eq.literal.param);
    try testing.expectEqual(@as(u16, 1), del.delete.param_count);

    // SET: at the root, under an operator, and in the WHERE.
    const up = try parse.statement(
        alloc,
        "UPDATE t SET a = a + $1, b = $2 WHERE c = $3;",
    );
    const sets = up.update.sets;
    try testing.expectEqual(@as(u16, 1), sets[0].value.binop.rhs.literal.param);
    try testing.expectEqual(@as(u16, 2), sets[1].value.literal.param);
    try testing.expectEqual(@as(u16, 3), up.update.where.?.eq.literal.param);
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
    params: parse.Params,
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
    const text = parse.Params{
        .values = &.{ "42", "hi ", null, "2000-02-29", "abcd", "x" },
        .formats = &.{ 0, 0, 0, 0, 0, 0 },
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

    // Binary values decode through pgwire.binary for the six
    // primitives; any other column type has no decoder (0A000).
    var f8_bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &f8_bytes, @bitCast(@as(f64, 1.5)), .big);
    const minus_two = [_]u8{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe };
    const binary = parse.Params{
        .values = &.{
            &.{ 0, 0, 0, 42 }, &.{1}, &minus_two, &f8_bytes, "hi", &.{ 0, 42 },
        },
        .formats = &.{ 1, 1, 1, 1, 1, 1 },
    };
    try expectEqual(Value{ .int4 = 42 }, try bindParam(binary, 1, .int4, null));
    const flag = try bindParam(binary, 2, .bool_, null);
    try expectEqual(Value{ .bool_ = true }, flag);
    try expectEqual(Value{ .int8 = -2 }, try bindParam(binary, 3, .int8, null));
    const float = try bindParam(binary, 4, .float8, null);
    try expectEqual(Value{ .float8 = 1.5 }, float);
    try expectError(error.UnsupportedType, bindParam(binary, 5, .text, null));
    // The wrong width for the column's type.
    try expectError(error.BadLength, bindParam(binary, 6, .int4, null));

    // A per-param format list decides per value.
    const mixed = parse.Params{
        .values = &.{ "7", &.{ 0, 0, 0, 8 } },
        .formats = &.{ 0, 1 },
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
