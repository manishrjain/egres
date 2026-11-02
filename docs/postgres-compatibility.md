---
title: Postgres compatibility
description: A checklist of the SQL and protocol features Egres supports today, and the ones not yet.
sidebar:
  order: 3
---

Egres speaks the PostgreSQL wire protocol and parses SQL with Postgres's own grammar (libpg_query), so every statement Postgres accepts parses. This page is a checklist of what executes in the current release. "Not yet" means exactly that: the list grows with each release.

:::note[About this page]
This page is generated and maintained with AI, read from the Egres source at each release rather than written by hand. If something here does not match what the server does, [open an issue](https://github.com/manishrjain/egres/issues).
:::

:::note[What a "not yet" looks like to a client]
A statement Egres recognises but does not run yet returns a Postgres error with SQLSTATE `0A000` (feature not supported) and a one-line reason, such as `only count(*) is supported in SELECT`. A statement kind it does not recognise returns `unsupported statement`, also `0A000`. Syntax errors return `42601`.
:::

## Protocol and clients

| Feature | Status | Notes |
|---|---|---|
| Simple query protocol | Yes | One statement per message. A message with several statements separated by `;` runs the first one only. |
| Extended query protocol (`Parse`, `Bind`, `Describe`, `Execute`, `Close`, `Sync`, `Flush`) | Yes | Each session caches its parsed statements by SQL text. A statement's `Describe` types a `$n` in a target by what `Parse` declared; undeclared, it takes its operand's type, or text on its own. `ParameterDescription` echoes the declared types and infers none. |
| `$1`-style parameters | Yes | In `WHERE`, `SET` and `VALUES`, up to `$65535`. |
| Parameters in text format | Yes | Any column type. |
| Parameters in binary format | Partial | Decoded by the type declared at `Parse`: `bool`, `int2`, `int4`, `int8`, `float4`, `float8`, `bytea`, `uuid`, `text`, `varchar`, `char`, `date`, `timestamp`, `timestamptz`. An integer assigns to any numeric column, range-checked; a float to a float column; a float into an integer column is `42804`. A binary value with no declared type is `0A000`. |
| Parameter type inference at `Parse` | Not&nbsp;yet | The server echoes the OIDs the client sent. |
| Result columns in text format | Yes | |
| Result columns in binary format | Yes | Every type a column is advertised as: the integers, floats and `bool`, `bytea`, the string types, `date`, `timestamp`, `timestamptz`, and `numeric` for `uint8`. A column advertised as `text` (`time`, `interval`, `uuid` today) sends its text in either format. One code for every column or one per column, as the protocol allows. |
| Portal row limits (`Execute` with a row count) | Partial | A result within the limit completes; a larger one returns `PortalSuspended` after the limit, and the next `Execute` on that portal is `0A000`. Covers JDBC's one-row `Execute` for statements it expects no rows from. |
| Empty query | Yes | `EmptyQueryResponse`, as in Postgres. |
| TLS | Not&nbsp;yet | `SSLRequest` is answered with `N`; clients with `sslmode=prefer` or `disable` continue over plain TCP. |
| Authentication | Not&nbsp;yet | Every connection is accepted. |
| `CancelRequest` | Not&nbsp;yet | The request is accepted and ignored. |
| `pg_catalog`, `information_schema` | Not&nbsp;yet | `psql`'s backslash commands (`\d`, `\dt`, `\l`) depend on them. |
| `COPY` | Not&nbsp;yet | |
| `LISTEN` / `NOTIFY` | Not&nbsp;yet | |
| `DEALLOCATE name`, `DEALLOCATE ALL` | Yes | Drops the session's prepared statements, as the protocol's `Close` does; psycopg sends `DEALLOCATE ALL` after every `DROP`. |
| Cursors, `PREPARE` as a statement | Not&nbsp;yet | |
| `NoticeResponse` | Not&nbsp;yet | |

The default SQL port is `3210`. The database `egres` exists from the first boot, and a session that names no database lands in it. `psql` reports the server version as `16.0 (Egres <tag>)`: the Postgres release Egres presents, which clients read for the features to use, then the Egres release tag. `SHOW egres_version` returns the tag alone, `dev` for a build without one. The clients used in the quickstart and the benchmarks are `psql` and `pgbench`.

## Statements

| Statement | Status | Notes |
|---|---|---|
| `CREATE DATABASE name` | Yes | `TEMPLATE source` branches the source database. `OWNER`, `ENCODING` and the other options are accepted and ignored. |
| `DROP DATABASE name` | Yes | Except the default database `egres`. |
| `CREATE TABLE [IF NOT EXISTS]` | Partial | See [Tables](#tables). |
| `CREATE TABLE dst AS TABLE src` | Yes | Branches `src` with its indexes. `WITH NO DATA` gives the definition and indexes, empty. `AS SELECT * FROM src` is the same statement. |
| `CREATE TABLE dst AS SELECT ...` with a `WHERE`, column list or other clause | Not&nbsp;yet | |
| `DROP TABLE [IF EXISTS] name` | Yes | One table per statement. |
| `CREATE [UNIQUE] INDEX [IF NOT EXISTS]` | Partial | See [Indexes](#indexes). |
| `DROP INDEX [IF EXISTS] name` | Yes | One index per statement. `CONCURRENTLY` is not needed: every index change is online. |
| `ALTER TABLE`, `ALTER INDEX`, `ALTER DATABASE` | Not&nbsp;yet | |
| `TRUNCATE` | Not&nbsp;yet | |
| `INSERT ... VALUES` | Yes | Multi-row `VALUES` and an explicit column list. |
| `INSERT ... ON CONFLICT (column) DO NOTHING \| DO UPDATE SET ... [WHERE ...]` | Yes | The column must carry a `PRIMARY KEY` or `UNIQUE` constraint. `DO NOTHING` without a target takes any unique constraint. The `SET` values may name the existing row's columns (through the table name or its `AS` alias) and `EXCLUDED.column`, the proposed row's; the `WHERE` reads the existing row and skips it when false. The tag counts updated rows with inserted ones. Not yet: `ON CONSTRAINT`, a partial-index `WHERE` on the target, `EXCLUDED` in the `WHERE`. |
| `INSERT ... RETURNING`, `UPDATE ... RETURNING`, `DELETE ... RETURNING` | Yes | Any target a `SELECT` takes: columns, `*`, expressions with `AS`. `UPDATE` returns the rows as rewritten, `DELETE` the rows as they were, `ON CONFLICT DO UPDATE` the updated row and `DO NOTHING` nothing for a skipped row. Aggregates are refused. |
| `INSERT ... SELECT`, `DEFAULT VALUES`, `DEFAULT` as a value | Not&nbsp;yet | |
| `SELECT` | Partial | See [Queries](#queries). |
| `UPDATE ... SET ... [WHERE]` | Yes | `SET` takes a literal, a parameter, a column, a call of a [supported function](#functions-and-operators), or `+ - * / % \|\|` over them. |
| `UPDATE ... FROM`, `WITH` | Not&nbsp;yet | |
| `DELETE FROM t [WHERE]` | Yes | Without `WHERE` it deletes every row. |
| `DELETE ... USING`, `WITH` | Not&nbsp;yet | |
| `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK` | Yes | Transaction modes such as `ISOLATION LEVEL` are accepted and ignored; isolation is snapshot isolation. |
| `SAVEPOINT`, `RELEASE SAVEPOINT`, `ROLLBACK TO SAVEPOINT` | Yes | `ROLLBACK TO` discards the writes since the savepoint, releases the row locks taken since, and revives a block an error failed. After a serialization failure, a deadlock or a snapshot-too-old error the block cannot be rolled back to a savepoint: `ROLLBACK TO` is `40001`, retry the transaction. |
| `PREPARE TRANSACTION` | Not&nbsp;yet | |
| DDL inside a transaction block | Not&nbsp;yet | Returns `25001`. Each DDL statement runs as its own transaction. |
| `SET`, `RESET` | Yes | Any parameter, accepted and ignored. |
| `SHOW` | Partial | The connection's `ParameterStatus` settings, `transaction_isolation` and `default_transaction_isolation` (`repeatable read`), `transaction_read_only`, `search_path`, `max_identifier_length`. Any other parameter is `42704`; `SHOW ALL` too. |
| `EXPLAIN`, `ANALYZE` | Not&nbsp;yet | |
| `VACUUM` | Not&nbsp;applicable | See [No VACUUM](design/no-vacuum.md). |
| `CREATE FUNCTION`, `CREATE PROCEDURE`, `DO` (user-defined functions and procedures) | Not&nbsp;yet | |
| `CREATE TRIGGER` | Not&nbsp;yet | |
| `CREATE VIEW`, `CREATE MATERIALIZED VIEW` | Not&nbsp;yet | |
| `CREATE SEQUENCE`, `nextval()` | Not&nbsp;yet | |
| `CREATE SCHEMA`, `CREATE EXTENSION` | Not&nbsp;yet | |
| `GRANT`, `REVOKE`, `CREATE ROLE` | Not&nbsp;yet | |

## Queries

The shape a `SELECT` takes today:

```sql
SELECT expression [AS name], ... | * | count(*)
FROM one_table
[WHERE predicate]
[FOR UPDATE]
[LIMIT n]
```

| Clause | Status | Notes |
|---|---|---|
| `SELECT 1`, constants with no `FROM` | Yes | One row; a pool's connection check. Aliases allowed. |
| Column references and `*` in the target list | Yes | The `egid` column can be selected by name. |
| Expressions in the target list, with `AS` | Yes | Arithmetic, `\|\|` and the [supported functions](#functions-and-operators) over the row's columns, literals and parameters. Without an alias the name is the column's, the function's, or `?column?`. |
| `count(*)` | Yes | Alone in the target list. |
| Other aggregates, casts, `CASE`, subqueries in the target list | Not&nbsp;yet | |
| One table in `FROM`, named directly | Yes | |
| `JOIN`, subqueries, `VALUES` in `FROM`, table aliases | Not&nbsp;yet | |
| `WHERE` | Partial | See below. |
| `FOR UPDATE` | Yes | Locks every matching row for the rest of the transaction. Not with `count(*)`. |
| `FOR SHARE`, `NOWAIT`, `SKIP LOCKED`, `FOR UPDATE OF` | Not&nbsp;yet | |
| `LIMIT n` with an integer literal, `LIMIT ALL` | Yes | |
| `LIMIT` with a parameter or expression, `OFFSET`, `FETCH ... WITH TIES` | Not&nbsp;yet | |
| `ORDER BY`, `GROUP BY`, `DISTINCT`, `HAVING` | Not&nbsp;yet | |

Rows come back in `egid` order, which is insertion order, whether a scan or an index produced them.

### WHERE

| Form | Status | Notes |
|---|---|---|
| `column = literal` | Yes | A number, a string, a boolean or `NULL`. `column = NULL` matches nothing, as in Postgres. A literal that does not parse as the column's type returns `22P02`. |
| `expression op expression` | Yes | Arithmetic, `\|\|` and the [supported functions](#functions-and-operators) on either side, a column compared with a column included. Two numbers or two strings compare; other pairings return `42883`. Checked row by row: only a bare column against a constant reads an index. |
| `expression IS [NOT] NULL` | Yes | |
| `column = $n` | Yes | |
| `column IS NULL`, `column IS NOT NULL` | Yes | |
| `AND`, `OR`, `NOT` | Yes | Nested to any depth. |
| `egid = N` | Yes | A single-hop point read; `N` is a positive integer. See [Extensions](#extensions). |
| `<`, `<=`, `>`, `>=`, `<>` (`!=`) | Yes | The literal may stand on either side. Text compares bytewise. On an indexed column `<`, `<=`, `>` and `>=` read the index between the bounds; `<>` scans. |
| `IN (...)`, `NOT IN (...)` | Yes | A list of literals, parameters or expressions; evaluated as the `OR` of equalities, so an indexed column reads one entry per value and `NOT IN` with a `NULL` in the list matches nothing, as in Postgres. |
| `BETWEEN`, `NOT BETWEEN` | Yes | Evaluated as `>=` and `<=`, so an indexed column reads the range. `BETWEEN SYMMETRIC`: not yet. |
| `IS [NOT] DISTINCT FROM` | Yes | Never unknown: two `NULL`s are not distinct. `IS NOT DISTINCT FROM` reads an index as `=` does. |
| `IS [NOT] TRUE`, `IS [NOT] FALSE`, `IS [NOT] UNKNOWN`, a bare boolean column | Yes | Over a boolean column or expression; anything else returns `42804`. `IS TRUE`, `IS FALSE`, `IS UNKNOWN` and a bare column read an index; the `NOT` forms scan. |
| `LIKE`, `ILIKE`, `CASE`, subqueries | Not&nbsp;yet | |

Any indexed column in the predicate drives the query through its index, an equality as one entry and an ordered comparison as the entries between its bounds, and `AND`, `OR` and `NOT` over indexed columns become bitmap operations. A predicate on an unindexed column scans the table with the predicate applied to each row. A mixed predicate uses the indexed terms to pick a superset of the rows and re-checks the whole predicate on each. An index still being built is used once its backfill completes.

## Tables

```sql
CREATE TABLE [IF NOT EXISTS] name (
  column type [PRIMARY KEY | UNIQUE],
  ...
);
```

| Feature | Status | Notes |
|---|---|---|
| `PRIMARY KEY` on one column | Yes | One per table. |
| `UNIQUE` on one column | Yes | `NULL` is allowed, and two `NULL`s do not collide, as in Postgres. |
| `NOT NULL` | Yes | Enforced on `INSERT` and `UPDATE` (`23502`). The `PRIMARY KEY` column is `NOT NULL`. |
| `DEFAULT`, `CHECK`, `REFERENCES`, `GENERATED` | Not&nbsp;yet | |
| Table-level constraints: `PRIMARY KEY (a, b)`, `UNIQUE (a, b)`, `FOREIGN KEY` | Not&nbsp;yet | |
| `char(n)`, `varchar(n)` length modifiers | Yes | Up to 10,485,760, as in Postgres. A bare `char` is `char(1)`; a bare `varchar` is `text`. |
| Length modifiers on other types | Not&nbsp;yet | |
| A table with no `PRIMARY KEY` | Yes | Keyed by `egid`. |

`NULL` in the primary key column returns `23502`. `egid` is a reserved column name. Table and index definitions live in reserved tables inside the database, so they branch with it.

## Indexes

```sql
CREATE [UNIQUE] INDEX [IF NOT EXISTS] name ON table (column);
```

| Feature | Status | Notes |
|---|---|---|
| Single-column index | Yes | One index per column, counting the index behind a `PRIMARY KEY` or `UNIQUE` column. |
| `UNIQUE` index | Yes | |
| `USING btree` | Yes | Accepted; every Egres index is a bitmap index. See [Indexes](design/indexes.md). |
| Online creation | Yes | The index is backfilled while writes continue and becomes visible when the backfill completes, so `CONCURRENTLY` is not needed. |
| Composite, expression and partial indexes | Not&nbsp;yet | |
| `INCLUDE`, `DESC`, `NULLS FIRST`, `TABLESPACE` | Not&nbsp;yet | |
| Other access methods (`hash`, `gin`, `gist`, `brin`) | Not&nbsp;yet | |
| Indexes on `interval` columns | Not&nbsp;yet | As a primary key, a unique column or by `CREATE INDEX`. |
| Dropping the primary key's index | Not&nbsp;yet | |

## Data types

| Postgres type | Status | Notes |
|---|---|---|
| `boolean` | Yes | Literals `t`, `true`, `1`, `yes`, `on` and their opposites. |
| `smallint`, `integer`, `bigint` | Yes | |
| `real`, `double precision` | Yes | |
| `text`, `varchar`, `varchar(n)`, `char(n)` | Yes | Values are valid UTF-8 (`22021` otherwise); a value longer than `n` returns `22001`. |
| `bytea` | Yes | Hex form in text, `'\x00ff'`, as `bytea_output = 'hex'`. |
| `date`, `timestamp`, `timestamptz` | Yes | |
| `uuid` | Yes | Postgres text input and canonical output; see below. |
| `time`, `interval` | Partial | Only `NULL`; other values return `0A000`. |
| `time with time zone` | Not&nbsp;yet | |
| `numeric`, `decimal`, `numeric(p, s)` | Yes | Exact, up to 38 digits, where Postgres allows more: a longer value is `22003`, and an arithmetic result past 38 digits keeps as many fractional digits as fit, rounded half away from zero. A value is rounded to the column's scale, half away from zero, and must fit its precision. Printed with every digit of its scale (`12.50`). `NaN` and `Infinity` are not accepted. |
| `serial`, `bigserial` | Not&nbsp;yet | `egid` gives an insertion-ordered id today. |
| `json`, `jsonb` | Not&nbsp;yet | |
| Arrays, `enum`, composite, range, `money`, `inet`, `bit`, `tsvector`, geometric types | Not&nbsp;yet | |

UUID input accepts upper- or lowercase hex digits, optional enclosing braces, and optional hyphens between groups of four hex digits, as in Postgres. Output is always lowercase in `8-4-4-4-12` form, such as `00112233-4455-6677-8899-aabbccddeeff`. Invalid UUID text returns `22P02`. UUID generation functions are not supported.

Assignments in `SET` follow Postgres: any integer into any numeric column with a range check (`22003` on overflow), floats into floats, strings into strings. A float into an integer column returns `42804`. Division by zero returns `22012`.

## Functions and operators

| Feature | Status | Notes |
|---|---|---|
| `count(*)` | Yes | Alone in the target list. |
| `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP` | Partial | In `INSERT ... VALUES`, into a `timestamp` or `timestamptz` column; the transaction's start time, as in Postgres. In `WHERE` or `SET`: not yet. |
| `+`, `-`, `*`, `/`, `%` on numeric types, unary minus | Yes | In `SELECT` targets, `WHERE` and `UPDATE ... SET`, over literals, parameters and columns of the same row. A `numeric` with an integer stays exact, with a float becomes `float8`; `numeric` division keeps 16 fractional digits or the dividend's scale, whichever is more. A literal with a decimal point or an exponent is `numeric`, as in Postgres, so `0.1 + 0.2` is exactly `0.3`. |
| `\|\|` on strings | Yes | In `SELECT` targets, `WHERE` and `UPDATE ... SET`. |
| A cast of a constant: `'6'::int8`, `CAST('x' AS text)` | Partial | In `VALUES`, `WHERE`, `SET` and a `SELECT` without `FROM`. The constant is typed by the column it meets, as a bare literal is; the named type is not checked. pgjdbc in simple query mode inlines every bind parameter this way. |
| `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`, `CONCAT`, `CONCAT_WS`, `LOWER`, `UPPER`, `LENGTH`, `SUBSTRING`, `REPLACE`, `TRIM`, `LTRIM`, `RTRIM`, `LEFT`, `RIGHT`, `ABS`, `SIGN`, `FLOOR`, `CEIL`, `ROUND`, `TRUNC`, `MOD`, `POWER`, `SQRT` | Yes | In `SELECT` targets, `WHERE` and `UPDATE ... SET`, over literals, parameters and columns of the row; strings and numbers. `LOWER` and `UPPER` fold ASCII letters only, where Postgres folds by collation. `CONCAT` skips NULL arguments and renders numbers as text. |
| `now()`, casts of columns and expressions, date functions, `CASE`, every other function and operator | Not&nbsp;yet | |
| User-defined functions, in SQL or any other language | Not&nbsp;yet | |

## Extensions

Things Egres accepts that Postgres does not.

- **`egid`.** Every row has a 64-bit id, assigned at insert and increasing within the table. `SELECT egid, ...` reads it and `WHERE egid = N` is a single-hop point read. It is assigned by the server, so an explicit value in `INSERT` or `UPDATE` is not accepted. It rides the wire as `numeric`.
- **Unsigned integers.** `uint2`, `uint4` and `uint8` are column types. Clients see `uint2` as `int4`, `uint4` as `int8` and `uint8` as `numeric`.
- **`CREATE DATABASE new TEMPLATE src`** branches a database instantly instead of copying its files, and **`CREATE TABLE dst AS TABLE src`** branches a table with its indexes instead of copying rows without them. See [Branching](design/branching.md).

## Behaviour today

- **Isolation is snapshot isolation.** Reads never block. A statement that locks a row committed after its snapshot re-runs itself at the newer snapshot; in a multi-statement transaction that may instead fail with `40001`, the same serialization failure Postgres returns. See [Transactions](design/transactions.md).
- **Deadlocks are detected at lock time.** The lock request that would close a cycle fails immediately with `40P01`, and that transaction's locks are released. There is no `deadlock_timeout` and no `lock_timeout`.
- **Writes run on the Raft leader.** This release runs a single node, which is always the leader. Multi-node clusters come in a later release.
- **A failed statement fails the transaction**, as in Postgres: only `COMMIT` and `ROLLBACK` are accepted until the block ends (`25P02` otherwise), and both roll back.
- **A read below the retention floor** returns `72000`, snapshot too old. See [Garbage collection](design/no-vacuum.md).
