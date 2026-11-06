#!/usr/bin/env python3
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""Convert Postgres regress .sql files into .pgout records.

    convert.py [--oracle DSN] pg/int8.sql...

Splits each file into statements the way psql does, runs them in order
against a reference Postgres in a fresh database, and writes
<name>.pgout beside pg/, one record per statement (see pgout.py). A
regress file may use tables it does not create, because pg_regress
runs test_setup.sql first; those come from pg/test_setup.sql: every
setup statement naming one of them goes ahead of the file's own.
"""

import argparse
import os
import re

import psycopg
from psycopg.conninfo import make_conninfo

import pgout

ORACLE = "host=127.0.0.1 port=5433 user=postgres dbname=postgres"
HERE = os.path.dirname(os.path.abspath(__file__))

_DOLLAR = re.compile(r"\$([A-Za-z_][A-Za-z_0-9]*)?\$")
_COPY_STDIN = re.compile(r"COPY\b.*\bFROM\s+STDIN\b", re.I | re.S)
# A blank line would end a record early (records are blank-separated).
_BLANK_LINE = re.compile(r"\n[ \t]*\n")


def _ident(ch):
    return ch.isalnum() or ch in "_$"


def _line_of(text, pos):
    return text.count("\n", 0, pos) + 1


def _end_of_quote(text, i, quote, backslash):
    """Offset just past the literal opening at i."""
    j = i + 1
    while j < len(text):
        if backslash and text[j] == "\\":
            j += 2
            continue
        if text[j] == quote:
            if text.startswith(quote * 2, j):
                j += 2  # doubled quote: an escaped one
                continue
            return j + 1
        j += 1
    raise ValueError(f"unterminated {quote} at line {_line_of(text, i)}")


def _end_of_block_comment(text, i):
    depth, j = 0, i
    while j < len(text):
        if text.startswith("/*", j):
            depth, j = depth + 1, j + 2
        elif text.startswith("*/", j):
            depth, j = depth - 1, j + 2
            if depth == 0:
                return j
        else:
            j += 1
    raise ValueError(f"unterminated comment at line {_line_of(text, i)}")


def _end_of_line(text, i):
    nl = text.find("\n", i)
    return len(text) if nl < 0 else nl


def _copy_data(text, i):
    """The COPY rows after the statement ending at i, and their end."""
    j = _end_of_line(text, i) + 1
    rows = []
    while j < len(text):
        end = _end_of_line(text, j)
        row = text[j:end]
        if row == "\\.":
            if not all(rows):
                line = _line_of(text, j)
                raise ValueError(f"empty COPY row before line {line}")
            return "\n".join(rows), end + 1
        rows.append(row)
        j = end + 1
    raise ValueError(f"COPY data without \\. after line {_line_of(text, i)}")


def split(text):
    """Yield (line, sql, copy_rows) per statement, psql-style.

    A ';' ends a statement only outside quotes, comments and
    parentheses. A line starting with a backslash between statements
    is a psql meta-command (\\d, \\set ...): skipped.
    """
    i, start, depth = 0, None, 0
    while i < len(text):
        ch = text[i]
        if start is None:
            if ch.isspace():
                i += 1
                continue
            if ch == "\\" and (i == 0 or text[i - 1] == "\n"):
                i = _end_of_line(text, i)
                continue
            if not text.startswith(("--", "/*"), i):
                start = i
        if text.startswith("--", i):
            i = _end_of_line(text, i)
        elif text.startswith("/*", i):
            i = _end_of_block_comment(text, i)
        elif ch in "'\"":
            # E'...' takes backslash escapes; a plain '...' does not
            # (standard_conforming_strings).
            escape = (
                ch == "'"
                and i > 0
                and text[i - 1] in "eE"
                and not (i > 1 and _ident(text[i - 2]))
            )
            end = _end_of_quote(text, i, ch, escape)
            _no_blank_line(text, i, end)
            i = end
        elif ch == "$" and not (i > 0 and _ident(text[i - 1])):
            m = _DOLLAR.match(text, i)
            if not m:
                i += 1
                continue
            close = text.find(m[0], m.end())
            if close < 0:
                raise ValueError(f"unterminated {m[0]} at {_line_of(text, i)}")
            end = close + len(m[0])
            _no_blank_line(text, i, end)
            i = end
        elif ch == ";" and depth == 0:
            sql = _clean(text[start:i])
            line = _line_of(text, start)
            copy = None
            i += 1
            if _COPY_STDIN.match(sql):
                copy, i = _copy_data(text, i)
            yield line, sql, copy
            start = None
        else:
            depth += {"(": 1, ")": -1}.get(ch, 0)
            i += 1
    if start is not None and text[start:].strip():
        raise ValueError(f"no ';' after line {_line_of(text, start)}")


def _no_blank_line(text, start, end):
    if _BLANK_LINE.search(text, start, end):
        raise ValueError(f"blank line in a literal at {_line_of(text, start)}")


def _clean(sql):
    # Blank lines go (none is inside a literal, see _no_blank_line), so
    # the statement reads as one block in the record.
    lines = [ln for ln in sql.strip().split("\n") if ln.strip()]
    if "----" in lines:
        raise ValueError(f"a '----' line would split the record: {sql!r}")
    return "\n".join(lines)


def setup_statements(setup, text):
    """The setup statements naming tables `text` uses but not creates."""
    created = set()
    for _, sql, _ in setup:
        m = pgout.CREATE_TABLE.match(sql)
        if m:
            created.add(m[1].lower())
    words = set(re.findall(r"\w+", text.lower()))
    own = {t.lower() for t in pgout.CREATE_TABLE.findall(text)}
    need = (created & words) - own
    return [
        s for s in setup if need & set(re.findall(r"\w+", s[1].lower()))
    ]


def fresh_database(dsn, name):
    db = pgout.database(name)
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {db}")
        conn.execute(f"CREATE DATABASE {db}")
    return make_conninfo(dsn, dbname=db)


def record(conn, source, sql, copy):
    rec = pgout.Record(source, "ok", sql, copy)
    out = pgout.execute(conn, sql, copy)
    if isinstance(out, pgout.Failed):
        rec.kind, rec.note = "error", f"{out.sqlstate} {out.message}"
    elif isinstance(out, pgout.Rows):
        rec.kind = "query"
        rec.types = "".join(pgout.type_class(o) for o in out.oids)
        rec.sort = "nosort" if pgout.has_order_by(sql) else "rowsort"
        rows = pgout.canon_rows(rec.types, out.rows, rec.sort)
        values = pgout.flatten(rows)
        if len(rows) > pgout.HASH_THRESHOLD:
            rec.count, rec.digest = len(values), pgout.digest(values)
        else:
            rec.values = values
    return rec


def convert(path, oracle, version):
    with open(path) as f:
        text = f.read()
    rel = os.path.relpath(os.path.abspath(path), HERE)
    stmts = []
    setup_path = os.path.join(HERE, "pg", "test_setup.sql")
    if os.path.abspath(path) != setup_path and os.path.exists(setup_path):
        with open(setup_path) as f:
            setup = list(split(f.read()))
        stmts += [
            (f"pg/test_setup.sql:{ln}", sql, copy)
            for ln, sql, copy in setup_statements(setup, text)
        ]
    stmts += [(f"{rel}:{ln}", sql, copy) for ln, sql, copy in split(text)]
    # Statements sharing a line share a source; number the later ones
    # so every record has a key of its own in <name>.known (run.py).
    seen = {}
    for i, (source, sql, copy) in enumerate(stmts):
        seen[source] = seen.get(source, 0) + 1
        if seen[source] > 1:
            stmts[i] = (f"{source}#{seen[source]}", sql, copy)

    name = os.path.splitext(os.path.basename(path))[0]
    records = []
    with pgout.connect(fresh_database(oracle, name)) as conn:
        for source, sql, copy in stmts:
            records.append(record(conn, source, sql, copy))
    out = os.path.join(HERE, name + ".pgout")
    pgout.write(
        out,
        f"# Generated by convert.py from {rel} against PostgreSQL "
        f"{version};\n# do not edit: rerun convert.py.\n",
        records,
    )
    return out, records


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--oracle", default=ORACLE, help="reference Postgres")
    ap.add_argument("files", nargs="+", help="regress .sql files")
    args = ap.parse_args()
    with psycopg.connect(args.oracle) as conn:
        row = conn.execute("SHOW server_version").fetchone()
    version = row[0].split()[0]
    for path in args.files:
        out, records = convert(path, args.oracle, version)
        kinds = {}
        for rec in records:
            kinds[rec.kind] = kinds.get(rec.kind, 0) + 1
        print(f"{os.path.relpath(out)}: {len(records)} records {kinds}")


if __name__ == "__main__":
    main()
