# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""The .pgout record format, shared by convert.py and run.py.

A sqllogictest-style file: records separated by blank lines, each led
by "# <source>" comments naming the statement's origin.

    statement ok             the statement succeeds
    statement error          the statement fails (any error)
    query <types> <sort>     it returns rows; one class letter per
                             column, then "----" and the values

A COPY ... FROM stdin statement record carries its data rows after a
"----" line. Query values are canonical, one per line, row by row:
NULL as NULL, the empty string as (empty), reals (R) to three
decimals, numerics (N) without trailing zeros, integers (I) and text
(T) as sent. "rowsort" sorts rows before comparing, "nosort" keeps
the server's order. A result of more than hash-threshold rows is
stored as "<n> values hashing to <md5>" instead, as sqllogictest does.

Both sides are driven the way psql drives a server: autocommit, text
results, the simple query protocol.
"""

import hashlib
import re
import threading
from dataclasses import dataclass, field

import psycopg

HASH_THRESHOLD = 50

# The table a CREATE TABLE makes: convert.py takes setup tables by it,
# run.py marks what an unsupported CREATE leaves missing.
CREATE_TABLE = re.compile(
    r"CREATE\s+(?:TEMP\w*\s+|UNLOGGED\s+)?TABLE\s+"
    r"(?:IF\s+NOT\s+EXISTS\s+)?(\w+)",
    re.I,
)

INT_OIDS = {20, 21, 23, 26}  # int8, int2, int4, oid
REAL_OIDS = {700, 701}  # float4, float8
NUMERIC_OID = 1700


@dataclass
class Record:
    source: str  # "pg/int8.sql:9": the key in <name>.known
    kind: str  # "ok", "error" or "query"
    sql: str
    copy: str | None = None  # COPY FROM stdin rows, newline-separated
    types: str = ""
    sort: str = "nosort"
    values: list[str] | None = None  # None when hashed
    count: int = 0  # values, when hashed
    digest: str = ""
    note: str = ""  # postgres's error, for "error" records

    @property
    def first_line(self):
        return self.sql.split("\n", 1)[0]


# --- outcomes of running one statement ------------------------------------


@dataclass
class Done:
    """Succeeded without a result set."""


@dataclass
class Failed:
    sqlstate: str | None
    message: str


@dataclass
class Rows:
    oids: list[int]
    rows: list[list[str | None]] = field(default_factory=list)


class Lost(Exception):
    """The connection died: the server crashed or hung up."""


def database(name):
    """The database a file runs in, on either server: a fresh one."""
    return f"sqllogic_{name}"


def connect(dsn):
    # prepare_threshold=None: never switch a repeated statement to a
    # server-side prepared one, so every record takes the same path.
    return psycopg.connect(dsn, autocommit=True, prepare_threshold=None)


def execute(conn, sql, copy=None, timeout=None):
    """Run one statement: Done, Failed or Rows; raises Lost."""
    timer = None
    if timeout:
        # A statement past the timeout is cancelled, so one slow
        # record fails instead of wedging the whole run.
        timer = threading.Timer(timeout, conn.cancel_safe)
        timer.start()
    cur = conn.cursor()
    try:
        if copy is not None:
            with cur.copy(sql) as cp:
                cp.write(copy + "\n")
            return Done()
        cur.execute(sql)
    except psycopg.Error as err:
        if conn.broken or conn.closed:
            raise Lost(str(err).strip()) from err
        return Failed(err.sqlstate, str(err).strip().split("\n", 1)[0])
    finally:
        if timer:
            timer.cancel()
    if cur.description is None or not cur.description:
        return Done()
    res = cur.pgresult
    return Rows(
        [res.ftype(c) for c in range(res.nfields)],
        [
            [_text(res.get_value(r, c)) for c in range(res.nfields)]
            for r in range(res.ntuples)
        ],
    )


def _text(raw):
    if raw is None:
        return None
    return bytes(raw).decode("utf-8", "backslashreplace")


# --- canonical values ------------------------------------------------------


def type_class(oid):
    if oid in INT_OIDS:
        return "I"
    if oid in REAL_OIDS:
        return "R"
    if oid == NUMERIC_OID:
        return "N"
    return "T"


def canon(cls, raw):
    """One value in canonical form, formatted by the expected class.

    A value that does not parse as its class is kept as sent, so a
    type mismatch shows up as a differing value.
    """
    if raw is None:
        return "NULL"
    if cls == "R":
        return _canon_real(raw)
    if cls == "N":
        return _canon_numeric(raw)
    if raw == "":
        return "(empty)"
    # One value per line: escape the line breaks a value could carry.
    return raw.replace("\r", "\\r").replace("\n", "\\n")


def _canon_real(raw):
    try:
        x = float(raw)
    except ValueError:
        return raw
    if x != x:
        return "NaN"
    if x in (float("inf"), float("-inf")):
        return "Infinity" if x > 0 else "-Infinity"
    s = f"{x:.3f}"
    return "0.000" if s == "-0.000" else s


_PLAIN_NUMBER = re.compile(r"-?\d+(\.\d*)?")


def _canon_numeric(raw):
    s = raw.strip()
    if not _PLAIN_NUMBER.fullmatch(s):
        return raw  # NaN, Infinity, or not a number at all
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return "0" if s in ("-0", "") else s


def canon_rows(types, rows, sort):
    """Canonical rows, in compare order, as lists of strings."""
    out = [[canon(cls, v) for cls, v in zip(types, row)] for row in rows]
    if sort == "rowsort":
        out.sort()
    return out


def flatten(rows):
    return [v for row in rows for v in row]


def digest(values):
    return hashlib.md5("".join(v + "\n" for v in values).encode()).hexdigest()


def has_order_by(sql):
    return re.search(r"\border\s+by\b", sql, re.IGNORECASE) is not None


# --- reading and writing -------------------------------------------------


def format_record(rec):
    lines = [f"# {rec.source}"]
    if rec.note:
        lines.append(f"# postgres: {rec.note}")
    if rec.kind == "query":
        lines.append(f"query {rec.types} {rec.sort}")
    else:
        lines.append(f"statement {rec.kind}")
    lines.append(rec.sql)
    if rec.copy is not None:
        lines += ["----", rec.copy]
    if rec.kind == "query":
        lines.append("----")
        if rec.values is None:
            lines.append(f"{rec.count} values hashing to {rec.digest}")
        else:
            lines += rec.values
    return "\n".join(lines) + "\n"


def write(path, header, records):
    with open(path, "w") as f:
        f.write(header)
        f.write(f"hash-threshold {HASH_THRESHOLD}\n")
        for rec in records:
            f.write("\n" + format_record(rec))


_HASHED = re.compile(r"(\d+) values hashing to ([0-9a-f]{32})")


def read(path):
    # Records end at an empty line: no value is one ("(empty)"), but a
    # value may be all blanks, so a blank-looking line does not end one.
    with open(path) as f:
        blocks = re.split(r"\n\n+", f.read())
    records = []
    for block in blocks:
        lines = block.strip("\n").split("\n")
        # Only leading lines are comments: a value may start with "#".
        n_comments = 0
        while n_comments < len(lines) and lines[n_comments].startswith("#"):
            n_comments += 1
        comments = [ln[2:] for ln in lines[:n_comments]]
        body = lines[n_comments:]
        if not body or body[0].startswith("hash-threshold"):
            continue
        head = body[0].split()
        sep = body.index("----") if "----" in body else len(body)
        rec = Record(
            source=comments[0] if comments else "?",
            kind=head[1] if head[0] == "statement" else "query",
            sql="\n".join(body[1:sep]),
        )
        for c in comments[1:]:
            if c.startswith("postgres: "):
                rec.note = c[len("postgres: ") :]
        rest = body[sep + 1 :]
        if rec.kind == "query":
            rec.types, rec.sort = head[1], head[2]
            m = _HASHED.fullmatch(rest[0]) if len(rest) == 1 else None
            if m:
                rec.count, rec.digest = int(m[1]), m[2]
            else:
                rec.values = rest
        elif sep < len(body):
            rec.copy = "\n".join(rest)
        records.append(rec)
    return records
