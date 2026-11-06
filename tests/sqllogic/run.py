#!/usr/bin/env python3
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""Replay .pgout records against egres and report how it compares.

    run.py [--write-skips] [--timeout SECONDS] DSN file.pgout...

Each file runs in a fresh database. Every record lands in one bin:

    passed       same outcome as Postgres
    unsupported  egres errored where Postgres succeeded
    blocked      it failed only through an earlier failure: its
                 table's CREATE failed (42P01), its transaction is
                 aborted (25P02), or it diverged reading a table an
                 earlier write left unlike Postgres's
    diverged     both succeeded with different results, or egres
                 accepted a statement Postgres rejects

Errors compare as failure versus success, never by text. Beside each
.pgout, <name>.known lists where egres stands on it, one record per
line, "<tag>\\t<source>\\t<first line>\\t<note>", keyed by the record's
origin ("pg/int8.sql:9"), which regenerating the .pgout keeps where a
position would shift. The tags:

    skip         egres does not support it yet; the note is its error.
                 Written by --write-skips, which keeps the other lines.
    bug          diverges and is filed; the note is the issue.
    accepted     diverges on purpose; the note is the reason.

The run fails on an unsupported record without a skip, a divergence
that is neither a bug nor accepted, or a lost connection. Entries that
now pass are reported so they can be removed.
"""

import argparse
import os
import re
import sys
from dataclasses import dataclass

from psycopg.conninfo import make_conninfo

import pgout

UNDEFINED_TABLE = "42P01"
IN_FAILED_TXN = "25P02"
# A write whose outcome differs leaves the table's rows unlike
# Postgres's, so what later reads it cannot be compared.
_WRITE = re.compile(
    r"(?:INSERT\s+INTO|COPY|UPDATE|DELETE\s+FROM)\s+(\w+)", re.I
)
# A record's origin as convert.py writes it: "pg/int8.sql:9", with
# "#2" and up for further statements on the same line.
_SOURCE = re.compile(r"\S+:\d+(?:#\d+)?")
# Each .known tag and the bin it stands for.
TAGS = {"skip": "unsupported", "bug": "diverged", "accepted": "diverged"}
KNOWN_HEADER = """\
# Where egres stands on each record of the .pgout beside this file:
# <tag>\t<source>\t<first line>\t<note>, in record order.
#   skip      not supported yet; note = egres's error. Written by
#             run.py --write-skips, which keeps every other line.
#   bug       diverges, filed; note = the issue.
#   accepted  diverges on purpose; note = the reason.
"""


@dataclass
class Result:
    rec: pgout.Record
    bin: str
    error: pgout.Failed | None = None  # egres's, when it failed
    got: list[list[str]] | None = None  # egres's rows, when diverged
    why: str = ""  # what diverged
    cause: str = ""  # what blocked it
    known: bool = False  # listed in .known with a matching tag
    note: str = ""  # the .known entry's tag and note


def key(line):
    # Whitespace-collapsed, so a tab in the SQL cannot split the TSV.
    return " ".join(line.split())


def load_known(path):
    """({source: (tag, first line, note)}, comment lines) from .known.

    The comments are the hand-written ones past the header, kept by
    write_known.
    """
    entries, comments = {}, []
    if not os.path.exists(path):
        return entries, comments
    header = set(KNOWN_HEADER.split("\n"))
    with open(path) as f:
        for n, line in enumerate(f, 1):
            line = line.rstrip("\n")
            if not line.strip():
                continue
            if line.startswith("#"):
                if line not in header:
                    comments.append(line)
                continue
            parts = line.split("\t", 3)
            if (
                len(parts) < 4
                or parts[0] not in TAGS
                or not _SOURCE.fullmatch(parts[1])
            ):
                sys.exit(
                    f"{path}:{n}: want <tag>\\t<source>\\t<first line>"
                    f"\\t<note>, tag one of {', '.join(TAGS)}"
                )
            entries[parts[1]] = (parts[0], key(parts[2]), parts[3])
    return entries, comments


def judge(rec, out):
    """(bin, why, egres rows) for one record's outcome on egres."""
    if rec.kind == "error":
        if isinstance(out, pgout.Failed):
            return "passed", "", None
        return "diverged", "egres succeeded; postgres: " + rec.note, None
    if isinstance(out, pgout.Failed):
        return "unsupported", "", None
    if rec.kind == "ok":
        return "passed", "", None
    if not isinstance(out, pgout.Rows):
        return "diverged", "egres returned no result set", None
    got_types = "".join(pgout.type_class(o) for o in out.oids)
    if len(got_types) != len(rec.types):
        why = f"{len(got_types)} columns, want {len(rec.types)}"
        return "diverged", why, out.rows
    rows = pgout.canon_rows(rec.types, out.rows, rec.sort)
    values = pgout.flatten(rows)
    if rec.values is None:
        same = (len(values), pgout.digest(values)) == (rec.count, rec.digest)
    else:
        same = values == rec.values
    whys = []
    if got_types != rec.types:
        whys.append(f"column types {got_types}, want {rec.types}")
    if not same:
        want = chunk(rec.values, len(rec.types))
        if rec.values is not None and sorted(rows) == sorted(want):
            whys.append("same rows, different order")
        else:
            whys.append("different rows")
    if not whys:
        return "passed", "", None
    return "diverged", "; ".join(whys), rows


def chunk(values, width):
    if values is None or not width:
        return []
    return [values[i : i + width] for i in range(0, len(values), width)]


def blocked_by(err, words, missing):
    """The earlier failure this one only echoes, or ""."""
    if err.sqlstate == IN_FAILED_TXN:
        return "aborted transaction"
    if err.sqlstate == UNDEFINED_TABLE:
        hit = sorted(words & missing.keys())
        if hit:
            return f"{hit[0]} missing ({missing[hit[0]]})"
    return ""


def fresh_database(dsn, name):
    db = pgout.database(name)
    with pgout.connect(dsn) as conn:
        pgout.execute(conn, f"DROP DATABASE {db}")  # fails when absent: fine
        out = pgout.execute(conn, f"CREATE DATABASE {db}")
    if isinstance(out, pgout.Failed):
        raise pgout.Lost(f"CREATE DATABASE {db}: {out.message}")
    return make_conninfo(dsn, dbname=db)


def run_file(dsn, path, timeout):
    """The Results of replaying one file, and why it stopped early."""
    name = os.path.splitext(os.path.basename(path))[0]
    records = pgout.read(path)
    # table -> source of the record whose failure lost it (CREATE) or
    # left its rows unlike Postgres's (a write).
    results, missing, tainted = [], {}, {}
    try:
        conn = pgout.connect(fresh_database(dsn, name))
    except Exception as err:  # noqa: BLE001 - any failure stops the file
        return results, f"could not connect: {err}"
    with conn:
        for rec in records:
            try:
                out = pgout.execute(conn, rec.sql, rec.copy, timeout)
            except pgout.Lost as err:
                return results, f"connection lost at {rec.source}: {err}"
            bin_, why, got = judge(rec, out)
            res = Result(rec, bin_, got=got, why=why)
            words = set(re.findall(r"\w+", rec.sql.lower()))
            if isinstance(out, pgout.Failed):
                res.error = out
                if bin_ == "unsupported":
                    res.cause = blocked_by(out, words, missing)
                m = pgout.CREATE_TABLE.match(rec.sql)
                if m:
                    missing.setdefault(m[1].lower(), rec.source)
            elif bin_ == "diverged":
                # An unsupported statement stays unsupported whatever
                # the data; a differing result may be only the data.
                hit = sorted(words & tainted.keys())
                if hit and not _WRITE.match(rec.sql):
                    res.cause = f"{hit[0]} rows differ ({tainted[hit[0]]})"
            if res.cause:
                res.bin = "blocked"
            w = _WRITE.match(rec.sql)
            if w and res.bin != "passed":
                tainted.setdefault(w[1].lower(), rec.source)
            results.append(res)
    return results, ""


def apply_known(results, known):
    """Mark known results; return entries that now pass or are stale."""
    by_source = {r.rec.source: r for r in results}
    notes = []
    for source, (tag, first, note) in known.items():
        res = by_source.get(source)
        if res is None:
            continue  # the run stopped before it, or it is gone
        if key(res.rec.first_line) != first:
            notes.append(f"stale {tag} {source}: {first}")
        elif res.bin == TAGS[tag]:
            res.known, res.note = True, f"{tag}: {note}"
        elif res.bin == "passed":
            notes.append(f"{tag} {source} now passes: {first}")
    return notes


def write_known(path, results, known, comments):
    """Rewrite the skip lines from this run; keep every other line."""
    rows = [
        (source, tag, first, note)
        for source, (tag, first, note) in known.items()
        if tag != "skip"
    ]
    rows += [
        (
            r.rec.source,
            "skip",
            key(r.rec.first_line),
            f"{r.error.sqlstate} {r.error.message}",
        )
        for r in results
        if r.bin == "unsupported"
    ]
    # Record order; an entry for a record the run never reached last.
    order = {r.rec.source: i for i, r in enumerate(results)}
    rows.sort(key=lambda row: order.get(row[0], len(order)))
    with open(path, "w") as f:
        f.write(KNOWN_HEADER)
        for c in comments:
            f.write(c + "\n")
        for source, tag, first, note in rows:
            f.write(f"{tag}\t{source}\t{first}\t{note}\n")


def show_rows(rows):
    return [" ".join(row) for row in rows]


def report_file(path, results, stopped, notes):
    """Print one file's report; return its count of failures."""
    count = {}
    for r in results:
        b = r.bin + ("/known" if r.known else "")
        count[b] = count.get(b, 0) + 1
    unsupported = [r for r in results if r.bin == "unsupported"]
    diverged = [r for r in results if r.bin == "diverged"]
    blocked_ = [r for r in results if r.bin == "blocked"]
    new_unsupported = [r for r in unsupported if not r.known]
    new_diverged = [r for r in diverged if not r.known]

    # An expected error passes on any egres error, even an unsupported
    # feature's, so say how much of "passed" that is.
    err_passes = sum(
        1 for r in results if r.bin == "passed" and r.rec.kind == "error"
    )
    print(f"== {path}: {len(results)} records run ==")
    print(
        f"  passed {count.get('passed', 0)} ({err_passes} expected errors), "
        f"unsupported {len(unsupported)} ({len(new_unsupported)} new), "
        f"blocked {len(blocked_)}, "
        f"diverged {len(diverged)} ({len(new_diverged)} new)"
    )

    if unsupported:
        print("\n  unsupported, by egres error (count, [*] = new):")
        groups = {}
        for r in unsupported:
            msg = f"{r.error.sqlstate} {r.error.message}"
            groups.setdefault(msg, []).append(r)
        for msg, rs in sorted(groups.items(), key=lambda g: -len(g[1])):
            new = sum(1 for r in rs if not r.known)
            print(f"  {len(rs):4}  {msg}" + (f"  [{new} new]" if new else ""))
            for r in rs[:3]:
                print(f"          {r.rec.source}  {r.rec.first_line[:56]}")

    if blocked_:
        print("\n  blocked, by the earlier failure they echo:")
        causes = {}
        for r in blocked_:
            causes[r.cause] = causes.get(r.cause, 0) + 1
        for cause, n in sorted(causes.items(), key=lambda c: -c[1]):
            print(f"  {n:4}  {cause}")

    for r in diverged:
        if r.known:
            mark = f"known {r.rec.source} [{r.note}]"
        else:
            mark = f"DIVERGED {r.rec.source}"
        print(f"\n  {mark}: {r.why}")
        for line in r.rec.sql.split("\n"):
            print(f"      {line}")
        if r.rec.kind != "query":
            continue
        if r.rec.values is None:
            want = [f"{r.rec.count} values hashing to {r.rec.digest}"]
        else:
            want = show_rows(chunk(r.rec.values, len(r.rec.types)))
        got = show_rows(r.got or [])
        width = min(40, max([len(w) for w in want] + [8]))
        print(f"      {'postgres':<{width}} | egres")
        for i in range(max(len(want), len(got))):
            w = want[i] if i < len(want) else ""
            g = got[i] if i < len(got) else ""
            flag = " " if w == g else "*"
            print(f"    {flag} {w:<{width}} | {g}")

    for note in notes:
        print(f"\n  note: {note}")
    if stopped:
        print(f"\n  STOPPED: {stopped}")
    print()
    return len(new_unsupported) + len(new_diverged) + (1 if stopped else 0)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("dsn", help="egres connection string")
    ap.add_argument("files", nargs="+", help=".pgout files")
    ap.add_argument(
        "--write-skips",
        action="store_true",
        help="rewrite the skip lines of each <name>.known from this run",
    )
    ap.add_argument(
        "--timeout", type=float, default=30, help="seconds per statement"
    )
    args = ap.parse_args()

    failures = 0
    for path in args.files:
        base = os.path.splitext(path)[0]
        results, stopped = run_file(args.dsn, path, args.timeout)
        known, comments = load_known(base + ".known")
        if args.write_skips and not stopped:
            write_known(base + ".known", results, known, comments)
            known, comments = load_known(base + ".known")
        notes = apply_known(results, known)
        failures += report_file(path, results, stopped, notes)
    print("sqllogic: OK" if not failures else f"sqllogic: {failures} failures")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
