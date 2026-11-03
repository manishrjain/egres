# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

"""Binary-format checks against a running Egres, driven by psycopg 3.

Run by scripts/sql_binary_e2e.sh with the connection string as the one
argument. Prints one ok/FAIL line per check and a summary; exits 1 on
any failure.
"""

import datetime as dt
import sys
from decimal import Decimal
from uuid import UUID

import psycopg

passed = 0
failed = 0


def check(desc, got, want):
    global passed, failed
    if got == want:
        passed += 1
        print(f"ok: {desc}")
    else:
        failed += 1
        print(f"FAIL: {desc} — got {got!r}, want {want!r}")


def main(dsn):
    conn = psycopg.connect(dsn, autocommit=True)
    text = conn.cursor(binary=False)
    bin_ = conn.cursor(binary=True)

    text.execute(
        "CREATE TABLE bt (id int4 PRIMARY KEY, b bool, i2 int2, i8 int8, "
        "f4 float4, f8 float8, t text, v varchar(5), c char(3), by bytea, "
        "d date, ts timestamp, tz timestamptz, u2 uint2, u4 uint4, u8 uint8);"
    )
    text.execute(
        "INSERT INTO bt VALUES (1, true, -7, 4611686018427387904, 1.5, 2.25, "
        "'hé', 'ab', 'ab', '\\x0001', '2024-02-29', "
        "'2024-02-29 12:34:56.789', '2024-02-29 12:34:56.789+00', "
        "65535, 4294967295, 18446744073709551615);"
    )
    text.execute("INSERT INTO bt (id) VALUES (2);")

    # --- every type through a binary cursor ---------------------------
    bin_.execute("SELECT * FROM bt WHERE id = %s", (1,))
    row = bin_.fetchone()
    check("binary: every column came back in binary",
          [bin_.pgresult.fformat(i) for i in range(len(row))],
          [1] * len(row))
    check("binary: int4, bool, int2, int8", row[:4],
          (1, True, -7, 4611686018427387904))
    check("binary: float4", row[4], 1.5)
    check("binary: float8", row[5], 2.25)
    check("binary: text, varchar, char(3) padded", row[6:9],
          ("hé", "ab", "ab "))
    check("binary: bytea", bytes(row[9]), b"\x00\x01")
    check("binary: date", row[10], dt.date(2024, 2, 29))
    check("binary: timestamp", row[11],
          dt.datetime(2024, 2, 29, 12, 34, 56, 789000))
    check("binary: timestamptz instant", row[12].timestamp(),
          dt.datetime(2024, 2, 29, 12, 34, 56, 789000,
                      tzinfo=dt.timezone.utc).timestamp())
    check("binary: uint2 as int4, uint4 as int8", row[13:15],
          (65535, 4294967295))
    check("binary: uint8 as numeric", row[15], Decimal(18446744073709551615))

    bin_.execute("SELECT * FROM bt WHERE id = %s", (2,))
    row = bin_.fetchone()
    check("binary: NULL in every column", row[1:], (None,) * 15)

    # --- the same row in text, unchanged ------------------------------
    text.execute("SELECT b, i8, f8, c, by, d, u8 FROM bt WHERE id = 1")
    row = text.fetchone()
    check("text: the same row", row[:4] + row[5:],
          (True, 4611686018427387904, 2.25, "ab ", dt.date(2024, 2, 29),
           Decimal(18446744073709551615)))
    check("text: bytea in hex", bytes(row[4]), b"\x00\x01")

    # --- binary parameters ---------------------------------------------
    bin_.execute(
        "INSERT INTO bt (id, b, i2, i8, f8, t, by, d, ts) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (3, False, 5, 10**12, 3.5, "x", b"\xff", dt.date(1999, 12, 31),
         dt.datetime(1999, 12, 31, 23, 59, 59)),
    )
    bin_.execute("SELECT b, i2, i8, f8, t, by, d, ts FROM bt WHERE id = %s",
                 (3,))
    check("binary params: read back", bin_.fetchone(),
          (False, 5, 10**12, 3.5, "x", b"\xff", dt.date(1999, 12, 31),
           dt.datetime(1999, 12, 31, 23, 59, 59)))

    # --- UUID binary parameter and result ------------------------------
    uid = UUID("00112233-4455-6677-8899-aabbccddeeff")
    text.execute("CREATE TABLE bu (id int PRIMARY KEY, uid uuid)")
    bin_.execute("INSERT INTO bu VALUES (1, %b)", (uid,))
    bin_.execute("SELECT uid FROM bu WHERE id = 1")
    row = bin_.fetchone()

    check("UUID: result OID", bin_.pgresult.ftype(0), 2950)
    check("UUID: result format is binary", bin_.pgresult.fformat(0), 1)
    check("UUID: binary round-trip", row, (uid,))

    text.execute("DROP TABLE bu")

    # --- past the prepare threshold: a named statement ----------------
    for i in range(7):
        bin_.execute("SELECT i8 FROM bt WHERE id = %s", (1,))
        check(f"named statement, execution {i + 1}", bin_.fetchone(),
              (4611686018427387904,))

    # --- the other row shapes ------------------------------------------
    bin_.execute("SELECT count(*) FROM bt")
    check("binary: count(*)", bin_.fetchone(), (3,))
    bin_.execute("SELECT 1")
    check("binary: SELECT 1", bin_.fetchone(), (1,))
    bin_.execute("SELECT 2.5, 'x', true, NULL")
    check("binary: SELECT constants", bin_.fetchone(),
          (Decimal("2.5"), "x", True, None))
    bin_.execute("INSERT INTO bt (id, i8) VALUES (%s, %s) RETURNING id, i8",
                 (4, 7))
    check("binary: RETURNING", bin_.fetchone(), (4, 7))
    bin_.execute("SELECT upper(t), i8 * 2 FROM bt WHERE id = %s", (3,))
    check("binary: expressions", bin_.fetchone(), ("X", 2 * 10**12))
    bin_.execute("UPDATE bt SET i8 = i8 + %s WHERE id = %s RETURNING i8",
                 (1, 4))
    check("binary: UPDATE ... RETURNING", bin_.fetchone(), (8,))

    # --- numeric: Decimal both ways, rounded to the column's scale -----
    text.execute("CREATE TABLE nb (id int4 PRIMARY KEY, p numeric(10, 2), r numeric)")
    bin_.execute("INSERT INTO nb VALUES (%s, %s, %s)",
                 (1, Decimal("1.005"), Decimal("0.321")))
    bin_.execute("SELECT p, r FROM nb WHERE id = %s", (1,))
    check("binary: numeric params and results", bin_.fetchone(),
          (Decimal("1.01"), Decimal("0.321")))
    text.execute("SELECT p, r FROM nb WHERE id = 1")
    check("text: numeric", text.fetchone(), (Decimal("1.01"), Decimal("0.321")))
    bin_.execute("SELECT r * %s FROM nb WHERE id = %s", (Decimal("3"), 1))
    check("binary: numeric arithmetic", bin_.fetchone(), (Decimal("0.963"),))
    text.execute("DROP TABLE nb")

    # --- a statement's Describe: Parse + Describe with no Bind ---------
    # A $n in a target has no value yet, only the type Parse declared:
    # psycopg's pgconn sends the two messages as libpq's PQprepare and
    # PQdescribePrepared do, and the result carries the row's OIDs and
    # the parameter OIDs. Those echo Parse: none declared is none
    # listed, where Postgres would infer them.
    def describe(sql, param_types=None):
        name = b"d"
        res = conn.pgconn.prepare(name, sql.encode(), param_types)
        if res.status != psycopg.pq.ExecStatus.COMMAND_OK:
            return res.error_message.decode()
        res = conn.pgconn.describe_prepared(name)
        if res.status != psycopg.pq.ExecStatus.COMMAND_OK:
            return res.error_message.decode()
        fields = [(res.fname(i).decode(), res.ftype(i))
                  for i in range(res.nfields)]
        params = [res.param_type(i) for i in range(res.nparams)]
        text.execute("DEALLOCATE d")
        return fields, params

    TEXT, INT4, INT8, NUMERIC = 25, 23, 20, 1700
    check("describe: an undeclared $1 under || is text",
          describe("SELECT t || $1 AS c FROM bt"), ([("c", TEXT)], []))
    check("describe: an undeclared $1 under + takes the column's type",
          describe("SELECT i8 + $1 AS m FROM bt"), ([("m", INT8)], []))
    check("describe: a bare undeclared $1 is text",
          describe("SELECT $1 AS p FROM bt"), ([("p", TEXT)], []))
    check("describe: a bare $1 declared int4 is int4, and echoed",
          describe("SELECT $1 AS p FROM bt", [INT4]), ([("p", INT4)], [INT4]))
    check("describe: i8 * $1 declared numeric is numeric",
          describe("SELECT i8 * $1 AS m FROM bt", [NUMERIC]),
          ([("m", NUMERIC)], [NUMERIC]))
    check("describe: RETURNING with a $n",
          describe("INSERT INTO bt (id) VALUES ($1) RETURNING id, t || $2"),
          ([("id", INT4), ("?column?", TEXT)], []))

    text.execute("DROP TABLE bt")
    conn.close()


if __name__ == "__main__":
    main(sys.argv[1])
    print()
    print(f"{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
