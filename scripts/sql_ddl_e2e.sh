#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# End-to-end smoke test for SQL DDL (CREATE/DROP DATABASE + TABLE + INDEX)
# pgwire endpoint. Builds, starts a fresh single-node egres, drives psql
# through the DDL matrix (including a restart-persistence leg), and greps
# the server log. Wipes $DATA (default /data/egres) — throw-away by
# convention; override it with DATA=<dir> (must be a real fs — SSTs are
# O_DIRECT).
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${DATA:-/data/egres}"
LOG=/tmp/zdb/server.log
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"
A="host=127.0.0.1 port=3210 dbname=app sslmode=disable"

pass=0
fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; fail=$((fail + 1)); }

# expect <desc> <needle> <cmd...>: run psql, assert output contains needle.
expect() {
    local desc=$1 needle=$2
    shift 2
    local out
    out=$("$@" 2>&1) || true
    if grep -q "$needle" <<<"$out"; then ok "$desc"; else bad "$desc — got: $out"; fi
}

# Ready = listener up AND election won — the listener opens a few hundred
# ms before become-leader, and proposals before that fail NotCommitted.
start_server() {
    ./zig-out/bin/egres --raft="id=1" \
        --store="dir_path=$DATA/sst" --wal="dir=$DATA/wal" >"$LOG" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 50); do
        if grep -q "sql listener started" "$LOG" 2>/dev/null &&
            grep -q "become-leader" "$LOG" 2>/dev/null; then return; fi
        sleep 0.2
    done
    echo "server never became ready"; exit 1
}

stop_server() {
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}

zig build
rm -rf "$DATA" && mkdir -p "$DATA" /tmp/zdb
start_server
trap stop_server EXIT

# --- create / duplicate / IF NOT EXISTS ------------------------------------
expect "CREATE TABLE" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE t (id int PRIMARY KEY, name text);"
expect "duplicate CREATE errors" "already exists" psql "$E" -X -c "CREATE TABLE t (id int);"
expect "IF NOT EXISTS is quiet" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE IF NOT EXISTS t (id int);"

# --- semantic rejections carry reasons + 0A000 ------------------------------
expect "NOT NULL rejected" "column constraints" \
    psql "$E" -X -c "CREATE TABLE u (a int NOT NULL);"
expect "serial rejected by name" "serial" psql "$E" -X -c "CREATE TABLE u (a serial);"
expect "composite pkey rejected" "table-level" psql "$E" -X -c "CREATE TABLE u (a int, b int, PRIMARY KEY (a, b));"

expect "interval pkey rejected" "interval columns" \
    psql "$E" -X -c "CREATE TABLE u (a interval PRIMARY KEY);"

# --- drop / missing / IF EXISTS / id never reused ---------------------------
expect "DROP TABLE" "DROP TABLE" psql "$E" -X -c "DROP TABLE t;"
expect "second DROP errors" "does not exist" psql "$E" -X -c "DROP TABLE t;"
expect "IF EXISTS is quiet" "DROP TABLE" psql "$E" -X -c "DROP TABLE IF EXISTS t;"
expect "re-CREATE after drop" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE t (id int8 PRIMARY KEY);"
# First 't' consumed 16 (table) + 17 (its _pkey index); re-CREATE gets 18.
grep -q "created table 't' -> ns=16 table_id=18" "$LOG" && ok "dropped ids not reused (got 18)" || bad "table_id reuse"

# --- unknown database fails per query ---------------------------------------
expect "unknown dbname errors" "database does not exist" \
    psql "host=127.0.0.1 port=3210 dbname=nope sslmode=disable" -X -c "CREATE TABLE x (a int);"

# --- restart: names + id counter survive WAL replay -------------------------
stop_server
start_server
expect "'t' survives restart" "already exists" psql "$E" -X -c "CREATE TABLE t (id int);"
expect "post-restart CREATE" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE post_restart (a int4 PRIMARY KEY);"
# Ids so far in ns16: t=16,17 then re-t=18,19 → next table is 20.
grep -q "created table 'post_restart' -> ns=16 table_id=20" "$LOG" && ok "id counter survived restart (20)" || bad "id counter reset"

# --- PK auto-index: names share the table namespace, DROP TABLE rejects it --
# post_restart has a PK, so its 'post_restart_pkey' index tree + name exist.
expect "index name reserved in table namespace" "already exists" \
    psql "$E" -X -c "CREATE TABLE post_restart_pkey (a int);"
expect "DROP TABLE on an index is rejected" "cannot DROP TABLE an index" \
    psql "$E" -X -c "DROP TABLE post_restart_pkey;"
# A table named <x>_pkey blocks a later 'x' whose PK index wants that name.
expect "CREATE TABLE mine_pkey" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE mine_pkey (a int);"
expect "PK index name collision fails CREATE" "already exists" \
    psql "$E" -X -c "CREATE TABLE mine (id int PRIMARY KEY);"

# --- restart from a raft snapshot: names must ride in the snapshot ----------
# Once a snapshot is taken the WAL below it is gone, so the database and
# table names have to be in the snapshot itself (engine.replayNames).
# A tiny memtable, flushed as soon as it is frozen, makes the rows
# below flush, which lets the snapshot reach past the CREATE TABLE; the
# snapshot flags make the leader take one within a second or two.
stop_server
./zig-out/bin/egres --raft="id=1" \
    --store="dir_path=$DATA/sst" --wal="dir=$DATA/wal" \
    --zdb="table-bytes=4096;flush-min-memtables=1" \
    --snapshot="min-bytes=1;min-age-seconds=1;poll-interval-seconds=1" \
    >"$LOG" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 50); do
    if grep -q "sql listener started" "$LOG" 2>/dev/null &&
        grep -q "become-leader" "$LOG" 2>/dev/null; then break; fi
    sleep 0.2
done
psql "$E" -X -qc "CREATE TABLE snap_t (a int PRIMARY KEY, s text);" >/dev/null
pad=$(head -c 200 /dev/zero | tr '\0' x)
rows=$(for i in $(seq 1 40); do printf "(%d,'%s')," "$i" "$pad"; done)
psql "$E" -X -qc "INSERT INTO snap_t VALUES ${rows%,};" >/dev/null
# Wait for a snapshot whose index is past snap_t's create_tree entry.
covered=0
for _ in $(seq 1 150); do
    created=$(grep -oE "enter idx=[0-9]+ cmd=create_tree" "$LOG" | tail -1 |
        grep -oE "[0-9]+" | head -1 || true)
    snap=$(grep -oE "snapshot proposed: index=[0-9]+" "$LOG" | tail -1 |
        grep -oE "[0-9]+" || true)
    if [ -n "$created" ] && [ -n "$snap" ] && [ "$snap" -ge "$created" ]; then
        covered=1; break
    fi
    sleep 0.2
done
if [ "$covered" = 1 ]; then ok "snapshot covers the DDL"; else
    bad "no snapshot past the CREATE TABLE (create=$created snap=$snap)"; fi
# The snapshot's timestamp is the read floor: 0 would mean compaction
# discards no old version and no orphan file is ever removed. It is
# the timestamp of the entry at the snapshot index, a data entry here.
floor=0
for _ in $(seq 1 50); do
    floor=$(grep -oE "read floor: snapshot index=[0-9]+ timestamp=[0-9]+" \
        "$LOG" | tail -1 | grep -oE "[0-9]+$" || true)
    if [ -n "$floor" ] && [ "$floor" != 0 ]; then break; fi
    sleep 0.2
done
if [ -n "$floor" ] && [ "$floor" != 0 ]; then
    ok "snapshot raises the read floor"
else bad "read floor stayed at 0 after a snapshot (floor=$floor)"; fi

# --- an open transaction holds the read floor ------------------------------
# One session reads, waits and reads again inside a transaction, while
# another writes in three rounds. With these flags each round freezes
# and flushes a memtable, which lets a snapshot land between rounds,
# past the point the open transaction reads at. Each snapshot carries
# the read floor, and the leader must keep that below the open
# transaction: its second read is answered, and sees what its first
# did. A floor that was the snapshot entry's own time refuses it with
# "snapshot too old" (checked by making ReadMarks.minReadTs ignore
# the open reads: this then fails).
HELD=/tmp/zdb/held.out
psql "$E" -X -qc "CREATE TABLE held_t (a int PRIMARY KEY, s text);" >/dev/null
psql "$E" -X -q -t >"$HELD" 2>&1 <<'SQL' &
BEGIN;
SELECT count(*) FROM snap_t;
\! sleep 7
SELECT count(*) FROM snap_t;
COMMIT;
SQL
held_pid=$!
sleep 1
for round in 0 1 2; do
    rows=$(for i in $(seq 1 40); do
        printf "(%d,'%s')," "$((round * 40 + i))" "$pad"; done)
    psql "$E" -X -qc "INSERT INTO held_t VALUES ${rows%,};" >/dev/null
    sleep 1.5
done
wait "$held_pid" || true
if [ "$(grep -cE '^ *40$' "$HELD")" = 2 ] && ! grep -q ERROR "$HELD"; then
    ok "an open transaction holds the read floor"
else bad "held transaction: $(tr '\n' ' ' <"$HELD")"; fi
stop_server
start_server
expect "rows resolve after a snapshot restart" "(40 rows)" \
    psql "$E" -X -c "SELECT * FROM snap_t;"
expect "table name survives a snapshot restart" "already exists" \
    psql "$E" -X -c "CREATE TABLE snap_t (a int PRIMARY KEY);"
expect "post_restart still known" "already exists" \
    psql "$E" -X -c "CREATE TABLE post_restart (a int4 PRIMARY KEY);"
psql "$E" -X -qc "DROP TABLE snap_t;" >/dev/null

# --- per-database name scoping + drop cascades ------------------------------
expect "CREATE DATABASE app" "CREATE DATABASE" psql "$E" -X -c "CREATE DATABASE app;"
expect "same table name in app" "CREATE TABLE" psql "$A" -X -c "CREATE TABLE t (x uint8);"
expect "DROP DATABASE with tables" "DROP DATABASE" psql "$E" -X -c "DROP DATABASE app;"
expect "dropped-db session fails" "database does not exist" psql "$A" -X -c "CREATE TABLE y (a int);"

# --- CREATE DATABASE ... TEMPLATE: a branch of the template ----------------
# The branch holds every row of the source as of the statement, rows
# flushed to SSTs (the restart above flushed the early ones) and rows
# still in memtables alike; after it, each side's writes stay its own.
B="host=127.0.0.1 port=3210 dbname=app2 sslmode=disable"
psql "$E" -X -qc "CREATE DATABASE app;" >/dev/null
psql "$A" -X -qc "CREATE TABLE br (id int PRIMARY KEY, v text);" >/dev/null
psql "$A" -X -qc "CREATE INDEX br_v ON br (v);" >/dev/null
psql "$A" -X -qc "INSERT INTO br VALUES (1,'one'),(2,'two'),(3,'one');" >/dev/null
expect "CREATE DATABASE TEMPLATE" "CREATE DATABASE" \
    psql "$E" -X -c "CREATE DATABASE app2 TEMPLATE app;"
expect "missing source errors" "template database does not exist" \
    psql "$E" -X -c "CREATE DATABASE app3 TEMPLATE nope;"
expect "branch has the rows" "(3 rows)" psql "$B" -X -c "SELECT id FROM br;"
expect "branch index answers" "(2 rows)" \
    psql "$B" -X -c "SELECT id FROM br WHERE v = 'one';"
expect "branch takes its own writes" "INSERT 0 1" \
    psql "$B" -X -c "INSERT INTO br VALUES (4,'four');"
expect "source write stays in the source" "UPDATE 1" \
    psql "$A" -X -c "UPDATE br SET v = 'uno' WHERE id = 1;"
expect "branch unchanged by the source" "(2 rows)" \
    psql "$B" -X -c "SELECT id FROM br WHERE v = 'one';"
expect "source unchanged by the branch" "(3 rows)" \
    psql "$A" -X -c "SELECT id FROM br;"
expect "branch sees its own row" "(4 rows)" psql "$B" -X -c "SELECT id FROM br;"
expect "branch allocates fresh row ids" "INSERT 0 1" \
    psql "$B" -X -c "INSERT INTO br VALUES (5,'five');"
expect "DROP the source" "DROP DATABASE" psql "$E" -X -c "DROP DATABASE app;"
expect "branch outlives the source" "(5 rows)" \
    psql "$B" -X -c "SELECT id FROM br;"
stop_server
start_server
expect "branch survives a restart" "(5 rows)" psql "$B" -X -c "SELECT id FROM br;"
expect "branch index survives a restart" "(2 rows)" \
    psql "$B" -X -c "SELECT id FROM br WHERE v = 'one';"
psql "$E" -X -qc "DROP DATABASE app2;" >/dev/null

# --- CREATE TABLE ... AS TABLE: a branch of one table ------------------------
# The branch holds every row of the source as of the statement, with
# its indexes; after it, each side's writes stay its own. WITH NO DATA
# takes the definition and the indexes, empty.
psql "$E" -X -qc "CREATE DATABASE tb;" >/dev/null
T="host=127.0.0.1 port=3210 dbname=tb sslmode=disable"
psql "$T" -X -qc "CREATE TABLE src (id int PRIMARY KEY, v text, u int UNIQUE);" \
    >/dev/null
psql "$T" -X -qc "CREATE INDEX src_v ON src (v);" >/dev/null
psql "$T" -X -qc "INSERT INTO src VALUES (1,'one',10),(2,'two',20),(3,'one',30);" \
    >/dev/null
expect "CREATE TABLE AS TABLE" "CREATE TABLE AS" \
    psql "$T" -X -c "CREATE TABLE dst AS TABLE src;"
expect "missing source errors" "source table does not exist" \
    psql "$T" -X -c "CREATE TABLE dst2 AS TABLE nope;"
expect "duplicate branch name" "already exists" \
    psql "$T" -X -c "CREATE TABLE dst AS TABLE src;"
expect "branch has the rows" "(3 rows)" psql "$T" -X -c "SELECT id FROM dst;"
expect "branch index answers" "(2 rows)" \
    psql "$T" -X -c "SELECT id FROM dst WHERE v = 'one';"
expect "branch pkey answers" "(1 row)" \
    psql "$T" -X -c "SELECT v FROM dst WHERE id = 2;"
expect "branch unique index holds" "duplicate key" \
    psql "$T" -X -c "INSERT INTO dst VALUES (9,'nine',10);"
expect "branch takes its own writes" "INSERT 0 1" \
    psql "$T" -X -c "INSERT INTO dst VALUES (4,'four',40);"
expect "source write stays in the source" "UPDATE 1" \
    psql "$T" -X -c "UPDATE src SET v = 'uno' WHERE id = 1;"
expect "branch unchanged by the source" "(2 rows)" \
    psql "$T" -X -c "SELECT id FROM dst WHERE v = 'one';"
expect "source unchanged by the branch" "(3 rows)" \
    psql "$T" -X -c "SELECT id FROM src;"
expect "branch sees its own row" "(4 rows)" psql "$T" -X -c "SELECT id FROM dst;"
expect "branch index names follow the branch" "CREATE INDEX" \
    psql "$T" -X -c "CREATE INDEX IF NOT EXISTS dst_v_idx ON dst (v);"
expect "WITH NO DATA is empty" "CREATE TABLE AS" \
    psql "$T" -X -c "CREATE TABLE empty AS TABLE src WITH NO DATA;"
expect "empty branch has no rows" "(0 rows)" \
    psql "$T" -X -c "SELECT id FROM empty;"
expect "empty branch has the index" "already indexed" \
    psql "$T" -X -c "CREATE INDEX another ON empty (v);"
expect "empty branch takes rows" "INSERT 0 1" \
    psql "$T" -X -c "INSERT INTO empty VALUES (1,'one',10);"
expect "DROP the source" "DROP TABLE" psql "$T" -X -c "DROP TABLE src;"
expect "branch outlives the source" "(4 rows)" \
    psql "$T" -X -c "SELECT id FROM dst;"
stop_server
start_server
expect "table branch survives a restart" "(4 rows)" \
    psql "$T" -X -c "SELECT id FROM dst;"
expect "table branch index survives a restart" "(2 rows)" \
    psql "$T" -X -c "SELECT id FROM dst WHERE v = 'one';"
psql "$E" -X -qc "DROP DATABASE tb;" >/dev/null

# --- secondary indexes: create / backfill / DML / drop ----------------------
expect "CREATE TABLE items" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE items (id int PRIMARY KEY, color text);"
expect "INSERT pre-index rows" "INSERT 0 3" psql "$E" -X -c "INSERT INTO items VALUES (1,'blue'),(2,'red'),(3,'blue');"
# Blocking online build: the reply means built + valid (backfill done).
expect "CREATE INDEX backfills" "CREATE INDEX" psql "$E" -X -c "CREATE INDEX items_color ON items (color);"
expect "duplicate index name" "already exists" psql "$E" -X -c "CREATE INDEX items_color ON items (color);"
expect "IF NOT EXISTS is quiet (index)" "CREATE INDEX" psql "$E" -X -c "CREATE INDEX IF NOT EXISTS items_color ON items (color);"
expect "second index on the column" "already indexed" psql "$E" -X -c "CREATE INDEX another ON items (color);"
expect "second index on the column, unique" "already indexed" \
    psql "$E" -X -c "CREATE UNIQUE INDEX u ON items (color);"
expect "index on the PK column" "already indexed" \
    psql "$E" -X -c "CREATE INDEX i_id ON items (id);"
expect "index on missing column" "column does not exist" psql "$E" -X -c "CREATE INDEX i2 ON items (nope);"
expect "index on missing table" "table does not exist" psql "$E" -X -c "CREATE INDEX i3 ON nope (a);"

# DML maintains postings; equality on the indexed column now routes
# through the posting bitmap (getBitmap + own-txn overlay).
expect "SELECT backfilled matches" "(2 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'blue';"
expect "INSERT with index live" "INSERT 0 1" psql "$E" -X -c "INSERT INTO items VALUES (4,'green');"
expect "UPDATE moves the index key" "UPDATE 1" psql "$E" -X -c "UPDATE items SET color = 'blue' WHERE id = 4;"
expect "DELETE with index live" "DELETE 1" psql "$E" -X -c "DELETE FROM items WHERE id = 2;"
expect "SELECT after churn" "(3 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'blue';"

# Bitmap-path semantics: own-txn pending postings, key moves in bulk,
# NULL (3VL `= NULL` vs IS NULL through the index's NULL key), and
# indexed UPDATE/DELETE (lock-through-bitmap).
expect "own pending INSERT visible via index" "(4 rows)" psql "$E" -X <<'PSQL'
BEGIN;
INSERT INTO items VALUES (6,'blue');
SELECT id FROM items WHERE color = 'blue';
ROLLBACK;
PSQL
expect "rollback leaves postings untouched" "(3 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'blue';"
expect "UPDATE routed via the index" "UPDATE 3" psql "$E" -X -c "UPDATE items SET color = 'cyan' WHERE color = 'blue';"
expect "moved rows found under the new key" "(3 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'cyan';"
expect "old key is empty" "(0 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'blue';"
expect "move the rows back" "UPDATE 3" psql "$E" -X -c "UPDATE items SET color = 'blue' WHERE color = 'cyan';"
expect "NULL cell inserted" "INSERT 0 1" psql "$E" -X -c "INSERT INTO items VALUES (7, NULL);"
expect "= NULL matches nothing" "(0 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = NULL;"
expect "IS NULL routed via the index" "(1 row)" psql "$E" -X -c "SELECT id FROM items WHERE color IS NULL;"
expect "IS NOT NULL scans" "(3 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color IS NOT NULL;"
expect "DELETE = NULL is 0" "DELETE 0" psql "$E" -X -c "DELETE FROM items WHERE color = NULL;"
expect "DELETE IS NULL routed via the index" "DELETE 1" psql "$E" -X -c "DELETE FROM items WHERE color IS NULL;"

# DROP INDEX and its rejections.
expect "DROP TABLE of an index name" "use DROP INDEX" psql "$E" -X -c "DROP TABLE items_color;"
expect "DROP INDEX of a base table" "not an index" psql "$E" -X -c "DROP INDEX items;"
expect "cannot drop the pkey index" "PRIMARY KEY index" psql "$E" -X -c "DROP INDEX items_pkey;"
expect "DROP INDEX" "DROP INDEX" psql "$E" -X -c "DROP INDEX items_color;"
expect "DROP INDEX missing errors" "does not exist" psql "$E" -X -c "DROP INDEX items_color;"
expect "DROP INDEX IF EXISTS quiet" "DROP INDEX" psql "$E" -X -c "DROP INDEX IF EXISTS items_color;"
expect "SELECT after drop still answers" "(3 rows)" psql "$E" -X -c "SELECT id FROM items WHERE color = 'blue';"
expect "re-CREATE freed index name" "CREATE INDEX" psql "$E" -X -c "CREATE INDEX items_color ON items (color);"

# --- UNIQUE indexes: the column constraint, CREATE UNIQUE INDEX, DROP ------
# A UNIQUE column gets its own index "<table>_<column>_key" with the
# table; a user index built over a populated table enforces from the
# reply on; a build over duplicates fails and frees its name and
# column; a user unique index drops like any other, the PK's does not.
expect "CREATE TABLE uq" "CREATE TABLE" psql "$E" -X \
    -c "CREATE TABLE uq (id int PRIMARY KEY, email text, code int4 UNIQUE);"
expect "UNIQUE column index name taken" "already exists" \
    psql "$E" -X -c "CREATE TABLE uq_code_key (a int);"
expect "INSERT uq rows" "INSERT 0 2" \
    psql "$E" -X -c "INSERT INTO uq VALUES (1,'a@x',10),(2,'b@x',20);"
expect "UNIQUE column enforced" "duplicate key" \
    psql "$E" -X -c "INSERT INTO uq VALUES (3,'c@x',10);"
expect "UNIQUE column routes equality" "(1 row)" \
    psql "$E" -X -c "SELECT id FROM uq WHERE code = 20;"
expect "CREATE UNIQUE INDEX over rows" "CREATE INDEX" \
    psql "$E" -X -c "CREATE UNIQUE INDEX uq_email ON uq (email);"
expect "built unique index enforced" "duplicate key" \
    psql "$E" -X -c "INSERT INTO uq VALUES (3,'a@x',30);"
expect "built unique index routes equality" "(1 row)" \
    psql "$E" -X -c "SELECT id FROM uq WHERE email = 'b@x';"
expect "two NULLs under a unique index" "INSERT 0 2" \
    psql "$E" -X -c "INSERT INTO uq VALUES (4,NULL,40),(5,NULL,50);"
expect "NULL into the PK column" "not-null" \
    psql "$E" -X -c "INSERT INTO uq VALUES (NULL,'z@x',60);"
expect "DROP a user unique index" "DROP INDEX" \
    psql "$E" -X -c "DROP INDEX uq_email;"
expect "dropped unique index no longer enforces" "INSERT 0 1" \
    psql "$E" -X -c "INSERT INTO uq VALUES (6,'a@x',60);"
expect "DROP the UNIQUE column's index" "DROP INDEX" \
    psql "$E" -X -c "DROP INDEX uq_code_key;"
expect "dropped column index no longer enforces" "INSERT 0 1" \
    psql "$E" -X -c "INSERT INTO uq VALUES (7,'d@x',10);"
expect "CREATE TABLE dup" "CREATE TABLE" \
    psql "$E" -X -c "CREATE TABLE dup (id int PRIMARY KEY, v int4);"
expect "INSERT duplicate values" "INSERT 0 3" \
    psql "$E" -X -c "INSERT INTO dup VALUES (1,5),(2,5),(3,6);"
expect "unique build over duplicates fails" "could not create unique index" \
    psql "$E" -X -c "CREATE UNIQUE INDEX dup_v ON dup (v);"
expect "failed build freed the name and column" "CREATE INDEX" \
    psql "$E" -X -c "CREATE INDEX dup_v ON dup (v);"
expect "the posting index answers" "(2 rows)" \
    psql "$E" -X -c "SELECT id FROM dup WHERE v = 5;"
expect "CREATE TABLE em" "CREATE TABLE" \
    psql "$E" -X -c "CREATE TABLE em (a int4, b text);"
expect "CREATE UNIQUE INDEX on an empty table" "CREATE INDEX" \
    psql "$E" -X -c "CREATE UNIQUE INDEX em_a ON em (a);"
expect "INSERT under the empty-built index" "INSERT 0 1" \
    psql "$E" -X -c "INSERT INTO em VALUES (1,'x');"
expect "empty-built index enforced" "duplicate key" \
    psql "$E" -X -c "INSERT INTO em VALUES (1,'y');"

# --- routing: prove indexed equality does NOT scan --------------------------
# SI discriminator: a statement that RAN A SCAN poisons any later
# read_ts bump in its txn (a scan can't be revalidated → 40001), while
# an index-routed read revalidates by its posting key and the bump
# succeeds. Same interleaving both times — read, concurrent commit on an
# unrelated row, then lock that row; success on the first leg is the
# proof the indexed SELECT never scanned.
expect "CREATE TABLE rt" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE rt (id int PRIMARY KEY, c text, d text);"
expect "CREATE INDEX rt_c" "CREATE INDEX" psql "$E" -X -c "CREATE INDEX rt_c ON rt (c);"
expect "INSERT rt rows" "INSERT 0 2" psql "$E" -X -c "INSERT INTO rt VALUES (1,'x','p'),(2,'y','q');"
expect "indexed read, then bump succeeds" "UPDATE 1" psql "$E" -X <<'PSQL'
BEGIN;
SELECT id FROM rt WHERE c = 'x';
\! psql "host=127.0.0.1 port=3210 dbname=egres sslmode=disable" -X -c "UPDATE rt SET d='q2' WHERE id = 2;" >/dev/null
UPDATE rt SET d = 'r' WHERE id = 2;
COMMIT;
PSQL
expect "scan read, then bump conflicts" "could not serialize" psql "$E" -X <<'PSQL'
BEGIN;
SELECT id FROM rt WHERE d = 'p';
\! psql "host=127.0.0.1 port=3210 dbname=egres sslmode=disable" -X -c "UPDATE rt SET d='s' WHERE id = 1;" >/dev/null
UPDATE rt SET d = 't' WHERE id = 1;
COMMIT;
PSQL
# Boolean WHERE: an indexed conjunct drives the read wherever it sits
# in the AND (the unindexed one is a residual, not a scan); an OR with
# an unindexed disjunct must scan.
expect "AND with an indexed conjunct routes" "UPDATE 1" psql "$E" -X <<'PSQL'
BEGIN;
SELECT id FROM rt WHERE c = 'x' AND d = 's';
\! psql "host=127.0.0.1 port=3210 dbname=egres sslmode=disable" -X -c "UPDATE rt SET d='q3' WHERE id = 2;" >/dev/null
UPDATE rt SET d = 'r2' WHERE id = 2;
COMMIT;
PSQL
expect "indexed conjunct routes from second position" "UPDATE 1" psql "$E" -X <<'PSQL'
BEGIN;
SELECT id FROM rt WHERE d = 's' AND c = 'x';
\! psql "host=127.0.0.1 port=3210 dbname=egres sslmode=disable" -X -c "UPDATE rt SET d='q4' WHERE id = 2;" >/dev/null
UPDATE rt SET d = 'r3' WHERE id = 2;
COMMIT;
PSQL
expect "OR with an unindexed disjunct scans" "could not serialize" psql "$E" -X <<'PSQL'
BEGIN;
SELECT id FROM rt WHERE c = 'x' OR d = 'zz';
\! psql "host=127.0.0.1 port=3210 dbname=egres sslmode=disable" -X -c "UPDATE rt SET d='s2' WHERE id = 1;" >/dev/null
UPDATE rt SET d = 't2' WHERE id = 1;
COMMIT;
PSQL
expect "DROP TABLE rt" "DROP TABLE" psql "$E" -X -c "DROP TABLE rt;"

# Restart: index tree, catalog row, and backfilled SSTs survive replay.
stop_server
start_server
expect "index survives restart" "already exists" psql "$E" -X -c "CREATE INDEX items_color ON items (color);"
expect "indexed write after restart" "INSERT 0 1" psql "$E" -X -c "INSERT INTO items VALUES (5,'red');"
expect "DROP TABLE cascades the index" "DROP TABLE" psql "$E" -X -c "DROP TABLE items;"
expect "cascaded index name freed" "CREATE TABLE" psql "$E" -X -c "CREATE TABLE items_color (a int);"

# --- char(n) / varchar(n): the length is part of the column ---------------
expect "char(n) and varchar(n) columns" "CREATE TABLE" \
    psql "$E" -X -c "CREATE TABLE cn (id int PRIMARY KEY, a char(5), b varchar(3), c char, d varchar);"
expect "char(0) rejected" "at least 1" psql "$E" -X -c "CREATE TABLE cn0 (a char(0));"
expect "char(1,2) is a syntax error" "syntax error" psql "$E" -X -c "CREATE TABLE cn2 (a char(1,2));"
expect "length on text rejected" "not allowed for type" psql "$E" -X -c "CREATE TABLE cn3 (a text(4));"
expect "DROP char(n) table" "DROP TABLE" psql "$E" -X -c "DROP TABLE cn;"

# --- VECTOR(n): pgwire text input, validation, and output ------------------
expect "VECTOR(n) column" "CREATE TABLE" \
    psql "$E" -X -c "CREATE TABLE vectors (id int PRIMARY KEY, embedding vector(3));"
expect "vector text round-trip" "[0.1,-2,3.25]" \
    psql "$E" -X -At -c "INSERT INTO vectors VALUES (1, '[0.1,-2,3.25]'); SELECT embedding FROM vectors WHERE id = 1;"
expect "wrong vector dimension rejected" "VectorDimensionMismatch" \
    psql "$E" -X -c "INSERT INTO vectors VALUES (2, '[1,2]');"
expect "non-finite vector rejected" "NonFiniteVector" \
    psql "$E" -X -c "INSERT INTO vectors VALUES (3, '[NaN,0,0]');"

# --- no unexpected server-side errors ---------------------------------------
if grep -qE '^[^ ]+ ERROR ' "$LOG"; then bad "server log has errors"; else ok "server log clean"; fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
