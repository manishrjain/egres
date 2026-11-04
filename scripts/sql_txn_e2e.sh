#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Concurrency e2e for interactive transactions over pgwire. Two long-lived
# psql sessions (FIFO-fed) drive: BEGIN/COMMIT/ROLLBACK visibility across
# sessions, read-your-own-writes (point + scan), row-lock blocking with
# FIFO hand-off and last-writer-wins, deadlock detection (40P01 for the
# edge-closer), failed-txn state (25P02, COMMIT→ROLLBACK), DDL-in-txn
# rejection (25001), table intent locks (DROP TABLE queues behind open
# DML txns), read_ts bumps (blocked writer restarts and applies with no
# abort; a stale prior read aborts 40001), SELECT FOR UPDATE (blocks
# writers not readers, deadlocks 40P01, a cell-filter scan locks exactly
# the matches, FOR SHARE / NOWAIT reject), PRIMARY KEY enforcement (unique
# index: 23505 duplicates in-statement + sequential + concurrent, 23502 NULL
# keys, DELETE frees the key, UPDATE-of-PK 0A000, and the index-key lock
# serializing racing inserters), and disconnect rollback + lock release.
# Wipes /data/egres.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${DATA:-/data/egres}"
LOG=/tmp/zdb/server.log
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"
D=$(mktemp -d)

pass=0
fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; fail=$((fail + 1)); }

q() { psql "$E" -X -c "$1"; }     # full output (row counts, tags)
v() { psql "$E" -X -tA -c "$1"; } # tuples-only, unaligned (bare value)

start_server() {
    ./zig-out/bin/egres --raft="id=1" \
        --store="dir_path=$DATA/sst" --wal="dir=$DATA/wal" >"$LOG" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 50); do
        if grep -q "sql listener started" "$LOG" 2>/dev/null &&
            grep -q "become-leader" "$LOG" 2>/dev/null; then return; fi
        sleep 0.2
    done
    echo "server never became ready"
    exit 1
}

stop_server() {
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
trap 'stop_server; rm -rf "$D"' EXIT

# --- session plumbing ------------------------------------------------------
# Session N: a FIFO (kept open on fd 3+N) feeding a long-lived psql whose
# output lands in $D/outN. `sync_session` sends an \echo marker and waits
# for it — i.e. for every statement queued before it to have finished.
# Line-buffered output, so a marker is in outN as soon as psql prints
# it: GNU stdbuf, gstdbuf from Homebrew coreutils, or neither on a bare
# macOS, where psql's own flush after each result has to do.
LINEBUF=$(command -v stdbuf || command -v gstdbuf || true)
[ -n "$LINEBUF" ] && LINEBUF="$LINEBUF -oL -eL"
open_session() { # $1 = 1|2
    mkfifo "$D/in$1"
    # 4>&- 5>&-: don't let this psql inherit the OTHER session's FIFO
    # write end, or closing that fd would never EOF the other psql.
    $LINEBUF psql "$E" -X -tA -f - <"$D/in$1" >"$D/out$1" 2>&1 4>&- 5>&- &
    eval "PID$1=\$!"
    eval "exec $((3 + $1))>\"$D/in$1\""
    send "$1" "\\set VERBOSITY verbose" # errors show their SQLSTATE
}
send() { printf '%s\n' "$2" >&$((3 + $1)); }
MARK=0
sync_session() { # wait (<=10s) for session $1 to drain its queue
    MARK=$((MARK + 1))
    send "$1" "\\echo MARK$MARK"
    for _ in $(seq 1 100); do
        if grep -q "MARK$MARK" "$D/out$1"; then return 0; fi
        sleep 0.1
    done
    bad "session $1 never reached MARK$MARK"
    return 1
}
close_session() {
    eval "exec $((3 + $1))>&-"
    eval "wait \$PID$1" 2>/dev/null || true
}

zig build
rm -rf "$DATA" && mkdir -p "$DATA" /tmp/zdb
start_server

q "CREATE TABLE t (id int4, name text);" >/dev/null
q "INSERT INTO t VALUES (1, 'base');" >/dev/null # egid 1

open_session 1
open_session 2

# --- commit visibility across sessions -------------------------------------
send 1 "BEGIN;"
send 1 "INSERT INTO t VALUES (2, 'pend');" # egid 2
sync_session 1
if [ -z "$(v "SELECT name FROM t WHERE egid = 2;")" ]; then
    ok "uncommitted insert invisible to other sessions"
else bad "uncommitted insert leaked"; fi
send 1 "COMMIT;"
sync_session 1
if [ "$(v "SELECT name FROM t WHERE egid = 2;")" = "pend" ]; then
    ok "committed insert visible"
else bad "committed insert not visible"; fi

# --- read-your-own-writes: point + scan, then rollback ----------------------
send 1 "BEGIN;"
send 1 "INSERT INTO t VALUES (3, 'ryow');" # egid 3 (burned by rollback)
send 1 "SELECT name FROM t WHERE egid = 3;" # point read via pending
send 1 "SELECT name FROM t;"                # scan overlays pending
sync_session 1
if [ "$(grep -c ryow "$D/out1")" = "2" ]; then
    ok "txn reads its own pending insert (point + scan)"
else bad "read-your-own-writes — out1: $(grep ryow "$D/out1" | tr '\n' ' ')"; fi
if [ -z "$(v "SELECT name FROM t WHERE egid = 3;")" ]; then
    ok "pending row invisible to other sessions"
else bad "pending row leaked mid-txn"; fi
send 1 "ROLLBACK;"
sync_session 1
if [ -z "$(v "SELECT name FROM t WHERE egid = 3;")" ]; then
    ok "rolled-back insert never lands"
else bad "rolled-back insert landed"; fi

# --- blocked writer: FIFO hand-off, last writer wins ------------------------
send 1 "BEGIN;"
send 1 "UPDATE t SET name = 's1' WHERE egid = 1;"
sync_session 1
send 2 "UPDATE t SET name = 's2' WHERE egid = 1;" # blocks on the row lock
sleep 0.3
if grep -q "UPDATE 1" "$D/out2"; then
    bad "writer was not blocked by the row lock"
else ok "concurrent writer blocks on the row lock"; fi
send 1 "COMMIT;"
sync_session 2
if grep -q "UPDATE 1" "$D/out2"; then
    ok "blocked writer completes after commit (hand-off)"
else bad "blocked writer never completed"; fi
if [ "$(v "SELECT name FROM t WHERE egid = 1;")" = "s2" ]; then
    ok "last writer wins after hand-off"
else bad "unexpected final value: $(v "SELECT name FROM t WHERE egid = 1;")"; fi

# --- deadlock: closing edge refused with 40P01, victim recovers --------------
# S1 holds row 1 and parks on row 2; S2 holds row 2 and asks for row 1 —
# S2 inserts the cycle's last edge and is refused on the spot (no
# timeout involved; detection runs before anyone parks).
send 1 "BEGIN;"
send 1 "UPDATE t SET name = 'd1' WHERE egid = 1;"
sync_session 1
send 2 "BEGIN;"
send 2 "UPDATE t SET name = 'd2' WHERE egid = 2;"
sync_session 2
send 1 "UPDATE t SET name = 'd1' WHERE egid = 2;" # parks behind S2
sleep 0.3 # S1 is parked mid-statement and cannot answer a sync marker
send 2 "UPDATE t SET name = 'd2' WHERE egid = 1;" # closes the cycle
sync_session 2
if grep -q "40P01" "$D/out2"; then
    ok "deadlock refused at the closing edge with 40P01"
else bad "no 40P01 for the deadlock closer"; fi
send 2 "SELECT name FROM t WHERE egid = 1;"
sync_session 2
if grep -q "25P02" "$D/out2"; then
    ok "failed txn rejects statements with 25P02"
else bad "failed txn accepted a statement"; fi
send 2 "COMMIT;" # of a failed txn -> rolls back
sync_session 2
if grep -q "ROLLBACK" "$D/out2"; then
    ok "COMMIT of a failed txn reports ROLLBACK"
else bad "failed-txn COMMIT did not roll back"; fi
# The victim's abort released row 2 — the survivor's parked UPDATE
# completes (third "UPDATE 1" in out1: hand-off leg + both of S1's).
sync_session 1
if [ "$(grep -c "UPDATE 1" "$D/out1")" = "3" ]; then
    ok "victim's abort unblocked the survivor"
else bad "survivor still blocked after victim aborted"; fi
send 1 "ROLLBACK;"
sync_session 1

# --- DDL inside a txn block is rejected -------------------------------------
send 2 "BEGIN;"
send 2 "CREATE TABLE t2 (a int4);"
sync_session 2
if grep -q "25001" "$D/out2"; then
    ok "DDL in txn block rejected with 25001"
else bad "DDL in txn block was not rejected"; fi
send 2 "ROLLBACK;"
sync_session 2

# --- table intent lock: DROP TABLE queues behind an open DML txn -------------
send 1 "BEGIN;"
send 1 "INSERT INTO t VALUES (7, 'intent');" # shared intent on t
sync_session 1
send 2 "DROP TABLE t;" # exclusive intent — parks behind S1's txn
sleep 0.3
# While the drop is queued, writer preference parks NEW DML on t too,
# so don't touch t from a third connection here.
if grep -q "DROP TABLE" "$D/out2"; then
    bad "DROP TABLE did not wait for the open txn"
else ok "DROP TABLE blocks behind an open txn's intent lock"; fi
send 1 "COMMIT;"
sync_session 2
if grep -q "DROP TABLE" "$D/out2"; then
    ok "DROP TABLE completes once the txn commits"
else bad "DROP TABLE never completed"; fi
# || true: psql exits 1 on the expected error; pipefail would otherwise
# fail the pipeline even when grep matches.
if { q "SELECT name FROM t;" 2>&1 || true; } | grep -q "does not exist"; then
    ok "table is gone after the queued drop"
else bad "table still queryable after drop"; fi

# --- column rename waits for an open reader transaction --------------------
q "CREATE TABLE rename_lock_t (email text);" >/dev/null
q "INSERT INTO rename_lock_t VALUES ('holder@x');" >/dev/null
rename_txn_out=$(psql "$E" -X -v VERBOSITY=verbose 2>&1 <<'SQL'
BEGIN;
ALTER TABLE rename_lock_t RENAME COLUMN email TO contact_email;
ROLLBACK;
SQL
)
if grep -q "25001" <<<"$rename_txn_out"; then
    ok "RENAME COLUMN in txn block rejected with 25001"
else
    bad "RENAME COLUMN in txn block was not rejected"
fi

send 1 "BEGIN;"
send 1 "SELECT email FROM rename_lock_t;"
sync_session 1
if ! grep -qx "holder@x" "$D/out1"; then
    bad "rename lock holder did not read its table"
    exit 1
fi
sync_session 2
send 2 "ALTER TABLE rename_lock_t RENAME COLUMN email TO contact_email;"
send 2 "\\echo RENAME_DONE"
# The completion marker follows the query even when it returns an error.
# Observe a bounded response window after the reader holds the table.
for _ in $(seq 1 10); do
    if grep -qx "RENAME_DONE" "$D/out2"; then break; fi
    sleep 0.1
done
rename_waited=1
if grep -qx "RENAME_DONE" "$D/out2"; then
    rename_waited=0
fi
send 1 "COMMIT;"
sync_session 1
sync_session 2
if [ "$rename_waited" -eq 1 ] && grep -qx "ALTER TABLE" "$D/out2"; then
    ok "RENAME COLUMN waits for the reader txn, then succeeds after commit"
else
    bad "RENAME COLUMN did not wait and succeed after commit"
fi
send 1 "SELECT contact_email FROM rename_lock_t;"
sync_session 1
if [ "$(grep -cx 'holder@x' "$D/out1")" -eq 2 ]; then
    ok "existing reader session sees renamed column after commit"
else
    bad "existing reader session did not see renamed column"
fi
q "DROP TABLE rename_lock_t;" >/dev/null

q "CREATE TABLE t2 (id int4, name text);" >/dev/null
q "INSERT INTO t2 VALUES (1, 'base');" >/dev/null # egid 1

# --- read_ts bump: a blocked writer lands with NO abort ----------------------
# S2's implicit UPDATE parks behind S1's lock; when S1 commits, S2's
# lock acquire finds the newer commit, bumps its read_ts, restarts the
# statement against the fresh snapshot, and applies — no 40001, no
# client retry.
send 1 "BEGIN;"
send 1 "UPDATE t2 SET name = 'b1' WHERE egid = 1;"
sync_session 1
send 2 "UPDATE t2 SET name = 'b2' WHERE egid = 1;" # parks on the row lock
sleep 0.3
send 1 "COMMIT;"
sync_session 2
if [ "$(v "SELECT name FROM t2 WHERE egid = 1;")" = "b2" ]; then
    ok "blocked writer bumped past the commit and applied"
else bad "bump+restart lost the blocked write"; fi
if grep -q "40001" "$D/out2"; then
    bad "bump+restart surfaced a serialization abort"
else ok "no serialization abort during bump+restart"; fi

# --- stale interactive read: the bump refuses and aborts (40001) -------------
# S2 point-reads the row in one statement; a concurrent commit changes
# it; S2's next UPDATE needs a bump, but its prior read is now stale —
# unrepairable (the value already reached the client) → 40001.
send 2 "BEGIN;"
send 2 "SELECT name FROM t2 WHERE egid = 1;"
sync_session 2
q "UPDATE t2 SET name = 'x1' WHERE egid = 1;" >/dev/null
send 2 "UPDATE t2 SET name = 'x2' WHERE egid = 1;"
sync_session 2
if grep -q "40001" "$D/out2"; then
    ok "stale prior read turns the bump into 40001"
else bad "conflicting update after a stale read did not abort"; fi
send 2 "ROLLBACK;"
sync_session 2
if [ "$(v "SELECT name FROM t2 WHERE egid = 1;")" = "x1" ]; then
    ok "aborted txn left the concurrent commit in place"
else bad "unexpected value after abort: $(v "SELECT name FROM t2 WHERE egid = 1;")"; fi

# --- SELECT FOR UPDATE: blocks writers, not readers --------------------------
send 1 "BEGIN;"
send 1 "SELECT name FROM t2 WHERE egid = 1 FOR UPDATE;"
sync_session 1
if grep -q "x1" "$D/out1"; then
    ok "FOR UPDATE returns the locked row"
else bad "FOR UPDATE returned nothing"; fi
NU=$(grep -c "UPDATE 1" "$D/out2" || true)
send 2 "UPDATE t2 SET name = 'w2' WHERE egid = 1;" # parks on S1's row lock
sleep 0.3
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$NU" ]; then
    ok "writer blocks behind FOR UPDATE"
else bad "writer was not blocked by FOR UPDATE"; fi
if [ "$(v "SELECT name FROM t2 WHERE egid = 1;")" = "x1" ]; then
    ok "plain read passes FOR UPDATE without blocking"
else bad "reader blocked or saw uncommitted data"; fi
send 1 "COMMIT;"
sync_session 2
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$((NU + 1))" ] &&
    [ "$(v "SELECT name FROM t2 WHERE egid = 1;")" = "w2" ]; then
    ok "blocked writer lands after FOR UPDATE holder commits"
else bad "blocked writer never landed after commit"; fi

# --- FOR UPDATE deadlock: closing edge refused with 40P01 --------------------
q "INSERT INTO t2 VALUES (2, 'row2');" >/dev/null # egid 2
send 1 "BEGIN;"
send 1 "SELECT name FROM t2 WHERE egid = 1 FOR UPDATE;"
sync_session 1
send 2 "BEGIN;"
send 2 "SELECT name FROM t2 WHERE egid = 2 FOR UPDATE;"
sync_session 2
N40=$(grep -c "40P01" "$D/out2" || true)
send 1 "SELECT name FROM t2 WHERE egid = 2 FOR UPDATE;" # parks behind S2
sleep 0.3 # S1 is parked mid-statement and cannot answer a sync marker
send 2 "SELECT name FROM t2 WHERE egid = 1 FOR UPDATE;" # closes the cycle
sync_session 2
if [ "$(grep -c "40P01" "$D/out2" || true)" = "$((N40 + 1))" ]; then
    ok "FOR UPDATE deadlock refused at the closing edge with 40P01"
else bad "no 40P01 for the FOR UPDATE deadlock closer"; fi
send 2 "COMMIT;" # of a failed txn -> rolls back; releases row 2
sync_session 2
sync_session 1 # S1's parked FOR UPDATE completes
if grep -q "row2" "$D/out1"; then
    ok "victim's abort unblocked the parked FOR UPDATE"
else bad "parked FOR UPDATE never completed"; fi
send 1 "ROLLBACK;"
sync_session 1

# --- FOR UPDATE scan: the cell filter locks the matches, nothing else --------
send 1 "BEGIN;"
send 1 "SELECT name FROM t2 WHERE name = 'row2' FOR UPDATE;" # locks egid 2
sync_session 1
NU=$(grep -c "UPDATE 1" "$D/out2" || true)
send 2 "UPDATE t2 SET name = 'nb1' WHERE egid = 1;" # unmatched row: no lock
sync_session 2
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$((NU + 1))" ]; then
    ok "unmatched row stays unlocked under FOR UPDATE scan"
else bad "FOR UPDATE scan locked an unmatched row"; fi
send 2 "UPDATE t2 SET name = 'z2' WHERE egid = 2;" # matched row: parks
sleep 0.3
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$((NU + 1))" ]; then
    ok "matched row is locked by FOR UPDATE scan"
else bad "matched row was not locked"; fi
send 1 "COMMIT;"
sync_session 2
if [ "$(v "SELECT name FROM t2 WHERE egid = 2;")" = "z2" ]; then
    ok "blocked writer lands after scan FOR UPDATE commits"
else bad "blocked writer lost after scan FOR UPDATE"; fi

# --- FOR UPDATE variants: implicit txn works, the rest reject cleanly --------
if q "SELECT name FROM t2 WHERE egid = 1 FOR UPDATE;" | grep -q "nb1"; then
    ok "implicit-txn FOR UPDATE works"
else bad "implicit-txn FOR UPDATE failed"; fi
if { q "SELECT name FROM t2 FOR SHARE;" 2>&1 || true; } |
    grep -q "only FOR UPDATE"; then
    ok "FOR SHARE rejected"
else bad "FOR SHARE not rejected"; fi
if { q "SELECT name FROM t2 FOR UPDATE NOWAIT;" 2>&1 || true; } |
    grep -q "not supported"; then
    ok "NOWAIT rejected"
else bad "NOWAIT not rejected"; fi

# --- PRIMARY KEY: unique-index enforcement + concurrency --------------------
# A real PK table (the suites above are egid-only). Enforcement is a unique
# index: duplicates 23505, NULL keys 23502, and the index entry is locked so
# concurrent inserters of one key serialize.
q "CREATE TABLE pk (id int4 PRIMARY KEY, name text);" >/dev/null
q "INSERT INTO pk VALUES (1, 'a'), (3, 'c');" >/dev/null
# q() runs a non-verbose psql, so these grep the error message text (the
# SQLSTATE only prints under \set VERBOSITY verbose, which the sessions set).
if { q "INSERT INTO pk VALUES (1, 'dup');" 2>&1 || true; } | grep -q "duplicate key"; then
    ok "sequential duplicate PK rejected"
else bad "sequential duplicate PK not rejected"; fi
if { q "INSERT INTO pk VALUES (8, 'x'), (8, 'y');" 2>&1 || true; } | grep -q "duplicate key"; then
    ok "in-statement duplicate PK rejected"
else bad "in-statement duplicate PK not rejected"; fi
if { q "INSERT INTO pk (name) VALUES ('z');" 2>&1 || true; } | grep -q "not-null constraint"; then
    ok "NULL primary key rejected"
else bad "NULL primary key not rejected"; fi
if q "INSERT INTO pk VALUES (4, 'd'), (5, 'e');" | grep -q "INSERT 0 2"; then
    ok "distinct multi-row INSERT succeeds"
else bad "distinct multi-row INSERT failed"; fi
# DELETE clears the index entry, so the same PK re-inserts (index maintained).
q "DELETE FROM pk WHERE id = 1;" >/dev/null
if q "INSERT INTO pk VALUES (1, 'a2');" | grep -q "INSERT 0 1"; then
    ok "re-INSERT of a deleted PK succeeds"
else bad "re-INSERT after delete rejected — stale index entry"; fi
# UPDATE of the PK column moves the index entry: the new key is found
# through the index, the old key is free again. Rows: 1, 3, 4, 5.
if q "UPDATE pk SET id = 99 WHERE id = 3;" | grep -q "UPDATE 1"; then
    ok "UPDATE of the PK column moves the key"
else bad "UPDATE of the PK column failed"; fi
if [ "$(v "SELECT name FROM pk WHERE id = 99;")" = "c" ]; then
    ok "the moved PK is found through the index"
else bad "moved PK not found: $(v "SELECT name FROM pk WHERE id = 99;")"; fi
if q "INSERT INTO pk VALUES (3, 'c2');" | grep -q "INSERT 0 1"; then
    ok "the old PK value is free after the move"
else bad "old PK value still taken after the move"; fi
q "DELETE FROM pk WHERE id = 3;" >/dev/null
# Onto an occupied key → 23505; two rows onto one value → 23505.
if { q "UPDATE pk SET id = 4 WHERE id = 99;" 2>&1 || true; } |
    grep -q "duplicate key"; then
    ok "UPDATE onto an occupied PK rejected"
else bad "UPDATE onto an occupied PK not rejected"; fi
if { q "UPDATE pk SET id = 7 WHERE id = 4 OR id = 5;" 2>&1 || true; } |
    grep -q "duplicate key"; then
    ok "two rows UPDATEd onto one PK value rejected"
else bad "two rows onto one PK value not rejected"; fi
if [ "$(v "SELECT name FROM pk WHERE id = 99;")" = "c" ]; then
    ok "a failed UPDATE changed nothing"
else bad "failed UPDATE left changes"; fi
# A run of rows each moving onto the next one's key works in one
# statement: every key is locked first, and each key's write is decided
# from what the whole statement does to it. Rows 4, 5 → 5, 6.
if q "UPDATE pk SET id = id + 1 WHERE id = 4 OR id = 5;" |
    grep -q "UPDATE 2"; then
    ok "PK keys shift in one statement"
else bad "PK shift failed"; fi
if [ "$(v "SELECT name FROM pk WHERE id = 6;")" = "e" ] &&
    [ "$(v "SELECT name FROM pk WHERE id = 5;")" = "d" ] &&
    [ -z "$(v "SELECT name FROM pk WHERE id = 4;")" ]; then
    ok "every shifted key is found through the index"
else bad "shifted keys wrong: $(v "SELECT id, name FROM pk;" | tr '\n' ' ')"; fi
q "UPDATE pk SET id = id - 1 WHERE id = 5 OR id = 6;" >/dev/null # back to 4, 5
# NULL into the PK column, literal or computed → 23502.
if { q "UPDATE pk SET id = NULL WHERE id = 99;" 2>&1 || true; } |
    grep -q "not-null constraint"; then
    ok "UPDATE PK to NULL rejected"
else bad "UPDATE PK to NULL not rejected"; fi
if { q "UPDATE pk SET id = NULL + 1 WHERE id = 99;" 2>&1 || true; } |
    grep -q "not-null constraint"; then
    ok "UPDATE PK to a computed NULL rejected"
else bad "UPDATE PK to a computed NULL not rejected"; fi
q "UPDATE pk SET id = 3 WHERE id = 99;" >/dev/null # back to 3

# Concurrent insert of one key: the loser parks on the index-key lock, then
# 23505 once the winner commits.
DUP0=$(grep -c "23505" "$D/out2" || true)
INS0=$(grep -c "INSERT 0 1" "$D/out2" || true)
send 1 "BEGIN;"
send 1 "INSERT INTO pk VALUES (20, 's1');"
sync_session 1
send 2 "INSERT INTO pk VALUES (20, 's2');" # parks on the index-key lock
sleep 0.3
if [ "$(grep -c "INSERT 0 1" "$D/out2")" = "$INS0" ] &&
    [ "$(grep -c "23505" "$D/out2")" = "$DUP0" ]; then
    ok "concurrent same-PK insert blocks on the index lock"
else bad "second inserter was not blocked"; fi
send 1 "COMMIT;"
sync_session 2
if [ "$(grep -c "23505" "$D/out2")" -gt "$DUP0" ]; then
    ok "loser gets 23505 once the winner commits"
else bad "loser did not get 23505 after winner committed"; fi

# Same race, but the holder ROLLBACKs → the key is free, the waiter lands.
INS1=$(grep -c "INSERT 0 1" "$D/out2" || true)
send 1 "BEGIN;"
send 1 "INSERT INTO pk VALUES (21, 'r1');"
sync_session 1
send 2 "INSERT INTO pk VALUES (21, 'r2');" # parks on the index-key lock
sleep 0.3
send 1 "ROLLBACK;"
sync_session 2
if [ "$(grep -c "INSERT 0 1" "$D/out2")" -gt "$INS1" ]; then
    ok "waiter's insert lands after the holder rolls back"
else bad "waiter blocked or rejected after rollback"; fi

# WHERE <pk> = v routes through the unique index (point lookup, not a scan).
if [ "$(v "SELECT name FROM pk WHERE id = 4;")" = "d" ]; then
    ok "SELECT WHERE pk routes through the index and returns the row"
else bad "index point SELECT wrong: $(v "SELECT name FROM pk WHERE id = 4;")"; fi

# FOR UPDATE on an ABSENT key locks nothing (readers lock rows they return,
# never keys — Postgres's rule): a concurrent INSERT of that key lands at
# once. The reader's own later INSERT of it then finds a commit above its
# snapshot on a key it already read — an unrepairable stale read → 40001,
# the SI outcome; the row is the other session's.
GI0=$(grep -c "INSERT 0 1" "$D/out2" || true)
S1_40001=$(grep -c "40001" "$D/out1" || true)
send 1 "BEGIN;"
send 1 "SELECT name FROM pk WHERE id = 50 FOR UPDATE;" # 0 rows, no lock
sync_session 1
send 2 "INSERT INTO pk VALUES (50, 'ghost');"
sync_session 2
if [ "$(grep -c "INSERT 0 1" "$D/out2")" = "$((GI0 + 1))" ]; then
    ok "FOR UPDATE on an absent PK does not block a concurrent insert"
else bad "insert of an absent key blocked behind a FOR UPDATE reader"; fi
send 1 "INSERT INTO pk VALUES (50, 'late');"
sync_session 1
if [ "$(grep -c "40001" "$D/out1")" = "$((S1_40001 + 1))" ]; then
    ok "the reader's own insert of that key sees its stale read: 40001"
else bad "no 40001 for an insert after a stale absent-key read"; fi
send 1 "ROLLBACK;"
sync_session 1
if [ "$(v "SELECT name FROM pk WHERE id = 50;")" = "ghost" ]; then
    ok "the concurrent insert's row is the one that stands"
else bad "unexpected row 50: $(v "SELECT name FROM pk WHERE id = 50;")"; fi

# --- FOR UPDATE over more than one executor batch (1024 rows) ---------------
# values_list <first id> <count>: "(i, 'ri'), ..." for one INSERT.
values_list() {
    local i=$1 end=$(($1 + $2)) sep=""
    while [ "$i" -lt "$end" ]; do
        printf '%s(%d, %s)' "$sep" "$i" "'r$i'"
        sep=", "
        i=$((i + 1))
    done
}
q "CREATE TABLE wide (id int4, name text);" >/dev/null
for s in $(seq 1 500 2500); do
    q "INSERT INTO wide VALUES $(values_list "$s" 500);" >/dev/null
done
W0=$(grep -cE '^[0-9]+$' "$D/out1" || true)
send 1 "BEGIN;"
send 1 "SELECT id FROM wide FOR UPDATE;" # 2500 rows: drain, lock all, emit
sync_session 1
if [ "$(grep -cE '^[0-9]+$' "$D/out1" || true)" = "$((W0 + 2500))" ]; then
    ok "FOR UPDATE emits every row across batches"
else bad "FOR UPDATE over 2500 rows emitted the wrong count"; fi
NU=$(grep -c "UPDATE 1" "$D/out2" || true)
send 2 "UPDATE wide SET name = 'w' WHERE id = 2000;" # a row past batch 1
sleep 0.3
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$NU" ]; then
    ok "a row in a later batch is locked by FOR UPDATE"
else bad "FOR UPDATE did not lock a row past the first batch"; fi
send 1 "COMMIT;"
sync_session 2
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$((NU + 1))" ]; then
    ok "blocked writer lands after the multi-batch FOR UPDATE commits"
else bad "blocked writer never landed"; fi

# --- FOR UPDATE LIMIT n locks the n rows it returns, nothing past them -------
send 1 "BEGIN;"
send 1 "SELECT id FROM wide FOR UPDATE LIMIT 2;" # ids 1 and 2
sync_session 1
NU=$(grep -c "UPDATE 1" "$D/out2" || true)
send 2 "UPDATE wide SET name = 'w3' WHERE id = 3;" # past the limit: no lock
sync_session 2
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$((NU + 1))" ]; then
    ok "a row past FOR UPDATE LIMIT stays unlocked"
else bad "FOR UPDATE LIMIT locked a row it did not return"; fi
send 2 "UPDATE wide SET name = 'w2' WHERE id = 2;" # returned row: parks
sleep 0.3
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$((NU + 1))" ]; then
    ok "a returned row is locked by FOR UPDATE LIMIT"
else bad "FOR UPDATE LIMIT did not lock a returned row"; fi
send 1 "COMMIT;"
sync_session 2
if [ "$(grep -c "UPDATE 1" "$D/out2" || true)" = "$((NU + 2))" ]; then
    ok "blocked writer lands after FOR UPDATE LIMIT commits"
else bad "writer blocked by FOR UPDATE LIMIT never landed"; fi

# --- restart mid-drain: a full-table UPDATE parks, bumps, re-runs once -------
# S2 holds row 1300 (mid-table). S1's UPDATE drains all 2500 rows, then
# its lock pass parks on 1300. S2's commit changes that row's id, so
# S1's lockRow sees a newer commit, bumps read_ts, Restarts, re-drains
# from scratch and applies exactly once: one UPDATE 2500, every row
# rewritten, and the re-read pre-image carries S2's id.
send 2 "BEGIN;"
send 2 "UPDATE wide SET id = 9999 WHERE id = 1300;"
sync_session 2
R0=$(grep -c "UPDATE 2500" "$D/out1" || true)
A0=$(grep -c "40001" "$D/out1" || true) # earlier legs expect some
send 1 "UPDATE wide SET name = 'r';" # drains, then parks on row 1300
sleep 0.5
send 2 "COMMIT;"
sync_session 1
if [ "$(grep -c "UPDATE 2500" "$D/out1" || true)" = "$((R0 + 1))" ]; then
    ok "full-table UPDATE restarted past the held row and applied once"
else bad "full-table UPDATE did not report UPDATE 2500 exactly once"; fi
if [ "$(grep -c "40001" "$D/out1" || true)" = "$A0" ]; then
    ok "no serialization abort during the mid-drain restart"
else bad "the mid-drain restart surfaced a serialization abort"; fi
if [ "$(v "SELECT name FROM wide WHERE id = 9999;")" = "r" ]; then
    ok "the restart re-read the concurrent commit"
else bad "the restarted UPDATE missed the concurrent commit"; fi
if q "SELECT id FROM wide WHERE name = 'r';" | grep -q "(2500 rows)"; then
    ok "every row carries the restarted UPDATE's value"
else bad "rows missing the restarted UPDATE's value"; fi

# --- old snapshot vs a freshly built index: scan, never misread ------------
# S1's BEGIN fixes its read_ts, and a first scan (of ANOTHER table: a
# statement on `snap` would hold its intent and park the CREATE INDEX
# barrier behind this open txn) makes any later bump a 40001. S2 then
# builds an index on `color` — its backfilled entries sit at the
# build's snapshot ts, above S1's — and inserts a row. S1's read of
# `snap` must not use the index (it would find no entries): it scans
# and returns the pre-index rows, with the concurrent insert invisible
# to its snapshot, and no 40001, since readers never bump. After
# COMMIT a fresh snapshot reads through the index.
q "CREATE TABLE snap (id int4, color text);" >/dev/null
q "INSERT INTO snap VALUES (7001, 'blue'), (7002, 'red');" >/dev/null
send 1 "BEGIN;"
send 1 "SELECT id FROM wide WHERE id = 1;"
sync_session 1
q "CREATE INDEX snap_color ON snap (color);" >/dev/null
q "INSERT INTO snap VALUES (7003, 'blue');" >/dev/null
A0=$(grep -c "40001" "$D/out1" || true)
OLD0=$(grep -c "^7001$" "$D/out1" || true) # ids > wide's, but count
NEW0=$(grep -c "^7003$" "$D/out1" || true) # relative all the same
send 1 "SELECT id FROM snap WHERE color = 'blue';"
send 1 "COMMIT;"
sync_session 1
if [ "$(grep -c "^7001$" "$D/out1" || true)" = "$((OLD0 + 1))" ] &&
    [ "$(grep -c "^7003$" "$D/out1" || true)" = "$NEW0" ]; then
    ok "old snapshot scans past the new index and sees its own rows"
else bad "old snapshot misread: $(tail -4 "$D/out1" | tr '\n' ' ')"; fi
if [ "$(grep -c "40001" "$D/out1" || true)" = "$A0" ]; then
    ok "a reader is never bumped by an index build"
else bad "the index build aborted a reader with 40001"; fi
send 1 "SELECT id FROM snap WHERE color = 'blue';"
sync_session 1
if [ "$(grep -c "^7003$" "$D/out1" || true)" = "$((NEW0 + 1))" ]; then
    ok "a fresh snapshot reads through the new index"
else bad "fresh snapshot missed the indexed row"; fi

# --- user unique index: the pk cases' shape, plus the old-snapshot writer --
q "CREATE TABLE uu (id int4 PRIMARY KEY, email text);" >/dev/null
q "INSERT INTO uu VALUES (1, 'a@x'), (2, 'b@x');" >/dev/null
q "CREATE UNIQUE INDEX uu_email ON uu (email);" >/dev/null
if { q "INSERT INTO uu VALUES (3, 'a@x');" 2>&1 || true; } |
    grep -q "duplicate key"; then
    ok "sequential duplicate on a user unique index rejected"
else bad "sequential duplicate on a user unique index not rejected"; fi
if { q "INSERT INTO uu VALUES (3, 'c@x'), (4, 'c@x');" 2>&1 || true; } |
    grep -q "duplicate key"; then
    ok "in-statement duplicate on a user unique index rejected"
else bad "in-statement duplicate on a user unique index not rejected"; fi
# Concurrent insert of one value: the loser parks on the key lock,
# then 23505 once the winner commits.
UDUP0=$(grep -c "23505" "$D/out2" || true)
send 1 "BEGIN;"
send 1 "INSERT INTO uu VALUES (3, 'c@x');"
sync_session 1
send 2 "INSERT INTO uu VALUES (4, 'c@x');" # parks on the key lock
sleep 0.3
if [ "$(grep -c "23505" "$D/out2")" = "$UDUP0" ]; then
    ok "concurrent same-value insert blocks on the unique key lock"
else bad "second inserter was not blocked by the unique key lock"; fi
send 1 "COMMIT;"
sync_session 2
if [ "$(grep -c "23505" "$D/out2")" -gt "$UDUP0" ]; then
    ok "loser gets 23505 once the winner commits (user unique)"
else bad "loser did not get 23505 (user unique)"; fi
q "DELETE FROM uu WHERE id = 3;" >/dev/null
if q "INSERT INTO uu VALUES (4, 'c@x');" | grep -q "INSERT 0 1"; then
    ok "DELETE frees the unique value"
else bad "unique value still taken after DELETE"; fi

# Old snapshot, writer: S1's txn predates a unique index S2 builds. An
# INSERT of a value the build indexed must see the backfilled entries,
# so S1 is bumped to the index's valid_ts: 23505 when nothing was read
# before, 40001 when a prior read makes the bump unsafe. A reader in
# the same position scans and is never bumped.
q "CREATE TABLE us (id int4 PRIMARY KEY, v text);" >/dev/null
q "INSERT INTO us VALUES (1, 'p'), (2, 'q');" >/dev/null
UA0=$(grep -c "40001" "$D/out1" || true)
UD0=$(grep -c "23505" "$D/out1" || true)
send 1 "BEGIN;"
sync_session 1
q "CREATE UNIQUE INDEX us_v ON us (v);" >/dev/null
send 1 "INSERT INTO us VALUES (3, 'p');"
send 1 "ROLLBACK;"
sync_session 1
if [ "$(grep -c "23505" "$D/out1")" = "$((UD0 + 1))" ] &&
    [ "$(grep -c "40001" "$D/out1")" = "$UA0" ]; then
    ok "old-snapshot writer is bumped to the new unique index: 23505"
else bad "old-snapshot writer: $(tail -3 "$D/out1" | tr '\n' ' ')"; fi
send 1 "BEGIN;"
send 1 "SELECT id FROM wide WHERE id = 1;" # a prior read: no bump allowed
sync_session 1
q "DROP INDEX us_v;" >/dev/null
q "CREATE UNIQUE INDEX us_v ON us (v);" >/dev/null # built after the snapshot
send 1 "INSERT INTO us VALUES (3, 'p');"
send 1 "ROLLBACK;"
sync_session 1
if [ "$(grep -c "40001" "$D/out1")" = "$((UA0 + 1))" ]; then
    ok "old-snapshot writer with a prior read gets 40001"
else bad "writer with a prior read: $(tail -3 "$D/out1" | tr '\n' ' ')"; fi
UP0=$(grep -c "^1$" "$D/out1" || true)
send 1 "BEGIN;"
send 1 "SELECT id FROM wide WHERE id = 1;"
sync_session 1
q "DROP INDEX us_v;" >/dev/null
q "CREATE UNIQUE INDEX us_v ON us (v);" >/dev/null
send 1 "SELECT id FROM us WHERE v = 'p';" # scans: the index is too new
send 1 "COMMIT;"
sync_session 1
if [ "$(grep -c "^1$" "$D/out1")" = "$((UP0 + 2))" ] &&
    [ "$(grep -c "40001" "$D/out1")" = "$((UA0 + 1))" ]; then
    ok "old-snapshot reader scans past the new unique index, no 40001"
else bad "old-snapshot reader: $(tail -4 "$D/out1" | tr '\n' ' ')"; fi

# --- disconnect mid-txn: rollback + lock release (t is dropped; use t2) ------
send 1 "BEGIN;"
send 1 "INSERT INTO t2 VALUES (9, 'ghost');"
send 1 "UPDATE t2 SET name = 'held' WHERE egid = 1;"
sync_session 1
close_session 1 # Terminate mid-txn
sleep 0.3
if [ -z "$(v "SELECT name FROM t2 WHERE name = 'ghost';")" ]; then
    ok "disconnect rolled the txn back"
else bad "disconnected txn's insert landed"; fi
if q "UPDATE t2 SET name = 'free' WHERE egid = 1;" | grep -q "UPDATE 1"; then
    ok "disconnect released the row lock"
else bad "row lock survived the disconnect"; fi

close_session 2

# --- no unexpected server-side errors --------------------------------------
if grep -qE '^[^ ]+ ERROR ' "$LOG"; then bad "server log has errors"; else ok "server log clean"; fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
