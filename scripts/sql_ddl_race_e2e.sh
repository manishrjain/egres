#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Regression test for manishrjain/egres#20: DDL is not atomic against
# concurrent DML. The table is (id int primary key, k int), so a
# statement may see it absent or complete and nothing in between: never
# XX000, never 42601 or 42703 for a statement that fits the table, and
# never a result that is not two columns wide.
#
# One session drops and recreates r in a loop while others INSERT,
# UPDATE, DELETE and SELECT, one of them through the extended
# protocol.
#
# Builds, and wipes $DATA (/data/egres unless set), as the other
# end-to-end scripts do.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${DATA:-/data/egres}"
LOG=/tmp/zdb/server.log
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"
# The generated statements and what each session printed.
D=$(mktemp -d)
INSERTERS=8
SELECTORS=2
ROUNDS="${ROUNDS:-4000}"
DDL="(id int primary key, k int)"

pass=0
fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; fail=$((fail + 1)); }

# zero <count> <what>: the check passes when nothing was counted.
zero() {
    if [ "$1" -eq 0 ]; then ok "no $2"; else bad "$2 x$1"; fi
}

start_server() {
    rm -rf "$DATA"
    mkdir -p "$DATA" "$(dirname "$LOG")"
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

SERVER_PID=
stop_server() {
    [ -n "$SERVER_PID" ] || return 0
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
trap 'stop_server; rm -rf "$D"' EXIT
# alive <what>: a server that died raced nothing, and the checks would
# pass on the silence, so stop there.
alive() {
    kill -0 "$SERVER_PID" 2>/dev/null && return 0
    echo "FAIL: server died during $1"
    exit 1
}
# clean <what> <err file>: the DDL session ran without error. A guard
# like `alive`, not a check: it holds with the issue open.
clean() {
    grep -q ERROR "$2" || return 0
    echo "FAIL: $1 failed, nothing was raced"
    exit 1
}

# hist <label> <err files...>: the sqlstate histogram of those sessions.
# psql prints "psql:<file>:<line>: ERROR:  <sqlstate>" per failure.
hist() {
    echo "$1 result codes:"
    shift
    cat "$@" | sed -n 's/.*ERROR:  *//p' | sort | uniq -c | sort -rn
}
# count <sqlstate> <err files...>
count() {
    local code=$1
    shift
    cat "$@" | grep -c "ERROR:  $code" || true
}
# Lines of unaligned psql output that are not a two-column header, a
# two-column row or a row-count footer.
TWO_COLS='^(id\|k|[0-9]+\|[0-9]+|\([0-9]+ rows?\))$'
not_two_cols() {
    cat "$@" | grep -cvE "$TWO_COLS" || true
}
# The distinct offending lines, most frequent first, for the report.
show_not_two_cols() {
    cat "$@" | { grep -vE "$TWO_COLS" || true; } |
        sort | uniq -c | sort -rn | head -5
}

zig build
start_server

# --- drop/create against every statement kind ------------------------------
psql "$E" -X -c "CREATE TABLE r $DDL;" >/dev/null

# repeat <file> <statement>: ROUNDS copies of one statement. sqlstate
# verbosity prints only the five-character code of each error.
repeat() {
    {
        echo '\set VERBOSITY sqlstate'
        for _ in $(seq 1 "$ROUNDS"); do echo "$2"; done
    } >"$D/$1"
}
{
    echo '\set VERBOSITY sqlstate'
    for _ in $(seq 1 "$ROUNDS"); do
        echo 'DROP TABLE IF EXISTS r;'
        echo "CREATE TABLE r $DDL;"
    done
} >"$D/chaos.sql"
repeat ins.sql 'INSERT INTO r VALUES (123, 1);'
repeat sel.sql 'SELECT * FROM r;'
repeat upd.sql 'UPDATE r SET k = 2 WHERE id = 123;'
# Deletes nothing, but names k: a schema torn down to id answers
# 42703.
repeat del.sql 'DELETE FROM r WHERE k = 99;'
# \bind sends Parse/Bind/Describe/Execute, the path a driver's
# prepared statement takes.
repeat bind.sql 'SELECT * FROM r WHERE id = $1 \bind 123 \g'

pids=()
psql "$E" -X -q -f "$D/chaos.sql" >/dev/null 2>"$D/chaos.err" &
pids+=($!)
for n in $(seq 1 "$INSERTERS"); do
    psql "$E" -X -q -f "$D/ins.sql" >/dev/null 2>"$D/dc.ins$n.err" &
    pids+=($!)
done
for n in $(seq 1 "$SELECTORS"); do
    psql "$E" -X -q -A -f "$D/sel.sql" \
        >"$D/sel$n.out" 2>"$D/dc.sel$n.err" &
    pids+=($!)
done
psql "$E" -X -q -A -f "$D/bind.sql" >"$D/sel.bind.out" 2>"$D/dc.bind.err" &
pids+=($!)
psql "$E" -X -q -f "$D/upd.sql" >/dev/null 2>"$D/dc.upd.err" &
pids+=($!)
psql "$E" -X -q -f "$D/del.sql" >/dev/null 2>"$D/dc.del.err" &
pids+=($!)
for pid in "${pids[@]}"; do wait "$pid" || true; done
alive "drop/create"
clean "DROP or CREATE TABLE" "$D/chaos.err"

hist inserter "$D"/dc.ins*.err
hist selector "$D"/dc.sel*.err "$D/dc.bind.err"
hist "update and delete" "$D/dc.upd.err" "$D/dc.del.err"
zero "$(count XX000 "$D"/dc.*.err)" \
    "XX000 failed to load table schema"
zero "$(count 42601 "$D"/dc.ins*.err)" \
    "42601 wrong number of values against a 2-column table"
zero "$(count 42703 "$D"/dc.*.err)" \
    "42703 column missing from a 2-column table"
zero "$(not_two_cols "$D"/sel*.out)" \
    "SELECT * results that are not two columns wide"
show_not_two_cols "$D"/sel*.out

if grep -qE '^[^ ]+ ERROR ' "$LOG"; then
    bad "server log has errors"
else
    ok "server log clean"
fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
