#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Regression test for manishrjain/egres#20: DDL is not atomic against
# concurrent DML. Every table here is (id int primary key, k int), so a
# statement may see it absent or complete and nothing in between: never
# XX000, never 42601 or 42703 for a statement that fits the table, and
# never a result that is not two columns wide. Four legs, in order:
#
#   drop/create: one session drops and recreates r in a loop while
#   others INSERT, UPDATE, DELETE and SELECT, one of them through the
#   extended protocol.
#
#   create only: one session creates t1..tN and never drops them while
#   others insert into each as it appears. A schema read mid-CREATE
#   must not outlive the CREATE: afterwards every table takes a
#   2-value INSERT and shows both columns.
#
#   create as: the same, for CREATE TABLE ... AS TABLE.
#
#   behind drop: one statement of each kind queued behind a DROP
#   TABLE, put in that order rather than raced. It must answer 42P01,
#   not run against the dropped table.
#
# Uses a temp data dir unless DATA is set, and wipes it.
set -euo pipefail
cd "$(dirname "$0")/.."

LOG=/tmp/zdb/server.log
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"
D=$(mktemp -d)
# A scratch data dir unless DATA is given; either way it is wiped.
DATA="${DATA:-$D/data}"
INSERTERS=8
SELECTORS=2
ROUNDS="${ROUNDS:-4000}"
TABLES="${TABLES:-1000}"
# Inserts a session spends on one table before it gives up and moves
# on, so a slow CREATE stalls no one for good.
ATTEMPTS="${ATTEMPTS:-300}"
# CREATE TABLE AS rounds: fewer, each costs more than a CREATE TABLE.
COPIES="${COPIES:-300}"
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
# alive <leg>: a leg whose server died raced nothing, and its checks
# would pass on the silence, so stop there.
alive() {
    kill -0 "$SERVER_PID" 2>/dev/null && return 0
    echo "FAIL: server died during $1"
    exit 1
}
# clean <what> <err file>: the DDL side of a leg ran without error.
# A guard like `alive`, not a check: it holds with the issue open.
clean() {
    grep -q ERROR "$2" || return 0
    echo "FAIL: $1 failed, the leg raced nothing"
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

# --- create only: a torn read must not outlive the CREATE ------------------
# create_leg <label> <prefix> <tables> <definition>: create that many
# tables `<prefix>N <definition>` under inserters following the names,
# then check each one with nothing else running.
create_leg() {
    local label=$1 pre=$2 tables=$3 def=$4
    {
        echo '\set VERBOSITY sqlstate'
        # The pause lets the inserters catch up and spin on the next
        # name, so each CREATE lands under all of them at once.
        for i in $(seq 1 "$tables"); do
            echo '\! sleep 0.02'
            echo "CREATE TABLE $pre$i $def;"
        done
    } >"$D/$pre.create.sql"
    # Per table, insert until one lands: `go` is the last statement's
    # ERROR, so a session spins on 42P01 until the name appears and
    # its next attempts fall inside the CREATE.
    awk -v pre="$pre" -v tables="$tables" -v attempts="$ATTEMPTS" 'BEGIN {
        print "\\set VERBOSITY sqlstate"
        for (i = 1; i <= tables; i++) {
            print "\\set go true"
            for (j = 1; j <= attempts; j++) {
                print "\\if :go"
                print "INSERT INTO " pre i " VALUES (:n, 1);"
                print "\\set go :ERROR"
                print "\\endif"
            }
        }
    }' >"$D/$pre.follow.sql"
    {
        echo '\set VERBOSITY sqlstate'
        for i in $(seq 1 "$tables"); do
            echo "INSERT INTO $pre$i VALUES (1000, 1);"
            echo "SELECT * FROM $pre$i WHERE id = 1000;"
        done
    } >"$D/$pre.verify.sql"

    local pids=() n pid rows
    for n in $(seq 1 "$INSERTERS"); do
        psql "$E" -X -q -v n="$n" -f "$D/$pre.follow.sql" \
            >/dev/null 2>"$D/$pre.follow$n.err" &
        pids+=($!)
    done
    psql "$E" -X -q -f "$D/$pre.create.sql" \
        >/dev/null 2>"$D/$pre.create.err" &
    pids+=($!)
    for pid in "${pids[@]}"; do wait "$pid" || true; done

    alive "$label"
    clean "$label" "$D/$pre.create.err"
    hist "$label inserter" "$D/$pre".follow*.err
    zero "$(count XX000 "$D/$pre".follow*.err)" "XX000 during $label"
    zero "$(count 42601 "$D/$pre".follow*.err)" "42601 during $label"

    # Nothing is running now, so any error is a schema left broken.
    psql "$E" -X -q -A -f "$D/$pre.verify.sql" \
        >"$D/$pre.verify.out" 2>"$D/$pre.verify.err"
    hist "after $label" "$D/$pre.verify.err"
    zero "$(grep -c ERROR "$D/$pre.verify.err" || true)" \
        "tables left unusable after $label"
    zero "$(not_two_cols "$D/$pre.verify.out")" \
        "tables left without both columns after $label"
    show_not_two_cols "$D/$pre.verify.out"
    rows=$(grep -c '^1000|1$' "$D/$pre.verify.out" || true)
    if [ "$rows" -eq "$tables" ]; then
        ok "all $tables tables hold the row just inserted"
    else
        bad "only $rows of $tables tables hold the row just inserted"
    fi
}

create_leg "CREATE TABLE" t "$TABLES" "$DDL"

# The source holds a row, so each copy branches data as well as schema.
psql "$E" -X -q -c "CREATE TABLE src $DDL;" \
    -c "INSERT INTO src VALUES (500, 5);" >/dev/null
create_leg "CREATE TABLE AS" c "$COPIES" "AS TABLE src"

# --- statements queued behind DROP TABLE -----------------------------------
# behind_drop <what> <statement>: an open block holds r, a DROP TABLE
# queues behind it and the statement behind the DROP. The block
# commits, the drop lands first, and the table the statement resolved
# is gone: it must answer 42P01 whatever holds the name by then.
behind_drop() {
    local what=$1 stmt=$2 took hold drop
    psql "$E" -X -q -c "DROP TABLE IF EXISTS r;" -c "CREATE TABLE r $DDL;" \
        -c "INSERT INTO r VALUES (1, 1);" >/dev/null
    psql "$E" -X -q -c "BEGIN" -c "SELECT * FROM r;" -c '\! sleep 2' \
        -c "COMMIT" >/dev/null 2>"$D/hold.err" &
    hold=$!
    sleep 0.5
    psql "$E" -X -q -c "DROP TABLE r;" 2>"$D/drop.err" &
    drop=$!
    sleep 0.5
    took=$( { time psql "$E" -X -v VERBOSITY=sqlstate -c "$stmt" \
        >"$D/behind.out" 2>&1 || true; } 2>&1)
    wait "$hold" "$drop" || true
    alive "$what behind DROP TABLE"
    clean "the open block" "$D/hold.err"
    clean "DROP TABLE" "$D/drop.err"
    # A statement that did not wait ran before the DROP queued or
    # after it landed, and the check below would pass either way.
    if ! awk -v t="$took" 'BEGIN { exit !(t >= 0.5) }'; then
        echo "FAIL: $what did not queue behind DROP TABLE (${took}s)"
        exit 1
    fi
    if grep -q 'ERROR:  42P01' "$D/behind.out"; then
        ok "$what queued behind DROP TABLE answers 42P01"
    else
        bad "$what queued behind DROP TABLE got: $(tr '\n' ' ' \
            <"$D/behind.out")"
    fi
}

TIMEFORMAT=%R
behind_drop INSERT "INSERT INTO r VALUES (5, 1);"
behind_drop SELECT "SELECT * FROM r;"
behind_drop UPDATE "UPDATE r SET k = 9;"
behind_drop DELETE "DELETE FROM r;"

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
