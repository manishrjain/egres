#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# A restart from a raft snapshot keeps the rows a branch copied out of
# a frozen memtable.
#
# CREATE DATABASE ... TEMPLATE copies the source's unflushed versions
# into the active memtable (Memtables.fork). The copies have no log
# entry of their own; replay rebuilds them only if the source's rows
# are back in memory when the branch entry replays. So the copying
# memtable's replay_from drops to the oldest memtable the fork read,
# and the snapshot horizon and recovery skip floor follow it. The test:
# the source's rows sit in a frozen memtable, the branch copies them,
# that memtable flushes (its SST holds the source only), the snapshot
# loop runs, and the node crashes before the copies flush. Without
# replay_from a snapshot lands past the source's write, and after the
# restart the branch's table has lost its catalog rows and its row.
#
# Wipes $DATA (default /data/egres), like every e2e script; fixed
# ports, one at a time.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${DATA:-/data/egres}"
LOG=/tmp/zdb/server.log
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"
A="host=127.0.0.1 port=3210 dbname=app sslmode=disable"
B="host=127.0.0.1 port=3210 dbname=app2 sslmode=disable"

pass=0
fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; fail=$((fail + 1)); }
# The scenario itself did not come about: no verdict on the bug.
setup_failed() {
    echo "SETUP: $1"
    exit 2
}

expect() {
    local desc=$1 needle=$2
    shift 2
    local out
    out=$("$@" 2>&1) || true
    if grep -q "$needle" <<<"$out"; then ok "$desc"; else
        bad "$desc — got: $out"; fi
}

# Memtables freeze at 64 KiB. Frozen memtables wait for a flush until
# 1000 pile up or an hour passes, so the script decides when one runs
# (admin `flag zdb`). The first flush after a start never waits; the
# warm-up below spends it. Snapshots come within a second or two.
ZDB_FLAGS="table-bytes=65536;flush-min-memtables=1000"
ZDB_FLAGS+=";flush-max-age-seconds=3600"
start_server() {
    ./zig-out/bin/egres --raft="id=1" \
        --store="dir_path=$DATA/sst" --wal="dir=$DATA/wal" \
        --zdb="$ZDB_FLAGS" \
        --snapshot="min-bytes=1;min-age-seconds=1;poll-interval-seconds=1" \
        >"$LOG" 2>&1 &
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

crash_server() {
    kill -KILL "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}

# admin <command>: one line to the admin port, the reply on stdout. The
# server keeps the connection open, so a quiet gap ends the reply.
admin() {
    python3 - "$1" <<'PY'
import socket, sys
s = socket.create_connection(("127.0.0.1", 5210), timeout=5)
s.sendall(sys.argv[1].encode() + b"\n")
out = b""
try:
    while True:
        b = s.recv(65536)
        if not b:
            break
        out += b
        s.settimeout(0.3)
except socket.timeout:
    pass
print(out.decode(errors="replace"))
PY
}

# Applied manifest commands, from the apply fiber's debug line (a Debug
# build logs it).
cmd_count() {
    grep -cE "enter idx=[0-9]+ cmd=$1 " "$LOG" || true
}
cmd_last_idx() {
    grep -oE "enter idx=[0-9]+ cmd=$1 " "$LOG" | tail -1 |
        grep -oE "[0-9]+" | head -1
}

# wait_until <tries of 0.2 s> <command...>: until the command succeeds.
wait_until() {
    local tries=$1
    shift
    for _ in $(seq 1 "$tries"); do
        if "$@"; then return 0; fi
        sleep 0.2
    done
    return 1
}

# pad <rows>: about 220 bytes a row into egres's pad table, enough
# rows to fill and freeze the active memtable.
pad_round=0
pad() {
    local rows i
    pad_round=$((pad_round + 1))
    local fill
    fill=$(head -c 200 /dev/zero | tr '\0' x)
    rows=$(for i in $(seq 1 "$1"); do
        printf "(%d,'%s')," "$((pad_round * 100000 + i))" "$fill"
    done)
    psql "$E" -X -qc "INSERT INTO pad VALUES ${rows%,};" >/dev/null
}

more_than() { [ "$(cmd_count "$1")" -gt "$2" ]; }

zig build
rm -rf "$DATA" && mkdir -p "$DATA" /tmp/zdb
start_server
trap stop_server EXIT

# --- warm-up: spend the first flush, which never waits ----------------------
psql "$E" -X -qc "CREATE TABLE pad (id int PRIMARY KEY, s text);" >/dev/null
flushes=$(cmd_count flush)
pad 400
wait_until 100 more_than flush "$flushes" ||
    setup_failed "warm-up memtable never flushed"

# --- the source's rows, then freeze their memtable --------------------------
psql "$E" -X -qc "CREATE DATABASE app;" >/dev/null
psql "$A" -X -qc "CREATE TABLE t (id int PRIMARY KEY, v text);" >/dev/null
psql "$A" -X -qc "INSERT INTO t VALUES (1, 'one');" >/dev/null
memtables=$(cmd_count new_memtable)
flushes=$(cmd_count flush)
pad 400
wait_until 100 more_than new_memtable "$memtables" ||
    setup_failed "the source's memtable never froze"
[ "$(cmd_count flush)" = "$flushes" ] ||
    setup_failed "the frozen memtable flushed before the branch"

# --- the branch copies them into the active memtable -------------------------
expect "CREATE DATABASE TEMPLATE" "CREATE DATABASE" \
    psql "$E" -X -c "CREATE DATABASE app2 TEMPLATE app;"
active_idx=$(cmd_last_idx new_memtable)
expect "branch reads its table before the crash" "(1 row)" \
    psql "$B" -X -c "SELECT * FROM t;"

# --- flush the frozen memtable only, then wait for the snapshot --------------
# Its SST holds the source's rows; the copies stay in the active
# memtable, which holds too little to freeze.
admin "flag zdb flush-min-memtables=1" >/dev/null
wait_until 100 more_than flush "$flushes" ||
    setup_failed "the frozen memtable never flushed"
admin "flag zdb flush-min-memtables=1000" >/dev/null
[ "$(cmd_last_idx new_memtable)" = "$active_idx" ] ||
    setup_failed "the active memtable froze: the copies would flush"

# Give the snapshot loop time to move. Before the fix the horizon was
# the active memtable, and a snapshot reached one below it within a
# few seconds; now the copies hold it below the source's write.
snap_reaches() {
    local snap
    snap=$(grep -oE "snapshot proposed: index=[0-9]+" "$LOG" | tail -1 |
        grep -oE "[0-9]+" || true)
    [ -n "$snap" ] && [ "$snap" -ge "$((active_idx - 1))" ]
}
wait_until 50 snap_reaches || true
sleep 1 # a snapshot installs and the log compacts

# --- crash before the copies flush, then restart -----------------------------
crash_server
start_server

expect "source table survives the restart" "(1 row)" \
    psql "$A" -X -c "SELECT * FROM t;"
expect "branch database survives the restart" "CREATE TABLE" \
    psql "$B" -X -c "CREATE TABLE probe (a int);"
# The branch's copies of t's catalog rows and row, rebuilt by replay.
expect "branch table survives the restart" "(1 row)" \
    psql "$B" -X -c "SELECT * FROM t;"

echo "branch restart e2e: $pass passed, $fail failed"
[ "$fail" = 0 ]
