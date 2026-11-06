#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# End-to-end test of memory bounds under a budget. Three server
# starts, since the memory flags are fixed at startup:
#   1. a burst of large INSERTs against a small statement pool: every
#      client ends with a row written or out of memory, the server's
#      peak resident size stays bounded, and it serves afterwards;
#   2. a pool smaller than one frame: the INSERT is refused after the
#      wait, deterministically, and a small one goes through;
#   3. a scan of wide rows to a client that reads nothing: the server
#      holds one batch of the result, not all of it.
# Wipes $DATA (default /data/egres) — that dir is throw-away.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${DATA:-/data/egres}"
LOG=/tmp/zdb/server.log
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"
OS=$(uname -s)

pass=0
fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; fail=$((fail + 1)); }

# expect <desc> <needle> <cmd...>: run, assert output contains needle.
expect() {
    local desc=$1 needle=$2
    shift 2
    local out
    out=$("$@" 2>&1) || true
    if grep -q -- "$needle" <<<"$out"; then ok "$desc"
    else bad "$desc — got: $out"; fi
}

q() { psql "$E" -X -c "$1"; }     # full output (row counts, headers)
v() { psql "$E" -X -tA -c "$1"; } # tuples-only, unaligned (bare value)
# A statement too long for an argv entry goes in on stdin.
qin() { psql "$E" -X -f - <<<"$1"; }

# start_server <memory flags> [flag...]. Ready = listener up AND
# election won, then one more second so the governor has sized the
# statement pool.
start_server() {
    local memory=$1
    shift
    local command=(./zig-out/bin/egres --raft="id=1" --memory="$memory"
        "$@" --store="dir_path=$DATA/sst" --wal="dir=$DATA/wal")
    SERVER_PID=
    if [ "$OS" = Darwin ]; then
        # time collects the child's kernel peak RSS when it exits. The
        # shell publishes its pid before exec so shutdown targets egres.
        : >"$DATA/server.pid"
        /usr/bin/time -l -o "$DATA/rusage" /bin/sh -c '
            echo $$ >"$1"
            shift
            exec "$@"
        ' sh "$DATA/server.pid" "${command[@]}" >"$LOG" 2>&1 &
        SERVER_WAIT_PID=$!
    else
        "${command[@]}" >"$LOG" 2>&1 &
        SERVER_PID=$!
        SERVER_WAIT_PID=$SERVER_PID
    fi
    for _ in $(seq 1 50); do
        if [ "$OS" = Darwin ] && [ -z "$SERVER_PID" ] &&
            [ -s "$DATA/server.pid" ]; then
            SERVER_PID=$(cat "$DATA/server.pid")
        fi
        if grep -q "sql listener started" "$LOG" 2>/dev/null &&
            grep -q "become-leader" "$LOG" 2>/dev/null; then
            sleep 1
            return
        fi
        sleep 0.2
    done
    echo "server never became ready"
    exit 1
}

stop_server() {
    if [ -n "$SERVER_PID" ]; then
        kill -TERM "$SERVER_PID" 2>/dev/null || true
    fi
    wait "$SERVER_WAIT_PID" 2>/dev/null || true
    SERVER_PID=
}

# Both BSD and Linux ps report RSS in KiB.
rss_kb() { ps -o rss= -p "$SERVER_PID"; }

zig build -Doptimize=ReleaseSafe
rm -rf "$DATA" && mkdir -p "$DATA" /tmp/zdb
trap stop_server EXIT

# --- 1. a burst of large INSERTs against a small pool ---------------------
# A 512 MiB budget with 2% for statements: a pool of about 10 MiB, so
# 8 MiB frames go through one at a time and the rest wait, up to the
# bound, then are refused. 16 of them at once are 128 MiB of frames.
start_server "total_bytes=512MiB;query_ratio=0.02"
q "CREATE TABLE burst (id int4, v text);" >/dev/null
ROW=$(head -c 8388608 /dev/zero | tr '\0' 'x')
clients=16
OUT=$(mktemp -d)
pids=()
for i in $(seq 1 $clients); do
    qin "INSERT INTO burst VALUES ($i, '$ROW');" >"$OUT/$i" 2>&1 &
    pids+=($!)
done
# By pid: a bare `wait` would also wait for the server.
for pid in "${pids[@]}"; do wait "$pid" || true; done
written=0
refused=0
for i in $(seq 1 $clients); do
    if grep -q "INSERT 0 1" "$OUT/$i"; then
        written=$((written + 1))
    elif grep -q "past its memory limit" "$OUT/$i"; then
        refused=$((refused + 1))
    fi
done
echo "burst: $written written, $refused refused"
if [ $((written + refused)) -eq $clients ]; then
    ok "every client ended with a row written or out of memory"
else
    bad "a client ended some other way: $(cat "$OUT"/*)"
fi
rm -rf "$OUT"
if kill -0 "$SERVER_PID" 2>/dev/null; then ok "server survived the burst"
else bad "server died during the burst"; fi
# One at a time through a 10 MiB pool, 16 clients outlast the wait
# bound: without admission none would be turned away.
if [ "$refused" -ge 1 ]; then ok "the full pool turned clients away"
else bad "no client was refused: nothing waited for the pool"; fi
if [ "$(v 'SELECT count(*) FROM burst;')" -eq "$written" ]; then
    ok "rows written are readable"
else
    bad "rows written are readable: expected $written"
fi
expect "a small statement runs after the burst" "INSERT 0 1" \
    q "INSERT INTO burst VALUES (0, 's');"
# Darwin's ru_maxrss is bytes; Linux's VmHWM is KiB. Neither relies
# on polling, so short-lived allocations count toward the ceiling.
if [ "$OS" = Darwin ]; then
    stop_server
    peak=$(awk '/maximum resident set size/ { print $1 / 1024 }' \
        "$DATA/rusage")
else
    peak=$(awk '/VmHWM/ { print $2 }' "/proc/$SERVER_PID/status")
    stop_server
fi
echo "burst: peak RSS ${peak} KiB"
# A coarse ceiling: about 860 MB measured in Debug, most of it the 16
# frames the connections read before admission and the process's own
# base. Without admission every client's further copies of its row
# are resident at once as well.
if [ "$peak" -lt 1572864 ]; then ok "peak RSS under 1.5 GiB"
else bad "peak RSS ${peak} KiB, over 1.5 GiB"; fi

# --- 2. a pool smaller than one frame: refused, then recovers -------------
# 0.1% of 512 MiB is about half a MiB: an 8 MiB frame is past the
# database's soft limit with the pool full on arrival, waits the bound
# out and is refused; a 1 KiB one is under it.
rm -rf "$DATA" && mkdir -p "$DATA"
start_server "total_bytes=512MiB;query_ratio=0.001"
q "CREATE TABLE small (id int4, v text);" >/dev/null
expect "an INSERT past the pool is out of memory" \
    "out of memory: the database is past its memory limit" \
    qin "INSERT INTO small VALUES (1, '$ROW');"
expect "its row is absent" "0" v "SELECT count(*) FROM small;"
expect "a small INSERT after the refusal is written" "INSERT 0 1" \
    q "INSERT INTO small VALUES (2, '$(head -c 1024 /dev/zero | tr '\0' 'y')');"
expect "the connection is usable" "1" v "SELECT count(*) FROM small;"

stop_server

# --- 3. a scan of wide rows to a client that reads nothing ----------------
# 64 rows of 2 MiB, flushed to SSTs as they come (4 MiB memtables,
# flushed one at a time). A scan borrows each row's bytes from its
# SST block, pinned until the batch is sent, so a client that reads
# nothing stalls the scan at one batch, and the server's resident
# size grows by a batch, not by the 128 MiB result.
rm -rf "$DATA" && mkdir -p "$DATA"
start_server "total_bytes=2GiB" \
    --zdb="table_bytes=4MiB;flush_min_memtables=1"
q "CREATE TABLE wide (id int4, b text);" >/dev/null
ROW=$(head -c 2097152 /dev/zero | tr '\0' 'w')
for i in $(seq 1 64); do
    qin "INSERT INTO wide VALUES ($i, '$ROW');" >/dev/null
done
sleep 3 # the last memtables flush
before=$(rss_kb)
# A raw pgwire client: StartupMessage (35 bytes, protocol 3.0, user
# and database egres), then the Query, and nothing read back.
exec 3<>/dev/tcp/127.0.0.1/3210
printf '\x00\x00\x00\x23\x00\x03\x00\x00' >&3
printf 'user\x00egres\x00database\x00egres\x00\x00' >&3
printf 'Q\x00\x00\x00\x18SELECT b FROM wide;\x00' >&3
peak=0
for _ in $(seq 1 25); do
    sleep 0.2
    now=$(rss_kb)
    [ "$now" -gt "$peak" ] && peak=$now
done
grown=$(((peak - before) / 1024))
echo "scan: RSS before ${before} KiB, peak ${peak} KiB, grown ${grown} MiB"
# One batch is one 2 MiB row plus the reply scratch; the socket takes
# a few more. Without the bound the whole table is pinned first.
if [ "$grown" -lt 64 ]; then ok "a stalled scan holds one batch"
else bad "a stalled scan grew the server by ${grown} MiB"; fi
# The client reads after all: the whole result arrives, then the
# Terminate sent behind the query closes the connection.
printf 'X\x00\x00\x00\x04' >&3
drained=$(cat <&3 | wc -c)
exec 3>&-
if [ "$drained" -gt $((64 * 2097152)) ]; then ok "the whole result was sent"
else bad "only ${drained} bytes of the result arrived"; fi
expect "the table reads after the scan" "64" v "SELECT count(*) FROM wide;"

# --- no unexpected server-side errors --------------------------------------
if grep -qE '^[^ ]+ ERROR ' "$LOG"; then bad "server log has errors"
else ok "server log clean"; fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
