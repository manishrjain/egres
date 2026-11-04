#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# End-to-end test of statement admission under a memory budget. Two
# server starts, since the memory flags are fixed at startup:
#   1. a burst of large INSERTs against a small statement pool: every
#      client ends with a row written or out of memory, the server's
#      peak resident size stays bounded, and it serves afterwards;
#   2. a pool smaller than one frame: the INSERT is refused after the
#      wait, deterministically, and a small one goes through.
# Wipes $DATA (default /data/egres) — that dir is throw-away.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${DATA:-/data/egres}"
LOG=/tmp/zdb/server.log
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"

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

# start_server <memory flags>. Ready = listener up AND election won,
# then one more second so the governor has sized the statement pool.
start_server() {
    ./zig-out/bin/egres --raft="id=1" --memory="$1" \
        --store="dir_path=$DATA/sst" --wal="dir=$DATA/wal" >"$LOG" 2>&1 &
    SERVER_PID=$!
    for _ in $(seq 1 50); do
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
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}

# The server's peak resident size so far, in KiB.
peak_rss_kb() { awk '/VmHWM/ { print $2 }' "/proc/$SERVER_PID/status"; }

zig build
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
written=$(grep -l "INSERT 0 1" "$OUT"/* | wc -l)
refused=$(grep -l "past its memory limit" "$OUT"/* | wc -l)
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
expect "rows written are readable" "$written" v "SELECT count(*) FROM burst;"
expect "a small statement runs after the burst" "INSERT 0 1" \
    q "INSERT INTO burst VALUES (0, 's');"
peak=$(peak_rss_kb)
echo "burst: peak RSS ${peak} KiB"
# A coarse ceiling: about 860 MB measured in Debug, most of it the 16
# frames the connections read before admission and the process's own
# base. Without admission every client's further copies of its row
# are resident at once as well.
if [ "$peak" -lt 1572864 ]; then ok "peak RSS under 1.5 GiB"
else bad "peak RSS ${peak} KiB, over 1.5 GiB"; fi
stop_server

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

# --- no unexpected server-side errors --------------------------------------
if grep -qE '^[^ ]+ ERROR ' "$LOG"; then bad "server log has errors"
else ok "server log clean"; fi

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
