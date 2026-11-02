#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# End-to-end test of the binary result and parameter formats, which
# psql cannot ask for: a psycopg script (scripts/binary_e2e.py) reads
# every column type through a binary cursor, binds parameters in
# binary, and runs a statement past psycopg's prepare threshold so a
# named server-side statement is used, as JDBC does after warm-up.
# psycopg is installed into a venv under /tmp/zdb on first use (it
# needs network once and libpq, which psql brings). Wipes /data/egres.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${DATA:-/data/egres}"
LOG=/tmp/zdb/server.log
VENV=/tmp/zdb/psycopg-venv
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"

# Ready = listener up AND election won (see sql_ddl_e2e.sh).
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

mkdir -p /tmp/zdb
if ! "$VENV/bin/python" -c 'import psycopg' 2>/dev/null; then
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install -q psycopg==3.2.9
fi

zig build
rm -rf "$DATA" && mkdir -p "$DATA"
start_server
trap stop_server EXIT

status=0
"$VENV/bin/python" scripts/binary_e2e.py "$E" || status=$?

if grep -qE '^[^ ]+ ERROR ' "$LOG"; then
    echo "FAIL: server log has errors"
    status=1
else
    echo "ok: server log clean"
fi
exit "$status"
