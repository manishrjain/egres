#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Postgres conformance: replays tests/sqllogic/*.pgout, records made from
# Postgres's own regress files by tests/sqllogic/convert.py, against
# egres (tests/sqllogic/run.py) and fails on any outcome not listed in
# tests/sqllogic/<name>.known. Extra arguments go to run.py
# (--write-skips refreshes the skip lines). Needs a built exe;
# psycopg is installed into a venv under /tmp/zdb on first use. Wipes
# $DATA (default /data/egres). CI (.github/workflows/ci.yml, job e2e)
# runs it on Linux after the psql-driven scripts.
set -euo pipefail
cd "$(dirname "$0")/.."

DATA="${DATA:-/data/egres}"
LOG=/tmp/zdb/server.log
VENV=/tmp/zdb/psycopg-venv
E="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"

[ -x zig-out/bin/egres ] || {
    echo "sqllogic: no zig-out/bin/egres; build first"
    exit 1
}

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

rm -rf "$DATA" && mkdir -p "$DATA"
start_server
trap stop_server EXIT

status=0
"$VENV/bin/python" tests/sqllogic/run.py "$E" tests/sqllogic/*.pgout "$@" ||
    status=$?

# run.py reports a lost connection; this names the cause.
if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "FAIL: egres died during the run; log tail:"
    tail -20 "$LOG"
    status=1
elif grep -qE '^[^ ]+ ERROR ' "$LOG"; then
    echo "FAIL: server log has errors"
    status=1
else
    echo "ok: server log clean"
fi
exit "$status"
