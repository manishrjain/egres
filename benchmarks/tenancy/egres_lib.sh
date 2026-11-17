#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Shared pieces for the Egres side of the tenancy benchmark: one Egres
# process, one database per tenant. Each tenant holds what a micro-VM
# tenant holds (table t, 1,000 rows) and answers the same query.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

RUN=$WORK/run
EGRES_BIN=${EGRES_BIN:-$WORK/egres-bin/bin/egres}
# Throw-away, and on a real disk: Egres reads SSTs with O_DIRECT.
EDATA=${EDATA:-$WORK/egres-data}
# Ports sit 50 above Egres's own, clear of a server under development.
OFFSET=${OFFSET:-50}
SQL_PORT=$((3210 + OFFSET))
ADDR=127.0.0.1:$SQL_PORT
# Every start's server log, numbered in start order: egres-1.log,
# egres-2.log, ... run.sh points this at the run's own directory, so a
# test that starts the server more than once keeps every log.
LOG_DIR=${LOG_DIR:-$RUN/logs}
# The current start's log; egres_launch sets it.
ELOG=
FLOOR_GIB=${FLOOR_GIB:-16}

mkdir -p "$RUN" "$RES"

# Egres keeps a file open per table and does not raise its own limit.
# At a login's 1,024 the server ran out near 650 tenants; this is the
# most the host allows.
ulimit -n "$(ulimit -Hn)"

avail_gib() { awk '/MemAvailable/ {print int($2/1048576)}' /proc/meminfo; }
avail_kb() { awk '/MemAvailable/ {print $2}' /proc/meminfo; }
have_room() { [ "$(avail_gib)" -ge "$FLOOR_GIB" ]; }

tenant() { echo "tenant_$1"; }
conn() { echo "host=127.0.0.1 port=$SQL_PORT dbname=$1 sslmode=disable"; }

# Launches the server and returns at once; callers that time the start
# take their clock before calling. Arguments are extra server flags.
# No memory flags: Egres sizes itself to the host, as a micro-VM host
# is given the whole machine.
egres_launch() {
    # A log of its own, so readiness is read from this start alone.
    mkdir -p "$LOG_DIR"
    local n=1
    while [ -e "$LOG_DIR/egres-$n.log" ]; do n=$((n + 1)); done
    ELOG=$LOG_DIR/egres-$n.log
    # Run under another name. Monitoring that reads /proc/<pid>/smaps
    # of every process called "egres" (one workstation's did, each
    # 10 s) stalls a server holding tens of GiB for 100-450 ms. Copied
    # whenever the bytes differ: by age alone, an older EGRES_BIN
    # would leave the last one running.
    local bin=$RUN/tenantdb
    cmp -s "$EGRES_BIN" "$bin" || cp "$EGRES_BIN" "$bin"
    # Egres probes the data directory's bandwidth at every start: 2.5 s,
    # and a different answer each time (tanland gave write speeds from
    # 27 to 1,753 MiB/s on one day, depending on what the drive was
    # doing). Once egres_calibrate.sh has measured the drive, every
    # start is given that profile and probes nothing. PROBE=1 asks for
    # the probe, to time a start as it comes out of the box.
    local io=()
    if [ "${PROBE:-0}" != 1 ] && [ -f "$RUN/egres_io.flags" ]; then
        io=(--io="$(cat "$RUN/egres_io.flags")")
    fi
    "$bin" --listen="offset=$OFFSET" --raft="id=1" \
        --store="dir-path=$EDATA/store" --wal="dir=$EDATA/wal" \
        "${io[@]}" "$@" >"$ELOG" 2>&1 &
    echo $! >"$RUN/egres.pid"
}

# Blocks until the server takes writes, or fails after 60s. A read is
# answered before the election is won, so the log is what tells.
egres_wait() {
    local i
    for ((i = 0; i < 600; i++)); do
        grep -q "become-leader" "$ELOG" 2>/dev/null &&
            psql "$(conn egres)" -X -qtAc "SELECT 1" >/dev/null 2>&1 &&
            return 0
        sleep 0.1
    done
    echo "egres did not come up; see $ELOG" >&2
    return 1
}

# Warms a server that has just started, before a test measures it, on
# the server's own database `egres`, never a tenant's. The first
# connections and statements after a start pay for the process (code
# faulted in, allocator arenas and pools filled), and that is no
# tenant's cost. The traffic is the tests' own: a tenant's table, point
# reads on new connections, for WARM_S seconds (5 unless set).
egres_warm() {
    local f=$RUN/warm_targets.txt
    psql "$(conn egres)" -X -qtAc "SELECT count(*) FROM t" >/dev/null 2>&1 ||
        run_seed egres "$(seed_sql)" || return 1
    echo "$ADDR/egres" >"$f"
    "$ROOT/probe/probe" load -addrs "$f" -rate 200 -dur "${WARM_S:-5}s" \
        >/dev/null
}

# What a test that does not time the start itself calls: up, then
# warmed. The start's own cost is timed with egres_launch and its
# first row, as test 3's process kinds and egres_restart.sh do.
egres_start() { egres_launch && egres_wait && egres_warm; }

egres_stop() {
    [ -f "$RUN/egres.pid" ] || return 0
    local pid
    pid=$(cat "$RUN/egres.pid")
    kill "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null; do sleep 0.05; done
    rm -f "$RUN/egres.pid"
}

egres_wipe() { egres_stop; rm -rf "$EDATA"; }

# The tenant's table and rows, the same 1,000 as the Postgres data
# image. One INSERT ... VALUES: Egres has no INSERT ... SELECT yet.
seed_sql() {
    local f=$RUN/seed.sql
    [ -f "$f" ] && { echo "$f"; return; }
    {
        echo "CREATE TABLE t (id bigint PRIMARY KEY, v text);"
        echo -n "INSERT INTO t VALUES "
        local g
        for g in $(seq 1 1000); do
            [ "$g" -gt 1 ] && echo -n ","
            echo -n "($g,'$(echo -n "$g" | md5sum | cut -d' ' -f1)')"
        done
        echo ";"
    } >"$f"
    echo "$f"
}

# Runs seed file $2 (a CREATE TABLE line, then INSERTs) in database $1.
# About one first INSERT in 430 fails with "failed to load table
# schema" when tenants are made in parallel: a read race in Egres
# (found 2026-11-14), after which the table is there and empty. Those
# get the INSERTs once more, and a line in results/ so they are
# counted.
run_seed() {
    local db=$1 file=$2 err
    err=$(mktemp)
    if psql "$(conn "$db")" -X -q -v ON_ERROR_STOP=1 -f "$file" \
        >/dev/null 2>"$err"; then
        rm -f "$err"
        return 0
    fi
    if grep -q "failed to load table schema" "$err" &&
        [ "$(psql "$(conn "$db")" -X -qtAc "SELECT count(*) FROM t")" = 0 ]
    then
        echo "$(date +%T) $db" >>"$RES/egres_schema_race.log"
        rm -f "$err"
        tail -n +2 "$file" | psql "$(conn "$db")" -X -q \
            -v ON_ERROR_STOP=1 >/dev/null
        return
    fi
    cat "$err" >&2
    rm -f "$err"
    return 1
}

create_tenant() {
    psql "$(conn egres)" -X -q -v ON_ERROR_STOP=1 \
        -c "CREATE DATABASE $(tenant "$1")" >/dev/null
    run_seed "$(tenant "$1")" "$(seed_sql)"
}

# Creates tenants [from, to), CREATE_PAR at a time. Returns 1 at the
# host's memory floor or when a tenant could not be made.
CREATE_PAR=${CREATE_PAR:-1}
create_range() {
    local from=$1 to=$2 i j p pids failed=0
    for ((i = from; i < to; i += CREATE_PAR)); do
        if ((i % 50 < CREATE_PAR)); then have_room || return 1; fi
        if ((CREATE_PAR == 1)); then
            create_tenant "$i" || return 1
            continue
        fi
        pids=()
        for ((j = i; j < i + CREATE_PAR && j < to; j++)); do
            create_tenant "$j" &
            pids+=($!)
        done
        for p in "${pids[@]}"; do wait "$p" || failed=$((failed + 1)); done
        [ "$failed" = 0 ] || { echo "a tenant failed to create" >&2; return 1; }
    done
}

# The big tenant of the sleeping-tenant and noisy-neighbour tests: the
# rows of assets/data_big.ext4, (g, md5(g)) for g in 1..ROWS, sent
# 1,000 to a statement.
ROWS=${ROWS:-100000}
# Big tenants loaded at once.
LOAD_PAR=${LOAD_PAR:-8}

big_seed() {
    local f=$RUN/seed_big_$ROWS.sql
    [ -f "$f" ] || python3 - "$ROWS" >"$f" <<'PY'
import hashlib, sys
rows = int(sys.argv[1])
print("CREATE TABLE t (id bigint PRIMARY KEY, v text);")
for lo in range(1, rows + 1, 1000):
    vals = ",".join(
        "(%d,'%s')" % (g, hashlib.md5(str(g).encode()).hexdigest())
        for g in range(lo, min(lo + 1000, rows + 1)))
    print("INSERT INTO t VALUES %s;" % vals)
PY
    echo "$f"
}

create_big() {
    psql "$(conn egres)" -X -q -v ON_ERROR_STOP=1 \
        -c "CREATE DATABASE $(tenant "$1")" >/dev/null
    run_seed "$(tenant "$1")" "$2"
}

# Creates big tenants [from, to), LOAD_PAR at a time.
load_big_range() {
    local from=$1 to=$2 i j p pids failed=0 seed
    seed=$(big_seed)
    for ((i = from; i < to; i += LOAD_PAR)); do
        have_room || { echo "host is out of room" >&2; return 1; }
        pids=()
        for ((j = i; j < i + LOAD_PAR && j < to; j++)); do
            create_big "$j" "$seed" &
            pids+=($!)
        done
        for p in "${pids[@]}"; do wait "$p" || failed=$((failed + 1)); done
        [ "$failed" = 0 ] || { echo "a tenant failed to load" >&2; return 1; }
    done
}

# Host memory the server holds, in KiB (proportional set size, the
# same measure the micro-VM side sums over its VMs).
egres_pss_kb() {
    awk '/^Pss:/ {print $2}' "/proc/$(cat "$RUN/egres.pid")/smaps_rollup"
}

egres_disk_kb() { du -sk "$EDATA" | cut -f1; }

# Files the server holds open. It keeps every table's file open, so
# this grows with the tenants and the host's hard limit (ulimit -Hn)
# caps it.
egres_fds() {
    # A file closed during the walk makes find fail; the count stands.
    { find "/proc/$(cat "$RUN/egres.pid")/fd" -mindepth 1 -maxdepth 1 \
        2>/dev/null || true; } | wc -l
}

# The store alone: the WAL's files are made at a fixed size up front
# and would hide what a tenant adds.
egres_store_kb() { du -sk "$EDATA/store" | cut -f1; }

targets_file() {
    local n=$1 f=$RUN/targets.txt i
    for ((i = 0; i < n; i++)); do echo "$ADDR/$(tenant "$i")"; done >"$f"
    echo "$f"
}

mib() { awk -v kb="$1" 'BEGIN {printf "%.1f", kb / 1024}'; }

# One command on the server's admin port; prints the reply.
egres_admin() {
    python3 - "$((5210 + OFFSET))" "$1" "${2:-5}" <<'PY'
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5)
s.sendall(sys.argv[2].encode() + b"\n")
s.settimeout(float(sys.argv[3]))
out = b""
try:
    while True:
        b = s.recv(65536)
        if not b:
            break
        out += b
        s.settimeout(0.5)
except socket.timeout:
    pass
print(out.decode(errors="replace"))
PY
}
