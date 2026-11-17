#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Control: every tenant as a database in one stock Postgres 18, no VM.
# At each tenant count it reports the server's memory in three states
# and the latency that goes with two of them:
#   idle    every tenant written, nobody connected
#   load    test 2's traffic: point reads at random tenants, a new
#           connection per query (memory read while it runs)
#   held    one connection kept open per tenant and used, which is
#           what a pooler per tenant leaves on the server (a Postgres
#           connection belongs to one database, so tenants cannot
#           share a pool)
# Each tenant is the micro-VM tenant: table t, 1,000 rows.
# Needs docker (image PG_IMAGE, env.sh) and sudo, to read the memory of the
# container's processes.
#   ./pg_shared.sh [counts...]      default: 100 400 1100
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
RUN=$WORK/run
PGDATA_DIR=${PGDATA_DIR:-$WORK/pg-shared}
PORT=${PORT:-5440}
NAME=tb-pg-shared
RATE=${RATE:-200}
SETTLE=${SETTLE:-30}
counts=("$@")
[ ${#counts[@]} -gt 0 ] || counts=(100 400 1100)
out=$RES/pg_shared.csv
mkdir -p "$RUN" "$RES"

sql() { psql "host=127.0.0.1 port=$PORT user=postgres dbname=$1" \
    -X -q -v ON_ERROR_STOP=1 -c "$2" >/dev/null; }

# Proportional set size of every process in the container, in KiB.
pss_kb() {
    local pid sum=0 kb
    for pid in $(docker top "$NAME" -eo pid | tail -n +2); do
        kb=$(sudo awk '/^Pss:/ {print $2}' "/proc/$pid/smaps_rollup" \
            2>/dev/null || echo 0)
        sum=$((sum + ${kb:-0}))
    done
    echo "$sum"
}
backends() { docker top "$NAME" -eo pid | tail -n +2 | wc -l; }
mib() { awk -v kb="$1" 'BEGIN {printf "%.1f", kb / 1024}'; }

stop() {
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    sudo rm -rf "$PGDATA_DIR"
}
stop
trap stop EXIT
mkdir -p "$PGDATA_DIR"
# Stock settings but for the connection limit, which has to admit one
# connection per tenant.
docker run -d --name "$NAME" --network host \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -v "$PGDATA_DIR:/var/lib/postgresql" "$PG_IMAGE" \
    -c port="$PORT" -c listen_addresses=127.0.0.1 \
    -c max_connections=$((counts[-1] + 100)) >/dev/null
for _ in $(seq 300); do
    sql postgres "SELECT 1" 2>/dev/null && break
    sleep 0.2
done
sql postgres "CREATE DATABASE tmpl"
sql tmpl "CREATE TABLE t (id bigint PRIMARY KEY, v text)"
sql tmpl "INSERT INTO t SELECT g, md5(g::text) FROM generate_series(1, 1000) g"
sql tmpl "VACUUM (FREEZE, ANALYZE)"

targets() {
    local n=$1 f=$RUN/pg_targets.txt i
    for ((i = 0; i < n; i++)); do echo "127.0.0.1:$PORT/tenant_$i"; done >"$f"
    echo "$f"
}

echo "tenants,state,pss_mib,processes,p50_ms,p90_ms,p99_ms,max_ms,ok,errors" \
    | tee "$out"
have=0
for n in "${counts[@]}"; do
    for ((i = have; i < n; i++)); do
        sql postgres "CREATE DATABASE tenant_$i TEMPLATE tmpl"
    done
    have=$n
    sql postgres CHECKPOINT
    sleep "$SETTLE"
    echo "$n,idle,$(mib "$(pss_kb)"),$(backends),,,,,," | tee -a "$out"

    f=$(targets "$n")
    "$ROOT/probe/probe" load -addrs "$f" -rate "$RATE" -dur 30s \
        >"$RUN/pg_load.out" &
    probe=$!
    sleep 15
    pss=$(pss_kb)
    procs=$(backends)
    wait "$probe"
    echo "$n,load,$(mib "$pss"),$procs,$(cat "$RUN/pg_load.out")" \
        | tee -a "$out"

    # The probe opens and uses one connection per tenant before its
    # 10 s warm-up, then sends the same point reads over them for 30 s.
    "$ROOT/probe/probe" noisy -quiet "$f" -heavy "$f" \
        -heavy-n 0 -sql "SELECT 1" -quiet-hold 1 -rate "$RATE" \
        -dur 30s -rows 1000 >"$RUN/pg_held.out" &
    probe=$!
    sleep 30
    pss=$(pss_kb)
    procs=$(backends)
    wait "$probe"
    # noisy's row: p50,p90,p99,p99.9,max,ok,errors,...
    lat=$(cut -d, -f1-3,5-7 "$RUN/pg_held.out")
    echo "$n,held,$(mib "$pss"),$procs,$lat" | tee -a "$out"
    sleep 5
done
