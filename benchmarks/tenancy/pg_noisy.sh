#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Control for test 5: the noisy-neighbour traffic of test5_noisy.sh
# against one stock Postgres 18 holding every tenant as a database, no
# VM. Nothing bounds a heavy tenant here: its backends compete with
# the quiet tenants' for the host's threads, and every tenant shares
# one WAL and one checkpointer.
#   KINDS="cpu write" HEAVY="0 1 2 4 8 16 32 64" ./pg_noisy.sh
#   QUIET=50 CONNS=4 RATE=50 DUR=60s HOLD=1 QHOLD=2 ROWS=100000
# Needs docker (image PG_IMAGE, env.sh). The data lives in pg-noisy/
# under WORK, so the writes reach the drive being measured.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
RUN=$WORK/run
PGDATA_DIR=${PGDATA_DIR:-$WORK/pg-noisy}
PORT=${PORT:-5441}
NAME=tb-pg-noisy
KINDS=${KINDS:-cpu write}
HEAVY=${HEAVY:-0 1 2 4 8 16 32 64}
QUIET=${QUIET:-50}
CONNS=${CONNS:-4}
RATE=${RATE:-50}
DUR=${DUR:-60s}
QHOLD=${QHOLD:-2}
ROWS=${ROWS:-100000}
hold=-hold
[ "${HOLD:-1}" = 1 ] || hold=
out=$RES/pg_noisy.csv
mkdir -p "$RUN" "$RES"

sql() { psql "host=127.0.0.1 port=$PORT user=postgres dbname=$1" \
    -X -q -v ON_ERROR_STOP=1 -c "$2" >/dev/null; }

heavy_sql() {
    case $1 in
    cpu) echo "SELECT count(*) FROM t" ;;
    write) echo "UPDATE t SET v = '{s}' WHERE id BETWEEN {r} AND {e}" ;;
    *) echo "unknown kind: $1" >&2; exit 1 ;;
    esac
}

max=0
for h in $HEAVY; do ((h > max)) && max=$h; done
total=$((QUIET + max))

stop() {
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    sudo rm -rf "$PGDATA_DIR"
}
stop
trap stop EXIT
mkdir -p "$PGDATA_DIR"
# Stock settings but for the connection limit: the quiet tenants'
# held connections, the heavy tenants' at the largest step, and room
# for the loader.
docker run -d --name "$NAME" --network host \
    -e POSTGRES_HOST_AUTH_METHOD=trust \
    -v "$PGDATA_DIR:/var/lib/postgresql" "$PG_IMAGE" \
    -c port="$PORT" -c listen_addresses=127.0.0.1 \
    -c max_connections=$((QUIET * QHOLD + max * CONNS + 100)) >/dev/null
for _ in $(seq 300); do
    sql postgres "SELECT 1" 2>/dev/null && break
    sleep 0.2
done
# The big tenant of build_big_image.sh, once, then a copy per tenant.
sql postgres "CREATE DATABASE tmpl"
sql tmpl "CREATE TABLE t (id bigint PRIMARY KEY, v text)"
sql tmpl "INSERT INTO t SELECT g, md5(g::text) FROM generate_series(1, $ROWS) g"
sql tmpl "VACUUM (FREEZE, ANALYZE)"
for ((i = 0; i < total; i++)); do
    sql postgres "CREATE DATABASE tenant_$i TEMPLATE tmpl"
done
sql postgres CHECKPOINT
sleep 30
for ((i = 0; i < QUIET; i++)); do echo "127.0.0.1:$PORT/tenant_$i"; done \
    >"$RUN/pg_quiet.txt"
for ((i = QUIET; i < total; i++)); do echo "127.0.0.1:$PORT/tenant_$i"; done \
    >"$RUN/pg_heavy.txt"

cols="p50_ms,p90_ms,p99_ms,p999_ms,max_ms,ok,errors"
cols+=",heavy_done,heavy_failed,heavy_mean_ms"
echo "kind,heavy,conns,$cols" | tee "$out"
for kind in $KINDS; do
    sql=$(heavy_sql "$kind")
    for h in $HEAVY; do
        lat=$("$ROOT/probe/probe" noisy -quiet "$RUN/pg_quiet.txt" \
            -heavy "$RUN/pg_heavy.txt" -heavy-n "$h" -conns "$CONNS" $hold \
            -sql "$sql" -quiet-hold "$QHOLD" -rate "$RATE" -dur "$DUR" \
            -rows "$ROWS")
        echo "$kind,$h,$CONNS,$lat" | tee -a "$out"
        sleep 5
    done
done
