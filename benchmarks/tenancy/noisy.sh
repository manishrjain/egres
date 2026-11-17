#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Noisy neighbours: QUIET tenants send an occasional point read; next
# to them a growing number of heavy tenants each run one statement flat
# out on CONNS connections. What is reported is the quiet tenants'
# latency at each heavy count; the row with 0 heavy tenants is what the
# others are compared with (probe/noisy.go). Every tenant holds ROWS
# rows. Two kinds of heavy statement: cpu, a count over the table;
# write, an UPDATE of a random 1,000-row range.
#
# A micro-VM tenant can use no more than its vCPU, but nothing reserves
# host CPU for the quiet ones: past one heavy tenant per host thread
# they compete. On Egres nothing bounds a heavy tenant but the server's
# meter, which gives each active tenant an equal share; its per-tenant
# lines are saved after each step.
#   BACKEND=vm ./noisy.sh
#   KINDS="cpu write" HEAVY="0 1 2 4 8 16 32 64" QUIET=50 CONNS=4
#   RATE=50 DUR=60s HOLD=1 QHOLD=2 ROWS=100000
source "$(dirname "$0")/common.sh"
platform_init "$@"
KINDS=${KINDS:-cpu write}
HEAVY=${HEAVY:-0 1 2 4 8 16 32 64}
QUIET=${QUIET:-50}
CONNS=${CONNS:-4}
RATE=${RATE:-50}
DUR=${DUR:-60s}
ROWS=${ROWS:-100000}
# Connections each quiet tenant keeps open, as an application's pool
# does; 0 opens one per query, which measures the connection more
# than the neighbours.
QHOLD=${QHOLD:-2}
# A heavy tenant keeps its connections, as a busy application's pool
# would. HOLD=0 opens a new one for every statement, which adds a
# connection storm to the load.
hold=-hold
[ "${HOLD:-1}" = 1 ] || hold=
out=$RES/noisy.csv

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

platform_down
trap platform_down EXIT
platform_up
create_range 0 "$total" "$ROWS"
sleep 30
for ((i = 0; i < QUIET; i++)); do tenant_addr "$i"; done >"$RUN/quiet.txt"
for ((i = QUIET; i < total; i++)); do tenant_addr "$i"; done >"$RUN/heavy.txt"

echo "kind,heavy,conns,p50_ms,p90_ms,p99_ms,ok,errors," \
    "heavy_done,heavy_failed,heavy_mean_ms" | tr -d ' ' | tee "$out"
for kind in $KINDS; do
    sql=$(heavy_sql "$kind")
    for h in $HEAVY; do
        lat=$("$ROOT/probe/probe" noisy -quiet "$RUN/quiet.txt" \
            -heavy "$RUN/heavy.txt" -heavy-n "$h" -conns "$CONNS" $hold \
            -sql "$sql" -quiet-hold "$QHOLD" -rate "$RATE" -dur "$DUR" \
            -rows "$ROWS")
        echo "$kind,$h,$CONNS,$lat" | tee -a "$out"
        [ "$(type -t platform_dump)" = function ] &&
            platform_dump "noisy_${kind}_$h"
        sleep 5
    done
done
