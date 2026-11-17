#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Test 5, Egres side: noisy neighbours. The tenants, traffic and heavy
# statements of test5_noisy.sh, with every tenant a database on one
# server. Nothing bounds a heavy tenant here but the server's own
# meter, which gives each active tenant an equal share of the machine.
# After each step the meter's per-tenant line is saved beside the
# results, so a heavy tenant that was held back shows as parked time.
#   KINDS="cpu write" HEAVY="0 1 2 4 8 16 32 64" ./egres_test5_noisy.sh
#   QUIET=50 CONNS=4 RATE=50 DUR=60s HOLD=1 QHOLD=2 ROWS=100000
# The data lives in egres-data-noisy/, apart from test 4's.
source "$(dirname "$0")/env.sh"
export EDATA=$WORK/egres-data-noisy
source "$(dirname "$0")/egres_lib.sh"
KINDS=${KINDS:-cpu write}
HEAVY=${HEAVY:-0 1 2 4 8 16 32 64}
QUIET=${QUIET:-50}
CONNS=${CONNS:-4}
RATE=${RATE:-50}
DUR=${DUR:-60s}
# Connections each quiet tenant keeps open, as an application's pool
# does; 0 opens one per query, which measures the connection more
# than the neighbours.
QHOLD=${QHOLD:-2}
# A heavy tenant keeps its connections, as a busy application's pool
# would. HOLD=0 opens a new one for every statement, which adds a
# connection storm to the load.
hold=-hold
[ "${HOLD:-1}" = 1 ] || hold=
out=$RES/egres_test5_noisy.csv

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

egres_wipe
trap egres_stop EXIT
egres_start
load_big_range 0 "$total"
sleep 30
for ((i = 0; i < QUIET; i++)); do echo "$ADDR/$(tenant "$i")"; done \
    >"$RUN/quiet.txt"
for ((i = QUIET; i < total; i++)); do echo "$ADDR/$(tenant "$i")"; done \
    >"$RUN/heavy.txt"

cols="p50_ms,p90_ms,p99_ms,p999_ms,max_ms,ok,errors"
cols+=",heavy_done,heavy_failed,heavy_mean_ms"
echo "kind,heavy,conns,$cols" | tee "$out"
for kind in $KINDS; do
    sql=$(heavy_sql "$kind")
    for h in $HEAVY; do
        lat=$("$ROOT/probe/probe" noisy -quiet "$RUN/quiet.txt" \
            -heavy "$RUN/heavy.txt" -heavy-n "$h" -conns "$CONNS" $hold \
            -sql "$sql" -quiet-hold "$QHOLD" -rate "$RATE" -dur "$DUR" \
            -rows "$ROWS")
        echo "$kind,$h,$CONNS,$lat" | tee -a "$out"
        egres_admin tenants >"$RES/egres_test5_${kind}_$h.tenants" 2>&1 || true
        sleep 5
    done
done
