#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Test 5: noisy neighbours. QUIET tenants send an occasional point
# read; next to them a growing number of heavy tenants each run one
# statement flat out on CONNS connections. What is reported is the
# quiet tenants' latency at each heavy count; the row with 0 heavy
# tenants is what the others are compared with (probe/noisy.go).
#
# Every tenant is its own VM with one vCPU, all awake. A heavy tenant
# can use no more than its vCPU, but nothing reserves host CPU for the
# quiet ones: past one heavy tenant per host thread they compete.
#   KINDS="cpu write" HEAVY="0 1 2 4 8 16 32 64" ./test5_noisy.sh
#   QUIET=50 CONNS=4 RATE=50 DUR=60s HOLD=1 QHOLD=2
# The image comes from build_big_image.sh. The VMs live in run_noisy/,
# apart from the sleeping tenants of test 4, and their disks pass a
# guest flush on to the host so a write waits for the disk as it would
# in Egres.
source "$(dirname "$0")/env.sh"
RUN_DIR=$WORK/run_noisy
export RUN_DIR DRIVE_CACHE=${DRIVE_CACHE:-Writeback}
source "$(dirname "$0")/lib.sh"
need_kvm "$@"
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
ROWS=$(cat "$A/data_big.rows")
out=$RES/test5_noisy.csv

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

destroy_all
trap destroy_all EXIT
for ((i = 0; i < total; i++)); do
    mkdir -p "$RUN/vm$i"
    cp --sparse=always "$A/data_big.ext4" "$RUN/vm$i/data.ext4"
done
start_range 0 "$total"
for ((i = 0; i < QUIET; i++)); do echo "$(vm_ip "$i"):5432"; done \
    >"$RUN/quiet.txt"
for ((i = QUIET; i < total; i++)); do echo "$(vm_ip "$i"):5432"; done \
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
        sleep 5
    done
done
