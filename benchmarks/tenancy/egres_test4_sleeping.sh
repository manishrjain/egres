#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Test 4, Egres side: the traffic of the sleeping-tenant test
# (probe/fleet.go) against one server holding every tenant as a
# database, with the server's memory capped at the same budget the
# micro-VMs get. There is no control plane here: nothing is put to
# sleep or woken, and what stays in memory is the server's own choice.
#
# Each tenant holds the rows of assets/data_big.ext4, so with enough
# tenants the data is larger than the budget and a quiet tenant's rows
# have to come from disk.
#   ./egres_test4_sleeping.sh [counts...]   default: 100 200 500 1000 2000 4000
#   BUDGET_MIB=16384 RATE=200 HOT=0.10 SESSION=30s WARM=60s ROWS=100000
#   KEEP=1 keeps the data directory of an earlier run and adds to it.
source "$(dirname "$0")/egres_lib.sh"
counts=("$@")
[ ${#counts[@]} -gt 0 ] || counts=(100 200 500 1000 2000 4000)
BUDGET_MIB=${BUDGET_MIB:-16384}
RATE=${RATE:-200}
HOT=${HOT:-0.10}
SESSION=${SESSION:-30s}
WARM=${WARM:-60s}
out=$RES/egres_test4_sleeping.csv
have_file=$EDATA/.big_tenants

# Resident memory from the process's own counter. Reading smaps here
# would stall a server this size for hundreds of milliseconds.
rss_mib() {
    awk '/^VmRSS:/ {printf "%d", $2 / 1024}' \
        "/proc/$(cat "$RUN/egres.pid")/status"
}

have=0
if [ "${KEEP:-0}" = 1 ] && [ -f "$have_file" ]; then
    have=$(cat "$have_file")
    egres_stop
else
    egres_wipe
fi
trap egres_stop EXIT
egres_launch --memory="total-bytes=$((BUDGET_MIB * 1048576))"
egres_wait && egres_warm || exit 1

cols="p50_ms,p90_ms,p99_ms,p999_ms,max_ms,ok,errors"
echo "tenants,budget_mib,$cols,rss_peak_mib,rss_end_mib,store_mib" | tee "$out"
for n in "${counts[@]}"; do
    if ((n > have)); then
        t0=$(date +%s)
        load_big_range "$have" "$n"
        echo "$n" >"$have_file"
        echo "loaded tenants $have..$n in $(($(date +%s) - t0))s" >&2
        have=$n
        sleep 30
    fi
    peak_file=$RUN/rss_peak
    (
        peak=0
        while :; do
            now=$(rss_mib)
            ((now > peak)) && peak=$now && echo "$peak" >"$peak_file"
            sleep 2
        done
    ) &
    sampler=$!
    lat=$("$ROOT/probe/probe" fleet -mode shared -n "$n" -addr "$ADDR" \
        -rate "$RATE" -hot "$HOT" -session "$SESSION" -warm "$WARM" \
        -rows "$ROWS" -trace "$RES/egres_test4_$n.trace")
    kill "$sampler" 2>/dev/null || true
    wait "$sampler" 2>/dev/null || true
    # Only the latency columns: the rest of fleet's row is for VMs.
    lat=$(echo "$lat" | cut -d, -f1-7)
    echo "$n,$BUDGET_MIB,$lat,$(cat "$peak_file"),$(rss_mib)," \
        "$(mib "$(egres_store_kb)")" | tr -d ' ' | tee -a "$out"
    egres_admin mem >"$RES/egres_test4_$n.mem" 2>&1 || true
done
