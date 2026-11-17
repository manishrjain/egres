#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Test 2, Egres side: how many tenants fit. Adds databases a step at a
# time; after each step sends point reads at random tenants and records
# the latency. Ends when p99 passes the target, queries fail, the host
# reaches its memory floor, or MAX tenants are in (Egres may not reach
# a limit where the micro-VM host did; MAX keeps the run finite).
#   STEP=100 RATE=200 P99_MS=50 MAX=1200 ./egres_test2_density.sh
source "$(dirname "$0")/egres_lib.sh"
STEP=${STEP:-100}
RATE=${RATE:-200}
P99_MS=${P99_MS:-50}
MAX=${MAX:-1200}
out=$RES/${OUT:-egres_test2_density.csv}

egres_wipe
trap egres_stop EXIT
egres_start
cols="p50_ms,p90_ms,p99_ms,max_ms,ok,errors"
echo "tenants,pss_mib,host_avail_gib,$cols,open_files" | tee "$out"
n=0
while ((n < MAX)); do
    if ! create_range "$n" $((n + STEP)); then
        echo "ended at the memory floor (${FLOOR_GIB} GiB available)" >&2
        break
    fi
    n=$((n + STEP))
    sleep 5
    lat=$("$ROOT/probe/probe" load -addrs "$(targets_file "$n")" \
        -rate "$RATE" -dur 15s)
    row="$n,$(mib "$(egres_pss_kb)"),$(avail_gib),$lat,$(egres_fds)"
    echo "$row" | tee -a "$out"
    p99=$(echo "$lat" | cut -d, -f3)
    errs=$(echo "$lat" | cut -d, -f6)
    if [ "$errs" -gt 0 ] || awk "BEGIN {exit !($p99 > $P99_MS)}"; then
        echo "ended at $n tenants: p99=${p99}ms errors=$errs" >&2
        break
    fi
done
((n < MAX)) || echo "ended at MAX=$MAX tenants with no limit reached" >&2
# The stop after the last step, timed: every tenant written, the
# latest of them still in memory.
t0=$(date +%s%N)
egres_stop
echo "stopped in $((($(date +%s%N) - t0) / 1000000)) ms" >&2
