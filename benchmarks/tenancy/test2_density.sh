#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Test 2: how many tenants fit. Adds VMs a step at a time; after each
# step sends point reads at random tenants and records the latency.
# Ends when p99 passes the target, queries fail, or the host reaches
# its memory floor. The last passing row is the density.
#   STEP=100 RATE=200 P99_MS=50 MAX_VMS=100000 ./test2_density.sh
source "$(dirname "$0")/lib.sh"
need_kvm "$@"
STEP=${STEP:-100}
RATE=${RATE:-200}
P99_MS=${P99_MS:-50}
# MAX_VMS caps the run, for a trial. With taps made ahead of time
# there are only so many, and making more needs sudo.
MAX_VMS=${MAX_VMS:-100000}
if [ -e "$WORK/.taps_precreated" ]; then
    taps=$(ip -br link | grep -c '^fctap')
    ((taps < MAX_VMS)) && MAX_VMS=$taps
fi
out=$RES/test2_density.csv

destroy_all
trap destroy_all EXIT
echo "tenants,pss_mib,host_avail_gib,p50_ms,p90_ms,p99_ms,max_ms,ok,errors" \
    | tee "$out"
n=0
while ((n + STEP <= MAX_VMS)); do
    if ! start_range "$n" $((n + STEP)); then
        echo "ended at the memory floor (${FLOOR_GIB} GiB available)" >&2
        break
    fi
    n=$((n + STEP))
    sleep 5
    lat=$("$ROOT/probe/probe" load -addrs "$(addrs_file "$n")" \
        -rate "$RATE" -dur 15s)
    echo "$n,$(($(vms_pss_kb) / 1024)),$(avail_gib),$lat" | tee -a "$out"
    p99=$(echo "$lat" | cut -d, -f3)
    errs=$(echo "$lat" | cut -d, -f6)
    if [ "$errs" -gt 0 ] || awk "BEGIN {exit !($p99 > $P99_MS)}"; then
        echo "ended at $n tenants: p99=${p99}ms errors=$errs" >&2
        break
    fi
done
