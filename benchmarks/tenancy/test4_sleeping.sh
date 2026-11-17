#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Test 4: sleeping tenants. A fixed memory budget, more tenants than
# fit awake in it, and traffic in which a tenth of the tenants are busy
# at a time (probe/fleet.go has the traffic and the control plane).
# For each tenant count it reports the latency of every query, the
# share that had to wake a tenant, and how many VMs the budget kept
# awake.
#
# Each tenant is put to sleep once, here, before any traffic: booted,
# read from, left until Postgres has stopped writing, then snapshotted
# and killed. From then on a wake restores that snapshot (WAKE=restore)
# or boots the VM afresh (WAKE=boot).
#   ./test4_sleeping.sh [counts...]      default: 100 200 500 1000 2000 4000
#   BUDGET_MIB=16384 RATE=200 HOT=0.10 SESSION=30s WARM=60s WAKE=restore
# The image comes from build_big_image.sh; there must be one tap per
# tenant (sudo ./setup.sh host).
source "$(dirname "$0")/lib.sh"
need_kvm "$@"
counts=("$@")
[ ${#counts[@]} -gt 0 ] || counts=(100 200 500 1000 2000 4000)
BUDGET_MIB=${BUDGET_MIB:-16384}
RATE=${RATE:-200}
HOT=${HOT:-0.10}
SESSION=${SESSION:-30s}
WARM=${WARM:-60s}
WAKE=${WAKE:-restore}
# VMs put to sleep at once. Each is awake for a little over a minute
# and holds about 90 MiB while it is.
PREP_PAR=${PREP_PAR:-48}
ROWS=$(cat "$A/data_big.rows")
out=$RES/test4_sleeping_$WAKE.csv

api() { curl -sS --fail --unix-socket "$1/api.sock" -X "$2" \
    "http://localhost$3" -H 'Content-Type: application/json' -d "$4"; }

mtime_ns() { date -r "$1" +%s%N; }

# A tenant is ready when its snapshot exists and its disk has not been
# written since the snapshot was taken.
prepared() {
    local d=$RUN/vm$1
    [ -f "$d/snap.stamp" ] && [ -f "$d/snap.mem" ] &&
        [ "$(mtime_ns "$d/data.ext4")" = "$(cat "$d/snap.stamp")" ]
}

put_to_sleep() {
    local i=$1 d=$RUN/vm$1 addr t0 last now quiet
    addr="$(vm_ip "$i"):5432"
    vm_kill "$i"
    rm -rf "$d"
    mkdir -p "$d"
    cp --sparse=always "$A/data_big.ext4" "$d/data.ext4"
    vm_prepare "$i"
    t0=$(date +%s)
    vm_launch "$i"
    vm_wait "$i"
    # What a tenant's first clients would leave behind: the caches a
    # connection fills, for both databases autovacuum will visit.
    for _ in 1 2 3 4 5; do
        "$ROOT/probe/probe" sql -addr "$addr" \
            -q "SELECT v FROM t WHERE id = $((RANDOM % ROWS + 1))"
    done
    "$ROOT/probe/probe" sql -addr "$addr/template1" -q "SELECT 1"
    "$ROOT/probe/probe" sql -addr "$addr" -q "CHECKPOINT"
    # A write after the snapshot would make the snapshot unusable, so
    # wait until the guest has nothing left to write. Postgres logs one
    # more record within 15s of a checkpoint, and the guest kernel may
    # hold a dirty page for 30s before writing it: 40s with the disk
    # untouched covers both.
    while (($(date +%s) - t0 < 20)); do sleep 0.5; done
    last=$(mtime_ns "$d/data.ext4")
    quiet=0
    while ((quiet < 40)); do
        sleep 5
        now=$(mtime_ns "$d/data.ext4")
        if [ "$now" = "$last" ]; then quiet=$((quiet + 5)); else quiet=0; fi
        last=$now
    done
    api "$d" PATCH /vm '{"state": "Paused"}'
    api "$d" PUT /snapshot/create "{\"snapshot_type\": \"Full\",
        \"snapshot_path\": \"$d/snap.state\",
        \"mem_file_path\": \"$d/snap.mem\"}"
    vm_kill "$i"
    mtime_ns "$d/data.ext4" >"$d/snap.stamp"
}

prepare_range() {
    local to=$1 i todo=() pids=() failed=0
    for ((i = 0; i < to; i++)); do prepared "$i" || todo+=("$i"); done
    [ ${#todo[@]} -gt 0 ] || return 0
    echo "putting ${#todo[@]} tenants to sleep, $PREP_PAR at a time" >&2
    for ((i = 0; i < ${#todo[@]}; i += PREP_PAR)); do
        have_room || { echo "host is out of room" >&2; return 1; }
        pids=()
        for n in "${todo[@]:i:PREP_PAR}"; do
            put_to_sleep "$n" >/dev/null &
            pids+=($!)
        done
        for p in "${pids[@]}"; do wait "$p" || failed=$((failed + 1)); done
    done
    [ "$failed" = 0 ] || {
        echo "$failed tenants failed to prepare" >&2
        return 1
    }
    sync
}

max=0
for n in "${counts[@]}"; do ((n > max)) && max=$n; done
prepare_range "$max"

cols="p50_ms,p90_ms,p99_ms,p999_ms,max_ms,ok,errors,woke_pct"
cols+=",wake_p50_ms,wake_p99_ms,waited,awake_avg,awake_peak"
cols+=",mem_avg_mib,mem_peak_mib,evictions,stale"
echo "tenants,wake,budget_mib,$cols" | tee "$out"
for n in "${counts[@]}"; do
    lat=$("$ROOT/probe/probe" fleet -mode vm -n "$n" -run "$RUN" \
        -fc "$A/firecracker" -wake "$WAKE" -budget-mib "$BUDGET_MIB" \
        -rate "$RATE" -hot "$HOT" -session "$SESSION" -warm "$WARM" \
        -rows "$ROWS" -trace "$RES/test4_${WAKE}_$n.trace")
    echo "$n,$WAKE,$BUDGET_MIB,$lat" | tee -a "$out"
done
echo "on disk for $max tenants:" \
    "snapshots $(du -sm --total "$RUN"/vm*/snap.mem |
        awk 'END {print $1}') MiB," \
    "data $(du -sm --total "$RUN"/vm*/data.ext4 | awk 'END {print $1}') MiB" >&2
