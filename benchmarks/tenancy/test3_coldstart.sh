#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Test 3: time from waking a stopped tenant to its first row.
#   boot      a fresh VM: kernel boot, Postgres start, first query
#   restore   the same VM brought back from a memory snapshot
# The clock starts when the Firecracker process is launched and stops
# when the query's row arrives. Disk, tap and config are ready before
# the clock starts, as a platform would keep them.
#   RUNS=20 ./test3_coldstart.sh
source "$(dirname "$0")/lib.sh"
need_kvm "$@"
RUNS=${RUNS:-20}
out=$RES/test3_coldstart.csv
i=${VM:-0}
d=$RUN/vm$i
addr="$(vm_ip $i):5432"

api() { curl -sS --fail --unix-socket "$d/api.sock" -X "$1" \
    "http://localhost$2" -H 'Content-Type: application/json' -d "$3"; }

destroy_all
trap destroy_all EXIT
vm_prepare $i
# Without the fixed neighbour entry the first SYN waits on ARP, which
# retries once a second: every boot would read about 300 ms long.
ip neigh show dev "fctap$i" | grep -q PERMANENT || {
    echo "fctap$i has no fixed neighbour entry; try VM=<another>" >&2
    exit 1
}
echo "kind,run,first_row_ms" | tee "$out"

for ((r = 1; r <= RUNS; r++)); do
    vm_prepare $i
    t0=$(date +%s%N)
    vm_launch $i
    ms=$("$ROOT/probe/probe" first -addr "$addr" -t0 "$t0")
    echo "boot,$r,$ms" | tee -a "$out"
    vm_kill $i
    rm -f "$d/data.ext4"
done

# Take one snapshot of a tenant that has answered a query, then time
# bringing it back. The data disk stays as the snapshot left it.
vm_prepare $i
vm_launch $i
vm_wait $i
api PATCH /vm '{"state": "Paused"}'
api PUT /snapshot/create "{\"snapshot_type\": \"Full\",
    \"snapshot_path\": \"$d/snap.state\", \"mem_file_path\": \"$d/snap.mem\"}"
vm_kill $i
cp --sparse=always "$d/data.ext4" "$d/data.snap"

for ((r = 1; r <= RUNS; r++)); do
    cp --sparse=always "$d/data.snap" "$d/data.ext4"
    t0=$(date +%s%N)
    "$A/firecracker" --api-sock "$d/api.sock" >"$d/console.log" 2>&1 &
    echo $! >"$d/pid"
    until [ -S "$d/api.sock" ]; do sleep 0.001; done
    api PUT /snapshot/load "{\"snapshot_path\": \"$d/snap.state\",
        \"mem_backend\": {\"backend_type\": \"File\",
        \"backend_path\": \"$d/snap.mem\"}, \"resume_vm\": true}"
    ms=$("$ROOT/probe/probe" first -addr "$addr" -t0 "$t0")
    echo "restore,$r,$ms" | tee -a "$out"
    vm_kill $i
done

for kind in boot restore; do
    grep "^$kind," "$out" | cut -d, -f3 | sort -n | awk -v k=$kind '
        {a[NR] = $1}
        END {printf "%s: median %.1f ms, min %.1f, max %.1f (n=%d)\n",
            k, a[int((NR + 1) / 2)], a[1], a[NR], NR}'
done
