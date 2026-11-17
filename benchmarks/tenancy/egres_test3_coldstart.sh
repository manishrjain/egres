#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Test 3, Egres side: time for a tenant that is not in memory to give
# its first row. Egres has no per-tenant sleep state, so there is no
# one counterpart to booting a VM. Three kinds are timed:
#   process       the whole server is down: launch it, first row from
#                 one tenant (start-up, WAL replay, query)
#   process_nocal the same with no start-up disk probe: Egres
#                 measures the data directory's bandwidth for about
#                 2.5s at every start unless told the numbers, which
#                 egres_calibrate.sh measures once
#   first_touch   the server is up, warmed on its own database
#                 (egres_warm), and has not read this tenant since it
#                 started: connect and first row
#   warm          the same tenant asked again: connect and first row
# The clock starts before the launch (process kinds) or before the
# connect (the other two) and stops when the row arrives. Each
# first_touch run uses a tenant nothing has read since the restart.
#   TENANTS=1100 RUNS=20 ./egres_test3_coldstart.sh
# SETTLE is the wait before the server is first stopped: past the 30s
# flush age the rows are in tables on disk, not replayed into memory
# at the next start. DROP=1 empties the host's page cache before the
# start that first_touch reads from, so that read comes off the drive.
# OUT names the CSV.
source "$(dirname "$0")/egres_lib.sh"
RUNS=${RUNS:-20}
TENANTS=${TENANTS:-1100}
SETTLE=${SETTLE:-5}
DROP=${DROP:-0}
out=$RES/${OUT:-egres_test3_coldstart.csv}
((TENANTS > RUNS)) || { echo "TENANTS must exceed RUNS" >&2; exit 1; }

first_row() {
    "$ROOT/probe/probe" first -addr "$ADDR/$(tenant "$1")" -t0 "$2"
}

egres_wipe
trap egres_stop EXIT
egres_start
create_range 0 "$TENANTS"
sleep "$SETTLE"
egres_stop
echo "kind,run,first_row_ms" | tee "$out"

for ((r = 1; r <= RUNS; r++)); do
    t0=$(date +%s%N)
    PROBE=1 egres_launch
    ms=$(first_row 0 "$t0")
    echo "process,$r,$ms" | tee -a "$out"
    egres_stop
done
# No probe: the drive's profile passed in (egres_calibrate.sh); with
# none measured, the flag's own numbers.
nocal=()
[ -f "$RUN/egres_io.flags" ] || nocal=(--io="auto-calibrate=false")
for ((r = 1; r <= RUNS; r++)); do
    t0=$(date +%s%N)
    egres_launch "${nocal[@]}"
    ms=$(first_row 0 "$t0")
    echo "process_nocal,$r,$ms" | tee -a "$out"
    egres_stop
done

if [ "$DROP" = 1 ]; then
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
fi
egres_start
for ((r = 1; r <= RUNS; r++)); do
    ms=$(first_row "$r" 0)
    echo "first_touch,$r,$ms" | tee -a "$out"
done
for ((r = 1; r <= RUNS; r++)); do
    ms=$(first_row "$r" 0)
    echo "warm,$r,$ms" | tee -a "$out"
done

for kind in process process_nocal first_touch warm; do
    grep "^$kind," "$out" | cut -d, -f3 | sort -n | awk -v k=$kind '
        {a[NR] = $1}
        END {printf "%s: median %.1f ms, min %.1f, max %.1f (n=%d)\n",
            k, a[int((NR + 1) / 2)], a[1], a[NR], NR}'
done
