#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Footprint and density: what a tenant costs, and how many fit. Brings
# the platform to each count in turn, every tenant made through SQL
# (its database, table t and ROWS rows); after each count sends point
# reads at random tenants and records the latency, and the memory and
# disk the tenants hold. After the last count it settles for SETTLE
# seconds, sampling the memory every SAMPLE seconds: what a tenant
# costs once it has gone quiet. Then, where the platform can, it is
# restarted on the same data and timed to a first row, and ten tenants
# nothing has read since are touched. Ends early at the memory floor.
#
# The per-tenant cost is the slope between counts; footprint_fit.txt
# estimates from it how many tenants this host's memory holds.
#   BACKEND=egres ./footprint.sh [counts...]   default: 1 10 100 400 1100
#   ROWS=1000 RATE=200 DUR=15s SETTLE=600 SAMPLE=60 RESTART=1 CREATE_PAR=16
source "$(dirname "$0")/common.sh"
platform_init "$@"
counts=("$@")
[ ${#counts[@]} -gt 0 ] || counts=(1 10 100 400 1100)
ROWS=${ROWS:-1000}
RATE=${RATE:-200}
DUR=${DUR:-15s}
SETTLE=${SETTLE:-600}
SAMPLE=${SAMPLE:-60}
RESTART=${RESTART:-1}
out=$RES/footprint.csv

fds() { [ "$(type -t tenants_fds)" = function ] && tenants_fds || true; }
# One row: the state, then memory, disk and whatever the caller adds.
row() {
    local n=$1 state=$2
    shift 2
    echo "$n,$state,$(mib "$(tenants_pss_kb)")," \
        "$(mib $((base_kb - $(avail_kb)))),$(mib "$(tenants_disk_kb)")," \
        "$(fds),$*" | tr -d ' ' | tee -a "$out"
}
probe_first() { "$ROOT/probe/probe" first -addr "$(tenant_addr "$1")" -t0 "$2" \
    -timeout 1800s; }

platform_down
trap platform_down EXIT
base_kb=$(avail_kb)
platform_up
echo "tenants,state,pss_mib,host_avail_drop_mib,disk_mib,open_files," \
    "p50_ms,p90_ms,p99_ms,ok,errors,first_row_ms" | tr -d ' ' | tee "$out"
sleep 5
# The platform with no tenants: its fixed cost.
row 0 loaded ",,,,,"
have=0
for n in "${counts[@]}"; do
    if ! create_range "$have" "$n" "$ROWS"; then
        echo "stopped below $n" >&2
        break
    fi
    have=$n
    sleep 5
    row "$n" loaded "$(point_reads "$(targets_file "$n" all)" "$RATE" "$DUR"),"
done

# Settle: memory every SAMPLE seconds, nothing else running.
for ((t = SAMPLE; t <= SETTLE; t += SAMPLE)); do
    sleep "$SAMPLE"
    row "$have" "settled_${t}s" ",,,,,"
done

if [ "$RESTART" != 0 ] && [ "$(type -t platform_restart)" = function ]; then
    t0=$(date +%s%N)
    platform_restart
    ms=$(probe_first 0 "$t0")
    sleep 10
    row "$have" restarted ",,,,,$ms"
    # Ten tenants nothing has read since the start, spread over the
    # range; the median is the cost of a first touch.
    touched=()
    for i in 1 2 3 4 5 6 7 8 9 10; do
        touched+=("$(probe_first $((have * i / 11)) 0)")
    done
    med=$(printf '%s\n' "${touched[@]}" | sort -n | sed -n 6p)
    row "$have" touched ",,,,,$med"
fi

# The estimate. The cost per tenant is the slope between the last two
# counts: the first tenants also pay for the platform's pools and
# arenas, which the fixed cost (the intercept) absorbs. The settled
# figure scales that slope by what the last count gave back.
awk -F, -v total_kb="$(total_kb)" -v floor_gib="$FLOOR_GIB" '
    $2 == "loaded" && $1 > 0 {pn = n; ploaded = loaded; n = $1; loaded = $3}
    $2 ~ /^settled/ {settled = $3}
    END {
        if (n == 0 || n == pn) exit
        slope = (loaded - ploaded) * 1024 / (n - pn)
        fixed = loaded - slope * n / 1024
        if (settled > 0)
            slope_settled = slope * (settled - fixed) / (loaded - fixed)
        room = total_kb / 1024 - floor_gib * 1024 - fixed
        printf "fixed cost: %.0f MiB (intercept)\n", fixed
        printf "per tenant: %.0f KiB just loaded", slope
        if (settled > 0) printf ", %.0f KiB settled", slope_settled
        printf "\n"
        if (slope_settled > 0)
            printf "fit: about %d tenants in %.0f MiB above the floor\n",
                room * 1024 / slope_settled, room
    }' "$out" | tee "$RES/footprint_fit.txt" >&2
