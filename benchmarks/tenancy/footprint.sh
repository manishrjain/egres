#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Footprint and density: what a tenant costs, and how many fit. Brings
# the platform to each count in turn, every tenant made through SQL
# (its database, table t and ROWS rows); after each count sends point
# reads at random tenants and records the latency, and the memory and
# disk the tenants hold. After the last count it settles for SETTLE
# seconds, sampling the memory every SAMPLE seconds: what a tenant
# costs once it has gone quiet. Ends early at the memory floor. (A
# tenant's first row after idleness is the agents test's first_*
# columns.)
#
# Three memory columns: pss_mib is the tenants' processes; held_mib is
# what the cgroup holds that the kernel cannot simply drop (all but
# the clean, unmapped file cache), what the budget is on; cgroup_mib
# is everything charged. The per-tenant cost is the slope of held_mib
# between counts; footprint_fit.txt estimates from it how many fit in
# MEM_GIB's budget. The counts stop when the tenants hold that much.
#   BACKEND=egres ./footprint.sh [counts...]   default: 1 10 100 300
#   ROWS=1000 RATE=200 DUR=15s SETTLE=600 SAMPLE=60 CREATE_PAR=16
source "$(dirname "$0")/common.sh"
in_cgroup "$@"
platform_init "$@"
counts=("$@")
[ ${#counts[@]} -gt 0 ] || counts=(1 10 100 300)
ROWS=${ROWS:-1000}
RATE=${RATE:-200}
DUR=${DUR:-15s}
SETTLE=${SETTLE:-600}
SAMPLE=${SAMPLE:-60}
out=$RES/footprint.csv

fds() { [ "$(type -t tenants_fds)" = function ] && tenants_fds || true; }
cache() {
    [ "$(type -t tenants_cache_kb)" = function ] && mib "$(tenants_cache_kb)" ||
        true
}
# One row: the state, then memory, disk and whatever the caller adds.
row() {
    local n=$1 state=$2
    shift 2
    echo "$n,$state,$(mib "$(tenants_pss_kb)"),$(mib "$(mem_held_kb)")," \
        "$(mib "$(cgroup_kb)"),$(cache),$(mib "$(tenants_disk_kb)")," \
        "$(fds),$*" | tr -d ' ' | tee -a "$out"
}

platform_down
trap platform_down EXIT
platform_up
# cache_mib: a server's block cache (Egres's memory report), the
# server's choice rather than the tenants' cost; the fit takes it off.
echo "tenants,state,pss_mib,held_mib,cgroup_mib,cache_mib,disk_mib," \
    "open_files,p50_ms,p95_ms,p99_ms,ok,errors" | tr -d ' ' | tee "$out"
sleep 5
# The platform with no tenants: its fixed cost.
row 0 loaded ",,,,"
have=0
for n in "${counts[@]}"; do
    if create_range "$have" "$n" "$ROWS"; then
        have=$n
    else
        # Out of room on the way: the tenants that were made are
        # measured as the last count, and the run goes on from there.
        [ "$MADE" -gt "$have" ] || break
        have=$MADE
        stopped=1
    fi
    sleep 5
    row "$have" loaded \
        "$(point_reads "$(targets_file "$have" all)" "$RATE" "$DUR")"
    [ -z "${stopped:-}" ] || break
done

# Settle: memory every SAMPLE seconds, nothing else running.
for ((t = SAMPLE; t <= SETTLE; t += SAMPLE)); do
    sleep "$SAMPLE"
    row "$have" "settled_${t}s" ",,,,"
done

# The estimate, on held_mib less cache_mib. The cost per tenant is the
# slope between the last two counts: the first tenants also pay for
# the platform's pools and arenas, which the fixed cost (the
# intercept) absorbs. The settled figure scales that slope by what the
# last count gave back.
awk -F, -v mem_gib="$MEM_GIB" -v budget_mib="$(($(budget_kb) / 1024))" '
    $2 == "loaded" && $1 > 0 {
        pn = n; ploaded = loaded; n = $1; loaded = $4 - $6
    }
    $2 ~ /^settled/ {settled = $4 - $6}
    END {
        if (n == 0 || n == pn) exit
        slope = (loaded - ploaded) * 1024 / (n - pn)
        fixed = loaded - slope * n / 1024
        if (settled > 0)
            slope_settled = slope * (settled - fixed) / (loaded - fixed)
        room = budget_mib - fixed
        printf "fixed cost: %.0f MiB (intercept)\n", fixed
        printf "per tenant: %.0f KiB just loaded", slope
        if (settled > 0) printf ", %.0f KiB settled", slope_settled
        printf "\n"
        if (slope_settled > 0)
            printf "fit: about %d tenants in %d GiB " \
                "(%.0f MiB budget for tenants past the fixed cost)\n",
                room * 1024 / slope_settled, mem_gib, room
    }' "$out" | tee "$RES/footprint_fit.txt" >&2
