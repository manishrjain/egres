#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Test 1, Egres side: what an idle tenant costs. Brings one server to
# each tenant count in turn, lets it settle, and records the memory
# and disk it holds. Row 0 is the server with no tenants: the fixed
# cost. The per-tenant cost is the slope between counts.
#
# Two states per count would double the run, so the restart is done
# once, at the last count: "warm" rows are tenants just written,
# "restarted" is the same server stopped and started with every tenant
# untouched since.
#   ./egres_test1_footprint.sh [counts...]     default: 1 10 100 400
source "$(dirname "$0")/egres_lib.sh"
counts=("$@")
[ ${#counts[@]} -gt 0 ] || counts=(1 10 100 400)
# Past the 30s flush age, so memtables have reached disk.
SETTLE=${SETTLE:-45}
out=$RES/egres_test1_footprint.csv

row() {
    local state=$1 n=$2 pss
    pss=$(egres_pss_kb)
    echo "$state,$n,$(mib "$pss"),$(mib $((base_kb - $(avail_kb))))," \
        "$(mib "$(egres_store_kb)"),$(mib "$(egres_disk_kb)")" | tr -d ' '
}

egres_wipe
trap egres_stop EXIT
base_kb=$(avail_kb)
egres_start
echo "state,tenants,pss_mib,host_avail_drop_mib,store_mib,disk_mib" \
    | tee "$out"
sleep 10
row warm 0 | tee -a "$out"
have=0
for n in "${counts[@]}"; do
    if ! create_range "$have" "$n"; then
        echo "stopped below $n: host under ${FLOOR_GIB} GiB available" >&2
        break
    fi
    have=$n
    sleep "$SETTLE"
    row warm "$n" | tee -a "$out"
done

egres_stop
egres_start
sleep "$SETTLE"
row restarted "$have" | tee -a "$out"
