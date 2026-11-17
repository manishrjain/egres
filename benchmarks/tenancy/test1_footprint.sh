#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Test 1: what an idle tenant costs. Brings the host to each tenant
# count in turn, lets it settle, and records the memory and disk the
# VMs hold. The per-tenant cost is the slope between counts.
#   ./test1_footprint.sh [counts...]     default: 1 10 100 400
source "$(dirname "$0")/lib.sh"
need_kvm "$@"
counts=("$@")
[ ${#counts[@]} -gt 0 ] || counts=(1 10 100 400)
SETTLE=${SETTLE:-30}
out=$RES/test1_footprint.csv

destroy_all
trap destroy_all EXIT
base_kb=$(avail_kb)
echo "tenants,pss_mib,pss_per_tenant_mib,host_avail_drop_mib,disk_mib" \
    | tee "$out"
have=0
for n in "${counts[@]}"; do
    if ! start_range "$have" "$n"; then
        echo "stopped below $n: host under ${FLOOR_GIB} GiB available" >&2
        break
    fi
    have=$n
    sleep "$SETTLE"
    pss=$(vms_pss_kb)
    echo "$n,$((pss / 1024)),$((pss / 1024 / n))," \
        "$(((base_kb - $(avail_kb)) / 1024)),$(($(vms_disk_kb) / 1024))" \
        | tr -d ' ' | tee -a "$out"
done
