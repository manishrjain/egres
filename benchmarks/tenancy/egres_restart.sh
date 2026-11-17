#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# After a density run: how long the server takes to come back with
# every tenant the run left in its data directory, and what the first
# touch of some of them costs then, and how long a clean stop takes
# (SIGTERM to exit). The data is kept. OUT names the CSV.
#   TENANTS=<count the run reached> ./egres_restart.sh
source "$(dirname "$0")/egres_lib.sh"
TENANTS=${TENANTS:?tenants in the data directory}
out=$RES/${OUT:-egres_restart.csv}
trap egres_stop EXIT
egres_stop
echo "tenants,kind,ms" | tee "$out"
# restart: as it comes, the start-up disk probe included.
# restart_nocal: no probe, the drive's profile passed in
# (egres_calibrate.sh); with none measured, the flag's own numbers.
for kind in restart restart_nocal; do
    flags=()
    probe=1
    if [ "$kind" = restart_nocal ]; then
        probe=0
        [ -f "$RUN/egres_io.flags" ] || flags=(--io="auto-calibrate=false")
    fi
    t0=$(date +%s%N)
    PROBE=$probe egres_launch "${flags[@]}"
    ms=$("$ROOT/probe/probe" first -addr "$ADDR/$(tenant 0)" -t0 "$t0" \
        -timeout 1800s)
    echo "$TENANTS,$kind,$ms" | tee -a "$out"
    # The start is timed above; a tenant's first touch is timed on a
    # warmed server.
    egres_warm
    for i in 1 2 3 4 5 6 7 8 9 10; do
        t=$((TENANTS * i / 11))
        ms=$("$ROOT/probe/probe" first -addr "$ADDR/$(tenant "$t")" -t0 0)
        echo "$TENANTS,first_touch,$ms" | tee -a "$out"
    done
    echo "$TENANTS,pss_mib,$(mib "$(egres_pss_kb)")" | tee -a "$out"
    t0=$(date +%s%N)
    egres_stop
    echo "$TENANTS,stop,$((($(date +%s%N) - t0) / 1000000))" | tee -a "$out"
done
