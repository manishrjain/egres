#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Agents: the workload the platforms are for. Tenants arrive over time,
# each creating its database and loading ROWS rows through SQL; at any
# moment a fraction ACTIVE of them are in a session, sending a mix of
# reads and writes at RATE statements a second for SESSION; the rest
# are idle. At each count in STEPS the arrivals pause and a WINDOW is
# measured: the latency of every statement kind, the time an arrival
# took to its first row, a session's first statement on its own (the
# time to a result from a tenant that was idle; on the micro-VM side
# that includes waking it), and the memory and disk held
# (probe/agents.go has the model and the columns).
#
# On the micro-VM side the probe is also the platform's control plane:
# when the test's cgroup holds more than BUDGET_MIB (all but the clean,
# unmapped file cache), idle VMs are put to sleep (paused, snapshotted
# to disk, killed), least recently used first, and arrivals wait for
# room; a session for a sleeping tenant first wakes it, by restoring
# the snapshot (WAKE=restore) or booting afresh (WAKE=boot); the wait
# is in that statement's latency. An arrival the platform does not
# take in 60 s ends the growth, as a stop, not a fault.
# The budget is BUDGET_PCT of MEM_GIB, the memory both sides run in
# and the Egres server is capped at. On Egres nothing is put to sleep
# or woken: what stays in memory is the server's own choice.
#
# The run stops growing at the memory or disk floor, or when a count
# no longer fits: the reads' p99 over STOP_P99_MS, arrivals' p99 over
# STOP_ARRIVE_MS, or more than STOP_ERRORS_PCT of statements failing.
# A breach is measured once more before it counts, since a drive's slow
# phase can lift one window's p99 on its own. The last count that
# passed is how many tenants fit at this activity.
#   BACKEND=egres ./agents.sh
#   STEPS=1000,2000,5000,10000,20000,50000,100000 ARRIVE=30 WINDOW=2m
#   ACTIVE=0.10 SESSION=2m RATE=1 ROWS=1000
#   MIX=point=60,range=15,update=15,insert=8,count=2
#   MEM_GIB=64 BUDGET_PCT=90 WAKE=restore
#   STOP_P99_MS=50 STOP_ARRIVE_MS=5000 STOP_ERRORS_PCT=1   (0 turns one off)
source "$(dirname "$0")/common.sh"
in_cgroup "$@"
platform_init "$@"
STEPS=${STEPS:-1000,2000,5000,10000,20000,50000,100000}
ARRIVE=${ARRIVE:-30}
WINDOW=${WINDOW:-2m}
ACTIVE=${ACTIVE:-0.10}
SESSION=${SESSION:-2m}
RATE=${RATE:-1}
ROWS=${ROWS:-1000}
MIX=${MIX:-point=60,range=15,update=15,insert=8,count=2}
WAKE=${WAKE:-restore}
STOP_P99_MS=${STOP_P99_MS:-50}
STOP_ARRIVE_MS=${STOP_ARRIVE_MS:-5000}
STOP_ERRORS_PCT=${STOP_ERRORS_PCT:-1}
BUDGET_MIB=$(($(budget_kb) / 1024))
out=$RES/agents.csv

platform_down
trap platform_down EXIT
platform_up
side=()
case $BACKEND in
vm)
    # The steps stop at the taps: an arrival past them has no network
    # and would end the run as a failure rather than a limit.
    cap=$(tenant_cap)
    steps=
    for n in ${STEPS//,/ }; do
        if ((n < cap)); then steps+=${steps:+,}$n; else capped=1; fi
    done
    [ -z "${capped:-}" ] || STEPS=${steps:+$steps,}$cap
    side=(-backend vm -run "$RUN" -fc "$A/firecracker" -kernel "$A/vmlinux"
        -rootfs "$A/rootfs.ext4" -blank "$A/data.ext4" -mem-mib "$MEM_MIB"
        -wake "$WAKE" -budget-mib "$BUDGET_MIB"
        -disk-floor-gib "$DISK_FLOOR_GIB")
    ;;
egres)
    side=(-backend shared -addr "$ADDR" -pid "$(platform_pid)")
    ;;
*) echo "agents runs on vm or egres, not $BACKEND" >&2; exit 2 ;;
esac
"$ROOT/probe/probe" agents "${side[@]}" -floor-gib "$FLOOR_GIB" \
    -steps "$STEPS" -arrive "$ARRIVE" -window "$WINDOW" -active "$ACTIVE" \
    -session "$SESSION" -rate "$RATE" -rows "$ROWS" -mix "$MIX" \
    -stop-p99-ms "$STOP_P99_MS" -stop-arrive-ms "$STOP_ARRIVE_MS" \
    -stop-errors-pct "$STOP_ERRORS_PCT" \
    -trace "$RES/agents.trace" | tee "$out"
[ "$(type -t platform_dump)" = function ] && platform_dump agents_end
echo "disk at the end: $(mib "$(tenants_disk_kb)") MiB" >&2
