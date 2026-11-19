#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Noisy neighbours: QUIET tenants send an occasional point read; next
# to them a growing number of heavy tenants each run one statement flat
# out on CONNS connections. What is reported is the quiet tenants'
# latency at each heavy count; the row with 0 heavy tenants is what the
# others are compared with (probe/noisy.go). Every tenant holds ROWS
# rows. Two kinds of heavy statement: cpu, a count over the table;
# write, an UPDATE of a random 1,000-row range.
#
# A micro-VM tenant can use no more than its vCPU, but nothing reserves
# host CPU for the quiet ones: past one heavy tenant per host thread
# they compete. On Egres nothing bounds a heavy tenant but the server's
# meter, which gives each active tenant an equal share; its per-tenant
# lines are saved after each step. On pgshared, one stock Postgres
# with a database per tenant, nothing bounds a heavy tenant at all:
# Egres's shape without its meter.
#   BACKEND=vm ./noisy.sh
#   KINDS=cpu,write HEAVY=0,1,2,4,8,16,32,64 QUIET=50 CONNS=4
#   RATE=50 DUR=60s HOLD=1 QHOLD=2 ROWS=100000
source "$(dirname "$0")/common.sh"
in_cgroup "$@"
platform_init "$@"
# Lists take commas or spaces: suite.txt has no quoting.
KINDS=${KINDS:-cpu write}
HEAVY=${HEAVY:-0 1 2 4 8 16 32 64}
KINDS=${KINDS//,/ }
HEAVY=${HEAVY//,/ }
QUIET=${QUIET:-50}
CONNS=${CONNS:-4}
RATE=${RATE:-50}
DUR=${DUR:-60s}
ROWS=${ROWS:-100000}
# Connections each quiet tenant keeps open, as an application's pool
# does; 0 opens one per query, which measures the connection more
# than the neighbours.
QHOLD=${QHOLD:-2}
# A heavy tenant keeps its connections, as a busy application's pool
# would. HOLD=0 opens a new one for every statement, which adds a
# connection storm to the load.
hold=-hold
[ "${HOLD:-1}" = 1 ] || hold=
out=$RES/noisy.csv

heavy_sql() {
    case $1 in
    cpu) echo "SELECT count(*) FROM t" ;;
    write) echo "UPDATE t SET v = '{s}' WHERE id BETWEEN {r} AND {e}" ;;
    *) echo "unknown kind: $1" >&2; exit 1 ;;
    esac
}

max=0
for h in $HEAVY; do ((h > max)) && max=$h; done
total=$((QUIET + max))

platform_down
trap platform_down EXIT
platform_up
create_range 0 "$total" "$ROWS"
sleep 30
for ((i = 0; i < QUIET; i++)); do tenant_addr "$i"; done >"$RUN/quiet.txt"
for ((i = QUIET; i < total; i++)); do tenant_addr "$i"; done >"$RUN/heavy.txt"

# read_mib and write_mib: the drive's traffic over the step, the
# heavy tenants' warm-up included. pss_mib and held_mib: the memory
# the side has at the end of the step, as footprint measures it (the
# tenants' processes; all the cgroup holds but clean unmapped cache).
echo "kind,heavy,conns,p50_ms,p95_ms,p99_ms,ok,errors," \
    "heavy_done,heavy_failed,heavy_mean_ms,read_mib,write_mib," \
    "pss_mib,held_mib" | tr -d ' ' | tee "$out"
for kind in $KINDS; do
    sql=$(heavy_sql "$kind")
    for h in $HEAVY; do
        read -r r0 w0 <<<"$(disk_io_kb)"
        # TRACE=1: one line per quiet query (due_ms, ms, late, connect,
        # startup, query), for finding where a slow one spent its time.
        trace=()
        [ "${TRACE:-0}" = 1 ] && trace=(-trace "$RES/noisy_${kind}_$h.trace")
        lat=$("$ROOT/probe/probe" noisy -quiet "$RUN/quiet.txt" \
            -heavy "$RUN/heavy.txt" -heavy-n "$h" -conns "$CONNS" $hold \
            -sql "$sql" -quiet-hold "$QHOLD" -rate "$RATE" -dur "$DUR" \
            -rows "$ROWS" "${trace[@]}")
        read -r r1 w1 <<<"$(disk_io_kb)"
        rd=$(((r1 - r0) / 1024)) wr=$(((w1 - w0) / 1024))
        echo "$kind,$h,$CONNS,$lat,$rd,$wr,$(mib "$(tenants_pss_kb)")," \
            "$(mib "$(mem_held_kb)")" | tr -d ' ' | tee -a "$out"
        [ "$(type -t platform_dump)" = function ] &&
            platform_dump "noisy_${kind}_$h"
        sleep 5
    done
done
