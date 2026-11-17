#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# What every test shares: the settings, the seed data, psql against a
# probe-style target, and the platform behind BACKEND. A test sources
# this and then speaks only the backend interface (backends/README):
#   platform_init "$@"   before anything else (the VM side may re-exec)
#   platform_up          the platform, empty
#   platform_down        everything stopped and removed
#   tenant_create i rows the tenant, its table and its rows, through SQL
#   tenant_addr i        ip:port[/database], the probe's target form
#   tenants_pss_kb       host memory the tenants hold (PSS)
#   tenants_disk_kb      disk the tenants' data holds
# and, where a backend has them: platform_restart, platform_pid,
# tenants_fds, platform_dump name.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
RUN=$WORK/run
# Memory each side gets, whatever the host has: the Egres server's
# cap, the micro-VMs' budget, and what the footprint's fit is per. The
# same figure on every machine keeps the results comparable.
MEM_GIB=${MEM_GIB:-64}
# What the host must keep free; a test stops adding tenants under it.
FLOOR_GIB=${FLOOR_GIB:-16}
# Tenants made at once by create_range. Past 16 the server sets the
# pace (about 75 tenants/s on Egres); 1 reproduces the serial runs.
CREATE_PAR=${CREATE_PAR:-16}
mkdir -p "$RUN" "$RES"

avail_gib() { awk '/MemAvailable/ {print int($2/1048576)}' /proc/meminfo; }
avail_kb() { awk '/MemAvailable/ {print $2}' /proc/meminfo; }
total_kb() { awk '/MemTotal/ {print $2}' /proc/meminfo; }
# Room for another tenant: the host above its floor, and the tenants
# under their memory. A backend with another limit (the VMs' disk)
# adds it.
have_room() {
    [ "$(avail_gib)" -ge "$FLOOR_GIB" ] &&
        [ "$(tenants_pss_kb)" -lt $((MEM_GIB << 20)) ]
}
mib() { awk -v kb="$1" 'BEGIN {printf "%.1f", kb / 1024}'; }

# "ip:port" or "ip:port/database" to a psql connection string. With no
# database, "postgres", as the probe does.
conn_of() {
    local addr=${1%%/*} db=postgres
    [[ $1 == */* ]] && db=${1#*/}
    echo "host=${addr%:*} port=${addr##*:} dbname=$db user=postgres" \
        "sslmode=disable"
}
sql() {
    psql "$(conn_of "$1")" -X -q -v ON_ERROR_STOP=1 -c "$2" >/dev/null
}
sql1() { psql "$(conn_of "$1")" -X -qtAc "$2"; }

# A tenant's data: table t, rows (g, md5(g)) for g in 1..rows, as
# INSERT ... VALUES of 1,000 rows a statement. The same file seeds a
# micro-VM's Postgres and an Egres database, which has no INSERT ...
# SELECT yet.
seed_file() {
    local rows=$1 f
    f=$RUN/seed_$rows.sql
    if [ ! -f "$f" ]; then
        python3 - "$rows" >"$f.$$" <<'PY' && mv "$f.$$" "$f"
import hashlib, sys
rows = int(sys.argv[1])
print("CREATE TABLE t (id bigint PRIMARY KEY, v text);")
for lo in range(1, rows + 1, 1000):
    vals = ",".join(
        "(%d,'%s')" % (g, hashlib.md5(str(g).encode()).hexdigest())
        for g in range(lo, min(lo + 1000, rows + 1)))
    print("INSERT INTO t VALUES %s;" % vals)
PY
    fi
    echo "$f"
}
seed() {
    psql "$(conn_of "$1")" -X -q -v ON_ERROR_STOP=1 -f "$(seed_file "$2")" \
        >/dev/null
}

# Creates tenants [from, to) with `rows` rows each, CREATE_PAR at a
# time. Returns 1 at the host's memory floor or when one failed.
create_range() {
    local from=$1 to=$2 rows=$3 i j p pids failed=0
    seed_file "$rows" >/dev/null
    for ((i = from; i < to; i += CREATE_PAR)); do
        have_room || { echo "host is out of room" >&2; return 1; }
        pids=()
        for ((j = i; j < i + CREATE_PAR && j < to; j++)); do
            tenant_create "$j" "$rows" &
            pids+=($!)
        done
        for p in "${pids[@]}"; do wait "$p" || failed=$((failed + 1)); done
        [ "$failed" = 0 ] ||
            { echo "$failed tenants failed to create" >&2; return 1; }
    done
}

# The probe's target list for tenants [0, n).
targets_file() {
    local n=$1 f=$RUN/targets_$2.txt i
    for ((i = 0; i < n; i++)); do tenant_addr "$i"; done >"$f"
    echo "$f"
}

# Open-loop point reads at random tenants: the density traffic.
# Prints p50_ms,p90_ms,p99_ms,ok,errors.
point_reads() { "$ROOT/probe/probe" load -addrs "$1" -rate "$2" -dur "$3"; }

BACKEND=${BACKEND:?vm, egres or pgsolo}
[ -f "$ROOT/backends/$BACKEND.sh" ] ||
    { echo "no backend $BACKEND" >&2; exit 2; }
source "$ROOT/backends/$BACKEND.sh"
