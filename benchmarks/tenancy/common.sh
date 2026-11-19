#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# What every test shares: the settings, the seed data, psql against a
# probe-style target, and the platform behind BACKEND. A test sources
# this and then speaks only the backend interface (backends/README):
#   in_cgroup "$@"       first: the test under its memory limit (re-exec)
#   platform_init "$@"   before anything else (the VM side may re-exec)
#   platform_up          the platform, empty
#   platform_down        everything stopped and removed
#   tenant_create i rows the tenant, its table and its rows, through SQL
#   tenant_addr i        ip:port[/database], the probe's target form
#   tenants_pss_kb       host memory the tenants' processes hold (PSS)
#   tenants_disk_kb      disk the tenants' data holds
# and, where a backend has them: platform_pid, tenants_fds,
# platform_dump name.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
RUN=$WORK/run
# Memory each side gets, whatever the host has: the Egres server's
# cap, the micro-VMs' budget, and what the footprint's fit is per. The
# same figure on every machine keeps the results comparable. It is
# enforced: in_cgroup puts the test in a cgroup allowed this much, so
# the page cache the tenants' files take on the host (a micro-VM's disk
# image and snapshot, a server's WAL) comes out of the same memory as
# their processes. PSS alone would miss it.
MEM_GIB=${MEM_GIB:-64}
# The share of MEM_GIB the tenants' processes may hold; the rest is for
# the kernel's own and the file cache. A test stops adding tenants
# there, and the micro-VM control plane puts VMs to sleep there.
BUDGET_PCT=${BUDGET_PCT:-90}
# What the host must keep free; a test stops adding tenants under it.
FLOOR_GIB=${FLOOR_GIB:-16}
# Tenants made at once by create_range. Past 16 the server sets the
# pace (about 75 tenants/s on Egres); 1 reproduces the serial runs.
CREATE_PAR=${CREATE_PAR:-16}
# The platform's CPUs and the load generator's, when the two are kept
# apart (on a 16-core box, PLATFORM_CPUS=0-13,16-29 PROBE_CPUS=
# 14,15,30,31: siblings N and N+16 share a core): the probe, psql and
# the seeders must neither steal from what they measure nor be starved
# by it. Empty, nothing is pinned. The test puts itself on PROBE_CPUS,
# so everything it starts inherits them; a backend starts its platform
# under on_platform. A server reading its CPU count from its affinity
# mask (Egres does) then sees the platform's share.
PLATFORM_CPUS=${PLATFORM_CPUS:-}
PROBE_CPUS=${PROBE_CPUS:-}
[ -z "$PROBE_CPUS" ] || taskset -pc "$PROBE_CPUS" $$ >/dev/null
on_platform() { [ -z "$PLATFORM_CPUS" ] || echo taskset -c "$PLATFORM_CPUS"; }

# The drive WORK is on (a partition's parent), and the KiB read and
# written on it since boot, from /proc/diskstats. Every side's files,
# images, snapshots, store, WAL, the container's volume, are under
# WORK, so what a step cost in disk traffic is the difference.
work_disk() {
    local mnt=$WORK src d
    [ -e "$mnt" ] || mnt=$(dirname "$WORK")
    src=$(findmnt -no SOURCE --target "$mnt" 2>/dev/null)
    d=$(lsblk -no PKNAME "$src" 2>/dev/null | head -1)
    echo "${d:-$(basename "$src")}"
}
disk_io_kb() {
    awk -v d="$(work_disk)" '$3 == d {printf "%d %d\n", $6 / 2, $10 / 2}' \
        /proc/diskstats
}
mkdir -p "$RUN" "$RES"

avail_gib() { awk '/MemAvailable/ {print int($2/1048576)}' /proc/meminfo; }
avail_kb() { awk '/MemAvailable/ {print $2}' /proc/meminfo; }
total_kb() { awk '/MemTotal/ {print $2}' /proc/meminfo; }

# Re-runs the test in a transient cgroup allowed MEM_GIB and no swap
# (systemd-run; the user's slice has the memory controller on both
# machines so far). Everything the test starts, the server, the VMs,
# the probe, is charged to it. When it is full the kernel kills one
# process in it, one tenant's VM or the server; OOMPolicy=continue
# keeps systemd from then stopping the whole test, whose rows should
# show the loss. CGROUP=0 runs without one. Docker's containers
# (pgshared) is the daemon's, outside it.
in_cgroup() {
    [ "${CGROUP:-1}" = 1 ] || return 0
    if [ -n "${IN_CGROUP:-}" ]; then
        echo "cgroup $(cgroup_dir):" \
            "memory.max $(cat "$(cgroup_dir)/memory.max")" >&2
        return 0
    fi
    export IN_CGROUP=1
    exec systemd-run --user --scope --quiet -p MemoryMax=$((MEM_GIB << 30)) \
        -p MemorySwapMax=0 -p OOMPolicy=continue "$0" "$@"
}
cgroup_dir() { echo "/sys/fs/cgroup$(sed -n 's#^0::##p' /proc/self/cgroup)"; }
cgroup_limited() { [ "$(cat "$(cgroup_dir)/memory.max" 2>/dev/null)" != max ]; }
# All the cgroup is charged, file cache included; "" without a limit.
cgroup_kb() {
    cgroup_limited || return 0
    echo $(($(cat "$(cgroup_dir)/memory.current") >> 10))
}
# What the cgroup holds that the kernel cannot simply drop: all but
# the clean, unmapped file cache. The tenants' processes with the
# kernel's own, the snapshot pages a restored VM has mapped, and what
# is dirty or being written. PSS where there is no cgroup limit.
mem_held_kb() {
    if cgroup_limited; then
        awk -v cur="$(cat "$(cgroup_dir)/memory.current")" '
            $1 == "file" {file = $2}
            $1 ~ /^file_(mapped|dirty|writeback)$/ {pinned += $2}
            END {
                clean = file - pinned
                if (clean < 0) clean = 0
                print int((cur - clean) / 1024)
            }' "$(cgroup_dir)/memory.stat"
    else
        tenants_pss_kb
    fi
}
budget_kb() { echo $(((MEM_GIB << 20) * BUDGET_PCT / 100)); }
# Room for another tenant: the host above its floor, and the tenants
# under their budget. A backend with another limit (the VMs' disk)
# adds it.
have_room() {
    [ "$(avail_gib)" -ge "$FLOOR_GIB" ] &&
        [ "$(mem_held_kb)" -lt "$(budget_kb)" ]
}
mib() { [ -z "$1" ] || awk -v kb="$1" 'BEGIN {printf "%.1f", kb / 1024}'; }

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
# time. Returns 1 at the host's memory floor or when one failed; MADE
# is then the count that exists, since the tenants made before the
# stop stay and a row must not be credited to fewer.
create_range() {
    local from=$1 to=$2 rows=$3 i j p pids failed=0
    MADE=$to
    seed_file "$rows" >/dev/null
    for ((i = from; i < to; i += CREATE_PAR)); do
        # Creation writes in bursts, and the dirty pages count as held
        # until the drive has them: one wait for writeback before the
        # room is called gone, and the figures when it is.
        if ! have_room; then
            sync
            sleep 10
            if ! have_room; then
                echo "out of room at $i: available $(avail_gib) GiB," \
                    "held $(mib "$(mem_held_kb)") MiB of a" \
                    "$(($(budget_kb) / 1024)) MiB budget" >&2
                MADE=$i
                return 1
            fi
        fi
        pids=()
        for ((j = i; j < i + CREATE_PAR && j < to; j++)); do
            tenant_create "$j" "$rows" &
            pids+=($!)
        done
        for p in "${pids[@]}"; do wait "$p" || failed=$((failed + 1)); done
        [ "$failed" = 0 ] ||
            { echo "$failed tenants failed to create" >&2; MADE=$i; return 1; }
    done
}

# The probe's target list for tenants [0, n).
targets_file() {
    local n=$1 f=$RUN/targets_$2.txt i
    for ((i = 0; i < n; i++)); do tenant_addr "$i"; done >"$f"
    echo "$f"
}

# Open-loop point reads at random tenants: the density traffic.
# Prints p50_ms,p95_ms,p99_ms,ok,errors.
point_reads() { "$ROOT/probe/probe" load -addrs "$1" -rate "$2" -dur "$3"; }

BACKEND=${BACKEND:?vm, egres or pgshared}
[ -f "$ROOT/backends/$BACKEND.sh" ] ||
    { echo "no backend $BACKEND" >&2; exit 2; }
source "$ROOT/backends/$BACKEND.sh"
