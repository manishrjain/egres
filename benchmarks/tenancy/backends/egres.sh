#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Egres: one server, every tenant a database on it. Sourced by
# common.sh; see the interface there.
#   EGRES_BIN  the server (default: the one setup.sh built)
#   EDATA      its data directory, thrown away by platform_up
#   OFFSET     ports sit this far above Egres's own (50: SQL 3260,
#              admin 5260), clear of a server under development
#   KEEP=1     platform_up keeps the data directory it finds
EGRES_BIN=${EGRES_BIN:-$WORK/egres-bin/bin/egres}
# On a real disk: Egres reads SSTs with O_DIRECT.
EDATA=${EDATA:-$WORK/egres-data}
OFFSET=${OFFSET:-50}
SQL_PORT=$((3210 + OFFSET))
ADDR=127.0.0.1:$SQL_PORT
ADMIN=127.0.0.1:$((5210 + OFFSET))
# Every start's log, numbered in start order: egres-1.log, egres-2.log,
# ... run.sh points this at the run's own directory.
LOG_DIR=${LOG_DIR:-$RUN/logs}
ELOG=

# Egres keeps a file open per table and does not raise its own limit;
# about 4 per tenant.
ulimit -n "$(ulimit -Hn)"

platform_init() { :; }
tenant_addr() { echo "$ADDR/tenant_$1"; }

# Launches the server and returns at once; a caller timing the start
# takes its clock before calling. Arguments are extra server flags.
# The server is given MEM_GIB, the memory the micro-VM side gets too;
# left alone it would size itself to the host.
egres_launch() {
    mkdir -p "$LOG_DIR"
    local n=1
    while [ -e "$LOG_DIR/egres-$n.log" ]; do n=$((n + 1)); done
    ELOG=$LOG_DIR/egres-$n.log
    # Run under another name: monitoring that reads the smaps of every
    # process called "egres" stalled a server holding tens of GiB.
    # Copied whenever the bytes differ.
    local bin=$RUN/tenantdb
    cmp -s "$EGRES_BIN" "$bin" || cp "$EGRES_BIN" "$bin"
    # Egres probes the data directory's bandwidth at every start, 2.5 s
    # and a different answer each time. Once setup.sh calibrate has
    # measured the drive, every start is given that profile. PROBE=1
    # asks for the probe, to time a start as it comes out of the box.
    local io=()
    if [ "${PROBE:-0}" != 1 ] && [ -f "$RUN/egres_io.flags" ]; then
        io=(--io="$(cat "$RUN/egres_io.flags")")
    fi
    # EGRES_MALLOC_CONF is handed to the server's jemalloc as
    # MALLOC_CONF, Egres's documented override, to measure an
    # allocator setting. Pinned to PLATFORM_CPUS the server needs one:
    # its jemalloc sees fewer CPUs in its affinity mask than the
    # machine has, turns its per-CPU arenas off and, as Egres builds
    # it (abort_conf), aborts; the arenas are off either way, so say
    # so and start. A pinned Egres is therefore measured without
    # per-CPU arenas, an unpinned one with them unless told otherwise.
    # EGRES_RAFT_WORKERS reaches the server through the environment (a
    # benchmark build reads it); named here so command.txt records it.
    : "${EGRES_RAFT_WORKERS:-}"
    local conf=${EGRES_MALLOC_CONF:-} env=()
    [ -n "$conf" ] || [ -z "$PLATFORM_CPUS" ] || conf=percpu_arena:disabled
    [ -z "$conf" ] || env=(env "MALLOC_CONF=$conf")
    # shellcheck disable=SC2046  # on_platform is a command prefix
    "${env[@]}" $(on_platform) "$bin" --listen="offset=$OFFSET" --raft="id=1" \
        --store="dir-path=$EDATA/store" --wal="dir=$EDATA/wal" \
        --memory="total-bytes=$((MEM_GIB << 30))" \
        "${io[@]}" "$@" >"$ELOG" 2>&1 &
    echo $! >"$RUN/egres.pid"
}

# Blocks until the server takes writes, or fails after 60 s. A read is
# answered before the election is won, so the log is what tells.
egres_wait() {
    # EGRES_WAIT_S: a server opening a store of hundreds of thousands
    # of tenants (a resumed run) takes longer than a fresh one.
    local i
    for ((i = 0; i < ${EGRES_WAIT_S:-60} * 10; i++)); do
        grep -q "become-leader" "$ELOG" 2>/dev/null &&
            sql1 "$ADDR/egres" "SELECT 1" >/dev/null 2>&1 && return 0
        sleep 0.1
    done
    echo "egres did not come up; see $ELOG" >&2
    return 1
}

# Warms a server that has just started, on its own database `egres`,
# never a tenant's: the first connections and statements after a start
# pay for the process (code faulted in, pools filled), and that is no
# tenant's cost.
egres_warm() {
    sql1 "$ADDR/egres" "SELECT count(*) FROM t" >/dev/null 2>&1 ||
        seed "$ADDR/egres" 1000 || return 1
    echo "$ADDR/egres" >"$RUN/warm_targets.txt"
    point_reads "$RUN/warm_targets.txt" 200 "${WARM_S:-5}s" >/dev/null
}

egres_stop() {
    [ -f "$RUN/egres.pid" ] || return 0
    local pid
    pid=$(cat "$RUN/egres.pid")
    kill "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null; do sleep 0.05; done
    rm -f "$RUN/egres.pid"
}

platform_up() {
    egres_stop
    [ "${KEEP:-0}" = 1 ] || rm -rf "$EDATA"
    egres_launch "$@" && egres_wait && egres_warm
}
platform_down() { egres_stop; }
platform_pid() { cat "$RUN/egres.pid"; }

tenant_create() {
    sql "$ADDR/egres" "CREATE DATABASE tenant_$1"
    seed "$(tenant_addr "$1")" "$2"
}

tenants_pss_kb() {
    awk '/^Pss:/ {print $2}' "/proc/$(platform_pid)/smaps_rollup"
}
# The store alone: the WAL's files are made at a fixed size up front
# and would hide what a tenant adds.
tenants_disk_kb() { du -sk "$EDATA/store" | cut -f1; }
# The block cache, KiB, from the server's memory report: the server's
# own choice of what to keep, not the tenants' cost, so the per-tenant
# figure is what is held less this.
tenants_cache_kb() {
    egres_admin mem 5 2>/dev/null | awk '$1 == "block_cache:" {
        v = $2; u = v; gsub(/[0-9.]/, "", u); sub(/[A-Za-z]+$/, "", v)
        k = v
        if (u == "GiB") k = v * 1048576
        else if (u == "MiB") k = v * 1024
        else if (u == "B") k = v / 1024
        printf "%d\n", k
    }'
}
# Files the server holds open; the host's hard limit caps it.
tenants_fds() {
    { find "/proc/$(platform_pid)/fd" -mindepth 1 -maxdepth 1 2>/dev/null ||
        true; } | wc -l
}

# One command on the admin port; prints the reply.
egres_admin() {
    python3 - "$((5210 + OFFSET))" "$1" "${2:-5}" <<'PY'
import socket, sys
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=5)
s.sendall(sys.argv[2].encode() + b"\n")
s.settimeout(float(sys.argv[3]))
out = b""
try:
    while True:
        b = s.recv(65536)
        if not b:
            break
        out += b
        s.settimeout(0.5)
except socket.timeout:
    pass
print(out.decode(errors="replace"))
PY
}
# The meter's per-tenant lines and the memory report, beside the
# results, so a tenant the meter held back shows as parked time.
# The meter, the memory, and the workers (per-worker CPU, lifetime,
# so a step's share is the difference from the dump before): how busy
# the raft lane is beside the SQL workers at each heavy count.
platform_dump() {
    egres_admin tenants >"$RES/$1.tenants" 2>&1 || true
    egres_admin mem >"$RES/$1.mem" 2>&1 || true
    egres_admin workers >"$RES/$1.workers" 2>&1 || true
}
