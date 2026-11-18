#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Control: every tenant a database in one stock Postgres 18, no VM,
# the shape Egres has (one server, a database per tenant) with none of
# its metering. The container gets MEM_GIB, page cache included, as
# the other sides do; stock settings but for the connection limit,
# since a Postgres connection belongs to one database and tenants
# cannot share a pool. Needs docker (image PG_IMAGE) and sudo, to read
# the memory of the container's processes.
#   PG_PORT=5440 PG_MAX_CONN=5000 PG_CONF="-c shared_buffers=16GB"
DIR=${DIR:-$WORK/pg-shared}
PG_PORT=${PG_PORT:-5440}
PG_MAX_CONN=${PG_MAX_CONN:-5000}
PG_CONF=${PG_CONF:-}
NAME=tb-pg-shared
ADDR=127.0.0.1:$PG_PORT

platform_init() { :; }
tenant_addr() { echo "$ADDR/tenant_$1"; }

platform_down() {
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    sudo rm -rf "$DIR"
}
platform_up() {
    platform_down
    mkdir -p "$DIR"
    # Postgres puts parallel workers' and CREATE DATABASE's dynamic
    # shared memory on /dev/shm, which docker caps at 64 MB unless
    # told otherwise; with 16 creators that cap stopped the 1,000 to
    # 10,000 step ("could not resize shared memory segment").
    # shellcheck disable=SC2086  # PG_CONF is a list of -c settings
    docker run -d --name "$NAME" --network host --memory="${MEM_GIB}g" \
        --shm-size=8g -e POSTGRES_HOST_AUTH_METHOD=trust \
        -v "$DIR:/var/lib/postgresql" "$PG_IMAGE" \
        -c port="$PG_PORT" -c listen_addresses=127.0.0.1 \
        -c max_connections="$PG_MAX_CONN" $PG_CONF >/dev/null
    "$ROOT/probe/probe" first -addr "$ADDR/postgres" -q "SELECT 1" \
        -t0 "$(date +%s%N)" -timeout 120s >/dev/null
}
# The postmaster, on the host: what the agents probe reads PSS from.
platform_pid() { docker inspect -f '{{.State.Pid}}' "$NAME"; }

tenant_create() {
    sql "$ADDR/postgres" "CREATE DATABASE tenant_$1"
    seed "$(tenant_addr "$1")" "$2"
}

# Every process in the container: the postmaster, its workers and
# one backend per open connection.
tenants_pss_kb() {
    local pid sum=0 kb
    for pid in $(docker top "$NAME" -eo pid | tail -n +2); do
        kb=$(sudo awk '/^Pss:/ {print $2}' "/proc/$pid/smaps_rollup" \
            2>/dev/null || echo 0)
        sum=$((sum + ${kb:-0}))
    done
    echo "$sum"
}
tenants_disk_kb() { sudo du -sk "$DIR" 2>/dev/null | cut -f1; }
