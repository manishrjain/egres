#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Control: one stock Postgres 18 per tenant with no VM around it, each
# in its own container on the host's kernel, from the micro-VM's data
# image (shared_buffers 16MB, 20 connections). What an idle tenant
# holds here is Postgres's share of the micro-VM figure; the rest is
# the guest kernel and the VM. Sourced by common.sh; see the interface
# there. Needs docker (image PG_IMAGE) and sudo, to copy the image's
# data directory and read the containers' memory.
DIR=${DIR:-$WORK/pg-solo}

platform_init() { :; }
name() { echo "tb-pg-solo-$1"; }
tenant_addr() { echo "127.0.0.1:$((7000 + $1))"; }

platform_down() {
    docker ps -aq --filter "name=tb-pg-solo-" | xargs -r docker rm -f \
        >/dev/null 2>&1 || true
    sudo umount "$DIR/mnt" 2>/dev/null || true
    sudo rm -rf "$DIR"
}
platform_up() {
    platform_down
    mkdir -p "$DIR/mnt"
    sudo mount -o loop,ro "$A/data.ext4" "$DIR/mnt"
}

tenant_create() {
    local i=$1
    sudo mkdir -p "$DIR/t$i"
    sudo cp -a "$DIR/mnt/pg" "$DIR/t$i/pg"
    docker run -d --name "$(name "$i")" --network host \
        -e PGDATA=/data/pg -v "$DIR/t$i:/data" "$PG_IMAGE" \
        postgres -p $((7000 + i)) -c listen_addresses=127.0.0.1 >/dev/null
    "$ROOT/probe/probe" first -addr "$(tenant_addr "$i")" -q "SELECT 1" \
        -t0 "$(date +%s%N)" -timeout 60s >/dev/null
    seed "$(tenant_addr "$i")" "$2"
}

# Only the Postgres processes, not docker's own per container.
tenants_pss_kb() {
    local c pid sum=0 kb
    for c in $(docker ps -q --filter "name=tb-pg-solo-"); do
        for pid in $(docker top "$c" -eo pid | tail -n +2); do
            kb=$(sudo awk '/^Pss:/ {print $2}' "/proc/$pid/smaps_rollup" \
                2>/dev/null || echo 0)
            sum=$((sum + ${kb:-0}))
        done
    done
    echo "$sum"
}
tenants_disk_kb() {
    sudo du -sk --total "$DIR"/t*/pg 2>/dev/null | awk 'END {print $1 + 0}'
}
