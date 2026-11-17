#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Control: one stock Postgres 18 per tenant, as in the micro-VM test,
# but with no VM around it: the same data directory (assets/data.ext4:
# shared_buffers 16MB, 20 connections, table t with 1,000 rows), each
# server in its own container on the host's kernel. What an idle
# tenant holds here is Postgres's share of the micro-VM's memory; the
# rest of that figure is the guest kernel and the VM.
# Counts only the Postgres processes, not docker's own per container.
# Needs docker (image PG_IMAGE, env.sh) and sudo.
#   ./pg_solo.sh [counts...]      default: 1 10 100
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
DIR=${DIR:-$WORK/pg-solo}
SETTLE=${SETTLE:-30}
counts=("$@")
[ ${#counts[@]} -gt 0 ] || counts=(1 10 100)
out=$RES/pg_solo.csv
mkdir -p "$RES"

name() { echo "tb-pg-solo-$1"; }
port() { echo $((7000 + $1)); }

pss_kb() {
    local n=$1 i pid sum=0 kb
    for ((i = 0; i < n; i++)); do
        for pid in $(docker top "$(name "$i")" -eo pid | tail -n +2); do
            kb=$(sudo awk '/^Pss:/ {print $2}' "/proc/$pid/smaps_rollup" \
                2>/dev/null || echo 0)
            sum=$((sum + ${kb:-0}))
        done
    done
    echo "$sum"
}

stop() {
    docker ps -aq --filter "name=tb-pg-solo-" | xargs -r docker rm -f \
        >/dev/null 2>&1 || true
    sudo umount "$DIR/mnt" 2>/dev/null || true
    sudo rm -rf "$DIR"
}
stop
trap stop EXIT
mkdir -p "$DIR/mnt"
sudo mount -o loop,ro "$A/data.ext4" "$DIR/mnt"

start() {
    local i=$1
    sudo mkdir -p "$DIR/t$i"
    sudo cp -a "$DIR/mnt/pg" "$DIR/t$i/pg"
    # PGDATA names the copied directory, so the image starts the
    # server on it and initialises nothing.
    docker run -d --name "$(name "$i")" --network host \
        -e PGDATA=/data/pg -v "$DIR/t$i:/data" "$PG_IMAGE" \
        postgres -p "$(port "$i")" -c listen_addresses=127.0.0.1 >/dev/null
}
wait_up() {
    "$ROOT/probe/probe" first -addr "127.0.0.1:$(port "$1")" \
        -t0 "$(date +%s%N)" -timeout 60s >/dev/null
}

echo "tenants,pss_mib,pss_per_tenant_mib" | tee "$out"
have=0
for n in "${counts[@]}"; do
    for ((i = have; i < n; i += 16)); do
        for ((j = i; j < i + 16 && j < n; j++)); do start "$j"; done
        for ((j = i; j < i + 16 && j < n; j++)); do wait_up "$j"; done
    done
    have=$n
    sleep "$SETTLE"
    pss=$(pss_kb "$n")
    echo "$n,$((pss / 1024)),$(awk -v kb="$pss" -v n="$n" \
        'BEGIN {printf "%.1f", kb / 1024 / n}')" | tee -a "$out"
done
