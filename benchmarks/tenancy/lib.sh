#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Shared pieces for the micro-VM side of the tenancy benchmark: one
# Firecracker VM per tenant, each running stock Postgres 18.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

# RUN_DIR moves the VMs' directories, for a test that must not disturb
# the sleeping tenants another test took a long time to prepare.
RUN=${RUN_DIR:-$WORK/run}
GW=10.200.0.1
MEM_MIB=${MEM_MIB:-128}
# Stop adding VMs once the host has less than this available. The box
# is a workstation with other work on it; an OOM here costs that work.
FLOOR_GIB=${FLOOR_GIB:-16}
# CONSOLE=1 keeps the guest's serial console in run/vmN/console.log.
# It slows boot, so cold-start runs leave it off.
CONSOLE=${CONSOLE:-0}
# How the data disk treats a guest flush. Unsafe, Firecracker's own
# choice, ignores it; Writeback passes it to the host, which a test
# that writes needs for the guest's fsync to mean anything.
DRIVE_CACHE=${DRIVE_CACHE:-Unsafe}

mkdir -p "$RUN" "$RES"

vm_ip() { local n=$(($1 + 256)); echo "10.200.$((n >> 8)).$((n & 255))"; }
vm_mac() {
    local n=$(($1 + 256))
    printf '06:00:0a:c8:%02x:%02x' $((n >> 8)) $((n & 255))
}

tap_mac() {
    local n=$(($1 + 256))
    printf '06:01:0a:c8:%02x:%02x' $((n >> 8)) $((n & 255))
}

avail_gib() { awk '/MemAvailable/ {print int($2/1048576)}' /proc/meminfo; }
avail_kb() { awk '/MemAvailable/ {print $2}' /proc/meminfo; }

disk_gib() { df -BG --output=avail "$RUN" | awk 'NR == 2 {print int($1)}'; }

# Each tenant's data image takes about 33 MiB, so disk can run out
# before memory does on a small volume.
DISK_FLOOR_GIB=${DISK_FLOOR_GIB:-5}
have_room() {
    [ "$(avail_gib)" -ge "$FLOOR_GIB" ] &&
        [ "$(disk_gib)" -ge "$DISK_FLOOR_GIB" ]
}

need_kvm() {
    [ -r /dev/kvm ] && [ -w /dev/kvm ] && return 0
    # In the kvm group, but this login is older than the membership:
    # run the script again under the group.
    if [ -z "${KVM_SG:-}" ] && id -nG "$USER" | grep -qw kvm; then
        export KVM_SG=1
        exec sg kvm -c "$(printf '%q ' "$0" "$@")"
    fi
    echo "no usable /dev/kvm: sudo ./setup.sh host" \
        "(virtualisation must be on in the BIOS)" >&2
    exit 1
}

# One tap per VM, no bridge (a Linux bridge stops at 1024 ports). The
# host answers as the gateway on every tap and routes the guest's /32
# down it. The permanent neighbour entry matters for cold-start timing:
# without it the first SYN waits on ARP, which retries once a second.
tap_up() {
    local i=$1 tap=fctap$1
    ip link show "$tap" >/dev/null 2>&1 && return 0
    sudo ip tuntap add dev "$tap" mode tap user "$USER"
    # A fixed MAC, for snapshots: see netns_setup.sh.
    sudo ip link set "$tap" address "$(tap_mac "$i")"
    sudo ip addr add "$GW/32" dev "$tap"
    sudo ip link set "$tap" up
    sudo ip route replace "$(vm_ip "$i")/32" dev "$tap"
    sudo ip neigh replace "$(vm_ip "$i")" lladdr "$(vm_mac "$i")" \
        dev "$tap" nud permanent
}

# Taps that netns_setup.sh made ahead of time stay: on a host without
# passwordless sudo the tests could not make them again.
tap_down() {
    [ -e "$WORK/.taps_precreated" ] && return 0
    sudo ip link del "fctap$1" 2>/dev/null || true
}

boot_args() {
    local i=$1 con="8250.nr_uarts=0 quiet loglevel=0"
    [ "$CONSOLE" = 1 ] && con="console=ttyS0"
    echo "$con reboot=k panic=1 pci=off init=/fc-init" \
        "ip=$(vm_ip "$i")::$GW:255.255.0.0::eth0:off"
}

vm_config() {
    local i=$1 d=$RUN/vm$1
    cat >"$d/config.json" <<EOF
{
  "boot-source": {
    "kernel_image_path": "$A/vmlinux",
    "boot_args": "$(boot_args "$i")"
  },
  "drives": [
    {"drive_id": "rootfs", "path_on_host": "$A/rootfs.ext4",
     "is_root_device": true, "is_read_only": true},
    {"drive_id": "data", "path_on_host": "$d/data.ext4",
     "is_root_device": false, "is_read_only": false,
     "cache_type": "$DRIVE_CACHE"}
  ],
  "machine-config": {"vcpu_count": 1, "mem_size_mib": $MEM_MIB},
  "network-interfaces": [
    {"iface_id": "eth0", "guest_mac": "$(vm_mac "$i")",
     "host_dev_name": "fctap$i"}
  ]
}
EOF
}

# Everything a VM needs before its process starts. Kept apart from
# vm_launch so cold-start timing covers the launch alone.
vm_prepare() {
    local i=$1 d=$RUN/vm$1
    mkdir -p "$d"
    [ -f "$d/data.ext4" ] || cp --sparse=always "$A/data.ext4" "$d/data.ext4"
    tap_up "$i"
    vm_config "$i"
}

vm_launch() {
    local i=$1 d=$RUN/vm$1
    rm -f "$d/api.sock"
    "$A/firecracker" --api-sock "$d/api.sock" \
        --config-file "$d/config.json" >"$d/console.log" 2>&1 &
    echo $! >"$d/pid"
}

vm_start() { vm_prepare "$1"; vm_launch "$1"; }

vm_kill() {
    local d=$RUN/vm$1
    [ -f "$d/pid" ] || return 0
    local pid
    pid=$(cat "$d/pid")
    kill "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null; do sleep 0.01; done
    rm -f "$d/pid" "$d/api.sock"
}

vm_destroy() { vm_kill "$1"; tap_down "$1"; rm -rf "$RUN/vm$1"; }

destroy_all() {
    local d
    for d in "$RUN"/vm*; do
        [ -d "$d" ] || continue
        vm_destroy "${d##*/vm}"
    done
}

# Blocks until the tenant answers a query, or fails after 60s.
vm_wait() {
    "$ROOT/probe/probe" first -addr "$(vm_ip "$1"):5432" \
        -t0 "$(date +%s%N)" -timeout 60s >/dev/null
}

# Starts VMs [from, to) sixteen at a time and waits for each to answer.
# Stops early, returning 1, when the host is at its memory floor.
start_range() {
    local from=$1 to=$2 i j
    for ((i = from; i < to; i += 16)); do
        have_room || return 1
        for ((j = i; j < i + 16 && j < to; j++)); do vm_start "$j"; done
        for ((j = i; j < i + 16 && j < to; j++)); do vm_wait "$j"; done
    done
}

# Host memory the VMs hold, in KiB: proportional set size, so pages
# shared between Firecracker processes are counted once.
vms_pss_kb() {
    local d sum=0 kb
    for d in "$RUN"/vm*; do
        [ -f "$d/pid" ] || continue
        kb=$(awk '/^Pss:/ {print $2}' "/proc/$(cat "$d/pid")/smaps_rollup")
        sum=$((sum + kb))
    done
    echo "$sum"
}

# Blocks the data disks really occupy, in KiB (the images are sparse).
vms_disk_kb() { du -sk --total "$RUN"/vm*/data.ext4 | awk 'END {print $1}'; }

addrs_file() {
    local n=$1 f=$RUN/addrs.txt i
    : >"$f"
    for ((i = 0; i < n; i++)); do echo "$(vm_ip "$i"):5432" >>"$f"; done
    echo "$f"
}
