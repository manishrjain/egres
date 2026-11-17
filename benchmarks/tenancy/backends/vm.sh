#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Micro-VMs: every tenant a Firecracker VM (1 vCPU, MEM_MIB) running
# stock Postgres 18 from the images setup.sh built. Sourced by
# common.sh; see the interface there. Tenant i has tap fctap<i>, made
# by setup.sh host in the tbench namespace; run.sh runs the test there.
#   MEM_MIB=128  CONSOLE=1 (a serial console in console.log)
GW=10.200.0.1
MEM_MIB=${MEM_MIB:-128}
CONSOLE=${CONSOLE:-0}
# The data disk's cache: Unsafe lets the guest's flushes stop at the
# host's page cache, as a platform would run it.
DRIVE_CACHE=${DRIVE_CACHE:-Unsafe}
DISK_FLOOR_GIB=${DISK_FLOOR_GIB:-5}

# Tenant i is 10.200.x.y with x.y from i+256, so .0.x stays free for
# the gateway.
vm_ip() { local n=$(($1 + 256)); echo "10.200.$((n >> 8)).$((n & 255))"; }
vm_mac() {
    local n=$(($1 + 256))
    printf '06:00:0a:c8:%02x:%02x' $((n >> 8)) $((n & 255))
}
tenant_addr() { echo "$(vm_ip "$1"):5432"; }
# The most tenants this side can have at once: one tap each.
tenant_cap() { ip -br link | grep -c '^fctap' || true; }

disk_gib() { df -BG --output=avail "$RUN" | awk 'NR == 2 {print int($1)}'; }
# Snapshots and data disks fill the drive before memory runs out.
have_room() {
    [ "$(avail_gib)" -ge "$FLOOR_GIB" ] &&
        [ "$(disk_gib)" -ge "$DISK_FLOOR_GIB" ]
}

# /dev/kvm is group kvm; a login before the group was given has to take
# it on, so the script re-runs itself under sg once.
platform_init() {
    [ -r /dev/kvm ] && [ -w /dev/kvm ] && return 0
    if [ -z "${KVM_SG:-}" ] && id -nG "$USER" | grep -qw kvm; then
        export KVM_SG=1
        exec sg kvm -c "$(printf '%q ' "$0" "$@")"
    fi
    echo "no usable /dev/kvm: sudo ./setup.sh host" \
        "(virtualisation must be on in the BIOS)" >&2
    exit 1
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

# The VM's directory: a copy of the blank data image (initdb done, no
# table) and its config.
vm_prepare() {
    local i=$1 d=$RUN/vm$1
    mkdir -p "$d"
    [ -f "$d/data.ext4" ] || cp --sparse=always "$A/data.ext4" "$d/data.ext4"
    vm_config "$i"
}

vm_launch() {
    local i=$1 d=$RUN/vm$1
    rm -f "$d/api.sock"
    "$A/firecracker" --api-sock "$d/api.sock" \
        --config-file "$d/config.json" >"$d/console.log" 2>&1 &
    echo $! >"$d/pid"
}

vm_kill() {
    local d=$RUN/vm$1
    [ -f "$d/pid" ] || return 0
    local pid
    pid=$(cat "$d/pid")
    kill "$pid" 2>/dev/null || true
    while kill -0 "$pid" 2>/dev/null; do sleep 0.01; done
    rm -f "$d/pid" "$d/api.sock"
}

destroy_all() {
    local d
    for d in "$RUN"/vm*; do
        [ -d "$d" ] || continue
        vm_kill "${d##*/vm}"
        rm -rf "$d"
    done
}

platform_up() { destroy_all; }
platform_down() { destroy_all; }

# Boot, wait for Postgres, then the table and rows through SQL, as an
# Egres tenant gets them.
tenant_create() {
    local i=$1
    vm_prepare "$i"
    vm_launch "$i"
    "$ROOT/probe/probe" first -addr "$(tenant_addr "$i")" -q "SELECT 1" \
        -t0 "$(date +%s%N)" -timeout 60s >/dev/null
    seed "$(tenant_addr "$i")" "$2"
}

tenants_pss_kb() {
    local d sum=0 kb
    for d in "$RUN"/vm*; do
        [ -f "$d/pid" ] || continue
        kb=$(awk '/^Pss:/ {print $2}' "/proc/$(cat "$d/pid")/smaps_rollup")
        sum=$((sum + kb))
    done
    echo "$sum"
}
tenants_disk_kb() {
    du -sk --total "$RUN"/vm*/data.ext4 2>/dev/null | awk 'END {print $1 + 0}'
}
