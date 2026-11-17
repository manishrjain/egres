#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# What a result was measured on: the machine, the disk under WORK, the
# settings that move latency, and the version of everything that ran.
# run.sh writes it into every run's directory as machine.txt. Needs
# no root.
#   ./machine.sh
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
source "$here/env.sh"

field() { printf '%-22s %s\n' "$1:" "${2:-unknown}"; }
read1() { cat "$1" 2>/dev/null || echo unknown; }
cpu() { lscpu | sed -n "s/^$1: *//p" | head -1; }

field date "$(date -u +%FT%TZ)"
field host "$(hostname)"
field product "$(read1 /sys/class/dmi/id/sys_vendor)\
 $(read1 /sys/class/dmi/id/product_name)"
# "none" on bare metal, which the micro-VM side needs to be fair.
field virtualisation "$(systemd-detect-virt 2>/dev/null)"
field os "$(. /etc/os-release && echo "$PRETTY_NAME")"
field kernel "$(uname -r)"

field cpu "$(cpu 'Model name')"
field sockets "$(cpu 'Socket(s)')"
field cores_per_socket "$(cpu 'Core(s) per socket')"
field threads_per_core "$(cpu 'Thread(s) per core')"
field governor \
    "$(read1 /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"
field boost "$(read1 /sys/devices/system/cpu/cpufreq/boost)"
field memory "$(awk '/MemTotal/ {printf "%.1f GiB", $2 / 1048576}' \
    /proc/meminfo)"
field swap "$(awk '/SwapTotal/ {printf "%.1f GiB", $2 / 1048576}' \
    /proc/meminfo)"
field thp "$(read1 /sys/kernel/mm/transparent_hugepage/enabled)"
field swappiness "$(read1 /proc/sys/vm/swappiness)"
field max_map_count "$(read1 /proc/sys/vm/max_map_count)"
field open_files "soft $(ulimit -Sn), hard $(ulimit -Hn)"

# The disk WORK is on: the partition's parent is the drive.
mnt=$WORK
[ -e "$mnt" ] || mnt=$(dirname "$WORK")
src=$(findmnt -no SOURCE --target "$mnt" 2>/dev/null)
disk=$(lsblk -no PKNAME "$src" 2>/dev/null | head -1)
[ -n "$disk" ] || disk=$(basename "$src")
field work "$WORK"
field work_fs "$(findmnt -no FSTYPE,OPTIONS --target "$mnt" 2>/dev/null)"
field work_device "$src"
field drive "$(lsblk -dno MODEL,SIZE "/dev/$disk" 2>/dev/null |
    tr -s ' ')"
field drive_firmware "$(read1 "/sys/block/$disk/device/firmware_rev")"
field drive_scheduler "$(read1 "/sys/block/$disk/queue/scheduler")"
field work_free "$(df -h --output=avail "$mnt" 2>/dev/null | tail -1 |
    tr -d ' ')"

bin=${EGRES_BIN:-$WORK/egres-bin/bin/egres}
rev=unknown
[ -z "${EGRES_BIN:-}" ] && rev=$(read1 "$WORK/egres-bin/COMMIT")
field egres_bin "$bin"
field egres_sha256 "$(sha256sum "$bin" 2>/dev/null | cut -c1-64)"
field egres_commit "$rev"
field egres_io "$(read1 "$WORK/run/egres_io.flags")"
field harness "$("$here/setup.sh" rev)"
field firecracker "$("$A/firecracker" --version 2>/dev/null | head -1)"
field guest_kernel "$(sha256sum "$A/vmlinux" 2>/dev/null | cut -c1-64)"
field guest_images "$(read1 "$A/images.txt")"
field guest_big_image "$(read1 "$A/images_big.txt")"
field docker "$(docker version --format '{{.Server.Version}}' 2>/dev/null)"
field psql "$(psql --version 2>/dev/null)"
field go "$(go version 2>/dev/null)"
field python "$(python3 --version 2>/dev/null)"
