#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Puts the micro-VM taps in a network namespace of their own, so the
# host's network daemons never see them. With 4,000 taps on the host,
# tailscaled took one to two cores at every VM start and stop (it
# rescans every interface on each link change) and systemd-resolved
# spun a core; a real micro-VM host keeps its taps away from such
# daemons too. The micro-VM tests then run inside the namespace:
#   sudo ./netns_setup.sh [taps]        default 4000
#   ./in_netns.sh ./test4_sleeping.sh ...
#   sudo ./netns_setup.sh remove
# Any fctap left on the host is removed first.
#
# Each tap gets a fixed MAC. A sleeping tenant's snapshot holds the
# guest's neighbour entry for the gateway, which is its tap's MAC: a
# tap made again with a new random MAC costs every wake about 8 s,
# until the guest gives up on the old address and asks again. Tenants
# put to sleep on taps with other MACs keep working if those are
# listed in $WORK/run/tap_macs.txt ("<vm> <mac> ..." per line).
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "run with sudo" >&2; exit 1; }
user=${SUDO_USER:?run with sudo, not as root directly}
source "$(dirname "$0")/env.sh"
ns=tbench
gw=10.200.0.1

host_taps_down() {
    local batch
    batch=$(mktemp)
    ip -br link | awk '$1 ~ /^fctap/ {print "link del " $1}' >"$batch"
    [ -s "$batch" ] && ip -batch "$batch"
    rm -f "$batch"
}

if [ "${1:-}" = remove ]; then
    ip netns del "$ns" 2>/dev/null || true
    rm -f "$WORK/.taps_precreated" "$WORK/.taps_netns"
    echo "namespace $ns removed"
    exit 0
fi

n=${1:-4000}
modprobe kvm_amd 2>/dev/null || modprobe kvm_intel
# KEEP_HOST=1 leaves the host's taps, for trying the namespace out
# while a run on them is still going.
[ "${KEEP_HOST:-0}" = 1 ] || host_taps_down
ip netns del "$ns" 2>/dev/null || true
ip netns add "$ns"
# The MAC goes on before the neighbour entry: changing a tap's MAC
# drops its neighbour entries, the permanent one included, and without
# that entry a wake's first SYN waits on ARP, which retries once a
# second.
declare -A kept
macs=$WORK/run/tap_macs.txt
if [ -f "$macs" ]; then
    while read -r vm mac _; do
        [ "$mac" = none ] || kept[$vm]=$mac
    done <"$macs"
fi
batch=$(mktemp)
echo "link set lo up" >"$batch"
for ((i = 0; i < n; i++)); do
    tap=fctap$i
    v=$((i + 256))
    addr=10.200.$((v >> 8)).$((v & 255))
    mac=$(printf '06:00:0a:c8:%02x:%02x' $((v >> 8)) $((v & 255)))
    tapmac=${kept[$i]:-$(printf '06:01:0a:c8:%02x:%02x' \
        $((v >> 8)) $((v & 255)))}
    cat >>"$batch" <<LINES
tuntap add dev $tap mode tap user $user
link set $tap address $tapmac
addr add $gw/32 dev $tap
link set $tap up
route replace $addr/32 dev $tap
neigh replace $addr lladdr $mac dev $tap nud permanent
LINES
done
ip -n "$ns" -batch "$batch"
rm -f "$batch"
sudo -u "$user" mkdir -p "$WORK"
sudo -u "$user" touch "$WORK/.taps_precreated" "$WORK/.taps_netns"
echo "$n taps ready in namespace $ns:" \
    "$(ip -n "$ns" -br link | grep -c '^fctap') there," \
    "$(ip -br link | grep -c '^fctap' || true) left on the host"
