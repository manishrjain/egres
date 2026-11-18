#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Makes a machine ready for the tenancy benchmark, or says what it
# still lacks. On a fresh Ubuntu machine, in this order:
#   ./setup.sh packages          the Ubuntu packages (sudo apt-get)
#   sudo ./setup.sh host [taps]  kvm group, the taps' namespace (4000)
#   ./setup.sh all               assets, probe, egres, calibrate, check
# or one step at a time:
#   ./setup.sh assets     Firecracker, the guest kernel, guest images
#   ./setup.sh probe      the load client
#   ./setup.sh egres      this checkout's server, into $WORK/egres-bin
#   ./setup.sh calibrate  the drive, measured once for Egres's starts
#   ./setup.sh check      what is in place and what is not (default)
# The namespace is gone after a reboot: `sudo ./setup.sh host` again.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
source "$here/env.sh"

# Pinned, so another machine runs the same bytes. The kernel is the
# one Firecracker's getting-started guide fetches from its CI bucket.
FC_VERSION=v1.18.0
FC_URL=https://github.com/firecracker-microvm/firecracker/releases/download
FC_URL=$FC_URL/$FC_VERSION/firecracker-$FC_VERSION-x86_64.tgz
FC_SHA256=1ac8eec866dd5eea14efecf6323e22b23eea24054545338a5b34bc5bca6623cd
KERNEL_URL=https://s3.amazonaws.com/spec.ccfc.min/firecracker-ci
KERNEL_URL=$KERNEL_URL/v1.15/x86_64/vmlinux-6.1.155
KERNEL_SHA256=e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2
# Taps in the namespace, so tenants a micro-VM test can run at once.
TAPS=4000
# Files Egres holds open per tenant: four with one table each (84,000
# tenants held 335,204). The hard limit caps the tenant count.
FILES_PER_TENANT=4

repo=$(cd "$ROOT/../.." && pwd)

sha_ok() { [ -f "$1" ] && [ "$(sha256sum "$1" | cut -c1-64)" = "$2" ]; }

fetch() {
    local url=$1 sha=$2 dst=$3
    sha_ok "$dst" "$sha" && return 0
    curl -fsSL --retry 3 -o "$dst.part" "$url"
    sha_ok "$dst.part" "$sha" || {
        echo "$url: sha256 is not $sha" >&2
        rm -f "$dst.part"
        return 1
    }
    mv "$dst.part" "$dst"
}

do_packages() {
    sudo apt-get update
    sudo apt-get install -y docker.io postgresql-client curl make \
        python3 golang-go e2fsprogs iproute2 util-linux
    sudo usermod -aG docker "$USER"
    echo "log in again for the docker group; Zig $(zig_wanted) is not" \
        "packaged: https://ziglang.org/download/"
}

# The version README.md's "Build from source" names.
zig_wanted() {
    sed -n 's/^\[Zig \([0-9.]*\)\].*/\1/p' "$repo/README.md" | head -1
}

# Root's part, once per boot: the user in the kvm group (stays), and
# the network namespace with one tap per VM (gone at a reboot).
do_host() {
    [ "$(id -u)" = 0 ] || { echo "run with sudo" >&2; exit 1; }
    local user=${SUDO_USER:?run with sudo, not as root directly}
    modprobe kvm_amd 2>/dev/null || modprobe kvm_intel
    usermod -aG kvm "$user"
    "$here/netns_setup.sh" "${1:-$TAPS}"
}

do_assets() {
    mkdir -p "$A"
    local tgz=$A/firecracker-$FC_VERSION-x86_64.tgz
    fetch "$FC_URL" "$FC_SHA256" "$tgz"
    tar -xzf "$tgz" -C "$A" \
        "release-$FC_VERSION-x86_64/firecracker-$FC_VERSION-x86_64"
    mv "$A/release-$FC_VERSION-x86_64/firecracker-$FC_VERSION-x86_64" \
        "$A/firecracker"
    rmdir "$A/release-$FC_VERSION-x86_64"
    fetch "$KERNEL_URL" "$KERNEL_SHA256" "$A/vmlinux"
    # Images from before the blank data image are rebuilt.
    grep -q blank "$A/images.txt" 2>/dev/null || "$here/build_images.sh"
}

do_probe() { (cd "$here/probe" && go build -o probe .); }

do_egres() {
    (cd "$repo" && zig build -Drelease --prefix "$WORK/egres-bin")
    harness_rev server >"$WORK/egres-bin/COMMIT"
}

# The checkout's commit, and "(modified)" when it has changes on top:
# any change, or with `server`, any outside benchmarks/.
harness_rev() {
    local skip='^$'
    [ "${1:-}" = server ] && skip=' benchmarks/'
    local rev changes
    if git -C "$repo" rev-parse HEAD >/dev/null 2>&1; then
        rev="git $(git -C "$repo" rev-parse HEAD)"
        changes=$(git -C "$repo" status --porcelain | sed 's/^/ /')
    elif jj -R "$repo" root >/dev/null 2>&1; then
        rev="jj $(jj -R "$repo" log -r @- --no-graph --no-pager -T commit_id)"
        changes=$(jj -R "$repo" diff -r @ --summary --no-pager)
    else
        echo unknown
        return
    fi
    if printf '%s\n' "$changes" | grep -v "$skip" | grep -q .; then
        rev="$rev (modified)"
    fi
    echo "$rev"
}

# Measures the drive once, the way Egres does at start-up, and keeps
# the result in run/egres_io.flags; every later start is given it and
# probes nothing. On an idle machine, before the tests. A probe that
# lands in a drive stall (writes under a tenth of reads) is thrown away
# and taken again.
do_calibrate() (
    export BACKEND=egres EDATA=$WORK/egres-data-calibrate
    source "$here/common.sh"
    trap 'egres_stop; rm -rf "$EDATA"' EXIT
    rm -f "$RUN/egres_io.flags"
    local try line stalled mib cost
    for try in 1 2 3 4 5; do
        egres_stop
        rm -rf "$EDATA"
        PROBE=1 egres_launch
        egres_wait
        line=$(grep -a "disk calibrated" "$ELOG" | head -1)
        stalled=$(grep -ac "look like a drive stall" "$ELOG" || true)
        egres_stop
        echo "probe $try: ${line#*disk calibrated: }" >&2
        if [ -z "$line" ] || [ "$stalled" != 0 ]; then
            sleep 20
            continue
        fi
        mib=$(echo "$line" | sed -n 's/.*bandwidth_bytes=\([0-9]*\)MiB.*/\1/p')
        cost=$(echo "$line" | sed -n 's/.*write_cost=\([0-9]*\).*/\1/p')
        # iops: random one-page reads a second, from 58b2ed31 on; an
        # older server prints none, and the flag is left out.
        iops=$(echo "$line" | sed -n 's/.*iops=\([0-9]*\).*/\1/p')
        printf 'auto-calibrate=false;bandwidth-bytes=%s;write-cost=%s%s\n' \
            "$((mib << 20))" "$cost" "${iops:+;iops=$iops}" \
            >"$RUN/egres_io.flags"
        echo "kept: $(cat "$RUN/egres_io.flags")"
        exit 0
    done
    echo "no clean probe in 5 tries; nothing kept" >&2
    exit 1
)

missing=0
ok() { printf '  ok       %s\n' "$*"; }
lack() { printf '  MISSING  %s\n' "$*"; missing=1; }
have() { command -v "$1" >/dev/null 2>&1; }

do_check() {
    echo "machine"
    [ "$(uname -m)" = x86_64 ] && ok "x86_64" || lack "x86_64: $(uname -m)"
    if [ -r /dev/kvm ] && [ -w /dev/kvm ]; then
        ok "/dev/kvm"
    elif id -nG "$USER" | grep -qw kvm; then
        ok "/dev/kvm (in the kvm group; the tests take it on)"
    else
        lack "/dev/kvm: sudo ./setup.sh host"
    fi
    local fs
    fs=$(findmnt -no FSTYPE --target "$(dirname "$WORK")" 2>/dev/null || true)
    case $fs in
    "") lack "WORK=$WORK: no such filesystem" ;;
    tmpfs | ramfs) lack "WORK=$WORK is on $fs: Egres needs O_DIRECT" ;;
    *) ok "WORK=$WORK on $fs, $(df -h --output=avail "$(dirname "$WORK")" |
        tail -1 | tr -d ' ') free" ;;
    esac
    local hn
    hn=$(ulimit -Hn)
    ok "open files: hard limit $hn, about $((hn / FILES_PER_TENANT))" \
        "Egres tenants"
    ok "memory: $(awk '/MemTotal/ {printf "%.0f GiB", $2 / 1048576}' \
        /proc/meminfo)"

    echo "tools"
    local t
    for t in docker psql curl make python3 go mkfs.ext4 nsenter ip sudo sg; do
        have "$t" && ok "$t" || lack "$t: ./setup.sh packages"
    done
    have zig && ok "zig $(zig version)" ||
        lack "zig $(zig_wanted), for ./setup.sh egres"
    if have docker; then
        docker info >/dev/null 2>&1 && ok "docker without sudo" ||
            lack "docker without sudo: in the docker group, logged in again"
    fi
    if have psql; then
        local v
        v=$(psql --version | awk '{print int($3)}')
        ((v >= 18)) && ok "psql $v" || lack "psql 18 or later, not $v"
    fi

    echo "assets ($A)"
    sha_ok "$A/firecracker-$FC_VERSION-x86_64.tgz" "$FC_SHA256" &&
        [ -x "$A/firecracker" ] && ok "firecracker $FC_VERSION" ||
        lack "firecracker: ./setup.sh assets"
    sha_ok "$A/vmlinux" "$KERNEL_SHA256" && ok "guest kernel 6.1.155" ||
        lack "guest kernel: ./setup.sh assets"
    grep -q blank "$A/images.txt" 2>/dev/null &&
        ok "guest images: $(cat "$A/images.txt")" ||
        lack "guest images: ./setup.sh assets"

    echo "built"
    [ -x "$here/probe/probe" ] && ok "probe" || lack "probe: ./setup.sh probe"
    local bin=${EGRES_BIN:-$WORK/egres-bin/bin/egres}
    if [ -x "$bin" ]; then
        local rev=
        [ -f "$WORK/egres-bin/COMMIT" ] && [ -z "${EGRES_BIN:-}" ] &&
            rev=", $(cat "$WORK/egres-bin/COMMIT")"
        ok "egres $bin$rev"
    else
        lack "egres: ./setup.sh egres, or EGRES_BIN in local.sh"
    fi

    echo "this boot"
    if [ -e /run/netns/tbench ]; then
        # Listing another namespace's links takes root.
        ok "namespace tbench, $(sudo -n ip -n tbench -br link 2>/dev/null |
            grep -c '^fctap' || true) taps"
    else
        lack "namespace tbench: sudo ./setup.sh host"
    fi
    [ -f "$WORK/run/egres_io.flags" ] &&
        ok "drive calibrated: $(cat "$WORK/run/egres_io.flags")" ||
        lack "drive calibration: ./setup.sh calibrate"
    return "$missing"
}

case ${1:-check} in
packages) do_packages ;;
host) shift; do_host "$@" ;;
assets) do_assets ;;
probe) do_probe ;;
egres) do_egres ;;
calibrate) do_calibrate ;;
check) do_check ;;
all) do_assets; do_probe; do_egres; do_calibrate; do_check ;;
rev) harness_rev ;;
*) sed -n '5,16p' "$0" >&2; exit 2 ;;
esac
