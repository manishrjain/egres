#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Runs a command as this user inside the benchmark's network
# namespace (netns_setup.sh), where the micro-VM taps live, with
# this shell's environment.
#   BACKEND=vm ./in_netns.sh ./footprint.sh 100 400
set -euo pipefail
ns=/run/netns/tbench
[ -e "$ns" ] || { echo "no namespace: sudo ./netns_setup.sh" >&2; exit 1; }
# This sudo cannot pass the whole environment on, so it travels in a
# file the command's shell reads back.
envf=$(mktemp)
export -p >"$envf"
exec sudo nsenter --net="$ns" sudo -u "$USER" \
    bash -c 'source "$0"; rm -f "$0"; exec "$@"' "$envf" "$@"
