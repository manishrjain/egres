#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Whether the drive under WORK slows down when left idle: single 4 KiB
# O_DIRECT reads after idle gaps from 0 to 2 s. A drive that sleeps
# shows the gap in every latency a test reports, so this runs before
# them. Reads the root image, whose blocks are all on the drive (the
# sparse data images would be read from holes).
#   ./drive_idle.sh
source "$(dirname "$0")/env.sh"
set -euo pipefail
mkdir -p "$RES"
python3 "$ROOT/idle_read.py" "$A/rootfs.ext4" | tee "$RES/drive_idle.txt"
