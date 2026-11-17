# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Where everything lives, for every script here. The scripts stay in
# the repository; what they make (images, VM and server data, results)
# goes under WORK, on the disk being measured. local.sh, when present,
# sets this machine's values (copy local.example.sh). Sourced, never
# run; sourcing it twice changes nothing.

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=/dev/null
[ -f "$ROOT/local.sh" ] && source "$ROOT/local.sh"

WORK=${WORK:-/data/tenancy}
# Guest kernel, Firecracker, the guest images, and what they were
# built from (images.txt).
A=$WORK/assets
# CSVs. run.sh points this at a directory of its own for each run.
RES=${RES:-$WORK/results}

# The guest images are built from this image of Postgres 18, pinned by
# digest so a rebuild on another machine has the same bytes.
PG_DIGEST=e3d24f6906d0760bf4da0dae872415391ededefad23fd471c47dc53b0ddf3c42
PG_IMAGE=${PG_IMAGE:-postgres:18@sha256:$PG_DIGEST}
