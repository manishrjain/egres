# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# This machine's settings for the tenancy benchmark. Copy to local.sh
# (ignored by git) and edit; env.sh reads it.

# Everything the benchmark makes: guest images, VM directories, Egres
# and Postgres data, results. Put it on the disk whose numbers you
# want; Egres reads its files with O_DIRECT, so not on tmpfs.
WORK=/data/tenancy

# The Egres server. `./setup.sh egres` builds this checkout into
# $WORK/egres-bin; set it to measure another build.
#EGRES_BIN=/path/to/egres

# Below this much available memory no test adds another tenant.
#FLOOR_GIB=16

# Egres listens this far above its own ports (SQL 3210, admin 5210), so
# a server under development on the same machine is left alone.
#OFFSET=50
