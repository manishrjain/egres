#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Builds $WORK/assets/data_big.ext4: one tenant's data directory with ROWS
# rows, for the sleeping-tenant and noisy-neighbour tests. The same
# settings as data.ext4 (build_images.sh) but for max_wal_size, cut
# from 1GB to 256MB so a writing tenant's WAL stays well inside its
# disk. Three extra steps make sure a tenant which is only read never
# writes to its disk:
#   - every row is frozen, so a read does not set hint bits
#   - statistics are current, so autovacuum has nothing to do
#   - the WAL is cut back to one segment and freed blocks are punched
#     out, which keeps the per-tenant copy small
# Needs docker and sudo. No KVM needed.
#   ROWS=100000 ./build_big_image.sh
source "$(dirname "$0")/lib.sh"
ROWS=${ROWS:-100000}
cd "$A"

rm -f data_big.ext4
# Sparse: only what Postgres writes takes space on the host. The room
# is for a tenant that writes flat out (test 5): on a 512M disk its
# WAL filled the disk in 40s and took the VM down.
truncate -s 4G data_big.ext4
# No lazy initialisation, so the guest kernel has no inode tables left
# to zero, and so to write, after it mounts the disk. An 8 MiB journal:
# its blocks are written once used, and the size mkfs would pick for
# 4G (64 MiB) would double what every tenant's copy takes on the host.
mkfs.ext4 -q -F -J size=8 -E lazy_itable_init=0,lazy_journal_init=0 \
    data_big.ext4
mkdir -p mnt
sudo mount -o loop,discard data_big.ext4 mnt
trap 'sudo umount "$A/mnt" 2>/dev/null || true' EXIT
docker run --rm -e ROWS="$ROWS" -v "$A/mnt:/data" "$PG_IMAGE" bash -ec '
chown postgres:postgres /data
gosu postgres initdb -D /data/pg -U postgres --auth=trust >/dev/null
cat >>/data/pg/postgresql.conf <<EOF
listen_addresses = '"'*'"'
shared_buffers = 16MB
max_connections = 20
max_wal_size = 256MB
EOF
echo "host all all 10.200.0.0/16 trust" >>/data/pg/pg_hba.conf
gosu postgres pg_ctl -D /data/pg -w -o "-c listen_addresses=" start >/dev/null
psql -q -U postgres <<EOF
CREATE TABLE t (id bigint PRIMARY KEY, v text);
INSERT INTO t SELECT g, md5(g::text) FROM generate_series(1, $ROWS) g;
EOF
# Analyze first: it leaves dead rows in the statistics catalog, which
# the vacuum after it removes. The other way round autovacuum finds
# them a minute after every start and writes.
vacuumdb -q -U postgres --all --analyze-only
vacuumdb -q -U postgres --all --freeze
psql -q -U postgres -c CHECKPOINT
gosu postgres pg_ctl -D /data/pg -w stop >/dev/null
gosu postgres pg_resetwal -D /data/pg >/dev/null
'
sudo fstrim mnt
sudo umount mnt
trap - EXIT
rmdir mnt
echo "$ROWS" >data_big.rows
echo "data_big.ext4: $PG_IMAGE, $ROWS rows" >images_big.txt

ls -l --block-size=M data_big.ext4
echo "data_big.ext4 holds $ROWS rows and occupies" \
    "$(du -m data_big.ext4 | cut -f1) MiB on disk"
