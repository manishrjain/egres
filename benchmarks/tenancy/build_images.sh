#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Builds the two guest images from the Postgres 18 container image
# (PG_IMAGE, pinned in env.sh):
#   $WORK/assets/rootfs.ext4  read-only, shared by every VM
#   $WORK/assets/data.ext4    one tenant's initialised data directory;
#                             each VM gets its own sparse copy
# Needs docker and sudo. No KVM needed.
source "$(dirname "$0")/lib.sh"
mkdir -p "$A"
cd "$A"

echo "== rootfs"
sudo rm -rf rootfs.d
mkdir rootfs.d
cid=$(docker create "$PG_IMAGE")
docker export "$cid" | sudo tar -x -C rootfs.d
docker rm "$cid" >/dev/null
sudo install -m 755 "$ROOT/fc-init" rootfs.d/fc-init
sudo mkdir -p rootfs.d/data
mib=$(($(sudo du -sm rootfs.d | cut -f1) + 200))
rm -f rootfs.ext4
sudo mkfs.ext4 -q -F -d rootfs.d rootfs.ext4 "${mib}M"
sudo chown "$USER:" rootfs.ext4
sudo rm -rf rootfs.d

echo "== data template"
rm -f data.ext4
truncate -s 256M data.ext4
mkfs.ext4 -q -F data.ext4
mkdir -p mnt
sudo mount -o loop data.ext4 mnt
trap 'sudo umount "$A/mnt" 2>/dev/null || true' EXIT
# A small-footprint configuration, the least a tenant's Postgres is
# usually given. Durability is left on (fsync, full WAL).
docker run --rm -v "$A/mnt:/data" "$PG_IMAGE" bash -ec '
chown postgres:postgres /data
gosu postgres initdb -D /data/pg -U postgres --auth=trust >/dev/null
cat >>/data/pg/postgresql.conf <<EOF
listen_addresses = '"'*'"'
shared_buffers = 16MB
max_connections = 20
EOF
echo "host all all 10.200.0.0/16 trust" >>/data/pg/pg_hba.conf
gosu postgres pg_ctl -D /data/pg -w -o "-c listen_addresses=" start >/dev/null
psql -q -U postgres <<EOF
CREATE TABLE t (id bigint PRIMARY KEY, v text);
INSERT INTO t SELECT g, md5(g::text) FROM generate_series(1, 1000) g;
CHECKPOINT;
EOF
gosu postgres pg_ctl -D /data/pg -w stop >/dev/null
'
sudo umount mnt
trap - EXIT
rmdir mnt

ls -l --block-size=M rootfs.ext4 data.ext4
echo "data template occupies $(du -m data.ext4 | cut -f1) MiB on disk"
# What the images hold, for machine.sh to report beside every result.
echo "rootfs.ext4 data.ext4: $PG_IMAGE," \
    "$(docker run --rm "$PG_IMAGE" postgres --version)" >images.txt
