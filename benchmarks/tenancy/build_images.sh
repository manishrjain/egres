#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Builds the guest images in $WORK/assets from the Postgres 18 image
# pinned in env.sh:
#   rootfs.ext4  the container's filesystem with fc-init as init, read
#                only in the guests
#   data.ext4    a blank data directory (initdb done, no table): every
#                tenant gets a copy and loads its own rows through SQL,
#                as an Egres tenant does. Sparse, so a copy costs what
#                initdb wrote.
# images.txt records what they were built from. Needs docker and sudo.
#   ./build_images.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"
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

echo "== data"
rm -f data.ext4
# Sparse, so a copy costs what has been written; 4 GiB of room because
# Postgres keeps up to 1 GiB of WAL before a checkpoint, and a tenant
# whose disk fills under test 1's updates dies with it.
truncate -s 4G data.ext4
mkfs.ext4 -q -F data.ext4
mkdir -p mnt
sudo mount -o loop data.ext4 mnt
trap 'sudo umount "$A/mnt" 2>/dev/null || true' EXIT
# Small, as a platform would run a tenant: 16 MB of buffers, 20
# connections, and a checkpoint every 256 MB of WAL rather than the
# stock 1 GB, so a tenant's disk use stays bounded. Trust from the
# taps' network; the host is the only client.
docker run --rm -v "$A/mnt:/data" "$PG_IMAGE" bash -ec '
chown postgres:postgres /data
gosu postgres initdb -D /data/pg -U postgres --auth=trust >/dev/null
cat >>/data/pg/postgresql.conf <<EOF
listen_addresses = '"'*'"'
shared_buffers = 16MB
max_connections = 20
max_wal_size = 256MB
EOF
echo "host all all 10.200.0.0/16 trust" >>/data/pg/pg_hba.conf
'
sudo umount mnt
trap - EXIT
rmdir mnt

ls -l --block-size=M rootfs.ext4 data.ext4
echo "data.ext4 occupies $(du -m data.ext4 | cut -f1) MiB on disk"
echo "rootfs.ext4 data.ext4 (blank): $PG_IMAGE," \
    "$(docker run --rm "$PG_IMAGE" postgres --version)" >images.txt
