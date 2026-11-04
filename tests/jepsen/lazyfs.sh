#!/usr/bin/env bash
# SPDX-License-Identifier: EPL-2.0
# Build lazyfs, the version Jepsen 0.3.13 installs, under
# tests/jepsen/lazyfs for --lazyfs. Needs git, g++, cmake and
# libfuse3-dev to build, and FUSE to run: /dev/fuse and a setuid
# fusermount3, as the fuse3 package installs them.
set -euo pipefail
cd "$(dirname "$0")"
version=0.3.0
if [[ ! -d lazyfs ]]; then
    git clone -q --depth 1 --branch "$version" \
        https://github.com/dsrhaslab/lazyfs.git lazyfs
fi
# Its toml dependency pins a CMake minimum that CMake 4 refuses.
export CMAKE_POLICY_VERSION_MINIMUM=3.5
(cd lazyfs/libs/libpcache && ./build.sh)
(cd lazyfs/lazyfs && ./build.sh)
