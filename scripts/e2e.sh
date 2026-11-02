#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0
#
# The end-to-end scripts, in sequence, stopping at the first failure:
# each starts ./zig-out/bin/egres, drives it with psql, and checks the
# server log. Needs psql on PATH and a built exe; DATA (default
# /data/egres) is the server's data dir, wiped by every script. CI
# (.github/workflows/ci.yml, job e2e) runs this on every push to main;
# it is not in prepush.sh, so a local push stays quick.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

command -v psql >/dev/null || { echo "e2e: psql not on PATH"; exit 1; }
[ -x zig-out/bin/egres ] || {
    echo "e2e: no zig-out/bin/egres; build first"
    exit 1
}

for script in sql_ddl_e2e sql_branch_restart_e2e sql_dml_e2e \
    sql_txn_e2e; do
    echo "e2e: scripts/$script.sh ..."
    "scripts/$script.sh"
done
echo "e2e: OK"
