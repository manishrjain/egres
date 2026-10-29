#!/usr/bin/env bash
# SPDX-License-Identifier: EPL-2.0
set -euo pipefail
cd "$(dirname "$0")/../.."

lein=${LEIN:-lein}
command -v "$lein" >/dev/null || {
    echo "Install Leiningen, or set LEIN to its executable path." >&2
    exit 1
}
command -v dot >/dev/null || {
    echo "Install Graphviz (dot); Elle uses it to render dependency graphs." >&2
    exit 1
}

zig build -Doptimize=ReleaseSafe egres
cd tests/jepsen
"$lein" test
exec "$lein" run test --nodes 127.0.0.1 --concurrency 8 "$@"
