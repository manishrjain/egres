#!/usr/bin/env bash
# The simulation's two promises, checked end to end on the test suite:
#
#   1. Determinism: one seed, run twice over a suite, prints the same
#      trace hash, which covers every dispatch and every disk and
#      network op; another seed prints a different one. Checked on the
#      Channel tests (pure scheduling), the WAL tests (the disk), the
#      pgwire driver tests (the network) and the SQL harness (a whole
#      node across crashes).
#   2. Deadlock detection: a fiber parked with nothing left to wake it
#      ends the run with outcome=deadlock and exit code 3.
#
# Usage: scripts/sim_check.sh [seed] [filter]
# A filter given here replaces the default set.
set -euo pipefail
cd "$(dirname "$0")/.."
seed=${1:-42}
if [ $# -ge 2 ]; then
    filters=("$2")
else
    filters=("Channel:" "diskwal" "driver:" "sim: sql")
fi

trace() {
    zig build test -- --sim-seed "$1" --filter "$2" 2>&1 |
        grep -E '^sim: ' || true
}

for filter in "${filters[@]}"; do
    echo "filter: $filter"
    a=$(trace "$seed" "$filter")
    b=$(trace "$seed" "$filter")
    c=$(trace $((seed + 1)) "$filter")
    echo "$a"
    echo "$b"
    echo "$c"
    [ -n "$a" ] || { echo "FAIL: no sim summary line"; exit 1; }
    [ "$a" = "$b" ] ||
        { echo "FAIL: seed $seed differs between runs"; exit 1; }
    ta=$(sed -E 's/.*trace=([0-9a-f]+).*/\1/' <<<"$a")
    tc=$(sed -E 's/.*trace=([0-9a-f]+).*/\1/' <<<"$c")
    [ "$ta" != "$tc" ] ||
        { echo "FAIL: seeds $seed and $((seed + 1)) agree"; exit 1; }
done
echo "determinism: ok"

set +e
out=$(zig build test -- --sim-seed "$seed" --run-hang \
    --filter "parks forever" 2>&1)
code=$?
set -e
grep -E '^sim: .*outcome=deadlock' <<<"$out" ||
    { echo "FAIL: no deadlock outcome"; echo "$out" | tail -20; exit 1; }
# zig build wraps the runner's exit 3 in its own exit 1; the outcome
# line above is the check, the code only confirms the run failed.
[ "$code" -ne 0 ] || { echo "FAIL: deadlock run exited 0"; exit 1; }
echo "deadlock detection: ok"
