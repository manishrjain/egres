#!/usr/bin/env bash
# The simulation's two promises, checked end to end on the test suite:
#
#   1. Determinism: one seed, run twice over tests that do no real I/O,
#      prints the same dispatch trace; another seed prints a different
#      one. Until the sim disk and network land, tests with real I/O
#      complete in real time and are not deterministic, so the default
#      filter is the Channel tests.
#   2. Deadlock detection: a fiber parked with nothing left to wake it
#      ends the run with outcome=deadlock and exit code 3.
#
# Usage: scripts/sim_check.sh [seed] [filter]
set -euo pipefail
cd "$(dirname "$0")/.."
seed=${1:-42}
filter=${2:-"Channel:"}

trace() {
    zig build test -- --sim-seed "$1" --filter "$filter" 2>&1 |
        grep -E '^sim: ' || true
}

a=$(trace "$seed")
b=$(trace "$seed")
c=$(trace $((seed + 1)))
echo "$a"
echo "$b"
echo "$c"
[ -n "$a" ] || { echo "FAIL: no sim summary line"; exit 1; }
[ "$a" = "$b" ] || { echo "FAIL: seed $seed differs between runs"; exit 1; }
ta=$(sed -E 's/.*trace=([0-9a-f]+).*/\1/' <<<"$a")
tc=$(sed -E 's/.*trace=([0-9a-f]+).*/\1/' <<<"$c")
[ "$ta" != "$tc" ] ||
    { echo "FAIL: seeds $seed and $((seed + 1)) agree"; exit 1; }
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
