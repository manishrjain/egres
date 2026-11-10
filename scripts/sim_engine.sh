#!/usr/bin/env bash
# The engine workload under the simulation (engine/sim_test.zig), one
# run per seed: writers against a real engine on the simulated disk,
# crashes, restarts, and the check that no acknowledged write is lost.
# A failing seed replays with the command printed for it; its log is
# kept under the directory named at the end.
#
# Usage: scripts/sim_engine.sh [first_seed] [count] [sched]
set -uo pipefail
cd "$(dirname "$0")/.."
first=${1:-1}
count=${2:-20}
sched=${3:-random}
logs=$(mktemp -d "${TMPDIR:-/tmp}/sim-engine.XXXXXX")
filter="sim: engine"

failed=0
for ((seed = first; seed < first + count; seed++)); do
    out="$logs/seed-$seed.log"
    timeout 600 zig build test -- --sim-seed "$seed" \
        --sim-sched "$sched" --filter "$filter" >"$out" 2>&1
    code=$?
    summary=$(grep -aE '^sim: ' "$out" | tail -1)
    if [ $code -eq 0 ]; then
        echo "ok   ${summary#sim: }"
        rm -f "$out"
    else
        failed=$((failed + 1))
        echo "FAIL seed=$seed exit=$code ${summary#sim: seed=* }"
        echo "     zig build test -- --sim-seed $seed" \
            "--sim-sched $sched --filter \"$filter\""
        grep -aE 'FAIL:|error\(sim_test\)|panic' "$out" | head -5 |
            cut -c1-200 | sed 's/^/     /'
    fi
done
echo "$failed of $count seeds failed; logs: $logs"
[ $failed -eq 0 ]
