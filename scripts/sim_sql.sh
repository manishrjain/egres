#!/usr/bin/env bash
# The SQL workload under the simulation (sql/sim_test.zig), one run per
# seed: pgwire clients against a real server on the simulated disk and
# network, crashes, restarts, and the check that no acknowledged
# statement is lost.
# A failing seed replays with the command printed for it; its log is
# kept under the directory named at the end.
#
# Usage: scripts/sim_sql.sh [first_seed] [count] [sched] [filter]
#   filter      a test name filter; one that starts with "BIG:" runs
#               the slow tests, such as the one with disk errors
set -uo pipefail
cd "$(dirname "$0")/.."
first=${1:-1}
count=${2:-20}
sched=${3:-random}
filter=${4:-sim: sql}
logs=$(mktemp -d "${TMPDIR:-/tmp}/sim-sql.XXXXXX")
# Unquoted where it is used: empty, or the one flag.
big=
case "$filter" in BIG:*) big=--bigtest ;; esac

failed=0
for ((seed = first; seed < first + count; seed++)); do
    out="$logs/seed-$seed.log"
    timeout 600 zig build test -- --sim-seed "$seed" \
        --sim-sched "$sched" $big --filter "$filter" >"$out" 2>&1
    code=$?
    # The runner runs a failed test once more and exits 0 when that
    # passes. Under a seed the first failure is the finding.
    if [ $code -eq 0 ] && grep -aq ' \.\.\. FAIL: ' "$out"; then
        code=retried
    fi
    summary=$(grep -aE '^sim: ' "$out" | tail -1)
    if [ "$code" = 0 ]; then
        echo "ok   ${summary#sim: }"
        rm -f "$out"
    else
        failed=$((failed + 1))
        echo "FAIL seed=$seed exit=$code ${summary#sim: seed=* }"
        echo "     zig build test -- --sim-seed $seed" \
            "--sim-sched $sched $big --filter \"$filter\""
        grep -aE 'FAIL:|error\(sim_test\)|panic' "$out" | head -5 |
            cut -c1-200 | sed 's/^/     /'
    fi
done
echo "$failed of $count seeds failed; logs: $logs"
[ $failed -eq 0 ]
