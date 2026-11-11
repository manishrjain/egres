#!/usr/bin/env bash
# Many seeds of the simulation for hours, several at a time: the test
# binary is built once, in ReleaseSafe, and copied into the run's
# directory, so a later build does not change what is running. Each
# seed is one process on its own simulated disk and network, so they
# share nothing. The scheduler rotates with the seed. A failing seed's
# log is kept, and the run stops early once enough have failed.
#
# Usage: scripts/sim_soak.sh <dir> [hours] [jobs] [filter] [first_seed]
#   dir         where the binary, the results and the logs go
#   filter      a test name filter; "" runs the whole suite per seed,
#               and one that starts with "BIG:" runs the slow tests
#
# $SIM_SOAK_FLAGS, if set, is more flags for the test runner, such as
# --exclude for a test that needs the source tree around it.
#
# Results: <dir>/ok.txt and <dir>/fail.txt, one line per seed, and
# <dir>/logs/<seed>.log for each failure. `touch <dir>/stop` ends the
# run after the seeds in flight.
set -uo pipefail
dir=${1:?usage: sim_soak.sh <dir> [hours] [jobs] [filter] [first_seed]}
# Relative to where the caller is, so before the move to the tree.
mkdir -p "$dir/logs"
dir=$(cd "$dir" && pwd)
cd "$(dirname "$0")/.."
hours=${2:-8}
jobs=${3:-8}
filter=${4-sim: sql}
first=${5:-1}
max_fail=25
scheds=(random priority_random priority_fifo)
# The slow tests are skipped unless asked for, and get an hour a seed
# where the others get half: with every core busy a slow one takes
# several times what it does alone.
big=()
limit_s=1800
case "$filter" in BIG:*)
    big=(--bigtest)
    limit_s=3600
    ;;
esac
read -ra more <<<"${SIM_SOAK_FLAGS:-}"

bin="$dir/test-ReleaseSafe"
if [ ! -x "$bin" ]; then
    zig build test-bin -Doptimize=ReleaseSafe || exit 1
    cp zig-out/bin/test-ReleaseSafe "$bin"
fi
rm -f "$dir/stop"
deadline=$(($(date +%s) + hours * 3600))

# One worker: every `jobs`-th seed from its own start.
worker() {
    local seed=$1
    while [ "$(date +%s)" -lt "$deadline" ] && [ ! -e "$dir/stop" ]; do
        local sched=${scheds[$((seed % ${#scheds[@]}))]}
        local out="$dir/logs/$seed.log"
        # A log is capped: a failure that prints in a loop must not
        # fill the disk.
        # The runner's watchdog counts real seconds, and a slow test
        # is one test for minutes: it dumps the fibers at half the
        # limit and ends the process shortly before it.
        timeout "$limit_s" "$bin" --sim-seed "$seed" --sim-sched "$sched" \
            --wd-stuck-tick-s $((limit_s / 2)) \
            --wd-kill-s $((limit_s - 300)) \
            "${big[@]}" "${more[@]}" --filter "$filter" 2>&1 |
            head -c 100000000 >"$out"
        local code=${PIPESTATUS[0]}
        # The runner runs a failed test once more and exits 0 when that
        # passes. Under a seed the first failure is the finding.
        if [ "$code" -eq 0 ] && grep -aq ' \.\.\. FAIL: ' "$out"; then
            code=retried
        fi
        local summary
        summary=$(grep -aE '^sim: ' "$out" | tail -1)
        if [ "$code" = 0 ]; then
            echo "ok ${summary#sim: }" >>"$dir/ok.txt"
            rm -f "$out"
        else
            echo "FAIL exit=$code seed=$seed sched=$sched" \
                "${summary#sim: seed=* sched=* }" >>"$dir/fail.txt"
            if [ "$(wc -l <"$dir/fail.txt")" -ge "$max_fail" ]; then
                touch "$dir/stop"
            fi
        fi
        seed=$((seed + jobs))
    done
}

touch "$dir/ok.txt" "$dir/fail.txt"
echo "soak: filter='$filter' jobs=$jobs hours=$hours first_seed=$first" \
    "started $(date '+%F %T')" | tee "$dir/run.txt"
for ((job = 0; job < jobs; job++)); do
    worker $((first + job)) &
done
wait

passed=$(wc -l <"$dir/ok.txt")
failed=$(wc -l <"$dir/fail.txt")
echo "soak: $passed passed, $failed failed, ended $(date '+%F %T')" |
    tee -a "$dir/run.txt"
if [ "$failed" -gt 0 ]; then
    echo "replay one with:" \
        "$bin --sim-seed N --sim-sched S ${big[*]} --filter '$filter'"
    cat "$dir/fail.txt"
fi
[ "$failed" -eq 0 ]
