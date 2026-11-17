#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# Runs one test, or the whole suite, each into a directory of its own
# under $WORK/results, with what it takes to read the result later:
#   command.txt  the test, its arguments, every setting it was given
#   machine.txt  the machine and the versions (machine.sh)
#   output.log   everything the test printed
#   status.txt   start, end, exit status
#   *.csv, logs/ the test's rows, and the Egres server's logs
# <test>-<backend>.latest beside them points at the newest run.
#   BACKEND=egres ./run.sh footprint 1000 10000   one test, its settings
#   ./run.sh suite [label]    every step of suite.txt (or $SUITE), from
#                             <label> on
#   ./run.sh list             the tests, and the suite's steps
# One test at a time on the machine: they measure the whole of it.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
source "$here/env.sh"
results=$WORK/results
suite=${SUITE:-$here/suite.txt}

usage() { sed -n '5,17p' "$0" >&2; exit 2; }

tests() { echo footprint; echo noisy; echo agents; echo drive_idle; }

# The settings a run was given: every NAME the scripts read as
# ${NAME:-...} or ${NAME:?...} that has a value, here or from local.sh.
# Not RES, LOG_DIR or SUITE: those are run.sh's own, and the run's
# directory is where this is written.
settings() {
    local k
    grep -ohE '\$\{[A-Z][A-Z0-9_]*:[-?]' "$here"/*.sh |
        sed -E 's/^\$\{//; s/:.$//' | sort -u |
        grep -vxE 'RES|LOG_DIR|SUITE' | while read -r k; do
        if [ -n "${!k+x}" ]; then echo "$k=${!k}"; fi
    done
}

# run_one <dir> <test> [args...]: the test, with the environment the
# caller gave, into <dir>.
run_one() {
    local dir=$1 test=$2
    shift 2
    [ -x "$here/$test.sh" ] || { echo "no test $test" >&2; return 2; }
    mkdir -p "$dir"
    {
        echo "test: $test"
        echo "args: $*"
        settings
    } >"$dir/command.txt"
    "$here/machine.sh" >"$dir/machine.txt" 2>/dev/null || true
    local cmd=("$here/$test.sh" "$@")
    # The micro-VM side runs beside its taps, in the namespace.
    if [ "${BACKEND:-}" = vm ] && [ -e /run/netns/tbench ]; then
        cmd=("$here/in_netns.sh" "${cmd[@]}")
    fi
    echo "start: $(date -u +%FT%TZ)" >"$dir/status.txt"
    echo "== $test -> $dir"
    local rc=0
    RES=$dir LOG_DIR=$dir/logs "${cmd[@]}" 2>&1 | tee "$dir/output.log" ||
        rc=$?
    {
        echo "end: $(date -u +%FT%TZ)"
        echo "exit: $rc"
    } >>"$dir/status.txt"
    ln -sfn "$dir" "$results/$test${BACKEND:+-$BACKEND}.latest"
    return "$rc"
}

# The suite's steps, one per line: label, test, then settings and
# arguments.
steps() { grep -vE '^[[:space:]]*(#|$)' "$suite"; }

run_suite() {
    local from=${1:-} dir n=0 started=
    dir=$results/suite-$(date +%Y%m%d-%H%M%S)
    mkdir -p "$dir"
    cp "$suite" "$dir/suite.txt"
    [ -z "$from" ] && started=1
    local label test words env args w rc t0
    while read -r label test words; do
        n=$((n + 1))
        [ "$label" = "$from" ] && started=1
        [ -n "$started" ] || continue
        env=()
        args=()
        for w in $words; do
            if [[ $w =~ ^[A-Z_][A-Z0-9_]*= ]]; then env+=("$w"); else
                args+=("$w"); fi
        done
        t0=$(date +%s)
        rc=0
        # A subshell per step: a step's settings end with it. Its stdin
        # is not the suite's lines, which a test reading stdin would eat.
        (
            for w in "${env[@]}"; do export "$w"; done
            run_one "$dir/$(printf '%02d' "$n")-$label" "$test" \
                "${args[@]}"
        ) </dev/null || rc=$?
        # A failed step is recorded and the suite goes on: the others
        # do not depend on it, and a night's run should not stop there.
        echo "$label $test exit=$rc $(($(date +%s) - t0))s" |
            tee -a "$dir/summary.txt"
    done < <(steps)
    [ -n "$started" ] || { echo "no step $from in $suite" >&2; exit 2; }
    ln -sfn "$dir" "$results/suite.latest"
}

case ${1:-} in
"" | -h | --help) usage ;;
list)
    echo "tests:"
    tests | sed 's/^/  /'
    echo "suite ($suite):"
    steps | awk '{printf "  %-22s %s\n", $1, $2}'
    ;;
suite) shift; run_suite "$@" ;;
*)
    test=$1
    shift
    run_one "$results/$test${BACKEND:+-$BACKEND}-$(date +%Y%m%d-%H%M%S)" \
        "$test" "$@"
    ;;
esac
