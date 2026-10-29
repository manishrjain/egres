#!/usr/bin/env bash
# SPDX-License-Identifier: EPL-2.0
set -euo pipefail
cd "$(dirname "$0")/../.."

lein=${LEIN:-lein}
duration=${DURATION:-60}
command -v "$lein" >/dev/null
command -v dot >/dev/null
zig build -Doptimize=ReleaseSafe egres
cd tests/jepsen
"$lein" test
mkdir -p runs
out=$(mktemp -d "$PWD/runs/matrix.XXXXXXXX")
printf 'case\texit\tstore\n' > "$out/summary.tsv"
echo "Matrix logs: $out"
failed=0

run_case() {
    local label=$1 before after status
    shift
    before=$(readlink store/current 2>/dev/null || true)
    echo "Running $label"
    status=0
    "$lein" run test --nodes 127.0.0.1 --concurrency 16 \
        --time-limit "$duration" --rate 300 "$@" \
        > "$out/$label.log" 2>&1 || status=$?
    after=$(readlink store/current 2>/dev/null || true)
    [[ "$after" != "$before" ]] || after=""
    printf '%s\t%s\t%s\n' "$label" "$status" "$after" \
        >> "$out/summary.tsv"
    echo "$label: exit=$status store=$after"
    [[ "$status" == 0 ]] || failed=1
}

for mode in simple extended; do
    for access in primary secondary scan; do
        run_case "$mode-$access-none" \
            --query-mode "$mode" --access "$access"
    done
done
for access in primary secondary scan; do
    run_case "extended-$access-kill" \
        --query-mode extended --access "$access" --nemesis kill
done
run_case "extended-primary-pause" \
    --query-mode extended --access primary --nemesis pause --fault-duration 8
for workload in rw internal; do
    run_case "$workload-extended-primary-none" \
        --workload "$workload" --query-mode extended --access primary
    run_case "$workload-extended-primary-kill" \
        --workload "$workload" --query-mode extended --access primary \
        --nemesis kill
done
echo "Matrix summary: $out/summary.tsv"
exit "$failed"
