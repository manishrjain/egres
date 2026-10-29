#!/usr/bin/env bash
# Copyright 2026 Manish R Jain
# SPDX-License-Identifier: Apache-2.0

# A soak: the orders benchmark cut to a CI runner's size, run in the
# foreground with pass/fail checks. It drives the same harness as a real
# run (benchmarks/run), so what passes here is what the benchmark
# measures, and the run dir holds the same summaries.
#
#   scripts/soak.sh    needs benchmarks/run/local.toml, a built binary
#                      (local.toml's egres_bin, else zig-out/bin/egres)
#                      and an empty <data_root>/egres-orders-soak.
#                      SOAK_LOCAL="out_root=/x egres_bin=/y" overrides
#                      local.toml's keys for the run.
#
# Phases: start the server; load with readers alongside for
# SOAK_LOAD_SECS, then the readers alone for SOAK_SETTLE_SECS; count the
# rows; one pgbench workload run of SOAK_WORKLOAD_SECS; stop. Fails on a
# failed load chunk, a failed or aborted pgbench transaction, a row
# count that differs from the load's, a panic or error line in the
# server log, a server that dies, or a shutdown that does not complete.
# Warnings are counted and shown, not failed on.
set -euo pipefail
cd "$(dirname "$0")/.."

LOAD_SECS=${SOAK_LOAD_SECS:-900}
SETTLE_SECS=${SOAK_SETTLE_SECS:-120}
WORKLOAD_SECS=${SOAK_WORKLOAD_SECS:-120}
STREAMS=${SOAK_STREAMS:-4}
READERS=${SOAK_READERS:-4}
CLIENTS=${SOAK_CLIENTS:-8}
# The data dir's cap. Unset = sized from the data root's free space
# below: the WAL beside it runs about half the data dir again, the cap
# is checked once a minute, and the logs need a little.
STOP_BYTES=${SOAK_STOP_BYTES:-}
CACHE=${SOAK_CACHE:-2GiB}
COMPACTORS=${SOAK_COMPACTORS:-2}

bench=benchmarks/run/bench.py
env=egres-orders
name=soak
common=(--name "$name" --no-monitor)
# local.toml overrides for this run, space-separated key=value pairs:
# a fresh build beside a pinned one, or roots on another disk.
for kv in ${SOAK_LOCAL:-}; do
    common+=(--local "$kv")
done
sets=(
    --set "load.streams=$STREAMS"
    --set "load.stop_secs=$LOAD_SECS"
    --set "load.after_secs=$SETTLE_SECS"
    --set "load.chunk=20000"
    --set "readers.reads=$READERS"
    --set "clients=$CLIENTS"
    --set "secs=$WORKLOAD_SECS"
    --set "runs=1"
    --set "server.memory=total_bytes=$CACHE"
    --set "server.zdb_extra=num_compactors=$COMPACTORS"
)

local_key() {
    python3 -c "import tomllib, sys
print(tomllib.load(open('benchmarks/run/local.toml', 'rb'))[sys.argv[1]])" \
        "$1"
}
# A whole-number float (pgbench's failed count comes as 0.0) prints as
# an int, so the checks below can compare it as a string.
json_key() { python3 -c "import json, sys
v = json.load(open(sys.argv[1]))[sys.argv[2]]
print(int(v) if isinstance(v, float) and v.is_integer() else v)" "$1" "$2"; }

pass=0
fail=0
ok() { echo "ok: $1"; pass=$((pass + 1)); }
bad() { echo "FAIL: $1"; fail=$((fail + 1)); }

out_root=$(local_key out_root)
data_root=$(local_key data_root)
pgbin=$(local_key pgbin)
for kv in ${SOAK_LOCAL:-}; do
    case "$kv" in
        out_root=*) out_root=${kv#out_root=} ;;
        data_root=*) data_root=${kv#data_root=} ;;
        pgbin=*) pgbin=${kv#pgbin=} ;;
    esac
done
if [ -z "$STOP_BYTES" ]; then
    # df -k is POSIX: the fourth column is free KiB. A third of it for
    # the data dir leaves the WAL, the overshoot and the logs their share.
    free_kib=$(df -k "$data_root" | awk 'NR == 2 {print $4}')
    STOP_BYTES=$((free_kib * 1024 / 3))
fi
sets+=(--set "load.stop_bytes=$STOP_BYTES")
conn="host=127.0.0.1 port=3210 dbname=egres sslmode=disable"

echo "soak: load ${LOAD_SECS}s + settle ${SETTLE_SECS}s + workload" \
    "${WORKLOAD_SECS}s; streams=$STREAMS readers=$READERS" \
    "clients=$CLIENTS cache=$CACHE data cap $((STOP_BYTES >> 20)) MiB"
df -h "$data_root"
"$bench" "$env" begin "${common[@]}" "${sets[@]}"
run_dir=$(cat "$out_root/$env-$name.latest")
# Whatever happens, no server outlives the script.
trap '"$bench" "$env" stop "${common[@]}" >/dev/null 2>&1 || true' EXIT

"$bench" "$env" start "${common[@]}" "${sets[@]}"
server_pid=$(cat "$run_dir/server.pid")
"$bench" "$env" load --foreground "${common[@]}" "${sets[@]}"

if kill -0 "$server_pid" 2>/dev/null; then
    ok "server alive after the load"
else
    bad "server died during the load; see $run_dir/server.log"
fi
summary=$run_dir/load-summary.json
rows=$(json_key "$summary" rows)
failed_chunks=$(json_key "$summary" failed_chunks)
if [ "$failed_chunks" = 0 ] && [ "$rows" -gt 0 ]; then
    ok "load committed $rows rows, no failed chunk"
else
    bad "load: rows=$rows failed_chunks=$failed_chunks"
fi
count=$("$pgbin/psql" "$conn" -X -tA -c "SELECT count(*) FROM orders" \
    || echo "query failed")
if [ "$count" = "$rows" ]; then
    ok "count(*) matches the load"
else
    bad "count(*) is $count, the load committed $rows"
fi

# The readers: every pgbench round's report, appended to one log.
readers_log=$run_dir/results/readers-reads.log
reader_failed=$(grep -o 'failed transactions: [0-9]*' "$readers_log" |
    awk '{s += $3} END {print s + 0}')
reader_aborted=$(grep -cE 'aborted|ERROR' "$readers_log" || true)
if [ "$reader_failed" = 0 ] && [ "$reader_aborted" = 0 ]; then
    ok "readers: no failed or aborted transaction"
else
    bad "readers: failed=$reader_failed aborted/errors=$reader_aborted"
fi

"$bench" "$env" workload 1 "${common[@]}" "${sets[@]}" \
    > "$run_dir/workload-1.json"
w_rc=$(json_key "$run_dir/workload-1.json" rc)
w_failed=$(json_key "$run_dir/workload-1.json" failed)
w_tps=$(json_key "$run_dir/workload-1.json" tps)
if [ "$w_rc" = 0 ] && [ "$w_failed" = 0 ]; then
    ok "workload: exit 0, no failed transaction, $w_tps tps"
else
    bad "workload: exit=$w_rc failed=$w_failed"
fi

"$bench" "$env" stop "${common[@]}"
trap - EXIT
for _ in $(seq 1 60); do
    if ! kill -0 "$server_pid" 2>/dev/null; then break; fi
    sleep 1
done
log=$run_dir/server.log
if ! kill -0 "$server_pid" 2>/dev/null && grep -q "shutdown complete" "$log"
then
    ok "shutdown complete"
else
    bad "server did not shut down cleanly"
fi
# The level is a line's second field: `<time> ERROR <scope>: ...`. A
# panic is Zig's own text, with no timestamp.
errors=$(grep -cE '^[^ ]+ ERROR ' "$log" || true)
panics=$(grep -c 'panic' "$log" || true)
warnings=$(grep -cE '^[^ ]+ WARN ' "$log" || true)
if [ "$errors" = 0 ] && [ "$panics" = 0 ]; then
    ok "server log: no error or panic ($warnings warnings)"
else
    bad "server log: errors=$errors panics=$panics warnings=$warnings"
    grep -E '^[^ ]+ ERROR |panic' "$log" | head -n 5
fi

echo
echo "--- $run_dir/load-summary.md"
grep -E '^- (Rows|Insert|CPU|Server memory)|p95' "$run_dir/load-summary.md" \
    | head -n 12 || true
echo "--- workload"
grep -E 'tps|latency' "$run_dir/workload-1.json" | head -n 4 || true
echo
echo "soak: $pass ok, $fail failed; run dir $run_dir"
[ "$fail" = 0 ]
