# Jepsen for Egres

Jepsen's Elle workloads through PostgreSQL JDBC against a local Egres
process, checked for snapshot isolation (`docs/design/transactions.md`).
The client adapts the SQL of the
[upstream PostgreSQL suite](https://github.com/jepsen-io/postgres) to
the subset Egres implements; see
[SQL-COMPATIBILITY.md](SQL-COMPATIBILITY.md). Licensing is in
[LICENSE-NOTES.md](LICENSE-NOTES.md).

## Requirements

- Zig 0.16.0 and Egres's build dependencies.
- Java (tested with OpenJDK 25) and [Leiningen](https://leiningen.org/).
- Graphviz (`dot`), which Elle uses to render dependency graphs.
- Three free local TCP ports: SQL at 13210, Raft at 13211, admin at
  13212. `--port` moves the first; the next two follow it.

## Running

From the repository root:

```sh
tests/jepsen/run.sh --time-limit 60 --rate 200
tests/jepsen/run.sh --query-mode extended --access secondary
tests/jepsen/run.sh --nemesis kill --time-limit 120 --rate 200
tests/jepsen/run.sh --workload rw --query-mode extended --nemesis pause
tests/jepsen/run.sh --workload internal --query-mode extended
tests/jepsen/matrix.sh
```

`run.sh` builds Egres in ReleaseSafe, runs the harness's unit tests
(`lein test`), then one Jepsen test with 8 clients. `matrix.sh` runs
these cases in sequence, each with 16 clients, rate 300 and `DURATION`
seconds (60): append over both query modes and all three access paths,
append with kills on each access path, append with 8-second pauses,
and rw and internal with and without kills. It continues after a
failed case, writes logs and a TSV summary under `runs/`, and exits
nonzero if any case fails. Set `LEIN` to use another Leiningen
executable. `.github/workflows/jepsen.yml` runs the matrix nightly on
a Linux runner and keeps the logs and the store as the job's artifact.

To reuse a build:

```sh
cd tests/jepsen
lein run test --nodes 127.0.0.1 --concurrency 16 \
  --time-limit 120 --rate 500 --key-count 4 --nemesis kill
lein run test --help
```

Run one test at a time on a given set of ports. Each test starts its
own Egres process and stops only that process; no SSH, containers or
sudo are involved.

## Options

- `--workload append` (default): Elle's list-append. Each key is a
  comma-separated list in a TEXT column; an append is `val = val || ?`,
  or INSERT for a new key. `rw`: Elle's write-read registers, BIGINT
  values written by UPDATE, or INSERT for a new key. `internal`:
  upstream's internal-consistency check. A transaction inserts, updates
  and reads recent keys, then reads every key it touched again and must
  see its own latest write; it has no Elle check and no final reads.
- `--query-mode simple|extended`: pgjdbc's protocol. Every statement
  is a prepared statement with `?` parameters. Extended sends integers
  in binary with their types declared at Parse; simple inlines them as
  cast literals.
- `--access primary|secondary|scan`: look keys up by primary key, by a
  bitmap index on `sk`, or by `sk` without an index.
- `--nemesis kill|pause`: every `--nemesis-interval` seconds (5),
  SIGKILL and restart the server on the same data directory, or
  SIGSTOP it, for `--fault-duration` seconds (1).
- `--table-count`, `--key-count`, `--max-txn-length`,
  `--max-writes-per-key`, `--rate`: workload shape.

The checker is Elle's snapshot-isolation check (the internal check for
that workload) plus: some write transactions committed, the final reads
of every key succeeded, no unhandled exceptions, and the requested
faults happened. Serialization failures (40001), deadlocks (40P01),
duplicate-key races (23505) and expired snapshots (72000) are failed
transactions; transport errors, including a lost COMMIT reply, are
indeterminate.

## Results

Jepsen writes each run under `store/<test>/<timestamp>/`, with
`store/current` pointing at the latest:

- `results.edn`: checker verdicts and counts.
- `history.edn`, `history.txt`: the recorded operations.
- `elle/`: anomalies and dependency graphs.
- `server/server.log`, `server/wal/`, `server/sst/`: the Egres process.

`lein run test` exits 0 for valid, 1 for invalid, 2 for inconclusive
and 255 for a crashed harness. To re-analyze a run or browse the store:

```sh
cd tests/jepsen
lein run analyze -t store/TEST/TIMESTAMP
lein run serve
```

Runs are ignored by version control and never removed by the harness;
WAL files can be large.
