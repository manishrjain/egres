# SQL compatibility notes

The reference is `docs/postgres-compatibility.md`. The workloads are
`io.jepsen/sql` 0.1.0, the library behind `jepsen-io/postgres` (commit
`60480a0f09313ade150408befc3f58ae1e4ccbad`), run unmodified: the
harness supplies the connection and the error mapping, as the
PostgreSQL suite does. What differs from a run against PostgreSQL:

- `--isolation` defaults to `repeatable-read` instead of
  `serializable`. Egres accepts any level and runs snapshot isolation,
  so `--expected-consistency-model` defaults to `snapshot-isolation`
  instead of following `--isolation`.
- `--key-types secondary` finds `sk` under a bitmap index unless
  `--no-secondary-index`; upstream scans an unindexed column.
- `--indirection` is refused: half its reads go through an
  `INNER JOIN`. `copy-on-write` upserts only occur with indirection,
  so they never run.
- The `internal-sim` workload is not offered: its generated schemas
  declare `TEXT COLLATE unicode` columns.
- Nemeses are kill and pause of the one local process, the kill with
  or without lazyfs losing the unsynced writes; upstream also
  partitions, skews clocks and changes membership.
- Errors are matched by SQLSTATE, below, where upstream matches the
  message.
- A final read of every key follows the last fault, retried on a
  connection that died across a restart; upstream has none.

The JDBC URL adds `prepareThreshold=0` (no server-named statements),
`binaryTransfer=true` (integer and float binds in binary) and
`preferQueryMode` from `--query-mode`.

Errors: 40001, 40P01, 23505, 72000, and 25P02 for a statement after
one of those, are definite failures, never retried, as is
`XX000: not the leader`; a failed INSERT under a savepoint is rolled
back to it by the workload, as upstream does. Transport errors
(`08xxx`, `57P0x`) are indeterminate; any other error fails the run.
