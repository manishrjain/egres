# SQL compatibility notes

The reference is `docs/postgres-compatibility.md`. The client follows
the SQL of `jepsen-io/postgres` (commit
`60480a0f09313ade150408befc3f58ae1e4ccbad`) and its `io.jepsen/sql`
workloads, adapted where Egres differs:

| Upstream SQL / behavior | Egres adaptation |
| --- | --- |
| `UPDATE ... SET val = CONCAT(val, ',', ?)` | `val = val \|\| ?` with the comma in the parameter. |
| `INSERT ... ON CONFLICT ... DO UPDATE` | UPDATE first, INSERT if absent. |
| Savepoints and rollback to savepoint | Abort on a duplicate-key race. |
| `INSERT INTO table AS t ...` | No table alias. |
| JDBC implicit BEGIN/query batching | Explicit, separate messages. |
| `rw` workload's `numeric`, `char` value encodings | BIGINT values only. |
| `internal-sim`, `default-value` workloads | Not ported: comparison operators, `ALTER TABLE`. |

Every value is a `?` bind parameter (a `long`, or the appended text);
table names come from an integer table index. Named server-side
prepared statements are not exercised (`prepareThreshold=0`).

Errors: 40001, 40P01, 23505 and 72000 are failed transactions, never
retried, as is `XX000: not the leader`; transport errors (`08xxx`,
`57P0x`) are indeterminate; any other error fails the run.
