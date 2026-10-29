# Licensing and source provenance

The harness is EPL-2.0, the license of the Jepsen suites it adapts.
The Egres server keeps its own Apache-2.0 license; these notes apply to
`tests/jepsen` only.

## Adapted files

These follow the structure of their upstream counterparts:

- `src/jepsen/egres.clj`: test assembly and CLI, after the PostgreSQL
  suite; the final-read generator, after Jepsen core.
- `src/jepsen/egres/client.clj`: the SQL client, after the SQL suite's
  append, register and internal workloads.
- `src/jepsen/egres/internal.clj`: the internal generator and checker,
  after the SQL suite.

Upstream revisions:

- PostgreSQL suite `60480a0f09313ade150408befc3f58ae1e4ccbad`, EPL-2.0
  with a conditional GPL secondary license.
- SQL suite `8bc137c722be0c5c4d2a2258d73dd875aa339fe8`, EPL-2.0 with
  the same secondary notice.
- Jepsen core 0.3.13, `0aad6ffdb6fb8064cb9ba4bf409bcc62f78e1482`,
  EPL-1.0; its section 7 allows electing a later version, and these
  adaptations elect EPL-2.0.

[NOTICE](resources/META-INF/NOTICE) keeps the upstream attribution and
secondary-license notice; the full license texts are in
[META-INF/licenses](resources/META-INF/licenses).

## Dependencies and distribution

| Component | License |
| --- | --- |
| Clojure 1.12.5 | EPL-1.0 |
| Jepsen core 0.3.13 | EPL-1.0 |
| Elle 0.2.7, through Jepsen | EPL-2.0 with conditional GPL secondary option |
| PostgreSQL JDBC 42.7.11 | BSD-2-Clause |

`lein jar` produces a source JAR with these notes, the NOTICE and the
license texts, and no dependency classes; `lein run` launches the
harness. A source distribution keeps the file notices and license
material; a compiled one makes the source available as EPL-2.0
section 3 requires. An uberjar or container that bundles dependencies
needs its own review of their licenses.
