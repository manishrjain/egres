# Egres

Egres is a Postgres-compatible database for heavy, multi-tenant workloads.
Written in Zig, Egres is not a Postgres rewrite: it is a ground-up,
first-principles redesign.

This README is barebones on purpose. The full documentation is at
[egresql.io][docs]. Also read the [announcement][ann] post. If you like the
project, give it a star.

[ann]: https://manishrjain.com/egres/
[docs]: https://egresql.io/docs

## Install

```sh
curl -fsSL https://egresql.io/install.sh | sh
```

The installer puts the latest release's `egres` on your path. Release
tarballs for Linux x86_64 and Apple Silicon are on the releases page,
with a `SHA256SUMS` beside them.

## Run

A single node needs no flags: it keeps its store and write-ahead log
under `egres-data` in the working directory and listens for Postgres
clients on port 3210.

```sh
egres
psql -h localhost -p 3210 -d egres
```

`egres --help` lists every flag with its default.

## Build from source

[Zig 0.16.0](https://ziglang.org/download/), plus `bash`, `make` and
`curl`: the first build downloads jemalloc and compiles it with Zig's own
C compiler. No gcc or clang is needed; the Postgres parser and the
compression libraries are vendored and compiled by Zig too.

```sh
zig build -Doptimize=ReleaseFast
zig-out/bin/egres --help
```

A plain `zig build` is a Debug build: slow, every assertion on, and
debug log lines in.

## Developing

`zig build test` runs the unit tests; `scripts/e2e.sh` drives a real
server over pgwire with `psql`; `scripts/prepush.sh` is the gate a push
to main passes, and CI runs both on every push. The conventions the
code follows are in [AGENTS.md](AGENTS.md).

[Jepsen tests](tests/jepsen/README.md) check snapshot isolation over pgwire,
including process crashes and pauses. See the
[SQL compatibility notes](tests/jepsen/SQL-COMPATIBILITY.md).

## AI Policy

Most of the Egres code base was written using AI agents: Claude Opus and then
Claude Fable. I drove both the architecture and code design. I reviewed every
live code path myself, line by line, and iterated on it until it was correct and
struck the right balance between performance and readability.

A pull request is expected to have had the same quality of human review
before it is considered.

## Contributing

Open an issue first for anything large; Egres has strong opinions about
its design, and a short discussion beats a week of rework. Run
`scripts/prepush.sh` before opening a pull request. A bug fix comes with
a test that fails without it. Aim to write commit messages by hand, so they
are readable by humans; look at the history to understand the format
that's expected from the message.

In this new world of AI coding, the biggest overhead is reviews for the maintainer. A good
contribution is small and with a specific goal, so it's easier to review.
You can look at the issues to find what can use your contribution.

Before a first pull request merges, its author must sign the
[Contributor License Agreement](CLA.md) once, from their GitHub account.
The CLA bot comments on the pull request, and replying with the sentence
it asks for is the signature. It is the Apache Software Foundation's
individual agreement with the parties renamed. You keep the copyright to
your work and grant the project a license to use and sublicense it.

## License

Apache License 2.0, see [LICENSE](LICENSE). Every source file carries
an SPDX header. `third_party/` and the
[Jepsen harness](tests/jepsen/LICENSE-NOTES.md) (EPL-2.0) keep their
own licenses.
