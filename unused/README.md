# unused/

Graveyard for source that is no longer wired into the build but kept on disk
for reference / possible future rehab. `build.zig`'s `isSkippedPath` excludes
this whole directory from test auto-discovery, and nothing in the live tree
imports it.

Current contents:

- `mux.zig` — the old stream-multiplexer (`Mux`). Superseded by `net/serial.zig`
  (one framed connection per peer/priority); no production code references it.
- `mux_test.zig` — tests for `mux.zig`.
- `transport_test.zig` — the old transport test, written against `Mux`. Needs a
  rewrite against `Serial` before it can return to `net/`.
