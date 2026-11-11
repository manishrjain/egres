# pgwire

Postgres v3 wire-protocol server. Pure-logic protocol layer + zio-backed I/O
driver. The protocol layer (`proto.zig` / `conn_state.zig` / `parser.zig` /
`binary.zig`) is socket-free; only `driver.zig` touches `zio.net.Stream`.

## Layers

| File | Role |
|------|------|
| `proto.zig` | Frame encode/decode + body parsers |
| `conn_state.zig` | `ConnState` state machine |
| `parser.zig` | libpg_query bridge (`@cImport`) |
| `binary.zig` | Encoders/decoders for the 6 primitive binary types |
| `driver.zig` | `Session` — a `zio.net.Stream`'s buffers, reader/writer and `ConnState` |
| `client.zig` | `Client` — the other end, for tests: startup, one simple query at a time, rows as text |

Tests live in `*_test.zig` siblings; auto-discovered by `build.zig`.

## Connection lifecycle

One `Session` per accepted TCP socket. The embedder spawns a fiber per
connection, opens a session, and runs the event loop itself, so its
handler is a concrete type:

```
const session = try pgwire.Session.open(alloc, &stream, .{});
defer session.close();
while (!session.closed()) {
    const state = &session.state;
    const ev = try session.nextEvent();
    switch (ev) {
        .startup_complete => |params| try h.onStartup(state, params),
        .query => |sql| {
            try h.onQuery(state, sql);
            try state.endCommand();          // queues RFQ
        },
        .execute => |ctx| try h.onExecute(state, ctx),
        .describe => |ctx| try h.onDescribe(state, ctx),
        .sync, .flush, .terminate => {},
    }
    if (pgwire.Session.flushAfter(ev)) try session.flush();
}
```

Execute and Describe replies stay in the write buffer until the batch's
Sync, a client Flush, or the buffer filling, as Postgres does: one
socket write per extended batch, not one per message.

`nextEvent` blocks on the Reader for input; `sendXxx` methods queue output
frames through the Writer (auto-flushes when the write buffer fills, giving
TCP backpressure during large result sets without holding the result in
memory). `close` does a final flush for a trailing fatal error.

## Events surfaced to the handler

| Event | Front-end action |
|-------|------------------|
| `startup_complete` | State machine has queued AuthOk + ParameterStatus + BackendKeyData + RFQ. |
| `query` | `h.onQuery(state, sql)`, then `state.endCommand()` (queues RFQ). |
| `execute` | `h.onExecute(state, ExecuteCtx)`. RFQ is **not** emitted here — it arrives on the next `Sync`. An error sets `state.ext_failed`. |
| `describe` | `h.onDescribe(state, DescribeCtx)`. ParameterDescription already emitted for `stmt` kind. |
| `sync` | RFQ already queued; the flush sends it. |
| `flush` | Client `H` frame: flush what is queued, ahead of any Sync. |
| `terminate` | Frontend `X` frame, clean EOF, or fatal error. Loop exits. |

## Features

### Implemented

- [x] Simple query (`Q` frame)
- [x] Extended query (`Parse` / `Bind` / `Describe` / `Execute` / `Close` /
      `Sync` / `Flush`) with per-connection statement and portal registries
- [x] Spec-correct extended-batch error recovery — drop until next `Sync`
      (§4.1.4)
- [x] Binary format for 6 primitive types: `bool`, `int2`, `int4`, `int8`,
      `float4`, `float8`
- [x] Startup handshake — AuthenticationOk + 11 ParameterStatus
      (incl. `standard_conforming_strings=on`) + BackendKeyData +
      ReadyForQuery
- [x] Format-code validation — 0 (text) and 1 (binary) only
- [x] SSL / GSSEnc decline (`'N'` byte; client retries plain)
- [x] EmptyQueryResponse for whitespace/comment-only queries
- [x] Length-framed message dispatch with partial-frame handling, up to a
      16 MiB per-frame ceiling
- [ ] SCRAM-SHA-256 / MD5 authentication — today only AuthenticationOk
- [ ] TLS — SSLRequest declined; no post-`S` handshake
- [ ] CancelRequest routing — sentinel accepted, no executor-cancel plumbing
- [ ] Server-side type inference at `Parse` — today we echo client-supplied
      OIDs, so a `Parse` with empty `param_oids` produces an empty
      `ParameterDescription`
- [ ] Binary format for non-primitive types (timestamps, numeric, UUIDs,
      arrays)
- [ ] Multi-statement Query — per-statement RowDescription / DataRow* /
      CommandComplete with a single trailing RFQ
- [ ] COPY protocol (`CopyInResponse` / `CopyOutResponse` / `CopyData` /
      `CopyDone` / `CopyFail`)
- [ ] LISTEN / NOTIFY — server-side `NotificationResponse`
- [ ] NoticeResponse — non-fatal warnings
- [ ] `pg_catalog` backend (`pg_class`, `pg_attribute`, etc. — what psql
      `\d` queries)
- [ ] Full `pg_type` OID table — today 7 primitive entries in `types.zig`
