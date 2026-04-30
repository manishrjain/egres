# egres

## Regenerating Buffers Code

Buffer schemas live in `<module>/buffers.zig` files (e.g. `zbuf/buffers.zig`,
`raft/buffers.zig`, `zdb/buffers.zig`). Each generates its `<module>/buffers.gen.zig`
sibling.

Regenerate every discovered `*/buffers.gen.zig` at once:

```bash
zig build buffers
```

To generate for a specific schema file (writes `<input>.gen.zig` next to it):

```bash
zig build zbuf -- path/to/buffers.zig
```
