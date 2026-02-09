# egres

## Regenerating Buffers Code

The buffer schema lives in `u/buffers.zig` and the generated code is written to `u/buffers.gen.zig`.

Regenerate the buffers code with:

```bash
zig build gen-zbuf
```

This runs the build-time generator at `zbuf/gen.zig` and updates `u/buffers.gen.zig`.

To generate for a specific schema file (and write `<input>.gen.zig` next to it), run:

```bash
zig build zbuf -- path/to/buffers.zig
```
