# Conventions

Read by people and by coding agents alike. Short on purpose: a rule is
here because the code does not make it obvious.

## Form

- Lines stay within 80 columns.
- A comment says why the code is there and what it assumes, next to
  the code and no longer than that. It never restates the line below.
  When a review adds a condition, rewrite the comment, do not append.
- `String` is the alias for `[]const u8` everywhere in this code,
  comptime tables and locals included.
- A file in the same package is imported with a leading underscore,
  `const _vfs = @import("vfs.zig")`; a package is imported plain,
  `const fiber = @import("zfiber")`. Each file logs through its own
  `const log = std.log.scoped(.name)`, named for the module.

## Naming

- Short names must still say what they hold: `col_idx` not `ci`,
  `where` not `w`, `tbl` for a table, `arloc` for an arena allocator.
- A struct per entity over parallel arrays.

## Shapes

- Collections are `ArrayList`, not a fixed array with a count; sets of
  u64 are `zroar.Bitmap`; maps that carry a payload stay hash maps.
- Direct control flow: a return over a labeled block that assigns.
- One mechanism per job: extend the type that owns the data rather than
  adding a wrapper, a parallel enum or a second path. Side effects such
  as uploads stay visible at the call site, never hidden in a helper.
- Wire formats are schemas in `<module>/buffers.zig`; the generated
  types are used directly. Generated code is changed through its
  generator: `zig build buffers` regenerates every `buffers.gen.zig`,
  the output is checked in, formatted, and tested with its module.
- Flags are `--group key=value` structs with a `docs` table; a doc
  string may not contain the word "default", since the help prints the
  value itself.
- Linux and macOS are both targets: a Linux syscall sits behind a
  comptime OS check with a fallback for the other.
- A spinlock is never held across a park or a yield.

## Simulation

- Application code uses fiber primitives and the VFS: no `std.Thread`,
  no atomics outside zfiber, no `std.time`, no `std.posix` I/O. A wall
  timestamp is `utils.timestampNow`; every other clock read, timeouts,
  ageing, elapsed time, is `utils.coarseMs()` or a clock handed in.
  Randomness is `fiber.random()` or a PRNG with a fixed seed, never
  drawn from the clock.
- No new process-wide singletons. State an engine owns lives on the
  Engine; state a node owns lives on the Node.
- A dependency with a real and a fake is a runtime interface (`ptr` plus
  `vtable`, or an enum switch as `vfs.Mode` is), not a comptime type
  parameter.
- Never hash or order by pointer address.

## Logs

- `error`: an operation failed and stayed failed, or data is suspect.
  `warn`: something unexpected that the server recovered from or runs
  degraded by. `info`: a state change, one line each. `debug`: per
  request or per batch, compiled out of release builds.
- A client's mistake is answered to the client, never a server warning
  or error. A line a retry loop would print on every pass goes through
  `utils.logEvery`.

## Verifying

- `zig build test` for the unit tests; the SQL end-to-end scripts in
  `scripts/` start a real server on fixed ports and one data directory,
  so only one runs at a time (`scripts/e2e.sh` runs them in order).
- ReleaseSafe for anything long-running that must be checked for correctness,
  ReleaseFast for benchmarks. A plain `zig build` is Debug.
- Processes are stopped by pid. `pkill -f` and `pgrep -f` match their
  own command line and have taken out the caller's shell before.
