# Use with: gdb -x tools/init.gdb --args ./binary <args>
# Or load mid-session with: (gdb) source tools/init.gdb
#
# Sets up sensible defaults for debugging the egres test binary
# (and other Zig binaries): no pagination, no confirmation prompts,
# breakpoints on every Zig panic entry, catchpoints on fatal signals,
# zio's stack-growth signal silenced so it doesn't interrupt stepping.

# ─── UI / interaction ────────────────────────────────────────────────
set pagination off
set confirm off
set print pretty on
set print thread-events off
set debuginfod enabled off

# ─── Signals zio/libxev use that should NOT stop the inferior ────────
# SIG34 is zio.coro's stack-growth signal (alt-stack SEGV handler
# extends commit). SIGUSR1/2 are used in some libxev paths.
handle SIG34 nostop noprint pass
handle SIGUSR1 nostop noprint pass
handle SIGUSR2 nostop noprint pass

# ─── Stop on these signals (fatal — the bug we want to inspect) ──────
catch signal SIGSEGV
catch signal SIGABRT
catch signal SIGBUS

# ─── Break on every Zig 0.16 panic entry ─────────────────────────────
# `rbreak` (regex break) handles the awkward `(function 'defaultPanic')`
# substring in the FullPanic instantiation symbol names.
rbreak ^debug.defaultPanic$
rbreak ^debug.FullPanic.*reachedUnreachable$
rbreak ^debug.FullPanic.*unwrapNull$
rbreak ^debug.FullPanic.*castToNull$
rbreak ^debug.FullPanic.*outOfBounds$
rbreak ^debug.FullPanic.*integerOverflow$
rbreak ^debug.FullPanic.*corruptSwitch$
rbreak ^debug.FullPanic.*inactiveUnionField$
rbreak ^debug.FullPanic.*copyLenMismatch$
rbreak ^debug.FullPanic.*forLenMismatch$
rbreak ^debug.FullPanic.*divideByZero$

# ─── Useful one-liners as named macros ───────────────────────────────

# `panic` — full triage dump at a panic / signal stop.
# Run this after the inferior stops on a panic breakpoint or signal.
define panic
    echo \n==== Crashing thread ====\n
    bt 40
    echo \n==== All threads ====\n
    info threads
    echo \n==== Each thread's top frames ====\n
    thread apply all bt 12
end
document panic
Triage dump at a panic / SEGV stop:
  - current thread's backtrace
  - list of all threads
  - top 12 frames of every thread
Run after the inferior hits a Zig panic breakpoint or SIGSEGV.
end

# `fiber-state` — inspect the current fiber if we're inside one.
# Useful when stopped at a runtime.* / fiber.* frame.
define fiber-state
    echo \n==== Worker.current TLS ====\n
    print runtime.Worker.current
    echo \n==== Current fiber ====\n
    print (*runtime.Worker.current).current_fiber
end
document fiber-state
Print the calling thread's Worker.current (TLS) and its
current_fiber. Useful when triaging runtime.* / channel.* / sync.*
panics from inside a fiber stack.
end

# ─── Helpful starting hint when launched ─────────────────────────────
echo \n--- tools/init.gdb loaded ---\n
echo Breakpoints on all Zig panic entries + catchpoints on\n
echo SIGSEGV/SIGABRT/SIGBUS are armed. Run `start` or `run` to begin.\n
echo Useful macros: `panic` (full triage), `fiber-state` (TLS state).\n\n
