// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Smoke test for the test_runner's process-level watchdog.
//!
//! Default run skips this file's tests (HANG:-prefixed, opted out by
//! default in the runner — see `Args.run_hang`). Pass `--run-hang` to
//! include them:
//!
//!     zig build test -- --filter "HANG:" --run-hang
//!
//! With the default watchdog settings (`--wd-stuck-tick-s 20`,
//! `--wd-kill-s 300`) the run should:
//!   • emit a ThreadWatchdog dump every 20s starting at t≈20s,
//!   • abort the process with exit code 124 at t≈300s.
//!
//! Bump --wd-kill-s past 360 to let the sleep complete naturally and
//! verify the dump cadence on its own.

const std = @import("std");
const fiber = @import("zfiber");

test "HANG: hangs for 6 minutes (watchdog smoke test)" {
    fiber.sleepMs(6 * 60 * 1000);
}

// Under the simulation (`--sim-seed`) this is not a hang: a parked
// fiber nothing can wake, with no deadline and no I/O in flight, is
// declared a deadlock and the run exits 3 (scripts/sim_check.sh checks
// it). `sleepMs` above would not do: virtual time jumps over it.
test "HANG: parks forever (simulation deadlock smoke test)" {
    fiber.park();
}
