// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const fiber = @import("zfiber");

test "Shutdown: start before wait returns immediately, idempotent" {
    var sd = fiber.Shutdown.init(std.testing.allocator);
    defer sd.deinit();

    try std.testing.expect(!sd.isStopping());
    sd.startShutdown();
    sd.startShutdown(); // idempotent
    try std.testing.expect(sd.isStopping());
    _ = sd.waitForStart(.shutdown, null); // must not block
    try std.testing.expect(sd.waitForStart(.shutdown, 60_000)); // nor this
}

test "Shutdown: waitTimeout times out while not started, wakes early on start" {
    var sd = fiber.Shutdown.init(std.testing.allocator);
    defer sd.deinit();

    // Not started: the wait runs its full course and reports timeout.
    try std.testing.expect(!sd.waitForStart(.shutdown, 20));
    try std.testing.expect(!sd.isStopping());

    // start() fired mid-wait ends a long wait immediately with true.
    const Waiter = struct {
        fn run(s: *fiber.Shutdown, woke: *std.atomic.Value(bool)) void {
            if (s.waitForStart(.shutdown, 60_000))
                woke.store(true, .release);
        }
    };
    var woke = std.atomic.Value(bool).init(false);
    var h = try fiber.spawn(.{}, Waiter.run, .{ &sd, &woke });
    fiber.sleepMs(20); // let it park
    try std.testing.expect(!woke.load(.acquire));
    sd.startShutdown();
    h.join();
    try std.testing.expect(woke.load(.acquire));
}

test "Shutdown: start wakes every parked waiter" {
    var sd = fiber.Shutdown.init(std.testing.allocator);
    defer sd.deinit();

    const N = 4;
    var woke = [_]std.atomic.Value(bool){.init(false)} ** N;
    const Waiter = struct {
        fn run(s: *fiber.Shutdown, flag: *std.atomic.Value(bool)) void {
            _ = s.waitForStart(.shutdown, null);
            flag.store(true, .release);
        }
    };
    var handles: [N]fiber.JoinHandle(void) = undefined;
    for (&handles, &woke) |*h, *w| h.* = try fiber.spawn(.{}, Waiter.run, .{ &sd, w });

    fiber.sleepMs(20); // let them park
    for (&woke) |*w| try std.testing.expect(!w.load(.acquire));

    sd.startShutdown();
    for (&handles) |*h| h.join();
    for (&woke) |*w| try std.testing.expect(w.load(.acquire));
}

test "Shutdown: startDrain wakes drain waiters only, start the rest" {
    var sd = fiber.Shutdown.init(std.testing.allocator);
    defer sd.deinit();

    const Waiter = struct {
        fn drain(s: *fiber.Shutdown, flag: *std.atomic.Value(bool)) void {
            _ = s.waitForStart(.drain, null);
            flag.store(true, .release);
        }
        fn stop(s: *fiber.Shutdown, flag: *std.atomic.Value(bool)) void {
            _ = s.waitForStart(.shutdown, null);
            flag.store(true, .release);
        }
    };
    var drained = std.atomic.Value(bool).init(false);
    var stopped = std.atomic.Value(bool).init(false);
    var hd = try fiber.spawn(.{}, Waiter.drain, .{ &sd, &drained });
    var hs = try fiber.spawn(.{}, Waiter.stop, .{ &sd, &stopped });
    fiber.sleepMs(20); // let them park

    try std.testing.expect(!sd.isDraining());
    sd.startDrain();
    sd.startDrain(); // idempotent
    hd.join();
    try std.testing.expect(sd.isDraining());
    try std.testing.expect(!sd.isStopping());
    try std.testing.expect(!stopped.load(.acquire));
    try std.testing.expect(!sd.waitForStart(.shutdown, 20));

    sd.startShutdown();
    hs.join();
    try std.testing.expect(sd.isStopping());
    try std.testing.expect(sd.isDraining());
    sd.startDrain(); // stopping never goes back to draining
    try std.testing.expect(sd.isStopping());
}

test "Shutdown: start without a drain wakes a drain waiter" {
    var sd = fiber.Shutdown.init(std.testing.allocator);
    defer sd.deinit();
    const Waiter = struct {
        fn run(s: *fiber.Shutdown, flag: *std.atomic.Value(bool)) void {
            _ = s.waitForStart(.drain, null);
            flag.store(true, .release);
        }
    };
    var woke = std.atomic.Value(bool).init(false);
    var h = try fiber.spawn(.{}, Waiter.run, .{ &sd, &woke });
    fiber.sleepMs(20);
    sd.startShutdown();
    h.join();
    try std.testing.expect(woke.load(.acquire));
    _ = sd.waitForStart(.drain, null); // must not block
}

test "Shutdown: contexts count in until the drain, the wait sees them" {
    var sd = fiber.Shutdown.init(std.testing.allocator);
    defer sd.deinit();

    try std.testing.expect(sd.waitForActiveCtxs(0)); // none in flight
    try std.testing.expect(sd.enterCtx());
    try std.testing.expect(sd.enterCtx());
    try std.testing.expectEqual(@as(u32, 2), sd.activeCtxs());
    try std.testing.expect(!sd.waitForActiveCtxs(40)); // times out
    sd.leaveCtx();
    sd.leaveCtx();
    try std.testing.expect(sd.waitForActiveCtxs(0));

    sd.startDrain();
    try std.testing.expect(!sd.enterCtx()); // refused, not counted
    try std.testing.expectEqual(@as(u32, 0), sd.activeCtxs());
}

test "Shutdown: a stage change ends the wait for contexts early" {
    var sd = fiber.Shutdown.init(std.testing.allocator);
    defer sd.deinit();
    try std.testing.expect(sd.enterCtx());
    defer sd.leaveCtx();
    const Waiter = struct {
        fn run(s: *fiber.Shutdown, out: *std.atomic.Value(bool)) void {
            out.store(s.waitForActiveCtxs(60_000), .release);
        }
    };
    var drained = std.atomic.Value(bool).init(true);
    var h = try fiber.spawn(.{}, Waiter.run, .{ &sd, &drained });
    fiber.sleepMs(30);
    sd.startDrain(); // the stage moved: the waiter gives up
    h.join();
    try std.testing.expect(!drained.load(.acquire));
}

test "Shutdown: install — first SIGTERM drains, second starts" {
    if (@import("builtin").os.tag == .windows) return;
    const posix = std.posix;

    // Save + restore the process handlers so this test can't leak its
    // sigaction into unrelated tests.
    var old_int: posix.Sigaction = undefined;
    var old_term: posix.Sigaction = undefined;
    posix.sigaction(posix.SIG.INT, null, &old_int);
    posix.sigaction(posix.SIG.TERM, null, &old_term);
    defer {
        posix.sigaction(posix.SIG.INT, &old_int, null);
        posix.sigaction(posix.SIG.TERM, &old_term, null);
    }

    var sd = fiber.Shutdown.init(std.testing.allocator);
    // deinit start()s (idempotent) then joins the watcher fiber.
    defer sd.deinit();

    // Not `install`: the tests beside this one would see its stopping
    // stage from their `maybeYield`.
    try sd.installSignals();
    try std.testing.expect(!sd.isDraining());
    try posix.raise(posix.SIG.TERM);
    // The handler counts the signal, the watcher fires startDrain.
    _ = sd.waitForStart(.drain, null);
    try std.testing.expect(sd.isDraining());
    try std.testing.expect(!sd.isStopping());
    try posix.raise(posix.SIG.INT);
    _ = sd.waitForStart(.shutdown, null); // the second signal fires start()
    try std.testing.expect(sd.isStopping());
}
