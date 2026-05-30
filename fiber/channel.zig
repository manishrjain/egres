//! Bounded MPMC channel of `T`. Built directly on `Spinlock` +
//! `WaitQueue` + the runtime's `park`/`wake` — no Mutex/Condition
//! layering. One short spinlock-protected critical section per op,
//! waiters park directly on a per-channel `senders` or `receivers`
//! queue.
//!
//! API matches zio's `Channel(T)` for migration:
//!   - `init(buffer)` / `initAlloc(allocator, capacity)`
//!   - `send` / `trySend` / `receive` / `tryReceive` / `close`
//!   - errors: `ChannelFull`, `ChannelEmpty`, `ChannelClosed`
//!   - `CloseMode.graceful` lets receivers drain;
//!     `CloseMode.immediate` clears the buffer.
//!
//! Cross-thread correctness: the spinlock serializes the composite
//! `{len, head, tail, closed, queue}` update; the fiber's
//! awaken-bit handles the "wake fires while we're between
//! sl.unlock and park" race; wake is always called outside the
//! spinlock so `scheduleLocal`'s global-queue spinlock isn't
//! nested under ours.

const std = @import("std");
const fb = @import("fiber.zig");
const sync = @import("sync.zig");
const rt = @import("runtime.zig");

const Fiber = fb.Fiber;
const Spinlock = rt.Spinlock;
const WaitQueue = sync.WaitQueue;

pub const CloseMode = enum {
    /// Stop accepting new sends but let receivers drain buffered
    /// values. Pending receivers see `error.ChannelClosed` only
    /// after the buffer is empty.
    graceful,
    /// Stop everything immediately. Clear the buffer; pending
    /// receivers see `error.ChannelClosed` on their next call.
    immediate,
};

pub fn Channel(comptime T: type) type {
    return struct {
        const Self = @This();

        /// `null` if buffer is caller-provided via `init`. Set to
        /// the allocator if buffer was heap-allocated via
        /// `initAlloc`; controls `deinit` cleanup.
        allocator: ?std.mem.Allocator,
        buf: []T,
        head: usize = 0,
        tail: usize = 0,
        len: usize = 0,
        closed: bool = false,

        sl: Spinlock = .{},
        /// Fibers parked because the buffer is full. Woken FIFO
        /// when a `receive` frees a slot or a `close` fires.
        senders: WaitQueue = .{},
        /// Fibers parked because the buffer is empty. Woken FIFO
        /// when a `send` deposits an item or a `close` fires.
        receivers: WaitQueue = .{},

        pub fn init(buffer: []T) Self {
            std.debug.assert(buffer.len > 0);
            return .{ .allocator = null, .buf = buffer };
        }

        pub fn initAlloc(allocator: std.mem.Allocator, capacity: usize) !Self {
            std.debug.assert(capacity > 0);
            const buf = try allocator.alloc(T, capacity);
            return .{ .allocator = allocator, .buf = buf };
        }

        pub fn deinit(self: *Self) void {
            if (self.allocator) |a| a.free(self.buf);
        }

        /// Block until space is available, then enqueue. Returns
        /// `error.ChannelClosed` if the channel is closed before
        /// the item can be enqueued. Spurious wake-ups (other
        /// senders raced in and re-filled the buffer) re-park
        /// transparently via the outer loop.
        pub fn send(self: *Self, item: T) error{ChannelClosed}!void {
            const me = rt.currentFiber();
            while (true) {
                self.sl.lock();
                if (self.closed) {
                    self.sl.unlock();
                    return error.ChannelClosed;
                }
                if (self.len < self.buf.len) {
                    self.buf[self.tail] = item;
                    self.tail = (self.tail + 1) % self.buf.len;
                    self.len += 1;
                    const waiter = self.receivers.popHead();
                    self.sl.unlock();
                    if (waiter) |w| rt.wake(w);
                    return;
                }
                self.senders.pushTail(me);
                self.sl.unlock();
                rt.park();
                // Loop: re-acquire sl and re-check state. Could be a
                // genuine slot, a close, or a spurious wake.
            }
        }

        /// Non-blocking send. `error.ChannelFull` if buffer is at
        /// capacity; `error.ChannelClosed` if the channel is
        /// closed. Common pattern: `ch.trySend(x) catch {}` to emit
        /// a wake-up without queuing if the recipient is already
        /// running.
        pub fn trySend(self: *Self, item: T) error{ ChannelFull, ChannelClosed }!void {
            self.sl.lock();
            if (self.closed) {
                self.sl.unlock();
                return error.ChannelClosed;
            }
            if (self.len == self.buf.len) {
                self.sl.unlock();
                return error.ChannelFull;
            }
            self.buf[self.tail] = item;
            self.tail = (self.tail + 1) % self.buf.len;
            self.len += 1;
            const waiter = self.receivers.popHead();
            self.sl.unlock();
            if (waiter) |w| rt.wake(w);
        }

        /// Block until an item is available, then dequeue. Returns
        /// `error.ChannelClosed` if the channel is closed and the
        /// buffer is empty (graceful close drains first; immediate
        /// close fails immediately).
        pub fn receive(self: *Self) error{ChannelClosed}!T {
            const me = rt.currentFiber();
            while (true) {
                self.sl.lock();
                if (self.len > 0) {
                    const item = self.buf[self.head];
                    self.head = (self.head + 1) % self.buf.len;
                    self.len -= 1;
                    const waiter = self.senders.popHead();
                    self.sl.unlock();
                    if (waiter) |w| rt.wake(w);
                    return item;
                }
                if (self.closed) {
                    self.sl.unlock();
                    return error.ChannelClosed;
                }
                self.receivers.pushTail(me);
                self.sl.unlock();
                rt.park();
            }
        }

        /// Non-blocking receive. `error.ChannelEmpty` if buffer is
        /// empty and channel is open; `error.ChannelClosed` if
        /// buffer is empty and channel is closed.
        pub fn tryReceive(self: *Self) error{ ChannelEmpty, ChannelClosed }!T {
            self.sl.lock();
            if (self.len == 0) {
                const err: error{ ChannelEmpty, ChannelClosed } =
                    if (self.closed) error.ChannelClosed else error.ChannelEmpty;
                self.sl.unlock();
                return err;
            }
            const item = self.buf[self.head];
            self.head = (self.head + 1) % self.buf.len;
            self.len -= 1;
            const waiter = self.senders.popHead();
            self.sl.unlock();
            if (waiter) |w| rt.wake(w);
            return item;
        }

        /// Close the channel. After this, `send`/`trySend` always
        /// fail with `ChannelClosed`; `receive`/`tryReceive` succeed
        /// while items remain (graceful) or fail immediately
        /// (immediate). All blocked senders and receivers are
        /// woken and re-check state on their next loop iteration.
        pub fn close(self: *Self, mode: CloseMode) void {
            self.sl.lock();
            self.closed = true;
            if (mode == .immediate) {
                self.len = 0;
                self.head = 0;
                self.tail = 0;
            }
            // Collect all waiters under sl into a transient list
            // (using `waker_next` as the link, same trick as
            // `Condition.broadcast`). Wake outside sl.
            var to_wake: ?*Fiber = null;
            while (self.senders.popHead()) |f| {
                f.waker_next = to_wake;
                to_wake = f;
            }
            while (self.receivers.popHead()) |f| {
                f.waker_next = to_wake;
                to_wake = f;
            }
            self.sl.unlock();

            while (to_wake) |f| {
                const next = f.waker_next;
                f.waker_next = null;
                rt.wake(f);
                to_wake = next;
            }
        }
    };
}
