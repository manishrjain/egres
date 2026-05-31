//! Module root. Re-exports every public symbol from sibling files so
//! consumers see one flat namespace (`fiber.Fiber`, `fiber.Runtime`,
//! `fiber.spawn`, …) regardless of which file the symbol lives in.

// From fiber.zig — the Fiber primitive and its types.
const fb = @import("fiber.zig");

pub const coro = fb.coro;
pub const default_stack_max = fb.default_stack_max;
pub const default_stack_commit = fb.default_stack_commit;
pub const Priority = fb.Priority;
pub const Tag = fb.Tag;
pub const State = fb.State;
pub const YieldIntent = fb.YieldIntent;
pub const SpawnOptions = fb.SpawnOptions;
pub const FiberFn = fb.FiberFn;
pub const Fiber = fb.Fiber;
pub const JoinState = fb.JoinState;
pub const ReturnOf = fb.ReturnOf;

// From runtime.zig — the scheduler.
const rt = @import("runtime.zig");

pub const Worker = rt.Worker;
pub const Runtime = rt.Runtime;
pub const CoarseClock = rt.CoarseClock;
pub const coarseMs = rt.coarseMs;
pub const clockMsUseRarely = @import("coarse_clock.zig").clockMsUseRarely;
pub const ClockKind = @import("coarse_clock.zig").ClockKind;
pub const Deadlines = rt.Deadlines;
pub const Timeout = rt.Timeout;
pub const Stats = rt.Stats;
pub const JoinHandle = rt.JoinHandle;
pub const spawn = rt.spawn;
pub const yield = rt.yield;
pub const park = rt.park;
pub const wake = rt.wake;
pub const stop = rt.stop;
pub const runtime = rt.runtime;
pub const currentWorker = rt.currentWorker;
pub const currentWorkerOrNull = rt.currentWorkerOrNull;
pub const currentFiber = rt.currentFiber;
pub const currentFiberOrNull = rt.currentFiberOrNull;
pub const dumpFibers = rt.dumpFibers;
pub const tscNsPerCycleQ32 = rt.tscNsPerCycleQ32;

// Lower-level data structure used by `Deadlines`.
pub const heap = @import("heap.zig");

// Sync primitives built on `park` / `wake`.
const sync = @import("sync.zig");
pub const WaitQueue = sync.WaitQueue;
pub const Mutex = sync.Mutex;
pub const Condition = sync.Condition;
pub const RWMutex = sync.RWMutex;
pub const Notify = sync.Notify;

const channel_mod = @import("channel.zig");
pub const Channel = channel_mod.Channel;
pub const CloseMode = channel_mod.CloseMode;

const group_mod = @import("group.zig");
pub const Group = group_mod.Group;

const watermark_mod = @import("watermark.zig");
pub const Watermark = watermark_mod.Watermark;

// From io.zig — I/O operations that cooperatively park.
const io = @import("io.zig");

pub const sleep = io.sleep;
pub const Stream = io.Stream;
pub const Listener = io.Listener;
pub const listen = io.listen;
pub const connect = io.connect;
