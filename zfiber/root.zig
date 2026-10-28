// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

//! Module root. Re-exports every public symbol from sibling files so
//! consumers see one flat namespace (`fiber.Fiber`, `fiber.Runtime`,
//! `fiber.spawn`, …) regardless of which file the symbol lives in.

// The libxev backend the runtime drives (loops, async/eventfd wakes, timers).
// Re-exported so tools/benches can use the same primitives the scheduler does.
pub const xev = @import("xev");

// From fiber.zig — the Fiber primitive and its types.
const _fb = @import("fiber.zig");

pub const coro = _fb.coro;
pub const default_stack_size = _fb.default_stack_size;
pub const Priority = _fb.Priority;
pub const Lane = _fb.Lane;
pub const Tag = _fb.Tag;
pub const State = _fb.State;
pub const YieldIntent = _fb.YieldIntent;
pub const SpawnOptions = _fb.SpawnOptions;
pub const FiberFn = _fb.FiberFn;
pub const Fiber = _fb.Fiber;
pub const JoinState = _fb.JoinState;
pub const JoinHandle = _fb.JoinHandle;
pub const ReturnOf = _fb.ReturnOf;

// From runtime.zig — the scheduler.
const _rt = @import("runtime.zig");
const _ctx = @import("ctx.zig");

pub const Worker = _rt.Worker;
pub const Runtime = _rt.Runtime;
pub const Deadlines = _rt.Deadlines;
pub const Timeout = _rt.Timeout;
pub const Stats = _rt.Stats;
pub const spawn = _rt.spawn;
pub const spawnDetached = _rt.spawnDetached;
pub const yield = _rt.yield;
pub const Spinlock = _rt.Spinlock;
pub const park = _rt.park;
pub const wake = _rt.wake;
pub const stop = _rt.stop;
pub const runtime = _rt.runtime;
pub const currentWorker = _rt.currentWorker;
pub const currentWorkerOrNull = _rt.currentWorkerOrNull;
pub const currentFiber = _rt.currentFiber;
pub const currentFiberOrNull = _rt.currentFiberOrNull;
pub const dumpFibers = _rt.dumpFibers;
pub const dumpWorkerCpu = _rt.dumpWorkerCpu;
pub const profile = @import("profile.zig");
pub const Ctx = _ctx.Ctx;
pub const IoClass = _ctx.IoClass;
pub const CachePolicy = _ctx.CachePolicy;
pub const ctx = _ctx.current;
pub const writeTraces = _ctx.writeTraces;
pub const tscNsPerCycleQ32 = _rt.tscNsPerCycleQ32;
pub const ticksPerSecond = _rt.ticksPerSecond;

// Metering: the tenant a statement's cpu, io and memory are charged
// to, with its resources and the policy (`Tenant.acquire`,
// `Tenant.charge`) over them.
pub const meter = @import("meter.zig");
pub const Bucket = meter.Bucket;
pub const Resource = meter.Resource;
pub const Kind = meter.Kind;
pub const Tenant = meter.Tenant;
pub const Tenants = meter.Tenants;

// Lower-level data structure used by `Deadlines`.
pub const heap = @import("heap.zig");

// Per-worker ready queue + its lock-mode constructor. Exported so benches can
// measure `LocalQueue(.none)` vs `LocalQueue(.withlock)` (the runtime uses
// `.none`); the scheduler reaches `queue.zig` directly.
const _queue = @import("queue.zig");
pub const LocalQueue = _queue.LocalQueue;
pub const QueueLockMode = _queue.LockMode;

// Sync primitives built on `park` / `wake`. `Mutex`/`RWMutex` are
// type constructors: `Mutex(.tracked)` records its holder for `dumpLocks`,
// `Mutex(.untracked)` is the zero-overhead variant.
const _sync = @import("sync.zig");
pub const WaitQueue = _sync.WaitQueue;
pub const Track = _sync.Track;
pub const Mutex = _sync.Mutex;
pub const Condition = _sync.Condition;
pub const RWMutex = _sync.RWMutex;
pub const Notify = _sync.Notify;
pub const Event = _sync.Event;

// Mutex-protected containers for state shared between fibers.
pub const SafeHashMap = @import("safe_hash_map.zig").SafeHashMap;
pub const SafeArray = @import("safe_array.zig").SafeArray;

// Lock-holder tracking for the tracked Mutex/RWMutex modes — `dumpLocks`
// answers "which locks are held, by what code?" on a wedged server.
const _lock_registry = @import("lock_registry.zig");
pub const dumpLocks = _lock_registry.dumpLocks;
pub const trackedLockCount = _lock_registry.trackedLockCount;
pub const checkLockRegistryForTests = _lock_registry.checkLockRegistryForTests;

const _channel = @import("channel.zig");
pub const Channel = _channel.Channel;
pub const CloseMode = _channel.CloseMode;

const _group = @import("group.zig");
pub const Group = _group.Group;

const _watermark = @import("watermark.zig");
pub const Watermark = _watermark.Watermark;

// Two-stage shutdown signal (drain, then stop): pollable (`isStopping`),
// blockable (`waitForStart`), with optional SIGINT/SIGTERM wiring.
pub const Shutdown = @import("shutdown.zig").Shutdown;

// The `BENCH:` test harness: keeps its handle on the current fiber.
pub const bench = @import("bench.zig");

pub const SemaphoreMap = @import("semaphore_map.zig").SemaphoreMap;

// `sleepMs` lives in deadlines.zig — it parks on the per-worker Deadlines array
// and PANICS on a spurious wake (resumed before its own deadline fired).
pub const sleepMs = @import("deadlines.zig").sleepMs;

// From fs.zig — file I/O that cooperatively parks (zio.File/Dir-compatible).
pub const fs = @import("fs.zig");
pub const File = fs.File;
pub const Dir = fs.Dir;
pub const DirEntry = fs.Entry;
pub const freeDirEntries = fs.freeEntries;

// From fsops.zig — blocking-op offload pool (fsync + fs metadata) that parks
// the calling fiber and wakes it when a pool thread finishes the syscall.
pub const fsops = @import("fsops.zig");

// From net.zig — TCP networking with zio.net-compatible Stream/Server/
// IpAddress and the std.Io.Reader/Writer socket adapters.
pub const net = @import("net.zig");

/// The parked-worker bitmap, exported for its tests.
pub const IdleWorkers = @import("queue.zig").IdleWorkers;
