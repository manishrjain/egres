// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const utils = @import("utils");

pub const Error = error{
    InvalidNumCompactors,
    InvalidCompactionFibers,
    InvalidTableSize,
    InvalidBlockSize,
    InvalidMaxLevels,
    InvalidL0MergeMinTables,
    InvalidL0BackpressureStartCount,
};

pub const Config = struct {
    read_only: bool = false,

    /// The size a table is aimed at, in memory and on disk: a memtable
    /// freezes once it holds this many bytes of writes, and every SST
    /// at every level is cut at it. One size, so a job at any level
    /// rewrites a file's share of the level below plus one file of
    /// edge, and an L0 push has as many L1 boundaries to partition on
    /// as L1 has files.
    table_bytes: usize = 64 * 1024 * 1024,

    // When the leader flushes. A flush merges every immutable memtable
    // waiting at that moment into one L0 table per tree (RocksDB's
    // `min_write_buffer_number_to_merge`): fewer and larger L0 tables, and
    // a key rewritten across those memtables reaches disk once. The flush
    // loop fires when `flush_min_memtables` immutables have accumulated,
    // or when `flush_max_age_seconds` have passed since the last flush
    // and at least one is waiting, whichever comes first. 1 and 0 give
    // the old behaviour: flush each memtable as soon as it is frozen.
    flush_min_memtables: u32 = 4,
    flush_max_age_seconds: u32 = 30,

    block_bytes: usize = 16 * 1024,
    num_compactors: u8 = 4,
    /// Fibers one merge into the level beneath splits into, along that
    /// level's table boundaries (compaction.partitions), each merging
    /// and writing its own key range. A merge on one fiber was up to
    /// 36 s of merge and codec work on one core. Bounded by
    /// `compaction.max_compaction_fibers`.
    compaction_fibers: u8 = 3,
    /// Bytes a compaction reads from each input table per request: a
    /// window of whole blocks ahead of the merge, so a table is read
    /// in a few large requests rather than one per block, on a file
    /// and from S3 alike. Each partition fiber holds one window per
    /// input table, so a job's read memory is about fibers times
    /// inputs times this. 0 reads block by block.
    compaction_window_bytes: usize = 4 << 20,

    // Level sizes. `lmax_target_bytes` is the bottom level's target; upper
    // levels cascade by dividing by `level_size_multiplier`. There is no
    // separate base-level-size knob. A tree splits once it holds five
    // fourths of the Lmax target in total (`splitBytes`): the upper
    // levels add about a quarter, so that is a tree whose Lmax is full.
    level_size_multiplier: i64 = 5,
    max_levels: u8 = 4,

    // The bottom level's (Lmax's) byte target, and so the per-tree size:
    // Lmax holds about 80% of a tree's data. After each compaction the
    // tree's total size is compared against `splitBytes`, and past it
    // the tree splits at its median key into two trees.
    lmax_target_bytes: i64 = 10 * 1024 * 1024 * 1024, // 10 GiB

    // Write backpressure (RocksDB-style write slowdown), one curve for
    // two brakes. A mutation sleeps `excess * backpressure_step_ms`
    // milliseconds before proposing, capped at `backpressure_max_ms`,
    // where excess is how far a count is above its start count; the
    // larger of the two sleeps applies. This is a *trigger*, not a
    // cap: the count may still grow past it, writes just slow down,
    // until the memtable stall (Engine.memtableStallCount) holds them.
    // With defaults, at the trigger 0 ms; +5 over it 100 ms; +25 hits
    // the cap of 500 ms. The values come from the memtable brake, set
    // against an observed runaway (below); the L0 brake ran 1 ms per
    // table before the merge, too gentle to matter.
    backpressure_step_ms: u32 = 20,
    backpressure_max_ms: u32 = 500,

    // L0 brake: a tree's L0 table count. Gives flush + compaction a
    // chance to drain L0 instead of letting it grow into 10s-of-GiB
    // monolithic compaction targets. There is no off switch: a start
    // count of 0 is invalid, since the L0 score divides by it. The
    // start count sits well above what L0 holds by design: a few
    // finished tables plus up to three per size tier, some 20 at
    // worst for a tree fed by very small flushes.
    l0_backpressure_start_count: u32 = 32,

    // Memtable brake: the frozen memtables waiting for a flush. Left
    // unbounded they reached 389 (30 GiB) in six minutes of a 160k
    // rows/s load, and every unique-key probe reads each of them,
    // which starved the flush of CPU in turn.
    memtable_backpressure_start_count: u32 = 8,

    // Input bytes one background job takes, whether a flush of frozen
    // memtables (their in-memory size, oldest first) or an L0→L1 push
    // (L0 file sizes plus their cumulative L1 overlap, oldest first).
    // At least one memtable or table is always taken, so an oversized
    // input still makes progress. A push starts once L0 reaches L1's
    // target, well under this, so the bound binds only when the job
    // has fallen behind: then it keeps one job from taking GiBs and
    // blocking its loop for minutes. L0→L0 merges have their own rule
    // and ignore it.
    job_max_bytes: u64 = 1 << 30, // 1 GiB

    // L0 is drained in two steps (RocksDB's intra-L0 compaction, made the
    // preferred job). Small L0 tables are first merged among themselves:
    // an L0→L0 job takes the tables after the newest finished one, in
    // order, up to twice the L0 file size (the memtable size, 64 MiB),
    // and writes ONE output table (see `l0MergeRun`). A run needs
    // `l0_merge_min_tables` tables; with more it runs ahead of the push
    // and the levels below. A table that has reached the
    // L0 file size is finished: it is never merged again and waits for
    // the push to L1. So a byte is rewritten within L0 only until it
    // sits in a finished table; an unfinished output is merged again
    // with the flushes after it until it is one. L1 is not touched, so
    // the job costs what its inputs weigh, where an L0→L1 job over the
    // same tables rewrites every L1 table they overlap. 0 disables L0→L0.
    //
    // An L0→L1 job is due once L0 holds as many bytes as L1's target size
    // (`levelTarget`; 410 MiB when Lmax targets 10 GiB), and then
    // competes with the levels below on score. An L0→L1 job rewrites
    // about all of L1, since L0 tables span the tree's range, so it costs
    // (L0 + L1) / L0 bytes written per byte drained: about 2 when L0 has
    // grown to L1's size, 10 and more when a few small tables are pushed.
    // There is no table-count rule. The count bounds itself: fewer than
    // `l0_merge_min_tables` unfinished tables sit after the newest
    // finished one, and the finished ones number at most L1's target
    // over the L0 file size.
    l0_merge_min_tables: u32 = 4,

    // Roaring posting rollup (zdb/emitter.zig's VersionTracker).
    // Compaction folds a
    // version run whose newest item is a roaring delta into one full
    // bitmap state, so reads stop at their first hit instead of
    // walking a delta chain. Only a compaction into L1 or deeper: an
    // L0 to L0 merge leaves chains as they are.
    //
    // A chain whose base state is among the compaction's own inputs
    // rolls up for free (the merge already streams every input byte
    // through a rebuild), gated only by `rollup_min_deltas` — a short
    // chain isn't worth rewriting a large bitmap for; reads fold it
    // on the fly.
    //
    // A chain WITHOUT its base must fetch it from the levels below
    // the destination — the one rollup path that costs extra read
    // I/O. Two triggers allow it, either one suffices:
    //   dest >= rollup_at_level   compacting into this level or
    //     deeper always regenerates, so the deep levels converge on
    //     one state per posting (default 2: L0→L1 stays free-only,
    //     L1→L2 and below regenerate).
    //   chain >= rollup_max_deltas   a chain this long regenerates at
    //     ANY level, bounding read-path fold depth and amortizing the
    //     deep seek to one per max_deltas writes to that index key.
    // Chain length is also what bounds rollup buffering — no
    // separate byte caps.
    rollup_enabled: bool = true,
    rollup_min_deltas: u32 = 1,
    rollup_at_level: u8 = 2,
    rollup_max_deltas: u32 = 32,

    /// `--zdb`: every field is a flag; the defaults above are what
    /// `--help` prints. Read once when the forest opens.
    pub const docs = .{
        .self = "LSM forest storage: memtable and flush policy, level" ++
            " shape, compaction and backpressure. SSTs live in the" ++
            " --store. Live: an update reaches the next flush," ++
            " compaction job or check.",
        .read_only = "Open the forest without writing to it.",
        .table_bytes = "Bytes a memtable holds before it is frozen and" ++
            " flushed, and the size every SST is cut at. Small values" ++
            " force frequent flushes, useful for tests.",
        .flush_min_memtables = "Flush once this many frozen memtables are" ++
            " waiting; one flush merges all of them into one L0 table per" ++
            " tree. 1 flushes each memtable as soon as it is frozen.",
        .flush_max_age_seconds = "Flush whatever is waiting once this long" ++
            " has passed since the last flush.",
        .block_bytes = "Target size of one SST data block, the unit of" ++
            " reads and of the block cache.",
        .num_compactors = "Compaction worker fibers. Must not be 1.",
        .compaction_fibers = "Fibers a compaction into the level below" ++
            " splits into along that level's table boundaries, 1 to 16.",
        .compaction_window_bytes = "Bytes a compaction reads from each" ++
            " input table per request, whole blocks. 0 reads block by" ++
            " block.",
        .level_size_multiplier = "Each level's byte target is the next" ++
            " deeper level's divided by this.",
        .max_levels = "Levels per tree, L0 included. At least 2.",
        .lmax_target_bytes = "Byte target of a tree's bottom level. The" ++
            " levels above are a fifth of the one below. A tree splits" ++
            " at its median key into two once it holds five fourths of" ++
            " this in total.",
        .backpressure_step_ms = "Milliseconds a write sleeps per L0" ++
            " table or frozen memtable above its start count.",
        .backpressure_max_ms = "Longest backpressure sleep for one" ++
            " write, in milliseconds.",
        .l0_backpressure_start_count = "L0 table count above which writes" ++
            " to the tree are slowed. Must be above 0.",
        .memtable_backpressure_start_count = "Frozen memtable count above" ++
            " which writes are slowed.",
        .job_max_bytes = "Input bytes one flush or L0 to L1 compaction" ++
            " takes, oldest first; at least one input is always taken.",
        .l0_merge_min_tables = "An L0 to L0 compaction merges the tables" ++
            " after the newest one that reached the table size, up to" ++
            " twice that size, into one table, once there are this many" ++
            " of them. L0 is compacted into L1 once it holds as many bytes" ++
            " as L1's target. 0 turns L0 to L0 compaction off.",
        .rollup_enabled = "Fold a roaring posting's delta chain into one" ++
            " bitmap during compaction.",
        .rollup_min_deltas = "Shortest delta chain worth rolling up when" ++
            " its base is among the compaction's inputs.",
        .rollup_at_level = "Compactions into this level or deeper fetch a" ++
            " missing base from below to roll the chain up.",
        .rollup_max_deltas = "A chain this long is rolled up at any" ++
            " level, fetching its base if needed.",
    };

    /// Read once when the forest opens: the mode, the level count (each
    /// tree's level array is sized by it) and the worker count (the
    /// fibers are spawned then). A live update to one of these is
    /// refused.
    pub const fixed = .{
        "read_only",
        "max_levels",
        "num_compactors",
    };

    /// The total size at which a tree splits: its Lmax target plus the
    /// quarter the levels above add once every level is at target.
    pub fn splitBytes(self: Config) i64 {
        return @divTrunc(self.lmax_target_bytes * 5, 4);
    }

    /// The block codec for a table written into `level`. Not a flag: L0
    /// and L1 are the churn levels (every L0 push rewrites about all of
    /// L1) and hold the hot data, so they take lz4's speed; L2 and below
    /// hold about 97% of a tree's bytes and are rewritten rarely, so
    /// they take zstd's ratio. Each table records its codec in its index,
    /// so readers never consult this.
    pub fn codecFor(self: Config, level: usize) utils.compression.Codec {
        _ = self;
        return if (level >= 2) .zstd else .lz4;
    }

    pub fn validate(self: Config) Error!void {
        // Badger semantics: exactly one compactor causes pathological level
        // behavior.
        if (self.num_compactors == 1) return error.InvalidNumCompactors;
        if (self.compaction_fibers == 0 or self.compaction_fibers > 16)
            return error.InvalidCompactionFibers;
        if (self.table_bytes == 0) return error.InvalidTableSize;
        if (self.block_bytes == 0) return error.InvalidBlockSize;
        // L0 needs an L1 to push into, and only a level below L0 rolls
        // postings up and discards old versions.
        if (self.max_levels < 2) return error.InvalidMaxLevels;
        // A merge of one table is a copy.
        if (self.l0_merge_min_tables == 1) return error.InvalidL0MergeMinTables;
        // The L0 score is a fraction of it.
        if (self.l0_backpressure_start_count == 0)
            return error.InvalidL0BackpressureStartCount;
    }
};
