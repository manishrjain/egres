---
title: 100x Faster Multi-Index Lookups
description: Rows keyed by a 64-bit egid, every index a table of roaring bitmaps, and batched execution.
sidebar:
  label: Indexes
  order: 5
---

*Status: available in the current release.*

Every row in an Egres table is keyed by a monotonically increasing 64-bit
integer id per table, called egid. In zdb, the rows are stored with keys
generated from these egids (big-endian order, 8-bytes per id) and map to the row
tuple. Each new commit for the same row adds a value version under the same key,
so egid assigned to a row never changes.

The user defined PRIMARY KEY is a unique secondary index from the encoded key to
the egid. This means a primary-key point lookup would read via 2 hops, first
from primary key to egid, then egid to row. In exchange, every index has the
same shape, index key -> egid(s), which are 64-bit ints and can be efficiently
encoded. Each index is a table in zdb with its own set of LSM trees.

NULL has an index key too, sorted after every value. A non-unique index keeps
all NULL rows under one key; a unique index gives each NULL row a key of its
own (the NULL tag followed by the egid), since NULLs never collide. So every
index holds every row, and walking an index in key order is `ORDER BY` on its
column.

After upgrading, drop and recreate unique indexes on nullable columns that
were built without NULL entries before querying their tables. Existing data
directories are not automatically migrated.

Egres exposes egid SQLite style. `SELECT egid, col1, col2 FROM table`. `...
WHERE egid = N` would avoid the index key -> rowid lookup, though that's an
optimization. In the [benchmarks](../benchmarks.md), point reads via *select*
queries, which use the primary index, Egres still came out ahead at p50: 0.18 ms
Egres vs 0.52 ms Postgres. The primary key to egid is a seek into SSTable blocks
that are already cached, and most likely to stay cached because of the frequency
of such lookups.

## Roaring Bitmaps Integration

[zroar][] is a Zig implementation of [roaring bitmaps][roar] that stores
containers in one serialized byte array, instead of an adaptive radix tree with
an allocation per container used by others. Its in-memory form is the same as
its on-disk form, so it avoids the need for a deserialization step to run bitmap
operations. Against [CRoaring][], it is 2x to 9x faster.

zdb understands that the values in an index table are 64-bit integers. A write
that touches an index produces a delta under the index key: plus row ids, minus
row ids. zdb stores these deltas as versions for the index key. As
compactions run, zdb folds these deltas into roaring bitmaps using zroar,
fetching the previous fold from lower levels if needed.

On index key lookup `getBitmap(index_key, read_ts)`, zdb fetches only the
versions (`<= read_ts`) up until the first fold it finds, loads the bitmap,
applies the delta versions in order and returns a zroar bitmap valid as of the
read timestamp.

[roar]: https://roaringbitmap.org/
[zroar]: https://github.com/manishrjain/zroar
[CRoaring]: https://github.com/roaringBitmap/CRoaring

<figure class="fig">
  <img src="../figures/index-bitmaps.svg" width="1000" height="424" alt="An index key's versions, newest first: writes add deltas of row ids under the key, a read walks down only to the first zroar state and applies the deltas on top, and compaction folds the deltas into one new state, so a later read is one seek and one bitmap" loading="lazy" />
</figure>

This native support for indexes results in efficient index operations. What
would be a bitmap index scan in Postgres (`WHERE col1 = x`) is a single index
key lookup returning pre-generated bitmaps in Egres. Where this shows up most is
a query across two indexes with no composite index: `WHERE col1 = x AND col2 =
y`. Two seeks, one intersection resulting in row ids.

AND, OR and NOT across indexes become bitmap operations: the planner, admittedly
simplistic as of this release, turns the index conditions into a tree of bitmap
nodes. Each leaf is an index read yielding a zroar, AND nodes intersect and stop
at the first empty set, OR nodes union, and the bitmap at the root picks the
rows to fetch.

In the [benchmarks](../benchmarks.md), this is what makes the *sku_day* query
126x faster at p95: 490 ms in Postgres against 3.9 ms in Egres. On
*customer_day_count*, the other two-index query, Postgres p95 took 15.1 ms
against Egres 2 ms.

## Batched Execution

Postgres pulls one row at a time through its tree of operators. Every row pays a
function call per operator and the CPU never sees enough work at once to
pipeline any of it. Egres does **batched execution** instead: the executor moves
a batch of rows through the operators, so the operator overhead is paid once per
batch rather than per row, and the rows an operator touches are already in
cache. To avoid copies, each operator updates a bit set marking which rows are
still selected.
