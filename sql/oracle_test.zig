// Copyright 2026 Manish R Jain
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const testing = std.testing;
const sql = @import("sql");
const String = sql.value.String;
const zdb = @import("zdb");

const Oracle = sql.oracle.Oracle;

// Test keys share one table of namespace `ns` unless a test names
// another; `h(p)` is the identity hash that `updateItems` records for
// a commit of `p`.
const ns = 7;

fn keyIn(namespace: u64, primary: String) zdb.kv.Key {
    return .{ .namespace = namespace, .table_id = 16, .primary = primary };
}

fn key(primary: String) zdb.kv.Key {
    return keyIn(ns, primary);
}

/// Record a commit of `primary` at `ts` in `namespace`, the way apply
/// does — a one-item, one-version run through updateItems.
fn commitIn(o: *Oracle, namespace: u64, primary: String, ts: u64) !void {
    try o.updateItems(namespace, &.{.{
        .key = keyIn(namespace, primary),
        .versions = .{ .one = .{ .ts = ts, .meta = .{}, .value = "" } },
    }});
}

fn commit(o: *Oracle, primary: String, ts: u64) !void {
    try commitIn(o, ns, primary, ts);
}

fn h(primary: String) u64 {
    return key(primary).identityHash();
}

test "oracle: unknown keys answer the horizon; updates answer exactly" {
    var o = try Oracle.init(testing.allocator, 1024);
    defer o.deinit();

    // Empty oracle: nothing ever committed — horizon 0 for any key.
    try testing.expectEqual(0, o.commitTs(ns, h("a")));

    try commit(&o, "a", 100);
    try testing.expectEqual(100, o.commitTs(ns, h("a")));
    try testing.expectEqual(0, o.commitTs(ns, h("b")));

    // Per-key max-fold: an older ts never regresses a newer one.
    try commit(&o, "a", 90);
    try testing.expectEqual(100, o.commitTs(ns, h("a")));
    try commit(&o, "a", 150);
    try testing.expectEqual(150, o.commitTs(ns, h("a")));
}

test "oracle: a tick within the period is skipped" {
    var o = try Oracle.init(testing.allocator, 1024);
    defer o.deinit();

    // init stamps the clock, so a period-long tick is too soon and
    // the entry stays in `current`; a forced rotation moves it.
    try commit(&o, "a", 100);
    o.rotate(std.math.maxInt(u64));
    try testing.expectEqual(1, o.cur_gen.map.count());
    o.rotate(0);
    try testing.expectEqual(0, o.cur_gen.map.count());
    try testing.expectEqual(1, o.prev_gen.map.count());
}

test "oracle: entries survive one rotation and die at the second" {
    var o = try Oracle.init(testing.allocator, 1024);
    defer o.deinit();

    try commit(&o, "a", 100);
    o.rotate(0); // h("a") now in `previous`
    try testing.expectEqual(100, o.commitTs(ns, h("a")));

    o.rotate(0); // dropped; the horizon absorbs its max
    try testing.expectEqual(100, o.horizonTs(ns));
    // Absent keys — h("a") included — answer the horizon now:
    // conservative, but the old commit can never hide.
    try testing.expectEqual(100, o.commitTs(ns, h("a")));
    try testing.expectEqual(100, o.commitTs(ns, h("b")));
}

test "oracle: keysFor fills the slots and takes no more" {
    var map: std.AutoHashMapUnmanaged(u64, u64) = .empty;
    defer map.deinit(testing.allocator);
    const keys = Oracle.keysFor(1024);
    try map.ensureTotalCapacity(testing.allocator, keys);
    try testing.expectEqual(1024, map.capacity());
    try map.ensureTotalCapacity(testing.allocator, keys + 1);
    try testing.expectEqual(2048, map.capacity());

    // The default is sized the same way, for 16M slots.
    try testing.expectEqual(13_421_772, Oracle.default_max_keys);
}

test "oracle: a full generation rotates by itself, maps keep capacity" {
    // Three keys per generation. The fourth commit ends the first
    // generation, the seventh the second; nothing is ever freed.
    var o = try Oracle.init(testing.allocator, 3);
    defer o.deinit();
    const cap = o.cur_gen.map.capacity();
    try testing.expect(cap >= 3);

    try commit(&o, "a", 10);
    try commit(&o, "b", 11);
    try commit(&o, "c", 12);
    try commit(&o, "d", 13); // rotation: a, b, c in previous
    try testing.expectEqual(1, o.cur_gen.map.count());
    try testing.expectEqual(3, o.prev_gen.map.count());
    try testing.expectEqual(10, o.commitTs(ns, h("a")));
    try testing.expectEqual(0, o.horizonTs(ns));

    try commit(&o, "e", 14);
    try commit(&o, "f", 15);
    try commit(&o, "g", 16); // rotation: a, b, c retired into the horizon
    try testing.expectEqual(12, o.horizonTs(ns));
    try testing.expectEqual(12, o.commitTs(ns, h("a")));
    try testing.expectEqual(13, o.commitTs(ns, h("d")));
    try testing.expectEqual(16, o.commitTs(ns, h("g")));
    try testing.expectEqual(cap, o.cur_gen.map.capacity());
    try testing.expectEqual(cap, o.prev_gen.map.capacity());
}

test "oracle: a re-committed key reads from current, not previous" {
    var o = try Oracle.init(testing.allocator, 1024);
    defer o.deinit();

    try commit(&o, "a", 100);
    o.rotate(0);
    try commit(&o, "a", 150); // lives in both generations now
    try testing.expectEqual(150, o.commitTs(ns, h("a")));

    o.rotate(0); // drops the generation holding the 100 copy
    try testing.expectEqual(100, o.horizonTs(ns));
    try testing.expectEqual(150, o.commitTs(ns, h("a")));
    o.rotate(0); // drops the 150 copy too
    try testing.expectEqual(150, o.horizonTs(ns));
}

test "oracle: maxCommitTs over a read set" {
    var o = try Oracle.init(testing.allocator, 1024);
    defer o.deinit();

    // One batched apply records the whole write set under a single hold.
    try o.updateItems(ns, &.{
        .{ .key = key("k1"), .versions = .{
            .one = .{ .ts = 100, .meta = .{}, .value = "" },
        } },
        .{ .key = key("k2"), .versions = .{
            .one = .{ .ts = 300, .meta = .{}, .value = "" },
        } },
        .{ .key = key("k3"), .versions = .{
            .one = .{ .ts = 200, .meta = .{}, .value = "" },
        } },
    });

    try testing.expectEqual(0, o.maxCommitTs(ns, &.{}));
    try testing.expectEqual(
        300,
        o.maxCommitTs(ns, &.{ h("k1"), h("k2"), h("k3") }),
    );
    try testing.expectEqual(200, o.maxCommitTs(ns, &.{ h("k1"), h("k3") }));
    // An absent key contributes the horizon (0 here).
    try testing.expectEqual(100, o.maxCommitTs(ns, &.{ h("k1"), h("k99") }));

    // Once every key has aged out, the horizon speaks for them all.
    o.rotate(0);
    o.rotate(0);
    try testing.expectEqual(300, o.horizonTs(ns));
    try testing.expectEqual(300, o.maxCommitTs(ns, &.{ h("k1"), h("k99") }));
}

test "oracle: a namespace's horizon follows its own commits only" {
    // Two keys per generation; namespace 8 writes fast enough to end
    // every generation, namespace 7 commits once and goes quiet.
    var o = try Oracle.init(testing.allocator, 2);
    defer o.deinit();
    const loud = 8;

    try commit(&o, "a", 100);
    try commitIn(&o, loud, "x1", 500);
    try commitIn(&o, loud, "x2", 600); // rotation: a, x1 in previous
    try testing.expectEqual(100, o.commitTs(ns, h("a")));
    try commitIn(&o, loud, "x3", 700);
    try commitIn(&o, loud, "x4", 800); // rotation: a, x1 retired

    // The quiet namespace's key is gone from both maps, and what it
    // answers is its own last commit, not the loud namespace's 500.
    try testing.expectEqual(100, o.horizonTs(ns));
    try testing.expectEqual(100, o.commitTs(ns, h("a")));
    try testing.expectEqual(100, o.commitTs(ns, h("never written")));
    try testing.expectEqual(500, o.horizonTs(loud));
    // A namespace that never committed still answers 0.
    try testing.expectEqual(0, o.horizonTs(9));
    try testing.expectEqual(0, o.commitTs(9, keyIn(9, "a").identityHash()));

    // More loud generations move the loud horizon and leave the quiet
    // one where its own commits put it.
    try commitIn(&o, loud, "x5", 900);
    try commitIn(&o, loud, "x6", 1000); // rotation: x2, x3 retired
    try testing.expectEqual(100, o.horizonTs(ns));
    try testing.expectEqual(700, o.horizonTs(loud));
}

test "oracle: one run that spans a rotation keeps its namespace's max" {
    // Three keys per generation and a five-key run: the first three
    // fill a generation, the rotation inside the run must still credit
    // them to the namespace, or their retirement would leave the
    // horizon below a commit that is no longer in either map.
    var o = try Oracle.init(testing.allocator, 3);
    defer o.deinit();

    var items: [5]zdb.kv.Item = undefined;
    const names = [_]String{ "a", "b", "c", "d", "e" };
    for (&items, names, 0..) |*item, name, i| item.* = .{
        .key = key(name),
        .versions = .{
            .one = .{ .ts = 10 + i, .meta = .{}, .value = "" },
        },
    };
    try o.updateItems(ns, &items); // a, b, c | rotation | d, e
    try testing.expectEqual(3, o.prev_gen.map.count());
    try testing.expectEqual(2, o.cur_gen.map.count());

    o.rotate(0); // a, b, c retired
    try testing.expectEqual(12, o.horizonTs(ns));
    try testing.expectEqual(12, o.commitTs(ns, h("a")));
    try testing.expectEqual(14, o.commitTs(ns, h("e")));
}
