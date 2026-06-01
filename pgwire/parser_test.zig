//! libpg_query wrapper tests. The parser layer is technically Layer 1
//! (no I/O), but it pulls in the C library so we test it separately.

const std = @import("std");
const testing = std.testing;
const fiber = @import("fiber");
const pgwire = @import("pgwire");

const parser = pgwire.parser_mod;

/// Walk into the first statement's "stmt" wrapper and return its key
/// (the statement-kind name libpg_query emits, e.g. "SelectStmt").
fn firstStmtKind(tree: *const parser.ParseTree) ?[]const u8 {
    const stmts = tree.statements() orelse return null;
    if (stmts.len == 0) return null;
    const first = stmts[0];
    const obj = switch (first) {
        .object => |o| o,
        else => return null,
    };
    const wrapper = obj.get("stmt") orelse return null;
    const wrapper_obj = switch (wrapper) {
        .object => |o| o,
        else => return null,
    };
    var it = wrapper_obj.iterator();
    if (it.next()) |entry| return entry.key_ptr.*;
    return null;
}

test "parse: SELECT 1 → SelectStmt" {
    const a = std.testing.allocator;
    var tree = try parser.parse(a, "SELECT 1", null);
    defer tree.deinit(a);
    try testing.expectEqualStrings("SelectStmt", firstStmtKind(&tree).?);
}

test "parse: SELECT a, b FROM t WHERE c = 1 → 2 target entries" {
    const a = std.testing.allocator;
    var tree = try parser.parse(
        a,
        "SELECT a, b FROM t WHERE c = 1",
        null,
    );
    defer tree.deinit(a);

    const stmts = tree.statements().?;
    const first = stmts[0].object;
    const wrapper = first.get("stmt").?.object;
    const select = wrapper.get("SelectStmt").?.object;
    const targets = select.get("targetList").?.array;
    try testing.expectEqual(@as(usize, 2), targets.items.len);
}

test "parse: CREATE TABLE → CreateStmt" {
    const a = std.testing.allocator;
    var tree = try parser.parse(
        a,
        "CREATE TABLE t (id int primary key, name text)",
        null,
    );
    defer tree.deinit(a);
    try testing.expectEqualStrings("CreateStmt", firstStmtKind(&tree).?);
}

test "parse: INSERT INTO t VALUES (1) → InsertStmt" {
    const a = std.testing.allocator;
    var tree = try parser.parse(
        a,
        "INSERT INTO t VALUES (1)",
        null,
    );
    defer tree.deinit(a);
    try testing.expectEqualStrings("InsertStmt", firstStmtKind(&tree).?);
}

test "parse: syntax error returns SyntaxError + cursor info" {
    const a = std.testing.allocator;
    var info: ?parser.ErrorInfo = null;
    // Unmatched paren — guaranteed syntax error in any Postgres
    // grammar. `FROMM x` would just be parsed as a column reference.
    const result = parser.parse(a, "SELECT 1 FROM (", &info);
    if (result) |*tree| {
        var t = tree.*;
        t.deinit(a);
        try testing.expect(false); // unreachable: must error
    } else |err| {
        try testing.expectEqual(error.SyntaxError, err);
    }
    try testing.expect(info != null);
    var info_unwrap = info.?;
    defer info_unwrap.deinit(a);
    // libpg_query reports cursor positions 1-based; we don't assert
    // an exact number (it's tied to libpg internals) but it must be
    // strictly positive.
    try testing.expect(info_unwrap.cursor_pos > 0);
    try testing.expect(info_unwrap.message.len > 0);
}

test "parse: 1000 concurrent fibers parse distinct queries (no leaks, no races)" {
    // Each fiber N parses `SELECT a, b, c FROM t_N WHERE id = N`, then
    // walks the parse tree to confirm relname == `t_N` and the WHERE
    // clause's integer constant == N. With parallel=16 workers running
    // 1000 fibers, this exercises libpg_query under genuine cross-thread
    // concurrency. If any per-thread libpg state ever leaked across
    // threads, fibers would see each other's parses and either the
    // relname or the int wouldn't match — the failure counter catches it.
    //
    // testing.allocator is GPA with thread_safe = !single_threaded, so
    // the leak check stays valid under concurrent fibers. libpg_query's
    // plain-malloc per-thread state is released on thread exit by its
    // own pthread destructor — no manual shutdown needed.
    const a = std.testing.allocator;
    var failures: std.atomic.Value(u32) = .{ .raw = 0 };

    const Worker = struct {
        fn run(alloc: std.mem.Allocator, n: u32, fail_count: *std.atomic.Value(u32)) void {
            const sql = std.fmt.allocPrint(
                alloc,
                "SELECT a, b, c FROM t_{d} WHERE id = {d}",
                .{ n, n },
            ) catch {
                _ = fail_count.fetchAdd(1, .seq_cst);
                return;
            };
            defer alloc.free(sql);

            var tree = parser.parse(alloc, sql, null) catch {
                _ = fail_count.fetchAdd(1, .seq_cst);
                return;
            };
            defer tree.deinit(alloc);

            if (!verifyParse(&tree, n)) {
                _ = fail_count.fetchAdd(1, .seq_cst);
            }
        }

        fn verifyParse(tree: *const parser.ParseTree, n: u32) bool {
            const stmts = tree.statements() orelse return false;
            if (stmts.len != 1) return false;
            const select = stmts[0].object.get("stmt").?.object.get("SelectStmt").?.object;

            // FROM: relname must be `t_<n>` for THIS fiber's n.
            const from = select.get("fromClause").?.array;
            if (from.items.len != 1) return false;
            const relname = from.items[0].object.get("RangeVar").?.object.get("relname").?.string;
            if (!std.mem.startsWith(u8, relname, "t_")) return false;
            const got_n = std.fmt.parseInt(u32, relname[2..], 10) catch return false;
            if (got_n != n) return false;

            // WHERE: integer constant must equal n. Independent witness
            // from the relname — both being right rules out a cross-fiber
            // bleed that happens to align on one of them.
            //
            // libpg_query's JSON serializer treats 0 as a default and emits
            // "ival":{} (no inner key) instead of "ival":{"ival":0}. So a
            // missing inner key means the value was 0.
            const where = select.get("whereClause").?.object.get("A_Expr").?.object;
            const aconst = where.get("rexpr").?.object.get("A_Const").?.object;
            const outer_ival = aconst.get("ival").?.object;
            const value: i64 = if (outer_ival.get("ival")) |v| v.integer else 0;
            return value == @as(i64, @intCast(n));
        }
    };

    var group = fiber.Group(void).init(a);
    defer group.deinit();
    try group.ensureTotalCapacity(1000);
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        try group.spawn(.{}, Worker.run, .{ a, i, &failures });
    }
    group.wait();
    try testing.expectEqual(@as(u32, 0), failures.load(.seq_cst));
}
