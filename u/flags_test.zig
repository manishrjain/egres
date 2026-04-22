const std = @import("std");
const flags = @import("flags.zig");

const SuperFlag = flags.SuperFlag;
const Help = flags.Help;
const Error = flags.Error;

test "parse happy path and key normalisation" {
    const alloc = std.testing.allocator;
    var sf = try SuperFlag.parse(alloc, "Bool_Key=true; int-key=5; name=manish; ;");
    defer sf.deinit();

    try std.testing.expectEqualStrings("true", sf.getString("bool-key"));
    try std.testing.expectEqualStrings("true", sf.getString("Bool_Key")); // lookup normalises
    try std.testing.expectEqualStrings("5", sf.getString("int-key"));
    try std.testing.expectEqualStrings("manish", sf.getString("name"));
    try std.testing.expectEqualStrings("", sf.getString("missing"));
    try std.testing.expect(sf.has("name"));
    try std.testing.expect(!sf.has("missing"));
}

test "parse empty input" {
    const alloc = std.testing.allocator;
    var sf = try SuperFlag.parse(alloc, "");
    defer sf.deinit();
    try std.testing.expectEqualStrings("", sf.getString("anything"));
}

test "parse returns MissingValue for segment without =" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.MissingValue, SuperFlag.parse(alloc, "key-without-value"));
    try std.testing.expectError(error.MissingValue, SuperFlag.parse(alloc, "ok=1; bad"));
}

test "parse allows empty value" {
    const alloc = std.testing.allocator;
    var sf = try SuperFlag.parse(alloc, "key=;");
    defer sf.deinit();
    try std.testing.expectEqualStrings("", sf.getString("key"));
    // has() treats empty as absent (same semantic as Go's SuperFlag.Has)
    try std.testing.expect(!sf.has("key"));
}

test "mergeDefaults fills missing, preserves user values" {
    const alloc = std.testing.allocator;
    var sf = try SuperFlag.parse(alloc, "a=1; b=2;");
    defer sf.deinit();
    try sf.mergeDefaults("a=0; b=0; c=3;");
    try std.testing.expectEqualStrings("1", sf.getString("a"));
    try std.testing.expectEqualStrings("2", sf.getString("b"));
    try std.testing.expectEqualStrings("3", sf.getString("c"));
}

test "mergeDefaults rejects unknown keys" {
    const alloc = std.testing.allocator;
    var sf = try SuperFlag.parse(alloc, "a=1; x=oops;");
    defer sf.deinit();
    try std.testing.expectError(error.UnknownOption, sf.mergeDefaults("a=0; b=0;"));
}

test "mergeDefaults idempotent when called twice with same defaults" {
    const alloc = std.testing.allocator;
    var sf = try SuperFlag.parse(alloc, "a=1;");
    defer sf.deinit();
    try sf.mergeDefaults("a=0; b=2;");
    try sf.mergeDefaults("a=0; b=2;");
    try std.testing.expectEqualStrings("1", sf.getString("a"));
    try std.testing.expectEqualStrings("2", sf.getString("b"));
}

test "getBool accepts short and long forms, case-insensitive" {
    const alloc = std.testing.allocator;
    const trues = [_][]const u8{ "true", "TRUE", "True", "t", "T", "1", "yes", "y", "on", "ON" };
    const falses = [_][]const u8{ "false", "FALSE", "f", "F", "0", "no", "n", "off", "OFF" };
    for (trues) |v| {
        const opt = try std.fmt.allocPrint(alloc, "k={s};", .{v});
        defer alloc.free(opt);
        var sf = try SuperFlag.parse(alloc, opt);
        defer sf.deinit();
        try std.testing.expect(try sf.getBool("k"));
    }
    for (falses) |v| {
        const opt = try std.fmt.allocPrint(alloc, "k={s};", .{v});
        defer alloc.free(opt);
        var sf = try SuperFlag.parse(alloc, opt);
        defer sf.deinit();
        try std.testing.expect(!try sf.getBool("k"));
    }

    var sf = try SuperFlag.parse(alloc, "k=garbage;");
    defer sf.deinit();
    try std.testing.expectError(error.InvalidBool, sf.getBool("k"));

    // missing key -> false, no error
    try std.testing.expect(!try sf.getBool("missing"));
}

test "getI64 / getU64 handle decimal, hex, octal, binary" {
    const alloc = std.testing.allocator;
    var sf = try SuperFlag.parse(alloc, "dec=42; hex=0xff; oct=0o17; bin=0b101; neg=-7;");
    defer sf.deinit();
    try std.testing.expectEqual(@as(i64, 42), try sf.getI64("dec"));
    try std.testing.expectEqual(@as(i64, 255), try sf.getI64("hex"));
    try std.testing.expectEqual(@as(i64, 15), try sf.getI64("oct"));
    try std.testing.expectEqual(@as(i64, 5), try sf.getI64("bin"));
    try std.testing.expectEqual(@as(i64, -7), try sf.getI64("neg"));
    try std.testing.expectEqual(@as(u64, 255), try sf.getU64("hex"));
    try std.testing.expectEqual(@as(u64, 0), try sf.getU64("missing"));
}

test "getF64" {
    const alloc = std.testing.allocator;
    var sf = try SuperFlag.parse(alloc, "ratio=0.05; neg=-1.5; bad=abc;");
    defer sf.deinit();
    try std.testing.expectApproxEqAbs(@as(f64, 0.05), try sf.getF64("ratio"), 1e-9);
    try std.testing.expectApproxEqAbs(@as(f64, -1.5), try sf.getF64("neg"), 1e-9);
    try std.testing.expectError(error.InvalidFloat, sf.getF64("bad"));
    try std.testing.expectEqual(@as(f64, 0), try sf.getF64("missing"));
}

test "getDurationMs days, standard units, compound, fractional" {
    const alloc = std.testing.allocator;
    var sf = try SuperFlag.parse(alloc,
        \\ms=500ms; s=2s; m=15m; h=12h; d=30d; compound=1h30m; frac=1.5s; neg=-5m; bad=abc;
    );
    defer sf.deinit();
    try std.testing.expectEqual(@as(i64, 500), try sf.getDurationMs("ms"));
    try std.testing.expectEqual(@as(i64, 2_000), try sf.getDurationMs("s"));
    try std.testing.expectEqual(@as(i64, 15 * 60 * 1000), try sf.getDurationMs("m"));
    try std.testing.expectEqual(@as(i64, 12 * 3600 * 1000), try sf.getDurationMs("h"));
    try std.testing.expectEqual(@as(i64, 30 * 24 * 3600 * 1000), try sf.getDurationMs("d"));
    try std.testing.expectEqual(@as(i64, (60 + 30) * 60 * 1000), try sf.getDurationMs("compound"));
    try std.testing.expectEqual(@as(i64, 1500), try sf.getDurationMs("frac"));
    try std.testing.expectEqual(@as(i64, -5 * 60 * 1000), try sf.getDurationMs("neg"));
    try std.testing.expectError(error.InvalidDuration, sf.getDurationMs("bad"));
    try std.testing.expectEqual(@as(i64, 0), try sf.getDurationMs("missing"));
}

test "getPath expands ~ and resolves relative segments" {
    const alloc = std.testing.allocator;
    const home = std.process.getEnvVarOwned(alloc, "HOME") catch return; // skip if HOME unset
    defer alloc.free(home);
    const cwd = try std.process.getCwdAlloc(alloc);
    defer alloc.free(cwd);

    var sf = try SuperFlag.parse(alloc,
        \\abs=/tmp/file.txt; home=~/file.txt; homedot=~/abc/../file.txt; homeonly=~/; rel=./file; curdir=./; tildelit=~file; empty=;
    );
    defer sf.deinit();

    {
        const p = try sf.getPath(alloc, "abs");
        defer alloc.free(p);
        try std.testing.expectEqualStrings("/tmp/file.txt", p);
    }
    {
        const p = try sf.getPath(alloc, "home");
        defer alloc.free(p);
        const expected = try std.fs.path.join(alloc, &.{ home, "file.txt" });
        defer alloc.free(expected);
        try std.testing.expectEqualStrings(expected, p);
    }
    {
        const p = try sf.getPath(alloc, "homedot");
        defer alloc.free(p);
        const expected = try std.fs.path.join(alloc, &.{ home, "file.txt" });
        defer alloc.free(expected);
        try std.testing.expectEqualStrings(expected, p);
    }
    {
        const p = try sf.getPath(alloc, "homeonly");
        defer alloc.free(p);
        try std.testing.expectEqualStrings(home, p);
    }
    {
        const p = try sf.getPath(alloc, "rel");
        defer alloc.free(p);
        const expected = try std.fs.path.join(alloc, &.{ cwd, "file" });
        defer alloc.free(expected);
        try std.testing.expectEqualStrings(expected, p);
    }
    {
        const p = try sf.getPath(alloc, "curdir");
        defer alloc.free(p);
        try std.testing.expectEqualStrings(cwd, p);
    }
    {
        const p = try sf.getPath(alloc, "tildelit");
        defer alloc.free(p);
        const expected = try std.fs.path.join(alloc, &.{ cwd, "~file" });
        defer alloc.free(expected);
        try std.testing.expectEqualStrings(expected, p);
    }
    {
        const p = try sf.getPath(alloc, "empty");
        defer alloc.free(p);
        try std.testing.expectEqualStrings("", p);
    }
}

test "writeTo serialises entries" {
    const alloc = std.testing.allocator;
    var sf = try SuperFlag.parse(alloc, "a=1; b=2;");
    defer sf.deinit();

    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try sf.writeTo(alloc, &out);

    // Key order is undefined, so accept either permutation.
    const s = out.items;
    const ok = std.mem.eql(u8, s, "a=1; b=2") or std.mem.eql(u8, s, "b=2; a=1");
    try std.testing.expect(ok);
}

test "Help.render produces sorted sections with defaults first" {
    const alloc = std.testing.allocator;
    var help = try Help.init(alloc, "enabled=true; path=/tmp;");
    defer help.deinit();
    help.setHead("Trace options");
    try help.addFlag("path", "The path to <something>.");
    try help.addFlag("enabled", "Turns on <something>.");
    try help.addFlag("another", "Not in defaults.");

    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(alloc);
    try help.render(alloc, &out);

    const expected =
        "Trace options\n" ++
        "    enabled=true; Turns on <something>.\n" ++
        "    path=/tmp; The path to <something>.\n" ++
        "    another=; Not in defaults.\n";
    try std.testing.expectEqualStrings(expected, out.items);
}
