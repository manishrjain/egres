const std = @import("std");

pub const Error = error{
    MissingValue,
    UnknownOption,
    UnknownModule,
    DuplicateModule,
    InvalidBool,
    InvalidInt,
    InvalidFloat,
    InvalidDuration,
    InvalidArgv,
} || std.mem.Allocator.Error;

const whitespace = " \t\r\n";

// ---------- key normalisation ----------

fn normalizeKeyInto(name: []const u8, buf: []u8) []u8 {
    std.debug.assert(buf.len >= name.len);
    for (name, 0..) |c, i| {
        const lower = std.ascii.toLower(c);
        buf[i] = if (lower == '_') '-' else lower;
    }
    return buf[0..name.len];
}

fn normalizeKeyAlloc(allocator: std.mem.Allocator, name: []const u8) Error![]u8 {
    const out = try allocator.alloc(u8, name.len);
    _ = normalizeKeyInto(name, out);
    return out;
}

/// Comptime-evaluated canonical form of a struct field name.
/// Returns a fixed-size array (the caller takes its address with `&`).
fn comptimeCanonical(comptime name: []const u8) [name.len]u8 {
    var buf: [name.len]u8 = undefined;
    for (name, 0..) |c, i| {
        const lower = std.ascii.toLower(c);
        buf[i] = if (lower == '_') '-' else lower;
    }
    return buf;
}

// ---------- value parsing ----------

fn parseBool(s: []const u8) Error!bool {
    const trimmed = std.mem.trim(u8, s, whitespace);
    const true_set = [_][]const u8{ "1", "t", "true", "y", "yes", "on" };
    const false_set = [_][]const u8{ "0", "f", "false", "n", "no", "off" };
    for (true_set) |w| if (std.ascii.eqlIgnoreCase(trimmed, w)) return true;
    for (false_set) |w| if (std.ascii.eqlIgnoreCase(trimmed, w)) return false;
    return error.InvalidBool;
}

/// Parse a duration literal into milliseconds.
/// Forms:
///   - bare integer ("500" → 500 ms)
///   - "<int>d" for days
///   - concatenated <number><unit> segments using ns/us/µs/ms/s/m/h
///   - optional leading sign
fn parseDurationMs(raw: []const u8) Error!i64 {
    const s = std.mem.trim(u8, raw, whitespace);
    if (s.len == 0) return 0;

    // Day shortcut: "<int>d"
    if (s[s.len - 1] == 'd') {
        const prefix = s[0 .. s.len - 1];
        if (std.fmt.parseInt(i64, prefix, 0)) |days| {
            return days * 24 * 60 * 60 * 1000;
        } else |_| {}
        // Fall through to general parser if it wasn't a clean "<int>d".
    }

    // Bare integer (no unit).
    if (std.fmt.parseInt(i64, s, 0)) |n| return n else |_| {}

    var i: usize = 0;
    var sign: f64 = 1;
    if (s[0] == '+') {
        i = 1;
    } else if (s[0] == '-') {
        sign = -1;
        i = 1;
    }
    if (i == s.len) return error.InvalidDuration;

    var total_ms: f64 = 0;
    var saw_segment = false;
    while (i < s.len) {
        const num_start = i;
        while (i < s.len and (std.ascii.isDigit(s[i]) or s[i] == '.')) : (i += 1) {}
        if (i == num_start) return error.InvalidDuration;
        const value = std.fmt.parseFloat(f64, s[num_start..i]) catch return error.InvalidDuration;

        const unit_start = i;
        while (i < s.len and !std.ascii.isDigit(s[i]) and s[i] != '.') : (i += 1) {}
        if (i == unit_start) return error.InvalidDuration;
        const unit = s[unit_start..i];

        const unit_ms: f64 = blk: {
            if (std.mem.eql(u8, unit, "ns")) break :blk 1.0 / 1_000_000.0;
            if (std.mem.eql(u8, unit, "us")) break :blk 1.0 / 1000.0;
            if (std.mem.eql(u8, unit, "µs")) break :blk 1.0 / 1000.0;
            if (std.mem.eql(u8, unit, "ms")) break :blk 1.0;
            if (std.mem.eql(u8, unit, "s")) break :blk 1000.0;
            if (std.mem.eql(u8, unit, "m")) break :blk 60_000.0;
            if (std.mem.eql(u8, unit, "h")) break :blk 3_600_000.0;
            return error.InvalidDuration;
        };
        total_ms += value * unit_ms;
        saw_segment = true;
    }
    if (!saw_segment) return error.InvalidDuration;
    return @intFromFloat(sign * total_ms);
}

// ---------- string tokenizer ----------

const KvMap = std.StringHashMap([]const u8);

/// Tokenize a `k=v; k=v;` string into a map of normalized-key → raw-value.
/// Both keys and values point into freshly-allocated memory owned by `allocator`.
/// Caller is responsible for freeing entries (use freeKvMap).
fn parseFlagString(allocator: std.mem.Allocator, input: []const u8) Error!KvMap {
    var map = KvMap.init(allocator);
    errdefer freeKvMap(allocator, &map);

    var it = std.mem.splitScalar(u8, input, ';');
    while (it.next()) |raw| {
        const segment = std.mem.trim(u8, raw, whitespace);
        if (segment.len == 0) continue;

        const eq = std.mem.indexOfScalar(u8, segment, '=') orelse return error.MissingValue;
        const raw_key = std.mem.trim(u8, segment[0..eq], whitespace);
        const raw_val = std.mem.trim(u8, segment[eq + 1 ..], whitespace);
        if (raw_key.len == 0) return error.MissingValue;

        const key = try normalizeKeyAlloc(allocator, raw_key);
        errdefer allocator.free(key);
        const val = try allocator.dupe(u8, raw_val);
        errdefer allocator.free(val);

        if (map.fetchRemove(key)) |old| {
            allocator.free(old.key);
            allocator.free(old.value);
        }
        try map.put(key, val);
    }
    return map;
}

fn freeKvMap(allocator: std.mem.Allocator, map: *KvMap) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        allocator.free(entry.value_ptr.*);
    }
    map.deinit();
}

// ---------- comptime shape validation ----------

fn isAllowedFieldType(comptime FT: type) bool {
    const info = @typeInfo(FT);
    return switch (info) {
        .bool => true,
        .int => |i| i.bits <= 64,
        .float => |f| f.bits == 32 or f.bits == 64,
        .pointer => |p| blk: {
            if (p.size != .slice) break :blk false;
            if (p.child != u8) break :blk false;
            if (!p.is_const) break :blk false;
            break :blk true;
        },
        else => false,
    };
}

fn validateShape(comptime T: type) void {
    const info = @typeInfo(T);
    if (info != .@"struct") {
        @compileError("flags.Live requires a struct type, got " ++ @typeName(T));
    }
    inline for (info.@"struct".fields) |f| {
        if (!isAllowedFieldType(f.type)) {
            @compileError("flags.Live: field '" ++ f.name ++
                "' has unsupported type " ++ @typeName(f.type) ++
                " (allowed: bool, int <=64, f32/f64, []const u8)");
        }
        if (std.mem.eql(u8, f.name, "self")) {
            @compileError("flags.Live: field name 'self' conflicts with the " ++
                "reserved 'docs.self' module-description key on " ++ @typeName(T));
        }
    }
    // Mandatory `pub const docs = .{ .field = "..." }`. Every field needs an
    // entry; every entry must name a real field. The special key `.self` is
    // an optional one-liner about the module itself (rendered next to the
    // module name in writeHelp).
    if (!@hasDecl(T, "docs")) {
        @compileError("flags.Live: '" ++ @typeName(T) ++ "' must declare " ++
            "'pub const docs = .{ .field = \"...\", ... }' covering every field");
    }
    const DocsT = @TypeOf(T.docs);
    const docs_info = @typeInfo(DocsT);
    if (docs_info != .@"struct") {
        @compileError("flags.Live: '" ++ @typeName(T) ++ ".docs' must be a struct literal");
    }
    inline for (info.@"struct".fields) |f| {
        if (!@hasField(DocsT, f.name)) {
            @compileError("flags.Live: '" ++ @typeName(T) ++ ".docs' is missing entry for field '" ++ f.name ++ "'");
        }
    }
    inline for (docs_info.@"struct".fields) |df| {
        if (std.mem.eql(u8, df.name, "self")) continue; // module-level doc; allowed
        if (!@hasField(T, df.name)) {
            @compileError("flags.Live: '" ++ @typeName(T) ++ ".docs." ++ df.name ++
                "' has no matching field on " ++ @typeName(T));
        }
    }
}

/// Returns the doc string for `field_name`. Both T and the entry are
/// guaranteed by validateShape to exist.
fn docFor(comptime T: type, comptime field_name: []const u8) []const u8 {
    return @field(T.docs, field_name);
}

/// Optional one-line module description from `T.docs.self`.
fn selfDocFor(comptime T: type) ?[]const u8 {
    if (!@hasField(@TypeOf(T.docs), "self")) return null;
    return @field(T.docs, "self");
}

// ---------- per-field parser ----------

fn parseField(
    comptime FT: type,
    comptime field_name: []const u8,
    value: []const u8,
    arena: std.mem.Allocator,
) Error!FT {
    const info = @typeInfo(FT);
    return switch (info) {
        .bool => try parseBool(value),
        .int => |int_info| blk: {
            // Duration handling for integer fields whose name ends in "_ms".
            if (comptime std.mem.endsWith(u8, field_name, "_ms")) {
                const ms = try parseDurationMs(value);
                if (int_info.signedness == .unsigned and ms < 0) return error.InvalidInt;
                break :blk std.math.cast(FT, ms) orelse error.InvalidInt;
            }
            const trimmed = std.mem.trim(u8, value, whitespace);
            break :blk std.fmt.parseInt(FT, trimmed, 0) catch error.InvalidInt;
        },
        .float => blk: {
            const trimmed = std.mem.trim(u8, value, whitespace);
            break :blk std.fmt.parseFloat(FT, trimmed) catch error.InvalidFloat;
        },
        .pointer => try arena.dupe(u8, value),
        else => unreachable,
    };
}

// ---------- Live(T) ----------

pub fn Live(comptime T: type) type {
    comptime validateShape(T);
    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        arena: std.heap.ArenaAllocator,
        mu: std.Thread.RwLock = .{},
        current: T = .{},
        version_val: u64 = 0,

        pub fn init(allocator: std.mem.Allocator, input: []const u8) Error!Self {
            var self: Self = .{
                .allocator = allocator,
                .arena = std.heap.ArenaAllocator.init(allocator),
                .current = .{},
            };
            errdefer self.arena.deinit();
            if (input.len > 0) {
                var kv = try parseFlagString(allocator, input);
                defer freeKvMap(allocator, &kv);
                var it = kv.iterator();
                while (it.next()) |entry| {
                    try assignByKey(&self.current, entry.key_ptr.*, entry.value_ptr.*, self.arena.allocator());
                }
            }
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.arena.deinit();
        }

        pub fn update(self: *Self, input: []const u8) Error!void {
            // Parse the input outside any lock — that work touches no shared
            // state. The coerce-and-install phase runs under the exclusive
            // write lock so the arena (not thread-safe) is single-writer.
            var kv = try parseFlagString(self.allocator, input);
            defer freeKvMap(self.allocator, &kv);

            self.mu.lock();
            defer self.mu.unlock();

            var next: T = self.current;
            var it = kv.iterator();
            while (it.next()) |entry| {
                try assignByKey(&next, entry.key_ptr.*, entry.value_ptr.*, self.arena.allocator());
            }
            self.current = next;
            self.version_val += 1;
        }

        pub fn version(self: *Self) u64 {
            self.mu.lockShared();
            defer self.mu.unlockShared();
            return self.version_val;
        }

        pub fn snapshot(self: *Self) T {
            self.mu.lockShared();
            defer self.mu.unlockShared();
            return self.current;
        }

        fn assignByKey(target: *T, key: []const u8, value: []const u8, arena: std.mem.Allocator) Error!void {
            const fields = @typeInfo(T).@"struct".fields;
            inline for (fields) |f| {
                const canonical = comptime comptimeCanonical(f.name);
                if (std.mem.eql(u8, &canonical, key)) {
                    @field(target, f.name) = try parseField(f.type, f.name, value, arena);
                    return;
                }
            }
            return error.UnknownOption;
        }
    };
}

// ---------- comptime help renderer (per-struct) ----------

/// Renders a help block for one module, mirroring ristretto/z SuperFlagHelp:
///
///   <module> -- <T.docs.self>     (the "-- ..." part is omitted if absent)
///       <field>=<default>; <field doc>
///       ...
pub fn writeHelp(
    comptime T: type,
    name: []const u8,
    out: *std.ArrayList(u8),
    allocator: std.mem.Allocator,
) Error!void {
    comptime validateShape(T);

    try out.appendSlice(allocator, "  ");
    try out.appendSlice(allocator, name);
    if (comptime selfDocFor(T)) |s| {
        try out.appendSlice(allocator, " -- ");
        try out.appendSlice(allocator, s);
    }
    try out.append(allocator, '\n');

    const fields = @typeInfo(T).@"struct".fields;
    const default_t: T = .{};
    inline for (fields) |f| {
        const canonical = comptime comptimeCanonical(f.name);
        try out.appendSlice(allocator, "      ");
        try out.appendSlice(allocator, &canonical);
        try out.append(allocator, '=');
        try writeDefault(f.type, @field(default_t, f.name), out, allocator);
        try out.appendSlice(allocator, " (");
        try out.appendSlice(allocator, comptime humanType(f.type, f.name));
        try out.appendSlice(allocator, "); ");
        try out.appendSlice(allocator, comptime docFor(T, f.name));
        try out.append(allocator, '\n');
    }
}

fn humanType(comptime FT: type, comptime field_name: []const u8) []const u8 {
    const info = @typeInfo(FT);
    return switch (info) {
        .bool => "bool",
        .int => blk: {
            if (comptime std.mem.endsWith(u8, field_name, "_ms")) break :blk "duration";
            break :blk @typeName(FT);
        },
        .float => "float",
        .pointer => "string",
        else => unreachable,
    };
}

fn writeDefault(comptime FT: type, value: FT, out: *std.ArrayList(u8), allocator: std.mem.Allocator) Error!void {
    const info = @typeInfo(FT);
    switch (info) {
        .bool => try out.appendSlice(allocator, if (value) "true" else "false"),
        .int => try std.fmt.format(out.writer(allocator), "{d}", .{value}),
        .float => try std.fmt.format(out.writer(allocator), "{d}", .{value}),
        .pointer => try out.appendSlice(allocator, value),
        else => unreachable,
    }
}

// ---------- Registry ----------

const Entry = struct {
    live_ptr: *anyopaque,
    update_fn: *const fn (*anyopaque, []const u8) Error!void,
    help_fn: *const fn (*anyopaque, []const u8, *std.ArrayList(u8), std.mem.Allocator) Error!void,
    deinit_fn: *const fn (*anyopaque, std.mem.Allocator) void,
};

pub const Registry = struct {
    allocator: std.mem.Allocator,
    mu: std.Thread.Mutex = .{},
    entries: std.StringHashMap(Entry),

    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{
            .allocator = allocator,
            .entries = std.StringHashMap(Entry).init(allocator),
        };
    }

    pub fn deinit(self: *Registry) void {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit_fn(entry.value_ptr.live_ptr, self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.entries.deinit();
    }

    /// Allocate a Live(T) owned by this registry, populate it from struct
    /// defaults, and return a pointer for the caller to read from. The
    /// registry will deinit the Live(T) when the registry itself is deinited;
    /// the caller MUST NOT call deinit() on the returned pointer.
    pub fn register(
        self: *Registry,
        name: []const u8,
        comptime T: type,
    ) Error!*Live(T) {
        const Wrappers = struct {
            fn update(p: *anyopaque, input: []const u8) Error!void {
                const l: *Live(T) = @ptrCast(@alignCast(p));
                return l.update(input);
            }
            fn help(p: *anyopaque, module_name: []const u8, out: *std.ArrayList(u8), alloc: std.mem.Allocator) Error!void {
                _ = p;
                return writeHelp(T, module_name, out, alloc);
            }
            fn deinit(p: *anyopaque, alloc: std.mem.Allocator) void {
                const l: *Live(T) = @ptrCast(@alignCast(p));
                l.deinit();
                alloc.destroy(l);
            }
        };

        const live = try self.allocator.create(Live(T));
        errdefer self.allocator.destroy(live);

        live.* = try Live(T).init(self.allocator, "");
        errdefer live.deinit();

        const key = try normalizeKeyAlloc(self.allocator, name);
        errdefer self.allocator.free(key);

        self.mu.lock();
        defer self.mu.unlock();
        if (self.entries.contains(key)) return error.DuplicateModule;

        try self.entries.put(key, .{
            .live_ptr = @ptrCast(live),
            .update_fn = Wrappers.update,
            .help_fn = Wrappers.help,
            .deinit_fn = Wrappers.deinit,
        });
        return live;
    }

    pub fn update(self: *Registry, name: []const u8, input: []const u8) Error!void {
        var name_buf: [256]u8 = undefined;
        if (name.len > name_buf.len) return error.UnknownModule;
        const key = normalizeKeyInto(name, &name_buf);

        self.mu.lock();
        const entry = self.entries.get(key) orelse {
            self.mu.unlock();
            return error.UnknownModule;
        };
        self.mu.unlock();

        return entry.update_fn(entry.live_ptr, input);
    }

    /// Walk a CLI argv slice and dispatch each `--<module>` arg to its
    /// registered Live(T).update. Three input shapes are handled:
    ///
    ///   1. `--<name>=<payload>`         — name + payload in one arg
    ///   2. `--<name>` then `<payload>`  — name and payload as two args
    ///   3. anything else                — pushed verbatim into `leftover`
    ///
    /// A bare `--` switches off flag parsing for the rest of argv (everything
    /// after it goes to `leftover`), matching the GNU convention.
    ///
    /// On `error.UnknownModule`, parsing stops at the offending arg. Modules
    /// dispatched before that point keep their updates; this is best-effort,
    /// not transactional.
    pub fn parseArgv(
        self: *Registry,
        argv: []const []const u8,
        leftover: *std.ArrayList([]const u8),
    ) Error!void {
        var i: usize = 0;
        while (i < argv.len) : (i += 1) {
            const arg = argv[i];

            // Non-flag positional arg — caller's responsibility.
            if (!std.mem.startsWith(u8, arg, "--")) {
                try leftover.append(self.allocator, arg);
                continue;
            }

            // Bare "--": stop parsing flags; remaining args are all positional.
            if (arg.len == 2) {
                i += 1;
                while (i < argv.len) : (i += 1) try leftover.append(self.allocator, argv[i]);
                return;
            }

            // `body` is the text after the leading `--`, e.g. "raft=tracing=true;"
            // or just "raft" if the payload is the next arg.
            const body = arg[2..];

            if (std.mem.indexOfScalar(u8, body, '=')) |eq| {
                // Form 1: --<name>=<payload>. Split on the FIRST `=` only —
                // the payload itself contains its own `=` separators.
                // Strip any surviving outer quotes around the payload.
                try self.update(body[0..eq], stripSurroundingQuotes(body[eq + 1 ..]));
            } else {
                // Form 2: --<name> <payload>. Consume the next arg as payload;
                // a trailing --<name> with no follow-up is a malformed argv.
                if (i + 1 >= argv.len) return error.InvalidArgv;
                try self.update(body, stripSurroundingQuotes(argv[i + 1]));
                i += 1;
            }
        }
    }

    pub fn writeHelpAll(
        self: *Registry,
        out: *std.ArrayList(u8),
    ) Error!void {
        // Snapshot entries under the lock: a concurrent register() could resize
        // the hashmap and invalidate the iterator. help_fn is stateless (comptime-
        // derived from T), so we needn't hold any lock across the per-module render
        // — and dropping it first keeps the registry responsive during a long write.
        self.mu.lock();
        var names = std.ArrayList([]const u8).empty;
        defer names.deinit(self.allocator);
        var it = self.entries.keyIterator();
        while (it.next()) |k| names.append(self.allocator, k.*) catch {
            self.mu.unlock();
            return error.OutOfMemory;
        };
        std.mem.sort([]const u8, names.items, {}, lessThanSlice);
        // Build a parallel list of entries so we don't re-look-up.
        var entries = std.ArrayList(Entry).empty;
        defer entries.deinit(self.allocator);
        for (names.items) |n| {
            entries.append(self.allocator, self.entries.get(n).?) catch {
                self.mu.unlock();
                return error.OutOfMemory;
            };
        }
        self.mu.unlock();

        for (names.items, entries.items, 0..) |n, e, idx| {
            if (idx > 0) try out.append(self.allocator, '\n');
            try e.help_fn(e.live_ptr, n, out, self.allocator);
        }
    }
};

fn lessThanSlice(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

/// Strip a single matched pair of surrounding `"..."` or `'...'` quotes.
/// Used for argv payloads where the shell may not have stripped them
/// (e.g. payloads injected from config files, wrapper scripts, or test code
/// that builds argv literally). Unmatched or absent quotes pass through.
fn stripSurroundingQuotes(s: []const u8) []const u8 {
    if (s.len < 2) return s;
    const first = s[0];
    if (first != '"' and first != '\'') return s;
    if (s[s.len - 1] != first) return s;
    return s[1 .. s.len - 1];
}

// ---------- global registry ----------

var g_registry: ?Registry = null;

/// Initialise the process-wide flag registry. Must be called once before g().
pub fn initGlobal(allocator: std.mem.Allocator) void {
    std.debug.assert(g_registry == null);
    g_registry = Registry.init(allocator);
}

/// Tear down the global registry and every Live(T) it owns.
pub fn deinitGlobal() void {
    if (g_registry) |*reg| {
        reg.deinit();
        g_registry = null;
    }
}

/// Process-wide flag registry. Panics if initGlobal was not called.
pub fn g() *Registry {
    if (g_registry == null) @panic("flags.initGlobal() must be called first");
    return &g_registry.?;
}
