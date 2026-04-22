const std = @import("std");

pub const Error = error{
    MissingValue,
    UnknownOption,
    InvalidBool,
    InvalidInt,
    InvalidFloat,
    InvalidDuration,
    PathExpansionFailed,
} || std.mem.Allocator.Error;

const whitespace = " \t\r\n";

pub const SuperFlag = struct {
    allocator: std.mem.Allocator,
    map: std.StringHashMap([]const u8),

    pub fn parse(allocator: std.mem.Allocator, input: []const u8) Error!SuperFlag {
        var self: SuperFlag = .{
            .allocator = allocator,
            .map = std.StringHashMap([]const u8).init(allocator),
        };
        errdefer self.deinit();
        try parseInto(&self, input);
        return self;
    }

    pub fn deinit(self: *SuperFlag) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.map.deinit();
    }

    pub fn mergeDefaults(self: *SuperFlag, defaults: []const u8) Error!void {
        var defs = std.StringHashMap([]const u8).init(self.allocator);
        defer {
            var it = defs.iterator();
            while (it.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
                self.allocator.free(entry.value_ptr.*);
            }
            defs.deinit();
        }
        try parseIntoMap(self.allocator, &defs, defaults);

        var self_it = self.map.keyIterator();
        while (self_it.next()) |k| {
            if (!defs.contains(k.*)) return error.UnknownOption;
        }

        var def_it = defs.iterator();
        while (def_it.next()) |entry| {
            if (self.map.contains(entry.key_ptr.*)) continue;
            const k_dup = try self.allocator.dupe(u8, entry.key_ptr.*);
            errdefer self.allocator.free(k_dup);
            const v_dup = try self.allocator.dupe(u8, entry.value_ptr.*);
            errdefer self.allocator.free(v_dup);
            try self.map.put(k_dup, v_dup);
        }
    }

    pub fn has(self: *const SuperFlag, key: []const u8) bool {
        const v = self.getString(key);
        return v.len != 0;
    }

    pub fn getString(self: *const SuperFlag, key: []const u8) []const u8 {
        var buf: [256]u8 = undefined;
        const norm = normalizeKeyBuf(key, &buf) orelse return "";
        return self.map.get(norm) orelse "";
    }

    pub fn getBool(self: *const SuperFlag, key: []const u8) Error!bool {
        const v = self.getString(key);
        if (v.len == 0) return false;
        return parseBool(v);
    }

    pub fn getI64(self: *const SuperFlag, key: []const u8) Error!i64 {
        const v = self.getString(key);
        if (v.len == 0) return 0;
        return std.fmt.parseInt(i64, v, 0) catch error.InvalidInt;
    }

    pub fn getU64(self: *const SuperFlag, key: []const u8) Error!u64 {
        const v = self.getString(key);
        if (v.len == 0) return 0;
        return std.fmt.parseInt(u64, v, 0) catch error.InvalidInt;
    }

    pub fn getU32(self: *const SuperFlag, key: []const u8) Error!u32 {
        const v = self.getString(key);
        if (v.len == 0) return 0;
        return std.fmt.parseInt(u32, v, 0) catch error.InvalidInt;
    }

    pub fn getF64(self: *const SuperFlag, key: []const u8) Error!f64 {
        const v = self.getString(key);
        if (v.len == 0) return 0;
        return std.fmt.parseFloat(f64, v) catch error.InvalidFloat;
    }

    pub fn getDurationMs(self: *const SuperFlag, key: []const u8) Error!i64 {
        const v = self.getString(key);
        if (v.len == 0) return 0;
        return parseDurationMs(v);
    }

    /// Expands a path value: leading `~` or `~/` becomes `$HOME`; the result is then
    /// resolved against the current working directory so relative paths and `..`/`.`
    /// become absolute. Caller owns the returned slice.
    pub fn getPath(self: *const SuperFlag, allocator: std.mem.Allocator, key: []const u8) Error![]u8 {
        const raw = self.getString(key);
        return expandPath(allocator, raw);
    }

    /// Write current entries as "k=v; k=v" (key order unspecified).
    pub fn writeTo(self: *const SuperFlag, allocator: std.mem.Allocator, out: *std.ArrayList(u8)) Error!void {
        var first = true;
        var it = self.map.iterator();
        while (it.next()) |entry| {
            if (!first) try out.appendSlice(allocator, "; ");
            first = false;
            try out.appendSlice(allocator, entry.key_ptr.*);
            try out.append(allocator, '=');
            try out.appendSlice(allocator, entry.value_ptr.*);
        }
    }
};

pub const Help = struct {
    allocator: std.mem.Allocator,
    head: []const u8 = "",
    defaults: SuperFlag,
    entries: std.StringHashMap([]const u8),

    pub fn init(allocator: std.mem.Allocator, defaults: []const u8) Error!Help {
        var sf = try SuperFlag.parse(allocator, defaults);
        errdefer sf.deinit();
        return .{
            .allocator = allocator,
            .defaults = sf,
            .entries = std.StringHashMap([]const u8).init(allocator),
        };
    }

    pub fn deinit(self: *Help) void {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.entries.deinit();
        self.defaults.deinit();
    }

    pub fn setHead(self: *Help, head: []const u8) void {
        self.head = head;
    }

    pub fn addFlag(self: *Help, name: []const u8, description: []const u8) Error!void {
        const key = try normalizeKey(self.allocator, name);
        errdefer self.allocator.free(key);
        if (self.entries.fetchRemove(key)) |old| {
            self.allocator.free(old.key);
            self.allocator.free(old.value);
        }
        const desc_dup = try self.allocator.dupe(u8, description);
        errdefer self.allocator.free(desc_dup);
        try self.entries.put(key, desc_dup);
    }

    /// Renders as:
    ///   <head>
    ///       <name>=<default>; <desc>   (flags with defaults, alphabetised)
    ///       <name>=; <desc>             (flags without defaults, alphabetised)
    pub fn render(self: *const Help, allocator: std.mem.Allocator, out: *std.ArrayList(u8)) Error!void {
        var with_default = std.ArrayList([]const u8).empty;
        defer with_default.deinit(allocator);
        var without_default = std.ArrayList([]const u8).empty;
        defer without_default.deinit(allocator);

        var it = self.entries.keyIterator();
        while (it.next()) |k| {
            if (self.defaults.map.contains(k.*)) {
                try with_default.append(allocator, k.*);
            } else {
                try without_default.append(allocator, k.*);
            }
        }
        std.mem.sort([]const u8, with_default.items, {}, lessThanSlice);
        std.mem.sort([]const u8, without_default.items, {}, lessThanSlice);

        try out.appendSlice(allocator, self.head);
        try out.append(allocator, '\n');
        for (with_default.items) |k| {
            const desc = self.entries.get(k).?;
            const val = self.defaults.map.get(k).?;
            try std.fmt.format(out.writer(allocator), "    {s}={s}; {s}\n", .{ k, val, desc });
        }
        for (without_default.items) |k| {
            const desc = self.entries.get(k).?;
            try std.fmt.format(out.writer(allocator), "    {s}=; {s}\n", .{ k, desc });
        }
    }
};

fn lessThanSlice(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.lessThan(u8, a, b);
}

// ---------- parsing helpers ----------

fn parseInto(self: *SuperFlag, input: []const u8) Error!void {
    return parseIntoMap(self.allocator, &self.map, input);
}

fn parseIntoMap(
    allocator: std.mem.Allocator,
    map: *std.StringHashMap([]const u8),
    input: []const u8,
) Error!void {
    var it = std.mem.splitScalar(u8, input, ';');
    while (it.next()) |raw| {
        const segment = std.mem.trim(u8, raw, whitespace);
        if (segment.len == 0) continue;

        const eq = std.mem.indexOfScalar(u8, segment, '=') orelse return error.MissingValue;
        const raw_key = std.mem.trim(u8, segment[0..eq], whitespace);
        const raw_val = std.mem.trim(u8, segment[eq + 1 ..], whitespace);
        if (raw_key.len == 0) return error.MissingValue;

        const key = try normalizeKey(allocator, raw_key);
        errdefer allocator.free(key);
        const val = try allocator.dupe(u8, raw_val);
        errdefer allocator.free(val);

        if (map.fetchRemove(key)) |old| {
            allocator.free(old.key);
            allocator.free(old.value);
        }
        try map.put(key, val);
    }
}

fn normalizeKey(allocator: std.mem.Allocator, key: []const u8) Error![]u8 {
    const out = try allocator.alloc(u8, key.len);
    for (key, 0..) |c, i| {
        const lower = std.ascii.toLower(c);
        out[i] = if (lower == '_') '-' else lower;
    }
    return out;
}

fn normalizeKeyBuf(key: []const u8, buf: []u8) ?[]const u8 {
    if (key.len > buf.len) return null;
    for (key, 0..) |c, i| {
        const lower = std.ascii.toLower(c);
        buf[i] = if (lower == '_') '-' else lower;
    }
    return buf[0..key.len];
}

// ---------- type coercions ----------

fn parseBool(s: []const u8) Error!bool {
    const trimmed = std.mem.trim(u8, s, whitespace);
    const true_set = [_][]const u8{ "1", "t", "true", "y", "yes", "on" };
    const false_set = [_][]const u8{ "0", "f", "false", "n", "no", "off" };
    for (true_set) |w| if (std.ascii.eqlIgnoreCase(trimmed, w)) return true;
    for (false_set) |w| if (std.ascii.eqlIgnoreCase(trimmed, w)) return false;
    return error.InvalidBool;
}

/// Parse a duration literal into milliseconds.
///
/// Accepted forms:
///   - `<N>d` for N days (N is a signed integer; e.g. `30d`, `-1d`).
///   - Concatenated `<number><unit>` segments, e.g. `1h30m`, `500ms`, `1.5s`.
///     Units: `ns`, `us`, `µs`, `ms`, `s`, `m`, `h`. Sub-ms units contribute
///     fractional milliseconds, which are truncated in the final i64 result.
///   - Optional leading `+` or `-` sign.
///   - The bare string `"0"` is accepted and returns 0.
fn parseDurationMs(raw: []const u8) Error!i64 {
    const s = std.mem.trim(u8, raw, whitespace);
    if (s.len == 0) return 0;

    // Day shortcut: "<int>d" where the trailing 'd' is the only unit.
    if (s[s.len - 1] == 'd') {
        const prefix = s[0 .. s.len - 1];
        if (std.fmt.parseInt(i64, prefix, 0)) |days| {
            return days * 24 * 60 * 60 * 1000;
        } else |_| {}
        // Fall through; let the general parser decide.
    }

    if (std.mem.eql(u8, s, "0")) return 0;

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
        // number
        const num_start = i;
        while (i < s.len and (std.ascii.isDigit(s[i]) or s[i] == '.')) : (i += 1) {}
        if (i == num_start) return error.InvalidDuration;
        const num_str = s[num_start..i];
        const value = std.fmt.parseFloat(f64, num_str) catch return error.InvalidDuration;

        // unit
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

fn expandPath(allocator: std.mem.Allocator, raw: []const u8) Error![]u8 {
    if (raw.len == 0) return allocator.alloc(u8, 0);

    // Tilde expansion: "~" alone, or "~/…"  (but not "~something").
    var expanded_owned: ?[]u8 = null;
    defer if (expanded_owned) |e| allocator.free(e);
    var to_resolve: []const u8 = raw;
    if (raw[0] == '~' and (raw.len == 1 or std.fs.path.isSep(raw[1]))) {
        const home = std.process.getEnvVarOwned(allocator, "HOME") catch |err| switch (err) {
            error.EnvironmentVariableNotFound, error.InvalidWtf8 => return error.PathExpansionFailed,
            error.OutOfMemory => return error.OutOfMemory,
        };
        defer allocator.free(home);
        const rest = raw[1..];
        const joined = try std.mem.concat(allocator, u8, &.{ home, rest });
        expanded_owned = joined;
        to_resolve = joined;
    }

    // std.fs.path.resolve normalises but does not prepend cwd for relative paths;
    // we want absolute output regardless of input.
    if (std.fs.path.isAbsolute(to_resolve)) {
        return try std.fs.path.resolve(allocator, &.{to_resolve});
    }
    const cwd = std.process.getCwdAlloc(allocator) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return error.PathExpansionFailed,
    };
    defer allocator.free(cwd);
    return try std.fs.path.resolve(allocator, &.{ cwd, to_resolve });
}
