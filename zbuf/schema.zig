const std = @import("std");

pub const Kind = enum { u8, u16, u32, u64, bytes, array, table, bool, enum_ };

pub const Field = struct {
    id: u32,
    has_wire_id: bool,
    name: []const u8,
    ty: type,
};

pub const Variant = struct {
    name: []const u8,
    tag: u8,
};

pub const U8 = struct {
    pub const schema_kind: Kind = .u8;
};

pub const U16 = struct {
    pub const schema_kind: Kind = .u16;
};

pub const U32 = struct {
    pub const schema_kind: Kind = .u32;
};

pub const U64 = struct {
    pub const schema_kind: Kind = .u64;
};

pub fn Bool(comptime cfg: struct { field_id: u32, bit: u16 }) type {
    return struct {
        pub const schema_kind: Kind = .bool;
        pub const bits_field_id: u32 = cfg.field_id;
        pub const bit_index: u16 = cfg.bit;
    };
}

pub const Bytes = struct {
    pub const schema_kind: Kind = .bytes;
};

pub fn EnumDef(comptime variant_list: anytype) type {
    return struct {
        pub const schema_variants = variant_list;
    };
}

pub fn Enum(comptime EnumT: type) type {
    return struct {
        pub const schema_kind: Kind = .enum_;
        pub const enum_def: type = EnumT;
    };
}

pub fn Array(comptime T: type) type {
    return struct {
        pub const schema_kind: Kind = .array;
        pub const child: type = T;
    };
}

pub fn Table(comptime field_list: anytype) type {
    return struct {
        pub const schema_kind: Kind = .table;
        pub const fields = field_list;
    };
}

pub fn kindOf(comptime T: type) Kind {
    if (@hasDecl(T, "schema_kind")) {
        return T.schema_kind;
    }
    @compileError("type missing schema_kind");
}

pub fn fieldList(comptime T: type) []const Field {
    if (!@hasDecl(T, "fields")) @compileError("table missing fields");
    return T.fields;
}

pub fn requireKind(comptime T: type, comptime want: Kind) void {
    if (kindOf(T) != want) {
        @compileError("schema kind mismatch");
    }
}

pub fn isScalar(comptime T: type) bool {
    return switch (kindOf(T)) {
        .u8, .u16, .u32, .u64 => true,
        else => false,
    };
}

pub fn isBytes(comptime T: type) bool {
    return kindOf(T) == .bytes;
}

pub fn isArray(comptime T: type) bool {
    return kindOf(T) == .array;
}

pub fn isTable(comptime T: type) bool {
    return kindOf(T) == .table;
}

pub fn childOfArray(comptime T: type) type {
    if (kindOf(T) != .array) @compileError("not array");
    return T.child;
}

pub fn slotSize(comptime T: type) u16 {
    return switch (kindOf(T)) {
        .u8 => 1,
        .u16 => 2,
        .u32 => 4,
        .u64 => 8,
        .bytes => 8,
        .array => 8,
        .table => 4,
        .bool => 0,
        .enum_ => 1,
    };
}

pub fn enumDefOf(comptime T: type) type {
    if (kindOf(T) != .enum_) @compileError("not an enum field");
    return T.enum_def;
}

pub fn enumVariants(comptime EnumT: type) []const Variant {
    if (!@hasDecl(EnumT, "schema_variants")) @compileError("EnumDef missing schema_variants");
    const raw = EnumT.schema_variants;
    var out: [raw.len]Variant = undefined;
    for (raw, 0..) |v, i| {
        out[i] = .{ .name = v.name, .tag = v.tag };
    }
    const out_const = out;
    return &out_const;
}

pub fn boolFieldId(comptime T: type) u32 {
    if (@hasDecl(T, "bits_field_id")) return T.bits_field_id;
    return 0;
}

pub fn boolBitIndex(comptime T: type) u16 {
    if (@hasDecl(T, "bit_index")) return T.bit_index;
    return 0;
}

pub fn typeName(comptime T: type) []const u8 {
    return @typeName(T);
}

pub fn sortFieldsById(comptime fields: anytype) [fields.len]Field {
    const count = fields.len;
    var out: [count]Field = undefined;

    inline for (fields, 0..) |f, i| {
        const f_ty = @TypeOf(f);
        var id: usize = 0;
        if (kindOf(f.ty) == .bool) {
            if (@hasField(f_ty, "id")) {
                @compileError("Bool field must not declare .id");
            }
        } else {
            if (!@hasField(f_ty, "id")) {
                @compileError("non-Bool field missing required .id");
            }
            id = f.id;
        }
        out[i] = .{
            .id = id,
            .has_wire_id = (id == 0),
            .name = f.name,
            .ty = f.ty,
        };
    }

    std.mem.sort(Field, out[0..], {}, fieldLessThan);

    var i: usize = 1;
    while (i < out.len) : (i += 1) {
        if (out[i - 1].id > 0 and out[i].id > 0 and out[i - 1].id == out[i].id) {
            @compileError("duplicate field id in schema");
        }
    }
    return out;
}

fn fieldLessThan(_: void, a: Field, b: Field) bool {
    return a.id < b.id;
}

pub fn camelName(alloc: std.mem.Allocator, name: []const u8) ![]const u8 {
    var out_len: usize = 0;
    var upper_next = false;
    for (name) |c| {
        if (c == '_') {
            upper_next = true;
            continue;
        }
        out_len += 1;
        upper_next = false;
    }

    const out = try alloc.alloc(u8, out_len);
    var i: usize = 0;
    upper_next = false;
    for (name) |c| {
        if (c == '_') {
            upper_next = true;
            continue;
        }
        out[i] = if (upper_next) std.ascii.toUpper(c) else c;
        i += 1;
        upper_next = false;
    }
    return out;
}

pub fn camelNameCapitalized(alloc: std.mem.Allocator, name: []const u8) ![]const u8 {
    const base = try camelName(alloc, name);
    defer alloc.free(base);
    if (base.len == 0) return try alloc.alloc(u8, 0);
    var out = try alloc.alloc(u8, base.len);
    std.mem.copyForwards(u8, out, base);
    out[0] = std.ascii.toUpper(out[0]);
    return out;
}
