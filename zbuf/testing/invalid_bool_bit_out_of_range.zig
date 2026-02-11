const schema = @import("schema");

pub const BadBit = schema.Table(.{
    .{ .id = 1, .name = "flags", .ty = schema.B64 },
    .{ .name = "too_high", .ty = schema.Bool(.{ .field_id = 1, .bit = 64 }) },
});

pub const tables = .{
    .{ .name = "BadBit", .ty = BadBit, .id = 1 },
};
