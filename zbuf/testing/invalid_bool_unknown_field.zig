const schema = @import("schema");

pub const BadUnknown = schema.Table(.{
    .{ .id = 1, .name = "flags", .ty = schema.B64 },
    .{ .name = "ghost", .ty = schema.Bool(.{ .field_id = 99, .bit = 1 }) },
});

pub const tables = .{
    .{ .name = "BadUnknown", .ty = BadUnknown, .id = 1 },
};
