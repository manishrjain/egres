const schema = @import("schema");

pub const Cell = schema.Table(.{
    .{ .id = 1, .name = "tag", .ty = schema.U32 },
    .{ .id = 2, .name = "data", .ty = schema.Bytes },
    .{ .id = 3, .name = "cell_flags", .ty = schema.B64 },
    .{ .name = "cell_hot", .ty = schema.Bool(.{ .field_id = 3, .bit = 0 }) },
});

pub const Row = schema.Table(.{
    .{ .id = 1, .name = "name", .ty = schema.Bytes },
    .{ .id = 2, .name = "value", .ty = schema.U64 },
    .{ .id = 3, .name = "cells", .ty = schema.Array(Cell) },
    .{ .id = 4, .name = "row_cell", .ty = Cell },
    .{ .id = 5, .name = "status", .ty = schema.B64 },
    .{ .id = 6, .name = "lifecycle", .ty = schema.B64 },
    .{ .name = "is_active", .ty = schema.Bool(.{ .field_id = 5, .bit = 0 }) },
    .{ .name = "is_draining", .ty = schema.Bool(.{ .field_id = 5, .bit = 33 }) },
    .{ .name = "is_bootstrapping", .ty = schema.Bool(.{ .field_id = 6, .bit = 1 }) },
    .{ .name = "is_retiring", .ty = schema.Bool(.{ .field_id = 6, .bit = 41 }) },
});

pub const Table = schema.Table(.{
    .{ .id = 1, .name = "rows", .ty = schema.Array(Row) },
    .{ .id = 2, .name = "payload", .ty = schema.Bytes },
    .{ .id = 3, .name = "count", .ty = schema.U32 },
    .{ .id = 4, .name = "table_row", .ty = Row },
});

pub const tables = .{
    .{ .name = "Cell", .ty = Cell, .id = 1 },
    .{ .name = "Row", .ty = Row, .id = 2 },
    .{ .name = "Table", .ty = Table, .id = 3 },
};
