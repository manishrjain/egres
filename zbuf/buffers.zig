const schema = @import("schema");

pub const Leaf = schema.Table(.{
    .{ .id = 1, .name = "tag", .ty = schema.U32 },
    .{ .id = 2, .name = "data", .ty = schema.Bytes },
});

pub const Node = schema.Table(.{
    .{ .id = 1, .name = "name", .ty = schema.Bytes },
    .{ .id = 2, .name = "value", .ty = schema.U64 },
    .{ .id = 3, .name = "leaves", .ty = schema.Array(Leaf) },
    .{ .id = 4, .name = "node_leaf", .ty = Leaf },
});

pub const Root = schema.Table(.{
    .{ .id = 1, .name = "nodes", .ty = schema.Array(Node) },
    .{ .id = 2, .name = "payload", .ty = schema.Bytes },
    .{ .id = 3, .name = "count", .ty = schema.U32 },
    .{ .id = 4, .name = "root_node", .ty = Node },
});

pub const tables = .{
    .{ .name = "Leaf", .ty = Leaf, .id = 1 },
    .{ .name = "Node", .ty = Node, .id = 2 },
    .{ .name = "Root", .ty = Root, .id = 3 },
};
