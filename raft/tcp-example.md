# TCP Messaging Between Two Processes in Zig

> **Note**: Zig master (v0.16) is undergoing a `std.Io` refactoring. The APIs
> below are based on the latest patterns from the Zig Cookbook and may need
> minor adjustments as master evolves. The core posix-level calls are stable.

---

## Part 1: Minimal Echo (verify your setup works)

### Server

```zig
const std = @import("std");
const net = std.net;
const posix = std.posix;

pub fn main() !void {
    // Parse address. Use Ip4Address.parse to get a typed address,
    // then wrap it in the generic Address union.
    const loopback = try net.Ip4Address.parse("127.0.0.1", 4040);
    const addr = net.Address{ .in = loopback };

    // Listen. This calls socket(), bind(), listen() under the hood.
    var server = try addr.listen(.{
        .reuse_address = true,
    });
    defer server.deinit();

    std.debug.print("listening on port 4040\n", .{});

    // Accept one connection (we'll loop later).
    const conn = try server.accept();
    defer conn.stream.close();

    std.debug.print("peer connected\n", .{});

    // Read → print → echo back.
    var buf: [1024]u8 = undefined;
    while (true) {
        const n = conn.stream.read(&buf) catch break;
        if (n == 0) break; // peer closed

        std.debug.print("received: {s}\n", .{buf[0..n]});

        // Echo it back.
        conn.stream.writeAll(buf[0..n]) catch break;
    }

    std.debug.print("peer disconnected\n", .{});
}
```

### Client

```zig
const std = @import("std");
const net = std.net;

pub fn main() !void {
    const peer = try net.Ip4Address.parse("127.0.0.1", 4040);
    const addr = net.Address{ .in = peer };

    // Connect. Returns a Stream (which wraps a socket fd).
    const stream = try net.tcpConnectToAddress(addr);
    defer stream.close();

    // Send a message.
    const msg = "hello from client";
    try stream.writeAll(msg);

    // Read the echo.
    var buf: [1024]u8 = undefined;
    const n = try stream.read(&buf);
    std.debug.print("echo: {s}\n", .{buf[0..n]});
}
```

---

## Part 2: Length-Prefixed Message Framing

Raw TCP is a byte stream with no message boundaries. We need framing.
The simplest reliable approach: **4-byte big-endian length prefix**.

```
Wire format:
┌──────────────┬─────────────────────────┐
│ len (4 bytes)│    payload (len bytes)  │
│  big-endian  │                         │
└──────────────┴─────────────────────────┘
```

### Framing helpers

```zig
const std = @import("std");
const net = std.net;
const mem = std.mem;

const max_message_size = 1_048_576; // 1 MiB safety cap

const FramingError = error{
    MessageTooLarge,
    UnexpectedEof,
};

/// Send a length-prefixed message over a TCP stream.
fn sendMessage(stream: net.Stream, payload: []const u8) !void {
    if (payload.len > max_message_size) return error.MessageTooLarge;

    // Write 4-byte big-endian length header.
    const len: u32 = @intCast(payload.len);
    const header = mem.toBytes(mem.nativeTo(u32, len, .big));
    try stream.writeAll(&header);

    // Write the payload.
    try stream.writeAll(payload);
}

/// Receive a length-prefixed message. Caller owns returned slice.
fn recvMessage(stream: net.Stream, allocator: mem.Allocator) ![]u8 {
    // Read the 4-byte length header.
    var header_buf: [4]u8 = undefined;
    const header_n = try stream.readAll(&header_buf);
    if (header_n < 4) return error.UnexpectedEof;

    const len: u32 = mem.readInt(u32, &header_buf, .big);
    if (len > max_message_size) return error.MessageTooLarge;

    // Allocate buffer and read exactly `len` bytes.
    const buf = try allocator.alloc(u8, len);
    errdefer allocator.free(buf);

    const n = try stream.readAll(buf);
    if (n < len) {
        allocator.free(buf);
        return error.UnexpectedEof;
    }

    return buf;
}

// Note: stream.readAll() reads until buf is full or EOF.
// If your Zig version doesn't have readAll on Stream, implement it:
//
// fn readAll(stream: net.Stream, buf: []u8) !usize {
//     var total: usize = 0;
//     while (total < buf.len) {
//         const n = stream.read(buf[total..]) catch |err| return err;
//         if (n == 0) return total; // EOF
//         total += n;
//     }
//     return total;
// }
```

---

## Part 3: Structured Messages (Raft-style)

Now let's send actual typed messages, not raw bytes.

### Message types

```zig
const MessageTag = enum(u8) {
    request_vote = 1,
    request_vote_reply = 2,
    append_entries = 3,
    append_entries_reply = 4,
};

const NodeId = u64;

// Fixed-size messages can use packed structs for zero-cost serialization.
const RequestVote = extern struct {
    term: u64,
    candidate_id: NodeId,
    last_log_index: u64,
    last_log_term: u64,
};

const RequestVoteReply = extern struct {
    term: u64,
    vote_granted: u8, // 0 or 1 (bool as byte for predictable layout)
    from: NodeId,
};
```

### Serialize and send

```zig
fn sendRequestVote(stream: net.Stream, rv: RequestVote) !void {
    // 1 byte tag + @sizeOf(RequestVote) bytes
    const payload_len = 1 + @sizeOf(RequestVote);
    var buf: [4 + payload_len]u8 = undefined;

    // Length prefix (covers tag + struct).
    mem.writeInt(u32, buf[0..4], @intCast(payload_len), .big);

    // Tag byte.
    buf[4] = @intFromEnum(MessageTag.request_vote);

    // Struct bytes. Use @bitCast to get the raw bytes of the extern struct.
    const struct_bytes = mem.asBytes(&rv);
    @memcpy(buf[5..][0..@sizeOf(RequestVote)], struct_bytes);

    try stream.writeAll(&buf);
}
```

### Receive and dispatch

```zig
fn recvAndDispatch(stream: net.Stream, allocator: mem.Allocator) !void {
    const payload = try recvMessage(stream, allocator);
    defer allocator.free(payload);

    if (payload.len < 1) return error.InvalidMessage;

    const tag: MessageTag = @enumFromInt(payload[0]);
    const body = payload[1..];

    switch (tag) {
        .request_vote => {
            if (body.len < @sizeOf(RequestVote)) return error.InvalidMessage;
            const rv = mem.bytesAsValue(RequestVote, body[0..@sizeOf(RequestVote)]);
            handleRequestVote(rv.*);
        },
        .request_vote_reply => {
            if (body.len < @sizeOf(RequestVoteReply)) return error.InvalidMessage;
            const rvr = mem.bytesAsValue(RequestVoteReply, body[0..@sizeOf(RequestVoteReply)]);
            handleRequestVoteReply(rvr.*);
        },
        .append_entries => {
            // AppendEntries has variable-length entries[].
            // See "Variable-length messages" below.
            handleAppendEntriesRaw(body);
        },
        .append_entries_reply => {
            // Fixed-size, same pattern as above.
        },
    }
}

fn handleRequestVote(rv: RequestVote) void {
    std.debug.print("RequestVote: term={} candidate={}\n", .{ rv.term, rv.candidate_id });
}

fn handleRequestVoteReply(rvr: RequestVoteReply) void {
    std.debug.print("RequestVoteReply: term={} granted={}\n", .{ rvr.term, rvr.vote_granted != 0 });
}
```

---

## Part 4: Variable-Length Messages (AppendEntries)

AppendEntries carries a slice of log entries. We serialize it as:

```
Wire format for AppendEntries payload (after the tag byte):
┌────────────────────────────────────────────┐
│  header (fixed-size extern struct)         │
│    term: u64                               │
│    leader_id: u64                          │
│    prev_log_index: u64                     │
│    prev_log_term: u64                      │
│    leader_commit: u64                      │
│    entry_count: u32                        │
├────────────────────────────────────────────┤
│  entry 0:                                  │
│    index: u64                              │
│    term: u64                               │
│    data_len: u32                           │
│    data: [data_len]u8                      │
├────────────────────────────────────────────┤
│  entry 1:                                  │
│    ...                                     │
└────────────────────────────────────────────┘
```

```zig
const AppendEntriesHeader = extern struct {
    term: u64,
    leader_id: NodeId,
    prev_log_index: u64,
    prev_log_term: u64,
    leader_commit: u64,
    entry_count: u32,
    _padding: u32 = 0, // explicit padding for alignment
};

const LogEntry = struct {
    index: u64,
    term: u64,
    data: []const u8,
};

fn sendAppendEntries(
    stream: net.Stream,
    header: AppendEntriesHeader,
    entries: []const LogEntry,
    allocator: mem.Allocator,
) !void {
    // Calculate total payload size.
    var payload_size: usize = 1 + @sizeOf(AppendEntriesHeader); // tag + header
    for (entries) |entry| {
        payload_size += 8 + 8 + 4; // index + term + data_len
        payload_size += entry.data.len;
    }

    // Allocate buffer for the entire framed message.
    const buf = try allocator.alloc(u8, 4 + payload_size);
    defer allocator.free(buf);

    var offset: usize = 0;

    // Length prefix.
    mem.writeInt(u32, buf[0..4], @intCast(payload_size), .big);
    offset = 4;

    // Tag.
    buf[offset] = @intFromEnum(MessageTag.append_entries);
    offset += 1;

    // Header.
    const header_bytes = mem.asBytes(&header);
    @memcpy(buf[offset..][0..@sizeOf(AppendEntriesHeader)], header_bytes);
    offset += @sizeOf(AppendEntriesHeader);

    // Entries.
    for (entries) |entry| {
        mem.writeInt(u64, buf[offset..][0..8], entry.index, .big);
        offset += 8;
        mem.writeInt(u64, buf[offset..][0..8], entry.term, .big);
        offset += 8;
        mem.writeInt(u32, buf[offset..][0..4], @intCast(entry.data.len), .big);
        offset += 4;
        @memcpy(buf[offset..][0..entry.data.len], entry.data);
        offset += entry.data.len;
    }

    // Single write for the entire message.
    try stream.writeAll(buf);
}
```

---

## Part 5: Full Two-Process Example

Two processes: Process A listens, Process B connects. They exchange
a RequestVote and RequestVoteReply.

### Process A (server / voter)

```zig
const std = @import("std");
const net = std.net;
const mem = std.mem;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const loopback = try net.Ip4Address.parse("127.0.0.1", 4040);
    const addr = net.Address{ .in = loopback };

    var server = try addr.listen(.{ .reuse_address = true });
    defer server.deinit();

    std.debug.print("[A] listening on :4040\n", .{});

    const conn = try server.accept();
    defer conn.stream.close();
    std.debug.print("[A] peer connected\n", .{});

    // Receive a RequestVote.
    const payload = try recvMessage(conn.stream, allocator);
    defer allocator.free(payload);

    if (payload.len < 1) return error.InvalidMessage;
    const tag: MessageTag = @enumFromInt(payload[0]);
    std.debug.print("[A] received message tag={}\n", .{tag});

    if (tag == .request_vote) {
        const rv = mem.bytesAsValue(RequestVote, payload[1..][0..@sizeOf(RequestVote)]);
        std.debug.print("[A] RequestVote: term={} candidate={}\n", .{ rv.term, rv.candidate_id });

        // Reply: grant the vote.
        const reply = RequestVoteReply{
            .term = rv.term,
            .vote_granted = 1,
            .from = 1, // our node id
        };
        try sendTypedMessage(conn.stream, .request_vote_reply, mem.asBytes(&reply));
        std.debug.print("[A] sent vote granted\n", .{});
    }
}
```

### Process B (client / candidate)

```zig
const std = @import("std");
const net = std.net;
const mem = std.mem;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const peer = try net.Ip4Address.parse("127.0.0.1", 4040);
    const addr = net.Address{ .in = peer };

    const stream = try net.tcpConnectToAddress(addr);
    defer stream.close();
    std.debug.print("[B] connected to :4040\n", .{});

    // Send RequestVote.
    const rv = RequestVote{
        .term = 5,
        .candidate_id = 2, // our node id
        .last_log_index = 42,
        .last_log_term = 4,
    };
    try sendTypedMessage(stream, .request_vote, mem.asBytes(&rv));
    std.debug.print("[B] sent RequestVote: term=5\n", .{});

    // Receive RequestVoteReply.
    const payload = try recvMessage(stream, allocator);
    defer allocator.free(payload);

    const tag: MessageTag = @enumFromInt(payload[0]);
    if (tag == .request_vote_reply) {
        const reply = mem.bytesAsValue(RequestVoteReply, payload[1..][0..@sizeOf(RequestVoteReply)]);
        std.debug.print("[B] vote granted={} from node {}\n", .{ reply.vote_granted != 0, reply.from });
    }
}
```

### Shared helper (used by both)

```zig
/// Send a tagged, length-prefixed message.
fn sendTypedMessage(stream: net.Stream, tag: MessageTag, body: []const u8) !void {
    const payload_len: u32 = @intCast(1 + body.len); // tag + body

    // Length prefix.
    const header = mem.toBytes(mem.nativeTo(u32, payload_len, .big));
    try stream.writeAll(&header);

    // Tag.
    try stream.writeAll(&[_]u8{@intFromEnum(tag)});

    // Body.
    try stream.writeAll(body);
}
```

### Expected output

```
Terminal 1:                          Terminal 2:
$ zig build run-server               $ zig build run-client
[A] listening on :4040
                                      [B] connected to :4040
[A] peer connected                    [B] sent RequestVote: term=5
[A] received message tag=request_vote
[A] RequestVote: term=5 candidate=2
[A] sent vote granted                [B] vote granted=true from node 1
```

---

## Endianness Note

The fixed-size structs (`RequestVote`, `RequestVoteReply`) use `extern struct`
which has a platform-defined layout. For messages between processes **on the
same machine or same architecture**, this is fine and avoids
serialization overhead.

If you ever need cross-architecture support, switch the struct fields to
explicit big-endian encoding (like we do for the length prefix and for the
entry fields in AppendEntries). For egres where you control the deployment,
native byte order with `extern struct` is the pragmatic choice.

---

## Zig API Summary

| Operation | Call |
|-----------|------|
| Parse IPv4 address | `net.Ip4Address.parse("127.0.0.1", port)` |
| Wrap in generic Address | `net.Address{ .in = ip4addr }` |
| Listen (creates server socket) | `addr.listen(.{ .reuse_address = true })` |
| Accept a connection | `server.accept()` → returns `.stream` |
| Connect to a peer | `net.tcpConnectToAddress(addr)` → returns `Stream` |
| Read bytes | `stream.read(&buf)` → returns count (0 = EOF) |
| Read exactly N bytes | `stream.readAll(buf[0..n])` |
| Write bytes | `stream.writeAll(data)` |
| Close | `stream.close()` / `server.deinit()` |

If any of these fail to compile on your Zig master checkout, check `zig std`
or `src/lib/std/net.zig` in the Zig source -- the function names are stable
but the module path may have shifted during the `std.Io` refactor.
