# FlatBuffers for Raft Messages: Schema → C → Zig

This shows how to define Raft messages in a FlatBuffers schema, generate C code
with `flatcc`, and wrap it from Zig using `@cImport`.

> **Alternative**: There is a pure-Zig FlatBuffers library
> ([flatbufferz](https://github.com/travisstaloch/flatbufferz)) that skips
> the C layer entirely. The approach below uses the C path because flatcc is
> the mature, battle-tested implementation, and Zig's `@cImport` makes
> wrapping C almost free.

---

## Step 1: FlatBuffers Schema (`raft.fbs`)

```fbs
namespace raft;

table RequestVote {
    term:           uint64;
    candidate_id:   uint64;
    last_log_index: uint64;
    last_log_term:  uint64;
}

table RequestVoteReply {
    term:         uint64;
    vote_granted: bool;
    from:         uint64;
}

table LogEntry {
    index: uint64;
    term:  uint64;
    data:  [ubyte];
}

table AppendEntries {
    term:           uint64;
    leader_id:      uint64;
    prev_log_index: uint64;
    prev_log_term:  uint64;
    leader_commit:  uint64;
    entries:        [LogEntry];
}

table AppendEntriesReply {
    term:           uint64;
    success:        bool;
    from:           uint64;
    match_index:    uint64;
    conflict_term:  uint64;
    conflict_index: uint64;
}

union MessageBody {
    RequestVote,
    RequestVoteReply,
    AppendEntries,
    AppendEntriesReply,
}

table Message {
    body: MessageBody;
}

root_type Message;
```

---

## Step 2: Generate C Code

```bash
# Install flatcc (https://github.com/dvidelabs/flatcc)
# Then generate:
flatcc -a -o gen/ raft.fbs
```

This produces in `gen/`:
```
gen/
├── raft_reader.h        # Zero-copy read accessors
├── raft_builder.h       # Builder API for constructing buffers
├── raft_verifier.h      # Buffer verification
└── flatbuffers_common_reader.h  (and other support headers)
```

---

## Step 3: What the Generated C Looks Like

You don't write this -- `flatcc` generates it. But understanding the pattern
is essential for writing the Zig wrapper.

### Generated reader API (from `raft_reader.h`)

```c
// --- Table accessors ---
// Each table gets a `_as_root` function to get the root table from a buffer,
// and field accessor functions.

// Get root Message from a buffer
static inline raft_Message_table_t raft_Message_as_root(const void *buffer);

// Access the MessageBody union
static inline raft_MessageBody_union_type_t raft_Message_body_type(raft_Message_table_t t);
static inline raft_MessageBody_union_t raft_Message_body_union(raft_Message_table_t t);

// RequestVote field accessors (zero-copy, just pointer arithmetic)
static inline uint64_t raft_RequestVote_term(raft_RequestVote_table_t t);
static inline uint64_t raft_RequestVote_candidate_id(raft_RequestVote_table_t t);
static inline uint64_t raft_RequestVote_last_log_index(raft_RequestVote_table_t t);
static inline uint64_t raft_RequestVote_last_log_term(raft_RequestVote_table_t t);

// AppendEntries — entries vector
static inline raft_LogEntry_vec_t raft_AppendEntries_entries(raft_AppendEntries_table_t t);
static inline size_t raft_LogEntry_vec_len(raft_LogEntry_vec_t v);
static inline raft_LogEntry_table_t raft_LogEntry_vec_at(raft_LogEntry_vec_t v, size_t i);

// LogEntry — data field (byte vector)
static inline flatbuffers_uint8_vec_t raft_LogEntry_data(raft_LogEntry_table_t t);
static inline size_t flatbuffers_uint8_vec_len(flatbuffers_uint8_vec_t v);
```

### Generated builder API (from `raft_builder.h`)

```c
// --- Atomic creation (all fields at once) ---
static inline raft_RequestVote_ref_t raft_RequestVote_create(
    flatcc_builder_t *B,
    uint64_t term,
    uint64_t candidate_id,
    uint64_t last_log_index,
    uint64_t last_log_term
);

// --- Sequential construction (field by field) ---
static inline int raft_RequestVote_start(flatcc_builder_t *B);
static inline int raft_RequestVote_term_add(flatcc_builder_t *B, uint64_t term);
static inline int raft_RequestVote_candidate_id_add(flatcc_builder_t *B, uint64_t id);
static inline int raft_RequestVote_last_log_index_add(flatcc_builder_t *B, uint64_t idx);
static inline int raft_RequestVote_last_log_term_add(flatcc_builder_t *B, uint64_t term);
static inline raft_RequestVote_ref_t raft_RequestVote_end(flatcc_builder_t *B);

// --- Root wrappers ---
// Wraps the table as the root of a buffer.
static inline int raft_RequestVote_start_as_root(flatcc_builder_t *B);
static inline int raft_RequestVote_end_as_root(flatcc_builder_t *B);

// --- Buffer finalization ---
// Returns a heap-allocated buffer. Caller must free with flatcc_builder_aligned_free.
void *flatcc_builder_finalize_aligned_buffer(flatcc_builder_t *B, size_t *size);
void flatcc_builder_aligned_free(void *p);
```

---

## Step 4: Zig Wrapper

### Import the generated C headers

```zig
// raft/flatbuf.zig
//
// Wraps the flatcc-generated C API for Raft messages.

const std = @import("std");
const mem = std.mem;

// Import flatcc headers. The build.zig must add the include paths.
const c = @cImport({
    @cInclude("flatbuffers/flatbuffers_common_builder.h");
    @cInclude("raft_builder.h");
    @cInclude("raft_reader.h");
    @cInclude("raft_verifier.h");
});
```

### Builder lifecycle

```zig
const FlatBuilder = struct {
    inner: c.flatcc_builder_t,

    fn init() FlatBuilder {
        var self: FlatBuilder = undefined;
        c.flatcc_builder_init(&self.inner);
        return self;
    }

    fn deinit(self: *FlatBuilder) void {
        c.flatcc_builder_clear(&self.inner);
    }

    /// Reset for reuse (avoids re-init overhead).
    fn reset(self: *FlatBuilder) void {
        c.flatcc_builder_reset(&self.inner);
    }

    /// Extract the finished buffer. Caller must call freeBuffer when done.
    fn finalize(self: *FlatBuilder) ?FlatBuffer {
        var size: usize = 0;
        const ptr = c.flatcc_builder_finalize_aligned_buffer(&self.inner, &size);
        if (ptr == null) return null;
        return FlatBuffer{
            .data = @as([*]u8, @ptrCast(ptr))[0..size],
        };
    }
};

const FlatBuffer = struct {
    data: []u8,

    fn bytes(self: FlatBuffer) []const u8 {
        return self.data;
    }

    fn free(self: FlatBuffer) void {
        c.flatcc_builder_aligned_free(self.data.ptr);
    }
};
```

### Serialize RequestVote

```zig
const RequestVote = struct {
    term: u64,
    candidate_id: u64,
    last_log_index: u64,
    last_log_term: u64,
};

/// Serialize a RequestVote into a FlatBuffer.
/// Returns a buffer that must be freed with buf.free().
fn encodeRequestVote(rv: RequestVote) ?FlatBuffer {
    var builder = FlatBuilder.init();
    defer builder.deinit();

    // Use the atomic create-as-root: writes all fields and wraps as root.
    _ = c.raft_RequestVote_start_as_root(&builder.inner);
    _ = c.raft_RequestVote_term_add(&builder.inner, rv.term);
    _ = c.raft_RequestVote_candidate_id_add(&builder.inner, rv.candidate_id);
    _ = c.raft_RequestVote_last_log_index_add(&builder.inner, rv.last_log_index);
    _ = c.raft_RequestVote_last_log_term_add(&builder.inner, rv.last_log_term);
    _ = c.raft_RequestVote_end_as_root(&builder.inner);

    return builder.finalize();
}
```

### Deserialize RequestVote

```zig
/// Decode a RequestVote from a raw FlatBuffer.
/// Zero-copy: the returned struct has values copied out of the buffer.
/// The buffer must remain valid during this call but can be freed after.
fn decodeRequestVote(buf: []const u8) ?RequestVote {
    // Verify the buffer first (optional but recommended).
    const verify_ok = c.raft_RequestVote_verify_as_root(buf.ptr, buf.len);
    if (verify_ok != 0) return null;

    const table = c.raft_RequestVote_as_root(buf.ptr);
    if (table == null) return null;

    return RequestVote{
        .term = c.raft_RequestVote_term(table),
        .candidate_id = c.raft_RequestVote_candidate_id(table),
        .last_log_index = c.raft_RequestVote_last_log_index(table),
        .last_log_term = c.raft_RequestVote_last_log_term(table),
    };
}
```

### Serialize AppendEntries (with nested entries vector)

```zig
const LogEntry = struct {
    index: u64,
    term: u64,
    data: []const u8,
};

const AppendEntries = struct {
    term: u64,
    leader_id: u64,
    prev_log_index: u64,
    prev_log_term: u64,
    leader_commit: u64,
    entries: []const LogEntry,
};

fn encodeAppendEntries(ae: AppendEntries) ?FlatBuffer {
    var builder = FlatBuilder.init();
    defer builder.deinit();

    const B = &builder.inner;

    // Start the root table.
    _ = c.raft_AppendEntries_start_as_root(B);
    _ = c.raft_AppendEntries_term_add(B, ae.term);
    _ = c.raft_AppendEntries_leader_id_add(B, ae.leader_id);
    _ = c.raft_AppendEntries_prev_log_index_add(B, ae.prev_log_index);
    _ = c.raft_AppendEntries_prev_log_term_add(B, ae.prev_log_term);
    _ = c.raft_AppendEntries_leader_commit_add(B, ae.leader_commit);

    // Build the entries vector.
    // FlatBuffers vectors of tables must be built by starting the vector,
    // pushing each table ref, then ending the vector.
    _ = c.raft_AppendEntries_entries_start(B);

    for (ae.entries) |entry| {
        // Create each LogEntry table. The data field is a byte vector,
        // which must be created before the table that references it.

        // Create the data byte vector.
        const data_ref = c.flatbuffers_uint8_vec_create(B, entry.data.ptr, entry.data.len);

        // Create the LogEntry table referencing that data.
        _ = c.raft_LogEntry_start(B);
        _ = c.raft_LogEntry_index_add(B, entry.index);
        _ = c.raft_LogEntry_term_add(B, entry.term);
        _ = c.raft_LogEntry_data_add(B, data_ref);
        const entry_ref = c.raft_LogEntry_end(B);

        // Push into the vector.
        _ = c.raft_AppendEntries_entries_push(B, entry_ref);
    }

    _ = c.raft_AppendEntries_entries_end(B);
    _ = c.raft_AppendEntries_end_as_root(B);

    return builder.finalize();
}
```

### Deserialize AppendEntries

```zig
fn decodeAppendEntries(buf: []const u8, allocator: mem.Allocator) !?AppendEntries {
    const verify_ok = c.raft_AppendEntries_verify_as_root(buf.ptr, buf.len);
    if (verify_ok != 0) return null;

    const table = c.raft_AppendEntries_as_root(buf.ptr);
    if (table == null) return null;

    // Read scalar fields (zero-copy, just reads from buffer).
    var ae: AppendEntries = .{
        .term = c.raft_AppendEntries_term(table),
        .leader_id = c.raft_AppendEntries_leader_id(table),
        .prev_log_index = c.raft_AppendEntries_prev_log_index(table),
        .prev_log_term = c.raft_AppendEntries_prev_log_term(table),
        .leader_commit = c.raft_AppendEntries_leader_commit(table),
        .entries = &.{},
    };

    // Read the entries vector. We must copy into owned memory because
    // the flatbuffer points into the input buffer.
    const entries_vec = c.raft_AppendEntries_entries(table);
    if (entries_vec != null) {
        const n = c.raft_LogEntry_vec_len(entries_vec);
        const entries = try allocator.alloc(LogEntry, n);
        errdefer allocator.free(entries);

        for (0..n) |i| {
            const entry_table = c.raft_LogEntry_vec_at(entries_vec, i);
            const data_vec = c.raft_LogEntry_data(entry_table);
            const data_len = c.flatbuffers_uint8_vec_len(data_vec);

            // Copy the data out of the flatbuffer.
            const data = try allocator.alloc(u8, data_len);
            @memcpy(data, @as([*]const u8, @ptrCast(data_vec))[0..data_len]);

            entries[i] = LogEntry{
                .index = c.raft_LogEntry_index(entry_table),
                .term = c.raft_LogEntry_term(entry_table),
                .data = data,
            };
        }
        ae.entries = entries;
    }

    return ae;
}
```

---

## Step 5: Using It (encode → send → receive → decode)

```zig
const net = std.net;

fn sendRequestVote(stream: net.Stream, rv: RequestVote) !void {
    const buf = encodeRequestVote(rv) orelse return error.EncodeFailed;
    defer buf.free();

    // Send as length-prefixed message (reuse framing from tcp-example.md).
    const len: u32 = @intCast(buf.data.len);
    const header = mem.toBytes(mem.nativeTo(u32, len, .big));
    try stream.writeAll(&header);
    try stream.writeAll(buf.bytes());
}

fn recvRequestVote(stream: net.Stream, allocator: mem.Allocator) !?RequestVote {
    // Read length prefix.
    var header_buf: [4]u8 = undefined;
    _ = try stream.readAll(&header_buf);
    const len = mem.readInt(u32, &header_buf, .big);

    // Read flatbuffer payload.
    const payload = try allocator.alloc(u8, len);
    defer allocator.free(payload);
    _ = try stream.readAll(payload);

    // Decode. The flatbuffer is read synchronously and values are
    // copied out, so we can free the payload immediately after.
    return decodeRequestVote(payload);
}
```

---

## Step 6: build.zig Integration

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "raft",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Link libc (required for flatcc runtime).
    exe.linkLibC();

    // Add flatcc include paths.
    //   - The flatcc install prefix (headers like flatcc/flatcc_builder.h)
    //   - The generated headers directory
    exe.addSystemIncludePath(.{ .cwd_relative = "/usr/local/include" }); // flatcc headers
    exe.addIncludePath(b.path("gen")); // generated raft_builder.h etc.

    // Link the flatcc runtime library.
    // flatcc installs libflatccrt.a (runtime only, no compiler).
    exe.addLibraryPath(.{ .cwd_relative = "/usr/local/lib" });
    exe.linkSystemLibrary("flatccrt");

    b.installArtifact(exe);
}
```

### Build commands

```bash
# 1. Generate C headers from schema
flatcc -a -o gen/ raft.fbs

# 2. Build the Zig project (links flatcc runtime automatically)
zig build
```

---

## Why FlatBuffers for Raft?

| Property | Benefit for Raft |
|----------|-----------------|
| Zero-copy reads | Followers can inspect `prevLogTerm` without deserializing the entire entries vector |
| No heap allocation on read | Reader functions return pointers into the buffer; scalars are read in-place |
| Schema evolution | Can add fields later (snapshots, pre-vote) without breaking wire compat |
| Verification | `_verify_as_root` catches corrupted/truncated buffers before you touch any field |
| Variable-length entries | AppendEntries carries a vector of LogEntry with variable-size data -- FlatBuffers handles this natively |

### Trade-off vs. hand-rolled serialization

Hand-rolled (from `tcp-example.md`) is ~50 lines and zero dependencies. FlatBuffers adds a codegen step and the flatcc runtime (~100KB static lib), but gives you schema evolution, verification, and handles the nested `entries[]` in AppendEntries without manual offset arithmetic.

For a production system that will eventually add snapshots, membership changes, and pre-vote -- FlatBuffers pays for itself.
