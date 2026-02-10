# Epoll-Based Event Loop for Raft

## Why epoll?

Tying a thread per connection wastes resources -- workers block on `read()` 99%
of the time waiting for the next message. With persistent Raft peer connections,
this means N threads idle for N peers.

Instead: one thread owns all connections via epoll. It reads complete messages
and processes them inline. No queue, no pool, no locks on Raft state.

The only blocking call is `wal.fsync()`, which is intentional -- Raft requires
durability before responding.

## Architecture

```
                     epoll_wait
                         |
          +--------------+--------------+------------------+
          |              |              |                  |
     listener_fd     timer_fd     heartbeat_fd       peer_fd(s)
          |              |              |                  |
       accept()    start_election  broadcast_hb     read → parse
       add to        send            send            → handle_message
       epoll       RequestVote    AppendEntries      → fsync
                                  (empty)            → respond
```

## Pseudocode

### Setup

```
listener = socket(TCP)
bind(listener, ":8080")
listen(listener, backlog=128)
set_nonblocking(listener)

epoll = epoll_create()
epoll_add(epoll, listener, EPOLLIN)

// Track partial reads per connection -- a message may arrive
// across multiple EPOLLIN events.
connections = HashMap<fd, ConnectionState>

struct ConnectionState {
    recv_buf: DynamicBuffer       // accumulates bytes until a full message
    peer_id: ?NodeId              // which raft peer this is (if known)
}

// Election timeout uses a timerfd so epoll can wake us for it.
timer_fd = timerfd_create()
reset_election_timer(timer_fd, random(1000..2000ms))
epoll_add(epoll, timer_fd, EPOLLIN)

heartbeat_fd = timerfd_create()   // only active when leader
epoll_add(epoll, heartbeat_fd, EPOLLIN)
```

### Main loop

```
while running:
    events = epoll_wait(epoll, timeout=-1)     // block until something happens

    for event in events:

        // -- New connection --
        if event.fd == listener:
            client_fd = accept(listener)
            set_nonblocking(client_fd)
            epoll_add(epoll, client_fd, EPOLLIN)
            connections[client_fd] = ConnectionState{}

        // -- Election timeout fired --
        else if event.fd == timer_fd:
            read(timer_fd)                     // consume the timerfd event
            raft.start_election()
            // sends RequestVote to all peers (write directly to their fds)
            reset_election_timer(timer_fd, random(1000..2000ms))

        // -- Heartbeat timer (leader only) --
        else if event.fd == heartbeat_fd:
            read(heartbeat_fd)
            raft.broadcast_heartbeat()         // empty AppendEntries to all peers

        // -- Data arrived on a connection --
        else if event.flags & EPOLLIN:
            conn = connections[event.fd]

            // Read whatever the kernel has ready (non-blocking).
            bytes = read(event.fd, scratch_buf)
            if bytes == 0:
                // Peer disconnected.
                epoll_del(epoll, event.fd)
                close(event.fd)
                connections.remove(event.fd)
                continue

            conn.recv_buf.append(bytes)

            // Try to parse complete messages from the buffer.
            // Multiple messages may have arrived in one read().
            while conn.recv_buf.len >= 4:
                msg_len = read_u32_big(conn.recv_buf[0..4])
                if conn.recv_buf.len < 4 + msg_len:
                    break                      // incomplete message, wait for more

                payload = conn.recv_buf[4 .. 4 + msg_len]
                message = deserialize(payload)

                // -- Process the message --
                // All Raft logic runs here, single-threaded, no locks.
                outgoing = raft.handle_message(message)

                // Persist before responding (Raft safety).
                if raft.hard_state_dirty:
                    wal.write_hard_state(raft.current_term, raft.voted_for)
                if raft.log_dirty:
                    wal.append_entries(raft.new_entries)
                wal.fsync()                    // <- the one blocking call

                // Send any outgoing messages.
                for out in outgoing:
                    peer_fd = get_peer_fd(out.to)
                    send_framed(peer_fd, serialize(out.msg))

                // Apply newly committed entries to the state machine.
                raft.apply_committed()

                // Consume the processed message from the buffer.
                conn.recv_buf.drain(4 + msg_len)

        // -- Connection error --
        else if event.flags & (EPOLLERR | EPOLLHUP):
            epoll_del(epoll, event.fd)
            close(event.fd)
            connections.remove(event.fd)
```

### Helpers

```
fn send_framed(fd, data):
    // Non-blocking write. If it would block, buffer it and
    // register EPOLLOUT to flush later.
    write(fd, u32_big(data.len) ++ data)

fn reset_election_timer(timer_fd, ms):
    timerfd_settime(timer_fd, ms, one_shot=true)
```

## Key design points

### One thread, no locks

All Raft state is touched only in the `handle_message` path. There is no
concurrent access, so no mutex is needed around currentTerm, votedFor, log, etc.

### Partial message handling

TCP is a byte stream. One `read()` may return half a message, or three messages
concatenated. Each connection keeps a `recv_buf` that accumulates bytes. The
inner `while` loop parses out complete length-prefixed messages:

```
[4 bytes: msg_len][msg_len bytes: payload][4 bytes: next_len][...]
```

Only complete messages are dispatched to `handle_message`. Incomplete messages
stay in the buffer until the next `EPOLLIN` event.

### Timers are fds

`timerfd_create` produces a file descriptor that becomes readable when the timer
expires. This integrates directly with epoll -- no need for a separate timer
thread or computed poll timeouts.

- **Election timer**: one-shot, re-randomized on every reset (1000--2000ms).
- **Heartbeat timer**: periodic, active only when this node is the leader.

### The one blocking call: fsync

The Raft protocol requires that `currentTerm`, `votedFor`, and log entries are
durable **before** responding to any RPC. So `wal.fsync()` is the one deliberate
blocking point in the loop. This is inherent to the protocol -- you cannot avoid
it without violating safety.

If fsync latency becomes a bottleneck, the optimization is batching: accumulate
multiple messages from one `epoll_wait` pass, write all their WAL entries, then
fsync once and send all responses.

### Write buffering (not shown above)

If a peer's socket send buffer is full, `write()` on a non-blocking socket
returns `EAGAIN`. The correct handling:

1. Buffer the unsent data per connection.
2. Register `EPOLLOUT` on that fd.
3. When epoll signals writable, drain the write buffer.
4. Once the buffer is empty, deregister `EPOLLOUT`.

This prevents the event loop from spinning on a slow peer.

### Reconnection (not shown above)

Peer connections may drop. The event loop should:

1. Detect disconnection via `EPOLLHUP` / `read() == 0`.
2. Remove the fd from epoll and close it.
3. Start a reconnect timer (exponential backoff).
4. On reconnect, add the new fd to epoll.

During disconnection, outgoing messages to that peer are dropped. Raft tolerates
this -- the leader will retry AppendEntries, and elections proceed with the
available majority.

## Future: WAL thread with batching

The single-threaded design has one weakness: `wal.fsync()` blocks the event loop.
If fsync takes 10-50ms and heartbeats fire every 100-200ms, a burst of messages
can delay heartbeats enough to trigger spurious elections.

The fix is to move fsync to a dedicated WAL thread. The event loop processes
messages and updates Raft state immediately, but defers responses until the WAL
thread confirms durability.

```
Event loop                           WAL thread
    |                                    |
t0  handle msg A → queue entries         |
t1  handle msg B → queue entries         |
t2  handle msg C → queue entries         |
    |                               pop all (A,B,C)
    |                               write A,B,C entries
t3  handle msg D → queue entries    fsync()  ← covers A,B,C
t4  handle msg E → queue entries    signal done(batch_id=1)
    |                               pop all (D,E)
    |                               write D,E entries
    ← epoll: done_fd readable       fsync()  ← covers D,E
    send responses for A,B,C        signal done(batch_id=2)
    |
    ← epoll: done_fd readable
    send responses for D,E
```

Multiple messages accumulate while the WAL thread is busy fsyncing. The next
fsync covers all of them -- this is natural batching.

**Critical invariant**: never respond before the data is durable. Processing
messages ahead of fsync is safe (if we crash, peers retry since they got no
response). Responding ahead of fsync violates Raft safety.

The WAL thread signals completion via an `eventfd`, which integrates with the
same epoll loop. The event loop tracks pending responses tagged with batch IDs,
and flushes them when the corresponding batch completes.

This keeps the single-owner model for Raft state (only the event loop touches
it), while removing fsync from the hot path.

**Not implementing this yet.** Starting with the simpler single-threaded design
where fsync is inline. Will revisit when fsync latency becomes a measurable
problem.

## Open questions

- [ ] Should client connections (proposals) be handled on the same epoll loop, or
  a separate listener?
- [ ] Write buffer sizing: fixed per-peer, or dynamically allocated?
- [ ] Should fsync batching be in the first iteration, or added later?
