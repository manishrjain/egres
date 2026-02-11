# Joint Consensus Sketch (Code-Level)

Below is a code-level sketch showing how joint consensus would be modeled in the
current Node design. This is not a full implementation; it shows the core data
structures and logic changes.

## 1) Config structures

```zig
pub const Config = struct {
    voters: []NodeId,
    learners: []NodeId,
};

pub const JointConfig = struct {
    old: Config,
    new: Config,
    in_joint: bool,
};
```

Node keeps:

```zig
config: JointConfig,
```

## 2) Config change entry

```zig
pub const ConfChangeV2 = struct {
    // One entry can contain multiple ops.
    changes: []ConfChange,
};

pub const ConfChange = struct {
    ty: ConfChangeType,
    node_id: NodeId,
};
```

## 3) Applying a config change

When a ConfChangeV2 entry is committed:

```zig
fn applyConfChange(self: *Node, cc: ConfChangeV2) void {
    if (!self.config.in_joint) {
        // Phase 1: enter joint config
        const new_cfg = applyChanges(self.config.old, cc);
        self.config = .{
            .old = self.config.old,
            .new = new_cfg,
            .in_joint = true,
        };
    } else {
        // Phase 2: leave joint config (finalize)
        self.config = .{
            .old = self.config.new,
            .new = self.config.new,
            .in_joint = false,
        };
    }
}
```

## 4) Quorum calculation

```zig
fn quorum(cfg: Config) usize {
    return (cfg.voters.len / 2) + 1;
}

fn hasQuorum(self: *Node, match_index: []u64, idx: u64) bool {
    if (!self.config.in_joint) {
        return countMatched(self.config.old.voters, match_index, idx) >= quorum(self.config.old);
    }

    // Joint config: need majority in BOTH old and new
    const ok_old = countMatched(self.config.old.voters, match_index, idx) >= quorum(self.config.old);
    const ok_new = countMatched(self.config.new.voters, match_index, idx) >= quorum(self.config.new);
    return ok_old and ok_new;
}
```

This changes `advanceCommitIndex()`.

## 5) Voting rules

When granting RequestVote:

```zig
fn canVote(self: *Node, candidate_id: NodeId) bool {
    if (self.config.in_joint) {
        return contains(self.config.old.voters, candidate_id) or
               contains(self.config.new.voters, candidate_id);
    } else {
        return contains(self.config.old.voters, candidate_id);
    }
}
```

## 6) Operator-facing API

```zig
// Add + remove in one change.
pub fn proposeJointChange(self: *Node, cc: ConfChangeV2) !void {
    // append confchange entry
}
```

Internally this becomes:
- first commit -> enter joint config
- second commit -> finalize

---

This is the minimal model. If you want, we can implement a skeleton that adds
these structs and uses them in quorum calculations, but does not yet handle
full membership state transitions.
