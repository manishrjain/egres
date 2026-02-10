pub const Config = struct {
    heartbeat_interval: u32 = 2,
    election_timeout_min: u32 = 10,
    election_timeout_max: u32 = 20,
    tick_interval_ms: u32 = 100,
    max_entries_per_msg: u32 = 64,
    max_msg_size: u32 = 1_048_576,

    pub fn validate(self: Config) !void {
        if (self.election_timeout_min == 0) return error.InvalidConfig;
        if (self.election_timeout_max < self.election_timeout_min) return error.InvalidConfig;
        if (self.heartbeat_interval == 0) return error.InvalidConfig;
        if (self.heartbeat_interval >= self.election_timeout_min) return error.InvalidConfig;
    }
};

pub const ConfigError = error{
    InvalidConfig,
};
