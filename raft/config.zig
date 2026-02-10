pub const Config = struct {
    heartbeat_ticks: u32 = 2,
    election_timeout_min_ticks: u32 = 10,
    election_timeout_max_ticks: u32 = 20,
    tick_interval_ms: u32 = 100,
    max_entries_per_msg: u32 = 64,
    max_msg_size: u32 = 1_048_576,
    enable_prevote: bool = true,

    pub fn validate(self: Config) !void {
        if (self.election_timeout_min_ticks == 0) return error.InvalidConfig;
        if (self.election_timeout_max_ticks < self.election_timeout_min_ticks) return error.InvalidConfig;
        if (self.heartbeat_ticks == 0) return error.InvalidConfig;
        if (self.heartbeat_ticks >= self.election_timeout_min_ticks) return error.InvalidConfig;
    }
};

pub const ConfigError = error{
    InvalidConfig,
};
