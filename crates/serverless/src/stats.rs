#[derive(Copy, Clone, Default)]
pub struct Stats {
    pub invocations: u64,
    pub cold_starts: u64,
    pub cold_starts_total_time: f64,
}
