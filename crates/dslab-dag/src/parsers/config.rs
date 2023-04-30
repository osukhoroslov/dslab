use serde::Deserialize;

#[derive(Debug, Deserialize)]
/// Common settings for DAG parsers.
pub struct ParserConfig {
    /// Reference machine speed in Gflop/s (default: 10).
    pub reference_speed: f64,
    /// Whether to ignore task memory (if present) and set it to 0 (default: false).
    pub ignore_memory: bool,
}

impl ParserConfig {
    pub fn with_reference_speed(speed: f64) -> Self {
        Self {
            reference_speed: speed,
            ignore_memory: false,
        }
    }
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            reference_speed: 10.,
            ignore_memory: false,
        }
    }
}
