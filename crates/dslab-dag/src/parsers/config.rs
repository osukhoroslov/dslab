use serde::Deserialize;

/// Common settings for DAG parsers.
#[derive(Debug, Deserialize)]
pub struct ParserConfig {
    /// Reference machine speed in Gflop/s (default: 10).
    pub reference_speed: f64,
    /// Whether to ignore the task memory requirement (if present) and set it to 0 (default: false).
    #[serde(default)]
    pub ignore_memory: bool,
    /// Options for generating cores requirements for tasks.
    pub generate_cores: Option<GenerateCoresOptions>,
    /// Options for generating memory requirements for tasks.
    pub generate_memory: Option<GenerateMemoryOptions>,
    /// Random seed for cores and memory generation.
    pub seed: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct GenerateCoresOptions {
    /// Minimum cores value.
    pub min: u32,
    /// Maximum cores value.
    pub max: u32,
    /// Whether to use the same value for all tasks of the same type
    /// (inferred by the task name prefix).
    pub regular: bool,
}

#[derive(Debug, Deserialize)]
pub struct GenerateMemoryOptions {
    /// Minimum memory value.
    pub min: u64,
    /// Maximum memory value.
    pub max: u64,
    /// Whether to use the same value for all tasks of the same type
    /// (inferred by the task name prefix).
    pub regular: bool,
}

impl ParserConfig {
    pub fn with_reference_speed(speed: f64) -> Self {
        Self {
            reference_speed: speed,
            ignore_memory: false,
            generate_cores: None,
            generate_memory: None,
            seed: None,
        }
    }
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            reference_speed: 10.,
            ignore_memory: false,
            generate_cores: None,
            generate_memory: None,
            seed: None,
        }
    }
}
