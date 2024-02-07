//! Synthetic trace generator.
use std::boxed::Box;
use std::iter::zip;

use rand::prelude::*;
use rand_pcg::Pcg64;

use crate::trace::{ApplicationData, RequestData, Trace};

/// Type erased version of rand::Distribution trait.
pub trait ErasedDistribution<T> {
    /// Generate a random value of T, using rng as the source of randomness.
    fn sample(&self, rng: &mut dyn rand::RngCore) -> T;
}

impl<T, D: Distribution<T> + ?Sized> ErasedDistribution<T> for D {
    fn sample(&self, rng: &mut dyn rand::RngCore) -> T {
        <Self as Distribution<T>>::sample(self, rng)
    }
}

/// Generator of invocation arrival times.
pub enum ArrivalGenerator {
    /// Random arrival times.
    Random(Box<dyn ErasedDistribution<f64>>),
    /// Generates equally spaced arrival times with given interval between consecutive arrivals.
    EquallySpaced(f64),
    /// Explicitly given arrivals.
    Fixed(Vec<f64>),
}

/// Generator of invocation durations.
pub enum DurationGenerator {
    /// Random durations.
    Random(Box<dyn ErasedDistribution<f64>>),
    /// Equal durations.
    Equal(f64),
    /// Explicitly given durations.
    Fixed(Vec<f64>),
}

/// Generator of container memory requirements.
pub enum MemoryGenerator {
    /// Random requirements.
    Random(Box<dyn ErasedDistribution<u64>>),
    /// Fixed requirement.
    Fixed(u64),
}

/// App generation settings.
pub struct SyntheticTraceAppConfig {
    /// Time interval that will contain all arrival times.
    pub activity_window: (f64, f64),
    /// Arrival times generator.
    pub arrival_generator: ArrivalGenerator,
    /// Container cold start latency.
    pub cold_start_latency: f64,
    /// App concurrency level.
    pub concurrency_level: usize,
    /// Container CPU share.
    pub cpu_share: f64,
    /// Invocation durations generator.
    pub duration_generator: DurationGenerator,
    /// Container memory requirement generator.
    pub memory_generator: MemoryGenerator,
}

/// Synthetic trace generation settings.
pub struct SyntheticTraceConfig {
    /// Application generator configs.
    pub apps: Vec<SyntheticTraceAppConfig>,
    /// Memory resource name.
    pub memory_name: String,
    /// Random generator seed.
    pub random_seed: u64,
}

/// Synthetically generated trace.
#[derive(Clone, Default)]
pub struct SyntheticTrace {
    apps: Vec<ApplicationData>,
    requests: Vec<RequestData>,
}

impl Trace for SyntheticTrace {
    fn app_iter(&self) -> Box<dyn Iterator<Item = ApplicationData> + '_> {
        Box::new(self.apps.iter().cloned())
    }

    fn request_iter(&self) -> Box<dyn Iterator<Item = RequestData> + '_> {
        Box::new(self.requests.iter().cloned())
    }

    fn function_iter(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(0..self.apps.len())
    }

    fn is_ordered_by_time(&self) -> bool {
        true
    }

    fn simulation_end(&self) -> Option<f64> {
        let mut time_range = 0.0;
        for req in self.requests.iter() {
            time_range = f64::max(time_range, req.time + req.duration);
        }
        Some(time_range)
    }
}

/// Generates synthetic trace.
pub fn generate_synthetic_trace(mut config: SyntheticTraceConfig) -> SyntheticTrace {
    let mut gen = Pcg64::seed_from_u64(config.random_seed);
    let mut trace: SyntheticTrace = Default::default();
    for (id, app_config) in config.apps.drain(..).enumerate() {
        let mem = match app_config.memory_generator {
            MemoryGenerator::Random(dist) => dist.sample(&mut gen),
            MemoryGenerator::Fixed(val) => val,
        };
        trace.apps.push(ApplicationData::new(
            app_config.concurrency_level,
            app_config.cold_start_latency,
            app_config.cpu_share,
            vec![(config.memory_name.clone(), mem)],
        ));
        let (win_l, win_r) = app_config.activity_window;
        let mut arrivals = Vec::new();
        match app_config.arrival_generator {
            ArrivalGenerator::Random(dist) => {
                let mut t = win_l;
                while t < win_r + 1e-9 {
                    arrivals.push(t);
                    t += dist.sample(&mut gen);
                }
            }
            ArrivalGenerator::EquallySpaced(step) => {
                let mut t = win_l;
                while t < win_r + 1e-9 {
                    arrivals.push(t);
                    t += step;
                }
            }
            ArrivalGenerator::Fixed(vec) => {
                arrivals = vec;
            }
        }
        let mut durations = Vec::new();
        match app_config.duration_generator {
            DurationGenerator::Random(dist) => {
                for _ in 0..arrivals.len() {
                    durations.push(dist.sample(&mut gen));
                }
            }
            DurationGenerator::Equal(duration) => {
                durations = vec![duration; arrivals.len()];
            }
            DurationGenerator::Fixed(vec) => {
                if vec.len() != arrivals.len() {
                    panic!("Error: fixed duration vector has different number of elements than arrivals.");
                }
                durations = vec;
            }
        }
        for (time, duration) in zip(arrivals, durations) {
            trace.requests.push(RequestData { id, duration, time });
        }
    }
    trace.requests.sort_by(|x, y| x.time.total_cmp(&y.time));
    trace
}
