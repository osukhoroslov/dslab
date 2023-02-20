//! Bandwidth models.
//!
//! These models allow to dynamically compute per-request bandwidth based on the request size, current simulation time,
//! etc. All implementations must have [`get_bandwidth`](ThroughputFactor::get_bandwidth) method, which returns bandwidth value
//! from `size` and simulation context `ctx`. Using `ctx`, simulation time and random engine can be accessed.
//! This method will be called each time when new disk read/write request is made, and the returned value will be used
//! as the bandwidth for this request.
//!
//! There are 3 predefined models:
//! * [`ConstantThroughputFactor`]
//! * [`RandomizedThroughputFactor`]
//! * [`EmpiricalThroughputFactor`]
//!
//! Bandwidth models are supported by simple disk model.

use rand::distributions::{Distribution, Uniform, WeightedError, WeightedIndex};

use dslab_core::context::SimulationContext;

/// Trait for bandwidth model.
pub trait ThroughputFactorFunction {
    /// Returns the bandwidth per request.
    ///
    /// It is called each time new read/write request is made.
    /// The model is provided with request size and simulation context.
    /// The latter can be used to obtain the current simulation time and the random engine.
    fn get_factor(&mut self, size: u64, ctx: &mut SimulationContext) -> f64;
}

///////////////////////////////////////////////////////////////////////////////

/// Simplest model with constant bandwidth.
pub struct ConstantThroughputFactor {
    value: f64,
}

impl ConstantThroughputFactor {
    /// Creates new constant bandwidth model with given value.
    pub fn new(value: f64) -> Self {
        Self { value }
    }
}

impl ThroughputFactorFunction for ConstantThroughputFactor {
    fn get_factor(&mut self, _: u64, _: &mut SimulationContext) -> f64 {
        self.value
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Model which generates random bandwidth values from the specified distribution.
pub struct RandomizedThroughputFactor<Dist: Distribution<f64>> {
    dist: Dist,
}

impl<Dist: Distribution<f64>> RandomizedThroughputFactor<Dist> {
    /// Creates new randomized bandwidth model with given distribution.
    pub fn new(dist: Dist) -> Self {
        Self { dist }
    }
}

impl<Dist: Distribution<f64>> ThroughputFactorFunction for RandomizedThroughputFactor<Dist> {
    fn get_factor(&mut self, _: u64, ctx: &mut SimulationContext) -> f64 {
        ctx.sample_from_distribution(&self.dist)
    }
}

/// Creates randomized throughput factor model with uniform distribution in `[low, high]` range.
pub fn make_uniform_bw_model(low: f64, high: f64) -> RandomizedThroughputFactor<Uniform<f64>> {
    RandomizedThroughputFactor::new(Uniform::<f64>::new(low, high))
}

///////////////////////////////////////////////////////////////////////////////

/// Struct for a bandwidth value associated to weight. Used by [`EmpiricalThroughputFactor`].
#[derive(Clone)]
pub struct WeightedBandwidth {
    /// Bandwidth value.
    pub value: u64,
    /// Weight of `value` in empirical distribution.
    pub weight: u64,
}

impl WeightedBandwidth {
    /// Creates new WeightedBandwidth.
    pub fn new(value: u64, weight: u64) -> Self {
        Self { value, weight }
    }
}

/// Model which generates random bandwidth from specified weighted points distribution.
pub struct EmpiricalThroughputFactor {
    /// Pairs of (value, weight).
    points: Vec<WeightedBandwidth>,
    /// Distribution used to pick a random index from `points`.
    dist: WeightedIndex<u64>,
}

impl EmpiricalThroughputFactor {
    /// Creates new empirical bandwidth model with given weighted points.
    pub fn new(points: &[WeightedBandwidth]) -> Result<Self, WeightedError> {
        let dist = WeightedIndex::new(points.iter().map(|item| item.weight))?;
        Ok(Self {
            points: points.to_vec(),
            dist,
        })
    }
}

impl ThroughputFactorFunction for EmpiricalThroughputFactor {
    fn get_factor(&mut self, _: u64, ctx: &mut SimulationContext) -> f64 {
        self.points[ctx.sample_from_distribution(&self.dist)].value as f64
    }
}
