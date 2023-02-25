//! Throughput factors function.
//!
//! These functions allow to dynamically compute per-request throughput factor based on the request size, current simulation time,
//! etc. All implementations must have [`get_factor`](ThroughputFactorFunction::get_factor) method, which returns factor value
//! from `size` and simulation context `ctx`. This factor will be multiplied by nominal bandwidth to compute effective bandwidth.
//! Using `ctx`, simulation time and random engine can be accessed. This method will be called each time when new disk read/write
//! request is made.
//!
//! There are 3 predefined models:
//! * [`ConstantThroughputFactorFunction`]
//! * [`RandomizedThroughputFactorFunction`]
//! * [`EmpiricalThroughputFactorFunction`]

use rand::distributions::{Distribution, Uniform, WeightedError, WeightedIndex};

use dslab_core::context::SimulationContext;

/// Trait for throughput factor function.
pub trait ThroughputFactorFunction {
    /// Returns the throughput factor per request.
    ///
    /// It is called each time new read/write request is made.
    /// The function is provided with request size and simulation context.
    /// The latter can be used to obtain the current simulation time and the random engine.
    fn get_factor(&mut self, size: u64, ctx: &mut SimulationContext) -> f64;
}

///////////////////////////////////////////////////////////////////////////////

/// Simplest function with constant factor.
pub struct ConstantThroughputFactorFunction {
    value: f64,
}

impl ConstantThroughputFactorFunction {
    /// Creates new constant factor function with given value.
    pub fn new(value: f64) -> Self {
        Self { value }
    }
}

impl ThroughputFactorFunction for ConstantThroughputFactorFunction {
    fn get_factor(&mut self, _: u64, _: &mut SimulationContext) -> f64 {
        self.value
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Function which generates random factor values from the specified distribution.
pub struct RandomizedThroughputFactorFunction<Dist: Distribution<f64>> {
    dist: Dist,
}

impl<Dist: Distribution<f64>> RandomizedThroughputFactorFunction<Dist> {
    /// Creates new randomized factor function with given distribution.
    pub fn new(dist: Dist) -> Self {
        Self { dist }
    }
}

impl<Dist: Distribution<f64>> ThroughputFactorFunction for RandomizedThroughputFactorFunction<Dist> {
    fn get_factor(&mut self, _: u64, ctx: &mut SimulationContext) -> f64 {
        ctx.sample_from_distribution(&self.dist)
    }
}

/// Creates randomized throughput factor function with uniform distribution in `[low, high]` range.
pub fn make_uniform_throughput_factor_function(
    low: f64,
    high: f64,
) -> RandomizedThroughputFactorFunction<Uniform<f64>> {
    RandomizedThroughputFactorFunction::new(Uniform::<f64>::new(low, high))
}

///////////////////////////////////////////////////////////////////////////////

/// Struct for a factor value associated to weight. Used by [`EmpiricalThroughputFactorFunction`].
#[derive(Clone)]
pub struct WeightedThroughputFactor {
    /// Factor value.
    pub value: f64,
    /// Weight of `value` in empirical distribution.
    pub weight: u64,
}

impl WeightedThroughputFactor {
    /// Creates new WeightedThroughputFactor.
    pub fn new(value: f64, weight: u64) -> Self {
        Self { value, weight }
    }
}

/// Function which generates random factor from specified weighted points distribution.
pub struct EmpiricalThroughputFactorFunction {
    /// Pairs of (value, weight).
    points: Vec<WeightedThroughputFactor>,
    /// Distribution used to pick a random index from `points`.
    dist: WeightedIndex<u64>,
}

impl EmpiricalThroughputFactorFunction {
    /// Creates new throughput factor function with given weighted points.
    pub fn new(points: &[WeightedThroughputFactor]) -> Result<Self, WeightedError> {
        let dist = WeightedIndex::new(points.iter().map(|item| item.weight))?;
        Ok(Self {
            points: points.to_vec(),
            dist,
        })
    }
}

impl ThroughputFactorFunction for EmpiricalThroughputFactorFunction {
    fn get_factor(&mut self, _: u64, ctx: &mut SimulationContext) -> f64 {
        self.points[ctx.sample_from_distribution(&self.dist)].value
    }
}
