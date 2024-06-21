//! Basic implementations of resource throughput and activity factor functions.

use rand::distributions::{Distribution, Uniform, WeightedError, WeightedIndex};
use sugars::boxed;

use dslab_core::context::SimulationContext;

use crate::throughput_sharing::model::ActivityFactorFn;
use crate::throughput_sharing::ResourceThroughputFn;

// Resource Throughput Functions ---------------------------------------------------------------------------------------

/// Creates resource throughput function which always returns the given value.
pub fn make_constant_throughput_fn(throughput: f64) -> ResourceThroughputFn {
    boxed!(move |_| throughput)
}

// Activity Factor Functions -------------------------------------------------------------------------------------------

/// Activity factor function with constant factor value.
pub struct ConstantFactorFn {
    value: f64,
}

impl ConstantFactorFn {
    /// Creates function with the given factor value.
    pub fn new(value: f64) -> Self {
        Self { value }
    }
}

impl<T> ActivityFactorFn<T> for ConstantFactorFn {
    fn get_factor(&mut self, _: &T, _: &SimulationContext) -> f64 {
        self.value
    }
}

/// Activity factor function which generates random factor values from the specified distribution.
pub struct RandomizedFactorFn<Dist: Distribution<f64>> {
    dist: Dist,
}

impl<Dist: Distribution<f64>> RandomizedFactorFn<Dist> {
    /// Creates function with the given distribution.
    pub fn new(dist: Dist) -> Self {
        Self { dist }
    }
}

impl<T, Dist: Distribution<f64>> ActivityFactorFn<T> for RandomizedFactorFn<Dist> {
    fn get_factor(&mut self, _: &T, ctx: &SimulationContext) -> f64 {
        ctx.sample_from_distribution(&self.dist)
    }
}

/// Creates randomized activity factor function with uniform distribution in `[low, high]` range.
pub fn make_uniform_factor_fn(low: f64, high: f64) -> RandomizedFactorFn<Uniform<f64>> {
    RandomizedFactorFn::new(Uniform::<f64>::new(low, high))
}

/// Activity factor function which generates random factor from the specified weighted points distribution.
pub struct EmpiricalFactorFn {
    /// Factor values.
    values: Vec<f64>,
    /// Distribution used to pick a random index from `values`.
    dist: WeightedIndex<u64>,
}

impl EmpiricalFactorFn {
    /// Creates function with the given weighted points.
    pub fn new(points: &[(f64, u64)]) -> Result<Self, WeightedError> {
        let values = points.iter().map(|item| item.0).collect();
        let dist = WeightedIndex::new(points.iter().map(|item| item.1))?;
        Ok(Self { values, dist })
    }
}

impl<T> ActivityFactorFn<T> for EmpiricalFactorFn {
    fn get_factor(&mut self, _: &T, ctx: &SimulationContext) -> f64 {
        self.values[ctx.sample_from_distribution(&self.dist)]
    }
}
