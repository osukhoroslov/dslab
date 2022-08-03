//! Bandwidth models.

use rand::distributions::{Distribution, Uniform, WeightedError, WeightedIndex};

use dslab_core::context::SimulationContext;

/// Trait for bandwidth model.
pub trait BWModel {
    /// Function which will be called each time when bandwidth is needed.
    fn get_bandwidth(&mut self, size: u64, ctx: &mut SimulationContext) -> u64;
}

///////////////////////////////////////////////////////////////////////////////

/// Simplest model with constant bandwidth.
pub struct ConstantBWModel {
    bandwidth: u64,
}

impl ConstantBWModel {
    /// Creates new constant bandwidth model with given value.
    pub fn new(bandwidth: u64) -> Self {
        Self { bandwidth }
    }
}

impl BWModel for ConstantBWModel {
    fn get_bandwidth(&mut self, _: u64, _: &mut SimulationContext) -> u64 {
        self.bandwidth
    }
}

///////////////////////////////////////////////////////////////////////////////

/// Model which generates random bandwidth values from the specified distribution.
pub struct RandomizedBWModel<Dist: Distribution<u64>> {
    dist: Dist,
}

impl<Dist: Distribution<u64>> RandomizedBWModel<Dist> {
    /// Creates new randomized bandwidth model with given distribution.
    pub fn new(dist: Dist) -> Self {
        Self { dist }
    }
}

impl<Dist: Distribution<u64>> BWModel for RandomizedBWModel<Dist> {
    fn get_bandwidth(&mut self, _: u64, ctx: &mut SimulationContext) -> u64 {
        ctx.sample_from_distribution(&self.dist)
    }
}

/// Creates randomized bandwidth model with uniform distribution in `[low, high]` range.
pub fn make_uniform_bw_model(low: u64, high: u64) -> RandomizedBWModel<Uniform<u64>> {
    RandomizedBWModel::new(Uniform::<u64>::new(low, high))
}

///////////////////////////////////////////////////////////////////////////////

/// Model which generates random bandwidth from specified weighted points distribution.
pub struct EmpiricalBWModel {
    /// Pairs of (value, weight).
    points: Vec<(u64, u64)>,
    /// Distribution used to pick a random index from `points`.
    dist: WeightedIndex<u64>,
}

impl EmpiricalBWModel {
    /// Creates new empirical bandwidth model with given weighted points.
    pub fn new(points: &[(u64, u64)]) -> Result<Self, WeightedError> {
        let dist = WeightedIndex::new(points.iter().map(|item| item.1))?;
        Ok(Self {
            points: points.to_vec(),
            dist,
        })
    }
}

impl BWModel for EmpiricalBWModel {
    fn get_bandwidth(&mut self, _: u64, ctx: &mut SimulationContext) -> u64 {
        self.points[ctx.sample_from_distribution(&self.dist)].0
    }
}
