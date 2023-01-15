//! Bandwidth models.
//!
//! These models allow to dynamically compute per-request bandwidth based on the request size, current simulation time,
//! etc. All implementations must have [`get_bandwidth`](BWModel::get_bandwidth) method, which returns bandwidth value
//! from `size` and simulation context `ctx`. Using `ctx`, simulation time and random engine can be accessed.
//! This method will be called each time when new disk read/write request is made, and the returned value will be used
//! as the bandwidth for this request.
//!
//! There are 3 predefined models:
//! * [`ConstantBWModel`]
//! * [`RandomizedBWModel`]
//! * [`EmpiricalBWModel`]
//!
//! Bandwidth models are supported by simple disk model.

use rand::distributions::{Distribution, Uniform, WeightedError, WeightedIndex};

use dslab_core::context::SimulationContext;

/// Trait for bandwidth model.
pub trait BWModel {
    /// Returns the bandwidth per request.
    ///
    /// It is called each time new read/write request is made.
    /// The model is provided with request size and simulation context.
    /// The latter can be used to obtain the current simulation time and the random engine.
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

/// Struct for a bandwidth value associated to weight. Used by [`EmpiricalBWModel`].
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
pub struct EmpiricalBWModel {
    /// Pairs of (value, weight).
    points: Vec<WeightedBandwidth>,
    /// Distribution used to pick a random index from `points`.
    dist: WeightedIndex<u64>,
}

impl EmpiricalBWModel {
    /// Creates new empirical bandwidth model with given weighted points.
    pub fn new(points: &[WeightedBandwidth]) -> Result<Self, WeightedError> {
        let dist = WeightedIndex::new(points.iter().map(|item| item.weight))?;
        Ok(Self {
            points: points.to_vec(),
            dist,
        })
    }
}

impl BWModel for EmpiricalBWModel {
    fn get_bandwidth(&mut self, _: u64, ctx: &mut SimulationContext) -> u64 {
        self.points[ctx.sample_from_distribution(&self.dist)].value
    }
}
