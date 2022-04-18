use rand::distributions::{Distribution, Uniform, WeightedError, WeightedIndex};
use simcore::context::SimulationContext;

pub trait BWModel {
    // will be called each time when bandwidth is needed
    fn get_bandwidth(&mut self, size: u64, ctx: &mut SimulationContext) -> u64;
}

///////////////////////////////////////////////////////////////////////////////

pub struct ConstantBWModel {
    bandwidth: u64,
}

impl ConstantBWModel {
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

pub struct RandomizedBWModel<Dist: Distribution<u64>> {
    dist: Dist,
}

impl<Dist: Distribution<u64>> RandomizedBWModel<Dist> {
    pub fn new(dist: Dist) -> Self {
        Self { dist }
    }
}

impl<Dist: Distribution<u64>> BWModel for RandomizedBWModel<Dist> {
    fn get_bandwidth(&mut self, _: u64, ctx: &mut SimulationContext) -> u64 {
        ctx.sample_from_distribution(&self.dist)
    }
}

pub fn make_uniform_bw_model(low: u64, high: u64) -> RandomizedBWModel<Uniform<u64>> {
    RandomizedBWModel::new(Uniform::<u64>::new(low, high))
}

///////////////////////////////////////////////////////////////////////////////

pub struct EmpiricalBWModel {
    points: Vec<(u64, u64)>, // (value, proportion)
    dist: WeightedIndex<u64>,
}

impl EmpiricalBWModel {
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
