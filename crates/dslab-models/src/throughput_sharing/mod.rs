#![doc = include_str!("README.md")]

mod fair_fast;
mod fair_slow;
mod model;
mod throughput_factor;

#[cfg(test)]
mod tests;

pub use fair_fast::FairThroughputSharingModel;
pub use fair_slow::SlowFairThroughputSharingModel;
pub use model::{make_constant_throughput_function, ThroughputFunction, ThroughputSharingModel};
pub use throughput_factor::{
    make_uniform_throughput_factor_function, ConstantThroughputFactorFunction, EmpiricalThroughputFactorFunction,
    RandomizedThroughputFactorFunction, ThroughputFactorFunction,
};
