#![doc = include_str!("README.md")]

mod fair_fast;
mod fair_fast_with_cancel;
mod fair_slow;
mod functions;
mod model;

#[cfg(test)]
mod tests;

pub use fair_fast::FairThroughputSharingModel;
pub use fair_fast_with_cancel::FairThroughputSharingModelWithCancel;
pub use fair_slow::SlowFairThroughputSharingModel;
pub use functions::{
    make_constant_throughput_fn, make_uniform_factor_fn, ConstantFactorFn, EmpiricalFactorFn, RandomizedFactorFn,
};
pub use model::{ActivityFactorFn, ActivityId, ResourceThroughputFn, ThroughputSharingModel};
