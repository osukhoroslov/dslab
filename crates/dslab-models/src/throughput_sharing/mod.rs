#![doc = include_str!("README.md")]

mod fair_fast;
mod fair_slow;
mod model;

#[cfg(test)]
mod tests;

pub use fair_fast::FairThroughputSharingModel;
pub use fair_slow::SlowFairThroughputSharingModel;
pub use model::{ThroughputFunction, ThroughputSharingModel};
