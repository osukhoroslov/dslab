#![doc = include_str!("README.md")]

mod fair_fast;
mod fair_slow;
mod model;

#[cfg(test)]
mod tests;

pub use fair_fast::FairThroughputSharingModel as FairThroughputSharingModel;
pub use model::{Model as ThroughputSharingModel, ThroughputFunction};
