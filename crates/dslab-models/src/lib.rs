#![warn(missing_docs)]
#![doc = include_str!("../README.md")]

pub mod fair_sharing;
pub mod fair_sharing_slow;
pub mod model;

#[cfg(test)]
mod tests;
