#![warn(missing_docs)]

//! Module for testing message passing code with model checking technique.

mod dependency;
pub mod events;
pub mod model_checker;
mod network;
mod node;
mod pending_events;
pub mod strategies;
pub mod strategy;
pub mod system;
