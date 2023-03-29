#![warn(missing_docs)]

//! Module for testing message passing code with model checking technique.

mod dependency;
mod events;
pub mod model_checker;
mod network;
mod node;
mod pending_events;
pub mod strategies;
pub mod strategy;
mod system;
