#![warn(missing_docs)]
#![doc = include_str!("../readme.md")]

pub mod async_mode;
pub mod component;
pub mod context;
pub mod event;
pub mod handler;
pub mod log;
pub mod simulation;
mod state;

pub use colored;
pub use component::Id;
pub use context::SimulationContext;
pub use event::{Event, EventData, EventId, TypedEvent};
pub use handler::{EventCancellationPolicy, EventHandler};
pub use simulation::Simulation;
pub use state::EPSILON;
