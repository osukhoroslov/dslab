//! Custom component standard interface.

use dslab_core::context::SimulationContext;
use dslab_core::handler::EventHandler;

pub trait CustomComponent: EventHandler {
    /// Create new components
    fn new(ctx: SimulationContext) -> Self
    where
        Self: Sized;

    /// Initialize component, spawn required events.
    fn init(&mut self);
}
