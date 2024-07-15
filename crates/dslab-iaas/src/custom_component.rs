//! Trait for implementation of custom components.

use simcore::context::SimulationContext;
use simcore::handler::EventHandler;

pub trait CustomComponent: EventHandler {
    /// Creates new component with provided simulation context.
    fn new(ctx: SimulationContext) -> Self
    where
        Self: Sized;

    /// Initializes component, emits required events.
    fn init(&mut self);
}
