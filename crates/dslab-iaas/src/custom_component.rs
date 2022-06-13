use dslab_core::context::SimulationContext;
use dslab_core::handler::EventHandler;

pub trait CustomComponent: EventHandler {
    fn new(ctx: SimulationContext) -> Self
    where
        Self: Sized;

    fn init(&mut self);
}
