use simcore::context::SimulationContext;
use simcore::handler::EventHandler;

pub trait CustomComponent: EventHandler {
    fn new(ctx: SimulationContext) -> Self
    where
        Self: Sized;

    fn init(&mut self);
}
