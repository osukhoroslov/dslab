use std::cell::RefCell;
use std::rc::Rc;

use simcore::context::SimulationContext;
use simcore::handler::EventHandler;

pub trait CustomComponent {
    fn new(ctx: SimulationContext) -> Self
    where
        Self: Sized;

    fn handler(&self) -> Rc<RefCell<dyn EventHandler>>
    where
        Self: Sized;

    fn init(&mut self);
}
