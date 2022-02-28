use std::cell::RefCell;
use std::rc::Rc;

use crate::event::EventData;
use crate::state::SimulationState;

pub struct SimulationContext {
    id: String,
    sim_state: Rc<RefCell<SimulationState>>,
}

impl SimulationContext {
    pub fn new(id: String, sim_state: Rc<RefCell<SimulationState>>) -> Self {
        Self { id, sim_state }
    }

    pub fn id(&self) -> &str {
        self.id.as_ref()
    }

    pub fn time(&self) -> f64 {
        self.sim_state.borrow().time()
    }

    pub fn rand(&mut self) -> f64 {
        self.sim_state.borrow_mut().rand()
    }

    pub fn emit<T, S>(&mut self, data: T, dest: S, delay: f64) -> u64
    where
        T: EventData,
        S: Into<String>,
    {
        self.sim_state
            .borrow_mut()
            .add_event(data, self.id.clone(), dest.into(), delay)
    }

    pub fn emit_now<T, S>(&mut self, data: T, dest: S) -> u64
    where
        T: EventData,
        S: Into<String>,
    {
        self.sim_state
            .borrow_mut()
            .add_event(data, self.id.clone(), dest.into(), 0.)
    }

    pub fn emit_self<T>(&mut self, data: T, delay: f64) -> u64
    where
        T: EventData,
    {
        self.sim_state
            .borrow_mut()
            .add_event(data, self.id.clone(), self.id.clone(), delay)
    }

    pub fn emit_self_now<T>(&mut self, data: T) -> u64
    where
        T: EventData,
    {
        self.sim_state
            .borrow_mut()
            .add_event(data, self.id.clone(), self.id.clone(), 0.)
    }

    pub fn emit_as<T, S>(&mut self, data: T, src: S, dest: S, delay: f64) -> u64
    where
        T: EventData,
        S: Into<String>,
    {
        self.sim_state
            .borrow_mut()
            .add_event(data, src.into(), dest.into(), delay)
    }

    pub fn cancel_event(&mut self, event_id: u64) {
        self.sim_state.borrow_mut().cancel_event(event_id);
    }
}
