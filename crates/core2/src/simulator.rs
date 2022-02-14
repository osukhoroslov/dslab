use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use crate::context::SimulationContext;
use crate::event::Event;
use crate::handler::EventHandler;
use crate::simulation::Simulation;

pub struct Simulator {
    sim: Rc<RefCell<Simulation>>,
    handlers: HashMap<String, Rc<RefCell<dyn EventHandler>>>,
    undelivered_events: Vec<Event>,
}

impl Simulator {
    pub fn new(seed: u64) -> Self {
        Self {
            sim: Rc::new(RefCell::new(Simulation::new(seed))),
            handlers: HashMap::new(),
            undelivered_events: Vec::new(),
        }
    }

    pub fn create_context<S>(&mut self, id: S) -> SimulationContext
    where
        S: Into<String>,
    {
        SimulationContext::new(id.into(), self.sim.clone())
    }

    pub fn add_handler<S>(&mut self, id: S, handler: Rc<RefCell<dyn EventHandler>>)
    where
        S: Into<String>,
    {
        self.handlers.insert(id.into(), handler);
    }

    pub fn step(&mut self) -> bool {
        let next = self.sim.borrow_mut().next_event();
        if let Some(event) = next {
            if let Some(handler) = self.handlers.get(&event.dest) {
                handler.borrow_mut().on(event);
            } else {
                self.undelivered_events.push(event);
            }
            true
        } else {
            false
        }
    }

    pub fn steps(&mut self, step_count: u32) -> bool {
        for _i in 0..step_count {
            if !self.step() {
                return false;
            }
        }
        true
    }

    pub fn step_until_no_events(&mut self) {
        while self.step() {}
    }

    pub fn step_for_duration(&mut self, duration: f64) {
        let end_time = self.sim.borrow().time() + duration;
        while self.step() && self.sim.borrow().time() < end_time {}
    }

    pub fn event_count(&self) -> u64 {
        self.sim.borrow().event_count()
    }
}
