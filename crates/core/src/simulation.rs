use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use log::Level::Trace;
use log::{log_enabled, trace};

use crate::context::SimulationContext;
use crate::event::Event;
use crate::handler::EventHandler;
use crate::state::SimulationState;

pub struct Simulation {
    sim_state: Rc<RefCell<SimulationState>>,
    handlers: HashMap<String, Rc<RefCell<dyn EventHandler>>>,
    undelivered_events: Vec<Event>,
}

impl Simulation {
    pub fn new(seed: u64) -> Self {
        Self {
            sim_state: Rc::new(RefCell::new(SimulationState::new(seed))),
            handlers: HashMap::new(),
            undelivered_events: Vec::new(),
        }
    }

    pub fn create_context<S>(&mut self, id: S) -> SimulationContext
    where
        S: Into<String>,
    {
        SimulationContext::new(id.into(), self.sim_state.clone())
    }

    pub fn add_handler<S>(&mut self, id: S, handler: Rc<RefCell<dyn EventHandler>>)
    where
        S: Into<String>,
    {
        self.handlers.insert(id.into(), handler);
    }

    pub fn time(&self) -> f64 {
        self.sim_state.borrow().time()
    }

    pub fn step(&mut self) -> bool {
        let next = self.sim_state.borrow_mut().next_event();
        if let Some(event) = next {
            if let Some(handler) = self.handlers.get(&event.dest) {
                if log_enabled!(Trace) {
                    trace!(
                        target: &event.dest,
                        "[{:.3} EVENT {}] {}",
                        self.sim_state.borrow().time(),
                        event.dest,
                        serde_json::to_string(&event).unwrap()
                    );
                }
                handler.borrow_mut().on(event);
            } else {
                self.undelivered_events.push(event);
            }
            true
        } else {
            false
        }
    }

    pub fn steps(&mut self, step_count: u64) -> bool {
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
        let end_time = self.sim_state.borrow().time() + duration;
        loop {
            if let Some(event) = self.sim_state.borrow().peek_event() {
                if event.time > end_time {
                    break;
                }
            } else {
                break;
            }
            self.step();
        }
    }

    pub fn event_count(&self) -> u64 {
        self.sim_state.borrow().event_count()
    }
}
