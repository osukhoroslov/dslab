use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use log::Level::Trace;
use log::{debug, log_enabled, trace};
use serde_json::json;
use serde_type_name::type_name;

use crate::component::Id;
use crate::context::SimulationContext;
use crate::handler::EventHandler;
use crate::log::log_undelivered_event;
use crate::state::SimulationState;

pub struct Simulation {
    sim_state: Rc<RefCell<SimulationState>>,
    name_to_id: HashMap<String, Id>,
    names: Rc<RefCell<Vec<String>>>,
    handlers: Vec<Option<Rc<RefCell<dyn EventHandler>>>>,
}

impl Simulation {
    pub fn new(seed: u64) -> Self {
        Self {
            sim_state: Rc::new(RefCell::new(SimulationState::new(seed))),
            name_to_id: HashMap::new(),
            names: Rc::new(RefCell::new(Vec::new())),
            handlers: Vec::new(),
        }
    }

    fn register(&mut self, name: &str) -> Id {
        if let Some(&id) = self.name_to_id.get(name) {
            return id;
        }
        let id = self.name_to_id.len() as Id;
        self.name_to_id.insert(name.to_owned(), id);
        self.names.borrow_mut().push(name.to_owned());
        self.handlers.push(None);
        id
    }

    pub fn lookup_id(&self, name: &str) -> Id {
        *self.name_to_id.get(name).unwrap()
    }

    pub fn lookup_name(&self, id: Id) -> String {
        self.names.borrow()[id as usize].clone()
    }

    pub fn create_context<S>(&mut self, name: S) -> SimulationContext
    where
        S: AsRef<str>,
    {
        let ctx = SimulationContext::new(
            self.register(name.as_ref()),
            name.as_ref(),
            self.sim_state.clone(),
            self.names.clone(),
        );
        debug!(
            target: "simulation",
            "[{:.3} DEBUG simulation] Created context: {}",
            self.time(), json!({"name": ctx.name(), "id": ctx.id()})
        );
        ctx
    }

    pub fn add_handler<S>(&mut self, name: S, handler: Rc<RefCell<dyn EventHandler>>) -> Id
    where
        S: AsRef<str>,
    {
        let id = self.register(name.as_ref());
        self.handlers[id as usize] = Some(handler);
        debug!(
            target: "simulation",
            "[{:.3} DEBUG simulation] Added handler: {}",
            self.time(), json!({"name": name.as_ref(), "id": id})
        );
        id
    }

    pub fn time(&self) -> f64 {
        self.sim_state.borrow().time()
    }

    pub fn step(&mut self) -> bool {
        let next = self.sim_state.borrow_mut().next_event();
        if let Some(event) = next {
            if let Some(handler_opt) = self.handlers.get(event.dest as usize) {
                if log_enabled!(Trace) {
                    let src_name = self.lookup_name(event.src);
                    let dest_name = self.lookup_name(event.dest);
                    trace!(
                        target: &dest_name,
                        "[{:.3} {} {}] {}",
                        event.time,
                        crate::log::get_colored("EVENT", colored::Color::BrightBlack),
                        dest_name,
                        json!({"type": type_name(&event.data).unwrap(), "data": event.data, "src": src_name})
                    );
                }
                if let Some(handler) = handler_opt {
                    handler.borrow_mut().on(event);
                } else {
                    log_undelivered_event(event);
                }
            } else {
                log_undelivered_event(event);
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
