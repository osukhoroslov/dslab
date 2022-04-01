use std::cell::{Ref, RefCell};
use std::rc::Rc;

use rand::distributions::uniform::{SampleRange, SampleUniform};

use crate::event::EventData;
use crate::state::SimulationState;

pub struct SimulationContext {
    id: u32,
    name: String,
    sim_state: Rc<RefCell<SimulationState>>,
    names: Rc<RefCell<Vec<String>>>,
}

impl SimulationContext {
    pub fn new(id: u32, name: &str, sim_state: Rc<RefCell<SimulationState>>, names: Rc<RefCell<Vec<String>>>) -> Self {
        Self {
            id,
            name: name.to_owned(),
            sim_state,
            names,
        }
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn time(&self) -> f64 {
        self.sim_state.borrow().time()
    }

    pub fn rand(&mut self) -> f64 {
        self.sim_state.borrow_mut().rand()
    }

    pub fn gen_range<T, R>(&mut self, range: R) -> T
    where
        T: SampleUniform,
        R: SampleRange<T>,
    {
        self.sim_state.borrow_mut().gen_range(range)
    }

    pub fn emit<T>(&mut self, data: T, dest: u32, delay: f64) -> u64
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, dest, delay)
    }

    pub fn emit_now<T>(&mut self, data: T, dest: u32) -> u64
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id.clone(), dest, 0.)
    }

    pub fn emit_self<T>(&mut self, data: T, delay: f64) -> u64
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, self.id, delay)
    }

    pub fn emit_self_now<T>(&mut self, data: T) -> u64
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, self.id, 0.)
    }

    pub fn emit_as<T>(&mut self, data: T, src: u32, dest: u32, delay: f64) -> u64
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, src, dest, delay)
    }

    pub fn cancel_event(&mut self, event_id: u64) {
        self.sim_state.borrow_mut().cancel_event(event_id);
    }

    pub fn lookup_name(&self, id: u32) -> String {
        self.names.borrow()[id as usize].clone()
    }
}
