use std::cell::RefCell;
use std::rc::Rc;

use rand::distributions::uniform::{SampleRange, SampleUniform};
use rand::prelude::Distribution;

use crate::component::Id;
use crate::event::{EventData, EventId};
use crate::state::SimulationState;

pub struct SimulationContext {
    id: Id,
    name: String,
    sim_state: Rc<RefCell<SimulationState>>,
    names: Rc<RefCell<Vec<String>>>,
}

impl SimulationContext {
    pub fn new(id: Id, name: &str, sim_state: Rc<RefCell<SimulationState>>, names: Rc<RefCell<Vec<String>>>) -> Self {
        Self {
            id,
            name: name.to_owned(),
            sim_state,
            names,
        }
    }

    pub fn id(&self) -> Id {
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

    pub fn sample_from_distribution<T, Dist: Distribution<T>>(&mut self, dist: &Dist) -> T {
        self.sim_state.borrow_mut().sample_from_distribution(dist)
    }

    pub fn emit<T>(&mut self, data: T, dest: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, dest, delay)
    }

    pub fn emit_now<T>(&mut self, data: T, dest: Id) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id.clone(), dest, 0.)
    }

    pub fn emit_self<T>(&mut self, data: T, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, self.id, delay)
    }

    pub fn emit_self_now<T>(&mut self, data: T) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, self.id, 0.)
    }

    pub fn emit_as<T>(&mut self, data: T, src: Id, dest: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, src, dest, delay)
    }

    pub fn cancel_event(&mut self, id: EventId) {
        self.sim_state.borrow_mut().cancel_event(id);
    }

    pub fn lookup_name(&self, id: Id) -> String {
        self.names.borrow()[id as usize].clone()
    }
}
