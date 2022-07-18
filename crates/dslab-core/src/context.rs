//! Accessing simulation from components.

use std::cell::RefCell;
use std::rc::Rc;

use rand::distributions::uniform::{SampleRange, SampleUniform};
use rand::prelude::Distribution;

use crate::component::Id;
use crate::event::{EventData, EventId};
use crate::state::SimulationState;

/// A facade for accessing the simulation state and producing events from simulation components.
pub struct SimulationContext {
    id: Id,
    name: String,
    sim_state: Rc<RefCell<SimulationState>>,
    names: Rc<RefCell<Vec<String>>>,
}

impl SimulationContext {
    pub(crate) fn new(
        id: Id,
        name: &str,
        sim_state: Rc<RefCell<SimulationState>>,
        names: Rc<RefCell<Vec<String>>>,
    ) -> Self {
        Self {
            id,
            name: name.to_owned(),
            sim_state,
            names,
        }
    }

    /// Returns the identifier of component associated with this context.
    pub fn id(&self) -> Id {
        self.id
    }

    /// Returns the name of component associated with this context.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the current simulation time.
    pub fn time(&self) -> f64 {
        self.sim_state.borrow().time()
    }

    /// Returns a random float in the range _[0, 1)_
    /// using the simulation-wide random number generator.
    pub fn rand(&mut self) -> f64 {
        self.sim_state.borrow_mut().rand()
    }

    /// Returns a random float in the specified range
    /// using the simulation-wide random number generator.
    pub fn gen_range<T, R>(&mut self, range: R) -> T
    where
        T: SampleUniform,
        R: SampleRange<T>,
    {
        self.sim_state.borrow_mut().gen_range(range)
    }

    /// Returns a random value from the specified distribution
    /// using the simulation-wide random number generator.
    pub fn sample_from_distribution<T, Dist: Distribution<T>>(&mut self, dist: &Dist) -> T {
        self.sim_state.borrow_mut().sample_from_distribution(dist)
    }

    /// Creates new event with specified payload, destination and delay.
    pub fn emit<T>(&mut self, data: T, dest: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, dest, delay)
    }

    /// Creates new immediate (zero-delay) event with specified payload and destination.
    pub fn emit_now<T>(&mut self, data: T, dest: Id) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id.clone(), dest, 0.)
    }

    /// Creates new event for itself with specified payload and delay.
    pub fn emit_self<T>(&mut self, data: T, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, self.id, delay)
    }

    /// Creates new immediate event for itself with specified payload.
    pub fn emit_self_now<T>(&mut self, data: T) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, self.id, 0.)
    }

    /// Creates new event with specified payload, source, destination and delay.
    pub fn emit_as<T>(&mut self, data: T, src: Id, dest: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, src, dest, delay)
    }

    /// Cancels the specified event.
    pub fn cancel_event(&mut self, id: EventId) {
        self.sim_state.borrow_mut().cancel_event(id);
    }

    /// Lookup component name by its identifier.
    pub fn lookup_name(&self, id: Id) -> String {
        self.names.borrow()[id as usize].clone()
    }
}
