//! Simulation configuration and execution.

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

/// Represents a simulation, provides methods for its configuration and execution.
pub struct Simulation {
    sim_state: Rc<RefCell<SimulationState>>,
    name_to_id: HashMap<String, Id>,
    names: Rc<RefCell<Vec<String>>>,
    handlers: Vec<Option<Rc<RefCell<dyn EventHandler>>>>,
}

impl Simulation {
    /// Creates a new simulation with specified random seed.
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

    /// Returns the identifier of component by its name.
    ///
    /// Panics if component with such name does not exist.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dslab_core::Simulation;
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp_ctx = sim.create_context("comp");
    /// let comp_id = sim.lookup_id(comp_ctx.name());
    /// assert_eq!(comp_id, 0);
    /// ```
    ///
    /// ```should_panic
    /// use dslab_core::Simulation;
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp_ctx = sim.create_context("comp");
    /// let comp1_id = sim.lookup_id("comp1");
    /// ```
    pub fn lookup_id(&self, name: &str) -> Id {
        *self.name_to_id.get(name).unwrap()
    }

    /// Returns the name of component by its identifier.
    ///
    /// Panics if component with such Id does not exist.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dslab_core::Simulation;
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp_ctx = sim.create_context("comp");
    /// let comp_name = sim.lookup_name(comp_ctx.id());
    /// assert_eq!(comp_name, "comp");
    /// ```
    ///
    /// ```should_panic
    /// use dslab_core::Simulation;
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp_ctx = sim.create_context("comp");
    /// let comp_name = sim.lookup_name(comp_ctx.id() + 1);
    /// ```
    pub fn lookup_name(&self, id: Id) -> String {
        self.names.borrow()[id as usize].clone()
    }

    /// Creates a new simulation context with specified name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dslab_core::Simulation;
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp_ctx = sim.create_context("comp");
    /// assert_eq!(comp_ctx.id(), 0); // component ids are assigned sequentially starting from 0
    /// assert_eq!(comp_ctx.name(), "comp");
    /// ```
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
            "[{:.3} {} simulation] Created context: {}",
            self.time(),
            crate::log::get_colored("DEBUG", colored::Color::Blue),
            json!({"name": ctx.name(), "id": ctx.id()})
        );
        ctx
    }

    /// Registers the event handler implementation for component with specified name, returns the component Id.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::{cast, Event, EventHandler, Simulation, SimulationContext};
    ///
    /// #[derive(Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// pub struct Component {
    ///     ctx: SimulationContext,
    /// }
    ///
    /// impl EventHandler for Component {
    ///     fn on(&mut self, event: Event) {
    ///         cast!(match event.data {
    ///             SomeEvent { } => {
    ///                 // some event processing logic...
    ///             }
    ///         })
    ///
    ///    }
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp_ctx = sim.create_context("comp");
    /// assert_eq!(comp_ctx.id(), 0);
    /// let comp = Rc::new(RefCell::new(Component { ctx: comp_ctx }));
    /// // When the handler is registered for component with existing context,
    /// // the component Id assigned in create_context() is reused.
    /// let comp_id = sim.add_handler("comp", comp);
    /// assert_eq!(comp_id, 0);
    /// ```
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::{cast, Event, EventHandler, Simulation, SimulationContext};
    ///
    /// #[derive(Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// pub struct Component {
    /// }
    ///
    /// impl EventHandler for Component {
    ///     fn on(&mut self, event: Event) {
    ///         cast!(match event.data {
    ///             SomeEvent { } => {
    ///                 // some event processing logic...
    ///             }
    ///         })
    ///
    ///    }
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp = Rc::new(RefCell::new(Component { }));
    /// // It is possible to register event handler for component without context.
    /// // In this case the component Id is assigned inside add_handler().
    /// let comp_id = sim.add_handler("comp", comp);
    /// assert_eq!(comp_id, 0);
    /// ```
    ///
    /// ```compile_fail
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// pub struct Component {
    ///     ctx: SimulationContext,
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp_ctx = sim.create_context("comp");
    /// let comp = Rc::new(RefCell::new(Component { ctx: comp_ctx }));
    /// // should not compile because Component does not implement EventHandler trait
    /// let comp_id = sim.add_handler("comp", comp);
    /// ```
    pub fn add_handler<S>(&mut self, name: S, handler: Rc<RefCell<dyn EventHandler>>) -> Id
    where
        S: AsRef<str>,
    {
        let id = self.register(name.as_ref());
        self.handlers[id as usize] = Some(handler);
        debug!(
            target: "simulation",
            "[{:.3} {} simulation] Added handler: {}",
            self.time(),
            crate::log::get_colored("DEBUG", colored::Color::Blue),
            json!({"name": name.as_ref(), "id": id})
        );
        id
    }

    /// Returns the current simulation time.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::Simulation;
    ///
    /// #[derive(Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp_ctx = sim.create_context("comp");
    /// assert_eq!(sim.time(), 0.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.2);
    /// sim.step();
    /// assert_eq!(sim.time(), 1.2);
    /// ```
    pub fn time(&self) -> f64 {
        self.sim_state.borrow().time()
    }

    /// Performs a single step through the simulation.
    ///
    /// Takes the next event from the queue, advances the simulation time to event time and tries to process it
    /// by invoking the [`EventHandler::on()`](crate::EventHandler::on()) method of the corresponding event handler.
    /// If there is no handler registered for component with Id `event.dest`, logs the undelivered event and discards it.
    ///
    /// Returns `true` if some pending event was found (no matter was it properly processed or not) and `false`
    /// otherwise. The latter means that there are no pending events, so no progress can be made.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::Simulation;
    ///
    /// #[derive(Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp_ctx = sim.create_context("comp");
    /// assert_eq!(sim.time(), 0.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.2);
    /// let mut status = sim.step();
    /// assert_eq!(status, true);
    /// assert_eq!(sim.time(), 1.2);
    /// status = sim.step();
    /// assert_eq!(status, false);
    /// ```
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

    /// Performs the specified number of steps through the simulation.
    ///
    /// This is a convenient wrapper around [`step()`](Self::step()), which invokes this method until the specified number of
    /// steps is made, or `false` is returned (no more pending events).
    ///
    /// Returns `true` if there could be more pending events and `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::Simulation;
    ///
    /// #[derive(Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp_ctx = sim.create_context("comp");
    /// assert_eq!(sim.time(), 0.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.2);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.3);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.4);
    /// let mut status = sim.steps(2);
    /// assert_eq!(status, true);
    /// assert_eq!(sim.time(), 1.3);
    /// status = sim.steps(2);
    /// assert_eq!(status, false);
    /// assert_eq!(sim.time(), 1.4);
    /// ```
    pub fn steps(&mut self, step_count: u64) -> bool {
        for _ in 0..step_count {
            if !self.step() {
                return false;
            }
        }
        true
    }

    /// Steps through the simulation until there are no pending events left.
    ///
    /// This is a convenient wrapper around [`step()`](Self::step()), which invokes this method until `false` is returned.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::Simulation;
    ///
    /// #[derive(Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp_ctx = sim.create_context("comp");
    /// assert_eq!(sim.time(), 0.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.2);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.3);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.4);
    /// sim.step_until_no_events();
    /// assert_eq!(sim.time(), 1.4);
    /// ```
    pub fn step_until_no_events(&mut self) {
        while self.step() {}
    }

    /// Steps through the simulation with duration limit.
    ///
    /// This is a convenient wrapper around [`step()`](Self::step()), which invokes this method until the next event
    /// time is above the specified threshold (`current_time + duration`) or there are no pending events left.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::Simulation;
    ///
    /// #[derive(Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp_ctx = sim.create_context("comp");
    /// assert_eq!(sim.time(), 0.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 2.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 3.5);
    /// sim.step_for_duration(1.5);
    /// assert_eq!(sim.time(), 1.0); // time equals to the first event
    /// sim.step_for_duration(0.1);
    /// assert_eq!(sim.time(), 1.0); // no progress is made
    /// sim.step_for_duration(3.0);
    /// assert_eq!(sim.time(), 3.5); // time equals to the last event
    /// ```
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

    /// Returns the total number of created events.
    ///
    /// Note that cancelled events are also counted here.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::Simulation;
    ///
    /// #[derive(Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp_ctx = sim.create_context("comp");
    /// assert_eq!(sim.time(), 0.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 2.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 3.5);
    /// assert_eq!(sim.event_count(), 3);
    /// ```
    pub fn event_count(&self) -> u64 {
        self.sim_state.borrow().event_count()
    }
}
