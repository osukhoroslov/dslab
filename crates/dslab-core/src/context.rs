//! Accessing simulation from components.

use std::cell::RefCell;
use std::rc::Rc;

use rand::distributions::uniform::{SampleRange, SampleUniform};
use rand::prelude::Distribution;

use crate::async_enabled;
use crate::component::Id;
use crate::event::{Event, EventData, EventId};
use crate::state::SimulationState;

async_enabled! {
    use std::any::TypeId;
    use std::any::type_name;

    use futures::Future;

    use crate::async_core::shared_state::{AwaitEventSharedState, AwaitKey, EventFuture};
    use crate::async_core::await_details::{AwaitResult, DetailsKey};
}

/// A facade for accessing the simulation state and producing events from simulation components.
#[derive(Clone)]
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
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp_ctx = sim.create_context("comp");
    /// let comp_id = comp_ctx.id();
    /// assert_eq!(comp_id, 0); // component ids are assigned sequentially starting from 0
    /// ```
    pub fn id(&self) -> Id {
        self.id
    }

    /// Returns the name of component associated with this context.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp_ctx = sim.create_context("comp");
    /// let comp_name = comp_ctx.name();
    /// assert_eq!(comp_name, "comp");
    /// ```
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the current simulation time.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp_ctx = sim.create_context("comp");
    /// let time = comp_ctx.time();
    /// assert_eq!(time, 0.0);
    /// ```
    pub fn time(&self) -> f64 {
        self.sim_state.borrow().time()
    }

    /// Returns a random float in the range _[0, 1)_
    /// using the simulation-wide random number generator.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp_ctx = sim.create_context("comp");
    /// let f: f64 = comp_ctx.rand();
    /// assert!(f >= 0.0 && f < 1.0);
    /// ```
    pub fn rand(&self) -> f64 {
        self.sim_state.borrow_mut().rand()
    }

    /// Returns a random number in the specified range
    /// using the simulation-wide random number generator.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp_ctx = sim.create_context("comp");
    /// let n: u32 = comp_ctx.gen_range(1..=10);
    /// assert!(n >= 1 && n <= 10);
    /// let f: f64 = comp_ctx.gen_range(0.1..0.5);
    /// assert!(f >= 0.1 && f < 0.5);
    /// ```
    pub fn gen_range<T, R>(&self, range: R) -> T
    where
        T: SampleUniform,
        R: SampleRange<T>,
    {
        self.sim_state.borrow_mut().gen_range(range)
    }

    /// Returns a random value from the specified distribution
    /// using the simulation-wide random number generator.
    pub fn sample_from_distribution<T, Dist: Distribution<T>>(&self, dist: &Dist) -> T {
        self.sim_state.borrow_mut().sample_from_distribution(dist)
    }

    /// Returns a random alphanumeric string of specified length
    /// using the simulation-wide random number generator.
    pub fn random_string(&self, len: usize) -> String {
        self.sim_state.borrow_mut().random_string(len)
    }

    /// Creates new event with specified payload, destination and delay, returns event id.
    ///
    /// The event time will be `current_time + delay`.
    /// It is not allowed to create events before the current simulation time, so `delay` should be non-negative.
    ///
    /// The event source will be equal to [`id`](Self::id()).
    /// See [`emit_as()`](Self::emit_as()) if you want to emit event on behalf of some other component.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::{cast, Event, EventHandler, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    ///     some_field: u32,
    /// }
    ///
    /// pub struct Component {
    ///     ctx: SimulationContext,
    /// }
    ///
    /// impl EventHandler for Component {
    ///     fn on(&mut self, event: Event) {
    ///         cast!(match event.data {
    ///             SomeEvent { some_field } => {
    ///                 assert_eq!(self.ctx.time(), 1.2);
    ///                 assert_eq!(event.time, 1.2);
    ///                 assert_eq!(event.id, 0);
    ///                 assert_eq!(some_field, 16);
    ///             }
    ///         })
    ///
    ///    }
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp1_ctx = sim.create_context("comp1");
    /// let mut comp2_ctx = sim.create_context("comp2");
    /// let comp2_id = sim.add_handler("comp2", Rc::new(RefCell::new(Component { ctx: comp2_ctx })));
    /// let event_id = comp1_ctx.emit(SomeEvent{ some_field: 16 }, comp2_id, 1.2);
    /// assert_eq!(event_id, 0); // events ids are assigned sequentially starting from 0
    /// sim.step();
    /// assert_eq!(sim.time(), 1.2);
    /// ```
    ///
    /// ```should_panic
    /// use serde::Serialize;
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp1_ctx = sim.create_context("comp1");
    /// let mut comp2_ctx = sim.create_context("comp2");
    /// comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), -1.0); // will panic because of negative delay
    /// ```
    pub fn emit<T>(&self, data: T, dst: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, dst, delay)
    }

    /// This and all other `emit_ordered...` functions are special variants of normal `emit_...` functions
    /// that allow adding events to ordered event deque instead of heap, which may improve simulation performance.
    ///
    /// Ordered events should be emitted in non-decreasing order of their time, otherwise the simulation will panic.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp1_ctx = sim.create_context("comp1");
    /// let mut comp2_ctx = sim.create_context("comp2");
    /// comp1_ctx.emit_ordered(SomeEvent{}, comp2_ctx.id(), 1.0);
    /// comp1_ctx.emit_ordered(SomeEvent{}, comp2_ctx.id(), 1.0);
    /// comp1_ctx.emit_ordered(SomeEvent{}, comp2_ctx.id(), 2.0);
    /// sim.step();
    /// assert_eq!(sim.time(), 1.0);
    /// sim.step();
    /// assert_eq!(sim.time(), 1.0);
    /// sim.step();
    /// assert_eq!(sim.time(), 2.0);
    /// ```
    ///
    /// ```should_panic
    /// use serde::Serialize;
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp1_ctx = sim.create_context("comp1");
    /// let mut comp2_ctx = sim.create_context("comp2");
    /// comp1_ctx.emit_ordered(SomeEvent{}, comp2_ctx.id(), 2.0);
    /// comp1_ctx.emit_ordered(SomeEvent{}, comp2_ctx.id(), 1.0); // will panic because of broken time order
    /// ```
    pub fn emit_ordered<T>(&self, data: T, dst: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_ordered_event(data, self.id, dst, delay)
    }

    /// Checks whether it is safe to emit an ordered event with the specified delay.
    ///
    /// The time of new event must be not less than the time of the previously emitted ordered event.   
    ///
    /// Returns true if this condition holds and false otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp1_ctx = sim.create_context("comp1");
    /// let mut comp2_ctx = sim.create_context("comp2");
    /// comp1_ctx.emit_ordered(SomeEvent{}, comp2_ctx.id(), 1.0);
    /// assert!(comp1_ctx.can_emit_ordered(1.0)); // 1.0 == 1.0
    /// assert!(comp1_ctx.can_emit_ordered(1.1)); // 1.1 > 1.0
    /// assert!(!comp1_ctx.can_emit_ordered(0.9)); // 0.9 < 1.0
    /// comp1_ctx.emit_ordered(SomeEvent{}, comp2_ctx.id(), 1.5);
    /// assert!(!comp1_ctx.can_emit_ordered(1.0)); // 1.0 < 1.5
    /// sim.step();
    /// assert_eq!(sim.time(), 1.0);
    /// assert!(comp1_ctx.can_emit_ordered(1.0)); // 2.0 > 1.5
    /// assert!(!comp1_ctx.can_emit_ordered(0.3)); // 1.3 < 1.5
    /// ```
    pub fn can_emit_ordered(&self, delay: f64) -> bool {
        self.sim_state.borrow().can_add_ordered_event(delay)
    }

    /// Creates new immediate (zero-delay) event with specified payload and destination, returns event id.
    ///
    /// This is a shorthand for [`emit()`](Self::emit()) with zero delay.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::{cast, Event, EventHandler, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    ///     some_field: u32,
    /// }
    ///
    /// pub struct Component {
    ///     ctx: SimulationContext,
    /// }
    ///
    /// impl EventHandler for Component {
    ///     fn on(&mut self, event: Event) {
    ///         cast!(match event.data {
    ///             SomeEvent { some_field } => {
    ///                 assert_eq!(self.ctx.time(), 0.0);
    ///                 assert_eq!(event.time, 0.0);
    ///                 assert_eq!(event.id, 0);
    ///                 assert_eq!(some_field, 16);
    ///             }
    ///         })
    ///
    ///    }
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp1_ctx = sim.create_context("comp1");
    /// let mut comp2_ctx = sim.create_context("comp2");
    /// let comp2_id = sim.add_handler("comp2", Rc::new(RefCell::new(Component { ctx: comp2_ctx })));
    /// let event_id = comp1_ctx.emit_now(SomeEvent{ some_field: 16 }, comp2_id);
    /// assert_eq!(event_id, 0); // events ids are assigned sequentially starting from 0
    /// sim.step();
    /// assert_eq!(sim.time(), 0.0);
    /// ```
    pub fn emit_now<T>(&self, data: T, dst: Id) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, dst, 0.)
    }

    /// See [`Self::emit_ordered`].
    pub fn emit_ordered_now<T>(&self, data: T, dst: Id) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_ordered_event(data, self.id, dst, 0.)
    }

    /// Creates new event for itself with specified payload and delay, returns event id.
    ///
    /// This is a shorthand for [`emit()`](Self::emit()) with event destination equals [`id`](Self::id()).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::{cast, Event, EventHandler, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    ///     some_field: u32,
    /// }
    ///
    /// pub struct Component {
    ///     ctx: SimulationContext,
    /// }
    ///
    /// impl Component {
    ///     fn start(&mut self) {
    ///         self.ctx.emit_self(SomeEvent{ some_field: 16 }, 6.4);
    ///     }
    /// }
    ///
    /// impl EventHandler for Component {
    ///     fn on(&mut self, event: Event) {
    ///         cast!(match event.data {
    ///             SomeEvent { some_field } => {
    ///                 assert_eq!(self.ctx.time(), 6.4);
    ///                 assert_eq!(event.time, 6.4);
    ///                 assert_eq!(event.id, 0);
    ///                 assert_eq!(event.src, self.ctx.id());
    ///                 assert_eq!(some_field, 16);
    ///             }
    ///         })
    ///
    ///    }
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp1 = Rc::new(RefCell::new(Component { ctx: sim.create_context("comp1") }));
    /// let comp1_id = sim.add_handler("comp1", comp1.clone());
    /// comp1.borrow_mut().start();
    /// sim.step();
    /// assert_eq!(sim.time(), 6.4);
    /// ```
    pub fn emit_self<T>(&self, data: T, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, self.id, delay)
    }

    /// See [`Self::emit_ordered`].
    pub fn emit_ordered_self<T>(&self, data: T, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state
            .borrow_mut()
            .add_ordered_event(data, self.id, self.id, delay)
    }

    /// Creates new immediate event for itself with specified payload, returns event id.
    ///
    /// This is a shorthand for [`emit()`](Self::emit()) with event destination equals [`id`](Self::id())
    /// and zero delay.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::{cast, Event, EventHandler, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    ///     some_field: u32,
    /// }
    ///
    /// pub struct Component {
    ///     ctx: SimulationContext,
    /// }
    ///
    /// impl Component {
    ///     fn start(&mut self) {
    ///         self.ctx.emit_self_now(SomeEvent{ some_field: 16 });
    ///     }
    /// }
    ///
    /// impl EventHandler for Component {
    ///     fn on(&mut self, event: Event) {
    ///         cast!(match event.data {
    ///             SomeEvent { some_field } => {
    ///                 assert_eq!(self.ctx.time(), 0.0);
    ///                 assert_eq!(event.time, 0.0);
    ///                 assert_eq!(event.id, 0);
    ///                 assert_eq!(event.src, self.ctx.id());
    ///                 assert_eq!(some_field, 16);
    ///             }
    ///         })
    ///
    ///    }
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp1 = Rc::new(RefCell::new(Component { ctx: sim.create_context("comp1") }));
    /// let comp1_id = sim.add_handler("comp1", comp1.clone());
    /// comp1.borrow_mut().start();
    /// sim.step();
    /// assert_eq!(sim.time(), 0.0);
    /// ```
    pub fn emit_self_now<T>(&self, data: T) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, self.id, self.id, 0.)
    }

    /// See [`Self::emit_ordered`].
    pub fn emit_ordered_self_now<T>(&self, data: T) -> EventId
    where
        T: EventData,
    {
        self.sim_state
            .borrow_mut()
            .add_ordered_event(data, self.id, self.id, 0.)
    }

    /// Creates new event with specified payload, source, destination and delay, returns event id.
    ///
    /// This is an extended version of [`emit()`](Self::emit()) for special cases when the event should be emitted
    /// on behalf of another component.
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::{cast, Event, EventHandler, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    ///     some_field: u32,
    /// }
    ///
    /// pub struct Component {
    ///     ctx: SimulationContext,
    /// }
    ///
    /// impl EventHandler for Component {
    ///     fn on(&mut self, event: Event) {
    ///         cast!(match event.data {
    ///             SomeEvent { some_field } => {
    ///                 assert_eq!(self.ctx.time(), 2.4);
    ///                 assert_eq!(event.time, 2.4);
    ///                 assert_eq!(event.id, 0);
    ///                 assert_eq!(event.src, 0);
    ///                 assert_eq!(self.ctx.id(), 1);
    ///                 assert_eq!(some_field, 8);
    ///             }
    ///         })
    ///
    ///    }
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp1 = Rc::new(RefCell::new(Component { ctx: sim.create_context("comp1") }));
    /// let comp1_id = sim.add_handler("comp1", comp1);
    /// let comp2 = Rc::new(RefCell::new(Component { ctx: sim.create_context("comp2") }));
    /// let comp2_id = sim.add_handler("comp2", comp2);
    /// let mut other_ctx = sim.create_context("other");
    /// other_ctx.emit_as(SomeEvent{ some_field: 8 }, comp1_id, comp2_id, 2.4);
    /// sim.step();
    /// assert_eq!(sim.time(), 2.4);
    /// ```
    pub fn emit_as<T>(&self, data: T, src: Id, dst: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_event(data, src, dst, delay)
    }

    /// See [`Self::emit_ordered`].
    pub fn emit_ordered_as<T>(&self, data: T, src: Id, dst: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_ordered_event(data, src, dst, delay)
    }

    /// Cancels the specified event.
    ///
    /// Use [`EventId`](crate::event::EventId) obtained when creating the event to cancel it.
    /// Note that already processed events cannot be cancelled.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp1_ctx = sim.create_context("comp1");
    /// let mut comp2_ctx = sim.create_context("comp2");
    /// let event1 = comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), 1.0);
    /// let event2 = comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), 2.0);
    /// sim.step();
    /// comp1_ctx.cancel_event(event2);
    /// sim.step_until_no_events();
    /// assert_eq!(sim.time(), 1.0);
    /// ```
    pub fn cancel_event(&self, id: EventId) {
        self.sim_state.borrow_mut().cancel_event(id);
    }

    /// Cancels events that satisfy the given predicate function.
    ///
    /// Note that already processed events cannot be cancelled.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::{Event, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp1_ctx = sim.create_context("comp1");
    /// let mut comp2_ctx = sim.create_context("comp2");
    /// let event1 = comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), 1.0);
    /// let event2 = comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), 2.0);
    /// let event2 = comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), 3.0);
    /// comp1_ctx.cancel_events(|e| e.id < 2);
    /// sim.step();
    /// assert_eq!(sim.time(), 3.0);
    /// ```
    pub fn cancel_events<F>(&self, pred: F)
    where
        F: Fn(&Event) -> bool,
    {
        self.sim_state.borrow_mut().cancel_events(pred);
    }

    /// Same as [`Self::cancel_events`], but ignores events added through `emit_ordered_...` methods.
    pub fn cancel_heap_events<F>(&self, pred: F)
    where
        F: Fn(&Event) -> bool,
    {
        self.sim_state.borrow_mut().cancel_heap_events(pred);
    }

    /// Returns component name by its identifier.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::{cast, Event, EventHandler, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
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
    ///                 // look up the name of event source
    ///                 let src_name = self.ctx.lookup_name(event.src);
    ///                 assert_eq!(src_name, "comp1");
    ///             }
    ///         })
    ///
    ///    }
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp1_ctx = sim.create_context("comp1");
    /// let mut comp2_ctx = sim.create_context("comp2");
    /// let comp2_id = sim.add_handler("comp2", Rc::new(RefCell::new(Component { ctx: comp2_ctx })));
    /// comp1_ctx.emit(SomeEvent{}, comp2_id, 1.0);
    /// sim.step();
    /// ```
    pub fn lookup_name(&self, id: Id) -> String {
        self.names.borrow()[id as usize].clone()
    }

    async_enabled! {
        /// Spawns a background async task.
        ///
        /// # Examples
        ///
        /// ```rust
        /// use std::rc::Rc;
        /// use std::cell::RefCell;
        ///
        /// use serde::Serialize;
        ///
        /// use dslab_core::{cast, Simulation, SimulationContext, Event, EventHandler};
        ///
        /// struct Client {
        ///     ctx: SimulationContext,
        /// }
        ///
        /// #[derive(Clone, Serialize)]
        /// struct Start {
        ///     jobs: u32,
        /// }
        ///
        /// impl Client {
        ///     fn on_start(&self, jobs: u32) {
        ///        for i in 1..=jobs {
        ///             self.ctx.spawn(self.step_waiting(i));
        ///        }
        ///     }
        ///
        ///     async fn step_waiting(&self, num_steps: u32) {
        ///         for _i in 0..num_steps {
        ///             self.ctx.async_sleep(1.).await;
        ///         }
        ///     }
        /// }
        ///
        /// impl EventHandler for Client {
        ///     fn on(&mut self, event: Event) {
        ///         cast!(match event.data {
        ///             Start { jobs } => {
        ///                 self.on_start(jobs);
        ///             }
        ///         })
        ///     }
        /// }
        ///
        /// let mut sim = Simulation::new(42);
        ///
        /// let client_ctx = sim.create_context("client");
        /// let client_id = client_ctx.id();
        /// let client = Rc::new(RefCell::new(Client {ctx: client_ctx }));
        ///
        /// sim.add_handler("client", client);
        ///
        /// let root_ctx = sim.create_context("root");
        /// root_ctx.emit(Start{ jobs: 10 }, client_id, 10.);
        ///
        /// sim.step_until_no_events();
        ///
        /// assert_eq!(sim.time(), 20.);
        /// ```
        ///
        /// ```should_panic
        /// use dslab_core::{Simulation, SimulationContext};
        ///
        /// struct Client {
        ///     ctx: SimulationContext,
        /// }
        ///
        /// impl Client {
        ///     fn start(&self, jobs: u32) {
        ///        for i in 1..=jobs {
        ///             self.ctx.spawn(self.step_waiting(i));
        ///        }
        ///     }
        ///
        ///     async fn step_waiting(&self, num_steps: u32) {
        ///         for _i in 0..num_steps {
        ///             self.ctx.async_sleep(1.).await;
        ///         }
        ///     }
        /// }
        ///
        /// let mut sim = Simulation::new(42);
        /// let mut client = Client { ctx: sim.create_context("client") };
        ///
        /// // Panics because spawning async tasks by a component without event handler
        /// // is prohibited due to safety reasons.
        /// // Register Client via Simulation::add_handler as in the previous example.
        /// client.start(10);
        /// ```
        pub fn spawn(&self, future: impl Future<Output = ()>) {
            self.sim_state.borrow_mut().spawn_component(self.id(), future);
        }

        /// Waits until `duration` has elapsed.
        ///
        /// # Examples
        ///
        /// ```rust
        /// use futures::{stream::FuturesUnordered, StreamExt};
        ///
        /// use dslab_core::Simulation;
        ///
        /// let mut sim = Simulation::new(42);
        ///
        /// let ctx = sim.create_context("client");
        ///
        /// sim.spawn(async move {
        ///     let initial_time = ctx.time();
        ///     ctx.async_sleep(5.).await;
        ///
        ///     let mut expected_time = initial_time + 5.;
        ///     assert_eq!(expected_time, ctx.time());
        ///
        ///     let mut concurrent_futures = FuturesUnordered::new();
        ///     for i in 1..=10 {
        ///         concurrent_futures.push(ctx.async_sleep(i as f64));
        ///     }
        ///
        ///     while let Some(_) = concurrent_futures.next().await {
        ///         expected_time += 1.;
        ///         assert_eq!(expected_time, ctx.time());
        ///     }
        /// });
        ///
        /// sim.step_until_no_events();
        /// assert_eq!(15., sim.time());
        /// ```
        pub async fn async_sleep(&self, duration: f64) {
            assert!(duration >= 0., "duration must be a positive value");

            let future = self.sim_state.borrow_mut().wait_for(self.id, duration);
            future.await;
        }

        /// Waits for any event of type `T` from component `src`.
        ///
        /// # Examples
        ///
        /// ```rust
        /// use serde::Serialize;
        ///
        /// use dslab_core::Simulation;
        /// use dslab_core::async_core::AwaitResult;
        ///
        /// #[derive(Clone, Serialize)]
        /// struct Message{
        ///     payload: u32,
        /// }
        ///
        /// let mut sim = Simulation::new(42);
        /// let client_ctx = sim.create_context("client");
        /// let client_id = client_ctx.id();
        /// let root_ctx = sim.create_context("root");
        /// let root_id = root_ctx.id();
        ///
        /// sim.spawn(async move {
        ///     root_ctx.emit(Message{ payload: 42 }, client_id, 50.);
        /// });
        ///
        /// sim.spawn(async move {
        ///     let (e, data) = client_ctx.async_wait_event::<Message>(root_id).await;
        ///     assert_eq!(e.src, root_id);
        ///     assert_eq!(data.payload, 42);
        /// });
        ///
        /// sim.step_until_no_events();
        /// assert_eq!(sim.time(), 50.);
        /// ```
        pub fn async_wait_event<T>(&self, src: Id) -> EventFuture<T>
        where
            T: EventData,
        {
            self.async_wait_for_event_to::<T>(src, self.id)
        }

        /// Waits for an event of type T from self.
        ///
        /// # Examples
        ///
        /// ```rust
        /// use serde::Serialize;
        ///
        /// use dslab_core::Simulation;
        ///
        /// #[derive(Clone, Serialize)]
        /// struct SomeEvent {
        ///     payload: u32,
        /// }
        ///
        /// let mut sim = Simulation::new(42);
        ///
        /// let client_ctx = sim.create_context("client");
        ///
        /// sim.spawn(async move {
        ///     client_ctx.emit_self(SomeEvent{payload: 23}, 10.);
        ///
        ///     let (e, data) = client_ctx.async_wait_event_from_self::<SomeEvent>().await;
        ///     assert_eq!(data.payload, 23);
        ///     assert_eq!(client_ctx.time(), 10.)
        /// });
        ///
        /// sim.step_until_no_events();
        /// assert_eq!(sim.time(), 10.);
        /// ```
        pub fn async_wait_event_from_self<T>(&self) -> EventFuture<T>
        where
            T: EventData,
        {
            self.async_wait_for_event_to::<T>(self.id, self.id)
        }

        /// Register the function for a type of EventData to get await details to further call
        /// Self::async_wait_event_detailed or Self::async_wait_event_detailed_for
        ///
        /// See [`Self::async_wait_event_detailed_for`].
        pub fn register_details_getter_for<T: EventData>(&self, details_getter: fn(&dyn EventData) -> DetailsKey) {
            self.sim_state
                .borrow_mut()
                .register_details_getter_for::<T>(details_getter);
        }

        /// Async wait for event of type T from src component with details flag.
        /// See [`Self::async_wait_event_detailed_for`].
        pub fn async_wait_event_detailed<T>(&self, src: Id, details: DetailsKey) -> EventFuture<T>
        where
            T: EventData,
        {
            self.async_wait_for_event_detailed_to::<T>(src, self.id, details)
        }

        /// Async detailed handling event from self.
        /// See [`Self::async_wait_event_from_self`, `Self::async_wait_event_detailed_for`]
        pub fn async_wait_event_detailed_from_self<T>(&self, details: DetailsKey) -> EventFuture<T>
        where
            T: EventData,
        {
            self.async_wait_for_event_detailed_to::<T>(self.id, self.id, details)
        }

        fn async_wait_for_event_to<T>(&self, src: Id, dst: Id) -> EventFuture<T>
        where
            T: EventData,
        {
            assert!(
                self.sim_state.borrow().get_details_getter(TypeId::of::<T>()).is_none(),
                "try to async handle event that has detailed key handling, use async details handlers"
            );

            let await_key = AwaitKey::new::<T>(src, dst);

            self.create_event_future(await_key)
        }

        fn async_wait_for_event_detailed_to<T>(
            &self,
            src: Id,
            dst: Id,
            details: DetailsKey,
        ) -> EventFuture<T>
        where
            T: EventData,
        {
            assert!(
                self.sim_state.borrow().get_details_getter(TypeId::of::<T>()).is_some(),
                "simulation does not have details getter for type {}, register it before using async_detailed getters",
                type_name::<T>()
            );

            let await_key = AwaitKey::new_with_details::<T>(src, dst, details);

            self.create_event_future(await_key)
        }

        fn create_event_future<T>(&self, await_key: AwaitKey) -> EventFuture<T>
        where
            T: EventData,
        {
            let state = Rc::new(RefCell::new(AwaitEventSharedState::<T>::new(await_key.to)));
            state.borrow_mut().shared_content = AwaitResult::timeout_with(await_key.from, await_key.to);

            self.sim_state
                .borrow_mut()
                .add_awaiter_handler(await_key, state.clone());

            EventFuture { state, sim_state: self.sim_state.clone() }
        }
    }
}
