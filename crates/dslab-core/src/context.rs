//! Accessing simulation from components.

use std::cell::RefCell;
use std::rc::Rc;

use rand::distributions::uniform::{SampleRange, SampleUniform};
use rand::prelude::Distribution;

use crate::async_mode_enabled;
use crate::component::Id;
use crate::event::{Event, EventData, EventId};
use crate::state::SimulationState;

async_mode_enabled!(
    use std::any::TypeId;
    use std::any::type_name;

    use futures::Future;

    use crate::async_mode::event_future::EventFuture;
    use crate::async_mode::EventKey;
    use crate::async_mode::timer_future::TimerFuture;
);

/// A facade for accessing the simulation state and producing events from simulation components.
pub struct SimulationContext {
    id: Id,
    name: String,
    sim_state: Rc<RefCell<SimulationState>>,
}

impl SimulationContext {
    pub(crate) fn new(id: Id, name: &str, sim_state: Rc<RefCell<SimulationState>>) -> Self {
        Self {
            id,
            name: name.to_owned(),
            sim_state,
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
    /// The event source will be equal to [`id`](Self::id).
    /// See [`emit_as`](Self::emit_as) if you want to emit event on behalf of some other component.
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
    /// struct SomeEvent {
    ///     some_field: u32,
    /// }
    ///
    /// struct Component {
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
    /// struct SomeEvent {
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
    /// struct SomeEvent {
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
    /// struct SomeEvent {
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
    /// struct SomeEvent {
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
    /// This is a shorthand for [`emit`](Self::emit) with zero delay.
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
    /// struct SomeEvent {
    ///     some_field: u32,
    /// }
    ///
    /// struct Component {
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

    /// See [`emit_ordered`](Self::emit_ordered).
    pub fn emit_ordered_now<T>(&self, data: T, dst: Id) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_ordered_event(data, self.id, dst, 0.)
    }

    /// Creates new event for itself with specified payload and delay, returns event id.
    ///
    /// This is a shorthand for [`emit`](Self::emit) with event destination equals [`id`](Self::id).
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
    /// struct SomeEvent {
    ///     some_field: u32,
    /// }
    ///
    /// struct Component {
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
    /// This is a shorthand for [`emit`](Self::emit) with event destination equals [`id`](Self::id)
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
    /// struct SomeEvent {
    ///     some_field: u32,
    /// }
    ///
    /// struct Component {
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

    /// See [`emit_ordered`](Self::emit_ordered).
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
    /// This is an extended version of [`emit`](Self::emit) for special cases when the event should be emitted
    /// on behalf of another component.
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::{cast, Event, EventHandler, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// struct SomeEvent {
    ///     some_field: u32,
    /// }
    ///
    /// struct Component {
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

    /// See [`emit_ordered`](Self::emit_ordered).
    pub fn emit_ordered_as<T>(&self, data: T, src: Id, dst: Id, delay: f64) -> EventId
    where
        T: EventData,
    {
        self.sim_state.borrow_mut().add_ordered_event(data, src, dst, delay)
    }

    /// Cancels the specified event.
    ///
    /// Use [`EventId`] obtained when creating the event to cancel it.
    /// Note that already processed events cannot be cancelled.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::{Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// struct SomeEvent {
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
    /// struct SomeEvent {
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

    /// Same as [`cancel_events`](Self::cancel_events), but ignores events added through `emit_ordered_...` methods.
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
    /// struct SomeEvent {
    /// }
    ///
    /// struct Component {
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
        self.sim_state.borrow().lookup_name(id)
    }

    async_mode_enabled!(
        /// Spawns a new asynchronous task for component associated with this context.
        ///
        /// Passing component's state to asynchronous tasks can be achieved by using `Rc<Self>` instead of `&self` reference.
        /// Mutating the component's state by asynchronous tasks can be achieved by wrapping this state into `RefCell<_>`.
        /// In order to spawn asynchronous tasks, component is required to be [registered](crate::Simulation::add_static_handler)
        /// as [`StaticEventHandler`](crate::StaticEventHandler). See the examples below.
        ///
        /// # Examples
        ///
        /// ```rust
        /// use std::rc::Rc;
        /// use serde::Serialize;
        /// use dslab_core::{cast, Simulation, SimulationContext, Event, StaticEventHandler};
        ///
        /// #[derive(Clone, Serialize)]
        /// struct Start {
        ///     tasks: u32,
        /// }
        ///
        /// struct Component {
        ///     ctx: SimulationContext,
        /// }
        ///
        /// impl Component {
        ///     fn on_start(self: Rc<Self>, tasks: u32) {
        ///        for i in 1..=tasks {
        ///             self.ctx.spawn(self.clone().step_waiting(i));
        ///        }
        ///     }
        ///
        ///     async fn step_waiting(self: Rc<Self>, num_steps: u32) {
        ///         for _ in 0..num_steps {
        ///             self.ctx.sleep(1.).await;
        ///         }
        ///     }
        /// }
        ///
        /// impl StaticEventHandler for Component {
        ///     fn on(self: Rc<Self>, event: Event) {
        ///         cast!(match event.data {
        ///             Start { tasks } => {
        ///                 self.on_start(tasks);
        ///             }
        ///         })
        ///     }
        /// }
        ///
        /// let mut sim = Simulation::new(123);
        ///
        /// let comp_ctx = sim.create_context("comp");
        /// let comp_id = sim.add_static_handler("comp", Rc::new(Component {ctx: comp_ctx }));
        ///
        /// let root_ctx = sim.create_context("root");
        /// root_ctx.emit(Start { tasks: 10 }, comp_id, 10.);
        ///
        /// sim.step_until_no_events();
        ///
        /// assert_eq!(sim.time(), 20.);
        /// ```
        ///
        /// ```should_panic
        /// use std::rc::Rc;
        /// use dslab_core::{Simulation, SimulationContext};
        ///
        /// struct Component {
        ///     ctx: SimulationContext,
        /// }
        ///
        /// impl Component {
        ///     fn start(self: Rc<Self>, tasks: u32) {
        ///        for i in 1..=tasks {
        ///             self.ctx.spawn(self.clone().step_waiting(i));
        ///        }
        ///     }
        ///
        ///     async fn step_waiting(self: Rc<Self>, num_steps: u32) {
        ///         for _i in 0..num_steps {
        ///             self.ctx.sleep(1.).await;
        ///         }
        ///     }
        /// }
        ///
        /// let mut sim = Simulation::new(123);
        /// let mut comp = Rc::new(Component { ctx: sim.create_context("comp") });
        ///
        /// // Panics because spawning async tasks for component without event handler is prohibited
        /// // due to safety reasons.
        /// // Register Component via Simulation::add_static_handler as in the previous example.
        /// comp.start(10);
        /// ```
        ///
        /// ```compile_fail
        /// use std::rc::Rc;
        /// use std::cell::RefCell;
        /// use dslab_core::{Simulation, SimulationContext, Event, EventHandler};
        ///
        /// struct Component {
        ///     ctx: SimulationContext,
        ///     counter: u32,
        /// }
        ///
        /// impl Component {
        ///     fn on_start(&mut self, tasks: u32) {
        ///        for i in 1..=tasks {
        ///             // Compile fails because reference to self is used in the async task,
        ///             // which is not allowed because of 'static requirements on the spawned future.
        ///             // 1. To spawn 'static futures register this component as StaticEventHandler.
        ///             // 2. Use RefCell to wrap the mutable state and access it in the async task via RefCell::borrow_mut.
        ///             // See the next example for details.
        ///             self.ctx.spawn(self.increase_counter(i));
        ///        }
        ///     }
        ///
        ///     async fn increase_counter(&mut self, num_steps: u32) {
        ///         for _ in 0..num_steps {
        ///             self.ctx.sleep(1.).await;
        ///             self.counter += 1;
        ///         }
        ///     }
        /// }
        ///
        /// impl EventHandler for Component {
        ///     fn on(&mut self, event: Event) {}
        /// }
        ///
        /// let mut sim = Simulation::new(123);
        ///
        /// let comp_ctx = sim.create_context("comp");
        /// let comp = Rc::new(RefCell::new(Component {ctx: comp_ctx, counter: 0 }));
        /// sim.add_handler("comp", comp.clone());
        ///
        /// comp.borrow_mut().on_start(10);
        ///
        /// sim.step_until_no_events();
        /// ```
        ///
        /// ```rust
        /// use std::rc::Rc;
        /// use std::cell::RefCell;
        /// use dslab_core::{Simulation, SimulationContext, Event, StaticEventHandler};
        ///
        /// struct Component {
        ///     ctx: SimulationContext,
        ///     counter: RefCell<u32>,
        /// }
        ///
        /// impl Component {
        ///     fn on_start(self: Rc<Self>, tasks: u32) {
        ///        for i in 1..=tasks {
        ///             self.ctx.spawn(self.clone().increase_counter(i));
        ///        }
        ///     }
        ///
        ///     async fn increase_counter(self: Rc<Self>, num_steps: u32) {
        ///         for _ in 0..num_steps {
        ///             self.ctx.sleep(1.).await;
        ///             *self.counter.borrow_mut() += 1;
        ///         }
        ///     }
        /// }
        ///
        /// impl StaticEventHandler for Component {
        ///     fn on(self: Rc<Self>, event: Event) {}
        /// }
        ///
        /// let mut sim = Simulation::new(123);
        ///
        /// let comp_ctx = sim.create_context("comp");
        /// let comp = Rc::new(Component {ctx: comp_ctx, counter: RefCell::new(0) });
        /// sim.add_static_handler("comp", comp.clone());
        ///
        /// comp.clone().on_start(10);
        ///
        /// sim.step_until_no_events();
        ///
        /// assert_eq!(sim.time(), 10.);
        /// // 1 + 2 + 3 + ... + 10 = 55
        /// assert_eq!(*comp.counter.borrow(), 55);
        /// ```
        pub fn spawn(&self, future: impl Future<Output = ()> + 'static) {
            self.sim_state.borrow_mut().spawn_component(self.id(), future);
        }

        /// Waits (asynchronously) until `duration` seconds have elapsed.
        ///
        /// # Examples
        ///
        /// ```rust
        /// use futures::{stream::FuturesUnordered, StreamExt};
        /// use dslab_core::Simulation;
        ///
        /// let mut sim = Simulation::new(123);
        ///
        /// let ctx = sim.create_context("comp");
        ///
        /// sim.spawn(async move {
        ///     let initial_time = ctx.time();
        ///     ctx.sleep(5.).await;
        ///
        ///     let mut expected_time = initial_time + 5.;
        ///     assert_eq!(expected_time, ctx.time());
        ///
        ///     let mut futures = FuturesUnordered::new();
        ///     for i in 1..=10 {
        ///         futures.push(ctx.sleep(i as f64));
        ///     }
        ///
        ///     while let Some(_) = futures.next().await {
        ///         expected_time += 1.;
        ///         assert_eq!(expected_time, ctx.time());
        ///     }
        /// });
        ///
        /// sim.step_until_no_events();
        /// assert_eq!(15., sim.time());
        /// ```
        pub fn sleep(&self, duration: f64) -> TimerFuture {
            assert!(duration >= 0., "Duration must be a positive value");
            self.sim_state
                .borrow_mut()
                .create_timer(self.id, duration, self.sim_state.clone())
        }

        /// Waits (asynchronously) for event of type `T` from any component.
        ///
        /// The returned future outputs the received event and event data.
        ///
        /// The timeout for waiting can be set by calling [`EventFuture::with_timeout`] on the returned future.
        ///
        /// # Examples
        ///
        /// ```rust
        /// use serde::Serialize;
        /// use dslab_core::Simulation;
        ///
        /// #[derive(Clone, Serialize)]
        /// struct Message {
        ///     payload: u32,
        /// }
        ///
        /// let mut sim = Simulation::new(123);
        /// let sender_ctx = sim.create_context("sender");
        /// let sender_id = sender_ctx.id();
        /// let receiver_ctx = sim.create_context("receiver");
        /// let receiver_id = receiver_ctx.id();
        ///
        /// sim.spawn(async move {
        ///     sender_ctx.emit(Message { payload: 321 }, receiver_id, 50.);
        /// });
        ///
        /// sim.spawn(async move {
        ///     let e = receiver_ctx.recv_event::<Message>().await;
        ///     assert_eq!(e.src, sender_id);
        ///     assert_eq!(e.data.payload, 321);
        /// });
        ///
        /// sim.step_until_no_events();
        /// assert_eq!(sim.time(), 50.);
        /// ```
        ///
        /// ```rust
        /// use serde::Serialize;
        /// use dslab_core::Simulation;
        ///
        /// #[derive(Clone, Serialize)]
        /// struct Message {
        ///     payload: u32,
        /// }
        ///
        /// let mut sim = Simulation::new(123);
        /// let sender1_ctx = sim.create_context("sender1");
        /// let sender1_id = sender1_ctx.id();
        /// let sender2_ctx = sim.create_context("sender2");
        /// let sender2_id = sender2_ctx.id();
        /// let receiver_ctx = sim.create_context("receiver");
        /// let receiver_id = receiver_ctx.id();
        ///
        /// sim.spawn(async move {
        ///     sender1_ctx.emit(Message { payload: 321 }, receiver_id, 50.);
        /// });
        ///
        /// sim.spawn(async move {
        ///    sender2_ctx.emit(Message { payload: 322 }, receiver_id, 100.);
        /// });
        ///
        /// sim.spawn(async move {
        ///     let e = receiver_ctx.recv_event::<Message>().await;
        ///     assert_eq!(receiver_ctx.time(), 50.);
        ///     assert_eq!(e.src, sender1_id);
        ///     assert_eq!(e.data.payload, 321);
        ///     let e = receiver_ctx.recv_event::<Message>().await;
        ///     assert_eq!(receiver_ctx.time(), 100.);
        ///     assert_eq!(e.src, sender2_id);
        ///     assert_eq!(e.data.payload, 322);
        /// });
        ///
        /// sim.step_until_no_events();
        /// assert_eq!(sim.time(), 100.);
        /// ```
        pub fn recv_event<T>(&self) -> EventFuture<T>
        where
            T: EventData,
        {
            self.recv_event_inner::<T>(self.id, None, None)
        }

        /// Waits (asynchronously) for event of type `T` from component `src`.
        ///
        /// The returned future outputs the received event and event data.
        ///
        /// The timeout for waiting can be set by calling [`EventFuture::with_timeout`] on the returned future.
        ///
        /// # Examples
        ///
        /// ```rust
        /// use serde::Serialize;
        /// use dslab_core::Simulation;
        ///
        /// #[derive(Clone, Serialize)]
        /// struct Message {
        ///     payload: u32,
        /// }
        ///
        /// let mut sim = Simulation::new(123);
        /// let sender_ctx = sim.create_context("sender");
        /// let sender_id = sender_ctx.id();
        /// let receiver_ctx = sim.create_context("receiver");
        /// let receiver_id = receiver_ctx.id();
        ///
        /// sim.spawn(async move {
        ///     sender_ctx.emit(Message { payload: 321 }, receiver_id, 50.);
        /// });
        ///
        /// sim.spawn(async move {
        ///     let e = receiver_ctx.recv_event_from::<Message>(sender_id).await;
        ///     assert_eq!(e.src, sender_id);
        ///     assert_eq!(e.data.payload, 321);
        /// });
        ///
        /// sim.step_until_no_events();
        /// assert_eq!(sim.time(), 50.);
        /// ```
        pub fn recv_event_from<T>(&self, src: Id) -> EventFuture<T>
        where
            T: EventData,
        {
            self.recv_event_inner::<T>(self.id, Some(src), None)
        }

        /// Waits (asynchronously) for event of type `T` from self.
        ///
        /// The returned future outputs the received event and event data.
        ///
        /// The timeout for waiting can be set by calling [`EventFuture::with_timeout`] on the returned future.
        ///
        /// # Examples
        ///
        /// ```rust
        /// use serde::Serialize;
        /// use dslab_core::Simulation;
        ///
        /// #[derive(Clone, Serialize)]
        /// struct SomeEvent {
        ///     payload: u32,
        /// }
        ///
        /// let mut sim = Simulation::new(123);
        /// let ctx = sim.create_context("comp");
        ///
        /// sim.spawn(async move {
        ///     ctx.emit_self(SomeEvent { payload: 321 }, 10.);
        ///
        ///     let e = ctx.recv_event_from_self::<SomeEvent>().await;
        ///     assert_eq!(e.data.payload, 321);
        ///     assert_eq!(ctx.time(), 10.)
        /// });
        ///
        /// sim.step_until_no_events();
        /// assert_eq!(sim.time(), 10.);
        /// ```
        pub fn recv_event_from_self<T>(&self) -> EventFuture<T>
        where
            T: EventData,
        {
            self.recv_event_inner::<T>(self.id, Some(self.id), None)
        }

        /// Registers a key getter function for event type `T` to be used with
        /// [`recv_event_by_key`](Self::recv_event_by_key) and [`recv_event_by_key_from`](Self::recv_event_by_key_from).
        pub fn register_key_getter_for<T: EventData>(&self, key_getter: impl Fn(&T) -> EventKey + 'static) {
            self.sim_state.borrow_mut().register_key_getter_for::<T>(key_getter);
        }

        /// Waits (asynchronously) for event of type `T` with key `key` from any component.
        ///
        /// The returned future outputs the received event and event data.
        ///
        /// The timeout for waiting can be set by calling [`EventFuture::with_timeout`] on the returned future.
        ///
        /// See [`recv_event_by_key_from`](Self::recv_event_by_key_from) and [`recv_event`](Self::recv_event) for examples.
        pub fn recv_event_by_key<T>(&self, key: EventKey) -> EventFuture<T>
        where
            T: EventData,
        {
            self.recv_event_inner::<T>(self.id, None, Some(key))
        }

        /// Waits (asynchronously) for event of type `T` with key `key` from component `src`.
        ///
        /// The returned future outputs the received event and event data.
        ///
        /// The timeout for waiting can be set by calling [`EventFuture::with_timeout`] on the returned future.
        ///
        /// # Examples
        ///
        /// ```rust
        /// use std::{cell::RefCell, rc::Rc};
        /// use serde::Serialize;
        /// use dslab_core::{cast, Id, Event, StaticEventHandler, Simulation, SimulationContext};
        /// use dslab_core::async_mode::EventKey;
        ///
        /// #[derive(Clone, Serialize)]
        /// struct SomeEvent {
        ///     key: u64,
        ///     payload: u32,
        /// }
        ///
        /// #[derive(Clone, Serialize)]
        /// struct Start {
        /// }
        ///
        /// struct Component {
        ///     ctx: SimulationContext,
        ///     root_id: Id,
        /// }
        ///
        /// impl Component {
        ///     async fn recv_event_for_key(self: Rc<Self>, key: EventKey) {
        ///         let e = self.ctx.recv_event_by_key_from::<SomeEvent>(self.root_id, key).await;
        ///         assert_eq!(e.data.key, key);
        ///     }
        /// }
        ///
        /// impl StaticEventHandler for Component {
        ///     fn on(self: Rc<Self>, event: Event) {
        ///         cast!(match event.data {
        ///             Start {} => {
        ///                 self.ctx.spawn(self.clone().recv_event_for_key(1));
        ///                 self.ctx.spawn(self.clone().recv_event_for_key(2));
        ///             }
        ///         })
        ///     }
        /// }
        ///
        /// let mut sim = Simulation::new(124);
        /// let root_ctx = sim.create_context("sender");
        /// let comp_ctx = sim.create_context("comp");
        /// let comp_id =  sim.add_static_handler("comp", Rc::new(Component { ctx: comp_ctx, root_id: root_ctx.id() }));
        ///
        /// sim.register_key_getter_for::<SomeEvent>(|message| message.key);
        ///
        /// root_ctx.emit_now(Start {}, comp_id);
        /// root_ctx.emit(SomeEvent { key: 1, payload: 321 }, comp_id, 50.);
        /// root_ctx.emit(SomeEvent { key: 2, payload: 322 }, comp_id, 100.);
        ///
        /// sim.step_until_no_events();
        /// assert_eq!(sim.time(), 100.);
        /// ```
        pub fn recv_event_by_key_from<T>(&self, src: Id, key: EventKey) -> EventFuture<T>
        where
            T: EventData,
        {
            self.recv_event_inner::<T>(self.id, Some(src), Some(key))
        }

        /// Waits (asynchronously) for event of type `T` with key `key` from self.
        ///
        /// The returned future outputs the received event and event data.
        ///
        /// The timeout for waiting can be set by calling [`EventFuture::with_timeout`] on the returned future.
        ///
        /// See [`recv_event_by_key_from`](Self::recv_event_by_key_from) and [`recv_event_from_self`](Self::recv_event_from_self) for examples.
        pub fn recv_event_by_key_from_self<T>(&self, key: EventKey) -> EventFuture<T>
        where
            T: EventData,
        {
            self.recv_event_inner::<T>(self.id, Some(self.id), Some(key))
        }

        fn recv_event_inner<T>(&self, dst: Id, src: Option<Id>, key: Option<EventKey>) -> EventFuture<T>
        where
            T: EventData,
        {
            if key.is_none() {
                assert!(
                    self.sim_state.borrow().get_key_getter(TypeId::of::<T>()).is_none(),
                    "Trying to receive event of type with registered key getter, use receive by key for such events"
                );
            } else {
                assert!(
                    self.sim_state.borrow().get_key_getter(TypeId::of::<T>()).is_some(),
                    "Trying to receive event by key for type {} without key getter, register it before using this feature",
                    type_name::<T>()
                );
            }
            let future_result =
                self.sim_state
                    .borrow_mut()
                    .create_event_future::<T>(dst, src, key, self.sim_state.clone());

            match future_result {
                Ok(future) => future,
                Err((_, e)) => panic!("Failed to create EventFuture: {}", e),
            }
        }
    );
}
