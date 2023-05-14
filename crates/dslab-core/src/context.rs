//! Accessing simulation from components.

#[allow(unused_imports)]
use core::panic;

use std::cell::RefCell;
use std::rc::Rc;

use rand::distributions::uniform::{SampleRange, SampleUniform};
use rand::prelude::Distribution;

async_core! {
    use std::any::TypeId;
    use futures::Future;

    use crate::async_core::shared_state::{AwaitEventSharedState, AwaitKey, AwaitResult, EventFuture};
}

async_details_core! {
    use std::any::type_name;
    use crate::async_core::shared_state::DetailsKey;
}

use crate::component::Id;
use crate::event::{Event, EventData, EventId};
use crate::state::SimulationState;
use crate::{async_core, async_details_core};

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

    async_core! {
        /// spawn a background separate task
        pub fn spawn(&self, future: impl Future<Output = ()>) {
            self.sim_state.borrow_mut().spawn(future);
        }

        /// wait for the given timeout.
        ///
        /// Example:
        ///
        /// ctx.async_wait_for(5.).await;
        ///
        pub async fn async_wait_for(&self, timeout: f64) {
            if timeout < 0.{
                panic!("timeout must be a positive value");
            }

            let future = self.sim_state.borrow_mut().wait_for(self.id, timeout);
            future.await;
        }


        /// async wait for any event of type T from src component with timeout
        /// Example:
        ///
        /// let event_result = ctx.async_wait_for_event::<PingMessage>(pinger_id, timeout).await;
        ///
        pub async fn async_wait_for_event<T>(&self, src: Id, timeout: f64) -> AwaitResult<T>
        where
            T: EventData,
        {
            if timeout < 0.{
                panic!("timeout must be a positive value");
            }

            self.async_wait_for_event_to(src, self.id, timeout).await
        }

        /// async wait for any event of type T from src component without timeout
        /// Example:
        ///
        /// let (event, data) = ctx.async_handle_event::<PingMessage>(pinger_id).await;
        ///
        pub async fn async_handle_event<T>(&self, src: Id) -> (Event, T)
        where
            T: EventData,
        {
            self.async_handle_event_to::<T>(src, self.id).await
        }

        /// async handle event from self
        pub async fn async_handle_self<T>(&self) -> (Event, T)
        where
            T: EventData,
        {
            self.async_handle_event_to::<T>(self.id, self.id).await
        }

        async fn async_handle_event_to<T>(&self, src: Id, dst: Id) -> (Event, T)
        where
            T: EventData,
        {
            let result = self.async_wait_for_event_to::<T>(src, dst, -1.).await;
            match result {
                AwaitResult::Ok(t) => t,
                AwaitResult::Timeout(_) => panic!("unexpected timeout"),
            }
        }

        fn async_wait_for_event_to<T>(&self, src: Id, dst: Id, timeout: f64) -> EventFuture<T>
        where
            T: EventData,
        {
            if self.sim_state.borrow().get_details_getter(TypeId::of::<T>()).is_some() {
                panic!("try to async handle event that has detailed key handling, use async details handlers");
            }

            let await_key = AwaitKey::new::<T>(src, dst);

            self.create_event_future(await_key, timeout)
        }
    }

    async_details_core! {
        /// async wait for event of type T from src component with details flag and timeout
        /// Example:
        ///
        /// let request_id = disk.send_data_read_request(/* some args */);
        /// let event_result = ctx.async_detailed_wait_for_event::<DataReadCompleted>(disk_id, request_id, timeout).await;
        ///
        pub async fn async_detailed_wait_for_event<T>(&self, src: Id, details: DetailsKey, timeout: f64) -> AwaitResult<T>
        where
            T: EventData,
        {
            if timeout < 0.{
                panic!("timeout must be a positive value");
            }

            self.async_detailed_wait_for_event_to(src, self.id, details, timeout).await
        }

        /// async wait for event of type T from src component with details flag without timeout
        /// Example:
        ///
        /// let request_id = disk.send_data_read_request(...);
        /// let (event, data) = ctx.async_detailed_handle_event::<DataReadCompleted>(disk_id, request_id).await;
        ///
        pub async fn async_detailed_handle_event<T>(&self, src: Id, details: DetailsKey) -> (Event, T)
        where
            T: EventData,
        {
            self.async_detailed_handle_event_to::<T>(src, self.id, details).await
        }

        /// async detailed handle event from self
        pub async fn async_detailed_handle_self<T>(&self, details: DetailsKey) -> (Event, T)
        where
            T: EventData,
        {
            self.async_detailed_handle_event_to::<T>(self.id, self.id, details)
                .await
        }

        /// Register the function for a type of EventData to get await details to futher call
        /// ctx.async_detailed_handle_event::<T>(from, details)
        ///
        /// # Example
        ///
        /// pub struct TaskCompleted {
        ///     request_id: u64
        ///     some_other_data: u64
        /// }
        ///
        /// pub fn get_task_completed_details(data: &dyn EventData) -> DetailsKey {
        ///     let event = data.downcast_ref::<TaskCompleted>().unwrap();
        ///     event.request_id as DetailsKey
        /// }
        ///
        /// let sim = Simulation::new(42);
        /// let ctx = sim.create_context("host")
        /// ctx.register_details_getter_for::<TaskCompleted>(get_task_completed_details);
        ///
        pub fn register_details_getter_for<T: EventData>(&self, details_getter: fn(&dyn EventData) -> DetailsKey) {
            self.sim_state
                .borrow_mut()
                .register_details_getter_for::<T>(details_getter);
        }

        fn async_detailed_wait_for_event_to<T>(
            &self,
            src: Id,
            dst: Id,
            details: DetailsKey,
            timeout: f64,
        ) -> EventFuture<T>
        where
            T: EventData,
        {
            if self.sim_state.borrow().get_details_getter(TypeId::of::<T>()).is_none() {
                panic!(
                    "simulation does not have details getter for type {}, register it before useing async_detailed getters",
                    type_name::<T>()
                );
            }

            let await_key = AwaitKey::new_with_details::<T>(src, dst, details);

            self.create_event_future(await_key, timeout)
        }

        async fn async_detailed_handle_event_to<T>(&self, src: Id, dst: Id, details: DetailsKey) -> (Event, T)
        where
            T: EventData,
        {
            let result = self.async_detailed_wait_for_event_to::<T>(src, dst, details, -1.).await;
            match result {
                AwaitResult::Ok(t) => t,
                AwaitResult::Timeout(_) => panic!("unexpected timeout"),
            }
        }
    }

    async_core! {
        fn create_event_future<T>(&self, await_key: AwaitKey, timeout: f64) -> EventFuture<T>
        where
            T: EventData,
        {
            let state = Rc::new(RefCell::new(AwaitEventSharedState::<T>::default()));
            state.borrow_mut().shared_content = AwaitResult::timeout_with(await_key.from, await_key.to);

            if timeout >= 0. {
                self.sim_state.borrow_mut().add_timer_on_state(await_key.to, timeout, state.clone());
            }

            self.sim_state
                .borrow_mut()
                .add_awaiter_handler(await_key, state.clone());

            EventFuture { state }
        }
    }
}
