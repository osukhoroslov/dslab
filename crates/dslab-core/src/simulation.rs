//! Simulation configuration and execution.

use std::cell::RefCell;
use std::rc::Rc;

use log::Level::Trace;
use log::{debug, log_enabled, trace};
use rand::distributions::uniform::{SampleRange, SampleUniform};
use rand::prelude::Distribution;
use serde_json::json;
use serde_type_name::type_name;

use crate::component::Id;
use crate::context::SimulationContext;
use crate::handler::{EventCancellationPolicy, EventHandler};
use crate::log::log_undelivered_event;
use crate::state::SimulationState;
use crate::{async_disabled, async_enabled, Event};

async_enabled! {
    use std::sync::mpsc::channel;

    use futures::Future;

    use crate::async_core::shared_state::AwaitKey;
    use crate::async_core::executor::Executor;
    use crate::event::EventData;
    use crate::async_core::await_details::EventKey;
    use crate::async_core::sync::queue::UnboundedBlockingQueue;
}

async_disabled! {
    /// Represents a simulation, provides methods for its configuration and execution.
    pub struct Simulation {
        sim_state: Rc<RefCell<SimulationState>>,
        handlers: Vec<Option<Rc<RefCell<dyn EventHandler>>>>,
    }
}

async_enabled! {
    /// Represents a simulation, provides methods for its configuration and execution.
    pub struct Simulation {
        sim_state: Rc<RefCell<SimulationState>>,
        handlers: Vec<Option<Rc<RefCell<dyn EventHandler>>>>,
        // Async stuff
        executor: Executor,
    }
}

impl Simulation {
    async_disabled! {
        /// Creates a new simulation with specified random seed.
        pub fn new(seed: u64) -> Self {
            Self {
                sim_state: Rc::new(RefCell::new(SimulationState::new(seed))),
                handlers: Vec::new(),
            }
        }
    }

    async_enabled! {
        /// Creates a new simulation with specified random seed.
        pub fn new(seed: u64) -> Self {
            let (task_sender, ready_queue) = channel();
            Self {
                sim_state: Rc::new(RefCell::new(SimulationState::new(seed, task_sender))),
                handlers: Vec::new(),
                executor: Executor::new(ready_queue),
            }
        }
    }

    fn register(&mut self, name: &str) -> Id {
        let id = self.sim_state.borrow_mut().register(name);
        if id as usize == self.handlers.len() {
            self.handlers.push(None);
        }
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
        self.sim_state.borrow().lookup_id(name)
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
        self.sim_state.borrow().lookup_name(id)
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
            self.sim_state.borrow().get_names(),
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
    /// #[derive(Clone, Serialize)]
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
        self.sim_state.borrow_mut().on_handler_added(id);
        debug!(
            target: "simulation",
            "[{:.3} {} simulation] Added handler: {}",
            self.time(),
            crate::log::get_colored("DEBUG", colored::Color::Blue),
            json!({"name": name.as_ref(), "id": id})
        );
        id
    }

    /// Removes the event handler for component with specified name.
    ///
    /// All subsequent events destined for this component will not be delivered until the handler is added again.
    ///
    /// Pending events to be cancelled upon the handler removal are specified via [`crate::handler::EventCancellationPolicy`].
    ///
    /// If the `async_core` feature is enabled, all pending timers and async activities related to this component are cancelled.
    /// In order to continue receiving events asynchronously, new async tasks must be spawned using [`SimulationContext::spawn()`].
    /// Otherwise, the simulation will deliver events via [`EventHandler::on()`] method as usual.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::{cast, Event, EventCancellationPolicy, EventHandler, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
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
    /// let comp_id1 = sim.add_handler("comp", comp.clone());
    /// sim.remove_handler("comp", EventCancellationPolicy::None);
    /// // Assigned component Id is not changed if we call `add_handler` again.
    /// let comp_id2 = sim.add_handler("comp", comp);
    /// assert_eq!(comp_id1, comp_id2);
    /// ```
    pub fn remove_handler<S>(&mut self, name: S, cancel_policy: EventCancellationPolicy)
    where
        S: AsRef<str>,
    {
        let id = self.lookup_id(name.as_ref());
        self.handlers[id as usize] = None;
        self.sim_state.borrow_mut().on_handler_removed(id);
        self.remove_handler_inner(id);

        // cancel pending events related to the removed component based on the cancellation policy
        match cancel_policy {
            EventCancellationPolicy::All => self.cancel_events(|e| e.src == id || e.dst == id),
            EventCancellationPolicy::Incoming => self.cancel_events(|e| e.dst == id),
            EventCancellationPolicy::Outgoing => self.cancel_events(|e| e.src == id),
            _ => {}
        }

        debug!(
            target: "simulation",
            "[{:.3} {} simulation] Removed handler: {}",
            self.time(),
            crate::log::get_colored("DEBUG", colored::Color::Blue),
            json!({"name": name.as_ref(), "id": id})
        );
    }

    async_disabled! {
        fn remove_handler_inner(&mut self, _id: u32) {}
    }

    async_enabled! {
        fn remove_handler_inner(&mut self, id: u32) {
            // cancel pending timers related to the removed component
            self.sim_state.borrow_mut().cancel_component_timers(id);
            self.sim_state.borrow_mut().cancel_component_promises(id);
        }
    }

    /// Returns the current simulation time.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::Simulation;
    ///
    /// #[derive(Clone, Serialize)]
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
    /// If there is no handler registered for component with Id `event.dst`, logs the undelivered event and discards it.
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
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp_ctx = sim.create_context("comp");
    /// assert_eq!(sim.time(), 0.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.2);
    /// let mut status = sim.step();
    /// assert!(status);
    /// assert_eq!(sim.time(), 1.2);
    /// status = sim.step();
    /// assert!(!status);
    /// ```
    pub fn step(&self) -> bool {
        self.step_inner()
    }

    async_disabled! {
        fn step_inner(&self) -> bool {
            let event_opt = self.sim_state.borrow_mut().next_event();
            match event_opt {
                Some(event) => {
                    self.deliver_event_via_handler(event);
                    true
                }
                None => false,
            }
        }
    }

    fn deliver_event_via_handler(&self, event: Event) {
        if let Some(handler_opt) = self.handlers.get(event.dst as usize) {
            self.log_trace_event(&event);
            if let Some(handler) = handler_opt {
                handler.borrow_mut().on(event);
            } else {
                log_undelivered_event(event);
            }
        } else {
            log_undelivered_event(event);
        }
    }

    fn log_trace_event(&self, event: &Event) {
        if log_enabled!(Trace) {
            let src_name = self.lookup_name(event.src);
            let dst_name = self.lookup_name(event.dst);
            trace!(
                target: &dst_name,
                "[{:.3} {} {}] {}",
                event.time,
                crate::log::get_colored("EVENT", colored::Color::BrightBlack),
                dst_name,
                json!({"type": type_name(&event.data).unwrap(), "data": event.data, "src": src_name})
            );
        }
    }

    async_enabled! {
        fn step_inner(&self) -> bool {
            if self.process_task() {
                return true;
            }

            let has_timer = self.sim_state.borrow_mut().peek_timer().is_some();
            let has_event = self.sim_state.borrow_mut().peek_event().is_some();

            if !has_timer && !has_event {
                return false;
            }
            if !has_timer {
                self.process_event();
                return true;
            }
            if !has_event {
                self.process_timer();
                return true;
            }

            let next_timer_time = self.sim_state.borrow_mut().peek_timer().unwrap().time;
            let next_event_time = self.sim_state.borrow_mut().peek_event().unwrap().time;

            if next_timer_time <= next_event_time {
                self.process_timer();
            } else {
                self.process_event();
            }

            true
        }

        fn process_event(&self) -> bool {
            let event = self.sim_state.borrow_mut().next_event().unwrap();

            let await_key = match self.sim_state.borrow().get_key_getter(event.data.type_id()) {
                Some(getter) => AwaitKey::new_by_ref(
                    event.dst,
                    event.data.as_ref(),
                    Some(getter(event.data.as_ref())),
                ),
                None => AwaitKey::new_by_ref(event.dst, event.data.as_ref(), None),
            };

            if self.sim_state.borrow_mut().has_promise_on_key(&event.src, &await_key) {
                self.log_trace_event(&event);

                self.sim_state.borrow_mut().complete_event_promise(event.src, &await_key, event);

                self.process_task();
                return true;
            }

            self.deliver_event_via_handler(event);

            true
        }

        fn process_task(&self) -> bool {
            self.executor.process_task()
        }

        fn process_timer(&self) {
            let next_timer = self.sim_state.borrow_mut().next_timer().unwrap();
            next_timer.set_completed();
            // drop timer to release the pointer to the state
            drop(next_timer);
            self.process_task();
        }

        /// Spawns a new asynchronous task.
        ///
        /// The task's type lifetime must be `'static`.
        /// This means that the spawned task must not contain any references to data owned outside the task.
        ///
        /// To spawn methods inside simulation components use [`SimulationContext::spawn`].
        ///
        /// # Examples
        ///
        /// ```rust
        /// use dslab_core::Simulation;
        ///
        /// let mut sim = Simulation::new(42);
        ///
        /// let ctx = sim.create_context("client");
        ///
        /// sim.spawn(async move {
        ///     let initial_time = ctx.time();
        ///     ctx.sleep(5.).await;
        /// });
        ///
        /// sim.step_until_no_events();
        /// assert_eq!(5., sim.time());
        /// ```
        pub fn spawn(&self, future: impl Future<Output = ()> + 'static) {
            self.sim_state.borrow_mut().spawn(future);
        }

        /// Registers a function that extracts [`EventKey`] from events of specific type `T`.
        ///
        /// This is required step before using the [`SimulationContext::recv_event_by_key_from`]
        /// function with type `T`.
        ///
        /// See [`SimulationContext::async_wait_event_detailed_for`].
        pub fn register_key_getter_for<T: EventData>(&self, key_getter: impl Fn(&T) -> EventKey + 'static) {
            self.sim_state
                .borrow_mut()
                .register_key_getter_for::<T>(key_getter);
        }

        /// Creates an [`UnboundedBlockingQueue`] for producer-consumer communication.
        ///
        /// This queue is designed to support convenient communication between several asynchronous tasks
        /// within a single simulation component.
        /// This enables implementing the component logic as a set of communicating concurrent activities.
        ///
        /// The use of this primitive for inter-component communication is discouraged in favor of passing events
        /// directly or via intermediate components.
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
        /// use dslab_core::async_core::sync::queue::UnboundedBlockingQueue;
        ///
        /// struct InternalMessage {
        ///     payload: u32,
        /// }
        ///
        /// #[derive(Clone, Serialize)]
        /// struct Start {}
        ///
        /// struct Client {
        ///     ctx: SimulationContext,
        ///     queue: UnboundedBlockingQueue<InternalMessage>,
        /// }
        ///
        /// impl Client {
        ///     fn on_start(&self) {
        ///         self.ctx.spawn(self.producer());
        ///         self.ctx.spawn(self.consumer());
        ///     }
        ///
        ///     async fn producer(&self) {
        ///         for i in 0..10 {
        ///             self.ctx.sleep(5.).await;
        ///             self.queue.send(InternalMessage {payload: i});
        ///         }
        ///     }
        ///
        ///     async fn consumer(&self) {
        ///         for i in 0..10 {
        ///             let msg = self.queue.receive().await;
        ///             assert_eq!(msg.payload, i);
        ///         }
        ///     }
        /// }
        ///
        /// impl EventHandler for Client {
        ///     fn on(&mut self, event: Event) {
        ///         cast!(match event.data {
        ///             Start {} => {
        ///                 self.on_start();
        ///             }
        ///         })
        ///     }
        /// }
        ///
        /// let mut sim = Simulation::new(42);
        ///
        /// let client_ctx = sim.create_context("client");
        /// let client_id = client_ctx.id();
        ///
        /// let queue: UnboundedBlockingQueue<InternalMessage> = sim.create_queue("client_queue");
        /// let client = Rc::new(RefCell::new(Client {ctx: client_ctx, queue }));
        ///
        /// sim.add_handler("client", client);
        ///
        /// let root_ctx = sim.create_context("root");
        /// root_ctx.emit(Start{}, client_id, 10.);
        ///
        /// sim.step_until_no_events();
        ///
        /// assert_eq!(sim.time(), 60.); // 10. from start delay + 5.*10 from send steps delays
        /// ```
        pub fn create_queue<T, S>(&mut self, name: S) -> UnboundedBlockingQueue<T>
        where
            S: AsRef<str>,
        {
            UnboundedBlockingQueue::new(self.create_context(name))
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
    /// #[derive(Clone, Serialize)]
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
    /// assert!(status);
    /// assert_eq!(sim.time(), 1.3);
    /// status = sim.steps(2);
    /// assert!(!status);
    /// assert_eq!(sim.time(), 1.4)
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
    /// #[derive(Clone, Serialize)]
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
    /// time is above the specified threshold (`initial_time + duration`) or there are no pending events left.
    ///
    /// This method also advances the simulation time to `initial_time + duration`. Note that the resulted time may
    /// slightly differ from the expected value due to the floating point errors. This issue can be avoided by using
    /// the [`step_until_time()`](Self::step_until_time()) method.
    ///
    /// Returns `true` if there could be more pending events and `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::Simulation;
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp_ctx = sim.create_context("comp");
    /// assert_eq!(sim.time(), 0.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 2.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 3.5);
    /// let mut status = sim.step_for_duration(1.8);
    /// assert_eq!(sim.time(), 1.8);
    /// assert!(status); // there are more events
    /// status = sim.step_for_duration(1.8);
    /// assert_eq!(sim.time(), 3.6);
    /// assert!(!status); // there are no more events
    /// ```
    pub fn step_for_duration(&mut self, duration: f64) -> bool {
        let end_time = self.sim_state.borrow().time() + duration;
        self.step_until_time(end_time)
    }

    /// Steps through the simulation until the specified time.
    ///
    /// This is a convenient wrapper around [`step()`](Self::step()), which invokes this method until the next event
    /// time is above the specified time or there are no pending events left.
    ///
    /// This method also advances the simulation time to the specified time.
    ///
    /// Returns `true` if there could be more pending events and `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::Simulation;
    ///
    /// #[derive(Clone, Serialize)]
    /// pub struct SomeEvent {
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let mut comp_ctx = sim.create_context("comp");
    /// assert_eq!(sim.time(), 0.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 1.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 2.0);
    /// comp_ctx.emit_self(SomeEvent{ }, 3.5);
    /// let mut status = sim.step_until_time(1.8);
    /// assert_eq!(sim.time(), 1.8);
    /// assert!(status); // there are more events
    /// status = sim.step_until_time(3.6);
    /// assert_eq!(sim.time(), 3.6);
    /// assert!(!status); // there are no more events
    /// ```
    pub fn step_until_time(&mut self, time: f64) -> bool {
        self.step_until_time_inner(time)
    }

    async_disabled! {
        fn step_until_time_inner(&mut self, time: f64) -> bool {
            let mut result = true;
            loop {
                if let Some(event) = self.sim_state.borrow_mut().peek_event() {
                    if event.time > time {
                        break;
                    }
                } else {
                    result = false;
                    break;
                }
                self.step();
            }
            self.sim_state.borrow_mut().set_time(time);
            result
        }
    }

    async_enabled! {
        fn step_until_time_inner(&mut self, time: f64) -> bool {
            let mut result;
            loop {
                while self.process_task() {}

                result = false;
                let mut step = false;

                if let Some(event) = self.sim_state.borrow_mut().peek_event() {
                    result = true;
                    if event.time <= time {
                        step = true;
                    }
                }

                if let Some(timer) = self.sim_state.borrow_mut().peek_timer() {
                    result = true;
                    if timer.time <= time {
                        step = true;
                    }
                }

                if step {
                    self.step();
                } else {
                    break;
                }
            }
            self.sim_state.borrow_mut().set_time(time);
            result
        }
    }

    /// Returns a random float in the range _[0, 1)_
    /// using the simulation-wide random number generator.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dslab_core::Simulation;
    ///
    /// let mut sim = Simulation::new(123);
    /// let f: f64 = sim.rand();
    /// assert!(f >= 0.0 && f < 1.0);
    /// ```
    pub fn rand(&mut self) -> f64 {
        self.sim_state.borrow_mut().rand()
    }

    /// Returns a random number in the specified range
    /// using the simulation-wide random number generator.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use dslab_core::Simulation;
    ///
    /// let mut sim = Simulation::new(123);
    /// let n: u32 = sim.gen_range(1..=10);
    /// assert!(n >= 1 && n <= 10);
    /// let f: f64 = sim.gen_range(0.1..0.5);
    /// assert!(f >= 0.1 && f < 0.5);
    /// ```
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

    /// Returns a random alphanumeric string of specified length
    /// using the simulation-wide random number generator.
    pub fn random_string(&mut self, len: usize) -> String {
        self.sim_state.borrow_mut().random_string(len)
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
    /// #[derive(Clone, Serialize)]
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
    /// comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), 1.0);
    /// comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), 2.0);
    /// comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), 3.0);
    /// sim.cancel_events(|e| e.id < 2);
    /// sim.step();
    /// assert_eq!(sim.time(), 3.0);
    /// ```
    pub fn cancel_events<F>(&mut self, pred: F)
    where
        F: Fn(&Event) -> bool,
    {
        self.sim_state.borrow_mut().cancel_events(pred);
    }

    /// Cancels events that satisfy the given predicate function and returns them.
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
    /// comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), 1.0);
    /// comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), 2.0);
    /// comp1_ctx.emit(SomeEvent{}, comp2_ctx.id(), 3.0);
    /// let cancelled = sim.cancel_and_get_events(|e| e.id < 2);
    /// assert_eq!(cancelled.len(), 2);
    /// sim.step();
    /// assert_eq!(sim.time(), 3.0);
    /// ```
    pub fn cancel_and_get_events<F>(&mut self, pred: F) -> Vec<Event>
    where
        F: Fn(&Event) -> bool,
    {
        self.sim_state.borrow_mut().cancel_and_get_events(pred)
    }

    /// Returns a copy of pending events sorted by time.
    ///
    /// Currently used for model checking in dslab-mp.
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
    /// let mut ctx1 = sim.create_context("comp1");
    /// let mut ctx2 = sim.create_context("comp2");
    /// let event1 = ctx1.emit(SomeEvent{}, ctx2.id(), 1.0);
    /// let event2 = ctx2.emit(SomeEvent{}, ctx1.id(), 1.0);
    /// let event3 = ctx1.emit(SomeEvent{}, ctx2.id(), 2.0);
    /// let events = sim.dump_events();
    /// assert_eq!(events.len(), 3);
    /// assert_eq!((events[0].id, events[0].time), (event1, 1.0));
    /// assert_eq!((events[1].id, events[1].time), (event2, 1.0));
    /// assert_eq!((events[2].id, events[2].time), (event3, 2.0));
    /// ```
    pub fn dump_events(&self) -> Vec<Event> {
        self.sim_state.borrow().dump_events()
    }
}
