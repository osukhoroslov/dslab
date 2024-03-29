//! Asynchronous waiting for events.

use std::any::Any;
use std::rc::Rc;
use std::{cell::RefCell, future::Future, task::Context};
use std::{
    pin::Pin,
    task::{Poll, Waker},
};

use futures::{select, FutureExt};
use serde::Serialize;

use crate::event::{EventData, TypedEvent};
use crate::state::SimulationState;
use crate::{Event, Id};

/// Type of key that represents the specific details of awaited event.
pub type EventKey = u64;

/// Represents a result of asynchronous waiting for event with timeout (see [`EventFuture::with_timeout`]).
pub enum AwaitResult<T: EventData> {
    /// Corresponds to successful event receipt.
    Ok(TypedEvent<T>),
    /// Corresponds to timeout expiration.
    Timeout {
        /// Source of the awaited event (None if it was not specified).
        src: Option<Id>,
        /// Key of the awaited event (None if it was not specified).
        event_key: Option<EventKey>,
        /// Timeout value.
        timeout: f64,
    },
}

// Event future --------------------------------------------------------------------------------------------------------

/// Future that represents asynchronous waiting for specific event.
pub struct EventFuture<T: EventData> {
    /// State with event data.
    state: Rc<RefCell<AwaitEventSharedState<T>>>,
    sim_state: Rc<RefCell<SimulationState>>,
    component_id: Id,
    event_key: Option<EventKey>,
    requested_src: Option<Id>,
}

impl<T: EventData> EventFuture<T> {
    pub(crate) fn new(
        state: Rc<RefCell<AwaitEventSharedState<T>>>,
        sim_state: Rc<RefCell<SimulationState>>,
        component_id: Id,
        requested_src: Option<Id>,
        event_key: Option<EventKey>,
    ) -> Self {
        Self {
            state,
            sim_state,
            requested_src,
            component_id,
            event_key,
        }
    }

    /// Waits for `EventFuture` with specified timeout and returns `AwaitResult`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    ///
    /// use dslab_core::Simulation;
    /// use dslab_core::async_mode::AwaitResult;
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
    ///     let mut res = client_ctx.recv_event_from::<Message>(root_id).with_timeout(10.).await;
    ///     match res {
    ///         AwaitResult::Ok(..) => panic!("expect timeout here"),
    ///         AwaitResult::Timeout {src, ..} => {
    ///             assert_eq!(src, Some(root_id));
    ///         }
    ///     }
    ///
    ///     res = client_ctx.recv_event_from::<Message>(root_id).with_timeout(50.).await;
    ///     match res {
    ///         AwaitResult::Ok(event) => {
    ///             assert_eq!(event.src, root_id);
    ///             assert_eq!(event.data.payload, 42);
    ///         }
    ///         AwaitResult::Timeout {..} => panic!("expect ok here"),
    ///     }
    /// });
    ///
    /// sim.step_until_no_events();
    /// assert_eq!(sim.time(), 50.);
    /// ```
    ///
    /// # Example with waiting by key
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    ///
    /// use serde::Serialize;
    ///
    /// use dslab_core::async_mode::{AwaitResult, EventKey};
    /// use dslab_core::event::EventData;
    /// use dslab_core::{cast, Event, EventHandler, Id, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// struct Start {}
    ///
    /// #[derive(Clone, Serialize)]
    /// struct SomeEvent {
    ///     request_id: u64,
    /// }
    ///
    /// struct Client {
    ///     ctx: SimulationContext,
    ///     root_id: Id,
    ///     actions_finished: RefCell<u32>,
    /// }
    ///
    /// impl Client {
    ///     fn on_start(&self) {
    ///         self.ctx.spawn(self.listen_first());
    ///         self.ctx.spawn(self.listen_second());
    ///     }
    ///
    ///     async fn listen_first(&self) {
    ///         let mut result = self
    ///             .ctx
    ///             .recv_event_by_key_from::<SomeEvent>(self.root_id, 1).with_timeout(10.)
    ///             .await;
    ///         if let AwaitResult::Timeout { src, event_key, .. } = result {
    ///             assert_eq!(src, Some(self.root_id));
    ///             assert_eq!(event_key, Some(1));
    ///         } else {
    ///             panic!("expect result timeout here");
    ///         }
    ///         result = self
    ///             .ctx
    ///             .recv_event_by_key_from::<SomeEvent>(self.root_id, 1).with_timeout(100.)
    ///             .await;
    ///         if let AwaitResult::Ok(event) = result {
    ///             assert_eq!(event.src, self.root_id);
    ///             assert_eq!(event.data.request_id, 1);
    ///         } else {
    ///             panic!("expected result ok");
    ///         }
    ///         *self.actions_finished.borrow_mut() += 1;
    ///     }
    ///
    ///     async fn listen_second(&self) {
    ///         let e = self.ctx.recv_event_by_key_from::<SomeEvent>(self.root_id, 2).await;
    ///         assert_eq!(e.src, self.root_id);
    ///         assert_eq!(e.data.request_id, 2);
    ///         *self.actions_finished.borrow_mut() += 1;
    ///     }
    /// }
    ///
    /// impl EventHandler for Client {
    ///     fn on(&mut self, event: Event) {
    ///         cast!(match event.data {
    ///             Start {} => {
    ///                 self.on_start();
    ///             }
    ///             SomeEvent { request_id } => {
    ///                 panic!(
    ///                     "unexpected handling SomeEvent with request id {} at time {}",
    ///                     request_id,
    ///                     self.ctx.time()
    ///                 );
    ///             }
    ///         })
    ///     }
    /// }
    ///
    /// let mut sim = Simulation::new(42);
    ///
    /// let root_ctx = sim.create_context("root");
    /// let client_ctx = sim.create_context("client");
    /// let client_id = client_ctx.id();
    /// let client = Rc::new(RefCell::new(Client {
    ///     ctx: client_ctx,
    ///     root_id: root_ctx.id(),
    ///     actions_finished: RefCell::new(0),
    /// }));
    /// sim.add_handler("client", client.clone());
    ///
    /// sim.register_key_getter_for::<SomeEvent>(|event| event.request_id as EventKey);
    ///
    /// root_ctx.emit_now(Start {}, client_id);
    /// root_ctx.emit(SomeEvent { request_id: 1 }, client_id, 50.);
    /// root_ctx.emit(SomeEvent { request_id: 2 }, client_id, 60.);
    ///
    /// sim.step_until_no_events();
    ///
    /// assert_eq!(*client.borrow().actions_finished.borrow(), 2);
    /// assert_eq!(sim.time(), 60.);
    /// ```
    pub async fn with_timeout(self, timeout: f64) -> AwaitResult<T> {
        assert!(timeout >= 0., "timeout must be a positive value");

        let component_id = self.component_id;
        let timer_future = self
            .sim_state
            .borrow_mut()
            .create_timer(component_id, timeout, self.sim_state.clone());

        let src = self.requested_src;
        let event_key = self.event_key;
        select! {
            event = self.fuse() => {
                AwaitResult::Ok(event)
            }
            _ = timer_future.fuse() => {
                AwaitResult::Timeout { src, event_key, timeout }
            }
        }
    }
}

impl<T: EventData> Future for EventFuture<T> {
    type Output = TypedEvent<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut state = self.state.as_ref().borrow_mut();

        if !state.completed {
            state.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        if let Some(data) = std::mem::take(&mut state.shared_content) {
            Poll::Ready(data)
        } else {
            panic!("internal error: unexpected timeout on future ready")
        }
    }
}

impl<T: EventData> Drop for EventFuture<T> {
    fn drop(&mut self) {
        if !self.state.borrow().completed {
            self.sim_state.borrow_mut().on_incomplete_event_future_drop::<T>(
                self.component_id,
                &self.requested_src,
                self.event_key,
            );
        }
    }
}

// Event promise -------------------------------------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct EventPromise {
    state: Rc<RefCell<dyn EventResultSetter>>,
}

impl EventPromise {
    pub fn contract<T: EventData>(
        sim_state: Rc<RefCell<SimulationState>>,
        dst: Id,
        requested_src: Option<Id>,
        event_key: Option<EventKey>,
    ) -> (Self, EventFuture<T>) {
        let state = Rc::new(RefCell::new(AwaitEventSharedState::<T>::default()));
        let future = EventFuture::new(state.clone(), sim_state, dst, requested_src, event_key);
        (Self { state }, future)
    }

    pub fn is_shared(&self) -> bool {
        Rc::strong_count(&self.state) > 1
    }

    pub fn set_completed(&self, e: Event) {
        self.state.borrow_mut().set_completed(e);
    }

    /// In case we need to cancel async activity we need to break reference
    /// cycle between EventFuture and Task. As far as Task is stored inside AwaitEventSharedState<T>
    /// as Waker, we take it out here and drop when state borrow is released.
    pub fn drop_shared_state(&mut self) {
        let _waker = self.state.borrow_mut().drop_state();
    }
}

// State shared between future and promise -----------------------------------------------------------------------------

pub(crate) struct AwaitEventSharedState<T: EventData> {
    pub completed: bool,
    pub waker: Option<Waker>,
    pub shared_content: Option<TypedEvent<T>>,
}

impl<T: EventData> Default for AwaitEventSharedState<T> {
    fn default() -> Self {
        Self {
            completed: false,
            waker: None,
            shared_content: None,
        }
    }
}

pub(crate) trait EventResultSetter: Any {
    fn set_completed(&mut self, event: Event);
    fn drop_state(&mut self) -> Option<Waker>;
}

#[derive(Serialize, Clone)]
pub(crate) struct EmptyData {}

impl<T: EventData> EventResultSetter for AwaitEventSharedState<T> {
    fn set_completed(&mut self, e: Event) {
        if self.completed {
            panic!("internal error: try to complete already completed state")
        }
        self.completed = true;
        self.shared_content = Some(Event::downcast::<T>(e));
        if let Some(waker) = self.waker.take() {
            waker.wake()
        }
    }

    fn drop_state(&mut self) -> Option<Waker> {
        // Set completed to true to prevent calling callback on EventFuture drop.
        self.completed = true;
        self.shared_content = None;
        // Take waker out of scope to release &mut self first and avoid several mutable borrows.
        // Next borrow() appears in EventFuture::drop to check if state is completed.
        self.waker.take()
    }
}
