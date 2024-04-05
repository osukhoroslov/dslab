//! Asynchronous waiting for events.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use futures::{select, FutureExt};

use crate::state::SimulationState;
use crate::{Event, EventData, Id, TypedEvent};

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
    dst: Id,
    src: Option<Id>,
    event_key: Option<EventKey>,
    // State with completion info shared with EventPromise.
    state: Rc<RefCell<TypedEventAwaitState<T>>>,
    sim_state: Rc<RefCell<SimulationState>>,
}

impl<T: EventData> EventFuture<T> {
    fn new(
        dst: Id,
        src: Option<Id>,
        event_key: Option<EventKey>,
        state: Rc<RefCell<TypedEventAwaitState<T>>>,
        sim_state: Rc<RefCell<SimulationState>>,
    ) -> Self {
        Self {
            dst,
            src,
            event_key,
            state,
            sim_state,
        }
    }

    /// Waits for event with specified timeout and returns result (either event of timeout).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use serde::Serialize;
    /// use dslab_core::Simulation;
    /// use dslab_core::async_mode::AwaitResult;
    ///
    /// #[derive(Clone, Serialize)]
    /// struct Message {
    ///     payload: u32,
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    /// let comp_ctx = sim.create_context("comp");
    /// let comp_id = comp_ctx.id();
    /// let root_ctx = sim.create_context("root");
    /// let root_id = root_ctx.id();
    ///
    /// sim.spawn(async move {
    ///     root_ctx.emit(Message { payload: 42 }, comp_id, 50.);
    /// });
    ///
    /// sim.spawn(async move {
    ///     let mut res = comp_ctx.recv_event_from::<Message>(root_id).with_timeout(10.).await;
    ///     match res {
    ///         AwaitResult::Ok(..) => panic!("Expect timeout here"),
    ///         AwaitResult::Timeout {src, event_key, timeout} => {
    ///             assert_eq!(src, Some(root_id));
    ///             assert_eq!(event_key, None);
    ///             assert_eq!(timeout, 10.);
    ///         }
    ///     }
    ///     res = comp_ctx.recv_event_from::<Message>(root_id).with_timeout(50.).await;
    ///     match res {
    ///         AwaitResult::Ok(event) => {
    ///             assert_eq!(event.src, root_id);
    ///             assert_eq!(event.dst, comp_id);
    ///             assert_eq!(event.time, 50.);
    ///             assert_eq!(event.data.payload, 42);
    ///         }
    ///         AwaitResult::Timeout {..} => panic!("Expect ok here"),
    ///     }
    /// });
    ///
    /// sim.step_until_no_events();
    /// assert_eq!(sim.time(), 50.);
    /// ```
    ///
    /// ## Example with waiting by event key
    ///
    /// ```rust
    /// use std::cell::RefCell;
    /// use std::rc::Rc;
    /// use serde::Serialize;
    /// use dslab_core::async_mode::{AwaitResult, EventKey};
    /// use dslab_core::{cast, Event, EventData, EventHandler, Id, Simulation, SimulationContext};
    ///
    /// #[derive(Clone, Serialize)]
    /// struct Start {}
    ///
    /// #[derive(Clone, Serialize)]
    /// struct SomeEvent {
    ///     request_id: u64,
    /// }
    ///
    /// struct Component {
    ///     root_id: Id,
    ///     actions_finished: RefCell<u32>,
    ///     ctx: SimulationContext,
    /// }
    ///
    /// impl Component {
    ///     fn new(root_id: Id, ctx: SimulationContext) -> Self {
    ///         Self {
    ///             root_id,
    ///             actions_finished: RefCell::new(0),
    ///             ctx,
    ///         }
    ///     }
    ///
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
    ///             panic!("Expect result timeout here");
    ///         }
    ///         result = self
    ///             .ctx
    ///             .recv_event_by_key_from::<SomeEvent>(self.root_id, 1).with_timeout(100.)
    ///             .await;
    ///         if let AwaitResult::Ok(event) = result {
    ///             assert_eq!(event.src, self.root_id);
    ///             assert_eq!(event.data.request_id, 1);
    ///             assert_eq!(event.time, 50.);
    ///             assert_eq!(self.ctx.time(), 50.);
    ///         } else {
    ///             panic!("Expected result ok");
    ///         }
    ///         *self.actions_finished.borrow_mut() += 1;
    ///     }
    ///
    ///     async fn listen_second(&self) {
    ///         let e = self.ctx.recv_event_by_key_from::<SomeEvent>(self.root_id, 2).await;
    ///         assert_eq!(e.src, self.root_id);
    ///         assert_eq!(e.data.request_id, 2);
    ///         assert_eq!(e.time, 60.);
    ///         assert_eq!(self.ctx.time(), 60.);
    ///         *self.actions_finished.borrow_mut() += 1;
    ///     }
    /// }
    ///
    /// impl EventHandler for Component {
    ///     fn on(&mut self, event: Event) {
    ///         cast!(match event.data {
    ///             Start {} => {
    ///                 self.on_start();
    ///             }
    ///             SomeEvent { request_id } => {
    ///                 panic!(
    ///                     "Unexpected handling of SomeEvent with request id {} at time {}",
    ///                     request_id,
    ///                     self.ctx.time()
    ///                 );
    ///             }
    ///         })
    ///     }
    /// }
    ///
    /// let mut sim = Simulation::new(123);
    ///
    /// let root_ctx = sim.create_context("root");
    /// let comp_ctx = sim.create_context("comp");
    /// let comp = Rc::new(RefCell::new(Component::new(root_ctx.id(), comp_ctx)));
    /// let comp_id = sim.add_handler("comp", comp.clone());
    ///
    /// sim.register_key_getter_for::<SomeEvent>(|event| event.request_id as EventKey);
    ///
    /// root_ctx.emit_now(Start {}, comp_id);
    /// root_ctx.emit(SomeEvent { request_id: 1 }, comp_id, 50.);
    /// root_ctx.emit(SomeEvent { request_id: 2 }, comp_id, 60.);
    ///
    /// sim.step_until_no_events();
    ///
    /// assert_eq!(*comp.borrow().actions_finished.borrow(), 2);
    /// assert_eq!(sim.time(), 60.);
    /// ```
    pub async fn with_timeout(self, timeout: f64) -> AwaitResult<T> {
        assert!(timeout >= 0., "Timeout must be a positive value");
        let timer_future = self
            .sim_state
            .borrow_mut()
            .create_timer(self.dst, timeout, self.sim_state.clone());
        let src = self.src;
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
    fn poll(self: Pin<&mut Self>, async_ctx: &mut Context) -> Poll<Self::Output> {
        let mut state = self.state.as_ref().borrow_mut();
        if state.completed {
            let event = std::mem::take(&mut state.event).expect("Completed EventFuture contains no event");
            Poll::Ready(event)
        } else {
            state.waker = Some(async_ctx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T: EventData> Drop for EventFuture<T> {
    fn drop(&mut self) {
        // We cannot call SimulationState::on_incomplete_event_future_drop when dropping futures on component handler
        // removal, because sim_state is already mutably borrowed in SimulationState::cancel_component_promises.
        // Instead, we do the necessary clean up directly in SimulationState::cancel_component_promises and set the
        // manually_dropped flag in the state.
        if !self.state.borrow().completed && !self.state.borrow().manually_dropped {
            self.sim_state
                .borrow_mut()
                .on_incomplete_event_future_drop::<T>(self.dst, &self.src, self.event_key);
        }
    }
}

// Event promise -------------------------------------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct EventPromise {
    // State with completion info shared with EventFuture.
    state: Rc<RefCell<dyn EventAwaitState>>,
}

impl EventPromise {
    pub fn contract<T: EventData>(
        dst: Id,
        src: Option<Id>,
        event_key: Option<EventKey>,
        sim_state: Rc<RefCell<SimulationState>>,
    ) -> (Self, EventFuture<T>) {
        let state = Rc::new(RefCell::new(TypedEventAwaitState::<T>::default()));
        let future = EventFuture::new(dst, src, event_key, state.clone(), sim_state);
        (Self { state }, future)
    }

    pub fn complete(&self, e: Event) {
        // Check if the state is still shared with some future
        if Rc::strong_count(&self.state) > 1 {
            self.state.borrow_mut().complete(e);
        } else {
            panic!("Trying to complete promise which state is no longer shared");
        }
    }

    // When cancelling asynchronous waiting for event we need to break a reference cycle
    // between EventFuture and Task by dropping the state which stores Task as a Waker.
    pub fn drop_state(&mut self) {
        // Take the waker out and drop it when the state borrow is released
        let _waker = self.state.borrow_mut().drop();
    }
}

// State shared between future and promise -----------------------------------------------------------------------------

struct TypedEventAwaitState<T: EventData> {
    pub completed: bool,
    pub manually_dropped: bool,
    pub event: Option<TypedEvent<T>>,
    pub waker: Option<Waker>,
}

impl<T: EventData> Default for TypedEventAwaitState<T> {
    fn default() -> Self {
        Self {
            completed: false,
            manually_dropped: false,
            event: None,
            waker: None,
        }
    }
}

trait EventAwaitState {
    fn complete(&mut self, event: Event);
    fn drop(&mut self) -> Option<Waker>;
}

impl<T: EventData> EventAwaitState for TypedEventAwaitState<T> {
    fn complete(&mut self, e: Event) {
        if self.completed {
            panic!("Trying to complete already completed state")
        }
        self.completed = true;
        self.event = Some(Event::downcast::<T>(e));
        if let Some(waker) = self.waker.take() {
            waker.wake()
        }
    }

    fn drop(&mut self) -> Option<Waker> {
        self.manually_dropped = true;
        self.event = None;
        // We cannot drop the waker immediately here because it will trigger EventFuture::drop,
        // which requires borrowing of (already mutably borrowed) state.
        // Instead, we take the waker out of scope to drop it when the state borrow is released.
        self.waker.take()
    }
}
