//! Shared state and event notification.

use std::any::{Any, TypeId};
use std::rc::Rc;
use std::{cell::RefCell, future::Future, task::Context};
use std::{
    pin::Pin,
    task::{Poll, Waker},
};

use futures::{select, FutureExt};
use serde::Serialize;

use crate::async_core::await_details::{AwaitResult, EventKey, TimeoutInfo};
use crate::event::EventData;
use crate::state::SimulationState;
use crate::{Event, Id};

#[derive(Serialize, Clone)]
pub(crate) struct EmptyData {}

#[derive(Clone)]
pub struct EventPromise {
    state: Rc<RefCell<dyn EventResultSetter>>,
}

impl EventPromise {
    pub fn contract<T: EventData>(
        sim_state: Rc<RefCell<SimulationState>>,
        await_key: &AwaitKey,
        requested_src: Option<Id>,
    ) -> (Self, EventFuture<T>) {
        let state = Rc::new(RefCell::new(AwaitEventSharedState::<T>::default()));
        let future = EventFuture::new(
            state.clone(),
            sim_state,
            await_key.to,
            requested_src,
            await_key.event_key,
        );
        (Self { state }, future)
    }

    pub fn is_shared(&self) -> bool {
        Rc::strong_count(&self.state) > 1
    }

    pub fn set_completed(&self, e: Event) {
        self.state.borrow_mut().set_completed(e);
    }
}

pub struct AwaitEventSharedState<T: EventData> {
    pub completed: bool,
    pub waker: Option<Waker>,
    pub shared_content: Option<(Event, T)>,
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

pub trait EventResultSetter: Any {
    fn set_completed(&mut self, event: Event);
}

impl<T: EventData> EventResultSetter for AwaitEventSharedState<T> {
    fn set_completed(&mut self, mut e: Event) {
        if self.completed {
            panic!("internal error: try to complete already completed state")
        }
        self.completed = true;
        let downcast_result = e.data.downcast::<T>();

        e.data = Box::new(EmptyData {});
        match downcast_result {
            Ok(data) => {
                self.shared_content = Some((e, *data));
            }
            Err(_) => {
                panic!("internal downcast conversion error");
            }
        };
        if let Some(waker) = self.waker.take() {
            waker.wake()
        }
    }
}

/// Future that represents AwaitResult for event (Ok or Timeout).
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
}

impl<T: EventData> Future for EventFuture<T> {
    type Output = (Event, T);
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
            let await_key = AwaitKey::new::<T>(self.component_id, self.event_key);
            self.sim_state
                .borrow_mut()
                .on_incomplete_event_future_drop(&await_key, &self.requested_src)
        }
    }
}

impl<T: EventData> EventFuture<T> {
    /// Adds timeout to any EventFuture.
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
    ///     let mut res = client_ctx.recv_event_from::<Message>(root_id).with_timeout(10.).await;
    ///     match res {
    ///         AwaitResult::Ok(..) => panic!("expect timeout here"),
    ///         AwaitResult::Timeout(info) => {
    ///             assert_eq!(info.src, Some(root_id));
    ///         }
    ///     }
    ///
    ///     res = client_ctx.recv_event_from::<Message>(root_id).with_timeout(50.).await;
    ///     match res {
    ///         AwaitResult::Ok((e, data)) => {
    ///             assert_eq!(e.src, root_id);
    ///             assert_eq!(data.payload, 42);
    ///         }
    ///         AwaitResult::Timeout(..) => panic!("expect ok here"),
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
    /// use dslab_core::async_core::{AwaitResult, EventKey};
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
    ///         if let AwaitResult::Timeout(info) = result {
    ///             assert_eq!(info.src, Some(self.root_id));
    ///             assert_eq!(info.event_key, Some(1));
    ///         } else {
    ///             panic!("expect result timeout here");
    ///         }
    ///         result = self
    ///             .ctx
    ///             .recv_event_by_key_from::<SomeEvent>(self.root_id, 1).with_timeout(100.)
    ///             .await;
    ///         if let AwaitResult::Ok((e, data)) = result {
    ///             assert_eq!(e.src, self.root_id);
    ///             assert_eq!(data.request_id, 1);
    ///         } else {
    ///             panic!("expected result ok");
    ///         }
    ///         *self.actions_finished.borrow_mut() += 1;
    ///     }
    ///
    ///     async fn listen_second(&self) {
    ///         let (e, data) = self.ctx.recv_event_by_key_from::<SomeEvent>(self.root_id, 2).await;
    ///         assert_eq!(e.src, self.root_id);
    ///         assert_eq!(data.request_id, 2);
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

        let timeout_info = TimeoutInfo {
            timeout,
            event_key: self.event_key,
            src: self.requested_src,
        };
        select! {
            data = self.fuse() => {
                AwaitResult::Ok(data)
            }
            _ = timer_future.fuse() => {
                AwaitResult::Timeout(timeout_info)
            }
        }
    }
}

#[derive(Hash, PartialEq, Eq, Debug, Clone, Copy)]
pub(crate) struct AwaitKey {
    pub to: Id,
    pub msg_type: TypeId,
    event_key: Option<EventKey>,
}

impl AwaitKey {
    pub fn new<T: EventData>(to: Id, event_key: Option<EventKey>) -> Self {
        Self {
            to,
            msg_type: TypeId::of::<T>(),
            event_key,
        }
    }

    pub fn new_by_ref(to: Id, data: &dyn EventData, event_key: Option<EventKey>) -> Self {
        Self {
            to,
            msg_type: data.type_id(),
            event_key,
        }
    }
}
