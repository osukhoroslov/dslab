//! Shared state and event notification.

use std::any::{Any, TypeId};
use std::rc::Rc;
use std::{cell::RefCell, future::Future, task::Context};
use std::{
    pin::Pin,
    task::{Poll, Waker},
};

use serde::Serialize;

use crate::async_core::await_details::{AwaitResult, EventKey};
use crate::event::EventData;
use crate::state::SimulationState;
use crate::{Event, Id};

#[derive(Serialize, Clone)]
pub(crate) struct EmptyData {}

pub struct AwaitEventSharedState<T: EventData> {
    pub completed: bool,
    pub component_id: Id,
    pub waker: Option<Waker>,
    pub shared_content: AwaitResult<T>,
}

impl<T: EventData> AwaitEventSharedState<T> {
    pub fn new(component_id: Id) -> Self {
        Self {
            completed: false,
            component_id,
            waker: None,
            shared_content: AwaitResult::<T>::default(),
        }
    }
}

pub trait AwaitResultSetter: Any {
    fn is_completed(&self) -> bool;
    fn set_completed(&mut self);
    fn set_ok_completed_with_event(&mut self, e: Event);
}

impl<T: EventData> AwaitResultSetter for AwaitEventSharedState<T> {
    fn is_completed(&self) -> bool {
        self.completed
    }

    fn set_completed(&mut self) {
        if self.completed {
            return;
        }
        self.completed = true;
        if let Some(waker) = self.waker.take() {
            waker.wake()
        }
    }

    fn set_ok_completed_with_event(&mut self, mut e: Event) {
        if self.completed {
            return;
        }

        let downcast_result = e.data.downcast::<T>();

        e.data = Box::new(EmptyData {});
        match downcast_result {
            Ok(data) => {
                self.shared_content = AwaitResult::Ok((e, *data));
                self.set_completed();
            }
            Err(_) => {
                panic!("internal downcast conversion error");
            }
        };
    }
}

/// Future that represents AwaitResult for event (Ok or Timeout).
pub struct EventFuture<T: EventData> {
    /// State with event data.
    pub(crate) state: Rc<RefCell<AwaitEventSharedState<T>>>,
    pub(crate) sim_state: Rc<RefCell<SimulationState>>,
}

impl<T: EventData> Future for EventFuture<T> {
    type Output = (Event, T);
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut state = self.state.as_ref().borrow_mut();

        if !state.completed {
            state.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let mut filler = AwaitResult::default();
        std::mem::swap(&mut filler, &mut state.shared_content);

        if let AwaitResult::Ok(data) = filler {
            Poll::Ready(data)
        } else {
            panic!("internal error: unexpected timeout on future ready")
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
    ///     let mut res = client_ctx.recv_event::<Message>(root_id).with_timeout(10.).await;
    ///     match res {
    ///         AwaitResult::Ok(..) => panic!("expect timeout here"),
    ///         AwaitResult::Timeout(e) => {
    ///             assert_eq!(e.src, root_id);
    ///             assert_eq!(e.dst, client_ctx.id());
    ///         }
    ///     }
    ///
    ///     res = client_ctx.recv_event::<Message>(root_id).with_timeout(50.).await;
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
    /// assert_eq!(sim.time(), 60.);
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
    ///             .recv_event_by_key::<SomeEvent>(self.root_id, 1).with_timeout(10.)
    ///             .await;
    ///         if let AwaitResult::Timeout(e) = result {
    ///             assert_eq!(e.src, self.root_id);
    ///         } else {
    ///             panic!("expect result timeout here");
    ///         }
    ///         result = self
    ///             .ctx
    ///             .recv_event_by_key::<SomeEvent>(self.root_id, 1).with_timeout(100.)
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
    ///         let (e, data) = self.ctx.recv_event_by_key::<SomeEvent>(self.root_id, 2).await;
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
    /// assert_eq!(sim.time(), 110.); // because of timers in listen_first
    /// ```
    pub async fn with_timeout(self, timeout: f64) -> AwaitResult<T> {
        assert!(timeout >= 0., "timeout must be a positive value");

        let component_id = self.state.borrow().component_id;
        self.sim_state
            .borrow_mut()
            .add_timer_on_state(component_id, timeout, self.state.clone());

        EventWithTimeoutFuture { state: self.state }.await
    }
}

/// Future that represents timer from simulation.
pub struct TimerFuture {
    /// State that should be completed after timer fired.
    pub(crate) state: Rc<RefCell<AwaitEventSharedState<EmptyData>>>,
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut state = self.state.as_ref().borrow_mut();

        if !state.completed {
            state.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        Poll::Ready(())
    }
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub(crate) struct AwaitKey {
    pub from: Id,
    pub to: Id,
    pub msg_type: TypeId,
    event_key: EventKey,
}

impl AwaitKey {
    pub fn new<T: EventData>(from: Id, to: Id) -> Self {
        Self {
            from,
            to,
            msg_type: TypeId::of::<T>(),
            event_key: 0,
        }
    }

    pub fn new_by_ref(from: Id, to: Id, data: &dyn EventData) -> Self {
        Self {
            from,
            to,
            msg_type: data.type_id(),
            event_key: 0,
        }
    }

    pub fn new_with_event_key<T: EventData>(from: Id, to: Id, event_key: EventKey) -> Self {
        Self {
            from,
            to,
            msg_type: TypeId::of::<T>(),
            event_key,
        }
    }

    pub fn new_with_event_key_by_ref(from: Id, to: Id, data: &dyn EventData, event_key: EventKey) -> Self {
        Self {
            from,
            to,
            msg_type: data.type_id(),
            event_key,
        }
    }
}

struct EventWithTimeoutFuture<T: EventData> {
    pub state: Rc<RefCell<AwaitEventSharedState<T>>>,
}

impl<T: EventData> Future for EventWithTimeoutFuture<T> {
    type Output = AwaitResult<T>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let mut state = self.state.as_ref().borrow_mut();

        if !state.completed {
            state.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let mut filler = AwaitResult::default();
        std::mem::swap(&mut filler, &mut state.shared_content);

        Poll::Ready(filler)
    }
}
