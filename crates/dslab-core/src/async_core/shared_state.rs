//! Shared state and event notification

use std::any::{Any, TypeId};
use std::rc::Rc;
use std::{cell::RefCell, future::Future, task::Context};
use std::{
    pin::Pin,
    task::{Poll, Waker},
};

use serde::Serialize;

use crate::async_core::await_details::{AwaitResult, DetailsKey};
use crate::event::EventData;
use crate::{async_details_core, Event, Id};

#[derive(Serialize, Clone)]
pub(crate) struct EmptyData {}

pub(crate) struct AwaitEventSharedState<T: EventData> {
    pub completed: bool,

    pub waker: Option<Waker>,

    pub shared_content: AwaitResult<T>,
}

impl<T: EventData> Default for AwaitEventSharedState<T> {
    fn default() -> Self {
        Self {
            completed: false,
            waker: None,
            shared_content: AwaitResult::<T>::default(),
        }
    }
}

pub(crate) trait AwaitResultSetter: Any {
    fn set_ok_completed_with_event(&mut self, e: Event);
    fn set_completed(&mut self);
    fn is_completed(&self) -> bool;
}

impl<T: EventData> AwaitResultSetter for AwaitEventSharedState<T> {
    fn is_completed(&self) -> bool {
        self.completed
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

    fn set_completed(&mut self) {
        if self.completed {
            return;
        }
        self.completed = true;
        if let Some(waker) = self.waker.take() {
            waker.wake()
        }
    }
}

/// Future represents AwaitResult for event (Ok or Timeout)
pub struct EventFuture<T: EventData> {
    /// state with event data
    pub(crate) state: Rc<RefCell<AwaitEventSharedState<T>>>,
}

impl<T: EventData> Future for EventFuture<T> {
    type Output = AwaitResult<T>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
        let mut state = self.state.as_ref().borrow_mut();

        if !state.completed {
            state.waker = Some(_cx.waker().clone());
            return Poll::Pending;
        }

        let mut filler = AwaitResult::default();
        std::mem::swap(&mut filler, &mut state.shared_content);

        Poll::Ready(filler)
    }
}

/// Future that represents timer from simulation
pub struct TimerFuture {
    /// state that should be completed after timer fired
    pub(crate) state: Rc<RefCell<AwaitEventSharedState<EmptyData>>>,
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
        // println!("Polling EventFuture...{}", self.state.borrow().completed);
        let mut state = self.state.as_ref().borrow_mut();

        if !state.completed {
            state.waker = Some(_cx.waker().clone());
            return Poll::Pending;
        }

        Poll::Ready(())
    }
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct AwaitKey {
    pub from: Id,
    pub to: Id,
    pub msg_type: TypeId,
    details: DetailsKey,
}

impl AwaitKey {
    pub fn new<T: EventData>(from: Id, to: Id) -> Self {
        Self {
            from,
            to,
            msg_type: TypeId::of::<T>(),
            details: 0,
        }
    }

    pub fn new_by_ref(from: Id, to: Id, data: &dyn EventData) -> Self {
        Self {
            from,
            to,
            msg_type: data.type_id(),
            details: 0,
        }
    }

    async_details_core! {
        pub fn new_with_details<T: EventData>(from: Id, to: Id, details: DetailsKey) -> Self {
            Self {
                from,
                to,
                msg_type: TypeId::of::<T>(),
                details,
            }
        }

        pub fn new_with_details_by_ref(from: Id, to: Id, data: &dyn EventData, details: DetailsKey) -> Self {
            Self {
                from,
                to,
                msg_type: data.type_id(),
                details,
            }
        }
    }
}
