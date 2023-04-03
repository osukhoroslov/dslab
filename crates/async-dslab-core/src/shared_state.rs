use dslab_core::event::EventData;
use dslab_core::{Event, Id};
use serde::Serialize;

use std::any::{Any, TypeId};
use std::rc::Rc;
use std::{cell::RefCell, future::Future, sync::Arc, task::Context};
use std::{
    pin::Pin,
    task::{Poll, Waker},
};

use crate::timer::Timer;

#[derive(Serialize)]
pub struct EmptyData {}

pub enum AwaitResult<T: EventData> {
    Timeout(Event),
    Ok((Event, T)),
}

impl<T: EventData> Default for AwaitResult<T> {
    fn default() -> Self {
        Self::Timeout(Event {
            id: 0,
            time: 0.,
            src: 0,
            dest: 0,
            data: Box::new(EmptyData {}),
        })
    }
}

impl<T: EventData> AwaitResult<T> {
    pub fn timeout_with(src: Id, dest: Id) -> Self {
        Self::Timeout(Event {
            id: 0,
            time: 0.,
            src,
            dest,
            data: Box::new(EmptyData {}),
        })
    }
}

pub struct SharedState<T: EventData> {
    /// Whether or not the sleep time has elapsed
    pub completed: bool,

    pub waker: Option<Waker>,

    pub shared_content: AwaitResult<T>,
}

impl<T: EventData> Default for SharedState<T> {
    fn default() -> Self {
        Self {
            completed: false,
            waker: None,
            shared_content: AwaitResult::<T>::default(),
        }
    }
}

pub trait EventSetter: Any {
    fn set_ok_completed_with_event(&mut self, e: Event);
    fn set_completed(&mut self);
}

impl<T: EventData> EventSetter for SharedState<T> {
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

pub struct EventFuture<T: EventData> {
    pub state: Rc<RefCell<SharedState<T>>>,
}

impl<T: EventData> Future for EventFuture<T> {
    type Output = AwaitResult<T>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
        // println!("Polling EventFuture...{}", self.state.borrow().completed);
        let mut state = self.state.as_ref().borrow_mut();

        if !state.completed {
            state.waker = Some(_cx.waker().clone());
            return Poll::Pending;
        }

        let mut filler = AwaitResult::default();
        std::mem::swap(&mut filler, &mut state.shared_content);

        return Poll::Ready(filler);
    }
}

pub struct TimerFuture {
    pub state: Rc<RefCell<SharedState<EmptyData>>>,
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

        return Poll::Ready(());
    }
}

#[derive(Hash, PartialEq, Eq, Debug, Clone)]
pub struct AwaitKey {
    pub from: Id,
    pub to: Id,
    pub msg_type: TypeId,
}

impl AwaitKey {
    pub fn new<T: EventData>(from: Id, to: Id) -> Self {
        Self {
            from,
            to,
            msg_type: TypeId::of::<T>(),
        }
    }
}
