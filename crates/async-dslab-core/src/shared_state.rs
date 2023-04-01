use dslab_core::event::EventData;
use dslab_core::{Event, Id};
use serde::Serialize;

use std::any::TypeId;
use std::rc::Rc;
use std::{cell::RefCell, future::Future, sync::Arc, task::Context};
use std::{
    pin::Pin,
    task::{Poll, Waker},
};

use crate::timer::Timer;

#[derive(Serialize)]
struct EmptyData {}

pub enum AwaitResult {
    Timeout(Event),
    Ok(Event),
}

impl Default for AwaitResult {
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

impl AwaitResult {
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

#[derive(Default)]
pub struct SharedState {
    /// Whether or not the sleep time has elapsed
    pub completed: bool,

    pub waker: Option<Waker>,

    pub shared_content: AwaitResult,
}

impl SharedState {
    pub fn set_ok_completed_with_event(&mut self, e: Event) {
        if self.completed {
            return;
        }

        self.shared_content = AwaitResult::Ok(e);
        self.set_completed();
    }

    pub fn set_completed(&mut self) {
        if self.completed {
            return;
        }
        self.completed = true;
        if let Some(waker) = self.waker.take() {
            waker.wake()
        }
    }
}

pub struct EventFuture {
    pub state: Rc<RefCell<SharedState>>,
}

impl Future for EventFuture {
    type Output = AwaitResult;
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
    pub state: Rc<RefCell<SharedState>>,
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
