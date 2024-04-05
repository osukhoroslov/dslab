//! Asynchronous waiting for timers.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::{state::SimulationState, Id};

// Timer identifier.
pub(crate) type TimerId = u64;

// Timer future --------------------------------------------------------------------------------------------------------

/// Future that represents asynchronous waiting for timer completion.
pub struct TimerFuture {
    // Unique timer identifier.
    timer_id: TimerId,
    // State with completion info shared with TimerPromise.
    state: Rc<RefCell<TimerAwaitState>>,
    sim_state: Rc<RefCell<SimulationState>>,
}

impl TimerFuture {
    fn new(timer_id: TimerId, state: Rc<RefCell<TimerAwaitState>>, sim_state: Rc<RefCell<SimulationState>>) -> Self {
        Self {
            timer_id,
            state,
            sim_state,
        }
    }
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, async_ctx: &mut Context) -> Poll<Self::Output> {
        let mut state = self.state.as_ref().borrow_mut();
        if state.completed {
            Poll::Ready(())
        } else {
            state.waker = Some(async_ctx.waker().clone());
            Poll::Pending
        }
    }
}

impl Drop for TimerFuture {
    fn drop(&mut self) {
        // We cannot call SimulationState::on_incomplete_timer_future_drop when dropping futures on component handler
        // removal, because sim_state is already mutably borrowed in SimulationState::cancel_component_timers.
        // Instead, we do the necessary clean up directly in SimulationState::cancel_component_timers and set the
        // manually_dropped flag in the state.
        if !self.state.borrow().completed && !self.state.borrow().manually_dropped {
            self.sim_state
                .borrow_mut()
                .on_incomplete_timer_future_drop(self.timer_id);
        }
    }
}

// Timer promise -------------------------------------------------------------------------------------------------------

#[derive(Clone)]
pub(crate) struct TimerPromise {
    // Unique timer identifier.
    pub id: TimerId,
    // Id of simulation component that set the timer.
    pub component_id: Id,
    // The time when the timer will be fired.
    pub time: f64,
    // State with completion info shared with TimerFuture.
    state: Rc<RefCell<TimerAwaitState>>,
}

impl TimerPromise {
    pub(crate) fn new(id: TimerId, component_id: Id, time: f64) -> Self {
        Self {
            id,
            component_id,
            time,
            state: Rc::new(RefCell::new(TimerAwaitState::new())),
        }
    }

    pub fn future(&self, sim_state: Rc<RefCell<SimulationState>>) -> TimerFuture {
        TimerFuture::new(self.id, self.state.clone(), sim_state)
    }

    pub fn complete(&self) {
        self.state.borrow_mut().complete();
    }

    // When cancelling asynchronous waiting for timer we need to break a reference cycle
    // between TimerFuture and Task by dropping the state which stores Task as a Waker.
    pub fn drop_state(&self) {
        // Take the waker out and drop it when the state borrow is released
        let _waker = self.state.borrow_mut().drop();
    }
}

impl Eq for TimerPromise {}

impl PartialEq for TimerPromise {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Ord for TimerPromise {
    fn cmp(&self, other: &Self) -> Ordering {
        other.time.total_cmp(&self.time).then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for TimerPromise {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct TimerAwaitState {
    pub completed: bool,
    pub manually_dropped: bool,
    pub waker: Option<Waker>,
}

impl TimerAwaitState {
    pub fn new() -> Self {
        Self {
            completed: false,
            manually_dropped: false,
            waker: None,
        }
    }

    pub fn complete(&mut self) {
        self.completed = true;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    pub fn drop(&mut self) -> Option<Waker> {
        self.manually_dropped = true;
        // We cannot drop the waker immediately here because it will trigger TimerFuture::drop,
        // which requires borrowing of (already mutably borrowed) state.
        // Instead, we take the waker out of scope to drop it when the state borrow is released.
        self.waker.take()
    }
}
