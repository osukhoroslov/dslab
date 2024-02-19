//! Timers for simulation.

use std::{
    cell::RefCell,
    cmp::Ordering,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use crate::{state::SimulationState, Id};

/// Future that represents timer from simulation.
pub struct TimerFuture {
    /// State that should be completed after timer fired.
    state: Rc<RefCell<AwaitTimerSharedState>>,
    sim_state: Rc<RefCell<SimulationState>>,
    timer_id: TimerId,
}

impl TimerFuture {
    pub(crate) fn new(
        state: Rc<RefCell<AwaitTimerSharedState>>,
        sim_state: Rc<RefCell<SimulationState>>,
        timer_id: TimerId,
    ) -> Self {
        Self {
            state,
            sim_state,
            timer_id,
        }
    }
}

impl Drop for TimerFuture {
    fn drop(&mut self) {
        if !self.state.borrow().completed {
            self.sim_state
                .borrow_mut()
                .on_incomplete_timer_future_drop(self.timer_id);
        }
    }
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

/// Timer identifier.
pub(crate) type TimerId = u64;

/// Timer will set the given `state` as completed at time.
#[derive(Clone)]
pub(crate) struct TimerPromise {
    /// Unique identifier of timer.
    pub id: TimerId,
    /// Id of simulation component that set the timer.
    pub component_id: Id,
    /// The time when the timer will be fired.
    pub time: f64,
    /// State to set completed after the timer is fired.
    state: Rc<RefCell<AwaitTimerSharedState>>,
}

impl TimerPromise {
    /// Creates a timer.
    pub(crate) fn new(id: TimerId, component_id: Id, time: f64) -> Self {
        Self {
            id,
            component_id,
            time,
            state: Rc::new(RefCell::new(AwaitTimerSharedState::new())),
        }
    }

    pub fn future(&self, sim_state: Rc<RefCell<SimulationState>>) -> TimerFuture {
        let timer_id = self.id;
        TimerFuture::new(self.state.clone(), sim_state, timer_id)
    }

    pub fn set_completed(&self) {
        self.state.borrow_mut().set_completed();
    }

    /// In case we need to cancel async activity we need to break reference
    /// cycle between TimerFuture and Task. As far as Task is stored inside AwaitTimerSharedState
    /// as Waker, we take it out here and drop when state borrow is released.
    pub fn drop_shared_state(&self) {
        let _waker = self.state.borrow_mut().drop_state();
    }
}

impl PartialEq for TimerPromise {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl Eq for TimerPromise {}

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

pub(crate) struct AwaitTimerSharedState {
    pub completed: bool,
    pub waker: Option<Waker>,
}

impl AwaitTimerSharedState {
    pub fn new() -> Self {
        Self {
            completed: false,
            waker: None,
        }
    }

    pub fn set_completed(&mut self) {
        self.completed = true;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    pub fn drop_state(&mut self) -> Option<Waker> {
        // Set completed to true to prevent calling callback on TimerFuture drop.
        self.completed = true;
        // Take waker out of scope to release &mut self first and avoid several mutable borrows.
        // Next borrow() appears in TimerFuture::drop to check if state is completed.
        self.waker.take()
    }
}
