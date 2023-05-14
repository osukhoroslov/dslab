//! Timers for simulation

use std::{cell::RefCell, cmp::Ordering, rc::Rc};

use crate::Id;

use super::shared_state::AwaitResultSetter;

/// Timer Identifier
pub type TimerId = u64;

/// Timer will set the given `state` as completed at time
#[derive(Clone)]
#[allow(dead_code)]
pub struct Timer {
    /// unique identifier of timer
    pub id: TimerId,
    /// id of simulation component the timer was set to
    pub component_id: Id,
    /// the time when Timer will be fired
    pub time: f64,
    /// state to set completed after timer fired
    pub(crate) state: Rc<RefCell<dyn AwaitResultSetter>>,
}

impl Timer {
    /// Create a timer
    #[allow(dead_code)]
    pub(crate) fn new(id: TimerId, component_id: Id, time: f64, state: Rc<RefCell<dyn AwaitResultSetter>>) -> Self {
        Self {
            id,
            component_id,
            time,
            state,
        }
    }
}

impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl Eq for Timer {}

impl Ord for Timer {
    fn cmp(&self, other: &Self) -> Ordering {
        other.time.total_cmp(&self.time).then_with(|| other.id.cmp(&self.id))
    }
}

impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
