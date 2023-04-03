use std::{cell::RefCell, cmp::Ordering, rc::Rc, sync::Arc};

use dslab_core::{Event, Id};

use crate::shared_state::{EventSetter, SharedState};

pub struct Timer {
    pub id: Id,
    pub time: f64,
    pub state: Rc<RefCell<dyn EventSetter>>,
}

impl Timer {
    pub fn new(time: f64, state: Rc<RefCell<dyn EventSetter>>) -> Self {
        static mut TIMER_COUNTER: Id = 0;
        unsafe {
            TIMER_COUNTER += 1;
            Self {
                id: TIMER_COUNTER,
                time,
                state,
            }
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
