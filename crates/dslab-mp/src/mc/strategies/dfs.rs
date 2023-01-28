use std::cell::RefCell;
use std::rc::Rc;

use crate::mc::strategy::Strategy;
use crate::mc::system::{McState, McSystem};

pub struct Dfs {
    prune: Box<dyn Fn(&McState) -> bool>,
    goal: Box<dyn Fn(&McState) -> bool>,
    invariant: Box<dyn Fn(&McState) -> bool>,
}

impl Dfs {
    pub fn new(
        prune: Box<dyn Fn(&McState) -> bool>,
        goal: Box<dyn Fn(&McState) -> bool>,
        invariant: Box<dyn Fn(&McState) -> bool>,
    ) -> Self {
        Self { prune, goal, invariant }
    }
}

impl Strategy for Dfs {
    fn run(&mut self, system: Rc<RefCell<McSystem>>) -> bool {
        let events_num = system.borrow().events.borrow().len();

        {
            let state = system.borrow().get_state();

            // Checking invariant on every step
            if !(self.invariant)(&state) {
                return false;
            }

            // Check final state of the system
            if events_num == 0 {
                return (self.goal)(&state);
            }

            // Check if execution branch is pruned
            if (self.prune)(&state) {
                return true;
            }
        }

        for i in 0..events_num {
            let state = system.borrow().get_state();
            let event = system.borrow_mut().events.borrow_mut().remove(i);
            system.borrow_mut().apply_event(event);
            if !self.run(system.clone()) {
                return false;
            }
            system.borrow_mut().set_state(state);
        }
        return true;
    }
}
