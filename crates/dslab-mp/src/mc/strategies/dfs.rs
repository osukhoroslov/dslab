use crate::mc::strategy::{LogMode, Strategy};
use crate::mc::system::{McState, McSystem};

pub struct Dfs {
    prune: Box<dyn Fn(&McState) -> bool>,
    goal: Box<dyn Fn(&McState) -> bool>,
    invariant: Box<dyn Fn(&McState) -> bool>,
    search_depth: u64,
    mode: LogMode,
}

impl Dfs {
    pub fn new(
        prune: Box<dyn Fn(&McState) -> bool>,
        goal: Box<dyn Fn(&McState) -> bool>,
        invariant: Box<dyn Fn(&McState) -> bool>,
        mode: LogMode,
    ) -> Self {
        Self {
            prune,
            goal,
            invariant,
            search_depth: 0,
            mode,
        }
    }
}

impl Strategy for Dfs {
    fn run(&mut self, system: &mut McSystem) -> bool {
        let events_num = system.events.borrow().len();
        let state = system.get_state(self.search_depth);

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

        for i in 0..events_num {
            let state = system.get_state(self.search_depth);
            let event = system.events.borrow_mut().remove(i);

            self.debug_log(&event, self.search_depth);

            system.apply_event(event);

            self.search_depth += 1;
            let run_success = self.run(system);
            self.search_depth -= 1;

            if !run_success {
                return false;
            }

            system.set_state(state);
        }
        true
    }

    fn log_mode(&self) -> &LogMode {
        &self.mode
    }
}
