use crate::mc::strategy::{LogMode, McSummary, Strategy};
use crate::mc::system::{McState, McSystem};

pub struct Dfs {
    prune: Box<dyn Fn(&McState) -> Option<String>>,
    goal: Box<dyn Fn(&McState) -> Option<String>>,
    invariant: Box<dyn Fn(&McState) -> Result<(), String>>,
    search_depth: u64,
    mode: LogMode,
}

impl Dfs {
    pub fn new(
        prune: Box<dyn Fn(&McState) -> Option<String>>,
        goal: Box<dyn Fn(&McState) -> Option<String>>,
        invariant: Box<dyn Fn(&McState) -> Result<(), String>>,
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
    fn run(&mut self, system: &mut McSystem) -> Result<McSummary, String> {
        let events_num = system.events.borrow().len();
        let state = system.get_state(self.search_depth);

        // Checking invariant on every step
        if let Err(inv_broken) = (self.invariant)(&state) {
            return Err(inv_broken);
        }

        // Check final state of the system
        if events_num == 0 {
            if let Some(status) = (self.goal)(&state) {
                let mut summary = McSummary::default();
                let counter = summary.states.entry(status).or_insert(0);
                *counter = *counter + 1;
                return Ok(summary);
            }
            return Ok(McSummary::default());
        }

        // Check if execution branch is pruned
        if let Some(status) = (self.prune)(&state) {
            let mut summary = McSummary::default();
            let counter = summary.states.entry(status).or_insert(0);
            *counter = *counter + 1;
            return Ok(summary);
        }

        let mut summary = McSummary::default();

        for i in 0..events_num {
            let state = system.get_state(self.search_depth);
            let event = system.events.borrow_mut().remove(i);

            if let LogMode::Debug = self.mode {
                Self::debug_log(&event, self.search_depth);
            }

            system.apply_event(event);

            self.search_depth += 1;
            let run_success = self.run(system);
            self.search_depth -= 1;

            if let Ok(rec_summary) = run_success {
                for (status, cnt) in rec_summary.states.into_iter() {
                    let cnt_ref = summary.states.entry(status).or_insert(0);
                    *cnt_ref = *cnt_ref + cnt;
                }
            } else {
                return run_success;
            }

            system.set_state(state);
        }
        Ok(summary)
    }

    fn log_mode(&self) -> LogMode {
        self.mode.clone()
    }
}
