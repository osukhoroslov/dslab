use crate::mc::strategy::{LogMode, McSummary, Strategy};
use crate::mc::system::{McState, McSystem};

pub struct Dfs {
    prune: Box<dyn Fn(&McState) -> Option<String>>,
    goal: Box<dyn Fn(&McState) -> Option<String>>,
    invariant: Box<dyn Fn(&McState) -> Result<(), String>>,
    search_depth: u64,
    log_mode: LogMode,
    summary: McSummary,
}

impl Dfs {
    pub fn new(
        prune: Box<dyn Fn(&McState) -> Option<String>>,
        goal: Box<dyn Fn(&McState) -> Option<String>>,
        invariant: Box<dyn Fn(&McState) -> Result<(), String>>,
        log_mode: LogMode,
    ) -> Self {
        Self {
            prune,
            goal,
            invariant,
            search_depth: 0,
            log_mode,
            summary: McSummary::default(),
        }
    }
}

impl Dfs {
    fn dfs(&mut self, system: &mut McSystem) -> Result<(), String> {
        let events_num = system.events.borrow().len();
        let state = system.get_state(self.search_depth);

        // Checking invariant on every step
        if let Err(inv_broken) = (self.invariant)(&state) {
            return Err(inv_broken);
        }

        // Check final state of the system
        if let Some(status) = (self.goal)(&state) {
            if let LogMode::Debug = self.log_mode {
                let counter = self.summary.states.entry(status).or_insert(0);
                *counter = *counter + 1;
            }
            return Ok(());
        }

        // Check if execution branch is pruned
        if let Some(status) = (self.prune)(&state) {
            if let LogMode::Debug = self.log_mode {
                let counter = self.summary.states.entry(status).or_insert(0);
                *counter = *counter + 1;
            }
            return Ok(());
        }

        // exhausted without goal completed
        if events_num == 0 {
            return Err("nothing left to do to reach the goal".to_owned());
        }

        for i in 0..events_num {
            let state = system.get_state(self.search_depth);
            let event = system.events.borrow_mut().remove(i);

            self.debug_log(&event, self.search_depth);

            let new_events = system.apply_event(event);
            let mut possible_events = Vec::new();
            for e in new_events {
                if !e.can_be_dropped {
                    system.events.borrow_mut().push(e.event);
                } else {
                    possible_events.push(e.event);
                }
            }
            // TODO: explore system executions that contain subsets of possible events!

            self.search_depth += 1;
            let run_success = self.dfs(system);
            self.search_depth -= 1;

            if let Err(err) = run_success {
                return Err(err);
            }

            system.set_state(state);
        }
        Ok(())
    }
}

impl Strategy for Dfs {
    fn run(&mut self, system: &mut McSystem) -> Result<McSummary, String> {
        let res = self.dfs(system);
        match res {
            Ok(()) => Ok(self.summary.clone()),
            Err(err) => Err(err),
        }
    }

    fn log_mode(&self) -> &LogMode {
        &self.log_mode
    }
}
