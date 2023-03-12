use std::collections::VecDeque;

use crate::mc::strategy::{LogMode, McSummary, Strategy};
use crate::mc::system::{McState, McSystem};

pub struct Bfs {
    prune: Box<dyn Fn(&McState) -> Option<String>>,
    goal: Box<dyn Fn(&McState) -> Option<String>>,
    invariant: Box<dyn Fn(&McState) -> Result<(), String>>,
    search_depth: u64,
    states_queue: VecDeque<McState>,
    log_mode: LogMode,
    summary: McSummary,
}

impl Bfs {
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
            states_queue: VecDeque::new(),
            log_mode,
            summary: McSummary::default(),
        }
    }
}

impl Bfs {
    fn bfs(&mut self, system: &mut McSystem) -> Result<(), String> {
        // Start search from initial state
        self.states_queue.push_back(system.get_state(self.search_depth));

        while !self.states_queue.is_empty() {
            let state = self.states_queue.pop_front().expect("BFS error");
            self.search_depth = state.search_depth;

            // Checking invariant on every step
            (self.invariant)(&state)?;

            // Check final state of the system
            if let Some(status) = (self.goal)(&state) {
                self.update_summary(status);
                continue;
            }

            // Check if execution branch is pruned
            if let Some(status) = (self.prune)(&state) {
                self.update_summary(status);
                continue;
            }

            system.set_state(state);
            let events_num = system.events.len();

            // exhausted without goal completed
            if events_num == 0 {
                return Err("nothing left to do to reach the goal".to_owned());
            }

            for i in 0..events_num {
                self.process_event(system, i)?;
            }
        }

        Ok(())
    }

    fn update_summary(&mut self, status: String) {
        if let LogMode::Debug = self.log_mode {
            let counter = self.summary.states.entry(status).or_insert(0);
            *counter = *counter + 1;
        }
    }
}

impl Strategy for Bfs {
    fn run(&mut self, system: &mut McSystem) -> Result<McSummary, String> {
        let res = self.bfs(system);
        match res {
            Ok(()) => Ok(self.summary.clone()),
            Err(err) => Err(err),
        }
    }

    fn search_step_impl(&mut self, system: &mut McSystem) -> Result<(), String> {
        self.states_queue.push_back(system.get_state(self.search_depth + 1));
        Ok(())
    }

    fn log_mode(&self) -> &LogMode {
        &self.log_mode
    }

    fn search_depth(&self) -> u64 {
        self.search_depth
    }
}
