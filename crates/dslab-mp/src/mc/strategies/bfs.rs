use std::collections::VecDeque;

use crate::mc::strategy::{GoalFn, InvariantFn, LogMode, McSummary, PruneFn, Strategy, VisitedStates};
use crate::mc::system::{McState, McSystem};

pub struct Bfs {
    prune: PruneFn,
    goal: GoalFn,
    invariant: InvariantFn,
    search_depth: u64,
    states_queue: VecDeque<McState>,
    log_mode: LogMode,
    summary: McSummary,
    visited: VisitedStates,
}

impl Bfs {
    pub fn new(prune: PruneFn, goal: GoalFn, invariant: InvariantFn, log_mode: LogMode) -> Self {
        let visited = Self::initialize_visited(&log_mode);
        Self {
            prune,
            goal,
            invariant,
            search_depth: 0,
            states_queue: VecDeque::new(),
            log_mode,
            summary: McSummary::default(),
            visited,
        }
    }
}

impl Bfs {
    fn bfs(&mut self, system: &mut McSystem) -> Result<(), String> {
        // Start search from initial state
        self.states_queue.push_back(system.get_state(self.search_depth));

        while !self.states_queue.is_empty() {
            let events_num = system.events.len();
            let state = self.states_queue.pop_front().expect("BFS error");
            self.search_depth = state.search_depth;

            let result = if self.have_visited(&state) {
                // Was already visited before
                Some(Ok(()))
            } else if let Err(err) = (self.invariant)(&state) {
                // Invariant is broken
                Some(Err(err))
            } else if let Some(status) = (self.goal)(&state) {
                // Reached final state of the system
                self.update_summary(status);
                Some(Ok(()))
            } else if let Some(status) = (self.prune)(&state) {
                // Execution branch is pruned
                self.update_summary(status);
                Some(Ok(()))
            } else if events_num == 0 {
                // exhausted without goal completed
                Some(Err("nothing left to do to reach the goal".to_owned()))
            } else {
                None
            };

            if let Some(result) = result {
                self.mark_visited(state);
                return result;
            }

            system.set_state(state);
            self.mark_visited(system.get_state(self.search_depth));

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

    fn visited(&mut self) -> &mut VisitedStates {
        &mut self.visited
    }
}
