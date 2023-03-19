use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use crate::mc::strategy::{GoalFn, InvariantFn, LogMode, McSummary, PruneFn, Strategy};
use crate::mc::system::{McState, McSystem};

enum VisitedStates {
    Full(HashSet<McState>),
    Partial(HashSet<u64>),
}

pub struct Dfs {
    prune: PruneFn,
    goal: GoalFn,
    invariant: InvariantFn,
    search_depth: u64,
    log_mode: LogMode,
    summary: McSummary,
    visited: VisitedStates,
}

impl Dfs {
    pub fn new(prune: PruneFn, goal: GoalFn, invariant: InvariantFn, log_mode: LogMode) -> Self {
        let visited = match log_mode {
            LogMode::Debug => VisitedStates::Full(HashSet::default()),
            LogMode::Default => VisitedStates::Partial(HashSet::default()),
        };
        Self {
            prune,
            goal,
            invariant,
            search_depth: 0,
            log_mode,
            summary: McSummary::default(),
            visited,
        }
    }
}

impl Dfs {
    fn dfs(&mut self, system: &mut McSystem) -> Result<(), String> {
        let events_num = system.events.len();
        let state = system.get_state(self.search_depth);

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

        self.mark_visited(state);
        if let Some(result) = result {
            return result;
        }

        for i in 0..events_num {
            self.process_event(system, i)?;
        }
        Ok(())
    }

    fn update_summary(&mut self, status: String) {
        if let LogMode::Debug = self.log_mode {
            let counter = self.summary.states.entry(status).or_insert(0);
            *counter = *counter + 1;
        }
    }

    fn have_visited(&self, state: &McState) -> bool {
        match self.visited {
            VisitedStates::Full(ref states) => states.contains(state),
            VisitedStates::Partial(ref hashes) => {
                let mut h = DefaultHasher::default();
                state.hash(&mut h);
                hashes.contains(&h.finish())
            }
        }
    }

    fn mark_visited(&mut self, state: McState) {
        match self.visited {
            VisitedStates::Full(ref mut states) => {
                states.insert(state);
            }
            VisitedStates::Partial(ref mut hashes) => {
                let mut h = DefaultHasher::default();
                state.hash(&mut h);
                hashes.insert(h.finish());
            }
        }
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

    fn search_step_impl(&mut self, system: &mut McSystem) -> Result<(), String> {
        self.search_depth += 1;
        let result = self.dfs(system);
        self.search_depth -= 1;
        result
    }

    fn log_mode(&self) -> &LogMode {
        &self.log_mode
    }

    fn search_depth(&self) -> u64 {
        self.search_depth
    }
}
