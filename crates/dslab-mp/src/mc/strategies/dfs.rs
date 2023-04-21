//! Implementation of model checking DFS search strategy.

use crate::mc::strategy::{ExecutionMode, GoalFn, InvariantFn, McSummary, PruneFn, Strategy, VisitedStates};
use crate::mc::system::{McState, McSystem};

/// The search strategy based on the [DFS](https://en.wikipedia.org/wiki/Depth-first_search) algorithm.
pub struct Dfs {
    prune: PruneFn,
    goal: GoalFn,
    invariant: InvariantFn,
    execution_mode: ExecutionMode,
    summary: McSummary,
    visited: VisitedStates,
}

impl Dfs {
    /// Creates a new Dfs instance with specified user-defined functions and execution mode.
    pub fn new(prune: PruneFn, goal: GoalFn, invariant: InvariantFn, execution_mode: ExecutionMode) -> Self {
        let visited = Self::initialize_visited(&execution_mode);
        Self {
            prune,
            goal,
            invariant,
            execution_mode,
            summary: McSummary::default(),
            visited,
        }
    }
}

impl Dfs {
    fn dfs(&mut self, system: &mut McSystem, state: McState) -> Result<(), String> {
        let available_events = system.available_events();

        let result = self.check_state(&state);

        self.mark_visited(state);
        if let Some(result) = result {
            return result;
        }

        for event_id in available_events {
            self.process_event(system, event_id)?;
        }
        Ok(())
    }
}

impl Strategy for Dfs {
    fn run(&mut self, system: &mut McSystem) -> Result<McSummary, String> {
        let state = system.get_state();

        let res = self.dfs(system, state);
        match res {
            Ok(()) => Ok(self.summary.clone()),
            Err(err) => Err(err),
        }
    }

    fn search_step_impl(&mut self, system: &mut McSystem, state: McState) -> Result<(), String> {
        self.dfs(system, state)
    }

    fn execution_mode(&self) -> &ExecutionMode {
        &self.execution_mode
    }

    fn visited(&mut self) -> &mut VisitedStates {
        &mut self.visited
    }

    fn prune(&self) -> &PruneFn {
        &self.prune
    }

    fn goal(&self) -> &GoalFn {
        &self.goal
    }

    fn invariant(&self) -> &InvariantFn {
        &self.invariant
    }

    fn summary(&mut self) -> &mut McSummary {
        &mut self.summary
    }
}
