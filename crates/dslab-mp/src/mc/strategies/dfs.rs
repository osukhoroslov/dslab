//! Implementation of model checking DFS search strategy.

use sugars::boxed;

use crate::mc::state::McState;
use crate::mc::strategy::{
    default_collect, default_goal, default_invariant, default_prune, CollectFn, ExecutionMode, GoalFn, InvariantFn,
    McResult, McStats, PruneFn, Strategy, VisitedStates,
};
use crate::mc::system::McSystem;

/// The search strategy based on the [DFS](https://en.wikipedia.org/wiki/Depth-first_search) algorithm.
pub struct Dfs {
    prune: PruneFn,
    goal: GoalFn,
    invariant: InvariantFn,
    collect: CollectFn,
    execution_mode: ExecutionMode,
    stats: McStats,
    visited: VisitedStates,
}

impl Default for Dfs {
    /// Creates a new Dfs instance.
    fn default() -> Self {
        Self {
            prune: boxed!(default_prune),
            goal: boxed!(default_goal),
            invariant: boxed!(default_invariant),
            collect: boxed!(default_collect),
            execution_mode: ExecutionMode::Default,
            stats: McStats::default(),
            visited: VisitedStates::Empty,
        }
    }
}
impl Dfs {
    fn dfs(&mut self, system: &mut McSystem, state: McState) -> Result<(), String> {
        let available_events = system.available_events();

        if let Some(result) = self.check_state(&state) {
            return result;
        }

        for event_id in available_events {
            self.process_event(system, event_id)?;
        }
        Ok(())
    }
}

impl Strategy for Dfs {
    fn run(&mut self, system: &mut McSystem) -> McResult {
        let state = system.get_state();

        let res = self.dfs(system, state);
        match res {
            Ok(()) => Ok(self.stats.clone()),
            Err(err) => Err(err),
        }
    }

    fn search_step_impl(&mut self, system: &mut McSystem, state: McState) -> Result<(), String> {
        self.dfs(system, state)
    }

    fn execution_mode(&self) -> &ExecutionMode {
        &self.execution_mode
    }

    fn set_execution_mode(&mut self, execution_mode: ExecutionMode) {
        self.execution_mode = execution_mode;
        *self.visited() = Self::initialize_visited(&self.execution_mode);
    }

    fn visited(&mut self) -> &mut VisitedStates {
        &mut self.visited
    }

    fn prune(&mut self) -> &mut PruneFn {
        &mut self.prune
    }

    fn goal(&mut self) -> &mut GoalFn {
        &mut self.goal
    }

    fn invariant(&mut self) -> &mut InvariantFn {
        &mut self.invariant
    }

    fn collect(&mut self) -> &mut CollectFn {
        &mut self.collect
    }

    fn stats(&mut self) -> &mut McStats {
        &mut self.stats
    }
}
