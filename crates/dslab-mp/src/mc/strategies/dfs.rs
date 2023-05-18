//! Implementation of model checking DFS search strategy.

use std::collections::HashSet;

use colored::*;

use crate::mc::strategy::{
    CollectFn, ExecutionMode, GoalFn, InvariantFn, McResult, McSummary, PruneFn, Strategy, VisitedStates,
};
use crate::mc::system::{McState, McSystem};
use crate::util::t;

/// The search strategy based on the [DFS](https://en.wikipedia.org/wiki/Depth-first_search) algorithm.
pub struct Dfs<'a> {
    prune: PruneFn<'a>,
    goal: GoalFn<'a>,
    invariant: InvariantFn<'a>,
    collect: Option<CollectFn<'a>>,
    execution_mode: ExecutionMode,
    summary: McSummary,
    visited: VisitedStates,
    collected: HashSet<McState>,
}

impl<'a> Dfs<'a> {
    /// Creates a new Dfs instance with specified user-defined functions and execution mode.
    pub fn new(
        prune: PruneFn<'a>,
        goal: GoalFn<'a>,
        invariant: InvariantFn<'a>,
        collect: Option<CollectFn<'a>>,
        execution_mode: ExecutionMode,
    ) -> Self {
        let visited = Self::initialize_visited(&execution_mode);
        Self {
            prune,
            goal,
            invariant,
            collect,
            execution_mode,
            summary: McSummary::default(),
            visited,
            collected: HashSet::new(),
        }
    }
}

impl<'a> Dfs<'a> {
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

impl<'a> Strategy for Dfs<'a> {
    fn run(&mut self, system: &mut McSystem) -> Result<McResult, String> {
        if self.execution_mode != ExecutionMode::Debug {
            t!(format!("RUNNING MODEL CHECKING THROUGH EVERY POSSIBLE EXECUTION PATH").yellow())
        }
        let state = system.get_state();

        let res = self.dfs(system, state);
        match res {
            Ok(()) => Ok(McResult {
                summary: self.summary.clone(),
                collected: self.collected.clone(),
            }),
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

    fn collect(&self) -> &Option<CollectFn> {
        &self.collect
    }

    fn collected(&mut self) -> &mut HashSet<McState> {
        &mut self.collected
    }

    fn summary(&mut self) -> &mut McSummary {
        &mut self.summary
    }
}
