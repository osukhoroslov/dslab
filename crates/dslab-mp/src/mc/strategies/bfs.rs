//! Implementation of model checking BFS search strategy.

use std::collections::VecDeque;

use crate::mc::strategy::{ExecutionMode, GoalFn, InvariantFn, McSummary, PruneFn, Strategy, VisitedStates};
use crate::mc::system::{McState, McSystem};

/// The search strategy based on the [BFS](https://en.wikipedia.org/wiki/Breadth-first_search) algorithm.
pub struct Bfs {
    prune: PruneFn,
    goal: GoalFn,
    invariant: InvariantFn,
    states_queue: VecDeque<McState>,
    execution_mode: ExecutionMode,
    summary: McSummary,
    visited: VisitedStates,
}

impl Bfs {
    /// Creates a new Bfs instance with specified user-defined functions and execution mode.
    pub fn new(prune: PruneFn, goal: GoalFn, invariant: InvariantFn, execution_mode: ExecutionMode) -> Self {
        let visited = Self::initialize_visited(&execution_mode);
        Self {
            prune,
            goal,
            invariant,
            states_queue: VecDeque::new(),
            execution_mode,
            summary: McSummary::default(),
            visited,
        }
    }
}

impl Bfs {
    fn bfs(&mut self, system: &mut McSystem) -> Result<(), String> {
        // Start search from initial state
        self.states_queue.push_back(system.get_state());

        while !self.states_queue.is_empty() {
            let available_events = system.available_events();
            let state = self.states_queue.pop_front().unwrap();

            let result = self.check_state(&state);

            if let Some(result) = result {
                self.mark_visited(state);
                result?;
                continue;
            }

            system.set_state(state);
            self.mark_visited(system.get_state());

            for event_id in available_events {
                self.process_event(system, event_id)?;
            }
        }

        Ok(())
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

    fn search_step_impl(&mut self, _system: &mut McSystem, state: McState) -> Result<(), String> {
        self.states_queue.push_back(state);
        Ok(())
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
