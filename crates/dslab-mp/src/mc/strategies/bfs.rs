//! Implementation of model checking BFS search strategy.

use std::collections::VecDeque;

use crate::mc::state::McState;
use crate::mc::strategy::{
    CollectFn, ExecutionMode, GoalFn, InvariantFn, McResult, McStats, PruneFn, Strategy, VisitedStates,
};
use crate::mc::system::McSystem;

/// The search strategy based on the [BFS](https://en.wikipedia.org/wiki/Breadth-first_search) algorithm.
pub struct Bfs {
    prune: PruneFn,
    goal: GoalFn,
    invariant: InvariantFn,
    collect: CollectFn,
    states_queue: VecDeque<McState>,
    execution_mode: ExecutionMode,
    stats: McStats,
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
            collect: Box::new(|_| false),
            states_queue: VecDeque::new(),
            execution_mode,
            stats: McStats::default(),
            visited,
        }
    }
}

impl Bfs {
    fn bfs(&mut self, system: &mut McSystem) -> Result<(), String> {
        // Start search from initial state
        self.states_queue.push_back(system.get_state());

        while !self.states_queue.is_empty() {
            let state = self.states_queue.pop_front().unwrap();

            if let Some(result) = self.check_state(&state) {
                result?;
                continue;
            }

            system.set_state(state);
            let available_events = system.available_events();
            for event_id in available_events {
                self.process_event(system, event_id)?;
            }
        }

        Ok(())
    }
}

impl Strategy for Bfs {
    fn run(&mut self, system: &mut McSystem) -> McResult {
        let res = self.bfs(system);
        match res {
            Ok(()) => Ok(self.stats.clone()),
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
