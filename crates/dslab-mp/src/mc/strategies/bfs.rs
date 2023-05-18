//! Implementation of model checking BFS search strategy.

use colored::*;
use std::collections::{HashSet, VecDeque};

use crate::mc::strategy::{
    CollectFn, ExecutionMode, GoalFn, InvariantFn, McResult, McSummary, PruneFn, Strategy, VisitedStates,
};
use crate::mc::system::{McState, McSystem};

use crate::util::t;

/// The search strategy based on the [BFS](https://en.wikipedia.org/wiki/Breadth-first_search) algorithm.
pub struct Bfs<'a> {
    prune: PruneFn<'a>,
    goal: GoalFn<'a>,
    invariant: InvariantFn<'a>,
    collect: Option<CollectFn<'a>>,
    states_queue: VecDeque<McState>,
    execution_mode: ExecutionMode,
    summary: McSummary,
    visited: VisitedStates,
    collected: HashSet<McState>,
}

impl<'a> Bfs<'a> {
    /// Creates a new Bfs instance with specified user-defined functions and execution mode.
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
            states_queue: VecDeque::new(),
            execution_mode,
            summary: McSummary::default(),
            visited,
            collected: HashSet::new(),
        }
    }
}

impl<'a> Bfs<'a> {
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

impl<'a> Strategy for Bfs<'a> {
    fn run(&mut self, system: &mut McSystem) -> Result<McResult, String> {
        if self.execution_mode == ExecutionMode::Default {
            t!(format!("RUNNING MODEL CHECKING THROUGH EVERY POSSIBLE EXECUTION PATH").yellow())
        }
        let res = self.bfs(system);
        match res {
            Ok(()) => Ok(McResult {
                summary: self.summary.clone(),
                collected: self.collected.clone(),
            }),
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
