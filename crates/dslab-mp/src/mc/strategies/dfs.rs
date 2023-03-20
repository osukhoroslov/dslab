use crate::mc::strategy::{GoalFn, InvariantFn, ExecutionMode, McSummary, PruneFn, Strategy, VisitedStates};
use crate::mc::system::McSystem;

pub struct Dfs {
    prune: PruneFn,
    goal: GoalFn,
    invariant: InvariantFn,
    search_depth: u64,
    execution_mode: ExecutionMode,
    summary: McSummary,
    visited: VisitedStates,
}

impl Dfs {
    pub fn new(prune: PruneFn, goal: GoalFn, invariant: InvariantFn, execution_mode: ExecutionMode) -> Self {
        let visited = Self::initialize_visited(&execution_mode);
        Self {
            prune,
            goal,
            invariant,
            search_depth: 0,
            execution_mode,
            summary: McSummary::default(),
            visited,
        }
    }
}

impl Dfs {
    fn dfs(&mut self, system: &mut McSystem) -> Result<(), String> {
        let events_num = system.events.len();
        let state = system.get_state(self.search_depth);

        let result = self.check_state(&state, events_num);

        self.mark_visited(state);
        if let Some(result) = result {
            return result;
        }

        for i in 0..events_num {
            self.process_event(system, i)?;
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

    fn search_step_impl(&mut self, system: &mut McSystem) -> Result<(), String> {
        self.search_depth += 1;
        let result = self.dfs(system);
        self.search_depth -= 1;
        result
    }

    fn execution_mode(&self) -> &ExecutionMode {
        &self.execution_mode
    }

    fn search_depth(&self) -> u64 {
        self.search_depth
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
