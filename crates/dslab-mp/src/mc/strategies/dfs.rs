use crate::mc::strategy::{GoalFn, InvariantFn, LogMode, McSummary, PruneFn, Strategy, VisitedStates};
use crate::mc::system::McSystem;

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
        let visited = Self::initialize_visited(&log_mode);
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
        let available_events = system.available_events();
        let state = system.get_state(self.search_depth);

        let result = self.check_state(&state, available_events.len());

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
