use crate::mc::strategy::{GoalFn, InvariantFn, LogMode, McSummary, PruneFn, Strategy};
use crate::mc::system::McSystem;

pub struct Dfs {
    prune: PruneFn,
    goal: GoalFn,
    invariant: InvariantFn,
    search_depth: u64,
    log_mode: LogMode,
    summary: McSummary,
}

impl Dfs {
    pub fn new(
        prune: PruneFn,
        goal: GoalFn,
        invariant: InvariantFn,
        log_mode: LogMode,
    ) -> Self {
        Self {
            prune,
            goal,
            invariant,
            search_depth: 0,
            log_mode,
            summary: McSummary::default(),
        }
    }
}

impl Dfs {
    fn dfs(&mut self, system: &mut McSystem) -> Result<(), String> {
        let events_num = system.events.len();
        let state = system.get_state(self.search_depth);

        // Checking invariant on every step
        (self.invariant)(&state)?;

        // Check final state of the system
        if let Some(status) = (self.goal)(&state) {
            self.update_summary(status);
            return Ok(());
        }

        // Check if execution branch is pruned
        if let Some(status) = (self.prune)(&state) {
            self.update_summary(status);
            return Ok(());
        }

        // exhausted without goal completed
        if events_num == 0 {
            return Err("nothing left to do to reach the goal".to_owned());
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
