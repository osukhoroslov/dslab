use network::network::Network;
use simcore::context::SimulationContext;

use crate::dag::DAG;
use crate::resource::Resource;
use crate::runner::Config;
use crate::task::TaskState;

#[derive(Debug)]
pub enum Action {
    Schedule {
        task: usize,
        resource: usize,
        cores: u32,
    },
    ScheduleOnCores {
        task: usize,
        resource: usize,
        cores: Vec<u32>,
    },
}

pub trait Scheduler {
    fn start(
        &mut self,
        dag: &DAG,
        resources: &Vec<Resource>,
        network: &Network,
        config: Config,
        ctx: &SimulationContext,
    ) -> Vec<Action>;
    fn on_task_state_changed(
        &mut self,
        task: usize,
        task_state: TaskState,
        dag: &DAG,
        resources: &Vec<Resource>,
        ctx: &SimulationContext,
    ) -> Vec<Action>;
}
