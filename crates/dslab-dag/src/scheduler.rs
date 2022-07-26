use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_network::network::Network;

use crate::dag::DAG;
use crate::resource::Resource;
use crate::runner::Config;
use crate::task::TaskState;

#[derive(Debug)]
pub enum Action {
    ScheduleTask {
        task: usize,
        resource: usize,
        cores: u32,
    },
    ScheduleTaskOnCores {
        task: usize,
        resource: usize,
        cores: Vec<u32>,
    },
    TransferData {
        data_item: usize,
        from: Id,
        to: Id,
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
