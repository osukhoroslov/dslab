use dslab_core::component::Id;
use dslab_core::context::SimulationContext;
use dslab_network::network::Network;

use crate::dag::DAG;
use crate::resource::Resource;
use crate::runner::Config;
use crate::task::TaskState;

#[derive(Debug)]
pub enum Action {
    /// Schedule given task on a given *number* of cores.
    ScheduleTask { task: usize, resource: usize, cores: u32 },
    /// Schedule given task on a given *set* of cores.
    ScheduleTaskOnCores {
        task: usize,
        resource: usize,
        cores: Vec<u32>,
    },
    /// Send data item from one actor to another.
    /// Action will be queued if there is no such data item right now.
    TransferData { data_item: usize, from: Id, to: Id },
}

pub trait Scheduler {
    /// This functions gets called once in the beginning of DAG execution.
    fn start(
        &mut self,
        dag: &DAG,
        resources: &Vec<Resource>,
        network: &Network,
        config: Config,
        ctx: &SimulationContext,
    ) -> Vec<Action>;
    /// This function gets called on every task state change.
    fn on_task_state_changed(
        &mut self,
        task: usize,
        task_state: TaskState,
        dag: &DAG,
        resources: &Vec<Resource>,
        ctx: &SimulationContext,
    ) -> Vec<Action>;
}
