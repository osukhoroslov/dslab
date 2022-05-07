use network::network::Network;
use simcore::component::Id;
use simcore::context::SimulationContext;

use crate::dag::DAG;
use crate::resource::Resource;
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

#[derive(Clone, PartialEq)]
pub enum DataTransferMode {
    ViaMasterNode,
    Direct,
}

impl DataTransferMode {
    pub fn net_time(&self, network: &Network, src: Id, dst: Id, runner: Id) -> f64 {
        match self {
            DataTransferMode::ViaMasterNode => {
                1. / network.bandwidth(src, runner) + 1. / network.bandwidth(runner, dst)
            }
            DataTransferMode::Direct => 1. / network.bandwidth(src, dst),
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub data_transfer_mode: DataTransferMode,
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
