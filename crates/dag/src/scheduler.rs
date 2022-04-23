use std::cell::RefCell;
use std::rc::Rc;

use crate::dag::DAG;
use crate::resource::Resource;
use crate::task::TaskState;

use network::model::NetworkModel;
use simcore::context::SimulationContext;

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

pub struct Config {
    pub network: Rc<RefCell<dyn NetworkModel>>,
}

pub trait Scheduler {
    fn set_config(&mut self, config: Config);
    fn start(&mut self, dag: &DAG, resources: &Vec<Resource>, ctx: &SimulationContext) -> Vec<Action>;
    fn on_task_state_changed(
        &mut self,
        task: usize,
        task_state: TaskState,
        dag: &DAG,
        resources: &Vec<Resource>,
        ctx: &SimulationContext,
    ) -> Vec<Action>;
}
