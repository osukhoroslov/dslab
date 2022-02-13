use crate::dag::DAG;
use crate::runner::Resource;
use crate::task::TaskState;

pub enum Action {
    Schedule { task: usize, resource: usize, cores: u32 },
}

pub trait Scheduler {
    fn start(&mut self, dag: &DAG, resources: &Vec<Resource>) -> Vec<Action>;
    fn on_task_state_changed(
        &mut self,
        task: usize,
        task_state: TaskState,
        dag: &DAG,
        resources: &Vec<Resource>,
    ) -> Vec<Action>;
}
