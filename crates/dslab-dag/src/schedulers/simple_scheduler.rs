use dslab_core::context::SimulationContext;

use crate::dag::DAG;
use crate::data_item::DataTransferMode;
use crate::runner::Config;
use crate::scheduler::{Action, Scheduler};
use crate::system::System;
use crate::task::*;

struct Resource {
    cores_available: u32,
    memory_available: u64,
}

pub struct SimpleScheduler {}

impl SimpleScheduler {
    pub fn new() -> Self {
        SimpleScheduler {}
    }

    fn schedule(&mut self, dag: &DAG, resources: &Vec<crate::resource::Resource>) -> Vec<Action> {
        let mut resources: Vec<Resource> = resources
            .iter()
            .map(|resource| Resource {
                cores_available: resource.cores_available,
                memory_available: resource.memory_available,
            })
            .collect();
        let mut result: Vec<Action> = Vec::new();
        let ready_tasks = dag.get_ready_tasks().clone();
        for task_id in ready_tasks {
            let task = dag.get_task(task_id);
            for (i, resource) in resources.iter_mut().enumerate() {
                if resource.cores_available < task.min_cores || resource.memory_available < task.memory {
                    continue;
                }
                let cores = resource.cores_available.min(task.max_cores);
                resource.cores_available -= cores;
                resource.memory_available -= task.memory;
                result.push(Action::ScheduleTask {
                    task: task_id,
                    resource: i,
                    cores,
                    expected_span: None,
                });
                break;
            }
        }
        result
    }
}

impl Scheduler for SimpleScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, _ctx: &SimulationContext) -> Vec<Action> {
        assert_ne!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "SimpleScheduler doesn't support DataTransferMode::Manual"
        );
        self.schedule(dag, system.resources)
    }

    fn on_task_state_changed(
        &mut self,
        _task: usize,
        _task_state: TaskState,
        dag: &DAG,
        system: System,
        _ctx: &SimulationContext,
    ) -> Vec<Action> {
        self.schedule(dag, system.resources)
    }

    fn is_static(&self) -> bool {
        false
    }
}
