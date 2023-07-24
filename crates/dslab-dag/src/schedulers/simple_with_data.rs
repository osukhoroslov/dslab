use std::collections::HashMap;

use dslab_core::component::Id;
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
    id: Id,
}

#[derive(Default)]
pub struct SimpleDataScheduler {
    data_location: HashMap<usize, Id>,
}

impl SimpleDataScheduler {
    pub fn new() -> Self {
        Self::default()
    }

    fn schedule(&mut self, dag: &DAG, resources: &[crate::resource::Resource], ctx: &SimulationContext) -> Vec<Action> {
        let mut resources: Vec<Resource> = resources
            .iter()
            .map(|resource| Resource {
                cores_available: resource.cores_available,
                memory_available: resource.memory_available,
                id: resource.id,
            })
            .collect();
        let mut result: Vec<Action> = Vec::new();
        let ready_tasks = dag.get_ready_tasks().clone();
        for task_id in ready_tasks {
            let task = dag.get_task(task_id);
            for (i, resource) in resources.iter_mut().enumerate() {
                if !task.is_allowed_on(i) {
                    continue;
                }
                if resource.cores_available < task.min_cores || resource.memory_available < task.memory {
                    continue;
                }
                let cores = resource.cores_available.min(task.max_cores);
                resource.cores_available -= cores;
                resource.memory_available -= task.memory;
                for &data_item in task.inputs.iter() {
                    if let Some(location) = self.data_location.get(&data_item) {
                        if *location != resource.id {
                            result.push(Action::TransferData {
                                data_item,
                                from: *location,
                                to: resource.id,
                            });
                        }
                    } else {
                        result.push(Action::TransferData {
                            data_item,
                            from: ctx.id(),
                            to: resource.id,
                        });
                    }
                }
                result.push(Action::ScheduleTask {
                    task: task_id,
                    resource: i,
                    cores,
                    expected_span: None,
                });
                for &data_item in task.outputs.iter() {
                    self.data_location.insert(data_item, resource.id);
                    if dag.get_data_item(data_item).consumers.is_empty() {
                        result.push(Action::TransferData {
                            data_item,
                            from: resource.id,
                            to: ctx.id(),
                        })
                    }
                }
                break;
            }
        }
        result
    }
}

impl Scheduler for SimpleDataScheduler {
    fn start(&mut self, dag: &DAG, system: System, config: Config, ctx: &SimulationContext) -> Vec<Action> {
        assert_eq!(
            config.data_transfer_mode,
            DataTransferMode::Manual,
            "SimpleDataScheduler supports only DataTransferMode::Manual"
        );
        self.schedule(dag, system.resources, ctx)
    }

    fn on_task_state_changed(
        &mut self,
        _task: usize,
        _task_state: TaskState,
        dag: &DAG,
        system: System,
        ctx: &SimulationContext,
    ) -> Vec<Action> {
        self.schedule(dag, system.resources, ctx)
    }

    fn is_static(&self) -> bool {
        false
    }
}
