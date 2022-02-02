use std::collections::HashMap;

use dag::dag::DAG;
use dag::data_item::*;
use dag::task::*;

pub struct Resource {
    pub speed: u64,
    pub cores_available: u32,
    pub memory_available: u64,
}

pub enum Action {
    Schedule { task: usize, resource: usize, cores: u32 },
}

pub trait Scheduler {
    fn start(&mut self) -> Vec<Action>;
    fn on_task_completed(&mut self, task: usize) -> Vec<Action>;
}

pub struct SimpleScheduler {
    dag: DAG,
    resources: Vec<Resource>,
    task_location: HashMap<usize, usize>,
    task_cores: HashMap<usize, u32>,
}

impl SimpleScheduler {
    pub fn new(dag: DAG, resources: Vec<Resource>) -> Self {
        SimpleScheduler {
            dag,
            resources,
            task_location: HashMap::new(),
            task_cores: HashMap::new(),
        }
    }

    fn schedule(&mut self) -> Vec<Action> {
        let mut result: Vec<Action> = Vec::new();
        let ready_tasks = self.dag.get_ready_tasks().clone();
        for task_id in ready_tasks {
            let task = self.dag.get_task(task_id);
            for (i, resource) in self.resources.iter_mut().enumerate() {
                if resource.cores_available < task.min_cores || resource.memory_available < task.memory {
                    continue;
                }
                let cores = resource.cores_available.min(task.max_cores);
                resource.cores_available -= cores;
                resource.memory_available -= task.memory;
                result.push(Action::Schedule {
                    task: task_id,
                    resource: i,
                    cores,
                });
                self.dag.update_task_state(task_id, TaskState::Scheduled);
                self.task_location.insert(task_id, i);
                self.task_cores.insert(task_id, cores);
                break;
            }
        }
        result
    }
}

impl Scheduler for SimpleScheduler {
    fn start(&mut self) -> Vec<Action> {
        self.schedule()
    }

    fn on_task_completed(&mut self, task: usize) -> Vec<Action> {
        for &data_item in self.dag.update_task_state(task, TaskState::Done).iter() {
            self.dag.update_data_item_state(data_item, DataItemState::Ready);
        }
        let resource = self.task_location.remove(&task).unwrap();
        self.resources[resource].cores_available += self.task_cores.remove(&task).unwrap();
        self.resources[resource].memory_available += self.dag.get_task(task).memory;
        self.schedule()
    }
}
