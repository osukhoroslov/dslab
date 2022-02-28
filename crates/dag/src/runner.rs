use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

use serde_json::json;

use compute::multicore::*;
use core::cast;
use core::context::SimulationContext;
use core::event::Event;
use core::handler::EventHandler;
use network::model::DataTransferCompleted;
use network::network::Network;

use crate::dag::DAG;
use crate::data_item::DataItemState;
use crate::scheduler::{Action, Scheduler};
use crate::task::TaskState;
use crate::trace_log::TraceLog;

pub struct Resource {
    pub id: String,
    pub compute: Rc<RefCell<Compute>>,
    pub speed: u64,
    pub cores_available: u32,
    pub memory_available: u64,
}

pub struct DataTransfer {
    pub data_id: usize,
    pub task_id: usize,
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug)]
pub struct QueuedTask {
    pub task_id: usize,
    pub cores: u32,
}

pub struct DAGRunner {
    id: String,
    dag: DAG,
    network: Rc<RefCell<Network>>,
    resources: Vec<Resource>,
    computations: HashMap<u64, usize>,
    task_location: HashMap<usize, usize>,
    data_transfers: HashMap<usize, DataTransfer>,
    task_cores: HashMap<usize, u32>,
    task_inputs: HashMap<usize, HashSet<usize>>,
    trace_log: TraceLog,
    scheduler: Box<dyn Scheduler>,
    actions: VecDeque<Action>,
    resource_queue: Vec<VecDeque<QueuedTask>>,
    ctx: SimulationContext,
}

impl DAGRunner {
    pub fn new<T: Scheduler + 'static>(
        dag: DAG,
        network: Rc<RefCell<Network>>,
        resources: Vec<Resource>,
        scheduler: T,
        ctx: SimulationContext,
    ) -> Self {
        let resource_count = resources.len();
        Self {
            id: ctx.id().to_string(),
            dag,
            network,
            resources,
            computations: HashMap::new(),
            task_location: HashMap::new(),
            data_transfers: HashMap::new(),
            task_cores: HashMap::new(),
            task_inputs: HashMap::new(),
            trace_log: TraceLog::new(),
            scheduler: Box::new(scheduler),
            actions: VecDeque::new(),
            resource_queue: vec![VecDeque::new(); resource_count],
            ctx,
        }
    }

    pub fn start(&mut self) {
        println!("{:>8.3} [{}] started DAG execution", self.ctx.time(), self.id);
        self.trace_config();
        self.actions.extend(self.scheduler.start(&self.dag, &self.resources));
        self.process_actions();
    }

    fn trace_config(&mut self) {
        for resource in self.resources.iter() {
            self.trace_log.resources.push(json!({
                "id": resource.id.clone(),
                "speed": resource.compute.borrow().speed(),
                "cores": resource.cores_available,
                "memory": resource.memory_available,
            }));
        }
        self.trace_log.log_dag(&self.dag);
    }

    pub fn trace_log(&self) -> &TraceLog {
        &self.trace_log
    }

    fn process_actions(&mut self) {
        for i in 0..self.resources.len() {
            self.process_resource_queue(i);
        }
        while !self.actions.is_empty() {
            match self.actions.pop_front().unwrap() {
                Action::Schedule { task, resource, cores } => {
                    if cores > self.resources[resource].compute.borrow().cores_total() {
                        println!("Wrong action, resource {} doesn't have enough cores", resource);
                        return;
                    }
                    let task_id = task;
                    let task = self.dag.get_task(task_id);
                    if task.memory > self.resources[resource].compute.borrow().memory_total() {
                        println!("Wrong action, resource {} doesn't have enough memory", resource);
                        return;
                    }
                    if cores < task.min_cores || task.max_cores < cores {
                        println!("Wrong action, task {} doesn't support {} cores", task_id, cores);
                        return;
                    }
                    if task.state == TaskState::Ready {
                        self.dag.update_task_state(task_id, TaskState::Runnable);
                    } else if task.state == TaskState::Pending {
                        self.dag.update_task_state(task_id, TaskState::Scheduled);
                    } else {
                        println!("Can't schedule task with state {:?}", task.state);
                        return;
                    }
                    self.resource_queue[resource].push_back(QueuedTask { task_id, cores });
                    self.process_resource_queue(resource);
                }
            };
        }
    }

    fn process_resource_queue(&mut self, resource_id: usize) {
        while !self.resource_queue[resource_id].is_empty() {
            if self.resource_queue[resource_id][0].cores > self.resources[resource_id].cores_available {
                break;
            }
            let task_id = self.resource_queue[resource_id][0].task_id;
            let task = self.dag.get_task(task_id);
            if task.memory > self.resources[resource_id].memory_available {
                break;
            }
            if task.state != TaskState::Runnable {
                break;
            }
            let queued_task = self.resource_queue[resource_id].pop_front().unwrap();
            let cores = queued_task.cores;
            let mut resource = &mut self.resources[resource_id];
            resource.cores_available -= cores;
            resource.memory_available -= task.memory;
            self.task_inputs.insert(task_id, task.inputs.iter().cloned().collect());
            self.task_cores.insert(task_id, cores);
            for &data_id in task.inputs.iter() {
                let data_item = self.dag.get_data_item(data_id);
                let data_event_id =
                    self.network
                        .borrow_mut()
                        .transfer_data(&self.id, &resource.id, data_item.size as f64, &self.id);
                self.data_transfers.insert(
                    data_event_id,
                    DataTransfer {
                        data_id: data_id,
                        task_id,
                        from: self.id.clone(),
                        to: resource.id.clone(),
                    },
                );
                self.trace_log.log_event(
                    &self.id,
                    json!({
                        "time": self.ctx.time(),
                        "type": "start_uploading",
                        "from": "scheduler",
                        "to": resource.id.clone(),
                        "data_id": data_event_id,
                        "data_name": data_item.name.clone(),
                        "task_id": task_id,
                    }),
                );
            }
            self.task_location.insert(task_id, resource_id);
            self.trace_log.log_event(
                &self.id,
                json!({
                    "time": self.ctx.time(),
                    "type": "task_scheduled",
                    "task_id": task_id,
                    "task_name": task.name.clone(),
                    "location": resource.id.clone(),
                    "cores": cores,
                    "memory": task.memory,
                }),
            );
            self.dag.update_task_state(task_id, TaskState::Running);

            if self.task_inputs.get(&task_id).unwrap().is_empty() {
                self.start_task(task_id);
            }
        }
    }

    fn on_task_completed(&mut self, task_id: usize) {
        let task_name = self.dag.get_task(task_id).name.clone();
        self.trace_log.log_event(
            &self.id,
            json!({
                "time": self.ctx.time(),
                "type": "task_completed",
                "task_id": task_id,
                "task_name": task_name,
            }),
        );
        let location = self.task_location.remove(&task_id).unwrap();
        self.resources[location].cores_available += self.task_cores.get(&task_id).unwrap();
        self.resources[location].memory_available += self.dag.get_task(task_id).memory;
        let data_items = self.dag.update_task_state(task_id, TaskState::Done);
        for &data_item_id in data_items.iter() {
            let data_item = self.dag.get_data_item(data_item_id);
            let data_id = self.network.borrow_mut().transfer_data(
                &self.resources[location].id,
                &self.id,
                data_item.size as f64,
                &self.id,
            );
            self.data_transfers.insert(
                data_id,
                DataTransfer {
                    data_id: data_item_id,
                    task_id,
                    from: self.resources[location].id.clone(),
                    to: self.id.clone(),
                },
            );
            self.trace_log.log_event(
                &self.id,
                json!({
                    "time": self.ctx.time(),
                    "type": "start_uploading",
                    "from": self.resources[location].id.clone(),
                    "to": "scheduler",
                    "data_id": data_id,
                    "data_name": data_item.name.clone(),
                    "task_id": task_id,
                }),
            );
        }

        self.actions.extend(
            self.scheduler
                .on_task_state_changed(task_id, TaskState::Done, &self.dag, &self.resources),
        );
        self.process_actions();

        if self.dag.is_completed() {
            println!("{:>8.3} [{}] completed DAG execution", self.ctx.time(), &self.id);
        }
    }

    fn start_task(&mut self, task_id: usize) {
        let task = self.dag.get_task(task_id);
        let location = *self.task_location.get(&task_id).unwrap();
        let cores = *self.task_cores.get(&task_id).unwrap();
        let computation_id = self.resources[location].compute.borrow_mut().run(
            task.flops,
            task.memory,
            cores,
            cores,
            task.cores_dependency,
            &self.id,
        );
        self.computations.insert(computation_id, task_id);

        self.trace_log.log_event(
            &self.id,
            json!({
                "time": self.ctx.time(),
                "type": "task_started",
                "task_id": task_id,
                "task_name": task.name.clone(),
            }),
        );
    }

    fn on_data_transfered(&mut self, data_event_id: usize) {
        let data_transfer = self.data_transfers.get(&data_event_id).unwrap();
        let data_id = data_transfer.data_id;
        let data_item = self.dag.get_data_item(data_id);
        let task_id = data_transfer.task_id;
        let task = self.dag.get_task(task_id);
        if data_transfer.from == self.id {
            let location = *self.task_location.get(&task_id).unwrap();
            self.trace_log.log_event(
                &self.id,
                json!({
                    "time": self.ctx.time(),
                    "type": "finish_uploading",
                    "from": "scheduler",
                    "to": self.resources[location].id.clone(),
                    "data_id": data_event_id,
                    "data_name": data_item.name.clone(),
                    "task_id": task_id,
                }),
            );

            let left_inputs = self.task_inputs.get_mut(&task_id).unwrap();
            left_inputs.remove(&data_id);
            if left_inputs.is_empty() {
                self.start_task(task_id);
            }
        } else {
            self.trace_log.log_event(
                &self.id,
                json!({
                    "time": self.ctx.time(),
                    "type": "finish_uploading",
                    "from": self.data_transfers.get(&data_event_id).unwrap().from.clone(),
                    "to": "scheduler",
                    "data_id": data_event_id,
                    "data_name": data_item.name.clone(),
                    "task_id": task_id,
                }),
            );
            for (task, state) in self
                .dag
                .update_data_item_state(data_id, DataItemState::Ready)
                .into_iter()
            {
                self.actions.extend(
                    self.scheduler
                        .on_task_state_changed(task, state, &self.dag, &self.resources),
                );
            }
        }
        self.process_actions();
    }
}

#[derive(Debug)]
pub struct Start {}

impl EventHandler for DAGRunner {
    fn on(&mut self, event: Event) {
        cast!(match event.data {
            Start {} => {
                self.start()
            }
            CompStarted { .. } => {}
            CompFinished { id } => {
                let task_id = self.computations.remove(&id).unwrap();
                self.on_task_completed(task_id);
            }
            DataTransferCompleted { data } => {
                self.on_data_transfered(data.id);
            }
        })
    }
}
