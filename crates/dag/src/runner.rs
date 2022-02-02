use serde_json::json;
use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::rc::Rc;

use crate::dag::DAG;
use crate::data_item::DataItemState;
use crate::scheduler::{Action, Scheduler};
use crate::task::TaskState;
use crate::trace_log::TraceLog;
use compute::multicore::*;
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use network::model::DataTransferCompleted;
use network::network_actor::Network;

pub struct Resource {
    pub compute: Rc<RefCell<Compute>>,
    pub id: ActorId,
    pub cores_available: u32,
    pub memory_available: u64,
}

pub struct DataTransfer {
    pub data_id: usize,
    pub task_id: usize,
    pub from: ActorId,
    pub to: ActorId,
}

#[derive(Clone)]
pub struct QueuedTask {
    pub task_id: usize,
    pub cores: u32,
}

pub struct DAGRunner {
    dag: DAG,
    scheduled_tasks: BTreeSet<usize>,
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
}

impl DAGRunner {
    pub fn new<T: Scheduler + 'static>(
        dag: DAG,
        network: Rc<RefCell<Network>>,
        resources: Vec<Resource>,
        scheduler: T,
    ) -> Self {
        let resource_count = resources.len();
        Self {
            dag,
            scheduled_tasks: BTreeSet::new(),
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
        }
    }

    pub fn start(&mut self, ctx: &mut ActorContext) {
        println!("{:>8.3} [{}] started DAG execution", ctx.time(), ctx.id);
        self.trace_config();
        self.actions.extend(self.scheduler.start());
        self.process_actions(ctx);
    }

    fn trace_config(&mut self) {
        for resource in self.resources.iter() {
            self.trace_log.resources.push(json!({
                "id": resource.id.to().clone(),
                "speed": resource.compute.borrow().speed(),
                "cores": resource.cores_available,
                "memory": resource.memory_available,
            }));
        }
    }

    pub fn trace_log(&self) -> &TraceLog {
        &self.trace_log
    }

    fn process_actions(&mut self, ctx: &mut ActorContext) {
        for i in 0..self.resources.len() {
            self.process_resource_queue(i, ctx);
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
                    if cores < task.min_cores || task.max_cores < cores {
                        println!("Wrong action, task {} doesn't support {} cores", task_id, cores);
                        return;
                    }
                    self.resource_queue[resource].push_back(QueuedTask { task_id, cores });
                    self.process_resource_queue(resource, ctx);
                }
            };
        }
    }

    fn process_resource_queue(&mut self, resource_id: usize, ctx: &mut ActorContext) {
        while !self.resource_queue[resource_id].is_empty() {
            if self.resource_queue[resource_id][0].cores > self.resources[resource_id].cores_available {
                break;
            }
            if self.dag.get_task(self.resource_queue[resource_id][0].task_id).state != TaskState::Ready {
                break;
            }
            let queued_task = self.resource_queue[resource_id].pop_front().unwrap();
            let task_id = queued_task.task_id;
            let task = self.dag.get_task(task_id);
            let cores = queued_task.cores;
            let mut resource = &mut self.resources[resource_id];
            resource.cores_available -= cores;
            resource.memory_available -= task.memory;
            self.task_inputs.insert(task_id, task.inputs.iter().cloned().collect());
            self.task_cores.insert(task_id, cores);
            for &data_id in task.inputs.iter() {
                let data_item = self.dag.get_data_item(data_id);
                let data_event_id = self.network.borrow_mut().transfer_data(
                    ctx.id.clone(),
                    resource.id.clone(),
                    data_item.size as f64,
                    ctx.id.clone(),
                    ctx,
                );
                self.data_transfers.insert(
                    data_event_id,
                    DataTransfer {
                        data_id: data_id,
                        task_id,
                        from: ctx.id.clone(),
                        to: resource.id.clone(),
                    },
                );
                self.trace_log.log_event(
                    ctx.id.to(),
                    json!({
                        "time": ctx.time(),
                        "type": "start_uploading",
                        "from": "scheduler",
                        "to": resource.id.to().clone(),
                        "id": data_event_id,
                        "name": data_item.name.clone(),
                        "task": task.name.clone(),
                    }),
                );
            }
            self.task_location.insert(task_id, resource_id);
            self.trace_log.log_event(
                ctx.id.to(),
                json!({
                    "time": ctx.time(),
                    "type": "task_scheduled",
                    "id": task_id,
                    "name": task.name.clone(),
                    "location": resource.id.to().clone(),
                    "cores": cores,
                    "memory": task.memory,
                }),
            );
            self.dag.update_task_state(task_id, TaskState::Scheduled);
            self.scheduled_tasks.insert(task_id);
        }
    }

    fn on_task_completed(&mut self, task_id: usize, ctx: &mut ActorContext) {
        let task = self.dag.get_task(task_id).clone();
        self.trace_log.log_event(
            ctx.id.to(),
            json!({
                "time": ctx.time(),
                "type": "task_completed",
                "id": task_id,
                "name": task.name,
            }),
        );
        self.scheduled_tasks.remove(&task_id);
        let location = self.task_location.remove(&task_id).unwrap();
        self.resources[location].cores_available += self.task_cores.get(&task_id).unwrap();
        self.resources[location].memory_available += task.memory;
        let data_items = self.dag.update_task_state(task_id, TaskState::Done);
        for &data_item_id in data_items.iter() {
            let data_item = self.dag.get_data_item(data_item_id);
            let data_id = self.network.borrow_mut().transfer_data(
                self.resources[location].id.clone(),
                ctx.id.clone(),
                data_item.size as f64,
                ctx.id.clone(),
                ctx,
            );
            self.data_transfers.insert(
                data_id,
                DataTransfer {
                    data_id: data_item_id,
                    task_id,
                    from: self.resources[location].id.clone(),
                    to: ctx.id.clone(),
                },
            );
            self.trace_log.log_event(
                ctx.id.to(),
                json!({
                    "time": ctx.time(),
                    "type": "start_uploading",
                    "from": self.resources[location].id.to().clone(),
                    "to": "scheduler",
                    "id": data_id,
                    "name": data_item.name.clone(),
                    "task": task.name.clone(),
                }),
            );
        }

        self.actions.extend(self.scheduler.on_task_completed(task_id));
        self.process_actions(ctx);

        if self.dag.is_completed() {
            println!("{:>8.3} [{}] completed DAG execution", ctx.time(), ctx.id);
        }
    }

    fn on_data_transfered(&mut self, data_event_id: usize, ctx: &mut ActorContext) {
        let data_transfer = self.data_transfers.get(&data_event_id).unwrap();
        let data_id = data_transfer.data_id;
        let data_item = self.dag.get_data_item(data_id);
        let task_id = data_transfer.task_id;
        let task = self.dag.get_task(task_id);
        if data_transfer.from == ctx.id {
            let location = *self.task_location.get(&task_id).unwrap();
            self.trace_log.log_event(
                ctx.id.to(),
                json!({
                    "time": ctx.time(),
                    "type": "finish_uploading",
                    "from": "scheduler",
                    "to": self.resources[location].id.to().clone(),
                    "id": data_event_id,
                    "name": data_item.name.clone(),
                    "task": task.name.clone(),
                }),
            );

            let left_inputs = self.task_inputs.get_mut(&task_id).unwrap();
            left_inputs.remove(&data_id);
            if left_inputs.is_empty() {
                let cores = *self.task_cores.get(&task_id).unwrap();
                let computation_id = self.resources[location].compute.borrow_mut().run(
                    task.flops,
                    task.memory,
                    cores,
                    cores,
                    CoresDependency::Linear,
                    ctx,
                );
                self.computations.insert(computation_id, task_id);

                self.trace_log.log_event(
                    ctx.id.to(),
                    json!({
                        "time": ctx.time(),
                        "type": "task_started",
                        "id": task_id,
                        "name": task.name.clone(),
                    }),
                );
            }
        } else {
            self.trace_log.log_event(
                ctx.id.to(),
                json!({
                    "time": ctx.time(),
                    "type": "finish_uploading",
                    "from": self.data_transfers.get(&data_event_id).unwrap().from.to().clone(),
                    "to": "scheduler",
                    "id": data_event_id,
                    "name": data_item.name.clone(),
                    "task": task.name.clone(),
                }),
            );
            self.dag.update_data_item_state(data_id, DataItemState::Ready);
        }
        self.process_actions(ctx);
    }
}

#[derive(Debug)]
pub struct Start {}

impl Actor for DAGRunner {
    fn on(&mut self, event: Box<dyn Event>, _from: ActorId, ctx: &mut ActorContext) {
        cast!(match event {
            Start {} => {
                self.start(ctx)
            }
            CompStarted { .. } => {}
            CompFinished { id } => {
                let task_id = self.computations.remove(id).unwrap();
                self.on_task_completed(task_id, ctx);
            }
            DataTransferCompleted { data } => {
                self.on_data_transfered(data.id, ctx);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
