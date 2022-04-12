use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

use serde::Serialize;
use serde_json::json;

use enum_iterator::IntoEnumIterator;

use compute::multicore::*;
use network::model::DataTransferCompleted;
use network::network::Network;
use simcore::cast;
use simcore::component::Id;
use simcore::context::SimulationContext;
use simcore::event::Event;
use simcore::handler::EventHandler;
use simcore::{log_error, log_info};

use crate::dag::DAG;
use crate::data_item::DataItemState;
use crate::resource::Resource;
use crate::scheduler::{Action, Scheduler};
use crate::task::TaskState;
use crate::trace_log::TraceLog;

pub struct DataTransfer {
    pub data_id: usize,
    pub task_id: usize,
    pub from: Id,
    pub to: Id,
}

#[derive(Clone, Debug)]
pub struct QueuedTask {
    pub task_id: usize,
    pub cores: u32,
}

pub struct DAGRunner {
    id: Id,
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
            id: ctx.id(),
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
        log_info!(
            self.ctx,
            "started DAG execution: total {} resources, {} tasks, {} data items",
            self.resources.len(),
            self.dag.get_tasks().len(),
            self.dag.get_data_items().len()
        );
        self.trace_config();
        self.actions
            .extend(self.scheduler.start(&self.dag, &self.resources, &self.ctx));
        self.process_actions();
    }

    fn trace_config(&mut self) {
        for resource in self.resources.iter() {
            self.trace_log.resources.push(json!({
                "name": resource.name.clone(),
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
                        log_error!(
                            self.ctx,
                            "Wrong action, resource {} doesn't have enough cores",
                            resource
                        );
                        return;
                    }
                    let task_id = task;
                    let task = self.dag.get_task(task_id);
                    if task.memory > self.resources[resource].compute.borrow().memory_total() {
                        log_error!(
                            self.ctx,
                            "Wrong action, resource {} doesn't have enough memory",
                            resource
                        );
                        return;
                    }
                    if cores < task.min_cores || task.max_cores < cores {
                        log_error!(
                            self.ctx,
                            "Wrong action, task {} doesn't support {} cores",
                            task_id,
                            cores
                        );
                        return;
                    }
                    if task.state == TaskState::Ready {
                        self.dag.update_task_state(task_id, TaskState::Runnable);
                    } else if task.state == TaskState::Pending {
                        self.dag.update_task_state(task_id, TaskState::Scheduled);
                    } else {
                        log_error!(self.ctx, "Can't schedule task with state {:?}", task.state);
                        return;
                    }
                    self.resource_queue[resource].push_back(QueuedTask { task_id, cores });
                    self.process_resource_queue(resource);
                }
            };
        }
    }

    fn process_resource_queue(&mut self, resource_idx: usize) {
        while !self.resource_queue[resource_idx].is_empty() {
            if self.resource_queue[resource_idx][0].cores > self.resources[resource_idx].cores_available {
                break;
            }
            let task_id = self.resource_queue[resource_idx][0].task_id;
            let task = self.dag.get_task(task_id);
            if task.memory > self.resources[resource_idx].memory_available {
                break;
            }
            if task.state != TaskState::Runnable {
                break;
            }
            let queued_task = self.resource_queue[resource_idx].pop_front().unwrap();
            let cores = queued_task.cores;
            let mut resource = &mut self.resources[resource_idx];
            resource.cores_available -= cores;
            resource.memory_available -= task.memory;
            self.task_inputs.insert(task_id, task.inputs.iter().cloned().collect());
            self.task_cores.insert(task_id, cores);
            for &data_id in task.inputs.iter() {
                let data_item = self.dag.get_data_item(data_id);
                let data_event_id =
                    self.network
                        .borrow_mut()
                        .transfer_data(self.id, resource.id, data_item.size as f64, self.id);
                self.data_transfers.insert(
                    data_event_id,
                    DataTransfer {
                        data_id: data_id,
                        task_id,
                        from: self.id,
                        to: resource.id,
                    },
                );
                self.trace_log.log_event(
                    &self.ctx,
                    json!({
                        "time": self.ctx.time(),
                        "type": "start_uploading",
                        "from": "scheduler",
                        "to": resource.name.clone(),
                        "data_id": data_event_id,
                        "data_name": data_item.name.clone(),
                        "task_id": task_id,
                    }),
                );
            }
            self.task_location.insert(task_id, resource_idx);
            self.trace_log.log_event(
                &self.ctx,
                json!({
                    "time": self.ctx.time(),
                    "type": "task_scheduled",
                    "task_id": task_id,
                    "task_name": task.name.clone(),
                    "location": resource.name.clone(),
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
            &self.ctx,
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
                self.resources[location].id,
                self.id,
                data_item.size as f64,
                self.id,
            );
            self.data_transfers.insert(
                data_id,
                DataTransfer {
                    data_id: data_item_id,
                    task_id,
                    from: self.resources[location].id,
                    to: self.id,
                },
            );
            self.trace_log.log_event(
                &self.ctx,
                json!({
                    "time": self.ctx.time(),
                    "type": "start_uploading",
                    "from": self.resources[location].name.clone(),
                    "to": "scheduler",
                    "data_id": data_id,
                    "data_name": data_item.name.clone(),
                    "task_id": task_id,
                }),
            );
        }

        self.actions.extend(self.scheduler.on_task_state_changed(
            task_id,
            TaskState::Done,
            &self.dag,
            &self.resources,
            &self.ctx,
        ));
        self.process_actions();

        self.check_and_log_completed();
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
            self.id,
        );
        self.computations.insert(computation_id, task_id);

        self.trace_log.log_event(
            &self.ctx,
            json!({
                "time": self.ctx.time(),
                "type": "task_started",
                "task_id": task_id,
                "task_name": task.name.clone(),
            }),
        );
    }

    fn on_data_transfered(&mut self, data_event_id: usize) {
        let data_transfer = self.data_transfers.remove(&data_event_id).unwrap();
        let data_id = data_transfer.data_id;
        let data_item = self.dag.get_data_item(data_id);
        let task_id = data_transfer.task_id;
        if data_transfer.from == self.id {
            let location = *self.task_location.get(&task_id).unwrap();
            self.trace_log.log_event(
                &self.ctx,
                json!({
                    "time": self.ctx.time(),
                    "type": "finish_uploading",
                    "from": "scheduler",
                    "to": self.resources[location].name.clone(),
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
                &self.ctx,
                json!({
                    "time": self.ctx.time(),
                    "type": "finish_uploading",
                    "from": self.ctx.lookup_name(data_transfer.from.clone()),
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
                self.actions.extend(self.scheduler.on_task_state_changed(
                    task,
                    state,
                    &self.dag,
                    &self.resources,
                    &self.ctx,
                ));
                self.process_actions();
            }
        }

        self.check_and_log_completed();
    }

    fn check_and_log_completed(&self) {
        if self.dag.is_completed() && self.data_transfers.is_empty() {
            log_info!(self.ctx, "finished DAG execution");
        }
    }

    pub fn validate_completed(&self) {
        if !self.dag.is_completed() {
            let mut states: Vec<String> = Vec::new();
            for task_state in TaskState::into_enum_iter() {
                let cnt = self
                    .dag
                    .get_tasks()
                    .iter()
                    .filter(|task| task.state == task_state)
                    .count();
                if cnt != 0 {
                    states.push(format!("{} {:?}", cnt, task_state));
                }
            }
            log_error!(self.ctx, "DAG is not completed, currently {} tasks", states.join(", "));
        }
    }
}

#[derive(Serialize)]
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
