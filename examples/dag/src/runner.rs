use std::cell::RefCell;
use std::fs::File;
use std::io::Write;
use std::rc::Rc;

use crate::{DataItemState, TaskState, DAG};
use core::actor::{Actor, ActorContext, ActorId, Event};
use core::cast;
use std::collections::{BTreeSet, HashMap, HashSet};

use compute::multicore::*;
use network::model::DataTransferCompleted;
use network::network_actor::Network;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Serialize, Deserialize)]
struct TraceLog {
    resources: Vec<Value>,
    events: Vec<Value>,
}

pub struct Resource {
    pub compute: Rc<RefCell<Compute>>,
    pub actor_id: ActorId,
    pub cores_available: u32,
    pub memory_available: u64,
}

pub struct DataTransfer {
    pub data_id: usize,
    pub task_id: usize,
    pub from: ActorId,
    pub to: ActorId,
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
}

impl DAGRunner {
    pub fn new(dag: DAG, network: Rc<RefCell<Network>>, resources: Vec<Resource>) -> Self {
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
            trace_log: TraceLog {
                resources: Vec::new(),
                events: Vec::new(),
            },
        }
    }

    pub fn start(&mut self, ctx: &mut ActorContext) {
        println!("{:>8.3} [{}] started DAG execution", ctx.time(), ctx.id);
        self.trace_config();
        self.schedule_ready_tasks(ctx);
    }

    fn trace_config(&mut self) {
        for resource in self.resources.iter() {
            self.trace_log.resources.push(json!({
                "id": resource.actor_id.to().clone(),
                "speed": resource.compute.borrow().speed(),
                "cores": resource.cores_available,
                "memory": resource.memory_available,
            }));
        }
    }

    pub fn on_task_completed(&mut self, task_id: usize, ctx: &mut ActorContext) {
        let task = (*self.dag.get_task(task_id)).clone();
        DAGRunner::log_event(
            &mut self.trace_log,
            ctx,
            json!({
                "time": ctx.time(),
                "type": "task_completed",
                "id": task_id,
                "name": task.name,
            }),
        );
        let data_items = self.dag.update_task_state(task_id, TaskState::Done);
        self.scheduled_tasks.remove(&task_id);
        let location = *self.task_location.get(&task_id).unwrap();
        self.task_location.remove(&task_id);
        self.resources[location].cores_available += self.task_cores.get(&task_id).unwrap();
        self.resources[location].memory_available += task.memory;
        for &data_item_id in data_items.iter() {
            let data_item = self.dag.get_data_item(data_item_id);
            let data_id = self.network.borrow_mut().transfer_data(
                self.resources[location].actor_id.clone(),
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
                    from: self.resources[location].actor_id.clone(),
                    to: ctx.id.clone(),
                },
            );
            DAGRunner::log_event(
                &mut self.trace_log,
                ctx,
                json!({
                    "time": ctx.time(),
                    "type": "start_uploading",
                    "from": self.resources[location].actor_id.to().clone(),
                    "to": "scheduler",
                    "id": data_id,
                    "name": data_item.name.clone(),
                }),
            );
        }

        self.schedule_ready_tasks(ctx);

        if self.dag.is_completed() {
            println!("{:>8.3} [{}] completed DAG execution", ctx.time(), ctx.id);
        }
    }

    pub fn save_trace(&self, filename: &str) {
        File::create(filename)
            .unwrap()
            .write_all(serde_json::to_string_pretty(&self.trace_log).unwrap().as_bytes())
            .unwrap();
    }

    pub fn on_data_transfered(&mut self, data_event_id: usize, ctx: &mut ActorContext) {
        let data_transfer = self.data_transfers.get(&data_event_id).unwrap();
        let data_id = data_transfer.data_id;
        let data_item = self.dag.get_data_item(data_id);
        if data_transfer.from == ctx.id {
            let task_id = data_transfer.task_id;
            let location = *self.task_location.get(&task_id).unwrap();
            DAGRunner::log_event(
                &mut self.trace_log,
                ctx,
                json!({
                    "time": ctx.time(),
                    "type": "finish_uploading",
                    "from": "scheduler",
                    "to": self.resources[location].actor_id.to().clone(),
                    "id": data_event_id,
                    "name": data_item.name.clone(),
                }),
            );
            let task = self.dag.get_task(task_id);

            let left_inputs = self.task_inputs.entry(task_id).or_default();
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

                DAGRunner::log_event(
                    &mut self.trace_log,
                    ctx,
                    json!({
                        "time": ctx.time(),
                        "type": "task_started",
                        "id": task_id,
                        "name": task.name.clone(),
                    }),
                );
            }
        } else {
            DAGRunner::log_event(
                &mut self.trace_log,
                ctx,
                json!({
                    "time": ctx.time(),
                    "type": "finish_uploading",
                    "from": self.data_transfers.get(&data_event_id).unwrap().from.to().clone(),
                    "to": "scheduler",
                    "id": data_event_id,
                    "name": data_item.name.clone(),
                }),
            );
            self.dag.update_data_item_state(data_id, DataItemState::Ready);
        }
        self.schedule_ready_tasks(ctx);
    }

    fn schedule_ready_tasks(&mut self, ctx: &mut ActorContext) {
        let mut scheduled = Vec::new();
        let ready_tasks = self.dag.get_ready_tasks().clone();
        for t in ready_tasks {
            if self.schedule_task(t, ctx) {
                scheduled.push(t);
            }
        }
        for t in scheduled {
            self.dag.update_task_state(t, TaskState::Scheduled);
            self.scheduled_tasks.insert(t);
        }
    }

    fn schedule_task(&mut self, task_id: usize, ctx: &mut ActorContext) -> bool {
        let task = self.dag.get_task(task_id);
        for (i, resource) in self.resources.iter_mut().enumerate() {
            if resource.cores_available < task.min_cores || resource.memory_available < task.memory {
                continue;
            }
            let cores = task.max_cores.min(resource.cores_available);
            resource.cores_available -= cores;
            resource.memory_available -= task.memory;
            self.task_inputs.insert(task_id, task.inputs.iter().cloned().collect());
            self.task_cores.insert(task_id, cores);
            for &data_id in task.inputs.iter() {
                let data_item = self.dag.get_data_item(data_id);
                let data_event_id = self.network.borrow_mut().transfer_data(
                    ctx.id.clone(),
                    resource.actor_id.clone(),
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
                        to: resource.actor_id.clone(),
                    },
                );
                DAGRunner::log_event(
                    &mut self.trace_log,
                    ctx,
                    json!({
                        "time": ctx.time(),
                        "type": "start_uploading",
                        "from": "scheduler",
                        "to": resource.actor_id.to().clone(),
                        "id": data_event_id,
                        "name": data_item.name.clone(),
                    }),
                );
            }
            self.task_location.insert(task_id, i);
            DAGRunner::log_event(
                &mut self.trace_log,
                ctx,
                json!({
                    "time": ctx.time(),
                    "type": "task_scheduled",
                    "id": task_id,
                    "name": task.name.clone(),
                    "location": resource.actor_id.to().clone(),
                    "cores": cores,
                    "memory": task.memory,
                }),
            );
            return true;
        }
        false
    }

    fn log_event(trace_log: &mut TraceLog, ctx: &mut ActorContext, event: Value) {
        let get_field = |name: &str| -> &str { event[name].as_str().unwrap() };
        let log_message = match event["type"].as_str().unwrap().as_ref() {
            "task_scheduled" => {
                format!(
                    "scheduled task {} to {} on {} cores",
                    get_field("name"),
                    get_field("location"),
                    event["cores"].as_u64().unwrap()
                )
            }
            "task_started" => {
                format!("started task {}", get_field("name"))
            }
            "task_completed" => {
                format!("completed task {}", get_field("name"))
            }
            "start_uploading" => {
                format!(
                    "data item {} started uploading from {} to {}",
                    get_field("name"),
                    get_field("from"),
                    get_field("to")
                )
            }
            "finish_uploading" => {
                format!(
                    "data item {} finished uploading from {} to {}",
                    get_field("name"),
                    get_field("from"),
                    get_field("to")
                )
            }
            _ => "unknown event".to_string(),
        };
        let time = event["time"].as_f64().unwrap();
        println!("{:>8.3} [{}] {}", time, ctx.id, log_message);
        trace_log.events.push(event);
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
                self.on_task_completed(*self.computations.get(id).unwrap(), ctx);
                self.computations.remove(id);
            }
            DataTransferCompleted { data } => {
                let data_id = data.id;
                self.on_data_transfered(data_id, ctx);
            }
        })
    }

    fn is_active(&self) -> bool {
        true
    }
}
