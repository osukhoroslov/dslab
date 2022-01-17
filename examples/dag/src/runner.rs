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
    pub speed: u64,
    pub available_cores: u64,
    pub available_memory: u64,
}

pub struct DAGRunner {
    dag: DAG,
    scheduled_tasks: BTreeSet<usize>,
    network: Rc<RefCell<Network>>,
    resources: Vec<Resource>,
    task_ids: HashMap<u64, usize>,
    task_location: HashMap<usize, usize>,
    data_ids: HashMap<usize, usize>,
    task_inputs: HashMap<usize, HashSet<usize>>,
    data_for_task: HashMap<usize, usize>,
    data_location: HashMap<usize, String>,
    trace_log: TraceLog,
}

impl DAGRunner {
    pub fn new(dag: DAG, network: Rc<RefCell<Network>>, resources: Vec<Resource>) -> Self {
        Self {
            dag,
            scheduled_tasks: BTreeSet::new(),
            network,
            resources,
            task_ids: HashMap::new(),
            task_location: HashMap::new(),
            data_ids: HashMap::new(),
            task_inputs: HashMap::new(),
            data_for_task: HashMap::new(),
            data_location: HashMap::new(),
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
                "speed": resource.speed,
                "cores": resource.available_cores,
            }));
        }
    }

    pub fn on_task_completed(&mut self, task_id: usize, ctx: &mut ActorContext) {
        let task = self.dag.get_task(task_id);
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
        self.resources[location].available_cores += 1;
        for data_item in data_items {
            let data_id = self.network.borrow_mut().transfer_data(
                self.resources[location].actor_id.clone(),
                ctx.id.clone(),
                data_item.size as f64,
                ctx.id.clone(),
                ctx,
            );
            self.data_ids.insert(data_id, data_item.id);
            self.data_location.insert(data_item.id, ctx.id.to().clone());
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
        let data_id = *self.data_ids.get(&data_event_id).unwrap();
        let data_item = self.dag.get_data_item(data_id);
        if let Some(&task_id) = self.data_for_task.get(&data_id) {
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

            let left_inputs = self.task_inputs.entry(task_id).or_insert(HashSet::new());
            left_inputs.remove(&data_id);
            if left_inputs.is_empty() {
                let computation_id = self.resources[location].compute.borrow_mut().run(
                    task.flops,
                    0,
                    1,
                    1,
                    CoresDependency::Linear,
                    ctx,
                );
                self.task_ids.insert(computation_id, task_id);

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
                    "from": self.data_location.get(&data_id).unwrap(),
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
            if !self.schedule_task(t, ctx) {
                break;
            }
            scheduled.push(t);
        }
        for t in scheduled {
            self.dag.update_task_state(t, TaskState::Scheduled);
            self.scheduled_tasks.insert(t);
        }
    }

    fn schedule_task(&mut self, task_id: usize, ctx: &mut ActorContext) -> bool {
        let task = self.dag.get_task(task_id);
        for (i, resource) in self.resources.iter_mut().enumerate() {
            if resource.available_cores == 0 {
                continue;
            }
            resource.available_cores -= 1;
            self.task_inputs.insert(task_id, task.inputs.iter().cloned().collect());
            for &data_id in task.inputs.iter() {
                let data_item = self.dag.get_data_item(data_id);
                let data_event_id = self.network.borrow_mut().transfer_data(
                    ctx.id.clone(),
                    resource.actor_id.clone(),
                    data_item.size as f64,
                    ctx.id.clone(),
                    ctx,
                );
                self.data_ids.insert(data_event_id, data_id);
                self.data_for_task.insert(data_id, task_id);
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
                format!("scheduled task {} to {}", get_field("name"), get_field("location"))
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
                self.on_task_completed(*self.task_ids.get(id).unwrap(), ctx);
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
