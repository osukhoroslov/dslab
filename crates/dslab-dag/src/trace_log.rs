//! DAG execution log.

use std::fs::File;
use std::io::Write;

use dslab_core::context::SimulationContext;
use dslab_core::log_debug;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::dag::DAG;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    TaskScheduled {
        time: f64,
        task_id: usize,
        task_name: String,
        location: String,
        cores: u32,
        memory: u64,
    },
    TaskStarted {
        time: f64,
        task_id: usize,
        task_name: String,
    },
    TaskCompleted {
        time: f64,
        task_id: usize,
        task_name: String,
    },
    StartUploading {
        time: f64,
        from: String,
        to: String,
        data_id: usize,
        data_item_id: usize,
        data_name: String,
    },
    FinishUploading {
        time: f64,
        from: String,
        to: String,
        data_id: usize,
        data_name: String,
    },
}

impl Event {
    pub fn time(&self) -> f64 {
        match self {
            Event::TaskScheduled { time, .. }
            | Event::TaskStarted { time, .. }
            | Event::TaskCompleted { time, .. }
            | Event::StartUploading { time, .. }
            | Event::FinishUploading { time, .. } => *time,
        }
    }
}

impl ToString for Event {
    fn to_string(&self) -> String {
        match self {
            Event::TaskScheduled {
                ref task_name,
                ref location,
                cores,
                ..
            } => format!("scheduled task {task_name} to {location} on {cores} cores"),
            Event::TaskStarted { ref task_name, .. } => format!("started task {task_name}"),
            Event::TaskCompleted { ref task_name, .. } => format!("completed task {task_name}"),
            Event::StartUploading {
                ref data_name,
                ref from,
                ref to,
                ..
            } => format!("data item {data_name} started uploading from {from} to {to}"),
            Event::FinishUploading {
                ref data_name,
                ref from,
                ref to,
                ..
            } => format!("data item {data_name} finished uploading from {from} to {to}"),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Task {
    pub name: String,
    pub flops: f64,
    pub memory: u64,
    pub min_cores: u32,
    pub max_cores: u32,
    pub inputs: Vec<usize>,
    pub outputs: Vec<usize>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct DataItem {
    pub name: String,
    pub size: f64,
    pub consumers: Vec<usize>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Graph {
    pub tasks: Vec<Task>,
    pub data_items: Vec<DataItem>,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct TraceLog {
    pub resources: Vec<Value>,
    pub graph: Graph,
    pub events: Vec<Event>,
}

impl TraceLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn log_event(&mut self, ctx: &SimulationContext, event: Event) {
        log_debug!(ctx, "{}", event.to_string());
        self.events.push(event);
    }

    pub fn log_dag(&mut self, dag: &DAG) {
        self.graph.tasks = dag
            .get_tasks()
            .iter()
            .map(|task| Task {
                name: task.name.clone(),
                flops: task.flops,
                memory: task.memory,
                min_cores: task.min_cores,
                max_cores: task.max_cores,
                inputs: task.inputs.clone(),
                outputs: task.outputs.clone(),
            })
            .collect();
        self.graph.data_items = dag
            .get_data_items()
            .iter()
            .map(|data_item| DataItem {
                name: data_item.name.clone(),
                size: data_item.size,
                consumers: data_item.consumers.clone(),
            })
            .collect();
    }

    pub fn save_to_file(&self, filename: &str) -> Result<(), std::io::Error> {
        File::create(filename)
            .unwrap()
            .write_all(serde_json::to_string_pretty(self).unwrap().as_bytes())
    }
}
