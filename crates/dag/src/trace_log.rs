use std::fs::File;
use std::io::Write;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::dag::DAG;

#[derive(Serialize, Deserialize, Clone)]
pub struct Task {
    pub name: String,
    pub flops: u64,
    pub memory: u64,
    pub min_cores: u32,
    pub max_cores: u32,
    pub inputs: Vec<usize>,
    pub outputs: Vec<usize>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct DataItem {
    pub name: String,
    pub size: u64,
    pub consumers: Vec<usize>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Graph {
    pub tasks: Vec<Task>,
    pub data_items: Vec<DataItem>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct TraceLog {
    pub resources: Vec<Value>,
    pub graph: Graph,
    pub events: Vec<Value>,
}

impl TraceLog {
    pub fn new() -> Self {
        TraceLog {
            resources: Vec::new(),
            events: Vec::new(),
            graph: Graph {
                tasks: Vec::new(),
                data_items: Vec::new(),
            },
        }
    }

    pub fn log_event<S: AsRef<str>>(&mut self, proc_id: S, event: Value) {
        let get_field = |name: &str| -> &str { event[name].as_str().unwrap() };
        let log_message = match event["type"].as_str().unwrap().as_ref() {
            "task_scheduled" => {
                format!(
                    "scheduled task {} to {} on {} cores",
                    get_field("task_name"),
                    get_field("location"),
                    event["cores"].as_u64().unwrap()
                )
            }
            "task_started" => {
                format!("started task {}", get_field("task_name"))
            }
            "task_completed" => {
                format!("completed task {}", get_field("task_name"))
            }
            "start_uploading" => {
                format!(
                    "data item {} started uploading from {} to {}",
                    get_field("data_name"),
                    get_field("from"),
                    get_field("to")
                )
            }
            "finish_uploading" => {
                format!(
                    "data item {} finished uploading from {} to {}",
                    get_field("data_name"),
                    get_field("from"),
                    get_field("to")
                )
            }
            _ => "unknown event".to_string(),
        };
        let time = event["time"].as_f64().unwrap();
        println!("{:>8.3} [{}] {}", time, proc_id.as_ref(), log_message);
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
