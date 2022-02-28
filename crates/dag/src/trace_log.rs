use std::fs::File;
use std::io::Write;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize)]
pub struct TraceLog {
    pub resources: Vec<Value>,
    pub events: Vec<Value>,
}

impl TraceLog {
    pub fn new() -> Self {
        TraceLog {
            resources: Vec::new(),
            events: Vec::new(),
        }
    }

    pub fn log_event<S: AsRef<str>>(&mut self, proc_id: S, event: Value) {
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
        println!("{:>8.3} [{}] {}", time, proc_id.as_ref(), log_message);
        self.events.push(event);
    }

    pub fn save_to_file(&self, filename: &str) -> Result<(), std::io::Error> {
        File::create(filename)
            .unwrap()
            .write_all(serde_json::to_string_pretty(self).unwrap().as_bytes())
    }
}
