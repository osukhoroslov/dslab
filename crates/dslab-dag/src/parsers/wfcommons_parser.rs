use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::from_str;

use dslab_compute::multicore::CoresDependency;

use crate::dag::*;

#[derive(Serialize, Deserialize)]
struct Cpu {
    speed: Option<u64>, // MHz
}

#[derive(Serialize, Deserialize)]
struct Machine {
    #[serde(rename = "nodeName")]
    name: String,
    cpu: Cpu,
}

#[derive(Serialize, Deserialize)]
struct File {
    link: String,
    name: String,
    size: u64,
}

#[derive(Serialize, Deserialize)]
struct Task {
    name: String,
    runtime: f64,
    files: Vec<File>,
    machine: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Workflow {
    jobs: Option<Vec<Task>>,  // v1.2
    tasks: Option<Vec<Task>>, // v1.3
    #[serde(default = "Vec::new")]
    machines: Vec<Machine>,
}

impl Workflow {
    fn tasks(&self) -> &Vec<Task> {
        if self.jobs.is_some() {
            return self.jobs.as_ref().unwrap();
        }
        self.tasks.as_ref().unwrap()
    }
}

#[derive(Serialize, Deserialize)]
struct Json {
    workflow: Workflow,
}

impl DAG {
    /// Reads DAG from a file in [WfCommons json format](https://wfcommons.org/format).
    pub fn from_wfcommons(file: &str, reference_flops: f64) -> Self {
        let json: Json =
            from_str(&std::fs::read_to_string(file).unwrap_or_else(|_| panic!("Can't read file {}", file)))
                .unwrap_or_else(|_| panic!("Can't parse WfCommons json from file {}", file));
        let workflow = json.workflow;
        let machine_speed: HashMap<String, u64> = workflow
            .machines
            .iter()
            .filter(|m| m.cpu.speed.is_some())
            .map(|machine| (machine.name.clone(), machine.cpu.speed.unwrap() * 1000000))
            .collect();
        let mut dag = DAG::new();
        let mut data_items: HashMap<String, usize> = HashMap::new();
        for task in workflow.tasks().iter() {
            let mut task_size = task.runtime;

            if let Some(machine_speed) = task.machine.as_ref().map(|m| machine_speed.get(m)).flatten() {
                task_size *= *machine_speed as f64;
            } else {
                task_size *= reference_flops;
            }

            let task_id = dag.add_task(&task.name, task_size as u64, 0, 1, 1, CoresDependency::Linear);
            for file in task.files.iter() {
                if file.link == "output" {
                    data_items.insert(file.name.clone(), dag.add_task_output(task_id, &file.name, file.size));
                }
            }
        }
        for (task_id, task) in workflow.tasks().iter().enumerate() {
            for file in task.files.iter() {
                if file.link == "input" {
                    if let Some(data_item_id) = data_items.get(&file.name) {
                        dag.add_data_dependency(*data_item_id, task_id);
                    } else {
                        let data_item_id = dag.add_data_item(&file.name, file.size);
                        data_items.insert(file.name.clone(), data_item_id);
                        dag.add_data_dependency(data_item_id, task_id);
                    }
                }
            }
        }
        dag
    }
}
