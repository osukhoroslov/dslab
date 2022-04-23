use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::from_str;

use compute::multicore::CoresDependency;

use crate::dag::*;

#[derive(Serialize, Deserialize)]
struct File {
    link: String,
    name: String,
    size: u64,
}

#[derive(Serialize, Deserialize)]
struct Job {
    name: String,
    runtime: f64,
    files: Vec<File>,
}

#[derive(Serialize, Deserialize)]
struct Workflow {
    jobs: Vec<Job>,
}

#[derive(Serialize, Deserialize)]
struct Json {
    workflow: Workflow,
}

impl DAG {
    pub fn from_wfcommons(file: &str, flops_coefficient: f64) -> Self {
        let json: Json = from_str(&std::fs::read_to_string(file).expect(&format!("Can't read file {}", file)))
            .expect(&format!("Can't parse WfCommons json from file {}", file));
        let workflow = json.workflow;
        let mut dag = DAG::new();
        let mut data_items: HashMap<String, usize> = HashMap::new();
        for job in workflow.jobs.iter() {
            let task_id = dag.add_task(
                &job.name,
                (job.runtime * flops_coefficient) as u64,
                0,
                1,
                1,
                CoresDependency::Linear,
            );
            for file in job.files.iter() {
                if file.link == "output" {
                    data_items.insert(file.name.clone(), dag.add_task_output(task_id, &file.name, file.size));
                }
            }
        }
        for (task_id, job) in workflow.jobs.iter().enumerate() {
            for file in job.files.iter() {
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
