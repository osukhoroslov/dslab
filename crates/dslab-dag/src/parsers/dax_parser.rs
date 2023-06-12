use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_xml_rs::from_str;

use dslab_compute::multicore::CoresDependency;

use crate::dag::*;
use crate::parsers::config::ParserConfig;

#[derive(Debug, Serialize, Deserialize)]
struct File {
    #[serde(rename = "file")]
    name: String,
    link: String,
    size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Job {
    id: String,
    name: String,
    runtime: f64,
    #[serde(rename = "uses")]
    files: Vec<File>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename = "adag")]
#[allow(clippy::upper_case_acronyms)]
struct DAX {
    #[serde(rename = "job")]
    jobs: Vec<Job>,
}

impl DAG {
    /// Reads DAG from a file in [DAX format](https://pegasus.isi.edu/documentation/development/schemas.html).
    pub fn from_dax<P: AsRef<Path>>(file: P, config: &ParserConfig) -> Self {
        let dax: DAX = from_str(
            &std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file.as_ref().display())),
        )
        .unwrap_or_else(|_| panic!("Can't parse DAX from file {}", file.as_ref().display()));
        let mut dag = DAG::new();
        let mut data_items: HashMap<String, usize> = HashMap::new();
        for job in dax.jobs.iter() {
            let task_id = dag.add_task(
                &format!("{}_{}", job.name, job.id),
                job.runtime * config.reference_speed,
                0, // task memory consumption is not present in DAX files
                1, // cores info is not present in DAX files
                1, // assuming all tasks require a single core
                CoresDependency::Linear,
            );
            for file in job.files.iter() {
                if file.link == "output" {
                    data_items.insert(
                        file.name.clone(),
                        dag.add_task_output(task_id, &file.name, file.size as f64 / 1e6),
                    );
                }
            }
        }
        for (task_id, job) in dax.jobs.iter().enumerate() {
            for file in job.files.iter() {
                if file.link == "input" {
                    if let Some(data_item_id) = data_items.get(&file.name) {
                        dag.add_data_dependency(*data_item_id, task_id);
                    } else {
                        let data_item_id = dag.add_data_item(&file.name, file.size as f64 / 1e6);
                        data_items.insert(file.name.clone(), data_item_id);
                        dag.add_data_dependency(data_item_id, task_id);
                    }
                }
            }
        }
        dag
    }
}
