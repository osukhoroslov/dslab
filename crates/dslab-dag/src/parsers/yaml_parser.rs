use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_yaml::Value;

use dslab_compute::multicore::CoresDependency;

use crate::dag::DAG;
use crate::parsers::config::ParserConfig;

fn one() -> u32 {
    1
}
fn zero() -> u64 {
    0
}

#[derive(Debug, Serialize, Deserialize)]
struct DataItem {
    name: String,
    // expected unit: MB
    size: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Task {
    name: String,
    // expected unit: Gflops
    flops: f64,
    #[serde(default = "zero")]
    // expected unit: MB
    memory: u64,
    #[serde(default = "one")]
    min_cores: u32,
    #[serde(default = "one")]
    max_cores: u32,
    cores_dependency: Option<Value>,
    #[serde(default)]
    inputs: Vec<String>,
    outputs: Vec<DataItem>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Yaml {
    tasks: Vec<Task>,
    #[serde(default = "Vec::new")]
    inputs: Vec<DataItem>,
}

impl DAG {
    /// Reads DAG from a file in
    /// [YAML format](https://github.com/osukhoroslov/dslab/blob/main/examples/dag-demo/dags/diamond.yaml).
    pub fn from_yaml<P: AsRef<Path>>(file: P, config: &ParserConfig) -> Self {
        let yaml: Yaml = serde_yaml::from_str(
            &std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file.as_ref().display())),
        )
        .unwrap_or_else(|e| panic!("Can't parse YAML from file {}: {e:?}", file.as_ref().display()));
        let mut dag = DAG::new();
        let mut data_items: HashMap<String, usize> = HashMap::new();
        for data_item in yaml.inputs.iter() {
            data_items.insert(
                data_item.name.clone(),
                dag.add_data_item(&data_item.name, data_item.size),
            );
        }
        for task in yaml.tasks.iter() {
            let task_id = dag.add_task(
                &task.name,
                task.flops,
                if !config.ignore_memory { task.memory } else { 0 },
                task.min_cores,
                task.max_cores,
                match &task.cores_dependency {
                    Some(Value::Number(number)) => CoresDependency::LinearWithFixed {
                        fixed_part: number.as_f64().unwrap(),
                    },
                    _ => CoresDependency::Linear,
                },
            );
            for output in task.outputs.iter() {
                data_items.insert(
                    output.name.clone(),
                    dag.add_task_output(task_id, &output.name, output.size),
                );
            }
        }
        for (task_id, task) in yaml.tasks.iter().enumerate() {
            for input in task.inputs.iter() {
                dag.add_data_dependency(*data_items.get(input).unwrap(), task_id);
            }
        }
        dag
    }
}
