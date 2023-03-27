use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::from_str;

use dslab_compute::multicore::CoresDependency;

use crate::dag::*;

#[derive(Serialize, Deserialize)]
struct Cpu {
    // CPU speed in MHz
    speed: Option<u64>,
}

#[derive(Serialize, Deserialize)]
struct Machine {
    // Machine node name
    #[serde(rename = "nodeName")]
    name: String,
    // Machine's CPU information
    cpu: Cpu,
}

#[derive(Serialize, Deserialize)]
struct File {
    // A human-readable name for the file
    name: String,
    // File size in KB
    size: u64,
    // Whether it is an input or output data
    link: String,
}

#[derive(Serialize, Deserialize)]
struct Task {
    // Task name
    name: String,
    // Task runtime in seconds
    runtime: f64,
    // Number of cores required by the task
    cores: Option<f64>, // some files specify cores as "1.0"
    // Memory (resident set) size of the process in KB
    memory: Option<u64>,
    // Task input/output files
    files: Vec<File>,
    // Machine used for task execution
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
    ///
    /// Reference machine speed should be in Gflop/s.
    pub fn from_wfcommons<P: AsRef<Path>>(file: P, reference_speed: f64) -> Self {
        let json: Json = from_str(
            &std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file.as_ref().display())),
        )
        .unwrap_or_else(|e| {
            panic!(
                "Can't parse WfCommons json from file {}: {}",
                file.as_ref().display(),
                e
            )
        });
        let workflow = json.workflow;
        let machine_speed: HashMap<String, f64> = workflow
            .machines
            .iter()
            .filter(|m| m.cpu.speed.is_some())
            // machine.cpu.speed in WfCommons format actually refers to CPU speed in MHz,
            // but it seems everyone use it as Mflop/s too...
            // here we convert it to Gflop/s
            .map(|machine| (machine.name.clone(), machine.cpu.speed.unwrap() as f64 / 1000.))
            .collect();
        let mut dag = DAG::new();
        let mut data_items: HashMap<String, usize> = HashMap::new();
        for task in workflow.tasks().iter() {
            let mut flops = task.runtime;
            if let Some(machine_speed) = task.machine.as_ref().and_then(|m| machine_speed.get(m)) {
                flops *= *machine_speed;
            } else {
                flops *= reference_speed;
            }

            let task_id = dag.add_task(
                &task.name,
                flops,
                (task.memory.unwrap_or(0) as f64 / 1000.).ceil() as u64, // convert KB to MB (round up to nearest)
                1,
                task.cores.unwrap_or(1.) as u32,
                CoresDependency::Linear,
            );
            for file in task.files.iter() {
                if file.link == "output" {
                    data_items.insert(
                        file.name.clone(),
                        dag.add_task_output(
                            task_id,
                            &file.name,
                            file.size as f64 / 1000., // convert KB to MB
                        ),
                    );
                }
            }
        }
        for (task_id, task) in workflow.tasks().iter().enumerate() {
            for file in task.files.iter() {
                if file.link == "input" {
                    if let Some(data_item_id) = data_items.get(&file.name) {
                        dag.add_data_dependency(*data_item_id, task_id);
                    } else {
                        let data_item_id = dag.add_data_item(
                            &file.name,
                            file.size as f64 / 1000., // convert KB to MB
                        );
                        data_items.insert(file.name.clone(), data_item_id);
                        dag.add_data_dependency(data_item_id, task_id);
                    }
                }
            }
        }
        dag
    }
}
