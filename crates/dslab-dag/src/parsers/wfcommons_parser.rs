use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::Path;

use rand::prelude::*;
use rand_pcg::Pcg64;
use serde::{Deserialize, Serialize};
use serde_json::from_str;

use dslab_compute::multicore::CoresDependency;

use crate::dag::*;
use crate::parsers::config::ParserConfig;

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
    // File size in KB (schema v1.3) or in bytes (schema v1.4)
    #[serde(alias = "sizeInBytes")]
    size: u64,
    // Whether it is an input or output data
    link: String,
}

#[derive(Serialize, Deserialize)]
struct Task {
    // Task name
    name: String,
    // Task runtime in seconds
    #[serde(alias = "runtimeInSeconds")]
    runtime: f64,
    // Number of cores required by the task
    cores: Option<f64>, // some files specify cores as "1.0"
    // Memory (resident set) size of the process in KB (schema v1.3) or in bytes (schema v1.4)
    //
    // TODO: Uncomment after this issue is fixed: https://github.com/wfcommons/makeflow-instances/issues/1
    //
    // #[serde(alias = "memoryInBytes")]
    // memory: Option<u64>,
    //
    // Until then we have to resort to using both `memory_in_bytes` and `memory`.
    #[serde(rename = "memoryInBytes")]
    memory_in_bytes: Option<u64>,
    memory: Option<u64>,
    // Task input/output files
    files: Vec<File>,
    // Machine used for task execution
    machine: Option<String>,
    // Parent tasks.
    parents: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Workflow {
    jobs: Option<Vec<Task>>,  // schema v1.2
    tasks: Option<Vec<Task>>, // schema v1.3
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
struct Wms {
    name: String,
}

#[derive(Serialize, Deserialize)]
struct Json {
    #[serde(rename = "schemaVersion")]
    schema_version: String,
    workflow: Workflow,
    wms: Wms,
}

impl DAG {
    /// Reads DAG from a file in [WfCommons json format](https://wfcommons.org/format).
    pub fn from_wfcommons<P: AsRef<Path>>(file: P, config: &ParserConfig) -> Self {
        let mut hasher = DefaultHasher::new();
        let str =
            &std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file.as_ref().display()));
        str.hash(&mut hasher);
        let hash = hasher.finish();
        let mut rand = Pcg64::seed_from_u64(hash + config.seed.unwrap_or(123));

        let json: Json = from_str(str).unwrap_or_else(|e| {
            panic!(
                "Can't parse WfCommons json from file {}: {}",
                file.as_ref().display(),
                e
            )
        });
        let schema_version = json.schema_version;
        let wms = json.wms.name;
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
        let mut stage_mem: HashMap<String, u64> = HashMap::new();
        let mut stage_cores: HashMap<String, u32> = HashMap::new();
        let mut task_ids: HashMap<String, usize> = HashMap::new();
        for task in workflow.tasks().iter() {
            let stage = task.name.split('_').next().unwrap().to_string();

            let cores = if let Some(cores_conf) = &config.generate_cores {
                if cores_conf.regular && stage_cores.contains_key(&stage) {
                    stage_cores[&stage]
                } else {
                    let cores = rand.gen_range(cores_conf.min..=cores_conf.max);
                    if cores_conf.regular {
                        stage_cores.insert(stage.clone(), cores);
                    }
                    cores
                }
            } else {
                task.cores.unwrap_or(1.) as u32
            };

            let memory = if config.ignore_memory {
                0
            } else if let Some(mem_conf) = &config.generate_memory {
                if mem_conf.regular && stage_mem.contains_key(&stage) {
                    stage_mem[&stage]
                } else {
                    let memory = (rand.gen_range(mem_conf.min..=mem_conf.max) as f64 / 1000.).ceil() as u64 * 1000;
                    if mem_conf.regular {
                        stage_mem.insert(stage, memory);
                    }
                    memory
                }
            } else {
                // TODO: Uncomment after this issue is fixed: https://github.com/wfcommons/makeflow-instances/issues/1
                // let memory = task.memory.unwrap_or(0);
                // if schema_version == "1.4" {
                //     (memory as f64 / 1e6).ceil() as u64 // convert bytes to MB (round up to nearest)
                // } else {
                //     (memory as f64 / 1e3).ceil() as u64 // convert KB to MB (round up to nearest)
                // }
                if let Some(memory) = task.memory_in_bytes {
                    (memory as f64 / 1e6).ceil() as u64 // convert bytes to MB (round up to nearest)
                } else {
                    (task.memory.unwrap_or(0) as f64 / 1e3).ceil() as u64 // convert KB to MB (round up to nearest)
                }
            };

            let mut flops = task.runtime * cores as f64;
            if let Some(machine_speed) = task.machine.as_ref().and_then(|m| machine_speed.get(m)) {
                flops *= *machine_speed;
            } else {
                flops *= config.reference_speed;
            }

            let task_id = dag.add_task(&task.name, flops, memory, cores, cores, CoresDependency::Linear);
            task_ids.insert(task.name.clone(), task_id);
            for file in task.files.iter() {
                if file.link == "output" {
                    data_items.insert(
                        file.name.clone(),
                        dag.add_task_output(task_id, &file.name, file_size_in_mb(file.size, &schema_version, &wms)),
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
                        let data_item_id =
                            dag.add_data_item(&file.name, file_size_in_mb(file.size, &schema_version, &wms));
                        data_items.insert(file.name.clone(), data_item_id);
                        dag.add_data_dependency(data_item_id, task_id);
                    }
                }
            }
            for parent in task.parents.iter() {
                let data_item_id = dag.add_task_output(task_ids[parent], &format!("{} -> {}", parent, task.name), 0.);
                dag.add_data_dependency(data_item_id, task_id);
            }
        }
        dag
    }
}

fn file_size_in_mb(size: u64, schema_version: &String, wms: &String) -> f64 {
    // Pegasus instances pre 1.4 have file sizes in bytes by mistake!
    // See https://github.com/wfcommons/pegasus-instances/issues/1
    //
    // TODO: Makeflow 1.4 instances still have sizes in KB, until this issue is fixed:
    // https://github.com/wfcommons/makeflow-instances/issues/1
    if (schema_version == "1.4" && wms != "Makeflow") || wms == "Pegasus" {
        size as f64 / 1e6 // convert bytes to MB
    } else {
        size as f64 / 1e3 // convert KB to MB
    }
}
