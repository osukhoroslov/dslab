//! Resource model.

use std::cell::RefCell;
use std::rc::Rc;

use serde::{Deserialize, Serialize};

use dslab_core::component::Id;
use dslab_core::simulation::Simulation;

use dslab_compute::multicore::*;

/// Represents a computing resource that can execute DAG tasks.
///
/// Described by the number of CPU cores, their speed in flop/s and amount of memory.
///
/// Supports execution of parallel tasks. The modeling of task execution is implemented by means of the
/// [multicore](https://github.com/osukhoroslov/dslab/tree/main/crates/dslab-compute/src/multicore.rs)
/// compute model from the dslab-compute crate.
#[derive(Clone)]
pub struct Resource {
    pub id: Id,
    pub name: String,
    pub compute: Rc<RefCell<Compute>>,
    pub speed: u64,
    pub cores_available: u32,
    pub memory_available: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct YamlResource {
    name: String,
    speed: u64,
    cores: u32,
    memory: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Resources {
    resources: Vec<YamlResource>,
}

/// Loads resources from YAML file.
///
/// Resources file example: https://github.com/osukhoroslov/dslab/blob/main/examples/dag/resources/cluster1.yaml.
pub fn load_resources(file: &str, sim: &mut Simulation) -> Vec<Resource> {
    let resources: Resources =
        serde_yaml::from_str(&std::fs::read_to_string(file).unwrap_or_else(|_| panic!("Can't read file {}", file)))
            .unwrap_or_else(|_| panic!("Can't parse YAML from file {}", file));
    let mut result: Vec<Resource> = Vec::new();
    for resource in resources.resources.into_iter() {
        let compute = Rc::new(RefCell::new(Compute::new(
            resource.speed,
            resource.cores,
            resource.memory,
            sim.create_context(&resource.name),
        )));
        let id = sim.add_handler(&resource.name, compute.clone());
        result.push(Resource {
            id,
            name: resource.name,
            compute,
            speed: resource.speed,
            cores_available: resource.cores,
            memory_available: resource.memory,
        });
    }
    result
}
