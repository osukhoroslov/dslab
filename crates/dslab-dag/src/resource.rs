//! Resource model.

use std::cell::RefCell;
use std::path::Path;
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
    /// CPU speed in Gflop/s.
    pub speed: f64,
    pub cores_available: u32,
    /// Memory size in MB.
    pub memory_available: u64,
}

/// Contains parameters of computing resource, can be used later to create a compute resource instance.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResourceConfig {
    pub name: String,
    /// CPU speed in Gflop/s.
    pub speed: f64,
    pub cores: u32,
    /// Memory size in MB.
    pub memory: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct Resources {
    resources: Vec<ResourceConfig>,
}

/// Loads resources from YAML file.
///
/// Configuration file example:
/// https://github.com/osukhoroslov/dslab/blob/main/examples/dag-demo/systems/cluster-het-4-32cores.yaml
pub fn load_resources<P: AsRef<Path>>(file: P, sim: &mut Simulation) -> Vec<Resource> {
    let resources = read_resources(&file);
    let mut result: Vec<Resource> = Vec::new();
    for resource in resources.into_iter() {
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

/// Reads resources from YAML file into simple structs without creating resource instances.
pub fn read_resources<P: AsRef<Path>>(file: P) -> Vec<ResourceConfig> {
    let resources: Resources = serde_yaml::from_str(
        &std::fs::read_to_string(&file).unwrap_or_else(|_| panic!("Can't read file {}", file.as_ref().display())),
    )
    .unwrap_or_else(|_| panic!("Can't parse YAML from file {}", file.as_ref().display()));
    resources.resources
}
