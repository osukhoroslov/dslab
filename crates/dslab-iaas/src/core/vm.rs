//! Representations of virtual machine and its status.

use std::fmt::{Display, Formatter};
use std::rc::Rc;

use serde::ser::{SerializeStruct, Serializer};
use serde::Serialize;

use crate::core::config::SimulationConfig;
use crate::core::load_model::LoadModel;

/// Status of virtual machine.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum VmStatus {
    Initializing,
    Running,
    Finished,
    Migrating,
    FailedToAllocate,
}

impl Display for VmStatus {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            VmStatus::Initializing => write!(f, "initializing"),
            VmStatus::Running => write!(f, "running"),
            VmStatus::Finished => write!(f, "finished"),
            VmStatus::Migrating => write!(f, "migrating"),
            VmStatus::FailedToAllocate => write!(f, "failed_to_allocate"),
        }
    }
}

/// Represents virtual machine (VM).
///
// VM is characterized by its ID, resource requirements (vCPUs and memory), start time, lifetime and load models.
// The latter model the actual resource utilization of VM in time, which may significantly differ from the VM's
// resource requirements.
#[derive(Clone)]
pub struct VirtualMachine {
    pub id: u32,
    pub cpu_usage: u32,
    pub memory_usage: u64,
    pub allocation_start_time: f64,
    lifetime: f64,
    start_time: f64,
    cpu_load_model: Box<dyn LoadModel>,
    memory_load_model: Box<dyn LoadModel>,
    sim_config: Rc<SimulationConfig>,
}

///
impl Serialize for VirtualMachine {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("VirtualMachine", 1)?;
        state.serialize_field("lifetime", &self.lifetime)?;
        state.end()
    }
}

impl VirtualMachine {
    /// Creates virtual machine with specified parameters.
    pub fn new(
        id: u32,
        cpu_usage: u32,
        memory_usage: u64,
        allocation_start_time: f64,
        lifetime: f64,
        cpu_load_model: Box<dyn LoadModel>,
        memory_load_model: Box<dyn LoadModel>,
        sim_config: Rc<SimulationConfig>,
    ) -> Self {
        Self {
            id,
            cpu_usage,
            memory_usage,
            allocation_start_time,
            lifetime,
            start_time: -1.,
            cpu_load_model,
            memory_load_model,
            sim_config,
        }
    }

    /// Returns VM lifetime (it is updated when VM is migrated).
    pub fn lifetime(&self) -> f64 {
        self.lifetime
    }

    /// Returns VM start time (it is updated when VM is migrated).
    pub fn start_time(&self) -> f64 {
        self.start_time
    }

    /// Returns VM start duration (the value is taken from the simulation config).
    pub fn start_duration(&self) -> f64 {
        self.sim_config.vm_start_duration
    }

    /// Returns VM stop duration (the value is taken from the simulation config).
    pub fn stop_duration(&self) -> f64 {
        self.sim_config.vm_stop_duration
    }

    /// Sets VM start time. Can be called multiple times due to VM migration.
    pub fn set_start_time(&mut self, time: f64) {
        self.start_time = time;
    }

    /// Changes VM lifetime. It is called only due to VM migration.
    pub fn set_lifetime(&mut self, lifetime: f64) {
        self.lifetime = lifetime;
    }

    /// Returns the current CPU load of VM by invoking the CPU load model.
    pub fn get_cpu_load(&self, time: f64) -> f64 {
        self.cpu_load_model.get_resource_load(time, time - self.start_time)
    }

    /// Returns the current memory load of VM by invoking the memory load model.
    pub fn get_memory_load(&self, time: f64) -> f64 {
        self.memory_load_model.get_resource_load(time, time - self.start_time)
    }
}
