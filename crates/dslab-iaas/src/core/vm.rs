use std::fmt::{Display, Formatter};
use std::rc::Rc;

use serde::ser::{SerializeStruct, Serializer};
use serde::Serialize;

use crate::core::config::SimulationConfig;
use crate::core::load_model::LoadModel;

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

    pub fn lifetime(&self) -> f64 {
        self.lifetime
    }

    pub fn start_time(&self) -> f64 {
        self.start_time
    }

    pub fn start_duration(&self) -> f64 {
        self.sim_config.vm_start_duration
    }

    pub fn stop_duration(&self) -> f64 {
        self.sim_config.vm_stop_duration
    }

    pub fn set_start_time(&mut self, time: f64) {
        self.start_time = time;
    }

    pub fn set_lifetime(&mut self, lifetime: f64) {
        self.lifetime = lifetime;
    }

    pub fn get_cpu_load(&self, time: f64) -> f64 {
        self.cpu_load_model.get_resource_load(time, time - self.start_time)
    }

    pub fn get_memory_load(&self, time: f64) -> f64 {
        self.memory_load_model.get_resource_load(time, time - self.start_time)
    }
}
