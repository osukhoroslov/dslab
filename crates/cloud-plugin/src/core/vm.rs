use std::fmt::{Display, Formatter};
use std::rc::Rc;

use serde::ser::{SerializeStruct, Serializer};
use serde::Serialize;

use crate::core::config::SimulationConfig;
use crate::core::load_model::LoadModel;

#[derive(Clone, PartialEq, Serialize)]
pub enum VmStatus {
    Initializing,
    Running,
    Deactivated,
    Migrating,
}

impl Display for VmStatus {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            VmStatus::Initializing => write!(f, "initializing"),
            VmStatus::Running => write!(f, "running"),
            VmStatus::Deactivated => write!(f, "deactivated"),
            VmStatus::Migrating => write!(f, "migrating"),
        }
    }
}

#[derive(Clone)]
pub struct VirtualMachine {
    lifetime: f64,
    start_time: f64,
    status: VmStatus,
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
        lifetime: f64,
        cpu_load_model: Box<dyn LoadModel>,
        memory_load_model: Box<dyn LoadModel>,
        sim_config: Rc<SimulationConfig>,
    ) -> Self {
        Self {
            lifetime,
            start_time: -1.,
            cpu_load_model,
            memory_load_model,
            sim_config,
            status: VmStatus::Initializing,
        }
    }

    pub fn lifetime(&self) -> f64 {
        self.lifetime
    }

    pub fn start_time(&self) -> f64 {
        self.start_time
    }

    pub fn status(&self) -> &VmStatus {
        &self.status
    }

    pub fn start_duration(&self) -> f64 {
        self.sim_config.vm_start_duration
    }

    pub fn stop_duration(&self) -> f64 {
        self.sim_config.vm_stop_duration
    }

    pub fn set_start_time(&mut self, time: f64) {
        // VM start time is set only once!
        if self.start_time == -1. {
            self.start_time = time;
        }
    }

    pub fn set_status(&mut self, status: VmStatus) {
        self.status = status;
    }

    pub fn get_cpu_load(&self, time: f64) -> f64 {
        if self.status == VmStatus::Running {
            self.cpu_load_model.get_resource_load(time, time - self.start_time)
        } else {
            0.
        }
    }

    pub fn get_memory_load(&self, time: f64) -> f64 {
        if self.status == VmStatus::Running {
            self.memory_load_model.get_resource_load(time, time - self.start_time)
        } else {
            0.
        }
    }
}
