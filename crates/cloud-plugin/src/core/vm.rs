use std::rc::Rc;

use serde::ser::{Serialize, SerializeStruct, Serializer};

use crate::core::config::SimulationConfig;
use crate::core::load_model::LoadModel;

#[derive(Clone)]
pub struct VirtualMachine {
    pub lifetime: f64,
    pub start_time: f64,
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
            start_time: 0.,
            cpu_load_model,
            memory_load_model,
            sim_config,
        }
    }

    pub fn lifetime(&self) -> f64 {
        self.lifetime
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

    pub fn get_cpu_load(&self, time: f64) -> f64 {
        return self.cpu_load_model.get_resource_load(time, time - self.start_time);
    }

    pub fn get_memory_load(&self, time: f64) -> f64 {
        return self.memory_load_model.get_resource_load(time, time - self.start_time);
    }
}
