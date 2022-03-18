use serde::ser::{Serialize, SerializeStruct, Serializer};

use crate::load_model::LoadModel;

static VM_START_DURATION: f64 = 1.0;
static VM_STOP_DURATION: f64 = 0.5;

pub struct VirtualMachine {
    lifetime: f64,
    start_time: f64,
    cpu_load_model: Box<dyn LoadModel>,
    memory_load_model: Box<dyn LoadModel>,
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
    pub fn new(lifetime: f64, cpu_load_model: Box<dyn LoadModel>, memory_load_model: Box<dyn LoadModel>) -> Self {
        Self {
            lifetime,
            start_time: 0.,
            cpu_load_model,
            memory_load_model,
        }
    }

    pub fn lifetime(&self) -> f64 {
        self.lifetime
    }

    pub fn start_duration(&self) -> f64 {
        VM_START_DURATION
    }

    pub fn stop_duration(&self) -> f64 {
        VM_STOP_DURATION
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
