//! Power consumption models.

use dyn_clone::{clone_trait_object, DynClone};

/// Model for computing power consumption of CPU component.
pub trait CPUPowerModel: DynClone {
    /// Computes the current power consumption.
    ///
    /// * `time` - current simulation time.
    /// * `utilization` - current component utilization (0-1).
    fn get_power(&self, time: f64, utilization: f64) -> f64;
}

/// Model for computing power consumption of RAM component.
pub trait MemoryPowerModel: DynClone {
    /// Computes the current power consumption.
    ///
    /// * `time` - current simulation time.
    /// * `utilization` - current component utilization (0-1).
    fn get_power(&self, time: f64, utilization: f64) -> f64;
}

clone_trait_object!(CPUPowerModel);
clone_trait_object!(MemoryPowerModel);

/// A power model with constant power consumption value.
#[derive(Clone)]
pub struct ConstantPowerModel {
    power: f64,
}

impl ConstantPowerModel {
    /// Creates constant power model with specified parameters.
    ///
    /// * `power` - Power consumption value.
    pub fn new(power: f64) -> Self {
        Self { power }
    }
}

impl CPUPowerModel for ConstantPowerModel {
    fn get_power(&self, _time: f64, utilization: f64) -> f64 {
        if utilization == 0. {
            return 0.;
        }
        self.power
    }
}

/// Computes the host power consumption using the provided power model.
#[derive(Clone)]
pub struct HostPowerModel {
    cpu_power_model: Option<Box<dyn CPUPowerModel>>,
    memory_power_model: Option<Box<dyn MemoryPowerModel>>,
}

impl HostPowerModel {
    /// Creates host power model. Only CPU power consumption is taken into account.
    pub fn cpu_only(cpu_power_model: Box<dyn CPUPowerModel>) -> Self {
        Self {
            cpu_power_model: Some(cpu_power_model),
            memory_power_model: None,
        }
    }

    /// Creates host power model. Only memory power consumption is taken into account.
    pub fn memory_only(memory_power_model: Box<dyn MemoryPowerModel>) -> Self {
        Self {
            cpu_power_model: None,
            memory_power_model: Some(memory_power_model),
        }
    }

    /// Returns the current power consumption of a physical host.
    pub fn get_power(&self, time: f64, cpu_util: f64, memory_util: f64) -> f64 {
        let mut result = 0.;
        if self.cpu_power_model.is_some() {
            result += self.cpu_power_model.as_ref().unwrap().get_power(time, cpu_util)
        }
        if self.memory_power_model.is_some() {
            result += self.memory_power_model.as_ref().unwrap().get_power(time, memory_util)
        }
        result
    }
}

impl Default for HostPowerModel {
    fn default() -> Self {
        Self::cpu_only(Box::new(ConstantPowerModel { power: 0. }))
    }
}
