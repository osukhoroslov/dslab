//! Power consumption models.

use dyn_clone::{clone_trait_object, DynClone};

/// Model for computing power consumption of some component.
pub trait PowerModel: DynClone {
    /// Computes the current power consumption.
    ///
    /// * `time` - current simulation time.
    /// * `utilization` - current component utilization (0-1).
    fn get_power(&self, time: f64, utilization: f64) -> f64;
}

clone_trait_object!(PowerModel);

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

impl PowerModel for ConstantPowerModel {
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
    cpu_power_model: Box<dyn PowerModel>,
    gpu_power_model: Box<dyn PowerModel>,
    memory_power_model: Box<dyn PowerModel>,
    network_power_model: Box<dyn PowerModel>,
}

impl HostPowerModel {
    /// Creates host power model.
    ///
    /// All components power consumptions are initialized as zero constants
    pub fn new() -> Self {
        Self {
            cpu_power_model: Box::new(ConstantPowerModel { power: 0. }),
            memory_power_model: Box::new(ConstantPowerModel { power: 0. }),
            gpu_power_model: Box::new(ConstantPowerModel { power: 0. }),
            network_power_model: Box::new(ConstantPowerModel { power: 0. }),
        }
    }

    /// Returns the current power consumption of a physical host.
    pub fn get_power(&self, time: f64, cpu_util: f64) -> f64 {
        self.cpu_power_model.get_power(time, cpu_util)
            + self.memory_power_model.get_power(time, cpu_util)
            + self.gpu_power_model.get_power(time, cpu_util)
            + self.network_power_model.get_power(time, cpu_util)
    }

    pub fn cpu_power_model(mut self, power_model: Box<dyn PowerModel>) -> Self {
        self.cpu_power_model = power_model;
        self
    }

    pub fn memory_power_model(mut self, power_model: Box<dyn PowerModel>) -> Self {
        self.memory_power_model = power_model;
        self
    }

    pub fn gpu_power_model(mut self, power_model: Box<dyn PowerModel>) -> Self {
        self.gpu_power_model = power_model;
        self
    }

    pub fn network_power_model(mut self, power_model: Box<dyn PowerModel>) -> Self {
        self.network_power_model = power_model;
        self
    }
}

impl Default for HostPowerModel {
    fn default() -> Self {
        Self::new()
    }
}
