//! Power consumption models.

use dyn_clone::{clone_trait_object, DynClone};

/// Model for computing power consumption of CPU component.
pub trait CPUPowerModel: DynClone {
    /// Computes the current power consumption.
    ///
    /// * `time` - current simulation time.
    /// * `utilization` - current component utilization (0-1).
    fn get_power(&self, utilization: f64) -> f64;
}

clone_trait_object!(CPUPowerModel);

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
    fn get_power(&self, utilization: f64) -> f64 {
        if utilization == 0. {
            return 0.;
        }
        self.power
    }
}

/// Computes the host power consumption using the provided power modelы.
#[derive(Clone)]
pub struct HostPowerModel {
    cpu_power_model: Option<Box<dyn CPUPowerModel>>,
    other_power: f64,
}

impl HostPowerModel {
    /// Creates host power model. Only CPU power consumption is taken into account.
    pub fn cpu_only(cpu_power_model: Box<dyn CPUPowerModel>) -> Self {
        Self {
            cpu_power_model: Some(cpu_power_model),
            other_power: 0.,
        }
    }

    /// Creates host power model. CPU power consumption is taken into account.
    /// Moreover, other host components consume power too, so, they are represented by
    /// other_power parameter.
    pub fn cpu_and_other(cpu_power_model: Box<dyn CPUPowerModel>, other_power: f64) -> Self {
        Self {
            cpu_power_model: Some(cpu_power_model),
            other_power,
        }
    }

    /// Returns the current power consumption of a physical host.
    pub fn get_power(&self, cpu_util: f64) -> f64 {
        let mut result = 0.;
        if let Some(model) = &self.cpu_power_model {
            result += model.get_power(cpu_util);
        }
        result += self.other_power;
        result
    }
}

impl Default for HostPowerModel {
    fn default() -> Self {
        Self::cpu_only(Box::new(ConstantPowerModel { power: 0. }))
    }
}
