//! Host power model.

use crate::power::cpu::CpuPowerModel;
use crate::power::cpu_models::constant::ConstantPowerModel;

/// A model for estimating the power consumption of a physical host.
///
/// The host power consumption is modeled as two parts:
/// - CPU consumption estimated using the provided CPU power model
/// - consumption of other host components modeled as a fixed value
#[derive(Clone)]
pub struct HostPowerModel {
    cpu_power_model: Option<Box<dyn CpuPowerModel>>,
    other_power: f64,
}

impl HostPowerModel {
    /// Creates the host power model using the provided CPU power model and power usage of other components.
    pub fn new(cpu_power_model: Box<dyn CpuPowerModel>, other_power: f64) -> Self {
        Self {
            cpu_power_model: Some(cpu_power_model),
            other_power,
        }
    }

    /// Creates the host power model using only the CPU power consumption part.
    pub fn cpu_only(cpu_power_model: Box<dyn CpuPowerModel>) -> Self {
        Self {
            cpu_power_model: Some(cpu_power_model),
            other_power: 0.,
        }
    }

    /// Returns the power consumption of a host (in W) based on CPU utilization.
    ///
    /// CPU utilization should be passed as a float in 0.0-1.0 range.
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
        Self::cpu_only(Box::new(ConstantPowerModel::new(0.)))
    }
}
