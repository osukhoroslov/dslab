//! Constant CPU power model.

use crate::power::cpu::CpuPowerModel;

/// A power model using a constant power consumption value.
#[derive(Clone)]
pub struct ConstantPowerModel {
    power: f64,
}

impl ConstantPowerModel {
    /// Creates a constant power model.
    ///
    /// * `power` - The power consumption in W.
    pub fn new(power: f64) -> Self {
        Self { power }
    }
}

impl CpuPowerModel for ConstantPowerModel {
    fn get_power(&self, _utilization: f64) -> f64 {
        self.power
    }
}
