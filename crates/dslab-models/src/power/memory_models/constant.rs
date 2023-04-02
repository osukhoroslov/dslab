//! Constant memory power model.

use crate::power::memory::MemoryPowerModel;

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

impl MemoryPowerModel for ConstantPowerModel {
    fn get_power(&self, _utilization: f64) -> f64 {
        self.power
    }

    fn get_power_adv(&self, _read_util: f64, _write_util: f64) -> f64 {
        self.power
    }
}
