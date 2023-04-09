//! Constant hard drive power model.

use crate::power::hard_drive::{HardDrivePowerModel, HardDriveState};

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

impl HardDrivePowerModel for ConstantPowerModel {
    fn get_power(&self, _hdd_state: HardDriveState) -> f64 {
        self.power
    }
}
