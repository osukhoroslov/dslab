//! Constant HDD power model.

use crate::power::hdd::{HddPowerModel, HddState};

/// A power model using a constant power consumption value independent of the disk state.
#[derive(Clone)]
pub struct ConstantHddPowerModel {
    power: f64,
}

impl ConstantHddPowerModel {
    /// Creates a constant power model.
    ///
    /// * `power` - The power consumption in Watts.
    pub fn new(power: f64) -> Self {
        Self { power }
    }
}

impl HddPowerModel for ConstantHddPowerModel {
    fn get_power(&self, _state: HddState) -> f64 {
        self.power
    }
}
